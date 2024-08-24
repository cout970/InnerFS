use fs::File;
use std::fmt::{Display, Formatter};
use crate::config::{read_config, StorageOption};
use crate::encryption::EncryptedObjectStorage;
use crate::obj_storage::{FsObjectStorage, ObjectStorage};
use crate::proxy_fs::ProxyFileSystem;
use crate::s3::S3ObjectStorage;
use crate::sql::{SQL};
use crate::sqlar::SqlarObjectStorage;
use crate::storage_interface::StorageInterface;
use env_logger::Env;
use log::{error, info, warn};
use std::{env, fs};
use std::ffi::OsStr;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};
use anyhow::Context;

mod config;
mod sql;
mod proxy_fs;
mod sql_fs;
mod storage;
mod obj_storage;
mod storage_interface;
mod sqlar;
mod s3;
mod encryption;
mod fs_tree;

use crate::sql_fs::SqlFileSystem;
use clap::{Parser, Subcommand, ValueEnum};
use flate2::{write::GzEncoder, Compression};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};
use crate::fs_tree::{FsTree, FsTreeKind};

/// Utility to mount a shadow filesystem, supports encryption and multiple storage backends: S3, Sqlar and FileSystem
#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Sets a custom config file
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// Turn debugging information on
    #[arg(short, long, default_value_t = false)]
    debug: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// mount the filesystem
    Mount,
    /// delete all data stored
    Nuke {
        /// Force the deletion without asking for confirmation
        #[arg(short, long, default_value_t = false)]
        force: bool,
    },
    /// Export the file metadata index to a file
    ExportIndex {
        /// export format: json or yaml
        #[arg(short, long, value_enum, default_value_t = IndexExportFormat::Json)]
        format: IndexExportFormat,
    },
    /// Export the whole filesystem to a file
    ExportFiles {
        /// export format: tar or zip
        #[arg(short, long, value_enum, default_value_t = FileExportFormat::Directory)]
        format: FileExportFormat,

        /// Export path
        #[arg(short, long, value_name = "FILE")]
        path: PathBuf,
    },
    // Generate a default config file
    GenerateConfig,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum IndexExportFormat {
    Json,
    Yaml,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum FileExportFormat {
    Directory,
    Tar,
    Zip,
}

fn main() {
    let cli = Cli::parse();

    let default_log_filter = if cli.debug { "debug" } else { "info" };
    env_logger::Builder::from_env(Env::default().default_filter_or(default_log_filter)).init();

    let config_path = cli.config.or_else(|| Some(PathBuf::from("./config.yml"))).unwrap();

    // This needs to be done before the check for the config file
    if let Some(Commands::GenerateConfig) = &cli.command {
        if fs::metadata(&config_path).is_ok() {
            warn!("Config file already exists at {:?}, Type 'yes' or 'y' to override this file", &config_path);
            if !ask_for_confirmation() {
                info!("Operation cancelled");
                return;
            }
        }

        let data = include_str!("./default_config.yml");
        fs::write(&config_path, data).expect("Unable to write config file");
        info!("Config file generated at {:?}", config_path);
        return;
    }

    if fs::metadata(&config_path).is_err() {
        let program_name = env::args().next()
            .as_ref()
            .map(Path::new)
            .and_then(Path::file_name)
            .and_then(OsStr::to_str)
            .map(String::from)
            .unwrap();

        error!("Config file not found at {:?}, try './{} generate-config'", &config_path, program_name);
        return;
    }

    info!("Starting");
    let config = read_config(&config_path).expect("Unable to read config");

    info!("Config loaded");
    let sql = Rc::new(SQL::open(&config.database_file));

    info!("Database opened");
    // Select the appropriate storage backend
    let mut obj_storage: Box<dyn ObjectStorage> = match config.storage_backend {
        StorageOption::FileSystem => {
            Box::new(FsObjectStorage {
                base_path: PathBuf::from(&config.blob_storage),
            })
        }
        StorageOption::Sqlar => {
            Box::new(SqlarObjectStorage {
                sql: sql.clone(),
            })
        }
        StorageOption::S3 => {
            Box::new(S3ObjectStorage::new(config.clone()))
        }
    };

    // Apply encryption if a key is provided
    if !config.encryption_key.is_empty() {
        obj_storage = Box::new(EncryptedObjectStorage::new(config.clone(), obj_storage));
    }

    // Wrap the storage backend in a StorageInterface, which provides a higher-level API
    let storage = Box::new(StorageInterface::new(config.clone(), obj_storage));
    let fs = SqlFileSystem::new(sql, config.clone(), storage);

    let cmd = cli.command.unwrap_or_else(|| Commands::Mount);

    match cmd {
        Commands::Mount => mount(fs),
        Commands::Nuke { force } => nuke(fs, force),
        Commands::ExportIndex { format } => export_index(fs, format).unwrap(),
        Commands::ExportFiles { format, path } => export_files(fs, format, path).unwrap(),
        Commands::GenerateConfig => unreachable!()
    }
}

fn mount(fs: SqlFileSystem) {
    let mount_point = fs.config.mount_point.clone();

    // Create a FUSE proxy filesystem to access the StorageInterface
    let proxy = ProxyFileSystem::new(fs);

    // Try to unmount the filesystem, it may be already mounted form a previous run
    // This must be performed before trying to check if the file exists
    info!("Attempting to unmount {} before trying to mount", &mount_point);
    let _ = Command::new("umount").arg(&mount_point).status();

    // Check if the mount point exists and is a directory
    let stat = fs::metadata(&mount_point).expect("Mount point does not exist");
    if !stat.is_dir() {
        panic!("Mount point is not a directory");
    }

    info!("Mounting filesystem at {}", &mount_point);
    fuse::mount(proxy, &mount_point, &[]).expect("Unable to mount filesystem");

    info!("Folder was unmounted successfully, exiting");
}

fn nuke(mut fs: SqlFileSystem, force: bool) {
    if !force {
        warn!("Are you sure you want to delete all data? This operation is irreversible. Type 'yes' or 'y' to confirm");
        if !ask_for_confirmation() {
            info!("Operation cancelled");
            return;
        }
    }

    info!("Deleting all data");
    fs.sql.nuke().unwrap();
    fs.storage.nuke().unwrap();
    info!("Done");
}

fn export_index(fs: SqlFileSystem, format: IndexExportFormat) -> Result<(), anyhow::Error> {
    info!("Exporting index");
    let tree = fs.sql.get_tree()?;

    let data = match format {
        IndexExportFormat::Json => serde_json::to_string_pretty(&tree)?,
        IndexExportFormat::Yaml => serde_yml::to_string(&tree)?,
    };

    let path = format!("./index.{}", format);
    fs::write(&path, data).context("Unable to write index file")?;
    info!("Index exported to {}", &path);
    Ok(())
}

fn export_files(mut fs: SqlFileSystem, format: FileExportFormat, mut path: PathBuf) -> Result<(), anyhow::Error> {
    info!("Exporting files to {:?}", &path);
    let tree = fs.sql.get_tree()?;

    match format {
        FileExportFormat::Directory => {
            fs::create_dir_all(&path)?;

            FsTree::for_each(tree, |child, child_path| {
                let child_path = path.join(child_path);

                if child.kind == FsTreeKind::Directory {
                    fs::create_dir_all(&child_path)?;
                } else {
                    let data = fs.read_all(child.id)?;
                    fs::write(&child_path, data).context("Unable to write file")?;
                }

                Ok(())
            })?;
        }
        FileExportFormat::Tar => {
            if !path.ends_with(".tar.gz") {
                path = path.with_extension("tar.gz");
            }

            let file = File::create(&path)?;
            let mut gz = GzEncoder::new(file, Compression::default());
            let mut tar = tar::Builder::new(&mut gz);

            FsTree::for_each(tree, |child, child_path| {
                let mut header = tar::Header::new_gnu();
                header.set_size(child.size as u64);
                header.set_mtime(child.updated_at as u64);
                header.set_mode(child.perms as u32);
                header.set_uid(child.uid as u64);
                header.set_gid(child.gid as u64);
                header.set_entry_type(if child.kind == FsTreeKind::Directory { tar::EntryType::Directory } else { tar::EntryType::Regular });
                header.set_cksum();

                if child.kind == FsTreeKind::Directory {
                    tar.append_data(&mut header, &child_path, &mut std::io::empty())?;
                } else {
                    let data = fs.read_all(child.id)?;
                    tar.append_data(&mut header, &child_path, data.as_slice())?;
                }
                Ok(())
            })?;

            tar.finish()?;
        }
        FileExportFormat::Zip => {
            if !path.ends_with(".zip") {
                path = path.with_extension("zip");
            }
            let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
            let mut zip = ZipWriter::new(File::create(&path)?);

            FsTree::for_each(tree, |child, child_path| {
                if child.kind == FsTreeKind::Directory {
                    zip.add_directory_from_path(&child_path, options)?;
                } else {
                    let data = fs.read_all(child.id)?;
                    zip.start_file_from_path(child_path, options)?;
                    zip.write_all(&data)?;
                }
                Ok(())
            })?;

            zip.finish()?;
        }
    };

    info!("Files exported successfully");
    Ok(())
}

pub fn ask_for_confirmation() -> bool {
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();
    let choice = input.trim().to_ascii_lowercase();
    choice == "yes" || choice == "y"
}

pub fn current_timestamp() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

impl Display for IndexExportFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexExportFormat::Json => write!(f, "json"),
            IndexExportFormat::Yaml => write!(f, "yaml"),
        }
    }
}

impl Display for FileExportFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FileExportFormat::Directory => write!(f, "directory"),
            FileExportFormat::Tar => write!(f, "tar"),
            FileExportFormat::Zip => write!(f, "zip"),
        }
    }
}