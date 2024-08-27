use crate::config::{read_config, Config, StorageOption};
use crate::encryption::EncryptedObjectStorage;
use crate::obj_storage::{FsObjectStorage, ObjectStorage};
use crate::proxy_fs::ProxyFileSystem;
use crate::s3::S3ObjectStorage;
use crate::sql::SQL;
use crate::sqlar::SqlarObjectStorage;
use crate::storage_interface::StorageInterface;
use anyhow::{anyhow, Context};
use env_logger::Env;
use fs::File;
use log::{error, info, warn};
use std::ffi::OsStr;
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fs};

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

use crate::fs_tree::{FsTree, FsTreeKind};
use crate::sql_fs::SqlFileSystem;
use clap::{Parser, Subcommand, ValueEnum};
use flate2::{write::GzEncoder, Compression};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

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

    info!("Starting v{}", VERSION);
    let config = read_config(&config_path).expect("Unable to read config");

    info!("Config loaded");
    let sql = Rc::new(SQL::open(&config.database_file));

    // Run migrations
    sql.run_migrations().expect("Unable to run migrations");

    // Check if the config file has changed in incompatible ways
    check_config_changes(config.clone(), sql.clone()).unwrap();

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
    fuse::mount(proxy, &mount_point, &[OsStr::new("noempty")]).expect("Unable to mount filesystem");

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

pub fn check_config_changes(config: Rc<Config>, sql: Rc<SQL>) -> Result<(), anyhow::Error> {
    // Changing storage_option will make all the files not available
    let storage_option = config.storage_backend.to_string();
    {
        let setting = sql.get_setting("storage_option")?;
        if let Some(setting) = setting {
            if setting != storage_option {
                error!("Storage option changed from {} to {}, this will cause loss of data, it's recommended to revert the setting or recreate the filesystem", setting, storage_option);
                info!("Do you want to proceed anyways? Type 'yes' or 'y' to confirm");
                if !ask_for_confirmation() {
                    return Err(anyhow!("Operation cancelled"));
                }
            }
        }
        sql.set_setting("storage_option", &storage_option)?;
    }

    // Changing encryption_key will make every file not readable
    let encryption_key = hex::encode(hmac_sha512::Hash::hash(&config.encryption_key));
    {
        let setting = sql.get_setting("encryption_key_sha512")?;

        if let Some(setting) = setting {
            if setting != encryption_key {
                error!("Encryption key changed, this will cause loss of data, it's recommended to revert the setting or recreate the filesystem");
                info!("Do you want to proceed anyways? Type 'yes' or 'y' to confirm");
                if !ask_for_confirmation() {
                    return Err(anyhow!("Operation cancelled"));
                }
            }
        }
        sql.set_setting("encryption_key_sha512", &encryption_key)?;
    }
    // Changing use_hash_as_filename will cause in a mismatch between previous and new filenames
    let use_hash_as_filename = config.use_hash_as_filename.to_string();
    {
        let setting = sql.get_setting("use_hash_as_filename")?;
        if let Some(setting) = setting {
            if setting != use_hash_as_filename {
                error!("use_hash_as_filename changed, this will cause loss of data, it's recommended to revert the setting or recreate the filesystem");
                info!("Do you want to proceed anyways? Type 'yes' or 'y' to confirm");
                if !ask_for_confirmation() {
                    return Err(anyhow!("Operation cancelled"));
                }
            }
        }
        sql.set_setting("use_hash_as_filename", &use_hash_as_filename)?;
    }

    // s3_bucket/s3_region/s3_endpoint_url
    if config.storage_backend == StorageOption::S3 {
        let bucket = sql.get_setting("s3_bucket")?.unwrap_or_else(|| "".to_string());
        let region = sql.get_setting("s3_region")?.unwrap_or_else(|| "".to_string());
        let endpoint_url = sql.get_setting("s3_endpoint_url")?.unwrap_or_else(|| "".to_string());

        if bucket != config.s3_bucket || region != config.s3_region || endpoint_url != config.s3_endpoint_url {
            error!("S3 settings changed, this will make the data inaccesible, it's recommended to revert the setting or recreate the filesystem");
            info!("Do you want to proceed anyways? Type 'yes' or 'y' to confirm");
            if !ask_for_confirmation() {
                return Err(anyhow!("Operation cancelled"));
            }
        }

        sql.set_setting("s3_bucket", &bucket)?;
        sql.set_setting("s3_region", &region)?;
        sql.set_setting("s3_endpoint_url", &endpoint_url)?;
    }

    // Changing blob_storage will make all the files not available
    if config.storage_backend == StorageOption::FileSystem {
        let setting = sql.get_setting("blob_storage")?;
        if let Some(setting) = setting {
            if setting != config.blob_storage {
                error!("Blob storage changed from {} to {}, this will make the data inaccesible, it's recommended to revert the setting or recreate the filesystem", setting, config.blob_storage);
                info!("Do you want to proceed anyways? Type 'yes' or 'y' to confirm");
                if !ask_for_confirmation() {
                    return Err(anyhow!("Operation cancelled"));
                }
            }
        }
        sql.set_setting("blob_storage", &config.blob_storage)?;
    }

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