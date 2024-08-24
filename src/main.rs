use crate::config::{read_config, StorageOption};
use crate::encryption::EncryptedObjectStorage;
use crate::obj_storage::{FsObjectStorage, ObjectStorage};
use crate::proxy_fs::ProxyFileSystem;
use crate::s3::S3ObjectStorage;
use crate::sql::{SQL};
use crate::sqlar::SqlarObjectStorage;
use crate::storage_interface::StorageInterface;
use env_logger::Env;
use log::info;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

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
use clap::{Parser, Subcommand};

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
        #[arg(short, long)]
        format: String,
    },
    /// Export the whole filesystem to a file
    ExportFiles {
        /// export format: tar or zip
        #[arg(short, long)]
        format: String,
    },
}

fn main() {
    let cli = Cli::parse();

    let default_log_filter = if cli.debug { "debug" } else { "info" };
    env_logger::Builder::from_env(Env::default().default_filter_or(default_log_filter)).init();

    let config_path = cli.config.or_else(|| Some(PathBuf::from("./config.yml"))).unwrap();

    info!("Starting");
    let config = read_config(&config_path).expect("Unable to read config");

    info!("Config loaded");
    let sql = Rc::new(SQL::open(&config.database_file));

    info!("Database opened");
    // Select the appropriate storage backend
    let mut obj_storage: Box<dyn ObjectStorage> = match config.storage_option {
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
        Commands::ExportIndex { format } => export_index(fs, format),
        Commands::ExportFiles { format } => export_files(fs, format),
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
        let mut input = String::new();
        println!("Are you sure you want to delete all data? This operation is irreversible. Type 'yes' or 'y' to confirm");
        std::io::stdin().read_line(&mut input).unwrap();
        let choice = input.trim().to_ascii_lowercase();
        if choice != "yes" && choice != "y" {
            println!("Operation cancelled");
            return;
        }
    }

    info!("Deleting all data");
    fs.sql.nuke().unwrap();
    fs.storage.nuke().unwrap();
    info!("Done");
}

fn export_index(fs: SqlFileSystem, format: String) {
    info!("Exporting index");
    let tree = fs.sql.get_tree().unwrap();

    let data = match format.as_str() {
        "json" => serde_json::to_string_pretty(&tree).unwrap(),
        "yml" | "yaml" => serde_yml::to_string(&tree).unwrap(),
        _ => panic!("Invalid format"),
    };

    let path = format!("./index.{}", format);
    fs::write(&path, data).expect("Unable to write index file");
    info!("Index exported to {}", &path);
}

fn export_files(_fs: SqlFileSystem, _format: String) {
    // info!("Exporting files");
    // let files = fs.storage.export_files();
    // let data = match format.as_str() {
    //     "tar" => {
    //         let mut archive = tar::Builder::new(Vec::new());
    //         for file in files {
    //             let path = file.full_path.clone();
    //             let content = fs.storage.get(&file).unwrap();
    //             let mut header = tar::Header::new_gnu();
    //             header.set_path(&path).unwrap();
    //             header.set_size(content.len() as u64);
    //             archive.append(&header, content.as_slice()).unwrap();
    //         }
    //         archive.into_inner().unwrap()
    //     }
    //     "zip" => {
    //         let mut archive = zip::ZipWriter::new(Vec::new());
    //         for file in files {
    //             let path = file.full_path.clone();
    //             let content = fs.storage.get(&file).unwrap();
    //             archive.start_file(&path, Default::default()).unwrap();
    //             archive.write_all(content.as_slice()).unwrap();
    //         }
    //         archive.finish().unwrap()
    //     }
    //     _ => panic!("Invalid format"),
    // };
    //
    // let path = fs.config.mount_point.clone() + "/files." + &format;
    // fs::write(&path, data).expect("Unable to write files archive");
    // info!("Files exported to {}", &path);
}

pub fn current_timestamp() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}