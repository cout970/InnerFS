use crate::config::{read_config, StorageOption};
use crate::encryption::EncryptedObjectStorage;
use crate::obj_storage::{FsObjectStorage, ObjectStorage};
use crate::proxy_fs::ProxyFileSystem;
use crate::s3::S3ObjectStorage;
use crate::sql::SQL;
use crate::sqlar::SqlarObjectStorage;
use crate::storage_interface::StorageInterface;
use env_logger::Env;
use log::{info};
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

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    info!("Starting");
    let config = read_config().expect("Unable to read config");

    info!("Config loaded");
    let sql = Rc::new(SQL::open(&config.database_file));

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

    // Create a FUSE proxy filesystem to access the StorageInterface
    let proxy = ProxyFileSystem::new(sql, config.clone(), storage);

    let mount_point = config.mount_point.clone();

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

pub fn current_timestamp() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}