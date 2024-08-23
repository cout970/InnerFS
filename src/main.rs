use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::config::{read_config, StorageOption};
use crate::obj_storage::{FsObjectStorage};
use crate::proxy_fs::ProxyFileSystem;
use crate::s3::S3ObjectStorage;
use crate::sql::SQL;
use crate::sqlar::SqlarObjectStorage;
use crate::storage_interface::StorageInterface;

mod config;
mod sql;
mod proxy_fs;
mod sql_fs;
mod storage;
mod obj_storage;
mod storage_interface;
mod sqlar;
mod s3;

fn main() {
    println!("Starting");
    let config = read_config().expect("Unable to read config");
    println!("Config loaded");
    let sql = Rc::new(SQL::open(&config.database_file));
    let mount_point = config.mount_point.clone();

    let storage = match config.storage_option {
        StorageOption::FileSystem => {
            StorageInterface::new(config.clone(), Box::new(FsObjectStorage {
                base_path: PathBuf::from(&config.blob_storage),
            }))
        }
        StorageOption::Sqlar => {
            StorageInterface::new(config.clone(), Box::new(SqlarObjectStorage {
                sql: sql.clone(),
            }))
        }
        StorageOption::S3 => {
            StorageInterface::new(config.clone(), Box::new(S3ObjectStorage::new(config.clone())))
        }
    };

    let proxy = ProxyFileSystem::new(sql, config, Box::new(storage));

    println!("Attempting to unmount {} before trying to mount", &mount_point);
    let _ = Command::new("umount").arg(&mount_point).status();

    let stat = fs::metadata(&mount_point).expect("Mount point does not exist");
    if !stat.is_dir() {
        panic!("Mount point is not a directory");
    }

    println!("Mounting filesystem at {}", &mount_point);
    fuse::mount(proxy, &mount_point, &[]).expect("Unable to mount filesystem");

    println!("Exiting");
}

pub fn current_timestamp() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}