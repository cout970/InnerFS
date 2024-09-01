use crate::config::{StorageConfig, StorageOption};
use crate::obj_storage::fs_object_storage::FsObjectStorage;
use crate::obj_storage::s3_object_storage::S3ObjectStorage;
use crate::metadata_db::{FileRow, MetadataDB};
use std::fmt::Display;
use std::path::PathBuf;
use std::rc::Rc;
use crate::AnyError;
use crate::obj_storage::compressed_object_storage::CompressedObjectStorage;
use crate::obj_storage::encrypted_object_storage::EncryptedObjectStorage;
use crate::obj_storage::rocks_db_object_storage::RocksDbObjectStorage;
use crate::obj_storage::sqlar_object_storage::SqlarObjectStorage;
use crate::storage::ObjInUseFn;

// Storage backends
pub mod fs_object_storage;
pub mod s3_object_storage;
pub mod sqlar_object_storage;
pub mod rocks_db_object_storage;
pub mod debug_object_storage;

// Wrappers
pub mod encrypted_object_storage;
pub mod replicated_object_storage;
pub mod compressed_object_storage;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ObjInfo {
    pub name: String,
    pub full_path: String,
    pub sha512: String,
    pub created_at: i64,
    pub accessed_at: i64,
    pub updated_at: i64,
    pub mode: u32,
    pub size: u64,
    pub encryption_key: String,
    pub compression: String,
}

/// Method to test is a file exists, to handle deletion of de-duplicated files.
/// When multiple files share the same object in storage, we need to check if the object is still
/// being used by any other file before deleting it.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub enum UniquenessTest {
    // Check if there are other files with the same path
    Path,
    // Check if there are other files with the same content
    Sha512,
}

pub trait ObjectStorage {
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, AnyError>;
    fn put(&mut self, info: &mut ObjInfo, content: &[u8]) -> Result<(), AnyError>;
    fn remove(&mut self, info: &ObjInfo, is_in_use: ObjInUseFn) -> Result<(), AnyError>;
    fn rename(&mut self, prev_info: &ObjInfo, new_info: &ObjInfo) -> Result<(), AnyError>;
    fn nuke(&mut self) -> Result<(), AnyError>;
}

impl Display for ObjInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl ObjInfo {
    pub fn new(file: &FileRow, full_path: &str) -> ObjInfo {
        ObjInfo {
            name: file.name.to_string(),
            full_path: full_path.to_string(),
            sha512: file.sha512.to_string(),
            created_at: file.created_at,
            accessed_at: file.accessed_at,
            updated_at: file.updated_at,
            mode: file.perms as u32,
            size: file.size as u64,
            encryption_key: file.encryption_key.to_string(),
            compression: file.compression.to_string(),
        }
    }
}

pub fn create_object_storage(config: Rc<StorageConfig>, sql: Rc<MetadataDB>) -> Box<dyn ObjectStorage> {
    let mut obj_storage: Box<dyn ObjectStorage> = match &config.storage_backend {
        StorageOption::FileSystem => {
            Box::new(FsObjectStorage {
                base_path: PathBuf::from(&config.blob_storage),
                config: config.clone(),
            })
        }
        StorageOption::Sqlar => {
            Box::new(SqlarObjectStorage {
                sql: sql.clone(),
                config: config.clone(),
            })
        }
        StorageOption::S3 => {
            Box::new(S3ObjectStorage::new(config.clone()))
        }
        StorageOption::RocksDb => {
            Box::new(RocksDbObjectStorage::new(config.clone()))
        }
    };

    if !config.encryption_key.is_empty() {
        // Apply encryption if a key is provided
        obj_storage = Box::new(EncryptedObjectStorage::new(config.clone(), obj_storage));
    } else if config.compression_level > 0 {
        // Apply compression if a level is provided
        obj_storage = Box::new(CompressedObjectStorage::new(obj_storage, config.compression_level));
    }

    obj_storage
}
