use crate::config::{StorageConfig, StorageOption};
use crate::metadata_db::{FileRow, MetadataDB};
use crate::obj_storage::fs_object_storage::FsBackend;
use crate::obj_storage::rocks_db_object_storage::RocksDbBackend;
use crate::obj_storage::s3_object_storage::S3Backend;
use crate::obj_storage::sqlar_object_storage::SqlarBackend;
use crate::AnyError;
use std::fmt::Display;
use std::path::PathBuf;
use std::sync::Arc;

// Storage backends
pub mod debug_object_storage;
pub mod fs_object_storage;
pub mod rocks_db_object_storage;
pub mod s3_object_storage;
pub mod sqlar_object_storage;

// Wrappers
pub mod compressed_object_storage;
pub mod encrypted_object_storage;
pub mod replicated_object_storage;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ObjInfo {
    pub id: i64,
    pub version: i64,
    pub name: String,
    pub external_id: String,
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

#[derive(Debug, Clone)]
pub struct RemoteBlob {
    pub path: String,
    pub contents: Vec<u8>,
}

pub trait BlobStorage: Send + Sync {
    // Load data
    fn get_multiple(&mut self, paths: &[&str]) -> Result<Vec<Vec<u8>>, AnyError>;

    // Store data
    fn put_multiple(&mut self, blobs: &[RemoteBlob]) -> Result<(), AnyError>;

    // Remove data
    fn remove_multiple(&mut self, paths: &[&str]) -> Result<(), AnyError>;

    // Remove everything
    fn nuke(&mut self) -> Result<(), AnyError>;
}

pub trait BlobProcessor: Send + Sync {
    fn on_store(&self, blob: Vec<u8>) -> Result<Vec<u8>, AnyError>;
    fn on_load(&self, blob: Vec<u8>) -> Result<Vec<u8>, AnyError>;
}

impl Display for ObjInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl ObjInfo {
    pub fn new(file: &FileRow, full_path: &str) -> ObjInfo {
        ObjInfo {
            id: file.id,
            version: file.version,
            name: file.name.to_string(),
            external_id: file.external_id.to_string(),
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

pub fn create_object_storage(config: Arc<StorageConfig>, sql: &MetadataDB) -> Box<dyn BlobStorage> {
    match &config.storage_backend {
        StorageOption::FileSystem => Box::new(FsBackend {
            base_path: PathBuf::from(&config.blob_storage),
        }),
        StorageOption::Sqlar => Box::new(SqlarBackend {
            sql: sql.clone(),
        }),
        StorageOption::S3 => Box::new(S3Backend::new(config.clone())),
        StorageOption::RocksDb => Box::new(RocksDbBackend::new(config.clone())),
    }
}
