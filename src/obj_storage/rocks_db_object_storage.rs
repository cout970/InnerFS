use crate::config::StorageConfig;
use crate::obj_storage::{BlobStorage, RemoteBlob};
use crate::AnyError;
use log::debug;
use rocksdb::{DBWithThreadMode, Options, SingleThreaded, DB};
use std::sync::Arc;

pub struct RocksDbBackend {
    db: DBWithThreadMode<SingleThreaded>,
}

impl RocksDbBackend {
    pub fn new(config: Arc<StorageConfig>) -> RocksDbBackend {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open_cf(&opts, &config.blob_storage, ["default"]).unwrap();
        RocksDbBackend { db }
    }
}

impl BlobStorage for RocksDbBackend {
    fn get_multiple(&mut self, paths: &[&str]) -> Result<Vec<Vec<u8>>, AnyError> {
        let mut blobs = Vec::new();
        for path in paths {
            match self.db.get(path)? {
                Some(v) => blobs.push(v.to_vec()),
                None => blobs.push(vec![]),
            }
        }
        Ok(blobs)
    }

    fn put_multiple(&mut self, blobs: &[RemoteBlob]) -> Result<(), AnyError> {
        for blob in blobs {
            self.db.put(&blob.path, &blob.contents)?;
        }
        Ok(())
    }

    fn remove_multiple(&mut self, paths: &[&str]) -> Result<(), AnyError> {
        for path in paths {
            self.db.delete(path)?;
        }
        Ok(())
    }

    fn nuke(&mut self) -> Result<(), AnyError> {
        debug!("Nuke");
        self.db.drop_cf("default")?;
        Ok(())
    }
}
