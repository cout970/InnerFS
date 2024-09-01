use crate::config::StorageConfig;
use crate::obj_storage::{ObjInfo, ObjectStorage, UniquenessTest};
use crate::storage::ObjInUseFn;
use crate::AnyError;
use log::{debug};
use rocksdb::{DBWithThreadMode, Options, SingleThreaded, DB};
use std::rc::Rc;

pub struct RocksDbObjectStorage {
    db: DBWithThreadMode<SingleThreaded>,
    config: Rc<StorageConfig>,
}

impl RocksDbObjectStorage {
    pub fn new(config: Rc<StorageConfig>) -> RocksDbObjectStorage {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open_cf(&opts, &config.blob_storage, ["default"]).unwrap();
        RocksDbObjectStorage { db, config }
    }

    pub fn path(&self, info: &ObjInfo) -> String {
        if self.config.use_hash_as_filename {
            format!("{}.dat", &info.sha512[..32])
        } else {
            info.full_path.to_string()
        }
    }
}

impl ObjectStorage for RocksDbObjectStorage {
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, AnyError> {
        let path = self.path(info);
        debug!("Get: {:?}", &path);

        match self.db.get(&path)? {
            Some(v) => Ok(v.to_vec()),
            None => Err(AnyError::msg("Object not found")),
        }
    }

    fn put(&mut self, info: &mut ObjInfo, content: &[u8]) -> Result<(), AnyError> {
        let path = self.path(info);
        debug!("Put: {:?}", &path);

        self.db.put(&path, content)?;
        Ok(())
    }

    fn remove(&mut self, info: &ObjInfo, is_in_use: ObjInUseFn) -> Result<(), AnyError> {
        let path = self.path(info);
        let test = if self.config.use_hash_as_filename {
            UniquenessTest::Sha512
        } else {
            UniquenessTest::Path
        };

        // If is object in use by other file (deduplication), do not remove it
        if is_in_use(info, test)? {
            return Ok(());
        }

        debug!("Remove: {:?}", &path);

        self.db.delete(&path)?;
        Ok(())
    }

    fn rename(&mut self, prev_info: &ObjInfo, new_info: &ObjInfo) -> Result<(), AnyError> {
        let prev_path = self.path(prev_info);
        let new_path = self.path(new_info);

        if prev_path == new_path {
            return Ok(());
        }

        debug!("Rename: {:?} -> {:?}", &prev_path, &new_path);

        let content = self.db.get(&prev_path)?.unwrap();
        self.db.put(&new_path, &content)?;
        self.db.delete(&prev_path)?;
        Ok(())
    }

    fn nuke(&mut self) -> Result<(), AnyError> {
        debug!("Nuke");
        self.db.drop_cf("default")?;
        Ok(())
    }
}