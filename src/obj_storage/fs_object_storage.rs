use std::fs;
use std::path::PathBuf;
use std::rc::Rc;
use anyhow::{anyhow, Context};
use log::{error, info};
use crate::AnyError;
use crate::config::StorageConfig;
use crate::obj_storage::{ObjInfo, ObjectStorage, UniquenessTest};
use crate::storage::ObjInUseFn;

pub struct FsObjectStorage {
    pub base_path: PathBuf,
    pub config: Rc<StorageConfig>,
}

impl FsObjectStorage {
    pub fn path(&self, info: &ObjInfo) -> PathBuf {
        let mut path = self.base_path.clone();
        path.push(self.config.path_of(&info));
        path
    }
}

impl ObjectStorage for FsObjectStorage {
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, AnyError> {
        let path = self.path(&info);
        info!("Get: {:?}", &path);

        fs::read(&path).context("FS failed to read file")
    }

    fn put(&mut self, info: &mut ObjInfo, content: &[u8]) -> Result<(), AnyError> {
        let path = self.path(&info);
        info!("Put: {:?}", &path);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).context("FS failed to create dir")?;
        }
        fs::write(&path, content).context("FS failed to write file")
    }

    fn remove(&mut self, info: &ObjInfo, is_in_use: ObjInUseFn) -> Result<(), AnyError> {
        let path = self.path(&info);
        let test = if self.config.use_hash_as_filename {
            UniquenessTest::Sha512
        } else {
            UniquenessTest::Path
        };

        // If is object in use by other file (deduplication), do not remove it
        if is_in_use(info, test)? {
            return Ok(());
        }

        info!("Remove: {:?}", &path);

        fs::remove_file(&path).map_err(|e| {
            anyhow!("FS failed to remove file '{:?}': {:?}", path, e)
        })
    }

    fn rename(&mut self, prev_info: &ObjInfo, new_info: &ObjInfo) -> Result<(), AnyError> {
        let prev_path = self.path(&prev_info);
        let new_path = self.path(&new_info);

        if prev_path == new_path {
            return Ok(());
        }

        // Noop, previous file does not exist
        if fs::metadata(&prev_path).is_err() {
            return Ok(());
        }

        info!("Rename: {:?} -> {:?}", &prev_path, &new_path);

        if let Some(parent) = new_path.parent() {
            fs::create_dir_all(parent).context("FS failed to create dir")?;
        }
        fs::rename(&prev_path, &new_path).map_err(|e| {
            anyhow!("FS failed to rename '{:?}' -> '{:?}': {:?}", prev_path, new_path, e)
        })
    }

    fn nuke(&mut self) -> Result<(), AnyError> {
        info!("Nuke: {:?}", &self.base_path);

        for entry_res in fs::read_dir(&self.base_path)? {
            let entry = match entry_res {
                Ok(e) => e,
                Err(e) => {
                    error!("[IGNORED] Failed to read entry: {:?}", e);
                    continue;
                }
            };

            let meta = entry.metadata()?;

            if meta.is_dir() {
                if let Err(e) = fs::remove_dir_all(entry.path()) {
                    error!("[IGNORED] Failed to remove '{:?}': {:?}", entry.path(), e);
                }
            } else {
                if let Err(e) = fs::remove_file(entry.path()) {
                    error!("[IGNORED] Failed to remove '{:?}': {:?}", entry.path(), e);
                }
            }
        }

        Ok(())
    }
}