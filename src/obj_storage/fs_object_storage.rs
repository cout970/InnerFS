use std::fs;
use std::path::PathBuf;
use std::rc::Rc;
use anyhow::{anyhow, Context};
use log::{error, info};
use crate::config::StorageConfig;
use crate::obj_storage::{ObjInfo, ObjectStorage, UniquenessTest};

pub struct FsObjectStorage {
    pub base_path: PathBuf,
    pub config: Rc<StorageConfig>
}

impl FsObjectStorage {
    pub fn path(&self, info: &ObjInfo) -> PathBuf {
        let mut path = self.base_path.clone();
        path.push(self.config.path_of(&info));
        path
    }
}

impl ObjectStorage for FsObjectStorage {
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, anyhow::Error> {
        let path = self.path(&info);
        info!("Get: {:?}", &path);

        fs::read(&path).context("FS failed to read file")
    }

    fn put(&mut self, info: &mut ObjInfo, content: &[u8]) -> Result<(), anyhow::Error> {
        let path = self.path(&info);
        info!("Put: {:?}", &path);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).context("FS failed to create dir")?;
        }
        fs::write(&path, content).context("FS failed to write file")
    }

    fn remove(&mut self, info: &ObjInfo) -> Result<(), anyhow::Error> {
        let path = self.path(&info);
        info!("Remove: {:?}", &path);

        fs::remove_file(&path).map_err(|e| {
            anyhow!("FS failed to remove file '{:?}': {:?}", path, e)
        })
    }

    fn nuke(&mut self) -> Result<(), anyhow::Error> {
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

    fn get_uniqueness_test(&self) -> UniquenessTest {
        if self.config.use_hash_as_filename {
            UniquenessTest::Sha512
        } else {
            UniquenessTest::Path
        }
    }
}