use crate::obj_storage::{BlobStorage, RemoteBlob};
use crate::AnyError;
use anyhow::{anyhow, Context};
use log::{debug, error};
use std::fs;
use std::path::PathBuf;

pub struct FsBackend {
    pub base_path: PathBuf,
}

impl FsBackend {
    pub fn real_path(&self, virtual_path: &str) -> PathBuf {
        self.base_path.join(virtual_path)
    }
}

impl BlobStorage for FsBackend {
    fn get_multiple(&mut self, paths: &[&str]) -> Result<Vec<Vec<u8>>, AnyError> {
        let mut blobs = Vec::with_capacity(paths.len());
        for path in paths {
            let path = self.real_path(path);
            debug!("Get: {:?}", &path);

            match fs::read(&path) {
                Ok(blob) => blobs.push(blob),
                Err(e) => {
                    return Err(anyhow!("FS failed to read file '{:?}': {:?}", path, e));
                }
            }
        }
        Ok(blobs)
    }

    fn put_multiple(&mut self, blobs: &[RemoteBlob]) -> Result<(), AnyError> {
        for blob in blobs {
            let path = self.real_path(&blob.path);
            debug!("Put: {:?}", &path);

            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).context("FS failed to create dir")?;
            }
            fs::write(&path, &blob.contents).context("FS failed to write file")?;
        }
        Ok(())
    }

    fn remove_multiple(&mut self, paths: &[&str]) -> Result<(), AnyError> {
        for path in paths {
            let path = self.real_path(path);
            debug!("Remove: {:?}", &path);

            match fs::remove_file(&path) {
                Ok(_) => {}
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::NotFound {
                        continue;
                    } else {
                        return Err(anyhow!("FS failed to remove file '{:?}': {:?}", path, e));
                    }
                }
            }
        }
        Ok(())
    }

    fn nuke(&mut self) -> Result<(), AnyError> {
        debug!("Nuke: {:?}", &self.base_path);

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
