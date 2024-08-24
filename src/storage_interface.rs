use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use anyhow::anyhow;
use crate::config::Config;
use crate::current_timestamp;
use crate::obj_storage::{ObjectStorage, ObjInfo};
use crate::sql::FileRow;
use crate::storage::Storage;

pub struct StorageInterface {
    pub obj_storage: Box<dyn ObjectStorage>,
    pub config: Rc<Config>,
    pub cache: HashMap<i64, StorageInterfaceCache>,
    pub pending_remove: HashSet<ObjInfo>,
}

pub struct StorageInterfaceCache {
    pub full_path: String,
    pub mode: i32,
    pub content: Vec<u8>,
    pub retrieved: bool,
    pub modified: bool,
}

impl StorageInterface {
    pub fn new(config: Rc<Config>, obj_storage: Box<dyn ObjectStorage>) -> Self {
        Self {
            obj_storage,
            config,
            cache: HashMap::new(),
            pending_remove: HashSet::new(),
        }
    }

    pub fn path(config: &Config, file: &FileRow, full_path: &str) -> String {
        if config.use_hash_as_filename {
            if file.sha512.is_empty() { "null".to_string() } else { format!("{}.dat", &file.sha512[..32]) }
        } else {
            full_path.trim_start_matches('/').to_string()
        }
    }
}

impl Storage for StorageInterface {
    fn open(&mut self, file: &mut FileRow, full_path: &str, mode: u32) -> Result<bool, anyhow::Error> {
        if (mode as i32) & libc::O_APPEND != 0 {
            return Err(anyhow::anyhow!("Append mode is not supported"));
        }

        self.cache.insert(file.id, StorageInterfaceCache {
            full_path: full_path.to_string(),
            mode: mode as i32,
            content: vec![],
            retrieved: false,
            modified: false,
        });

        Ok(false)
    }

    fn read(&mut self, file: &FileRow, offset: u64, buff: &mut [u8]) -> Result<usize, anyhow::Error> {
        let row = self.cache.get_mut(&file.id).ok_or_else(||
            anyhow!("Trying to use a file that was closed or never opened: {}", file.id)
        )?;

        if row.mode & libc::O_WRONLY != 0 {
            return Err(anyhow::anyhow!("File is write-only ({})", file.name));
        }

        if !row.retrieved {
            let content = if !file.sha512.is_empty() {
                let info = ObjInfo::new(file, &Self::path(&self.config, file, &row.full_path));
                self.obj_storage.get(&info)?
            } else {
                vec![]
            };
            row.content = content;
            row.retrieved = true;
        }

        if offset >= row.content.len() as u64 {
            return Ok(0);
        }

        let remaining_content_slice = &row.content[offset as usize..];
        let read_len = min(buff.len(), remaining_content_slice.len());
        buff[..read_len].copy_from_slice(&remaining_content_slice[..read_len]);
        Ok(read_len)
    }

    fn write(&mut self, file: &FileRow, offset: u64, buff: &[u8]) -> Result<usize, anyhow::Error> {
        let row = self.cache.get_mut(&file.id).ok_or_else(||
            anyhow!("Trying to use a file that was closed or never opened: {}", file.id)
        )?;

        if row.mode & libc::O_RDONLY != 0 {
            return Err(anyhow::anyhow!("File is read-only"));
        }

        if row.retrieved {
            row.content.clear();
            row.retrieved = false;
        }

        let offset = offset as usize;


        if offset == buff.len() {
            // Append to the end
            row.content.extend(buff.iter());
        } else {
            // Overwrite
            if offset + buff.len() > row.content.len() {
                row.content.resize(offset + buff.len(), 0);
            }
            row.content[offset..offset + buff.len()].copy_from_slice(buff);
        }

        row.modified = true;
        Ok(buff.len())
    }

    fn close(&mut self, file: &mut FileRow) -> Result<bool, anyhow::Error> {
        let mut modified = false;
        {
            let row = self.cache.get_mut(&file.id).ok_or_else(||
                anyhow!("Trying to use a file that was closed or never opened: {}", file.id)
            )?;

            if row.modified {
                // Shas of contents as id for the object
                let sha512 = hex::encode(hmac_sha512::Hash::hash(&row.content));

                // Remove old object
                if !file.sha512.is_empty() && file.sha512 != sha512 {
                    let info = ObjInfo::new(file, &Self::path(&self.config, file, &row.full_path));
                    self.pending_remove.insert(info);
                }

                file.sha512 = sha512;
                let mut info = ObjInfo::new(file, &Self::path(&self.config, file, &row.full_path));

                // Store new object
                self.obj_storage.put(&mut info, &row.content)?;
                // Update file metadata
                file.encryption_key = info.encryption_key;
                file.size = row.content.len() as i64;
                file.updated_at = current_timestamp();
                modified = true;
            }
        }

        self.cache.remove(&file.id);
        Ok(modified)
    }

    fn remove(&mut self, file: &FileRow, full_path: &str) -> Result<(), anyhow::Error> {
        if !file.sha512.is_empty() {
            self.pending_remove.insert(ObjInfo::new(file, &Self::path(&self.config, file, full_path)));
        }
        Ok(())
    }

    fn cleanup(&mut self, is_in_use: Box<dyn Fn(&ObjInfo) -> Result<bool, anyhow::Error>>) -> Result<(), anyhow::Error> {
        for info in &self.pending_remove {
            let in_use = (&is_in_use)(info)?;

            if !in_use {
                self.obj_storage.remove(info)?;
            }
        }

        self.pending_remove.clear();
        Ok(())
    }

    fn nuke(&mut self) -> Result<(), anyhow::Error> {
        self.cache.clear();
        self.pending_remove.clear();
        self.obj_storage.nuke()
    }
}