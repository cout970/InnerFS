use std::cmp::min;
use std::collections::{HashMap, HashSet};
use anyhow::anyhow;
use libc::{O_APPEND, O_RDONLY};
use crate::AnyError;
use crate::obj_storage::{ObjInfo, ObjectStorage};
use crate::metadata_db::{FileRow, FILE_KIND_DIRECTORY};
use crate::fuse_fs::OpenFlags;
use crate::storage::{ObjInUseFn, Storage};
use crate::utils::current_timestamp;

pub struct StorageInterface {
    pub obj_storage: Box<dyn ObjectStorage>,
    pub cache: HashMap<i64, StorageInterfaceCache>,
    pub pending_remove: HashSet<ObjInfo>,
}

pub struct StorageInterfaceCache {
    pub full_path: String,
    pub mode: i32,
    pub content: Vec<u8>,
    pub retrieved: bool,
    pub modified: bool,
    pub count: i32,
}

impl StorageInterface {
    pub fn new(obj_storage: Box<dyn ObjectStorage>) -> Self {
        Self {
            obj_storage,
            cache: HashMap::new(),
            pending_remove: HashSet::new(),
        }
    }
}

impl Storage for StorageInterface {
    fn open(&mut self, file: &mut FileRow, full_path: &str, mode: u32) -> Result<bool, AnyError> {
        if (mode as i32) & O_APPEND != 0 {
            return Err(anyhow::anyhow!("Append mode is not supported"));
        }

        // Allow multiple read-only opens
        {
            let prev = self.cache.get_mut(&file.id);

            if let Some(cache) = prev {
                let prev_flags = OpenFlags::from(cache.mode);
                let new_flags = OpenFlags::from(mode as i32);
                // Only allowed to open in read mode if it was previously opened in read mode
                let valid = prev_flags.read_only && new_flags.read_only && !prev_flags.exclusive && !new_flags.exclusive;

                if !valid {
                    return Err(anyhow!(
                        "File {} is already open in write mode:\n  prev={:?},\n  new={:?}",
                        file.id, prev_flags, new_flags)
                    );
                }

                cache.count += 1;
            }
        }

        self.cache.insert(file.id, StorageInterfaceCache {
            full_path: full_path.to_string(),
            mode: mode as i32,
            content: vec![],
            retrieved: false,
            modified: false,
            count: 1,
        });

        Ok(false)
    }

    fn read(&mut self, file: &FileRow, offset: u64, buff: &mut [u8]) -> Result<usize, AnyError> {
        let row = self.cache.get_mut(&file.id).ok_or_else(||
            anyhow!("Trying to use a file that was closed or never opened: {}", file.id)
        )?;

        if row.mode & libc::O_WRONLY != 0 {
            return Err(anyhow::anyhow!("File is write-only ({})", file.name));
        }

        if !row.retrieved {
            let content = if !file.sha512.is_empty() {
                let info = ObjInfo::new(file, &row.full_path);
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

    fn write(&mut self, file: &FileRow, offset: u64, buff: &[u8]) -> Result<usize, AnyError> {
        let row = self.cache.get_mut(&file.id).ok_or_else(||
            anyhow!("Trying to use a file that was closed or never opened: {}", file.id)
        )?;

        if row.mode & O_RDONLY != 0 {
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

    fn close(&mut self, file: &mut FileRow) -> Result<bool, AnyError> {
        let count = {
            let row = self.cache.get_mut(&file.id).ok_or_else(||
                anyhow!("Trying to use a file that was closed or never opened: {}", file.id)
            )?;

            row.count -= 1;
            row.count
        };

        match self.flush(file) {
            Ok(modified) => {
                if count <= 0 {
                    self.cache.remove(&file.id);
                }
                Ok(modified)
            }
            Err(e) => {
                // Clean up file even if there was an error
                if count <= 0 {
                    self.cache.remove(&file.id);
                }
                Err(e)
            }
        }
    }

    fn flush(&mut self, file: &mut FileRow) -> Result<bool, AnyError> {
        let mut modified = false;
        let row = self.cache.get_mut(&file.id).unwrap();

        if row.modified {
            // Shas of contents as id for the object
            let sha512 = hex::encode(hmac_sha512::Hash::hash(&row.content));

            // Remove old object
            if !file.sha512.is_empty() && file.sha512 != sha512 {
                let info = ObjInfo::new(file, &row.full_path);
                self.pending_remove.insert(info);
            }

            file.sha512 = sha512;
            let mut info = ObjInfo::new(file, &row.full_path);
            info.size = row.content.len() as u64;

            // Store new object
            self.obj_storage.put(&mut info, &row.content)?;

            // Update file metadata
            file.encryption_key = info.encryption_key;
            file.compression = info.compression;
            file.size = row.content.len() as i64;
            file.updated_at = current_timestamp();
            modified = true;
        }
        Ok(modified)
    }

    fn remove(&mut self, file: &FileRow, full_path: &str) -> Result<(), AnyError> {
        if self.cache.contains_key(&file.id) {
            return Err(anyhow!("File is open, cannot remove"));
        }
        if !file.sha512.is_empty() {
            self.pending_remove.insert(ObjInfo::new(file, full_path));
        }
        Ok(())
    }

    fn rename(&mut self, file: &FileRow, prev_full_path: &str, new_full_path: &str) -> Result<(), AnyError> {
        if self.cache.contains_key(&file.id) {
            return Err(anyhow!("File is open, cannot rename"));
        }

        // Directories are not stored as objects
        if file.kind == FILE_KIND_DIRECTORY {
            return Ok(());
        }

        let prev_info = ObjInfo::new(file, prev_full_path);
        let new_info = ObjInfo::new(file, new_full_path);

        self.obj_storage.rename(&prev_info, &new_info)?;
        Ok(())
    }

    fn cleanup(&mut self, is_in_use: ObjInUseFn) -> Result<(), AnyError> {
        for info in &self.pending_remove {
            self.obj_storage.remove(info, is_in_use.clone())?;
        }

        self.pending_remove.clear();
        Ok(())
    }

    fn nuke(&mut self) -> Result<(), AnyError> {
        self.cache.clear();
        self.pending_remove.clear();
        self.obj_storage.nuke()
    }
}