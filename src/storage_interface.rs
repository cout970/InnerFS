use crate::metadata_db::{FileRow, FILE_KIND_DIRECTORY};
use crate::obj_storage::{ObjInfo, ObjectStorage, PathGenerator};
use crate::utils::current_timestamp;
use crate::AnyError;
use anyhow::anyhow;
use libc::{O_APPEND, O_RDONLY};
use std::cmp::min;
use std::collections::HashMap;
use std::sync::Arc;

/// Callback to detects if a file is still in use, allowing correct deletion of de-duplicated files.
pub type ObjInUseFn = Arc<dyn Fn(&ObjInfo, PathGenerator) -> Result<bool, AnyError>>;

pub struct StorageInterface {
    pub obj_storage: Box<dyn ObjectStorage + Send + Sync>,
    pub open_files: HashMap<u64, OpenFile>,
    pub unlinked_files: HashMap<i64, ObjInfo>,
    pub file_page_cache: HashMap<i64, Vec<u8>>,
    pub fh_counter: u64,
}

pub struct OpenFile {
    pub fh: u64,
    pub ino: i64,
    pub full_path: String,
    pub mode: i32,
    pub modified: bool,
}

impl StorageInterface {
    pub fn new(obj_storage: Box<dyn ObjectStorage>) -> Self {
        Self {
            obj_storage,
            open_files: HashMap::new(),
            unlinked_files: HashMap::new(),
            file_page_cache: HashMap::new(),
            fh_counter: 0,
        }
    }

    pub fn clone(&self) -> StorageInterface {
        Self {
            obj_storage: self.obj_storage.clone(),
            open_files: HashMap::new(),
            unlinked_files: HashMap::new(),
            file_page_cache: HashMap::new(),
            fh_counter: 0,
        }
    }

    pub fn is_file_open(&self, id: i64) -> bool {
        self.open_files.contains_key(&(id as u64))
    }

    pub fn get_open_files_by_ino(&self, ino: i64) -> Vec<u64> {
        self.open_files
            .values()
            .filter(|f| f.ino == ino)
            .map(|f| f.fh)
            .collect()
    }

    /// Opens a file for reading or writing. Returns true if the file was opened successfully.
    pub fn open(&mut self, file: &mut FileRow, full_path: &str, mode: u32) -> Result<u64, AnyError> {
        if (mode as i32) & O_APPEND != 0 {
            return Err(anyhow::anyhow!("Append mode is not supported"));
        }

        self.fh_counter += 1;
        let fh = self.fh_counter;

        self.open_files.insert(
            fh,
            OpenFile {
                fh,
                ino: file.id,
                full_path: full_path.to_string(),
                mode: mode as i32,
                modified: false,
            },
        );

        Ok(fh)
    }

    /// Reads data from a file. Returns the number of bytes read.
    pub fn read(&mut self, fh: u64, file: &FileRow, offset: u64, buff: &mut [u8]) -> Result<usize, AnyError> {
        let row = self
            .open_files
            .get_mut(&fh)
            .ok_or_else(|| anyhow!("Trying to use a file that is not open, fd: {}, ino: {}", fh, file.id))?;

        if row.mode & libc::O_WRONLY != 0 {
            return Err(anyhow::anyhow!("File is write-only ({})", file.name));
        }

        if !self.file_page_cache.contains_key(&file.id) {
            let content = if !file.sha512.is_empty() {
                let info = ObjInfo::new(file, &row.full_path);
                self.obj_storage.get(&info)?
            } else {
                vec![]
            };
            self.file_page_cache.insert(file.id, content);
        }

        let cache = self.file_page_cache.get(&file.id).unwrap();

        if offset >= cache.len() as u64 {
            return Ok(0);
        }

        let remaining_content_slice = &cache[offset as usize..];
        let read_len = min(buff.len(), remaining_content_slice.len());
        buff[..read_len].copy_from_slice(&remaining_content_slice[..read_len]);
        Ok(read_len)
    }

    /// Writes data to a file. Returns the number of bytes written.
    pub fn write(&mut self, fh: u64, file: &FileRow, offset: u64, buff: &[u8]) -> Result<usize, AnyError> {
        let row = self
            .open_files
            .get_mut(&fh)
            .ok_or_else(|| anyhow!("Trying to use a file that is not open, fd: {}, ino: {}", fh, file.id))?;

        if row.mode & O_RDONLY != 0 {
            return Err(anyhow::anyhow!("File is read-only"));
        }

        let offset = offset as usize;
        let cache = self.file_page_cache.entry(file.id).or_insert_with(|| Vec::with_capacity(1024 * 16));

        if offset == buff.len() {
            // Append to the end
            cache.extend(buff.iter());
        } else {
            // Overwrite
            if offset + buff.len() > cache.len() {
                cache.resize(offset + buff.len(), 0);
            }
            cache[offset..offset + buff.len()].copy_from_slice(buff);
        }

        row.modified = true;
        Ok(buff.len())
    }

    /// Closes a file. Returns true if the file was modified between open and close.
    pub fn close(&mut self, fh: u64, file: &mut FileRow) -> Result<bool, AnyError> {
        if !self.open_files.contains_key(&fh) {
            return Err(anyhow!("Trying to close a file that is not open, fd: {}, ino: {}", fh, file.id));
        }

        match self.flush(fh, file) {
            Ok(modified) => {
                self.open_files.remove(&fh);
                if !self.is_file_open(file.id) {
                    self.file_page_cache.remove(&file.id);
                }
                Ok(modified)
            }
            Err(e) => {
                // Clean up file even if there was an error
                self.open_files.remove(&fh);
                Err(e)
            }
        }
    }

    /// Flushes the file to disk. Returns true if the file was modified.
    pub fn flush(&mut self, fh: u64, file: &mut FileRow) -> Result<bool, AnyError> {
        if !self.open_files.contains_key(&fh) {
            return Err(anyhow!("Trying to close a file that is not open, fd: {}, ino: {}", fh, file.id));
        }

        let mut modified = false;
        let row = self.open_files.get_mut(&fh).unwrap();

        if row.modified {
            let empty: Vec<u8> = vec![];
            let cache = self.file_page_cache.get(&file.id).unwrap_or(&empty);

            // Shas of contents as id for the object
            let sha512 = hex::encode(hmac_sha512::Hash::hash(&cache));

            // This operation was disabled on purpose
            // When the storage backend uses a sha512 as the object id, new write will create a new object
            // so the old object needs to be removed, however, this is not always the case.
            // If the backend uses the original file path or the external_id as the object id,
            // removing the old object, which is done at cleanup, will remove the recently overwritten object.
            // Remove old object
            // if !file.sha512.is_empty() && file.sha512 != sha512 {
            //     let info = ObjInfo::new(file, &row.full_path);
            //     self.overrided_objects.push(info.id, info);
            // }

            file.sha512 = sha512;
            let mut info = ObjInfo::new(file, &row.full_path);
            info.size = cache.len() as u64;

            // Store new object
            self.obj_storage.put(&mut info, &cache)?;

            // Update file metadata
            file.encryption_key = info.encryption_key;
            file.compression = info.compression;
            file.size = cache.len() as i64;
            file.updated_at = current_timestamp();
            modified = true;
        }
        Ok(modified)
    }

    /// Removes a file from the storage, the operation will be performed when cleanup is called.
    pub fn queue_remove(&mut self, file: &FileRow, full_path: &str) -> Result<(), AnyError> {
        self.unlinked_files.insert(file.id, ObjInfo::new(file, full_path));
        Ok(())
    }

    /// Renames a file.
    pub fn rename(&mut self, file: &FileRow, prev_full_path: &str, new_full_path: &str) -> Result<(), AnyError> {
        // TODO check if rename is supported while a file is being written
        if !self.get_open_files_by_ino(file.id).is_empty() {
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

    /// Performs the remove operation on all files that are pending removal.
    pub fn cleanup(&mut self, is_in_use: ObjInUseFn) -> Result<Vec<(i64, String)>, AnyError> {
        let mut removed = vec![];

        for (id, info) in &self.unlinked_files {
            if self.get_open_files_by_ino(*id).is_empty() {
                self.obj_storage.remove(info, is_in_use.clone())?;
                removed.push((*id, info.external_id.clone()));
            }
        }

        for (id, _) in &removed {
            self.unlinked_files.remove(id);
        }
        Ok(removed)
    }

    /// Removes all files from the storage.
    pub fn nuke(&mut self) -> Result<(), AnyError> {
        self.open_files.clear();
        self.unlinked_files.clear();
        self.obj_storage.nuke()
    }
}
