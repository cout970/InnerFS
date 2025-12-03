use crate::chunking::split_in_chunks;
use crate::metadata_db::{BlobRow, FileBlobRow, FileRow, MetadataDB, FILE_KIND_DIRECTORY};
use crate::obj_storage::replicated_object_storage::ReplicatedStorage;
use crate::obj_storage::{BlobStorage, ObjInfo, RemoteBlob};
use crate::utils::{current_timestamp, fast_hash, slow_hash};
use crate::AnyError;
use anyhow::{anyhow, Context};
use libc::{O_APPEND, O_RDONLY};
use std::cmp::min;
use std::collections::HashMap;

pub struct StorageInterface {
    pub blob_storage: ReplicatedStorage,
    pub metadata_db: MetadataDB,
    pub open_files: HashMap<u64, OpenFile>,
    pub unlinked_files: HashMap<i64, ObjInfo>,
    pub file_page_cache: HashMap<i64, Vec<u8>>,
    pub fh_counter: u64,
}

pub struct OpenFile {
    pub fh: u64,
    pub ino: i64,
    pub mode: i32,
    pub modified: bool,
}

impl StorageInterface {
    pub fn new(blob_storage: ReplicatedStorage, metadata_db: MetadataDB) -> Self {
        Self {
            blob_storage,
            metadata_db,
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
    pub fn open(&mut self, file: &mut FileRow, mode: u32) -> Result<u64, AnyError> {
        self.fh_counter += 1;
        let fh = self.fh_counter;

        self.open_files.insert(
            fh,
            OpenFile {
                fh,
                ino: file.id,
                mode: mode as i32,
                modified: false,
            },
        );

        Ok(fh)
    }

    /// Reads data from a file. Returns the number of bytes read.
    pub fn read(&mut self, fh: u64, file: &FileRow, offset: u64, buff: &mut [u8]) -> Result<usize, AnyError> {
        {
            let row = self
                .open_files
                .get_mut(&fh)
                .ok_or_else(|| anyhow!("Trying to use a file that is not open, fd: {}, ino: {}", fh, file.id))?;

            if row.mode & libc::O_WRONLY != 0 {
                return Err(anyhow::anyhow!("File is write-only ({})", file.name));
            }
        }

        if !self.file_page_cache.contains_key(&file.id) {
            let content = if !file.sha512.is_empty() {
                self.read_file(&file)?
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

    fn read_file(&mut self, file: &FileRow) -> Result<Vec<u8>, AnyError> {
        let blobs = self.metadata_db.get_file_blobs_by_file(file.id)?;

        let paths = blobs
            .iter()
            .map(|b| Self::get_blob_path(&b.blob.as_ref().unwrap().hash))
            .collect::<Vec<_>>();

        let remote_blobs = self
            .blob_storage
            .get_multiple(&paths.iter().map(|f| f.as_str()).collect::<Vec<&str>>())?;

        let mut content = Vec::with_capacity(file.size as usize);
        let mut offset = 0;

        for (index, blob) in remote_blobs.iter().enumerate() {
            // Verify offset
            let b = &blobs[index];
            if b.offset != offset {
                return Err(anyhow!(
                    "Something went wrong, blob offsets do not match, expected: {}, got: {}",
                    offset,
                    b.offset
                ));
            }

            let len = blob.len();
            content.extend(blob);
            offset += len as i64;
        }
        Ok(content)
    }

    fn write_file(&mut self, file: &mut FileRow, bytes: &[u8]) -> Result<(), AnyError> {
        // Sha512 of contents as id for the object
        file.sha512 = slow_hash(bytes);
        file.size = bytes.len() as i64;
        file.updated_at = current_timestamp();

        let chunks = split_in_chunks(bytes);
        let mut blobs = vec![];

        for chunk in chunks {
            let blob = BlobRow {
                id: 0,
                hash: fast_hash(&chunk),
                size: chunk.len() as i64,
                uses: 1,
                content: Some(chunk.to_vec()),
            };
            blobs.push(blob);
        }

        let found = self.metadata_db.get_existing_blobs(&blobs)?;

        self.metadata_db
            .execute0("BEGIN TRANSACTION")
            .context("Database error")?;

        let run = || {
            let mut remote_blobs = HashMap::new();
            let mut local_blobs = vec![];

            for blob in blobs {
                // Skip existing blobs
                let existing = found.iter().find(|b| b.hash == blob.hash && b.size == blob.size);
                if let Some(existing) = existing {
                    let local_blob = BlobRow {
                        id: existing.id,
                        hash: blob.hash.clone(),
                        size: blob.size,
                        uses: existing.uses + 1,
                        content: None,
                    };
                    local_blobs.push(local_blob.clone());
                    continue;
                }
                let local_blob = BlobRow {
                    id: 0,
                    hash: blob.hash.clone(),
                    size: blob.size,
                    uses: 1,
                    content: None,
                };
                local_blobs.push(local_blob.clone());

                if !remote_blobs.contains_key(&blob.hash) {
                    let remote_blob = RemoteBlob {
                        path: Self::get_blob_path(&blob.hash),
                        contents: blob.content.unwrap(),
                    };
                    remote_blobs.insert(blob.hash.clone(), remote_blob);
                }
            }

            let blobs_to_create = local_blobs.iter().filter(|b| b.id == 0).cloned().collect::<Vec<_>>();
            let new_blobs_ids = self.metadata_db.add_blobs(&blobs_to_create)?;

            for (index, b) in local_blobs.iter_mut().filter(|b| b.id == 0).enumerate() {
                b.id = new_blobs_ids[index];
            }

            let mut file_blobs = vec![];
            let mut offset = 0;

            for blob in local_blobs {
                file_blobs.push(FileBlobRow {
                    id: 0,
                    file_id: file.id,
                    offset,
                    blob_id: blob.id,
                    blob: None,
                });
                offset += blob.size;
            }

            self.metadata_db.replace_file_blobs(file.id, &file_blobs)?;

            let unused = self.metadata_db.get_unused_blobs()?;
            self.metadata_db.remove_unused_blobs()?;

            // Save the content of the blobs
            if !remote_blobs.is_empty() {
                let remote_blobs = remote_blobs.into_values().collect::<Vec<RemoteBlob>>();
                self.blob_storage.put_multiple(&remote_blobs)?;
            }

            // Delete unused blobs from storage
            if !unused.is_empty() {
                let paths = unused.iter().map(|b| Self::get_blob_path(&b.hash)).collect::<Vec<_>>();
                self.blob_storage
                    .remove_multiple(&paths.iter().map(|s| s.as_str()).collect::<Vec<&str>>())?;
            }

            Ok(())
        };

        let res: Result<(), AnyError> = run();

        if res.is_ok() {
            self.metadata_db.execute0("COMMIT").context("Database error")?;
        } else {
            self.metadata_db.execute0("ROLLBACK").context("Database error")?;
        }

        res
    }

    fn get_blob_path(hash: &str) -> String {
        format!("{}.dat", hash)
    }

    /// Writes data to a file. Returns the number of bytes written.
    pub fn write(&mut self, fh: u64, file: &FileRow, offset: u64, buff: &[u8]) -> Result<usize, AnyError> {
        let mode = {
            let row = self
                .open_files
                .get_mut(&fh)
                .ok_or_else(|| anyhow!("Trying to use a file that is not open, fd: {}, ino: {}", fh, file.id))?;

            row.mode
        };

        if mode & O_RDONLY != 0 {
            return Err(anyhow::anyhow!("File is read-only"));
        }

        // If the file is opened in append mode, load existing content to prevent overwriting
        if mode & O_APPEND != 0 && !self.file_page_cache.contains_key(&file.id) {
            let content = if !file.sha512.is_empty() {
                self.read_file(&file)?
            } else {
                vec![]
            };
            self.file_page_cache.insert(file.id, content);
        }

        let offset = offset as usize;
        let cache = self
            .file_page_cache
            .entry(file.id)
            .or_insert_with(|| Vec::with_capacity(1024 * 16));

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

        {
            let row = self
                .open_files
                .get_mut(&fh)
                .ok_or_else(|| anyhow!("Trying to use a file that is not open, fd: {}, ino: {}", fh, file.id))?;

            row.modified = true;
        }
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

        if !self.open_files.get(&fh).unwrap().modified {
            return Ok(false);
        }

        let mode = self.open_files.get(&fh).unwrap().mode;

        // If the file is opened in append mode, load existing content to prevent overwriting
        if mode & O_APPEND != 0 && !self.file_page_cache.contains_key(&file.id) {
            let content = if !file.sha512.is_empty() {
                self.read_file(&file)?
            } else {
                vec![]
            };
            self.file_page_cache.insert(file.id, content);
        }

        let empty: Vec<u8> = vec![];
        let cache = self.file_page_cache.get(&file.id).unwrap_or(&empty).clone();

        self.write_file(file, &cache)?;
        Ok(true)
    }

    /// Removes a file from the storage, the operation will be performed when cleanup is called.
    pub fn queue_remove(&mut self, file: &FileRow, full_path: &str) -> Result<(), AnyError> {
        self.unlinked_files.insert(file.id, ObjInfo::new(file, full_path));
        Ok(())
    }

    /// Renames a file.
    pub fn rename(&mut self, file: &FileRow, _prev_full_path: &str, _new_full_path: &str) -> Result<(), AnyError> {
        // TODO check if rename is supported while a file is being written
        if !self.get_open_files_by_ino(file.id).is_empty() {
            return Err(anyhow!("File is open, cannot rename"));
        }

        // Directories are not stored as objects
        if file.kind == FILE_KIND_DIRECTORY {
            return Ok(());
        }

        // Blobs are independent of the file name
        // No need to rename them

        Ok(())
    }

    /// Performs the remove operation on all files that are pending removal.
    pub fn cleanup(&mut self) -> Result<Vec<(i64, String)>, AnyError> {
        let mut removed = vec![];

        self.metadata_db
            .execute0("BEGIN TRANSACTION")
            .context("Database error")?;

        let mut run = || {
            for (id, info) in &self.unlinked_files {
                if self.get_open_files_by_ino(*id).is_empty() {
                    // Decrease the use count of the blobs
                    self.metadata_db.replace_file_blobs(info.id, &[])?;
                    removed.push((*id, info.external_id.clone()));
                }
            }

            let unused = self.metadata_db.get_unused_blobs()?;
            self.metadata_db.remove_unused_blobs()?;

            if !unused.is_empty() {
                let paths = unused.iter().map(|b| Self::get_blob_path(&b.hash)).collect::<Vec<_>>();
                self.blob_storage
                    .remove_multiple(&paths.iter().map(|s| s.as_str()).collect::<Vec<&str>>())?;
            }

            Ok(())
        };

        let result = run();

        if result.is_ok() {
            self.metadata_db.execute0("COMMIT").context("Database error")?;
        } else {
            self.metadata_db.execute0("ROLLBACK").context("Database error")?;
            return Err(result.unwrap_err());
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
        self.blob_storage.nuke()
    }
}
