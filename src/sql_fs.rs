use std::error::Error;
use std::fmt::{Display, Formatter};
use std::rc::Rc;

use crate::config::Config;
use crate::AnyError;
use crate::metadata_db::{DirectoryEntry, FileChangeKind, FileRow, MetadataDB, FILE_KIND_DIRECTORY, FILE_KIND_REGULAR};
use crate::storage::Storage;
use anyhow::{anyhow, Context};
use libc::{EEXIST, EINVAL, EIO, EISDIR, ENOENT, ENOTDIR, ENOTEMPTY, O_RDONLY, O_WRONLY};
use crate::obj_storage::UniquenessTest;
use crate::utils::current_timestamp;

pub struct SqlFileSystem {
    pub sql: Rc<MetadataDB>,
    pub config: Rc<Config>,
    pub storage: Box<dyn Storage>,
}

#[derive(Debug)]
pub struct SqlFileSystemError {
    pub code: i32,
    pub error: AnyError,
}

impl SqlFileSystem {
    pub fn new(sql: Rc<MetadataDB>, config: Rc<Config>, storage: Box<dyn Storage>) -> Self {
        Self { sql, config, storage }
    }

    pub fn read_all(&mut self, id: i64) -> Result<Vec<u8>, SqlFileSystemError> {
        const BLOCK_SIZE: usize = 65536; // 64kb

        let mut file = self.get_file_or_err(id)?;
        let full_path = self.sql.get_file_path(file.id)?;
        let modified = self.storage.open(&mut file, &full_path, O_RDONLY as u32)?;

        if modified {
            self.sql.update_file(&file)?;

            if self.config.store_file_change_history {
                self.sql.register_file_change(&file, FileChangeKind::UpdatedMetadata)?;
            }
        } else if self.config.update_access_time {
            self.sql.file_set_access_time(file.id, current_timestamp())?;
        }

        let mut complete_buff: Vec<u8> = Vec::with_capacity(file.size as usize);
        let mut buff = vec![0u8; BLOCK_SIZE];
        let mut offset = 0;

        loop {
            let len = self.storage.read(&file, offset as u64, &mut buff)?;
            if len == 0 {
                break;
            }
            offset += len;
            complete_buff.extend(&buff[..len]);
        }

        let modified = self.storage.close(&mut file)?;
        if modified {
            self.sql.update_file(&file)?;

            if self.config.store_file_change_history {
                self.sql.register_file_change(&file, FileChangeKind::UpdatedContents)?;
            }
        } else if self.config.update_access_time {
            self.sql.file_set_access_time(file.id, current_timestamp())?;
        }

        self.cleanup()?;
        Ok(complete_buff)
    }

    #[allow(dead_code)]
    pub fn write_all(&mut self, id: i64, contents: &[u8]) -> Result<(), SqlFileSystemError> {
        const BLOCK_SIZE: usize = 65536; // 64kb

        let mut file = self.get_file_or_err(id)?;
        let full_path = self.sql.get_file_path(file.id)?;
        let modified = self.storage.open(&mut file, &full_path, O_WRONLY as u32)?;

        if modified {
            self.sql.update_file(&file)?;

            if self.config.store_file_change_history {
                self.sql.register_file_change(&file, FileChangeKind::UpdatedMetadata)?;
            }
        } else if self.config.update_access_time {
            self.sql.file_set_access_time(file.id, current_timestamp())?;
        }

        let mut offset = 0;

        loop {
            let section = if offset + BLOCK_SIZE < contents.len() {
                &contents[offset..(offset + BLOCK_SIZE)]
            } else {
                &contents[offset..]
            };

            if section.is_empty() { break; }

            let len = self.storage.write(&file, offset as u64, section)?;
            if len == 0 {
                break;
            }
            offset += len;
        }

        let modified = self.storage.close(&mut file)?;
        if modified {
            self.sql.update_file(&file)?;

            if self.config.store_file_change_history {
                self.sql.register_file_change(&file, FileChangeKind::UpdatedContents)?;
            }
        } else if self.config.update_access_time {
            self.sql.file_set_access_time(file.id, current_timestamp())?;
        }

        self.cleanup()?;
        Ok(())
    }

    pub fn move_file(&mut self, parent_id: i64, name: &str, new_parent_id: i64, new_name: &str) -> Result<(), SqlFileSystemError> {
        self.transaction(|this| {
            let now = current_timestamp();
            let old_path = format!("{}/{}", this.sql.get_file_path(parent_id)?, name);
            let new_path = format!("{}/{}", this.sql.get_file_path(new_parent_id)?, new_name);

            // Remove the already existing file in the target location
            if let Some(new_entry) = this.sql.find_directory_entry(new_parent_id, new_name)? {
                this.sql.remove_file(new_entry.entry_file_id)?;
            }

            let old_entry = this.find_directory_entry_or_err(parent_id, name)?;
            let mut file = this.get_file_or_err(old_entry.entry_file_id)?;

            // Unlink from old parent
            this.sql.remove_directory_entry(old_entry.id)?;

            // Link to new parent
            this.sql.add_directory_entry(&DirectoryEntry {
                id: 0,
                directory_file_id: new_parent_id,
                entry_file_id: file.id,
                name: new_name.to_string(),
                kind: file.kind,
            })?;

            // Update file metadata
            file.name = new_name.to_string();
            file.updated_at = current_timestamp();
            file.accessed_at = current_timestamp();
            this.sql.update_file(&file)?;

            // Move in backend storage
            this.storage.rename(&file, &old_path, &new_path)?;

            if this.config.update_access_time {
                this.sql.file_set_access_time(file.id, now)?;
                this.sql.file_set_access_time(parent_id, now)?;
                this.sql.file_set_access_time(new_parent_id, now)?;
            }

            if this.config.store_file_change_history {
                let old_parent = this.get_file_or_err(parent_id)?;
                let new_parent = this.get_file_or_err(new_parent_id)?;

                this.sql.register_file_change(&file, FileChangeKind::UpdatedContents)?;
                this.sql.register_file_change(&old_parent, FileChangeKind::UpdatedContents)?;
                this.sql.register_file_change(&new_parent, FileChangeKind::UpdatedContents)?;
            }

            Ok(())
        })?;

        Ok(())
    }

    #[allow(dead_code)]
    pub fn copy_file(&mut self, parent_id: i64, name: &str, new_parent_id: i64, new_name: &str) -> Result<i64, SqlFileSystemError> {
        if !self.is_validate_file_name(name) {
            return error(EINVAL, anyhow!("Invalid file name: {}", name));
        }

        if !self.is_validate_file_name(new_name) {
            return error(EINVAL, anyhow!("Invalid file name: {}", new_name));
        }

        let new_parent = self.get_file_or_err(new_parent_id)?;
        let new_entry = self.sql.find_directory_entry(new_parent.id, new_name)?;

        if new_entry.is_some() {
            return error(EEXIST, anyhow!("File already exists: {}", new_name));
        }

        let file = self.lookup(parent_id, name)?.unwrap();

        self.transaction(|this| {
            let now = current_timestamp();
            let mut new_file = FileRow {
                id: 0,
                version: 1,
                kind: FILE_KIND_REGULAR,
                name: new_name.to_string(),
                uid: file.uid,
                gid: file.gid,
                perms: file.perms,
                size: 0,
                sha512: "".to_string(),
                encryption_key: "".to_string(),
                accessed_at: if this.config.update_access_time { now } else { 0 },
                created_at: now,
                updated_at: now,
            };

            let new_id = this.sql.add_file(&new_file)?;
            new_file.id = new_id;

            this.sql.add_directory_entry(&DirectoryEntry {
                id: 0,
                directory_file_id: new_parent.id,
                entry_file_id: new_file.id,
                name: new_name.to_string(),
                kind: file.kind,
            })?;

            // Copy file contents
            let contents = this.read_all(file.id)?;
            this.write_all(new_file.id, &contents)?;

            if this.config.update_access_time {
                this.sql.file_set_access_time(parent_id, now)?;
                this.sql.file_set_access_time(new_parent_id, now)?;
            }

            if this.config.store_file_change_history {
                this.sql.register_file_change(&new_file, FileChangeKind::Created)?;
                this.sql.register_file_change(&new_parent, FileChangeKind::UpdatedContents)?;
            }

            Ok(new_id)
        })
    }

    pub fn lookup(&mut self, parent: i64, name: &str) -> Result<Option<FileRow>, SqlFileSystemError> {
        let dir_file = self.get_file_or_err(parent)?;

        if dir_file.kind != FILE_KIND_DIRECTORY {
            return error(ENOTDIR, anyhow!("Not a directory: {}", parent));
        }

        if self.config.update_access_time {
            self.sql.file_set_access_time(parent, current_timestamp())?;
        }

        let entry = self.sql.find_directory_entry(dir_file.id, name)?;

        if entry.is_none() {
            return Ok(None);
        }

        let entry = entry.unwrap();
        let file = self.get_file_or_err(entry.entry_file_id)?;
        Ok(Some(file))
    }

    pub fn getattr(&mut self, id: i64) -> Result<FileRow, SqlFileSystemError> {
        self.get_file_or_err(id)
    }

    pub fn setattr(
        &mut self, id: i64, mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, size: Option<u64>,
        atime: Option<i64>, mtime: Option<i64>, crtime: Option<i64>,
    ) -> Result<FileRow, SqlFileSystemError> {
        self.transaction(|this| {
            let mut file = this.get_file_or_err(id)?;

            if let Some(mode) = mode {
                file.perms = mode as i64;
            }
            if let Some(uid) = uid {
                file.uid = uid as i64;
            }
            if let Some(gid) = gid {
                file.gid = gid as i64;
            }
            if let Some(size) = size {
                file.size = size as i64;
            }
            if let Some(atime) = atime {
                file.accessed_at = atime;
            }
            if let Some(mtime) = mtime {
                file.updated_at = mtime;
            }
            if let Some(crtime) = crtime {
                file.created_at = crtime;
            }

            this.sql.update_file(&file)?;

            if this.config.store_file_change_history {
                this.sql.register_file_change(&file, FileChangeKind::UpdatedMetadata)?;
            }
            Ok(file)
        })
    }

    pub fn mkdir(&mut self, parent: i64, name: &str, uid: u32, gid: u32, mode: u32) -> Result<FileRow, SqlFileSystemError> {
        if !self.is_validate_file_name(name) {
            return error(EINVAL, anyhow!("Invalid file name: {}", name));
        }

        let parent_directory = self.get_file_or_err(parent)?;

        self.transaction(|this| {
            let now = current_timestamp();
            let mut file = FileRow {
                id: 0,
                version: 1,
                kind: FILE_KIND_DIRECTORY,
                name: name.to_string(),
                uid: uid as i64,
                gid: gid as i64,
                perms: mode as i64,
                size: 0,
                sha512: "".to_string(),
                encryption_key: "".to_string(),
                accessed_at: if this.config.update_access_time { now } else { 0 },
                created_at: now,
                updated_at: now,
            };

            let id = this.sql.add_file(&file)?;
            file.id = id;

            // child entry to itself
            this.sql.add_directory_entry(&DirectoryEntry {
                id: 0,
                directory_file_id: id,
                entry_file_id: id,
                name: ".".to_string(),
                kind: FILE_KIND_DIRECTORY,
            })?;

            // child entry to parent
            this.sql.add_directory_entry(&DirectoryEntry {
                id: 0,
                directory_file_id: id,
                entry_file_id: parent,
                name: "..".to_string(),
                kind: FILE_KIND_DIRECTORY,
            })?;

            // parent entry to child
            this.sql.add_directory_entry(&DirectoryEntry {
                id: 0,
                directory_file_id: parent,
                entry_file_id: id,
                name: name.to_string(),
                kind: file.kind,
            })?;

            if this.config.update_access_time {
                this.sql.file_set_access_time(parent, now)?;
            }

            if this.config.store_file_change_history {
                this.sql.register_file_change(&file, FileChangeKind::Created)?;
                this.sql.register_file_change(&parent_directory, FileChangeKind::UpdatedContents)?;
            }

            Ok(file)
        })
    }

    pub fn mknod(&mut self, parent: i64, name: &str, uid: u32, gid: u32, mode: u32) -> Result<FileRow, SqlFileSystemError> {
        if !self.is_validate_file_name(name) {
            return error(EINVAL, anyhow!("Invalid file name: {}", name));
        }

        let parent_directory = self.get_file_or_err(parent)?;

        if parent_directory.kind != FILE_KIND_DIRECTORY {
            return error(ENOTDIR, anyhow!("Not a directory: {}", parent));
        }

        let existing_entry = self.sql.find_directory_entry(parent_directory.id, name)?;

        if existing_entry.is_some() {
            return error(EEXIST, anyhow!("File already exists: {}", name));
        }

        let id = self.transaction(|this| {
            let now = current_timestamp();
            let mut file = FileRow {
                id: 0,
                version: 1,
                kind: FILE_KIND_REGULAR,
                name: name.to_string(),
                uid: uid as i64,
                gid: gid as i64,
                perms: mode as i64,
                size: 0,
                sha512: "".to_string(),
                encryption_key: "".to_string(),
                accessed_at: if this.config.update_access_time { now } else { 0 },
                created_at: now,
                updated_at: now,
            };

            let id = this.sql.add_file(&file)?;
            file.id = id;

            // parent entry to child
            this.sql.add_directory_entry(&DirectoryEntry {
                id: 0,
                directory_file_id: parent,
                entry_file_id: id,
                name: name.to_string(),
                kind: file.kind,
            })?;

            if this.config.update_access_time {
                this.sql.file_set_access_time(parent, now)?;
            }

            if this.config.store_file_change_history {
                this.sql.register_file_change(&file, FileChangeKind::Created)?;
                this.sql.register_file_change(&parent_directory, FileChangeKind::UpdatedContents)?;
            }
            Ok(id)
        })?;

        self.get_file_or_err(id)
    }

    pub fn unlink(&mut self, parent: i64, name: &str) -> Result<(), SqlFileSystemError> {
        if !self.is_validate_file_name(name) {
            return error(EINVAL, anyhow!("Invalid file name: {}", name));
        }

        let dir_entry = self.find_directory_entry_or_err(parent, name)?;
        let file = self.get_file_or_err(dir_entry.entry_file_id)?;

        if file.kind == FILE_KIND_DIRECTORY {
            return error(EISDIR, anyhow!("Cannot unlink directory"));
        }

        let parent_directory = self.get_file_or_err(parent)?;
        let full_path = self.sql.get_file_path(file.id)?;
        self.storage.remove(&file, &full_path)?;
        self.sql.remove_file(dir_entry.entry_file_id)?;

        if self.config.store_file_change_history {
            self.sql.register_file_change(&file, FileChangeKind::Deleted)?;
            self.sql.register_file_change(&parent_directory, FileChangeKind::UpdatedContents)?;
        }
        self.cleanup()?;
        Ok(())
    }

    pub fn rmdir(&mut self, parent: i64, name: &str) -> Result<(), SqlFileSystemError> {
        if !self.is_validate_file_name(name) {
            return error(EINVAL, anyhow!("Invalid file name: {}", name));
        }

        let dir_entry = self.find_directory_entry_or_err(parent, name)?;
        let file = self.get_file_or_err(dir_entry.entry_file_id)?;
        let parent_directory = self.get_file_or_err(dir_entry.directory_file_id)?;

        // File is not a directory
        if file.kind != FILE_KIND_DIRECTORY {
            return error(ENOTDIR, anyhow!("Not a directory: {}", file.id));
        }

        let entries = self.sql.get_directory_entries(dir_entry.entry_file_id, 10, 0)?;

        // Cannot delete non-empty directory
        if entries.len() > 2 {
            return error(ENOTEMPTY, anyhow!("Directory not empty: {}", file.id));
        }

        self.sql.remove_file(dir_entry.entry_file_id)?;
        self.cleanup()?;

        if self.config.store_file_change_history {
            self.sql.register_file_change(&file, FileChangeKind::Deleted)?;
            self.sql.register_file_change(&parent_directory, FileChangeKind::UpdatedContents)?;
        }
        Ok(())
    }

    pub fn rename(&mut self, parent: i64, old_name: &str, new_name: &str) -> Result<(), SqlFileSystemError> {
        if !self.is_validate_file_name(old_name) {
            return error(EINVAL, anyhow!("Invalid file name: {}", old_name));
        }

        if !self.is_validate_file_name(new_name) {
            return error(EINVAL, anyhow!("Invalid file name: {}", new_name));
        }

        let entry = self.find_directory_entry_or_err(parent, &old_name)?;
        let new_entry = self.sql.find_directory_entry(parent, &new_name)?;

        if let Some(new_entry) = new_entry {
            match new_entry.kind {
                FILE_KIND_DIRECTORY => {
                    return error(EISDIR, anyhow!("Cannot overwrite directory: {} -> {}", old_name, new_name));
                }
                FILE_KIND_REGULAR => {
                    // When moving into an existing file, unlink it first
                    self.unlink(parent, new_name)?;
                }
                _ => {
                    return error(EEXIST, anyhow!("File already exists: {}", new_name));
                }
            }
        }

        self.transaction(|this| {
            let parent_directory = this.get_file_or_err(parent)?;
            let mut entry = entry;

            entry.name = new_name.to_string();
            this.sql.update_directory_entry(&entry)?;

            let mut file = this.get_file_or_err(entry.entry_file_id)?;
            let prev_path = this.sql.get_file_path(file.id)?;

            file.name = new_name.to_string();
            this.sql.update_file(&file)?;

            let new_path = this.sql.get_file_path(file.id)?;
            this.storage.rename(&file, &prev_path, &new_path)?;

            if this.config.store_file_change_history {
                this.sql.register_file_change(&file, FileChangeKind::UpdatedContents)?;
                this.sql.register_file_change(&parent_directory, FileChangeKind::UpdatedContents)?;
            }

            Ok(())
        })
    }

    pub fn open(&mut self, id: i64, flags: u32) -> Result<(), SqlFileSystemError> {
        let mut file = self.get_file_or_err(id)?;

        let full_path = self.sql.get_file_path(file.id)?;
        let modified = self.storage.open(&mut file, &full_path, flags).context("Error opening file")?;

        file.accessed_at = current_timestamp();

        if modified {
            self.sql.update_file(&file)?;

            if self.config.store_file_change_history {
                self.sql.register_file_change(&file, FileChangeKind::UpdatedMetadata)?;
            }
        } else if self.config.update_access_time {
            self.sql.file_set_access_time(file.id, current_timestamp())?;
        }

        Ok(())
    }

    pub fn read(&mut self, id: i64, offset: i64, size: usize) -> Result<Vec<u8>, SqlFileSystemError> {
        let file = self.get_file_or_err(id)?;

        let mut buff = vec![0u8; size];
        let len = self.storage.read(&file, offset as u64, &mut buff)?;
        buff.truncate(len);
        Ok(buff)
    }

    pub fn write(&mut self, id: i64, offset: i64, data: &[u8]) -> Result<usize, SqlFileSystemError> {
        let file = self.get_file_or_err(id)?;

        let len = self.storage.write(&file, offset as u64, data)?;
        Ok(len)
    }

    pub fn release(&mut self, id: i64) -> Result<(), SqlFileSystemError> {
        let mut file = self.get_file_or_err(id)?;
        let modified = self.storage.close(&mut file)?;

        if modified {
            self.sql.update_file(&file)?;

            if self.config.store_file_change_history {
                self.sql.register_file_change(&file, FileChangeKind::UpdatedContents)?;
            }
        }

        self.cleanup()?;
        Ok(())
    }

    pub fn readdir(&mut self, id: i64, offset: i64) -> Result<Vec<DirectoryEntry>, SqlFileSystemError> {
        let entries = self.sql.get_directory_entries(id, 1024, offset)?;

        if self.config.update_access_time {
            self.sql.file_set_access_time(id, current_timestamp())?;
        }

        Ok(entries)
    }

    pub fn cleanup(&mut self) -> Result<(), SqlFileSystemError> {
        let sql = self.sql.clone();
        self.storage.cleanup(Rc::new(move |info, test| {
            let exists = match test {
                UniquenessTest::Path => {
                    sql.get_file_by_path(&info.full_path)?.is_some()
                }
                UniquenessTest::Sha512 => {
                    sql.get_file_by_sha512(&info.sha512)?.is_some()
                }
            };
            Ok(exists)
        }))?;
        Ok(())
    }

    pub fn get_file_or_err(&mut self, id: i64) -> Result<FileRow, SqlFileSystemError> {
        let file = self.sql.get_file(id)?;

        if file.is_none() {
            return error(ENOENT, anyhow!("File not found: {}", id));
        }

        Ok(file.unwrap())
    }

    pub fn find_directory_entry_or_err(&mut self, id: i64, name: &str) -> Result<DirectoryEntry, SqlFileSystemError> {
        let entry = self.sql.find_directory_entry(id, name)?;

        if entry.is_none() {
            return error(ENOENT, anyhow!("Directory entry not found: {}", id));
        }

        Ok(entry.unwrap())
    }

    pub fn is_validate_file_name(&self, name: &str) -> bool {
        name.len() > 0 && name.len() <= 255 && !name.contains("/") && name != "." && name != ".."
    }

    pub fn transaction<R>(&mut self, func: impl FnOnce(&mut Self) -> Result<R, SqlFileSystemError>) -> Result<R, SqlFileSystemError> {
        self.sql.connection.execute("BEGIN TRANSACTION").context("Database error")?;
        let res = func(self);
        if res.is_ok() {
            self.sql.connection.execute("COMMIT").context("Database error")?;
        } else {
            self.sql.connection.execute("ROLLBACK").context("Database error")?;
        }
        res
    }
}

fn error<T>(code: i32, error: AnyError) -> Result<T, SqlFileSystemError> {
    Err(SqlFileSystemError { code, error })
}

impl From<AnyError> for SqlFileSystemError {
    fn from(value: AnyError) -> Self {
        SqlFileSystemError { code: EIO, error: value }
    }
}

impl Display for SqlFileSystemError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SqlFileSystemError: {} {}", self.code, self.error)
    }
}

impl Error for SqlFileSystemError {}
