use crate::metadata_db::FileRow;
use crate::obj_storage::{ObjInfo, PathGenerator};
use crate::AnyError;
use std::rc::Rc;

/// Callback to detects if a file is still in use, allowing correct deletion of de-duplicated files.
pub type ObjInUseFn = Rc<dyn Fn(&ObjInfo, PathGenerator) -> Result<bool, AnyError>>;

/// Interface for file storage, the blob storage must be delegated to an ObjectStorage implementation.
pub trait Storage {
    /// Opens a file for reading or writing. Returns true if the file was opened successfully.
    fn open(&mut self, file: &mut FileRow, full_path: &str, mode: u32) -> Result<bool, AnyError>;

    /// Reads data from a file. Returns the number of bytes read.
    fn read(&mut self, file: &FileRow, offset: u64, buff: &mut [u8]) -> Result<usize, AnyError>;

    /// Writes data to a file. Returns the number of bytes written.
    fn write(&mut self, file: &FileRow, offset: u64, buff: &[u8]) -> Result<usize, AnyError>;

    /// Closes a file. Returns true if the file was modified between open and close.
    fn close(&mut self, file: &mut FileRow) -> Result<bool, AnyError>;

    // Flushes the file to disk. Returns true if the file was modified.
    fn flush(&mut self, file: &mut FileRow) -> Result<bool, AnyError>;

    // Removes a file from the storage, the operation will be performed when cleanup is called.
    fn remove(&mut self, file: &FileRow, full_path: &str) -> Result<(), AnyError>;

    /// Renames a file.
    fn rename(&mut self, file: &FileRow, prev_full_path: &str, new_full_path: &str) -> Result<(), AnyError>;

    /// Performs the remove operation on all files that are pending removal.
    fn cleanup(&mut self, is_in_use: ObjInUseFn) -> Result<(), AnyError>;

    /// Removes all files from the storage.
    fn nuke(&mut self) -> Result<(), AnyError>;

    /// Creates a new storage instance with the same settings.
    #[allow(dead_code)]
    fn clone(&self) -> Box<dyn Storage>;
}
