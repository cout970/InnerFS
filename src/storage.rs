use std::rc::Rc;
use crate::AnyError;
use crate::obj_storage::{ObjInfo, UniquenessTest};
use crate::metadata_db::{FileRow};

pub type ObjInUseFn = Rc<dyn Fn(&ObjInfo, UniquenessTest) -> Result<bool, AnyError>>;

pub trait Storage {
    fn open(&mut self, file: &mut FileRow, full_path: &str, mode: u32) -> Result<bool, AnyError>;
    fn read(&mut self, file: &FileRow, offset: u64, buff: &mut [u8]) -> Result<usize, AnyError>;
    fn write(&mut self, file: &FileRow, offset: u64, buff: &[u8]) -> Result<usize, AnyError>;
    fn close(&mut self, file: &mut FileRow) -> Result<bool, AnyError>;
    fn flush(&mut self, file: &mut FileRow) -> Result<bool, AnyError>;
    fn remove(&mut self, file: &FileRow, full_path: &str) -> Result<(), AnyError>;
    fn rename(&mut self, file: &FileRow, prev_full_path: &str, new_full_path: &str) -> Result<(), AnyError>;
    fn cleanup(&mut self, is_in_use: ObjInUseFn) -> Result<(), AnyError>;
    fn nuke(&mut self) -> Result<(), AnyError>;
}
