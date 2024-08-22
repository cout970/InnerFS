use crate::obj_storage::ObjInfo;
use crate::sql::{FileRow};

pub trait Storage {
    fn open(&mut self, file: &mut FileRow, full_path: &str, mode: u32) -> Result<bool, anyhow::Error>;
    fn read(&mut self, file: &FileRow, offset: u64, buff: &mut [u8]) -> Result<usize, anyhow::Error>;
    fn write(&mut self, file: &FileRow, offset: u64, buff: &[u8]) -> Result<usize, anyhow::Error>;
    fn close(&mut self, file: &mut FileRow) -> Result<bool, anyhow::Error>;
    fn remove(&mut self, file: &FileRow, full_path: &str) -> Result<(), anyhow::Error>;
    fn cleanup(&mut self, is_in_use: Box<dyn Fn(&ObjInfo) -> Result<bool, anyhow::Error>>) -> Result<(), anyhow::Error>;
}
