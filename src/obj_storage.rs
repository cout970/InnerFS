use std::fmt::Display;
use std::fs;
use std::path::PathBuf;
use anyhow::Context;
use crate::sql::FileRow;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ObjInfo {
    pub name: String,
    pub full_path: String,
    pub sha512: String,
    pub created_at: i64,
    pub accessed_at: i64,
    pub updated_at: i64,
    pub mode: u32,
}

pub trait ObjectStorage {
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, anyhow::Error>;
    fn set(&mut self, info: &ObjInfo, content: &[u8]) -> Result<(), anyhow::Error>;
    fn remove(&mut self, info: &ObjInfo) -> Result<(), anyhow::Error>;
}

impl Display for ObjInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl ObjInfo {
    pub fn new(file: &FileRow, full_path: &str) -> ObjInfo {
        ObjInfo {
            name: file.name.to_string(),
            full_path: full_path.to_string(),
            sha512: file.sha512.to_string(),
            created_at: file.created_at,
            accessed_at: file.accessed_at,
            updated_at: file.updated_at,
            mode: file.perms as u32,
        }
    }
}

pub struct DebugObjectStorage {}

impl ObjectStorage for DebugObjectStorage {
    fn get(&mut self, name: &ObjInfo) -> Result<Vec<u8>, anyhow::Error> {
        println!("Get: {}", name);
        Ok(vec![])
    }

    fn set(&mut self, name: &ObjInfo, _content: &[u8]) -> Result<(), anyhow::Error> {
        println!("Create: {}", name);
        Ok(())
    }

    fn remove(&mut self, name: &ObjInfo) -> Result<(), anyhow::Error> {
        println!("Remove: {}", name);
        Ok(())
    }
}

pub struct FsObjectStorage {
    pub base_path: PathBuf,
}

impl ObjectStorage for FsObjectStorage {
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, anyhow::Error> {
        let mut path = self.base_path.clone();
        path.push(format!("{}.dat", &info.sha512[..32]));
        println!("Get: {:?}", &path);
        fs::read(&path).context("FS failed to read file")
    }

    fn set(&mut self, info: &ObjInfo, content: &[u8]) -> Result<(), anyhow::Error> {
        let mut path = self.base_path.clone();
        path.push(format!("{}.dat", &info.sha512[..32]));
        println!("Create: {:?}", &path);
        fs::write(&path, content).context("FS failed to write file")
    }

    fn remove(&mut self, info: &ObjInfo) -> Result<(), anyhow::Error> {
        let mut path = self.base_path.clone();
        path.push(format!("{}.dat", &info.sha512[..32]));
        println!("Remove: {:?}", &path);
        fs::remove_file(&path).context("FS failed to remove file")
    }
}