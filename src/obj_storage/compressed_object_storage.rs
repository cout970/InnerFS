use std::io::{Read, Write};
use flate2::Compression;
use crate::AnyError;
use crate::obj_storage::{ObjInfo, ObjectStorage};
use crate::storage::ObjInUseFn;

pub struct CompressedObjectStorage {
    pub proxy: Box<dyn ObjectStorage>,
    pub level: u32,
}

impl CompressedObjectStorage {
    pub fn new(proxy: Box<dyn ObjectStorage>, level: u32) -> CompressedObjectStorage {
        CompressedObjectStorage { proxy, level }
    }
}

impl ObjectStorage for CompressedObjectStorage {
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, AnyError> {
        let bytes = self.proxy.get(info)?;

        // No compression was used for this object
        if info.compression.is_empty() {
            return Ok(bytes);
        }

        let mut buff = vec![];
        {
            let mut gz = flate2::read::GzDecoder::new(&bytes[..]);
            gz.read_to_end(&mut buff)?;
        }

        Ok(buff)
    }

    fn put(&mut self, info: &mut ObjInfo, content: &[u8]) -> Result<(), AnyError> {
        let mut buff = vec![];
        {
            let mut gz = flate2::write::GzEncoder::new(&mut buff, Compression::new(self.level));
            gz.write_all(content)?;
            gz.finish()?;
        }

        info.compression = format!("gzip:{}", self.level);
        self.proxy.put(info, buff.as_slice())?;
        Ok(())
    }

    fn remove(&mut self, info: &ObjInfo, is_in_use: ObjInUseFn) -> Result<(), AnyError> {
        self.proxy.remove(info, is_in_use)?;
        Ok(())
    }

    fn rename(&mut self, prev_info: &ObjInfo, new_info: &ObjInfo) -> Result<(), AnyError> {
        self.proxy.rename(prev_info, new_info)?;
        Ok(())
    }

    fn nuke(&mut self) -> Result<(), AnyError> {
        self.proxy.nuke()?;
        Ok(())
    }
}