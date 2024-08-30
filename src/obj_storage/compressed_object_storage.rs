use std::io::{Read, Write};
use anyhow::Error;
use flate2::Compression;
use crate::obj_storage::{ObjInfo, ObjectStorage, UniquenessTest};

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
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, Error> {
        let bytes = self.proxy.get(info)?;
        let mut buff = vec![];
        {
            let mut gz = flate2::read::GzDecoder::new(&bytes[..]);
            gz.read_to_end(&mut buff)?;
        }

        Ok(buff)
    }

    fn put(&mut self, info: &mut ObjInfo, content: &[u8]) -> Result<(), Error> {
        let mut buff = vec![];
        {
            let mut gz = flate2::write::GzEncoder::new(&mut buff, Compression::new(self.level));
            gz.write_all(content)?;
            gz.finish()?;
        }

        self.proxy.put(info, buff.as_slice())?;
        Ok(())
    }

    fn remove(&mut self, info: &ObjInfo) -> Result<(), Error> {
        self.proxy.remove(info)?;
        Ok(())
    }

    fn nuke(&mut self) -> Result<(), Error> {
        self.proxy.nuke()?;
        Ok(())
    }

    fn get_uniqueness_test(&self) -> UniquenessTest {
        self.proxy.get_uniqueness_test()
    }
}