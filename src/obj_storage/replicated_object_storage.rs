use anyhow::Error;
use crate::obj_storage::{ObjInfo, ObjectStorage, UniquenessTest};

pub struct ReplicatedObjectStorage {
    pub primary: Box<dyn ObjectStorage>,
    pub replicas: Vec<Box<dyn ObjectStorage>>,
}

impl ObjectStorage for ReplicatedObjectStorage {
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, Error> {
        self.primary.get(info)
    }

    fn put(&mut self, info: &mut ObjInfo, content: &[u8]) -> Result<(), Error> {
        self.primary.put(info, content)?;
        for replica in &mut self.replicas {
            replica.put(info, content)?;
        }
        Ok(())
    }

    fn remove(&mut self, info: &ObjInfo) -> Result<(), Error> {
        self.primary.remove(info)?;
        for replica in &mut self.replicas {
            replica.remove(info)?;
        }
        Ok(())
    }

    fn nuke(&mut self) -> Result<(), Error> {
        self.primary.nuke()?;
        for replica in &mut self.replicas {
            replica.nuke()?;
        }
        Ok(())
    }

    fn get_uniqueness_test(&self) -> UniquenessTest {
        let mut test = self.primary.get_uniqueness_test();

        for replica in &self.replicas {
            if replica.get_uniqueness_test() != test {
                test = UniquenessTest::AlwaysUnique
            }
        }

        test
    }
}