use crate::AnyError;
use crate::obj_storage::{ObjInfo, ObjectStorage};
use crate::storage::ObjInUseFn;

pub struct ReplicatedObjectStorage {
    pub primary: Box<dyn ObjectStorage>,
    pub replicas: Vec<Box<dyn ObjectStorage>>,
}

impl ObjectStorage for ReplicatedObjectStorage {
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, AnyError> {
        self.primary.get(info)
    }

    fn put(&mut self, info: &mut ObjInfo, content: &[u8]) -> Result<(), AnyError> {
        self.primary.put(info, content)?;
        for replica in &mut self.replicas {
            replica.put(info, content)?;
        }
        Ok(())
    }

    fn remove(&mut self, info: &ObjInfo, is_in_use: ObjInUseFn) -> Result<(), AnyError> {
        self.primary.remove(info, is_in_use.clone())?;
        for replica in &mut self.replicas {
            replica.remove(info, is_in_use.clone())?;
        }
        Ok(())
    }

    fn rename(&mut self, prev_info: &ObjInfo, new_info: &ObjInfo) -> Result<(), AnyError> {
        self.primary.rename(prev_info, new_info)?;
        for replica in &mut self.replicas {
            replica.rename(prev_info, new_info)?;
        }
        Ok(())
    }

    fn nuke(&mut self) -> Result<(), AnyError> {
        self.primary.nuke()?;
        for replica in &mut self.replicas {
            replica.nuke()?;
        }
        Ok(())
    }
}