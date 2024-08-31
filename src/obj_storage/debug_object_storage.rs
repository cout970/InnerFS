use log::info;
use crate::AnyError;
use crate::obj_storage::{ObjInfo, ObjectStorage};
use crate::storage::ObjInUseFn;

pub struct DebugObjectStorage {}

impl ObjectStorage for DebugObjectStorage {
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, AnyError> {
        info!("Get: {}", info);
        Ok(vec![])
    }

    fn put(&mut self, info: &mut ObjInfo, _content: &[u8]) -> Result<(), AnyError> {
        info!("Create: {}", info);
        Ok(())
    }

    fn remove(&mut self, info: &ObjInfo, _is_in_use: ObjInUseFn) -> Result<(), AnyError> {
        info!("Remove: {}", info);
        Ok(())
    }

    fn rename(&mut self, prev_info: &ObjInfo, new_info: &ObjInfo) -> Result<(), AnyError> {
        info!("Rename: {} to {}", prev_info, new_info);
        Ok(())
    }

    fn nuke(&mut self) -> Result<(), AnyError> {
        info!("Nuke");
        Ok(())
    }
}