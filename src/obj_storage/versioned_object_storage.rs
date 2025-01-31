use crate::obj_storage::{ObjInfo, ObjectStorage};
use crate::storage::ObjInUseFn;
use crate::AnyError;

pub struct VersionedObjectStorage {
    pub storage: Box<dyn ObjectStorage>,
}

impl VersionedObjectStorage {
    pub fn new(storage: Box<dyn ObjectStorage>) -> Self {
        Self { storage }
    }

    pub fn get_path(&mut self, info: &ObjInfo) -> String {
        format!("{}.{}", info.full_path, &info.sha512[0..16])
    }
}

impl ObjectStorage for VersionedObjectStorage {
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, AnyError> {
        let mut clone = info.clone();
        let path = self.get_path(info);
        clone.full_path = path;
        self.storage.get(&clone)
    }

    fn put(&mut self, info: &mut ObjInfo, content: &[u8]) -> Result<(), AnyError> {
        let prev_path = info.full_path.clone();
        let path = self.get_path(info);
        info.full_path = path;
        self.storage.put(info, content)?;
        info.full_path = prev_path;
        Ok(())
    }

    fn remove(&mut self, _info: &ObjInfo, _is_in_use: ObjInUseFn) -> Result<(), AnyError> {
        // Versioned object storage does not remove the blob, the version persists
        // This allow 2 independent versions of the same object to coexist
        // and 2 instances of the metadata to point to different versions
        Ok(())
    }

    fn rename(&mut self, prev_info: &ObjInfo, new_info: &ObjInfo) -> Result<(), AnyError> {
        let content = self.get(prev_info)?;
        let mut clone = new_info.clone();
        self.put(&mut clone, &content)?;
        Ok(())
    }

    fn nuke(&mut self) -> Result<(), AnyError> {
        self.storage.nuke()
    }

    fn clone(&self) -> Box<dyn ObjectStorage> {
        Box::new(Self {
            storage: self.storage.clone(),
        })
    }
}
