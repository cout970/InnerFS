use log::info;
use crate::obj_storage::{ObjInfo, ObjectStorage, UniquenessTest};

pub struct DebugObjectStorage {}

impl ObjectStorage for DebugObjectStorage {
    fn get(&mut self, name: &ObjInfo) -> Result<Vec<u8>, anyhow::Error> {
        info!("Get: {}", name);
        Ok(vec![])
    }

    fn put(&mut self, name: &mut ObjInfo, _content: &[u8]) -> Result<(), anyhow::Error> {
        info!("Create: {}", name);
        Ok(())
    }

    fn remove(&mut self, name: &ObjInfo) -> Result<(), anyhow::Error> {
        info!("Remove: {}", name);
        Ok(())
    }

    fn nuke(&mut self) -> Result<(), anyhow::Error> {
        info!("Nuke");
        Ok(())
    }

    fn get_uniqueness_test(&self) -> UniquenessTest {
        UniquenessTest::AlwaysUnique
    }
}