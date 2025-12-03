use crate::obj_storage::{BlobStorage, RemoteBlob};
use crate::AnyError;
use log::info;

pub struct DebugBackend {}

impl BlobStorage for DebugBackend {
    fn get_multiple(&mut self, paths: &[&str]) -> Result<Vec<Vec<u8>>, AnyError> {
        info!("Get multiple: {:?}", paths);
        Ok(vec![vec![]; paths.len()])
    }

    fn put_multiple(&mut self, blobs: &[RemoteBlob]) -> Result<(), AnyError> {
        info!("Put multiple: {:?}", blobs);
        Ok(())
    }

    fn remove_multiple(&mut self, paths: &[&str]) -> Result<(), AnyError> {
        info!("Remove multiple: {:?}", paths);
        Ok(())
    }

    fn nuke(&mut self) -> Result<(), AnyError> {
        info!("Nuke");
        Ok(())
    }
}
