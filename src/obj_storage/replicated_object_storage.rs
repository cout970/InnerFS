use rayon::prelude::*;
use crate::obj_storage::{BlobProcessor, BlobStorage, RemoteBlob};
use crate::AnyError;

pub struct ReplicatedStorage {
    pub primary: Box<dyn BlobStorage>,
    pub primary_processors: Vec<Box<dyn BlobProcessor>>,
    pub replicas: Vec<Box<dyn BlobStorage>>,
    pub replica_processors: Vec<Vec<Box<dyn BlobProcessor>>>,
}

impl BlobStorage for ReplicatedStorage {
    fn get_multiple(&mut self, paths: &[&str]) -> Result<Vec<Vec<u8>>, AnyError> {
        let mut remote_blobs = self.primary.get_multiple(paths)?;

        for processor in &self.primary_processors {
            remote_blobs = remote_blobs
                .into_iter()
                .map(|b| processor.on_load(b))
                .collect::<Result<Vec<_>, AnyError>>()?;
        }

        Ok(remote_blobs)
    }

    fn put_multiple(&mut self, blobs: &[RemoteBlob]) -> Result<(), AnyError> {
        let mut remote_blobs = blobs.to_vec();

        for processor in &self.primary_processors {
            remote_blobs = remote_blobs
                .into_par_iter()
                .map(|b| {
                    Ok(RemoteBlob {
                        path: b.path,
                        contents: processor.on_store(b.contents)?,
                    })
                })
                .collect::<Result<Vec<_>, AnyError>>()?;
        }

        self.primary.put_multiple(&remote_blobs)?;

        for (index, replica) in  self.replicas.iter_mut().enumerate() {
            let mut remote_blobs = blobs.to_vec();

            for processor in &self.replica_processors[index] {
                remote_blobs = remote_blobs
                    .into_par_iter()
                    .map(|b| {
                        Ok(RemoteBlob {
                            path: b.path,
                            contents: processor.on_store(b.contents)?,
                        })
                    })
                    .collect::<Result<Vec<_>, AnyError>>()?;
            }

            replica.put_multiple(&remote_blobs)?;
        }
        Ok(())
    }

    fn remove_multiple(&mut self, paths: &[&str]) -> Result<(), AnyError> {
        self.primary.remove_multiple(paths)?;
        for replica in &mut self.replicas {
            replica.remove_multiple(paths)?;
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
