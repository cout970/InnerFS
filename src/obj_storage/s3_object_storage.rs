use crate::config::StorageConfig;
use crate::obj_storage::{BlobStorage, RemoteBlob};
use crate::AnyError;
use anyhow::{anyhow, Error};
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use aws_smithy_types::retry::RetryConfig;
use aws_types::region::Region;
use log::debug;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle;

pub struct S3Backend {
    pub config: Arc<StorageConfig>,
    pub client: Client,
    pub rt: Runtime,
}

impl S3Backend {
    pub fn new(config: Arc<StorageConfig>) -> Self {
        let rt = Builder::new_current_thread().enable_time().enable_io().build().unwrap();

        let creds = Credentials::new(&config.s3_access_key, &config.s3_secret_key, None, None, "config.yml");

        let s3_config = aws_types::sdk_config::Builder::default()
            .retry_config(RetryConfig::standard().with_max_attempts(5))
            .region(Region::new(config.s3_region.to_string()))
            .endpoint_url(config.s3_endpoint_url.to_string())
            .credentials_provider(SharedCredentialsProvider::new(creds))
            .build();

        let client = Client::new(&s3_config);

        S3Backend { config, client, rt }
    }

    pub fn final_path(&self, path: &str) -> String {
        let basename = self.config.s3_base_path.trim_end_matches('/');
        let filename = path.trim_start_matches('/');
        format!("{}/{}", basename, filename).trim_matches('/').to_string()
    }
}

impl BlobStorage for S3Backend {
    fn get_multiple(&mut self, paths: &[&str]) -> Result<Vec<Vec<u8>>, AnyError> {
        let bucket_name = &self.config.s3_bucket;
        let paths = paths.iter().map(|p| self.final_path(p)).collect::<Vec<_>>();
        let client = &mut self.client;
        let rt = &mut self.rt;

        rt.block_on(async {
            let mut handles: Vec<JoinHandle<Result<Vec<u8>, AnyError>>> = vec![];

            // Get all objects in parallel
            for path in paths {
                let bucket_name = bucket_name.clone();
                let client = client.clone();

                debug!("Get: {:?} ({:?})", &path, bucket_name);
                let h = rt.spawn(async move {
                    let res = client.get_object().bucket(bucket_name).key(&path).send().await?;
                    let content = res.body.collect().await?.to_vec();
                    Ok(content)
                });
                handles.push(h);
            }

            // Wait for all tasks to finish
            let mut results = vec![];
            for handle in handles {
                match handle.await {
                    Ok(result) => results.push(result?),
                    Err(e) => return Err(anyhow!("Failed to get object: {:?}", e).into()),
                }
            }

            Ok(results)
        })
    }

    fn put_multiple(&mut self, blobs: &[RemoteBlob]) -> Result<(), AnyError> {
        let bucket_name = &self.config.s3_bucket;
        let mut final_paths = vec![];

        for blob in blobs {
            final_paths.push(self.final_path(&blob.path));
        }

        let client = &mut self.client;
        let rt = &mut self.rt;

        rt.block_on(async {
            let mut handles: Vec<JoinHandle<Result<(), AnyError>>> = vec![];

            // Get all objects in parallel
            for i in 0..blobs.len() {
                let bucket_name = bucket_name.clone();
                let client = client.clone();
                let path = final_paths[i].clone();
                let content = blobs[i].contents.to_vec();

                debug!("Put: {:?} ({:?})", &path, bucket_name);
                let h = rt.spawn(async move {
                    client
                        .put_object()
                        .bucket(bucket_name)
                        .key(&path)
                        .body(ByteStream::from(content))
                        .send()
                        .await?;

                    Ok(())
                });
                handles.push(h);
            }

            // Wait for all tasks to finish
            for handle in handles {
                if let Err(e) = handle.await {
                    return Err(anyhow!("Failed to pet object: {:?}", e).into());
                }
            }

            Ok(())
        })
    }

    fn remove_multiple(&mut self, paths: &[&str]) -> Result<(), AnyError> {
        let bucket_name = &self.config.s3_bucket;
        let paths = paths.iter().map(|p| self.final_path(p)).collect::<Vec<_>>();
        let client = &mut self.client;
        let rt = &mut self.rt;

        rt.block_on(async {
            let mut handles: Vec<JoinHandle<Result<(), AnyError>>> = vec![];

            // Get all objects in parallel
            for path in paths {
                let bucket_name = bucket_name.clone();
                let client = client.clone();

                debug!("Remove: {:?} ({:?})", &path, bucket_name);
                let h = rt.spawn(async move {
                    client.delete_object().bucket(bucket_name).key(&path).send().await?;

                    Ok(())
                });
                handles.push(h);
            }

            // Wait for all tasks to finish
            for handle in handles {
                if let Err(e) = handle.await {
                    return Err(anyhow!("Failed to pet object: {:?}", e).into());
                }
            }

            Ok(())
        })
    }

    fn nuke(&mut self) -> Result<(), AnyError> {
        let path = self.config.s3_base_path.trim_matches('/').to_string();
        let bucket_name = &self.config.s3_bucket;
        debug!("Nuke: {:?} ({:?})", &path, bucket_name);

        // https://github.com/awslabs/aws-sdk-rust/blob/22f71f0e82804f709469f21bdd389f5d56cf8ed1/examples/examples/s3/src/s3-service-lib.rs#L31
        pub async fn delete_objects(client: &Client, bucket_name: &str, base_path: &str) -> Result<(), Error> {
            loop {
                let objects = client
                    .list_objects_v2()
                    .bucket(bucket_name)
                    .prefix(base_path)
                    .max_keys(1000)
                    .send()
                    .await?;

                let key_count = objects
                    .key_count()
                    .ok_or_else(|| anyhow!("Failed to get object count"))?;

                if key_count == 0 {
                    return Ok(());
                }

                let mut delete_objects: Vec<ObjectIdentifier> = vec![];

                for obj in objects.contents() {
                    let key = obj.key().unwrap().to_string();

                    let obj_id = ObjectIdentifier::builder()
                        .set_key(Some(key))
                        .build()
                        .map_err(Error::from)?;

                    delete_objects.push(obj_id);
                }

                client
                    .delete_objects()
                    .bucket(bucket_name)
                    .delete(
                        Delete::builder()
                            .set_objects(Some(delete_objects))
                            .build()
                            .map_err(Error::from)?,
                    )
                    .send()
                    .await?;
            }
        }

        self.rt.block_on(async {
            delete_objects(&self.client, &bucket_name, &path).await?;
            Ok(())
        })
    }
}
