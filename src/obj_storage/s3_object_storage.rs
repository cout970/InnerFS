use std::rc::Rc;
use anyhow::{anyhow, Error};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_types::region::Region;
use itertools::Itertools;
use log::{info};
use tokio::runtime::{Builder, Runtime};

use crate::config::{StorageConfig};
use crate::obj_storage::{ObjectStorage, ObjInfo, UniquenessTest};

pub struct S3ObjectStorage {
    pub config: Rc<StorageConfig>,
    pub client: Client,
    pub rt: Runtime,
}

impl S3ObjectStorage {
    pub fn new(config: Rc<StorageConfig>) -> Self {
        let rt = Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        let creds = Credentials::new(&config.s3_access_key, &config.s3_secret_key, None, None, "config.yml");

        let s3_config = aws_types::sdk_config::Builder::default()
            .region(Region::new(config.s3_region.to_string()))
            .endpoint_url(config.s3_endpoint_url.to_string())
            .credentials_provider(SharedCredentialsProvider::new(creds))
            .build();

        let client = Client::new(&s3_config);

        S3ObjectStorage { config, client, rt }
    }

    pub fn path(&self, info: &ObjInfo) -> String {
        let path = self.config.path_of(&info);
        format!("{}/{}", self.config.s3_base_path.trim_matches('/'), &path)
    }
}

impl ObjectStorage for S3ObjectStorage {
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, Error> {
        let path = self.path(info);
        let bucket_name = &self.config.s3_bucket;
        info!("Get: {:?}", &path);

        self.rt.block_on(async {
            let res = self.client
                .get_object()
                .bucket(bucket_name)
                .key(&path)
                .send().await?;

            let content = res.body.collect().await?.to_vec();
            Ok(content)
        })
    }

    fn put(&mut self, info: &mut ObjInfo, content: &[u8]) -> Result<(), Error> {
        let path = self.path(info);
        let bucket_name = &self.config.s3_bucket;
        info!("Create: {:?}", &path);

        self.rt.block_on(async {
            self.client
                .put_object()
                .bucket(bucket_name)
                .key(&path)
                .body(ByteStream::from(content.to_vec()))
                .send().await?;

            Ok(())
        })
    }

    fn remove(&mut self, info: &ObjInfo) -> Result<(), Error> {
        let path = self.path(info);
        let bucket_name = &self.config.s3_bucket;
        info!("Remove: {:?}", &path);

        self.rt.block_on(async {
            self.client
                .delete_object()
                .bucket(bucket_name)
                .key(&path)
                .send().await?;

            Ok(())
        })
    }

    fn get_uniqueness_test(&self) -> UniquenessTest {
        if self.config.use_hash_as_filename {
            UniquenessTest::Sha512
        } else {
            UniquenessTest::Path
        }
    }

    fn nuke(&mut self) -> Result<(), Error> {
        let path = self.config.s3_base_path.trim_end_matches('/').to_string() + "/";
        let bucket_name = &self.config.s3_bucket;
        info!("Remove: {:?}", &path);

        // https://github.com/awslabs/aws-sdk-rust/blob/22f71f0e82804f709469f21bdd389f5d56cf8ed1/examples/examples/s3/src/s3-service-lib.rs#L31
        pub async fn delete_objects(client: &Client, bucket_name: &str, base_path: &str) -> Result<(), Error> {
            let objects = client.list_objects_v2()
                .bucket(bucket_name)
                .prefix(base_path)
                .send()
                .await?;

            let mut delete_objects: Vec<ObjectIdentifier> = vec![];

            for obj in objects.contents() {
                let key = obj.key().unwrap().to_string();

                let obj_id = ObjectIdentifier::builder()
                    .set_key(Some(key))
                    .build()
                    .map_err(Error::from)?;

                delete_objects.push(obj_id);
            }

            if !delete_objects.is_empty() {
                client.delete_objects()
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

            let objects = client.list_objects_v2()
                .bucket(bucket_name)
                .prefix(base_path)
                .send()
                .await?;

            if let Some(key_count) = objects.key_count() {
                if key_count > 0i32 {
                    let keys = objects.contents().iter().map(|obj| obj.key.as_ref().unwrap().to_string()).collect_vec();

                    return Err(anyhow!("Failed to delete all objects, remaining: {:?}", keys));
                }
            }

            Ok(())
        }

        self.rt.block_on(async {
            delete_objects(&self.client, &bucket_name, &path).await?;
            Ok(())
        })
    }
}