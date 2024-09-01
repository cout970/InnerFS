use crate::config::StorageConfig;
use crate::obj_storage::{ObjInfo, ObjectStorage, UniquenessTest};
use crate::storage::ObjInUseFn;
use crate::AnyError;
use anyhow::{anyhow, Error};
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use aws_types::region::Region;
use log::{debug};
use std::rc::Rc;
use tokio::runtime::{Builder, Runtime};

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
        let basename = self.config.s3_base_path.trim_end_matches('/');
        let filename = path.trim_start_matches('/');
        format!("{}/{}", basename, filename).trim_matches('/').to_string()
    }
}

impl ObjectStorage for S3ObjectStorage {
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, Error> {
        let path = self.path(info);
        let bucket_name = &self.config.s3_bucket;
        debug!("Get: {:?} ({:?})", &path, bucket_name);

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
        debug!("Put: {:?} ({:?})", &path, bucket_name);

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

    fn remove(&mut self, info: &ObjInfo, is_in_use: ObjInUseFn) -> Result<(), Error> {
        let test = if self.config.use_hash_as_filename {
            UniquenessTest::Sha512
        } else {
            UniquenessTest::Path
        };

        // If is object in use by other file (deduplication), do not remove it
        if is_in_use(info, test)? {
            return Ok(());
        }

        let path = self.path(info);
        let bucket_name = &self.config.s3_bucket;
        debug!("Remove: {:?} ({:?})", &path, bucket_name);

        self.rt.block_on(async {
            self.client
                .delete_object()
                .bucket(bucket_name)
                .key(&path)
                .send().await?;

            Ok(())
        })
    }

    fn rename(&mut self, prev_info: &ObjInfo, new_info: &ObjInfo) -> Result<(), AnyError> {
        let prev_path = self.path(prev_info);
        let new_path = self.path(new_info);
        let bucket_name = &self.config.s3_bucket;
        debug!("Rename: {:?} -> {:?} ({:?})", &prev_path, &new_path, bucket_name);

        self.rt.block_on(async {
            self.client
                .copy_object()
                .bucket(bucket_name)
                .copy_source(format!("{}/{}", bucket_name, prev_path))
                .key(&new_path)
                .send().await?;

            self.client
                .delete_object()
                .bucket(bucket_name)
                .key(&prev_path)
                .send().await?;

            Ok(())
        })
    }

    fn nuke(&mut self) -> Result<(), Error> {
        let path = self.config.s3_base_path.trim_matches('/').to_string();
        let bucket_name = &self.config.s3_bucket;
        debug!("Nuke: {:?} ({:?})", &path, bucket_name);

        // https://github.com/awslabs/aws-sdk-rust/blob/22f71f0e82804f709469f21bdd389f5d56cf8ed1/examples/examples/s3/src/s3-service-lib.rs#L31
        pub async fn delete_objects(client: &Client, bucket_name: &str, base_path: &str) -> Result<(), Error> {
            loop {
                let objects = client.list_objects_v2()
                    .bucket(bucket_name)
                    .prefix(base_path)
                    .max_keys(1000)
                    .send()
                    .await?;

                let key_count = objects.key_count().ok_or_else(|| anyhow!("Failed to get object count"))?;

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
        }

        self.rt.block_on(async {
            delete_objects(&self.client, &bucket_name, &path).await?;
            Ok(())
        })
    }
}