use std::rc::Rc;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use aws_types::region::Region;
use tokio::runtime::{Builder, Runtime};

use crate::config::Config;
use crate::obj_storage::{ObjectStorage, ObjInfo};

pub struct S3ObjectStorage {
    pub config: Rc<Config>,
    pub client: Client,
    pub rt: Runtime,
}

impl S3ObjectStorage {
    pub fn new(config: Rc<Config>) -> Self {
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
        format!("{}/{}", self.config.s3_base_path.trim_matches('/'), &info.full_path.trim_start_matches('/'))
    }
}

impl ObjectStorage for S3ObjectStorage {
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, anyhow::Error> {
        let path = self.path(info);
        let bucket_name = &self.config.s3_bucket;
        println!("Get: {:?}", &path);

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

    fn put(&mut self, info: &ObjInfo, content: &[u8]) -> Result<(), anyhow::Error> {
        let path = self.path(info);
        let bucket_name = &self.config.s3_bucket;
        println!("Create: {:?}", &path);

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

    fn remove(&mut self, info: &ObjInfo) -> Result<(), anyhow::Error> {
        let path = self.path(info);
        let bucket_name = &self.config.s3_bucket;
        println!("Remove: {:?}", &path);

        self.rt.block_on(async {
            self.client
                .delete_object()
                .bucket(bucket_name)
                .key(&path)
                .send().await?;

            Ok(())
        })
    }
}