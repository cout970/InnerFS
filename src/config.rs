use std::fs;
use std::rc::Rc;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct YamlConfig {
    database_file: Option<String>,
    mount_point: Option<String>,
    blob_storage: Option<String>,
    storage_option: Option<String>,
    s3_endpoint_url: Option<String>,
    s3_region: Option<String>,
    s3_bucket: Option<String>,
    s3_base_path: Option<String>,
    s3_access_key: Option<String>,
    s3_secret_key: Option<String>,
    encryption_key: Option<String>,
    update_access_time: Option<bool>,
    use_hash_as_filename: Option<bool>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StorageOption {
    FileSystem,
    Sqlar,
    S3,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub database_file: String,
    pub mount_point: String,
    pub blob_storage: String,
    pub storage_option: StorageOption,
    pub s3_endpoint_url: String,
    pub s3_region: String,
    pub s3_bucket: String,
    pub s3_base_path: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub encryption_key: String,
    pub update_access_time: bool,
    pub use_hash_as_filename: bool,
}

pub fn read_config() -> Result<Rc<Config>, anyhow::Error> {
    let yaml_config = fs::read_to_string("config.yml").expect("Unable to read file './config.yml'");
    let config: YamlConfig = serde_yml::from_str(&yaml_config).expect("Unable to parse yaml");

    let storage_option_str = config.storage_option.unwrap_or("FileSystem".to_string());
    let storage_option = match storage_option_str.as_str().to_ascii_lowercase().as_str() {
        "filesystem" => StorageOption::FileSystem,
        "sqlar" => StorageOption::Sqlar,
        "s3" => StorageOption::S3,
        _ => return Err(anyhow!("Invalid storage option")),
    };

    let cfg = Config {
        database_file: config.database_file.unwrap_or("./index.db".to_string()),
        mount_point: config.mount_point.unwrap_or("./data".to_string()),
        storage_option,
        blob_storage: config.blob_storage.unwrap_or("./blob".to_string()),
        s3_endpoint_url: config.s3_endpoint_url.unwrap_or("".to_string()),
        s3_region: config.s3_region.unwrap_or("".to_string()),
        s3_bucket: config.s3_bucket.unwrap_or("".to_string()),
        s3_base_path: config.s3_base_path.unwrap_or("".to_string()),
        s3_access_key: config.s3_access_key.unwrap_or("".to_string()),
        s3_secret_key: config.s3_secret_key.unwrap_or("".to_string()),
        encryption_key: config.encryption_key.unwrap_or("".to_string()),
        update_access_time: config.update_access_time.unwrap_or(true),
        use_hash_as_filename: config.use_hash_as_filename.unwrap_or(true),
    };

    let mut errors = vec![];

    if cfg.storage_option == StorageOption::S3 {
        if cfg.s3_access_key.is_empty() {
            errors.push("S3 access key is required".to_string());
        }
        if cfg.s3_secret_key.is_empty() {
            errors.push("S3 secret key is required".to_string());
        }
        if cfg.s3_bucket.is_empty() {
            errors.push("S3 bucket is required".to_string());
        }
    }

    if cfg.storage_option == StorageOption::FileSystem {
        if cfg.blob_storage.is_empty() {
            errors.push("Blob storage path is required for FileSystem storage option".to_string());
        }
    }

    if !errors.is_empty() {
        return Err(anyhow!("Config errors detected:\n - {}", errors.join("\n - ")));
    }

    Ok(Rc::new(cfg))
}