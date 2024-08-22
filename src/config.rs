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
    update_access_time: Option<bool>,
}

#[derive(Debug, Clone)]
pub enum StorageOption {
    FileSystem,
    Sqlar,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub database_file: String,
    pub mount_point: String,
    pub blob_storage: String,
    pub storage_option: StorageOption,
    pub update_access_time: bool,
}

pub fn read_config() -> Result<Rc<Config>, anyhow::Error> {
    let yaml_config = fs::read_to_string("config.yml").expect("Unable to read file './config.yml'");
    let config: YamlConfig = serde_yml::from_str(&yaml_config).expect("Unable to parse yaml");

    let storage_option_str = config.storage_option.unwrap_or("FileSystem".to_string());
    let storage_option = match storage_option_str.as_str().to_ascii_lowercase().as_str() {
        "filesystem" => StorageOption::FileSystem,
        "sqlar" => StorageOption::Sqlar,
        _ => return Err(anyhow!("Invalid storage option")),
    };

    Ok(Rc::new(Config {
        database_file: config.database_file.unwrap_or("./index.db".to_string()),
        mount_point: config.mount_point.unwrap_or("./data".to_string()),
        storage_option,
        blob_storage: config.blob_storage.unwrap_or("./blob".to_string()),
        update_access_time: config.update_access_time.unwrap_or(true),
    }))
}