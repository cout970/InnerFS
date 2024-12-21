use crate::metadata_db::MetadataDB;
use crate::obj_storage::{ObjInfo, PathGenerator};
use crate::utils::ask_for_confirmation;
use crate::AnyError;
use anyhow::{anyhow, Error};
use log::error;
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::{env, fs};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct YamlConfig {
    database_file: Option<String>,
    mount_point: Option<String>,
    update_access_time: Option<bool>,
    store_file_change_history: Option<bool>,
    primary: Option<YamlStorageConfig>,
    replicas: Option<Vec<YamlStorageConfig>>,
    readonly: Option<bool>,
    // Default value for each backend
    blob_storage: Option<String>,
    storage_backend: Option<String>,
    s3_endpoint_url: Option<String>,
    s3_region: Option<String>,
    s3_bucket: Option<String>,
    s3_base_path: Option<String>,
    s3_access_key: Option<String>,
    s3_secret_key: Option<String>,
    encryption_key: Option<YamlEncryptionKeyConfig>,
    compression_level: Option<u32>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct YamlStorageConfig {
    name: Option<String>,
    storage_backend: Option<String>,
    blob_storage: Option<String>,
    s3_endpoint_url: Option<String>,
    s3_region: Option<String>,
    s3_bucket: Option<String>,
    s3_base_path: Option<String>,
    s3_access_key: Option<String>,
    s3_secret_key: Option<String>,
    encryption_key: Option<YamlEncryptionKeyConfig>,
    compression_level: Option<u32>,
    use_hash_as_filename: Option<bool>,
    use_id_as_filename: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum YamlEncryptionKeyConfig {
    Plain(String),
    External { path: String },
    Ask { ask: bool },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StorageOption {
    FileSystem,
    Sqlar,
    S3,
    RocksDb,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub database_file: String,
    pub mount_point: String,
    pub primary: Rc<StorageConfig>,
    pub replicas: Vec<Rc<StorageConfig>>,
    pub update_access_time: bool,
    pub store_file_change_history: bool,
    pub readonly: bool,
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub name: String,
    pub storage_backend: StorageOption,
    pub blob_storage: String,
    pub s3_endpoint_url: String,
    pub s3_region: String,
    pub s3_bucket: String,
    pub s3_base_path: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub encryption_key: String,
    pub compression_level: u32,
    pub path_generator: PathGenerator,
}

/// Read and parse the main config file
pub fn read_config(config_path: &PathBuf) -> Result<Rc<Config>, Error> {
    if fs::metadata(&config_path).is_err() {
        let program_name = env::args()
            .next()
            .as_ref()
            .map(Path::new)
            .and_then(Path::file_name)
            .and_then(OsStr::to_str)
            .map(String::from)
            .unwrap();

        return Err(anyhow!(
            "Config file not found at {:?}, try './{} generate-config'",
            &config_path,
            program_name
        ));
    }

    let yaml_config = fs::read_to_string(config_path)
        .map_err(|e| anyhow!("Unable to read config file {:?}: {}", config_path, e))?;

    let config: YamlConfig = serde_yml::from_str(&yaml_config)
        .map_err(|e| anyhow!("Unable to parse YAML config file {:?}: {}", config_path, e))?;

    // Fields in the global config are the defaults for primary and replicas
    let primary_clone = config.primary.clone();
    let primary = primary_clone.as_ref();
    let mut path_generation = PathGenerator::Path;

    if let Some(p) = primary.clone() {
        if p.use_id_as_filename.unwrap_or(false) {
            path_generation = PathGenerator::Id;
        } else if p.use_hash_as_filename.unwrap_or(false) {
            path_generation = PathGenerator::Sha512;
        }
    }

    let primary = Rc::new(StorageConfig {
        name: primary
            .and_then(|p| p.name.clone())
            .unwrap_or("primary".to_string()),
        storage_backend: StorageOption::from_string(
            &primary
                .and_then(|p| p.storage_backend.clone())
                .or(config.storage_backend.clone()),
        )?,
        blob_storage: primary
            .and_then(|p| p.blob_storage.clone())
            .or(config.blob_storage.clone())
            .unwrap_or("./blob".to_string()),
        s3_endpoint_url: primary
            .and_then(|p| p.s3_endpoint_url.clone())
            .or(config.s3_endpoint_url.clone())
            .unwrap_or("".to_string()),
        s3_region: primary
            .and_then(|p| p.s3_region.clone())
            .or(config.s3_region.clone())
            .unwrap_or("".to_string()),
        s3_bucket: primary
            .and_then(|p| p.s3_bucket.clone())
            .or(config.s3_bucket.clone())
            .unwrap_or("".to_string()),
        s3_base_path: primary
            .and_then(|p| p.s3_base_path.clone())
            .or(config.s3_base_path.clone())
            .unwrap_or("".to_string()),
        s3_access_key: primary
            .and_then(|p| p.s3_access_key.clone())
            .or(config.s3_access_key.clone())
            .unwrap_or("".to_string()),
        s3_secret_key: primary
            .and_then(|p| p.s3_secret_key.clone())
            .or(config.s3_secret_key.clone())
            .unwrap_or("".to_string()),
        encryption_key: primary
            .and_then(|p| p.encryption_key.clone())
            .or(config.encryption_key.clone())
            .map(|value| extract_encryption_key(value))
            .unwrap_or("".to_string()),
        compression_level: primary
            .and_then(|p| p.compression_level.clone())
            .or(config.compression_level.clone())
            .unwrap_or(0)
            .clamp(0, 9),
        path_generator: path_generation,
    });

    let mut cfg = Config {
        database_file: config.database_file.unwrap_or("./index.db".to_string()),
        mount_point: config.mount_point.unwrap_or("./data".to_string()),
        primary,
        replicas: vec![],
        update_access_time: config.update_access_time.unwrap_or(false),
        store_file_change_history: config.store_file_change_history.unwrap_or(true),
        readonly: config.readonly.unwrap_or(false),
    };

    let replicas = config.replicas.clone().unwrap_or_default();
    let mut index = 0;
    for replica in &replicas {
        let mut path_generation = PathGenerator::Path;

        if replica.use_id_as_filename.unwrap_or(false) {
            path_generation = PathGenerator::Id;
        } else if replica.use_hash_as_filename.unwrap_or(false) {
            path_generation = PathGenerator::Sha512;
        }

        cfg.replicas.push(Rc::new(StorageConfig {
            name: replica.name.clone().unwrap_or(format!("replica{}", index)),
            storage_backend: StorageOption::from_string(
                &replica
                    .storage_backend
                    .clone()
                    .or(config.storage_backend.clone()),
            )?,
            blob_storage: replica
                .blob_storage
                .clone()
                .or(config.blob_storage.clone())
                .unwrap_or("./blob".to_string()),
            s3_endpoint_url: replica
                .s3_endpoint_url
                .clone()
                .or(config.s3_endpoint_url.clone())
                .unwrap_or("".to_string()),
            s3_region: replica
                .s3_region
                .clone()
                .or(config.s3_region.clone())
                .unwrap_or("".to_string()),
            s3_bucket: replica
                .s3_bucket
                .clone()
                .or(config.s3_bucket.clone())
                .unwrap_or("".to_string()),
            s3_base_path: replica
                .s3_base_path
                .clone()
                .or(config.s3_base_path.clone())
                .unwrap_or("".to_string()),
            s3_access_key: replica
                .s3_access_key
                .clone()
                .or(config.s3_access_key.clone())
                .unwrap_or("".to_string()),
            s3_secret_key: replica
                .s3_secret_key
                .clone()
                .or(config.s3_secret_key.clone())
                .unwrap_or("".to_string()),
            encryption_key: replica
                .encryption_key
                .clone()
                .or(config.encryption_key.clone())
                .map(|value| extract_encryption_key(value))
                .unwrap_or("".to_string()),
            compression_level: replica
                .compression_level
                .clone()
                .or(config.compression_level.clone())
                .unwrap_or(0)
                .clamp(0, 9),
            path_generator: path_generation,
        }));
        index += 1;
    }

    validate_storage(&cfg.primary)?;

    for i in &cfg.replicas {
        validate_storage(i)?;
    }

    Ok(Rc::new(cfg))
}

fn extract_encryption_key(value: YamlEncryptionKeyConfig) -> String {
    match value {
        YamlEncryptionKeyConfig::Plain(key) => key,
        YamlEncryptionKeyConfig::External { path } => {
            let contents = fs::read_to_string(&path).expect("Unable to read encryption key file");
            contents.trim().to_string()
        }
        YamlEncryptionKeyConfig::Ask { ask } => {
            if !ask {
                panic!("Invalid encryption key configuration, set 'ask: true' to ask for the key at startup");
            }
            rpassword::prompt_password("Enter encryption key: ").unwrap()
        }
    }
}

pub fn check_config_changes(
    prefix: &str,
    config: Rc<StorageConfig>,
    sql: Rc<MetadataDB>,
) -> Result<(), AnyError> {
    // Changing storage_option will make all the files not available
    let setting_storage_option = format!("{}:storage_option", prefix);
    let storage_option = config.storage_backend.to_string();
    {
        let setting = sql.get_setting(&setting_storage_option)?;
        if let Some(setting) = setting {
            if setting != storage_option {
                error!("Storage option changed from {} to {}, this will cause loss of data, it's recommended to revert the setting or recreate the filesystem", setting, storage_option);
                if !ask_for_confirmation(
                    "Do you want to proceed anyways? Type 'yes' or 'y' to confirm",
                ) {
                    return Err(anyhow!("Operation cancelled"));
                }
            }
        }
    }
    sql.set_setting(&setting_storage_option, &storage_option)?;

    // Changing encryption_key will make every file not readable
    let setting_encryption_key_hash = format!("{}:encryption_key_hash", prefix);
    let encryption_key = hex::encode(&hmac_sha512::Hash::hash(&config.encryption_key)[0..32]);

    if let Some(setting) = sql.get_setting(&setting_encryption_key_hash)? {
        if setting != encryption_key {
            error!("Encryption key changed, this will cause loss of data, it's recommended to revert the setting or recreate the filesystem");
            if !ask_for_confirmation("Do you want to proceed anyways? Type 'yes' or 'y' to confirm")
            {
                return Err(anyhow!("Operation cancelled"));
            }
        }
    }
    sql.set_setting(&setting_encryption_key_hash, &encryption_key)?;

    // Changing path_generator will cause in a mismatch between previous and new filenames
    let setting_path_generator = format!("{}:path_generator", prefix);
    let path_generator = config.path_generator.to_string();
    {
        let setting = sql.get_setting(&setting_path_generator)?;
        if let Some(setting) = setting {
            if setting != path_generator {
                error!("use_hash_as_filename or use_id_as_filename changed, this will cause loss of data, it's recommended to revert the setting or recreate the filesystem");
                if !ask_for_confirmation(
                    "Do you want to proceed anyways? Type 'yes' or 'y' to confirm",
                ) {
                    return Err(anyhow!("Operation cancelled"));
                }
            }
        }
    }
    sql.set_setting(&setting_path_generator, &path_generator)?;

    // Changing s3 settings will make the data inaccesible
    let setting_s3_bucket = format!("{}:s3_bucket", prefix);
    let setting_s3_region = format!("{}:s3_region", prefix);
    let setting_s3_endpoint_url = format!("{}:s3_endpoint_url", prefix);

    if config.storage_backend == StorageOption::S3 {
        let mut changed = false;

        if let Some(bucket) = sql.get_setting(&setting_s3_bucket)? {
            if bucket != config.s3_bucket {
                changed = true;
            }
        }

        if let Some(region) = sql.get_setting(&setting_s3_region)? {
            if region != config.s3_region {
                changed = true;
            }
        }

        if let Some(endpoint_url) = sql.get_setting(&setting_s3_endpoint_url)? {
            if endpoint_url != config.s3_endpoint_url {
                changed = true;
            }
        }

        if changed {
            error!("S3 settings changed, this will make the data inaccesible, it's recommended to revert the setting or recreate the filesystem");
            if !ask_for_confirmation("Do you want to proceed anyways? Type 'yes' or 'y' to confirm")
            {
                return Err(anyhow!("Operation cancelled"));
            }
        }
    }

    sql.set_setting(&setting_s3_bucket, &config.s3_bucket)?;
    sql.set_setting(&setting_s3_region, &config.s3_region)?;
    sql.set_setting(&setting_s3_endpoint_url, &config.s3_endpoint_url)?;

    // Changing blob_storage will make all the files not available
    let blob_storage = format!("{}:blob_storage", prefix);

    if config.storage_backend == StorageOption::FileSystem
        || config.storage_backend == StorageOption::RocksDb
    {
        let setting = sql.get_setting(&blob_storage)?;
        if let Some(setting) = setting {
            if setting != config.blob_storage {
                error!("Blob storage changed from {} to {}, this will make the data inaccesible, it's recommended to revert the setting or recreate the filesystem", setting, config.blob_storage);
                if !ask_for_confirmation(
                    "Do you want to proceed anyways? Type 'yes' or 'y' to confirm",
                ) {
                    return Err(anyhow!("Operation cancelled"));
                }
            }
        }
    }
    sql.set_setting(&blob_storage, &config.blob_storage)?;

    Ok(())
}

fn validate_storage(cfg: &StorageConfig) -> Result<(), Error> {
    let mut errors = vec![];

    if cfg.storage_backend == StorageOption::S3 {
        if cfg.s3_access_key.is_empty() {
            errors.push("S3 access key is required".to_string());
        }
        if cfg.s3_secret_key.is_empty() {
            errors.push("S3 secret key is required".to_string());
        }
        if cfg.s3_bucket.is_empty() {
            errors.push("S3 bucket is required".to_string());
        }
        if cfg.s3_region.is_empty() && cfg.s3_endpoint_url.is_empty() {
            errors.push("S3 region or endpoint_url must be provided".to_string());
        }
    }

    if cfg.storage_backend == StorageOption::FileSystem {
        if cfg.blob_storage.is_empty() {
            errors.push("Blob storage path is required for FileSystem storage option".to_string());
        }
    }

    if !cfg.encryption_key.is_empty() && cfg.path_generator == PathGenerator::Sha512 {
        errors.push("The option use_hash_as_filename is incompatible with encryption, use use_id_as_filename instead".to_string());
    }

    if !errors.is_empty() {
        return Err(anyhow!(
            "Config errors detected:\n - {}",
            errors.join("\n - ")
        ));
    }

    Ok(())
}

impl StorageOption {
    pub fn from_string(storage_backend: &Option<String>) -> Result<StorageOption, Error> {
        let binding = storage_backend
            .as_ref()
            .map(|i| i.as_str())
            .unwrap_or("FileSystem")
            .to_ascii_lowercase();

        let storage_backend_str = binding.as_str();

        match storage_backend_str {
            "filesystem" => Ok(StorageOption::FileSystem),
            "sqlar" => Ok(StorageOption::Sqlar),
            "s3" => Ok(StorageOption::S3),
            "rocksdb" => Ok(StorageOption::RocksDb),
            _ => Err(anyhow!("Invalid storage option")),
        }
    }
}

impl Display for StorageOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageOption::FileSystem => write!(f, "filesystem"),
            StorageOption::Sqlar => write!(f, "sqlar"),
            StorageOption::S3 => write!(f, "s3"),
            StorageOption::RocksDb => write!(f, "rocksdb"),
        }
    }
}

impl Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Config {{\n")?;
        write!(f, "  database_file: {}\n", self.database_file)?;
        write!(f, "  mount_point: {}\n", self.mount_point)?;
        write!(f, "  primary: {}\n", self.primary)?;
        write!(f, "  replicas: {:?}\n", self.replicas)?;
        write!(f, "  update_access_time: {}\n", self.update_access_time)?;
        write!(f, "}}")
    }
}

impl Display for StorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StorageConfig {{\n")?;
        write!(f, "  storage_backend: {}\n", self.storage_backend)?;
        write!(f, "  blob_storage: {}\n", self.blob_storage)?;
        write!(f, "  s3_endpoint_url: {}\n", self.s3_endpoint_url)?;
        write!(f, "  s3_region: {}\n", self.s3_region)?;
        write!(f, "  s3_bucket: {}\n", self.s3_bucket)?;
        write!(f, "  s3_base_path: {}\n", self.s3_base_path)?;
        write!(f, "  s3_access_key: {}\n", self.s3_access_key)?;
        write!(f, "  s3_secret_key: {}\n", self.s3_secret_key)?;
        write!(f, "  encryption_key: {}\n", self.encryption_key)?;
        write!(f, "  compression_level: {}\n", self.compression_level)?;
        write!(f, "}}")
    }
}

impl StorageConfig {
    pub fn path_of(&self, info: &ObjInfo) -> String {
        match self.path_generator {
            PathGenerator::Sha512 => {
                if info.sha512.is_empty() {
                    "null".to_string()
                } else {
                    format!("{}.dat", &info.sha512[..32])
                }
            }
            PathGenerator::Id => info.id.to_string(),
            PathGenerator::Path => info.full_path.trim_start_matches('/').to_string(),
        }
    }
}
