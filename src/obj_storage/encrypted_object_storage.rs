use crate::config::StorageConfig;
use crate::obj_storage::{ObjInfo, ObjectStorage, PathGenerator};
use crate::storage::ObjInUseFn;
use crate::AnyError;
use aes_gcm::aead::consts::U12;
use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Nonce, Payload};
use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm,
};
use anyhow::{anyhow, Error};
use pbkdf2::pbkdf2_hmac;
use sha2::Sha256;
use std::rc::Rc;
use std::sync::Arc;

const AES_KEY_LEN: usize = 32;
const SALT_LEN: usize = 32;
const NONCE_LEN: usize = 12;
const AEAD_LEN: usize = 10;
// More rounds are better, but slower, since they are used every file access, we need to keep them low
// Technically, we are not storing the password nor the salted password, so it's **fine** (tm)
const PBKDF2_ITERATIONS: u32 = 256;

pub struct EncryptedObjectStorage {
    config: Arc<StorageConfig>,
    fs: Box<dyn ObjectStorage>,
}

pub struct FileKey {
    salt: [u8; SALT_LEN],
    nonce: [u8; NONCE_LEN],
    aead: String,
}

fn vec_to_array<T, const N: usize>(v: Vec<T>) -> Result<[T; N], Error> {
    let len = v.len();
    v.try_into()
        .map_err(|_| anyhow!("Expected a Vec of length {} but it was {}", N, len))
}

impl FileKey {
    pub fn serialize(&self) -> String {
        format!(
            "{}:{}:{}",
            hex::encode(self.salt),
            hex::encode(self.nonce),
            self.aead
        )
    }

    pub fn deserialize(s: &str) -> Result<FileKey, Error> {
        if s.len() != 100 {
            return Err(anyhow!("Invalid file key: incorrect length"));
        }

        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 3 {
            return Err(anyhow!("Invalid file key"));
        }

        // Validate that the parts are valid hex
        let salt = hex::decode(parts[0])?;
        if salt.len() != SALT_LEN {
            return Err(anyhow!("Invalid file key: incorrect salt length"));
        }
        let nonce = hex::decode(parts[1])?;
        if nonce.len() != NONCE_LEN {
            return Err(anyhow!("Invalid file key: incorrect nonce length"));
        }
        if parts[2].len() != AEAD_LEN {
            return Err(anyhow!("Invalid file key: incorrect AEAD length"));
        }

        Ok(FileKey {
            salt: vec_to_array(salt)?,
            nonce: vec_to_array(nonce)?,
            aead: parts[2].to_string(),
        })
    }
}

impl EncryptedObjectStorage {
    pub fn new(config: Arc<StorageConfig>, fs: Box<dyn ObjectStorage>) -> EncryptedObjectStorage {
        EncryptedObjectStorage { config, fs }
    }

    pub fn generate_salt() -> [u8; SALT_LEN] {
        let mut salt = [0u8; SALT_LEN];
        OsRng.fill_bytes(&mut salt);
        salt
    }

    pub fn salt_password(password: &str, salt: &[u8]) -> [u8; SALT_LEN] {
        let mut key1 = [0u8; SALT_LEN];
        pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, PBKDF2_ITERATIONS, &mut key1);
        key1
    }

    pub fn encrypt(
        private_key: &str,
        content: &[u8],
        content_sha512: &str,
    ) -> Result<(FileKey, Vec<u8>), Error> {
        let salt = Self::generate_salt();
        let aes_key = Self::salt_password(private_key, &salt);

        let nonce_array: GenericArray<u8, U12> = Aes256Gcm::generate_nonce(OsRng);
        let mut nonce = [0u8; NONCE_LEN];
        nonce.copy_from_slice(&nonce_array);

        let aead = content_sha512[..AEAD_LEN].to_string();

        let file_key = FileKey { salt, nonce, aead };
        let ciphertext = Self::encrypt_internal(&aes_key, &file_key, content)?;

        Ok((file_key, ciphertext))
    }

    pub fn encrypt_internal(
        aes_key: &[u8; AES_KEY_LEN],
        key: &FileKey,
        content: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let mut nonce: GenericArray<u8, U12> = Nonce::<Aes256Gcm>::default();
        nonce.copy_from_slice(&key.nonce);

        let cipher = Aes256Gcm::new_from_slice(aes_key)?;

        let ciphertext = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: content,
                    aad: key.aead.as_bytes(),
                },
            )
            .map_err(|_| anyhow!("Encryption failed"))?;

        Ok(ciphertext)
    }

    pub fn decrypt(
        private_key: &str,
        file_key: &FileKey,
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let aes_key = Self::salt_password(private_key, &file_key.salt);
        let plaintext = Self::decrypt_internal(&aes_key, file_key, ciphertext)?;

        Ok(plaintext)
    }

    pub fn decrypt_internal(
        aes_key: &[u8; AES_KEY_LEN],
        file_key: &FileKey,
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let mut nonce = Nonce::<Aes256Gcm>::default();
        nonce.copy_from_slice(&file_key.nonce);

        let cipher = Aes256Gcm::new_from_slice(aes_key)?;

        let plaintext = cipher
            .decrypt(
                &nonce,
                Payload {
                    msg: ciphertext,
                    aad: file_key.aead.as_bytes(),
                },
            )
            .map_err(|_| anyhow!("Decryption failed"))?;

        Ok(plaintext)
    }

    fn path(&self, _key: &FileKey, original_path: &str, id: i64) -> String {
        match self.config.path_generator {
            PathGenerator::Path => original_path.to_string(),
            PathGenerator::Sha512 => {
                // Use the hash of the path instead of the content, since the content is encrypted and multiple files with the same content will collide with the same hash but different keys
                let sha512 = hex::encode(hmac_sha512::Hash::hash(original_path.as_bytes()));
                format!("{}.enc", &sha512[..32])
            }
            PathGenerator::Id => format!("{}.enc", id),
        }
    }

    fn add_to_keychain(keychain: &str, name: &str, key: &FileKey) -> String {
        let key = format!("[{}]{}", name, key.serialize());

        if keychain.is_empty() {
            return key;
        }

        let parts: Vec<String> = keychain.split(',').map(|i| i.to_string()).collect();
        let mut keys = vec![key];

        for part in parts {
            if part.starts_with("[") {
                // Tagged key
                let key_start = part.find("]").expect("Invalid keychain");
                let key_name = &part[1..key_start];

                if key_name != name {
                    keys.push(part);
                }
            } else {
                // Untagged key
                keys.push(part);
            }
        }

        keys.join(",")
    }

    fn get_keys_from_keychain(keychain: &str, name: &str) -> Result<Vec<FileKey>, Error> {
        let parts: Vec<String> = keychain.split(',').map(|i| i.to_string()).collect();
        let mut keys = vec![];

        for part in parts {
            if part.starts_with("[") {
                // Tagged key
                let key_start = part.find("]").ok_or_else(|| anyhow!("Invalid keychain"))?;
                let key_name = &part[1..key_start];
                let key_value = &part[key_start + 1..];

                if key_name == name {
                    keys.push(FileKey::deserialize(key_value)?);
                }
            } else {
                // Untagged key
                keys.push(FileKey::deserialize(&part)?);
            }
        }

        Ok(keys)
    }
}

impl ObjectStorage for EncryptedObjectStorage {
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, Error> {
        let keys = Self::get_keys_from_keychain(&info.encryption_key, &self.config.name)?;

        fn try_key(
            this: &mut EncryptedObjectStorage,
            info: &ObjInfo,
            key: FileKey,
        ) -> Result<Vec<u8>, Error> {
            let mut info = info.clone();
            info.full_path = this.path(&key, &info.full_path, info.id);

            let bytes = this.fs.get(&info)?;
            EncryptedObjectStorage::decrypt(&this.config.encryption_key, &key, &bytes)
        }

        let mut last_error = None;

        for key in keys {
            match try_key(self, info, key) {
                Ok(bytes) => return Ok(bytes),
                Err(e) => {
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("No valid key found")))
    }

    fn put(&mut self, info: &mut ObjInfo, content: &[u8]) -> Result<(), Error> {
        let (key, bytes) = Self::encrypt(&self.config.encryption_key, &content, &info.sha512)?;
        let full_path = self.path(&key, &info.full_path, info.id);
        let prev_path = info.full_path.clone();

        // Hide real path, to avoid leaking information (only has effect if config.path_generator is Path)
        info.full_path = full_path;
        info.encryption_key = Self::add_to_keychain(&info.encryption_key, &self.config.name, &key);
        self.fs.put(info, &bytes)?;
        info.full_path = prev_path;
        Ok(())
    }

    fn remove(&mut self, info: &ObjInfo, is_in_use: ObjInUseFn) -> Result<(), Error> {
        let keys = Self::get_keys_from_keychain(&info.encryption_key, &self.config.name)?;

        fn try_key(
            this: &mut EncryptedObjectStorage,
            info: &ObjInfo,
            key: FileKey,
            is_in_use: ObjInUseFn,
        ) -> Result<(), Error> {
            let original_info = info.clone();
            let mut info_copy = info.clone();
            info_copy.full_path = this.path(&key, &info_copy.full_path, info_copy.id);

            this.fs.remove(&info_copy, Rc::new(move |_, pg| is_in_use(&original_info, pg)))
        }

        let mut last_error = None;

        for key in keys {
            match try_key(self, info, key, is_in_use.clone()) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("No valid key found")))
    }

    fn rename(&mut self, prev_info: &ObjInfo, new_info: &ObjInfo) -> Result<(), AnyError> {
        let keys = Self::get_keys_from_keychain(&prev_info.encryption_key, &self.config.name)?;

        fn try_key(
            this: &mut EncryptedObjectStorage,
            prev_info: &ObjInfo,
            new_info: &ObjInfo,
            key: FileKey,
        ) -> Result<(), AnyError> {
            let prev_path = this.path(&key, &prev_info.full_path, prev_info.id);
            let new_path = this.path(&key, &new_info.full_path, new_info.id);

            if prev_path != new_path {
                let mut prev_info = prev_info.clone();
                let mut new_info = new_info.clone();

                prev_info.full_path = prev_path;
                new_info.full_path = new_path;

                this.fs.rename(&prev_info, &new_info)?;
            }
            Ok(())
        }

        let mut last_error = None;

        for key in keys {
            match try_key(self, prev_info, new_info, key) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("No valid key found")))
    }

    fn nuke(&mut self) -> Result<(), Error> {
        self.fs.nuke()
    }

    fn clone(&self) -> Box<dyn ObjectStorage> {
        Box::new(Self {
            config: self.config.clone(),
            fs: self.fs.clone(),
        })
    }
}

#[test]
fn test_key_derivation() {
    let password = "1234";
    let salt_bytes = b"1234";
    let salted_password = EncryptedObjectStorage::salt_password(password, salt_bytes);

    println!("Password: {:?}", password);
    println!("Salt: {:?}", hex::encode(salt_bytes));
    println!("Salted password: {:?}", hex::encode(salted_password));
}

#[test]
fn test_encryption() {
    let password = "1234";
    let content = "Hello world".as_bytes();
    let content_sha512 = hex::encode(hmac_sha512::Hash::hash(content));

    let (file_key, ciphertext) =
        EncryptedObjectStorage::encrypt(&password, content, &content_sha512).unwrap();
    let serialized_file_key = file_key.serialize();

    // Storage and later retrieval

    let deserialized_file_key = FileKey::deserialize(&serialized_file_key).unwrap();
    let plaintext =
        EncryptedObjectStorage::decrypt(&password, &deserialized_file_key, &ciphertext).unwrap();

    println!("Password: {:?}", password);
    println!("Salt: {:?}", hex::encode(file_key.salt));
    println!("Nonce: {:?}", hex::encode(file_key.nonce));
    println!("AEAD: {:?}", file_key.aead);
    println!("PBKDF2 rounds: {:?}", PBKDF2_ITERATIONS);
    println!("Ciphertext: {:?}", hex::encode(&ciphertext));
    println!("FileKey: {:?}", serialized_file_key);
    println!("Content SHA512: {:?}", content_sha512);
    println!("Content: {:?}", String::from_utf8_lossy(content));
    println!("Plaintext: {:?}", String::from_utf8_lossy(&plaintext));

    // Using the provided script to decrypt the ciphertext with all the parameters
    println!(
        r#"deno run -A ./scripts/aes_decrypt.ts "{}" "{}" "{}" "{}" "{}""#,
        password,
        hex::encode(file_key.salt),
        hex::encode(file_key.nonce),
        file_key.aead,
        hex::encode(&ciphertext)
    );
}
