use std::path::PathBuf;
use std::rc::Rc;
use aes_gcm::{aead::{Aead, AeadCore, KeyInit, OsRng}, Aes256Gcm};
use aes_gcm::aead::Payload;

use aes_gcm::aead::rand_core::RngCore;
use anyhow::{anyhow, Context, Error};
use pbkdf2::{pbkdf2_hmac};
use crate::config::Config;
use crate::obj_storage::{ObjInfo, ObjectStorage};
use sha2::Sha256;

pub struct EncryptedObjectStorage {
    config: Rc<Config>,
    fs: Box<dyn ObjectStorage>,
}

pub struct FileKey {
    salt: String,
    nonce: String,
    aead: String,
}

impl FileKey {
    pub fn serialize(&self) -> String {
        format!("{}:{}:{}", self.salt, self.nonce, self.aead)
    }

    pub fn deserialize(s: &str) -> Result<FileKey, Error> {
        let parts: Vec<&str> = s.split(':').collect();

        if parts.len() != 3 {
            return Err(anyhow!("Invalid file key"));
        }

        // Validate that the parts are valid hex
        hex::decode(parts[0])?;
        hex::decode(parts[1])?;
        hex::decode(parts[2])?;

        Ok(FileKey {
            salt: parts[0].to_string(),
            nonce: parts[1].to_string(),
            aead: parts[2].to_string(),
        })
    }
}

impl EncryptedObjectStorage {
    pub fn new(config: Rc<Config>, fs: Box<dyn ObjectStorage>) -> EncryptedObjectStorage {
        EncryptedObjectStorage { config, fs }
    }

    fn generate_salt(&self) -> Vec<u8> {
        let mut salt = vec![0u8; 32];
        OsRng.fill_bytes(&mut salt);
        salt
    }

    fn salt_password(&self, password: &str, salt: &[u8]) -> [u8; 32] {
        let mut key1 = [0u8; 32];
        // More rounds are better, but slower, since they are used every file access, we need to keep them low
        pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, 256, &mut key1);
        key1
    }

    fn encrypt(&self, content: &[u8]) -> Result<(FileKey, Vec<u8>), Error> {
        let salt = self.generate_salt();
        let salted_password = self.salt_password(&self.config.encryption_key, &salt);

        let nonce = Aes256Gcm::generate_nonce(OsRng);
        let cipher = Aes256Gcm::new_from_slice(&salted_password)?;
        let content_sha512 = hmac_sha512::Hash::hash(content);

        let ciphertext = cipher.encrypt(&nonce, Payload {
            msg: content,
            aad: content_sha512.as_slice(),
        }).map_err(|_| anyhow!("Encryption failed"))?;

        let file_key = FileKey {
            salt: hex::encode(salt),
            nonce: hex::encode(nonce),
            aead: hex::encode(content_sha512),
        };

        Ok((file_key, ciphertext))
    }

    fn decrypt(&self, file_key: &FileKey, ciphertext: &[u8]) -> Result<Vec<u8>, Error> {
        let salt = hex::decode(&file_key.salt).context("Invalid salt")?;
        let nonce = hex::decode(&file_key.nonce).context("Invalid nonce")?;
        let aead = hex::decode(&file_key.aead).context("Invalid aead")?;

        let salted_password = self.salt_password(&self.config.encryption_key, &salt);
        let cipher = Aes256Gcm::new_from_slice(&salted_password)?;

        let mut aes256gcm_nonce = aes_gcm::aead::Nonce::<Aes256Gcm>::default();
        aes256gcm_nonce.copy_from_slice(&nonce);

        let plaintext = cipher.decrypt(&aes256gcm_nonce, Payload {
            msg: ciphertext,
            aad: aead.as_slice(),
        }).map_err(|_| anyhow!("Decryption failed"))?;

        Ok(plaintext)
    }

    fn path(&self, key: &FileKey, original_path: &str) -> String {
        if self.config.use_hash_as_filename {
            let uniq = hex::encode(&key.nonce);
            let mut path = PathBuf::from(original_path);
            path.pop();
            path.push(format!("{}.enc", uniq));
            path.to_string_lossy().to_string()
        } else {
            original_path.to_string()
        }
    }
}

impl ObjectStorage for EncryptedObjectStorage {
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, Error> {
        let key = FileKey::deserialize(&info.encryption_key)?;
        let mut info = info.clone();
        info.full_path = self.path(&key, &info.full_path);

        let bytes = self.fs.get(&info)?;
        let original_bytes = self.decrypt(&key, &bytes)?;

        Ok(original_bytes)
    }

    fn put(&mut self, info: &mut ObjInfo, content: &[u8]) -> Result<(), Error> {
        let (key, bytes) = self.encrypt(&content)?;
        let full_path = self.path(&key, &info.full_path);
        let prev_path = info.full_path.clone();

        info.full_path = full_path;
        info.encryption_key = key.serialize();
        self.fs.put(info, &bytes)?;
        info.full_path = prev_path;
        Ok(())
    }

    fn remove(&mut self, info: &ObjInfo) -> Result<(), Error> {
        let key = FileKey::deserialize(&info.encryption_key)?;
        let mut info = info.clone();
        info.full_path = self.path(&key, &info.full_path);

        self.fs.remove(&info)
    }
}
