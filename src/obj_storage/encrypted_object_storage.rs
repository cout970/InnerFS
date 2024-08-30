use crate::config::{StorageConfig};
use crate::obj_storage::{ObjInfo, ObjectStorage, UniquenessTest};
use aes_gcm::aead::consts::U12;
use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Nonce, Payload};
use aes_gcm::{aead::{Aead, AeadCore, KeyInit, OsRng}, Aes256Gcm};
use anyhow::{anyhow, Error};
use pbkdf2::pbkdf2_hmac;
use sha2::Sha256;
use std::path::PathBuf;
use std::rc::Rc;

const AES_KEY_LEN: usize = 32;
const SALT_LEN: usize = 32;
const NONCE_LEN: usize = 12;
const AEAD_LEN: usize = 10;
// More rounds are better, but slower, since they are used every file access, we need to keep them low
// Technically, we are not storing the password nor the salted password, so it's **fine** (tm)
const PBKDF2_ITERATIONS: u32 = 256;

pub struct EncryptedObjectStorage {
    config: Rc<StorageConfig>,
    fs: Box<dyn ObjectStorage>,
}

pub struct FileKey {
    salt: [u8; SALT_LEN],
    nonce: [u8; NONCE_LEN],
    aead: String,
}

fn vec_to_array<T, const N: usize>(v: Vec<T>) -> Result<[T; N], Error> {
    let len = v.len();
    v.try_into().map_err(|_| anyhow!("Expected a Vec of length {} but it was {}", N, len))
}

impl FileKey {
    pub fn serialize(&self) -> String {
        format!("{}:{}:{}", hex::encode(self.salt), hex::encode(self.nonce), self.aead)
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
    pub fn new(config: Rc<StorageConfig>, fs: Box<dyn ObjectStorage>) -> EncryptedObjectStorage {
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

    pub fn encrypt(private_key: &str, content: &[u8], content_sha512: &str) -> Result<(FileKey, Vec<u8>), Error> {
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

    pub fn encrypt_internal(aes_key: &[u8; AES_KEY_LEN], key: &FileKey, content: &[u8]) -> Result<Vec<u8>, Error> {
        let mut nonce: GenericArray<u8, U12> = Nonce::<Aes256Gcm>::default();
        nonce.copy_from_slice(&key.nonce);

        let cipher = Aes256Gcm::new_from_slice(aes_key)?;

        let ciphertext = cipher.encrypt(&nonce, Payload {
            msg: content,
            aad: key.aead.as_bytes(),
        }).map_err(|_| anyhow!("Encryption failed"))?;

        Ok(ciphertext)
    }

    pub fn decrypt(private_key: &str, file_key: &FileKey, ciphertext: &[u8]) -> Result<Vec<u8>, Error> {
        let aes_key = Self::salt_password(private_key, &file_key.salt);
        let plaintext = Self::decrypt_internal(&aes_key, file_key, ciphertext)?;

        Ok(plaintext)
    }

    pub fn decrypt_internal(aes_key: &[u8; AES_KEY_LEN], file_key: &FileKey, ciphertext: &[u8]) -> Result<Vec<u8>, Error> {
        let mut nonce = Nonce::<Aes256Gcm>::default();
        nonce.copy_from_slice(&file_key.nonce);

        let cipher = Aes256Gcm::new_from_slice(aes_key)?;

        let plaintext = cipher.decrypt(&nonce, Payload {
            msg: ciphertext,
            aad: file_key.aead.as_bytes(),
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
        let original_bytes = Self::decrypt(&self.config.encryption_key, &key, &bytes)?;

        Ok(original_bytes)
    }

    fn put(&mut self, info: &mut ObjInfo, content: &[u8]) -> Result<(), Error> {
        let (key, bytes) = Self::encrypt(&self.config.encryption_key, &content, &info.sha512)?;
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

    fn nuke(&mut self) -> Result<(), Error> {
        self.fs.nuke()
    }

    fn get_uniqueness_test(&self) -> UniquenessTest {
        UniquenessTest::AlwaysUnique
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

    let (file_key, ciphertext) = EncryptedObjectStorage::encrypt(&password, content, &content_sha512).unwrap();
    let serialized_file_key = file_key.serialize();

    // Storage and later retrieval

    let deserialized_file_key = FileKey::deserialize(&serialized_file_key).unwrap();
    let plaintext = EncryptedObjectStorage::decrypt(&password, &deserialized_file_key, &ciphertext).unwrap();

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
    println!(r#"deno run -A ./scripts/aes_decrypt.ts "{}" "{}" "{}" "{}" "{}""#, password, hex::encode(file_key.salt), hex::encode(file_key.nonce), file_key.aead, hex::encode(&ciphertext));
}
