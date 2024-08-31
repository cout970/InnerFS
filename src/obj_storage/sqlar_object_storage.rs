use std::rc::Rc;
use log::{info};
use crate::AnyError;
use crate::config::StorageConfig;
use crate::obj_storage::{ObjectStorage, ObjInfo, UniquenessTest};
use crate::metadata_db::MetadataDB;
use crate::storage::ObjInUseFn;

pub struct SqlarObjectStorage {
    pub sql: Rc<MetadataDB>,
    pub config: Rc<StorageConfig>,
}

// https://sqlite.org/sqlar.html
// CREATE TABLE IF NOT EXISTS sqlar(
//   name TEXT PRIMARY KEY,  -- name of the file
//   mode INT,               -- access permissions
//   mtime INT,              -- last modification time
//   sz INT,                 -- original file size
//   data BLOB               -- compressed content
// );
#[allow(dead_code)]
pub struct SqlarFile {
    pub name: String,
    pub mode: i64,
    pub mtime: i64,
    pub sz: i64,
    pub data: Vec<u8>,
}

impl ObjectStorage for SqlarObjectStorage {
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, AnyError> {
        info!("Get: {}", info);
        let name = self.path(&info);
        let file = self.get_sqlar_file(&name)?;
        if file.is_none() {
            return Err(anyhow::anyhow!("File not found ({})", info.name));
        }
        Ok(file.unwrap().data)
    }

    fn put(&mut self, info: &mut ObjInfo, content: &[u8]) -> Result<(), AnyError> {
        let name = self.path(&info);
        info!("Create: {}", name);

        let file = SqlarFile {
            name: name.clone(),
            mode: info.mode as i64,
            mtime: info.updated_at,
            sz: info.size as i64,
            data: content.to_vec(),
        };
        self.set_sqlar_file(&name, &file)?;
        Ok(())
    }

    fn remove(&mut self, info: &ObjInfo, is_in_use: ObjInUseFn) -> Result<(), AnyError> {
        let test = if self.config.use_hash_as_filename {
            UniquenessTest::Sha512
        } else {
            UniquenessTest::Path
        };

        // If is object in use by other file (deduplication), do not remove it
        if is_in_use(info, test)? {
            return Ok(());
        }

        let name = self.path(&info);
        info!("Remove: {}", name);

        self.remove_sqlar_file(&name)?;
        Ok(())
    }

    fn rename(&mut self, prev_info: &ObjInfo, new_info: &ObjInfo) -> Result<(), AnyError> {
        let prev_name = self.path(&prev_info);
        let new_name = self.path(&new_info);
        info!("Rename: {} -> {}", prev_name, new_name);

        self.rename_sqlar_file(&prev_name, &new_name)?;
        Ok(())
    }

    fn nuke(&mut self) -> Result<(), AnyError> {
        info!("Nuke");
        self.sql.execute0("DELETE FROM sqlar")?;
        Ok(())
    }
}

impl SqlarObjectStorage {
    pub fn get_sqlar_file(&mut self, name: &str) -> Result<Option<SqlarFile>, AnyError> {
        self.sql.get_row(
            "SELECT mode, mtime, sz, data FROM sqlar WHERE name = :name",
            (":name", name),
            |row| {
                Ok(SqlarFile {
                    name: name.to_string(),
                    mode: row.read::<i64, _>(0)?,
                    mtime: row.read::<i64, _>(1)?,
                    sz: row.read::<i64, _>(2)?,
                    data: row.read::<Vec<u8>, _>(3)?,
                })
            })
    }

    pub fn set_sqlar_file(&mut self, name: &str, file: &SqlarFile) -> Result<(), AnyError> {
        self.sql.execute5(
            "INSERT OR REPLACE INTO sqlar (name, mode, mtime, sz, data) VALUES (:name, :mode, :mtime, :sz, :data)",
            (":name", name),
            (":mode", file.mode),
            (":mtime", file.mtime),
            (":sz", file.sz),
            (":data", file.data.as_slice()),
        )?;
        Ok(())
    }

    pub fn rename_sqlar_file(&mut self, prev_name: &str, new_name: &str) -> Result<(), AnyError> {
        self.sql.execute2(
            "UPDATE sqlar SET name = :new_name WHERE name = :prev_name",
            (":new_name", new_name),
            (":prev_name", prev_name),
        )?;
        Ok(())
    }

    pub fn remove_sqlar_file(&mut self, name: &str) -> Result<(), AnyError> {
        self.sql.execute1("DELETE FROM sqlar WHERE name = :name", (":name", name))?;
        Ok(())
    }

    pub fn path(&self, info: &ObjInfo) -> String {
        self.config.path_of(&info)
    }
}