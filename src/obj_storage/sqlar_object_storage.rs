use crate::metadata_db::MetadataDB;
use crate::obj_storage::{BlobStorage, RemoteBlob};
use crate::AnyError;
use log::debug;
use crate::utils::{current_timestamp, humanize_bytes_binary};

#[derive(Clone)]
pub struct SqlarBackend {
    pub sql: MetadataDB,
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

impl SqlarBackend {
    pub fn get_sqlar_file(&mut self, name: &str) -> Result<Option<SqlarFile>, AnyError> {
        self.sql
            .get_row("SELECT mode, mtime, sz, data FROM sqlar WHERE name = :name", (":name", name), |row| {
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

    pub fn remove_sqlar_file(&mut self, name: &str) -> Result<(), AnyError> {
        self.sql
            .execute1("DELETE FROM sqlar WHERE name = :name", (":name", name))?;
        Ok(())
    }
}

impl BlobStorage for SqlarBackend {
    fn get_multiple(&mut self, paths: &[&str]) -> Result<Vec<Vec<u8>>, AnyError> {
        let mut result = Vec::with_capacity(paths.len());
        for path in paths {
            let file = self.get_sqlar_file(path)?;
            if file.is_none() {
                return Err(anyhow::anyhow!("File not found ({})", path));
            }
            result.push(file.unwrap().data);
        }
        Ok(result)
    }

    fn put_multiple(&mut self, blobs: &[RemoteBlob]) -> Result<(), AnyError> {
        for blob in blobs {
            let name = blob.path.to_string();
            debug!("Put: {} ({})", name, humanize_bytes_binary(blob.contents.len()));

            let file = SqlarFile {
                name: name.clone(),
                mode: 0o777,
                mtime: current_timestamp(),
                sz: blob.contents.len() as i64,
                data: blob.contents.to_vec(),
            };
            self.set_sqlar_file(&name, &file)?;
        }
        Ok(())
    }

    fn remove_multiple(&mut self, paths: &[&str]) -> Result<(), AnyError> {
        for path in paths {
            let name = path.to_string();
            debug!("Remove: {}", name);
            self.remove_sqlar_file(&name)?;
        }
        Ok(())
    }

    fn nuke(&mut self) -> Result<(), AnyError> {
        debug!("Nuke");
        self.sql.execute0("DELETE FROM sqlar")?;
        Ok(())
    }
}