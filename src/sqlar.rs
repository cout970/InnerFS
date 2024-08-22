use std::rc::Rc;

use crate::obj_storage::{ObjectStorage, ObjInfo};
use crate::sql::SQL;

pub struct SqlarObjectStorage {
    pub sql: Rc<SQL>,
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
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, anyhow::Error> {
        println!("Get: {}", info);
        let name = info.full_path.clone();
        let file = self.get_sqlar_file(&name)?;
        if file.is_none() {
            return Err(anyhow::anyhow!("File not found ({})", info.name));
        }
        Ok(file.unwrap().data)
    }

    fn set(&mut self, info: &ObjInfo, content: &[u8]) -> Result<(), anyhow::Error> {
        println!("Create: {}", info);
        let name = info.full_path.clone();
        let file = SqlarFile {
            name: name.clone(),
            mode: info.mode as i64,
            mtime: info.updated_at,
            sz: content.len() as i64,
            data: content.to_vec(),
        };
        self.set_sqlar_file(&name, &file)?;
        Ok(())
    }

    fn remove(&mut self, info: &ObjInfo) -> Result<(), anyhow::Error> {
        println!("Remove: {}", info);
        let name = info.full_path.clone();
        self.remove_sqlar_file(&name)?;
        Ok(())
    }
}

impl SqlarObjectStorage {
    pub fn get_sqlar_file(&mut self, name: &str) -> Result<Option<SqlarFile>, anyhow::Error> {
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

    pub fn set_sqlar_file(&mut self, name: &str, file: &SqlarFile) -> Result<(), anyhow::Error> {
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

    pub fn remove_sqlar_file(&mut self, name: &str) -> Result<(), anyhow::Error> {
        self.sql.execute1("DELETE FROM sqlar WHERE name = :name", (":name", name))?;
        Ok(())
    }
}