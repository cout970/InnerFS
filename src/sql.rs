use std::path::PathBuf;
use anyhow::anyhow;
use sqlite::{Bindable, State, Statement};

pub struct SQL {
    pub connection: sqlite::Connection,
}

const ROOT_DIRECTORY_ID: i64 = 1;

#[derive(Debug, Clone)]
pub struct FileRow {
    pub id: i64,
    pub kind: i64,
    pub name: String,
    pub uid: i64,
    pub gid: i64,
    pub perms: i64,
    pub size: i64,
    pub sha512: String,
    pub encryption_key: String,
    pub accessed_at: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct DirectoryEntry {
    pub id: i64,
    pub directory_file_id: i64,
    pub entry_file_id: i64,
    pub name: String,
    pub kind: i64,
}

#[allow(dead_code)]
impl SQL {
    pub fn open(database_file: &str) -> SQL {
        let connection = sqlite::open(database_file).expect("Unable to open database");

        let seed = include_str!("./seed.sql");
        connection.execute(seed).unwrap();

        SQL { connection }
    }

    pub fn add_file(self: &SQL, file: &FileRow) -> Result<i64, anyhow::Error> {
        self.execute11(
            "INSERT INTO files (kind, name, uid, gid, perms, size, sha512, encryption_key, accessed_at, created_at, updated_at) \
            VALUES (:kind, :name, :uid, :gid, :perms, :size, :sha512, :encryption_key, :accessed_at, :created_at, :updated_at)",
            (":kind", file.kind),
            (":name", file.name.as_str()),
            (":uid", file.uid),
            (":gid", file.gid),
            (":perms", file.perms),
            (":size", file.size),
            (":sha512", file.sha512.as_str()),
            (":encryption_key", file.encryption_key.as_str()),
            (":accessed_at", file.accessed_at),
            (":created_at", file.created_at),
            (":updated_at", file.updated_at),
        )?;

        let id = self.get_last_inserted_row_id()?;
        Ok(id)
    }

    pub fn get_file(self: &SQL, id: i64) -> Result<Option<FileRow>, anyhow::Error> {
        self.get_row(
            "SELECT * FROM files WHERE id = :id",
            (":id", id),
            |row| {
                Ok(FileRow {
                    id: row.read("id")?,
                    kind: row.read("kind")?,
                    name: row.read("name")?,
                    uid: row.read("uid")?,
                    gid: row.read("gid")?,
                    perms: row.read("perms")?,
                    size: row.read("size")?,
                    sha512: row.read("sha512")?,
                    encryption_key: row.read("encryption_key")?,
                    accessed_at: row.read("accessed_at")?,
                    created_at: row.read("created_at")?,
                    updated_at: row.read("updated_at")?,
                })
            })
    }

    pub fn get_file_by_sha512(self: &SQL, sha512: &str) -> Result<Option<FileRow>, anyhow::Error> {
        self.get_row(
            "SELECT * FROM files WHERE sha512 = :sha512 LIMIT 1",
            (":sha512", sha512),
            |row| {
                Ok(FileRow {
                    id: row.read("id")?,
                    kind: row.read("kind")?,
                    name: row.read("name")?,
                    uid: row.read("uid")?,
                    gid: row.read("gid")?,
                    perms: row.read("perms")?,
                    size: row.read("size")?,
                    sha512: row.read("sha512")?,
                    encryption_key: row.read("encryption_key")?,
                    accessed_at: row.read("accessed_at")?,
                    created_at: row.read("created_at")?,
                    updated_at: row.read("updated_at")?,
                })
            })
    }

    pub fn get_file_by_path(self: &SQL, path: &str) -> Result<Option<FileRow>, anyhow::Error> {
        let buff = PathBuf::from(path);
        let mut current = ROOT_DIRECTORY_ID;

        for part in buff.iter() {
            let name = part.to_string_lossy();
            let entry = self.find_directory_entry(current, &name)?;

            match entry {
                Some(e) => {
                    current = e.entry_file_id;
                },
                None => {
                    return Ok(None);
                }
            }
        }

        self.get_file(current)
    }

    pub fn update_file(self: &SQL, file: &FileRow) -> Result<(), anyhow::Error> {
        self.execute12(
            "UPDATE files SET kind = :kind, name = :name, uid = :uid, gid = :gid, perms = :perms, size = :size, sha512 = :sha512, encryption_key = :encryption_key, accessed_at = :accessed_at, created_at = :created_at, updated_at = :updated_at WHERE id = :id",
            (":kind", file.kind),
            (":name", file.name.as_str()),
            (":uid", file.uid),
            (":gid", file.gid),
            (":perms", file.perms),
            (":size", file.size),
            (":sha512", file.sha512.as_str()),
            (":encryption_key", file.encryption_key.as_str()),
            (":accessed_at", file.accessed_at),
            (":created_at", file.created_at),
            (":updated_at", file.updated_at),
            (":id", file.id),
        )?;
        Ok(())
    }

    pub fn remove_file(self: &SQL, id: i64) -> Result<(), anyhow::Error> {
        self.execute1("DELETE FROM files WHERE id = :id", (":id", id))?;
        self.execute1("DELETE FROM directory_entry WHERE entry_file_id = :id OR directory_file_id = :id", (":id", id))?;
        Ok(())
    }

    pub fn find_directory_entry(self: &SQL, directory_file_id: i64, name: &str) -> Result<Option<DirectoryEntry>, anyhow::Error> {
        self.get_row(
            "SELECT * FROM directory_entry WHERE directory_file_id = :directory_file_id and name = :name",
            &[(":directory_file_id", directory_file_id.to_string().as_str()), (":name", name)][..],
            |row| {
                Ok(DirectoryEntry {
                    id: row.read("id")?,
                    directory_file_id: row.read("directory_file_id")?,
                    entry_file_id: row.read("entry_file_id")?,
                    name: row.read("name")?,
                    kind: row.read("kind")?,
                })
            })
    }

    pub fn find_parent_directory(self: &SQL, file_id: i64) -> Result<Option<i64>, anyhow::Error> {
        self.get_row(
            "SELECT directory_file_id FROM directory_entry WHERE entry_file_id = :file_id and name <> '.' and name <> '..'",
            &[(":file_id", file_id)][..],
            |row| {
                Ok(row.read::<i64, _>("directory_file_id")?)
            })
    }

    pub fn get_file_path(self: &SQL, file_id: i64) -> Result<String, anyhow::Error> {
        let mut path_components = vec![];
        let mut current_file_id = file_id;

        loop {
            let file = self.get_file(current_file_id)?;
            if file.is_none() {
                return Err(anyhow!("Unable to get file path ({})", file_id));
            }
            let file = file.unwrap();

            if file.name == "/" {
                break;
            }

            path_components.push(file.name.to_string());

            let parent_directory_id = self.find_parent_directory(current_file_id)?;
            if parent_directory_id.is_none() || parent_directory_id.unwrap() == current_file_id {
                break;
            }

            current_file_id = parent_directory_id.unwrap();
        }

        let mut path = String::new();
        for p in path_components.iter().rev() {
            path.push('/');
            path.push_str(p);
        }

        Ok(path)
    }

    pub fn get_directory_entries(self: &SQL, directory_file_id: i64, limit: i64, offset: i64) -> Result<Vec<DirectoryEntry>, anyhow::Error> {
        let query = "\
            SELECT * \
            FROM directory_entry \
            WHERE directory_file_id = :directory_file_id \
            LIMIT :limit \
            OFFSET :offset";

        self.get_rows(
            query,
            &[
                (":directory_file_id", directory_file_id),
                (":limit", limit),
                (":offset", offset)
            ][..],
            |row| {
                Ok(DirectoryEntry {
                    id: row.read("id")?,
                    directory_file_id: row.read("directory_file_id")?,
                    entry_file_id: row.read("entry_file_id")?,
                    name: row.read("name")?,
                    kind: row.read("kind")?,
                })
            })
    }

    pub fn update_directory_entry(self: &SQL, entry: &DirectoryEntry) -> Result<(), anyhow::Error> {
        self.execute5(
            "UPDATE directory_entry SET directory_file_id = :directory_file_id, entry_file_id = :entry_file_id, name = :name, kind = :kind WHERE id = :id",
            (":directory_file_id", entry.directory_file_id),
            (":entry_file_id", entry.entry_file_id),
            (":name", entry.name.as_str()),
            (":kind", entry.kind),
            (":id", entry.id),
        )?;
        Ok(())
    }

    pub fn add_directory_entry(self: &SQL, entry: &DirectoryEntry) -> Result<i64, anyhow::Error> {
        self.execute4(
            "INSERT INTO directory_entry (directory_file_id, entry_file_id, name, kind) \
            VALUES (:directory_file_id, :entry_file_id, :name, :kind)",
            (":directory_file_id", entry.directory_file_id),
            (":entry_file_id", entry.entry_file_id),
            (":name", entry.name.as_str()),
            (":kind", entry.kind),
        )?;

        let id = self.get_last_inserted_row_id()?;
        Ok(id)
    }

    pub fn get_last_inserted_row_id(self: &SQL) -> Result<i64, anyhow::Error> {
        let mut stm2 = self.connection.prepare("SELECT last_insert_rowid()")?;
        stm2.next()?;
        let id: i64 = stm2.read::<i64, _>(0)?;
        Ok(id)
    }

    pub fn file_set_access_time(self: &SQL, id: i64, accessed_at: i64) -> Result<(), anyhow::Error> {
        self.execute2(
            "UPDATE files SET accessed_at = :accessed_at WHERE id = :id",
            (":accessed_at", accessed_at),
            (":id", id),
        )?;
        Ok(())
    }

    pub fn get_row<'l, 'q, T, M, R>(self: &'q SQL, query: &str, bindings: T, mapper: M) -> Result<Option<R>, anyhow::Error>
    where
        T: Bindable + Clone,
        M: FnOnce(&Statement<'l>) -> Result<R, anyhow::Error>,
        'q: 'l,
    {
        let mut statement = self.connection.prepare(query)?;
        statement.bind(bindings)?;

        if let State::Row = statement.next()? {
            return mapper(&statement).map(Some);
        }

        Ok(None)
    }

    pub fn get_rows<'l, 'q, T, M, R>(self: &'q SQL, query: &str, bindings: T, mapper: M) -> Result<Vec<R>, anyhow::Error>
    where
        T: Bindable + Clone,
        M: Fn(&Statement<'l>) -> Result<R, anyhow::Error>,
        'q: 'l,
    {
        let mut statement = self.connection.prepare(query)?;
        statement.bind(bindings)?;
        let mut result = vec![];

        while let State::Row = statement.next()? {
            result.push(mapper(&statement)?);
        }

        Ok(result)
    }

    pub fn execute0(self: &SQL, query: &str) -> Result<(), anyhow::Error> {
        let mut statement = self.connection.prepare(query)?;
        statement.next()?;
        Ok(())
    }

    pub fn execute1<B0>(self: &SQL, query: &str, b0: B0) -> Result<(), anyhow::Error>
    where
        B0: Bindable + Clone,
    {
        let mut statement = self.connection.prepare(query)?;
        statement.bind(b0)?;
        statement.next()?;
        Ok(())
    }

    pub fn execute2<B0, B1>(self: &SQL, query: &str, b0: B0, b1: B1) -> Result<(), anyhow::Error>
    where
        B0: Bindable + Clone,
        B1: Bindable + Clone,
    {
        let mut statement = self.connection.prepare(query)?;
        statement.bind(b0)?;
        statement.bind(b1)?;
        statement.next()?;
        Ok(())
    }

    pub fn execute3<B0, B1, B2>(self: &SQL, query: &str, b0: B0, b1: B1, b2: B2) -> Result<(), anyhow::Error>
    where
        B0: Bindable + Clone,
        B1: Bindable + Clone,
        B2: Bindable + Clone,
    {
        let mut statement = self.connection.prepare(query)?;
        statement.bind(b0)?;
        statement.bind(b1)?;
        statement.bind(b2)?;
        statement.next()?;
        Ok(())
    }

    pub fn execute4<B0, B1, B2, B3>(self: &SQL, query: &str, b0: B0, b1: B1, b2: B2, b3: B3) -> Result<(), anyhow::Error>
    where
        B0: Bindable + Clone,
        B1: Bindable + Clone,
        B2: Bindable + Clone,
        B3: Bindable + Clone,
    {
        let mut statement = self.connection.prepare(query)?;
        statement.bind(b0)?;
        statement.bind(b1)?;
        statement.bind(b2)?;
        statement.bind(b3)?;
        statement.next()?;
        Ok(())
    }

    pub fn execute5<B0, B1, B2, B3, B4>(self: &SQL, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4) -> Result<(), anyhow::Error>
    where
        B0: Bindable + Clone,
        B1: Bindable + Clone,
        B2: Bindable + Clone,
        B3: Bindable + Clone,
        B4: Bindable + Clone,
    {
        let mut statement = self.connection.prepare(query)?;
        statement.bind(b0)?;
        statement.bind(b1)?;
        statement.bind(b2)?;
        statement.bind(b3)?;
        statement.bind(b4)?;
        statement.next()?;
        Ok(())
    }

    pub fn execute6<B0, B1, B2, B3, B4, B5>(self: &SQL, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4, b5: B5) -> Result<(), anyhow::Error>
    where
        B0: Bindable + Clone,
        B1: Bindable + Clone,
        B2: Bindable + Clone,
        B3: Bindable + Clone,
        B4: Bindable + Clone,
        B5: Bindable + Clone,
    {
        let mut statement = self.connection.prepare(query)?;
        statement.bind(b0)?;
        statement.bind(b1)?;
        statement.bind(b2)?;
        statement.bind(b3)?;
        statement.bind(b4)?;
        statement.bind(b5)?;
        statement.next()?;
        Ok(())
    }

    pub fn execute7<B0, B1, B2, B3, B4, B5, B6>(self: &SQL, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4, b5: B5, b6: B6) -> Result<(), anyhow::Error>
    where
        B0: Bindable + Clone,
        B1: Bindable + Clone,
        B2: Bindable + Clone,
        B3: Bindable + Clone,
        B4: Bindable + Clone,
        B5: Bindable + Clone,
        B6: Bindable + Clone,
    {
        let mut statement = self.connection.prepare(query)?;
        statement.bind(b0)?;
        statement.bind(b1)?;
        statement.bind(b2)?;
        statement.bind(b3)?;
        statement.bind(b4)?;
        statement.bind(b5)?;
        statement.bind(b6)?;
        statement.next()?;
        Ok(())
    }

    pub fn execute8<B0, B1, B2, B3, B4, B5, B6, B7>(self: &SQL, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4, b5: B5, b6: B6, b7: B7) -> Result<(), anyhow::Error>
    where
        B0: Bindable + Clone,
        B1: Bindable + Clone,
        B2: Bindable + Clone,
        B3: Bindable + Clone,
        B4: Bindable + Clone,
        B5: Bindable + Clone,
        B6: Bindable + Clone,
        B7: Bindable + Clone,
    {
        let mut statement = self.connection.prepare(query)?;
        statement.bind(b0)?;
        statement.bind(b1)?;
        statement.bind(b2)?;
        statement.bind(b3)?;
        statement.bind(b4)?;
        statement.bind(b5)?;
        statement.bind(b6)?;
        statement.bind(b7)?;
        statement.next()?;
        Ok(())
    }

    pub fn execute9<B0, B1, B2, B3, B4, B5, B6, B7, B8>(self: &SQL, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4, b5: B5, b6: B6, b7: B7, b8: B8) -> Result<(), anyhow::Error>
    where
        B0: Bindable + Clone,
        B1: Bindable + Clone,
        B2: Bindable + Clone,
        B3: Bindable + Clone,
        B4: Bindable + Clone,
        B5: Bindable + Clone,
        B6: Bindable + Clone,
        B7: Bindable + Clone,
        B8: Bindable + Clone,
    {
        let mut statement = self.connection.prepare(query)?;
        statement.bind(b0)?;
        statement.bind(b1)?;
        statement.bind(b2)?;
        statement.bind(b3)?;
        statement.bind(b4)?;
        statement.bind(b5)?;
        statement.bind(b6)?;
        statement.bind(b7)?;
        statement.bind(b8)?;
        statement.next()?;
        Ok(())
    }

    pub fn execute10<B0, B1, B2, B3, B4, B5, B6, B7, B8, B9>(self: &SQL, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4, b5: B5, b6: B6, b7: B7, b8: B8, b9: B9) -> Result<(), anyhow::Error>
    where
        B0: Bindable + Clone,
        B1: Bindable + Clone,
        B2: Bindable + Clone,
        B3: Bindable + Clone,
        B4: Bindable + Clone,
        B5: Bindable + Clone,
        B6: Bindable + Clone,
        B7: Bindable + Clone,
        B8: Bindable + Clone,
        B9: Bindable + Clone,
    {
        let mut statement = self.connection.prepare(query)?;
        statement.bind(b0)?;
        statement.bind(b1)?;
        statement.bind(b2)?;
        statement.bind(b3)?;
        statement.bind(b4)?;
        statement.bind(b5)?;
        statement.bind(b6)?;
        statement.bind(b7)?;
        statement.bind(b8)?;
        statement.bind(b9)?;
        statement.next()?;
        Ok(())
    }

    pub fn execute11<B0, B1, B2, B3, B4, B5, B6, B7, B8, B9, B10>(self: &SQL, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4, b5: B5, b6: B6, b7: B7, b8: B8, b9: B9, b10: B10) -> Result<(), anyhow::Error>
    where
        B0: Bindable + Clone,
        B1: Bindable + Clone,
        B2: Bindable + Clone,
        B3: Bindable + Clone,
        B4: Bindable + Clone,
        B5: Bindable + Clone,
        B6: Bindable + Clone,
        B7: Bindable + Clone,
        B8: Bindable + Clone,
        B9: Bindable + Clone,
        B10: Bindable + Clone,
    {
        let mut statement = self.connection.prepare(query)?;
        statement.bind(b0)?;
        statement.bind(b1)?;
        statement.bind(b2)?;
        statement.bind(b3)?;
        statement.bind(b4)?;
        statement.bind(b5)?;
        statement.bind(b6)?;
        statement.bind(b7)?;
        statement.bind(b8)?;
        statement.bind(b9)?;
        statement.bind(b10)?;
        statement.next()?;
        Ok(())
    }

    pub fn execute12<B0, B1, B2, B3, B4, B5, B6, B7, B8, B9, B10, B11>(self: &SQL, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4, b5: B5, b6: B6, b7: B7, b8: B8, b9: B9, b10: B10, b11: B11) -> Result<(), anyhow::Error>
    where
        B0: Bindable + Clone,
        B1: Bindable + Clone,
        B2: Bindable + Clone,
        B3: Bindable + Clone,
        B4: Bindable + Clone,
        B5: Bindable + Clone,
        B6: Bindable + Clone,
        B7: Bindable + Clone,
        B8: Bindable + Clone,
        B9: Bindable + Clone,
        B10: Bindable + Clone,
        B11: Bindable + Clone,
    {
        let mut statement = self.connection.prepare(query)?;
        statement.bind(b0)?;
        statement.bind(b1)?;
        statement.bind(b2)?;
        statement.bind(b3)?;
        statement.bind(b4)?;
        statement.bind(b5)?;
        statement.bind(b6)?;
        statement.bind(b7)?;
        statement.bind(b8)?;
        statement.bind(b9)?;
        statement.bind(b10)?;
        statement.bind(b11)?;
        statement.next()?;
        Ok(())
    }

    pub fn transaction<R>(self: &SQL, func: impl FnOnce() -> Result<R, anyhow::Error>) -> Result<R, anyhow::Error> {
        self.connection.execute("BEGIN TRANSACTION")?;
        let res = func();
        if res.is_ok() {
            self.connection.execute("COMMIT")?;
        } else {
            self.connection.execute("ROLLBACK")?;
        }
        res
    }
}