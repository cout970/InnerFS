use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;
use anyhow::anyhow;
use sqlite::{Bindable, State, Statement};
use crate::AnyError;
use crate::fs_tree::{FsTree, FsTreeRef};

pub struct MetadataDB {
    pub connection: sqlite::Connection,
}

pub const ROOT_DIRECTORY_ID: i64 = 1;
pub const FILE_KIND_REGULAR: i64 = 0;
pub const FILE_KIND_DIRECTORY: i64 = 1;
pub const NO_BINDINGS: [i64; 0] = [];

#[derive(Debug, Clone)]
pub struct FileRow {
    pub id: i64,
    pub version: i64,
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

#[derive(Debug, Clone)]
pub enum FileChangeKind {
    Created,
    UpdatedMetadata,
    UpdatedContents,
    Deleted,
}

#[allow(dead_code)]
impl MetadataDB {
    pub fn open(database_file: &str) -> MetadataDB {
        let connection = sqlite::open(database_file).expect("Unable to open database");

        let seed = include_str!("./seed.sql");
        connection.execute(seed).unwrap();

        MetadataDB { connection }
    }

    pub fn run_migrations(&self) -> Result<(), AnyError> {
        let sql = "SELECT version FROM migrations ORDER BY id DESC";
        #[allow(clippy::unnecessary_cast)]
        let versions = self.get_rows(sql, NO_BINDINGS.as_ref(), |stm| {
            Ok(stm.read::<String, _>("version")?)
        })?;

        if !versions.contains(&"1.0.0".to_string()) {
            panic!("Incorrect migrations table, please re-create the database");
        }

        if !versions.contains(&"1.0.1".to_string()) {
            // New version column, or ignore error if it already exists
            let _ = self.connection.execute("ALTER TABLE files ADD COLUMN version INTEGER NOT NULL DEFAULT 1");

            // Mark migration as complete
            self.connection.execute("INSERT INTO migrations (version, created_at) VALUES ('1.0.1', unixepoch('now'))")?;
        }

        if !versions.contains(&"1.0.2".to_string()) {
            // Rename column
            let _ = self.connection.execute("ALTER TABLE file_changes RENAME COLUMN file_sha512 file_hash TEXT NOT NULL");

            // Mark migration as complete
            self.connection.execute("INSERT INTO migrations (version, created_at) VALUES ('1.0.2', unixepoch('now'))")?;
        }

        Ok(())
    }

    pub fn get_setting(&self, name: &str) -> Result<Option<String>, AnyError> {
        self.get_row("SELECT setting_value FROM persistent_settings WHERE setting_name = :name", (":name", name), |row| {
            Ok(row.read("setting_value")?)
        })
    }

    pub fn set_setting(&self, name: &str, value: &str) -> Result<(), AnyError> {
        self.execute2(
            "INSERT OR REPLACE INTO persistent_settings (setting_name, setting_value, updated_at) VALUES (:name, :value, unixepoch('now'))",
            (":name", name),
            (":value", value),
        )
    }

    pub fn add_file(&self, file: &FileRow) -> Result<i64, AnyError> {
        self.execute12(
            "INSERT INTO files (version, kind, name, uid, gid, perms, size, sha512, encryption_key, accessed_at, created_at, updated_at) \
            VALUES (:version, :kind, :name, :uid, :gid, :perms, :size, :sha512, :encryption_key, :accessed_at, :created_at, :updated_at)",
            (":version", 1),
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

    pub fn get_file(&self, id: i64) -> Result<Option<FileRow>, AnyError> {
        self.get_row(
            "SELECT * FROM files WHERE id = :id",
            (":id", id),
            |row| {
                Ok(FileRow {
                    id: row.read("id")?,
                    version: row.read("version")?,
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

    pub fn get_file_by_sha512(&self, sha512: &str) -> Result<Option<FileRow>, AnyError> {
        self.get_row(
            "SELECT * FROM files WHERE sha512 = :sha512 LIMIT 1",
            (":sha512", sha512),
            |row| {
                Ok(FileRow {
                    id: row.read("id")?,
                    version: row.read("version")?,
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

    pub fn get_file_by_path(&self, path: &str) -> Result<Option<FileRow>, AnyError> {
        let buff = PathBuf::from(path);
        let mut current = ROOT_DIRECTORY_ID;

        for part in buff.iter() {
            let name = part.to_string_lossy();
            let entry = self.find_directory_entry(current, &name)?;

            match entry {
                Some(e) => {
                    current = e.entry_file_id;
                }
                None => {
                    return Ok(None);
                }
            }
        }

        self.get_file(current)
    }

    pub fn update_file(&self, file: &FileRow) -> Result<(), AnyError> {
        self.execute12(
            "UPDATE files SET version = version + 1, kind = :kind, name = :name, uid = :uid, gid = :gid, perms = :perms, size = :size, sha512 = :sha512, encryption_key = :encryption_key, accessed_at = :accessed_at, created_at = :created_at, updated_at = :updated_at WHERE id = :id",
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

    pub fn get_file_version(&self, id: i64) -> Result<Option<i64>, AnyError> {
        self.get_row("SELECT version FROM files WHERE id = :id", (":id", id), |row| {
            Ok(row.read::<i64, _>("version")?)
        })
    }

    pub fn register_file_change(&self, file: &FileRow, kind: FileChangeKind) -> Result<(), AnyError> {
        let version = self.get_file_version(file.id)?.unwrap_or_else(|| 1);
        let sha512 = file.hash();

        self.execute4(
            "INSERT INTO file_changes (file_id, file_version, kind, file_hash, changed_at) values (:file_id, :file_version, :kind, :file_hash, unixepoch('now'))",
            (":file_id", file.id),
            (":file_version", version),
            (":kind", kind.to_i64()),
            (":file_hash", sha512[..16].to_string().as_str()),
        )?;
        Ok(())
    }

    pub fn remove_file(&self, id: i64) -> Result<(), AnyError> {
        self.execute1("DELETE FROM files WHERE id = :id", (":id", id))?;
        self.execute1("DELETE FROM directory_entry WHERE entry_file_id = :id OR directory_file_id = :id", (":id", id))?;
        Ok(())
    }

    pub fn remove_directory_entry(&self, entry_id: i64) -> Result<(), AnyError> {
        self.execute1("DELETE FROM directory_entry WHERE id = :id", (":id", entry_id))?;
        Ok(())
    }

    pub fn find_directory_entry(&self, directory_file_id: i64, name: &str) -> Result<Option<DirectoryEntry>, AnyError> {
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

    pub fn find_parent_directory(&self, file_id: i64) -> Result<Option<i64>, AnyError> {
        self.get_row(
            "SELECT directory_file_id FROM directory_entry WHERE entry_file_id = :file_id and name <> '.' and name <> '..'",
            &[(":file_id", file_id)][..],
            |row| {
                Ok(row.read::<i64, _>("directory_file_id")?)
            })
    }

    pub fn get_file_path(&self, file_id: i64) -> Result<String, AnyError> {
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

    pub fn get_directory_entries(&self, directory_file_id: i64, limit: i64, offset: i64) -> Result<Vec<DirectoryEntry>, AnyError> {
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

    pub fn update_directory_entry(&self, entry: &DirectoryEntry) -> Result<(), AnyError> {
        self.execute5(
            "UPDATE directory_entry SET directory_file_id = :directory_file_id, entry_file_id = :entry_file_id, name = :name, kind = :kind WHERE id = :id",
            (":directory_file_id", entry.directory_file_id),
            (":entry_file_id", entry.entry_file_id),
            (":name", entry.name.as_str()),
            (":kind", entry.kind),
            (":id", entry.id),
        )?;
        self.execute1(
            "UPDATE files SET version = version + 1 WHERE id = :directory_file_id",
            (":directory_file_id", entry.directory_file_id),
        )?;
        Ok(())
    }

    pub fn add_directory_entry(&self, entry: &DirectoryEntry) -> Result<i64, AnyError> {
        self.execute4(
            "INSERT INTO directory_entry (directory_file_id, entry_file_id, name, kind) \
            VALUES (:directory_file_id, :entry_file_id, :name, :kind)",
            (":directory_file_id", entry.directory_file_id),
            (":entry_file_id", entry.entry_file_id),
            (":name", entry.name.as_str()),
            (":kind", entry.kind),
        )?;
        let id = self.get_last_inserted_row_id()?;

        self.execute1(
            "UPDATE files SET version = version + 1 WHERE id = :id",
            (":id", entry.directory_file_id),
        )?;

        Ok(id)
    }

    pub fn get_last_inserted_row_id(&self) -> Result<i64, AnyError> {
        let mut stm2 = self.connection.prepare("SELECT last_insert_rowid()")?;
        stm2.next()?;
        let id: i64 = stm2.read::<i64, _>(0)?;
        Ok(id)
    }

    pub fn file_set_access_time(&self, id: i64, accessed_at: i64) -> Result<(), AnyError> {
        self.execute2(
            "UPDATE files SET accessed_at = :accessed_at WHERE id = :id",
            (":accessed_at", accessed_at),
            (":id", id),
        )?;
        Ok(())
    }

    pub fn get_tree(&self) -> Result<FsTreeRef, AnyError> {
        #[allow(clippy::unnecessary_cast)]
        let entries: Vec<DirectoryEntry> = self.get_rows(
            "SELECT * FROM directory_entry",
            NO_BINDINGS.as_ref(),
            |row| {
                Ok(DirectoryEntry {
                    id: row.read("id")?,
                    directory_file_id: row.read("directory_file_id")?,
                    entry_file_id: row.read("entry_file_id")?,
                    name: row.read("name")?,
                    kind: row.read("kind")?,
                })
            })?;

        // In memory index of directory entries
        let mut children: HashMap<i64, Vec<DirectoryEntry>> = HashMap::new();

        for e in entries {
            if e.name == "." || e.name == ".." {
                continue;
            }
            match children.get_mut(&e.directory_file_id) {
                Some(vec) => {
                    vec.push(e);
                }
                None => {
                    children.insert(e.directory_file_id, vec![e]);
                }
            }
        }

        let root: FsTree = self.get_file(ROOT_DIRECTORY_ID)?.unwrap().into();

        let mut by_id: HashMap<i64, Rc<RefCell<FsTree>>> = HashMap::new();
        let mut queue = vec![];

        queue.push(root.id);
        by_id.insert(root.id, Rc::new(RefCell::new(root)));

        while !queue.is_empty() {
            let node_id = queue.pop().unwrap();

            for c in children.get(&node_id).cloned().unwrap_or_else(|| vec![]) {
                queue.push(c.entry_file_id);
                let file = self.get_file(c.entry_file_id)?.unwrap();
                let new_node: FsTree = file.into();
                let new_node_id = new_node.id;
                let new_node = Rc::new(RefCell::new(new_node));

                by_id.insert(new_node_id, new_node.clone());

                {
                    let node = by_id.get_mut(&node_id).unwrap();
                    node.borrow_mut().children.push(new_node);
                }
            }
        }

        let root = by_id.remove(&ROOT_DIRECTORY_ID).unwrap();
        Ok(root)
    }

    pub fn nuke(&self) -> Result<(), AnyError> {
        self.execute0("DELETE FROM directory_entry")?;
        self.execute0("DELETE FROM files")?;
        self.execute0("DELETE FROM file_changes")?;
        self.execute0("DELETE FROM migrations")?;
        self.execute0("DELETE FROM persistent_settings")?;
        Ok(())
    }

    pub fn get_row<'l, 'q, T, M, R>(self: &'q MetadataDB, query: &str, bindings: T, mapper: M) -> Result<Option<R>, AnyError>
    where
        T: Bindable + Clone,
        M: FnOnce(&Statement<'l>) -> Result<R, AnyError>,
        'q: 'l,
    {
        let mut statement = self.connection.prepare(query)?;
        statement.bind(bindings)?;

        if let State::Row = statement.next()? {
            return mapper(&statement).map(Some);
        }

        Ok(None)
    }

    pub fn get_rows<'l, 'q, T, M, R>(self: &'q MetadataDB, query: &str, bindings: T, mapper: M) -> Result<Vec<R>, AnyError>
    where
        T: Bindable + Clone,
        M: Fn(&Statement<'l>) -> Result<R, AnyError>,
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

    pub fn execute0(&self, query: &str) -> Result<(), AnyError> {
        let mut statement = self.connection.prepare(query)?;
        statement.next()?;
        Ok(())
    }

    pub fn execute1<B0>(&self, query: &str, b0: B0) -> Result<(), AnyError>
    where
        B0: Bindable + Clone,
    {
        let mut statement = self.connection.prepare(query)?;
        statement.bind(b0)?;
        statement.next()?;
        Ok(())
    }

    pub fn execute2<B0, B1>(&self, query: &str, b0: B0, b1: B1) -> Result<(), AnyError>
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

    pub fn execute3<B0, B1, B2>(&self, query: &str, b0: B0, b1: B1, b2: B2) -> Result<(), AnyError>
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

    pub fn execute4<B0, B1, B2, B3>(&self, query: &str, b0: B0, b1: B1, b2: B2, b3: B3) -> Result<(), AnyError>
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

    pub fn execute5<B0, B1, B2, B3, B4>(&self, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4) -> Result<(), AnyError>
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

    pub fn execute6<B0, B1, B2, B3, B4, B5>(&self, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4, b5: B5) -> Result<(), AnyError>
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

    pub fn execute7<B0, B1, B2, B3, B4, B5, B6>(&self, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4, b5: B5, b6: B6) -> Result<(), AnyError>
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

    pub fn execute8<B0, B1, B2, B3, B4, B5, B6, B7>(&self, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4, b5: B5, b6: B6, b7: B7) -> Result<(), AnyError>
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

    pub fn execute9<B0, B1, B2, B3, B4, B5, B6, B7, B8>(&self, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4, b5: B5, b6: B6, b7: B7, b8: B8) -> Result<(), AnyError>
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

    pub fn execute10<B0, B1, B2, B3, B4, B5, B6, B7, B8, B9>(&self, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4, b5: B5, b6: B6, b7: B7, b8: B8, b9: B9) -> Result<(), AnyError>
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

    pub fn execute11<B0, B1, B2, B3, B4, B5, B6, B7, B8, B9, B10>(&self, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4, b5: B5, b6: B6, b7: B7, b8: B8, b9: B9, b10: B10) -> Result<(), AnyError>
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

    pub fn execute12<B0, B1, B2, B3, B4, B5, B6, B7, B8, B9, B10, B11>(&self, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4, b5: B5, b6: B6, b7: B7, b8: B8, b9: B9, b10: B10, b11: B11) -> Result<(), AnyError>
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

    pub fn execute13<B0, B1, B2, B3, B4, B5, B6, B7, B8, B9, B10, B11, B12>(&self, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4, b5: B5, b6: B6, b7: B7, b8: B8, b9: B9, b10: B10, b11: B11, b12: B12) -> Result<(), AnyError>
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
        B12: Bindable + Clone,
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
        statement.bind(b12)?;
        statement.next()?;
        Ok(())
    }

    pub fn execute14<B0, B1, B2, B3, B4, B5, B6, B7, B8, B9, B10, B11, B12, B13>(&self, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4, b5: B5, b6: B6, b7: B7, b8: B8, b9: B9, b10: B10, b11: B11, b12: B12, b13: B13) -> Result<(), AnyError>
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
        B12: Bindable + Clone,
        B13: Bindable + Clone,
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
        statement.bind(b12)?;
        statement.bind(b13)?;
        statement.next()?;
        Ok(())
    }

    pub fn execute15<B0, B1, B2, B3, B4, B5, B6, B7, B8, B9, B10, B11, B12, B13, B14>(&self, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4, b5: B5, b6: B6, b7: B7, b8: B8, b9: B9, b10: B10, b11: B11, b12: B12, b13: B13, b14: B14) -> Result<(), AnyError>
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
        B12: Bindable + Clone,
        B13: Bindable + Clone,
        B14: Bindable + Clone,
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
        statement.bind(b12)?;
        statement.bind(b13)?;
        statement.bind(b14)?;
        statement.next()?;
        Ok(())
    }

    pub fn execute16<B0, B1, B2, B3, B4, B5, B6, B7, B8, B9, B10, B11, B12, B13, B14, B15>(&self, query: &str, b0: B0, b1: B1, b2: B2, b3: B3, b4: B4, b5: B5, b6: B6, b7: B7, b8: B8, b9: B9, b10: B10, b11: B11, b12: B12, b13: B13, b14: B14, b15: B15) -> Result<(), AnyError>
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
        B12: Bindable + Clone,
        B13: Bindable + Clone,
        B14: Bindable + Clone,
        B15: Bindable + Clone,
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
        statement.bind(b12)?;
        statement.bind(b13)?;
        statement.bind(b14)?;
        statement.bind(b15)?;
        statement.next()?;
        Ok(())
    }

    pub fn transaction<R>(&self, func: impl FnOnce() -> Result<R, AnyError>) -> Result<R, AnyError> {
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

impl FileRow {
    pub fn hash(&self) -> String {
        let mut hash = hmac_sha512::Hash::new();
        hash.update(&self.id.to_string());
        hash.update(&self.kind.to_string());
        hash.update(&self.name);
        hash.update(&self.uid.to_string());
        hash.update(&self.gid.to_string());
        hash.update(&self.perms.to_string());
        hash.update(&self.size.to_string());
        hash.update(&self.sha512);
        hash.update(&self.encryption_key);
        hash.update(&self.created_at.to_string());
        hash.update(&self.updated_at.to_string());
        hex::encode(hash.finalize())
    }
}

impl FileChangeKind {
    pub fn to_i64(&self) -> i64 {
        match self {
            FileChangeKind::Created => 0,
            FileChangeKind::UpdatedMetadata => 1,
            FileChangeKind::UpdatedContents => 2,
            FileChangeKind::Deleted => 3,
        }
    }
}