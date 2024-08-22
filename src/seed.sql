-- sqlite initial configuration
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind INTEGER NOT NULL, -- 0: file, 1: directory
    name TEXT NOT NULL,
    uid INTEGER NOT NULL,
    gid INTEGER NOT NULL,
    perms INTEGER NOT NULL,
    size INTEGER NOT NULL,
    sha512 TEXT NOT NULL,
    accessed_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS directory_entry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    directory_file_id INTEGER NOT NULL,
    entry_file_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    kind INTEGER NOT NULL
);

INSERT OR IGNORE INTO files (id, kind, name, uid, gid, perms, size, sha512, accessed_at, created_at, updated_at)
values (1, 1, '/', 0, 0, 511, 0, '', 0, 0, 0);

INSERT OR IGNORE INTO directory_entry (id, directory_file_id, entry_file_id, name, kind)
values (1, 1, 1, '.', 1), (2, 1, 1, '..', 1);

-- Inline storage
CREATE TABLE IF NOT EXISTS sqlar(
  name TEXT PRIMARY KEY,  -- name of the file
  mode INT,               -- access permissions
  mtime INT,              -- last modification time
  sz INT,                 -- original file size
  data BLOB               -- compressed content
);