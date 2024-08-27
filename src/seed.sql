-- sqlite initial configuration
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- Schema migrations
CREATE TABLE IF NOT EXISTS migrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

INSERT OR IGNORE INTO migrations (id, version, created_at)
values (1, '1.0.0', unixepoch("now"));

-- Persistent settings
CREATE TABLE IF NOT EXISTS persistent_settings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    setting_name TEXT NOT NULL UNIQUE,
    setting_value TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Files / inodes
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL DEFAULT 1,
    kind INTEGER NOT NULL, -- 0: file, 1: directory
    name TEXT NOT NULL,
    uid INTEGER NOT NULL,
    gid INTEGER NOT NULL,
    perms INTEGER NOT NULL,
    size INTEGER NOT NULL,
    sha512 TEXT NOT NULL,
    encryption_key TEXT NOT NULL,
    accessed_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

INSERT OR IGNORE INTO files (id, kind, name, uid, gid, perms, size, sha512, encryption_key, accessed_at, created_at, updated_at)
values (1, 1, '/', 1000, 1000, 493, 0, '', '', unixepoch("now"), unixepoch("now"), unixepoch("now"));

-- Directory entries
CREATE TABLE IF NOT EXISTS directory_entry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    directory_file_id INTEGER NOT NULL,
    entry_file_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    kind INTEGER NOT NULL
);

INSERT OR IGNORE INTO directory_entry (id, directory_file_id, entry_file_id, name, kind)
values (1, 1, 1, '.', 1), (2, 1, 1, '..', 1);

-- Change history
CREATE TABLE IF NOT EXISTS file_changes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL,
    file_version INTEGER NOT NULL,
    kind INTEGER NOT NULL, -- 0: created, 1: updated, 2: deleted
    file_sha512 TEXT NOT NULL,
    changed_at INTEGER NOT NULL
);

-- Internal sqlar storage
CREATE TABLE IF NOT EXISTS sqlar(
  name TEXT PRIMARY KEY,  -- name of the file
  mode INT,               -- access permissions
  mtime INT,              -- last modification time
  sz INT,                 -- original file size
  data BLOB               -- compressed content
);