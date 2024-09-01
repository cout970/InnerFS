
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
    compression TEXT NOT NULL, -- '', 'gzip:1', 'gzip:9', etc.
    accessed_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);