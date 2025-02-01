-- Files / inodes
CREATE TABLE IF NOT EXISTS files
(
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    version        INTEGER NOT NULL DEFAULT 1,
    kind           INTEGER NOT NULL, -- 0: file, 1: directory
    name           TEXT    NOT NULL,
    external_id    TEXT    NOT NULL DEFAULT (lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) ||
                                             '-4' ||
                                             substr(lower(hex(randomblob(2))), 2) || '-' ||
                                             substr('89ab', abs(random()) % 4 + 1, 1) ||
                                             substr(lower(hex(randomblob(2))), 2) || '-' ||
                                             lower(hex(randomblob(6)))),
    uid            INTEGER NOT NULL,
    gid            INTEGER NOT NULL,
    perms          INTEGER NOT NULL,
    size           INTEGER NOT NULL,
    sha512         TEXT    NOT NULL,
    encryption_key TEXT    NOT NULL,
    compression    TEXT    NOT NULL, -- '', 'gzip:1', 'gzip:9', etc.
    accessed_at    INTEGER NOT NULL,
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL
);