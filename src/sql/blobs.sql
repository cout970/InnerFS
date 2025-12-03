-- Blobs
CREATE TABLE IF NOT EXISTS blobs
(
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    hash TEXT    NOT NULL,
    size INTEGER NOT NULL,
    uses INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS by_hash ON blobs (hash);

CREATE TABLE IF NOT EXISTS file_blobs
(
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL,
    offset  INTEGER NOT NULL,
    blob_id TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS by_file_id_and_offset ON file_blobs (file_id, offset);