-- External change history
CREATE TABLE IF NOT EXISTS external_file_changes
(
    id               INTEGER PRIMARY KEY,
    file_external_id TEXT    NOT NULL,
    file_version     INTEGER NOT NULL,
    kind             INTEGER NOT NULL, -- 0: directory, 1: regular
    file_hash        TEXT    NOT NULL,
    changed_at       INTEGER NOT NULL,
    status           INTEGER NOT NULL, -- 0 = pending, 1 = success, 2 = failed
    retries          INTEGER NOT NULL,
    imported_at      INTEGER NOT NULL
);