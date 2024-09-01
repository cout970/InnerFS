
-- Change history
CREATE TABLE IF NOT EXISTS file_changes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL,
    file_version INTEGER NOT NULL,
    kind INTEGER NOT NULL, -- 0: created, 1: updated, 2: deleted
    file_hash TEXT NOT NULL,
    changed_at INTEGER NOT NULL
);