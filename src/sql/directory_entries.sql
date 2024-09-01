
-- Directory entries
CREATE TABLE IF NOT EXISTS directory_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    directory_file_id INTEGER NOT NULL,
    entry_file_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    kind INTEGER NOT NULL
);
