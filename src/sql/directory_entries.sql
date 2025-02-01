-- Directory entries
CREATE TABLE IF NOT EXISTS directory_entries
(
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    directory_file_id INTEGER NOT NULL,
    entry_file_id     INTEGER NOT NULL,
    name              TEXT    NOT NULL,
    kind              INTEGER NOT NULL,
    external_id       TEXT    NOT NULL DEFAULT (lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) ||
                                                '-4' ||
                                                substr(lower(hex(randomblob(2))), 2) || '-' ||
                                                substr('89ab', abs(random()) % 4 + 1, 1) ||
                                                substr(lower(hex(randomblob(2))), 2) || '-' ||
                                                lower(hex(randomblob(6))))
);
