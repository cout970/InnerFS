
INSERT OR IGNORE INTO files (id, kind, name, uid, gid, perms, size, sha512, encryption_key, compression, accessed_at, created_at, updated_at)
values (1, 1, '/', 1000, 1000, 493, 0, '', '', '', unixepoch("now"), unixepoch("now"), unixepoch("now"));

INSERT OR IGNORE INTO directory_entries (id, directory_file_id, entry_file_id, name, kind)
values (1, 1, 1, '.', 1), (2, 1, 1, '..', 1);
