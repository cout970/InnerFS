
-- Persistent settings
CREATE TABLE IF NOT EXISTS persistent_settings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    setting_name TEXT NOT NULL UNIQUE,
    setting_value TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);
