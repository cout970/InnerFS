use crate::api::sync::message::MessageStream;
use crate::api::sync::{DirectoryEntryExchange, FileChangeExchange, FileMetadataExchange};
use crate::inner_file_system::SafeInnerFileSystem;
use crate::metadata_db::{
    DirectoryEntry, ExternalFileChange, FileRow, FILE_KIND_DIRECTORY, FILE_KIND_REGULAR, ROOT_DIRECTORY_ID,
};
use crate::utils::current_timestamp;
use crate::AnyError;
use flate2::Crc;
use libc::O_WRONLY;
use log::{info, warn};
use serde_json::json;
use std::collections::HashSet;
use anyhow::anyhow;
use tokio::time::sleep;

pub async fn fs_full_sync(fs: &SafeInnerFileSystem, message_stream: &mut MessageStream) -> Result<(), AnyError> {
    let sync_contents = { !fs.lock().expect("Failed to access fs mutex").config.sync.metadata_only };

    // Make a full sync request
    let resp = message_stream.send_query_message("get_full_index", json!({})).await?;

    let files: Vec<FileMetadataExchange> = serde_json::from_value(resp["files"].clone())?;
    let directory_entries: Vec<DirectoryEntryExchange> = serde_json::from_value(resp["directory_entries"].clone())?;
    let changes: Option<FileChangeExchange> = serde_json::from_value(resp["last_change"].clone())?;

    let mut contents_to_sync: HashSet<String> = HashSet::new();

    {
        let db = &fs.lock().expect("Failed to access fs mutex").sql;
        let mut file_map = std::collections::HashMap::new();
        let mut id_map2 = std::collections::HashMap::new();
        let mut starting_file_ids = HashSet::new();

        db.get_all_files()?.into_iter().for_each(|f| {
            starting_file_ids.insert(f.id);
            file_map.insert(f.external_id.clone(), f);
        });

        // All files
        for file in files {
            // Update or create a file
            match file_map.get(&file.external_id) {
                Some(prev_file) => {
                    starting_file_ids.remove(&prev_file.id);
                    if sync_contents && file.kind == FILE_KIND_REGULAR && prev_file.sha512 != file.sha512 {
                        contents_to_sync.insert(file.external_id.clone());
                    }
                    db.update_external_file(&FileRow {
                        id: prev_file.id,
                        version: file.version,
                        kind: file.kind,
                        name: file.name,
                        external_id: file.external_id.clone(),
                        uid: file.uid,
                        gid: file.gid,
                        perms: file.perms,
                        size: file.size,
                        sha512: file.sha512,
                        encryption_key: file.encryption_key,
                        compression: file.compression,
                        accessed_at: file.accessed_at,
                        created_at: file.created_at,
                        updated_at: file.updated_at,
                    })?;
                }
                None => {
                    let mut new_file = FileRow {
                        id: 0,
                        version: file.version,
                        kind: file.kind,
                        name: file.name,
                        external_id: file.external_id.clone(),
                        uid: file.uid,
                        gid: file.gid,
                        perms: file.perms,
                        size: file.size,
                        sha512: file.sha512,
                        encryption_key: file.encryption_key,
                        compression: file.compression,
                        accessed_at: file.accessed_at,
                        created_at: file.created_at,
                        updated_at: file.updated_at,
                    };
                    let file_id = db.add_external_file(&new_file)?;
                    new_file.id = file_id;

                    file_map.insert(file.external_id.clone(), new_file);
                }
            }
        }

        // Remove no longer existing files
        for id in starting_file_ids {
            db.remove_file(id)?;
        }

        let mut starting_entry_ids = HashSet::new();
        db.get_all_directory_entries()?.into_iter().for_each(|d| {
            starting_entry_ids.insert(d.id);
            id_map2.insert(d.external_id.clone(), d.id);
        });

        // All directory entries
        for entry in directory_entries {
            let parent_id = file_map.get(&entry.parent_external_id).expect("Parent not found").id;
            let child_id = file_map.get(&entry.child_external_id).expect("Child not found").id;

            match id_map2.get(&entry.external_id) {
                Some(id) => {
                    starting_entry_ids.remove(id);
                    db.update_directory_entry(&DirectoryEntry {
                        id: *id,
                        external_id: entry.external_id.clone(),
                        directory_file_id: parent_id,
                        entry_file_id: child_id,
                        name: entry.child_name.clone(),
                        kind: entry.child_kind,
                    })?
                }
                None => {
                    db.add_external_directory_entry(&DirectoryEntry {
                        id: 0,
                        external_id: entry.external_id.clone(),
                        directory_file_id: parent_id,
                        entry_file_id: child_id,
                        name: entry.child_name.clone(),
                        kind: entry.child_kind,
                    })?;
                }
            }
        }

        for id in starting_entry_ids {
            db.remove_directory_entry(id)?;
        }

        // Last change to start tracking changes incrementally after the full sync
        if let Some(change) = changes {
            let prev_id = db.get_last_external_change_id()?;
            if prev_id < change.id {
                db.add_external_file_change(ExternalFileChange {
                    id: change.id,
                    kind: change.kind,
                    changed_at: change.changed_at,
                    file_external_id: change.file_external_id.clone(),
                    file_version: change.file_version,
                    file_hash: change.file_hash.clone(),
                    status: 0,
                    retries: 0,
                    imported_at: 0,
                })?;
            }
        }
    }

    for external_id in contents_to_sync {
        if let Err(e) = sync_file_contents(fs, message_stream, &external_id).await {
            warn!("Error syncing file contents: {}", e);
        }
    }

    Ok(())
}

// Incremental sync replaying changes from the server
pub async fn fs_incremental_sync(fs: &SafeInnerFileSystem, message_stream: &mut MessageStream) -> Result<(), AnyError> {
    let mut last_change = {
        fs.lock()
            .expect("Failed to access fs mutex")
            .sql
            .get_last_external_change_id()?
    };
    loop {
        let resp = message_stream
            .send_query_message("list_changes", json!({"since": last_change, "limit": 10}))
            .await?;

        let changes: Vec<FileChangeExchange> = serde_json::from_value(resp["changes"].clone())?;

        // Agregate ids of files with changes to avoid updating the same file multiple times
        for change in &changes {
            last_change = last_change.max(change.id);
            {
                let db = &fs.lock().expect("Failed to access fs mutex").sql;
                db.add_external_file_change(ExternalFileChange {
                    id: change.id,
                    kind: change.kind,
                    changed_at: change.changed_at,
                    file_external_id: change.file_external_id.clone(),
                    file_version: change.file_version,
                    file_hash: change.file_hash.clone(),
                    status: 0,
                    retries: 0,
                    imported_at: 0,
                })?
            }
        }

        process_pending_external_changes(&fs, message_stream).await?;

        if changes.is_empty() {
            // Wait a substantial amount of time before retrying
            sleep(std::time::Duration::from_millis(1000)).await;
            continue;
        }

        // Wait until next sync iteration, to prevent flooding the server
        sleep(std::time::Duration::from_millis(500)).await;

        if false {
            break;
        }
    }
    Ok(())
}

async fn process_pending_external_changes(fs: &SafeInnerFileSystem, message_stream: &mut MessageStream) -> Result<(), AnyError> {
    let sync_contents = { !fs.lock().expect("Failed to access fs mutex").config.sync.metadata_only };

    let mut file_to_sync: HashSet<String> = HashSet::new();
    let mut dirs_to_sync: HashSet<String> = HashSet::new();
    let mut contents_to_sync: HashSet<String> = HashSet::new();

    let mut changes = {
        let db = &fs.lock().expect("Failed to access fs mutex").sql;
        db.get_pending_external_changes(200)?
    };

    if changes.is_empty() {
        return Ok(());
    }

    // Group changes into files
    for change in &mut changes {
        // Ignore changes after 5 failed attempts
        if change.retries > 5 {
            change.status = 2;
            continue;
        }
        change.retries += 1;
        change.status = 1;
        change.imported_at = current_timestamp();
        file_to_sync.insert(change.file_external_id.clone());
    }

    // Sync metadata
    for external_id in file_to_sync {
        let resp = message_stream
            .send_query_message("get_file_metadata", json!({ "file_external_id": &external_id }))
            .await?;

        if resp["file_metadata"].is_null() {
            info!("File deleted: {}", external_id);
            {
                let mut guard = fs.lock().expect("Failed to access fs mutex");
                let opt = guard.sql.get_file_by_external_id(&external_id)?;
                if let Some(file) = opt {
                    guard.remove_file(file.id)?;
                }
            }
        } else {
            let remote_file: FileMetadataExchange = serde_json::from_value(resp["file_metadata"].clone())?;

            if remote_file.kind == FILE_KIND_DIRECTORY {
                dirs_to_sync.insert(remote_file.external_id.clone());
            }

            {
                let db = &fs.lock().expect("Failed to access fs mutex").sql;
                let opt = {
                    // Exception for root directory
                    if remote_file.name == "/" {
                        db.get_file(ROOT_DIRECTORY_ID)?
                    } else {
                        db.get_file_by_external_id(&remote_file.external_id)?
                    }
                };

                if let Some(mut existing_file) = opt {
                    existing_file.version = remote_file.version;
                    existing_file.kind = remote_file.kind;
                    existing_file.name = remote_file.name;
                    existing_file.external_id = remote_file.external_id.clone();
                    existing_file.uid = remote_file.uid;
                    existing_file.gid = remote_file.gid;
                    existing_file.perms = remote_file.perms;
                    existing_file.size = remote_file.size;
                    existing_file.sha512 = remote_file.sha512;
                    existing_file.encryption_key = remote_file.encryption_key;
                    existing_file.compression = remote_file.compression;
                    existing_file.accessed_at = remote_file.accessed_at;
                    existing_file.created_at = remote_file.created_at;
                    existing_file.updated_at = remote_file.updated_at;

                    if existing_file.kind == FILE_KIND_REGULAR {
                        contents_to_sync.insert(remote_file.external_id);
                    }
                    db.update_external_file(&existing_file)?;
                } else {
                    // File does not exist, create it
                    db.add_external_file(&FileRow {
                        id: 0,
                        version: remote_file.version,
                        kind: remote_file.kind,
                        name: remote_file.name,
                        external_id: remote_file.external_id.clone(),
                        uid: remote_file.uid,
                        gid: remote_file.gid,
                        perms: remote_file.perms,
                        size: remote_file.size,
                        sha512: remote_file.sha512,
                        encryption_key: remote_file.encryption_key,
                        compression: remote_file.compression,
                        accessed_at: remote_file.accessed_at,
                        created_at: remote_file.created_at,
                        updated_at: remote_file.updated_at,
                    })?;
                    contents_to_sync.insert(remote_file.external_id);
                }
            }
        }
    }

    // Sync directories
    for external_id in dirs_to_sync {
        let resp = message_stream
            .send_query_message("get_directory_entries", json!({ "file_external_id": external_id }))
            .await?;

        let entries: Vec<DirectoryEntryExchange> = serde_json::from_value(resp["directory_entries"].clone())?;

        {
            let db = &fs.lock().expect("Failed to access fs mutex").sql;
            let opt = db.get_file_id_by_external_id(&external_id)?;

            if let Some(id) = opt {
                let mut existing_entries = db.get_directory_entries(id)?;
                let mut remote_entries: HashSet<String> = HashSet::new();
                let mut retry = false;

                // Update or create entries
                for entry in &entries {
                    remote_entries.insert(entry.external_id.clone());
                    let child_id = db.get_file_id_by_external_id(&entry.child_external_id)?;

                    let Some(child_id) = child_id else {
                        warn!("Child file not found: {}", entry.child_external_id);
                        retry = true;
                        continue;
                    };

                    let existing_entry = existing_entries.iter().find(|x| x.external_id == entry.external_id);

                    if let Some(existing_entry) = existing_entry {
                        if existing_entry.entry_file_id != child_id {
                            // Update entry
                            db.update_external_directory_entry(&DirectoryEntry {
                                id: existing_entry.id,
                                external_id: entry.external_id.clone(),
                                directory_file_id: id,
                                entry_file_id: child_id,
                                name: entry.child_name.clone(),
                                kind: entry.child_kind,
                            })?;
                        }
                    } else {
                        // Create entry
                        db.add_external_directory_entry(&DirectoryEntry {
                            id: 0,
                            external_id: entry.external_id.clone(),
                            directory_file_id: id,
                            entry_file_id: child_id,
                            name: entry.child_name.clone(),
                            kind: entry.child_kind,
                        })?;
                    }
                }

                if retry {
                    for change in &mut changes {
                        if change.file_external_id == external_id {
                            change.status = 0;
                            change.imported_at = 0;
                        }
                    }
                }

                // Remove entries that are not in the remote list
                existing_entries.retain(|x| !remote_entries.contains(&x.external_id));
                for entry in existing_entries {
                    db.remove_directory_entry(entry.id)?;
                }
            } else {
                warn!("Directory not found: {}", external_id);
                for change in &mut changes {
                    if change.file_external_id == external_id {
                        change.status = 0;
                        change.imported_at = 0;
                    }
                }
            }
        }
    }

    // Sync file contents
    if !contents_to_sync.is_empty() && sync_contents {
        for external_id in contents_to_sync {
            if let Err(e) = sync_file_contents(fs, message_stream, &external_id).await {
                warn!("Error syncing file contents: {}", e);
                for change in &mut changes {
                    if change.file_external_id == external_id {
                        change.status = 0;
                        change.imported_at = 0;
                    }
                }
            }
        }
    }

    {
        let db = &fs.lock().expect("Failed to access fs mutex").sql;
        for change in changes {
            db.update_external_file_change(change)?;
        }
    }

    Ok(())
}

async fn sync_file_contents(fs: &SafeInnerFileSystem, message_stream: &mut MessageStream, external_id: &str) -> Result<(), AnyError> {
    let resp = message_stream
        .send_query_message("get_file_contents", json!({ "file_external_id": &external_id }))
        .await?;

    let found = resp["found"].as_bool().expect("Invalid param 'found'");

    if found {
        let size = resp["content_length"].as_u64().expect("Invalid content length field");
        let orig_crc: u32 = resp["crc"].as_u64().expect("Invalid crc field") as u32;
        let bytes = message_stream.read_bytes(size).await?;

        let mut crc = Crc::new();
        crc.update(&bytes);
        let calc_crc: u32 = crc.sum();

        if calc_crc != orig_crc {
            return Err(anyhow!("CRC mismatch: expected {}, got {}", orig_crc, calc_crc));
        }

        {
            let mut guard = fs.lock().expect("Failed to access fs mutex");
            let id = guard.sql.get_file_id_by_external_id(&external_id)?;

            if let Some(id) = id {
                let mut file = guard.get_file_or_err(id)?;
                let fh = guard.storage.open(&mut file, O_WRONLY as u32)?;
                guard.storage.write(fh, &mut file, 0, &bytes)?;
                guard.storage.close(fh, &mut file)?;
            } else {
                return Err(anyhow!("Local file not found: {}", external_id));
            }
        }
    } else {
        warn!("File not found: {}", external_id);
    }
    Ok(())
}