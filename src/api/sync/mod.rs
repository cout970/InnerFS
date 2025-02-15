use crate::api::sync::message::{Message, MessageStream, NetworkError};
use crate::inner_file_system::SafeInnerFileSystem;
use crate::metadata_db::{
    DirectoryEntry, ExternalFileChange, FileRow, FILE_KIND_DIRECTORY, FILE_KIND_REGULAR, ROOT_DIRECTORY_ID,
};
use crate::utils::current_timestamp;
use crate::AnyError;
use flate2::Crc;
use libc::O_WRONLY;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashSet, VecDeque};
use tokio::net::TcpStream;
use tokio::time::sleep;

mod message;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileChangeExchange {
    pub id: i64,
    pub kind: i64,
    pub changed_at: i64,
    pub file_external_id: String,
    pub file_version: i64,
    pub file_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadataExchange {
    pub external_id: String,
    pub version: i64,
    pub kind: i64,
    pub name: String,
    pub uid: i64,
    pub gid: i64,
    pub perms: i64,
    pub size: i64,
    pub sha512: String,
    pub encryption_key: String,
    pub compression: String,
    pub accessed_at: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryEntryExchange {
    external_id: String,
    parent_external_id: String,
    child_external_id: String,
    child_name: String,
    child_kind: i64,
}

pub async fn sync_handle_s2c_connection(fs: SafeInnerFileSystem, socket: TcpStream) -> Result<(), AnyError> {
    let mut message_stream = MessageStream::new(socket);
    info!("Handling connection");

    loop {
        let message = match message_stream.read_message().await {
            Ok(x) => x,
            Err(NetworkError::ConnectionClosed) => break,
            Err(NetworkError::Other(e)) => {
                info!("Error reading message: {}", e);
                break;
            }
        };

        info!("Received: {:?}", message.method);
        match message.method.as_str() {
            "list_index" => {
                let tree = {
                    let db = &fs.lock().expect("Failed to access fs mutex").sql;
                    db.get_tree()?
                };
                let tree = serde_json::to_value(tree)?;

                message_stream
                    .send_message(Message::new("list_index_response", json!({ "tree": tree })))
                    .await?;
            }
            "list_changes" => {
                let since = message.params["since"].as_u64().unwrap_or(0);
                let limit = message.params["limit"].as_u64().unwrap_or(20);
                let changes = {
                    let db = &fs.lock().expect("Failed to access fs mutex").sql;
                    db.get_file_changes(since as i64, limit as i64)?
                };

                let changes: Vec<FileChangeExchange> = changes
                    .into_iter()
                    .map(|x| FileChangeExchange {
                        id: x.id,
                        kind: x.kind,
                        changed_at: x.changed_at,
                        file_external_id: x.file_external_id,
                        file_version: x.file_version,
                        file_hash: x.file_hash,
                    })
                    .collect();

                message_stream
                    .send_message(Message::new("list_changes_response", json!({ "changes": changes })))
                    .await?;
            }
            "get_file_metadata" => {
                let external_id = message.params["file_external_id"].as_str().unwrap_or("");
                let file = {
                    let db = &fs.lock().expect("Failed to access fs mutex").sql;
                    db.get_file_by_external_id(external_id)?
                };

                let file_metadata = match file {
                    Some(file) => serde_json::to_value(FileMetadataExchange {
                        external_id: file.external_id,
                        version: file.version,
                        kind: file.kind,
                        name: file.name,
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
                    })?,
                    None => Value::Null,
                };

                message_stream
                    .send_message(Message::new("get_file_metadata_response", json!({ "file_metadata": file_metadata })))
                    .await?;
            }
            "get_directory_entries" => {
                let external_id = message.params["file_external_id"].as_str().unwrap().to_string();
                let mut found = false;
                let mut entries: Vec<DirectoryEntryExchange> = vec![];

                {
                    let db = &fs.lock().expect("Failed to access fs mutex").sql;
                    let opt = db.get_file_id_by_external_id(&external_id)?;
                    if let Some(id) = opt {
                        found = true;
                        for entry in db.get_directory_entries(id)? {
                            let file_external_id = db.get_file_external_id(entry.entry_file_id)?;

                            if let Some(file_external_id) = file_external_id {
                                entries.push(DirectoryEntryExchange {
                                    external_id: entry.external_id,
                                    parent_external_id: external_id.to_string(),
                                    child_external_id: file_external_id,
                                    child_name: entry.name,
                                    child_kind: entry.kind,
                                });
                            }
                        }
                    }
                };

                message_stream
                    .send_message(Message::new(
                        "get_directory_entries_response",
                        json!({ "is_dir": found, "directory_entries": entries }),
                    ))
                    .await?;
            }
            "get_file_contents" => {
                let external_id = message.params["file_external_id"].as_str().unwrap().to_string();
                let mut found = false;
                let mut content: Vec<u8> = vec![];

                {
                    let mut guard = fs.lock().expect("Failed to access fs mutex");
                    let opt = guard.sql.get_file_id_by_external_id(&external_id)?;

                    if let Some(id) = opt {
                        content = match guard.read_all(id) {
                            Ok(bytes) => {
                                found = true;
                                bytes
                            }
                            Err(e) => {
                                warn!("Error reading file: {}", e);
                                vec![]
                            }
                        };
                    }
                }

                let mut crc = Crc::new();
                crc.update(&content);
                let crc: u32 = crc.sum();

                message_stream
                    .send_message(Message::new(
                        "get_file_contents_response",
                        json!({ "found": found, "content_length": content.len(), "crc": crc }),
                    ))
                    .await?;

                if found {
                    message_stream.write_bytes(&content).await?;
                }
            }
            "stop" => break,
            _ => info!("Unknown message: {:#?}", message),
        };
    }

    info!("Done handling connection");
    Ok(())
}

pub async fn sync_handle_c2s_connection(fs: SafeInnerFileSystem, socket: TcpStream) -> Result<(), AnyError> {
    let mut message_stream = MessageStream::new(socket);

    // Make a full sync request
    // TODO find an efficient way to do this if we are not starting from scratch
    // send_message("list_index", json!({}), &mut socket).await?;
    // let tree = match read_message(&mut socket).await {
    //     Ok((name, args)) if name == "list_index_response" => serde_json::from_value::<FsTreeRef>(args["tree"].clone())?,
    //     Ok(a) => return Err(anyhow!("Unexpected message: {:?}", a)),
    //     Err(ReadError::ConnectionClosed) => return Ok(()),
    //     Err(ReadError::Other(e)) => return Err(e),
    // };
    //
    // info!("Received tree: {:#?}", tree);
    // {
    //     let db = &fs.lock().expect("Failed to access fs mutex").sql;
    //     db.nuke()?;
    //     db.import_tree(tree)?;
    // }

    // Incremental sync
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

        process_changes(&fs, &mut message_stream).await?;

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

    message_stream.send_command("stop").await?;
    message_stream.close().await?;
    info!("Done handling connection");
    Ok(())
}

async fn process_changes(fs: &SafeInnerFileSystem, message_stream: &mut MessageStream) -> Result<(), AnyError> {
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
                    warn!("CRC mismatch, skipping: expected {}, got {}", orig_crc, calc_crc);
                    for change in &mut changes {
                        if change.file_external_id == external_id {
                            change.status = 0;
                            change.imported_at = 0;
                        }
                    }
                    continue;
                }

                {
                    let mut guard = fs.lock().expect("Failed to access fs mutex");
                    let id = guard.sql.get_file_id_by_external_id(&external_id)?;

                    if let Some(id) = id {
                        let mut file = guard.get_file_or_err(id)?;
                        let full_path = guard.sql.get_file_path(file.id)?;
                        let fh = guard.storage.open(&mut file, &full_path, O_WRONLY as u32)?;
                        guard.storage.write(fh, &mut file, 0, &bytes)?;
                        guard.storage.close(fh, &mut file)?;
                    } else {
                        warn!("Local file not found: {}", external_id);
                        for change in &mut changes {
                            if change.file_external_id == external_id {
                                change.status = 0;
                                change.imported_at = 0;
                            }
                        }
                    }
                }
            } else {
                warn!("File not found: {}", external_id);
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
