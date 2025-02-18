use std::collections::HashMap;
use crate::api::sync::message::{Message, MessageStream};
use crate::api::sync::{DirectoryEntryExchange, FileChangeExchange, FileMetadataExchange};
use crate::inner_file_system::SafeInnerFileSystem;
use crate::AnyError;
use flate2::Crc;
use log::{info, warn};
use serde_json::{json, Value};

pub async fn server_process_message(
    fs: &SafeInnerFileSystem,
    message_stream: &mut MessageStream,
    message: Message,
) -> Result<(), AnyError> {
    info!("Received: {:?}", message.method);
    match message.method.as_str() {
        "get_full_index" => {
            let mut files: Vec<FileMetadataExchange> = vec![];
            let mut directory_entries: Vec<DirectoryEntryExchange> = vec![];
            let last_change: Option<FileChangeExchange>;

            {
                let db = &fs.lock().expect("Failed to access fs mutex").sql;
                let mut id_map = HashMap::new();

                // Last change to start tracking changes incrementally after the full sync
                last_change = db.get_last_file_change()?.map(|i| FileChangeExchange {
                    id: i.id,
                    kind: i.kind,
                    changed_at: i.changed_at,
                    file_external_id: i.file_external_id,
                    file_version: i.file_version,
                    file_hash: i.file_hash,
                });

                // All files
                db.get_all_files()?.into_iter().for_each(|f| {
                    id_map.insert(f.id, f.external_id.clone());

                    files.push(FileMetadataExchange {
                        external_id: f.external_id,
                        version: f.version,
                        kind: f.kind,
                        name: f.name,
                        uid: f.uid,
                        gid: f.gid,
                        perms: f.perms,
                        size: f.size,
                        sha512: f.sha512,
                        encryption_key: f.encryption_key,
                        compression: f.compression,
                        accessed_at: f.accessed_at,
                        created_at: f.created_at,
                        updated_at: f.updated_at,
                    });
                });

                // All directory entries
                db.get_all_directory_entries()?.into_iter().for_each(|d| {
                    // If the ids are not found, the files where deleted, skip this entry
                    let Some(parent_external_id) = id_map.get(&d.directory_file_id) else { return };
                    let Some(child_external_id) = id_map.get(&d.entry_file_id) else { return };

                    directory_entries.push(DirectoryEntryExchange {
                        external_id: d.external_id,
                        parent_external_id: parent_external_id.clone(),
                        child_external_id: child_external_id.clone(),
                        child_name: d.name,
                        child_kind: d.kind,
                    });
                });
            };

            message_stream
                .send_message(Message::new("get_full_index_response", json!({
                    "files": files,
                    "directory_entries": directory_entries,
                    "last_change": last_change,
                })))
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
        "stop" => {}
        _ => info!("Unknown message: {:#?}", message),
    }
    Ok(())
}
