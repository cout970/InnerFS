use crate::api::sync::client::{fs_full_sync, fs_incremental_sync};
use crate::api::sync::message::{MessageStream, NetworkError};
use crate::api::sync::server::server_process_message;
use crate::inner_file_system::SafeInnerFileSystem;
use crate::AnyError;
use libc::{SIGINT};
use log::{info};
use serde::{Deserialize, Serialize};
use signal_hook::iterator::Signals;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use tokio::net::TcpStream;

mod client;
mod message;
mod server;

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

    let mut signals = Signals::new([SIGINT])?;
    let keep = Arc::new(AtomicBool::new(true));
    let keep2 = keep.clone();

    thread::spawn(move || {
        // Wait for a SIGINT signal to stop the server
        for _ in signals.forever() {
            info!("Received SIGINT, trying to stop the server");
            keep2.store(false, std::sync::atomic::Ordering::SeqCst);
            // Finish this thread
            break;
        }
    });

    while keep.load(std::sync::atomic::Ordering::SeqCst) {
        let message = match message_stream.read_message().await {
            Ok(x) => x,
            Err(NetworkError::ConnectionClosed) => break,
            Err(NetworkError::Other(e)) => {
                info!("Error reading message: {}", e);
                break;
            }
        };

        info!("Received: {:?}", message.method);
        server_process_message(&fs, &mut message_stream, message).await?;
    }

    info!("Done handling connection");
    Ok(())
}

pub async fn sync_handle_c2s_connection(fs: SafeInnerFileSystem, socket: TcpStream) -> Result<(), AnyError> {
    let mut message_stream = MessageStream::new(socket);

    info!("Stating full sync");
    fs_full_sync(&fs, &mut message_stream).await?;

    info!("Stating incremental sync");
    fs_incremental_sync(&fs, &mut message_stream).await?;

    message_stream.send_command("stop").await?;
    message_stream.close().await?;
    info!("Done handling connection");
    Ok(())
}
