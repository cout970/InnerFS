use crate::api::sync::{sync_handle_c2s_connection, sync_handle_s2c_connection};
use crate::inner_file_system::InnerFileSystem;
use crate::{init_fs, AnyError};
use anyhow::anyhow;
use log::{error, info};
use std::sync::Arc;
use std::thread;
use tiny_http::Server;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Builder;
use webdav::*;

mod sync;
mod webdav;

pub fn start_webdav_server(fs: InnerFileSystem, addr: Option<String>) -> Result<(), AnyError> {
    let webdav_conf = fs.config.webdav.clone();
    let address = addr
        .as_ref()
        .map(|i| i.clone())
        .unwrap_or_else(|| format!("{}:{}", webdav_conf.address, webdav_conf.port));

    let server = Arc::new(Server::http(&address).map_err(|e| anyhow!("Unable to start server at {}: {}", address, e))?);
    let mut guards = Vec::with_capacity(4);
    info!("Server started on http://{}", address);

    let config = fs.config.clone();

    for _ in 0..webdav_conf.threads.max(1) {
        let server = server.clone();
        let config_copy = config.clone();

        let guard = thread::spawn(move || {
            let mut fs = init_fs(config_copy);
            loop {
                let request = server.recv().unwrap();
                match webdav_handle_request(request, &mut fs) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Server error: {}", e);
                    }
                }
            }
        });

        guards.push(guard);
    }

    for guard in guards {
        guard.join().unwrap();
    }

    Ok(())
}

pub fn start_sync_server(fs: InnerFileSystem, address: Option<String>) -> Result<(), AnyError> {
    let rt = Builder::new_current_thread().enable_time().enable_io().build()?;
    let address = address.unwrap_or_else(|| {
        format!("{}:{}", fs.config.sync.address, fs.config.sync.port)
    });

    let fs = fs.into_safe();

    let s: Result<(), AnyError> = rt.block_on(async {
        let tcp = TcpListener::bind(address).await?;

        info!("Listening on: {}", tcp.local_addr()?);

        loop {
            let (socket, remote_addr) = tcp.accept().await?;
            info!("Accepted connection from: {}", remote_addr);
            let fs = fs.clone();

            tokio::spawn(async move {
                sync_handle_s2c_connection(fs, socket)
                    .await
                    .expect("[server] Error handling connection");
            });
        }
    });
    s?;

    Ok(())
}

pub fn connect_to_sync_server(fs: InnerFileSystem, address: Option<String>) -> Result<(), AnyError> {
    if !fs.config.readonly {
        error!("Refusing to sync with server without the readonly option enabled");
        info!("If you enable sync with a server, every local change will be overwritten by the server state");
        return Ok(());
    }

    let address = address.unwrap_or_else(|| {
        format!("{}:{}", fs.config.sync.address, fs.config.sync.port)
    });

    let rt = Builder::new_current_thread().enable_time().enable_io().build()?;
    let fs = fs.into_safe();

    let s: Result<(), AnyError> = rt.block_on(async {
        let socket = TcpStream::connect(&address).await?;
        info!("Connected to: {}", address);

        sync_handle_c2s_connection(fs, socket)
            .await
            .expect("[client] Error handling connection");

        Ok(())
    });
    s?;

    Ok(())
}
