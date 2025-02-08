use crate::inner_file_system::InnerFileSystem;
use crate::{init_fs, AnyError};
use anyhow::anyhow;
use log::{error, info};
use std::sync::Arc;
use std::thread;
use tiny_http::Server;
use webdav::*;

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
                match handle_request(request, &mut fs) {
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
