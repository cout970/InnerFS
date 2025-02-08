use crate::sql_fs::SqlFileSystem;
use crate::{init_fs, AnyError};
use anyhow::anyhow;
use log::{error, info};
use std::sync::Arc;
use std::thread;
use tiny_http::Server;
use webdav::*;

mod webdav;

pub fn start_webdav_server(fs: SqlFileSystem, addr: Option<String>) -> Result<(), AnyError> {
    let address = addr.as_ref().map(|i| i.as_str()).unwrap_or("127.0.0.1:8080");

    let server = Arc::new(Server::http(address).map_err(|e| anyhow!("Unable to start server: {}", e))?);
    let mut guards = Vec::with_capacity(4);
    info!("Server started on http://{}", address);

    let config = fs.config.clone();

    for _ in 0..4 {
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
