use crate::AnyError;
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const DEBUG_LOG: bool = false;

type JsonValue = serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub jsonrpc: String,
    pub method: String,
    pub params: JsonValue,
}

pub struct MessageStream {
    socket: TcpStream,
    buffer: VecDeque<u8>,
}

#[derive(Debug)]
pub enum NetworkError {
    ConnectionClosed,
    Other(AnyError),
}

impl Display for NetworkError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NetworkError::ConnectionClosed => write!(f, "Connection closed"),
            NetworkError::Other(e) => write!(f, "Network error: {}", e),
        }
    }
}

impl std::error::Error for NetworkError {}

impl Message {
    pub fn new(method: &str, params: JsonValue) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            method: method.to_string(),
            params
        }
    }
}

impl MessageStream {
    pub fn new(socket: TcpStream) -> Self {
        Self {
            socket,
            buffer: VecDeque::new(),
        }
    }

    pub async fn wait_for_message(&mut self, name: &str) -> Result<Message, NetworkError> {
        loop {
            if DEBUG_LOG { debug!("Waiting for message: {}", name); }
            let msg = self.read_message().await?;
            if msg.method == name {
                return Ok(msg);
            }
            warn!("Unexpected message, ignoring: {:?}", msg);
        }
    }

    pub async fn read_message(&mut self) -> Result<Message, NetworkError> {
        if DEBUG_LOG { debug!("Reading message"); }
        let line = self.read_line().await?;
        let message: Message = serde_json::from_str(&line)?;
        Ok(message)
    }

    pub async fn send_command(&mut self, cmd: &str) -> Result<(), NetworkError> {
        self.send_message(Message::new(cmd, JsonValue::Null)).await
    }

    pub async fn send_message(&mut self, message: Message) -> Result<(), NetworkError> {
        if DEBUG_LOG { debug!("Sending: {:?}", message); }
        let json_str = serde_json::to_string(&message)?;
        self.socket.write_all(json_str.as_bytes()).await?;
        self.socket.write_all("\n".as_bytes()).await?;
        self.socket.flush().await?;
        Ok(())
    }

    pub async fn send_query_message(&mut self, cmd: &str, args: JsonValue) -> Result<JsonValue, NetworkError> {
        self.send_message(Message::new(cmd, args)).await?;

        let resp = self.wait_for_message(&format!("{}_response", cmd)).await?;
        Ok(resp.params)
    }

    pub async fn read_bytes(&mut self, size: u64) -> Result<Vec<u8>, NetworkError> {
        loop {
            if self.buffer.len() >= size as usize {
                let data = self.buffer.drain(..size as usize).collect();
                return Ok(data);
            }

            self.read_internal().await?;
        }
    }

    pub async fn write_bytes(&mut self, data: &[u8]) -> Result<(), NetworkError> {
        self.socket.write_all(data).await?;
        self.socket.write_all("\n".as_bytes()).await?;
        self.socket.flush().await?;
        Ok(())
    }

    async fn read_line(&mut self) -> Result<String, NetworkError> {
        loop {
            let pos = self.buffer.iter().position(|b| *b == b'\n');

            if let Some(pos) = pos {
                // Skip empty lines
                if pos == 0 {
                    self.buffer.pop_front();
                    continue;
                }
                let line = String::from_utf8_lossy(&self.buffer.make_contiguous()[..pos]).to_string();
                self.buffer.drain(..pos + 1);
                return Ok(line);
            }

            self.read_internal().await?;
        }
    }

    async fn read_internal(&mut self) -> Result<(), NetworkError> {
        loop {
            let mut tmp_buffer = [0; 1024 * 16];

            // Wait for the socket to be readable/have something to read
            self.socket.readable().await?;

            let read = match self.socket.read(&mut tmp_buffer).await {
                Ok(read) => read,
                Err(e) => {
                    if DEBUG_LOG { debug!("Error reading: {:?}", e); }
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        continue;
                    }
                    if e.kind() == std::io::ErrorKind::ConnectionReset
                        || e.kind() == std::io::ErrorKind::ConnectionAborted
                        || e.kind() == std::io::ErrorKind::BrokenPipe
                        || e.kind() == std::io::ErrorKind::NotConnected
                    {
                        return Err(NetworkError::ConnectionClosed);
                    }
                    return Err(e.into());
                }
            };

            if DEBUG_LOG { debug!("Read {} bytes", read); }
            if read == 0 {
                return Err(NetworkError::ConnectionClosed);
            }
            self.buffer.extend(&tmp_buffer[..read]);
            return Ok(());
        }
    }

    pub async fn close(mut self) -> Result<(), NetworkError> {
        self.socket.shutdown().await?;
        while self.socket.read(&mut [0; 1024]).await? > 0 {}
        Ok(())
    }
}

impl From<AnyError> for NetworkError {
    fn from(e: AnyError) -> Self {
        NetworkError::Other(e.into())
    }
}

impl From<serde_json::Error> for NetworkError {
    fn from(e: serde_json::Error) -> Self {
        NetworkError::Other(e.into())
    }
}

impl From<std::io::Error> for NetworkError {
    fn from(e: std::io::Error) -> Self {
        NetworkError::Other(e.into())
    }
}
