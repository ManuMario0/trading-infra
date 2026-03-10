//! TCP backend — internal implementation detail for the socket layer.

use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::transport::{Receiver, Sender};

/// A single TCP connection with length-prefixed framing.
pub(crate) struct TcpConnection {
    stream: TcpStream,
}

impl TcpConnection {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }
}

impl Sender for TcpConnection {
    async fn send_bytes(&mut self, data: &[u8]) -> Result<(), io::Error> {
        let len = data.len() as u32;
        self.stream.write_all(&len.to_be_bytes()).await?;
        self.stream.write_all(data).await?;
        self.stream.flush().await?;
        Ok(())
    }
}

impl Receiver for TcpConnection {
    async fn recv_bytes(&mut self) -> Result<Vec<u8>, io::Error> {
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut data = vec![0u8; len];
        self.stream.read_exact(&mut data).await?;
        Ok(data)
    }
}
