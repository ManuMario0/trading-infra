use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::address::Address;
use crate::transport::{Client, Receiver, Sender, Server};

/// A TCP Connection that implements `Sender`, `Receiver`, and `Client`.
pub struct TcpConnection {
    stream: TcpStream,
}

impl TcpConnection {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }
}

impl Sender for TcpConnection {
    async fn send_bytes(&mut self, data: &[u8]) -> Result<(), io::Error> {
        // Frame the message: 4 bytes length + payload
        let len = data.len() as u32;
        self.stream.write_all(&len.to_be_bytes()).await?;
        self.stream.write_all(data).await?;
        Ok(())
    }
}

impl Receiver for TcpConnection {
    async fn recv_bytes(&mut self) -> Result<Vec<u8>, io::Error> {
        // Read 4 bytes length
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        // Read payload
        let mut data = vec![0u8; len];
        self.stream.read_exact(&mut data).await?;
        Ok(data)
    }

    async fn try_recv(&mut self) -> Result<Vec<u8>, io::Error> {
        // Attempt to read the length prefix non-blocking
        let mut len_buf = [0u8; 4];
        match self.stream.try_read(&mut len_buf) {
            Ok(0) => Err(io::Error::new(io::ErrorKind::ConnectionAborted, "Closed")),
            Ok(4) => {
                // If we got the length, we block to finish reading the frame to avoid state desync.
                let len = u32::from_be_bytes(len_buf) as usize;
                let mut data = vec![0u8; len];
                self.stream.read_exact(&mut data).await?;
                Ok(data)
            }
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Partial length prefix read across non-blocking boundary",
            )),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                Err(io::Error::new(io::ErrorKind::WouldBlock, "WouldBlock"))
            }
            Err(e) => Err(e),
        }
    }
}

impl Client for TcpConnection {
    async fn connect_to(address: Address) -> Result<Self, io::Error> {
        if let Address::TCP(addr) = address {
            let stream = TcpStream::connect(addr).await?;
            Ok(TcpConnection::new(stream))
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TcpConnection requires Address::TCP",
            ))
        }
    }
}

/// A TCP Server that listens for incoming connections.
pub struct TcpBackendServer {
    listener: TcpListener,
}

impl Server for TcpBackendServer {
    type Connection = TcpConnection;

    async fn listen_to(address: Address) -> Result<Self, io::Error> {
        if let Address::TCP(addr) = address {
            let listener = TcpListener::bind(addr).await?;
            Ok(Self { listener })
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TcpBackendServer requires Address::TCP",
            ))
        }
    }

    async fn accept(&mut self) -> Result<Self::Connection, io::Error> {
        let (stream, _) = self.listener.accept().await?;
        Ok(TcpConnection::new(stream))
    }
}
