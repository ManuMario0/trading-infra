//! Typed sockets with ZMQ-like interface.
//!
//! - `PullSocket::bind(address)` — listens on an address, accepts all incoming
//!   connections internally, and multiplexes their messages into a single `receive()`.
//! - `PushSocket::connect(address)` — connects to a remote address and sends
//!   typed messages through `send()`.

use std::io;
use std::marker::PhantomData;

use serde::{Serialize, de::DeserializeOwned};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::backends::tcp::TcpConnection;
use crate::transport::{Receiver, Sender};

/// A socket that collects (receives) typed data from all connected pushers.
///
/// Internally manages a TCP listener, accepts connections, and multiplexes
/// all incoming messages into a single channel.
pub struct PullSocket<C> {
    rx: mpsc::Receiver<C>,
    _phantom: PhantomData<C>,
}

impl<C> PullSocket<C>
where
    C: DeserializeOwned + Send + 'static,
{
    /// Bind to an address and start accepting connections in the background.
    ///
    /// All incoming messages from any connected pusher are funneled into
    /// a single internal channel that `receive()` reads from.
    pub async fn bind(address: crate::address::Address) -> Result<Self, io::Error> {
        let addr_str = match &address {
            crate::address::Address::TCP(s) => s.clone(),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "PullSocket::bind requires Address::TCP",
                ));
            }
        };

        let listener = TcpListener::bind(&addr_str).await?;
        let (tx, rx) = mpsc::channel::<C>(10_000);

        // Background task: accept loop
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let tx = tx.clone();
                        // Per-connection reader task
                        tokio::spawn(async move {
                            let mut conn = TcpConnection::new(stream);
                            loop {
                                match conn.recv_bytes().await {
                                    Ok(bytes) => {
                                        match postcard::from_bytes::<C>(&bytes) {
                                            Ok(msg) => {
                                                if tx.send(msg).await.is_err() {
                                                    // Channel closed, stop reading
                                                    break;
                                                }
                                            }
                                            Err(e) => {
                                                eprintln!("Deserialization error: {}", e);
                                            }
                                        }
                                    }
                                    Err(_) => {
                                        // Connection closed or error
                                        break;
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => {
                        eprintln!("Accept error: {}", e);
                    }
                }
            }
        });

        Ok(Self {
            rx,
            _phantom: PhantomData,
        })
    }

    /// Receive the next typed message from any connected pusher.
    pub async fn receive(&mut self) -> Result<C, io::Error> {
        self.rx.recv().await.ok_or_else(|| {
            io::Error::new(io::ErrorKind::ConnectionAborted, "All pushers disconnected")
        })
    }
}

/// A socket that pushes (sends) typed data to a remote puller.
pub struct PushSocket<C> {
    conn: TcpConnection,
    _phantom: PhantomData<C>,
}

impl<C> PushSocket<C>
where
    C: Serialize,
{
    /// Connect to a remote PullSocket at the given address.
    pub async fn connect(address: crate::address::Address) -> Result<Self, io::Error> {
        let addr_str = match &address {
            crate::address::Address::TCP(s) => s.clone(),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "PushSocket::connect requires Address::TCP",
                ));
            }
        };

        let stream = TcpStream::connect(&addr_str).await?;
        let conn = TcpConnection::new(stream);

        Ok(Self {
            conn,
            _phantom: PhantomData,
        })
    }

    /// Serialize and send a typed payload.
    pub async fn send(&mut self, payload: &C) -> Result<(), io::Error> {
        let bytes = postcard::to_allocvec(payload)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.conn.send_bytes(&bytes).await
    }
}
