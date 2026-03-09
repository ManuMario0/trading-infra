//! Transport layer traits.

use std::io;

use crate::address::Address;

/// Abstraction for the incoming transport layer (reading raw bytes).
/// Implementation details (ZMQ, TCP, UDP, Memory) are hidden behind this trait.
#[allow(async_fn_in_trait)]
pub trait Receiver: Send + Sync {
    /// Receive the next full frame/message as bytes.
    async fn recv_bytes(&mut self) -> Result<Vec<u8>, io::Error>;

    /// Try to receive the next full frame/message as bytes.
    /// If no message is available, returns an error.
    async fn try_recv(&mut self) -> Result<Vec<u8>, io::Error>;
}

/// Abstraction for the outgoing transport layer (sending raw bytes).
#[allow(async_fn_in_trait)]
pub trait Sender: Send + Sync + Sized {
    /// Send a full frame/message.
    async fn send_bytes(&mut self, data: &[u8]) -> Result<(), io::Error>;
}

/// Abstraction of the server side of a connection.
#[allow(async_fn_in_trait)]
pub trait Server: Send + Sync + Sized {
    /// The type of connection this server provides (usually both Sender and Receiver).
    type Connection: Sender + Receiver;

    /// Starts listening to a specific address.
    async fn listen_to(address: Address) -> Result<Self, io::Error>;

    /// Blocks until a new connection is established, and returns it.
    async fn accept(&mut self) -> Result<Self::Connection, io::Error>;
}

/// Abstraction of the client side of a connection.
#[allow(async_fn_in_trait)]
pub trait Client: Send + Sync + Sized {
    /// Connects to a specific server address.
    async fn connect_to(address: Address) -> Result<Self, io::Error>;
}
