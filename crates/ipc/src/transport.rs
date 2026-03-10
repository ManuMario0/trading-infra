//! Transport layer traits.
//!
//! These traits represent raw byte I/O on a single stream.
//! They are internal building blocks used by the socket layer.

use std::io;

/// Abstraction for the incoming transport layer (reading raw bytes).
#[allow(async_fn_in_trait)]
pub(crate) trait Receiver: Send + Sync {
    /// Receive the next full frame/message as bytes.
    async fn recv_bytes(&mut self) -> Result<Vec<u8>, io::Error>;
}

/// Abstraction for the outgoing transport layer (sending raw bytes).
#[allow(async_fn_in_trait)]
pub(crate) trait Sender: Send + Sync {
    /// Send a full frame/message.
    async fn send_bytes(&mut self, data: &[u8]) -> Result<(), io::Error>;
}
