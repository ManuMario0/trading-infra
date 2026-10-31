//! Network events produced by the IPC layer.
//!
//! The `NetworkEventLogger` trait is the sink; any struct implementing it
//! can be dynamically dispatched to receive system telemetry optimally.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::address::Address;

/// Fixed metadata holding the static network configuration for a specific port.
///
/// Kept purely decoupled from dynamic event flow. The Engine holds onto an
/// `Arc` of this to avoid string allocations when firing transient events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortInfo {
    pub port_name: String,
    pub addr: Address,
    pub transport: String,
    pub kind: String,
    pub role: String,
}

/// A structured event emitted natively by the IPC network thread.
#[derive(Debug, Serialize, Deserialize)]
pub struct NetworkEvent {
    pub kind: NetworkEventKind,
    pub port_info: PortInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NetworkEventKind {
    /// A remote peer successfully connected.
    Connected {
        peer: SocketAddr,
    },
    /// A remote peer disconnected (read returned 0 / peer closed connection).
    Disconnected {
        peer: SocketAddr,
    },
    /// We are actively backing off before retrying the connection.
    Reconnecting {
        attempt: u32,
    },
    /// The engine successfully re-established a previously broken connection.
    Reconnected {
        peer: SocketAddr,
    },
    /// The backoff timer exceeded its maximum allowed duration.
    ///
    /// We still wait for try to reconnect, but trigger timeout event anyways.
    Timeout,

    // Telemetry / Ring-buffer pressure
    ChannelCapacity50,
    ChannelCapacity75,
    ChannelFull,

    /// A remote client tried to connect on an already bind point-to-point server.
    RejectedDuplicateClient {
        peer: SocketAddr,
    },

    /// OS / System-level operational error.
    Error(NetworkError),

    /// Invalid payload
    InvalidData,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NetworkError {
    SystemError { os_error_code: Option<i32> },
    FramingError { description: String },
}

/// Implement this trait and pass it to the network engines to receive
/// all telemetry natively during the `mio::Poll` loop.
pub trait NetworkEventLogger {
    fn log(&mut self, event: NetworkEvent);
}

/// A highly-optimized no-op logger. When used, the Rust compiler handles
/// aggressively eliminating event allocations via Dead-Code Elimination.
pub struct NoopLogger;
impl NetworkEventLogger for NoopLogger {
    fn log(&mut self, _event: NetworkEvent) {}
}
