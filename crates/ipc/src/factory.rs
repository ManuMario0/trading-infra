//! The core configurable typestate machinery for the IPC layer.
//!
//! This module defines the Zero-Sized Types (ZSTs) that represent the various
//! configuration states of a network port (Transport, Kind, Role).
//!
//! By using ZSTs, the `SocketFactory` trait enables 100% static, compile-time
//! specialization of the network engines, guaranteeing perfectly optimal memory
//! layouts with zero dynamic dispatch or monolithic enum overhead.

use std::sync::Arc;

use mio::Waker;

use crate::address::Address;
use crate::token::TokenRegistry;

// ── Protocol / Transports ──────────────────────────────────────────────────

pub struct TCP;
pub struct UDP;
pub struct UDS; // Unix Domain Sockets

// ── Topology Kinds ─────────────────────────────────────────────────────────

/// 1 Sender -> Many Receivers
pub struct Broadcast;
/// Many Senders -> 1 Receiver
pub struct Aggregate;
/// 1 Sender -> 1 Receiver
pub struct PointToPoint;

// ── Connection Roles ────────────────────────────────────────────────────────

/// Listens for incoming connections
pub struct Bind;
/// Initiates outward connections
pub struct Connect;

// ── The Factory Trait ───────────────────────────────────────────────────────

/// A factory that produces a highly-specialized `Engine` and user-facing `Handle`
/// based on the specific `(Transport, Kind, Role)` combination required.
pub trait SocketFactory<Tx, Rx = ()> {
    /// The background network-thread struct (e.g., `TcpBroadcastConnectEngine`).
    /// This struct handles raw Mio events, TCP framing, and serialization.
    type Engine;

    /// The user-facing typed socket (e.g., `SendSocket<Msg>` or `RecvSocket<Msg>`).
    /// This handle only interacts with the SPSC queues.
    type Handle;

    /// Instantiates the specific IPC branch.
    fn build(
        name: &str,
        addr: Address,
        registry: TokenRegistry,
        waker: Arc<Waker>,
    ) -> (Self::Engine, Self::Handle);
}
