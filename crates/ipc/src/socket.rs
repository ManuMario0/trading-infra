//! Typed sockets — the user-facing handles to the IPC channels.
//!
//! After calling `IpcInit::build`, the caller receives a `Vec<RawSocket>`
//! (one per port in the blueprint).  These raw handles are type-erased;
//! wrap them in `TypedSender` / `TypedReceiver` for ergonomic use.
//!
//! # Direction convention
//! - `SendSocket<T>` — user writes, network reads and sends.
//! - `RecvSocket<T>` — network reads and writes here, user reads.

use std::sync::Arc;

use mio::Waker;
use serde::Serialize;
use sync::spsc;

// ── Re-export so callers can use these from a single path ──────────────────

#[derive(Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocketKind {
    /// Many senders → one receiver.
    Aggregate,
    /// One sender → many receivers.
    Broadcast,
    /// One sender → one receiver.
    PointToPoint,
}

#[derive(Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocketRole {
    /// This side starts the listener.
    Bind,
    /// This side connects to the listener.
    Connect,
}

// ── Typed Handles ───────────────────────────────────────────────────────────

/// A strongly-typed user-facing write handle.
///
/// Wraps `spsc::Sender<T>` and a `Waker` that notifies the network thread
/// that new data is available. The SPSC channel stores the raw structs,
/// meaning zero allocations on the hot path. Serialization is offloaded
/// to the background network thread.
pub struct SendSocket<T> {
    pub(crate) channel: spsc::Sender<T>,
    pub(crate) waker: Arc<Waker>,
}

impl<T> SendSocket<T> {
    pub(crate) fn new(channel: spsc::Sender<T>, waker: Arc<Waker>) -> Self {
        Self { channel, waker }
    }

    /// Enqueue `value` for sending.
    ///
    /// This operation is extremely dumb and Non-Blocking. If the user-side
    /// SPSC ring-buffer is full, the packet is silently dropped to avoid
    /// blocking the critical path. The network thread handles tracking
    /// bottlenecks and reporting telemetry.
    #[inline(always)]
    pub fn send(&mut self, value: T) {
        if self.channel.send(value).is_ok() {
            // Best-effort wake — ignore transient OS errors.
            let _ = self.waker.wake();
        }
    }
}

/// A strongly-typed user-facing read handle.
///
/// The network thread does the parsing and deserialization, and pushes
/// native structs down this channel for the user to consume with zero logic.
pub struct RecvSocket<T> {
    pub(crate) channel: spsc::Receiver<T>,
}

impl<T> RecvSocket<T> {
    pub(crate) fn new(channel: spsc::Receiver<T>) -> Self {
        Self { channel }
    }

    /// Try to receive the next message without blocking.
    ///
    /// Returns `None` when the queue is empty.
    #[inline(always)]
    pub fn try_recv(&mut self) -> Option<T> {
        self.channel.recv()
    }
}
