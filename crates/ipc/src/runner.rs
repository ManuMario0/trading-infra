//! The network thread entry point.
//!
//! `IpcRunner<T>` is the blocking event loop that lives on the dedicated
//! network thread. `IpcHandle` is the lightweight, `Clone`-able control
//! surface that lives on any other thread.
//!
//! Because the topology is **static** (all ports known at init time), there is
//! no command channel for `Bind`/`Connect` operations. The only runtime control is shutdown.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};

use journal::journal::Journal;

use crate::dispatcher::EngineDispatcher;

// ── IpcHandle ────────────────────────────────────────────────────────────────

/// The user-facing control handle for the network thread.
///
/// Cheap to clone. Thread-safe.
#[derive(Clone, Copy, Default)]
pub struct IpcHandle {}

impl IpcHandle {
    pub fn new() -> Self {
        Self {}
    }

    /// Request graceful shutdown of the network thread.
    ///
    /// The runner will finish the current poll iteration and then return.
    pub fn shutdown(&self) {
        SHUTDOWN_REQUESTED.store(true, Ordering::Relaxed);
    }
}

// Global shutdown flag — simplest approach for a static design.
// One flag per process is fine because only one IpcRunner exists per process.
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

// ── IpcRunner ────────────────────────────────────────────────────────────────

/// The network-thread entry point.
pub struct IpcRunner<T: EngineDispatcher> {
    pub(crate) engines: T,
}

impl<T: EngineDispatcher> IpcRunner<T> {
    pub fn new(engines: T) -> Self {
        Self { engines }
    }

    /// The blocking event loop.  Returns when shutdown is requested.
    pub fn run(mut self, mut journal: Journal) -> io::Result<()> {
        loop {
            self.engines.on_tick(&mut journal);

            if SHUTDOWN_REQUESTED.load(Ordering::Relaxed) {
                break;
            }

            std::hint::spin_loop();
        }

        Ok(())
    }
}
