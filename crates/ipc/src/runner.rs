//! The network thread entry point.
//!
//! `IpcRunner<T>` is the blocking event loop that lives on the dedicated
//! network thread.  `IpcHandle` is the lightweight, `Clone`-able control
//! surface that lives on any other thread.
//!
//! Because the topology is **static** (all ports known at init time), there is
//! no command channel for `Bind`/`Connect` operations. The only runtime control is shutdown.

use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use mio::{Events, Poll, Token, Waker};

use crate::dispatcher::EngineDispatcher;

// Token reserved for the shutdown waker (see init.rs / macro)
pub const WAKER_TOKEN: Token = Token(usize::MAX);

// ── IpcHandle ────────────────────────────────────────────────────────────────

/// The user-facing control handle for the network thread.
///
/// Cheap to clone.  Thread-safe.
#[derive(Clone)]
pub struct IpcHandle {
    pub(crate) waker: Arc<Waker>,
}

impl IpcHandle {
    pub fn new(waker: Arc<Waker>) -> Self {
        Self { waker }
    }

    /// Request graceful shutdown of the network thread.
    ///
    /// The runner will finish the current poll iteration and then return.
    pub fn shutdown(&self) -> io::Result<()> {
        SHUTDOWN_REQUESTED.store(true, Ordering::Relaxed);
        self.waker.wake()
    }
}

// Global shutdown flag — simplest approach for a static design.
// One flag per process is fine because only one IpcRunner exists per process.
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

// ── IpcRunner ────────────────────────────────────────────────────────────────

/// The network-thread entry point.
///
/// Owns the mio `Poll`, the `Events` buffer, and the statically-typed engines.
/// Call `run()` from a dedicated `std::thread::spawn`.
pub struct IpcRunner<T: EngineDispatcher> {
    pub(crate) poll: Poll,
    pub(crate) events: Events,
    pub(crate) engines: T,
    /// Keeps the shutdown waker alive for the duration of the runner.
    pub(crate) _shutdown_waker: Arc<Waker>,
}

impl<T: EngineDispatcher> IpcRunner<T> {
    pub fn new(poll: Poll, engines: T, shutdown_waker: Arc<Waker>) -> Self {
        Self {
            poll,
            events: Events::with_capacity(1024),
            engines,
            _shutdown_waker: shutdown_waker,
        }
    }

    /// The blocking event loop.  Returns when shutdown is requested.
    pub fn run(mut self) -> io::Result<()> {
        // Maximum time to sleep in poll before checking reconnect timers.
        let tick = Duration::from_millis(50);

        loop {
            self.poll.poll(&mut self.events, Some(tick))?;

            let mut shutdown = false;

            for event in &self.events {
                if event.token() == WAKER_TOKEN {
                    if SHUTDOWN_REQUESTED.load(Ordering::Relaxed) {
                        shutdown = true;
                        break;
                    }
                    self.engines.on_wake();
                } else {
                    // Zero cost static dispatch directly to the correct engine
                    self.engines.on_event(event.token());
                }
            }

            if shutdown {
                break;
            }

            // Reconnect timers
            self.engines.on_tick();
        }

        Ok(())
    }
}
