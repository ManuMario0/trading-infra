//! `ipc` — Inter-process communication layer for the trading infrastructure.
//!
//! ## Quick start
//!
//! ```no_run
//! use ipc::make_ipc;
//! use ipc::factory::{Aggregate, Bind, TCP};
//!
//! // 1. Statically generate the IPC topology via the macro
//! let (runner, blueprint, handle, data_in_socket) = make_ipc! {
//!     DATA_IN: kind=Aggregate, role=Bind, capacity=1024, ty=u64, addr="127.0.0.1:9000", transport=TCP
//! };
//!
//! // 2. Spawn the zero-cost optimal network event loop
//! std::thread::spawn(move || {
//!     runner.run().unwrap();
//! });
//!
//! // 3. Easily use the typed sockets dynamically
//! // let msg = data_in_socket.recv().unwrap();
//! ```

pub mod address;
pub mod blueprint;
pub mod event;
pub mod factory;
pub mod macros;
pub mod runner;
pub mod socket;
pub mod token;

pub(crate) mod backoff;
pub(crate) mod dispatcher;
pub(crate) mod engines;
pub(crate) mod framing;
