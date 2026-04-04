//! Defines the data structure and runners.
//!
//! A data pump is a mechanism that allows on MacOS to simulate the
//! behaviour of linux' io_uring.
//!
//! The idea is to order the reads directly to the NVMe chip from
//! different threads (one thread per NVMe queue is ideal).
//! They try to keep the NVMe queue full of pending reads.
//! Using F_NOCACHE, we ensure the OS don't try to cache any part of the files.
//! We want to avoid this at all cost as it would reduce our own access to
//! the NVMe chip lanes.

pub mod block;
pub mod cursor;
pub mod runner;
