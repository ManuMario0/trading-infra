//! IEX TOPS Binary Feed Parser (v0 - Correctness)
//!
//! Notes:
//! - Timestamps in the IEX protocol can be assumed to form a reliable
//!   order on events (e.g. timestamp_A < timestamp_B means the event producing
//!   message A happend before the event producing message B)

pub mod bookkeeper;
pub mod parser;
pub mod pcap;
pub mod pump;
pub mod telemetry;
