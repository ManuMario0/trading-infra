use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use trading_sdk::{identity::ServiceId, journal::lineage::LineageId};

pub const MAX_EVENT_SIZE: usize = 4096;

#[derive(Serialize, Deserialize, Clone, Copy)]
pub enum ServiceKind {
    Orchestrator,
    Ledger,
    Logger,
    Multiplexer,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub enum ThreadKind {
    Main,
    Network,
    Journal,
    Other,
}

/// A structured event that can be recorded for audit/replay.
#[derive(Serialize, Deserialize)]
pub struct JournalEntry {
    /// Service id
    pub service_id: ServiceId,
    /// Service kind
    pub service_kind: ServiceKind,
    /// Thread kind
    pub thread_kind: ThreadKind,

    /// Auto-assigned by the runtime
    pub lineage_id: LineageId,
    /// Millisecond timestamp
    pub timestamp: u128,
    /// Correlation ID — ties together events in the same causal chain
    pub parents: Vec<LineageId>,
    /// Level of gravity of the entry
    pub log_level: LogLevel,
    /// The actual event payload
    #[serde(with = "serde_bytes")]
    pub event: [u8; MAX_EVENT_SIZE],
}

impl JournalEntry {
    pub fn make_entry(
        service_id: ServiceId,
        service_kind: ServiceKind,
        thread_kind: ThreadKind,
        lineage_id: LineageId,
        parents: Vec<LineageId>,
        log_level: LogLevel,
        event: [u8; MAX_EVENT_SIZE],
    ) -> Self {
        Self {
            service_id,
            service_kind,
            thread_kind,
            lineage_id,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            parents,
            log_level,
            event,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub enum LogLevel {
    Info,
    Warn,
    Error,
}
