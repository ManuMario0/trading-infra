use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use trading_sdk::{identity::ServiceId, journal::lineage::LineageId};

#[derive(Serialize, Deserialize, Clone, Copy)]
pub enum ServiceKind {
    Orchestrator,
    Ledger,
    Logger,
    Multiplexer,
    Strategy,
    Manager,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub enum ThreadKind {
    Main,
    Network,
    Journal,
    Other,
}

/// A trait for event payloads that can be serialized without knowing the concrete type.
pub trait JournalEvent: erased_serde::Serialize + Send + 'static {}
erased_serde::serialize_trait_object!(JournalEvent);

impl<T: erased_serde::Serialize + Send + 'static> JournalEvent for T {}

/// A structured event that can be recorded for audit/replay.
#[derive(Serialize)]
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
    /// Note: This is skipped during deserialization because trait objects cannot be
    /// instantiated without a registry of concrete types.
    #[serde(skip_deserializing)]
    pub event: Box<dyn JournalEvent>,
}

impl JournalEntry {
    pub fn make_entry(
        service_id: ServiceId,
        service_kind: ServiceKind,
        thread_kind: ThreadKind,
        lineage_id: LineageId,
        parents: Vec<LineageId>,
        log_level: LogLevel,
        event: Box<dyn JournalEvent>,
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
