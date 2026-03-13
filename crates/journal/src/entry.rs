use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use service::identity::Identity;
use trading_sdk::journal::lineage::LineageId;

use crate::event::JournalEvent;

/// A structured event that can be recorded for audit/replay.
#[derive(Serialize, Deserialize)]
pub struct JournalEntry {
    /// Auto-assigned by the runtime
    pub lineage_id: LineageId,
    /// Millisecond timestamp
    pub timestamp: u128,
    /// Which component emitted this
    pub source: Identity,
    /// Correlation ID — ties together events in the same causal chain
    pub parents: Vec<LineageId>,
    /// Level of gravity of the entry
    pub log_level: LogLevel,
    /// The actual event payload
    pub event: JournalEvent,
}

impl JournalEntry {
    pub fn make_entry(
        lineage_id: LineageId,
        source: Identity,
        parents: Vec<LineageId>,
        log_level: LogLevel,
        event: JournalEvent,
    ) -> Self {
        Self {
            lineage_id,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            source,
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
