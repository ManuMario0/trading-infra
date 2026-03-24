//! Lineage ID generator.
//!
//! The lineage ID is a unique identifier for a causal chain of events.
//! It is formed as follows;
//!
//! ```ignore
//! <service_id>-<counter>
//! ```

use std::sync::atomic::{AtomicU32, Ordering};

use trading_sdk::{identity::ServiceId, journal::lineage::LineageId};

pub struct LineageIdGenerator {
    service_id: ServiceId,
    counter: AtomicU32, // Maximum amount of logs per service is
}

impl LineageIdGenerator {
    pub fn new(service_id: ServiceId) -> Self {
        Self {
            service_id,
            counter: AtomicU32::new(0),
        }
    }

    pub fn next(&self) -> LineageId {
        let counter = self.counter.fetch_add(1, Ordering::Relaxed);
        LineageId::from_parts(self.service_id.get_inner(), counter as u64)
    }
}
