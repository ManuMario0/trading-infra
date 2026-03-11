//! Lineage ID generator.
//!
//! The lineage ID is a unique identifier for a causal chain of events.
//! It is formed as follows;
//!
//! ```
//! <service_id>-<counter>
//! ```

use trading_sdk::{identity::ServiceId, journal::lineage::LineageId};

pub struct LineageIdGenerator {
    service_id: ServiceId,
    counter: u32, // Maximum amount of logs per service is
}

impl LineageIdGenerator {
    pub fn new(service_id: ServiceId) -> Self {
        Self {
            service_id,
            counter: 0,
        }
    }

    pub fn next(&mut self) -> LineageId {
        self.counter += 1;
        LineageId::from_parts(self.service_id.get_inner(), self.counter as u64)
    }
}
