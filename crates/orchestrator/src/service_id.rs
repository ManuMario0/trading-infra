//! Service ID allocator.
//!
//! It provides an automatic tracker of allocated ID. Considering that we have the possibility of launching up to
//! 2^20 services on one machine, we don't recycle the ID (which is extremely useful for full reconstruction of log events,
//! as every single ID represent exactly one services than ran at some point).
//!
//! It uses the node id to build unique IDs for the services on the specific node.

use trading_sdk::identity::ServiceId;

pub struct ServiceIdAllocator {
    node_id: u64,
    next_id: u64,
}

impl ServiceIdAllocator {
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            next_id: 0,
        }
    }

    pub fn next(&mut self) -> ServiceId {
        let id = self.next_id;
        self.next_id += 1;
        ServiceId::new(self.node_id | id)
    }
}
