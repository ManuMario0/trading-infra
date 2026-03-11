//! Service ID allocator

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
