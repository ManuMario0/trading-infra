//! Service registry.

use std::collections::BTreeMap;

use trading_sdk::identity::ServiceId;

use crate::service::info::ServiceInfo;

pub struct ServiceRegistry {
    registry: BTreeMap<ServiceId, ServiceInfo>,
}

impl ServiceRegistry {
    pub fn new() -> Self {
        Self {
            registry: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, service_info: ServiceInfo) {
        self.registry.insert(service_info.identifier, service_info);
    }
}
