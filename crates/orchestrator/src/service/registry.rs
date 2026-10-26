//! Service registry.
//!
//! I am slightly unsure of its usefulness as it appears to not have any querying functions on the registry.

use std::{collections::BTreeMap, path::PathBuf};

use trading_sdk::identity::ServiceId;

use crate::service::info::ServiceInfo;

pub struct ServiceRegistry {
    registry: BTreeMap<ServiceId, ServiceInfo>,
    working_dir: PathBuf,
}

impl ServiceRegistry {
    pub fn new(working_dir: PathBuf) -> Self {
        Self {
            registry: BTreeMap::new(),
            working_dir,
        }
    }

    pub fn insert(&mut self, service_info: ServiceInfo) {
        self.registry.insert(service_info.identifier, service_info);
    }

    pub fn working_dir(&self) -> &PathBuf {
        &self.working_dir
    }
}
