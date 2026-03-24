use std::process::Child;

use trading_sdk::identity::ServiceId;

use ipc::address::Address;
use service::manifest::Manifest;

/// Informations about a running service.
///
/// This structure will get more complex as the interactions
/// between the orchestrator and the services becomes more complex.
pub struct ServiceInfo {
    pub handle: Child,
    pub identifier: ServiceId,
    pub manifest: Manifest,
    pub ports: Vec<Address>,
}
