//! Service lifetime events

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ServiceLifetimeEvent {
    pub service_type: String,
    pub kind: ServiceEventKind,
}

#[derive(Serialize, Deserialize)]
pub enum ServiceEventKind {
    Startup,
}
