//! Service lifetime events.
//!
//! We define the service event type here to have a unified journal accross all the different services.

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ServiceLifetimeEvent {
    pub service_type: String,
    pub kind: ServiceEventKind,
}

#[derive(Serialize, Deserialize)]
pub enum ServiceEventKind {
    Startup,
    Info(String),
}
