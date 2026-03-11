pub mod orchestrator;
pub mod service;

use serde::{Deserialize, Serialize};
use trading_sdk::journal::event::ServiceEvent;

use crate::event::{orchestrator::OrchestratorEvent, service::ServiceLifetimeEvent};

#[derive(Serialize, Deserialize)]
pub enum JournalEvent {
    /// Service logic events
    ServiceEvent(ServiceEvent),

    /// Orchestrator events
    OrchestratorEvent(OrchestratorEvent),

    /// Service lifetime events
    ServiceLifetimeEvent(ServiceLifetimeEvent),
}
