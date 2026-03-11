pub mod orchestrator;

use serde::{Deserialize, Serialize};
use trading_sdk::journal::event::ServiceEvent;

use crate::event::orchestrator::OrchestratorEvent;

#[derive(Serialize, Deserialize)]
pub enum JournalEvent {
    /// Service logic events
    ServiceEvent(ServiceEvent),

    /// Network events
    NetworkEvent(NetworkEvent),

    /// Orchestrator events
    OrchestratorEvent(OrchestratorEvent),
}

#[derive(Serialize, Deserialize)]
pub enum NetworkEvent {}
