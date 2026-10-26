//! Events used by the orchestrator.
//!
//! We structure them for easier processing later down the line.

use std::{collections::BTreeMap, path::PathBuf};

use ipc::address::Address;
use serde::{Deserialize, Serialize};
use service::manifest::Manifest;
use trading_sdk::identity::ServiceId;

use crate::service::error::ServiceBootError;

/// An event from the orchestrator.
#[derive(Serialize, Deserialize)]
pub enum OrchestratorEvent {
    Command(CommandEvent),
    Lifecycle(LifecycleEvent),
}

/// A life cycle event is an event that is related to the internal working of the orchestrator, to keep track of it's actions.
#[derive(Serialize, Deserialize)]
pub enum LifecycleEvent {
    Startup {
        working_dir: String,
    },
    ServiceSpawned {
        // Who is the service
        path: PathBuf,
        // Identifier attributed
        service_id: ServiceId,
        // Manifest of the service
        service_manifest: Manifest,
    },
    ServiceSpawnFailed(ServiceBootError),
}

/// A command event is an event triggered by the user.
///
/// Everytime a user enters a commant, it is recorded in the journal.
#[derive(Serialize, Deserialize, Clone)]
pub enum CommandEvent {
    Deploy {
        service_path: PathBuf,
        addr_map: BTreeMap<String, Address>,
    },
    Stop {
        service_id: u32,
    },
}
