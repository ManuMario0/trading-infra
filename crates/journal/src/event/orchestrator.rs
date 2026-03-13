use std::{collections::BTreeMap, path::PathBuf};

use ipc::address::Address;
use serde::{Deserialize, Serialize};
use service::manifest::Manifest;
use trading_sdk::identity::ServiceId;

use crate::event::{context::Context, runtime::RuntimeErrorKind};

#[derive(Serialize, Deserialize)]
pub enum OrchestratorEvent {
    Command(CommandEvent),
    Lifecycle(LifecycleEvent),
}

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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServiceBootError {
    // Who is the service
    pub path: PathBuf,
    // What was the service asked to do
    pub context: Context,
    // What went wrong
    pub kind: RuntimeErrorKind,
}

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
