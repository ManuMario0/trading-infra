use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use service::manifest::Manifest;

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
    LoggerSpawned {
        service_id: u32,
        assigned_dir: String,
    },
    ServiceSpawned {
        service_id: u32,
        service_manifest: Manifest,
    },
    ServiceSpawnFailed {
        error: ServiceBootError,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ServiceBootError {
    SpawnFailed {
        path: PathBuf,
        os_error_code: Option<i32>,
        reason: String,
    },
    ManifestTimeout {
        path: PathBuf,
    },
    StdoutCaptureFailed {
        path: PathBuf,
    },
    ManifestParseError {
        path: PathBuf,
        reason: String,
    },
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CommandEvent {
    pub kind: CommandKind,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum CommandKind {
    Deploy(DeployCommand),
    Stop(StopCommand),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DeployCommand {
    pub service_path: PathBuf,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StopCommand {
    pub service_id: usize,
}
