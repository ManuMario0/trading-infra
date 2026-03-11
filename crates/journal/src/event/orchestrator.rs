use std::path::PathBuf;

use serde::{Deserialize, Serialize};

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
        service_id: u32,
        service_type: String,
        assigned_dir: String,
    },
}

#[derive(Serialize, Deserialize)]
pub struct CommandEvent {
    pub kind: CommandKind,
}

#[derive(Serialize, Deserialize)]
pub enum CommandKind {
    Deploy(DeployCommand),
    Stop(StopCommand),
}

#[derive(Serialize, Deserialize)]
pub struct DeployCommand {
    pub service_path: PathBuf,
}

#[derive(Serialize, Deserialize)]
pub struct StopCommand {
    pub service_id: usize,
}
