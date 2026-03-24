//! Defines the shape of the service.
//!
//! This includes:
//! - Inputs and outputs, alongside the expected types
//! - Instance specific parameters
//!
//! At startup, those are provided to the service as environment variables.
//!
//! However, some parameters environment variables are always present for all processes
//! and must not be specify in the manifest. For example, the environment variable
//! `IPC_LOGGER` is always set and can be used safely to setup the logger.

use ipc::blueprint::Blueprint;
use serde::{Deserialize, Serialize};

pub use ipc::blueprint::PortDefinition;

/// A manifest describes what a process is and what it requires at startup.
///
/// Think of the manifest as the identity card of a microservice, one should
/// be able to start the process correctly and pipe it with only being aware
/// of its manifest.
///
/// A service should be self descriptive, that is, a service should output
/// the manifest in stdout and yield if some expected environemnt variable
/// does not exists.
#[derive(Serialize, Deserialize, Clone)]
pub struct Manifest {
    pub blueprint: Blueprint,
    pub name: String,
    pub version: String,
    pub description: String,
    pub service_type: String,
}
