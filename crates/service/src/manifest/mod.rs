//! Defines what the service is and how it interacts.
//!
//! It provides all the necessary informations for the orchestrator to:
//! - inform and inform the end user of the exact services that are available for deployement and
//! - when deployed, setup communications.
//!
//! For more details on how to specify the communication layout, see [`ipc::blueprint`].

use ipc::blueprint::Blueprint;
use serde::{Deserialize, Serialize};

pub use ipc::blueprint::PortDefinition;

/// Defines what the service is and how it interacts.
///
/// For more details see [`self`].
#[derive(Serialize, Deserialize, Clone)]
pub struct Manifest {
    /// The communication layout.
    ///
    /// See [`ipc::blueprint`] for details.
    pub blueprint: Blueprint,

    /// The name of the service.
    pub name: String,

    /// The version of the service.
    pub version: String,

    /// The description of the service.
    pub description: String,

    /// The type of the service.
    ///
    /// Examples include manager, multiplexer or strategy.
    pub service_type: String,
}

impl Manifest {
    pub fn json_serialize(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self)
    }
}
