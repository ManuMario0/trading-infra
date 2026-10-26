//! All environment variables that can be specified to a service at startup.
//!
//! Any other environment variables that might be provided should be specified in the [`crate::manifest::Manifest`]
//! structure.

use std::{env, str::FromStr};

use ipc::address::Address;

pub const IPC_INPUT: &str = "IPC_INPUT";
pub const IPC_OUTPUT: &str = "IPC_OUTPUT";

/// Logger address.
///
/// The logger address is alway available at startup, and should be used
/// to record all events.
pub const IPC_LOGGER_ADDR: &str = "IPC_LOGGER_ADDR";

/// Service ID.
///
/// It is a unique identifier for the service.
/// It is systematically provided by the orchestrator at startup when [`MANIFEST_REQUEST`] is set to false.
pub const SERVICE_ID: &str = "SERVICE_ID";

/// Boolean flag to request the manifest.
///
/// If set to true, the service is expected to print it's manifest to stdout and exit.
/// When this flag is set to true, no other (useful) environment variables are set.
///
/// # Note
///
/// If the service does not return a properly formed manifest when queried, the service will be considered
/// inexistent and will not be shown as an available service to the user.
pub const MANIFEST_REQUEST: &str = "MANIFEST_REQUEST";

/// Node identifier.
///
/// It is provided to the orchestrator at startup for service identifier assignment.
/// It is also provided to the services, but will mostlikely be useless.
pub const NODE_ID: &str = "NODE_ID";

/// Working directory for microservices.
///
/// It should be used to record service-local informations.
/// Data written in this folder will be archived by the orchestrator once the service shuts down.
pub const WORKING_DIR: &str = "WORKING_DIR";

/// Directory for logging.
///
/// It should be used ONLY for logging. It will become handy to have a dedicated file
/// to later aggregate the logs of all the services.
pub const LOGGING_DIR: &str = "LOGGING_DIR";

/// Port range for the orchestrator.
pub const PORT_RANGE_MIN: &str = "PORT_RANGE_MIN";
pub const PORT_RANGE_MAX: &str = "PORT_RANGE_MAX";

/// Tries to parse a environment variable.
///
/// It returns the `default` value if it was impossible to parse the environment variable, or if it was not set.
pub fn parse_or<T: FromStr>(env: &str, default: T) -> T {
    env::var(env)
        .map_err(|_| ()) // dump error
        .and_then(|s| s.parse::<T>().map_err(|_| ())) // dump error
        .unwrap_or(default)
}

/// Tries to parse the input address from the environment.
///
/// Should probably be moved in the IPC crate.
pub fn parse_ipc_input_addr(field_name: &str) -> Address {
    let env_name = format!("{}_{}", IPC_INPUT, field_name);
    env::var(&env_name)
        .ok()
        .and_then(|s| s.parse::<Address>().ok())
        .unwrap_or_else(|| panic!("Missing required env var '{}'.", env_name))
}

/// Tries to parse the output address from the environment.
///
/// Should probably be moved in the IPC crate.
pub fn parse_ipc_output_addr(field_name: &str) -> Address {
    let env_name = format!("{}_{}", IPC_OUTPUT, field_name);
    env::var(&env_name)
        .ok()
        .and_then(|s| s.parse::<Address>().ok())
        .unwrap_or_else(|| panic!("Missing required env var '{}'.", env_name))
}
