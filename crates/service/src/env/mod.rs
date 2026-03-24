//! This files defines all the conventions for the environment varialbe naming.

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
/// It is systematically provided by the orchestrator at startup.
pub const SERVICE_ID: &str = "SERVICE_ID";

/// Boolean flag to request the manifest.
///
/// If set to true, the service should print it's manifest to stdout and exit.
/// The service must assume that no other flags are set.
pub const MANIFEST_REQUEST: &str = "MANIFEST_REQUEST";

/// Node identifier.
///
/// It is provided to the orchestrator at startup for service identifier assignment.
/// It is also provided to the services, but will mostlikely be useless.
pub const NODE_ID: &str = "NODE_ID";

/// Working directory for microservices to read/write persistent data.
pub const WORKING_DIR: &str = "WORKING_DIR";

/// Directory for logging.
pub const LOGGING_DIR: &str = "LOGGING_DIR";

/// Port range for the service.
pub const PORT_RANGE_MIN: &str = "PORT_RANGE_MIN";
pub const PORT_RANGE_MAX: &str = "PORT_RANGE_MAX";

pub fn parse_or<T: FromStr>(env: &str, default: T) -> T {
    env::var(env)
        .map_err(|_| ()) // dump error
        .and_then(|s| s.parse::<T>().map_err(|_| ())) // dump error
        .unwrap_or(default)
}

pub fn parse_ipc_input_addr(field_name: &str) -> Address {
    let env_name = format!("{}_{}", IPC_INPUT, field_name);
    env::var(&env_name)
        .ok()
        .and_then(|s| s.parse::<Address>().ok())
        .unwrap_or_else(|| panic!("Missing required env var '{}'.", env_name))
}

pub fn parse_ipc_output_addr(field_name: &str) -> Address {
    let env_name = format!("{}_{}", IPC_OUTPUT, field_name);
    env::var(&env_name)
        .ok()
        .and_then(|s| s.parse::<Address>().ok())
        .unwrap_or_else(|| panic!("Missing required env var '{}'.", env_name))
}
