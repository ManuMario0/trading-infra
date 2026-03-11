//! This files defines all the conventions for the environment varialbe naming.

use std::{env, str::FromStr};

pub const IPC_INPUT: &str = "IPC_INPUT";
pub const IPC_OUTPUT: &str = "IPC_OUTPUT";

/// Logger address.
///
/// The logger address is alway available at startup, and should be used
/// to record all events.
pub const IPC_LOGGER: &str = "IPC_LOGGER";

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

/// File path to the logger executable (needed by the orchestrator to spawn the logger).
pub const SYSTEM_LOGGER_BIN: &str = "SYSTEM_LOGGER_BIN";

pub fn parse_or<T: FromStr>(env: &str, default: T) -> T {
    env::var(env)
        .map_err(|_| ()) // dump error
        .and_then(|s| s.parse::<T>().map_err(|_| ())) // dump error
        .unwrap_or(default)
}
