//! This files defines all the conventions for the environment varialbe naming.

pub const IPC_INPUT: &str = "IPC_INPUT";
pub const IPC_OUTPUT: &str = "IPC_OUTPUT";

/// Logger address.
///
/// The logger address is alway available at startup, and should be used
/// to record all events.
pub const IPC_LOGGER: &str = "IPC_LOGGER";
