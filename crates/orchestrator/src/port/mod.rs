//! Port is responsible for the port handling and assignment on a machine.

/// Valid port range. Everythings outside of this range will be clamped accordingly.
pub const PORT_RANGE_MIN: u16 = 1024;
pub const PORT_RANGE_MAX: u16 = 65535;

pub mod allocator;

use ipc::socket::SocketRole;
use trading_sdk::identity::ServiceId;

pub struct Port(pub u16);

/// Usage information.
#[derive(Debug, Clone)]
pub struct UseInfo {
    /// Identifier of the service using the port
    pub service_id: ServiceId,
    /// What role was it assigned
    pub role: SocketRole,
    /// Local port name
    pub port_name: String,
}
