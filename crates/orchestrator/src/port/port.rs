//! Network port.

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
