//! Network event logs.

use serde::{Deserialize, Serialize};

use trading_sdk::identity::ServiceId;

use ipc::socket::{SocketKind, SocketRole};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum NetworkEventKind {
    /// Two services attempted to bind the same port/address.
    Collision {
        port: u16,
        existing_service: ServiceId,
    },
    /// The port kind being assigned does not match the port's existing topology.
    TopologyMismatch {
        expected: SocketKind,
        actual: SocketKind,
    },
    /// The role (Bind/Connect) is invalid for this point in the lifecycle.
    RoleConflict { role: SocketRole },
    /// The peer died or refused connection.
    Inaccessible { address: String },
    /// Connection was established but timed out during the protocol handshake.
    HandshakeTimeout { max_time_ms: u64 },
}
