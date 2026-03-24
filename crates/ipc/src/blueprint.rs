//! Static blueprint for a service's IPC topology.
//!
//! The blueprint is supplied at startup and is **never mutated** at runtime.
//! All channels are allocated upfront based on it.

use serde::{Deserialize, Serialize};

use crate::socket::{SocketKind, SocketRole};

/// Full topology description for one service.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Blueprint {
    /// Ports this service uses.
    pub ports: Vec<PortDefinition>,
}

impl Blueprint {
    /// Iterate over all ports regardless of direction.
    pub fn all_ports(&self) -> impl Iterator<Item = &PortDefinition> {
        self.ports.iter()
    }
}

/// Self-describing definition of one network port.
///
/// Both the address **and** channel capacity are baked in at topology-design
/// time, keeping the static guarantee end-to-end.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PortDefinition {
    /// Unique port name within a service (UPPER_SNAKE_CASE).
    name: String,
    /// Human-readable type name for diagnostics / introspection.
    pub data_type: String,
    /// Communication pattern.
    pub kind: SocketKind,
    /// Whether we listen (Bind) or initiate (Connect).
    pub role: SocketRole,
    /// SPSC ring-buffer capacity (rounded up to next power-of-two by the
    /// allocator).  Controls how many serialized messages can be buffered
    /// between the user thread and the network thread.
    pub channel_capacity: usize,
}

impl PortDefinition {
    pub fn new(
        name: String,
        data_type: String,
        kind: SocketKind,
        role: SocketRole,
        channel_capacity: usize,
    ) -> PortDefinition {
        assert!(
            name.chars().all(|c| c.is_ascii_uppercase() || c == '_'),
            "Port name must be UPPER_SNAKE_CASE, got: {}",
            name
        );
        PortDefinition {
            name,
            data_type,
            kind,
            role,
            channel_capacity,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
