//! Address models for network configuration.
//!
//! Defines the `Address` enum for abstracting over different transport protocols (ZMQ, Memory).

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Represents a network address for communication endpoints.
///
/// This enum allows shielding the application from specific transport implementations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Address {
    /// TCP connection address.
    ///
    /// Those containts both the IP address and the port.
    TCP(String),

    /// No connection (used for dynamic Multiplexers starting empty)
    Empty,
}

impl Address {
    /// Creates a new TCP address.
    ///
    /// # Arguments
    ///
    /// * `ip` - The IP address (e.g., "127.0.0.1").
    /// * `port` - The TCP port.
    ///
    /// # Returns
    ///
    /// A `Address::TCP` variant.
    pub fn tcp(ip: &str, port: u16) -> Self {
        Address::TCP(format!("{}:{}", ip, port))
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Address::TCP(addr) => write!(f, "tcp://{}", addr),
            Address::Empty => write!(f, "empty"),
        }
    }
}

impl Default for Address {
    fn default() -> Self {
        Address::TCP("127.0.0.1:5555".to_string())
    }
}

impl FromStr for Address {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(stripped) = s.strip_prefix("tcp://") {
            let addr = stripped.to_string();
            Ok(Address::TCP(addr))
        } else if s == "empty" {
            Ok(Address::Empty)
        } else {
            Err(format!("Unknown address format: {}", s))
        }
    }
}
