//! Address models for network configuration.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

/// A network address for a communication endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Address {
    /// TCP address (IP:port).
    TCP(String),
}

impl Address {
    /// Convenience constructor.
    pub fn tcp(ip: &str, port: u16) -> Self {
        Address::TCP(format!("{}:{}", ip, port))
    }

    /// Parse into a `std::net::SocketAddr` for use with mio/std.
    pub fn as_socket_addr(&self) -> Result<SocketAddr, String> {
        match self {
            Address::TCP(s) => s
                .parse()
                .map_err(|e| format!("Invalid TCP address '{}': {}", s, e)),
        }
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Address::TCP(addr) => write!(f, "tcp://{}", addr),
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
            Ok(Address::TCP(stripped.to_string()))
        } else {
            Err(format!("Unknown address format: {}", s))
        }
    }
}
