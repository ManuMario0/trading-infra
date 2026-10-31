//! Address models for network configuration.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::str::FromStr;

/// A network address for a communication endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Address {
    /// Socket address
    IP(SocketAddr),
    /// Shared memory file name
    SHM(String),
}

impl Address {
    /// IP V4 constructor.
    pub fn ip_v4(ip: Ipv4Addr, port: u16) -> Self {
        Address::IP(SocketAddr::V4(SocketAddrV4::new(ip, port)))
    }

    /// Parse into a `std::net::SocketAddr` for use with mio/std.
    pub fn as_socket_addr(&self) -> Result<SocketAddr, String> {
        match self {
            Address::IP(s) => Ok(*s),
            _ => Err("not an ip address".to_string()),
        }
    }

    pub fn as_shared_memory(&self) -> Result<String, String> {
        match self {
            Address::SHM(s) => Ok(s.clone()),
            _ => Err("not a shared memory address".to_string()),
        }
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Address::IP(addr) => write!(f, "{addr}"),
            Address::SHM(file_name) => write!(f, "{file_name}"),
        }
    }
}

impl Default for Address {
    fn default() -> Self {
        Address::IP("127.0.0.1:5555".parse().unwrap())
    }
}

impl FromStr for Address {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(addr) = s.parse::<SocketAddrV4>() {
            Ok(Address::IP(SocketAddr::V4(addr)))
        } else if let Ok(addr) = s.parse::<SocketAddrV6>() {
            Ok(Address::IP(SocketAddr::V6(addr)))
        } else if s.starts_with('/') {
            Ok(Address::SHM(s.to_string()))
        } else {
            Err(format!("unknown address format: {}", s))
        }
    }
}
