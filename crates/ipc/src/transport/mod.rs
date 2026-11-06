//! Transport layer for the IPC crate.
//!
//! It defines the required traits to be implemented by individual transport backends (IP, SHM, etc.)
//! and provides the unified enum wrappers used by the IPC engines.

pub(crate) mod ip;
pub(crate) mod shm;

use std::io;

use crate::address::Address;
use crate::transport::ip::{IpConnector, IpListener, IpTransport};

// ── Backend Traits ──────────────────────────────────────────────────────────

pub(crate) trait TransportBackend<R> {
    /// Attempts to read one complete message.
    fn try_recv_with<F, E>(&mut self, f: F) -> io::Result<Option<R>>
    where
        F: FnOnce(&[u8]) -> Result<R, E>;

    /// Attempts to write a complete message.
    fn try_write_with<F, E>(&mut self, payload: &R, f: F) -> io::Result<()>
    where
        F: FnOnce(&R, &mut [u8]) -> Result<usize, E>;

    /// Checks if the transport is ready to be written to.
    fn is_writable(&mut self) -> bool;

    /// Returns the address of the remote connection.
    fn address(&self) -> Address;
}

pub(crate) trait ConnectorBackend<R> {
    type Connection: TransportBackend<R>;

    fn try_connect(&mut self) -> io::Result<Self::Connection>;
}

pub(crate) trait ListenerBackend<R> {
    type Connection: TransportBackend<R>;

    fn try_accept(&mut self) -> io::Result<(Self::Connection, Address)>;
}

// ── Unified Enums ───────────────────────────────────────────────────────────

pub enum Transport<R> {
    IP(IpTransport<R>),
}

impl<R> Transport<R> {
    pub fn try_recv_with<F, E>(&mut self, f: F) -> io::Result<Option<R>>
    where
        F: FnOnce(&[u8]) -> Result<R, E>,
    {
        match self {
            Self::IP(t) => t.try_recv_with(f),
        }
    }

    pub fn try_write_with<F, E>(&mut self, payload: &R, f: F) -> io::Result<()>
    where
        F: FnOnce(&R, &mut [u8]) -> Result<usize, E>,
    {
        match self {
            Self::IP(t) => t.try_write_with(payload, f),
        }
    }

    pub fn is_writable(&mut self) -> bool {
        match self {
            Self::IP(t) => t.is_writable(),
        }
    }

    pub fn address(&self) -> Address {
        match self {
            Self::IP(t) => t.address(),
        }
    }
}

impl<R> TransportBackend<R> for Transport<R> {
    fn try_recv_with<F, E>(&mut self, f: F) -> io::Result<Option<R>>
    where
        F: FnOnce(&[u8]) -> Result<R, E>,
    {
        self.try_recv_with(f)
    }

    fn try_write_with<F, E>(&mut self, payload: &R, f: F) -> io::Result<()>
    where
        F: FnOnce(&R, &mut [u8]) -> Result<usize, E>,
    {
        self.try_write_with(payload, f)
    }

    fn is_writable(&mut self) -> bool {
        self.is_writable()
    }

    fn address(&self) -> Address {
        self.address()
    }
}

pub enum Connector<R> {
    IP(IpConnector<R>),
}

impl<R> Connector<R> {
    pub fn new(addr: Address) -> io::Result<Self> {
        match addr {
            Address::IP(addr) => Ok(Self::IP(IpConnector::new(addr)?)),
            _ => {
                todo!()
            }
        }
    }

    pub fn try_connect(&mut self) -> io::Result<Transport<R>> {
        match self {
            Self::IP(c) => c.try_connect().map(Transport::IP),
        }
    }
}

impl<R> ConnectorBackend<R> for Connector<R> {
    type Connection = Transport<R>;

    fn try_connect(&mut self) -> io::Result<Self::Connection> {
        self.try_connect()
    }
}

pub enum Listener<R> {
    IP(IpListener<R>),
}

impl<R> Listener<R> {
    pub fn new(addr: Address) -> io::Result<Self> {
        match addr {
            Address::IP(addr) => Ok(Self::IP(IpListener::new(addr)?)),
            _ => {
                todo!()
            }
        }
    }

    pub fn try_accept(&mut self) -> io::Result<(Transport<R>, Address)> {
        match self {
            Self::IP(l) => {
                let (conn, addr) = l.try_accept()?;
                Ok((Transport::IP(conn), addr))
            }
        }
    }
}

impl<R> ListenerBackend<R> for Listener<R> {
    type Connection = Transport<R>;

    fn try_accept(&mut self) -> io::Result<(Self::Connection, Address)> {
        self.try_accept()
    }
}
