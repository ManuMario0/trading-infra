//! Allocator of port. Keep track of what ports are available and
//! which one are currently in use.
//!
//! It does not store any information about the actual use of the port.

use std::ops::RangeInclusive;

use crate::port::port::Port;

pub struct Allocator {
    port_range: RangeInclusive<u16>,
    ports_usage: Vec<bool>,
    lowest_available_port_index: usize,
}

impl Allocator {
    /// Creates a new allocator, with the given port pool.
    ///
    /// The port pool must be a full range. The allocator does not verify
    /// that the ports are available to use on the machine, the user must
    /// ensure the ports are free to use.
    pub fn new(port_range: RangeInclusive<u16>) -> Self {
        let ports_usage = vec![false; (port_range.end() - port_range.start() + 1) as usize];
        Allocator {
            port_range,
            ports_usage,
            lowest_available_port_index: 0,
        }
    }

    /// Allocate a port in the range of available port.
    pub fn allocate(&mut self) -> Option<Port> {
        if self.lowest_available_port_index >= self.ports_usage.len() {
            return None;
        }
        let port = self.port_range.start() + self.lowest_available_port_index as u16;
        self.ports_usage[self.lowest_available_port_index] = true;
        while self.lowest_available_port_index <= self.ports_usage.len()
            && self.ports_usage[self.lowest_available_port_index]
        {
            self.lowest_available_port_index += 1;
        }
        Some(Port(port))
    }

    /// Free the port.
    ///
    /// Ensure that no processes listen or emit through this port
    /// before freeing it. Once a port is free, it will be reused.
    pub fn free(&mut self, port: Port) {
        let port_index = port.0 - self.port_range.start();
        self.ports_usage[port_index as usize] = false;
        if (port_index as usize) < self.lowest_available_port_index {
            self.lowest_available_port_index = port_index as usize;
        }
    }
}
