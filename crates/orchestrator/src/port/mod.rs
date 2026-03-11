//! Port is responsible for the port handling and assignment on a machine.

/// Valid port range. Everythings outside of this range will be clamped accordingly.
pub const PORT_RANGE_MIN: u16 = 1024;
pub const PORT_RANGE_MAX: u16 = 65535;

pub mod allocator;
pub mod port;
