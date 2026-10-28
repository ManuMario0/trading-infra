//! Zero-copy transport layer header stripping.
//!
//! Functions that validate and skip Ethernet, IPv4, and UDP headers
//! by reading individual bytes from a FileCursor and returning the
//! offset where the next layer begins.

use crate::pump::cursor::FileCursor;

// ──────────────────────────── Constants ────────────────────────────

const ETHERNET_HEADER_LEN: usize = 14;
const ETHERTYPE_IPV4: u16 = 0x0800;

const IPV4_MIN_HEADER_LEN: usize = 20;
const IPV4_PROTOCOL_UDP: u8 = 17;

const UDP_HEADER_LEN: usize = 8;

// ──────────────────────────── Public API ───────────────────────────

/// Strips Ethernet + IPv4 + UDP headers starting at `offset` in the cursor.
///
/// Returns the absolute offset of the UDP payload (i.e. the IEX-TP region),
/// or `None` if the packet is not a valid IPv4/UDP packet.
#[inline(always)]
pub fn strip_transport(cursor: &mut FileCursor, offset: usize, packet_len: usize) -> Option<usize> {
    let after_eth = strip_ethernet(cursor, offset, packet_len)?;
    let after_ip = strip_ipv4(cursor, after_eth, offset + packet_len)?;
    strip_udp(after_ip, offset + packet_len)
}

// ──────────────────────────── Internals ────────────────────────────

/// Validates the Ethernet header and returns the offset after it if ethertype is IPv4.
#[inline(always)]
fn strip_ethernet(cursor: &mut FileCursor, offset: usize, packet_len: usize) -> Option<usize> {
    if packet_len < ETHERNET_HEADER_LEN {
        return None;
    }
    let ethertype = u16::from_be_bytes([cursor.get(offset + 12), cursor.get(offset + 13)]);
    if ethertype != ETHERTYPE_IPV4 {
        return None;
    }
    Some(offset + ETHERNET_HEADER_LEN)
}

/// Validates the IPv4 header and returns the offset after it if protocol is UDP.
#[inline(always)]
fn strip_ipv4(cursor: &mut FileCursor, offset: usize, packet_end: usize) -> Option<usize> {
    let remaining = packet_end - offset;
    if remaining < IPV4_MIN_HEADER_LEN {
        return None;
    }
    let ihl = (cursor.get(offset) & 0x0F) as usize;
    let header_len = ihl * 4;
    if header_len < IPV4_MIN_HEADER_LEN || remaining < header_len {
        return None;
    }
    let protocol = cursor.get(offset + 9);
    if protocol != IPV4_PROTOCOL_UDP {
        return None;
    }
    Some(offset + header_len)
}

/// Validates the UDP header and returns the offset of the UDP payload.
#[inline(always)]
fn strip_udp(offset: usize, packet_end: usize) -> Option<usize> {
    let remaining = packet_end - offset;
    if remaining < UDP_HEADER_LEN {
        return None;
    }
    Some(offset + UDP_HEADER_LEN)
}

// ──────────────────────────── FAST PATH (Slice-based) ───────────────────────────

/// Strips Ethernet + IPv4 + UDP headers from raw packet data and returns
/// the UDP payload (i.e. the IEX-TP region).
#[inline(always)]
pub fn strip_transport_slice(packet_data: &[u8]) -> Option<&[u8]> {
    // Let's try to make everything much faster, and see if it works
    // We trust the fact that the underlying protocol is full IPv4 and with the exact expected options.
    if packet_data.len() < ETHERNET_HEADER_LEN + IPV4_MIN_HEADER_LEN + UDP_HEADER_LEN {
        None
    } else {
        Some(&packet_data[ETHERNET_HEADER_LEN + IPV4_MIN_HEADER_LEN + UDP_HEADER_LEN..])
    }
}
