//! PcapNG parser for IEX TOPS feed data.
//!
//! Uses a FileCursor to stream through the file sequentially,
//! reading bytes on demand from the data pump's SPSC channels.

pub mod transport;

use crate::bookkeeper::SoaBookKeeper;
use crate::parser::QuoteUpdateMessage;
use crate::pump::cursor::FileCursor;
use crate::telemetry::LatencyProbe;
use crate::telemetry::StatsCollector;

// ──────────────────────────── PcapNG Constants ────────────────────

/// PcapNG Enhanced Packet Block type.
const EPB_BLOCK_TYPE: u32 = 0x0000_0006;

/// EPB fixed-header size before packet data.
const EPB_HEADER_LEN: usize = 28;

/// Minimum valid PcapNG block size.
const MIN_BLOCK_LEN: usize = 12;

// ──────────────────────────── PcapNG Parser ──────────────────────

/// Processes a single captured packet using the cursor fallback (slow path).
#[inline(always)]
fn slow_process_pcap_block(
    cursor: &mut FileCursor,
    packet_start: usize,
    captured_len: usize,
    bookkeeper: &mut SoaBookKeeper,
    probe: &mut LatencyProbe,
) {
    let packet_end = packet_start + captured_len;

    // Strip Eth/IP/UDP to find the IEX-TP payload offset
    if let Some(iex_offset) = transport::strip_transport(cursor, packet_start, captured_len) {
        let iex_len = packet_end - iex_offset;
        if iex_len >= 40 {
            // IEX-TP header: message_count at offset 14
            let message_count = cursor.read_u16_le(iex_offset + 14);
            let mut msg_pos = iex_offset + 40;

            for _ in 0..message_count {
                if msg_pos + 2 > packet_end {
                    break;
                }
                let message_len = cursor.read_u16_le(msg_pos) as usize;
                probe.mark(message_len as u64);
                msg_pos += 2;

                if msg_pos + message_len > packet_end {
                    break;
                }

                // Check message type
                if cursor.get(msg_pos) == b'Q' && message_len == 42 {
                    let quote = QuoteUpdateMessage::from_cursor(cursor, msg_pos);
                    bookkeeper.update_quote(quote);
                }

                msg_pos += message_len;
            }
        }
    }
}

/// Processes a single captured packet using the fast slice-based path.
#[inline(always)]
fn fast_process_pcap_block(
    packet_slice: &[u8],
    bookkeeper: &mut SoaBookKeeper,
    probe: &mut LatencyProbe,
) {
    if let Some(iex_payload) = transport::strip_transport_slice(packet_slice) {
        if iex_payload.len() >= 40 {
            // IEX-TP header: message_count at offset 14
            let message_count = u16::from_le_bytes(iex_payload[14..16].try_into().unwrap());
            let mut msg_pos = 40;

            for _ in 0..message_count {
                if msg_pos + 2 > iex_payload.len() {
                    break;
                }
                let message_len =
                    u16::from_le_bytes(iex_payload[msg_pos..msg_pos + 2].try_into().unwrap())
                        as usize;
                probe.mark(message_len as u64);
                msg_pos += 2;

                if msg_pos + message_len > iex_payload.len() {
                    break;
                }

                // Check message type
                let message = &iex_payload[msg_pos..msg_pos + message_len];
                if message[0] == b'Q' && message_len == 42 {
                    let quote = QuoteUpdateMessage::from_bytes(message);
                    bookkeeper.update_quote(quote);
                }

                msg_pos += message_len;
            }
        }
    }
}

/// Runs the PcapNG parser over the file via a FileCursor.
///
/// Walks through PcapNG blocks linearly, extracts Enhanced Packet Blocks,
/// strips transport headers, and feeds IEX-TP messages to the bookkeeper.
pub fn run(cursor: &mut FileCursor, bookkeeper: &mut SoaBookKeeper) -> StatsCollector {
    let mut stats = StatsCollector::new();
    let file_len = cursor.file_length();
    let mut pos: usize = 0;

    // Skip SHB (Section Header Block)
    if pos + 8 > file_len {
        return stats;
    }
    let shb_total_len = cursor.read_u32_le(pos + 4) as usize;
    pos += shb_total_len;

    // Skip IDB (Interface Description Block)
    if pos + 8 > file_len {
        return stats;
    }
    let idb_total_len = cursor.read_u32_le(pos + 4) as usize;
    pos += idb_total_len;

    // Walk through PcapNG blocks
    while pos + MIN_BLOCK_LEN <= file_len {
        let block_type = cursor.read_u32_le(pos);
        let block_total_len = cursor.read_u32_le(pos + 4) as usize;

        if block_total_len < MIN_BLOCK_LEN {
            break;
        }

        let block_end = pos + block_total_len;
        if block_end > file_len {
            break;
        }

        if block_type == EPB_BLOCK_TYPE && block_total_len > EPB_HEADER_LEN {
            let mut probe = LatencyProbe::start();

            let captured_len = cursor.read_u32_le(pos + 20) as usize;
            let packet_start = pos + EPB_HEADER_LEN;
            let packet_end = packet_start + captured_len;

            if packet_end <= block_end {
                if let Some(packet_slice) = cursor.try_get_slice(packet_start, captured_len) {
                    fast_process_pcap_block(packet_slice, bookkeeper, &mut probe);
                } else {
                    slow_process_pcap_block(
                        cursor,
                        packet_start,
                        captured_len,
                        bookkeeper,
                        &mut probe,
                    );
                }
            }

            probe.stop(&mut stats);
        }

        pos = block_end;
    }

    stats
}
