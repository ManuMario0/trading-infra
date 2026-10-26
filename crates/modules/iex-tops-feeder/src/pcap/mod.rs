//! PcapNG parser for IEX TOPS feed data.
//!
//! Uses a FileCursor to stream through the file sequentially,
//! reading bytes on demand from the data pump's SPSC channels.

pub mod transport;

use std::path::PathBuf;

use crate::bookkeeper::SoaBookKeeper;
use crate::parser::QuoteUpdateMessage;
use crate::pump::cursor::FileCursor;
use crate::telemetry::LatencyProbe;
use crate::telemetry::StatsCollector;

const BLOCK_SIZE: usize = 2 * 1024 * 1024;
const NUM_RUNNERS: usize = 4;

// ──────────────────────────── PcapNG Constants ────────────────────

/// PcapNG Enhanced Packet Block type.
const EPB_BLOCK_TYPE: u32 = 0x0000_0006;

/// EPB fixed-header size before packet data.
const EPB_HEADER_LEN: usize = 28;

/// Minimum valid PcapNG block size.
const MIN_BLOCK_LEN: usize = 12;

// ──────────────────────────── PcapNG Parser ──────────────────────

/// A parser for a pcap CAPS file.
///
/// It updates the bookkeeper one UDP packet at a time.
pub struct Parser {
    cursor: FileCursor,
    stats: StatsCollector,
    file_length: usize,
    pos: usize,
    handles: Vec<std::thread::JoinHandle<()>>,
}

impl Parser {
    /// Creates a new parser for the file.
    pub fn new(file: PathBuf) -> Self {
        let (mut cursor, runners) = FileCursor::new(file, NUM_RUNNERS, BLOCK_SIZE);
        let handles = runners
            .into_iter()
            .map(|mut r| std::thread::spawn(move || r.run()))
            .collect();
        let stats = StatsCollector::new();
        let file_length = cursor.file_length();

        // Setup initial position (skip pcap header).
        let mut pos: usize = 0;

        // Skip SHB (Section Header Block)
        let shb_total_len = cursor.read_u32_le(pos + 4) as usize;
        pos += shb_total_len;

        // Skip IDB (Interface Description Block)
        let idb_total_len = cursor.read_u32_le(pos + 4) as usize;
        pos += idb_total_len;

        Self {
            cursor,
            stats,
            file_length,
            pos,
            handles,
        }
    }

    /// Parse the next block and update the book keeper accordingly.
    pub fn read_next_block(&mut self, bookkeeper: &mut SoaBookKeeper) -> bool {
        if self.pos + MIN_BLOCK_LEN <= self.file_length {
            let block_type = self.cursor.read_u32_le(self.pos);
            let block_total_len = self.cursor.read_u32_le(self.pos + 4) as usize;

            if block_total_len < MIN_BLOCK_LEN {
                return false;
            }

            let block_end = self.pos + block_total_len;
            if block_end > self.file_length {
                return false;
            }

            if block_type == EPB_BLOCK_TYPE && block_total_len > EPB_HEADER_LEN {
                let mut probe = LatencyProbe::start();

                let captured_len = self.cursor.read_u32_le(self.pos + 20) as usize;
                let packet_start = self.pos + EPB_HEADER_LEN;
                let packet_end = packet_start + captured_len;

                if packet_end <= block_end {
                    if let Some(packet_slice) =
                        self.cursor.try_get_slice(packet_start, captured_len)
                    {
                        Parser::fast_process_pcap_block(packet_slice, bookkeeper, &mut probe);
                    } else {
                        self.slow_process_pcap_block(
                            packet_start,
                            captured_len,
                            bookkeeper,
                            &mut probe,
                        );
                    }
                }

                probe.stop(&mut self.stats);
            }

            self.pos = block_end;
        }
        true
    }

    /// Destroys the parser and returns the stats collector.
    pub fn terminate(mut self) -> StatsCollector {
        let _ = self.cursor.get(self.file_length - 1);
        self.handles.into_iter().for_each(|h| h.join().unwrap());
        self.stats
    }

    /// Process the block using the cursor interface.
    ///
    /// This is slow because it is usually used when we are at a block limit.
    /// The cursor writes data from disk to memory block by block, and those blocks are not yet
    /// linear and so we have to be careful when dealing with pcap blocks that are at the limit of one block.
    fn slow_process_pcap_block(
        &mut self,
        packet_start: usize,
        captured_len: usize,
        bookkeeper: &mut SoaBookKeeper,
        probe: &mut LatencyProbe,
    ) {
        let packet_end = packet_start + captured_len;

        // Strip Eth/IP/UDP to find the IEX-TP payload offset
        if let Some(iex_offset) =
            transport::strip_transport(&mut self.cursor, packet_start, captured_len)
        {
            let iex_len = packet_end - iex_offset;
            if iex_len >= 40 {
                // IEX-TP header: message_count at offset 14
                let message_count = self.cursor.read_u16_le(iex_offset + 14);
                let mut msg_pos = iex_offset + 40;

                for _ in 0..message_count {
                    if msg_pos + 2 > packet_end {
                        break;
                    }
                    let message_len = self.cursor.read_u16_le(msg_pos) as usize;
                    probe.mark(message_len as u64);
                    msg_pos += 2;

                    if msg_pos + message_len > packet_end {
                        break;
                    }

                    // Check message type
                    if self.cursor.get(msg_pos) == b'Q' && message_len == 42 {
                        let quote = QuoteUpdateMessage::from_cursor(&mut self.cursor, msg_pos);
                        bookkeeper.update_quote(quote);
                    }

                    msg_pos += message_len;
                }
            }
        }
    }

    /// Process a pcap block in the slice of memory provided.
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
}
