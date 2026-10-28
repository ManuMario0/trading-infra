//! Generic framing layer for byte streams.
//!
//! Implements 4-byte big-endian length-prefix framing over any non-blocking
//! stream implementing `std::io::Read` and `std::io::Write`.
//! All operations handle `WouldBlock` and `Interrupted` correctly.

use std::collections::VecDeque;
use std::io::{self, Read, Write};

/// The protocol header sent before every message.
///
/// We explicitly mandate Big-Endian byte serialization over the wire
/// to ensure multi-architecture safety (e.g., Apple Silicon ARM64 -> Intel x86).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub payload_len: u32,
}

impl FrameHeader {
    pub const BYTES: usize = std::mem::size_of::<u32>();

    #[inline(always)]
    pub fn to_bytes(self) -> [u8; Self::BYTES] {
        self.payload_len.to_be_bytes()
    }

    #[inline(always)]
    pub fn from_bytes(bytes: [u8; Self::BYTES]) -> Self {
        Self {
            payload_len: u32::from_be_bytes(bytes),
        }
    }
}

const HEADER_BYTES: usize = FrameHeader::BYTES;

// ── Read-side state machine ─────────────────────────────────────────────────

/// Per-connection read state.
///
/// Drives a 4-byte length-prefix framing state machine over a non-blocking
/// stream. Call `try_read` whenever the event loop signals that data is readable.
pub struct FrameReader {
    /// Accumulation buffer; resized per phase.
    buf: Vec<u8>,
    /// How many bytes of `buf` are already filled.
    filled: usize,
    /// Total bytes expected for the current phase.
    expected: usize,
    /// False = reading header, true = reading body.
    reading_body: bool,
}

impl Default for FrameReader {
    fn default() -> Self {
        Self {
            buf: vec![0u8; HEADER_BYTES],
            filled: 0,
            expected: HEADER_BYTES,
            reading_body: false,
        }
    }
}

impl FrameReader {
    pub fn new() -> Self {
        Self::default()
    }

    /// Drain the stream and return all complete messages received.
    ///
    /// * `Ok(messages)` — zero or more complete messages.
    /// * `Err(WouldBlock)` → caller should stop reading until next READABLE.
    /// * `Err(ConnectionReset)` → peer closed; caller should clean up.
    /// * `Err(other)` → connection broken.
    pub fn try_read<S: Read>(&mut self, stream: &mut S) -> io::Result<Vec<Vec<u8>>> {
        let mut completed = Vec::new();
        loop {
            match stream.read(&mut self.buf[self.filled..self.expected]) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        "peer closed connection",
                    ));
                }
                Ok(n) => {
                    self.filled += n;
                    if self.filled == self.expected {
                        if self.reading_body {
                            completed.push(self.buf[..self.expected].to_vec());
                            // Reset to header phase
                            self.buf.resize(HEADER_BYTES, 0);
                            self.filled = 0;
                            self.expected = HEADER_BYTES;
                            self.reading_body = false;
                        } else {
                            // Safely parse the exact bytes into our header struct
                            let header = FrameHeader::from_bytes(
                                self.buf[..HEADER_BYTES]
                                    .try_into()
                                    .expect("guaranteed by expected length"),
                            );

                            let body_len = header.payload_len as usize;
                            self.buf.resize(body_len, 0);
                            self.filled = 0;
                            self.expected = body_len;
                            self.reading_body = true;
                        }
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(completed);
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
    }
}

// ── Write-side state machine ────────────────────────────────────────────────

/// Per-connection write state.
///
/// Enqueue serialised messages via `enqueue`; call `try_flush` whenever
/// the event loop signals that the stream is writable.
#[derive(Default)]
pub struct FrameWriter {
    /// FIFO of fully-framed on-wire messages (length prefix + body).
    queue: VecDeque<Vec<u8>>,
    /// Bytes of `queue.front()` already written.
    write_cursor: usize,
}

impl FrameWriter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Frame and enqueue `payload` (body-only bytes; header is prepended here).
    pub fn enqueue(&mut self, payload: Vec<u8>) {
        let header = FrameHeader {
            payload_len: payload.len() as u32,
        };

        let mut frame = Vec::with_capacity(HEADER_BYTES + payload.len());
        frame.extend_from_slice(&header.to_bytes());
        frame.extend_from_slice(&payload);
        self.queue.push_back(frame);
    }

    /// Flush queued frames to `stream`.
    ///
    /// * `Ok(true)` — all data sent.
    /// * `Ok(false)` — more data remains; re-register WRITABLE.
    /// * `Err(WouldBlock)` — same as `Ok(false)`.
    /// * `Err(other)` — connection broken.
    pub fn try_flush<S: Write>(&mut self, stream: &mut S) -> io::Result<bool> {
        while let Some(frame) = self.queue.front() {
            let remaining = &frame[self.write_cursor..];
            match stream.write(remaining) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "write returned 0 bytes",
                    ));
                }
                Ok(n) => {
                    self.write_cursor += n;
                    if self.write_cursor == frame.len() {
                        self.queue.pop_front();
                        self.write_cursor = 0;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(false),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(true)
    }
}
