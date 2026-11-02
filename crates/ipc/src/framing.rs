//! Generic framing layer for byte streams.
//!
//! Implements 4-byte big-endian length-prefix framing over any non-blocking
//! stream implementing `std::io::Read` and `std::io::Write`.
//! All operations handle `WouldBlock` and `Interrupted` correctly.

use std::io::{self, Read, Write};

/// The maximum size (in bytes) of a frame that can be sent or received through this interface.
pub const MAX_FRAME_SIZE: usize = 64 * 1024;

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

/// An accumulation reader for fixed-length byte segments.
///
/// Configured with an expected byte count, it reads from a non-blocking stream
/// until the full requested number of bytes has been accumulated.
struct InnerFrameReader {
    /// Fixed-capacity accumulation buffer.
    buf: Box<[u8; MAX_FRAME_SIZE]>,
    /// Number of bytes read into the buffer so far.
    filled: usize,
    /// Number of bytes expected for the current phase.
    expected: usize,
}

impl InnerFrameReader {
    /// Creates a fresh inner frame reader with an allocated accumulation buffer.
    pub fn new() -> Self {
        Self {
            buf: Box::new([0u8; MAX_FRAME_SIZE]),
            filled: 0,
            expected: 0,
        }
    }

    /// Resets the read cursor and sets the expected number of bytes to accumulate.
    ///
    /// # Panics
    ///
    /// Panics if `expected_byte_count` exceeds [`MAX_FRAME_SIZE`].
    pub fn expect(&mut self, expected_byte_count: usize) {
        assert!(expected_byte_count <= MAX_FRAME_SIZE);
        self.filled = 0;
        self.expected = expected_byte_count;
    }

    /// Tries to read the remaining expected bytes from `stream`.
    ///
    /// * Returns `Ok(Some(&buf[..expected]))` once the expected byte count has been accumulated.
    /// * Returns `Ok(None)` if the stream would block before completing the expected count.
    /// * Returns `Err(ConnectionReset)` if the peer closed the connection (`read` returns 0).
    /// * Returns `Err(other)` for any other I/O errors.
    pub fn try_read<S: Read>(&mut self, stream: &mut S) -> io::Result<Option<&[u8]>> {
        match stream.read(&mut self.buf[self.filled..self.expected]) {
            Ok(0) => Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "peer closed connection",
            )),
            Ok(n) => {
                self.filled += n;
                if self.filled == self.expected {
                    Ok(Some(&self.buf[..self.expected]))
                } else {
                    Ok(None)
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
    }
}

/// Per-connection read state.
///
/// Drives a 4-byte length-prefixed framing state machine over a non-blocking
/// stream. Call `try_read_with` (or `try_read`) whenever the event loop signals
/// that the stream is readable.
pub struct FrameReader {
    /// Inner accumulation reader and buffer.
    inner_frame_reader: InnerFrameReader,
    /// `false` = reading header, `true` = reading body.
    reading_body: bool,
}

impl Default for FrameReader {
    fn default() -> Self {
        let mut inner_frame_reader = InnerFrameReader::new();
        inner_frame_reader.expect(HEADER_BYTES);
        Self {
            inner_frame_reader,
            reading_body: false,
        }
    }
}

impl FrameReader {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resets the frame reader to its initial state.
    ///
    /// Useful when a connection is dropped or reset and a clean state is required
    /// without reallocating the underlying buffer.
    #[allow(unused)]
    pub fn reset(&mut self) {
        self.reading_body = false;
        self.inner_frame_reader.expect(HEADER_BYTES);
    }

    /// Tries to read and return a complete message payload as an owned vector.
    ///
    /// * `Ok(Some(message))` — a complete message payload.
    /// * `Ok(None)` — a complete message is not yet available (stream would block).
    /// * `Err(_)` — connection closed or broken.
    ///
    /// Prefer using [`Self::try_read_with`] on performance-sensitive paths, as it avoids
    /// copying the payload slice and allocating a new vector.
    #[allow(unused)]
    pub fn try_read<S: Read>(&mut self, stream: &mut S) -> io::Result<Option<Vec<u8>>> {
        self.try_read_with(stream, |a| Ok::<Vec<u8>, ()>(a.to_vec()))
    }

    /// Tries to read and parse a complete message using the provided closure `f`.
    ///
    /// * `Ok(Some(message))` — a successfully parsed message.
    /// * `Ok(None)` — a complete message is not yet available (stream would block).
    /// * `Err(e)` with [`io::ErrorKind::InvalidData`] — frame exceeds [`MAX_FRAME_SIZE`] or parsing failed.
    /// * `Err(_)` — connection closed or broken.
    pub fn try_read_with<S, F, R, E>(&mut self, stream: &mut S, f: F) -> io::Result<Option<R>>
    where
        S: Read,
        F: FnOnce(&[u8]) -> Result<R, E>,
    {
        if !self.reading_body {
            // Header phase: read and parse the 4-byte length prefix.
            match self.inner_frame_reader.try_read(stream)? {
                Some(value) => {
                    let payload_size =
                        FrameHeader::from_bytes(value[..HEADER_BYTES].try_into().unwrap())
                            .payload_len as usize;
                    if payload_size > MAX_FRAME_SIZE {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "received corrupted frame",
                        ));
                    }
                    if payload_size == 0 {
                        self.inner_frame_reader.expect(HEADER_BYTES);
                        match f(&[]) {
                            Ok(value) => {
                                return Ok(Some(value));
                            }
                            Err(_) => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "unable to parse received data",
                                ));
                            }
                        }
                    }
                    self.reading_body = true;
                    self.inner_frame_reader.expect(payload_size);
                }
                None => {
                    return Ok(None);
                }
            }
        }

        // Body phase: read the full payload and pass it to the parser closure.
        match self.inner_frame_reader.try_read(stream)? {
            Some(payload) => {
                let payload = match f(payload) {
                    Ok(value) => value,
                    Err(_) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "unable to parse received data",
                        ));
                    }
                };
                self.reading_body = false;
                self.inner_frame_reader.expect(HEADER_BYTES);
                Ok(Some(payload))
            }
            None => Ok(None),
        }
    }
}

// ── Write-side state machine ────────────────────────────────────────────────

/// Per-connection write state.
pub struct FrameWriter {
    /// Fully-framed on-wire message.
    buf: Box<[u8; HEADER_BYTES + MAX_FRAME_SIZE]>,
    /// Bytes of the frame already written.
    written: usize,
    /// Size of the frame stored in `buf`.
    frame_size: usize,
}

impl FrameWriter {
    pub fn new() -> Self {
        Self {
            buf: Box::new([0u8; HEADER_BYTES + MAX_FRAME_SIZE]),
            written: 0,
            frame_size: 0,
        }
    }

    /// Frames and enqueues `payload` (body-only bytes; header is prepended here).
    ///
    /// Returns an [`io::ErrorKind::InvalidData`] error if `payload.len()` exceeds [`MAX_FRAME_SIZE`].
    #[allow(unused)]
    pub fn try_write<S: Write>(&mut self, stream: &mut S, payload: &[u8]) -> io::Result<()> {
        let len = payload.len();
        if len > MAX_FRAME_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "payload is too big",
            ));
        };
        let mut static_payload: [u8; MAX_FRAME_SIZE] = [0u8; MAX_FRAME_SIZE];
        static_payload.as_mut_slice().write_all(payload)?;
        self.try_write_with(stream, &static_payload, |payload, mut slice| {
            slice.write(&payload[..len])
        })
    }

    pub fn try_write_with<S: Write, F, R, E>(
        &mut self,
        stream: &mut S,
        payload: &R,
        f: F,
    ) -> io::Result<()>
    where
        F: FnOnce(&R, &mut [u8]) -> Result<usize, E>,
    {
        // First trying to flush any existing frame leftover.
        if self.written != self.frame_size {
            let written = stream.write(&self.buf[self.written..self.frame_size])?;
            self.written += written;
            if self.written != self.frame_size {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "the payload could not be fully written",
                ));
            }
        }

        // Now writing up new frame
        let payload_len = match f(payload, &mut self.buf[HEADER_BYTES..]) {
            Ok(len) => len,
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unable to serialize payload",
                ));
            }
        };

        let header = FrameHeader {
            payload_len: payload_len as u32,
        };

        self.buf[..HEADER_BYTES].copy_from_slice(&header.to_bytes());

        self.frame_size = payload_len + HEADER_BYTES;
        self.written = 0;
        let written = stream.write(&self.buf[self.written..self.frame_size])?;
        self.written += written;

        Ok(())
    }

    pub fn is_writable<S: Write>(&mut self, stream: &mut S) -> bool {
        if self.written != self.frame_size {
            let written = match stream.write(&self.buf[self.written..self.frame_size]) {
                Ok(a) => a,
                Err(_) => {
                    return false;
                }
            };
            self.written += written;
            self.written == self.frame_size
        } else {
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Buffer {
        storage: [u8; 2048],
        write_head: usize,
        read_head: usize,
        max_reads: Vec<usize>,
        max_writes: Vec<usize>,
    }

    impl Buffer {
        fn new(max_reads: Vec<usize>, max_writes: Vec<usize>) -> Self {
            Self {
                storage: [0; 2048],
                write_head: 0,
                read_head: 0,
                max_reads,
                max_writes,
            }
        }
    }

    impl Read for Buffer {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let len = buf
                .len()
                .min(self.write_head - self.read_head)
                .min(self.max_reads.pop().unwrap_or(usize::MAX));
            if len == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "No more bytes to read",
                ));
            }
            let slice = &mut self.storage[self.read_head..(self.read_head + len)];
            buf[0..len].copy_from_slice(slice);
            self.read_head += len;
            Ok(len)
        }
    }

    impl Write for Buffer {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let len = buf
                .len()
                .min(self.storage.len() - self.write_head)
                .min(self.max_writes.pop().unwrap_or(usize::MAX));
            if len == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "No more bytes to write",
                ));
            }
            let slice = &mut self.storage[self.write_head..(self.write_head + len)];
            slice.copy_from_slice(&buf[0..len]);
            self.write_head += len;
            Ok(len)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn send_recv_empty_payload() {
        let payload = vec![];
        let mut buffer = Buffer::new(vec![], vec![]);

        let mut writer = FrameWriter::new();
        let mut reader = FrameReader::new();

        writer.try_write(&mut buffer, &payload.clone()).unwrap();

        let read_result = reader.try_read(&mut buffer);
        assert!(read_result.is_ok());

        assert_eq!(read_result.unwrap(), Some(payload));
    }

    #[test]
    fn send_recv_full_payload() {
        let payload = vec![0, 1, 2, 3, 4];
        let mut buffer = Buffer::new(vec![], vec![]);

        let mut writer = FrameWriter::new();
        let mut reader = FrameReader::new();

        writer.try_write(&mut buffer, &payload.clone()).unwrap();

        let read_result = reader.try_read(&mut buffer);
        assert!(read_result.is_ok());

        assert_eq!(read_result.unwrap(), Some(payload));
    }

    #[test]
    fn partial_flush() {
        let payload = vec![0, 1, 2, 3, 4];
        let mut buffer = Buffer::new(vec![], vec![1, 0]);

        let mut writer = FrameWriter::new();
        let mut reader = FrameReader::new();

        assert!(writer.try_write(&mut buffer, &payload.clone()).is_err());

        assert_eq!(writer.is_writable(&mut buffer), false);

        let read_result = reader.try_read(&mut buffer);
        assert!(read_result.is_ok());

        assert_eq!(read_result.unwrap(), None);

        assert_eq!(writer.is_writable(&mut buffer), true);

        let read_result = reader.try_read(&mut buffer);
        assert!(read_result.is_ok());

        assert_eq!(read_result.unwrap(), Some(payload));
    }

    #[test]
    fn multiple_payload() {
        let payload = vec![0, 1, 2, 3, 4];
        let mut buffer = Buffer::new(vec![], vec![]);

        let mut writer = FrameWriter::new();
        let mut reader = FrameReader::new();

        writer.try_write(&mut buffer, &payload.clone()).unwrap();
        writer.try_write(&mut buffer, &payload.clone()).unwrap();

        assert_eq!(writer.is_writable(&mut buffer), true);

        let read_result = reader.try_read(&mut buffer);
        assert!(read_result.is_ok());

        assert_eq!(read_result.unwrap(), Some(payload.clone()));

        let read_result = reader.try_read(&mut buffer);
        assert!(read_result.is_ok());

        assert_eq!(read_result.unwrap(), Some(payload));
    }

    #[test]
    fn huge_payload() {
        let payload = vec![0u8; MAX_FRAME_SIZE + 1];
        let mut buffer = Buffer::new(vec![], vec![]);

        let mut writer = FrameWriter::new();

        assert!(writer.try_write(&mut buffer, &payload).is_err());
    }
}
