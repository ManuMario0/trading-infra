//! IP transport implementation.

use std::marker::PhantomData;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Instant;

use crate::address::Address;
use crate::backoff::BackoffTimer;
use crate::framing::{FrameReader, FrameWriter};
use crate::transport::{ConnectorBackend, ListenerBackend, TransportBackend};

pub struct IpTransport<R> {
    stream: TcpStream,

    writer: FrameWriter,
    reader: FrameReader,

    __phantom: PhantomData<R>,
}

impl<R> IpTransport<R> {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            writer: FrameWriter::new(),
            reader: FrameReader::new(),
            __phantom: PhantomData,
        }
    }
}

impl<R> TransportBackend<R> for IpTransport<R> {
    fn try_recv_with<F, E>(&mut self, f: F) -> std::io::Result<Option<R>>
    where
        F: FnOnce(&[u8]) -> Result<R, E>,
    {
        self.reader.try_read_with(&mut self.stream, f)
    }

    fn try_write_with<F, E>(&mut self, payload: &R, f: F) -> std::io::Result<()>
    where
        F: FnOnce(&R, &mut [u8]) -> Result<usize, E>,
    {
        self.writer.try_write_with(&mut self.stream, payload, f)
    }

    fn is_writable(&mut self) -> bool {
        self.writer.is_writable(&mut self.stream)
    }

    fn address(&self) -> Address {
        Address::IP(self.stream.peer_addr().unwrap())
    }
}

pub struct IpConnector<R> {
    remote_addr: SocketAddr,

    backoff: BackoffTimer,

    __phantom: PhantomData<R>,
}

impl<R> IpConnector<R> {
    pub fn new(addr: SocketAddr) -> std::io::Result<Self> {
        Ok(Self {
            remote_addr: addr,
            backoff: BackoffTimer::default_network(),
            __phantom: PhantomData,
        })
    }
}

impl<R> ConnectorBackend<R> for IpConnector<R> {
    type Connection = IpTransport<R>;

    fn try_connect(&mut self) -> std::io::Result<Self::Connection> {
        // Checks timer
        if self.backoff.can_retry(Instant::now()) {
            let stream = TcpStream::connect(self.remote_addr).inspect_err(|_| {
                self.backoff.mark_failed();
            })?;

            // Prepare stream
            stream.set_nodelay(true).inspect_err(|_| {
                self.backoff.mark_failed();
            })?;
            stream.set_nonblocking(true).inspect_err(|_| {
                self.backoff.mark_failed();
            })?;

            self.backoff.reset();

            Ok(IpTransport::new(stream))
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "waiting for cool down",
            ))
        }
    }
}

pub struct IpListener<R> {
    listener: TcpListener,

    backoff: BackoffTimer,

    __phantom: PhantomData<R>,
}

impl<R> IpListener<R> {
    pub fn new(addr: SocketAddr) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            backoff: BackoffTimer::default_network(),
            __phantom: PhantomData,
        })
    }
}

impl<R> ListenerBackend<R> for IpListener<R> {
    type Connection = IpTransport<R>;

    fn try_accept(&mut self) -> std::io::Result<(Self::Connection, crate::address::Address)> {
        if self.backoff.can_retry(Instant::now()) {
            let (stream, addr) = self.listener.accept().inspect_err(|_| {
                self.backoff.mark_failed();
            })?;

            // Prepare stream
            stream.set_nodelay(true).inspect_err(|_| {
                self.backoff.mark_failed();
            })?;
            stream.set_nonblocking(true).inspect_err(|_| {
                self.backoff.mark_failed();
            })?;

            let connection = IpTransport::new(stream);
            let remote_addr = Address::IP(addr);

            self.backoff.reset();

            Ok((connection, remote_addr))
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "waiting for cool down",
            ))
        }
    }
}
