//! Implementations of the highly specialized IPC network engines.

use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use mio::net::{TcpListener, TcpStream};
use mio::{Interest, Waker};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sync::spsc;

use crate::backoff::BackoffTimer;
use crate::dispatcher::NetworkEngine;
use crate::event::PortInfo;
use crate::factory::{Aggregate, Bind, Broadcast, Connect, SocketFactory, TCP};
use crate::framing::{FrameReader, FrameWriter};
use crate::socket::{RecvSocket, SendSocket};
use crate::token::{LocalIndex, TokenRegistry};

/// Set `TCP_NODELAY` on a newly-created mio TCP stream.
///
/// For a 1-second trading engine, Nagle's algorithm can add unacceptable
/// latency for small, frequent messages.
pub(crate) fn set_nodelay(stream: &TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)
}

// ── (TCP + Broadcast + Connect) ──────────────────────────────────

/// The native background engine for `(TCP, Broadcast, Connect)`.
pub struct TcpBroadcastConnectEngine<Msg> {
    /// Generic informations on the port for logging purposes
    port_info: PortInfo,

    addr: SocketAddr,
    queue: spsc::Sender<Msg>,
    registry: TokenRegistry,

    stream: Option<TcpStream>,
    reader: FrameReader,
    backoff: BackoffTimer,

    _marker: PhantomData<Msg>,
}

impl<Msg: DeserializeOwned> TcpBroadcastConnectEngine<Msg> {
    fn new(
        port_info: PortInfo,
        addr: SocketAddr,
        capacity: usize,
        registry: TokenRegistry,
    ) -> (Self, RecvSocket<Msg>) {
        let (tx, rx) = spsc::new(capacity);

        let engine = Self {
            port_info,
            addr,
            queue: tx,
            registry,
            stream: None,
            reader: FrameReader::new(),
            backoff: BackoffTimer::default_network(),
            _marker: PhantomData,
        };

        (engine, RecvSocket::new(rx))
    }

    /// Tries to establish (or re-establish) the connection on a regular interval.
    fn on_tick(&mut self) {
        if self.stream.is_none() && self.backoff.can_retry(std::time::Instant::now()) {
            match TcpStream::connect(self.addr) {
                Ok(mut stream) => {
                    // Try registering with Mio
                    if self
                        .registry
                        .register(&mut stream, LocalIndex(0), Interest::READABLE)
                        .is_ok()
                    {
                        let _ = set_nodelay(&stream);
                        self.stream = Some(stream);
                        self.backoff.reset();
                    } else {
                        self.backoff.mark_failed();
                    }
                }
                Err(_) => {
                    // Typical connection refused. Apply backoff.
                    self.backoff.mark_failed();
                }
            }
        }
    }

    /// Handles readable events fired by Mio on this port's specific token.
    fn on_event(&mut self, _local_index: LocalIndex) {
        if let Some(stream) = &mut self.stream {
            match self.reader.try_read(stream) {
                Ok(messages) => {
                    for raw_msg in messages {
                        // Deserialize the raw framing buffer directly to the typed Struct
                        if let Ok(msg) = postcard::from_bytes::<Msg>(&raw_msg) {
                            // Non-blocking push. If the strategy algorithm is too slow to read
                            // them, we just cleanly drop market data packets.
                            let _ = self.queue.send(msg);
                        }
                    }
                }
                Err(e)
                    if e.kind() == io::ErrorKind::ConnectionReset
                        || e.kind() == io::ErrorKind::Interrupted =>
                {
                    // Connection dropped! Flush the reader buffer and delete the stream.
                    self.stream = None;
                    self.reader = FrameReader::new();
                }
                Err(_) => {
                    // Unexpected structural error. Restart gracefully.
                    self.stream = None;
                    self.reader = FrameReader::new();
                    self.backoff.mark_failed();
                }
            }
        }
    }

    #[inline(always)]
    fn on_wake(&mut self) {} // do nothing, we are not listening to an SPSC channel
}

// ── Factory Implementation ──────────────────────────────────────────────────

impl<Msg: DeserializeOwned> SocketFactory<Msg> for (TCP, Broadcast, Connect) {
    type Engine = TcpBroadcastConnectEngine<Msg>;
    type Handle = RecvSocket<Msg>;

    fn build(
        name: &str,
        addr: &str,
        capacity: usize,
        registry: TokenRegistry,
        _waker: Arc<Waker>,
    ) -> (Self::Engine, Self::Handle) {
        let parsed_addr: SocketAddr = addr.parse().expect("Invalid SocketAddr for TCP connect");
        let port_info = PortInfo {
            port_name: name.to_string(),
            addr: addr.to_owned(),
            transport: "TCP".to_string(),
            kind: "Broadcast".to_string(),
            role: "Connect".to_string(),
            capacity,
        };
        TcpBroadcastConnectEngine::new(port_info, parsed_addr, capacity, registry)
    }
}

impl<Msg: DeserializeOwned> NetworkEngine for TcpBroadcastConnectEngine<Msg> {
    fn on_event(&mut self, local_index: LocalIndex) {
        self.on_event(local_index);
    }

    fn on_tick(&mut self) {
        self.on_tick();
    }

    fn on_wake(&mut self) {
        self.on_wake();
    }
}

// ── (TCP + Aggregate + Connect) ──────────────────────────────────

/// The native background engine for `(TCP, Aggregate, Connect)`.
pub struct TcpAggregateConnectEngine<Msg> {
    /// Generic informations on the port for logging purposes
    port_info: PortInfo,

    addr: SocketAddr,
    queue: spsc::Receiver<Msg>,
    waker: Arc<Waker>,
    registry: TokenRegistry,

    stream: Option<TcpStream>,
    writer: FrameWriter,
    backoff: BackoffTimer,

    _marker: PhantomData<Msg>,
}

impl<Msg: Serialize> TcpAggregateConnectEngine<Msg> {
    fn new(
        port_info: PortInfo,
        addr: SocketAddr,
        capacity: usize,
        registry: TokenRegistry,
        waker: Arc<Waker>,
    ) -> (Self, SendSocket<Msg>) {
        let (tx, rx) = spsc::new(capacity);

        let engine = Self {
            port_info,
            addr,
            queue: rx,
            waker: waker.clone(),
            registry,
            stream: None,
            writer: FrameWriter::new(),
            backoff: BackoffTimer::default_network(),
            _marker: PhantomData,
        };

        (engine, SendSocket::new(tx, waker))
    }

    /// Tries to establish (or re-establish) the connection on a regular interval.
    fn on_tick(&mut self) {
        if self.stream.is_none() && self.backoff.can_retry(std::time::Instant::now()) {
            match TcpStream::connect(self.addr) {
                Ok(mut stream) => {
                    // We register for writable events to be notified when the connection is closed.
                    let _ = self
                        .registry
                        .register(&mut stream, LocalIndex(1), Interest::WRITABLE);
                    let _ = set_nodelay(&stream);
                    self.stream = Some(stream);
                    self.backoff.reset();
                }
                Err(_) => {
                    // Typical connection refused. Apply backoff.
                    self.backoff.mark_failed();
                }
            }
        }
    }

    /// Handles readable events fired by Mio on this port's specific token.
    fn on_event(&mut self, local_index: LocalIndex) {
        if let Some(stream) = &mut self.stream {
            // We don't deal with the waker as this is processed externally
            match local_index.0 {
                1 => {
                    match self.writer.try_flush(stream) {
                        Ok(_) => {}
                        Err(e) => {
                            if e.kind() == io::ErrorKind::WouldBlock {
                            } else {
                                // Unexpected structural error. Restart gracefully.
                                // Note that we drop packets here (the ones in flight in the writer).
                                self.stream = None;
                                self.writer = FrameWriter::new();
                                self.backoff.mark_failed();
                            }
                        }
                    }
                }
                _ => {
                    unreachable!();
                }
            }
        }
    }

    #[inline(always)]
    /// Triggered when the waker is called
    fn on_wake(&mut self) {
        if let Some(stream) = &mut self.stream {
            while let Some(msg) = self.queue.recv() {
                // Expensive allocation here for now
                self.writer.enqueue(postcard::to_allocvec(&msg).unwrap());
                match self.writer.try_flush(stream) {
                    Ok(_) => {}
                    Err(e) => {
                        if e.kind() == io::ErrorKind::WouldBlock {
                            // We simply ignore and continue, should be recorded eventually for telemetry
                            break;
                        } else {
                            // Unexpected structural error. Restart gracefully.
                            // Note that we drop packets here (the ones in flight in the writer).
                            self.stream = None;
                            self.writer = FrameWriter::new();
                            self.backoff.mark_failed();
                            break;
                        }
                    }
                }
            }
        }
    }
}

// ── Factory Implementation ──────────────────────────────────────────────────

impl<Msg: Serialize> SocketFactory<Msg> for (TCP, Aggregate, Connect) {
    type Engine = TcpAggregateConnectEngine<Msg>;
    type Handle = SendSocket<Msg>;

    fn build(
        name: &str,
        addr: &str,
        capacity: usize,
        registry: TokenRegistry,
        waker: Arc<Waker>,
    ) -> (Self::Engine, Self::Handle) {
        let parsed_addr: SocketAddr = addr.parse().expect("Invalid SocketAddr for TCP connect");
        let port_info = PortInfo {
            port_name: name.to_string(),
            addr: addr.to_owned(),
            transport: "TCP".to_string(),
            kind: "Aggregate".to_string(),
            role: "Connect".to_string(),
            capacity,
        };
        TcpAggregateConnectEngine::new(port_info, parsed_addr, capacity, registry, waker)
    }
}

impl<Msg: Serialize> NetworkEngine for TcpAggregateConnectEngine<Msg> {
    fn on_event(&mut self, local_index: LocalIndex) {
        self.on_event(local_index);
    }

    fn on_tick(&mut self) {
        self.on_tick();
    }

    fn on_wake(&mut self) {
        self.on_wake();
    }
}

// ── (TCP + Broadcast + Bind) ──────────────────────────────────

const MAX_CLIENTS: usize = 1024;
const LISTENER_INDEX: usize = MAX_CLIENTS;

/// The native background engine for `(TCP, Broadcast, Bind)`.
///
/// The subtlety here is that we need to keep track of all the clients,
/// so the management of tokens is a bit more complex.
///
/// The tokens 0..MAX_CLIENTS-1 are to register the clients for writing,
/// the token MAX_CLIENTS is to register the listener, and MAX_CLIENT+1 is
/// for the waker.
pub struct TcpBroadcastBindEngine<Msg> {
    /// Generic informations on the port for logging purposes
    port_info: PortInfo,

    addr: SocketAddr,
    queue: spsc::Receiver<Msg>,
    waker: Arc<Waker>,
    registry: TokenRegistry,

    listener: Option<TcpListener>,
    backoff: BackoffTimer,

    clients: [Option<BroadcastClient>; MAX_CLIENTS],

    _marker: PhantomData<Msg>,
}

struct BroadcastClient {
    stream: TcpStream,
    writer: FrameWriter,
    remote_addr: SocketAddr,
}

impl<Msg: Serialize> TcpBroadcastBindEngine<Msg> {
    fn new(
        port_info: PortInfo,
        addr: SocketAddr,
        capacity: usize,
        registry: TokenRegistry,
        waker: Arc<Waker>,
    ) -> (Self, SendSocket<Msg>) {
        let (tx, rx) = spsc::new(capacity);

        let engine = Self {
            port_info,
            addr,
            queue: rx,
            waker: waker.clone(),
            registry,
            clients: [const { None }; MAX_CLIENTS],
            _marker: PhantomData,
            listener: None,
            backoff: BackoffTimer::default_network(),
        };

        (engine, SendSocket::new(tx, waker))
    }

    /// Tries to establish (or re-establish) the connection on a regular interval.
    fn on_tick(&mut self) {
        if self.listener.is_none() && self.backoff.can_retry(std::time::Instant::now()) {
            match TcpListener::bind(self.addr) {
                Ok(mut listener) => {
                    // We register for writable events to be notified when the connection is closed.
                    let _ = self.registry.register(
                        &mut listener,
                        LocalIndex(LISTENER_INDEX),
                        Interest::READABLE,
                    );
                    self.listener = Some(listener);
                    self.backoff.reset();
                }
                Err(_) => {
                    // Typical connection refused. Apply backoff.
                    self.backoff.mark_failed();
                }
            }
        }
    }

    /// Handles readable events fired by Mio on this port's specific token.
    fn on_event(&mut self, local_index: LocalIndex) {
        if let Some(listener) = &mut self.listener {
            match local_index.0 {
                LISTENER_INDEX => {
                    match listener.accept() {
                        Ok((mut stream, addr)) => {
                            let available_slot =
                                self.clients.iter().position(|client| client.is_none());
                            if available_slot.is_none() {
                                // TODO: log error
                                return;
                            }
                            let slot = available_slot.unwrap();
                            let _ = set_nodelay(&stream);
                            // We register for writable events to be notified when the connection is closed.
                            let _ = self.registry.register(
                                &mut stream,
                                LocalIndex(slot),
                                Interest::WRITABLE,
                            );
                            self.clients[slot] = Some(BroadcastClient {
                                stream,
                                writer: FrameWriter::new(),
                                remote_addr: addr,
                            });
                        }
                        Err(_) => {
                            // Typical connection refused. Apply backoff.
                            self.backoff.mark_failed();
                        }
                    }
                }
                _ => {
                    // This handles `LocalIndex(slot)` for clients (0..MAX_CLIENTS)
                    let slot = local_index.0;
                    if slot < MAX_CLIENTS {
                        let mut disconnect = false;
                        if let Some(client) = &mut self.clients[slot] {
                            match client.writer.try_flush(&mut client.stream) {
                                Ok(_) => {}
                                Err(e) => {
                                    if e.kind() == io::ErrorKind::WouldBlock {
                                    } else {
                                        disconnect = true;
                                    }
                                }
                            }
                        }
                        if disconnect {
                            self.clients[slot] = None;
                        }
                    }
                }
            }
        }
    }

    #[inline(always)]
    fn on_wake(&mut self) {
        while let Some(msg) = self.queue.recv() {
            // Bonus optimization: serialize once for all clients!
            let serialized = postcard::to_allocvec(&msg).unwrap();
            for client_opt in self.clients.iter_mut() {
                let mut disconnect = false;
                if let Some(client) = client_opt {
                    client.writer.enqueue(serialized.clone());
                    match client.writer.try_flush(&mut client.stream) {
                        Ok(_) => {}
                        Err(e) => {
                            if e.kind() == io::ErrorKind::WouldBlock {
                                // We simply ignore and continue, should be recorded eventually for telemetry
                            } else {
                                disconnect = true;
                            }
                        }
                    }
                }
                if disconnect {
                    *client_opt = None;
                }
            }
        }
    }
}

// ── Factory Implementation ──────────────────────────────────────────────────

impl<Msg: Serialize> SocketFactory<Msg> for (TCP, Broadcast, Bind) {
    type Engine = TcpBroadcastBindEngine<Msg>;
    type Handle = SendSocket<Msg>;

    fn build(
        name: &str,
        addr: &str,
        capacity: usize,
        registry: TokenRegistry,
        waker: Arc<Waker>,
    ) -> (Self::Engine, Self::Handle) {
        let parsed_addr: SocketAddr = addr.parse().expect("Invalid SocketAddr for TCP connect");
        let port_info = PortInfo {
            port_name: name.to_string(),
            addr: addr.to_owned(),
            transport: "TCP".to_string(),
            kind: "Broadcast".to_string(),
            role: "Bind".to_string(),
            capacity,
        };
        TcpBroadcastBindEngine::new(port_info, parsed_addr, capacity, registry, waker)
    }
}

impl<Msg: Serialize> NetworkEngine for TcpBroadcastBindEngine<Msg> {
    fn on_event(&mut self, local_index: LocalIndex) {
        self.on_event(local_index);
    }

    fn on_tick(&mut self) {
        self.on_tick();
    }

    fn on_wake(&mut self) {
        self.on_wake();
    }
}

// ── (TCP + Aggregate + Bind) ──────────────────────────────────

/// The native background engine for `(TCP, Aggregate, Bind)`.
pub struct TcpAggregateBindEngine<Msg> {
    /// Generic informations on the port for logging purposes
    port_info: PortInfo,

    addr: SocketAddr,
    queue: spsc::Sender<Msg>,
    registry: TokenRegistry,

    listener: Option<TcpListener>,
    backoff: BackoffTimer,

    clients: [Option<AggregateClient>; MAX_CLIENTS],

    _marker: PhantomData<Msg>,
}

struct AggregateClient {
    stream: TcpStream,
    reader: FrameReader,
    remote_addr: SocketAddr,
}

impl<Msg: DeserializeOwned> TcpAggregateBindEngine<Msg> {
    fn new(
        port_info: PortInfo,
        addr: SocketAddr,
        capacity: usize,
        registry: TokenRegistry,
    ) -> (Self, RecvSocket<Msg>) {
        let (tx, rx) = spsc::new(capacity);

        let engine = Self {
            port_info,
            addr,
            queue: tx,
            registry,
            clients: [const { None }; MAX_CLIENTS],
            _marker: PhantomData,
            listener: None,
            backoff: BackoffTimer::default_network(),
        };

        (engine, RecvSocket::new(rx))
    }

    /// Tries to establish (or re-establish) the connection on a regular interval.
    fn on_tick(&mut self) {
        if self.listener.is_none() && self.backoff.can_retry(std::time::Instant::now()) {
            match TcpListener::bind(self.addr) {
                Ok(mut listener) => {
                    let _ = self.registry.register(
                        &mut listener,
                        LocalIndex(LISTENER_INDEX),
                        Interest::READABLE,
                    );
                    self.listener = Some(listener);
                    self.backoff.reset();
                }
                Err(_) => {
                    // Typical connection refused. Apply backoff.
                    self.backoff.mark_failed();
                }
            }
        }
    }

    /// Handles readable events fired by Mio on this port's specific token.
    fn on_event(&mut self, local_index: LocalIndex) {
        if let Some(listener) = &mut self.listener {
            match local_index.0 {
                LISTENER_INDEX => {
                    match listener.accept() {
                        Ok((mut stream, addr)) => {
                            let available_slot =
                                self.clients.iter().position(|client| client.is_none());
                            if available_slot.is_none() {
                                // TODO: log error
                                return;
                            }
                            let slot = available_slot.unwrap();
                            let _ = set_nodelay(&stream);
                            // We register for readable events to receive incoming data.
                            let _ = self.registry.register(
                                &mut stream,
                                LocalIndex(slot),
                                Interest::READABLE,
                            );
                            self.clients[slot] = Some(AggregateClient {
                                stream,
                                reader: FrameReader::new(),
                                remote_addr: addr,
                            });
                        }
                        Err(_) => {
                            // Typical connection refused. Apply backoff.
                            self.backoff.mark_failed();
                        }
                    }
                }
                _ => {
                    // This handles `LocalIndex(slot)` for clients (0..MAX_CLIENTS)
                    let slot = local_index.0;
                    if slot < MAX_CLIENTS {
                        let mut disconnect = false;
                        if let Some(client) = &mut self.clients[slot] {
                            match client.reader.try_read(&mut client.stream) {
                                Ok(messages) => {
                                    for raw_msg in messages {
                                        if let Ok(msg) = postcard::from_bytes::<Msg>(&raw_msg) {
                                            // Provide backpressure by dropping packets cleanly
                                            // if the receiver is too slow.
                                            let _ = self.queue.send(msg);
                                        }
                                    }
                                }
                                Err(e)
                                    if e.kind() == io::ErrorKind::ConnectionReset
                                        || e.kind() == io::ErrorKind::Interrupted =>
                                {
                                    disconnect = true;
                                }
                                Err(_) => {
                                    disconnect = true;
                                }
                            }
                        }
                        if disconnect {
                            self.clients[slot] = None;
                        }
                    }
                }
            }
        }
    }

    fn on_wake(&mut self) {}
}

// ── Factory Implementation ──────────────────────────────────────────────────

impl<Msg: DeserializeOwned> SocketFactory<Msg> for (TCP, Aggregate, Bind) {
    type Engine = TcpAggregateBindEngine<Msg>;
    type Handle = RecvSocket<Msg>;

    fn build(
        name: &str,
        addr: &str,
        capacity: usize,
        registry: TokenRegistry,
        _waker: Arc<Waker>,
    ) -> (Self::Engine, Self::Handle) {
        let parsed_addr: SocketAddr = addr.parse().expect("Invalid SocketAddr for TCP bind");
        let port_info = PortInfo {
            port_name: name.to_string(),
            addr: addr.to_owned(),
            transport: "TCP".to_string(),
            kind: "Aggregate".to_string(),
            role: "Bind".to_string(),
            capacity,
        };
        TcpAggregateBindEngine::new(port_info, parsed_addr, capacity, registry)
    }
}

impl<Msg: DeserializeOwned> NetworkEngine for TcpAggregateBindEngine<Msg> {
    fn on_event(&mut self, local_index: LocalIndex) {
        self.on_event(local_index);
    }

    fn on_tick(&mut self) {
        self.on_tick();
    }

    fn on_wake(&mut self) {
        self.on_wake();
    }
}
