//! Implementations of the highly specialized IPC network engines.

use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use cachec::channel::spsc;
use journal::entry::{LogLevel, ThreadKind};
use journal::journal::Journal;
use mio::net::{TcpListener, TcpStream};
use mio::{Interest, Waker};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::backoff::BackoffTimer;
use crate::dispatcher::NetworkEngine;
use crate::event::{NetworkEvent, NetworkEventKind, PortInfo};
use crate::factory::{Aggregate, Bind, Broadcast, Connect, PointToPoint, SocketFactory, TCP};
use crate::framing::{FrameReader, FrameWriter};
use crate::socket::CHANNEL_SIZE;
use crate::socket::{RecvSocket, SendSocket};
use crate::token::{LocalIndex, TokenRegistry};

/// Set `TCP_NODELAY` on a newly-created mio TCP stream.
///
/// For a 1-second trading engine, Nagle's algorithm can add unacceptable
/// latency for small, frequent messages.
pub(crate) fn set_nodelay(stream: &TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)
}

/// Helper to fire a network event to the journal with minimal boilerplate.
#[inline(always)]
fn log_event(journal: &mut Journal, port_info: &PortInfo, level: LogLevel, kind: NetworkEventKind) {
    let event = NetworkEvent {
        port_info: port_info.clone(),
        kind,
    };
    let _ = journal.log(ThreadKind::Network, vec![], level, event);
}

// ── (TCP + Broadcast + Connect) ──────────────────────────────────

/// The native background engine for `(TCP, Broadcast, Connect)`.
pub struct TcpBroadcastConnectEngine<Msg> {
    /// Generic informations on the port for logging purposes
    port_info: PortInfo,

    addr: SocketAddr,
    queue: spsc::Producer<Msg, CHANNEL_SIZE>,
    registry: TokenRegistry,

    stream: Option<TcpStream>,
    reader: FrameReader,
    backoff: BackoffTimer,
    is_established: bool,

    _marker: PhantomData<Msg>,
}

impl<Msg: DeserializeOwned> TcpBroadcastConnectEngine<Msg> {
    fn new(
        port_info: PortInfo,
        addr: SocketAddr,
        registry: TokenRegistry,
    ) -> (Self, RecvSocket<Msg>) {
        let (tx, rx) = spsc::Channel::new().split();

        let engine = Self {
            port_info,
            addr,
            queue: tx,
            registry,
            stream: None,
            reader: FrameReader::new(),
            backoff: BackoffTimer::default_network(),
            is_established: false,
            _marker: PhantomData,
        };

        (engine, RecvSocket::new(rx))
    }

    /// Tries to establish (or re-establish) the connection on a regular interval.
    fn on_tick(&mut self, journal: &mut Journal) {
        if self.stream.is_none() && self.backoff.can_retry(std::time::Instant::now()) {
            log_event(
                journal,
                &self.port_info,
                LogLevel::Info,
                NetworkEventKind::Reconnecting {
                    attempt: self.backoff.attempt(),
                },
            );
            match TcpStream::connect(self.addr) {
                Ok(mut stream) => {
                    // We register for readable events to receive actual data and detect disconnection.
                    let _ = self.registry.register(
                        &mut stream,
                        0.into(),
                        Interest::READABLE | Interest::WRITABLE,
                    );

                    let _ = set_nodelay(&stream);
                    self.stream = Some(stream);
                }
                Err(_) => {
                    self.backoff.mark_failed();
                }
            }
        }

        // Timeout detection
        if self.backoff.is_maxed() {
            log_event(
                journal,
                &self.port_info,
                LogLevel::Error,
                NetworkEventKind::Timeout,
            );
        }
    }

    /// Handles readable events fired by Mio on this port's specific token.
    fn on_event(&mut self, local_index: LocalIndex, journal: &mut Journal) {
        if let Some(stream) = &mut self.stream {
            if usize::from(local_index) == 0 {
                if !self.is_established {
                    match stream.take_error() {
                        Ok(None) => {
                            self.is_established = true;
                            self.backoff.reset();
                            log_event(
                                journal,
                                &self.port_info,
                                LogLevel::Info,
                                NetworkEventKind::Connected { peer: self.addr },
                            );
                        }
                        _ => {
                            self.stream = None;
                            self.backoff.mark_failed();
                        }
                    }
                } else {
                    match self.reader.try_read(stream) {
                        Ok(messages) => {
                            for raw_msg in messages {
                                // Deserialize the raw framing buffer directly to the typed Struct
                                if let Ok(msg) = postcard::from_bytes::<Msg>(&raw_msg)
                                    && self.queue.try_write(msg).is_some()
                                {
                                    log_event(
                                        journal,
                                        &self.port_info,
                                        LogLevel::Warn,
                                        NetworkEventKind::ChannelFull,
                                    );
                                }
                            }
                        }
                        Err(e)
                            if e.kind() == io::ErrorKind::ConnectionReset
                                || e.kind() == io::ErrorKind::Interrupted =>
                        {
                            if !self.is_established {
                                self.stream = None;
                                self.backoff.mark_failed();
                                return;
                            }
                            log_event(
                                journal,
                                &self.port_info,
                                LogLevel::Warn,
                                NetworkEventKind::Disconnected { peer: self.addr },
                            );
                            self.stream = None;
                            self.is_established = false;
                            self.reader = FrameReader::new();
                        }
                        Err(_) => {
                            if !self.is_established {
                                self.stream = None;
                                self.backoff.mark_failed();
                                return;
                            }
                            log_event(
                                journal,
                                &self.port_info,
                                LogLevel::Error,
                                NetworkEventKind::Disconnected { peer: self.addr },
                            );
                            self.stream = None;
                            self.is_established = false;
                            self.reader = FrameReader::new();
                            self.backoff.mark_failed();
                        }
                    }
                }
            } else {
                unreachable!()
            }
        }
    }

    #[inline(always)]
    fn on_wake(&mut self, _journal: &mut Journal) {
        // do nothing, we are not listening to an SPSC channel
    }
}

// ── Factory Implementation ──────────────────────────────────────────────────

impl<Msg: DeserializeOwned> SocketFactory<Msg, ()> for (TCP, Broadcast, Connect) {
    type Engine = TcpBroadcastConnectEngine<Msg>;
    type Handle = RecvSocket<Msg>;

    fn build(
        name: &str,
        addr: &str,
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
        };
        TcpBroadcastConnectEngine::new(port_info, parsed_addr, registry)
    }
}

impl<Msg: DeserializeOwned> NetworkEngine for TcpBroadcastConnectEngine<Msg> {
    fn on_event(&mut self, local_index: LocalIndex, journal: &mut Journal) {
        self.on_event(local_index, journal);
    }

    fn on_tick(&mut self, journal: &mut Journal) {
        self.on_tick(journal);
    }

    fn on_wake(&mut self, journal: &mut Journal) {
        self.on_wake(journal);
    }
}

// ── (TCP + Aggregate + Connect) ──────────────────────────────────

/// The native background engine for `(TCP, Aggregate, Connect)`.
pub struct TcpAggregateConnectEngine<Msg> {
    /// Generic informations on the port for logging purposes
    port_info: PortInfo,

    addr: SocketAddr,
    queue: spsc::Consumer<Msg, CHANNEL_SIZE>,
    registry: TokenRegistry,

    stream: Option<TcpStream>,
    writer: FrameWriter,
    backoff: BackoffTimer,

    is_established: bool,
    _marker: PhantomData<Msg>,
}

impl<Msg: Serialize> TcpAggregateConnectEngine<Msg> {
    fn new(
        port_info: PortInfo,
        addr: SocketAddr,
        registry: TokenRegistry,
        waker: Arc<Waker>,
    ) -> (Self, SendSocket<Msg>) {
        let (tx, rx) = spsc::Channel::new().split();

        let engine = Self {
            port_info,
            addr,
            queue: rx,
            registry,
            stream: None,
            writer: FrameWriter::new(),
            backoff: BackoffTimer::default_network(),
            is_established: false,
            _marker: PhantomData,
        };

        (engine, SendSocket::new(tx, waker))
    }

    /// Tries to establish (or re-establish) the connection on a regular interval.
    fn on_tick(&mut self, journal: &mut Journal) {
        if self.stream.is_none() && self.backoff.can_retry(std::time::Instant::now()) {
            log_event(
                journal,
                &self.port_info,
                LogLevel::Info,
                NetworkEventKind::Reconnecting {
                    attempt: self.backoff.attempt(),
                },
            );
            match TcpStream::connect(self.addr) {
                Ok(mut stream) => {
                    // We register for writable events to identify when the connection is established.
                    let _ = self
                        .registry
                        .register(&mut stream, 1.into(), Interest::WRITABLE);
                    // We also register for readable events to identify when the connection is closed.
                    let _ = self
                        .registry
                        .register(&mut stream, 2.into(), Interest::READABLE);

                    let _ = set_nodelay(&stream);
                    self.stream = Some(stream);
                }
                Err(_) => {
                    // Typical connection refused. Apply backoff.
                    self.backoff.mark_failed();
                }
            }
        }

        // Timeout detection
        if self.backoff.is_maxed() {
            log_event(
                journal,
                &self.port_info,
                LogLevel::Error,
                NetworkEventKind::Timeout,
            );
        }
    }

    /// Handles readable events fired by Mio on this port's specific token.
    fn on_event(&mut self, local_index: LocalIndex, journal: &mut Journal) {
        if let Some(stream) = &mut self.stream {
            match usize::from(local_index) {
                1 => {
                    if !self.is_established {
                        match stream.take_error() {
                            Ok(None) => {
                                self.is_established = true;
                                self.backoff.reset();
                                log_event(
                                    journal,
                                    &self.port_info,
                                    LogLevel::Info,
                                    NetworkEventKind::Connected { peer: self.addr },
                                );
                            }
                            _ => {
                                self.stream = None;
                                self.backoff.mark_failed();
                                return;
                            }
                        }
                    }

                    match self.writer.try_flush(stream) {
                        Ok(_) => {}
                        Err(e) => {
                            if e.kind() == io::ErrorKind::WouldBlock {
                            } else {
                                log_event(
                                    journal,
                                    &self.port_info,
                                    LogLevel::Error,
                                    NetworkEventKind::Disconnected { peer: self.addr },
                                );
                                self.stream = None;
                                self.is_established = false;
                                self.writer = FrameWriter::new();
                                self.backoff.mark_failed();
                            }
                        }
                    }
                }
                2 => {
                    let mut buf = [0u8; 1];
                    use std::io::Read;
                    match stream.read(&mut buf) {
                        Ok(0) | Err(_) => {
                            log_event(
                                journal,
                                &self.port_info,
                                LogLevel::Error,
                                NetworkEventKind::Disconnected { peer: self.addr },
                            );
                            self.stream = None;
                            self.is_established = false;
                            self.writer = FrameWriter::new();
                            self.backoff.mark_failed();
                        }
                        _ => {}
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
    fn on_wake(&mut self, journal: &mut Journal) {
        if let Some(stream) = &mut self.stream {
            while let Some(msg) = self.queue.try_read() {
                self.writer.enqueue(postcard::to_allocvec(&msg).unwrap());
                match self.writer.try_flush(stream) {
                    Ok(_) => {}
                    Err(e) => {
                        if e.kind() == io::ErrorKind::WouldBlock {
                            break;
                        } else {
                            log_event(
                                journal,
                                &self.port_info,
                                LogLevel::Error,
                                NetworkEventKind::Disconnected { peer: self.addr },
                            );
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

impl<Msg: Serialize> SocketFactory<Msg, ()> for (TCP, Aggregate, Connect) {
    type Engine = TcpAggregateConnectEngine<Msg>;
    type Handle = SendSocket<Msg>;

    fn build(
        name: &str,
        addr: &str,
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
        };
        TcpAggregateConnectEngine::new(port_info, parsed_addr, registry, waker)
    }
}

impl<Msg: Serialize> NetworkEngine for TcpAggregateConnectEngine<Msg> {
    fn on_event(&mut self, local_index: LocalIndex, journal: &mut Journal) {
        self.on_event(local_index, journal);
    }

    fn on_tick(&mut self, journal: &mut Journal) {
        self.on_tick(journal);
    }

    fn on_wake(&mut self, journal: &mut Journal) {
        self.on_wake(journal);
    }
}

// ── (TCP + Broadcast + Bind) ──────────────────────────────────

const MAX_CLIENTS: usize = 32;
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
    queue: spsc::Consumer<Msg, CHANNEL_SIZE>,
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
        registry: TokenRegistry,
        waker: Arc<Waker>,
    ) -> (Self, SendSocket<Msg>) {
        let (tx, rx) = spsc::Channel::new().split();

        let engine = Self {
            port_info,
            addr,
            queue: rx,
            registry,
            clients: [const { None }; MAX_CLIENTS],
            _marker: PhantomData,
            listener: None,
            backoff: BackoffTimer::default_network(),
        };

        (engine, SendSocket::new(tx, waker))
    }

    /// Tries to establish (or re-establish) the bind on a regular interval.
    fn on_tick(&mut self, journal: &mut Journal) {
        if self.listener.is_none() && self.backoff.can_retry(std::time::Instant::now()) {
            log_event(
                journal,
                &self.port_info,
                LogLevel::Info,
                NetworkEventKind::Reconnecting {
                    attempt: self.backoff.attempt(),
                },
            );
            match TcpListener::bind(self.addr) {
                Ok(mut listener) => {
                    let _ = self.registry.register(
                        &mut listener,
                        LISTENER_INDEX.into(),
                        Interest::READABLE,
                    );
                    self.listener = Some(listener);
                    self.backoff.reset();
                    log_event(
                        journal,
                        &self.port_info,
                        LogLevel::Info,
                        NetworkEventKind::Connected { peer: self.addr },
                    );
                }
                Err(_) => {
                    self.backoff.mark_failed();
                }
            }
        }

        // Timeout detection
        if self.backoff.is_maxed() {
            log_event(
                journal,
                &self.port_info,
                LogLevel::Error,
                NetworkEventKind::Timeout,
            );
        }
    }

    /// Handles readable events fired by Mio on this port's specific token.
    fn on_event(&mut self, local_index: LocalIndex, journal: &mut Journal) {
        if let Some(listener) = &mut self.listener {
            match usize::from(local_index) {
                LISTENER_INDEX => match listener.accept() {
                    Ok((mut stream, addr)) => {
                        let available_slot =
                            self.clients.iter().position(|client| client.is_none());
                        if available_slot.is_none() {
                            log_event(
                                journal,
                                &self.port_info,
                                LogLevel::Error,
                                NetworkEventKind::ChannelFull,
                            );
                            return;
                        }
                        let slot = available_slot.unwrap();
                        let _ = set_nodelay(&stream);
                        let _ =
                            self.registry
                                .register(&mut stream, slot.into(), Interest::WRITABLE);
                        log_event(
                            journal,
                            &self.port_info,
                            LogLevel::Info,
                            NetworkEventKind::Connected { peer: addr },
                        );
                        self.clients[slot] = Some(BroadcastClient {
                            stream,
                            writer: FrameWriter::new(),
                            remote_addr: addr,
                        });
                    }
                    Err(_) => {
                        self.backoff.mark_failed();
                    }
                },
                _ => {
                    let slot = usize::from(local_index);
                    if slot < MAX_CLIENTS {
                        let mut disconnect = false;
                        let mut peer_addr = None;
                        if let Some(client) = &mut self.clients[slot] {
                            match client.writer.try_flush(&mut client.stream) {
                                Ok(_) => {}
                                Err(e) => {
                                    if e.kind() != io::ErrorKind::WouldBlock {
                                        peer_addr = Some(client.remote_addr);
                                        disconnect = true;
                                    }
                                }
                            }
                        }
                        if disconnect {
                            if let Some(addr) = peer_addr {
                                log_event(
                                    journal,
                                    &self.port_info,
                                    LogLevel::Warn,
                                    NetworkEventKind::Disconnected { peer: addr },
                                );
                            }
                            self.clients[slot] = None;
                        }
                    }
                }
            }
        }
    }

    #[inline(always)]
    fn on_wake(&mut self, journal: &mut Journal) {
        for _ in 0..CHANNEL_SIZE {
            if let Some(msg) = self.queue.try_read() {
                let serialized = postcard::to_allocvec(&msg).unwrap();
                for client_opt in self.clients.iter_mut() {
                    let mut disconnect = false;
                    let mut peer_addr = None;
                    if let Some(client) = client_opt {
                        client.writer.enqueue(serialized.clone());
                        match client.writer.try_flush(&mut client.stream) {
                            Ok(_) => {}
                            Err(e) => {
                                if e.kind() != io::ErrorKind::WouldBlock {
                                    peer_addr = Some(client.remote_addr);
                                    disconnect = true;
                                }
                            }
                        }
                    }
                    if disconnect {
                        if let Some(addr) = peer_addr {
                            log_event(
                                journal,
                                &self.port_info,
                                LogLevel::Warn,
                                NetworkEventKind::Disconnected { peer: addr },
                            );
                        }
                        *client_opt = None;
                    }
                }
            } else {
                break;
            }
        }
    }
}

// ── Factory Implementation ──────────────────────────────────────────────────

impl<Msg: Serialize> SocketFactory<Msg, ()> for (TCP, Broadcast, Bind) {
    type Engine = TcpBroadcastBindEngine<Msg>;
    type Handle = SendSocket<Msg>;

    fn build(
        name: &str,
        addr: &str,
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
        };
        TcpBroadcastBindEngine::new(port_info, parsed_addr, registry, waker)
    }
}

impl<Msg: Serialize> NetworkEngine for TcpBroadcastBindEngine<Msg> {
    fn on_event(&mut self, local_index: LocalIndex, journal: &mut Journal) {
        self.on_event(local_index, journal);
    }

    fn on_tick(&mut self, journal: &mut Journal) {
        self.on_tick(journal);
    }

    fn on_wake(&mut self, journal: &mut Journal) {
        self.on_wake(journal);
    }
}

// ── (TCP + Aggregate + Bind) ──────────────────────────────────

/// The native background engine for `(TCP, Aggregate, Bind)`.
pub struct TcpAggregateBindEngine<Msg> {
    /// Generic informations on the port for logging purposes
    port_info: PortInfo,

    addr: SocketAddr,
    queue: spsc::Producer<Msg, CHANNEL_SIZE>,
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
        registry: TokenRegistry,
    ) -> (Self, RecvSocket<Msg>) {
        let (tx, rx) = spsc::Channel::new().split();

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

    /// Tries to establish (or re-establish) the bind on a regular interval.
    fn on_tick(&mut self, journal: &mut Journal) {
        if self.listener.is_none() && self.backoff.can_retry(std::time::Instant::now()) {
            log_event(
                journal,
                &self.port_info,
                LogLevel::Info,
                NetworkEventKind::Reconnecting {
                    attempt: self.backoff.attempt(),
                },
            );
            match TcpListener::bind(self.addr) {
                Ok(mut listener) => {
                    let _ = self.registry.register(
                        &mut listener,
                        LISTENER_INDEX.into(),
                        Interest::READABLE,
                    );
                    self.listener = Some(listener);
                    self.backoff.reset();
                    log_event(
                        journal,
                        &self.port_info,
                        LogLevel::Info,
                        NetworkEventKind::Connected { peer: self.addr },
                    );
                }
                Err(_) => {
                    self.backoff.mark_failed();
                }
            }
        }

        // Timeout detection
        if self.backoff.is_maxed() {
            log_event(
                journal,
                &self.port_info,
                LogLevel::Error,
                NetworkEventKind::Timeout,
            );
        }
    }

    /// Handles readable events fired by Mio on this port's specific token.
    fn on_event(&mut self, local_index: LocalIndex, journal: &mut Journal) {
        if let Some(listener) = &mut self.listener {
            match usize::from(local_index) {
                LISTENER_INDEX => match listener.accept() {
                    Ok((mut stream, addr)) => {
                        let available_slot =
                            self.clients.iter().position(|client| client.is_none());
                        if available_slot.is_none() {
                            log_event(
                                journal,
                                &self.port_info,
                                LogLevel::Error,
                                NetworkEventKind::ChannelFull,
                            );
                            return;
                        }
                        let slot = available_slot.unwrap();
                        let _ = set_nodelay(&stream);
                        let _ =
                            self.registry
                                .register(&mut stream, slot.into(), Interest::READABLE);
                        log_event(
                            journal,
                            &self.port_info,
                            LogLevel::Info,
                            NetworkEventKind::Connected { peer: addr },
                        );
                        self.clients[slot] = Some(AggregateClient {
                            stream,
                            reader: FrameReader::new(),
                            remote_addr: addr,
                        });
                    }
                    Err(_) => {
                        self.backoff.mark_failed();
                    }
                },
                _ => {
                    let slot = usize::from(local_index);
                    if slot < MAX_CLIENTS {
                        let mut disconnect = false;
                        let mut peer_addr = None;
                        if let Some(client) = &mut self.clients[slot] {
                            match client.reader.try_read(&mut client.stream) {
                                Ok(messages) => {
                                    for raw_msg in messages {
                                        if let Ok(msg) = postcard::from_bytes::<Msg>(&raw_msg)
                                            && self.queue.try_write(msg).is_some()
                                        {
                                            log_event(
                                                journal,
                                                &self.port_info,
                                                LogLevel::Warn,
                                                NetworkEventKind::ChannelFull,
                                            );
                                        }
                                    }
                                }
                                Err(e)
                                    if e.kind() == io::ErrorKind::ConnectionReset
                                        || e.kind() == io::ErrorKind::Interrupted =>
                                {
                                    peer_addr = Some(client.remote_addr);
                                    disconnect = true;
                                }
                                Err(_) => {
                                    peer_addr = Some(client.remote_addr);
                                    disconnect = true;
                                }
                            }
                        }
                        if disconnect {
                            if let Some(addr) = peer_addr {
                                log_event(
                                    journal,
                                    &self.port_info,
                                    LogLevel::Warn,
                                    NetworkEventKind::Disconnected { peer: addr },
                                );
                            }
                            self.clients[slot] = None;
                        }
                    }
                }
            }
        }
    }

    fn on_wake(&mut self, _journal: &mut Journal) {}
}

// ── Factory Implementation ──────────────────────────────────────────────────

impl<Msg: DeserializeOwned> SocketFactory<Msg, ()> for (TCP, Aggregate, Bind) {
    type Engine = TcpAggregateBindEngine<Msg>;
    type Handle = RecvSocket<Msg>;

    fn build(
        name: &str,
        addr: &str,
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
        };
        TcpAggregateBindEngine::new(port_info, parsed_addr, registry)
    }
}

impl<Msg: DeserializeOwned> NetworkEngine for TcpAggregateBindEngine<Msg> {
    fn on_event(&mut self, local_index: LocalIndex, journal: &mut Journal) {
        self.on_event(local_index, journal);
    }

    fn on_tick(&mut self, journal: &mut Journal) {
        self.on_tick(journal);
    }

    fn on_wake(&mut self, journal: &mut Journal) {
        self.on_wake(journal);
    }
}

// ── (TCP + PointToPoint + Bind) ──────────────────────────────────

/// The native background engine for `(TCP, PointToPoint, Bind)`.
pub struct TcpPointToPointBindEngine<Msg, RepMsg> {
    /// Generic informations on the port for logging purposes
    port_info: PortInfo,

    addr: SocketAddr,
    recv_queue: spsc::Producer<RepMsg, CHANNEL_SIZE>,
    send_queue: spsc::Consumer<Msg, CHANNEL_SIZE>,
    registry: TokenRegistry,

    listener: Option<TcpListener>,
    backoff: BackoffTimer,

    client: Option<PointToPointClient>,
}

struct PointToPointClient {
    stream: TcpStream,
    reader: FrameReader,
    writer: FrameWriter,
    remote_addr: SocketAddr,
}

const POINT_TO_POINT_LISTENER_INDEX: usize = 0;
const POINT_TO_POINT_CLIENT_WRITABLE_INDEX: usize = 1;
const POINT_TO_POINT_CLIENT_READABLE_INDEX: usize = 2;

impl<Msg: Serialize, RepMsg: DeserializeOwned> TcpPointToPointBindEngine<Msg, RepMsg> {
    fn new(
        port_info: PortInfo,
        addr: SocketAddr,
        registry: TokenRegistry,
        waker: Arc<Waker>,
    ) -> (Self, (RecvSocket<RepMsg>, SendSocket<Msg>)) {
        let (recv_tx, recv_rx) = spsc::Channel::new().split();
        let (send_tx, send_rx) = spsc::Channel::new().split();

        let engine = Self {
            port_info,
            addr,
            recv_queue: recv_tx,
            send_queue: send_rx,
            registry,
            client: None,
            backoff: BackoffTimer::default_network(),
            listener: None,
        };

        (
            engine,
            (
                RecvSocket::new(recv_rx),
                SendSocket::new(send_tx, waker.clone()),
            ),
        )
    }

    /// Tries to establish (or re-establish) the bind on a regular interval.
    fn on_tick(&mut self, journal: &mut Journal) {
        if self.listener.is_none() && self.backoff.can_retry(std::time::Instant::now()) {
            log_event(
                journal,
                &self.port_info,
                LogLevel::Info,
                NetworkEventKind::Reconnecting {
                    attempt: self.backoff.attempt(),
                },
            );
            match TcpListener::bind(self.addr) {
                Ok(mut listener) => {
                    let _ = self.registry.register(
                        &mut listener,
                        POINT_TO_POINT_LISTENER_INDEX.into(),
                        Interest::READABLE,
                    );
                    self.listener = Some(listener);
                    self.backoff.reset();
                    log_event(
                        journal,
                        &self.port_info,
                        LogLevel::Info,
                        NetworkEventKind::Connected { peer: self.addr },
                    );
                }
                Err(_) => {
                    self.backoff.mark_failed();
                }
            }
        }

        // Timeout detection
        if self.backoff.is_maxed() {
            log_event(
                journal,
                &self.port_info,
                LogLevel::Error,
                NetworkEventKind::Timeout,
            );
        }
    }

    /// Handles readable events fired by Mio on this port's specific token.
    fn on_event(&mut self, local_index: LocalIndex, journal: &mut Journal) {
        if let Some(listener) = &mut self.listener {
            match usize::from(local_index) {
                POINT_TO_POINT_LISTENER_INDEX => match listener.accept() {
                    Ok((mut stream, addr)) => {
                        if self.client.is_some() {
                            log_event(
                                journal,
                                &self.port_info,
                                LogLevel::Error,
                                NetworkEventKind::RejectedDuplicateClient { peer: addr },
                            );
                            return;
                        }
                        let _ = set_nodelay(&stream);
                        let _ = self.registry.register(
                            &mut stream,
                            POINT_TO_POINT_CLIENT_WRITABLE_INDEX.into(),
                            Interest::WRITABLE,
                        );
                        let _ = self.registry.register(
                            &mut stream,
                            POINT_TO_POINT_CLIENT_READABLE_INDEX.into(),
                            Interest::READABLE,
                        );
                        log_event(
                            journal,
                            &self.port_info,
                            LogLevel::Info,
                            NetworkEventKind::Connected { peer: addr },
                        );
                        self.client = Some(PointToPointClient {
                            stream,
                            reader: FrameReader::new(),
                            writer: FrameWriter::new(),
                            remote_addr: addr,
                        });
                    }
                    Err(_) => {
                        self.backoff.mark_failed();
                    }
                },
                POINT_TO_POINT_CLIENT_WRITABLE_INDEX => {
                    let client = self.client.as_mut().unwrap();
                    match client.writer.try_flush(&mut client.stream) {
                        Ok(_) => {}
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                        Err(_) => {
                            let peer_addr = client.remote_addr;
                            log_event(
                                journal,
                                &self.port_info,
                                LogLevel::Info,
                                NetworkEventKind::Disconnected { peer: peer_addr },
                            );
                            self.client = None;
                        }
                    }
                }
                POINT_TO_POINT_CLIENT_READABLE_INDEX => {
                    let client = self.client.as_mut().unwrap();
                    match client.reader.try_read(&mut client.stream) {
                        Ok(messages) => {
                            for raw_msg in messages {
                                if let Ok(msg) = postcard::from_bytes::<RepMsg>(&raw_msg)
                                    && self.recv_queue.try_write(msg).is_some()
                                {
                                    log_event(
                                        journal,
                                        &self.port_info,
                                        LogLevel::Warn,
                                        NetworkEventKind::ChannelFull,
                                    );
                                }
                            }
                        }
                        Err(e)
                            if e.kind() == io::ErrorKind::ConnectionReset
                                || e.kind() == io::ErrorKind::Interrupted =>
                        {
                            let peer_addr = client.remote_addr;
                            self.client = None;
                            log_event(
                                journal,
                                &self.port_info,
                                LogLevel::Info,
                                NetworkEventKind::Disconnected { peer: peer_addr },
                            );
                        }
                        Err(_) => {
                            self.client = None;
                            self.backoff.mark_failed();
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    fn on_wake(&mut self, journal: &mut Journal) {
        let mut disconnected = false;
        if let Some(client) = &mut self.client {
            while let Some(msg) = self.send_queue.try_read() {
                client.writer.enqueue(postcard::to_allocvec(&msg).unwrap());
                match client.writer.try_flush(&mut client.stream) {
                    Ok(true) => continue,
                    Ok(false) => break,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(_) => {
                        let peer_addr = client.remote_addr;
                        log_event(
                            journal,
                            &self.port_info,
                            LogLevel::Info,
                            NetworkEventKind::Disconnected { peer: peer_addr },
                        );
                        disconnected = true;
                        break;
                    }
                }
            }
        }

        if disconnected {
            self.client = None;
        }
    }
}

// ── Factory Implementation ──────────────────────────────────────────────────

impl<Msg: Serialize, RepMsg: DeserializeOwned> SocketFactory<Msg, RepMsg>
    for (TCP, PointToPoint, Bind)
{
    type Engine = TcpPointToPointBindEngine<Msg, RepMsg>;
    type Handle = (RecvSocket<RepMsg>, SendSocket<Msg>);

    fn build(
        name: &str,
        addr: &str,
        registry: TokenRegistry,
        waker: Arc<Waker>,
    ) -> (Self::Engine, Self::Handle) {
        let parsed_addr: SocketAddr = addr.parse().expect("Invalid SocketAddr for TCP bind");
        let port_info = PortInfo {
            port_name: name.to_string(),
            addr: addr.to_owned(),
            transport: "TCP".to_string(),
            kind: "PointToPoint".to_string(),
            role: "Bind".to_string(),
        };
        TcpPointToPointBindEngine::new(port_info, parsed_addr, registry, waker)
    }
}

impl<Msg: Serialize, RepMsg: DeserializeOwned> NetworkEngine
    for TcpPointToPointBindEngine<Msg, RepMsg>
{
    fn on_event(&mut self, local_index: LocalIndex, journal: &mut Journal) {
        self.on_event(local_index, journal);
    }

    fn on_tick(&mut self, journal: &mut Journal) {
        self.on_tick(journal);
    }

    fn on_wake(&mut self, journal: &mut Journal) {
        self.on_wake(journal);
    }
}

// ── (TCP + PointToPoint + Connect) ──────────────────────────────────

/// The native background engine for `(TCP, PointToPoint, Connect)`.
pub struct TcpPointToPointConnectEngine<Msg, RepMsg> {
    /// Generic informations on the port for logging purposes
    port_info: PortInfo,

    addr: SocketAddr,
    recv_queue: spsc::Producer<RepMsg, CHANNEL_SIZE>,
    send_queue: spsc::Consumer<Msg, CHANNEL_SIZE>,
    registry: TokenRegistry,

    stream: Option<TcpStream>,
    reader: FrameReader,
    writer: FrameWriter,
    backoff: BackoffTimer,

    is_established: bool,
}

const POINT_TO_POINT_CONNECT_WRITABLE_INDEX: usize = 1;
const POINT_TO_POINT_CONNECT_READABLE_INDEX: usize = 2;

impl<Msg: Serialize, RepMsg: DeserializeOwned> TcpPointToPointConnectEngine<Msg, RepMsg> {
    fn new(
        port_info: PortInfo,
        addr: SocketAddr,
        registry: TokenRegistry,
        waker: Arc<Waker>,
    ) -> (Self, (RecvSocket<RepMsg>, SendSocket<Msg>)) {
        let (recv_tx, recv_rx) = spsc::Channel::new().split();
        let (send_tx, send_rx) = spsc::Channel::new().split();

        let engine = Self {
            port_info,
            addr,
            recv_queue: recv_tx,
            send_queue: send_rx,
            registry,
            stream: None,
            reader: FrameReader::new(),
            writer: FrameWriter::new(),
            backoff: BackoffTimer::default_network(),
            is_established: false,
        };

        (
            engine,
            (
                RecvSocket::new(recv_rx),
                SendSocket::new(send_tx, waker.clone()),
            ),
        )
    }

    /// Tries to establish (or re-establish) the connection on a regular interval.
    fn on_tick(&mut self, journal: &mut Journal) {
        if self.stream.is_none() && self.backoff.can_retry(std::time::Instant::now()) {
            log_event(
                journal,
                &self.port_info,
                LogLevel::Info,
                NetworkEventKind::Reconnecting {
                    attempt: self.backoff.attempt(),
                },
            );
            match TcpStream::connect(self.addr) {
                Ok(mut stream) => {
                    let _ = self.registry.register(
                        &mut stream,
                        POINT_TO_POINT_CONNECT_WRITABLE_INDEX.into(),
                        Interest::WRITABLE,
                    );
                    let _ = self.registry.register(
                        &mut stream,
                        POINT_TO_POINT_CONNECT_READABLE_INDEX.into(),
                        Interest::READABLE,
                    );

                    let _ = set_nodelay(&stream);
                    self.stream = Some(stream);
                }
                Err(_) => {
                    self.backoff.mark_failed();
                }
            }
        }

        if self.backoff.is_maxed() {
            log_event(
                journal,
                &self.port_info,
                LogLevel::Error,
                NetworkEventKind::Timeout,
            );
        }
    }

    /// Handles readable/writable events fired by Mio on this port's specific tokens.
    fn on_event(&mut self, local_index: LocalIndex, journal: &mut Journal) {
        if let Some(stream) = &mut self.stream {
            match usize::from(local_index) {
                POINT_TO_POINT_CONNECT_WRITABLE_INDEX => {
                    if !self.is_established {
                        match stream.take_error() {
                            Ok(None) => {
                                self.is_established = true;
                                self.backoff.reset();
                                log_event(
                                    journal,
                                    &self.port_info,
                                    LogLevel::Info,
                                    NetworkEventKind::Connected { peer: self.addr },
                                );
                            }
                            _ => {
                                self.stream = None;
                                self.backoff.mark_failed();
                                return;
                            }
                        }
                    }

                    match self.writer.try_flush(stream) {
                        Ok(_) => {}
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                        Err(_) => {
                            log_event(
                                journal,
                                &self.port_info,
                                LogLevel::Error,
                                NetworkEventKind::Disconnected { peer: self.addr },
                            );
                            self.stream = None;
                            self.is_established = false;
                            self.writer = FrameWriter::new();
                            self.reader = FrameReader::new();
                            self.backoff.mark_failed();
                        }
                    }
                }
                POINT_TO_POINT_CONNECT_READABLE_INDEX => match self.reader.try_read(stream) {
                    Ok(messages) => {
                        for raw_msg in messages {
                            if let Ok(msg) = postcard::from_bytes::<RepMsg>(&raw_msg)
                                && self.recv_queue.try_write(msg).is_some()
                            {
                                log_event(
                                    journal,
                                    &self.port_info,
                                    LogLevel::Warn,
                                    NetworkEventKind::ChannelFull,
                                );
                            }
                        }
                    }
                    Err(e)
                        if e.kind() == io::ErrorKind::ConnectionReset
                            || e.kind() == io::ErrorKind::Interrupted =>
                    {
                        if !self.is_established {
                            self.stream = None;
                            self.backoff.mark_failed();
                            return;
                        }
                        log_event(
                            journal,
                            &self.port_info,
                            LogLevel::Info,
                            NetworkEventKind::Disconnected { peer: self.addr },
                        );
                        self.stream = None;
                        self.is_established = false;
                        self.writer = FrameWriter::new();
                        self.reader = FrameReader::new();
                    }
                    Err(_) => {
                        if !self.is_established {
                            self.stream = None;
                            self.backoff.mark_failed();
                            return;
                        }
                        log_event(
                            journal,
                            &self.port_info,
                            LogLevel::Error,
                            NetworkEventKind::Disconnected { peer: self.addr },
                        );
                        self.stream = None;
                        self.is_established = false;
                        self.writer = FrameWriter::new();
                        self.reader = FrameReader::new();
                        self.backoff.mark_failed();
                    }
                },
                _ => unreachable!(),
            }
        }
    }

    fn on_wake(&mut self, journal: &mut Journal) {
        let mut disconnected = false;
        if let Some(stream) = &mut self.stream {
            while let Some(msg) = self.send_queue.try_read() {
                self.writer.enqueue(postcard::to_allocvec(&msg).unwrap());
                match self.writer.try_flush(stream) {
                    Ok(true) => continue,
                    Ok(false) => break,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(_) => {
                        log_event(
                            journal,
                            &self.port_info,
                            LogLevel::Info,
                            NetworkEventKind::Disconnected { peer: self.addr },
                        );
                        disconnected = true;
                        break;
                    }
                }
            }
        }

        if disconnected {
            self.stream = None;
            self.is_established = false;
            self.writer = FrameWriter::new();
            self.reader = FrameReader::new();
            self.backoff.mark_failed();
        }
    }
}

// ── Factory Implementation ──────────────────────────────────────────────────

impl<Msg: Serialize, RepMsg: DeserializeOwned> SocketFactory<Msg, RepMsg>
    for (TCP, PointToPoint, Connect)
{
    type Engine = TcpPointToPointConnectEngine<Msg, RepMsg>;
    type Handle = (RecvSocket<RepMsg>, SendSocket<Msg>);

    fn build(
        name: &str,
        addr: &str,
        registry: TokenRegistry,
        waker: Arc<Waker>,
    ) -> (Self::Engine, Self::Handle) {
        let parsed_addr: SocketAddr = addr.parse().expect("Invalid SocketAddr for TCP connect");
        let port_info = PortInfo {
            port_name: name.to_string(),
            addr: addr.to_owned(),
            transport: "TCP".to_string(),
            kind: "PointToPoint".to_string(),
            role: "Connect".to_string(),
        };
        TcpPointToPointConnectEngine::new(port_info, parsed_addr, registry, waker)
    }
}

impl<Msg: Serialize, RepMsg: DeserializeOwned> NetworkEngine
    for TcpPointToPointConnectEngine<Msg, RepMsg>
{
    fn on_event(&mut self, local_index: LocalIndex, journal: &mut Journal) {
        self.on_event(local_index, journal);
    }

    fn on_tick(&mut self, journal: &mut Journal) {
        self.on_tick(journal);
    }

    fn on_wake(&mut self, journal: &mut Journal) {
        self.on_wake(journal);
    }
}
