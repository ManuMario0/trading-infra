//! Implementations of the highly specialized IPC network engines.

use cachec::channel::spsc;
use journal::entry::{LogLevel, ThreadKind};
use journal::journal::Journal;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::address::Address;
use crate::dispatcher::NetworkEngine;
use crate::event::{NetworkEvent, NetworkEventKind, PortInfo};
use crate::factory::{Aggregate, Bind, Broadcast, Connect, PointToPoint, SocketFactory};
use crate::socket::CHANNEL_SIZE;
use crate::socket::{RecvSocket, SendSocket};
use crate::transport::{Connector, Listener, Transport};

/// Any operations that have no upper bound on the amount of operations should use this value for
/// upperbound reference.
///
/// The main use is for unknown message count inbound, to enable fair sharing of the network thread.
const FUEL_TANK: usize = 16;

const MAX_CLIENTS: usize = 32;

/// Helper to fire a network event to the journal with minimal boilerplate.
#[inline(always)]
fn log_event(journal: &mut Journal, port_info: &PortInfo, level: LogLevel, kind: NetworkEventKind) {
    let event = NetworkEvent {
        port_info: port_info.clone(),
        kind,
    };
    let _ = journal.log(ThreadKind::Network, vec![], level, event);
}

// ── (Broadcast + Connect) ──────────────────────────────────

/// The native background engine for `(TCP, Broadcast, Connect)`.
pub struct BroadcastConnectEngine<Msg> {
    /// Generic informations on the port for logging purposes
    port_info: PortInfo,

    /// Handle to connect to remote host
    connector: Connector<Msg>,

    /// Connection handle
    transport: Option<Transport<Msg>>,

    /// Channel to transmit back data to main thread
    queue: spsc::Producer<Msg, CHANNEL_SIZE>,
}

impl<Msg: DeserializeOwned> BroadcastConnectEngine<Msg> {
    fn new(port_info: PortInfo, addr: Address) -> (Self, RecvSocket<Msg>) {
        let (tx, rx) = spsc::Channel::new().split();

        let engine = Self {
            port_info,
            connector: addr.into_connector().unwrap(),
            transport: None,
            queue: tx,
        };

        (engine, RecvSocket::new(rx))
    }

    fn on_tick(&mut self, journal: &mut Journal) {
        match &mut self.transport {
            Some(transport) => {
                for _ in 0..FUEL_TANK {
                    match transport.try_recv_with(|bytes| postcard::from_bytes(bytes)) {
                        Ok(Some(msg)) => {
                            if self.queue.try_write(msg).is_some() {
                                log_event(
                                    journal,
                                    &self.port_info,
                                    LogLevel::Warn,
                                    NetworkEventKind::ChannelFull,
                                );
                            }
                        }
                        Ok(None) => {
                            break;
                        }
                        Err(_) => {
                            log_event(
                                journal,
                                &self.port_info,
                                LogLevel::Error,
                                NetworkEventKind::Disconnected {
                                    peer: self.port_info.addr.clone(),
                                },
                            );
                            self.transport = None;
                            break;
                        }
                    }
                }
            }
            None => match self.connector.try_connect() {
                Ok(transport) => {
                    log_event(
                        journal,
                        &self.port_info,
                        LogLevel::Info,
                        NetworkEventKind::Connected {
                            peer: self.port_info.addr.clone(),
                        },
                    );
                    self.transport = Some(transport);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(_) => {
                    log_event(
                        journal,
                        &self.port_info,
                        LogLevel::Warn,
                        NetworkEventKind::Disconnected {
                            peer: self.port_info.addr.clone(),
                        },
                    );
                }
            },
        }
    }
}

// ── Factory Implementation ──────────────────────────────────────────────────

impl<Msg: DeserializeOwned> SocketFactory<Msg, ()> for (Broadcast, Connect) {
    type Engine = BroadcastConnectEngine<Msg>;
    type Handle = RecvSocket<Msg>;

    fn build(name: &str, addr: Address) -> (Self::Engine, Self::Handle) {
        let port_info = PortInfo {
            port_name: name.to_string(),
            addr: addr.clone(),
            kind: "Broadcast".to_string(),
            role: "Connect".to_string(),
        };
        BroadcastConnectEngine::new(port_info, addr)
    }
}

impl<Msg: DeserializeOwned> NetworkEngine for BroadcastConnectEngine<Msg> {
    fn on_tick(&mut self, journal: &mut Journal) {
        self.on_tick(journal);
    }
}

// ── (Aggregate + Connect) ──────────────────────────────────

/// The native background engine for `(Aggregate, Connect)`.
pub struct AggregateConnectEngine<Msg> {
    /// Generic informations on the port for logging purposes
    port_info: PortInfo,

    /// Handle to connect to remote host
    connector: Connector<Msg>,

    /// Connection handle
    transport: Option<Transport<Msg>>,

    /// Channel to acquire data to be transmited
    queue: spsc::Consumer<Msg, CHANNEL_SIZE>,
}

impl<Msg: Serialize> AggregateConnectEngine<Msg> {
    fn new(port_info: PortInfo, addr: Address) -> (Self, SendSocket<Msg>) {
        let (tx, rx) = spsc::Channel::new().split();

        let engine = Self {
            port_info,
            connector: addr.into_connector().unwrap(),
            transport: None,
            queue: rx,
        };

        (engine, SendSocket::new(tx))
    }

    fn on_tick(&mut self, journal: &mut Journal) {
        match &mut self.transport {
            Some(transport) => {
                for _ in 0..FUEL_TANK {
                    if transport.is_writable() {
                        if let Some(msg) = self.queue.try_read() {
                            if transport
                                .try_write_with(&msg, |msg, dest| {
                                    postcard::to_io(msg, dest).map(|written| written.len())
                                })
                                .is_err()
                            {
                                log_event(
                                    journal,
                                    &self.port_info,
                                    LogLevel::Error,
                                    NetworkEventKind::Disconnected {
                                        peer: self.port_info.addr.clone(),
                                    },
                                );
                                self.transport = None;
                                break;
                            }
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
            None => match self.connector.try_connect() {
                Ok(transport) => {
                    log_event(
                        journal,
                        &self.port_info,
                        LogLevel::Info,
                        NetworkEventKind::Connected {
                            peer: self.port_info.addr.clone(),
                        },
                    );
                    self.transport = Some(transport);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(_) => {
                    log_event(
                        journal,
                        &self.port_info,
                        LogLevel::Warn,
                        NetworkEventKind::Disconnected {
                            peer: self.port_info.addr.clone(),
                        },
                    );
                }
            },
        }
    }
}

// ── Factory Implementation ──────────────────────────────────────────────────

impl<Msg: Serialize> SocketFactory<Msg, ()> for (Aggregate, Connect) {
    type Engine = AggregateConnectEngine<Msg>;
    type Handle = SendSocket<Msg>;

    fn build(name: &str, addr: Address) -> (Self::Engine, Self::Handle) {
        let port_info = PortInfo {
            port_name: name.to_string(),
            addr: addr.clone(),
            kind: "Aggregate".to_string(),
            role: "Connect".to_string(),
        };
        AggregateConnectEngine::new(port_info, addr)
    }
}

impl<Msg: Serialize> NetworkEngine for AggregateConnectEngine<Msg> {
    fn on_tick(&mut self, journal: &mut Journal) {
        self.on_tick(journal);
    }
}

// ── (Broadcast + Bind) ──────────────────────────────────

/// The native background engine for `(Broadcast, Bind)`.
///
/// The subtlety here is that we need to keep track of all the clients,
/// so the management of tokens is a bit more complex.
///
/// The tokens 0..MAX_CLIENTS-1 are to register the clients for writing,
/// the token MAX_CLIENTS is to register the listener, and MAX_CLIENT+1 is
/// for the waker.
pub struct BroadcastBindEngine<Msg> {
    /// Generic informations on the port for logging purposes
    port_info: PortInfo,

    /// Handle to bind remote client
    listener: Listener<Msg>,

    /// Connection handles
    clients: [Option<Transport<Msg>>; MAX_CLIENTS],

    /// Number of active clients
    active_clients_count: usize,

    /// Channel to acquire data to be transmited
    queue: spsc::Consumer<Msg, CHANNEL_SIZE>,
}

impl<Msg: Serialize> BroadcastBindEngine<Msg> {
    fn new(port_info: PortInfo, addr: Address) -> (Self, SendSocket<Msg>) {
        let (tx, rx) = spsc::Channel::new().split();

        let engine = Self {
            port_info,
            listener: addr.into_listener().unwrap(),
            queue: rx,
            clients: [const { None }; MAX_CLIENTS],
            active_clients_count: 0,
        };

        (engine, SendSocket::new(tx))
    }

    fn insert_client(&mut self, client: Transport<Msg>) {
        for c in self.clients.iter_mut() {
            if c.is_none() {
                *c = Some(client);
                return;
            }
        }
        unreachable!()
    }

    /// On tick is responsible for both:
    /// - Checking if new clients can be connected ;
    /// - Checks if any messages have to be broacasted and braocast them ;
    /// - Cleanup any client that might have disconnected.
    fn on_tick(&mut self, journal: &mut Journal) {
        if self.active_clients_count < MAX_CLIENTS {
            // Try to connect at most one client, we don't loop
            if let Ok((client, addr)) = self.listener.try_accept() {
                self.insert_client(client);
                self.active_clients_count += 1;
                log_event(
                    journal,
                    &self.port_info,
                    LogLevel::Info,
                    NetworkEventKind::Connected { peer: addr },
                );
            }
        }

        // Purge queue only with active clients
        if self.active_clients_count > 0 {
            for _ in 0..FUEL_TANK {
                match self.queue.try_read() {
                    Some(msg) => {
                        for c in self.clients.iter_mut() {
                            if c.is_some() {
                                let client = c.as_mut().unwrap();
                                if client
                                    .try_write_with(&msg, |msg, dest| {
                                        postcard::to_io(msg, dest).map(|written| written.len())
                                    })
                                    .is_err()
                                {
                                    log_event(
                                        journal,
                                        &self.port_info,
                                        LogLevel::Error,
                                        NetworkEventKind::Disconnected {
                                            peer: client.address(),
                                        },
                                    );
                                    *c = None;
                                    self.active_clients_count -= 1;
                                }
                            }
                        }
                    }
                    None => {
                        break;
                    }
                }
                if self.active_clients_count == 0 {
                    break;
                }
            }
        }
    }
}

// ── Factory Implementation ──────────────────────────────────────────────────

impl<Msg: Serialize> SocketFactory<Msg, ()> for (Broadcast, Bind) {
    type Engine = BroadcastBindEngine<Msg>;
    type Handle = SendSocket<Msg>;

    fn build(name: &str, addr: Address) -> (Self::Engine, Self::Handle) {
        let port_info = PortInfo {
            port_name: name.to_string(),
            addr: addr.clone(),
            kind: "Broadcast".to_string(),
            role: "Bind".to_string(),
        };
        BroadcastBindEngine::new(port_info, addr)
    }
}

impl<Msg: Serialize> NetworkEngine for BroadcastBindEngine<Msg> {
    fn on_tick(&mut self, journal: &mut Journal) {
        self.on_tick(journal);
    }
}

// ── (Aggregate + Bind) ──────────────────────────────────

/// The native background engine for `(Aggregate, Bind)`.
pub struct AggregateBindEngine<Msg> {
    /// Generic informations on the port for logging purposes
    port_info: PortInfo,

    /// Handle to bind remote client
    listener: Listener<Msg>,

    /// Connection handles
    clients: [Option<Transport<Msg>>; MAX_CLIENTS],

    /// Number of active clients
    active_clients_count: usize,

    /// Channel to transmit received data
    queue: spsc::Producer<Msg, CHANNEL_SIZE>,
}

impl<Msg: DeserializeOwned> AggregateBindEngine<Msg> {
    fn new(port_info: PortInfo, addr: Address) -> (Self, RecvSocket<Msg>) {
        let (tx, rx) = spsc::Channel::new().split();

        let engine = Self {
            port_info,
            listener: addr.into_listener().unwrap(),
            queue: tx,
            clients: [const { None }; MAX_CLIENTS],
            active_clients_count: 0,
        };

        (engine, RecvSocket::new(rx))
    }

    fn insert_client(&mut self, client: Transport<Msg>) {
        for c in self.clients.iter_mut() {
            if c.is_none() {
                *c = Some(client);
                return;
            }
        }
        unreachable!()
    }

    fn on_tick(&mut self, journal: &mut Journal) {
        if self.active_clients_count < MAX_CLIENTS {
            // Try to connect at most one client, we don't loop
            if let Ok((client, addr)) = self.listener.try_accept() {
                self.insert_client(client);
                self.active_clients_count += 1;
                log_event(
                    journal,
                    &self.port_info,
                    LogLevel::Info,
                    NetworkEventKind::Connected { peer: addr },
                );
            }
        }

        // Purge queue only with active clients
        if self.active_clients_count > 0 {
            for c in self.clients.iter_mut() {
                if c.is_some() {
                    let client = c.as_mut().unwrap();

                    match client.try_recv_with(|bytes| postcard::from_bytes(bytes)) {
                        Ok(Some(msg)) => {
                            if self.queue.try_write(msg).is_some() {
                                log_event(
                                    journal,
                                    &self.port_info,
                                    LogLevel::Warn,
                                    NetworkEventKind::ChannelFull,
                                );
                            }
                        }
                        Ok(None) => {}
                        Err(_) => {
                            log_event(
                                journal,
                                &self.port_info,
                                LogLevel::Error,
                                NetworkEventKind::Disconnected {
                                    peer: client.address(),
                                },
                            );
                            *c = None;
                            self.active_clients_count -= 1;
                        }
                    }
                }
            }
        }
    }
}

// ── Factory Implementation ──────────────────────────────────────────────────

impl<Msg: DeserializeOwned> SocketFactory<Msg, ()> for (Aggregate, Bind) {
    type Engine = AggregateBindEngine<Msg>;
    type Handle = RecvSocket<Msg>;

    fn build(name: &str, addr: Address) -> (Self::Engine, Self::Handle) {
        let port_info = PortInfo {
            port_name: name.to_string(),
            addr: addr.clone(),
            kind: "Aggregate".to_string(),
            role: "Bind".to_string(),
        };
        AggregateBindEngine::new(port_info, addr)
    }
}

impl<Msg: DeserializeOwned> NetworkEngine for AggregateBindEngine<Msg> {
    fn on_tick(&mut self, journal: &mut Journal) {
        self.on_tick(journal);
    }
}

// ── (PointToPoint + Bind) ──────────────────────────────────

/// The native background engine for `(PointToPoint, Bind)`.
pub struct PointToPointBindEngine<Msg> {
    /// Generic informations on the port for logging purposes
    port_info: PortInfo,

    /// Handle to connect to remote host
    listener: Listener<Msg>,

    /// Connection handle
    transport: Option<Transport<Msg>>,

    /// Channel to transmit back data to main thread
    recv_queue: spsc::Producer<Msg, CHANNEL_SIZE>,
    send_queue: spsc::Consumer<Msg, CHANNEL_SIZE>,
}

impl<Msg: Serialize + DeserializeOwned> PointToPointBindEngine<Msg> {
    fn new(port_info: PortInfo, addr: Address) -> (Self, (RecvSocket<Msg>, SendSocket<Msg>)) {
        let (recv_tx, recv_rx) = spsc::Channel::new().split();
        let (send_tx, send_rx) = spsc::Channel::new().split();

        let engine = Self {
            port_info,
            recv_queue: recv_tx,
            send_queue: send_rx,
            listener: addr.into_listener().unwrap(),
            transport: None,
        };

        (engine, (RecvSocket::new(recv_rx), SendSocket::new(send_tx)))
    }

    fn on_tick(&mut self, journal: &mut Journal) {
        match &mut self.transport {
            Some(transport) => {
                for _ in 0..FUEL_TANK {
                    match transport.try_recv_with(|bytes| postcard::from_bytes(bytes)) {
                        Ok(Some(msg)) => {
                            if self.recv_queue.try_write(msg).is_some() {
                                log_event(
                                    journal,
                                    &self.port_info,
                                    LogLevel::Warn,
                                    NetworkEventKind::ChannelFull,
                                );
                            }
                        }
                        Ok(None) => {
                            break;
                        }
                        Err(_) => {
                            log_event(
                                journal,
                                &self.port_info,
                                LogLevel::Error,
                                NetworkEventKind::Disconnected {
                                    peer: self.port_info.addr.clone(),
                                },
                            );
                            self.transport = None;
                            return;
                        }
                    }
                }

                for _ in 0..FUEL_TANK {
                    if transport.is_writable() {
                        if let Some(msg) = self.send_queue.try_read() {
                            if transport
                                .try_write_with(&msg, |msg, dest| {
                                    postcard::to_io(msg, dest).map(|written| written.len())
                                })
                                .is_err()
                            {
                                log_event(
                                    journal,
                                    &self.port_info,
                                    LogLevel::Error,
                                    NetworkEventKind::Disconnected {
                                        peer: self.port_info.addr.clone(),
                                    },
                                );
                                self.transport = None;
                                break;
                            }
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
            None => match self.listener.try_accept() {
                Ok((transport, addr)) => {
                    log_event(
                        journal,
                        &self.port_info,
                        LogLevel::Info,
                        NetworkEventKind::Connected { peer: addr },
                    );
                    self.transport = Some(transport);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(_) => {
                    log_event(
                        journal,
                        &self.port_info,
                        LogLevel::Warn,
                        NetworkEventKind::Disconnected {
                            peer: self.port_info.addr.clone(),
                        },
                    );
                }
            },
        }
    }
}

// ── Factory Implementation ──────────────────────────────────────────────────

impl<Msg: Serialize + DeserializeOwned> SocketFactory<Msg> for (PointToPoint, Bind) {
    type Engine = PointToPointBindEngine<Msg>;
    type Handle = (RecvSocket<Msg>, SendSocket<Msg>);

    fn build(name: &str, addr: Address) -> (Self::Engine, Self::Handle) {
        let port_info = PortInfo {
            port_name: name.to_string(),
            addr: addr.clone(),
            kind: "PointToPoint".to_string(),
            role: "Bind".to_string(),
        };
        PointToPointBindEngine::new(port_info, addr)
    }
}

impl<Msg: Serialize + DeserializeOwned> NetworkEngine for PointToPointBindEngine<Msg> {
    fn on_tick(&mut self, journal: &mut Journal) {
        self.on_tick(journal);
    }
}

// ── (PointToPoint + Connect) ──────────────────────────────────

/// The native background engine for `(PointToPoint, Connect)`.
pub struct PointToPointConnectEngine<Msg> {
    /// Generic informations on the port for logging purposes
    port_info: PortInfo,

    /// Handle to connect to remote host
    connector: Connector<Msg>,

    /// Connection handle
    transport: Option<Transport<Msg>>,

    /// Channel to transmit back data to main thread
    recv_queue: spsc::Producer<Msg, CHANNEL_SIZE>,
    send_queue: spsc::Consumer<Msg, CHANNEL_SIZE>,
}

impl<Msg: Serialize + DeserializeOwned> PointToPointConnectEngine<Msg> {
    fn new(port_info: PortInfo, addr: Address) -> (Self, (RecvSocket<Msg>, SendSocket<Msg>)) {
        let (recv_tx, recv_rx) = spsc::Channel::new().split();
        let (send_tx, send_rx) = spsc::Channel::new().split();

        let engine = Self {
            port_info,
            recv_queue: recv_tx,
            send_queue: send_rx,
            connector: addr.into_connector().unwrap(),
            transport: None,
        };

        (engine, (RecvSocket::new(recv_rx), SendSocket::new(send_tx)))
    }

    /// Tries to establish (or re-establish) the connection on a regular interval.
    fn on_tick(&mut self, journal: &mut Journal) {
        match &mut self.transport {
            Some(transport) => {
                for _ in 0..FUEL_TANK {
                    match transport.try_recv_with(|bytes| postcard::from_bytes(bytes)) {
                        Ok(Some(msg)) => {
                            if self.recv_queue.try_write(msg).is_some() {
                                log_event(
                                    journal,
                                    &self.port_info,
                                    LogLevel::Warn,
                                    NetworkEventKind::ChannelFull,
                                );
                            }
                        }
                        Ok(None) => {
                            break;
                        }
                        Err(_) => {
                            log_event(
                                journal,
                                &self.port_info,
                                LogLevel::Error,
                                NetworkEventKind::Disconnected {
                                    peer: self.port_info.addr.clone(),
                                },
                            );
                            self.transport = None;
                            return;
                        }
                    }
                }

                for _ in 0..FUEL_TANK {
                    if transport.is_writable() {
                        if let Some(msg) = self.send_queue.try_read() {
                            if transport
                                .try_write_with(&msg, |msg, dest| {
                                    postcard::to_io(msg, dest).map(|written| written.len())
                                })
                                .is_err()
                            {
                                log_event(
                                    journal,
                                    &self.port_info,
                                    LogLevel::Error,
                                    NetworkEventKind::Disconnected {
                                        peer: self.port_info.addr.clone(),
                                    },
                                );
                                self.transport = None;
                                break;
                            }
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
            None => match self.connector.try_connect() {
                Ok(transport) => {
                    log_event(
                        journal,
                        &self.port_info,
                        LogLevel::Info,
                        NetworkEventKind::Connected {
                            peer: self.port_info.addr.clone(),
                        },
                    );
                    self.transport = Some(transport);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(_) => {
                    log_event(
                        journal,
                        &self.port_info,
                        LogLevel::Warn,
                        NetworkEventKind::Disconnected {
                            peer: self.port_info.addr.clone(),
                        },
                    );
                }
            },
        }
    }
}

// ── Factory Implementation ──────────────────────────────────────────────────

impl<Msg: Serialize + DeserializeOwned> SocketFactory<Msg> for (PointToPoint, Connect) {
    type Engine = PointToPointConnectEngine<Msg>;
    type Handle = (RecvSocket<Msg>, SendSocket<Msg>);

    fn build(name: &str, addr: Address) -> (Self::Engine, Self::Handle) {
        let port_info = PortInfo {
            port_name: name.to_string(),
            addr: addr.clone(),
            kind: "PointToPoint".to_string(),
            role: "Connect".to_string(),
        };
        PointToPointConnectEngine::new(port_info, addr)
    }
}

impl<Msg: Serialize + DeserializeOwned> NetworkEngine for PointToPointConnectEngine<Msg> {
    fn on_tick(&mut self, journal: &mut Journal) {
        self.on_tick(journal);
    }
}
