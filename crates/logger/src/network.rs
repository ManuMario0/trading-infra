//! Network connectivity.

use ipc::{address::Address, socket::PullSocket};
use trading_sdk::journal::event::JournalEntry;

pub struct Network {
    socket: PullSocket<JournalEntry>,
}

impl Network {
    pub async fn new(address: Address) -> Result<Self, std::io::Error> {
        let socket = PullSocket::bind(address).await?;
        Ok(Self { socket })
    }

    pub async fn receive(&mut self) -> Result<JournalEntry, std::io::Error> {
        self.socket.receive().await
    }
}
