//! Defines a journal that wraps the IPC connection and the lineage_id attribution.

use ipc::address::Address;
use ipc::socket::PushSocket;
use service::identity::Identity;
use trading_sdk::journal::lineage::LineageId;

use crate::entry::JournalEntry;
use crate::event::JournalEvent;
use crate::lineage::LineageIdGenerator;

pub struct Journal {
    identity: Identity,
    lineage_id_generator: LineageIdGenerator,
    logger_ipc_connection: PushSocket<JournalEntry>,
}

impl Journal {
    /// Creates a new journal.
    ///
    /// The journal creates a connection with the logger on creation.
    pub async fn new(identity: Identity, logger_addr: Address) -> Result<Self, std::io::Error> {
        let lineage_id_generator = LineageIdGenerator::new(identity.identifier);
        let logger_ipc_connection = PushSocket::connect(logger_addr).await?;
        Ok(Self {
            identity,
            lineage_id_generator,
            logger_ipc_connection,
        })
    }

    /// Logs an event.
    ///
    /// The log are automatically sent to the logger.
    pub async fn log(
        &mut self,
        parents: Vec<LineageId>,
        event: JournalEvent,
    ) -> Result<LineageId, std::io::Error> {
        let lineage_id = self.lineage_id_generator.next();
        let journal_entry =
            JournalEntry::make_entry(lineage_id, self.identity.clone(), parents, event);
        self.logger_ipc_connection.send(&journal_entry).await?;
        Ok(lineage_id)
    }
}
