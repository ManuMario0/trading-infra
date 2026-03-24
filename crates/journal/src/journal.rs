//! Defines a journal that wraps the IPC connection and the lineage_id attribution.

use std::sync::Arc;

use mio::Waker;
use service::identity::Identity;
use sync::mpsc;
use trading_sdk::journal::lineage::LineageId;

use crate::entry::{JournalEntry, LogLevel};
use crate::event::JournalEvent;
use crate::lineage::LineageIdGenerator;

pub(crate) const MAX_PRODUCERS: usize = 32;

#[derive(Clone)]
pub struct Journal {
    mpsc_sender: mpsc::Sender<JournalEntry, MAX_PRODUCERS>,
    identity: Identity,
    waker: Arc<Waker>,
    lineage_id_generator: Arc<LineageIdGenerator>,
}

impl Journal {
    /// Initializes the Journaling system, returning the Frontend handler and the Background Runner.
    pub fn init(
        capacity: usize,
        identity: Identity,
        working_dir: std::path::PathBuf,
        log_dir: std::path::PathBuf,
    ) -> (Self, crate::runner::JournalRunner) {
        let (tx, rx) = mpsc::new::<JournalEntry, MAX_PRODUCERS>(capacity);
        let writer = crate::writer::Writer::new(working_dir, log_dir, identity.identifier);
        let (runner, waker) = crate::runner::JournalRunner::new(rx, writer);

        let journal = Self {
            mpsc_sender: tx,
            identity: identity.clone(),
            waker,
            lineage_id_generator: Arc::new(LineageIdGenerator::new(identity.identifier)),
        };

        (journal, runner)
    }

    /// Log an event quietly on the background thread.
    pub fn log(
        &self,
        parents: Vec<LineageId>,
        log_level: LogLevel,
        event: JournalEvent,
    ) -> Result<LineageId, std::io::Error> {
        let lineage_id = self.lineage_id_generator.next();
        let journal_entry =
            JournalEntry::make_entry(lineage_id, self.identity.clone(), parents, log_level, event);

        self.mpsc_sender.send(journal_entry).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to send journal entry: Queue Full",
            )
        })?;

        // Wake the background runner to process the queue
        self.waker.wake()?;

        Ok(lineage_id)
    }
}
