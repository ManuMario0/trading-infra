//! Defines a journal runner and handle to write events to disk.

use std::sync::Arc;

use mio::Waker;
use serde::Serialize;
use sync::mpsc;
use trading_sdk::identity::ServiceId;
use trading_sdk::journal::lineage::LineageId;

use crate::entry::{JournalEntry, LogLevel, MAX_EVENT_SIZE, ServiceKind, ThreadKind};
use crate::lineage::LineageIdGenerator;

pub(crate) const MAX_PRODUCERS: usize = 32;

#[derive(Clone)]
pub struct Journal {
    mpsc_sender: mpsc::Sender<JournalEntry, MAX_PRODUCERS>,
    service_id: ServiceId,
    service_kind: ServiceKind,
    waker: Arc<Waker>,
    lineage_id_generator: Arc<LineageIdGenerator>,
}

impl Journal {
    /// Initializes the Journaling system, returning the Frontend handler and the Background Runner.
    pub fn init(
        capacity: usize,
        service_id: ServiceId,
        service_kind: ServiceKind,
        working_dir: std::path::PathBuf,
        log_dir: std::path::PathBuf,
    ) -> (Self, crate::runner::JournalRunner) {
        let (tx, rx) = mpsc::new::<JournalEntry, MAX_PRODUCERS>(capacity);
        let writer = crate::writer::Writer::new(working_dir, log_dir, service_id);
        let (runner, waker) = crate::runner::JournalRunner::new(rx, writer);

        let journal = Self {
            mpsc_sender: tx,
            service_id,
            service_kind,
            waker,
            lineage_id_generator: Arc::new(LineageIdGenerator::new(service_id)),
        };

        (journal, runner)
    }

    /// Log an event quietly on the background thread.
    pub fn log<Event: Serialize>(
        &self,
        thread_kind: ThreadKind,
        parents: Vec<LineageId>,
        log_level: LogLevel,
        event: Event,
    ) -> Result<LineageId, std::io::Error> {
        let mut event_bytes = [0u8; MAX_EVENT_SIZE];
        let event = serde_json::to_string(&event).unwrap();
        event_bytes[..event.len()].copy_from_slice(event.as_bytes());
        let lineage_id = self.lineage_id_generator.next();
        let journal_entry = JournalEntry::make_entry(
            self.service_id,
            self.service_kind,
            thread_kind,
            lineage_id,
            parents,
            log_level,
            event_bytes,
        );

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
