//! Defines the journal runner.

use std::sync::Arc;

use cachec::channel::mpsc;
use mio::{Events, Poll, Token, Waker};

use crate::entry::JournalEntry;
use crate::journal::{CAPACITY, MAX_PRODUCERS};
use crate::writer::Writer;

pub struct JournalRunner {
    poll: Poll,
    events: Events,
    mpsc_recv: mpsc::Consumer<JournalEntry, CAPACITY, MAX_PRODUCERS>,
    writer: Writer,
}

impl JournalRunner {
    pub(crate) fn new(
        mpsc_recv: mpsc::Consumer<JournalEntry, CAPACITY, MAX_PRODUCERS>,
        writer: Writer,
    ) -> (Self, Arc<Waker>) {
        let poll = Poll::new().unwrap();
        let events = Events::with_capacity(1024);
        let waker = Arc::new(Waker::new(poll.registry(), Token(0)).unwrap());
        (
            Self {
                poll,
                events,
                mpsc_recv,
                writer,
            },
            waker,
        )
    }

    pub fn run(&mut self) {
        loop {
            self.poll.poll(&mut self.events, None).unwrap();
            for _ in self.events.iter() {
                while let Some(event) = self.mpsc_recv.try_read() {
                    // Purge the queue
                    let mut json = serde_json::to_string(&event).unwrap();
                    json.push('\n');
                    self.writer.write(json.as_bytes()).unwrap();
                }
            }
        }
    }
}
