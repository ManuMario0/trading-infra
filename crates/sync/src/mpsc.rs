//! Simple SPSC ring buffer

use std::cell::Cell;
use std::sync::Arc;
use std::sync::Mutex;

use crate::error::ChannelError;
use crate::spsc;

/// Shared state for the MPSC channel, mainly used to store spare senders for cloning.
struct MpscInner<C> {
    spare_senders: Mutex<Vec<spsc::Sender<C>>>,
}

pub struct Sender<C, const MAX_PRODUCERS: usize> {
    inner: spsc::Sender<C>,
    shared: Arc<MpscInner<C>>,
}

pub struct Receiver<C, const MAX_PRODUCERS: usize> {
    receivers: [spsc::Receiver<C>; MAX_PRODUCERS],
    // We keep a reference to the shared state to ensure it lives as long as the receiver
    _shared: Arc<MpscInner<C>>,
    last_producer_id_checked: Cell<usize>,
}

impl<C> MpscInner<C> {
    fn new<const MAX_PRODUCERS: usize>(
        capacity: usize,
    ) -> (Sender<C, MAX_PRODUCERS>, Receiver<C, MAX_PRODUCERS>) {
        let mut txs = Vec::with_capacity(MAX_PRODUCERS);
        let mut rxs = Vec::with_capacity(MAX_PRODUCERS);

        for _ in 0..MAX_PRODUCERS {
            let (tx, rx) = spsc::new(capacity);
            txs.push(tx);
            rxs.push(rx);
        }

        // The first producer gets the first sender handle
        let first_tx = txs.remove(0);

        let shared = Arc::new(Self {
            spare_senders: Mutex::new(txs),
        });

        (
            Sender {
                inner: first_tx,
                shared: shared.clone(),
            },
            Receiver {
                receivers: rxs
                    .try_into()
                    .unwrap_or_else(|_| panic!("Failed to convert receivers")),
                _shared: shared,
                last_producer_id_checked: Cell::new(0),
            },
        )
    }
}

pub fn new<C, const MAX_PRODUCERS: usize>(
    capacity: usize,
) -> (Sender<C, MAX_PRODUCERS>, Receiver<C, MAX_PRODUCERS>) {
    MpscInner::new(capacity)
}

impl<C, const MAX_PRODUCERS: usize> Sender<C, MAX_PRODUCERS> {
    pub fn send(&self, value: C) -> Result<(), ChannelError> {
        self.inner.send(value)
    }
}

impl<C, const MAX_PRODUCERS: usize> Receiver<C, MAX_PRODUCERS> {
    pub fn recv(&self) -> Option<C> {
        let last_producer_id_checked = self.last_producer_id_checked.get();

        for producer_id in last_producer_id_checked..MAX_PRODUCERS {
            if let Some(value) = self.receivers[producer_id].recv() {
                self.last_producer_id_checked
                    .set((producer_id + 1) % MAX_PRODUCERS);
                return Some(value);
            }
        }
        for producer_id in 0..last_producer_id_checked {
            if let Some(value) = self.receivers[producer_id].recv() {
                self.last_producer_id_checked
                    .set((producer_id + 1) % MAX_PRODUCERS);
                return Some(value);
            }
        }
        None
    }
}

impl<C, const MAX_PRODUCERS: usize> Clone for Sender<C, MAX_PRODUCERS> {
    fn clone(&self) -> Self {
        let mut spares = self.shared.spare_senders.lock().unwrap();
        if spares.is_empty() {
            panic!(
                "Exceeded MAX_PRODUCERS completely in the MPSC channel! Increase your generic constant."
            );
        }

        Self {
            inner: spares.remove(0),
            shared: self.shared.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mpsc() {
        let (sender, receiver) = new::<i32, 2>(10);
        let sender2 = sender.clone();
        sender.send(1).unwrap();
        sender2.send(2).unwrap();
        assert_eq!(receiver.recv(), Some(1));
        assert_eq!(receiver.recv(), Some(2));
    }

    #[test]
    #[should_panic]
    fn test_mpsc_overflow() {
        let (sender, _) = new::<i32, 2>(10);
        let sender2 = sender.clone();
        let sender3 = sender2.clone();
        sender3.send(1).unwrap();
    }
}
