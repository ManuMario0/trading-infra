//! Simple SPSC ring buffer

use std::array;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::error::ChannelError;
use crate::spsc;

struct Mpsc<C, const MAX_PRODUCERS: usize> {
    /// The spsc channels
    producers: [(spsc::Sender<C>, spsc::Receiver<C>); MAX_PRODUCERS],

    /// Atomic counter for the number of active producers
    active_producers: AtomicUsize,
}

unsafe impl<C: Send, const MAX_PRODUCERS: usize> Send for Mpsc<C, MAX_PRODUCERS> {}
unsafe impl<C: Send, const MAX_PRODUCERS: usize> Sync for Mpsc<C, MAX_PRODUCERS> {}

pub struct Sender<C, const MAX_PRODUCERS: usize> {
    channel: Arc<Mpsc<C, MAX_PRODUCERS>>,
    producer_id: usize,
}

pub struct Receiver<C, const MAX_PRODUCERS: usize> {
    channel: Arc<Mpsc<C, MAX_PRODUCERS>>,
    last_producer_id_checked: AtomicUsize,
}

impl<C, const MAX_PRODUCERS: usize> Mpsc<C, MAX_PRODUCERS> {
    fn new(capacity: usize) -> (Sender<C, MAX_PRODUCERS>, Receiver<C, MAX_PRODUCERS>) {
        let producers = array::from_fn(|_| spsc::new(capacity));
        let mpsc = Arc::new(Self {
            producers,
            active_producers: AtomicUsize::new(1),
        });
        (
            Sender {
                channel: mpsc.clone(),
                producer_id: 0,
            },
            Receiver {
                channel: mpsc,
                last_producer_id_checked: AtomicUsize::new(0),
            },
        )
    }
}

pub fn new<C, const MAX_PRODUCERS: usize>(
    capacity: usize,
) -> (Sender<C, MAX_PRODUCERS>, Receiver<C, MAX_PRODUCERS>) {
    Mpsc::new(capacity)
}

impl<C, const MAX_PRODUCERS: usize> Sender<C, MAX_PRODUCERS> {
    pub fn send(&self, value: C) -> Result<(), ChannelError> {
        self.channel.producers[self.producer_id].0.send(value)
    }
}

impl<C, const MAX_PRODUCERS: usize> Receiver<C, MAX_PRODUCERS> {
    pub fn recv(&self) -> Option<C> {
        let active_producers = self.channel.active_producers.load(Ordering::Relaxed);
        let last_producer_id_checked = self.last_producer_id_checked.load(Ordering::Relaxed);

        for producer_id in last_producer_id_checked..active_producers {
            if let Some(value) = self.channel.producers[producer_id].1.recv() {
                // Store `producer_id + 1` so the absolute next call starts checking the NEXT queue,
                // guaranteeing true round-robin fairness without queue starvation.
                if producer_id + 1 == active_producers {
                    self.last_producer_id_checked.store(0, Ordering::Relaxed);
                } else {
                    self.last_producer_id_checked
                        .store(producer_id + 1, Ordering::Relaxed);
                }
                return Some(value);
            }
        }
        for producer_id in 0..last_producer_id_checked {
            if let Some(value) = self.channel.producers[producer_id].1.recv() {
                self.last_producer_id_checked
                    .store(producer_id + 1, Ordering::Relaxed);
                return Some(value);
            }
        }
        None
    }
}

impl<C, const MAX_PRODUCERS: usize> Clone for Sender<C, MAX_PRODUCERS> {
    fn clone(&self) -> Self {
        let producer_id = self
            .channel
            .active_producers
            .fetch_add(1, Ordering::Relaxed);
        if producer_id >= MAX_PRODUCERS {
            panic!(
                "Exceeded MAX_PRODUCERS completely in the MPSC channel! Increase your generic constant."
            );
        }

        Self {
            channel: self.channel.clone(),
            producer_id,
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
