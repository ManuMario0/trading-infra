//! Simple SPSC ring buffer

use std::alloc;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::ChannelError;

struct Spsc<C> {
    /// The buffer.
    buffer: *mut MaybeUninit<C>,

    /// The memory layout of the buffer.
    memory_layout: alloc::Layout,

    /// The capacity of the ring buffer.
    capacity: u64,

    /// The mask to use for the ring buffer.
    mask: u64,

    /// The head of the ring buffer.
    head: AtomicU64,

    /// Buffer to avoid false sharing.
    _padding: [u8; 64],

    /// The tail of the ring buffer.
    tail: AtomicU64,
}

unsafe impl<C: Send> Send for Spsc<C> {}
unsafe impl<C: Send> Sync for Spsc<C> {}

pub struct Sender<C> {
    channel: Arc<Spsc<C>>,
}

pub struct Receiver<C> {
    channel: Arc<Spsc<C>>,
}

impl<C> Spsc<C> {
    pub(crate) fn new(capacity: usize) -> (Sender<C>, Receiver<C>) {
        let real_capacity = capacity.next_power_of_two();
        let layout = alloc::Layout::array::<MaybeUninit<C>>(real_capacity)
            .unwrap()
            .align_to(128)
            .unwrap();
        let buffer = unsafe { alloc::alloc(layout).cast() };
        let spsc = Arc::new(Self {
            buffer,
            memory_layout: layout,
            capacity: real_capacity as u64,
            mask: real_capacity as u64 - 1,
            head: AtomicU64::new(0),
            _padding: [0; 64],
            tail: AtomicU64::new(0),
        });
        (
            Sender {
                channel: spsc.clone(),
            },
            Receiver { channel: spsc },
        )
    }
}

pub fn new<C>(capacity: usize) -> (Sender<C>, Receiver<C>) {
    Spsc::new(capacity)
}

impl<C> Sender<C> {
    pub fn send(&self, value: C) -> Result<(), ChannelError> {
        let head = self.channel.head.load(Ordering::Relaxed);
        let tail = self.channel.tail.load(Ordering::Acquire);
        if head - tail == self.channel.capacity {
            return Err(ChannelError::FullBuffer {
                buffer_size: self.channel.capacity,
            });
        }
        let index = (head & self.channel.mask) as usize;
        unsafe {
            self.channel
                .buffer
                .add(index)
                .write(MaybeUninit::new(value));
        }
        self.channel.head.store(head + 1, Ordering::Release);
        Ok(())
    }
}

impl<C> Receiver<C> {
    pub fn recv(&self) -> Option<C> {
        let tail = self.channel.tail.load(Ordering::Relaxed);
        let head = self.channel.head.load(Ordering::Acquire);
        if head - tail == 0 {
            return None;
        }
        let index = (tail & self.channel.mask) as usize;
        let value = unsafe { self.channel.buffer.add(index).read().assume_init() };
        self.channel.tail.store(tail + 1, Ordering::Release);
        Some(value)
    }
}

impl<C> Drop for Spsc<C> {
    fn drop(&mut self) {
        let mut tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);
        while tail != head {
            let index = (tail & self.mask) as usize;
            unsafe {
                self.buffer.add(index).drop_in_place();
            };
            tail += 1;
        }
        unsafe {
            alloc::dealloc(self.buffer.cast(), self.memory_layout);
        };
    }
}
