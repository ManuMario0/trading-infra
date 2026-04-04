//! Simple SPSC ring buffer

use std::alloc;
use std::cell::Cell;
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
    best_known_tail: Cell<u64>,
    local_head: Cell<u64>,
    _padding: [u8; 64], // avoid false sharing in array of senders
}

pub struct Receiver<C> {
    channel: Arc<Spsc<C>>,
    best_known_head: Cell<u64>,
    local_tail: Cell<u64>,
    is_tail_up_to_date: Cell<bool>,
    _padding: [u8; 64], // avoid false sharing in array of receivers
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
                best_known_tail: Cell::new(0),
                local_head: Cell::new(0),
                _padding: [0; 64],
            },
            Receiver {
                channel: spsc,
                best_known_head: Cell::new(0),
                local_tail: Cell::new(0),
                is_tail_up_to_date: Cell::new(true),
                _padding: [0; 64],
            },
        )
    }
}

pub fn new<C>(capacity: usize) -> (Sender<C>, Receiver<C>) {
    Spsc::new(capacity)
}

impl<C> Sender<C> {
    #[inline(always)]
    pub fn send(&self, value: C) -> Result<(), ChannelError> {
        let head = self.head();
        let tail = self.update_tail();

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
        self.local_head.set(head + 1);
        self.channel.head.store(head + 1, Ordering::Release);
        Ok(())
    }

    /// Returns the number of available slots in the buffer.
    ///
    /// This function forces a L1 cache line synchronisation with the
    /// producer, so it should be used sparingly.
    #[inline(always)]
    pub fn available(&self) -> u64 {
        let head = self.head();
        let tail = self.sync_tail(); // To get real available space

        self.channel.capacity - (head - tail)
    }

    /// Returns the capacity of the buffer.
    #[inline(always)]
    pub fn capacity(&self) -> u64 {
        self.channel.capacity
    }

    // Local implementation

    #[inline(always)]
    fn head(&self) -> u64 {
        self.local_head.get()
    }

    #[inline(always)]
    fn best_known_tail(&self) -> u64 {
        self.best_known_tail.get()
    }

    /// Updates the tail if needed.
    #[inline(always)]
    fn update_tail(&self) -> u64 {
        let head = self.head();
        let best_known_tail = self.best_known_tail();

        if head - best_known_tail >= self.channel.capacity {
            self.sync_tail()
        } else {
            best_known_tail
        }
    }

    /// Sync with the latest known tail.
    #[inline(always)]
    fn sync_tail(&self) -> u64 {
        let tail = self.channel.tail.load(Ordering::Acquire);
        self.best_known_tail.set(tail);
        tail
    }
}

impl<C> Receiver<C> {
    #[inline(always)]
    pub fn recv(&self) -> Option<C> {
        let tail = self.local_tail.get();
        let head = self.update_head();

        if head - tail == 0 {
            // Queue is empty. Before returning, ensure the producer sees
            // our latest shared tail so it can fill more space.
            if !self.is_tail_up_to_date.get() {
                self.channel.tail.store(tail, Ordering::Release);
                self.is_tail_up_to_date.set(true);
            }
            return None;
        }

        let index = (tail & self.channel.mask) as usize;
        let value = unsafe { self.channel.buffer.add(index).read().assume_init() };

        let next_tail = tail + 1;
        self.local_tail.set(next_tail);
        self.is_tail_up_to_date.set(false);

        // Amortized store: Only update the shared tail every 32 messages
        // to reduce atomic traffic and cache-line bouncing.
        if (next_tail & (if self.channel.capacity > 64 { 31 } else { 1 })) == 0 {
            self.channel.tail.store(next_tail, Ordering::Release);
            self.is_tail_up_to_date.set(true);
        }

        Some(value)
    }

    /// Returns the number of available slots in the buffer.
    ///
    /// This function forces a L1 cache line synchronisation with the
    /// consumer, so it should be used sparingly.
    #[inline(always)]
    pub fn available(&self) -> u64 {
        let head = self.update_head();
        let tail = self.tail();

        self.channel.capacity - (head - tail)
    }

    // Local implementation

    #[inline(always)]
    fn best_known_head(&self) -> u64 {
        self.best_known_head.get()
    }

    #[inline(always)]
    fn tail(&self) -> u64 {
        self.local_tail.get()
    }

    #[inline(always)]
    fn update_head(&self) -> u64 {
        let tail = self.tail();
        let best_known_head = self.best_known_head();

        if best_known_head > tail {
            best_known_head
        } else {
            self.sync_head()
        }
    }

    #[inline(always)]
    fn sync_head(&self) -> u64 {
        let head = self.channel.head.load(Ordering::Acquire);
        self.best_known_head.set(head);
        head
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
