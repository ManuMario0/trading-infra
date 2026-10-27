//! Implement a simple view over two distinct u8 slices.
//!
//! It allows one to access the memory as if it was a single slice.

use std::fs;
use std::path;

use cachec::channel::spsc;

use crate::pump::block::DataBlock;
use crate::pump::runner::DataPumpRunner;

/// The number of chunks pre allocated per runner.
pub const NUM_CHUNKS: usize = 4;

/// The size of the channels.
pub const CHANNEL_SIZE: usize = (NUM_CHUNKS + 2).next_power_of_two();

pub struct BlockManager {
    /// Inbound SPSC channels
    inbound: Vec<spsc::Consumer<DataBlock, CHANNEL_SIZE>>,
    /// Recycled blocks
    outbound: Vec<spsc::Producer<DataBlock, CHANNEL_SIZE>>,
    /// Next queue to use
    current_queue: usize,
    /// Current data block
    flying_block: Option<DataBlock>,
}

impl BlockManager {
    fn new(file: fs::File, num_runners: usize, block_size: usize) -> (Self, Vec<DataPumpRunner>) {
        assert!(num_runners > 0);
        assert!(block_size > 2048);

        let mut inbound = Vec::with_capacity(num_runners);
        let mut recycled_outbound = Vec::with_capacity(num_runners);
        let mut runners = Vec::with_capacity(num_runners);

        for i in 0..num_runners {
            // Queue for filled blocks sent to the consumer
            let (tx_outbound, rx_inbound) = spsc::Channel::new().split();
            // Queue for empty blocks sent back to the pump
            let (mut tx_recycled_outbound, rx_recycled_inbound) = spsc::Channel::new().split();

            // We setup the system to have 4 chunks of memory per
            // runner so they always have work to do.
            for _ in 0..NUM_CHUNKS {
                let block = DataBlock::new(block_size);
                if tx_recycled_outbound.try_write(block).is_some() {
                    panic!("Failed to send block");
                }
            }

            inbound.push(rx_inbound);
            recycled_outbound.push(tx_recycled_outbound);

            let runner = DataPumpRunner::new(
                file.try_clone().expect("Failed to clone file"),
                rx_recycled_inbound,
                tx_outbound,
                num_runners,
                i * block_size,
                block_size,
            );
            runners.push(runner);
        }

        let block_manager = BlockManager {
            inbound,
            outbound: recycled_outbound,
            current_queue: 0,
            flying_block: None,
        };

        (block_manager, runners)
    }

    #[inline(always)]
    fn next(&mut self) {
        match self.flying_block.take() {
            Some(old_block) => {
                // Recycle old block
                self.outbound[self.current_queue].write(old_block);
                // Go for the next queue
                self.current_queue = (self.current_queue + 1) % self.inbound.len();
                self.flying_block = Some(self.inbound[self.current_queue].read());
            }
            None => {
                // We are just starting, we need to get the first block
                self.flying_block = Some(self.inbound[self.current_queue].read());
            }
        }
    }

    #[inline(always)]
    fn get(&mut self, offset: usize) -> u8 {
        if self.flying_block.is_none() {
            self.next(); // initialize the first block
        }
        unsafe { *self.flying_block.as_ref().unwrap().get_unchecked(offset) }
    }

    #[inline(always)]
    fn get_slice(&mut self, offset: usize, len: usize) -> &[u8] {
        if self.flying_block.is_none() {
            self.next(); // initialize the first block
        }
        &self.flying_block.as_ref().unwrap().as_slice()[offset..offset + len]
    }
}

pub struct FileCursor {
    /// Block manager
    block_manager: BlockManager,
    /// File length
    file_length: usize,
    /// Current data block offset
    current_data_block_offset: usize,
    /// Block size
    block_size: usize,
    /// Current end of the data block
    current_data_block_end: usize,
}

impl FileCursor {
    pub fn new(
        file_name: path::PathBuf,
        num_runners: usize,
        block_size: usize,
    ) -> (Self, Vec<DataPumpRunner>) {
        let file = fs::File::open(file_name).expect("Failed to open file");
        let file_len = file.metadata().expect("Failed to get file metadata").len();
        let (block_manager, runners) = BlockManager::new(file, num_runners, block_size);
        let view = Self {
            block_manager,
            file_length: file_len as usize,
            current_data_block_offset: 0,
            block_size,
            current_data_block_end: block_size,
        };
        (view, runners)
    }

    /// Returns a reference to the byte at the given offset.
    ///
    /// The offset is relative to the start of the file.
    ///
    /// The view can only be accessed in increasing order of offsets.
    /// Doing otherwise might lead to a panic.
    #[inline(always)]
    pub fn get(&mut self, offset: usize) -> u8 {
        if offset < self.current_data_block_end {
            return self
                .block_manager
                .get(offset - self.current_data_block_offset);
        }

        let mut offset_in_block = offset - self.current_data_block_offset;

        // Loop until we get the correct block.
        // In practice, we will never loop more than once.
        while offset_in_block >= self.block_size {
            self.block_manager.next();
            self.current_data_block_offset += self.block_size;
            offset_in_block -= self.block_size;
            self.current_data_block_end += self.block_size;
        }

        self.block_manager.get(offset_in_block)
    }

    #[inline(always)]
    pub fn read_u16_le(&mut self, offset: usize) -> u16 {
        u16::from_le_bytes([self.get(offset), self.get(offset + 1)])
    }

    #[inline(always)]
    pub fn read_u32_le(&mut self, offset: usize) -> u32 {
        match self.try_get_slice(offset, 4) {
            Some(slice) => u32::from_le_bytes(slice.try_into().unwrap()),
            None => u32::from_le_bytes([
                self.get(offset),
                self.get(offset + 1),
                self.get(offset + 2),
                self.get(offset + 3),
            ]),
        }
    }

    #[inline(always)]
    pub fn read_u64_le(&mut self, offset: usize) -> u64 {
        match self.try_get_slice(offset, 8) {
            Some(slice) => u64::from_le_bytes(slice.try_into().unwrap()),
            None => u64::from_le_bytes([
                self.get(offset),
                self.get(offset + 1),
                self.get(offset + 2),
                self.get(offset + 3),
                self.get(offset + 4),
                self.get(offset + 5),
                self.get(offset + 6),
                self.get(offset + 7),
            ]),
        }
    }

    #[inline(always)]
    pub const fn file_length(&self) -> usize {
        self.file_length
    }

    /// Tries to get a slice of bytes at the given offset.
    #[inline(always)]
    pub fn try_get_slice(&mut self, offset: usize, len: usize) -> Option<&[u8]> {
        while offset >= self.current_data_block_end {
            self.block_manager.next();
            self.current_data_block_offset += self.block_size;
            self.current_data_block_end += self.block_size;
        }
        if offset + len <= self.current_data_block_end {
            return Some(
                &self
                    .block_manager
                    .get_slice(offset - self.current_data_block_offset, len),
            );
        }
        None
    }
}
