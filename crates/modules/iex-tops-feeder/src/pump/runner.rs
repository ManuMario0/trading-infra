//! Data pump runner thread that move data from the disk to pre-allocated buffers.

use std::fs;
use std::os::unix::fs::FileExt;
use std::thread;
use std::time::Duration;

use cachec::channel::spsc;

use crate::pump::block::DataBlock;
use crate::pump::cursor::CHANNEL_SIZE;

pub struct DataPumpRunner {
    /// The file to read from
    file: fs::File,
    /// The recycled blocks to fill
    recycled: spsc::Consumer<DataBlock, CHANNEL_SIZE>,
    /// The channel to send filled blocks to
    outbound: spsc::Producer<DataBlock, CHANNEL_SIZE>,
    /// The total number of runners
    num_runners: usize,
    /// The current offset
    current_offset: usize,
    /// The size of the blocks to read
    block_size: usize,
}

impl DataPumpRunner {
    /// Creates a new data pump runner.
    pub(super) fn new(
        file: fs::File,
        recycled: spsc::Consumer<DataBlock, CHANNEL_SIZE>,
        outbound: spsc::Producer<DataBlock, CHANNEL_SIZE>,
        num_runners: usize,
        current_offset: usize,
        block_size: usize,
    ) -> Self {
        Self {
            file,
            recycled,
            outbound,
            num_runners,
            current_offset,
            block_size,
        }
    }

    /// Runs the pump.
    pub fn run(&mut self) {
        #[cfg(target_os = "macos")]
        unsafe {
            use std::os::fd::AsRawFd;

            let fd = self.file.as_raw_fd();
            libc::fcntl(fd, libc::F_NOCACHE, 1);
        }
        loop {
            match self.recycled.try_read() {
                Some(mut block) => {
                    let slice = block.as_mut_slice();
                    let len = self
                        .file
                        .read_at(slice, self.current_offset as u64)
                        .expect("Failed to read block");
                    block.set_len(len);
                    self.current_offset += self.block_size * self.num_runners;
                    self.outbound.write(block);
                    if len != self.block_size {
                        break;
                    }
                }
                None => {
                    thread::sleep(Duration::from_micros(100));
                }
            }
            std::hint::spin_loop();
        }
    }
}
