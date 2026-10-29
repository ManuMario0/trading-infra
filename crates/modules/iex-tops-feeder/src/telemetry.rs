use std::fs::File;
use std::io::{BufWriter, Write};

#[inline(always)]
pub fn get_cycles() -> u64 {
    #[cfg(target_arch = "aarch64")]
    unsafe {
        std::arch::aarch64::_read_cntvct()
    }
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_rdtsc()
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    0
}

const BUCKET_SIZE: u64 = 10;
const BUCKET_COUNT: usize = 2048;

#[derive(Default)]
pub struct StatsCollector {
    total_messages: u64,
    total_bytes: u64,
    hitogram: Histogram<BUCKET_SIZE, BUCKET_COUNT>,
}

impl StatsCollector {
    pub fn new() -> Self {
        Self {
            total_messages: 0,
            total_bytes: 0,
            hitogram: Histogram::new(),
        }
    }

    /// Record a batch of messages.
    ///
    /// For now, we update the histogram every records, which might add too much overhead.
    #[inline(always)]
    pub fn record(&mut self, cycle_count: u64, message_count: u64, total_bytes: u64) {
        if message_count == 0 {
            return;
        }
        self.total_messages += message_count;
        self.total_bytes += total_bytes;
        // Collect average cycles per messages
        self.hitogram
            .add(cycle_count / message_count, message_count);
    }

    pub fn export_histogram(&self, path: &str) {
        let file = File::create(path).expect("Failed to create CSV file");
        let mut writer = BufWriter::new(file);
        // Write CSV header
        writeln!(&mut writer, "cycles,count").unwrap();
        for (bucket, count) in self.hitogram.buckets.iter().enumerate() {
            writeln!(&mut writer, "{},{}", bucket, count).unwrap();
        }
    }

    pub fn percentile(&self, p: f64) -> u64 {
        self.hitogram.percentile(p, self.total_messages)
    }

    pub fn total_messages(&self) -> u64 {
        self.total_messages
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

pub struct Histogram<const BUCKET_SIZE: u64, const BUCKET_COUNT: usize> {
    buckets: [u64; BUCKET_COUNT], // BUCKET_COUNT buckets of BUCKET_SIZE cycles, so up to BUCKET_COUNT * BUCKET_SIZE cycles per messages
}

impl<const BUCKET_SIZE: u64, const BUCKET_COUNT: usize> Histogram<BUCKET_SIZE, BUCKET_COUNT> {
    pub fn new() -> Self {
        Self {
            buckets: [0; BUCKET_COUNT],
        }
    }

    #[inline(always)]
    pub fn add(&mut self, cycles: u64, count: u64) {
        let bucket = (cycles / BUCKET_SIZE).min(BUCKET_COUNT as u64 - 1);
        self.buckets[bucket as usize] += count;
    }

    #[inline(always)]
    pub fn percentile(&self, p: f64, total_messages: u64) -> u64 {
        let mut total = 0;
        for (bucket, count) in self.buckets.iter().enumerate() {
            total += count;
            if total > (total_messages as f64 * p) as u64 {
                return bucket as u64 * BUCKET_SIZE;
            }
        }
        BUCKET_COUNT as u64 * BUCKET_SIZE
    }
}

impl<const BUCKET_SIZE: u64, const BUCKET_COUNT: usize> Default
    for Histogram<BUCKET_SIZE, BUCKET_COUNT>
{
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
pub struct LatencyProbe {
    start_cycles: u64,
    message_count: u64,
    bytes_count: u64,
}

impl LatencyProbe {
    pub fn new() -> Self {
        Self {
            start_cycles: get_cycles(),
            message_count: 0,
            bytes_count: 0,
        }
    }

    pub fn start() -> Self {
        Self::new()
    }

    #[inline(always)]
    pub fn mark(&mut self, bytes_count: u64) {
        self.message_count += 1;
        self.bytes_count += bytes_count;
    }

    pub fn stop(self, stats: &mut StatsCollector) {
        let elapsed_cycles = get_cycles().saturating_sub(self.start_cycles);
        stats.record(elapsed_cycles, self.message_count, self.bytes_count);
    }
}
