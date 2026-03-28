//! Shared measurement utilities: rdtsc, TSC calibration, and Stats reporting.

use std::time::{Duration, Instant};

// ── rdtsc helper ────────────────────────────────────────────────────────────

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub fn rdtsc() -> u64 {
    unsafe { core::arch::x86_64::_rdtsc() }
}

// ── TSC frequency calibration ───────────────────────────────────────────────

/// Measures the TSC ticks per nanosecond by spinning for `duration` and
/// comparing wall-clock elapsed to cycle-counter delta.
pub fn calibrate_tsc(duration: Duration) -> f64 {
    let start_cycles = rdtsc();
    let start_wall = Instant::now();
    while start_wall.elapsed() < duration {
        core::hint::spin_loop();
    }
    let end_cycles = rdtsc();
    let elapsed_ns = start_wall.elapsed().as_nanos() as f64;
    let delta_cycles = (end_cycles - start_cycles) as f64;
    delta_cycles / elapsed_ns
}

// ── Statistics ──────────────────────────────────────────────────────────────

pub struct Stats {
    samples: Vec<u64>,
    tsc_per_ns: f64,
}

impl Stats {
    pub fn new(mut samples: Vec<u64>, tsc_per_ns: f64) -> Self {
        samples.sort_unstable();
        Self {
            samples,
            tsc_per_ns,
        }
    }

    pub fn count(&self) -> usize {
        self.samples.len()
    }

    pub fn avg_cycles(&self) -> f64 {
        let sum: u64 = self.samples.iter().sum();
        sum as f64 / self.samples.len() as f64
    }

    pub fn percentile_cycles(&self, p: f64) -> u64 {
        let idx = ((p / 100.0) * self.samples.len() as f64) as usize;
        self.samples[idx.min(self.samples.len() - 1)]
    }

    pub fn cycles_to_ns(&self, cycles: u64) -> f64 {
        cycles as f64 / self.tsc_per_ns
    }

    pub fn min_cycles(&self) -> u64 {
        self.samples[0]
    }

    pub fn max_cycles(&self) -> u64 {
        *self.samples.last().unwrap()
    }

    pub fn median_cycles(&self) -> u64 {
        self.samples[self.samples.len() / 2]
    }

    pub fn print_latency(&self, title: &str) {
        println!();
        println!("╔══════════════════════════════════════════════════════╗");
        println!("║  {:^50}  ║", title);
        println!("╠══════════════════════════════════════════════════════╣");
        println!(
            "║  TSC frequency  : {:7.3} GHz                        ║",
            self.tsc_per_ns
        );
        println!(
            "║  Samples        : {:10}                         ║",
            self.count()
        );
        println!("╠══════════════════════════════════════════════════════╣");
        println!(
            "║  {:>10}  {:>12}  {:>15}           ║",
            "Metric", "Cycles", "Nanoseconds"
        );
        println!("╠══════════════════════════════════════════════════════╣");

        let rows: Vec<(&str, u64)> = vec![
            ("Min", self.min_cycles()),
            ("Median", self.median_cycles()),
            ("Avg", self.avg_cycles() as u64),
            ("P80", self.percentile_cycles(80.0)),
            ("P90", self.percentile_cycles(90.0)),
            ("P99", self.percentile_cycles(99.0)),
            ("P99.9", self.percentile_cycles(99.9)),
            ("Max", self.max_cycles()),
        ];

        for (label, cycles) in rows {
            let ns = self.cycles_to_ns(cycles);
            println!(
                "║  {:>10}  {:>12}  {:>12.1} ns           ║",
                label, cycles, ns
            );
        }

        println!("╚══════════════════════════════════════════════════════╝");
        println!();
    }
}

// ── Throughput report ───────────────────────────────────────────────────────

pub fn print_throughput(
    title: &str,
    total_messages: u64,
    elapsed: Duration,
    extra_lines: &[String],
) {
    let elapsed_secs = elapsed.as_secs_f64();
    let throughput = total_messages as f64 / elapsed_secs;
    let ns_per_msg = elapsed.as_nanos() as f64 / total_messages as f64;

    println!();
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║  {:^50}  ║", title);
    println!("╠══════════════════════════════════════════════════════╣");
    println!(
        "║  Total messages : {:>10}                         ║",
        total_messages
    );
    println!(
        "║  Elapsed        : {:>10.3} s                       ║",
        elapsed_secs
    );
    println!(
        "║  Throughput     : {:>10.0} msg/s                   ║",
        throughput
    );
    println!(
        "║  Latency        : {:>10.1} ns/msg                  ║",
        ns_per_msg
    );
    for line in extra_lines {
        println!("║  {}  ║", line);
    }
    println!("╚══════════════════════════════════════════════════════╝");
    println!();
}
