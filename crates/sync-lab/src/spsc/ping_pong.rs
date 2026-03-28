//! SPSC Ping-Pong latency benchmark.
//!
//! Two threads pinned to separate cores exchange messages through a pair of
//! SPSC channels (A->B, B->A).  Thread A timestamps each round-trip with
//! `rdtsc` and records one-way latency = (round-trip cycles) / 2.

use std::thread;
use std::time::Duration;

use sync::spsc;

use crate::stats::{Stats, calibrate_tsc, rdtsc};

// ── Benchmark ───────────────────────────────────────────────────────────────

const WARMUP_ROUNDS: usize = 10_000;
const MEASURED_ROUNDS: usize = 1_000_000;
const CHANNEL_CAPACITY: usize = 64;

pub fn run() {
    println!("Calibrating TSC frequency (500ms spin)...");
    let tsc_per_ns = calibrate_tsc(Duration::from_millis(500));
    println!("TSC frequency: {:.3} GHz", tsc_per_ns);

    // Two channels: A->B and B->A
    let (a_tx, a_rx) = spsc::new::<u64>(CHANNEL_CAPACITY);
    let (b_tx, b_rx) = spsc::new::<u64>(CHANNEL_CAPACITY);

    // Get available cores
    let cores = core_affinity::get_core_ids().expect("Failed to get core IDs");
    if cores.len() < 2 {
        eprintln!("Need at least 2 cores for ping-pong, found {}", cores.len());
        std::process::exit(1);
    }
    let core_a = cores[0];
    let core_b = cores[1];
    println!(
        "Pinning Thread A to core {:?}, Thread B to core {:?}",
        core_a, core_b
    );

    // Thread B: echo loop (receive from A, send back to A)
    let total_rounds = WARMUP_ROUNDS + MEASURED_ROUNDS;
    let thread_b = thread::spawn(move || {
        core_affinity::set_for_current(core_b);
        for _ in 0..total_rounds {
            // Busy-wait for message from A
            let msg = loop {
                if let Some(v) = a_rx.recv() {
                    break v;
                }
                core::hint::spin_loop();
            };
            // Echo it back
            loop {
                if b_tx.send(msg).is_ok() {
                    break;
                }
                core::hint::spin_loop();
            }
        }
    });

    // Thread A: timestamped ping, measure round-trip
    core_affinity::set_for_current(core_a);

    // Warmup
    println!("Warming up ({} rounds)...", WARMUP_ROUNDS);
    for i in 0..WARMUP_ROUNDS {
        loop {
            if a_tx.send(i as u64).is_ok() {
                break;
            }
            core::hint::spin_loop();
        }
        loop {
            if b_rx.recv().is_some() {
                break;
            }
            core::hint::spin_loop();
        }
    }

    // Measured rounds
    println!("Measuring ({} rounds)...", MEASURED_ROUNDS);
    let mut samples = Vec::with_capacity(MEASURED_ROUNDS);

    for i in 0..MEASURED_ROUNDS {
        let start = rdtsc();

        loop {
            if a_tx.send(i as u64).is_ok() {
                break;
            }
            core::hint::spin_loop();
        }

        loop {
            if b_rx.recv().is_some() {
                break;
            }
            core::hint::spin_loop();
        }

        let end = rdtsc();
        // One-way latency = round-trip / 2
        samples.push((end - start) / 2);
    }

    thread_b.join().expect("Thread B panicked");

    let stats = Stats::new(samples, tsc_per_ns);
    stats.print_latency("SPSC Ping-Pong -- One-Way Latency");
}
