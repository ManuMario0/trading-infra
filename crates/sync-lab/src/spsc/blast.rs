//! SPSC unidirectional throughput blast.
//!
//! One producer blasts messages as fast as possible to one consumer.
//! Measures aggregate throughput (messages/sec) and time per message.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Instant;

use sync::spsc;

use crate::stats;

const TOTAL_MESSAGES: u64 = 10_000_000;
const WARMUP_MESSAGES: u64 = 100_000;
const CHANNEL_CAPACITY: usize = 1024;

pub fn run() {
    let cores = core_affinity::get_core_ids().expect("Failed to get core IDs");
    if cores.len() < 2 {
        eprintln!(
            "Need at least 2 cores for SPSC blast, found {}",
            cores.len()
        );
        std::process::exit(1);
    }

    let (tx, rx) = spsc::new::<u64>(CHANNEL_CAPACITY);
    let producer_core = cores[0];
    let consumer_core = cores[1];

    println!("SPSC Blast: 1 producer → 1 consumer");
    println!(
        "  Producer on core {:?}, Consumer on core {:?}",
        producer_core, consumer_core
    );
    println!(
        "  Warmup: {} msgs, Measured: {} msgs",
        WARMUP_MESSAGES, TOTAL_MESSAGES
    );

    let ready = Arc::new(AtomicBool::new(false));
    let ready_clone = ready.clone();

    // Consumer thread
    let consumer = thread::spawn(move || {
        core_affinity::set_for_current(consumer_core);

        // Drain warmup
        let mut warmup_count = 0u64;
        while warmup_count < WARMUP_MESSAGES {
            if rx.recv().is_some() {
                warmup_count += 1;
            } else {
                core::hint::spin_loop();
            }
        }

        // Signal producer that warmup is done
        ready_clone.store(true, Ordering::Release);

        // Measured phase
        let start = Instant::now();
        let mut count = 0u64;
        while count < TOTAL_MESSAGES {
            if rx.recv().is_some() {
                count += 1;
            } else {
                core::hint::spin_loop();
            }
        }
        let elapsed = start.elapsed();
        (count, elapsed)
    });

    // Producer (main thread)
    core_affinity::set_for_current(producer_core);

    // Send warmup
    for i in 0..WARMUP_MESSAGES {
        loop {
            if tx.send(i).is_ok() {
                break;
            }
            core::hint::spin_loop();
        }
    }

    // Wait for consumer to finish warmup
    while !ready.load(Ordering::Acquire) {
        core::hint::spin_loop();
    }

    // Measured blast
    for i in 0..TOTAL_MESSAGES {
        loop {
            if tx.send(i).is_ok() {
                break;
            }
            core::hint::spin_loop();
        }
    }

    let (count, elapsed) = consumer.join().expect("Consumer panicked");
    assert_eq!(count, TOTAL_MESSAGES);

    stats::print_throughput("SPSC Blast — Throughput", TOTAL_MESSAGES, elapsed, &[]);
}
