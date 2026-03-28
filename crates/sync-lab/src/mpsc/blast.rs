//! MPSC unidirectional throughput blast.
//!
//! N-1 producers (one per available core) blast messages into the MPSC channel.
//! 1 consumer (main thread) drains all messages and reports aggregate throughput
//! and per-producer fairness.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Instant;

use sync::mpsc;

use crate::stats;

const TOTAL_MESSAGES: u64 = 10_000_000;
const WARMUP_PER_PRODUCER: u64 = 10_000;
const CHANNEL_CAPACITY: usize = 1024;

/// Maximum producers at compile time. At runtime we use min(core_count - 1, MAX).
const MAX: usize = 8;

pub fn run() {
    let cores = core_affinity::get_core_ids().expect("Failed to get core IDs");
    if cores.len() < 2 {
        eprintln!(
            "Need at least 2 cores for MPSC blast, found {}",
            cores.len()
        );
        std::process::exit(1);
    }

    let num_producers = (cores.len() - 1).min(MAX);
    let msgs_per_producer = TOTAL_MESSAGES / num_producers as u64;
    let actual_total = msgs_per_producer * num_producers as u64;
    let total_warmup = WARMUP_PER_PRODUCER * num_producers as u64;

    println!("MPSC Blast: {} producers → 1 consumer", num_producers);
    println!("  Consumer on core {:?}", cores[0]);
    for i in 0..num_producers {
        println!(
            "  Producer {} on core {:?} ({} msgs)",
            i,
            cores[i + 1],
            msgs_per_producer
        );
    }
    println!(
        "  Warmup: {} msgs total, Measured: {} msgs total",
        total_warmup, actual_total
    );

    let (tx, rx) = mpsc::new::<u64, MAX>(CHANNEL_CAPACITY);

    // Create producer senders by cloning
    let mut senders = Vec::with_capacity(num_producers);
    senders.push(tx); // producer 0 gets the original
    for _ in 1..num_producers {
        senders.push(senders[0].clone());
    }

    let ready = Arc::new(AtomicBool::new(false));

    // Spawn producer threads
    let mut handles = Vec::with_capacity(num_producers);
    for (i, sender) in senders.into_iter().enumerate() {
        let core_id = cores[i + 1];
        let ready = ready.clone();
        handles.push(thread::spawn(move || {
            core_affinity::set_for_current(core_id);

            // Warmup: send tagged messages (producer_id in upper bits, sequence in lower)
            for seq in 0..WARMUP_PER_PRODUCER {
                let msg = ((i as u64) << 48) | seq;
                loop {
                    if sender.send(msg).is_ok() {
                        break;
                    }
                    core::hint::spin_loop();
                }
            }

            // Wait for consumer to signal warmup complete
            while !ready.load(Ordering::Acquire) {
                core::hint::spin_loop();
            }

            // Measured blast
            for seq in 0..msgs_per_producer {
                let msg = ((i as u64) << 48) | seq;
                loop {
                    if sender.send(msg).is_ok() {
                        break;
                    }
                    core::hint::spin_loop();
                }
            }
        }));
    }

    // Consumer (main thread, pinned to core 0)
    core_affinity::set_for_current(cores[0]);

    // Drain warmup
    let mut warmup_count = 0u64;
    while warmup_count < total_warmup {
        if rx.recv().is_some() {
            warmup_count += 1;
        } else {
            core::hint::spin_loop();
        }
    }

    // Signal producers that warmup is over
    ready.store(true, Ordering::Release);

    // Measured phase: track per-producer counts for fairness
    let mut per_producer = vec![0u64; num_producers];
    let mut count = 0u64;

    let start = Instant::now();
    while count < actual_total {
        if let Some(msg) = rx.recv() {
            let producer_id = (msg >> 48) as usize;
            if producer_id < num_producers {
                per_producer[producer_id] += 1;
            }
            count += 1;
        } else {
            core::hint::spin_loop();
        }
    }
    let elapsed = start.elapsed();

    // Wait for all producers to finish
    for h in handles {
        h.join().expect("Producer panicked");
    }

    // Build fairness report lines
    let expected_per = msgs_per_producer;
    let mut extra = Vec::new();
    extra.push(format!("{:<50}", format!("Producers: {}", num_producers)));
    for (i, &c) in per_producer.iter().enumerate() {
        let pct = (c as f64 / expected_per as f64) * 100.0;
        extra.push(format!(
            "{:<50}",
            format!("  P{}: {} msgs ({:.1}%)", i, c, pct)
        ));
    }

    stats::print_throughput("MPSC Blast — Throughput", actual_total, elapsed, &extra);
}
