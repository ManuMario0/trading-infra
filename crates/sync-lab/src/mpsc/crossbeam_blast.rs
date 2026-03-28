//! Crossbeam MPSC bounded-channel blast for comparison.
//!
//! Same protocol as `mpsc::blast` but uses `crossbeam_channel::bounded`
//! so we can see how a traditional atomic-based MPSC compares to the
//! multi-SPSC approach.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Instant;

use crate::stats;

const TOTAL_MESSAGES: u64 = 10_000_000;
const WARMUP_PER_PRODUCER: u64 = 10_000;
const CHANNEL_CAPACITY: usize = 1024;

pub fn run() {
    let cores = core_affinity::get_core_ids().expect("Failed to get core IDs");
    if cores.len() < 2 {
        eprintln!(
            "Need at least 2 cores for crossbeam blast, found {}",
            cores.len()
        );
        std::process::exit(1);
    }

    let num_producers = (cores.len() - 1).min(16);
    let msgs_per_producer = TOTAL_MESSAGES / num_producers as u64;
    let actual_total = msgs_per_producer * num_producers as u64;
    let total_warmup = WARMUP_PER_PRODUCER * num_producers as u64;

    println!(
        "Crossbeam MPSC Blast: {} producers -> 1 consumer",
        num_producers
    );
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

    let (tx, rx) = crossbeam_channel::bounded::<u64>(CHANNEL_CAPACITY);

    let ready = Arc::new(AtomicBool::new(false));

    // Spawn producer threads
    let mut handles = Vec::with_capacity(num_producers);
    for i in 0..num_producers {
        let core_id = cores[i + 1];
        let ready = ready.clone();
        let sender = tx.clone();
        handles.push(thread::spawn(move || {
            core_affinity::set_for_current(core_id);

            // Warmup
            for seq in 0..WARMUP_PER_PRODUCER {
                let msg = ((i as u64) << 48) | seq;
                // crossbeam send blocks when full, no busy-retry needed
                sender.send(msg).unwrap();
            }

            // Wait for consumer to signal warmup complete
            while !ready.load(Ordering::Acquire) {
                core::hint::spin_loop();
            }

            // Measured blast
            for seq in 0..msgs_per_producer {
                let msg = ((i as u64) << 48) | seq;
                sender.send(msg).unwrap();
            }
        }));
    }
    // Drop the original sender so the channel can close when producers finish
    drop(tx);

    // Consumer (main thread, pinned to core 0)
    core_affinity::set_for_current(cores[0]);

    // Drain warmup
    let mut warmup_count = 0u64;
    while warmup_count < total_warmup {
        if let Ok(_) = rx.try_recv() {
            warmup_count += 1;
        } else {
            core::hint::spin_loop();
        }
    }

    // Signal producers that warmup is over
    ready.store(true, Ordering::Release);

    // Measured phase
    let mut per_producer = vec![0u64; num_producers];
    let mut count = 0u64;

    let start = Instant::now();
    while count < actual_total {
        if let Ok(msg) = rx.try_recv() {
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

    for h in handles {
        h.join().expect("Producer panicked");
    }

    // Build fairness report
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

    stats::print_throughput(
        "Crossbeam MPSC Blast -- Throughput",
        actual_total,
        elapsed,
        &extra,
    );
}
