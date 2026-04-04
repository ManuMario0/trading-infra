use std::path::PathBuf;
use std::time::Instant;

use iex_tops_feeder::bookkeeper::SoaBookKeeper;
use iex_tops_feeder::pump::cursor::FileCursor;

const BLOCK_SIZE: usize = 2 * 1024 * 1024;
const NUM_RUNNERS: usize = 4;

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("Usage: iex-tops-feeder <path-to-pcapng>");

    eprintln!("[*] Opening {path}");

    // Startup of the threads
    let (mut cursor, runners) = FileCursor::new(PathBuf::from(&path), NUM_RUNNERS, BLOCK_SIZE);
    let handles: Vec<_> = runners
        .into_iter()
        .map(|mut r| {
            std::thread::Builder::new()
                .name("pump-runner".into())
                .spawn(move || r.run())
                .expect("Failed to spawn pump runner")
        })
        .collect();

    eprintln!("[*] Parsing...");
    let start = Instant::now();

    let mut bookkeeper = SoaBookKeeper::new();

    // Mainloop
    let stats = iex_tops_feeder::pcap::run(&mut cursor, &mut bookkeeper);
    let secs = start.elapsed().as_secs_f64();

    eprintln!("[*] Done.");
    eprintln!("    Messages:   {}", stats.total_messages());
    eprintln!(
        "    Payload:    {:.2} MB",
        stats.total_bytes() as f64 / (1024.0 * 1024.0)
    );
    eprintln!(
        "    Message Throughput: {:.0} msg/s",
        stats.total_messages() as f64 / secs
    );

    eprintln!("    P50: {}", stats.percentile(0.50));
    eprintln!("    P90: {}", stats.percentile(0.90));
    eprintln!("    P99: {}", stats.percentile(0.99));
    eprintln!("    P99.9: {}", stats.percentile(0.999));

    for h in handles {
        let _ = h.join();
    }
}
