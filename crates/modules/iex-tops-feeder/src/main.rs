use std::path::PathBuf;
use std::thread;
use std::thread::sleep;
use std::time::Duration;

use iex_tops_feeder::bookkeeper::SoaBookKeeper;
use iex_tops_feeder::pcap::Parser;
use ipc::make_ipc;
use journal::entry::{LogLevel, ServiceKind, ThreadKind};
use journal::journal::Journal;
use service::env;
use service::event::{ServiceEventKind, ServiceLifetimeEvent};
use service::manifest::Manifest;
use trading_sdk::MarketDataBatch;
use trading_sdk::identity::ServiceId;

fn main() {
    let path = env::parse_or("PCAP_PATH", "".to_string());

    let market_data_output_addr = env::parse_or("MARKET_DATA_OUTPUT", "127.0.0.1:8001".to_string());

    // Setup IPC
    let (runner, blueprint, _stop_handle, mut market_data_socket) = make_ipc!(
        MARKET_DATA_OUTPUT: kind=Broadcast, role=Bind, ty=MarketDataBatch, addr=&market_data_output_addr, transport=TCP,
    );

    let manifest = Manifest {
        blueprint,
        name: "iex-tops-feeder".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        description: env!("CARGO_PKG_DESCRIPTION").to_string(),
        service_type: "data feeder".to_string(),
    };

    // Checks if the flag manifest is set
    let is_manifest_request = env::parse_or(env::MANIFEST_REQUEST, false);
    if is_manifest_request {
        println!("{}", manifest.json_serialize().unwrap());
        return;
    }

    let service_id = ServiceId::new(env::parse_or(env::SERVICE_ID, 0));
    let (mut journal, mut journal_runner) = Journal::init(
        service_id,
        ServiceKind::DataFeed,
        PathBuf::from(env::parse_or(env::WORKING_DIR, "/tmp".to_string())),
        PathBuf::from(env::parse_or(env::LOGGING_DIR, "/tmp".to_string())),
    );

    let _journal_handle = thread::spawn(move || {
        journal_runner.run();
    });

    // Start IPC
    let ipc_journal = journal.clone();
    let _ipc_handle = thread::spawn(move || {
        runner.run(ipc_journal).unwrap();
    });

    let mut parser = Parser::new(PathBuf::from(&path));
    let mut bookkeeper = SoaBookKeeper::new();

    let startup_event = ServiceLifetimeEvent {
        service_type: "data feeder".to_string(),
        kind: ServiceEventKind::Startup,
    };
    let startup_lineage = journal
        .log(ThreadKind::Main, vec![], LogLevel::Info, startup_event)
        .unwrap(); // crash if initial log fails

    sleep(Duration::from_secs(5));

    while parser.read_next_block(&mut bookkeeper) {
        sleep(Duration::from_micros(1));
        let emit_batch_event = ServiceLifetimeEvent {
            service_type: "data feeder".to_string(),
            kind: ServiceEventKind::Info(format!("Sending data batch")),
        };
        let emit_lineage = journal
            .log(
                ThreadKind::Main,
                vec![startup_lineage],
                LogLevel::Info,
                emit_batch_event,
            )
            .expect("Unabled to log emit message");
        let batch = bookkeeper.into_market_data_batch(emit_lineage);
        market_data_socket.send(batch);
    }

    let stats = parser.terminate();

    eprintln!("[*] Done.");
    eprintln!("    Messages:   {}", stats.total_messages());
    eprintln!(
        "    Payload:    {:.2} MB",
        stats.total_bytes() as f64 / (1024.0 * 1024.0)
    );

    eprintln!("    P50: {}", stats.percentile(0.50));
    eprintln!("    P90: {}", stats.percentile(0.90));
    eprintln!("    P99: {}", stats.percentile(0.99));
    eprintln!("    P99.9: {}", stats.percentile(0.999));
}
