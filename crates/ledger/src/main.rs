use std::path::PathBuf;
use std::thread;

use ipc::make_ipc;
use journal::entry::JournalEntry;
use journal::entry::LogLevel;
use journal::event::JournalEvent;
use journal::event::service::{ServiceEventKind, ServiceLifetimeEvent};
use journal::journal::Journal;
use ledger::core::ledger::Ledger;
use ledger::model::account::AccountName;
use service::env;
use service::identity::Identity;
use service::manifest::Manifest;
use trading_sdk::identity::ServiceId;
use trading_sdk::model::ledger::LedgerUpdate;

pub const MARKET_DATA_PIPE: &str = "market_data";
pub const EXECUTION_RESULT_PIPE: &str = "execution_result";
pub const LEDGER_UPDATE_PIPE: &str = "ledger_update";

fn main() {
    // Setup IPC
    let (
        runner,
        blueprint,
        handle,
        market_data_socket,
        ledger_update_socket,
        execution_result_socket,
    ) = make_ipc!(
        MARKET_DATA_PIPE: kind=Broadcast, role=Connect, capacity=1024, ty=JournalEntry, addr="127.0.0.1:8001", transport=TCP,
        LEDGER_UPDATE_PIPE: kind=Broadcast, role=Bind, capacity=1024, ty=LedgerUpdate, addr="127.0.0.1:8002", transport=TCP,
        EXECUTION_RESULT_PIPE: kind=Broadcast, role=Connect, capacity=1024, ty=JournalEntry, addr="127.0.0.1:8003", transport=TCP,
    );

    let manifest = Manifest {
        blueprint,
        name: env!("CARGO_PKG_NAME").to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        description: env!("CARGO_PKG_DESCRIPTION").to_string(),
        service_type: "ledger".to_string(),
    };

    // Check manifest request
    if env::parse_or(env::MANIFEST_REQUEST, false) {
        println!("{}", serde_json::to_string(&manifest).unwrap());
        return;
    }

    let ipc_handle = thread::spawn(move || {
        runner.run().unwrap();
    });

    // Make identity
    let service_id = env::parse_or(env::SERVICE_ID, 0);
    let identity = Identity::new(
        manifest.name.clone(),
        manifest.version.clone(),
        ServiceId::new(service_id),
    );

    // Connect to logger
    let working_dir = env::parse_or(env::WORKING_DIR, "/tmp".to_string());
    let log_dir = env::parse_or(env::LOGGING_DIR, "/tmp".to_string());
    let (journal, mut journal_runner) = Journal::init(
        1024,
        identity,
        PathBuf::from(working_dir),
        PathBuf::from(log_dir),
    );
    let journal_handle = thread::spawn(move || {
        journal_runner.run();
    });

    // Send service started event
    let startup_lineage = journal
        .log(
            vec![],
            LogLevel::Info,
            JournalEvent::ServiceLifetimeEvent(ServiceLifetimeEvent {
                service_type: manifest.service_type.clone(),
                kind: ServiceEventKind::Startup,
            }),
        )
        .unwrap(); // crash if initial log fails

    println!("Starting {} v{}...", manifest.name, manifest.version);

    // Connecting to other services
    // let market_data_addr = env::parse_ipc_input_addr(manifest::MARKET_DATA_PIPE);
    // let execution_result_addr = env::parse_ipc_input_addr(manifest::EXECUTION_RESULT_PIPE);
    // let ledger_update_addr = env::parse_ipc_output_addr(manifest::LEDGER_UPDATE_PIPE);

    // Initialize ledger
    let mut ledger = Ledger::new();
    ledger.add_account(AccountName::from_str("Assets:Cash"), 0);
    ledger.add_account(AccountName::from_str("Assets:Broker"), 0);
    ledger.add_account(AccountName::from_str("Liabilities:Broker"), 0);
    ledger.add_account(AccountName::from_str("Equity:Capital"), 0);

    // TODO: Implement service loop
}
