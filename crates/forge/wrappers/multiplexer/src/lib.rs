//! Multiplexer wrapper library.
//!
//! Contains all the wrapper logic for multiplexer-type modules:
//! manifest construction, IPC setup, lifecycle management, etc.
//!
//! This is a real crate in the workspace so you get full rust-analyzer
//! support while developing. Forge only generates a thin main.rs that
//! wires the user module into this wrapper.

pub mod port;

use std::path::PathBuf;
use std::thread::{self, sleep};
use std::time::Duration;

use ipc::make_ipc;
use journal::entry::{LogLevel, ServiceKind, ThreadKind};
use journal::journal::Journal;
use service::envelope::Envelope;
use service::event::{ServiceEventKind, ServiceLifetimeEvent};
use service::manifest::Manifest;
use service::{env, identity::Identity};
use trading_sdk::Allocation;
use trading_sdk::model::ledger::LedgerUpdate;
use trading_sdk::{Multiplexist, identity::ServiceId};

/// Metadata about the user module, injected by Forge at build time.
pub struct ModuleInfo {
    pub name: &'static str,
    pub version: &'static str,
    pub description: &'static str,
}

/// Runs the multiplexer wrapper lifecycle.
///
/// This is the main entry point called by the Forge-generated binary.
/// It takes an instance of a user-defined multiplexer (returned by `entry_point()`)
/// and a [`ModuleInfo`] describing the module being wrapped.
pub fn run(
    mut _multiplexer: Box<dyn Multiplexist>,
    info: ModuleInfo,
) -> Result<(), Box<dyn std::error::Error>> {
    // Setup IPC
    let (
        runner,
        blueprint,
        handle,
        market_data_socket,
        ledger_update_socket,
        execution_result_socket,
    ) = make_ipc!(
        ALLOCATION_INPUT: kind=Aggregate, role=Bind, capacity=1024, ty=Envelope<Allocation>, addr="127.0.0.1:8001", transport=TCP,
        LEDGER_UPDATE_INPUT: kind=Aggregate, role=Bind, capacity=1024, ty=Envelope<LedgerUpdate>, addr="127.0.0.1:8002", transport=TCP,
        ALLOCATION_OUTPUT: kind=Aggregate, role=Connect, capacity=1024, ty=Envelope<Allocation>, addr="127.0.0.1:8003", transport=TCP,
    );

    let manifest = Manifest {
        blueprint,
        name: info.name.to_string(),
        version: info.version.to_string(),
        description: info.description.to_string(),
        service_type: "multiplexer".to_string(),
    };

    // Checks if the flag manifest is set
    let is_manifest_request = env::parse_or(env::MANIFEST_REQUEST, false);
    if is_manifest_request {
        println!("{}", serde_json::to_string(&manifest).unwrap());
        return Ok(());
    }

    // Get our identity.
    let service_id = ServiceId::new(env::parse_or(env::SERVICE_ID, 0));
    let service_identity = Identity::new(info.name, info.version, service_id);

    // Setup the journal.
    let (journal, mut journal_runner) = Journal::init(
        1024,
        service_identity.identifier,
        ServiceKind::Multiplexer,
        PathBuf::from(env::parse_or(env::WORKING_DIR, "/tmp".to_string())),
        PathBuf::from(env::parse_or(env::LOGGING_DIR, "/tmp".to_string())),
    );
    let journal_handle = thread::spawn(move || {
        journal_runner.run();
    });

    // Start IPC
    let ipc_journal = journal.clone();
    let ipc_handle = thread::spawn(move || {
        runner.run(ipc_journal).unwrap();
    });

    let startup_event = ServiceLifetimeEvent {
        service_type: "multiplexer".to_string(),
        kind: ServiceEventKind::Startup,
    };
    let startup_lineage = journal
        .log(ThreadKind::Main, vec![], LogLevel::Info, startup_event)
        .unwrap(); // crash if initial log fails

    println!("Starting Multiplexer Wrapper...");

    // TODO: Build and register manifest
    // let _manifest = manifest::make_manifest();

    // TODO: Setup IPC sockets and routing

    // V0: Infinite sleep to keep the process alive for testing the forge spawn
    loop {
        sleep(Duration::from_secs(60));
    }
}
