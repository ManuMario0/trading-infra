//! Strategy wrapper library.
//!
//! Contains all the wrapper logic for strategy-type modules:
//! manifest construction, IPC setup, lifecycle management, etc.
//!
//! This is a real crate in the workspace so you get full rust-analyzer
//! support while developing. Forge only generates a thin main.rs that
//! wires the user module into this wrapper.

use std::path::PathBuf;
use std::thread;

use ipc::make_ipc;
use journal::entry::{LogLevel, ServiceKind, ThreadKind};
use journal::journal::Journal;
use service::envelope::Envelope;
use service::event::{ServiceEventKind, ServiceLifetimeEvent};
use service::manifest::Manifest;
use service::{env, identity::Identity};
use trading_sdk::identity::ServiceId;
use trading_sdk::model::context::Context;
use trading_sdk::model::ledger::LedgerUpdate;
use trading_sdk::{Allocation, MarketDataBatch, Strategist};

/// Metadata about the user module, injected by Forge at build time.
pub struct ModuleInfo {
    pub name: &'static str,
    pub version: &'static str,
    pub description: &'static str,
}

pub struct JournalWrapper {
    journal: Journal,
}

impl trading_sdk::journal::journal::Journal for JournalWrapper {
    fn record(
        &mut self,
        parents: Vec<trading_sdk::journal::lineage::LineageId>,
        event: trading_sdk::journal::event::ServiceEvent,
    ) -> trading_sdk::journal::lineage::LineageId {
        self.journal
            .log(ThreadKind::Main, parents, LogLevel::Info, event)
            .unwrap() // assume journal never fails
    }
}

/// Runs the strategy wrapper lifecycle.
///
/// This is the main entry point called by the Forge-generated binary.
/// It takes an instance of a user-defined strategy (returned by `entry_point()`)
/// and a [`ModuleInfo`] describing the module being wrapped.
pub fn run(
    mut strategy: Box<dyn Strategist>,
    info: ModuleInfo,
) -> Result<(), Box<dyn std::error::Error>> {
    // Acquire addresses
    let market_data_input_addr = env::parse_or("MARKET_DATA_INPUT", "127.0.0.1:8001".to_string());
    let ledger_update_input_addr =
        env::parse_or("LEDGER_UPDATE_INPUT", "127.0.0.1:8002".to_string());
    let allocation_output_addr = env::parse_or("ALLOCATION_OUTPUT", "127.0.0.1:8003".to_string());

    // Setup IPC
    let (
        runner,
        blueprint,
        _stop_handle,
        mut market_data_socket,
        mut _ledger_update_socket,
        mut execution_result_socket,
    ) = make_ipc!(
        MARKET_DATA_INPUT: kind=Broadcast, role=Connect, ty=MarketDataBatch, addr=&market_data_input_addr, transport=TCP,
        LEDGER_UPDATE_INPUT: kind=Broadcast, role=Connect, ty=Envelope<LedgerUpdate>, addr=&ledger_update_input_addr, transport=TCP,
        ALLOCATION_OUTPUT: kind=Aggregate, role=Connect, ty=Envelope<Allocation>, addr=&allocation_output_addr, transport=TCP,
    );

    let manifest = Manifest {
        blueprint,
        name: info.name.to_string(),
        version: info.version.to_string(),
        description: info.description.to_string(),
        service_type: "strategy".to_string(),
    };

    // Checks if the flag manifest is set
    let is_manifest_request = env::parse_or(env::MANIFEST_REQUEST, false);
    if is_manifest_request {
        println!("{}", manifest.json_serialize().unwrap());
        return Ok(());
    }

    // Get our identity.
    let service_id = ServiceId::new(env::parse_or(env::SERVICE_ID, 0));
    let service_identity = Identity::new(info.name, info.version, service_id);

    // Setup the journal.
    let (mut journal, mut journal_runner) = Journal::init(
        service_identity.identifier,
        ServiceKind::Strategy,
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

    let startup_event = ServiceLifetimeEvent {
        service_type: "strategy".to_string(),
        kind: ServiceEventKind::Startup,
    };
    let _startup_lineage = journal
        .log(ThreadKind::Main, vec![], LogLevel::Info, startup_event)
        .unwrap(); // crash if initial log fails

    let mut journal_wrapper = JournalWrapper { journal };

    loop {
        std::hint::spin_loop();

        while let Some(market_data_msg) = market_data_socket.try_recv() {
            let context = Context {
                journal: &mut journal_wrapper,
            };
            let allocations = strategy.on_market_data(context, market_data_msg);
            execution_result_socket.send(Envelope {
                source: service_id,
                payload: allocations,
            });
            // TODO: implement local ledger to estimate P&L and broadcast it to the multiplexer.
        }
    }
}
