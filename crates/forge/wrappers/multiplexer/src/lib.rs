//! Multiplexer wrapper library.
//!
//! Contains all the wrapper logic for multiplexer-type modules:
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
use trading_sdk::Allocation;
use trading_sdk::model::context::Context;
use trading_sdk::model::ledger::LedgerUpdate;
use trading_sdk::{Multiplexist, identity::ServiceId};

/// Metadata about the user module, injected by Forge at build time.
pub struct ModuleInfo {
    pub name: &'static str,
    pub version: &'static str,
    pub description: &'static str,
}

pub struct JournalWrapper<'a> {
    journal: &'a Journal,
}

impl<'a> trading_sdk::journal::journal::Journal for JournalWrapper<'a> {
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

/// Runs the multiplexer wrapper lifecycle.
///
/// This is the main entry point called by the Forge-generated binary.
/// It takes an instance of a user-defined multiplexer (returned by `entry_point()`)
/// and a [`ModuleInfo`] describing the module being wrapped.
pub fn run(
    mut multiplexer: Box<dyn Multiplexist>,
    info: ModuleInfo,
) -> Result<(), Box<dyn std::error::Error>> {
    // Acquire addresses
    let allocation_input_addr = env::parse_or("ALLOCATION_INPUT", "127.0.0.1:8001".to_string());
    let ledger_update_input_addr =
        env::parse_or("LEDGER_UPDATE_INPUT", "127.0.0.1:8002".to_string());
    let allocation_output_addr = env::parse_or("ALLOCATION_OUTPUT", "127.0.0.1:8003".to_string());

    // Setup IPC
    let (
        runner,
        blueprint,
        _stop_handle,
        mut allocation_input_socket,
        mut ledger_update_socket,
        mut execution_result_socket,
    ) = make_ipc!(
        ALLOCATION_INPUT: kind=Aggregate, role=Bind, capacity=1024, ty=Envelope<Allocation>, addr=&allocation_input_addr, transport=TCP,
        LEDGER_UPDATE_INPUT: kind=Broadcast, role=Connect, capacity=1024, ty=Envelope<LedgerUpdate>, addr=&ledger_update_input_addr, transport=TCP,
        ALLOCATION_OUTPUT: kind=Aggregate, role=Connect, capacity=1024, ty=Envelope<Allocation>, addr=&allocation_output_addr, transport=TCP,
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
    let _journal_handle = thread::spawn(move || {
        journal_runner.run();
    });

    // Start IPC
    let ipc_journal = journal.clone();
    let _ipc_handle = thread::spawn(move || {
        runner.run(ipc_journal).unwrap();
    });

    let startup_event = ServiceLifetimeEvent {
        service_type: "multiplexer".to_string(),
        kind: ServiceEventKind::Startup,
    };
    let _startup_lineage = journal
        .log(ThreadKind::Main, vec![], LogLevel::Info, startup_event)
        .unwrap(); // crash if initial log fails

    println!("Starting Multiplexer Wrapper...");

    loop {
        std::hint::spin_loop();

        while let Some(allocation_msg) = allocation_input_socket.try_recv() {
            let batch = allocation_msg.payload;
            let source_id = allocation_msg.source;
            let context = Context {
                journal: &mut JournalWrapper { journal: &journal },
            };
            let return_batch = multiplexer
                .as_mut()
                .on_allocation(context, source_id, batch);
            execution_result_socket.send(Envelope {
                source: service_id,
                payload: return_batch,
            });
        }
        while let Some(ledger_update_msg) = ledger_update_socket.try_recv() {
            let batch = ledger_update_msg.payload;
            let source_id = ledger_update_msg.source;
            let context = Context {
                journal: &mut JournalWrapper { journal: &journal },
            };
            multiplexer
                .as_mut()
                .on_ledger_update(context, source_id, batch);
        }
    }
}
