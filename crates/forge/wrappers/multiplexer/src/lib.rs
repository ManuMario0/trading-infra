//! Multiplexer wrapper library.
//!
//! Contains all the wrapper logic for multiplexer-type modules:
//! manifest construction, IPC setup, lifecycle management, etc.
//!
//! This is a real crate in the workspace so you get full rust-analyzer
//! support while developing. Forge only generates a thin main.rs that
//! wires the user module into this wrapper.

pub mod manifest;
pub mod port;

use std::{str::FromStr, time::Duration};

use ipc::address::Address;
use journal::{
    entry::LogLevel,
    event::{
        JournalEvent,
        service::{ServiceEventKind, ServiceLifetimeEvent},
    },
    journal::Journal,
};
use service::{env, identity::Identity};
use tokio::time::sleep;
use trading_sdk::{Multiplexist, identity::ServiceId};

use crate::manifest::make_manifest;

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
pub async fn run(
    mut _multiplexer: Box<dyn Multiplexist>,
    info: ModuleInfo,
) -> Result<(), Box<dyn std::error::Error>> {
    // Checks if the flag manifest is set
    let is_manifest_request = env::parse_or(env::MANIFEST_REQUEST, false);
    if is_manifest_request {
        let manifest = make_manifest(info);
        println!("{}", serde_json::to_string(&manifest).unwrap());
        return Ok(());
    }

    // Get our identity.
    let service_id = ServiceId::new(env::parse_or(env::SERVICE_ID, 0));
    let service_identity = Identity::new(info.name, info.version, service_id);

    // Setup the journal.
    let logger_addr_str: String =
        env::parse_or(env::IPC_LOGGER_ADDR, "tcp://127.0.0.1:5555".to_string());
    let logger_addr = Address::from_str(&logger_addr_str).unwrap();
    let mut journal = Journal::new(service_identity, logger_addr).await.unwrap();

    let startup_service_event = ServiceLifetimeEvent {
        service_type: "multiplexer".to_string(),
        kind: ServiceEventKind::Startup,
    };
    let startup_event = JournalEvent::ServiceLifetimeEvent(startup_service_event);
    journal
        .log(vec![], LogLevel::Info, startup_event)
        .await
        .unwrap(); // crash if connections is already down

    println!("Starting Multiplexer Wrapper...");

    // TODO: Build and register manifest
    // let _manifest = manifest::make_manifest();

    // TODO: Setup IPC sockets and routing

    // V0: Infinite sleep to keep the process alive for testing the forge spawn
    loop {
        sleep(Duration::from_secs(60)).await;
    }
}
