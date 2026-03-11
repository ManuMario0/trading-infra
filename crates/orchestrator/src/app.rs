//! Core application state for the orchestrator.

use tokio::time;

use ipc::address::Address;
use ipc::socket::PushSocket;
use journal::entry::JournalEntry;
use journal::event::JournalEvent;
use journal::lineage::LineageIdGenerator;
use service::env::parse_or;
use service::identity::Identity;
use service::make_identity;
use trading_sdk::identity::ServiceId;

use crate::port::allocator::Allocator;
use crate::port::port::Port;
use crate::service_id::ServiceIdAllocator;

pub struct App {
    pub identity: Identity,
    pub service_allocator: ServiceIdAllocator,
    pub port_allocator: Allocator,
    pub lineage_generator: LineageIdGenerator,

    // Connections
    pub logger: PushSocket<JournalEntry>,
}

impl App {
    pub async fn bootstrap() -> Self {
        // Get node identifier to boot identifier generator.
        let node_id: u64 = parse_or(service::env::NODE_ID, 0);
        // A node identifier should be 44 bits, we discard the upper 20 bits.
        let node_id = node_id & 0x0000_000F_FFFF_FFFF;
        // We shift the node identifier to the left by 20 bits to make room for the service identifier.
        // The orchestrator service is always the first service to start, so it has the identifier 0.
        let orchestrator_service_id = node_id << 20;
        let mut service_id_allocator = ServiceIdAllocator::new(orchestrator_service_id);
        let orchestrator_service_id = ServiceId::new(orchestrator_service_id);

        // Make identity for the orchestrator.
        let orchestrator_identity = make_identity!(orchestrator_service_id);

        // Create working directories
        let working_dir = parse_or(service::env::WORKING_DIR, "/tmp/trading-infra".to_string());
        let logger_dir = std::path::Path::new(&working_dir).join("logger");
        std::fs::create_dir_all(&logger_dir).expect("Failed to create logger working directory");

        // Spawn Logger
        let logger_bin_path = parse_or(service::env::SYSTEM_LOGGER_BIN, "".to_string());
        if logger_bin_path.is_empty() {
            panic!("SYSTEM_LOGGER_BIN environment variable is required to spawn the logger.");
        }

        let logger_port_num = parse_or(service::env::IPC_LOGGER, 5555);
        let logger_port = Port(logger_port_num);
        let logger_service_id = service_id_allocator.next();

        println!(
            "Spawning Logger Service (ID: {}) from {}",
            logger_service_id.get_inner(),
            logger_bin_path
        );

        let _child = std::process::Command::new(&logger_bin_path)
            .env(service::env::IPC_LOGGER, logger_port_num.to_string())
            .env(service::env::WORKING_DIR, logger_dir.to_str().unwrap())
            // Pass standard service identifer for the logger too
            .env("SERVICE_ID", logger_service_id.get_inner().to_string())
            .spawn()
            .expect("Failed to spawn logger process");

        // Connect to the logger via TCP
        let logger_ipc_address = Address::tcp("127.0.0.1", logger_port.0);
        time::sleep(time::Duration::from_millis(100)).await;
        let logger_socket: Result<PushSocket<JournalEntry>, _> =
            // Retry a few times while logger boots
            PushSocket::connect(logger_ipc_address).await;

        let mut logger_socket = logger_socket.expect("Failed to bind IPC socket to logger");

        // Setup our lineage ID generator.
        let mut lineage_id_generator = LineageIdGenerator::new(orchestrator_service_id);

        // Record Lifecycle Events
        let startup_event = JournalEvent::OrchestratorEvent(
            journal::event::orchestrator::OrchestratorEvent::Lifecycle(
                journal::event::orchestrator::LifecycleEvent::Startup {
                    working_dir: working_dir.clone(),
                },
            ),
        );
        let startup_entry = JournalEntry::make_entry(
            lineage_id_generator.next(),
            orchestrator_identity.clone(),
            Vec::new(),
            startup_event,
        );
        logger_socket.send(&startup_entry).await.unwrap();

        let logger_spawn_event = JournalEvent::OrchestratorEvent(
            journal::event::orchestrator::OrchestratorEvent::Lifecycle(
                journal::event::orchestrator::LifecycleEvent::ServiceSpawned {
                    service_id: logger_service_id.get_inner() as u32,
                    service_type: "SYSTEM_LOGGER".to_string(),
                    assigned_dir: logger_dir.to_string_lossy().to_string(),
                },
            ),
        );
        let logger_spawn_entry = JournalEntry::make_entry(
            lineage_id_generator.next(),
            orchestrator_identity.clone(),
            Vec::new(),
            logger_spawn_event,
        );
        logger_socket.send(&logger_spawn_entry).await.unwrap();

        // Create a new allocator for the ports.
        // The allocator will be used to allocate ports for the services.
        let allocator = Allocator::new(logger_port.0 + 1..=logger_port.0 + 100);

        Self {
            identity: orchestrator_identity,
            service_allocator: service_id_allocator,
            port_allocator: allocator,
            lineage_generator: lineage_id_generator,
            logger: logger_socket,
        }
    }
}
