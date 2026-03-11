//! Core application state for the orchestrator.

use journal::event::orchestrator::{CommandEvent, CommandKind, OrchestratorEvent};
use journal::journal::Journal;
use tokio::time;

use ipc::address::Address;
use journal::event::JournalEvent;
use service::env::parse_or;
use service::identity::Identity;
use service::make_identity;
use trading_sdk::identity::ServiceId;

use crate::port::allocator::Allocator;
use crate::port::port::Port;
use crate::service::info::ServiceInfo;
use crate::service::registry::ServiceRegistry;
use crate::service_id::ServiceIdAllocator;

pub struct App {
    pub identity: Identity,
    pub service_allocator: ServiceIdAllocator,
    pub port_allocator: Allocator,
    pub journal: Journal,
    pub service_registry: ServiceRegistry,
}

impl App {
    pub async fn bootstrap() -> Self {
        // Get node identifier to boot identifier generator.
        let node_id: u64 = parse_or(service::env::NODE_ID, 0);
        // A node identifier should be 44 bits, we discard the upper 20 bits.
        let node_id = node_id & 0x0000_000F_FFFF_FFFF;
        // We shift the node identifier to the left by 20 bits to make room for the service identifier.
        // The orchestrator service is always the first service to start, so it has the identifier 0.
        let service_id_namespace = node_id << 20;
        let mut service_id_allocator = ServiceIdAllocator::new(service_id_namespace);
        let orchestrator_service_id = service_id_allocator.next();

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

        // Create journal
        let mut journal = Journal::new(orchestrator_identity.clone(), logger_ipc_address)
            .await
            .expect("Failed to bind IPC socket to logger");

        // Record Lifecycle Events
        let startup_event = JournalEvent::OrchestratorEvent(
            journal::event::orchestrator::OrchestratorEvent::Lifecycle(
                journal::event::orchestrator::LifecycleEvent::Startup {
                    working_dir: working_dir.clone(),
                },
            ),
        );
        journal.log(vec![], startup_event).await.unwrap();

        let logger_spawn_event = JournalEvent::OrchestratorEvent(
            journal::event::orchestrator::OrchestratorEvent::Lifecycle(
                journal::event::orchestrator::LifecycleEvent::LoggerSpawned {
                    service_id: logger_service_id.get_inner() as u32,
                    assigned_dir: logger_dir.to_string_lossy().to_string(),
                },
            ),
        );
        journal.log(vec![], logger_spawn_event).await.unwrap();

        // Create a new allocator for the ports.
        // The allocator will be used to allocate ports for the services.
        let allocator = Allocator::new(logger_port.0 + 1..=logger_port.0 + 100);

        Self {
            identity: orchestrator_identity,
            service_allocator: service_id_allocator,
            port_allocator: allocator,
            journal,
            service_registry: ServiceRegistry::new(),
        }
    }

    pub async fn process_command(&mut self, cmd_kind: CommandKind) {
        let cmd = CommandEvent {
            kind: cmd_kind.clone(),
        };
        let event = JournalEvent::OrchestratorEvent(OrchestratorEvent::Command(cmd));
        self.journal.log(vec![], event).await.unwrap();

        match cmd_kind {
            CommandKind::Deploy(deploy_command) => {
                // Try to startup the service.
                let service_info = ServiceInfo::startup_service(
                    &mut self.port_allocator,
                    &mut self.service_allocator,
                    deploy_command.service_path,
                )
                .await;
                match service_info {
                    Ok(service_info) => {
                        let event = JournalEvent::OrchestratorEvent(OrchestratorEvent::Lifecycle(
                            journal::event::orchestrator::LifecycleEvent::ServiceSpawned {
                                service_id: service_info.identifier.get_inner() as u32,
                                service_manifest: service_info.manifest.clone(),
                            },
                        ));
                        self.service_registry.insert(service_info);
                        self.journal.log(vec![], event).await.unwrap();
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to spawn service: {:?}", e);
                        // Log structured error to journal
                        let event = JournalEvent::OrchestratorEvent(OrchestratorEvent::Lifecycle(
                            journal::event::orchestrator::LifecycleEvent::ServiceSpawnFailed {
                                error: e,
                            },
                        ));
                        self.journal.log(vec![], event).await.unwrap();
                        eprintln!("{}", error_msg);
                    }
                }
            }
            CommandKind::Stop(stop_command) => (),
        }
    }
}
