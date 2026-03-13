use std::collections::BTreeMap;
use std::path::PathBuf;

use tokio::time;

use trading_sdk::journal::lineage::LineageId;

use ipc::address::Address;
use journal::entry::LogLevel;
use journal::event::JournalEvent;
use journal::event::context::Context;
use journal::event::orchestrator::{CommandEvent, OrchestratorEvent};
use journal::journal::Journal;
use service::env::parse_or;
use service::identity::Identity;
use service::make_identity;

use crate::port::allocator::Allocator;
use crate::service::info::ServiceInfo;
use crate::service::registry::ServiceRegistry;
use crate::service_id::ServiceIdAllocator;

pub struct App {
    pub identity: Identity,
    pub service_allocator: ServiceIdAllocator,
    pub port_allocator: Allocator,
    pub journal: Journal,
    pub service_registry: ServiceRegistry,
    pub logger_addr: Address,
    pub startup_id: LineageId,
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

        // Create a new allocator for the ports.
        // The allocator will be used to allocate ports for the services.
        let port_range_min = parse_or(service::env::PORT_RANGE_MIN, 10000);
        let port_range_max = parse_or(service::env::PORT_RANGE_MAX, 12000);
        let mut allocator = Allocator::new(port_range_min..=port_range_max);

        let logger_bin_path = parse_or(service::env::SYSTEM_LOGGER_BIN, "".to_string());
        if logger_bin_path.is_empty() {
            panic!("SYSTEM_LOGGER_BIN environment variable is required to spawn the logger.");
        }

        // Use startup_service to boot the logger.
        // The logger itself doesn't need to connect to another logger (it is the logger),
        // so we pass a dummy address.
        let mut context = Context::new();
        context.add("bootstrap logger");
        let logger_info = ServiceInfo::startup_service(
            &mut allocator,
            &mut service_id_allocator,
            Address::tcp("127.0.0.1", 0),
            PathBuf::from(&logger_bin_path),
            Some(logger_dir.clone()),
            BTreeMap::new(),
            context,
        )
        .expect("Failed to spawn logger service");

        let logger_addr = logger_info.input_ports[0].clone();
        let logger_id = logger_info.identifier;

        // Connect to the logger via TCP
        time::sleep(time::Duration::from_millis(100)).await;

        // Create journal
        let mut journal = Journal::new(orchestrator_identity.clone(), logger_addr.clone())
            .await
            .expect("Failed to bind IPC socket to logger");

        // Now record the spawn of the logger
        let logger_spawn_event = JournalEvent::OrchestratorEvent(OrchestratorEvent::Lifecycle(
            journal::event::orchestrator::LifecycleEvent::ServiceSpawned {
                path: PathBuf::from(logger_bin_path),
                service_id: logger_id,
                service_manifest: logger_info.manifest.clone(),
            },
        ));
        let logger_spawn_id = journal
            .log(vec![], LogLevel::Info, logger_spawn_event)
            .await
            .unwrap();

        // Record Lifecycle Events
        let startup_event = JournalEvent::OrchestratorEvent(
            journal::event::orchestrator::OrchestratorEvent::Lifecycle(
                journal::event::orchestrator::LifecycleEvent::Startup {
                    working_dir: working_dir.clone(),
                },
            ),
        );
        let startup_id = journal
            .log(vec![logger_spawn_id], LogLevel::Info, startup_event)
            .await
            .unwrap();

        // Register the logger
        let mut service_registry = ServiceRegistry::new();
        service_registry.insert(logger_info);

        Self {
            identity: orchestrator_identity,
            service_allocator: service_id_allocator,
            port_allocator: allocator,
            journal,
            service_registry,
            logger_addr,
            startup_id,
        }
    }

    pub async fn process_command(&mut self, cmd: CommandEvent) {
        let event = JournalEvent::OrchestratorEvent(OrchestratorEvent::Command(cmd.clone()));
        let cmd_lineage_id = self
            .journal
            .log(vec![self.startup_id], LogLevel::Info, event)
            .await
            .unwrap();

        match cmd {
            CommandEvent::Deploy {
                service_path,
                addr_map,
            } => {
                // Try to startup the service.
                let service_info = ServiceInfo::startup_service(
                    &mut self.port_allocator,
                    &mut self.service_allocator,
                    self.logger_addr.clone(),
                    service_path.clone(),
                    None,
                    addr_map,
                    Context::new(),
                );
                match service_info {
                    Ok(service_info) => {
                        let event = JournalEvent::OrchestratorEvent(OrchestratorEvent::Lifecycle(
                            journal::event::orchestrator::LifecycleEvent::ServiceSpawned {
                                path: service_path,
                                service_id: service_info.identifier,
                                service_manifest: service_info.manifest.clone(),
                            },
                        ));
                        self.service_registry.insert(service_info);
                        self.journal
                            .log(vec![cmd_lineage_id], LogLevel::Info, event)
                            .await
                            .unwrap();
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to spawn service: {:?}", e);
                        // Log structured error to journal
                        let event = JournalEvent::OrchestratorEvent(OrchestratorEvent::Lifecycle(
                            journal::event::orchestrator::LifecycleEvent::ServiceSpawnFailed(e),
                        ));
                        self.journal
                            .log(vec![cmd_lineage_id], LogLevel::Error, event)
                            .await
                            .unwrap();
                        eprintln!("{}", error_msg);
                    }
                }
            }
            CommandEvent::Stop { service_id: _ } => (),
        }
    }
}
