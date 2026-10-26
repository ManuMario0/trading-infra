//! Bootstrap of the orchestrator.
//!
//! It gets all the environment variables loaded and launch it's internal journal.
//!
//! This file is slightly too big but will get reformated.

use std::path::PathBuf;
use std::thread;

use service::manifest::Manifest;
use trading_sdk::journal::lineage::LineageId;

use journal::entry::{LogLevel, ServiceKind, ThreadKind};
use journal::journal::Journal;
use service::env::parse_or;
use service::identity::Identity;
use service::make_identity;

use crate::event::{CommandEvent, LifecycleEvent, OrchestratorEvent};
use crate::port::allocator::Allocator;
use crate::service::boot;
use crate::service::error::{Context, ServiceBootError};
use crate::service::info::ServiceInfo;
use crate::service::registry::ServiceRegistry;
use crate::service_id::ServiceIdAllocator;

pub struct App {
    pub identity: Identity,
    pub service_allocator: ServiceIdAllocator,
    pub port_allocator: Allocator,
    pub journal: Journal,
    pub service_registry: ServiceRegistry,
    pub startup_id: LineageId,
    pub logging_dir: PathBuf,

    _journal_thread: thread::JoinHandle<()>,
}

impl App {
    pub fn bootstrap() -> Self {
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

        // Create a new allocator for the ports.
        // The allocator will be used to allocate ports for the services.
        let port_range_min = parse_or(service::env::PORT_RANGE_MIN, 10000);
        let port_range_max = parse_or(service::env::PORT_RANGE_MAX, 12000);
        let allocator = Allocator::new(port_range_min..=port_range_max);

        // Create journal
        let logging_dir = parse_or(
            service::env::LOGGING_DIR,
            PathBuf::from(working_dir.clone()).join("logs"),
        );
        std::fs::create_dir_all(&logging_dir).unwrap_or_else(|_| ());

        let (journal, mut journal_runner) = Journal::init(
            1024,
            orchestrator_identity.identifier,
            ServiceKind::Orchestrator,
            PathBuf::from(working_dir.clone()),
            logging_dir.clone(),
        );
        let journal_thread = thread::spawn(move || {
            journal_runner.run();
        });

        // Record Lifecycle Events
        let startup_event = OrchestratorEvent::Lifecycle(LifecycleEvent::Startup {
            working_dir: working_dir.clone(),
        });
        let startup_id = journal
            .log(ThreadKind::Main, vec![], LogLevel::Info, startup_event)
            .unwrap();

        let service_working_dir = PathBuf::from(working_dir.clone()).join("services");
        std::fs::create_dir_all(&service_working_dir).unwrap_or_else(|_| ());
        let service_registry = ServiceRegistry::new(service_working_dir);

        Self {
            identity: orchestrator_identity,
            service_allocator: service_id_allocator,
            port_allocator: allocator,
            journal,
            service_registry,
            startup_id,
            logging_dir,
            _journal_thread: journal_thread,
        }
    }

    pub fn process_command(&mut self, cmd: CommandEvent) {
        let event = OrchestratorEvent::Command(cmd.clone());
        let cmd_lineage_id = self
            .journal
            .log(
                ThreadKind::Main,
                vec![self.startup_id],
                LogLevel::Info,
                event,
            )
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
                    service_path.clone(),
                    self.service_registry.working_dir().clone(),
                    self.logging_dir.clone(),
                    addr_map,
                    Context::new(),
                );
                match service_info {
                    Ok(service_info) => {
                        println!(
                            "Started service {} with id {}",
                            service_info.manifest.name,
                            service_info.identifier.get_inner()
                        );
                        println!("List of attributed port numbers:");
                        for (p, d) in service_info
                            .ports
                            .iter()
                            .zip(service_info.manifest.blueprint.all_ports())
                        {
                            println!("{}: {p}", d.name());
                        }
                        let event = OrchestratorEvent::Lifecycle(LifecycleEvent::ServiceSpawned {
                            path: service_path,
                            service_id: service_info.identifier,
                            service_manifest: service_info.manifest.clone(),
                        });
                        self.service_registry.insert(service_info);
                        self.journal
                            .log(
                                ThreadKind::Main,
                                vec![cmd_lineage_id],
                                LogLevel::Info,
                                event,
                            )
                            .unwrap();
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to spawn service: {:?}", e);
                        // Log structured error to journal
                        let event =
                            OrchestratorEvent::Lifecycle(LifecycleEvent::ServiceSpawnFailed(e));
                        self.journal
                            .log(
                                ThreadKind::Main,
                                vec![cmd_lineage_id],
                                LogLevel::Error,
                                event,
                            )
                            .unwrap();
                        eprintln!("{}", error_msg);
                    }
                }
            }
            CommandEvent::Stop { service_id: _ } => (),
        }
    }

    /// Returns the manifest of a service if possible.
    pub fn acquire_manifest(
        &self,
        path: PathBuf,
        context: Context,
    ) -> Result<Manifest, ServiceBootError> {
        boot::acquire_manifest(path, context)
    }
}
