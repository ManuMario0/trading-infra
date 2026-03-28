use std::collections::BTreeMap;
use std::io::Read;
use std::path::PathBuf;
use std::process::{Child, Output};
use std::thread;
use std::time::{Duration, Instant};

use ipc::address::Address;
use ipc::socket::SocketRole;
use journal::event::context::Context;
use journal::event::orchestrator::ServiceBootError;
use journal::event::runtime::RuntimeErrorKind;
use service::manifest::PortDefinition;

use crate::port::allocator::Allocator;
use crate::service::info::ServiceInfo;
use crate::service_id::ServiceIdAllocator;

impl ServiceInfo {
    pub fn startup_service(
        port_allocator: &mut Allocator,
        service_allocator: &mut ServiceIdAllocator,
        path: PathBuf,
        working_directory: PathBuf,
        logging_directory: PathBuf,
        overrides: BTreeMap<String, Address>,
        mut context: Context,
    ) -> Result<Self, ServiceBootError> {
        context.add("service launch");

        let manifest = acquire_manifest(path.clone(), context.clone())?;

        let ports = assign_ports(
            path.clone(),
            context.clone(),
            port_allocator,
            &manifest.blueprint.ports,
            &overrides,
        )?;

        // Now bootup the service for real,
        // providing it with logger addr and service id
        let service_id = service_allocator.next();
        let mut command = std::process::Command::new(&path);
        command.env(service::env::SERVICE_ID, service_id.get_inner().to_string());
        let working_directory = working_directory.join(service_id.get_inner().to_string());
        std::fs::create_dir_all(&working_directory).unwrap_or_else(|_| ());
        command.env(
            service::env::WORKING_DIR,
            working_directory.to_string_lossy().to_string(),
        );
        command.env(
            service::env::LOGGING_DIR,
            logging_directory.to_string_lossy().to_string(),
        );

        // Inject ports
        for (i, port_def) in manifest.blueprint.all_ports().enumerate() {
            let env_name = format!("{}_{}", service::env::IPC_INPUT, port_def.name());
            command.env(env_name, ports[i].to_string());
        }

        let child = command.spawn().map_err(|e| ServiceBootError {
            path: path.clone(),
            context: context.clone(),
            kind: RuntimeErrorKind::SystemError {
                os_error_code: e.raw_os_error(),
            },
        })?;

        Ok(ServiceInfo {
            handle: child,
            identifier: service_id,
            manifest,
            ports,
        })
    }
}

fn acquire_manifest(
    path: PathBuf,
    mut context: Context,
) -> Result<service::manifest::Manifest, ServiceBootError> {
    context.add("manifest acquisition");

    // First get the manifest.
    let mut process_launch_context = context.clone();
    process_launch_context.add("launch service");
    let mut child = std::process::Command::new(&path)
        .env(service::env::MANIFEST_REQUEST, true.to_string())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| ServiceBootError {
            path: path.clone(),
            context: process_launch_context,
            kind: RuntimeErrorKind::SystemError {
                os_error_code: e.raw_os_error(),
            },
        })?;

    let output = wait_on_child_with_output(
        path.clone(),
        context.clone(),
        &mut child,
        Duration::from_secs(5),
    )?;

    let mut parse_context = context.clone();
    parse_context.add("parse service stdout");
    let manifest_str = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(&manifest_str).map_err(|e| ServiceBootError {
        path,
        context: parse_context,
        kind: RuntimeErrorKind::ProtocolError {
            reason: format!("Failed to parse manifest: {} (Raw: {})", e, manifest_str),
        },
    })
}

fn wait_on_child_with_output(
    path: PathBuf,
    mut context: Context,
    child: &mut Child,
    timeout: Duration,
) -> Result<Output, ServiceBootError> {
    context.add("wait service termination");

    let mut stdout = child.stdout.take().expect("stdout not piped");
    let mut stderr = child.stderr.take().expect("stderr not piped");

    let (stdout_tx, stdout_rx) = std::sync::mpsc::channel();
    let (stderr_tx, stderr_rx) = std::sync::mpsc::channel();

    thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stdout.read_to_end(&mut buf);
        let _ = stdout_tx.send(buf);
    });

    thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stderr.read_to_end(&mut buf);
        let _ = stderr_tx.send(buf);
    });

    let start = Instant::now();
    let mut sleep_duration = Duration::from_millis(10);

    while start.elapsed() < timeout {
        match child.try_wait() {
            Ok(Some(status)) => {
                let stdout_bytes = stdout_rx.recv().unwrap_or_default();
                let stderr_bytes = stderr_rx.recv().unwrap_or_default();
                return Ok(Output {
                    status,
                    stdout: stdout_bytes,
                    stderr: stderr_bytes,
                });
            }
            Ok(None) => {
                thread::sleep(sleep_duration);
                sleep_duration = std::cmp::min(sleep_duration * 2, Duration::from_millis(100));
            }
            Err(e) => {
                let _ = child.kill();
                return Err(ServiceBootError {
                    path,
                    context,
                    kind: RuntimeErrorKind::SystemError {
                        os_error_code: e.raw_os_error(),
                    },
                });
            }
        }
    }

    // Timeout
    let _ = child.kill();
    Err(ServiceBootError {
        path,
        context,
        kind: RuntimeErrorKind::Timeout {
            max_time_ms: timeout.as_millis() as u64,
        },
    })
}

fn assign_ports(
    path: PathBuf,
    mut context: Context,
    allocator: &mut Allocator,
    port_definitions: &[PortDefinition],
    overrides: &BTreeMap<String, Address>,
) -> Result<Vec<Address>, ServiceBootError> {
    context.add("assign ports");
    let mut addresses = Vec::new();
    for port_def in port_definitions.iter() {
        match port_def.role {
            SocketRole::Bind => {
                let port = allocator.allocate().ok_or(ServiceBootError {
                    path: path.clone(),
                    context: context.clone(),
                    kind: RuntimeErrorKind::InternalError {
                        reason: "no more ports available".to_string(),
                    },
                })?;
                addresses.push(Address::tcp("127.0.0.1", port.0));
            }
            SocketRole::Connect => {
                let addr = overrides
                    .get(port_def.name())
                    .cloned()
                    .ok_or(ServiceBootError {
                        path: path.clone(),
                        context: context.clone(),
                        kind: RuntimeErrorKind::ProtocolError {
                            reason: format!(
                                "missing connection address for port {}",
                                port_def.name()
                            ),
                        },
                    })?;
                addresses.push(addr);
            }
        }
    }
    Ok(addresses)
}
