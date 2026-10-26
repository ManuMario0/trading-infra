//! CLI control loop for the orchestrator.

use std::collections::BTreeMap;
use std::io::{self, Write};

use ipc::socket::SocketRole;

use crate::app::App;
use crate::event::CommandEvent;
use crate::service::error::Context;

pub fn run_cli(app: &mut App) {
    // Bootup main loop
    loop {
        println!("\n=== Orchestrator Control ===");
        println!("1. Deploy a new service");
        println!("2. Stop a running service");
        println!("3. Exit");
        print!("Select an option (1/2/3): ");
        io::stdout().flush().unwrap(); // Ensure prompt prints before waiting for input

        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            eprintln!("Failed to read input");
            continue;
        }

        match input.trim() {
            "1" => {
                print!("Enter path to service executable: ");
                io::stdout().flush().unwrap();
                let mut path_input = String::new();
                io::stdin().read_line(&mut path_input).unwrap();

                let service_path = std::path::PathBuf::from(path_input.trim());

                let manifest = match app.acquire_manifest(service_path.clone(), Context::new()) {
                    Ok(manifest) => manifest,
                    Err(_) => {
                        println!("Cannot read the manifest of the service");
                        continue;
                    }
                };

                let mut addr_map = BTreeMap::new();
                println!(
                    "Enter port overrides (FORMAT: PORT_NAME:address, e.g. LOGS:tcp://127.0.0.1:5000)"
                );

                for p in manifest.blueprint.all_ports() {
                    let mut success = false;
                    while !success {
                        match p.role {
                            SocketRole::Bind => {
                                print!("{} (auto assign possible): ", p.name());
                            }
                            SocketRole::Connect => {
                                print!("{}: ", p.name());
                            }
                        }
                        io::stdout().flush().unwrap();
                        let mut override_input = String::new();
                        io::stdin().read_line(&mut override_input).unwrap();
                        let trimmed = override_input.trim();
                        if !trimmed.is_empty() {
                            match trimmed.parse::<ipc::address::Address>() {
                                Ok(addr) => {
                                    addr_map.insert(p.name().to_string(), addr);
                                    success = true;
                                }
                                Err(e) => {
                                    println!("-> Error: Invalid address '{}': {}", trimmed, e);
                                }
                            }
                        } else {
                            if p.role == SocketRole::Bind {
                                success = true;
                            } else {
                                println!("You have to enter an address for this socket");
                            }
                        }
                    }
                }

                let cmd = CommandEvent::Deploy {
                    service_path,
                    addr_map,
                };
                app.process_command(cmd);
                println!("-> Sent Command: Deploy");
            }
            "2" => {
                print!("Enter service ID to stop: ");
                io::stdout().flush().unwrap();
                let mut id_input = String::new();
                io::stdin().read_line(&mut id_input).unwrap();

                match id_input.trim().parse::<u32>() {
                    Ok(service_id) => {
                        let cmd = CommandEvent::Stop { service_id };
                        app.process_command(cmd);
                        println!("-> Sent Command: Stop (ID: {})", service_id);
                    }
                    Err(_) => println!("-> Error: Invalid ID provided. Must be a number."),
                }
            }
            "3" => {
                break;
            }
            _ => {
                println!("-> Unknown option. Please type 1, 2, or 3.");
            }
        }
    }
}
