//! CLI control loop for the orchestrator.

use std::io::{self, Write};

use journal::entry::JournalEntry;
use journal::event::JournalEvent;
use journal::event::orchestrator::{
    CommandEvent, CommandKind, DeployCommand, OrchestratorEvent, StopCommand,
};

use crate::app::App;

pub async fn run_cli(app: &mut App) {
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
                let cmd_kind = CommandKind::Deploy(DeployCommand { service_path });
                let cmd = CommandEvent { kind: cmd_kind };
                let event = JournalEvent::OrchestratorEvent(OrchestratorEvent::Command(cmd));
                let entry = JournalEntry::make_entry(
                    app.lineage_generator.next(),
                    app.identity.clone(),
                    Vec::new(),
                    event,
                );
                app.logger.send(&entry).await.unwrap();
                println!("-> Built Command: Deploy");
            }
            "2" => {
                print!("Enter service ID to stop: ");
                io::stdout().flush().unwrap();
                let mut id_input = String::new();
                io::stdin().read_line(&mut id_input).unwrap();

                match id_input.trim().parse::<usize>() {
                    Ok(service_id) => {
                        let cmd_kind = CommandKind::Stop(StopCommand { service_id });
                        let cmd = CommandEvent { kind: cmd_kind };
                        let event =
                            JournalEvent::OrchestratorEvent(OrchestratorEvent::Command(cmd));
                        let entry = JournalEntry::make_entry(
                            app.lineage_generator.next(),
                            app.identity.clone(),
                            Vec::new(),
                            event,
                        );
                        app.logger.send(&entry).await.unwrap();
                        println!("-> Built Command: Stop (ID: {})", service_id);
                    }
                    Err(_) => println!("-> Error: Invalid ID provided. Must be a number."),
                }
            }
            "3" => {
                break;
            }
            _ => {
                println!("-> Unknown option. Please type 1 or 2.");
            }
        }
    }
}
