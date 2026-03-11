use std::path::Path;

use ipc::address::Address;
use service::env::{IPC_LOGGER, WORKING_DIR, parse_or};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port: u16 = parse_or(IPC_LOGGER, 5555);
    let working_dir: String = parse_or(WORKING_DIR, ".".to_string());

    // Ensure the directory exists
    std::fs::create_dir_all(&working_dir)?;

    let output_file_path = Path::new(&working_dir).join("journal.jsonl");

    println!("Starting logger on 127.0.0.1:{}...", port);
    println!("Writing journal to: {:?}", output_file_path);

    let address = Address::tcp("127.0.0.1", port);
    let mut network = logger::network::Network::new(address).await?;
    let mut exporter = logger::export::Exporter::new(&output_file_path.to_string_lossy())?;

    println!("Logger ready, waiting for events...");

    loop {
        match network.receive().await {
            Ok(event) => {
                if let Err(e) = exporter.export(event) {
                    eprintln!("Failed to write event: {}", e);
                }
            }
            Err(e) => {
                eprintln!("Receive error: {}", e);
                break;
            }
        }
    }

    Ok(())
}
