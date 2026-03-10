use ipc::address::Address;

mod export;
mod file;
mod network;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting logger on 127.0.0.1:5555...");

    let address = Address::tcp("127.0.0.1", 5555);
    let mut network = network::Network::new(address).await?;
    let mut exporter = export::Exporter::new("journal.csv")?;

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
