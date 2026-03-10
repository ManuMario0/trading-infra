//!  Spawn a logger and a publisher and test the communication.

use ipc::{address::Address, socket::PushSocket};
use logger::network;
use tokio::time::sleep;
use trading_sdk::{
    identity::{AgentId, Identity},
    journal::{
        event::{JournalEntry, JournalEvent, LogLevel},
        lineage::LineageId,
    },
};

#[tokio::test]
async fn test_logger() {
    let handle = tokio::spawn(async move {
        // Process each socket concurrently.
        logger_runner().await
    });
    sleep(std::time::Duration::from_millis(100)).await;

    let journal_event = JournalEvent::Annotation {
        level: LogLevel::Info,
        message: "test".to_string(),
    };
    let journal_entry = JournalEntry {
        lineage_id: LineageId::new(0),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        source: Identity::new("test", "0.0.1", AgentId::new(0)),
        parents: Vec::new(),
        event: journal_event,
    };

    let address = Address::tcp("127.0.0.1", 5555);
    let publisher: Result<PushSocket<JournalEntry>, _> = PushSocket::connect(address).await;
    let mut publisher = publisher.unwrap();
    publisher.send(&journal_entry).await.unwrap();

    assert_eq!(
        handle.await.expect("Thread crashed").timestamp,
        journal_entry.timestamp
    );
}

async fn logger_runner() -> JournalEntry {
    println!("Starting logger on 127.0.0.1:5555...");

    let address = Address::tcp("127.0.0.1", 5555);
    let network: Result<network::Network, _> = network::Network::new(address).await;
    let mut network = network.unwrap();

    println!("Logger ready, waiting for events...");

    loop {
        match network.receive().await {
            Ok(event) => {
                return event;
            }
            Err(e) => {
                eprintln!("Receive error: {}", e);
            }
        }
    }
}
