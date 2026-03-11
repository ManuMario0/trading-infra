use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting Multiplexer Wrapper...");

    // The forge modifies Cargo.toml to inject the module dependency here,
    // and we call the entry point defined by the export_multiplexer! macro.
    let mut _multiplexer = __target_module__::entry_point();

    // V0: Infinite sleep to keep the process alive for testing the forge spawn
    // Future: Setup IPC sockets and routing here!
    loop {
        sleep(Duration::from_secs(60)).await;
    }
}
