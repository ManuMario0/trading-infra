use std::path::Path;

use logger::manifest;
use service::env::{WORKING_DIR, parse_ipc_input_addr, parse_or};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Check if manifest request
    let is_manifest_request = parse_or(service::env::MANIFEST_REQUEST, false);
    if is_manifest_request {
        let manifest = logger::manifest::make_manifest();
        println!("{}", serde_json::to_string_pretty(&manifest).unwrap());
        return Ok(());
    }

    let addr = parse_ipc_input_addr(manifest::LOG_PIPE);

    let working_dir: String = parse_or(WORKING_DIR, ".".to_string());
    std::fs::create_dir_all(&working_dir)?;

    let output_file_path = Path::new(&working_dir).join("journal.jsonl");

    println!("Starting logger on {}...", addr);
    println!("Writing journal to: {:?}", output_file_path);

    let _exporter = logger::export::Exporter::new(&output_file_path.to_string_lossy())?;

    println!("Logger ready, waiting for events...");

    Ok(())
}
