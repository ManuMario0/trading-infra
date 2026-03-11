//! Export an event to a file.

use journal::entry::JournalEntry;

use crate::file::File;

pub struct Exporter {
    file: File,
}

impl Exporter {
    pub fn new(path: &str) -> Result<Self, std::io::Error> {
        let file = File::new(path)?;
        Ok(Self { file })
    }

    pub fn export(&mut self, entry: JournalEntry) -> Result<(), std::io::Error> {
        // We write the structured entry entirely as JSON-Lines (.jsonl).
        // This flawlessly supports nested structures, arrays, and complex enums natively!
        match serde_json::to_string(&entry) {
            Ok(json_str) => self.file.append(&format!("{}\n", json_str)),
            Err(e) => {
                eprintln!("Failed to serialize journal entry to JSON: {}", e);
                // Return generic IO error if serialization fails
                Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            }
        }
    }
}
