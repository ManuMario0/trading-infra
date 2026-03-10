//! Export an event to a file.

use trading_sdk::journal::event::{JournalEntry, JournalEvent};

use crate::file::File;

pub struct Exporter {
    file: File,
}

impl Exporter {
    pub fn new(path: &str) -> Result<Self, std::io::Error> {
        let file = File::new(path)?;
        Ok(Self { file })
    }

    pub fn export(&mut self, event: JournalEntry) -> Result<(), std::io::Error> {
        self.file.append(&to_csv_entry(event))
    }
}

fn to_csv_entry(entry: JournalEntry) -> String {
    let mut serializing_vec = Vec::new();
    serializing_vec.push(entry.lineage_id.get_inner().to_string());
    serializing_vec.push(entry.timestamp.to_string());
    serializing_vec.push(entry.source.get_identifier().get_inner().to_string());
    serializing_vec.push(entry.source.get_name().to_string());
    serializing_vec.push(entry.source.get_version().to_string());
    serializing_vec.push(format!(
        "[{}]",
        entry
            .parents
            .iter()
            .map(|p| p.get_inner().to_string())
            .collect::<Vec<_>>()
            .join(";")
    ));
    let event_str = match entry.event {
        JournalEvent::StrategySignal {
            instrument_id,
            signal_name,
            signal_value,
            metadata,
        } => {
            format!(
                "StrategySignal[instrument={:?} name={:?} val={:?} meta={:?}]",
                instrument_id, signal_name, signal_value, metadata
            )
        }
        JournalEvent::Annotation { level, message } => {
            let level_str = match level {
                trading_sdk::journal::event::LogLevel::Info => "Info",
                trading_sdk::journal::event::LogLevel::Warn => "Warn",
                trading_sdk::journal::event::LogLevel::Error => "Error",
            };
            format!("Annotation[level={} msg={:?}]", level_str, message)
        }
        JournalEvent::MarketDataReceived { batch_size, window } => {
            format!(
                "MarketDataReceived[batch_size={} window={:?}]",
                batch_size, window
            )
        }
        JournalEvent::AllocationGenerated { allocation } => {
            format!("AllocationGenerated[alloc={:?}]", allocation)
        }
        JournalEvent::OrderSubmitted { order } => {
            format!("OrderSubmitted[order={:?}]", order)
        }
        JournalEvent::ExecutionReceived { result } => {
            format!("ExecutionReceived[result={:?}]", result)
        }
    };
    // Replace commas from inner Debug formatting with semicolons to maintain CSV structural integrity
    serializing_vec.push(event_str.replace(",", ";"));
    format!("{}\n", serializing_vec.join(","))
}
