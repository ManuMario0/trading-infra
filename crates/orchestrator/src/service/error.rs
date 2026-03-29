//! Boot error.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Context {
    context: Vec<String>,
}

impl Context {
    pub fn new() -> Self {
        Self {
            context: Vec::new(),
        }
    }

    pub fn add(&mut self, context: &str) {
        self.context.push(context.to_string());
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RuntimeErrorKind {
    /// Some OS command failed (e.g., bin not found, permissions, pipe failed).
    SystemError { os_error_code: Option<i32> },
    /// An operation took longer than our patience allowed.
    Timeout { max_time_ms: u64 },
    /// The process sent data that does not follow the protocol (e.g., bad JSON, empty stdout).
    ProtocolError { reason: String },
    /// The process exited prematurely with a non-zero status.
    UnexpectedExit { code: Option<i32> },
    /// Internal error, e.g. not having any ports available to allocate to the process.
    InternalError { reason: String },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServiceBootError {
    // Who is the service
    pub path: PathBuf,
    // What was the service asked to do
    pub context: Context,
    // What went wrong
    pub kind: RuntimeErrorKind,
}
