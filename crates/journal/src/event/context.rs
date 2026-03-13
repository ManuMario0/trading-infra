//! Generic context handler to attach to events.

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
