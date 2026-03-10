//! Responsible for file recording of logs.

use std::{fs, io::Write};

pub struct File {
    file: fs::File,
}

impl File {
    pub fn new(path: &str) -> Result<Self, std::io::Error> {
        let file = fs::File::create(path)?;
        Ok(Self { file })
    }

    pub fn append(&mut self, event: &str) -> Result<(), std::io::Error> {
        self.file.write_all(event.as_bytes())
    }
}
