//! Channel errors

#[derive(Debug)]
pub enum ChannelError {
    FullBuffer { buffer_size: u64 },
}

impl ChannelError {
    pub fn buffer_size(&self) -> u64 {
        match self {
            ChannelError::FullBuffer { buffer_size } => *buffer_size,
        }
    }
}
