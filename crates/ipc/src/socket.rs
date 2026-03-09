//! Typed socket for clean transport layer.

use std::io;
use std::marker::PhantomData;

use serde::{Serialize, de::DeserializeOwned};

use crate::transport::{Receiver, Sender};

/// A socket that only publishes (sends) strongly typed data.
pub struct PubSocket<C, T> {
    transport: T,
    _phantom: PhantomData<C>,
}

impl<C, T> PubSocket<C, T>
where
    C: Serialize,
    T: Sender,
{
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            _phantom: PhantomData,
        }
    }

    /// Serializes the typed payload using postcard and publishes it.
    pub async fn publish(&mut self, payload: &C) -> Result<(), io::Error> {
        let bytes = postcard::to_allocvec(payload)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.transport.send_bytes(&bytes).await
    }
}

/// A socket that only subscribes (receives) typed data.
pub struct SubSocket<C, T> {
    transport: T,
    _phantom: PhantomData<C>,
}

impl<C, T> SubSocket<C, T>
where
    C: DeserializeOwned,
    T: Receiver,
{
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            _phantom: PhantomData,
        }
    }

    /// Blocks until a new message is received and deserializes it.
    pub async fn receive(&mut self) -> Result<C, io::Error> {
        let bytes = self.transport.recv_bytes().await?;
        postcard::from_bytes(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Tries to receive a message without blocking and deserialize it.
    pub async fn try_receive(&mut self) -> Result<C, io::Error> {
        let bytes = self.transport.try_recv().await?;
        postcard::from_bytes(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}
