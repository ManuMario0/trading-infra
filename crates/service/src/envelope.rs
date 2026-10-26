//! A small socket to abstract underlying communication
//! layer and use our own ServiceId identifiers for communication.
//!
//! It allows us to not rely on IP (which might get overlapped between
//! different services if machines are shared).

use serde::{Deserialize, Serialize};
use trading_sdk::identity::ServiceId;

#[derive(Serialize, Deserialize)]
pub struct Envelope<T> {
    pub source: ServiceId,
    pub payload: T,
}
