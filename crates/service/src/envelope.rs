//! Envelope type to wrap source.

use serde::{Deserialize, Serialize};
use trading_sdk::identity::ServiceId;

#[derive(Serialize, Deserialize)]
pub struct Envelope<T> {
    pub source: ServiceId,
    pub payload: T,
}
