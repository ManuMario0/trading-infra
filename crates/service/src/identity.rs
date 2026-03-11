use serde::{Deserialize, Serialize};
use trading_sdk::identity::ServiceId;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Identity {
    pub name: String,
    pub version: String,
    pub identifier: ServiceId,
}

impl Identity {
    pub fn new(name: impl Into<String>, version: impl Into<String>, identifier: ServiceId) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            identifier,
        }
    }
}

#[macro_export]
macro_rules! make_identity {
    ($identifier:expr) => {
        $crate::identity::Identity::new(
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
            $identifier,
        )
    };
}
