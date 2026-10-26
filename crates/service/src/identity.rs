//! Unique identity of a service.
//!
//! A unique identifier is provided at startup for every services through the environment variable [`crate::env::SERVICE_ID`].
//! Its use is strictly limited to the orchestrator and the identification of the services.
//!
//! # Note
//!
//! After investigation, this structure is mostly outdated. All informations can be reconstructed from row
//! service id and manifest. Should be removed.
//!
//! # Example
//!
//! ```rust
//! # use service;
//! # use trading_sdk::identity::ServiceId;
//!
//! let identity = service::make_identity!(ServiceId::new(0));
//! ```

use serde::{Deserialize, Serialize};
use trading_sdk::identity::ServiceId;

/// A structure that uniquely identity a service.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Identity {
    pub name: String,
    pub version: String,
    pub identifier: ServiceId,
}

impl Identity {
    /// Creates a new identity.
    ///
    /// Prefer the use of the [`make_identity`] macro.
    pub fn new(name: impl Into<String>, version: impl Into<String>, identifier: ServiceId) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            identifier,
        }
    }
}

/// Creates a new identity from the unique identifier provided.
///
/// # Example
///
/// ```rust
/// # use service;
/// # use trading_sdk::identity::ServiceId;
///
/// let identity = service::make_identity!(ServiceId::new(0));
/// ```
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
