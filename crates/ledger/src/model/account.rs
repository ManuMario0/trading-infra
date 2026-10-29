use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};
use trading_sdk::InstrumentId;

use crate::model::balance::Balance;

/// A unique identifier for an account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AccountId(pub usize);

/// A structured, hierarchical account name.
/// Stored as a vector of components (e.g., ["Assets", "Broker", "Cash"]).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AccountName(pub Vec<String>);

impl AccountName {
    /// Checks if this account is a child of or equal to another account prefix.
    pub fn starts_with(&self, other: &AccountName) -> bool {
        if other.0.len() > self.0.len() {
            return false;
        }
        self.0.iter().zip(other.0.iter()).all(|(a, b)| a == b)
    }
}

impl FromStr for AccountName {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.split(':').map(|p| p.to_string()).collect()))
    }
}

impl fmt::Display for AccountName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.join(":"))
    }
}

/// Represents a single account with its structured name and current balance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub id: AccountId,
    pub name: AccountName,
    pub asset: InstrumentId,
    pub balance: Balance,
}
