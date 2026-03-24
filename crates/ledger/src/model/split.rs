use crate::model::account::AccountId;
use crate::model::balance::Balance;
use serde::{Deserialize, Serialize};

/// A specific movement of value within a transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Split {
    pub account: AccountId,
    /// The movement in the account. The sum of all splits in a transaction must be zero.
    pub amount: Balance,
}
