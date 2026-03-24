use serde::{Deserialize, Serialize};

use crate::model::account::AccountId;
use crate::model::balance::Balance;
use crate::model::split::Split;

/// A balanced double-entry transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    id: u64,
    timestamp: u128,
    description: String,
    splits: Vec<Split>,
}

impl Transaction {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn timestamp(&self) -> u128 {
        self.timestamp
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn splits(&self) -> &[Split] {
        &self.splits
    }
}

pub struct TransactionBuilder {
    id: u64,
    timestamp: u128,
    description: String,
    splits: Vec<Split>,
}

impl TransactionBuilder {
    pub fn new(id: u64, timestamp: u128, description: String) -> Self {
        Self {
            id,
            timestamp,
            description,
            splits: Vec::new(),
        }
    }

    pub fn add_split(mut self, split: Split) -> Self {
        self.splits.push(split);
        self
    }

    /// Adds a balanced "Bridge" exchange between two different unit types.
    /// This creates 4 splits:
    /// 1. From Source Account (e.g. Assets:Cash)
    /// 2. To Source Bridge (e.g. Trading:USD)
    /// 3. From Target Bridge (e.g. Trading:AAPL)
    /// 4. To Target Account (e.g. Assets:AAPL)
    pub fn add_exchange(
        mut self,
        source_acc: AccountId,
        source_bridge: AccountId,
        target_bridge: AccountId,
        target_acc: AccountId,
        source_amount: Balance, // e.g. -1500 USD
        target_amount: Balance, // e.g. +10 AAPL
    ) -> Self {
        // Source side (Currency)
        self.splits.push(Split {
            account: source_acc,
            amount: source_amount,
        });
        self.splits.push(Split {
            account: source_bridge,
            amount: crate::model::balance::Balance(
                source_amount.0 * rust_decimal::Decimal::NEGATIVE_ONE,
            ),
        });

        // Target side (Asset)
        self.splits.push(Split {
            account: target_bridge,
            amount: crate::model::balance::Balance(
                target_amount.0 * rust_decimal::Decimal::NEGATIVE_ONE,
            ),
        });
        self.splits.push(Split {
            account: target_acc,
            amount: target_amount,
        });

        self
    }

    pub fn build(self) -> Transaction {
        Transaction {
            id: self.id,
            timestamp: self.timestamp,
            description: self.description,
            splits: self.splits,
        }
    }
}
