use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use trading_sdk::InstrumentId;

use crate::core::error::LedgerError;
use crate::model::account::{Account, AccountId, AccountName};
use crate::model::balance::Balance;
use crate::model::transaction::Transaction;

/// The core double-entry engine.
/// Purely focused on mathematical integrity and hierarchical tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ledger {
    pub accounts: Vec<Account>,
    pub history: Vec<Transaction>,
}

impl Ledger {
    pub fn new() -> Self {
        Self {
            accounts: Vec::new(),
            history: Vec::new(),
        }
    }

    pub fn add_account(&mut self, name: AccountName, asset: InstrumentId) -> AccountId {
        let id = AccountId(self.accounts.len());
        self.accounts.push(Account {
            id,
            name,
            asset,
            balance: Balance::ZERO,
        });
        id
    }

    /// Records a transaction if it is balanced.
    pub fn record(&mut self, transaction: Transaction) -> Result<(), LedgerError> {
        let mut instrument_sums: BTreeMap<InstrumentId, Decimal> = BTreeMap::new();

        // 1. Group by Instrument and check balance
        for split in transaction.splits() {
            let account = &self.accounts[split.account.0];
            let instrument_sum = instrument_sums
                .entry(account.asset)
                .or_insert(Decimal::ZERO);
            *instrument_sum += split.amount.0;
        }

        // 2. Validate all involved instruments balance to zero
        for (instrument, sum) in instrument_sums {
            if !sum.is_zero() {
                return Err(LedgerError::UnbalancedInstrument { instrument, sum });
            }
        }

        // 3. Apply balanced splits to vectorized balances
        for split in transaction.splits() {
            self.accounts[split.account.0].balance += split.amount;
        }

        self.history.push(transaction);
        Ok(())
    }

    /// Returns the sum of all accounts matching a specific hierarchical prefix.
    pub fn get_total(&self, prefix: &AccountName) -> Balance {
        self.accounts
            .iter()
            .filter(|acc| acc.name.starts_with(prefix))
            .map(|acc| acc.balance)
            .fold(Balance::ZERO, |acc, b| acc + b)
    }

    /// Computes the net value of the ledger given a set of conversion rates.
    /// Conversion rates should be keyed by AccountName or a string representation.
    pub fn compute_value(&self, conversion_rates: &BTreeMap<AccountName, Decimal>) -> Balance {
        let mut total = Decimal::ZERO;
        for account in self.accounts.iter() {
            if let Some(rate) = conversion_rates.get(&account.name) {
                total += account.balance.0 * rate;
            } else {
                total += account.balance.0;
            }
        }
        Balance(total)
    }
}

#[cfg(test)]
mod tests {
    use crate::model::{split::Split, transaction::TransactionBuilder};

    use super::*;

    #[test]
    fn test_add_account() {
        let mut ledger = Ledger::new();
        let account_id = ledger.add_account(AccountName::from_str("Assets:Cash"), 0);
        assert_eq!(account_id, AccountId(0));
        assert_eq!(ledger.accounts.len(), 1);
        assert_eq!(ledger.accounts[0].balance, Balance::ZERO);
    }

    #[test]
    fn test_record_balanced_transaction() {
        let mut ledger = Ledger::new();
        let account_id = ledger.add_account(AccountName::from_str("Assets:Cash"), 0);
        let transaction = TransactionBuilder::new(0, 0, "Test".to_string())
            .add_split(Split {
                account: account_id,
                amount: Balance(Decimal::from(10)),
            })
            .add_split(Split {
                account: account_id,
                amount: Balance(Decimal::from(-10)),
            })
            .build();
        let result = ledger.record(transaction);
        assert!(result.is_ok());
        assert_eq!(ledger.accounts[0].balance, Balance(Decimal::from(0)));
    }

    #[test]
    fn test_record_unbalanced_transaction() {
        let mut ledger = Ledger::new();
        let account_id = ledger.add_account(AccountName::from_str("Assets:Cash"), 0);
        let transaction = TransactionBuilder::new(0, 0, "Test".to_string())
            .add_split(Split {
                account: account_id,
                amount: Balance(Decimal::from(10)),
            })
            .build();
        let result = ledger.record(transaction);
        assert!(result.is_err());
    }
}
