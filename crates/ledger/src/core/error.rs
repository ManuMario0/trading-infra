use rust_decimal::Decimal;

pub enum LedgerError {
    UnbalancedInstrument {
        instrument: trading_sdk::InstrumentId,
        sum: Decimal,
    },
    AccountNotFound(String),
}
