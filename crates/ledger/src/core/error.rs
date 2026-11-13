use rust_decimal::Decimal;

pub enum LedgerError {
    UnbalancedInstrument {
        instrument: trading_sdk::model::instrument::InstrumentId,
        sum: Decimal,
    },
    AccountNotFound(String),
}
