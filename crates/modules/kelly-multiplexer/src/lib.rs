use trading_sdk::model::allocation::Allocation;
use trading_sdk::model::ledger::LedgerUpdate;

/// Kelly Multiplexer logic.
///
/// It should receive allocations from multiple strategies and ledger updates,
/// and output aggregated allocations based on the Kelly Criterion.
pub struct KellyMultiplexer {
    // Current state of the account
    pub ledger: Option<LedgerUpdate>,
    // You might want to store a list of current target allocations
}

impl KellyMultiplexer {
    pub fn new() -> Self {
        Self { ledger: None }
    }

    /// Process an incoming allocation request from a strategy.
    /// Should return the modified allocation (proportionally scaled).
    pub fn on_allocation(&mut self, allocation: Allocation) -> Option<Allocation> {
        // TODO: Implement Kelly logic here
        // 1. Check if we have ledger info
        // 2. Calculate the fraction f* for this specific asset/strategy
        // 3. Scale the allocation: new_size = strategy_request * f*

        Some(allocation) // For now, just a pass-through
    }

    /// Update the internal state with the latest ledger info.
    pub fn on_ledger_update(&mut self, update: LedgerUpdate) {
        self.ledger = Some(update);
    }
}
