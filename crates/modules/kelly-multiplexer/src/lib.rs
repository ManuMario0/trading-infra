use trading_sdk::identity::ServiceId;
use trading_sdk::model::ledger::LedgerUpdate;
use trading_sdk::{Allocation, Initiable, Multiplexist};

pub struct KellyMultiplexer {}

impl Multiplexist for KellyMultiplexer {
    fn on_allocation(
        &mut self,
        context: trading_sdk::model::context::Context,
        source_id: ServiceId,
        batch: Allocation,
    ) -> trading_sdk::Allocation {
        todo!()
    }

    fn on_ledger_update(
        &mut self,
        context: trading_sdk::model::context::Context,
        source_id: ServiceId,
        ledger_update: LedgerUpdate,
    ) {
        todo!()
    }
}

impl Initiable for KellyMultiplexer {
    fn init() -> Self {
        todo!()
    }
}

trading_sdk::export_multiplexer!(KellyMultiplexer);
