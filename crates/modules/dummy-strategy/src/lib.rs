use trading_sdk::Strategist;

pub struct DummyStrategy {}

impl Strategist for DummyStrategy {
    fn on_market_data(
        &mut self,
        context: trading_sdk::model::context::Context,
        md: trading_sdk::MarketDataBatch,
    ) -> trading_sdk::Allocation {
        todo!()
    }
}

pub fn entry_point() -> Box<dyn Strategist> {
    Box::new(DummyStrategy {})
}
