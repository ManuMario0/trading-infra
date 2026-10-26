use std::collections::BTreeMap;

/// To showcare the capabilities of the system and test it, we implement here a very simple moving average strategy.
///
/// We compute the EMA20 and buy 100 when stock line is above trading line and sell down to 0 when it crosses in the
/// other direction.
/// We do that for every instrument.
use trading_sdk::{
    Allocation, InstrumentId, Strategist,
    journal::event::{LogLevel, ServiceEvent},
    model::urgency::Urgency,
};

const PERIOD_IMA: u64 = 100;
const MULT_FACTOR_IMA: f64 = 2. / (PERIOD_IMA as f64);

pub struct DummyStrategy {
    emas: BTreeMap<InstrumentId, f64>,
}

impl Strategist for DummyStrategy {
    fn on_market_data(
        &mut self,
        context: trading_sdk::model::context::Context,
        md: trading_sdk::MarketDataBatch,
    ) -> trading_sdk::Allocation {
        let lineage_alloc = context.journal.record(
            vec![md.lineage_id],
            ServiceEvent::Annotation {
                level: LogLevel::Info,
                message: "Generating a fresh allocation".to_string(),
            },
        );

        let mut alloc = Allocation::new(lineage_alloc, Urgency::BestEffort);

        let total_share = md.instrument_ids.len() as f64;

        for (index, instrument) in md.instrument_ids.into_iter().enumerate() {
            let price = md.ask_prices[index] as f64;

            // Update EMA
            let ema = match self.emas.get(&instrument) {
                Some(ema) => {
                    let new_ema = ema * (1. - MULT_FACTOR_IMA) + MULT_FACTOR_IMA * price;
                    self.emas.insert(instrument, new_ema);
                    new_ema
                }
                None => {
                    self.emas.insert(instrument, price);
                    price
                }
            };

            // Checks allocation
            if ema < price {
                alloc.update_position(instrument, 100. / total_share);
            } else {
                alloc.update_position(instrument, 0.);
            }
        }

        alloc
    }
}

pub fn entry_point() -> Box<dyn Strategist> {
    Box::new(DummyStrategy {
        emas: BTreeMap::new(),
    })
}
