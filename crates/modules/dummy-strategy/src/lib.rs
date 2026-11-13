//! To showcare the capabilities of the system and test it, we implement here a very simple moving average strategy.
//!
//! We compute the EMA20 and buy 100 when stock line is above trading line and sell down to 0 when it crosses in the
//! other direction.
//! We do that for every instrument.

use std::collections::BTreeMap;

use trading_sdk::journal::event::{LogLevel, ServiceEvent};
use trading_sdk::model::allocation::{Allocation, Position, Weight};
use trading_sdk::model::instrument::InstrumentId;
use trading_sdk::model::urgency::Urgency;
use trading_sdk::traits::strategist::Strategist;

const PERIOD_IMA: u64 = 100;
const MULT_FACTOR_IMA: f64 = 2. / (PERIOD_IMA as f64);

pub struct DummyStrategy {
    emas: BTreeMap<InstrumentId, f64>,
}

impl Strategist for DummyStrategy {
    fn on_market_data(
        &mut self,
        context: trading_sdk::model::context::Context,
        md: trading_sdk::model::market_data::MarketDataBatch,
    ) -> trading_sdk::model::allocation::Allocation {
        let lineage_alloc = context.journal.record(
            vec![md.lineage_id],
            ServiceEvent::Annotation {
                level: LogLevel::Info,
                message: "Generating a fresh allocation".to_string(),
            },
        );

        let mut alloc = Allocation::new();

        let total_share = md.instrument_ids.len() as f64;

        for (index, instrument) in md.instrument_ids.into_iter().enumerate() {
            let price = f64::from(md.ask_prices[index]);

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
                let position = Position::new(
                    Weight::new(100. / total_share).unwrap(),
                    Urgency::BestEffort,
                    lineage_alloc,
                );
                alloc.update_position(instrument, position);
            } else {
                let position =
                    Position::new(Weight::new(0.).unwrap(), Urgency::BestEffort, lineage_alloc);
                alloc.update_position(instrument, position);
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
