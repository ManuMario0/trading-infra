//! Defines a top of the book structure that stores the last known bid/ask and volumes.
//! I will start implementation with hashmap and see what perf we get.
//!
//! I will want to compare after with SOA static preallocated slice implementation.

use std::{
    collections::{BTreeMap, HashMap},
    fmt::Display,
    time::{SystemTime, UNIX_EPOCH},
};

use trading_sdk::{MarketDataBatch, journal::lineage::LineageId};

use crate::parser::QuoteUpdateMessage;

/// Quite slow: P90 at 50 cycles per messages.
pub struct HashMapBookKeeper {
    quote_map: HashMap<[u8; 8], SimpleQuote>,
}

pub struct SimpleQuote {
    bid_price: u64,
    bid_size: u32,
    ask_price: u64,
    ask_size: u32,
}

impl HashMapBookKeeper {
    pub fn new() -> Self {
        Self {
            quote_map: HashMap::new(),
        }
    }

    #[inline(always)]
    pub fn update_quote(&mut self, update: QuoteUpdateMessage) {
        // We insert and keep it in the map. This way we don't have to
        // pay the cost of remove and insertion everytime.
        if update.bid_price == 0 && update.ask_price == 0 {
            return;
        }
        let quote = self.quote_map.entry(update.symbol).or_insert(SimpleQuote {
            bid_price: 0,
            bid_size: 0,
            ask_price: 0,
            ask_size: 0,
        });
        quote.bid_price = update.bid_price;
        quote.bid_size = update.bid_size;
        quote.ask_price = update.ask_price;
        quote.ask_size = update.ask_size;
    }
}

impl Display for HashMapBookKeeper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "HashMapBookKeeper:")?;
        for (symbol, quote) in &self.quote_map {
            write!(
                f,
                "\t{}: {} @ {} / {} @ {}\n",
                String::from_utf8_lossy(symbol),
                quote.bid_size,
                quote.bid_price,
                quote.ask_size,
                quote.ask_price
            )?;
        }
        Ok(())
    }
}

/// Super slow: P90 at 180 cycles per messages.
pub struct BTreeMapBookKeeper {
    quote_map: BTreeMap<[u8; 8], SimpleQuote>,
}

impl BTreeMapBookKeeper {
    pub fn new() -> Self {
        Self {
            quote_map: BTreeMap::new(),
        }
    }

    #[inline(always)]
    pub fn update_quote(&mut self, update: QuoteUpdateMessage) {
        // We insert and keep it in the map. This way we don't have to
        // pay the cost of remove and insertion everytime.
        if update.bid_price == 0 && update.ask_price == 0 {
            return;
        }
        let quote = self.quote_map.entry(update.symbol).or_insert(SimpleQuote {
            bid_price: 0,
            bid_size: 0,
            ask_price: 0,
            ask_size: 0,
        });
        quote.bid_price = update.bid_price;
        quote.bid_size = update.bid_size;
        quote.ask_price = update.ask_price;
        quote.ask_size = update.ask_size;
    }
}

impl Display for BTreeMapBookKeeper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "BTreeMapBookKeeper:")?;
        for (symbol, quote) in &self.quote_map {
            write!(
                f,
                "\t{}: {} @ {} / {} @ {}\n",
                String::from_utf8_lossy(symbol),
                quote.bid_size,
                quote.bid_price,
                quote.ask_size,
                quote.ask_price
            )?;
        }
        Ok(())
    }
}

// ──────────────────────────── SOA BookKeeper ───────────────────────────

const SOA_CAPACITY: usize = 32768; // Must be a power of 2
const SOA_MASK: usize = SOA_CAPACITY - 1;

/// Insane perfs: P90 at 30 cycles.
pub struct SoaBookKeeper {
    /// Stores the u64 representation of the 8-byte symbol.
    /// 0 means the slot is empty.
    symbols: Vec<u64>,

    bid_price: Vec<u64>,
    ask_price: Vec<u64>,
    bid_size: Vec<u32>,
    ask_size: Vec<u32>,
}

impl SoaBookKeeper {
    pub fn new() -> Self {
        Self {
            symbols: vec![0; SOA_CAPACITY],
            bid_price: vec![0; SOA_CAPACITY],
            ask_price: vec![0; SOA_CAPACITY],
            bid_size: vec![0; SOA_CAPACITY],
            ask_size: vec![0; SOA_CAPACITY],
        }
    }

    #[inline(always)]
    fn hash(val: u64) -> usize {
        // Fast integer multiplication hash (FxHash style)
        let hash = val.wrapping_mul(0x517cc1b727220a95);
        // Shift down to fit in our capacity
        (hash >> (64 - 15)) as usize // 15 bits = 32768
    }

    #[inline(always)]
    pub fn update_quote(&mut self, update: QuoteUpdateMessage) {
        if update.bid_price == 0 && update.ask_price == 0 {
            return;
        }

        // 1 instruction to cast
        let symbol_u64 = u64::from_le_bytes(update.symbol);

        // Find the slot via linear probing
        let mut idx = Self::hash(symbol_u64) & SOA_MASK;

        loop {
            let slot_sym = self.symbols[idx];
            if slot_sym == symbol_u64 {
                // Found existing symbol
                break;
            }
            if slot_sym == 0 {
                // Found empty slot claim it
                self.symbols[idx] = symbol_u64;
                break;
            }
            // Collision, check next slot
            idx = (idx + 1) & SOA_MASK;
        }

        // Update the SOA
        self.bid_price[idx] = update.bid_price;
        self.ask_price[idx] = update.ask_price;
        self.bid_size[idx] = update.bid_size;
        self.ask_size[idx] = update.ask_size;
    }

    pub fn into_market_data_batch(&self, lineage_id: LineageId) -> MarketDataBatch {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let mut batch = MarketDataBatch {
            lineage_id,
            instrument_ids: vec![],
            bid_prices: vec![],
            ask_prices: vec![],
            last_prices: vec![],
            timestamps: vec![],
        };
        for (index, instrument_id) in self.symbols.iter().enumerate() {
            if *instrument_id != 0 {
                batch.instrument_ids.push(*instrument_id as usize);
                batch.bid_prices.push(self.bid_price[index]);
                batch.ask_prices.push(self.ask_price[index]);
                batch.last_prices.push(self.ask_price[index]);
                batch.timestamps.push(timestamp);
            }
        }
        batch
    }
}

impl Display for SoaBookKeeper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "SoaBookKeeper:")?;
        for idx in 0..SOA_CAPACITY {
            let sym_u64 = self.symbols[idx];
            if sym_u64 != 0 {
                let symbol_bytes = sym_u64.to_le_bytes();
                write!(
                    f,
                    "\t{}: {} @ {} / {} @ {}\n",
                    String::from_utf8_lossy(&symbol_bytes),
                    self.bid_size[idx],
                    self.bid_price[idx],
                    self.ask_size[idx],
                    self.ask_price[idx]
                )?;
            }
        }
        Ok(())
    }
}
