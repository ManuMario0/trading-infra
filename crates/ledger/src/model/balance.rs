use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::ops::{Add, AddAssign, Sub, SubAssign};

/// A wrapper around the numeric value used for accounting.
/// This allows us to swap the underlying fixed-point or big-int implementation
/// without touching the ledger logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Balance(pub Decimal);

impl Balance {
    pub const ZERO: Self = Self(Decimal::ZERO);

    pub fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

impl Add for Balance {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self(self.0 + other.0)
    }
}

impl AddAssign for Balance {
    fn add_assign(&mut self, other: Self) {
        self.0 += other.0;
    }
}

impl Sub for Balance {
    type Output = Self;
    fn sub(self, other: Self) -> Self {
        Self(self.0 - other.0)
    }
}

impl SubAssign for Balance {
    fn sub_assign(&mut self, other: Self) {
        self.0 -= other.0;
    }
}

impl Default for Balance {
    fn default() -> Self {
        Self::ZERO
    }
}
