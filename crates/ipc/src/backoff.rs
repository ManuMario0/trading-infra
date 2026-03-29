//! Exponential backoff timer logic.
//!
//! Provides a standalone structural timer used by the network reconnect engines
//! to dictate how long to wait before attempting another connection after a failure.
//! This isolates the retry logic and math completely from the socket abstractions.

use std::time::{Duration, Instant};

/// A generic exponential backoff timer for network reconnections.
pub struct BackoffTimer {
    base_duration: Duration,
    max_duration: Duration,
    multiplier: u32,

    current_duration: Duration,
    pub attempt: u32,
    next_retry: Instant,
}

impl BackoffTimer {
    /// Create a new backoff timer with custom parameters.
    pub fn new(base_duration: Duration, max_duration: Duration, multiplier: u32) -> Self {
        Self {
            base_duration,
            max_duration,
            multiplier,
            current_duration: base_duration,
            attempt: 0,
            // Initializes to 'now' so the very first attempt doesn't block
            next_retry: Instant::now(),
        }
    }

    /// Instantiates a standard network backoff timer.
    /// Starts at 100ms, maxes out at 30s, doubles on every failure.
    pub fn default_network() -> Self {
        Self::new(Duration::from_millis(100), Duration::from_secs(30), 2)
    }

    /// Check if enough time has passed to try again.
    #[inline(always)]
    pub fn can_retry(&self, now: Instant) -> bool {
        now >= self.next_retry
    }

    /// Record a failure: advance the attempt counter and compute the next retry time.
    pub fn mark_failed(&mut self) {
        self.attempt += 1;

        // Compute new duration, clamped to max_duration
        self.current_duration = self.current_duration.saturating_mul(self.multiplier);
        if self.current_duration > self.max_duration {
            self.current_duration = self.max_duration;
        }

        self.next_retry = Instant::now() + self.current_duration;
    }

    /// Record a success: reset the attempt counter and backoff duration.
    pub fn reset(&mut self) {
        self.attempt = 0;
        self.current_duration = self.base_duration;
        self.next_retry = Instant::now();
    }

    pub fn is_maxed(&self) -> bool {
        self.current_duration == self.max_duration
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }
}
