//! A registry is a subset of all available tokens.

use std::sync::Arc;

use mio::{Interest, Token, Waker, event::Source};

#[derive(PartialEq, Eq)]
pub struct LocalIndex(pub usize);

impl From<usize> for LocalIndex {
    fn from(value: usize) -> Self {
        Self(value)
    }
}

impl From<LocalIndex> for usize {
    fn from(value: LocalIndex) -> Self {
        value.0
    }
}

pub struct TokenRegistry {
    /// The 8 first bits of the token, fixed prefix for each engine.
    prefix: u8,

    /// The registry of tokens for the current engine.
    registry: Arc<mio::Registry>,
}

impl TokenRegistry {
    #[inline]
    pub const fn new(prefix: u8, registry: Arc<mio::Registry>) -> Self {
        Self { prefix, registry }
    }

    /// Build a token for the given local index.
    #[inline]
    pub const fn token(&self, local_index: LocalIndex) -> Token {
        assert!(
            local_index.0 < (1 << 56),
            "local_index must be less than 2^56"
        );

        Token((self.prefix as usize) << 56 | local_index.0)
    }

    /// Register a token with the registry.
    #[inline]
    pub fn register(
        &self,
        source: &mut impl Source,
        local_index: LocalIndex,
        interest: Interest,
    ) -> std::io::Result<()> {
        let token = self.token(local_index);
        self.registry.register(source, token, interest)
    }

    pub fn register_waker(&self, local_index: LocalIndex) -> std::io::Result<Waker> {
        let waker = Waker::new(&self.registry, self.token(local_index))?;
        Ok(waker)
    }
}

/// Return the prefix of a token.
#[inline]
pub const fn prefix(token: Token) -> u8 {
    (token.0 >> 56) as u8
}

/// Returns the local index from a token.
#[inline]
pub const fn local_index(token: Token) -> LocalIndex {
    LocalIndex(token.0 & ((1 << 56) - 1))
}
