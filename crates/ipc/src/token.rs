//! A small token factory.
//!
//! Each port engine obtains a unique token prefix - called a [Domain]. This allows the IPC runner to route properly poll interuptions.
//! Then, each port engine is responsible for managing it's sub-token range as it may fit. A token registry is exactly that.
//! It is a small wrapper to easily register sources in a specific port engine.

use std::sync::Arc;

use mio::{Interest, Token, Waker, event::Source};

/// A domain assigned to a port.
///
/// A domain is used to route the token for the correct port.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Domain(u8);

impl From<u8> for Domain {
    fn from(value: u8) -> Self {
        Self(value)
    }
}

impl From<Domain> for u8 {
    fn from(value: Domain) -> Self {
        value.0
    }
}

impl From<Token> for Domain {
    fn from(value: Token) -> Self {
        Self((value.0 >> PREFIX_INDEX) as u8)
    }
}

/// An index used by some port.
///
/// A local index is not an index that will be recorded in the registry instance. It will be used by local port engines
/// to generate local index for general token generation. The token will be this local index prefixed with the prefix
/// assigned to the port.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct LocalIndex(usize);

impl From<usize> for LocalIndex {
    fn from(value: usize) -> Self {
        assert!(
            value < (1 << PREFIX_INDEX),
            "Local index cannot be bigger than 2^{PREFIX_INDEX}"
        );
        Self(value)
    }
}

impl From<LocalIndex> for usize {
    fn from(value: LocalIndex) -> Self {
        value.0
    }
}

impl From<Token> for LocalIndex {
    fn from(value: Token) -> Self {
        Self(value.0 & ((1 << PREFIX_INDEX) - 1))
    }
}

const PREFIX_INDEX: usize = 56;

/// A token registry for the local port.
///
/// It can generate a token from a local index, and is also capable of registering the local index to be available to
/// the poll instance.
pub struct TokenRegistry {
    /// The 8 first bits of the token, fixed prefix for each engine.
    domain: Domain,

    /// The registry of tokens for the current engine.
    registry: Arc<mio::Registry>,
}

impl TokenRegistry {
    /// Creates a new registry from the `prefix` provided at creation and the registry instance.
    #[inline]
    pub const fn new(domain: Domain, registry: Arc<mio::Registry>) -> Self {
        Self { domain, registry }
    }

    /// Registers a source to a local index.
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

    /// Registers a waker to the local index.
    pub fn register_waker(&self, local_index: LocalIndex) -> std::io::Result<Waker> {
        let waker = Waker::new(&self.registry, self.token(local_index))?;
        Ok(waker)
    }

    /// Builds a token for the given local index.
    #[inline]
    fn token(&self, local_index: LocalIndex) -> Token {
        assert!(
            local_index.0 < (1 << PREFIX_INDEX),
            "local_index must be less than 2^{PREFIX_INDEX}"
        );

        Token((self.domain.0 as usize) << PREFIX_INDEX | local_index.0)
    }
}
