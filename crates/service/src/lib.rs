//! Set of definitions and structures that defines a service.
//!
//! This is an interface between the services and the orchestrator. It defines:
//! - The unique identity of a service in [`self::identity`] ;
//! - A generic envelope for IPC traffic in [`self::envelope`] ;
//! - A manifest that describes how the service interacts with the network in [`self::manifest`] ;
//! - A list of environment variables that are provided to services at startup for parametrisation in [`self::env`].
//!

pub mod env;
pub mod envelope;
pub mod event;
pub mod identity;
pub mod manifest;
