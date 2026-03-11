//! Defines the shape of the service.
//!
//! This includes:
//! - Inputs and outputs, alongside the expected types
//! - Instance specific parameters
//!
//! At startup, those are provided to the service as environment variables.
//!
//! However, some parameters environment variables are always present for all processes
//! and must not be specify in the manifest. For example, the environment variable
//! `IPC_LOGGER` is always set and can be used safely to setup the logger.

use std::{env, process::exit, str::FromStr};

use ipc::address::Address;
use serde::{Deserialize, Serialize};

use crate::env::{IPC_INPUT, IPC_OUTPUT};

/// A manifest describes what a process is and what it requires at startup.
///
/// Think of the manifest as the identity card of a microservice, one should
/// be able to start the process correctly and pipe it with only being aware
/// of its manifest.
///
/// A service should be self descriptive, that is, a service should output
/// the manifest in stdout and yield if some expected environemnt variable
/// does not exists.
#[derive(Serialize, Deserialize, Clone)]
pub struct Manifest {
    pub blueprint: Blueprint,
    pub name: String,
    pub version: String,
    pub description: String,
    pub service_type: String,
}

impl Manifest {
    /// Returns the addresses binded to the input required by the manifest.
    ///
    /// The addresses are returned in the same order as the blueprint inputs.
    ///
    /// # Panics
    ///
    /// Panics if the environment variable was not provided before bootup of the program.
    pub fn get_input_address(&self) -> Vec<Address> {
        self.blueprint
            .inputs
            .iter()
            .map(|input| {
                let env_name = format!("{}_{}", IPC_INPUT, input.name);
                match env::var(env_name) {
                    Ok(addr_str) => Address::from_str(&addr_str).unwrap(),
                    Err(_) => {
                        panic!()
                    }
                }
            })
            .collect()
    }

    /// Returns the addresses binded to the output required by the manifest.
    ///
    /// The addresses are returned in the same order as the blueprint inputs.
    ///
    /// # Panics
    ///
    /// Panics if the environment variable was not provided before bootup of the program.
    pub fn get_output_address(&self) -> Vec<Address> {
        self.blueprint
            .outputs
            .iter()
            .map(|input| {
                let env_name = format!("{}_{}", IPC_OUTPUT, input.name);
                match env::var(env_name) {
                    Ok(addr_str) => Address::from_str(&addr_str).unwrap(),
                    Err(_) => {
                        panic!()
                    }
                }
            })
            .collect()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Blueprint {
    /// Inputs this service expects.
    ///
    /// It is guarented that all inputs will be provided at startup.
    pub inputs: Vec<PortDefinition>,

    /// Outputs this service produces.
    pub outputs: Vec<PortDefinition>,
}

impl Blueprint {
    pub fn get_input_address(&self) -> Vec<Address> {
        self.inputs
            .iter()
            .map(|input| {
                let env_name = format!("IPC_INPUT_{}", input.name);
                match env::var(env_name) {
                    Ok(addr_str) => Address::from_str(&addr_str).unwrap(),
                    Err(_) => exit(0),
                }
            })
            .collect()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PortDefinition {
    name: String,
    pub data_type: String,
}

impl PortDefinition {
    pub fn new(name: String, data_type: String) -> PortDefinition {
        assert!(name.chars().all(|c| c.is_ascii_uppercase() || c == '_'));
        PortDefinition { name, data_type }
    }

    pub const fn name<'a>(&'a self) -> &'a String {
        &self.name
    }
}
