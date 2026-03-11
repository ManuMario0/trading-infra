//! Manifest construction for multiplexer-type modules.

use std::any::type_name;

use service::{
    envelope::Envelope,
    manifest::{Blueprint, Manifest, PortDefinition},
};
use trading_sdk::{Allocation, model::ledger::LedgerUpdate};

use crate::{ModuleInfo, port};

pub fn make_manifest(module_info: ModuleInfo) -> Manifest {
    let name = module_info.name.to_string();
    let version = module_info.version.to_string();
    let description = module_info.description.to_string();
    let service_type = "multiplexer".to_string();

    // Now we build the blueprint
    let allocation_input = PortDefinition::new(
        port::ALLOCATION_INPUT_NAME.to_string(),
        type_name::<Envelope<Allocation>>().to_owned(),
    );
    let ledger_update_input = PortDefinition::new(
        port::LEDGER_UPDATE_INPUT_NAME.to_string(),
        type_name::<Envelope<LedgerUpdate>>().to_owned(),
    );

    let allocation_output = PortDefinition::new(
        port::ALLOCTION_OUTPUT_NAME.to_string(),
        type_name::<Envelope<Allocation>>().to_owned(),
    );

    let blueprint = Blueprint {
        inputs: vec![allocation_input, ledger_update_input],
        outputs: vec![allocation_output],
    };

    Manifest {
        blueprint,
        name,
        version,
        description,
        service_type,
    }
}
