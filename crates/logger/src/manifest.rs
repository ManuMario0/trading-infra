use std::any::type_name;

use journal::entry::JournalEntry;
use service::manifest::{Blueprint, Manifest, PortDefinition};

pub const LOG_PIPE: &str = "LOG_PIPE";

pub fn make_manifest() -> Manifest {
    let name = env!("CARGO_PKG_NAME");
    let version = env!("CARGO_PKG_VERSION");
    let description = env!("CARGO_PKG_DESCRIPTION");
    let service_type = "logger";

    let logger_input = PortDefinition::new(
        LOG_PIPE.to_string(),
        type_name::<JournalEntry>().to_owned(),
        ipc::socket::SocketKind::Aggregate,
        ipc::socket::SocketRole::Bind,
    );

    let blueprint = Blueprint {
        inputs: vec![logger_input],
        outputs: vec![],
    };

    Manifest {
        blueprint,
        name: name.to_string(),
        version: version.to_string(),
        description: description.to_string(),
        service_type: service_type.to_string(),
    }
}
