use ipc::blueprint::Blueprint;
use service::manifest::Manifest;

pub const LOG_PIPE: &str = "LOG_PIPE";

pub fn make_manifest() -> Manifest {
    let name = env!("CARGO_PKG_NAME");
    let version = env!("CARGO_PKG_VERSION");
    let description = env!("CARGO_PKG_DESCRIPTION");
    let service_type = "logger";

    let blueprint = Blueprint { ports: vec![] };

    Manifest {
        blueprint,
        name: name.to_string(),
        version: version.to_string(),
        description: description.to_string(),
        service_type: service_type.to_string(),
    }
}
