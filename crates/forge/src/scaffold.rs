//! Scaffolds a temporary build workspace from a wrapper crate.
//!
//! Instead of copying template files, this module generates a thin `main.rs`
//! and `Cargo.toml` that wire the user module into the corresponding wrapper
//! library crate. All complex wrapper logic lives in the wrapper crate itself.

use std::fs;
use std::path::{Path, PathBuf};

use crate::manifest::ModuleManifest;

/// Prepares a build directory by generating a thin glue crate that connects
/// the user module to the corresponding wrapper library.
///
/// Returns the path to the build directory on success.
pub fn scaffold(
    module: &ModuleManifest,
    target_dir: &Path,
    workspace_root: &Path,
) -> Option<PathBuf> {
    let build_dir = workspace_root
        .join("target")
        .join("forge")
        .join(&module.name);

    // Only clean src/ and Cargo.toml to preserve cached dependencies in target/
    let build_src = build_dir.join("src");
    if build_src.exists() {
        fs::remove_dir_all(&build_src).unwrap();
    }
    let build_cargo = build_dir.join("Cargo.toml");
    if build_cargo.exists() {
        fs::remove_file(&build_cargo).unwrap();
    }
    fs::create_dir_all(&build_src).unwrap();

    // Verify a wrapper crate exists for this module type
    let wrapper_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("wrappers")
        .join(&module.module_type);

    if !wrapper_dir.exists() {
        eprintln!(
            "Error: No forge wrapper exists for type '{}'",
            module.module_type
        );
        return None;
    }

    let crate_name = module.name.replace("-", "_");
    let wrapper_crate = format!("{}-wrapper", module.module_type);
    let wrapper_crate_ident = wrapper_crate.replace("-", "_");

    // Generate the thin main.rs that wires user module → wrapper
    let main_rs = format!(
        "\
use {crate_name}::entry_point;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {{
    {wrapper_crate_ident}::run(
        entry_point(),
        {wrapper_crate_ident}::ModuleInfo {{
            name: \"{name}\",
            version: \"{version}\",
            description: \"{description}\",
        }},
    ).await
}}
",
        name = module.name,
        version = module.version,
        description = module.description.escape_default(),
    );
    fs::write(build_src.join("main.rs"), main_rs).unwrap();

    // Generate Cargo.toml with dependencies on the wrapper and user module
    let cargo_toml = format!(
        "\
[package]
name = \"{name}-forged\"
version = \"0.1.0\"
edition = \"2024\"

[workspace]

[dependencies]
tokio = {{ version = \"1.43.0\", features = [\"full\"] }}
{wrapper_crate} = {{ path = \"{wrapper}\" }}
{name} = {{ path = \"{target}\" }}
",
        name = module.name,
        wrapper_crate = wrapper_crate,
        wrapper = wrapper_dir.display(),
        target = target_dir.display(),
    );
    fs::write(build_dir.join("Cargo.toml"), cargo_toml).unwrap();

    Some(build_dir)
}
