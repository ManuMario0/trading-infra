//! Scaffolds a temporary build workspace from a template directory.

use std::fs;
use std::path::{Path, PathBuf};

use crate::manifest::ModuleManifest;

/// Prepares a build directory from a template, injecting the target module as a dependency.
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

    // Clean and recreate the build directory
    if build_dir.exists() {
        fs::remove_dir_all(&build_dir).unwrap();
    }
    fs::create_dir_all(&build_dir).unwrap();
    fs::create_dir_all(build_dir.join("src")).unwrap();

    // Locate the template for this module type
    let template_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("templates")
        .join(&module.module_type);

    if !template_dir.exists() {
        eprintln!(
            "Error: No forge template exists for type '{}'",
            module.module_type
        );
        return None;
    }

    // Generate Cargo.toml with injected dependency
    let template_manifest = fs::read_to_string(template_dir.join("Cargo.toml")).unwrap();
    let dependency_injection = format!(
        "\n{} = {{ path = \"{}\" }}\n",
        module.name,
        target_dir.display()
    );
    let final_manifest = template_manifest
        .replace("multiplexer-wrapper", &format!("{}-wrapper", module.name))
        + &dependency_injection;
    fs::write(build_dir.join("Cargo.toml"), final_manifest).unwrap();

    // Generate src/main.rs with injected crate name
    let template_src = fs::read_to_string(template_dir.join("src").join("main.rs")).unwrap();
    let crate_name = module.name.replace("-", "_");
    let final_src = template_src.replace("__target_module__", &crate_name);
    fs::write(build_dir.join("src").join("main.rs"), final_src).unwrap();

    Some(build_dir)
}
