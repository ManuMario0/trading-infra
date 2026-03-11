//! Discovers and parses module metadata from Cargo.toml files.

use std::fs;
use std::path::Path;

use toml::Value;

/// Metadata extracted from a module's Cargo.toml.
pub struct ModuleManifest {
    /// The package name (e.g., "kelly-multiplexer").
    pub name: String,
    /// The package version (e.g., "0.1.0").
    pub version: String,
    /// The package description, or empty if not provided.
    pub description: String,
    /// The module type from [package.metadata.trading] (e.g., "multiplexer").
    pub module_type: String,
}

/// Parses a module's Cargo.toml and extracts the package name and trading type.
///
/// Returns `None` if the manifest is missing the required `[package.metadata.trading]` section.
pub fn parse_manifest(target_dir: &Path) -> Option<ModuleManifest> {
    let manifest_path = target_dir.join("Cargo.toml");
    if !manifest_path.exists() {
        eprintln!("Error: No Cargo.toml found in {}", target_dir.display());
        return None;
    }

    let manifest_str = fs::read_to_string(&manifest_path).unwrap();
    let manifest: Value = toml::from_str(&manifest_str).unwrap_or_else(|e| {
        eprintln!("Error: Failed to parse Cargo.toml: {}", e);
        std::process::exit(1);
    });

    let package_name = manifest["package"]["name"].as_str().unwrap().to_string();
    let package_version = manifest["package"]["version"].as_str().unwrap().to_string();
    let package_description = manifest["package"]
        .get("description")
        .and_then(|d| d.as_str())
        .unwrap_or("")
        .to_string();

    let module_type = manifest
        .get("package")
        .and_then(|p| p.get("metadata"))
        .and_then(|m| m.get("trading"))
        .and_then(|t| t.get("type"))
        .and_then(|t| t.as_str())
        .map(|s| s.to_string());

    match module_type {
        Some(module_type) => Some(ModuleManifest {
            name: package_name,
            version: package_version,
            description: package_description,
            module_type,
        }),
        None => {
            eprintln!(
                "Skipping {}: Missing [package.metadata.trading] type",
                package_name
            );
            None
        }
    }
}
