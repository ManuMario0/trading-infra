//! Compiles the scaffolded wrapper project.

use std::path::Path;
use std::process::Command;

/// Runs `cargo build` inside the given build directory.
///
/// Returns `true` if compilation succeeded.
pub fn compile(build_dir: &Path, package_name: &str) -> bool {
    println!("Compiling wrapper for {}...", package_name);

    let status = Command::new("cargo")
        .arg("build")
        .current_dir(build_dir)
        .status()
        .unwrap();

    if status.success() {
        println!(
            "Success! Executable is at {}/target/debug/{}-forged",
            build_dir.display(),
            package_name
        );
        true
    } else {
        eprintln!("Compilation failed for {}", package_name);
        false
    }
}
