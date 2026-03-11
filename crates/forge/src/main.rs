mod compile;
mod manifest;
mod scaffold;

use std::env;
use std::path::Path;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: cargo run -p forge -- <target_module_paths...>");
        std::process::exit(1);
    }

    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();

    for target_path_str in &args[1..] {
        let target_dir = Path::new(target_path_str)
            .canonicalize()
            .unwrap_or_else(|_| {
                eprintln!("Error: Could not find directory '{}'", target_path_str);
                std::process::exit(1);
            });

        println!("\n=== Forging Module: {} ===", target_dir.display());

        // 1. Discover module metadata
        let module = match manifest::parse_manifest(&target_dir) {
            Some(m) => m,
            None => continue,
        };

        println!("Detected Type: {}", module.module_type);
        println!("Detected Name: {}", module.name);

        // 2. Scaffold the wrapper project from the template
        let build_dir = match scaffold::scaffold(&module, &target_dir, workspace_root) {
            Some(dir) => dir,
            None => continue,
        };

        // 3. Compile the wrapper
        compile::compile(&build_dir, &module.name);
    }
}
