use orchestrator::app::App;
use orchestrator::cli::run_cli;

fn main() {
    println!("Bootstrapping Orchestrator...");
    let mut app = App::bootstrap();

    println!("Starting Control Interface...");
    run_cli(&mut app);
}
