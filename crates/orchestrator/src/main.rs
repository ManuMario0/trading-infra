use orchestrator::app::App;
use orchestrator::cli::run_cli;

#[tokio::main]
async fn main() {
    println!("Bootstrapping Orchestrator...");
    let mut app = App::bootstrap().await;

    println!("Starting Control Interface...");
    run_cli(&mut app).await;
}
