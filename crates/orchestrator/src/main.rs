//! A local orchestrator (node local) that is capable of launching and stoping services on a machine.
//!
//! In the future, it should not be lanched manually but by an hypervisor that can access the machine.
//! For now it has to be provided with the following environment variables:
//! - NODE_ID: a unique identifier that represents the machine that the orchestrator is running on ;
//! - WORKING_DIR: the directory in which the orchestrator will be working.
//!   It will also use this directory to assign locations for the services it launches to write to.
//! - PORT_RANGE_MIN: the smallest port number that can automatically be assigned for a service port.
//! - PORT_RANGE_MAX: the highest port number that can automatically be assigned for a service port.

use orchestrator::app::App;
use orchestrator::cli::run_cli;

fn main() {
    println!("Bootstrapping Orchestrator...");
    let mut app = App::bootstrap();

    println!("Starting Control Interface...");
    run_cli(&mut app);
}
