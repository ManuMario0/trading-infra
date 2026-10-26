//! Performance harness of the sync library. Once replacement wiht cachec library is done, this should go.

mod cli;
mod mpsc;
mod spsc;
mod stats;

use clap::Parser;

use crate::cli::{Cli, Command, MpscCommand, SpscCommand};

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Spsc(args) => match args.command {
            SpscCommand::PingPong => spsc::ping_pong::run(),
            SpscCommand::Blast => spsc::blast::run(),
        },
        Command::Mpsc(args) => match args.command {
            MpscCommand::Blast => mpsc::blast::run(),
            MpscCommand::CrossbeamBlast => mpsc::crossbeam_blast::run(),
        },
    }
}
