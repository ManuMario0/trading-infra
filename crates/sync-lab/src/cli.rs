use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "sync-lab",
    about = "Low-level latency benchmarks for sync primitives"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    Spsc(SpscArgs),
    Mpsc(MpscArgs),
}

#[derive(Parser)]
pub struct SpscArgs {
    #[command(subcommand)]
    pub command: SpscCommand,
}

#[derive(Subcommand)]
pub enum SpscCommand {
    /// SPSC ping-pong round-trip latency measurement
    PingPong,
    /// SPSC unidirectional throughput blast (not yet implemented)
    Blast,
}

#[derive(Parser)]
pub struct MpscArgs {
    #[command(subcommand)]
    pub command: MpscCommand,
}

#[derive(Subcommand)]
pub enum MpscCommand {
    /// MPSC unidirectional throughput blast
    Blast,
    /// Crossbeam bounded MPSC blast for comparison
    CrossbeamBlast,
}
