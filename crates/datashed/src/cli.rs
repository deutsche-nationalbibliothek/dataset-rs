use clap::{Parser, Subcommand};

use crate::commands::*;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
pub(crate) struct Args {
    /// Number of threads to use. If this options isn't set or a value
    /// of "0" is chosen, the maximum number of available threads
    /// is used.
    #[clap(
        short = 'j',
        long,
        env = "DATASET_NUM_JOBS",
        hide_env_values = true
    )]
    pub(crate) num_jobs: Option<usize>,

    #[command(subcommand)]
    pub(crate) cmd: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    Archive(Archive),
    Config(Config),
    Index(Index),
    #[clap(alias = "new")]
    Init(Init),
    Restore(Restore),
    Status(Status),
    Verify(Verify),
    Version(Version),
}
