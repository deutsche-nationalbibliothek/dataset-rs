use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(version, about, long_about = None, max_term_width = 72)]
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

use crate::commands::*;

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    Config(Config),
    #[clap(alias = "new")]
    Init(Init),
}
