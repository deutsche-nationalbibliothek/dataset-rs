use clap::{Parser, Subcommand};

use crate::commands::config::Config;
use crate::commands::init::Init;
use crate::commands::remote::Remote;
use crate::commands::update::Update;
use crate::commands::verify::Verify;
use crate::commands::version::Version;

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
    Init(Init),
    Config(Config),
    Remote(Remote),
    Update(Update),
    Verify(Verify),
    Version(Version),
}
