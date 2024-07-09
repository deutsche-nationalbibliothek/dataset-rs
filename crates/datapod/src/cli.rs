use clap::{Parser, Subcommand};

use crate::commands::config::Config;
use crate::commands::init::Init;
use crate::commands::version::Version;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
pub(crate) struct Args {
    #[command(subcommand)]
    pub(crate) cmd: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    Init(Init),
    Config(Config),
    Version(Version),
}
