use clap::{Parser, Subcommand};

use crate::commands::init::Init;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
pub(crate) struct Args {
    #[command(subcommand)]
    pub(crate) cmd: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    Init(Init),
}
