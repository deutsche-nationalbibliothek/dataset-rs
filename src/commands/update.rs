use std::path::PathBuf;

use clap::Parser;

use super::remote;
use crate::error::DatasetError;

#[derive(Debug, Parser)]
pub(crate) struct Update {
    /// Run verbosely. Print additional progress information to the
    /// standard error stream. This option conflicts with the
    /// `--quiet` option.
    #[arg(short, long, conflicts_with = "quiet")]
    pub(crate) verbose: bool,

    /// Operate quietly; do not show progress. This option conflicts
    /// with the `--verbose` option.
    #[arg(short, long, conflicts_with = "verbose")]
    pub(crate) quiet: bool,

    /// The path to the PICA+ dump
    pub(crate) path: PathBuf,
}

pub(crate) fn execute(args: Update) -> Result<(), DatasetError> {
    remote::SyncCommand::new(&args).execute()?;
    Ok(())
}
