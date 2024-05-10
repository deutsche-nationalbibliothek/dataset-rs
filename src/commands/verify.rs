use clap::{Parser, ValueEnum};

use super::remote;
use crate::error::DatasetError;

#[derive(Clone, Debug, PartialEq, PartialOrd, Default, ValueEnum)]
pub(crate) enum VerifyMode {
    Permissive,
    #[default]
    Strict,
    Pedantic,
}

/// Verify that the indexed documents are consistent with the tracked
/// data sources (remotes).
#[derive(Debug, Parser)]
pub(crate) struct Verify {
    /// Run verbosely. Print additional progress information to the
    /// standard error stream. This option conflicts with the
    /// `--quiet` option.
    #[arg(short, long, conflicts_with = "quiet")]
    verbose: bool,

    /// Operate quietly; do not show progress. This option conflicts
    /// with the `--verbose` option.
    #[arg(short, long, conflicts_with = "verbose")]
    quiet: bool,

    /// Set the verify mode: permissive, strict (default), or
    /// pedantic.
    #[arg(
        short,
        long,
        default_value = "strict",
        value_name = "mode",
        hide_possible_values = true,
        hide_default_value = true
    )]
    mode: VerifyMode,
}

pub(crate) fn execute(args: Verify) -> Result<(), DatasetError> {
    remote::VerifyCommand::new(args.quiet, args.verbose, args.mode)
        .execute()
}
