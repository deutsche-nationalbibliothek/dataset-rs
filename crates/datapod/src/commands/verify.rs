use std::fs::File;
use std::path::{Path, PathBuf};

use clap::{Parser, ValueEnum};
use indicatif::ParallelProgressIterator;
use polars::prelude::*;
use rayon::prelude::*;

use crate::datapod::Datapod;
use crate::document::Document;
use crate::error::{bail, DatapodError, DatapodResult};
use crate::progress::ProgressBarBuilder;

const PBAR_VERIFY: &str =
    "Verifying documents: {human_pos} ({percent}%) | \
        elapsed: {elapsed_precise}{msg}";

#[derive(Clone, Debug, PartialEq, PartialOrd, Default, ValueEnum)]
pub(crate) enum VerifyMode {
    Permissive,
    #[default]
    Strict,
    Pedantic,
}

/// Verify that the inventory of documents matches the index.
#[derive(Debug, Default, Parser)]
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

    /// Read the index from `filename`. By default, the index will
    /// be read from the internal data directory.
    #[arg(value_name = "filename")]
    path: Option<PathBuf>,
}

pub(crate) fn execute(args: Verify) -> DatapodResult<()> {
    let datapod = Datapod::discover()?;

    let path = match args.path {
        None => datapod.data_dir().join(Datapod::INDEX),
        Some(path) => path,
    };

    let df = IpcReader::new(File::open(path)?)
        .memory_mapped(None)
        .finish()?;

    let path = df.column("path")?.str()?;
    let hash = df.column("hash")?.str()?;
    let mtime = df.column("mtime")?.u64()?;
    let size = df.column("len")?.u64()?;

    let pbar = ProgressBarBuilder::new(PBAR_VERIFY, args.quiet)
        .len(df.height() as u64)
        .build();

    (0..df.height())
        .into_par_iter()
        .progress_with(pbar)
        .try_for_each(|idx| -> Result<(), DatapodError> {
            let path = path.get(idx).unwrap();
            if !Path::new(path).is_file() {
                bail!(
                    "verification failed: document not found \
                    (path = {path:?})"
                );
            }

            let doc = Document::from_path(path)?;
            let actual = doc.hash();
            let expected = hash.get(idx).unwrap();

            if !actual.starts_with(expected) {
                bail!(
                    "verification failed: hash mismatch \
                        (expected '{actual}' to starts with \
                        '{expected}', path = {path:?})"
                );
            }

            if args.mode >= VerifyMode::Strict
                && doc.modified() != mtime.get(idx).unwrap()
            {
                bail!(
                    "verification failed: mtime mismatch \
                        (path = {path:?})"
                );
            }

            if args.mode >= VerifyMode::Pedantic
                && doc.len() != size.get(idx).unwrap()
            {
                bail!( "verification failed: size mismatch (path = {path:?})");
            }

            Ok(())
        })
}
