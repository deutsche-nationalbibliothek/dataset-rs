use std::path::Path;

use clap::{Parser, ValueEnum};
use indicatif::ParallelProgressIterator;
use rayon::prelude::*;

use crate::datashed::Datashed;
use crate::document::Document;
use crate::error::{bail, DatashedError, DatashedResult};
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
}

impl Verify {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        let datashed = Datashed::discover()?;
        let index = datashed.index()?;

        let path = index.column("path")?.str()?;
        let hash = index.column("hash")?.str()?;
        let mtime = index.column("mtime")?.u64()?;
        let size = index.column("size")?.u64()?;

        let pbar = ProgressBarBuilder::new(PBAR_VERIFY, self.quiet)
            .len(index.height() as u64)
            .build();

        (0..index.height())
        .into_par_iter()
        .progress_with(pbar)
        .try_for_each(|idx| -> Result<(), DatashedError> {
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

            if self.mode >= VerifyMode::Strict
                && doc.modified() != mtime.get(idx).unwrap()
            {
                bail!(
                    "verification failed: mtime mismatch \
                        (path = {path:?})"
                );
            }

            if self.mode >= VerifyMode::Pedantic
                && doc.size() != size.get(idx).unwrap()
            {
                bail!( "verification failed: size mismatch (path = {path:?})");
            }

            Ok(())
        })
    }
}
