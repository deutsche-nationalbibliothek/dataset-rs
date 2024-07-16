use std::collections::BTreeSet;
use std::fs::{remove_file, File};

use clap::Parser;
use dialoguer::theme::ColorfulTheme;
use dialoguer::Confirm;
use glob::glob_with;
use indicatif::ProgressIterator;
use polars::prelude::*;

use crate::datashed::Datashed;
use crate::error::{DatashedError, DatashedResult};
use crate::progress::ProgressBarBuilder;
use crate::utils::relpath;

const PBAR_COLLECT: &str = "Collecting documents: {human_pos} | \
        elapsed: {elapsed_precise}{msg}";

#[derive(Debug, Default, Parser)]
pub(crate) struct Clean {
    /// Run verbosely. Print additional progress information to the
    /// standard error stream. This option conflicts with the
    /// `--quiet` option.
    #[arg(short, long, conflicts_with = "quiet")]
    verbose: bool,

    /// Operate quietly; do not show progress. This option conflicts
    /// with the `--verbose` option.
    #[arg(short, long, conflicts_with = "verbose")]
    quiet: bool,

    /// Whether to confirm delete operations or not.
    #[arg(short, long)]
    force: bool,
}

impl Clean {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        let datashed = Datashed::discover()?;
        let data_dir = datashed.data_dir();
        let base_dir = datashed.base_dir();

        let pattern = format!("{}/**/*.txt", data_dir.display());
        let pbar =
            ProgressBarBuilder::new(PBAR_COLLECT, self.quiet).build();

        let mut missing: Vec<_> = vec![];
        let mut untracked: BTreeSet<_> =
            glob_with(&pattern, Default::default())
                .map_err(|e| DatashedError::Other(e.to_string()))?
                .progress_with(pbar)
                .filter_map(Result::ok)
                .map(|path| relpath(path, base_dir))
                .collect();

        let index = datashed.index()?;
        let path = index.column("path")?.str()?;

        for idx in 0..index.height() {
            let index_path = path.get(idx).unwrap();

            if !untracked.remove(index_path) {
                missing.push(index_path);
            }
        }

        if !untracked.is_empty() {
            let confirm = self.force
                || Confirm::with_theme(&ColorfulTheme::default())
                    .with_prompt(format!(
                        "Delete {} untracked document(s))?",
                        untracked.len()
                    ))
                    .default(true)
                    .show_default(true)
                    .interact()
                    .unwrap();

            if confirm {
                untracked.into_iter().try_for_each(|relpath| {
                    remove_file(base_dir.join(relpath))?;
                    Ok::<_, DatashedError>(())
                })?;
            }
        }

        if !missing.is_empty() {
            let confirm = self.force
                || Confirm::with_theme(&ColorfulTheme::default())
                    .with_prompt(format!(
                        "Delete {} missing index entries)?",
                        missing.len()
                    ))
                    .default(true)
                    .show_default(true)
                    .interact()
                    .unwrap();

            if confirm {
                let missing = Series::from_iter(missing);
                let mut df = index
                    .lazy()
                    .filter(col("path").is_in(lit(missing)).not())
                    .collect()?;

                let path = base_dir.join(Datashed::INDEX);
                let mut writer = IpcWriter::new(File::create(path)?)
                    .with_compression(Some(IpcCompression::ZSTD));
                writer.finish(&mut df)?;
            }
        }

        Ok(())
    }
}
