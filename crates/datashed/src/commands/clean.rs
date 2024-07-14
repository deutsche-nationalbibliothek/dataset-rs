use std::collections::BTreeSet;
use std::fs::remove_file;

use clap::Parser;
use dialoguer::theme::ColorfulTheme;
use dialoguer::Confirm;
use glob::{glob_with, MatchOptions};
use polars::prelude::*;

use crate::datashed::Datashed;
use crate::error::{DatashedError, DatashedResult};
use crate::utils::relpath;

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
}

pub(crate) fn execute(_args: Clean) -> DatashedResult<()> {
    let datashed = Datashed::discover()?;
    let data_dir = datashed.data_dir();
    let base_dir = datashed.base_dir();

    let pattern = format!("{}/**/*.txt", data_dir.display());
    let options = MatchOptions::default();

    let mut missing: Vec<_> = vec![];
    let mut untracked: BTreeSet<_> = glob_with(&pattern, options)
        .map_err(|e| DatashedError::Other(e.to_string()))?
        .filter_map(Result::ok)
        .map(|path| relpath(path, base_dir))
        .collect();

    let index = datashed.index()?;
    let path = index.column("path")?.str()?;

    for idx in 0..index.height() {
        let index_path = path.get(idx).unwrap();

        if untracked.remove(index_path) {
            continue;
        } else {
            missing.push(index_path);
        }
    }

    if !untracked.is_empty() {
        let confirm = Confirm::with_theme(&ColorfulTheme::default())
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
        println!("{}", index);

        let missing = Series::from_iter(missing);

        println!(
            "{:?}",
            index
                .lazy()
                .filter(col("path").is_in(lit(missing)))
                .collect()
        );
    }

    Ok(())
}
