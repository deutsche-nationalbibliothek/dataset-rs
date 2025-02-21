use std::fs::File;
use std::io::{Cursor, stdout};
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use indicatif::{HumanCount, ProgressBar};
use polars::prelude::*;
use polars::sql::SQLContext;

use crate::prelude::*;

#[derive(Debug, Parser)]
pub(crate) struct Fetch {
    /// Run verbosely. Print additional progress information to the
    /// standard error stream. This option conflicts with the
    /// `--quiet` option.
    #[arg(short, long, conflicts_with = "quiet")]
    verbose: bool,

    /// Operate quietly; do not show progress. This option conflicts
    /// with the `--verbose` option.
    #[arg(short, long, conflicts_with = "verbose")]
    quiet: bool,

    /// If set, the index will be written in CSV format to the standard
    /// output (stdout).
    #[arg(long, conflicts_with = "output")]
    stdout: bool,

    /// Write the index into `filename`. By default (if `--stdout`
    /// isn't set), the index will be written to `index.ipc` into
    /// the root directory.
    #[arg(short, long, value_name = "filename")]
    output: Option<PathBuf>,
}

impl Fetch {
    pub(crate) async fn execute(self) -> DatasetResult<()> {
        let dataset = Dataset::discover()?;
        let dot_dir = dataset.dot_dir();
        let config = dataset.config()?;
        let remotes = config.remotes;
        let mut dfs = vec![];

        for (name, remote) in remotes.iter() {
            let pbar = if !self.quiet {
                ProgressBar::new_spinner()
            } else {
                ProgressBar::hidden()
            };

            pbar.enable_steady_tick(Duration::from_millis(100));
            pbar.set_message(format!("Fetching {name}..."));

            let mut index_url = remote.url.clone();
            index_url.set_path("/index.ipc");

            let body = reqwest::get(index_url).await?.bytes().await?;
            if body.is_empty() {
                bail!(
                    "unable to get datashed index (remote = {})",
                    name
                );
            }

            let mut index =
                IpcReader::new(Cursor::new(body)).finish()?;
            if let Some(ref predicate) = remote.predicate {
                let mut ctx = SQLContext::new();
                ctx.register("index", index.lazy());
                index = ctx
                    .execute(&format!(
                        "SELECT * FROM index WHERE {predicate}"
                    ))?
                    .collect()?
            }

            let cnt = index.height();
            if cnt > 0 {
                dfs.push(index.lazy());
            }

            pbar.finish_and_clear();

            if !self.quiet {
                eprintln!(
                    "Fetching {name}: {} documents, done.",
                    HumanCount(cnt as u64)
                );
            }
        }

        let pbar = if !self.quiet {
            ProgressBar::new_spinner()
        } else {
            ProgressBar::hidden()
        };

        pbar.enable_steady_tick(Duration::from_millis(100));
        pbar.set_message("Creating compound index");

        let args = UnionArgs {
            to_supertypes: true,
            ..Default::default()
        };

        let mut df = concat(dfs, args)?
            .select([col("*").shrink_dtype()])
            .collect()?;

        match self.output {
            Some(path) => {
                let mut writer = IpcWriter::new(File::create(path)?)
                    .with_compression(Some(IpcCompression::ZSTD));
                writer.finish(&mut df)?;
            }
            None if self.stdout => {
                let mut writer = CsvWriter::new(stdout().lock());
                writer.finish(&mut df)?;
            }
            None => {
                let mut writer = IpcWriter::new(File::create(
                    dot_dir.join(Dataset::REMOTES),
                )?)
                .with_compression(Some(IpcCompression::ZSTD));
                writer.finish(&mut df)?;
            }
        }

        pbar.finish_and_clear();
        Ok(())
    }
}
