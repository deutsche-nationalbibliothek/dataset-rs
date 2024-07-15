use std::fs::File;
use std::io::stdout;
use std::path::PathBuf;

use clap::Parser;
use glob::{glob_with, MatchOptions};
use indicatif::ParallelProgressIterator;
use polars::prelude::*;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::prelude::*;
use crate::utils::relpath;

const PBAR_INDEX: &str =
    "Indexing documents: {human_pos} ({percent}%) | \
        elapsed: {elapsed_precise}{msg}";

/// Create an index of all available documents.
#[derive(Debug, Default, Parser)]
pub(crate) struct Index {
    /// Run verbosely. Print additional progress information to the
    /// standard error stream. This option conflicts with the
    /// `--quiet` option.
    #[arg(short, long, conflicts_with = "quiet")]
    verbose: bool,

    /// Operate quietly; do not show progress. This option conflicts
    /// with the `--verbose` option.
    #[arg(short, long, conflicts_with = "verbose")]
    quiet: bool,

    /// Write the index into `filename`. By default, the index will
    /// be written to stdout in CSV format.
    #[arg(short, long, value_name = "filename")]
    output: Option<PathBuf>,
}

#[derive(Debug, Default)]
struct Row {
    idn: String,
    path: PathBuf,
    size: u64,
    mtime: u64,
    hash: String,
}

impl TryFrom<&PathBuf> for Row {
    type Error = DatashedError;

    fn try_from(path: &PathBuf) -> Result<Self, Self::Error> {
        let doc = Document::from_path(path)?;
        Ok(Row {
            idn: doc.idn(),
            path: path.into(),
            size: doc.size(),
            mtime: doc.modified(),
            hash: doc.hash(),
        })
    }
}

impl Index {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        let datashed = Datashed::discover()?;
        let config = datashed.config()?;
        let data_dir = datashed.data_dir();
        let base_dir = datashed.base_dir();

        let pattern = format!("{}/**/*.txt", data_dir.display());
        let options = MatchOptions::default();

        let files: Vec<_> = glob_with(&pattern, options)
            .map_err(|e| DatashedError::Other(e.to_string()))?
            .filter_map(Result::ok)
            .collect();

        let pbar = ProgressBarBuilder::new(PBAR_INDEX, self.quiet)
            .len(files.len() as u64)
            .build();

        let rows = files
            .par_iter()
            .progress_with(pbar)
            .map(Row::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| {
                DatashedError::Other(
                    "unable to index documents!".into(),
                )
            })?;

        let mut idn: Vec<String> = vec![];
        let mut remote: Vec<&str> = vec![];
        let mut path: Vec<String> = vec![];
        let mut size: Vec<u64> = vec![];
        let mut mtime: Vec<u64> = vec![];
        let mut hash: Vec<String> = vec![];

        for row in rows.into_iter() {
            idn.push(row.idn);
            remote.push(&config.metadata.name);
            path.push(relpath(&row.path, base_dir));
            size.push(row.size);
            mtime.push(row.mtime);
            hash.push(row.hash[0..8].to_string());
        }

        let mut df = DataFrame::new(vec![
            Series::new("idn", idn),
            Series::new("remote", remote),
            Series::new("path", path),
            Series::new("size", size),
            Series::new("mtime", mtime),
            Series::new("hash", hash),
        ])?;

        match self.output {
            None => {
                let mut writer = CsvWriter::new(stdout().lock());
                writer.finish(&mut df)?;
            }
            Some(path) => {
                let mut writer = IpcWriter::new(File::create(path)?)
                    .with_compression(Some(IpcCompression::ZSTD));
                writer.finish(&mut df)?;
            }
        }

        Ok(())
    }
}