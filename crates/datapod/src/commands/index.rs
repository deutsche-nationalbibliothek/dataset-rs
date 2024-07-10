use std::fs::File;
use std::io::stdout;
use std::path::PathBuf;

use clap::Parser;
use glob::{glob_with, MatchOptions};
use indicatif::ParallelProgressIterator;
use polars::prelude::*;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::datapod::Datapod;
use crate::document::Document;
use crate::error::{DatapodError, DatapodResult};
use crate::progress::ProgressBarBuilder;
use crate::utils::relpath;

const PBAR_INDEX: &str =
    "Indexing documents: {human_pos} ({percent}%) | \
        elapsed: {elapsed_precise}{msg}";

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
    len: u64,
}

impl TryFrom<&PathBuf> for Row {
    type Error = DatapodError;

    fn try_from(path: &PathBuf) -> Result<Self, Self::Error> {
        let doc = Document::from_path(path)?;
        Ok(Row {
            idn: doc.idn(),
            path: path.into(),
            len: doc.len(),
        })
    }
}

pub(crate) fn execute(args: Index) -> DatapodResult<()> {
    let datapod = Datapod::discover()?;
    let config = datapod.config()?;
    let data_dir = datapod.data_dir();
    let base_dir = datapod.base_dir();

    let pattern = format!("{}/**/*.txt", data_dir.display());
    let options = MatchOptions::default();

    let files: Vec<_> = glob_with(&pattern, options)
        .map_err(|e| DatapodError::Other(e.to_string()))?
        .filter_map(Result::ok)
        .collect();

    let pbar = ProgressBarBuilder::new(PBAR_INDEX, args.quiet)
        .len(files.len() as u64)
        .build();

    let rows = files
        .par_iter()
        .progress_with(pbar)
        .map(Row::try_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| {
            DatapodError::Other("unable to index documents!".into())
        })?;

    let mut idn: Vec<String> = vec![];
    let mut remote: Vec<&str> = vec![];
    let mut path: Vec<String> = vec![];
    let mut len: Vec<u64> = vec![];

    for row in rows.into_iter() {
        idn.push(row.idn);
        remote.push(&config.metadata.name);
        path.push(relpath(&row.path, base_dir));
        len.push(row.len);
    }

    let mut df = DataFrame::new(vec![
        Series::new("idn", idn),
        Series::new("remote", remote),
        Series::new("path", path),
        Series::new("len", len),
    ])?;

    match args.output {
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
