use std::fs::File;
use std::io::stdout;
use std::path::PathBuf;

use clap::Parser;
use glob::glob_with;
use indicatif::{ParallelProgressIterator, ProgressIterator};
use polars::prelude::*;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::doctype::DocumentType;
use crate::prelude::*;
use crate::utils::relpath;

// const PBAR_METADATA: &str = "Collecting metadata: {human_pos} | \
//         elapsed: {elapsed_precise}{msg}";

const PBAR_COLLECT: &str = "Collecting documents: {human_pos} | \
        elapsed: {elapsed_precise}{msg}";

const PBAR_INDEX: &str = "Indexing documents: {human_pos} ({percent}%) | \
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

    /// If set, the index will be written in CSV format to the standard
    /// output (stdout).
    #[arg(long, conflicts_with = "output")]
    stdout: bool,

    /// Write the index into `filename`. By default (if `--stdout`
    /// isn't set), the index will be written to `index.ipc` into
    /// the root directory.
    #[arg(short, long, value_name = "filename")]
    output: Option<PathBuf>,

    /// The path to the PICA+ dump
    path: Option<PathBuf>,
}

#[derive(Debug, Default)]
struct Row {
    path: PathBuf,
    hash: String,
    ppn: String,
    doctype: DocumentType,
    lang_code: Option<String>,
    lang_score: Option<f64>,
    lfreq: Option<f64>,
    alpha: f64,
    alphanum: f64,
    words: u64,
    avg_word_len: f32,
    size: u64,
    strlen: u64,
    mtime: u64,
}

impl TryFrom<&PathBuf> for Row {
    type Error = DatashedError;

    fn try_from(path: &PathBuf) -> Result<Self, Self::Error> {
        let mut doc = Document::from_path(path)?;
        let (lang_code, lang_score) = match doc.lang() {
            Some((lang_code, lang_score)) => {
                (Some(lang_code), Some(lang_score))
            }
            _ => (None, None),
        };

        Ok(Row {
            path: path.into(),
            hash: doc.hash(),
            ppn: doc.ppn(),
            doctype: doc.doctype(),
            lfreq: doc.lfreq(),
            alpha: doc.alpha(),
            alphanum: doc.alphanum(),
            words: doc.word_count(),
            avg_word_len: doc.avg_word_len(),
            size: doc.size(),
            strlen: doc.strlen(),
            mtime: doc.modified(),
            lang_code,
            lang_score,
        })
    }
}

impl Index {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        let datashed = Datashed::discover()?;
        let data_dir = datashed.data_dir();
        let base_dir = datashed.base_dir();
        let config = datashed.config()?;

        let pattern = format!("{}/**/*.txt", data_dir.display());
        let pbar =
            ProgressBarBuilder::new(PBAR_COLLECT, self.quiet).build();

        let files: Vec<_> = glob_with(&pattern, Default::default())
            .map_err(|e| DatashedError::Other(e.to_string()))?
            .progress_with(pbar)
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
                DatashedError::other("unable to index documents!")
            })?;

        let mut remote: Vec<&str> = vec![];
        let mut path: Vec<String> = vec![];
        let mut hash: Vec<String> = vec![];
        let mut ppn: Vec<String> = vec![];
        let mut doctype: Vec<String> = vec![];
        let mut lang_code: Vec<Option<String>> = vec![];
        let mut lang_score: Vec<Option<f64>> = vec![];
        let mut lfreq: Vec<Option<f64>> = vec![];
        let mut alpha: Vec<f64> = vec![];
        let mut alphanum: Vec<f64> = vec![];
        let mut words: Vec<u64> = vec![];
        let mut avg_word_len: Vec<f32> = vec![];
        let mut size: Vec<u64> = vec![];
        let mut strlen: Vec<u64> = vec![];
        let mut mtime: Vec<u64> = vec![];

        for row in rows.into_iter() {
            let path_ = relpath(&row.path, base_dir);
            let hash_ = row.hash[0..8].to_string();

            remote.push(&config.metadata.name);
            hash.push(hash_);
            path.push(path_);
            doctype.push(row.doctype.to_string());
            lang_code.push(row.lang_code);
            lang_score.push(row.lang_score);
            lfreq.push(row.lfreq);
            alpha.push(row.alpha);
            alphanum.push(row.alphanum);
            words.push(row.words);
            avg_word_len.push(row.avg_word_len);
            size.push(row.size);
            strlen.push(row.strlen);
            mtime.push(row.mtime);
            ppn.push(row.ppn);
        }

        let df = DataFrame::new(vec![
            Column::new("remote".into(), remote),
            Column::new("path".into(), path),
            Column::new("hash".into(), hash),
            Column::new("ppn".into(), ppn),
            Column::new("doctype".into(), doctype),
            Column::new("lang_code".into(), lang_code),
            Column::new("lang_score".into(), lang_score),
            Column::new("lfreq".into(), lfreq),
            Column::new("alpha".into(), alpha),
            Column::new("alphanum".into(), alphanum),
            Column::new("words".into(), words),
            Column::new("avg_word_len".into(), avg_word_len),
            Column::new("size".into(), size),
            Column::new("strlen".into(), strlen),
            Column::new("mtime".into(), mtime),
        ])?;

        let mut df: DataFrame =
            df.lazy().select([col("*").shrink_dtype()]).collect()?;

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
                    base_dir.join(Datashed::INDEX),
                )?)
                .with_compression(Some(IpcCompression::ZSTD));
                writer.finish(&mut df)?;
            }
        }

        Ok(())
    }
}
