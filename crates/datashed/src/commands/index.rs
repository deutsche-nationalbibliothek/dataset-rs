use std::fs::File;
use std::io::stdout;
use std::path::PathBuf;

use clap::Parser;
use glob::glob_with;
use indicatif::{ParallelProgressIterator, ProgressIterator};
use pica_record::io::{ReaderBuilder, RecordsIterator};
use polars::prelude::*;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::document::DocumentKind;
use crate::kindmap::KindMap;
use crate::mscmap::MscMap;
use crate::prelude::*;
use crate::utils::relpath;

const PBAR_METADATA: &str = "Collecting metadata: {human_pos} | \
        elapsed: {elapsed_precise}{msg}";

const PBAR_COLLECT: &str = "Collecting documents: {human_pos} | \
        elapsed: {elapsed_precise}{msg}";

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
    idn: String,
    kind: DocumentKind,
    msc: Option<String>,
    path: PathBuf,
    lang: Option<(String, f64)>,
    lfreq: Option<f64>,
    alpha: f64,
    ttr: f64,
    size: u64,
    strlen: u64,
    mtime: u64,
    hash: String,
}

impl TryFrom<&PathBuf> for Row {
    type Error = DatashedError;

    fn try_from(path: &PathBuf) -> Result<Self, Self::Error> {
        let mut doc = Document::from_path(path)?;
        let lang = doc.lang();

        Ok(Row {
            idn: doc.idn(),
            kind: doc.kind(),
            path: path.into(),
            lfreq: doc.lfreq(),
            alpha: doc.alpha(),
            ttr: doc.type_token_ratio(),
            size: doc.size(),
            strlen: doc.strlen(),
            mtime: doc.modified(),
            hash: doc.hash(),
            lang,
            ..Default::default()
        })
    }
}

impl Index {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        let datashed = Datashed::discover()?;
        let data_dir = datashed.data_dir();
        let base_dir = datashed.base_dir();
        let config = datashed.config()?;

        let mut kind_map = KindMap::from_config(&config)?;
        let mut msc_map = MscMap::from_config(&config)?;

        if let Some(path) = self.path {
            let pbar =
                ProgressBarBuilder::new(PBAR_METADATA, self.quiet)
                    .build();

            let mut reader = ReaderBuilder::new().from_path(path)?;
            while let Some(result) = reader.next() {
                if let Ok(record) = result {
                    kind_map.process_record(&record);
                    msc_map.process_record(&record);
                }

                pbar.inc(1);
            }

            pbar.finish_using_style();
        }

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

        let mut idn: Vec<String> = vec![];
        let mut kind: Vec<String> = vec![];
        let mut msc: Vec<Option<String>> = vec![];
        let mut remote: Vec<&str> = vec![];
        let mut path: Vec<String> = vec![];
        let mut lang_code: Vec<Option<String>> = vec![];
        let mut lang_score: Vec<Option<f64>> = vec![];
        let mut lfreq: Vec<Option<f64>> = vec![];
        let mut alpha: Vec<f64> = vec![];
        let mut ttr: Vec<f64> = vec![];
        let mut size: Vec<u64> = vec![];
        let mut strlen: Vec<u64> = vec![];
        let mut mtime: Vec<u64> = vec![];
        let mut hash: Vec<String> = vec![];

        for row in rows.into_iter() {
            let new_kind = kind_map
                .get(&(row.idn.clone(), row.kind.clone()))
                .unwrap_or(&row.kind)
                .to_owned();

            kind.push(new_kind.to_string());
            msc.push(msc_map.get(&row.idn).cloned());
            remote.push(&config.metadata.name);
            path.push(relpath(&row.path, base_dir));
            lfreq.push(row.lfreq);
            alpha.push(row.alpha);
            ttr.push(row.ttr);
            size.push(row.size);
            strlen.push(row.strlen);
            mtime.push(row.mtime);
            hash.push(row.hash[0..8].to_string());
            idn.push(row.idn);

            if let Some((code, score)) = row.lang {
                lang_code.push(Some(code));
                lang_score.push(Some(score));
            } else {
                lang_code.push(None);
                lang_score.push(None);
            }
        }

        let mut df = DataFrame::new(vec![
            Series::new("remote", remote),
            Series::new("idn", idn),
            Series::new("kind", kind),
            Series::new("msc", msc),
            Series::new("path", path),
            DataFrame::new(vec![
                Series::new("code", lang_code),
                Series::new("score", lang_score),
            ])?
            .into_struct("lang")
            .into_series(),
            Series::new("lfreq", lfreq),
            Series::new("alpha", alpha),
            Series::new("ttr", ttr),
            Series::new("size", size),
            Series::new("strlen", strlen),
            Series::new("mtime", mtime),
            Series::new("hash", hash),
        ])?;

        match self.output {
            Some(path) => {
                let mut writer = IpcWriter::new(File::create(path)?)
                    .with_compression(Some(IpcCompression::ZSTD));
                writer.finish(&mut df)?;
            }
            None if self.stdout => {
                // The lang column must be unnested, because CSV
                // doesn't support nested columns.
                let mut df = df.unnest(["lang"])?;

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
