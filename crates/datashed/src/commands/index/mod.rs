use std::fs::File;
use std::io::stdout;
use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;
use glob::glob_with;
use hashbrown::HashMap;
use indicatif::{ParallelProgressIterator, ProgressIterator};
use kind::KindMap;
use msc::MscMap;
use pica_record::prelude::*;
use polars::prelude::*;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::document::DocumentKind;
use crate::prelude::*;
use crate::utils::relpath;

const PBAR_METADATA: &str = "Collecting metadata: {human_pos} | \
        elapsed: {elapsed_precise}{msg}";

const PBAR_COLLECT: &str = "Collecting documents: {human_pos} | \
        elapsed: {elapsed_precise}{msg}";

const PBAR_INDEX: &str = "Indexing documents: {human_pos} ({percent}%) | \
        elapsed: {elapsed_precise}{msg}";

mod kind;
mod msc;

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

    /// A list of kind refinements that take precedence over any filter
    /// citeria. To be applied a document must match the `path` and the
    /// `hash` value. The list must be given in CSV format.
    #[arg(long, short = 'R', value_name = "filename")]
    refinements: Vec<PathBuf>,

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
    doc_id: String,
    kind: DocumentKind,
    #[allow(dead_code)]
    msc: Option<String>,
    lang_code: Option<String>,
    lang_score: Option<f64>,
    lfreq: Option<f64>,
    alpha: f64,
    words: u64,
    avg_word_len: f32,
    ttr: f64,
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
            doc_id: doc.file_stem(),
            kind: doc.kind(),
            lfreq: doc.lfreq(),
            alpha: doc.alpha(),
            words: doc.word_count(),
            avg_word_len: doc.avg_word_len(),
            ttr: doc.type_token_ratio(),
            size: doc.size(),
            strlen: doc.strlen(),
            mtime: doc.modified(),
            lang_code,
            lang_score,
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
        let mut refinements = HashMap::new();

        for path in self.refinements.iter() {
            let df = CsvReadOptions::default()
                .with_has_header(true)
                .with_infer_schema_length(Some(0))
                .try_into_reader_with_file_path(Some(path.into()))?
                .finish()?;

            let path_ = df.column("path")?.str()?;
            let hash_ = df.column("hash")?.str()?;
            let kind_ = df.column("kind")?.str()?;

            for i in 0..df.height() {
                let v = DocumentKind::from_str(kind_.get(i).unwrap())
                    .unwrap();
                let k = (
                    path_.get(i).unwrap().to_string(),
                    hash_.get(i).unwrap().to_string(),
                );

                refinements.insert(k, v);
            }
        }

        if let Some(path) = self.path {
            let pbar =
                ProgressBarBuilder::new(PBAR_METADATA, self.quiet)
                    .build();

            let mut reader = ReaderBuilder::new().from_path(path)?;
            while let Some(result) = reader.next_byte_record() {
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

        let mut remote: Vec<&str> = vec![];
        let mut path: Vec<String> = vec![];
        let mut hash: Vec<String> = vec![];
        let mut doc_id: Vec<String> = vec![];
        let mut kind: Vec<String> = vec![];
        let mut msc: Vec<Option<String>> = vec![];
        let mut lang_code: Vec<Option<String>> = vec![];
        let mut lang_score: Vec<Option<f64>> = vec![];
        let mut lfreq: Vec<Option<f64>> = vec![];
        let mut alpha: Vec<f64> = vec![];
        let mut words: Vec<u64> = vec![];
        let mut avg_word_len: Vec<f32> = vec![];
        let mut ttr: Vec<f64> = vec![];
        let mut size: Vec<u64> = vec![];
        let mut strlen: Vec<u64> = vec![];
        let mut mtime: Vec<u64> = vec![];

        for row in rows.into_iter() {
            let path_ = relpath(&row.path, base_dir);
            let hash_ = row.hash[0..8].to_string();
            let kind_ = refinements
                .remove(&(path_.clone(), hash_.clone()))
                .or(kind_map
                    .remove(&(row.doc_id.clone(), row.kind.clone())))
                .unwrap_or(row.kind);

            remote.push(&config.metadata.name);
            hash.push(hash_);
            path.push(path_);
            kind.push(kind_.to_string());
            msc.push(msc_map.get(&row.doc_id).cloned());
            lang_code.push(row.lang_code);
            lang_score.push(row.lang_score);
            lfreq.push(row.lfreq);
            alpha.push(row.alpha);
            words.push(row.words);
            avg_word_len.push(row.avg_word_len);
            ttr.push(row.ttr);
            size.push(row.size);
            strlen.push(row.strlen);
            mtime.push(row.mtime);
            doc_id.push(row.doc_id);
        }

        let df = DataFrame::new(vec![
            Column::new("remote".into(), remote),
            Column::new("path".into(), path),
            Column::new("hash".into(), hash),
            Column::new("doc_id".into(), doc_id),
            Column::new("kind".into(), kind),
            Column::new("msc".into(), msc),
            Column::new("lang_code".into(), lang_code),
            Column::new("lang_score".into(), lang_score),
            Column::new("lfreq".into(), lfreq),
            Column::new("alpha".into(), alpha),
            Column::new("words".into(), words),
            Column::new("avg_word_len".into(), avg_word_len),
            Column::new("ttr".into(), ttr),
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
