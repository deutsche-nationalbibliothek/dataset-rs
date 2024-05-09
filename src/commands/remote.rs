use std::fs::File;
use std::path::PathBuf;

use clap::Parser;
use indicatif::ParallelProgressIterator;
use polars::prelude::*;
use rayon::prelude::*;
use url::Url;

use crate::dataset::Dataset;
use crate::document::{Document, DocumentKind};
use crate::error::DatasetError;
use crate::progress::ProgressBarBuilder;

const CATEGORICAL: DataType =
    DataType::Categorical(None, CategoricalOrdering::Lexical);

/// Manage set of tracked data sources (remotes).
#[derive(Debug, Parser)]
pub(crate) struct Remote {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, Parser)]
pub(crate) enum Command {
    /// Add a new remote to the dataset.
    Add {
        /// The suffix of the documents.
        #[arg(short, long, default_value = ".txt")]
        suffix: String,

        /// The name of the remote.
        name: String,

        /// The URL of the remote.
        url: Url,
    },

    /// Remove the remote named <name>.
    #[clap(visible_alias = "rm")]
    Remove {
        /// The name of the remote.
        name: String,
    },

    /// Changes the URL for the remote <name>.
    SetUrl {
        /// The name of the remote.
        name: String,

        /// The URL of the remote.
        url: Url,
    },

    /// Change the file suffix for the remote <name>.
    SetSuffix {
        /// The name of the remote.
        name: String,

        /// The suffix of the documents.
        suffix: String,
    },

    Sync(SyncCommand),
}

pub(crate) fn execute(args: Remote) -> Result<(), DatasetError> {
    use crate::remote::Remote;

    let dataset = Dataset::discover()?;
    let mut config = dataset.config()?;

    match args.cmd {
        Command::Sync(cmd) => return cmd.execute(),
        Command::Add { name, suffix, url } => {
            if config.remotes.contains_key(&name) {
                return Err(DatasetError::Other(format!(
                    "remote with name '{name}' already exists"
                )));
            }

            let remote = Remote::new(url, suffix)?;
            config.remotes.insert(name, remote);
        }

        Command::Remove { name } => {
            if !config.remotes.contains_key(&name) {
                return Err(DatasetError::Other(format!(
                    "remote with name '{name}' does not exists.",
                )));
            }

            config.remotes.remove(&name);
        }

        Command::SetUrl { name, url } => {
            if let Some(remote) = config.remotes.get_mut(&name) {
                match remote {
                    Remote::Local { suffix, .. } => {
                        *remote = Remote::new(url, suffix.to_string())?;
                    }
                }
            } else {
                return Err(DatasetError::Other(format!(
                    "remote with name '{name}' does not exists.",
                )));
            }
        }

        Command::SetSuffix { name, suffix } => {
            if let Some(remote) = config.remotes.get_mut(&name) {
                match remote {
                    Remote::Local { path, .. } => {
                        *remote = Remote::Local {
                            path: path.to_path_buf(),
                            suffix,
                        }
                    }
                }
            } else {
                return Err(DatasetError::Other(format!(
                    "remote with name '{name}' does not exists.",
                )));
            }
        }
    }

    config.save()?;
    Ok(())
}

/// Update the document index based on the remote config.
#[derive(Debug, Default, Parser)]
pub(crate) struct SyncCommand {
    /// Run verbosely. Print additional progress information to the
    /// standard error stream. This option conflicts with the
    /// `--quiet` option.
    #[arg(short, long, conflicts_with = "quiet")]
    verbose: bool,

    /// Operate quietly; do not show progress. This option conflicts
    /// with the `--verbose` option.
    #[arg(short, long, conflicts_with = "verbose")]
    quiet: bool,

    /// Write the index into <filename>. By default, the index will
    /// be written to the internal data directory.
    #[arg(short, long, value_name = "filename")]
    output: Option<PathBuf>,
}

#[derive(Debug)]
struct Row {
    remote: String,
    idn: String,
    kind: DocumentKind,
    path: String,
    lang_code: &'static str,
    lang_score: f64,
    size: u64,
    strlen: u64,
    modified: u64,
    hash: String,
}

const PBAR_COLLECT: &str = "Collecting documents: {human_pos} | \
        elapsed: {elapsed_precise}{msg}";

const PBAR_INDEX: &str = "Indexing documents: {human_pos} | \
        elapsed: {elapsed_precise}{msg}";

impl SyncCommand {
    pub(crate) fn new(quiet: bool, verbose: bool) -> Self {
        Self {
            quiet,
            verbose,
            ..Default::default()
        }
    }

    pub(crate) fn execute(self) -> Result<(), DatasetError> {
        let dataset = Dataset::discover()?;
        let config = dataset.config()?;
        let mut documents: Vec<(&str, Document)> = vec![];
        let mut records: Vec<Row> = vec![];

        // To achieve a maximum throughput through parallelization it's
        // necessary to collect the documents first. Access to documents
        // through remotes is a prerequisite. If a document isn't
        // accessible the update stops with an error message.
        let pbar =
            ProgressBarBuilder::new(PBAR_COLLECT, self.quiet).build();

        for (name, remote) in config.remotes.iter() {
            for document in remote.documents() {
                documents.push((name, document?));
                pbar.inc(1);
            }
        }

        pbar.finish_using_style();

        // Process documents in parallel and transform each document
        // into a row, containing all (meta-)data of the index.
        let pbar = ProgressBarBuilder::new(PBAR_INDEX, self.quiet)
            .len(documents.len() as u64)
            .build();

        records.par_extend(
            documents.into_par_iter().progress_with(pbar).map(
                |(name, mut document)| {
                    let remote = config.remotes.get(name).unwrap();
                    let (lang_code, lang_score) =
                        document.lang().unwrap();

                    Row {
                        remote: name.into(),
                        idn: document.idn(),
                        kind: document.kind(),
                        path: document.relpath(remote),
                        size: document.size(),
                        strlen: document.strlen().unwrap() as u64,
                        modified: document.modified(),
                        hash: document.hash(8).unwrap(),
                        lang_code,
                        lang_score,
                    }
                },
            ),
        );

        let mut doc_id = vec![];
        let mut idn = vec![];
        let mut remote = vec![];
        let mut kind = vec![];
        let mut path = vec![];
        let mut lang_code = vec![];
        let mut lang_score = vec![];
        let mut size = vec![];
        let mut strlen = vec![];
        let mut mtime = vec![];
        let mut hash = vec![];

        for (id, record) in records.into_iter().enumerate() {
            doc_id.push(id as u32 + 1);
            remote.push(record.remote);
            idn.push(record.idn);
            kind.push(record.kind.to_string());
            path.push(record.path);
            lang_code.push(record.lang_code);
            lang_score.push(record.lang_score);
            size.push(record.size);
            strlen.push(record.strlen);
            mtime.push(record.modified);
            hash.push(record.hash);
        }

        let mut df = DataFrame::new(vec![
            Series::new("doc_id", doc_id),
            Series::new("idn", idn),
            Series::new("remote", remote).cast(&CATEGORICAL)?,
            Series::new("kind", kind).cast(&CATEGORICAL)?,
            Series::new("path", path),
            Series::new("lang_code", lang_code).cast(&CATEGORICAL)?,
            Series::new("lang_score", lang_score),
            Series::new("size", size),
            Series::new("strlen", strlen),
            Series::new("mtime", mtime),
            Series::new("hash", hash),
        ])?;

        let path = match self.output {
            None => dataset.data_dir().join(Dataset::REMOTES_INDEX),
            Some(path) => path,
        };

        let mut writer = IpcWriter::new(File::create(path)?)
            .with_compression(Some(IpcCompression::ZSTD));
        writer.finish(&mut df)?;

        Ok(())
    }
}
