use std::fs::File;
use std::path::PathBuf;

use clap::Parser;
use indicatif::ParallelProgressIterator;
use polars::prelude::*;
use rayon::prelude::*;
use url::Url;

use super::verify::VerifyMode;
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

    /// Update the document index based on the remote config.
    Sync(SyncCommand),

    /// Verify that the indexed documents are consistent with the
    /// tracked data sources (remotes).
    Verify(VerifyCommand),
}

pub(crate) fn execute(args: Remote) -> Result<(), DatasetError> {
    use crate::remote::Remote;

    let dataset = Dataset::discover()?;
    let mut config = dataset.config()?;

    match args.cmd {
        Command::Sync(cmd) => return cmd.execute(),
        Command::Verify(cmd) => return cmd.execute(),
        Command::Add { name, suffix, url } => {
            if config.remotes.contains_key(&name) {
                return Err(DatasetError::Remote(format!(
                    "remote with name '{name}' already exists"
                )));
            }

            let remote = Remote::new(url, suffix)?;
            config.remotes.insert(name, remote);
        }

        Command::Remove { name } => {
            if !config.remotes.contains_key(&name) {
                return Err(DatasetError::Remote(format!(
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
                return Err(DatasetError::Remote(format!(
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
                return Err(DatasetError::Remote(format!(
                    "remote with name '{name}' does not exists.",
                )));
            }
        }
    }

    config.save()?;
    Ok(())
}

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
    alpha: f64,
    size: u64,
    strlen: u64,
    modified: u64,
    hash: String,
}

const PBAR_COLLECT: &str =
    "remote: Collecting documents: {human_pos} | \
        elapsed: {elapsed_precise}{msg}";

const PBAR_INDEX: &str =
    "remote: Indexing documents: {human_pos} ({percent}%) | \
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
                    let language = document.lang().unwrap();

                    Row {
                        remote: name.into(),
                        idn: document.idn(),
                        kind: document.kind(),
                        path: document.relpath(remote),
                        lang_code: language.0,
                        lang_score: language.1,
                        alpha: document.alpha().unwrap(),
                        size: document.size(),
                        strlen: document.strlen().unwrap() as u64,
                        modified: document.modified(),
                        hash: document.hash(8).unwrap(),
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
        let mut alpha = vec![];
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
            alpha.push(record.alpha);
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
            Series::new("alpha", alpha),
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

#[derive(Debug, Default, Parser)]
pub(crate) struct VerifyCommand {
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

    /// Read the index from <filename>. By default, the index will
    /// be read from the internal data directory.
    #[arg(value_name = "filename")]
    path: Option<PathBuf>,
}

const PBAR_VERIFY: &str =
    "remote: Verifying documents: {human_pos} ({percent}%) | \
        elapsed: {elapsed_precise}{msg}";

impl VerifyCommand {
    pub(crate) fn new(
        quiet: bool,
        verbose: bool,
        mode: VerifyMode,
    ) -> Self {
        Self {
            quiet,
            verbose,
            mode,
            ..Default::default()
        }
    }

    pub(crate) fn execute(self) -> Result<(), DatasetError> {
        let dataset = Dataset::discover()?;
        let config = dataset.config()?;

        let path = match self.path {
            None => dataset.data_dir().join(Dataset::REMOTES_INDEX),
            Some(path) => path,
        };

        let df = IpcReader::new(File::open(path)?)
            .memory_mapped(None)
            .finish()?;

        let temp = &df.column("remote")?.cast(&DataType::String)?;
        let remote = temp.str()?;

        let temp = &df.column("kind")?.cast(&DataType::String)?;
        let kind = temp.str()?;

        let path = df.column("path")?.str()?;
        let hash = df.column("hash")?.str()?;
        let mtime = df.column("mtime")?.u64()?;
        let size = df.column("size")?.u64()?;

        let pbar = ProgressBarBuilder::new(PBAR_VERIFY, self.quiet)
            .len(df.height() as u64)
            .build();

        (0..df.height())
            .into_par_iter()
            .progress_with(pbar)
            .try_for_each(|idx| -> Result<(), DatasetError> {
                let name = remote.get(idx).unwrap();
                let remote = config.remotes.get(name).unwrap();
                let path = path.get(idx).unwrap();

                let result = remote.document(path);
                if result.is_err() {
                    return Err(DatasetError::Remote(format!(
                        "verification failed: document not found \
                            (path = {path:?}, remote = {name})"
                    )));
                }

                let mut document = result.unwrap();
                let expected = hash.get(idx).unwrap();
                let actual = document.hash(64).unwrap();
                if !actual.starts_with(expected) {
                    return Err(DatasetError::Remote(format!(
                        "verification failed: hash mismatch \
                            (path = {path:?}, remote = {name})"
                    )));
                }

                let kind = kind.get(idx).unwrap().parse().unwrap();
                if document.kind() != kind {
                    return Err(DatasetError::Remote(format!(
                        "verification failed: kind mismatch \
                            (path = {path:?}, remote = {name})"
                    )));
                }

                if self.mode >= VerifyMode::Strict
                    && document.modified() != mtime.get(idx).unwrap()
                {
                    return Err(DatasetError::Remote(format!(
                        "verification failed: mtime mismatch \
                                (path = {path:?}, remote = {name})"
                    )));
                }

                if self.mode >= VerifyMode::Pedantic
                    && document.size() != size.get(idx).unwrap()
                {
                    return Err(DatasetError::Remote(format!(
                        "verification failed: size mismatch \
                            (path = {path:?}, remote = {name})"
                    )));
                }

                Ok(())
            })
    }
}
