use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;
use indicatif::ParallelProgressIterator;
use pica_matcher::RecordMatcher;
use pica_path::{Path, PathExt};
use pica_record::io::{ReaderBuilder, RecordsIterator};
use pica_record::ByteRecord;
use polars::prelude::*;
use rayon::prelude::*;
use url::Url;

use super::update::Update;
use super::verify::VerifyMode;
use crate::dataset::Dataset;
use crate::document::{Document, DocumentKind};
use crate::error::{DatasetError, DatasetResult};
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

    /// Remove the remote named `name`.
    #[clap(visible_alias = "rm")]
    Remove {
        /// The name of the remote.
        name: String,
    },

    /// Changes the URL for the remote `name`.
    SetUrl {
        /// The name of the remote.
        name: String,

        /// The URL of the remote.
        url: Url,
    },

    /// Change the file suffix for the remote `name`.
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

            let remote = Remote::new(url, suffix, Default::default())?;
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
                    Remote::Local {
                        suffix,
                        refinements,
                        ..
                    } => {
                        *remote = Remote::new(
                            url,
                            suffix.to_string(),
                            refinements.clone(),
                        )?;
                    }
                }
            } else {
                return Err(DatasetError::Remote(format!(
                    "remote with name '{name}' does not exists.",
                )));
            }
        }
        Command::SetSuffix { name, suffix } => {
            let new_suffix = suffix;

            if let Some(remote) = config.remotes.get_mut(&name) {
                match remote {
                    Remote::Local { ref mut suffix, .. } => {
                        *suffix = new_suffix;
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

    /// Write the index into `filename`. By default, the index will
    /// be written to the internal data directory.
    #[arg(short, long, value_name = "filename")]
    output: Option<PathBuf>,

    /// The path to the PICA+ dump
    path: PathBuf,
}

#[derive(Debug, Default)]
pub(crate) struct DocumentKindMap {
    refinements: HashMap<(String, DocumentKind, String), DocumentKind>,
    matchers:
        HashMap<(String, DocumentKind, DocumentKind), RecordMatcher>,
}

impl Deref for DocumentKindMap {
    type Target = HashMap<(String, DocumentKind, String), DocumentKind>;

    fn deref(&self) -> &Self::Target {
        &self.refinements
    }
}

impl DerefMut for DocumentKindMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.refinements
    }
}

impl DocumentKindMap {
    pub(crate) fn new(dataset: &Dataset) -> DatasetResult<Self> {
        let config = dataset.config()?;
        let mut matchers = HashMap::new();

        for (name, remote) in config.remotes.iter() {
            for refinement in remote.refinements() {
                let matcher =
                    RecordMatcher::from_str(&refinement.filter)
                        .map_err(|_| {
                            DatasetError::Other(format!(
                                "invalid record matcher: '{}'",
                                &refinement.filter
                            ))
                        })?;

                let key = (
                    name.to_string(),
                    refinement.from.clone(),
                    refinement.to.clone(),
                );

                let _ = matchers.insert(key, matcher);
            }
        }

        Ok(Self {
            matchers,
            ..Default::default()
        })
    }

    pub(crate) fn process_record(&mut self, record: &ByteRecord) {
        for ((name, from, to), matcher) in self.matchers.iter() {
            if matcher.is_match(record, &Default::default()) {
                let idn = record.idn().unwrap_or_default().to_string();
                let key = (name.to_string(), from.clone(), idn.clone());
                let _ = self.refinements.insert(key, to.clone());
                return;
            }
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct SubjectCategoryMap {
    paths: Vec<Path>,
    allow_list: BTreeSet<String>,
    map: HashMap<String, String>,
}

impl Deref for SubjectCategoryMap {
    type Target = HashMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl DerefMut for SubjectCategoryMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

impl SubjectCategoryMap {
    pub(crate) fn new(_dataset: &Dataset) -> DatasetResult<Self> {
        let paths = vec![
            r#"045E{ e | E == "i" && H == "dnb" }"#,
            r#"045E{ e | E == "i" && H == "dnb-pa" }"#,
            r#"045E{ e | !E? && !H? }"#,
            r#"045E{ e | E == "m" && H in ["aepsg", "emasg"] }"#,
            r#"045E{ e | E == "a" }"#,
        ];

        let allow_list = BTreeSet::from_iter(
            [
                "000", "004", "010", "020", "030", "050", "060", "070",
                "080", "090", "100", "130", "150", "200", "220", "230",
                "290", "300", "310", "320", "330", "333.7", "340",
                "350", "355", "360", "370", "380", "390", "400", "420",
                "430", "439", "440", "450", "460", "470", "480", "490",
                "491.8", "500", "510", "520", "530", "540", "550",
                "560", "570", "580", "590", "600", "610", "620",
                "621.3", "624", "630", "640", "650", "660", "670",
                "690", "700", "710", "720", "730", "740", "741.5",
                "750", "760", "770", "780", "790", "791", "792", "793",
                "796", "800", "810", "820", "830", "839", "840", "850",
                "860", "870", "880", "890", "891.8", "900", "910",
                "914.3", "920", "930", "940", "943", "950", "960",
                "970", "980", "990", "B", "K", "S",
            ]
            .map(String::from),
        );

        Ok(Self {
            paths: paths.into_iter().map(Path::new).collect(),
            allow_list,
            ..Default::default()
        })
    }

    pub(crate) fn process_record(&mut self, record: &ByteRecord) {
        for path in self.paths.iter() {
            for ddc_sc in record.path(path, &Default::default()) {
                let ddc_sc = ddc_sc.to_string();
                if self.allow_list.contains(&ddc_sc) {
                    self.insert(
                        record.idn().unwrap().to_string(),
                        ddc_sc,
                    );
                    return;
                }
            }
        }
    }
}

#[derive(Debug)]
struct Row {
    remote: String,
    idn: String,
    kind: DocumentKind,
    sc: Option<String>,
    path: String,
    lang_code: &'static str,
    lang_score: f64,
    alpha: f64,
    size: u64,
    strlen: u64,
    modified: u64,
    hash: String,
}

const PBAR_METADATA: &str =
    "remote: Collecting metadata: {human_pos} | \
        elapsed: {elapsed_precise}{msg}";

const PBAR_COLLECT: &str =
    "remote: Collecting documents: {human_pos} | \
        elapsed: {elapsed_precise}{msg}";

const PBAR_INDEX: &str =
    "remote: Indexing documents: {human_pos} ({percent}%) | \
        elapsed: {elapsed_precise}{msg}";

impl SyncCommand {
    pub(crate) fn new(args: &Update) -> Self {
        Self {
            quiet: args.quiet,
            verbose: args.verbose,
            path: args.path.clone(),
            ..Default::default()
        }
    }

    fn doc_ids(
        &self,
        dataset: &Dataset,
    ) -> DatasetResult<HashMap<String, u32>> {
        let mut map = HashMap::new();

        let path = match self.output {
            None => dataset.data_dir().join(Dataset::REMOTES_INDEX),
            Some(ref path) => path.to_path_buf(),
        };

        if let Ok(fh) = File::open(path) {
            let df = IpcReader::new(fh).memory_mapped(None).finish()?;

            let temp = &df.column("remote")?.cast(&DataType::String)?;
            let remote = temp.str()?;

            let temp = &df.column("kind")?.cast(&DataType::String)?;
            let kind = temp.str()?;

            let doc_id = df.column("doc_id")?.u32()?;
            let idn = df.column("idn")?.str()?;

            for row in 0..df.height() {
                map.insert(
                    format!(
                        "{}-{}-{}",
                        remote.get(row).unwrap(),
                        idn.get(row).unwrap(),
                        kind.get(row).unwrap(),
                    ),
                    doc_id.get(row).unwrap(),
                );
            }
        }

        Ok(map)
    }

    pub(crate) fn execute(self) -> DatasetResult<()> {
        let dataset = Dataset::discover()?;
        let config = dataset.config()?;

        let doc_ids = self.doc_ids(&dataset)?;
        let doc_id_max = doc_ids.values().max().unwrap_or(&0);

        let mut subject_categories = SubjectCategoryMap::new(&dataset)?;
        let mut document_kinds = DocumentKindMap::new(&dataset)?;

        let pbar =
            ProgressBarBuilder::new(PBAR_METADATA, self.quiet).build();

        let mut reader = ReaderBuilder::new().from_path(&self.path)?;
        while let Some(result) = reader.next() {
            if let Ok(record) = result {
                subject_categories.process_record(&record);
                document_kinds.process_record(&record);
            }

            pbar.inc(1);
        }

        pbar.finish_using_style();

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

                    let idn = document.idn();
                    let sc = subject_categories.get(&idn).cloned();
                    let kind = document.kind();
                    let kind = document_kinds
                        .get(&(
                            name.to_string(),
                            kind.clone(),
                            idn.clone(),
                        ))
                        .unwrap_or(&kind)
                        .to_owned();

                    Row {
                        remote: name.into(),
                        idn,
                        kind,
                        sc,
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
        let mut ddc_sc = vec![];
        let mut path = vec![];
        let mut lang_code = vec![];
        let mut lang_score = vec![];
        let mut alpha = vec![];
        let mut size = vec![];
        let mut strlen = vec![];
        let mut mtime = vec![];
        let mut hash = vec![];

        let mut offset = 0;

        for record in records.into_iter() {
            let key = format!(
                "{}-{}-{}",
                record.remote, record.idn, record.kind,
            );

            doc_id.push(doc_ids.get(&key).copied().unwrap_or({
                offset += 1;
                doc_id_max + offset
            }));

            remote.push(record.remote);
            idn.push(record.idn);
            kind.push(record.kind.to_string());
            ddc_sc.push(record.sc);
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
            Series::new("ddc_sc", ddc_sc).cast(&CATEGORICAL)?,
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

    /// Read the index from `filename`. By default, the index will
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
