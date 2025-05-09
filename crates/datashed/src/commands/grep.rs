use std::ffi::OsStr;
use std::fs::File;
use std::io::stdout;
use std::path::PathBuf;

use clap::Parser;
use indicatif::ParallelProgressIterator;
use polars::prelude::*;
use polars::sql::SQLContext;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use regex::bytes::RegexBuilder;

use crate::doctype::DocumentType;
use crate::prelude::*;

const PBAR_PROCESS: &str = "Processing documents: {human_pos} ({percent}%) | \
        elapsed: {elapsed_precise}{msg}";

/// Find documents matching a pattern.
#[derive(Debug, Default, Parser)]
pub(crate) struct Grep {
    /// Run verbosely. Print additional progress information to the
    /// standard error stream. This option conflicts with the
    /// `--quiet` option.
    #[arg(short, long, conflicts_with = "quiet")]
    verbose: bool,

    /// Operate quietly; do not show progress. This option conflicts
    /// with the `--verbose` option.
    #[arg(short, long, conflicts_with = "verbose")]
    quiet: bool,

    /// Ignore the datashed's index and use `filename` instead.
    #[arg(long, short = 'I')]
    index: Option<PathBuf>,

    /// Ignore documents which are *not* explicitly listed in the given
    /// allow-lists.
    #[arg(long = "allow-list", short = 'A')]
    allow_list: Option<PathBuf>,

    /// Ignore documents which are explicitly listed in the given
    /// deny-lists.
    #[arg(long = "deny-list", short = 'D')]
    deny_list: Option<PathBuf>,

    /// If set, all patterns will be search case insensitive.
    #[arg(long = "ignore-case", short = 'i')]
    case_ignore: bool,

    /// Keep documents that don't match.
    #[arg(long = "invert-match")]
    invert: bool,

    /// Use only the first NUM bytes to search for the given pattern.
    /// If the value is 0 or greater than the document size the entire
    /// document is used for searching.
    #[arg(long, short = 'n', value_name = "NUM")]
    max_bytes: Option<u64>,

    /// Whether to append to an existing file or not.
    #[arg(long, short = 'a', requires = "output")]
    append: bool,

    /// Write the sub-index into `filename`. By default output will be
    /// written in CSV format to the standard output (`stdout`).
    #[arg(short, long, value_name = "filename")]
    output: Option<PathBuf>,

    /// An optional predicate to filter the document-set.
    #[arg(long = "where")]
    predicate: Option<String>,

    /// Retrieve specific columns from the index.
    #[arg(long, short = 'H')]
    header: Option<String>,

    /// If specified, overwrite the document type with the specified
    /// value
    #[arg(long = "doc-type", short = 'd')]
    doctype: Option<DocumentType>,

    /// Add a comment column at the end of the sub-index.
    #[arg(long, short = 'c')]
    comment: Option<String>,

    ///  A regular expression used for searching
    pattern: String,
}

fn read_filter_list(path: PathBuf) -> DatashedResult<DataFrame> {
    Ok(match path.extension().and_then(OsStr::to_str) {
        Some("ipc" | "arrow") => IpcReader::new(File::open(path)?)
            .memory_mapped(None)
            .finish()?,
        _ => CsvReadOptions::default()
            .with_has_header(true)
            .with_infer_schema_length(Some(0))
            .try_into_reader_with_file_path(Some(path))?
            .finish()?,
    })
}

impl Grep {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        let datashed = Datashed::discover()?;
        let base_dir = datashed.base_dir();

        let index = if let Some(path) = self.index {
            IpcReader::new(File::open(path)?)
                .memory_mapped(None)
                .finish()?
        } else {
            let datashed = Datashed::discover()?;
            datashed.index()?
        };

        let re = RegexBuilder::new(&self.pattern)
            .case_insensitive(self.case_ignore)
            .build()
            .map_err(|_| DatashedError::other("invalid pattern"))?;

        let mut df: LazyFrame = if let Some(predicate) = self.predicate
        {
            let mut ctx = SQLContext::new();
            ctx.register("df", index.lazy());
            ctx.execute(&format!("SELECT * FROM df WHERE {predicate}"))?
        } else {
            index.lazy()
        };

        if let Some(path) = self.allow_list {
            df = df.semi_join(
                read_filter_list(path)?.lazy(),
                col("ppn"),
                col("ppn"),
            );
        }

        if let Some(path) = self.deny_list {
            df = df.anti_join(
                read_filter_list(path)?.lazy(),
                col("ppn"),
                col("ppn"),
            );
        }

        let df = df.collect()?;
        let path = df.column("path")?.str()?;

        let pbar = ProgressBarBuilder::new(PBAR_PROCESS, self.quiet)
            .len(df.height() as u64)
            .build();

        let paths: Vec<String> = (0..df.height())
            .into_par_iter()
            .progress_with(pbar)
            .filter_map(|idx| -> Option<String> {
                let path = path.get(idx).unwrap();
                let doc =
                    Document::from_path(base_dir.join(path)).unwrap();

                let mut bytes = doc.as_ref();
                if let Some(n) = self.max_bytes {
                    if n < doc.size() && n > 0 {
                        bytes = &bytes[0..=(n as usize)];
                    }
                }

                if re.is_match(bytes) ^ self.invert {
                    Some(path.to_string())
                } else {
                    None
                }
            })
            .collect();

        let paths =
            DataFrame::new(vec![Column::new("path".into(), &paths)])?;

        let mut df =
            df.lazy().semi_join(paths.lazy(), col("path"), col("path"));

        if let Some(doctype) = self.doctype {
            df = df
                .with_column(lit(doctype.to_string()).alias("doctype"));
        }

        if let Some(comment) = self.comment {
            df = df.with_column(lit(comment).alias("comment"));
        }

        if let Some(header) = self.header {
            let mut ctx = SQLContext::new();
            ctx.register("df", df);

            df = ctx.execute(&format!("SELECT {header} FROM df"))?
        }

        let mut df: DataFrame = df.collect()?;
        if let Some(ref path) = self.output {
            match path.extension().and_then(OsStr::to_str) {
                Some("csv") => {
                    if self.append {
                        let existing = CsvReadOptions::default()
                            .with_has_header(true)
                            .with_infer_schema_length(Some(0))
                            .try_into_reader_with_file_path(Some(
                                path.into(),
                            ))?
                            .finish()?;

                        df = existing.vstack(&df)?;
                    }

                    let mut writer =
                        CsvWriter::new(File::create(path)?);
                    writer.finish(&mut df)?;
                }
                _ => {
                    if self.append {
                        let existing =
                            IpcReader::new(File::open(path)?)
                                .memory_mapped(None)
                                .finish()?;
                        df = existing.vstack(&df)?;
                    }

                    let mut writer =
                        IpcWriter::new(File::create(path)?)
                            .with_compression(Some(
                                IpcCompression::ZSTD,
                            ));
                    writer.finish(&mut df)?;
                }
            }
        } else {
            let mut writer = CsvWriter::new(stdout().lock());
            writer.finish(&mut df)?;
        }

        Ok(())
    }
}
