use std::ffi::OsStr;
use std::fs::File;
use std::io::stdout;
use std::path::PathBuf;

use clap::Parser;
use polars::prelude::*;
use polars::sql::SQLContext;

use crate::prelude::*;

/// Select data from the index.
#[derive(Debug, Default, Parser)]
pub(crate) struct Select {
    /// Run verbosely. Print additional progress information to the
    /// standard error stream. This option conflicts with the
    /// `--quiet` option.
    #[arg(short, long, conflicts_with = "quiet")]
    verbose: bool,

    /// Operate quietly; do not show progress. This option conflicts
    /// with the `--verbose` option.
    #[arg(short, long, conflicts_with = "verbose")]
    quiet: bool,

    /// Ingore the datashed's index and use `filename` instead.
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

    #[arg(long, default_value = "idn", conflicts_with_all = ["left_on", "right_on"])]
    on: String,

    #[arg(long, requires = "right_on", conflicts_with = "on")]
    left_on: Option<String>,

    #[arg(long, requires = "left_on", conflicts_with = "on")]
    right_on: Option<String>,

    /// The columns names of the index to select.
    #[arg(long, short, default_value = "*")]
    columns: String,
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

impl Select {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        let datashed = Datashed::discover()?;
        let index = if let Some(path) = self.index {
            IpcReader::new(File::open(path)?)
                .memory_mapped(None)
                .finish()?
        } else {
            datashed.index()?
        };

        let columns = self.columns;
        let mut ctx = SQLContext::new();
        ctx.register("df", index.lazy());

        let mut df = if let Some(predicate) = self.predicate {
            ctx.execute(&format!(
                "SELECT {columns} FROM df WHERE {predicate}",
            ))?
        } else {
            ctx.execute(&format!("SELECT {columns} FROM df"))?
        };

        let left_on: Vec<_> = self
            .left_on
            .unwrap_or(self.on.clone())
            .split(',')
            .map(str::trim)
            .map(col)
            .collect();

        let right_on: Vec<_> = self
            .right_on
            .unwrap_or(self.on)
            .split(',')
            .map(str::trim)
            .map(col)
            .collect();

        if let Some(path) = self.allow_list {
            df = df.join(
                read_filter_list(path)?.lazy(),
                left_on.clone(),
                right_on.clone(),
                JoinArgs::new(JoinType::Semi),
            );
        }

        if let Some(path) = self.deny_list {
            df = df.join(
                read_filter_list(path)?.lazy(),
                left_on,
                right_on,
                JoinArgs::new(JoinType::Anti),
            );
        }

        let mut df = df.collect()?;

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
