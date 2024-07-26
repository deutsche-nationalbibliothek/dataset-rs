use std::collections::BTreeMap;
use std::fs::File;
use std::io::stdout;
use std::path::PathBuf;

use bstr::ByteSlice;
use clap::Parser;
use indicatif::ParallelProgressIterator;
use polars::prelude::*;
use polars::sql::SQLContext;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::prelude::*;

const PBAR_PROCESS: &str =
    "Processing documents: {human_pos} ({percent}%) | \
        elapsed: {elapsed_precise}{msg}";

/// Create an index of all available documents.
#[derive(Debug, Default, Parser)]
pub(crate) struct Vocab {
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

    /// Write the vocabulary into `filename`. By default output will be
    /// written in CSV format to the standard output (`stdout`).
    #[arg(short, long, value_name = "filename")]
    output: Option<PathBuf>,

    #[arg(long = "where")]
    predicate: Option<String>,
}

type VocabMap = BTreeMap<String, u64>;

impl Vocab {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        let datashed = Datashed::discover()?;
        let base_dir = datashed.base_dir();
        let index = datashed.index()?;

        let df: DataFrame = if let Some(predicate) = self.predicate {
            let mut ctx = SQLContext::new();
            ctx.register("df", index.lazy());
            ctx.execute(&format!("SELECT * FROM df WHERE {predicate}"))?
                .collect()?
        } else {
            index
        };

        let path = df.column("path")?.str()?;

        let pbar = ProgressBarBuilder::new(PBAR_PROCESS, self.quiet)
            .len(df.height() as u64)
            .build();

        let vocab = (0..df.height())
            .into_par_iter()
            .progress_with(pbar)
            .map(|idx| -> VocabMap {
                let path = path.get(idx).unwrap();
                let doc =
                    Document::from_path(base_dir.join(path)).unwrap();

                doc.as_ref().words().map(str::to_lowercase).fold(
                    VocabMap::new(),
                    |mut vocab, token| {
                        vocab
                            .entry(token)
                            .and_modify(|cnt| *cnt += 1)
                            .or_insert(1);
                        vocab
                    },
                )
            })
            .reduce(VocabMap::new, |mut acc, rhs| {
                for (token, count) in rhs.into_iter() {
                    acc.entry(token)
                        .and_modify(|cnt| *cnt += count)
                        .or_insert(count);
                }

                acc
            });

        let mut tokens = vec![];
        let mut counts = vec![];

        for (token, count) in vocab.into_iter() {
            tokens.push(token);
            counts.push(count);
        }

        let sort_options = SortMultipleOptions::default()
            .with_order_descending_multi([true, false]);

        let mut df = DataFrame::new(vec![
            Series::new("token", tokens),
            Series::new("count", counts),
        ])?
        .sort(["count", "token"], sort_options)?;

        if let Some(path) = self.output {
            let mut writer = IpcWriter::new(File::create(path)?)
                .with_compression(Some(IpcCompression::ZSTD));
            writer.finish(&mut df)?;
        } else {
            let mut writer = CsvWriter::new(stdout().lock());
            writer.finish(&mut df)?;
        }

        Ok(())
    }
}