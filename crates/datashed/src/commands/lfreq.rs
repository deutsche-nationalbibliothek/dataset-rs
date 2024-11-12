use std::ffi::OsStr;
use std::fs::File;
use std::io::stdout;
use std::path::{Path, PathBuf};

use bstr::ByteSlice;
use hashbrown::HashMap;
use indicatif::ParallelProgressIterator;
use polars::prelude::*;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use unicode_normalization::UnicodeNormalization;

use crate::prelude::*;

const PBAR_PROCESS: &str =
    "Processing documents: {human_pos} ({percent}%) | \
        elapsed: {elapsed_precise}{msg}";

/// Create a frequency table over a fixed alphabet.
#[derive(Debug, clap::Parser)]
pub(crate) struct Lfreq {
    /// Run verbosely. Print additional progress information to the
    /// standard error stream. This option conflicts with the
    /// `--quiet` option.
    #[arg(short, long, conflicts_with = "quiet")]
    verbose: bool,

    /// Operate quietly; do not show progress. This option conflicts
    /// with the `--verbose` option.
    #[arg(short, long, conflicts_with = "verbose")]
    quiet: bool,

    /// The alphabet used to determine the letter frequencies.
    ///
    /// Note that the given alphabet is normalized to lowercase
    /// characters, duplicate characters are removed and the remaining
    /// characters are sorted in ascending order.
    #[arg(
        long,
        default_value = "abcdefghijklmnopqrstuvwxyzäöüß",
        value_name = "alphabet"
    )]
    alphabet: String,

    /// Write output to `filename` instead of `stdout`.
    #[arg(short, long)]
    output: Option<PathBuf>,
}

struct Row {
    path: String,
    total: u64,
    freqs: HashMap<char, u64>,
}

impl Lfreq {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        let datashed = Datashed::discover()?;
        let index = datashed.index()?;
        let path = index.column("path")?.str()?;

        let pbar = ProgressBarBuilder::new(PBAR_PROCESS, self.quiet)
            .len(index.height() as u64)
            .build();

        let mut alphabet = self
            .alphabet
            .to_lowercase()
            .chars()
            .nfc()
            .collect::<Vec<char>>();

        alphabet.sort_unstable();
        alphabet.dedup();

        let result: Result<Vec<Row>, _> = (0..index.height())
            .into_par_iter()
            .progress_with(pbar)
            .map(|idx| -> Result<Row, DatashedError> {
                let path = path.get(idx).unwrap();
                if !Path::new(path).is_file() {
                    bail!(
                        "verification failed: file not found \
                            (path = {path})."
                    );
                }

                let doc = Document::from_path(path)?;
                let content = doc
                    .as_ref()
                    .to_str()
                    .map_err(|_| DatashedError::other("utf8 error"))?;

                let freqs = content
                    .to_lowercase()
                    .nfc()
                    .filter(|c| alphabet.contains(c))
                    .fold(HashMap::<char, u64>::new(), |mut acc, x| {
                        acc.entry(x)
                            .and_modify(|e| *e += 1)
                            .or_insert(1);
                        acc
                    });

                Ok(Row {
                    path: path.to_string(),
                    total: freqs.values().sum(),
                    freqs,
                })
            })
            .collect();

        let rows = result?;

        let mut freqs = HashMap::<char, Vec<u64>>::new();
        let mut path = vec![];
        let mut total = vec![];

        for row in rows.into_iter() {
            for c in alphabet.iter() {
                let count = row.freqs.get(c).unwrap_or(&0);
                freqs
                    .entry(*c)
                    .and_modify(|e| e.push(*count))
                    .or_insert(vec![*count]);
            }

            total.push(row.total);
            path.push(row.path);
        }

        let mut series = vec![];
        series.push(Column::new("path".into(), path));
        series.push(Column::new("total".into(), total));

        for c in alphabet {
            series.push(Column::new(
                c.to_string().into(),
                freqs.get(&c).unwrap(),
            ));
        }

        let mut df: DataFrame = DataFrame::new(series)?
            .lazy()
            .select([col("*").shrink_dtype()])
            .collect()?;

        match self.output {
            Some(path) => {
                let file = File::create(&path)?;
                match path.extension().and_then(OsStr::to_str) {
                    Some("ipc" | "arrow") => {
                        let compression = Some(IpcCompression::ZSTD);
                        let mut writer = IpcWriter::new(file)
                            .with_compression(compression);
                        writer.finish(&mut df)?;
                    }
                    _ => {
                        let mut writer = CsvWriter::new(file);
                        writer.finish(&mut df)?;
                    }
                }
            }
            None => {
                let mut writer = CsvWriter::new(stdout().lock());
                writer.finish(&mut df)?;
            }
        };

        Ok(())
    }
}
