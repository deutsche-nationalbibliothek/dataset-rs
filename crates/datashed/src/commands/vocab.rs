use std::ffi::OsStr;
use std::fs::{File, read_to_string};
use std::io::stdout;
use std::path::PathBuf;

use bstr::ByteSlice;
use clap::{Parser, ValueEnum};
use hashbrown::{HashMap, HashSet};
use indicatif::ParallelProgressIterator;
use polars::prelude::*;
use polars::sql::SQLContext;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use unicode_categories::UnicodeCategories;

use crate::prelude::*;

const PBAR_PROCESS: &str = "Processing documents: {human_pos} ({percent}%) | \
        elapsed: {elapsed_precise}{msg}";

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
enum UnicodeCategory {
    #[clap(name = "a")]
    All,
    #[clap(name = "l")]
    Lowercase,
    #[clap(name = "u")]
    Uppercase,
    #[clap(name = "t")]
    Titlecase,
    #[clap(name = "m")]
    Modifier,
    #[clap(name = "o")]
    Other,
}

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

    #[arg(long, conflicts_with_all = ["bigrams", "trigrams"])]
    unigrams: bool,

    #[arg(long, conflicts_with_all = ["unigrams", "trigrams"])]
    bigrams: bool,

    #[arg(long, conflicts_with_all = ["unigrams", "bigrams"])]
    trigrams: bool,

    /// Includes only those tokens in the vocabulary where at least one
    /// character belongs to one of the specified Unicode categories.
    #[arg(
        short = 'L',
        value_name = "category",
        hide_possible_values = true
    )]
    categories: Vec<UnicodeCategory>,

    #[arg(long)]
    stopwords: Option<PathBuf>,

    /// Ignore tokens with a length less than `n`.
    #[arg(
        long = "min-tl",
        short = 'l',
        default_value = "2",
        value_name = "n"
    )]
    min_token_len: usize,

    /// Ignore tokens with a frequency less than `n`.
    #[arg(long = "min-tf", default_value = "1", value_name = "n")]
    min_token_freq: u64,

    /// Ignore tokens with a document frequency less than `n`.
    #[arg(long = "min-df", default_value = "1", value_name = "n")]
    min_doc_freq: u64,

    /// Ignore documents which are *not* explicitly listed in the given
    /// allow-lists.
    #[arg(long = "allow-list", short = 'A')]
    allow_list: Option<PathBuf>,

    /// Ignore documents which are explicitly listed in the given
    /// deny-lists.
    #[arg(long = "deny-list", short = 'D')]
    deny_list: Option<PathBuf>,

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

type VocabMap = HashMap<String, (u64, u64)>;

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

impl Vocab {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        let datashed = Datashed::discover()?;
        let base_dir = datashed.base_dir();
        let index = datashed.index()?;

        let mut df: DataFrame = if let Some(predicate) = self.predicate
        {
            let mut ctx = SQLContext::new();
            ctx.register("df", index.lazy());
            ctx.execute(&format!("SELECT * FROM df WHERE {predicate}"))?
                .collect()?
        } else {
            index
        };

        if let Some(path) = self.allow_list {
            df = df
                .lazy()
                .semi_join(
                    read_filter_list(path)?.lazy(),
                    col("ppn"),
                    col("ppn"),
                )
                .collect()?;
        }

        if let Some(path) = self.deny_list {
            df = df
                .lazy()
                .semi_join(
                    read_filter_list(path)?.lazy(),
                    col("ppn"),
                    col("ppn"),
                )
                .collect()?;
        }

        let stopwords: HashSet<String> =
            if let Some(path) = self.stopwords {
                read_to_string(path)?
                    .lines()
                    .filter(|word| word.len() >= self.min_token_len)
                    .map(String::from)
                    .collect()
            } else {
                HashSet::new()
            };

        let size = if self.trigrams {
            3
        } else if self.bigrams {
            2
        } else {
            1
        };

        let path = df.column("path")?.str()?;

        let pbar = ProgressBarBuilder::new(PBAR_PROCESS, self.quiet)
            .len(df.height() as u64)
            .build();

        let predicates: Vec<fn(char) -> bool> = self
            .categories
            .iter()
            .map(|category| {
                use UnicodeCategory::*;
                match category {
                    Lowercase => UnicodeCategories::is_letter_lowercase,
                    Uppercase => UnicodeCategories::is_letter_uppercase,
                    Titlecase => UnicodeCategories::is_letter_titlecase,
                    Modifier => UnicodeCategories::is_letter_modifier,
                    Other => UnicodeCategories::is_letter_other,
                    All => UnicodeCategories::is_letter,
                }
            })
            .collect();

        let mut vocab = (0..df.height())
            .into_par_iter()
            .progress_with(pbar)
            .map(|idx| -> VocabMap {
                let path = path.get(idx).unwrap();
                let doc =
                    Document::from_path(base_dir.join(path)).unwrap();

                let words: Vec<String> = doc
                    .as_ref()
                    .words()
                    .filter(|word| {
                        word.chars().count() >= self.min_token_len
                    })
                    .filter(|word| {
                        if self.categories.is_empty() {
                            return true;
                        }

                        predicates.iter().any(|f| word.chars().any(f))
                    })
                    .filter(|word| {
                        stopwords.is_empty()
                            || !stopwords.contains(&word.to_lowercase())
                    })
                    .map(str::to_lowercase)
                    .collect();

                words.windows(size).fold(
                    VocabMap::new(),
                    |mut vocab, tokens| {
                        let token = tokens.join(" ");
                        vocab
                            .entry(token)
                            .and_modify(|(tf, _)| *tf += 1)
                            .or_insert((1, 1));
                        vocab
                    },
                )
            })
            .reduce(VocabMap::new, |mut acc, rhs| {
                for (token, count) in rhs.into_iter() {
                    acc.entry(token)
                        .and_modify(|(tf, df)| {
                            *tf += count.0;
                            *df += count.1;
                        })
                        .or_insert(count);
                }

                acc
            });

        if self.min_token_freq > 1 || self.min_doc_freq > 1 {
            vocab.retain(|_, (tf, df)| {
                *tf >= self.min_token_freq && *df >= self.min_doc_freq
            });
        }

        let mut tokens = Vec::with_capacity(vocab.len());
        let mut freqs = Vec::with_capacity(vocab.len());
        let mut docs = Vec::with_capacity(vocab.len());

        for (token, (tf, df)) in vocab.into_iter() {
            tokens.push(token);
            freqs.push(tf);
            docs.push(df);
        }

        let sort_options = SortMultipleOptions::default()
            .with_order_descending_multi([true, true, false]);

        let mut df = DataFrame::new(vec![
            Column::new("token".into(), tokens),
            Column::new("tf".into(), freqs),
            Column::new("df".into(), docs),
        ])?
        .sort(["tf", "df", "token"], sort_options)?;

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
