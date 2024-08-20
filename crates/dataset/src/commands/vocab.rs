use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;
use csv::WriterBuilder;
use pica_matcher::{MatcherOptions, RecordMatcher};
use pica_path::{Path, PathExt};
use pica_record::io::{ReaderBuilder, RecordsIterator};
use pica_record::ByteRecord;
use pica_select::{Query, QueryExt};
use serde::{Deserialize, Serialize};

use crate::prelude::*;
use crate::vocab::{KindConfig, VocabKind};

#[derive(Debug, Parser)]
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

    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Parser)]
pub(crate) enum Command {
    Update {
        /// If set, the index will be written in CSV format to the
        /// standard output (stdout).
        #[arg(long, conflicts_with = "output")]
        stdout: bool,

        /// Write the index into `filename`. By default (if `--stdout`
        /// isn't set), the index will be written to `index.ipc` into
        /// the root directory.
        #[arg(short, long, value_name = "filename")]
        output: Option<PathBuf>,

        /// The path to the PICA+ dump
        path: PathBuf,
    },
}

const PBAR_PROCESS: &str = "Processing records: {human_pos} | \
        elapsed: {elapsed_precise}{msg}";

#[derive(Debug, Serialize, Deserialize)]
struct AuthorityRecord {
    pub(crate) uri: String,
    pub(crate) label: String,
    pub(crate) notation: String,
    #[serde(skip)]
    pub(crate) kind: VocabKind,
}

fn bbg_to_kind<T: AsRef<[u8]>>(bbg: T) -> DatasetResult<VocabKind> {
    let bbg = bbg.as_ref();

    if bbg.len() < 3 {
        return Err(DatasetError::other("invalid bbg"));
    }

    Ok(match &bbg[0..2] {
        b"Tb" => VocabKind::CorporateBody,
        b"Tf" => VocabKind::Conference,
        b"Tg" => VocabKind::PlaceOrGeoName,
        b"Tp" => VocabKind::Person,
        b"Ts" => VocabKind::SubjectHeading,
        b"Tu" => VocabKind::Work,
        _ => return Err(DatasetError::other("invalid kind")),
    })
}

macro_rules! pref_label {
    ($record:expr, $query:expr, $parens:expr) => {{
        let outcome: Vec<Vec<String>> = $record
            .query(&Query::new($query), &Default::default())
            .into_inner();

        if outcome.len() > 0 {
            let mut label = String::new();
            if !outcome[0][0].is_empty() {
                label.push_str(&outcome[0][0]);
            }

            if !outcome[0][1].is_empty() {
                if $parens {
                    label.push_str(&format!(" ({})", &outcome[0][1]));
                } else {
                    label.push_str(&format!(", {}", &outcome[0][1]));
                }
            }

            if !label.is_empty() {
                Some(label)
            } else {
                None
            }
        } else {
            None
        }
    }};
}

impl TryFrom<&ByteRecord<'_>> for AuthorityRecord {
    type Error = DatasetError;

    fn try_from(record: &ByteRecord<'_>) -> Result<Self, Self::Error> {
        use VocabKind::*;

        let Some(idn) = record.idn().map(ToString::to_string) else {
            return Err(DatasetError::other("unable to get idn"));
        };

        let options = MatcherOptions::default();

        let values = record.path(&Path::new("002@.0"), &options);
        let kind = match values.first() {
            None => {
                return Err(DatasetError::other("unable to get bbg"))
            }
            Some(bbg) => bbg_to_kind(bbg)?,
        };

        let label = match kind {
            Conference => pref_label!(record, "030A{a, g}", false),
            CorporateBody => pref_label!(record, "029A{a, g}", false),
            Person => pref_label!(record, "028A{a, d}", true),
            PlaceOrGeoName => pref_label!(record, "065A{a, g}", false),
            SubjectHeading => pref_label!(record, "041A{a, g}", false),
            Work => pref_label!(record, "022A{a, g}", false),
        };

        Ok(AuthorityRecord {
            uri: format!("https://d-nb.info/gnd/{idn}"),
            label: label.unwrap_or(format!("IDN : {idn}")),
            notation: "".into(),
            kind,
        })
    }
}

impl Vocab {
    pub(crate) fn execute(self) -> DatasetResult<()> {
        match self.cmd {
            Command::Update { .. } => self.update(),
        }
    }

    pub(crate) fn update(&self) -> DatasetResult<()> {
        let Command::Update {
            stdout,
            output,
            path,
        } = &self.cmd;

        let dataset = Dataset::discover()?;
        let config = dataset.config()?;

        let mut freqs: BTreeMap<String, usize> = BTreeMap::new();
        let mut vocab: BTreeMap<String, AuthorityRecord> =
            BTreeMap::new();

        let mut reader = ReaderBuilder::new().from_path(path)?;
        let matcher = RecordMatcher::from_str(&config.vocab.filter)?;
        let options = MatcherOptions::new()
            .strsim_threshold(config.vocab.strsim_threshold)
            .case_ignore(config.vocab.case_ignore);

        let pbar =
            ProgressBarBuilder::new(PBAR_PROCESS, self.quiet).build();

        while let Some(result) = reader.next() {
            pbar.inc(1);

            let Ok(record) = result else {
                continue;
            };

            let idn = record.idn().map(ToString::to_string).unwrap();
            let mut seen = BTreeSet::new();

            if matcher.is_match(&record, &options) {
                let record = AuthorityRecord::try_from(&record)?;
                vocab.insert(idn, record);
                continue;
            }

            for target in config.vocab.targets.iter() {
                if let Some(ref matcher_str) = target.predicate {
                    let matcher = RecordMatcher::from_str(matcher_str)?;
                    if !matcher.is_match(&record, &options) {
                        continue;
                    }
                }

                record
                    .path(&Path::new(&target.source), &options)
                    .iter()
                    .for_each(|idn| {
                        if !idn.is_empty() && !seen.contains(idn) {
                            seen.insert(idn.to_owned());
                            freqs
                                .entry(idn.to_string())
                                .and_modify(|value| *value += 1)
                                .or_insert(1);
                        }
                    });
            }
        }

        pbar.finish_using_style();

        let inner: Box<dyn Write> = match output {
            Some(path) => Box::new(File::create(path)?),
            None if *stdout => Box::new(io::stdout().lock()),
            None => Box::new(File::create(
                dataset.base_dir().join(Dataset::VOCAB),
            )?),
        };

        let mut writer = WriterBuilder::new().from_writer(inner);
        for (idn, record) in vocab.into_iter() {
            if let Some(KindConfig { threshold }) =
                config.vocab.kinds.get(&record.kind)
            {
                let count = freqs.remove(&idn).unwrap_or(0);
                if count < *threshold {
                    continue;
                }
            }

            writer.serialize(record)?
        }

        writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    type TestResult = anyhow::Result<()>;

    #[test]
    fn bbg_to_kind_ok() -> TestResult {
        assert_eq!(bbg_to_kind("Tb1")?, VocabKind::CorporateBody);
        assert_eq!(bbg_to_kind("Tf1")?, VocabKind::Conference);
        assert_eq!(bbg_to_kind("Tg1")?, VocabKind::PlaceOrGeoName);
        assert_eq!(bbg_to_kind("Tp1")?, VocabKind::Person);
        assert_eq!(bbg_to_kind("Ts1")?, VocabKind::SubjectHeading);
        assert_eq!(bbg_to_kind("Tu1")?, VocabKind::Work);

        Ok(())
    }

    #[test]
    #[should_panic]
    fn bbg_to_kind_panic() {
        bbg_to_kind("Tp").unwrap();
    }
}
