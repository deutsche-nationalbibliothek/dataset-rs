use std::fmt::{self, Display};
use std::fs::File;
use std::io::stdout;
use std::path::PathBuf;

use clap::Parser;
use ddc::DdcMatcher;
use indicatif::ParallelProgressIterator;
use isbn::IsbnMatcher;
use isni::IsniMatcher;
use issn::IssnMatcher;
use orcid::OrcidMatcher;
use polars::prelude::*;
use rayon::prelude::*;

use crate::prelude::*;

mod ddc;
mod isbn;
mod isni;
mod issn;
mod orcid;

#[derive(Debug)]
pub(crate) struct Reference {
    kind: RefKind,
    value: String,
    start: usize,
    end: usize,
}

#[derive(Debug)]
pub(crate) enum RefKind {
    Isbn,
    Issn,
    Ddc,
    Orcid,
    Isni,
}

impl Display for RefKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Isbn => write!(f, "isbn"),
            Self::Issn => write!(f, "issn"),
            Self::Ddc => write!(f, "ddc"),
            Self::Orcid => write!(f, "orcid"),
            Self::Isni => write!(f, "isni"),
        }
    }
}

trait Matcher: Sync {
    fn matches(&self, content: &[u8]) -> Vec<Reference>;
}

const PBAR_PROCESS: &str = "Processing documents: {human_pos} ({percent}%) | \
        elapsed: {elapsed_precise}{msg}";

/// Verify that the inventory of documents matches the index.
#[derive(Debug, Default, Parser)]
pub(crate) struct BibRefs {
    /// Run verbosely. Print additional progress information to the
    /// standard error stream. This option conflicts with the
    /// `--quiet` option.
    #[arg(short, long, conflicts_with = "quiet")]
    verbose: bool,

    /// Operate quietly; do not show progress. This option conflicts
    /// with the `--verbose` option.
    #[arg(short, long, conflicts_with = "verbose")]
    quiet: bool,

    /// Write the bibrefs into `filename`. By default output will be
    /// written in CSV format to the standard output (`stdout`).
    /// the root directory.
    #[arg(short, long, value_name = "filename")]
    output: Option<PathBuf>,
}

#[derive(Debug)]
struct Record {
    path: String,
    r#type: String,
    value: String,
    start: u64,
    end: u64,
}

impl BibRefs {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        let datashed = Datashed::discover()?;
        let index = datashed.index()?;

        let matchers: Vec<Box<dyn Matcher>> = vec![
            Box::new(IsbnMatcher::default()),
            Box::new(IssnMatcher::default()),
            Box::new(DdcMatcher::default()),
            Box::new(OrcidMatcher::default()),
            Box::new(IsniMatcher::default()),
        ];

        let pbar = ProgressBarBuilder::new(PBAR_PROCESS, self.quiet)
            .len(index.height() as u64)
            .build();

        let path = index.column("path")?.str()?;

        let records: Vec<Record> = (0..index.height())
            .into_par_iter()
            .progress_with(pbar)
            .flat_map(|idx| {
                let path = path.get(idx).unwrap();
                let doc = Document::from_path(path).unwrap();
                let content = doc.as_ref();
                matchers
                    .iter()
                    .flat_map(|m| m.matches(content))
                    .map(|reference| Record {
                        path: path.to_string(),
                        r#type: reference.kind.to_string(),
                        value: reference.value,
                        start: reference.start as u64,
                        end: reference.end as u64,
                    })
                    .collect::<Vec<Record>>()
            })
            .collect();

        let mut path = vec![];
        let mut r#type = vec![];
        let mut value = vec![];
        let mut start = vec![];
        let mut end = vec![];

        for record in records.into_iter() {
            path.push(record.path);
            r#type.push(record.r#type);
            value.push(record.value);
            start.push(record.start);
            end.push(record.end);
        }

        let mut df = DataFrame::new(vec![
            Column::new("path".into(), path),
            Column::new("type".into(), r#type),
            Column::new("value".into(), value),
            Column::new("start".into(), start),
            Column::new("end".into(), end),
        ])?;

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
