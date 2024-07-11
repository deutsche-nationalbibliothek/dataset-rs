use std::fs::File;
use std::io::{stdout, Write};
use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use flate2::write::GzEncoder;
use flate2::Compression;
use indicatif::ProgressIterator;

use crate::datashed::Datashed;
use crate::error::{DatashedError, DatashedResult};
use crate::progress::ProgressBarBuilder;

const PBAR_ARCHIVE: &str =
    "Archive documents: {human_pos} ({percent}%) | \
        elapsed: {elapsed_precise}{msg}";

#[derive(Clone, Debug, PartialEq, PartialOrd, Default, ValueEnum)]
pub(crate) enum VerifyMode {
    Permissive,
    #[default]
    Strict,
    Pedantic,
}

/// Create an archive (tar.gz) of the index and all documents.
#[derive(Debug, Default, Parser)]
pub(crate) struct Archive {
    /// Run verbosely. Print additional progress information to the
    /// standard error stream. This option conflicts with the
    /// `--quiet` option.
    #[arg(short, long, conflicts_with = "quiet")]
    verbose: bool,

    /// Operate quietly; do not show progress. This option conflicts
    /// with the `--verbose` option.
    #[arg(short, long, conflicts_with = "verbose")]
    quiet: bool,

    #[arg(long, conflicts_with = "best")]
    fast: bool,

    #[arg(long, conflicts_with = "fast")]
    best: bool,

    /// Write the archive to `filename` instead of stdout.
    #[arg(short, long, value_name = "filename")]
    output: Option<PathBuf>,
}

pub(crate) fn execute(args: Archive) -> DatashedResult<()> {
    let datashed = Datashed::discover()?;
    let index = datashed.index()?;
    let paths = index.column("path")?.str()?;

    let level = if args.fast {
        Compression::fast()
    } else if args.best {
        Compression::best()
    } else {
        Compression::default()
    };

    let out: Box<dyn Write> = match args.output {
        Some(path) => Box::new(File::create(path)?),
        None => Box::new(stdout().lock()),
    };

    let gzip = GzEncoder::new(out, level);
    let mut archive = tar::Builder::new(gzip);

    let pbar = ProgressBarBuilder::new(PBAR_ARCHIVE, args.quiet)
        .len(paths.len() as u64)
        .build();

    paths.iter().progress_with(pbar).try_for_each(|path| {
        let path = path.unwrap();

        let mut file =
            File::open(datashed.base_dir().join(path)).unwrap();
        archive.append_file(path, &mut file).unwrap();

        Ok::<(), DatashedError>(())
    })?;

    let mut index =
        File::open(datashed.base_dir().join(Datashed::INDEX))?;
    archive.append_file("index.ipc", &mut index)?;

    let mut config =
        File::open(datashed.base_dir().join(Datashed::CONFIG))?;
    archive.append_file(Datashed::CONFIG, &mut config)?;

    archive.finish()?;
    Ok(())
}
