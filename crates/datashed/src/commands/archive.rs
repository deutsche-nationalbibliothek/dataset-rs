use std::fs::File;
use std::io::{Write, stdout};
use std::path::PathBuf;

use clap::Parser;
use flate2::Compression;
use flate2::write::GzEncoder;
use indicatif::ProgressIterator;

use crate::prelude::*;

const PBAR_ARCHIVE: &str = "Archive documents: {human_pos} ({percent}%) | \
        elapsed: {elapsed_precise}{msg}";

/// Create an archive (tar.gz) of the index, config and all documents.
///
/// By default, the compression is biased towards high compression ratio
/// at expense of speed. To change this setting, use the `--fast` or
/// `--best` flag.
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

    /// Uses the lowest compression at the highest speed.
    #[arg(long, conflicts_with = "best")]
    fast: bool,

    /// Uses the best compression at the lowest speed.
    #[arg(long, conflicts_with = "fast")]
    best: bool,

    /// Write the archive to `filename` instead of stdout.
    #[arg(short, long, value_name = "filename")]
    output: Option<PathBuf>,
}

impl Archive {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        let datashed = Datashed::discover()?;
        let index = datashed.index()?;
        let paths = index.column("path")?.str()?;

        let level = if self.fast {
            Compression::fast()
        } else if self.best {
            Compression::best()
        } else {
            Compression::default()
        };

        let out: Box<dyn Write> = match self.output {
            Some(path) => Box::new(File::create(path)?),
            None => Box::new(stdout().lock()),
        };

        let gzip = GzEncoder::new(out, level);
        let mut archive = tar::Builder::new(gzip);

        let pbar = ProgressBarBuilder::new(PBAR_ARCHIVE, self.quiet)
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
        archive.append_file(Datashed::INDEX, &mut index)?;

        let mut config =
            File::open(datashed.base_dir().join(Datashed::CONFIG))?;
        archive.append_file(Datashed::CONFIG, &mut config)?;

        archive.finish()?;
        Ok(())
    }
}
