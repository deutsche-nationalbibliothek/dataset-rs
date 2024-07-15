use std::fs::{create_dir, File};
use std::path::PathBuf;

use clap::Parser;
use flate2::read::GzDecoder;
use tar::Archive;

use crate::prelude::*;

/// Restore a datashed archive (tar.gz).
#[derive(Debug, Default, Parser)]
pub(crate) struct Restore {
    /// Run verbosely. Print additional progress information to the
    /// standard error stream. This option conflicts with the
    /// `--quiet` option.
    #[arg(short, long, conflicts_with = "quiet")]
    verbose: bool,

    /// Operate quietly; do not show progress. This option conflicts
    /// with the `--verbose` option.
    #[arg(short, long, conflicts_with = "verbose")]
    quiet: bool,

    /// The destination directory.
    #[arg(short = 'C', long = "directory", default_value = ".")]
    dest: PathBuf,

    /// The datashed archive to be restored.
    archive: PathBuf,
}

impl Restore {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        if !self.dest.is_dir() {
            create_dir(&self.dest)?;

            if self.verbose {
                eprintln!(
                    "created destination directory '{}'.",
                    self.dest.display()
                )
            }
        }

        let reader = GzDecoder::new(File::open(self.archive)?);
        let mut archive = Archive::new(reader);
        archive.unpack(&self.dest)?;

        if !self.dest.join(Datashed::DATA_DIR).is_dir() {
            bail!("corrupt archive: missing data dir!");
        }

        if !self.dest.join(Datashed::INDEX).is_file() {
            bail!("corrupt archive: missing index!");
        }

        if !self.dest.join(Datashed::CONFIG).is_file() {
            bail!("corrupt archive: missing config!");
        }

        if !self.quiet {
            eprintln!(
                "Successfully restored archive. \
                Verify consistency with `datashed verify`."
            )
        }

        Ok(())
    }
}
