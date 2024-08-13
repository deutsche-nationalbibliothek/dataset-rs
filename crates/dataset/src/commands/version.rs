use clap::{Parser, ValueEnum};
use semver::Version as SemVer;

use crate::prelude::*;

#[derive(Debug, Clone, ValueEnum)]
enum Bump {
    Major,
    Minor,
    Patch,
}

/// Get or set the version of the dataset.
#[derive(Debug, Parser)]
pub(crate) struct Version {
    /// Whether to overwrite the current version or not.
    #[arg(short, long)]
    force: bool,

    /// Run verbosely. Print additional progress information to the
    /// standard error stream. This option conflicts with the
    /// `--quiet` option.
    #[arg(short, long, conflicts_with = "quiet")]
    verbose: bool,

    /// Operate quietly; do not show progress. This option conflicts
    /// with the `--verbose` option.
    #[arg(short, long, conflicts_with = "verbose")]
    quiet: bool,

    #[arg(short, long, conflicts_with = "version")]
    bump: Option<Bump>,

    /// The new version of the dataset. Unless the `--force`/`-f`
    /// option is set, the new version must be greater than the
    /// current version. A dataset version consists of three
    /// separated integers, which must conform to the semantic
    /// versioning standard; invalid version strings are rejected.
    #[arg(conflicts_with = "bump")]
    version: Option<SemVer>,
}

impl Version {
    pub(crate) fn execute(self) -> DatasetResult<()> {
        let dataset = Dataset::discover()?;
        let mut config = dataset.config()?;

        if let Some(version) = self.version {
            if !self.force && version <= config.metadata.version {
                let current = config.metadata.version.to_string();
                bail!("{version} must be greater than {current}");
            }

            config.metadata.version = version;
            config.save()?;
        } else if let Some(bump) = self.bump {
            let major = config.metadata.version.major;
            let minor = config.metadata.version.minor;
            let patch = config.metadata.version.patch;

            let version = match bump {
                Bump::Patch => SemVer::new(major, minor, patch + 1),
                Bump::Minor => SemVer::new(major, minor + 1, 0),
                Bump::Major => SemVer::new(major + 1, 0, 0),
            };

            if self.verbose {
                println!("bumped version to {version}");
            }

            config.metadata.version = version;
            config.save()?;
        } else {
            println!("{}", config.metadata.version);
        }

        Ok(())
    }
}
