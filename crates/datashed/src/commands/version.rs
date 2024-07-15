use clap::Parser;

use crate::prelude::*;

/// Get or set the version of the datashed.
#[derive(Debug, Parser)]
pub(crate) struct Version {
    /// Whether to overwrite the current version or not.
    #[arg(short, long)]
    force: bool,

    /// The new version of the datashed. Unless the `--force`/`-f`
    /// option is set, the new version must be greater than the
    /// current version. A datashed version consists of three
    /// separated integers, which must conform to the semantic
    /// versioning standard; invalid version strings are rejected.
    version: Option<semver::Version>,
}

impl Version {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        let datashed = Datashed::discover()?;
        let mut config = datashed.config()?;

        if let Some(version) = self.version {
            if !self.force && version <= config.metadata.version {
                let current = config.metadata.version.to_string();
                bail!("{version} must be greater than {current}");
            }

            config.metadata.version = version;
            config.save()?;
        } else {
            println!("{}", config.metadata.version);
        }

        Ok(())
    }
}
