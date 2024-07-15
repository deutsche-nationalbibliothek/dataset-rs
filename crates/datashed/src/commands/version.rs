use clap::Parser;

use crate::prelude::*;

/// Get or set the version of the data pod.
#[derive(Debug, Parser)]
pub(crate) struct Version {
    /// Whether to overwrite the current version or not.
    #[arg(short, long)]
    force: bool,

    /// The new version of the data pod. Unless the `--force` option is
    /// set, the new version must be greater than the current version.
    version: Option<semver::Version>,
}

impl Version {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        let datashed = Datashed::discover()?;
        let mut config = datashed.config()?;

        if let Some(version) = self.version {
            if !self.force && version <= config.metadata.version {
                bail!(
                    "{} must be greater than {}",
                    version,
                    config.metadata.version
                );
            }

            config.metadata.version = version;
            config.save()?;
        } else {
            println!("{}", config.metadata.version);
        }

        Ok(())
    }
}
