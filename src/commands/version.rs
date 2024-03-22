use clap::Parser;

use crate::dataset::Dataset;
use crate::error::DatasetError;

/// Get or set the version of the dataset.
#[derive(Debug, Parser)]
pub(crate) struct Version {
    /// Whether to overwrite the current version or not.
    #[arg(short, long)]
    force: bool,

    /// The new version of the dataset. Unless the `--force` option is
    /// set, the new version must be greater than the current version.
    version: Option<semver::Version>,
}

pub(crate) fn execute(args: Version) -> Result<(), DatasetError> {
    let dataset = Dataset::discover()?;
    let mut config = dataset.config()?;

    if let Some(version) = args.version {
        if !args.force && version <= config.metadata.version {
            return Err(DatasetError::Other(format!(
                "{} must be greater than {}",
                version, config.metadata.version
            )));
        }

        config.metadata.version = version;
        config.save()?;
    } else {
        println!("{}", config.metadata.version);
    }

    Ok(())
}
