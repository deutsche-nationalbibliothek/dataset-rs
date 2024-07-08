use clap::Parser;

use crate::datapod::Datapod;
use crate::error::{DatapodError, DatapodResult};

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

pub(crate) fn execute(args: Version) -> DatapodResult<()> {
    let datapod = Datapod::discover()?;
    let mut config = datapod.config()?;

    if let Some(version) = args.version {
        if !args.force && version <= config.metadata.version {
            return Err(DatapodError::Other(format!(
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
