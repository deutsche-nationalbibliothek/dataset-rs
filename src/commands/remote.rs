use clap::Parser;
use url::Url;

use crate::dataset::Dataset;
use crate::error::DatasetError;

/// Manage set of tracked data sources (remotes).
#[derive(Debug, Parser)]
pub(crate) struct Remote {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, Parser)]
pub(crate) enum Command {
    /// Add a new remote to the dataset.
    Add {
        /// The suffix of the documents.
        #[arg(short, long, default_value = ".txt")]
        suffix: String,

        /// The name of the remote.
        name: String,

        /// The URL of the remote.
        url: Url,
    },

    /// Remove the remote named <name>.
    #[clap(visible_alias = "rm")]
    Remove {
        /// The name of the remote.
        name: String,
    },

    /// Changes the URL for the remote <name>.
    SetUrl {
        /// The name of the remote.
        name: String,

        /// The URL of the remote.
        url: Url,
    },

    /// Change the file suffix for the remote <name>.
    SetSuffix {
        /// The name of the remote.
        name: String,

        /// The suffix of the documents.
        suffix: String,
    },
}

pub(crate) fn execute(args: Remote) -> Result<(), DatasetError> {
    use crate::remote::Remote;

    let dataset = Dataset::discover()?;
    let mut config = dataset.config()?;

    match args.cmd {
        Command::Add { name, suffix, url } => {
            if config.remotes.contains_key(&name) {
                return Err(DatasetError::Other(format!(
                    "remote with name '{name}' already exists"
                )));
            }

            let remote = Remote::new(url, suffix)?;
            config.remotes.insert(name, remote);
        }

        Command::Remove { name } => {
            if !config.remotes.contains_key(&name) {
                return Err(DatasetError::Other(format!(
                    "remote with name '{name}' does not exists.",
                )));
            }

            config.remotes.remove(&name);
        }

        Command::SetUrl { name, url } => {
            if let Some(remote) = config.remotes.get_mut(&name) {
                match remote {
                    Remote::Local { suffix, .. } => {
                        *remote = Remote::new(url, suffix.to_string())?;
                    }
                }
            } else {
                return Err(DatasetError::Other(format!(
                    "remote with name '{name}' does not exists.",
                )));
            }
        }

        Command::SetSuffix { name, suffix } => {
            if let Some(remote) = config.remotes.get_mut(&name) {
                match remote {
                    Remote::Local { path, .. } => {
                        *remote = Remote::Local {
                            path: path.to_path_buf(),
                            suffix,
                        }
                    }
                }
            } else {
                return Err(DatasetError::Other(format!(
                    "remote with name '{name}' does not exists.",
                )));
            }
        }
    }

    config.save()?;
    Ok(())
}
