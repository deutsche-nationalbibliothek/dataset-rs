use clap::Parser;
use url::Url;

use crate::prelude::*;

/// Manage set of tracked datasheds (data sources).
#[derive(Debug, Parser)]
pub(crate) struct Remote {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, Parser)]
pub(crate) enum Command {
    /// Add a new remote to the dataset.
    Add {
        /// A where clause to filter documents.
        #[arg(long = "where", short = 'W')]
        query: Option<String>,

        /// The name of the remote.
        name: String,

        /// The URL of the remote.
        url: Url,
    },

    /// Remove the remote named `name`.
    #[clap(visible_alias = "rm")]
    Remove {
        /// The name of the remote.
        name: String,
    },

    /// Changes the URL for the remote `name`.
    SetUrl {
        /// The name of the remote.
        name: String,

        /// The URL of the remote.
        url: Url,
    },

    /// Changes the where clause for the remote `name`.
    SetPredicate {
        /// The name of the remote.
        name: String,

        /// The where clause to filter documents.
        predicate: String,
    },
}

impl Remote {
    pub(crate) fn execute(self) -> DatasetResult<()> {
        use crate::remote::Remote;

        let dataset = Dataset::discover()?;
        let mut config = dataset.config()?;

        match self.cmd {
            Command::Add { query, name, url } => {
                if config.remotes.contains_key(&name) {
                    bail!("remote '{name}' already exist.")
                }

                let remote = Remote::new(url, query)?;
                config.remotes.insert(name, remote);
            }
            Command::Remove { name } => {
                if !config.remotes.contains_key(&name) {
                    bail!("remote '{name}' does not exist.")
                }

                config.remotes.remove(&name);
            }
            Command::SetUrl { name, url } => {
                if let Some(remote) = config.remotes.get_mut(&name) {
                    remote.set_url(url)?;
                } else {
                    bail!("remote '{name}' does not exist.")
                }
            }
            Command::SetPredicate { name, predicate } => {
                if let Some(remote) = config.remotes.get_mut(&name) {
                    remote.set_predicate(predicate);
                } else {
                    bail!("remote '{name}' does not exist.")
                }
            }
        }

        config.save()?;
        Ok(())
    }
}
