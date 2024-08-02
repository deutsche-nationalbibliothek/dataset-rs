use crate::config;
use crate::prelude::*;

/// Manage users of the datashed.
#[derive(Debug, clap::Parser)]
pub(crate) struct User {
    /// Run verbosely. Print additional progress information to the
    /// standard error stream. This option conflicts with the
    /// `--quiet` option.
    #[arg(short, long, conflicts_with = "quiet")]
    verbose: bool,

    /// Operate quietly; do not show progress. This option conflicts
    /// with the `--verbose` option.
    #[arg(short, long, conflicts_with = "verbose")]
    quiet: bool,

    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Parser)]
pub(crate) enum Command {
    /// Add a new user to the datashed.
    Add { username: String, secret: String },

    /// Remove the user \<username\> from the datashed.
    #[clap(visible_alias = "rm")]
    Remove { username: String },

    /// Set a new secret for the user \<username\>.
    SetSecret { username: String, secret: String },
}

impl User {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        let datashed = Datashed::discover()?;
        let mut config = datashed.config()?;

        match self.cmd {
            Command::Add { username, secret } => {
                if config.users.contains_key(&username) {
                    bail!("user '{}' already exist.", username);
                }

                config.users.insert(username, config::User { secret });
            }
            Command::Remove { username } => {
                if !config.users.contains_key(&username) {
                    bail!("user '{}' does not exist.", username);
                }

                config.users.remove(&username);
            }
            Command::SetSecret { username, secret } => {
                let Some(user) = config.users.get_mut(&username) else {
                    bail!("user '{}' does not exist.", username);
                };

                *user = config::User { secret };
            }
        }

        config.save()?;
        Ok(())
    }
}
