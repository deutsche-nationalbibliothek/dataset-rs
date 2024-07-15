use std::ffi::OsStr;
use std::path::PathBuf;
use std::process::Stdio;
use std::{env, fs, process};

use clap::{Parser, ValueEnum};
use semver::Version;

use crate::prelude::*;

const GITIGNORE: &str = "# datashed\n/data\n/index.ipc\n";

/// Initialize a new or re-initialize an existing datashed.
#[derive(Debug, Parser)]
pub(crate) struct Init {
    /// The name of the data pod.
    #[arg(short, long)]
    name: Option<String>,

    /// The version of the data pod.
    #[arg(long, default_value = "0.1.0")]
    version: Version,

    /// A short blurb about the data pod.
    #[arg(short, long)]
    description: Option<String>,

    /// A list of people or organizations, which are considered as the
    /// authors of the datashed. By default the list is populated with
    /// the git identity (if available).
    #[arg(short, long = "author")]
    authors: Vec<String>,

    /// Initialize the data pod for the given version control system
    /// (VCS).
    #[arg(long, default_value = "git")]
    vcs: Vcs,

    /// Whether to overwrite config with default values or not.
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

    /// The location of the data pod.
    #[arg(default_value = ".")]
    path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, ValueEnum)]
enum Vcs {
    Git,
    None,
}

#[inline]
fn is_inside_git_work_tree(path: &PathBuf) -> bool {
    process::Command::new("git")
        .arg("rev-parse")
        .arg("--is-inside-work-tree")
        .current_dir(path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

#[inline]
fn git_init(path: &PathBuf) -> bool {
    process::Command::new("git")
        .arg("init")
        .current_dir(path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn git_user(path: &PathBuf) -> Option<String> {
    let mut user = String::new();

    let result = process::Command::new("git")
        .arg("config")
        .arg("--get")
        .arg("user.name")
        .current_dir(path)
        .stdout(Stdio::piped())
        .output();

    if let Ok(output) = result {
        if let Ok(name) = std::str::from_utf8(&output.stdout) {
            user.push_str(name.trim_end());
        }
    }

    if user.is_empty() {
        return None;
    }

    let result = process::Command::new("git")
        .arg("config")
        .arg("--get")
        .arg("user.email")
        .current_dir(path)
        .stdout(Stdio::piped())
        .output();

    if let Ok(output) = result {
        if let Ok(email) = std::str::from_utf8(&output.stdout) {
            user.push_str(&format!(" <{}>", email.trim_end()));
        }
    }

    Some(user)
}

impl Init {
    pub(crate) fn execute(mut self) -> DatashedResult<()> {
        let root_dir = env::current_dir()?.join(self.path);
        let data_dir = root_dir.join(Datashed::DATA_DIR);
        let config = root_dir.join(Datashed::CONFIG);

        if !root_dir.exists() {
            fs::create_dir_all(&root_dir)?;

            if self.verbose {
                eprintln!(
                    "Initialize new data pod in {}",
                    root_dir.display()
                );
            }
        } else if self.verbose {
            eprintln!(
                "Re-Initialize exiting data pod in {}",
                root_dir.display()
            );
        }

        if !data_dir.exists() {
            fs::create_dir_all(&data_dir)?;
        }

        if self.vcs == Vcs::Git {
            if !is_inside_git_work_tree(&root_dir)
                && !git_init(&root_dir)
            {
                bail!("Failed to initialize Git repository");
            }

            if !root_dir.join(".gitignore").is_file() {
                fs::write(root_dir.join(".gitignore"), GITIGNORE)?;
            }
        }

        if !config.exists() || self.force {
            if self.authors.is_empty() {
                if let Some(author) = git_user(&root_dir) {
                    if self.verbose {
                        eprintln!(
                            "Set authors to Git identity '{author}'."
                        );
                    }

                    self.authors.push(author)
                }
            }

            let mut config = Config::create(config)?;
            config.metadata.description = self.description;
            config.metadata.authors = self.authors;
            config.metadata.version = self.version;
            config.metadata.name = self.name.unwrap_or(
                root_dir
                    .file_name()
                    .and_then(OsStr::to_str)
                    .unwrap_or_default()
                    .to_string(),
            );

            config.save()?;
        }

        Ok(())
    }
}
