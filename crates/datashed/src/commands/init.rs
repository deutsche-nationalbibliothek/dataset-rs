use std::ffi::OsStr;
use std::path::PathBuf;
use std::process::Stdio;
use std::{env, fs, process};

use clap::{Parser, ValueEnum};
use semver::Version;

use crate::config::Config;
use crate::datashed::Datashed;
use crate::error::{DatashedError, DatashedResult};

const GITIGNORE: &str = "# Datashed\n/data\n/index.ipc\n";
const DATA_DIR: &str = "data";

/// Initialize a new or re-initialize an existing data pod.
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
    /// authors of the data pod.
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

pub(crate) fn execute(args: Init) -> DatashedResult<()> {
    let root_dir = env::current_dir()?.join(args.path);
    let data_dir = root_dir.join(Datashed::DATA_DIR);
    let config = root_dir.join(Datashed::CONFIG);

    if !root_dir.exists() {
        fs::create_dir_all(&root_dir)?;

        if args.verbose {
            eprintln!("Initialize new data pod in {root_dir:?}");
        }
    } else if args.verbose {
        eprintln!("Re-Initialize exiting data pod in {root_dir:?}");
    }

    if !data_dir.exists() {
        fs::create_dir_all(&data_dir)?;
    }

    if args.vcs == Vcs::Git {
        if !is_inside_git_work_tree(&root_dir) && !git_init(&root_dir) {
            return Err(DatashedError::Other(
                "Failed to initialize Git repository".into(),
            ));
        }

        if !root_dir.join(".gitignore").is_file() {
            fs::write(root_dir.join(".gitignore"), GITIGNORE)?;
        }
    }

    if !config.exists() || args.force {
        let mut config = Config::create(config)?;
        config.metadata.description = args.description;
        config.metadata.authors = args.authors;
        config.metadata.version = args.version;
        config.metadata.name = args.name.unwrap_or(
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
