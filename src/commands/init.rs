use std::ffi::OsStr;
use std::path::PathBuf;
use std::process::Stdio;
use std::{env, fs, process};

use clap::{Parser, ValueEnum};
use semver::Version;

use crate::config::Config;
use crate::error::DatasetError;

const DVCIGNORE: &[u8] = b"/.dataset/tmp\n";
const GITGINORE: &[u8] = b"/.dataset\n";

/// Initialize a new or re-initialize an existing dataset.
#[derive(Debug, Parser)]
pub(crate) struct Init {
    /// The name of the dataset.
    #[arg(long)]
    name: Option<String>,

    /// The version of the dataset.
    #[arg(long, default_value = "0.1.0")]
    version: Version,

    /// A short blurb about the dataset.
    #[arg(long)]
    description: Option<String>,

    /// A list of people or organizations, which are considered as the
    /// authors of the dataset.
    #[arg(long = "author")]
    authors: Vec<String>,

    /// Initialize the dataset for the given version control system
    /// (VCS).
    #[arg(long, default_value = "git")]
    vcs: Vcs,

    /// If set, initialize the dataset as an DVC project.
    #[arg(long)]
    dvc: bool,

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

    /// The location of the dataset
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

pub(crate) fn execute(args: Init) -> Result<(), DatasetError> {
    let root_dir = env::current_dir()?.join(args.path);
    let config = root_dir.join(Config::FILENAME);

    if !root_dir.exists() {
        fs::create_dir_all(&root_dir)?;

        if args.verbose {
            eprintln!("Initialize new dataset in {root_dir:?}");
        }
    } else if args.verbose {
        eprintln!("Re-Initialize exiting dataset in {root_dir:?}");
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

    if args.vcs == Vcs::Git {
        if !is_inside_git_work_tree(&root_dir) && !git_init(&root_dir) {
            return Err(DatasetError::Other(
                "Failed to initialize Git repository".into(),
            ));
        }

        fs::write(root_dir.join(".gitignore"), GITGINORE)?;
    }

    if args.dvc {
        fs::write(root_dir.join(".dvcignore"), DVCIGNORE)?;
    }

    Ok(())
}
