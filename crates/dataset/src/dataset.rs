use std::env;
use std::fs::{self, File};
use std::path::PathBuf;

use polars::prelude::*;

use crate::config::Config;
use crate::prelude::*;

pub(crate) struct Dataset {
    /// The root directory of the dataset.
    root_dir: PathBuf,
}

impl Dataset {
    pub(crate) const CONFIG: &'static str = "config.toml";
    pub(crate) const REMOTES: &'static str = "remotes.ipc";
    pub(crate) const VOCAB: &'static str = "vocab.csv";

    pub(crate) const DOT_DIR: &'static str = ".dataset";
    pub(crate) const DATA_DIR: &'static str = "data";
    pub(crate) const TMP_DIR: &'static str = "tmp";

    /// Discovers the root of the dataset.
    ///
    /// This function fails, if neither the current directory nor any
    /// parent directory contains a dataset [Config].
    pub(crate) fn discover() -> DatasetResult<Self> {
        let mut root_dir = env::current_dir()?;

        loop {
            if let Ok(metadata) =
                fs::metadata(root_dir.join(Self::DOT_DIR))
            {
                if metadata.is_dir() {
                    break;
                }
            }

            if !root_dir.pop() {
                bail!("not a dataset (or any parent directory)");
            }
        }

        Ok(Self { root_dir })
    }

    /// Returns the config associated with the dataset.
    #[inline]
    pub(crate) fn config(&self) -> DatasetResult<Config> {
        Config::from_path(self.dot_dir().join(Self::CONFIG))
    }

    /// Returns the base directory of the dataset.
    #[inline]
    pub(crate) fn base_dir(&self) -> &PathBuf {
        &self.root_dir
    }

    /// Returns the dot directory of the dataset.
    #[inline]
    pub(crate) fn dot_dir(&self) -> PathBuf {
        self.root_dir.join(Self::DOT_DIR)
    }

    /// Returns the data directory of the dataset.
    #[inline]
    pub(crate) fn data_dir(&self) -> PathBuf {
        self.root_dir.join(Self::DATA_DIR)
    }

    /// Returns the tmp directory of the dataset.
    #[inline]
    pub(crate) fn tmp_dir(&self) -> PathBuf {
        self.dot_dir().join(Self::TMP_DIR)
    }

    /// Returns the remote index.
    #[inline]
    pub(crate) fn remotes(&self) -> DatasetResult<DataFrame> {
        Ok(IpcReader::new(File::open(
            self.dot_dir().join(Self::REMOTES),
        )?)
        .memory_mapped(None)
        .finish()?)
    }
}
