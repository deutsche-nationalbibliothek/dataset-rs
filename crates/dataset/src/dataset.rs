use std::path::PathBuf;
use std::{env, fs};

use crate::config::Config;
use crate::prelude::*;

pub(crate) struct Dataset {
    /// The root directory of the dataset.
    root_dir: PathBuf,
}

impl Dataset {
    pub(crate) const CONFIG: &'static str = "dataset.toml";
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
                fs::metadata(root_dir.join(Self::CONFIG))
            {
                if metadata.is_file() {
                    break;
                }
            }

            if !root_dir.pop() {
                bail!("not a dataset (or any parent directory)");
            }
        }

        Ok(Self { root_dir })
    }

    /// Returns the config associated with the datashed.
    #[inline]
    pub(crate) fn config(&self) -> DatasetResult<Config> {
        Config::from_path(self.root_dir.join(Self::CONFIG))
    }

    /// Returns the base directory of the datashed.
    #[inline]
    pub(crate) fn base_dir(&self) -> &PathBuf {
        &self.root_dir
    }

    /// Returns the data directory of the datashed.
    #[inline]
    pub(crate) fn data_dir(&self) -> PathBuf {
        self.root_dir.join(Self::DATA_DIR)
    }

    /// Returns the temp directory of the datashed.
    #[inline]
    pub(crate) fn temp_dir(&self) -> PathBuf {
        self.root_dir.join(Self::TMP_DIR)
    }
}
