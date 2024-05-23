use std::path::PathBuf;
use std::{env, fs};

use crate::config::Config;
use crate::error::DatasetError;

pub(crate) struct Dataset {
    /// The root directory of the dataset.
    root_dir: PathBuf,
}

impl Dataset {
    pub(crate) const DOT_DIR: &'static str = ".dataset";
    pub(crate) const DATA_DIR: &'static str = "data";

    /// The file path of an index over documents of all remotes (data
    /// sources).
    pub(crate) const REMOTES_INDEX: &'static str = "remotes.ipc";

    /// Discovers the root of the dataset.
    ///
    /// This function fails, if neither the current directory nor any
    /// parent directory contains a dataset [Config].
    pub(crate) fn discover() -> Result<Self, DatasetError> {
        let mut root_dir = env::current_dir()?;

        loop {
            if let Ok(metadata) =
                fs::metadata(root_dir.join(Config::FILENAME))
            {
                if metadata.is_file() {
                    break;
                }
            }

            if !root_dir.pop() {
                return Err(DatasetError::Other(
                    "not a dataset (or any parent directory)".into(),
                ));
            }
        }

        Ok(Self { root_dir })
    }

    /// Returns the manifest associated with the dataset.
    #[inline]
    pub(crate) fn config(&self) -> Result<Config, DatasetError> {
        Config::from_path(self.root_dir.join(Config::FILENAME))
    }

    /// Returns the app directory of the dataset.
    #[inline]
    pub(crate) fn app_dir(&self) -> PathBuf {
        self.root_dir.join(Self::DOT_DIR)
    }

    /// Returns the temporary directory of the dataset.
    #[inline]
    pub(crate) fn data_dir(&self) -> PathBuf {
        self.app_dir().join(Self::DATA_DIR)
    }
}
