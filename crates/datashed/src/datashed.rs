use std::fs::File;
use std::path::PathBuf;
use std::{env, fs};

use polars::prelude::*;

use crate::config::Config;
use crate::error::{DatashedError, DatashedResult};

pub(crate) struct Datashed {
    /// The root directory of the data pod.
    root_dir: PathBuf,
}

impl Datashed {
    pub(crate) const CONFIG: &'static str = "datashed.toml";
    pub(crate) const INDEX: &'static str = "index.ipc";
    pub(crate) const DATA_DIR: &'static str = "data";

    /// Discovers the root of the data pod.
    ///
    /// This function fails, if neither the current directory nor any
    /// parent directory contains a data pod [Config].
    pub(crate) fn discover() -> DatashedResult<Self> {
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
                return Err(DatashedError::Other(
                    "not a data pod (or any parent directory)".into(),
                ));
            }
        }

        Ok(Self { root_dir })
    }

    /// Returns the config associated with the datashed.
    #[inline]
    pub(crate) fn config(&self) -> DatashedResult<Config> {
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

    /// Returns the index associated with the datashed.
    #[inline]
    pub(crate) fn index(&self) -> DatashedResult<DataFrame> {
        Ok(IpcReader::new(File::open(
            self.base_dir().join(Self::INDEX),
        )?)
        .memory_mapped(None)
        .finish()?)
    }
}
