use std::path::PathBuf;
use std::{env, fs};

use crate::config::Config;
use crate::error::{DatapodError, DatapodResult};

pub(crate) struct Datapod {
    /// The root directory of the data pod.
    root_dir: PathBuf,
}

impl Datapod {
    pub(crate) const CONFIG: &'static str = "datapod.toml";
    pub(crate) const DATA_DIR: &'static str = "data";

    /// Discovers the root of the data pod.
    ///
    /// This function fails, if neither the current directory nor any
    /// parent directory contains a data pod [Config].
    pub(crate) fn discover() -> DatapodResult<Self> {
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
                return Err(DatapodError::Other(
                    "not a data pod (or any parent directory)".into(),
                ));
            }
        }

        Ok(Self { root_dir })
    }

    /// Returns the manifest associated with the data pod.
    #[inline]
    pub(crate) fn config(&self) -> DatapodResult<Config> {
        Config::from_path(self.root_dir.join(Self::CONFIG))
    }
}
