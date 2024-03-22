use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::DatasetError;

/// Dataset manifest.
#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct Config {
    /// The path of the config.
    #[serde(skip)]
    path: PathBuf,

    /// Number of threads to use. If this options isn't set or a value
    /// of "0" is chosen, the maximum number of available threads
    /// is used.
    pub(crate) num_jobs: Option<usize>,

    /// This structure should always be constructed using a public
    /// constructor or using the update syntax:
    ///
    /// ```ignore
    /// use crate::config::Config;
    ///
    /// let config = Config {
    ///     num_jobs: Some(23),
    ///     ..Default::default()
    /// };
    /// ```
    #[doc(hidden)]
    #[serde(skip)]
    __non_exhaustive: (),
}

impl Config {
    /// The filename of the manifest.
    pub(crate) const FILENAME: &'static str = "dataset.toml";

    /// Creates a new Manifest from a path.
    pub(crate) fn from_path<P>(path: P) -> Result<Self, DatasetError>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().into();
        let content = fs::read_to_string(&path)?;
        let mut manifest: Self = toml::from_str(&content)?;
        manifest.path = path;

        Ok(manifest)
    }

    /// Saves the manifest.
    pub(crate) fn save(&self) -> Result<(), DatasetError> {
        let content = toml::to_string(self).expect("valid toml");
        let mut out = File::create(&self.path)?;
        out.write_all(content.as_bytes())?;
        Ok(())
    }
}
