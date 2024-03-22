use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::error::DatasetError;

/// A remote is a data source, which allows access to documents. Currently
/// only local remotes are supported.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum Remote {
    Local { path: PathBuf, suffix: String },
}

impl Remote {
    /// Creates a new Remote variant based on the given URL scheme.
    pub(crate) fn new<U, S>(url: U, suffix: S) -> Result<Self, DatasetError>
    where
        U: Into<Url>,
        S: Into<String>,
    {
        let url = url.into();
        let scheme = url.scheme();
        let suffix = suffix.into();
        let path = PathBuf::from(url.path());

        if scheme != "file" {
            return Err(DatasetError::Other(format!(
                "unsupported remote scheme '{scheme}'"
            )));
        }

        let ok = if let Ok(metadata) = fs::metadata(&path) {
            metadata.is_dir()
        } else {
            false
        };

        if !ok {
            return Err(DatasetError::Other(format!(
                "path '{path:?}' is not a directory",
            )));
        }

        Ok(Self::Local { path, suffix })
    }
}
