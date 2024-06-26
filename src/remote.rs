use std::fs;
use std::path::{Path, PathBuf};

use glob::{glob_with, MatchOptions};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::document::{Document, DocumentKind};
use crate::error::DatasetError;

/// A remote is a data source, which allows access to documents.
/// Currently only local remotes are supported.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum Remote {
    Local {
        path: PathBuf,
        suffix: String,
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        refinements: Vec<Refinement>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Refinement {
    pub(crate) from: DocumentKind,
    pub(crate) to: DocumentKind,
    pub(crate) filter: String,
}

impl Remote {
    /// Creates a new Remote variant based on the given URL scheme.
    pub(crate) fn new<U, S>(
        url: U,
        suffix: S,
        refinements: Vec<Refinement>,
    ) -> Result<Self, DatasetError>
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

        Ok(Self::Local {
            path,
            suffix,
            refinements,
        })
    }

    /// Returns the list of kind refinements
    pub(crate) fn refinements(&self) -> &Vec<Refinement> {
        match self {
            Self::Local { refinements, .. } => refinements,
        }
    }

    /// Returns a document relative to the base path of a remote.
    pub(crate) fn document<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<Document, DatasetError> {
        match self {
            Self::Local { path: base, .. } => {
                Document::from_path(base.join(path))
            }
        }
    }

    /// Returns an iterator over all documents managned by this remote.
    pub(crate) fn documents(
        &self,
    ) -> Box<dyn Iterator<Item = Result<Document, DatasetError>>> {
        match self {
            Self::Local { path, suffix, .. } => {
                let path_str = path.to_str().unwrap();
                let pattern = format!("{path_str}/**/*{suffix}");
                let options = MatchOptions::default();
                let iter = glob_with(&pattern, options)
                    .expect("glob")
                    .filter_map(Result::ok)
                    .map(Document::from_path);

                Box::new(iter)
            }
        }
    }
}
