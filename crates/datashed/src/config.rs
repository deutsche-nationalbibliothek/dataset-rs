use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use semver::Version;
use serde::{Deserialize, Serialize};

use crate::document::DocumentKind;
use crate::error::DatashedResult;

/// Datashed config.
#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct Config {
    /// The path of the config.
    #[serde(skip)]
    path: PathBuf,

    /// Datashed metadata.
    pub(crate) metadata: Metadata,

    /// Runtime options.
    pub(crate) runtime: Option<Runtime>,

    /// A set of document kind refinements.
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub(crate) kinds: HashMap<DocumentKind, KindSpec>,

    /// This structure should always be constructed using a public
    /// constructor or using the update syntax:
    ///
    /// ```ignore
    /// use crate::config::Config;
    ///
    /// let config = Config {
    ///     ..Default::default()
    /// };
    /// ```
    #[doc(hidden)]
    #[serde(skip)]
    __non_exhaustive: (),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Metadata {
    /// The name of the data pod.
    pub(crate) name: String,

    /// The version of the data pod.
    pub(crate) version: Version,

    /// A short blurb about the data pod.
    pub(crate) description: Option<String>,

    /// A list of people or organizations, which are considered as the
    /// authors of the data pod.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub(crate) authors: Vec<String>,
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            name: "".into(),
            version: Version::new(0, 1, 0),
            description: None,
            authors: vec![],
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct Runtime {
    /// Number of threads to use. If this options isn't set or a value
    /// of "0" is chosen, the maximum number of available threads
    /// is used.
    pub(crate) num_jobs: Option<usize>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Hash)]
pub(crate) struct KindSpec {
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub(crate) refinements: Vec<Refinement>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash)]
pub(crate) struct Refinement {
    pub(crate) target: DocumentKind,
    pub(crate) filter: String,
}

impl Config {
    /// Creates a new default config and sets the file location.
    pub(crate) fn create<P>(path: P) -> DatashedResult<Self>
    where
        P: AsRef<Path>,
    {
        Ok(Self {
            path: path.as_ref().into(),
            ..Default::default()
        })
    }

    /// Loads an existing config from a path.
    pub(crate) fn from_path<P>(path: P) -> DatashedResult<Self>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().into();
        let content = fs::read_to_string(&path)?;
        let mut config: Self = toml::from_str(&content)?;
        config.path = path;

        Ok(config)
    }

    /// Saves the config.
    pub(crate) fn save(&self) -> DatashedResult<()> {
        let content = toml::to_string(self).expect("valid toml");
        let mut out = File::create(&self.path)?;
        out.write_all(content.as_bytes())?;
        Ok(())
    }
}
