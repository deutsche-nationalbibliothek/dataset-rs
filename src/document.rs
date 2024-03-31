use std::fmt::Write;
use std::fs::{read_to_string, Metadata};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use sha2::{Digest, Sha256};

use crate::error::DatasetError;
use crate::remote::Remote;

#[derive(Debug)]
pub(crate) struct Document {
    path: PathBuf,
    metadata: Metadata,
    content: String,
}

impl Document {
    /// Creates a new document from a path.
    ///
    /// This function fails if either no metadata could be extracted
    /// (file doesn't exists or isn't readable) or the file content
    /// could not be read.
    pub(crate) fn from_path<P>(path: P) -> Result<Self, DatasetError>
    where
        P: AsRef<Path>,
    {
        let path: PathBuf = path.as_ref().into();
        let content = read_to_string(&path)?;
        let metadata = path.metadata()?;

        Ok(Self {
            path,
            metadata,
            content,
        })
    }

    /// Returns the identifier of the document.
    ///
    /// The identifier is just the file stem (filename without
    /// extension) of the document.
    ///
    /// # Panics
    ///
    /// This function panics if either the file name can't be extracted
    /// or the id cant be converted to a string.
    pub(crate) fn idn(&self) -> String {
        self.path.file_stem().unwrap().to_str().unwrap().to_string()
    }

    /// Returns the location of the document relative to the base path
    /// of the remote.
    ///
    /// # Panics
    ///
    /// This function panics if either the remote's prefix can't be
    /// stripped from file path or the relpath cant't be converted.
    /// This should never happened, because the document is always
    /// located in a subdirectory of the remote's base path.
    pub(crate) fn relpath(&self, remote: &Remote) -> String {
        match remote {
            Remote::Local { path, .. } => self
                .path
                .strip_prefix(path)
                .expect("valid prefix")
                .to_str()
                .expect("valid path")
                .to_string(),
        }
    }

    /// Returns the size of the file, in bytes.
    #[inline]
    pub(crate) fn size(&self) -> u64 {
        self.metadata.len()
    }

    /// Returns the last modification time of the document.
    ///
    /// # Panics
    ///
    /// This function panics, if the platform doesn't support the mtime
    /// field.
    pub(crate) fn modified(&self) -> u64 {
        self.metadata
            .modified()
            .ok()
            .and_then(|x| x.duration_since(UNIX_EPOCH).ok())
            .map(|x| x.as_secs())
            .expect("valid mtime")
    }

    /// Returns the SHA256 digest of the document.
    ///
    /// Use the `len` parameter to shorten the digest to the specified
    /// length.
    pub(crate) fn hash(&self, len: usize) -> String {
        let mut hasher = Sha256::new();
        hasher.update(&self.content);

        let hash = hasher.finalize();
        let digest =
            hash.iter().take(len).fold(String::new(), |mut out, b| {
                let _ = write!(out, "{b:02x}");
                out
            });

        digest
    }
}
