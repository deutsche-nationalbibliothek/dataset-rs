use std::fmt::Write;
use std::fs::{File, Metadata};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use bstr::BString;
use sha2::{Digest, Sha256};

use crate::error::DatashedResult;

#[derive(Debug)]
pub(crate) struct Document {
    path: PathBuf,
    metadata: Metadata,
    buf: BString,
}

impl Document {
    pub(crate) fn from_path<P: AsRef<Path>>(
        path: P,
    ) -> DatashedResult<Self> {
        let path = path.as_ref().to_path_buf();
        let metadata = path.metadata()?;
        let mut file = File::open(&path)?;
        let mut buf = Vec::new();

        let _ = file.read_to_end(&mut buf)?;

        Ok(Self {
            path,
            metadata,
            buf: BString::from(buf),
        })
    }

    pub(crate) fn idn(&self) -> String {
        self.path.file_stem().unwrap().to_str().unwrap().to_string()
    }

    /// Returns the length of the document in bytes.
    #[inline]
    pub(crate) fn size(&self) -> u64 {
        self.buf.len() as u64
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
    pub(crate) fn hash(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(&self.buf);

        let hash = hasher.finalize();
        hash.iter().fold(String::new(), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
    }
}
