use std::collections::HashSet;
use std::fmt::Write;
use std::fs::{File, Metadata};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use bstr::{BString, ByteSlice};
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

    /// Returns the number of characters in the document
    #[inline]
    pub(crate) fn strlen(&self) -> u64 {
        self.buf.chars().count() as u64
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

    /// Returns the ratio of alphabetic characters to the total number
    /// of characters in the document.
    ///
    /// ## Description
    ///
    /// The `alpha` score of a document is the ratio of alphabetic
    /// characters to the total number of characters. An alphabetic
    /// character is a character which satisfy the _Alphabetic_ property
    /// of the [Unicode Standard] described in Chapter 4 (Character
    /// Properties). The score is defined as
    ///
    /// $$
    /// alpha \triangleq \frac{1}{N}\sum_{i = 1}^{N} \mathbf{1}_A(c_i)
    /// $$
    ///
    /// where $N$ is total number of characters of the document, $c_i$
    /// is the i-th character of the document, $A$ is the subset of all
    /// characters, which satisfy the _Alphabetic_ property and
    /// $\mathbf{1}_A$ is the indicator function, which returns 1 if
    /// the i-th character is alphabetic and otherwise 0.
    ///
    /// ## Note
    ///
    /// The range of the function is $[0, 1]$ and the score of an empty
    /// document is defined to $0.0$.
    ///
    /// [Unicode Standard]: https://www.unicode.org/versions/latest/
    pub(crate) fn alpha(&self) -> f64 {
        let total = self.buf.chars().count() as f64;
        if total <= 0.0 {
            return 0.0;
        }

        let alpha = self
            .buf
            .chars()
            .filter(|c: &char| c.is_alphabetic())
            .count() as f64;

        alpha / total
    }

    /// Returns the type-token ratio (TTR) of the document.
    ///
    /// The TTR is the ratio of unique words (types) to the total number
    /// of words (tokens).
    ///
    /// ## Note
    ///
    /// The range of the function is $[0, 1]$ and the score of an empty
    /// document is defined to $0.0$.
    pub(crate) fn type_token_ratio(&self) -> f64 {
        let total = self.buf.words().count() as f64;
        if total == 0.0 {
            return 0.0;
        }

        let iter = self.buf.words().map(str::to_lowercase);
        let words = HashSet::<String>::from_iter(iter);
        let unique = words.len() as f64;

        unique / total
    }
}
