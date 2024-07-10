use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use bstr::BString;

use crate::error::DatapodResult;

#[derive(Debug, PartialEq)]
pub(crate) struct Document {
    path: PathBuf,
    buf: BString,
}

impl Document {
    pub(crate) fn from_path<P: AsRef<Path>>(
        path: P,
    ) -> DatapodResult<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;
        let mut buf = Vec::new();

        let _ = file.read_to_end(&mut buf)?;

        Ok(Self {
            path,
            buf: BString::from(buf),
        })
    }

    pub(crate) fn idn(&self) -> String {
        self.path.file_stem().unwrap().to_str().unwrap().to_string()
    }

    /// Returns the length of the document in bytes.
    ///
    /// ```rust
    /// use document_stats::Document;
    ///
    /// let doc = Document::new("a ∉ ℕ");
    /// assert_eq!(doc.len(), 9);
    /// ```
    #[inline]
    pub(crate) fn len(&self) -> u64 {
        self.buf.len() as u64
    }
}
