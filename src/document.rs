use std::fmt::{self, Display, Write};
use std::fs::{read_to_string, Metadata};
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::time::UNIX_EPOCH;

use lingua::Language;
use sha2::{Digest, Sha256};

use crate::error::{DatasetError, DatasetResult};
use crate::lang::{lang_to_639_2b, language_detector};
use crate::remote::Remote;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DocumentKind {
    Article,
    Blurb,
    Book,
    Other,
    Title,
    Toc,
    Wp,
}

impl Display for DocumentKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Article => write!(f, "article"),
            Self::Blurb => write!(f, "blurb"),
            Self::Book => write!(f, "book"),
            Self::Other => write!(f, "other"),
            Self::Title => write!(f, "title"),
            Self::Toc => write!(f, "toc"),
            Self::Wp => write!(f, "wp"),
        }
    }
}

impl FromStr for DocumentKind {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "article" => Ok(Self::Article),
            "blurb" | "iht" => Ok(Self::Blurb),
            "book" => Ok(Self::Book),
            "other" | "ft" => Ok(Self::Other),
            "title" => Ok(Self::Title),
            "toc" => Ok(Self::Toc),
            "wp" => Ok(Self::Wp),
            _ => Err(()),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Document {
    path: PathBuf,
    metadata: Metadata,
    content: Option<String>,
    lang: Option<(Language, f64)>,
}

impl Document {
    /// Creates a new document from a path.
    ///
    /// This function fails if either no metadata could be extracted
    /// (file doesn't exists or isn't readable) or the file content
    /// could not be read.
    pub(crate) fn from_path<P>(path: P) -> DatasetResult<Self>
    where
        P: AsRef<Path>,
    {
        let path: PathBuf = path.as_ref().into();
        let metadata = path.metadata()?;
        let content = None;
        let lang = None;

        Ok(Self {
            path,
            metadata,
            content,
            lang,
        })
    }

    /// Returns a reference to the file content.
    #[inline]
    pub(crate) fn content(&mut self) -> DatasetResult<&str> {
        if self.content.is_none() {
            self.content = Some(read_to_string(&self.path)?);
        }

        Ok(self.content.as_ref().unwrap())
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

    /// Returns the kind of the document.
    ///
    /// # Panics
    ///
    /// This function panics if it's not possible to extract a valid
    /// document kind of the path's components.
    pub(crate) fn kind(&self) -> DocumentKind {
        self.path
            .components()
            .filter_map(|c| {
                if let Component::Normal(s) = c {
                    s.to_str()
                } else {
                    None
                }
            })
            .find_map(|s| DocumentKind::from_str(s).ok())
            .unwrap()
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

    /// Returns the string length of the file.
    #[inline]
    pub(crate) fn strlen(&mut self) -> DatasetResult<usize> {
        Ok(self.content()?.chars().count())
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
    pub(crate) fn hash(&mut self, len: usize) -> DatasetResult<String> {
        let mut hasher = Sha256::new();
        hasher.update(self.content()?);

        let hash = hasher.finalize();
        let digest =
            hash.iter().take(len).fold(String::new(), |mut out, b| {
                let _ = write!(out, "{b:02x}");
                out
            });

        Ok(digest)
    }

    /// Returns the ISO 639-2/B language code along with a confidence
    /// value of the document.
    pub(crate) fn lang(
        &mut self,
    ) -> DatasetResult<(&'static str, f64)> {
        if self.lang.is_none() {
            self.lang = language_detector()
                .compute_language_confidence_values(self.content()?)
                .into_iter()
                .next();
        }

        self.lang
            .map(|(code, score)| (lang_to_639_2b(&code), score))
            .ok_or(DatasetError::Other(
                "unable to detect language".into(),
            ))
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
    pub(crate) fn alpha(&mut self) -> DatasetResult<f64> {
        let content = self.content()?;
        let total = content.chars().count() as f64;

        if total > 0.0 {
            let alpha = content
                .chars()
                .filter(|c: &char| c.is_alphabetic())
                .count() as f64;

            Ok(alpha / total)
        } else {
            Ok(0.0)
        }
    }
}
