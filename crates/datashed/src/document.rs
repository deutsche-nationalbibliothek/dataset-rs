use std::collections::HashSet;
use std::fmt::{self, Display, Write};
use std::fs::{File, Metadata};
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::sync::OnceLock;
use std::time::UNIX_EPOCH;

use bstr::{BString, ByteSlice};
use lingua::{Language, LanguageDetector, LanguageDetectorBuilder};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::DatashedResult;
use crate::lfreq::{lfreq_eng, lfreq_ger};
use crate::prelude::{bail, DatashedError};

fn language_detector() -> &'static LanguageDetector {
    static DETECTOR: OnceLock<LanguageDetector> = OnceLock::new();
    DETECTOR.get_or_init(|| {
        LanguageDetectorBuilder::from_all_languages().build()
    })
}

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Hash,
    Clone,
    PartialOrd,
    Ord,
)]
#[serde(rename_all = "lowercase")]
pub(crate) enum DocumentKind {
    Article,
    Blurb,
    Book,
    #[default]
    Other,
    Toc,
}

impl Display for DocumentKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Article => write!(f, "article"),
            Self::Blurb => write!(f, "blurb"),
            Self::Book => write!(f, "book"),
            Self::Other => write!(f, "other"),
            Self::Toc => write!(f, "toc"),
        }
    }
}

impl FromStr for DocumentKind {
    type Err = DatashedError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "article" => Ok(Self::Article),
            "blurb" => Ok(Self::Blurb),
            "book" => Ok(Self::Book),
            "other" | "ft" => Ok(Self::Other),
            "toc" => Ok(Self::Toc),
            _ => bail!("invalid document kind '{s}'"),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Document {
    path: PathBuf,
    metadata: Metadata,
    buf: BString,
    _lang: Option<(Language, f64)>,
}

impl AsRef<[u8]> for Document {
    fn as_ref(&self) -> &[u8] {
        self.buf.as_ref()
    }
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
            _lang: None,
        })
    }

    pub(crate) fn idn(&self) -> String {
        self.path.file_stem().unwrap().to_str().unwrap().to_string()
    }

    /// Returns the kind of the document.
    ///
    /// # Note
    ///
    /// If the kind can be derived by multiple path components, the
    /// function chooses the broadest.
    pub(crate) fn kind(&self) -> DocumentKind {
        self.path
            .components()
            .filter_map(|component| {
                if let Component::Normal(s) = component {
                    s.to_str()
                } else {
                    None
                }
            })
            .find_map(|s| DocumentKind::from_str(s).ok())
            .unwrap_or_default()
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

    /// Returns the most probable language and its confidence value.
    ///
    /// # Note
    ///
    /// If the language detection fails, the function returns `None`.
    pub(crate) fn lang(&mut self) -> Option<(String, f64)> {
        if self._lang.is_none() {
            let content = self.buf.to_string();
            self._lang = language_detector()
                .compute_language_confidence_values(content)
                .into_iter()
                .next();
        }

        if let Some((code, score)) = self._lang {
            let code = match code {
                Language::Albanian => "alb".to_string(),
                Language::Armenian => "arm".to_string(),
                Language::Basque => "baq".to_string(),
                Language::Chinese => "chi".to_string(),
                Language::Czech => "cze".to_string(),
                Language::Dutch => "dut".to_string(),
                Language::French => "fre".to_string(),
                Language::Georgian => "geo".to_string(),
                Language::German => "ger".to_string(),
                Language::Greek => "gre".to_string(),
                Language::Macedonian => "mac".to_string(),
                Language::Malay => "may".to_string(),
                Language::Maori => "mao".to_string(),
                Language::Persian => "per".to_string(),
                Language::Romanian => "rum".to_string(),
                Language::Slovak => "slo".to_string(),
                Language::Welsh => "wel".to_string(),
                lang => lang.iso_code_639_3().to_string(),
            };

            Some((code, score))
        } else {
            None
        }
    }

    /// Returns the letter frequency of the document.
    ///
    /// The letter frequency is computed against reference values.
    pub(crate) fn lfreq(&mut self) -> Option<f64> {
        if let Some((lang, _)) = self.lang() {
            match lang.as_str() {
                "ger" => lfreq_ger(&self.buf),
                "eng" => lfreq_eng(&self.buf),
                _ => None,
            }
        } else {
            None
        }
    }

    /// Returns the total number of words
    #[inline]
    pub(crate) fn word_count(&self) -> u64 {
        self.buf.words().count() as u64
    }

    /// Returns the average word length of the document.
    #[inline]
    pub(crate) fn avg_word_len(&self) -> f32 {
        let total = self.word_count() as f32;
        let word_lens =
            self.buf.words().map(|word| word.len() as f32).sum::<f32>();

        if total > 0.0 {
            word_lens / total
        } else {
            0.0
        }
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
        let total = self.word_count() as f64;
        if total == 0.0 {
            return 0.0;
        }

        let iter = self.buf.words().map(str::to_lowercase);
        let words = HashSet::<String>::from_iter(iter);
        let unique = words.len() as f64;

        unique / total
    }
}
