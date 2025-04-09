use std::fmt::{self, Display};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::prelude::*;

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
pub(crate) enum DocumentType {
    Article,
    Book,
    Chapter,
    Issue,
    #[default]
    Other,
    Toc,
}

impl Display for DocumentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Article => write!(f, "article"),
            Self::Book => write!(f, "book"),
            Self::Chapter => write!(f, "chapter"),
            Self::Issue => write!(f, "issue"),
            Self::Other => write!(f, "other"),
            Self::Toc => write!(f, "toc"),
        }
    }
}

impl FromStr for DocumentType {
    type Err = DatashedError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "article" => Ok(Self::Article),
            "book" => Ok(Self::Book),
            "chapter" => Ok(Self::Chapter),
            "issue" => Ok(Self::Issue),
            "other" | "ft" => Ok(Self::Other),
            "toc" => Ok(Self::Toc),
            _ => bail!("invalid document kind '{s}'"),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_kind_from_str() {
        use DocumentType::*;

        assert_eq!(DocumentType::from_str("article").unwrap(), Article);
        assert_eq!(DocumentType::from_str("book").unwrap(), Book);
        assert_eq!(DocumentType::from_str("chapter").unwrap(), Chapter);
        assert_eq!(DocumentType::from_str("issue").unwrap(), Issue);
        assert_eq!(DocumentType::from_str("other").unwrap(), Other);
        assert_eq!(DocumentType::from_str("ft").unwrap(), Other);
        assert_eq!(DocumentType::from_str("toc").unwrap(), Toc);

        assert!(DocumentType::from_str("wp").is_err());
    }

    #[test]
    fn document_kind_to_string() {
        use DocumentType::*;

        assert_eq!(Article.to_string(), "article");
        assert_eq!(Book.to_string(), "book");
        assert_eq!(Chapter.to_string(), "chapter");
        assert_eq!(Issue.to_string(), "issue");
        assert_eq!(Other.to_string(), "other");
        assert_eq!(Toc.to_string(), "toc");
    }

    #[test]
    fn document_kind_default() {
        assert_eq!(DocumentType::default(), DocumentType::Other);
    }
}
