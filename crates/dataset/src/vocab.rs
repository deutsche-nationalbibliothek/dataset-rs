use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(
    Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum VocabKind {
    CorporateBody,
    Conference,
    PlaceOrGeoName,
    Person,
    #[default]
    SubjectHeading,
    Work,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct KindConfig {
    // The minimum frequency of ground truth documents for the given
    // kind of authority record.
    pub(crate) threshold: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct LabelSource {
    // An expression to get the targets of a record.
    pub(crate) source: String,

    // An optional predicate to filtzer for records that serve as a
    // label source.
    #[serde(rename = "where")]
    pub(crate) predicate: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct VocabConfig {
    // A pica matcher expression to determine if a pica record is a
    // authority record and part of the vocabulary.
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub(crate) filter: String,

    #[serde(default)]
    pub(crate) strsim_threshold: f64,

    #[serde(default)]
    pub(crate) case_ignore: bool,

    // A list of pica path expressions to get bibliographic records
    // linked to a authority records.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub(crate) targets: Vec<LabelSource>,

    // Parameters to fit a subset of authority records (kind) according
    // to available documents.
    #[serde(
        rename = "kind",
        skip_serializing_if = "HashMap::is_empty",
        default
    )]
    pub(crate) kinds: HashMap<VocabKind, KindConfig>,
}

impl VocabConfig {
    pub(crate) fn is_empty(&self) -> bool {
        self.filter.is_empty()
            && self.targets.is_empty()
            && self.kinds.is_empty()
    }
}

impl Default for VocabConfig {
    fn default() -> Self {
        Self {
            filter: "002@{ 0 =^ 'T' && 0 =~ '^T[bfgpsu][1z]$'".into(),
            case_ignore: false,
            strsim_threshold: 0.8,
            targets: vec![LabelSource {
                source: "041A/*{ (9, a) | 9? }".into(),
                predicate: None,
            }],
            kinds: HashMap::new(),
        }
    }
}
