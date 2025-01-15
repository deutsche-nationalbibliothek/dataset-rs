use std::ops::{Deref, DerefMut};

use hashbrown::HashMap;
use pica_record::prelude::*;

use crate::document::DocumentKind;
use crate::prelude::*;

#[derive(Debug)]
struct Matcher {
    from: DocumentKind,
    to: DocumentKind,
    matcher: RecordMatcher,
}

impl Matcher {
    #[inline]
    fn is_match(&self, record: &ByteRecord) -> bool {
        self.matcher.is_match(record, &Default::default())
    }
}

#[derive(Debug, Default)]
pub(crate) struct KindMap {
    refinements: HashMap<(String, DocumentKind), DocumentKind>,
    matchers: Vec<Matcher>,
}

impl Deref for KindMap {
    type Target = HashMap<(String, DocumentKind), DocumentKind>;

    fn deref(&self) -> &Self::Target {
        &self.refinements
    }
}

impl DerefMut for KindMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.refinements
    }
}

impl KindMap {
    pub(crate) fn from_config(config: &Config) -> DatashedResult<Self> {
        let mut matchers = vec![];

        for (from, spec) in config.kinds.iter() {
            for refinement in spec.refinements.iter() {
                let filter = &refinement.filter;
                let to = &refinement.target;

                let matcher =
                    RecordMatcher::new(filter).map_err(|_| {
                        DatashedError::other(format!(
                            "Invalid record matcher '{filter}'"
                        ))
                    })?;

                matchers.push(Matcher {
                    from: from.clone(),
                    to: to.clone(),
                    matcher,
                });
            }
        }

        Ok(Self {
            matchers,
            refinements: HashMap::new(),
        })
    }

    pub(crate) fn process_record(&mut self, record: &ByteRecord) {
        for matcher in self.matchers.iter() {
            if matcher.is_match(record) {
                let idn = record.ppn().to_string();
                let _ = self.refinements.insert(
                    (idn, matcher.from.clone()),
                    matcher.to.clone(),
                );

                break;
            }
        }
    }
}
