use std::sync::OnceLock;

use bstr::ByteSlice;
use regex::bytes::Regex;

use super::{Matcher, RefKind, Reference};

fn issn_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?ix)ISSN(?::?\s*)?\s(\d{4}-\d{4})").unwrap()
    })
}

#[derive(Default)]
pub(crate) struct IssnMatcher {}

impl Matcher for IssnMatcher {
    fn matches(&self, content: &[u8]) -> Vec<Reference> {
        issn_re()
            .captures_iter(content)
            .map(|caps| {
                let m = caps.get(0).unwrap();
                let (_, [value]) = caps.extract();
                let value = value.to_str().unwrap();
                Reference {
                    kind: RefKind::Issn,
                    value: value.to_string(),
                    start: m.start(),
                    end: m.end(),
                }
            })
            .collect()
    }
}
