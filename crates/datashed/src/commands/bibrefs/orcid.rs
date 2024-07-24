use std::sync::OnceLock;

use bstr::ByteSlice;
use regex::bytes::Regex;

use super::{Matcher, RefKind, Reference};

fn orcid_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?ix)
            (?:https?:\/\/orcid\.org\/)
            (\d{4}-\d{4}-\d{4}-\d{3}(?:\d|X))",
        )
        .unwrap()
    })
}

#[derive(Default)]
pub(crate) struct OrcidMatcher {}

impl Matcher for OrcidMatcher {
    fn matches(&self, content: &[u8]) -> Vec<Reference> {
        orcid_re()
            .captures_iter(content)
            .map(|caps| {
                let m = caps.get(0).unwrap();
                let (_, [value]) = caps.extract();
                Reference {
                    kind: RefKind::Isbn,
                    value: value.to_str().unwrap().to_string(),
                    start: m.start(),
                    end: m.end(),
                }
            })
            .collect()
    }
}
