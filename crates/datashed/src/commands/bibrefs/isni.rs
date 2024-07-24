use std::sync::OnceLock;

use bstr::ByteSlice;
use regex::bytes::Regex;

use super::{Matcher, RefKind, Reference};

fn isni_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?:https?:\/\/isni\.org\/isni\/)(\d{15}(?:\d|X))")
            .unwrap()
    })
}

#[derive(Default)]
pub(crate) struct IsniMatcher {}

impl Matcher for IsniMatcher {
    fn matches(&self, content: &[u8]) -> Vec<Reference> {
        isni_re()
            .captures_iter(content)
            .map(|caps| {
                let m = caps.get(0).unwrap();
                let (_, [value]) = caps.extract();
                Reference {
                    kind: RefKind::Isni,
                    value: value.to_str().unwrap().to_string(),
                    start: m.start(),
                    end: m.end(),
                }
            })
            .collect()
    }
}
