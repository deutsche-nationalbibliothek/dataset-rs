use std::sync::OnceLock;

use bstr::ByteSlice;
use regex::bytes::Regex;

use super::{Matcher, RefKind, Reference};

fn isbn_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?ix)
                ISBN(?:-1[03])?(?::?\s*)?\s
                ((?:97[89][-\ ]?)?
                 \d{1,5}[-\ ]?
                 (?:\d+[-\ ]?){2}
                 (?:\d|X))",
        )
        .unwrap()
    })
}

#[derive(Default)]
pub(crate) struct IsbnMatcher {}

impl Matcher for IsbnMatcher {
    fn matches(&self, content: &[u8]) -> Vec<Reference> {
        isbn_re()
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
