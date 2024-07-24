use std::sync::OnceLock;

use bstr::ByteSlice;
use regex::bytes::Regex;

use super::{Matcher, RefKind, Reference};

fn ddc_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?i)DDC\s+(\d{3}(?:\.\d+))(?:—dc23)").unwrap()
    })
}

#[derive(Default)]
pub(crate) struct DdcMatcher {}

impl Matcher for DdcMatcher {
    fn matches(&self, content: &[u8]) -> Vec<Reference> {
        ddc_re()
            .captures_iter(content)
            .map(|caps| {
                let m = caps.get(0).unwrap();
                let (_, [value]) = caps.extract();
                Reference {
                    kind: RefKind::Ddc,
                    value: value.to_str().unwrap().to_string(),
                    start: m.start(),
                    end: m.end(),
                }
            })
            .collect()
    }
}
