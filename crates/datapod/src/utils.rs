use std::path::Path;

#[inline]
pub(crate) fn relpath<P1, P2>(path: P1, prefix: P2) -> String
where
    P1: AsRef<Path>,
    P2: AsRef<Path>,
{
    path.as_ref()
        .strip_prefix(prefix)
        .expect("valid prefix")
        .to_str()
        .unwrap()
        .into()
}
