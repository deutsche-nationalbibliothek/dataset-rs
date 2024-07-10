use std::path::Path;

#[inline]
pub(crate) fn relpath<P: AsRef<Path>>(path: P, prefix: P) -> String {
    path.as_ref()
        .strip_prefix(prefix)
        .expect("valid prefix")
        .to_str()
        .unwrap()
        .into()
}
