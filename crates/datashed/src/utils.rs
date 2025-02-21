use std::fs::create_dir_all;
use std::path::{Path, PathBuf};

use directories::ProjectDirs;

use crate::error::{DatashedError, DatashedResult, bail};

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

pub(crate) fn state_dir() -> DatashedResult<PathBuf> {
    if let Some(project_dirs) =
        ProjectDirs::from("de.dnb", "DNB", "datashed")
    {
        if let Some(state_dir) = project_dirs.state_dir() {
            if !state_dir.exists() {
                create_dir_all(state_dir)?;
            }

            return Ok(state_dir.to_path_buf());
        }
    }

    bail!("unable determine state directory!")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::relpath;

    #[test]
    fn relpath_ok() {
        let path = PathBuf::from("/home/foo/bar/baz.txt");
        let prefix = PathBuf::from("/home/foo");
        assert_eq!(relpath(path, prefix), "bar/baz.txt");
    }

    #[test]
    #[should_panic]
    fn relpath_panic() {
        let path = PathBuf::from("/home/foo/bar/baz.txt");
        let prefix = PathBuf::from("/home/bar");
        let _ = relpath(path, prefix);
    }
}
