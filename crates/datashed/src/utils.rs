use std::fs::create_dir_all;
use std::path::{Path, PathBuf};

use directories::ProjectDirs;

use crate::error::{bail, DatashedError, DatashedResult};

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
