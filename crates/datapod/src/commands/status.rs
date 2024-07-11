use std::collections::BTreeSet;
use std::env::current_dir;
// use std::env::current_dir;
use std::path::PathBuf;

use clap::Parser;
use comfy_table::{presets, Row, Table};
use glob::{glob_with, MatchOptions};

use crate::datapod::Datapod;
use crate::document::Document;
use crate::error::{DatapodError, DatapodResult};
use crate::utils::relpath;

const PBAR_COLLECT: &str =
    "Collecting documents: {human_pos} ({percent}%) | \
        elapsed: {elapsed_precise}{msg}";

/// Show the datapod status
#[derive(Debug, Default, Parser)]
pub(crate) struct Status {
    /// Run verbosely. Print additional progress information to the
    /// standard error stream. This option conflicts with the
    /// `--quiet` option.
    #[arg(short, long, conflicts_with = "quiet")]
    verbose: bool,

    /// Operate quietly; do not show progress. This option conflicts
    /// with the `--verbose` option.
    #[arg(short, long, conflicts_with = "verbose")]
    quiet: bool,

    /// Write the archive to `filename` instead of stdout.
    #[arg(short, long, value_name = "filename")]
    output: Option<PathBuf>,
}

enum DocumentStatus {
    Untracked,
}

pub(crate) fn execute(_args: Status) -> DatapodResult<()> {
    let datapod = Datapod::discover()?;
    let data_dir = datapod.data_dir();
    let base_dir = datapod.base_dir();
    let current_dir = current_dir()?;
    let config = datapod.config()?;
    let index = datapod.index()?;

    let mut table = Table::new();
    table.set_header(Row::from(vec![
        "status", "H", "M", "S", "document",
    ]));
    table.load_preset(presets::UTF8_FULL_CONDENSED);

    let pattern = format!("{}/**/*.txt", data_dir.display());
    let options = MatchOptions::default();

    let mut files: BTreeSet<_> = glob_with(&pattern, options)
        .map_err(|e| DatapodError::Other(e.to_string()))?
        .filter_map(Result::ok)
        .map(|path| relpath(path, base_dir))
        .collect();

    let path = index.column("path")?.str()?;
    let hash = index.column("hash")?.str()?;
    let mtime = index.column("mtime")?.u64()?;
    let size = index.column("size")?.u64()?;

    for idx in 0..index.height() {
        let index_path = path.get(idx).unwrap();

        if files.remove(index_path) {
            let mut valid = true;
            let mut checksum = "✓";
            let mut modified = "✓";
            let mut filesize = "✓";

            let doc = Document::from_path(index_path)?;
            let expected = hash.get(idx).unwrap();
            let actual = doc.hash();

            if !actual.starts_with(expected) {
                valid = false;
                checksum = "✗";
            }

            if doc.modified() != mtime.get(idx).unwrap() {
                valid = false;
                modified = "✗";
            }

            if doc.size() != size.get(idx).unwrap() {
                valid = false;
                filesize = "✗";
            }

            if !valid {
                let path =
                    relpath(base_dir.join(index_path), &current_dir);
                table.add_row(vec![
                    "changed", checksum, modified, filesize, &path,
                ]);
            }
        } else {
            let path = relpath(base_dir.join(index_path), &current_dir);
            table.add_row(vec!["missing", "", "", "", &path]);
        }
    }

    let mut untracked = Vec::from_iter(files);
    untracked.sort();

    for file in untracked {
        let path = relpath(base_dir.join(file), &current_dir);
        table.add_row(vec!["untracked", "", "", "", &path]);
    }

    eprintln!(
        "datapod '{}', version {}.\n",
        config.metadata.name, config.metadata.version
    );

    if table.is_empty() {
        println!("OK, index and inventory are consistent.");
    } else {
        eprintln!("Status:\n{table}");
    }

    Ok(())
}
