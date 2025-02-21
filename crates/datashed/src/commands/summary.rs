use std::fs::{self};
use std::path::PathBuf;

use clap::Parser;
use comfy_table::{Row, Table, presets};
use humansize::{BINARY, make_format};
use polars::lazy::dsl::col;
use polars::prelude::{DataType, IntoLazy, SortMultipleOptions};
use serde_json::{Map, json};

use crate::prelude::*;

/// Prints a summary of the datashed.
#[derive(Debug, Default, Parser)]
pub(crate) struct Summary {
    /// Run verbosely. Print additional progress information to the
    /// standard error stream. This option conflicts with the
    /// `--quiet` option.
    #[arg(short, long, conflicts_with = "quiet")]
    verbose: bool,

    /// Operate quietly; do not show progress. This option conflicts
    /// with the `--verbose` option.
    #[arg(short, long, conflicts_with = "verbose")]
    quiet: bool,

    /// Write summary in JSON format to `filename` instead of standard
    /// output (stdout).
    #[arg(short, long, value_name = "filename")]
    output: Option<PathBuf>,
}

impl Summary {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        let datashed = Datashed::discover()?;
        let index = datashed.index()?;
        let df = index
            .lazy()
            .group_by([col("remote"), col("kind")])
            .agg([
                col("ppn").count().alias("docs"),
                col("size").sum(),
                col("hash").n_unique().alias("unique"),
            ])
            .with_columns([(col("docs") - col("unique")).alias("dups")])
            .select([
                col("remote"),
                col("kind"),
                col("docs"),
                col("size").cast(DataType::UInt64),
                col("dups"),
            ])
            .sort(["kind"], SortMultipleOptions::default())
            .collect()?;

        let kinds = df.column("kind")?.str()?;
        let docs = df.column("docs")?.u32()?;
        let sizes = df.column("size")?.u64()?;
        let dups = df.column("dups")?.u32()?;

        if let Some(path) = self.output {
            let mut map = Map::new();

            for idx in 0..df.height() {
                let kind = kinds.get(idx).unwrap();
                let docs = docs.get(idx).unwrap();
                let size = sizes.get(idx).unwrap();
                let dups = dups.get(idx).unwrap();

                map.insert(
                    kind.to_string(),
                    json!({
                        "docs": docs,
                        "size": size,
                        "duplicates": dups,

                    }),
                );
            }

            let value: serde_json::Value = map.into();
            fs::write(path, value.to_string())?;
        } else {
            let formatter = make_format(BINARY);
            let mut table = Table::new();
            table.load_preset(presets::UTF8_FULL_CONDENSED);
            table.set_header(Row::from(vec![
                "kind",
                "docs",
                "size",
                "duplicates",
            ]));

            for idx in 0..df.height() {
                let kind = kinds.get(idx).unwrap();
                let docs = docs.get(idx).unwrap();
                let size = sizes.get(idx).unwrap();
                let dups = dups.get(idx).unwrap();

                table.add_row([
                    kind.to_string(),
                    docs.to_string(),
                    formatter(size),
                    dups.to_string(),
                ]);
            }

            println!("{table}");
        }

        Ok(())
    }
}
