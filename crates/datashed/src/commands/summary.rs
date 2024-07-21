use clap::Parser;
use comfy_table::{presets, Row, Table};
use humansize::{make_format, BINARY};
use polars::datatypes::DataType;
use polars::lazy::dsl::col;
use polars::prelude::IntoLazy;

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
}

impl Summary {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        let datashed = Datashed::discover()?;
        let index = datashed.index()?;
        let df = index
            .lazy()
            .group_by([col("remote"), col("kind")])
            .agg([
                col("idn").count().alias("docs"),
                col("size").sum(),
                col("hash").n_unique().alias("unique"),
            ])
            .with_columns([(col("docs") - col("unique")).alias("dups")])
            .select([
                col("remote"),
                col("kind"),
                col("docs").cast(DataType::UInt64),
                col("size").cast(DataType::UInt64),
                col("unique").cast(DataType::UInt64),
            ])
            .collect()?;

        let remotes = df.column("remote")?.str()?;
        let kinds = df.column("kind")?.str()?;
        let docs = df.column("docs")?.u64()?;
        let sizes = df.column("size")?.u64()?;
        let uniques = df.column("unique")?.u64()?;

        let formatter = make_format(BINARY);

        let mut table = Table::new();
        table.load_preset(presets::UTF8_FULL_CONDENSED);
        table.set_header(Row::from(vec![
            "remote", "kind", "docs", "size", "unique",
        ]));

        for idx in 0..df.height() {
            let remote = remotes.get(idx).unwrap();
            let kind = kinds.get(idx).unwrap();
            let docs = docs.get(idx).unwrap();
            let size = sizes.get(idx).unwrap();
            let unique = uniques.get(idx).unwrap();

            table.add_row([
                remote.to_string(),
                kind.to_string(),
                docs.to_string(),
                formatter(size),
                unique.to_string(),
            ]);
        }

        println!("{table}");
        Ok(())
    }
}
