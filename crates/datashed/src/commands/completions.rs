use std::fs::File;
use std::io::{stdout, Write};
use std::path::PathBuf;

use clap::CommandFactory;
use clap_complete::{generate, Shell};

use crate::cli::Args;
use crate::prelude::*;

/// Generate completion scripts for various shells.
#[derive(Debug, clap::Parser)]
pub(crate) struct Completions {
    /// Write output to `filename` instead of `stdout`.
    #[arg(long, short, value_name = "filename")]
    output: Option<PathBuf>,

    /// Shell for which a completion script is to be generated.
    #[arg(value_name = "shell")]
    shell: Shell,
}

impl Completions {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        let mut cmd = Args::command();
        let mut wtr: Box<dyn Write> = match self.output {
            Some(path) => Box::new(File::create(path)?),
            None => Box::new(stdout().lock()),
        };

        generate(self.shell, &mut cmd, "datashed", &mut wtr);
        wtr.flush()?;
        Ok(())
    }
}
