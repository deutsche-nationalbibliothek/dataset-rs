use std::process;

use clap::Parser;
use cli::Args;
use error::DatashedResult;

mod cli;
mod error;

fn run(_args: Args) -> DatashedResult<()> {
    Ok(())
}

fn main() {
    let args = Args::parse();

    match run(args) {
        Ok(()) => process::exit(0),
        Err(e) => {
            eprintln!("error: {e:#}");
            process::exit(1);
        }
    }
}
