use std::io::ErrorKind;
use std::process;

use clap::Parser;
use cli::{Args, Command};
use datapod::Datapod;
use error::{DatapodError, DatapodResult};
use rayon::ThreadPoolBuilder;

mod cli;
mod commands;
mod config;
mod datapod;
mod document;
mod error;
mod progress;
mod utils;

fn num_threads(args: &Args) -> usize {
    if let Some(num_threads) = args.num_jobs {
        return num_threads;
    }

    if let Ok(config) = Datapod::discover().and_then(|dp| dp.config()) {
        if let Some(runtime) = config.runtime {
            if let Some(num_threads) = runtime.num_jobs {
                return num_threads;
            }
        }
    }

    0
}

fn run(args: Args) -> DatapodResult<()> {
    match args.cmd {
        Command::Init(args) => commands::init::execute(args),
        Command::Config(args) => commands::config::execute(args),
        Command::Index(args) => commands::index::execute(args),
        Command::Verify(args) => commands::verify::execute(args),
        Command::Version(args) => commands::version::execute(args),
    }
}

fn main() {
    let args = Args::parse();

    ThreadPoolBuilder::new()
        .num_threads(num_threads(&args))
        .build_global()
        .unwrap();

    match run(args) {
        Ok(()) => process::exit(0),
        Err(DatapodError::IO(e))
            if e.kind() == ErrorKind::BrokenPipe =>
        {
            process::exit(0)
        }
        Err(e) => {
            eprintln!("error: {e:#}");
            process::exit(1);
        }
    }
}
