use std::io::ErrorKind;
use std::process;

use clap::Parser;
use cli::{Args, Command};
use datashed::Datashed;
use error::{DatashedError, DatashedResult};
use rayon::ThreadPoolBuilder;

mod cli;
mod commands;
mod config;
mod datashed;
mod document;
mod error;
mod progress;
mod utils;

fn num_threads(args: &Args) -> usize {
    if let Some(num_threads) = args.num_jobs {
        return num_threads;
    }

    if let Ok(config) = Datashed::discover().and_then(|dp| dp.config())
    {
        if let Some(runtime) = config.runtime {
            if let Some(num_threads) = runtime.num_jobs {
                return num_threads;
            }
        }
    }

    0
}

fn run(args: Args) -> DatashedResult<()> {
    match args.cmd {
        Command::Init(args) => commands::init::execute(args),
        Command::Config(args) => commands::config::execute(args),
        Command::Index(args) => commands::index::execute(args),
        Command::Verify(args) => commands::verify::execute(args),
        Command::Archive(args) => commands::archive::execute(args),
        Command::Restore(args) => commands::restore::execute(args),
        Command::Status(args) => commands::status::execute(args),
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
        Err(DatashedError::IO(e))
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
