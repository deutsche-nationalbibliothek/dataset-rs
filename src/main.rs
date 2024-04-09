use std::process;

use clap::Parser;
use cli::Command;
use dataset::Dataset;
use error::DatasetError;
use rayon::ThreadPoolBuilder;

use crate::cli::Args;

mod cli;
mod commands;
mod config;
mod dataset;
mod document;
mod error;
mod progress;
mod remote;

fn num_threads(args: &Args) -> usize {
    if let Some(num_threads) = args.num_jobs {
        return num_threads;
    }

    if let Ok(config) = Dataset::discover().and_then(|ds| ds.config()) {
        if let Some(runtime) = config.runtime {
            if let Some(num_threads) = runtime.num_jobs {
                return num_threads;
            }
        }
    }

    0
}

fn run(args: Args) -> Result<(), DatasetError> {
    match args.cmd {
        Command::Init(args) => commands::init::execute(args),
        Command::Config(args) => commands::config::execute(args),
        Command::Remote(args) => commands::remote::execute(args),
        Command::Update(args) => commands::update::execute(args),
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
        Err(DatasetError::IO(e))
            if e.kind() == std::io::ErrorKind::BrokenPipe =>
        {
            process::exit(0)
        }
        Err(e) => {
            eprintln!("error: {e:#}");
            process::exit(1);
        }
    }
}
