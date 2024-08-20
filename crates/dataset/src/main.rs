use std::io::ErrorKind;
use std::process;

use clap::Parser;
use cli::{Args, Command};
use dataset::Dataset;
use error::{DatasetError, DatasetResult};
use rayon::ThreadPoolBuilder;

mod cli;
mod commands;
mod config;
mod dataset;
mod error;
mod prelude;
mod progress;
mod remote;
mod vocab;

async fn run(args: Args) -> DatasetResult<()> {
    match args.cmd {
        Command::Completions(cmd) => cmd.execute(),
        Command::Config(cmd) => cmd.execute(),
        Command::Fetch(cmd) => cmd.execute().await,
        Command::Init(cmd) => cmd.execute(),
        Command::Remote(cmd) => cmd.execute(),
        Command::Version(cmd) => cmd.execute(),
        Command::Vocab(cmd) => cmd.execute(),
    }
}

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

#[tokio::main]
async fn main() {
    let args = Args::parse();

    ThreadPoolBuilder::new()
        .num_threads(num_threads(&args))
        .build_global()
        .unwrap();

    match run(args).await {
        Ok(()) => process::exit(0),
        Err(DatasetError::IO(e))
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
