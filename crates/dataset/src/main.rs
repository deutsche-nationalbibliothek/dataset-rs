use std::io::ErrorKind;
use std::process;

use clap::Parser;
use cli::{Args, Command};
use error::{DatasetError, DatasetResult};
use rayon::ThreadPoolBuilder;

mod cli;
mod commands;
mod config;
mod dataset;
mod error;
mod prelude;

fn num_threads(args: &Args) -> usize {
    if let Some(num_threads) = args.num_jobs {
        return num_threads;
    }

    0
}

async fn run(args: Args) -> DatasetResult<()> {
    match args.cmd {
        Command::Init(cmd) => cmd.execute(),
    }
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
