use std::process;

use clap::Parser;
use cli::Args;
use error::DatashedResult;
use rayon::ThreadPoolBuilder;

mod cli;
mod error;

fn num_threads(args: &Args) -> usize {
    if let Some(num_threads) = args.num_jobs {
        return num_threads;
    }

    0
}

fn run(_args: Args) -> DatashedResult<()> {
    Ok(())
}

fn main() {
    let args = Args::parse();

    ThreadPoolBuilder::new()
        .num_threads(num_threads(&args))
        .build_global()
        .unwrap();

    if run(args).is_ok() {
        process::exit(0);
    }
}
