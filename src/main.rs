use clap::Parser;
use dataset::Dataset;
use rayon::ThreadPoolBuilder;

use crate::cli::Args;

mod cli;
mod config;
mod dataset;
mod error;

fn num_threads(args: &Args) -> usize {
    if let Some(num_threads) = args.num_jobs {
        return num_threads;
    }

    if let Ok(config) = Dataset::discover().and_then(|ds| ds.config()) {
        if let Some(num_threads) = config.num_jobs {
            return num_threads;
        }
    }

    0
}

fn main() {
    let args = Args::parse();
    eprintln!("num_threads = {:?}", num_threads(&args));

    ThreadPoolBuilder::new()
        .num_threads(num_threads(&args))
        .build_global()
        .unwrap();
}
