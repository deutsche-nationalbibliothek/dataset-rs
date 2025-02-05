use std::io::ErrorKind;
use std::process;

use clap::Parser;
use cli::{Args, Command};
use datashed::Datashed;
use env_logger::Env;
use error::{DatashedError, DatashedResult};
use jemallocator::Jemalloc;
use polars::error::PolarsError;
use rayon::ThreadPoolBuilder;

mod cli;
mod commands;
mod config;
mod datashed;
mod document;
mod error;
mod lfreq;
mod prelude;
mod progress;
mod utils;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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

async fn run(args: Args) -> DatashedResult<()> {
    match args.cmd {
        Command::Archive(cmd) => cmd.execute(),
        Command::Bibrefs(cmd) => cmd.execute(),
        Command::Clean(cmd) => cmd.execute(),
        Command::Completions(cmd) => cmd.execute(),
        Command::Config(cmd) => cmd.execute(),
        Command::Grep(cmd) => cmd.execute(),
        Command::Index(cmd) => cmd.execute(),
        Command::Init(cmd) => cmd.execute(),
        Command::Lfreq(cmd) => cmd.execute(),
        Command::Restore(cmd) => cmd.execute(),
        Command::Rate(cmd) => cmd.execute().await,
        Command::Select(cmd) => cmd.execute(),
        Command::Serve(cmd) => cmd.execute().await,
        Command::Status(cmd) => cmd.execute(),
        Command::Summary(cmd) => cmd.execute(),
        Command::User(cmd) => cmd.execute(),
        Command::Verify(cmd) => cmd.execute(),
        Command::Version(cmd) => cmd.execute(),
        Command::Vocab(cmd) => cmd.execute(),
    }
}

fn init_logger() {
    let env = Env::default()
        .filter("DATASHED_LOG_LEVEL")
        .write_style("DATASHED_LOG_STYLE")
        .default_filter_or("info");

    env_logger::Builder::from_env(env)
        .format_module_path(false)
        .format_target(false)
        .init();
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    ThreadPoolBuilder::new()
        .num_threads(num_threads(&args))
        .build_global()
        .unwrap();

    init_logger();

    match run(args).await {
        Ok(()) => process::exit(0),
        Err(DatashedError::IO(e))
            if e.kind() == ErrorKind::BrokenPipe =>
        {
            process::exit(0)
        }
        Err(DatashedError::Polars(PolarsError::IO {
            error, ..
        })) if error.kind() == ErrorKind::BrokenPipe => {
            process::exit(0);
        }
        Err(e) => {
            eprintln!("error: {e:#}");
            process::exit(1);
        }
    }
}
