use std::io::ErrorKind;
use std::process;

use clap::Parser;
use cli::{Args, Command};
use error::{DatapodError, DatapodResult};

mod cli;
mod commands;
mod config;
mod datapod;
#[macro_use]
mod error;

fn run(args: Args) -> DatapodResult<()> {
    match args.cmd {
        Command::Init(args) => commands::init::execute(args),
        Command::Config(args) => commands::config::execute(args),
        Command::Version(args) => commands::version::execute(args),
    }
}

fn main() {
    let args = Args::parse();

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
