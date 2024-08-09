use clap::Parser;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None, max_term_width = 72)]
pub(crate) struct Args {}
