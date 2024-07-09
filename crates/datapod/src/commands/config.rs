use clap::Parser;

use crate::config::Runtime;
use crate::datapod::Datapod;
use crate::error::{bail, DatapodError, DatapodResult};

/// Get and set dataset options.
#[derive(Debug, Parser)]
pub(crate) struct Config {
    /// Get the value for the given key.
    #[arg(long, conflicts_with_all = ["value", "unset", "set"])]
    get: bool,

    /// Remove the key from the config.
    #[arg(long, conflicts_with_all = ["value", "get", "set"])]
    unset: bool,

    /// Set the value for the given key.
    #[arg(long, requires = "value", conflicts_with_all = ["get", "unset"])]
    set: bool,

    key: String,

    #[arg(conflicts_with_all = ["get", "unset"])]
    value: Option<String>,
}

#[inline]
fn print_option<T>(key: &str, value: Option<T>)
where
    T: ToString,
{
    println!(
        "{key} = {}",
        match value {
            Some(value) => value.to_string(),
            None => "None".to_string(),
        }
    );
}

pub(crate) fn execute(args: Config) -> DatapodResult<()> {
    let datapod = Datapod::discover()?;
    let mut config = datapod.config()?;
    let key = args.key.as_str();

    if args.value.is_some() {
        let value = args.value.unwrap();
        match key {
            "runtime.num_jobs" => {
                if let Ok(value) = value.parse::<usize>() {
                    if let Some(ref mut runtime) = config.runtime {
                        runtime.num_jobs = Some(value);
                    } else {
                        config.runtime = Some(Runtime {
                            num_jobs: Some(value),
                        });
                    }

                    config.save()?;
                } else {
                    bail!("invalid value `{value}`");
                }
            }
            _ => {
                bail!("unknown or unsupported config option `{key}`");
            }
        }
    } else if args.get || (!args.unset && !args.set) {
        match key {
            "runtime.num_jobs" => {
                print_option(
                    key,
                    config.runtime.and_then(|rt| rt.num_jobs),
                );
            }
            _ => {
                bail!("unknown or unsupported config option `{key}`");
            }
        }
    } else if args.unset {
        match key {
            "runtime.num_jobs" => {
                config.runtime = None;
                config.save()?;
            }
            _ => {
                bail!("unknown or unsupported config option `{key}`");
            }
        }
    } else {
        unreachable!()
    }

    Ok(())
}
