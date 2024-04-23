use clap::Parser;

use crate::config::Runtime;
use crate::dataset::Dataset;
use crate::error::DatasetError;

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

pub(crate) fn execute(args: Config) -> Result<(), DatasetError> {
    let dataset = Dataset::discover()?;
    let mut config = dataset.config()?;

    if args.value.is_some() {
        let value = args.value.unwrap();
        match args.key.as_str() {
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
                    return Err(DatasetError::Other(format!(
                        "invalid value `{value}`"
                    )));
                }
            }
            _ => {
                return Err(DatasetError::Other(format!(
                    "unknown or unsupported config option `{}`",
                    args.key
                )))
            }
        }
    } else if args.get || (!args.unset && !args.set) {
        match args.key.as_str() {
            "runtime.num_jobs" => {
                print_option(
                    &args.key,
                    config.runtime.and_then(|rt| rt.num_jobs),
                );
            }
            _ => {
                return Err(DatasetError::Other(format!(
                    "unknown or unsupported config option `{}`",
                    args.key
                )))
            }
        }
    } else if args.unset {
        match args.key.as_str() {
            "runtime.num_jobs" => {
                config.runtime = None;
                config.save()?;
            }
            _ => {
                return Err(DatasetError::Other(format!(
                    "unknown or unsupported config option `{}`",
                    args.key
                )))
            }
        }
    } else {
        unreachable!()
    }

    Ok(())
}
