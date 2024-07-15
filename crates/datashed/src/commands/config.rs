use clap::Parser;

use crate::prelude::*;

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

impl Config {
    pub(crate) fn execute(self) -> DatashedResult<()> {
        let datashed = Datashed::discover()?;
        let mut config = datashed.config()?;
        let key = self.key.as_str();

        if self.value.is_some() {
            let value = self.value.unwrap();
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
                    bail!(
                        "unknown or unsupported config option `{key}`"
                    );
                }
            }
        } else if self.unset {
            match key {
                "runtime.num_jobs" => {
                    config.runtime = None;
                    config.save()?;
                }
                _ => {
                    bail!(
                        "unknown or unsupported config option `{key}`"
                    );
                }
            }
        } else if self.get || (!self.unset && !self.set) {
            match key {
                "runtime.num_jobs" => {
                    print_option(
                        key,
                        config.runtime.and_then(|rt| rt.num_jobs),
                    );
                }
                _ => {
                    bail!(
                        "unknown or unsupported config option `{key}`"
                    );
                }
            }
        } else {
            unreachable!()
        }

        Ok(())
    }
}
