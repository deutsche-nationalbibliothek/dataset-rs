use std::net::IpAddr;

use clap::Parser;

use crate::config::Server;
use crate::prelude::*;

/// Get and set datashed config options.
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

    /// The name of the config option.
    name: String,

    /// The (new) value of the config option.
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

        let name = match self.name.as_str() {
            name if name == "runtime.num_jobs" => name,
            name if name == "server.address" => name,
            name if name == "server.port" => name,
            name => {
                bail!("unknown config option `{name}`");
            }
        };

        if self.value.is_some() {
            let value = self.value.unwrap();
            match name {
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
                "server.address" => {
                    if let Ok(value) = value.parse::<IpAddr>() {
                        if let Some(ref mut server) = config.server {
                            server.address = Some(value);
                        } else {
                            config.server = Some(Server {
                                address: Some(value),
                                ..Default::default()
                            });
                        }
                        config.save()?;
                    } else {
                        bail!("invalid value `{value}`");
                    }
                }
                "server.port" => {
                    if let Ok(value) = value.parse::<u16>() {
                        if let Some(ref mut server) = config.server {
                            server.port = Some(value);
                        } else {
                            config.server = Some(Server {
                                port: Some(value),
                                ..Default::default()
                            });
                        }
                        config.save()?;
                    } else {
                        bail!("invalid value `{value}`");
                    }
                }
                _ => unreachable!(),
            }
        } else if self.unset {
            match name {
                "runtime.num_jobs" => {
                    config.runtime = None;
                    config.save()?;
                }
                "server.address" => {
                    if let Some(ref mut server) = config.server {
                        if server.port.is_some() {
                            server.address = None;
                        } else {
                            config.server = None;
                        }
                        config.save()?;
                    }
                }
                "server.port" => {
                    if let Some(ref mut server) = config.server {
                        if server.address.is_none() {
                            config.server = None;
                        } else {
                            server.port = None;
                        }
                        config.save()?;
                    }
                }
                _ => unreachable!(),
            }
        } else if self.get || (!self.unset && !self.set) {
            match name {
                "runtime.num_jobs" => {
                    print_option(
                        name,
                        config.runtime.and_then(|rt| rt.num_jobs),
                    );
                }
                "server.address" => print_option(
                    name,
                    config.server.and_then(|srv| srv.address),
                ),
                "server.port" => print_option(
                    name,
                    config.server.and_then(|srv| srv.port),
                ),
                _ => unreachable!(),
            }
        } else {
            unreachable!()
        }

        Ok(())
    }
}
