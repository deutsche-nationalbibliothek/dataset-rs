use std::fs::{self, File, OpenOptions};
use std::io::Cursor;
use std::path::PathBuf;

use dialoguer::{Confirm, Input, Password, Select};
use minus::{page_all, ExitStrategy, Pager};
use polars::io::SerReader;
use polars::prelude::*;
use reqwest::{Client, StatusCode, Url};

use crate::prelude::*;
use crate::utils::state_dir;

/// Rate the data quality of documents.
#[derive(Debug, clap::Parser)]
pub(crate) struct Rate {
    /// Run verbosely. Print additional progress information to the
    /// standard error stream. This option conflicts with the
    /// `--quiet` option.
    #[arg(short, long, conflicts_with = "quiet")]
    verbose: bool,

    /// Operate quietly; do not show progress. This option conflicts
    /// with the `--verbose` option.
    #[arg(short, long, conflicts_with = "verbose")]
    quiet: bool,

    /// The port of the datashed.
    #[arg(short, long, default_value = "9001")]
    port: Option<u16>,

    /// The address of the datashed.
    #[arg(long, default_value = "127.0.0.1")]
    address: Option<String>,

    /// The username with which the rating is to be carried out.
    #[arg(short, long, env = "DATASHED_USERNAME")]
    username: Option<String>,

    /// The secret (API token) associated with the username.
    #[arg(short, long, env = "DATASHED_SECRET")]
    secret: Option<String>,

    /// Write ...
    #[arg(short, long, value_name = "filename")]
    output: Option<PathBuf>,

    /// List of documents to be evaluated (in CSV format).
    path: Option<PathBuf>,
}

#[derive(Debug, serde::Serialize)]
struct Request {
    username: String,
    secret: String,
    path: String,
    hash: String,
    rating: String,
    comment: String,
}

impl Rate {
    pub(crate) async fn execute(self) -> DatashedResult<()> {
        let username = match self.username {
            Some(username) => username,
            None => Input::new()
                .with_prompt("Enter your username")
                .interact_text()
                .unwrap(),
        };

        let secret = match self.secret {
            Some(secret) => secret,
            None => Password::new()
                .with_prompt("Enter your secret")
                .interact()
                .unwrap(),
        };

        let mut base_uri = Url::parse("http://localhost").unwrap();
        base_uri.set_port(self.port).unwrap();
        if let Some(host) = self.address {
            base_uri.set_host(Some(&host)).unwrap();
        }

        // Index
        let mut index_url = base_uri.clone();
        index_url.set_path("/index.ipc");

        let body = reqwest::get(index_url).await?.bytes().await?;
        if body.is_empty() {
            bail!("unable to get datashed index");
        }

        let mut index = IpcReader::new(Cursor::new(body)).finish()?;
        if let Some(path) = self.path {
            let paths = CsvReader::new(File::open(path)?)
                .finish()?
                .column("path")?
                .clone();

            index = index
                .lazy()
                .filter(col("path").is_in(lit(paths)))
                .collect()?;
        }

        let state_file = state_dir()?.join("ratings.csv");
        if !state_file.exists() {
            fs::write(
                &state_file,
                "remote,path,hash,rating,comment,username\n",
            )?;
        }

        if index.height() > 0 {
            let remote = index.column("remote")?.str()?.get(0).unwrap();
            let state_df = CsvReader::new(File::open(&state_file)?)
                .finish()?
                .lazy()
                .filter(col("remote").eq(lit(remote)))
                .collect()?;

            let paths = state_df.column("path")?.clone();
            index = index
                .lazy()
                .filter(col("path").is_in(lit(paths)).not())
                .collect()?;
        }

        let mut state_writer =
            csv::WriterBuilder::new().has_headers(false).from_writer(
                OpenOptions::new().append(true).open(state_file)?,
            );

        let remote = index.column("remote")?.str()?;
        let path = index.column("path")?.str()?;
        let hash = index.column("hash")?.str()?;
        let idn = index.column("idn")?.str()?;
        let len = index.height();

        let mut ratings_url = base_uri.clone();
        ratings_url.set_path("/ratings");
        let client = Client::new();

        for idx in 0..len {
            let remote = remote.get(idx).unwrap();
            let filename = path.get(idx).unwrap();
            let hash = hash.get(idx).unwrap();
            let idn = idn.get(idx).unwrap();

            print!("\x1B[2J");
            let header = format!(
                "Rating {}/{len} (path = {filename}, hash = {hash})",
                idx + 1
            );
            println!("{header}\n{0}\n", "~".repeat(header.len()));
            println!("Portal:\n\thttps://d-nb.info/{idn}\n",);
            println!(
                "Record Browser:\n\t\
                http://etc.dnb.de/pica-record-browser/show.xhtml\
                ?src=prsx&idn={idn}\n"
            );

            let stop = Confirm::new()
                .with_prompt("Do you want to stop?")
                .show_default(true)
                .default(false)
                .interact()
                .unwrap();

            if stop {
                break;
            }

            let mut document_url = base_uri.clone();
            document_url.set_path(filename);
            let content =
                reqwest::get(document_url).await?.text().await?;

            let pager = Pager::new();
            pager.set_exit_strategy(ExitStrategy::PagerQuit)?;
            pager.set_run_no_overflow(true)?;
            pager.set_prompt(filename)?;
            pager.push_str(&content)?;
            page_all(pager)?;

            let prompt = "Select rating of data quality";
            let rating = loop {
                let interaction = Select::new()
                    .with_prompt(prompt)
                    .items(&[
                        "C  (correct)",
                        "C- (correct minus)",
                        "P+ (partial plus)",
                        "P  (partial)",
                        "P- (partial minus)",
                        "I  (incorrect)",
                    ])
                    .default(0)
                    .interact();

                match interaction {
                    Ok(0) => break "C",
                    Ok(1) => break "C-",
                    Ok(2) => break "P+",
                    Ok(3) => break "P",
                    Ok(4) => break "P-",
                    Ok(5) => break "I",
                    _ => continue,
                }
            };

            let prompt = "Enter a comment or press <Return> to skip";
            let comment: String = Input::new()
                .with_prompt(prompt)
                .allow_empty(true)
                .interact_text()
                .unwrap();

            let result = client
                .post(ratings_url.clone())
                .json(&Request {
                    username: username.clone(),
                    secret: secret.clone(),
                    path: filename.to_string(),
                    hash: hash.to_string(),
                    rating: rating.to_string(),
                    comment: comment.to_string(),
                })
                .send()
                .await;

            let Ok(res) = result else {
                bail!("unable to send request!");
            };

            match res.status() {
                StatusCode::OK => {
                    state_writer.write_record([
                        remote,
                        filename,
                        hash,
                        rating,
                        comment.as_str(),
                        username.as_str(),
                    ])?;
                    state_writer.flush()?;
                    continue;
                }
                _ => {
                    bail!("got status code '{}'", res.status());
                }
            }
        }

        state_writer.flush()?;
        Ok(())
    }
}
