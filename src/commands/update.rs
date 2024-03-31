use std::fs::File;
use std::path::PathBuf;

use clap::Parser;
use indicatif::{
    ParallelProgressIterator, ProgressBar, ProgressFinish,
    ProgressStyle,
};
use polars::prelude::*;
use rayon::prelude::*;

use crate::dataset::Dataset;
use crate::document::Document;
use crate::error::DatasetError;

#[derive(Debug, Parser)]
pub(crate) struct Update {
    /// The path to the PICA+ dump
    path: PathBuf,
}

const PBAR_COLLECT: &str = "Collecting documents: {human_pos} | \
        elapsed: {elapsed_precise}{msg}";

const PBAR_INDEX: &str = "Indexing documents: {human_pos} | \
        elapsed: {elapsed_precise}{msg}";

macro_rules! vecs {
    ($x:ident) => (let mut $x = vec![];);
    ($x:ident, $($y:ident),+) => {
        vecs!($x);
        vecs!($($y),+);
    };
}

#[derive(Debug)]
struct Row {
    remote: String,
    idn: String,
    path: String,
    size: u64,
    modified: u64,
    hash: String,
}

pub(crate) fn execute(_args: Update) -> Result<(), DatasetError> {
    let dataset = Dataset::discover()?;
    let config = dataset.config()?;
    let mut documents: Vec<(&str, Document)> = vec![];
    let mut records: Vec<Row> = vec![];

    // To achieve a maximum throughput through parallelization it's
    // necessary to collect the documents first. Access to documents
    // through remotes is a prerequisite. If a document isn't accessible
    // the update stops with an error message.
    let pbar = ProgressBar::new_spinner().with_style(
        ProgressStyle::with_template(PBAR_COLLECT).unwrap(),
    );

    for (name, remote) in config.remotes.iter() {
        for document in remote.documents() {
            documents.push((name, document?));
            pbar.inc(1);
        }
    }

    pbar.finish_with_message(", done.");

    // Process documents in parallel and transform each document into a
    // row, containing all (meta-)data of the index.
    let pbar = ProgressBar::new(documents.len() as u64)
        .with_style(ProgressStyle::with_template(PBAR_INDEX).unwrap())
        .with_finish(ProgressFinish::AbandonWithMessage(
            ", done.".into(),
        ));

    records.par_extend(
        documents.into_par_iter().progress_with(pbar).map(
            |(name, document)| {
                let remote = config.remotes.get(name).unwrap();
                Row {
                    remote: name.into(),
                    idn: document.idn(),
                    path: document.relpath(remote),
                    size: document.size(),
                    modified: document.modified(),
                    hash: document.hash(8),
                }
            },
        ),
    );

    vecs!(ids, remotes, idns, paths, sizes, mtime, hashes);
    for (id, record) in records.into_iter().enumerate() {
        ids.push(id as u32 + 1);
        remotes.push(record.remote);
        idns.push(record.idn);
        paths.push(record.path);
        sizes.push(record.size);
        mtime.push(record.modified);
        hashes.push(record.hash);
    }

    let cat_dt =
        DataType::Categorical(None, CategoricalOrdering::Lexical);

    let mut df = DataFrame::new(vec![
        Series::new("id", ids),
        Series::new("idn", idns),
        Series::new("remote", remotes).cast(&cat_dt)?,
        Series::new("path", paths),
        Series::new("size", sizes),
        Series::new("mtime", mtime),
        Series::new("hash", hashes),
    ])?;

    let path = dataset.data_dir().join("documents.ipc");
    let mut writer = IpcWriter::new(File::create(path)?)
        .with_compression(Some(IpcCompression::ZSTD));
    writer.finish(&mut df)?;

    Ok(())
}
