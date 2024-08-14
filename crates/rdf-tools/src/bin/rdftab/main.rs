use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::path::PathBuf;
use std::process;

use clap::Parser;
use csv::{Writer, WriterBuilder};
use flate2::read::GzDecoder;
use sophia::api::prelude::*;
use sophia::turtle::parser::nt::parse_bufread as parse_nt;
use sophia::turtle::parser::turtle::parse_bufread as parse_ttl;
use sophia::xml::parser::parse_bufread as parse_xml;

#[derive(Clone, Parser)]
struct Cli {
    #[arg(long = "predicate", short)]
    predicates: Vec<String>,

    #[arg(long)]
    no_header: bool,

    #[arg(long)]
    no_subject: bool,

    #[arg(long)]
    no_predicate: bool,

    #[arg(long)]
    no_object: bool,

    #[arg(long)]
    no_language: bool,

    #[arg(long)]
    strip_base_uri: Option<String>,

    #[arg(long, short)]
    output: Option<PathBuf>,

    filenames: Vec<PathBuf>,
}

fn stringify<T: Term>(t: T) -> String {
    use TermKind::*;

    match t.kind() {
        Iri => t.iri().unwrap().to_string(),
        Literal => t.lexical_form().unwrap().to_string(),
        BlankNode => format!("_:{}", *t.bnode_id().unwrap()),
        _ => unimplemented!("{:?}", t.kind()),
    }
}

fn process<TS: TripleSource + 'static>(
    mut ts: TS,
    writer: &mut Writer<Box<dyn Write>>,
    predicates: &BTreeSet<String>,
    cli: &Cli,
) -> anyhow::Result<()> {
    ts.for_each_triple(|t| {
        let s = stringify(t.s());
        let p = stringify(t.p());
        let o = stringify(t.o());

        let language = t
            .o()
            .language_tag()
            .as_deref()
            .map(ToString::to_string)
            .unwrap_or_default();

        if predicates.is_empty() || predicates.contains(&p) {
            let mut record = Vec::with_capacity(4);

            if !cli.no_subject {
                if let Some(ref prefix) = cli.strip_base_uri {
                    record.push(
                        s.strip_prefix(prefix).unwrap_or(&s).to_owned(),
                    );
                } else {
                    record.push(s);
                }
            }
            if !cli.no_predicate {
                record.push(p);
            }
            if !cli.no_object {
                record.push(o);
            }
            if !cli.no_language {
                record.push(language);
            }

            if !record.is_empty() {
                writer.write_record(record).expect("write");
            }
        }
    })
    .unwrap();

    writer.flush()?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let predicates = BTreeSet::from_iter(cli.clone().predicates);
    let mut writer: Writer<Box<dyn Write>> = if let Some(ref path) =
        cli.output
    {
        WriterBuilder::new().from_writer(Box::new(File::create(path)?))
    } else {
        WriterBuilder::new().from_writer(Box::new(io::stdout().lock()))
    };

    if !cli.no_header {
        let mut header = Vec::with_capacity(4);

        if !cli.no_subject {
            header.push("subject");
        }

        if !cli.no_predicate {
            header.push("predicate");
        }

        if !cli.no_object {
            header.push("object");
        }

        if !cli.no_language {
            header.push("language");
        }

        writer.write_record(&header)?;
    }

    for path in &cli.filenames {
        let reader: BufReader<Box<dyn Read>> =
            match path.extension().and_then(OsStr::to_str) {
                Some("gz") => BufReader::new(Box::new(GzDecoder::new(
                    File::open(path)?,
                ))),
                _ => BufReader::new(Box::new(File::open(path)?)),
            };

        let filename_str = path
            .to_str()
            .map(ToString::to_string)
            .expect("valid filename");

        if filename_str.ends_with(".xml")
            || filename_str.ends_with(".xml.gz")
            || filename_str.ends_with(".rdf")
            || filename_str.ends_with(".rdf.gz")
        {
            process(parse_xml(reader), &mut writer, &predicates, &cli)?;
        } else if filename_str.ends_with(".ttl")
            || filename_str.ends_with(".ttl.gz")
        {
            process(parse_ttl(reader), &mut writer, &predicates, &cli)?;
        } else if filename_str.ends_with(".nt")
            || filename_str.ends_with(".nt.gz")
        {
            process(parse_nt(reader), &mut writer, &predicates, &cli)?;
        } else {
            eprintln!("invalid file extension");
            process::exit(1);
        };
    }

    Ok(())
}
