use std::cmp::max;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::{io, usize};

use aw_shuffle::persistent::rocksdb::Shuffler;
use aw_shuffle::persistent::PersistentShuffler;
use aw_shuffle::AwShuffler;
use clap::{Parser, Subcommand};
use rocksdb::{Options, DB};
use tempfile::tempdir;
use unicode_width::UnicodeWidthStr;

#[derive(clap::Parser)]
#[command(name = "strpick", about = "Selects random strings from stdin.")]
struct Opt {
    #[arg(long, value_parser)]
    /// The RocksDB database used for storing persistent data between runs.
    db: PathBuf,

    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Read strings from stdin and pick NUM of them, attempting to make them unique.
    /// If no strings are provided the DB will be read as-is.
    Pick { num: usize },
    /// Dump the current contents of the database to stdout.
    /// This will work on any aw-shuffler databases that store strings.
    Dump,
    /// Dump the contents of any valid aw-shuffler database.
    DumpRaw,
    /// Repair an existing database if rocksdb has corrupted itself.
    Repair,
}

fn main() {
    let opt = Opt::parse();


    match &opt.cmd {
        Command::Pick { num } => pick(&opt.db, *num),
        Command::Dump => dump(&opt.db, |v| {
            if let rmpv::Value::String(s) = v {
                s.as_str().unwrap().to_owned()
            } else {
                panic!("Item {} is not string", v)
            }
        }),
        Command::DumpRaw => dump(&opt.db, |v| v.to_string()),
        Command::Repair => repair(&opt.db),
    }
}

fn dump<F: Fn(rmpv::Value) -> String>(db: &Path, f: F) {
    let tdir = tempdir().unwrap();
    let mut options = Options::default();
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);

    let db = DB::open_as_secondary(&options, db, tdir.path()).unwrap();

    let mut contents = Vec::new();

    for (key, value) in db.iterator(rocksdb::IteratorMode::Start).flatten() {
        let k = rmpv::decode::value::read_value(&mut key.as_ref()).unwrap();
        let gen = rmpv::decode::value::read_value(&mut value.as_ref()).unwrap();

        let gen = if let rmpv::Value::Integer(g) = gen {
            g.as_u64().unwrap()
        } else {
            panic!("Generation not integer")
        };

        contents.push((f(k), gen));
    }

    print(contents);

    drop(db);
    drop(tdir);
}

fn print(mut vals: Vec<(String, u64)>) {
    vals.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

    let (kw, vw) = vals.iter().fold((0, 0), |(kw, vw), (s, g)| {
        let gw = if *g == 0 { 1 } else { (*g as f64).log10() as usize + 1 };
        (max(kw, UnicodeWidthStr::width(s.as_str())), max(vw, gw))
    });

    for (s, g) in vals {
        let padding = " ".repeat(kw - UnicodeWidthStr::width(s.as_str()));
        println!("{}{} | {2:>3$}", s, padding, g, vw);
    }
}

fn pick(db: &Path, num: usize) {
    let stdin = io::stdin();
    let strings: Vec<_> = stdin.lock().lines().flatten().collect();

    let strings = if !strings.is_empty() { Some(strings) } else { None };

    let mut s: Shuffler<String> = Shuffler::new_default(db, strings)
        .unwrap_or_else(|e| panic!("Failed to open the database at {:?}: {}", db, e));

    for s in s.try_unique_n(num).unwrap().into_iter().flatten() {
        println!("{}", s)
    }

    s.close_leak().unwrap();
}

fn repair(db: &Path) {
    let mut options = Options::default();
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);

    DB::repair(&options, db).unwrap();
}
