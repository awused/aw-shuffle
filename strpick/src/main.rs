use std::cmp::max;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::{io, usize};

use aw_shuffle::persistent::rocksdb::Shuffler;
use aw_shuffle::persistent::PersistentShuffler;
use aw_shuffle::AwShuffler;
use rocksdb::{Options, DB};
use structopt::StructOpt;
use tempfile::tempdir;

#[derive(StructOpt)]
#[structopt(name = "strpick", about = "Selects random strings from stdin.")]
struct Opt {
    #[structopt(long)]
    /// The RocksDB database used for storing persistent data between runs.
    db: PathBuf,

    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(StructOpt)]
enum Command {
    /// Read strings from stdin and pick \[NUM\] of them, attempting to make them unique.
    /// If no strings are provided the DB will be read as-is.
    Pick { num: usize },
    /// Dump the current contents of the database to stdout.
    /// This will work on any aw-shuffler databases that store strings.
    Dump,
    /// Dump the contents of any valid aw-shuffler database.
    DumpRaw,
}

fn main() {
    let opt = Opt::from_args();


    match &opt.cmd {
        Command::Pick { num } => pick(&opt.db, *num),
        Command::Dump => dump(&opt.db, |v| match v {
            rmpv::Value::String(s) => s.as_str().unwrap().to_owned(),
            _ => panic!("Item {} is not string", v),
        }),
        Command::DumpRaw => dump(&opt.db, |v| v.to_string()),
    }
}

fn dump<F: Fn(rmpv::Value) -> String>(db: &Path, f: F) {
    let tdir = tempdir().unwrap();
    let mut options = Options::default();
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);

    let db = DB::open_as_secondary(&options, db, tdir.path()).unwrap();

    let mut contents = Vec::new();

    for (key, value) in db.iterator(rocksdb::IteratorMode::Start) {
        let k = rmpv::decode::value::read_value(&mut key.as_ref()).unwrap();
        let gen = rmpv::decode::value::read_value(&mut value.as_ref()).unwrap();

        let gen = match gen {
            rmpv::Value::Integer(g) => g.as_u64().unwrap(),
            _ => panic!("Generation not integer"),
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
        let gw = if *g == 0 {
            1
        } else {
            (*g as f64).log10() as usize + 1
        };
        (max(kw, s.len()), max(vw, gw))
    });

    for (s, g) in vals {
        println!("{0:1$} | {2:>3$}", s, kw, g, vw);
    }
}

fn pick(db: &Path, num: usize) {
    let stdin = io::stdin();
    let strings: Vec<_> = stdin.lock().lines().flatten().collect();

    let strings = if !strings.is_empty() {
        Some(strings)
    } else {
        None
    };

    let mut s: Shuffler<String> = Shuffler::new_default(db, strings)
        .unwrap_or_else(|e| panic!("Failed to open the database at {:?}: {}", db, e));

    for s in s.try_unique_n(num).unwrap().into_iter().flatten() {
        println!("{}", s)
    }

    s.close_leak().unwrap();
}