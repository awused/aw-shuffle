use std::convert::TryInto;
use std::time::{Duration, Instant};

use aw_shuffle::_secret_do_not_use::Rbtree;
use aw_shuffle::{AwShuffler, NewItemHandling, Shuffler};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::Rng;
use rand::distr::Uniform;
use rand::prelude::{Distribution, SliceRandom};

const CHARACTERS: &[u8] =
    b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456790123456789._-...";

// static SEQUENTIAL_COUNTS: &[usize] = &[1, 10, 100, 1000, 10000, 50000, 100_000, 500_000,
// 1_000_000];
static SEQUENTIAL_COUNTS: &[usize] = &[1000, 10000, 100_000, 1_000_000];

fn random_strings(n: usize) -> Vec<String> {
    let mut rng = rand::rng();

    (0..n)
        .map(|_| {
            (0..50)
                .map(|_| {
                    let idx = rng.random_range(0..CHARACTERS.len());
                    CHARACTERS[idx] as char
                })
                .collect()
        })
        .collect()
}

fn sequential_strings(n: usize) -> Vec<String> {
    let strlen = n.to_string().len();

    (0..n).map(|i| format!("{i:0strlen$}")).collect()
}

fn sequential_inserts(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_inserts");
    group.sample_size(10);

    for n in SEQUENTIAL_COUNTS {
        let strings = sequential_strings(*n);

        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, _s| {
            b.iter_custom(|iters| {
                let mut total = Duration::from_secs(0);

                for _i in 0..iters {
                    let input = strings.clone();
                    let start = Instant::now();
                    let mut rb = Rbtree::default();
                    input.into_iter().enumerate().for_each(|(i, s)| {
                        rb.insert(s, i.try_into().unwrap());
                    });

                    total += start.elapsed();
                }
                total
            })
        });
    }
}


fn shuffled_inserts(c: &mut Criterion) {
    let mut group = c.benchmark_group("shuffled_inserts");
    group.sample_size(10);
    let mut rng = rand::rng();

    for n in SEQUENTIAL_COUNTS {
        let strings = sequential_strings(*n);

        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, _s| {
            b.iter_custom(|iters| {
                let mut total = Duration::from_secs(0);

                for _i in 0..iters {
                    let mut input = strings.clone();
                    input.shuffle(&mut rng);

                    let start = Instant::now();
                    let mut rb = Rbtree::default();
                    input.into_iter().enumerate().for_each(|(i, s)| {
                        rb.insert(s, i.try_into().unwrap());
                    });

                    total += start.elapsed();
                }
                total
            })
        });
    }
}

fn insert_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_random");
    group.sample_size(10);

    for n in SEQUENTIAL_COUNTS {
        let strings = random_strings(*n);

        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, _s| {
            b.iter_custom(|iters| {
                let mut total = Duration::from_secs(0);

                for _i in 0..iters {
                    let input = strings.clone();
                    let start = Instant::now();
                    let mut rb = Rbtree::default();
                    input.into_iter().enumerate().for_each(|(i, s)| {
                        rb.insert(s, i.try_into().unwrap());
                    });

                    // drop(rb);
                    total += start.elapsed();
                }
                total
            })
        });
    }
}
fn sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("shuffled_insert_delete");
    group.sample_size(10);
    let mut rng = rand::rng();

    for n in SEQUENTIAL_COUNTS {
        let strings = sequential_strings(*n);

        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, _s| {
            b.iter_custom(|iters| {
                let mut total = Duration::from_secs(0);

                for _i in 0..iters {
                    let mut input = strings.clone();
                    input.shuffle(&mut rng);
                    let mut deletes = strings.clone();
                    deletes.shuffle(&mut rng);

                    let start = Instant::now();
                    let mut rb = Rbtree::default();
                    input.into_iter().enumerate().for_each(|(i, s)| {
                        rb.insert(s, i.try_into().unwrap());
                    });

                    deletes.iter().for_each(|s| {
                        rb.delete(s);
                    });
                    // drop(rb);
                    total += start.elapsed();
                }
                total
            })
        });
    }
}


fn find_next(c: &mut Criterion) {
    let mut group = c.benchmark_group("find_next");
    let mut rng = rand::rng();

    for n in SEQUENTIAL_COUNTS {
        let mut input = sequential_strings(*n);
        input.shuffle(&mut rng);

        let mut rb = Rbtree::default();
        input.into_iter().enumerate().for_each(|(i, s)| {
            rb.insert(s, i.try_into().unwrap());
        });

        let between = Uniform::try_from(0..*n).unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, _s| {
            b.iter(|| {
                rb.find_next(
                    between.sample(&mut rng),
                    between.sample(&mut rng).try_into().unwrap(),
                );
            })
        });
    }
}

fn shuffler_next(c: &mut Criterion) {
    let mut group = c.benchmark_group("shuffler_infallible_next");

    for n in SEQUENTIAL_COUNTS {
        let mut shuffler = Shuffler::new(2.0, NewItemHandling::NeverSelected);
        for s in sequential_strings(*n) {
            let _ignored = shuffler.add(s);
        }

        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, _s| {
            b.iter(|| {
                let _ignored = shuffler.next();
            })
        });
    }
}

criterion_group!(
    benches,
    sequential_inserts,
    shuffled_inserts,
    insert_random,
    sequential,
    find_next,
    shuffler_next,
);
criterion_main!(benches);
