Aw-Shuffle
==========

An efficient random shuffler for selecting items, providing weighted randomized
selection with replacement, favouring less-recently selected items, with optional
persistence using RocksDB. Supports live insertion and deletion of items. It's
meant for user-facing randomization such as shuffling songs in a media
player, where it's desirable to bias the selection without completely ruling out
picking the same item twice in a row.

Typical shuffle implementations in media players are complete randomization or
offline shuffling. With a library of a few thousand songs it can take an
unreasonably long time to hear every song with unbiased randomization. Making a
shuffled playlist is an offline algorithm that handles new songs poorly: either
requiring a full or partial reshuffle when new items are added. This library
avoids those pitfalls but has its own considerations.

Aw-Shuffle offers `O(log(n))` performance for operations on individual items and
`O(n)` memory usage while being a fully online algorithm that can accept adding
and removing items at any time. The algorithm is not suitable for cryptography
and it does not make any rigorous claims about its random distribution but its
output feels both random and fair in practice to a human user.

## Usage

```rust
use aw_shuffle::{Shuffler, InfallibleShuffler, AwShuffler};

let mut shuffler = Shuffler::default();

let items = vec![1, 2, 3, 4, 5];

items.into_iter().for_each(|i| {
    shuffler.add(i).unwrap();
});

// May be any of the numbers with no weighting, since no items have been selected before.
let next_number = match shuffler.next() {
    Ok(Some(num)) => num,
    Ok(None) => unreachable!(), // This shuffler is definitely non-empty.
    Err(e) => unreachable!(), // In-memory shufflers are infallible.
};

// Infallible methods skip the Result step when it is impossible.
let next_number = match shuffler.inf_next() {
    Some(num) => num,
    None => unreachable!(), // This shuffler is definitely non-empty.
};

// More likely to be one of the four not already selected.
let second_number = shuffler.next().unwrap().unwrap();

// This returns 3 items, where it's more likely that the items are from the 3 that haven't
// been previously selected. It's possible for this to have repeats.
let next_3_numbers = shuffler.next_n(3).unwrap().unwrap();

// Will contain repeats, but is not guaranteed to contain every single number.
let next_10_numbers = shuffler.next_n(10).unwrap().unwrap();

// Will not contain repeats.
let next_3_unique_numbers = shuffler.unique_n(3).unwrap().unwrap();

// Every number exactly once. After this, all numbers in the tree have the same generation
// assigned since there are only 5 numbers in this example.
let next_5_unique_numbers = shuffler.unique_n(5).unwrap().unwrap();

// Try to get 10 unique items, which will fail, but fall back to getting 10 non-unique
// items.
let try_unique_10 = shuffler.try_unique_n(10).unwrap().unwrap();
```

The [InfallibleShuffler] trait offers a more ergnonomic API for in-memory
shufflers that cannot return errors.

## Persistent Shufflers

Aw-Shuffler offers optional persistence through the
[`PersistentShuffler`](persistent::PersistentShuffler) trait. Currently the only
storage backend is RocksDB controlled by the `rocksdb` feature flag.

Use [`close`](persistent::PersistentShuffler::close) to safely close persistent
shufflers. If close is not called any errors will be lost on drop.

## Standalone Executable

The [strpick](https://github.com/awused/aw-shuffle/strpick) directory contains a standalone executable that can be used in shell scripts to select random strings. It reads newline separated strings from stdin and uses a RocksDB database for persistence between runs.

## How It Works

Builds an in-memory 1-dimensional min/max k-d tree and tracks the recency of each item by assigning each one a generation. Every time an item is selected, it gets assigned a new generation one higher than the previous maximum generation.

To select the next item it randomly selects a number between the maximum and minimum generations and picks a random index in the tree. Using that index it searches forward until it finds the first item with a generation older than that random number, wrapping around to the beginning of the tree if necessary. This biases the selector towards less recently selected items in `O(log(n))` time.

For the currently implemented [`rocksdb::Shuffler`](persistent::rocksdb::Shuffler) all database reads and writes are performed synchronously, but batching is used where appropriate to attempt to limit the impact of operations on many items.

## Limitations

All items need to be kept in memory.

This library does heavily bias towards picking less recently picked items, but not in a way that is easy to define mathematically. This library doesn't provide any guarantees of fairness and does not try to be random in a way that is useful for cryptography or generating passwords. The design goal was to give fast, weighted, results using an online algorithm that never spends `O(n)` time on any one operation.

The generations are stored as `u64`. In the extremely unlikely event of an overflow all generations are reset to 0. For the use cases this library is meant for, namely "human-facing" randomness, this is unlikely to ever be a problem.


