//! Module containing the [`PersistentShuffler`] backed by RocksDB.

use std::fmt::Display;
use std::hash::Hasher;
use std::mem::ManuallyDrop;
use std::path::Path;

use ahash::{AHashSet, AHasher};
use rand::prelude::StdRng;
use rand::Rng;
use rmp_serde::{decode, encode, Deserializer};
use rocksdb::IteratorMode::Start;
use rocksdb::{WriteBatch, DB};
use serde::Deserialize;

use super::{Item, Options, PersistentShuffler};
use crate::{AwShuffler, InfallibleShuffler, ShufflerGeneric as BaseShuffler};


/// A simple wrapper around the different sources of errors that can happen.
///
/// Once an error is returned the state of the in-memory shuffler is no longer guaranteed to be
/// in sync with the database and it should no longer be used.
#[derive(Debug)]
pub enum Error {
    /// An error during serialization when attempting to insert a key into the database.
    Serialization(encode::Error),
    /// An error during deserialization.
    ///
    /// When [`Options::remove_on_deserialization_error`] is set to true this will never be
    /// constructed.
    Deserialization(decode::Error),
    /// An error from a database operation.
    DB(rocksdb::Error),
}

impl From<encode::Error> for Error {
    fn from(e: encode::Error) -> Self {
        Self::Serialization(e)
    }
}

impl From<decode::Error> for Error {
    fn from(e: decode::Error) -> Self {
        Self::Deserialization(e)
    }
}

impl From<rocksdb::Error> for Error {
    fn from(e: rocksdb::Error) -> Self {
        Self::DB(e)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialization(e) => e.fmt(f),
            Self::Deserialization(e) => e.fmt(f),
            Self::DB(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Serialization(e) => e,
            Self::Deserialization(e) => e,
            Self::DB(e) => e,
        })
    }
}

/// A shuffler backed by RocksDB, where all database operations are completed synchronously.
///
/// See [`PersistentShuffler`] for more documentation.
pub struct ShufflerGeneric<T: Item, H: Hasher + Clone, R: Rng> {
    internal: ManuallyDrop<BaseShuffler<T, H, R>>,
    db: DB,
    closed: bool,
    leak: bool,
}

/// Type alias for [`ShufflerGeneric`] with the default hasher and rng implementations.
pub type Shuffler<T> = ShufflerGeneric<T, AHasher, StdRng>;


impl<T, H, R> PersistentShuffler for ShufflerGeneric<T, H, R>
where
    T: Item,
    H: Hasher + Clone,
    R: Rng,
{
    fn load(&mut self, item: Self::Item) -> Result<bool, Self::Error> {
        if self.internal.tree.find_node(&item).is_some() {
            return Ok(false);
        }

        match self.get(&item)? {
            Some(gen) => Ok(self.internal.tree.insert(item, gen)),
            None => self.add(item),
        }
    }

    fn soft_remove(&mut self, item: &Self::Item) -> Result<Option<Self::Item>, Self::Error> {
        Ok(self.internal.inf_remove(item))
    }

    fn compact(&mut self) -> Result<(), Self::Error> {
        self.db.compact_range::<&[u8], &[u8]>(None, None);
        self.db.flush().map_err(Into::into)
    }

    fn close(mut self) -> Result<(), Self::Error> {
        self.closed = true;
        self.db.flush()?;
        self.db.cancel_all_background_work(true);
        Ok(())
    }

    fn close_leak(mut self) -> Result<(), Self::Error> {
        self.leak = true;
        self.close()
    }
}

impl<T, H, R> AwShuffler for ShufflerGeneric<T, H, R>
where
    T: Item,
    H: Hasher + Clone,
    R: Rng,
{
    type Error = Error;
    type Item = T;

    fn add(&mut self, item: Self::Item) -> Result<bool, Self::Error> {
        let gen = self.internal.add_generation();

        Self::put_batch(&self.db, &[&item], gen)?;
        Ok(self.internal.tree.insert(item, gen))
    }

    fn remove(&mut self, item: &Self::Item) -> Result<Option<Self::Item>, Self::Error> {
        let removed = self.internal.inf_remove(item);
        if removed.is_some() {
            self.delete(item)?;
        }
        Ok(removed)
    }

    fn next(&mut self) -> Result<Option<&Self::Item>, Self::Error> {
        let (gen, reset) = self.internal.next_generation();
        if reset {
            self.handle_reset()?;
        }

        let next = self.internal.inf_next();
        if let Some(next) = next {
            Self::put_batch(&self.db, &[next], gen.get())?;
        }
        Ok(next)
    }

    fn next_n(&mut self, n: usize) -> Result<Option<Vec<&Self::Item>>, Self::Error> {
        let (gen, reset) = self.internal.next_generation();
        if reset {
            self.handle_reset()?;
        }

        let next = self.internal.inf_next_n(n);
        if let Some(next) = &next {
            Self::put_batch(&self.db, next, gen.get())?;
        }
        Ok(next)
    }

    fn unique_n(&mut self, n: usize) -> Result<Option<Vec<&Self::Item>>, Self::Error> {
        let (gen, reset) = self.internal.next_generation();
        if reset {
            self.handle_reset()?;
        }

        let next = self.internal.inf_unique_n(n);
        if let Some(next) = &next {
            Self::put_batch(&self.db, next, gen.get())?;
        }
        Ok(next)
    }

    fn size(&self) -> usize {
        self.internal.size()
    }

    fn values(&self) -> Vec<&Self::Item> {
        self.internal.values()
    }

    fn dump(&self) -> Vec<(&Self::Item, u64)> {
        self.internal.dump()
    }
}

impl<T, H, R> Drop for ShufflerGeneric<T, H, R>
where
    T: Item,
    H: Hasher + Clone,
    R: Rng,
{
    fn drop(&mut self) {
        if !self.closed {
            drop(self.db.flush());
            self.db.cancel_all_background_work(false);
        }
        if !self.leak {
            unsafe {
                // Trivially safe, we're dropping this from within the destructor for the owning
                // struct.
                ManuallyDrop::drop(&mut self.internal);
            }
        }
    }
}


impl<T, H, R> ShufflerGeneric<T, H, R>
where
    T: Item,
    H: Hasher + Clone,
    R: Rng,
{
    fn get(&mut self, item: &T) -> Result<Option<u64>, Error> {
        let key = encode::to_vec(item)?;

        match self.db.get_pinned(key)? {
            Some(value) => Ok(Some(u64::deserialize(&mut Deserializer::new(&*value))?)),
            None => Ok(None),
        }
    }

    fn load_all(
        db: &DB,
        internal: &mut BaseShuffler<T, H, R>,
        remove_error: bool,
        keep_unrecognized: bool,
        items: Option<Vec<T>>,
    ) -> Result<(), Error> {
        let mut batch = WriteBatch::default();

        let mut valid: Option<AHashSet<_>> = items.map(|v| v.into_iter().collect());

        for r in db.iterator(Start) {
            let (key, value) = match r {
                Ok((k, v)) => (k, v),
                Err(e) => return Err(e.into()),
            };

            // Fallibly deserialize every key and value pair
            let item = match T::deserialize(&mut Deserializer::new(&*key)) {
                Ok(k) => k,
                Err(e) => {
                    if remove_error {
                        batch.delete(key);
                        continue;
                    }
                    return Err(e.into());
                }
            };

            let gen = match u64::deserialize(&mut Deserializer::new(&*value)) {
                Ok(g) => g,
                Err(e) => {
                    if remove_error {
                        batch.delete(key);
                        continue;
                    }
                    return Err(e.into());
                }
            };

            // Add it to the tree if it's a valid item, otherwise plan to delete it.
            if let Some(valid) = &mut valid {
                if let Some(item) = valid.take(&item) {
                    internal.tree.insert(item, gen);
                } else {
                    batch.delete(key);
                }
            } else {
                internal.tree.insert(item, gen);
            }
        }

        if keep_unrecognized {
            batch.clear();
        }

        // Add all of the new items to the tree
        for item in valid.into_iter().flatten() {
            let gen = internal.add_generation();

            let key = encode::to_vec(&item)?;
            let value = encode::to_vec(&gen)?;
            batch.put(key, value);

            internal.tree.insert(item, gen);
        }

        if !batch.is_empty() {
            db.write(batch)?;
        }
        Ok(())
    }

    fn put_batch(db: &DB, items: &[&T], gen: u64) -> Result<(), Error> {
        let gen = encode::to_vec(&gen)?;

        let mut batch = WriteBatch::default();

        for item in items {
            let key = encode::to_vec(*item)?;

            batch.put(key, &gen);
        }

        db.write(batch).map_err(Into::into)
    }

    fn handle_reset(&self) -> Result<(), Error> {
        Self::put_batch(&self.db, &self.values(), 0)
    }

    fn delete(&self, item: &T) -> Result<(), Error> {
        let key = encode::to_vec(item)?;

        self.db.delete(key).map_err(Into::into)
    }
}


impl<T: Item> Shuffler<T> {
    /// Creates a new [`Shuffler`] pointing to the given RocksDB database with default behaviour.
    ///
    /// The database will be created if it does not exist, but any missing parent directories will
    /// not be created.
    ///
    /// All items and data tracking how recently they were selected will be loaded from the
    /// database.
    ///
    /// If `items` is not `None` then it will be taken as the set of valid items. Any items present
    /// in the database that are not present in `items` will be removed, as if by calling
    /// [`remove`](AwShuffler::remove). Any items in `items` that are not present in the database
    /// will be added as if by calling [`add`](AwShuffler::add). Using `items` is more efficient
    /// than calling [`values`](AwShuffler::values) to manually add and remove items.
    pub fn new_default<P: AsRef<Path>>(path: P, items: Option<Vec<T>>) -> Result<Self, Error> {
        Self::new(path, Options::default(), items)
    }

    /// Creates a new [`Shuffler`] pointing to the given RocksDB database.
    ///
    /// The database will be created if it does not exist, but any missing parent directories will
    /// not be created.
    ///
    /// See the documentation for [`Shuffler::new`](crate::Shuffler::new) and [`Options`].
    ///
    /// See [`new_default`](Self::new_default) for an explanation of `items`.
    ///
    /// # Panics
    /// Panics if given a negative or NaN value in `options.bias`.
    pub fn new<P: AsRef<Path>>(
        path: P,
        options: Options,
        items: Option<Vec<T>>,
    ) -> Result<Self, Error> {
        let mut db_options = rocksdb::Options::default();
        db_options.set_max_open_files(100);
        db_options.set_compression_type(rocksdb::DBCompressionType::Lz4);
        db_options.create_if_missing(true);
        db_options.create_missing_column_families(true);
        // Much more efficient on slower storage, probably minimal impact on fast storage.
        db_options.set_compaction_readahead_size(2 * 1024 * 1024);
        db_options.set_keep_log_file_num(10);

        let db = DB::open(&db_options, path)?;

        let mut internal = crate::Shuffler::new(options.bias, options.new_item_handling);

        Self::load_all(
            &db,
            &mut internal,
            options.remove_on_deserialization_error,
            options.keep_unrecognized,
            items,
        )?;

        let shuffler = Self {
            internal: ManuallyDrop::new(internal),
            db,
            closed: false,
            leak: false,
        };

        Ok(shuffler)
    }
}


impl<T, H, R> crate::private::Sealed for ShufflerGeneric<T, H, R>
where
    T: Item,
    H: Hasher + Clone,
    R: Rng,
{
}
