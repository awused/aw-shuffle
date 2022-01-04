//! Module containing shufflers that are backed by a persistent database.

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::{AwShuffler, NewItemHandling};

#[cfg(feature = "rocks")]
pub mod rocksdb;


/// The minimum set of traits any item needs to implement for use in a [`PersistentShuffler`].
///
/// It is a logic error for an item to be mutated in a way that changes its hash, equality, or its
/// serialized representation. Items may be lost if two unequal items serialize to the same
/// representation.
///
/// Changing the serialized representation may result in duplicate or stale values being
/// deserialied from the database in the future.
///
/// # Performance
/// Serialization and deserialization is expected to be fast and cheap, since it is performed on
/// many actions. If the `Item` is slow to serialize then it is best to store
/// the items externally and only use a minimal unique key as the item in the shuffler.
///
/// # Limitations
/// The backing database may impose a limit on the serialized size of each item. For
/// [`rocksdb::Shuffler`] the limit is 8MB, using MessagePack.
pub trait Item: super::Item + Serialize + DeserializeOwned {}
impl<I: super::Item + Serialize + DeserializeOwned> Item for I {}


#[allow(clippy::module_name_repetitions)]
/// The trait for [`AwShuffler`]s that store their state in a persistent database.
///
/// Most operations are the same but cause an additional database read or write.
///
/// It is a logic error for an [`Item`] to be mutated in a way that changes its hash, equality, or
/// its serialized representation.
///
/// See [`Item`] for documentation on performance concerns and limitations.
///
/// The [`PersistentShuffler`] should be closed to ensure all
/// updates have been flushed to disk. If the [`PersistentShuffler`] is not closed it will be
/// closed on drop, but any errors will be lost.
///
/// # Syncing with the Database
///
/// There are two ways to use a persistent shuffler. Either as a drop-in, durable
/// replacement for an in-memory [`AwShuffler`] where the database reflects the same state as the
/// in-memory shuffler 1:1, or allowing the in-memory shuffler to diverge from the database.
///
/// To use the shuffler as a regular shuffler that only saves its state between runs, use the
/// regular [`AwShuffler::add`] and [`AwShuffler::remove`] methods to manage items. Leave
/// [`Options::keep_unrecognized`] set to `false` when creating a shuffler.
///
/// When the set of items might change over time with items being removed and reintroduced, you can
/// make use of [`load`](Self::load) and [`soft_remove`](Self::soft_remove) in place of
/// [`add`](AwShuffler::add) and [`remove`](AwShuffler::remove). Setting
/// [`Options::keep_unrecognized`] to `true` and using [`soft_remove`](Self::soft_remove) will keep
/// items in the database for the future. Using [`load`](Self::load) will attempt to load items from
/// the database if they're present.
pub trait PersistentShuffler: AwShuffler
where
    Self::Item: Item,
{
    /// Add an item to the shuffler, preferring to read the item's data from the database when
    /// possible. If the item is not present in the database this is equivalent to calling
    /// [`add`](AwShuffler::add).
    ///
    /// This is only meaningful if the item has been removed with
    /// [`soft_remove`](Self::soft_remove) or kept on initialization with
    /// [`Options::keep_unrecognized`] set to `true`.
    ///
    /// Returns `true` if the item was not present in memory.
    fn load(&mut self, item: Self::Item) -> Result<bool, Self::Error>;

    /// Removes the item from the shuffler, returning it if it was present in memory. Does not
    /// remove the item from the underlying database, leaving it available for future runs or
    /// future [`load`](Self::load) calls.
    ///
    /// If an item has been removed with `soft_remove` then it cannot be removed from the database
    /// using [`remove`](AwShuffler::remove) alone, it will need to be added then removed, or
    /// cleared by a future shuffler initialized with [`Options::keep_unrecognized`] set to
    /// `true`.
    fn soft_remove(&mut self, item: &Self::Item) -> Result<Option<Self::Item>, Self::Error>;


    /// Flushes any pending changes to disk and runs any garbage collection or compaction routines
    /// for the underlying storage provider.
    ///
    /// Calling this is optional but may improve disk usage or performance. It is not automatically
    /// called, but the backing database may have its own automatic routines.
    fn compact(&mut self) -> Result<(), Self::Error>;

    /// Cleanly shut down the persistent shuffler and ensures all data is flushed to disk.
    ///
    /// If this is not called it will be called on drop, but any errors will be lost.
    fn close(self) -> Result<(), Self::Error>;

    /// Cleanly shut down the database connection but leak the in-memory shuffler.
    ///
    /// This can be used to defer cleanup until the process is terminated. It's only useful when
    /// leaking memory is no longer a concern.
    ///
    /// Hidden in docs because this is generally a bad idea.
    #[doc(hidden)]
    fn close_leak(self) -> Result<(), Self::Error>;
}

/// Options for initializing a [`PersistentShuffler`].
pub struct Options {
    bias: f64,
    new_item_handling: NewItemHandling,
    remove_on_deserialization_error: bool,
    keep_unrecognized: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            bias: 2.0,
            new_item_handling: NewItemHandling::NeverSelected,
            remove_on_deserialization_error: false,
            keep_unrecognized: false,
        }
    }
}

impl Options {
    /// Controls how strongly the shuffler is biased towards older items. See
    /// [`Shuffler::new`](crate::Shuffler::new).
    ///
    /// # Panics
    /// Panics if bias is negative or NaN.
    #[must_use]
    pub fn bias(mut self, bias: f64) -> Self {
        assert!(!bias.is_nan(), "bias {} cannot be NaN.", bias);
        assert!(bias.is_sign_positive(), "bias {} cannot be negative.", bias);
        self.bias = bias;
        self
    }

    /// See [`Shuffler::new`](crate::Shuffler::new)
    #[must_use]
    pub const fn new_item_handling(mut self, new_item_handling: NewItemHandling) -> Self {
        self.new_item_handling = new_item_handling;
        self
    }

    /// Controls how deserialization errors are handled. By default a key that can't be
    /// deserialized will be treated as an error. This guards against accidentally opening a
    /// database with the wrong type. The default value is `false`.
    ///
    /// Setting this to `true` will cause any keys that can't be deserialized to be removed from the
    /// database silently without exposing an error. The intended use case is for when the
    /// structure or serialized format is expected to change in a partially backwards-incompatible
    /// way.
    #[must_use]
    pub const fn remove_on_deserialization_error(
        mut self,
        remove_on_deserialization_error: bool,
    ) -> Self {
        self.remove_on_deserialization_error = remove_on_deserialization_error;
        self
    }

    /// Controls whether unrecognized items are removed from the database when creating a new
    /// Shuffler backed by an existing database.
    ///
    /// The default value is `false`.
    ///
    /// Setting this to `true` will cause any items not in the [`items`](rocksdb::Shuffler::new)
    /// vector to be removed from RocksDB. These unrecognized items are ignored until the database
    /// is reopened by a new Shuffler instance.
    #[must_use]
    pub const fn keep_unrecognized(mut self, keep_unrecognized: bool) -> Self {
        self.keep_unrecognized = keep_unrecognized;
        self
    }
}
