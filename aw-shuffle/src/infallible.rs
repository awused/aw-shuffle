use std::convert::Infallible;

use crate::{AwShuffler, Item};

#[allow(clippy::module_name_repetitions)]
/// In-memory shufflers are infallible. This interface simplifies usage when there are no
/// operations that can return errors.
pub trait InfallibleShuffler: AwShuffler {
    /// Adds the item to the shuffler.
    ///
    /// Returns true if the item was not already present.
    fn inf_add(&mut self, item: Self::Item) -> bool;

    /// Removes the item from the shuffler, returning it if it was present.
    fn inf_remove(&mut self, item: &Self::Item) -> Option<Self::Item>;

    /// Returns the next item from the shuffler, weighted based on recency and the configured bias.
    ///
    /// Returns `None` when the shuffler is empty.
    fn inf_next(&mut self) -> Option<&Self::Item>;

    /// Returns the next `n` items from the shuffler, weighted based on recency and the configured
    /// bias. This is not quite equivalent to calling next() `n` times. As `n` grows larger with
    /// respect to the number of items being shuffled, this approaches an unweighted random
    /// shuffle.
    ///
    /// All the returned items will be treated as having been selected at the same time for
    /// future calls.
    ///
    /// Returns `None` when the shuffler is empty, even if `n` is 0.
    fn inf_next_n(&mut self, n: usize) -> Option<Vec<&Self::Item>>;

    /// Returns the next `n` items from the shuffler, weighted based on recency and the configured
    /// bias. Items are guaranteed to be unique.
    ///
    /// All the returned items will be treated as having been selected at the same time for future
    /// calls.
    ///
    /// Returns `None` when the shuffler does not contain enough unique items to fulfill the
    /// request or when the shuffler is empty, even if `n` is 0.
    fn inf_unique_n(&mut self, n: usize) -> Option<Vec<&Self::Item>>;


    /// Returns the next `n` unique items, if enough unique items exist, otherwise returns the next
    /// `n` items ignoring uniqueness.
    ///
    /// This is functionally equivalent to calling [`inf_unique_n`](Self::inf_unique_n) then calling
    /// [`inf_next_n`](Self::inf_next_n) if it returned `Ok(None)`.
    ///
    /// Returns `Ok(None)` when the shuffler is empty.
    fn inf_try_unique_n(&mut self, n: usize) -> Option<Vec<&Self::Item>>;
}

impl<T: Item, S> InfallibleShuffler for S
where
    S: AwShuffler<Item = T, Error = Infallible>,
{
    fn inf_add(&mut self, item: Self::Item) -> bool {
        self.add(item).unwrap()
    }

    fn inf_remove(&mut self, item: &Self::Item) -> Option<Self::Item> {
        self.remove(item).unwrap()
    }

    fn inf_next(&mut self) -> Option<&Self::Item> {
        self.next().unwrap()
    }

    fn inf_next_n(&mut self, n: usize) -> Option<Vec<&Self::Item>> {
        self.next_n(n).unwrap()
    }

    fn inf_unique_n(&mut self, n: usize) -> Option<Vec<&Self::Item>> {
        self.unique_n(n).unwrap()
    }

    fn inf_try_unique_n(&mut self, n: usize) -> Option<Vec<&Self::Item>> {
        self.try_unique_n(n).unwrap()
    }
}
