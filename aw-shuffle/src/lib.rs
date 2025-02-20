#![warn(missing_docs)]
#![warn(unsafe_op_in_unsafe_fn)]
#![doc = include_str!("../../README.md")]
use std::convert::Infallible;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::num::NonZeroU64;

use ahash::AHasher;
use rand::distr::Uniform;
use rand::prelude::{Distribution, StdRng};
use rand::{Rng, SeedableRng};
use rbtree::{Node, Rbtree};

mod infallible;
#[cfg(feature = "persistent")]
pub mod persistent;
mod rbtree;

pub use infallible::*;

#[doc(hidden)]
// Just for benchmarking
pub mod _secret_do_not_use {
    pub use super::rbtree::*;
}

/// The minimum set of traits any item needs to implement for use in the shuffler.
///
/// It is a logic error for an item to be mutated in a way that changes its hash or equality.
pub trait Item: Hash + Eq + Ord {}
impl<T: Hash + Eq + Ord> Item for T {}

/// The generic trait all shufflers implement.
///
/// It is a logic error for an [`Item`] to be mutated in a way that changes its hash or equality.
///
/// See also [`InfallibleShuffler`] and [`persistent::PersistentShuffler`].
pub trait AwShuffler: private::Sealed {
    /// The type of item stored in the shuffler. All items are stored in memory.
    type Item: Item;
    /// The type of errors returned by the shuffler on failure.
    type Error: Error;

    /// Adds the item to the shuffler. How the item is handled is controlled by the value of
    /// [`NewItemHandling`] set when creating the shuffler.
    ///
    /// Returns `true` if the item was not already present.
    ///
    /// For [`PersistentShuffler`](persistent::PersistentShuffler)s this does not query the
    /// database. See [`PersistentShuffler::load`](persistent::PersistentShuffler::load) for an
    /// alternative that does read from the database.
    fn add(&mut self, item: Self::Item) -> Result<bool, Self::Error>;

    /// Removes the item from the shuffler, returning it if it was present.
    ///
    /// For [`PersistentShuffler`](persistent::PersistentShuffler)s this immediately removes the
    /// item from the database. See
    /// [`PersistentShuffler::soft_remove`](persistent::PersistentShuffler::soft_remove) for an
    /// alternative that does retain the item in the database for the future.
    fn remove(&mut self, item: &Self::Item) -> Result<Option<Self::Item>, Self::Error>;

    /// Returns the next item from the shuffler, weighted based on recency and the configured bias.
    ///
    /// Returns `Ok(None)` when the shuffler is empty.
    fn next(&mut self) -> Result<Option<&Self::Item>, Self::Error>;

    /// Returns the next `n` items from the shuffler, weighted based on recency and the configured
    /// bias. This is not quite equivalent to calling next() `n` times. As `n` grows larger with
    /// respect to the number of items being shuffled, this approaches an unweighted random
    /// shuffle.
    ///
    /// All the returned items will be treated as having been selected at the same time for
    /// future calls.
    ///
    /// Returns `Ok(None)` when the shuffler is empty, even if `n` is 0.
    fn next_n(&mut self, n: usize) -> Result<Option<Vec<&Self::Item>>, Self::Error>;

    /// Returns the next `n` items from the shuffler, weighted based on recency and the configured
    /// bias. Items are guaranteed to be unique.
    ///
    /// All the returned items will be treated as having been selected at the same time for future
    /// calls.
    ///
    /// Returns `Ok(None)` when the shuffler does not contain enough unique items to fulfill the
    /// request or when the shuffler is empty, even if `n` is 0.
    fn unique_n(&mut self, n: usize) -> Result<Option<Vec<&Self::Item>>, Self::Error>;

    /// Returns the next `n` unique items, if enough unique items exist, otherwise returns the next
    /// `n` items ignoring uniqueness.
    ///
    /// This is functionally equivalent to calling [`unique_n`](Self::unique_n) then calling
    /// [`next_n`](Self::next_n) if it returned `Ok(None)`.
    ///
    /// Returns `Ok(None)` when the shuffler is empty.
    fn try_unique_n(&mut self, n: usize) -> Result<Option<Vec<&Self::Item>>, Self::Error> {
        let s = self.size();
        if s == 0 || s < n { self.next_n(n) } else { self.unique_n(n) }
    }

    /// Returns the number of items currently in the shuffler.
    fn size(&self) -> usize;

    /// Returns all of the values currently in the shuffler in no specific order.
    ///
    /// For [`PersistentShuffler`](persistent::PersistentShuffler)s this only counts the items
    /// currently loaded in memory. See the documentation for persistent shufflers for more
    /// information.
    fn values(&self) -> Vec<&Self::Item>;

    /// Consumes the shuffler and returns all the items in no specific order.
    ///
    /// For [`PersistentShuffler`](persistent::PersistentShuffler)s this only counts the items
    /// currently loaded in memory. See the documentation for persistent shufflers for more
    /// information.
    fn into_values(self) -> Vec<Self::Item>;

    /// Returns all of the values currently in the shuffler and their generations in no specific
    /// order.
    ///
    /// The generation is not really meaningful on its own but is useful for satisfying curiosity.
    ///
    /// For [`PersistentShuffler`](persistent::PersistentShuffler)s this only counts the items
    /// currently loaded in memory. See the documentation for persistent shufflers for more
    /// information.
    fn dump(&self) -> Vec<(&Self::Item, u64)>;
}

mod private {
    use std::hash::Hasher;

    use rand::Rng;

    use crate::{Item, ShufflerGeneric};

    pub trait Sealed {}

    impl<T: Item, H: Hasher + Clone, R: Rng> Sealed for ShufflerGeneric<T, H, R> {}
}

/// How items should be treated when they're first added to the shuffler.
#[derive(Debug)]
pub enum NewItemHandling {
    /// Treat new items as if they had never been selected, making them very likely to be selected
    /// next. Gives new items the same weight as the least recently selected item.
    NeverSelected,
    /// Treat new items as if they were just selected, making them very unlikely to be chosen next.
    /// Gives new items the same weight as the most recently selected item.
    RecentlySelected,
    /// Randomly distribute the weights of new items so they're neither likely nor unlikely to be
    /// selected.
    Random,
}

/// Standard in-memory shuffler with no persistence. All data tracking how recently items were
/// selected only lives as long as this struct.
///
/// See the documentation for [`AwShuffler`] and [`InfallibleShuffler`] for more information.
#[derive(Debug)]
pub struct ShufflerGeneric<T, H, R> {
    pub(crate) tree: Rbtree<T, H>,
    rng: R,
    bias: f64,
    new_items: NewItemHandling,
}


/// Type alias for [`ShufflerGeneric`] with the default hasher and rng implementations.
pub type Shuffler<T> = ShufflerGeneric<T, AHasher, StdRng>;


impl<T: Item> Default for Shuffler<T> {
    fn default() -> Self {
        Self {
            tree: Rbtree::default(),
            rng: StdRng::from_os_rng(),
            bias: 2.0,
            new_items: NewItemHandling::NeverSelected,
        }
    }
}

impl<T> Shuffler<T> {
    /// Creates a new Shuffler with a given bias and handling behaviour for new items.
    ///
    /// `bias` controls how strongly the shuffler biases itself towards less recently selected
    /// items, with larger values more strongly. `bias` must be non-negative and not a NaN value. A
    /// value of 0 means the shuffler ignores how recently selected items were while a value of
    /// `f64::INFINITY` will cause it to only return the least-recently selected items. The default
    /// `bias` is 2.0.
    ///
    /// # Panics
    /// Panics if given a negative or NaN bias.
    #[must_use]
    pub fn new(bias: f64, new_item_handling: NewItemHandling) -> Self {
        assert!(!bias.is_nan(), "bias {bias} cannot be NaN.");
        assert!(bias.is_sign_positive(), "bias {bias} cannot be negative.");

        Self {
            tree: Rbtree::default(),
            rng: StdRng::from_os_rng(),
            bias,
            new_items: new_item_handling,
        }
    }
}

impl<T, H, R> ShufflerGeneric<T, H, R>
where
    T: Item,
    H: Hasher + Clone,
    R: Rng,
{
    /// Creates a new Shuffler with a given bias and handling behaviour for new items, with a
    /// custom hasher and Rng implementation.
    ///
    /// `bias` controls how strongly the shuffler biases itself towards less recently selected
    /// items, with larger values more strongly. `bias` must be non-negative and not a NaN value. A
    /// value of 0 means the shuffler ignores how recently selected items were while a value of
    /// `f64::INFINITY` will cause it to only return the least-recently selected items. The default
    /// `bias` is 2.0.
    ///
    /// # Panics
    /// Panics if given a negative or NaN bias.
    #[must_use]
    #[allow(dead_code)]
    fn new_custom(bias: f64, new_item_handling: NewItemHandling, hasher: H, rng: R) -> Self {
        assert!(!bias.is_nan(), "bias {bias} cannot be NaN.");
        assert!(bias.is_sign_positive(), "bias {bias} cannot be negative.");

        Self {
            tree: Rbtree::new(hasher),
            rng,
            bias,
            new_items: new_item_handling,
        }
    }

    fn add_generation(&mut self) -> u64 {
        let (min_gen, max_gen) = self.tree.generations();

        match self.new_items {
            NewItemHandling::NeverSelected => min_gen,
            NewItemHandling::RecentlySelected => max_gen,
            // TODO -- there is an opportunity to cache this range as a Uniform for multiple uses
            // when inserting many values at once.
            NewItemHandling::Random => self.rng.random_range(min_gen..=max_gen),
        }
    }

    fn next_generation(&mut self) -> (NonZeroU64, bool) {
        let (_, max_gen) = self.tree.generations();
        unsafe {
            if max_gen != u64::MAX {
                // trivially safe
                (NonZeroU64::new_unchecked(max_gen + 1), false)
            } else {
                // This branch will almost never be taken
                self.tree.reset();
                (NonZeroU64::new_unchecked(1), true)
            }
        }
    }

    fn random_generation(&mut self) -> u64 {
        let (min_gen, max_gen) = self.tree.generations();
        self.random_generation_internal(min_gen, max_gen)
    }

    fn random_generation_below(&mut self, limit: NonZeroU64) -> u64 {
        let (min_gen, mut max_gen) = self.tree.generations();
        if max_gen == limit.get() {
            max_gen = limit.get() - 1;
            assert!(max_gen >= min_gen);
        }
        self.random_generation_internal(min_gen, max_gen)
    }

    fn random_generation_internal(&mut self, min_gen: u64, max_gen: u64) -> u64 {
        if min_gen == max_gen {
            return max_gen;
        }

        let span = max_gen - min_gen;
        // Generates in the range [0, 1)
        let biased = self.rng.random::<f64>().powf(self.bias);
        let mut offset = (span.saturating_add(1) as f64 * biased).floor() as u64;

        if offset > span {
            // Should never happen
            offset = span;
        }

        min_gen + offset
    }
}

impl<T, H, R> AwShuffler for ShufflerGeneric<T, H, R>
where
    T: Item,
    H: Hasher + Clone,
    R: Rng,
{
    type Error = Infallible;
    type Item = T;

    fn add(&mut self, item: Self::Item) -> Result<bool, Self::Error> {
        let g = self.add_generation();
        Ok(self.tree.insert(item, g))
    }

    fn remove(&mut self, item: &Self::Item) -> Result<Option<Self::Item>, Self::Error> {
        let removed = self.tree.delete(item).map(|(removed, _)| removed);
        Ok(removed)
    }

    fn next(&mut self) -> Result<Option<&Self::Item>, Self::Error> {
        let size = self.tree.size();
        if size == 0 {
            return Ok(None);
        }

        let random_gen = self.random_generation();
        let index = self.rng.random_range(0..size);

        let node = self.tree.find_next(index, random_gen);
        let (next_gen, _) = self.next_generation();

        Node::set_generation(node, next_gen.get());

        unsafe { Ok(Some(node.as_ref().get())) }
    }

    fn next_n(&mut self, n: usize) -> Result<Option<Vec<&Self::Item>>, Self::Error> {
        let size = self.tree.size();
        if size == 0 {
            return Ok(None);
        }

        // Won't fail, size > 0
        let index_range = Uniform::new(0, size).unwrap();
        let mut selected = Vec::with_capacity(n);

        let (next_gen, _) = self.next_generation();
        // It's possible to have reset the tree here but it's not worth optimizing for.

        for _ in 0..n {
            let random_gen = self.random_generation();
            let index = index_range.sample(&mut self.rng);

            let node = self.tree.find_next(index, random_gen);

            // Set the generation here to try to prioritize other items.
            Node::set_generation(node, next_gen.get());

            selected.push(node)
        }


        let output = selected.into_iter().map(|n| unsafe { n.as_ref().get() }).collect();

        Ok(Some(output))
    }

    fn unique_n(&mut self, n: usize) -> Result<Option<Vec<&Self::Item>>, Self::Error> {
        let size = self.tree.size();
        if size == 0 || size < n {
            return Ok(None);
        }

        // Won't fail, size > 0
        let index_range = Uniform::new(0, size).unwrap();
        let mut selected = Vec::with_capacity(n);

        let (next_gen, _) = self.next_generation();
        // It's possible to have reset the tree here but it's not worth optimizing for.

        for _ in 0..n {
            let random_gen = self.random_generation_below(next_gen);
            let index = index_range.sample(&mut self.rng);

            let node = self.tree.find_next(index, random_gen);

            // Set the generation here to try to prioritize other items.
            Node::set_generation(node, next_gen.get());

            selected.push(node)
        }


        let output = selected.into_iter().map(|n| unsafe { n.as_ref().get() }).collect();

        Ok(Some(output))
    }

    fn size(&self) -> usize {
        self.tree.size()
    }

    fn values(&self) -> Vec<&Self::Item> {
        self.tree.values()
    }

    fn into_values(self) -> Vec<Self::Item> {
        self.tree.into_values()
    }

    fn dump(&self) -> Vec<(&Self::Item, u64)> {
        self.tree.dump()
    }
}

#[cfg(test)]
mod tests {
    use rand::RngCore;

    use crate::rbtree::Rbtree;
    use crate::{AwShuffler, InfallibleShuffler, NewItemHandling, ShufflerGeneric};


    #[derive(Default)]
    struct AlwaysOldestRnd {}

    impl RngCore for AlwaysOldestRnd {
        fn next_u32(&mut self) -> u32 {
            // With Lemire's method in rand, returning 0 here will always fail
            rand::rng().next_u32()
        }

        fn next_u64(&mut self) -> u64 {
            0
        }

        fn fill_bytes(&mut self, _dest: &mut [u8]) {
            unimplemented!()
        }
    }

    #[test]
    fn empty() {
        let mut shuffler = ShufflerGeneric::default();

        assert_eq!(shuffler.size(), 0);
        assert!(shuffler.values().is_empty());
        assert!(shuffler.next().unwrap().is_none());
        assert!(shuffler.next_n(0).unwrap().is_none());
        assert!(shuffler.next_n(10).unwrap().is_none());
        assert!(shuffler.unique_n(0).unwrap().is_none());
        assert!(shuffler.unique_n(10).unwrap().is_none());
        assert!(shuffler.remove(&0).unwrap().is_none());

        assert!(shuffler.inf_next().is_none());
        assert!(shuffler.inf_next_n(0).is_none());
        assert!(shuffler.inf_next_n(10).is_none());
        assert!(shuffler.inf_unique_n(0).is_none());
        assert!(shuffler.inf_unique_n(10).is_none());
        assert!(shuffler.inf_remove(&0).is_none());
        assert_eq!(shuffler.tree.generations().1, 0);
    }

    #[test]
    fn one_item_fal() {
        let mut shuffler = ShufflerGeneric::default();

        assert!(shuffler.add(0).unwrap());
        assert!(!shuffler.add(0).unwrap());

        assert_eq!(shuffler.size(), 1);
        assert_eq!(shuffler.values()[0], &0);
        assert_eq!(shuffler.tree.generations(), (0, 0));
        assert_eq!(shuffler.next().unwrap().unwrap(), &0);
        assert_eq!(shuffler.tree.generations(), (1, 1));
        assert!(shuffler.next_n(0).unwrap().unwrap().is_empty());
        assert_eq!(shuffler.tree.generations(), (1, 1));

        let n = shuffler.next_n(1).unwrap().unwrap();
        assert_eq!(n.len(), 1);
        assert_eq!(n[0], &0);
        assert_eq!(shuffler.tree.generations(), (2, 2));

        let n = shuffler.next_n(2).unwrap().unwrap();
        assert_eq!(n.len(), 2);
        assert_eq!((n[0], n[1]), (&0, &0));
        assert_eq!(shuffler.tree.generations(), (3, 3));

        assert!(shuffler.unique_n(0).unwrap().unwrap().is_empty());

        let n = shuffler.unique_n(1).unwrap().unwrap();
        assert_eq!(n.len(), 1);
        assert_eq!(n[0], &0);
        assert_eq!(shuffler.tree.generations(), (4, 4));
        assert!(shuffler.unique_n(2).unwrap().is_none());

        assert_eq!(shuffler.remove(&0).unwrap().unwrap(), 0);
        assert_eq!(shuffler.tree.generations(), (0, 0));

        assert!(shuffler.remove(&0).unwrap().is_none());
    }

    #[test]
    fn one_item_inf() {
        let mut shuffler = ShufflerGeneric::default();

        assert!(shuffler.add(0).unwrap());

        assert_eq!(shuffler.inf_next().unwrap(), &0);
        assert!(shuffler.inf_next_n(0).unwrap().is_empty());
        assert_eq!(shuffler.tree.generations(), (1, 1));

        let n = shuffler.inf_next_n(1).unwrap();
        assert_eq!(n.len(), 1);
        assert_eq!(n[0], &0);
        assert_eq!(shuffler.tree.generations(), (2, 2));

        let n = shuffler.inf_next_n(2).unwrap();
        assert_eq!(n.len(), 2);
        assert_eq!((n[0], n[1]), (&0, &0));
        assert_eq!(shuffler.tree.generations(), (3, 3));

        assert!(shuffler.inf_unique_n(0).unwrap().is_empty());

        let n = shuffler.inf_unique_n(1).unwrap();
        assert_eq!(n.len(), 1);
        assert_eq!(n[0], &0);
        assert_eq!(shuffler.tree.generations(), (4, 4));
        assert!(shuffler.inf_unique_n(2).is_none());

        assert_eq!(shuffler.inf_remove(&0).unwrap(), 0);
        assert_eq!(shuffler.tree.generations(), (0, 0));

        assert!(shuffler.inf_remove(&0).is_none());
    }

    #[test]
    fn fuzz_always_oldest() {
        let mut shuffler = ShufflerGeneric {
            tree: Rbtree::new_dummy(&[]),
            rng: AlwaysOldestRnd::default(),
            bias: f64::INFINITY,
            new_items: NewItemHandling::NeverSelected,
        };

        assert!(shuffler.add("a").is_ok());
        assert!(shuffler.add("b").is_ok());
        assert!(shuffler.add("c").is_ok());
        assert!(shuffler.add("d").is_ok());
        assert!(shuffler.add("e").is_ok());
        assert!(shuffler.add("f").is_ok());

        // Since we're always selecting the oldest ones, we should always get unique elements
        for _ in 0..100 {
            let younger = shuffler.next_n(3).unwrap().unwrap();
            assert_eq!(younger.len(), 3);
            let younger: Vec<_> = younger.into_iter().copied().collect();

            let older = shuffler.next_n(3).unwrap().unwrap();
            assert_eq!(older.len(), 3);

            for old in older {
                assert!(!younger.contains(old));
            }

            let (min_gen, max_gen) = shuffler.tree.generations();
            assert_eq!(min_gen, max_gen - 1);

            // This should force it to select all items and, in doing so, set all generations to
            // the same value
            let _unused = shuffler.next_n(6).unwrap().unwrap();

            let (min_gen, max_gen) = shuffler.tree.generations();
            assert_eq!(min_gen, max_gen);
        }
    }
}
