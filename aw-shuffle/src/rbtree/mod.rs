#![allow(missing_docs)]

use std::cell::UnsafeCell;
use std::cmp::{Ordering, max, min};
use std::fmt::Debug;
use std::hash::{BuildHasher, Hasher};
use std::mem::swap;
use std::ptr::NonNull;

use ahash::{AHasher, RandomState};

use crate::Item;

// This was originally written in Go, translated to a version using Rc<RefCell<>>, debugged and
// fuzzed, then converted into this code.

// Shrink the arena when it is less loaded than this
const MIN_LOAD_FACTOR: f64 = 0.5;

struct Arena<T> {
    vec: Vec<UnsafeCell<Node<T>>>,
}

impl<T> Default for Arena<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Debug for Arena<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Arena").field("vec_len", &self.vec.len()).finish()
    }
}

impl<T> Arena<T> {
    const fn new() -> Self {
        Self { vec: Vec::new() }
    }

    fn alloc(
        &mut self,
        item: T,
        hash: u64,
        generation: u64,
        red: bool,
        parent: Option<usize>,
    ) -> usize {
        let index = self.vec.len();
        let node = UnsafeCell::new(Node {
            item,
            hash,
            generation,
            red,
            children: 0,
            min_gen: generation,
            max_gen: generation,

            index,
            parent,
            left: None,
            right: None,
        });

        self.vec.push(node);
        index
    }

    // deallocates the node at i
    // Returns true if the root was moved to position i.
    // Undefined behaviour if the node is referenced by any other nodes.
    unsafe fn dealloc(&mut self, i: usize) -> (bool, Node<T>) {
        let old = self.vec.len() - 1;
        if i == old {
            (false, self.vec.pop().unwrap().into_inner())
        } else {
            self.vec.swap(i, old);
            let removed = self.vec.pop().unwrap().into_inner();

            if self.vec.capacity() > 100 {
                let fill = self.vec.len() as f64 / self.vec.capacity() as f64;

                if fill <= MIN_LOAD_FACTOR {
                    self.vec.shrink_to_fit();
                    // let newcap = (self.vec.capacity() as f64 * SHRINK_LOAD_FACTOR).round() as
                    // usize; self.vec.shrink_to(MIN_LOAD_FACTOR);
                }
            }

            unsafe {
                (*self.vec[i].get()).index = i;

                if let Some(left) = (*self.vec[i].get()).left {
                    self.vec[left].get_mut().parent = Some(i);
                }
                if let Some(right) = (*self.vec[i].get()).right {
                    self.vec[right].get_mut().parent = Some(i);
                }

                if let Some(parent) = (*self.vec[i].get()).parent {
                    let p = self.get_mut(parent);

                    if (*p).is_left_child(old) {
                        (*p).left = Some(i);
                    } else {
                        (*p).right = Some(i);
                    }
                    (false, removed)
                } else {
                    (true, removed)
                }
            }
        }
    }

    fn get(&self, i: usize) -> &Node<T> {
        unsafe { &*self.vec.get_unchecked(i).get() }
    }

    // Cursed, but safe as long as we never call get_mut twice on the same node
    unsafe fn get_mut(&self, i: usize) -> &mut Node<T> {
        unsafe { self.vec.get_unchecked(i).get().as_mut().unwrap() }
    }
}

pub struct Node<T> {
    pub(crate) item: T,
    hash: u64,
    generation: u64,
    red: bool,
    children: usize,
    min_gen: u64,
    max_gen: u64,

    pub(crate) index: usize,
    parent: Option<usize>,
    left: Option<usize>,
    right: Option<usize>,
}

impl<T: Ord> Ord for Node<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.hash.cmp(&other.hash).then_with(|| self.item.cmp(&other.item))
    }
}

impl<T: Ord> PartialOrd for Node<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Eq> Eq for Node<T> {}

impl<T: PartialEq> PartialEq for Node<T> {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.item == other.item
    }
}

impl<T: Debug> Debug for Node<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("item", &self.item)
            .field("hash", &self.hash)
            .field("gen", &self.generation)
            .field("red", &self.red)
            .field("children", &self.children)
            .field("min_gen", &self.min_gen)
            .field("max_gen", &self.max_gen)
            .finish()
    }
}

enum SoleRedChild {
    Right(usize),
    Left(usize),
    None,
}

impl<T> Node<T> {
    #[inline]
    pub(crate) const fn get(&self) -> &T {
        &self.item
    }

    #[inline]
    pub(crate) const fn index(&self) -> usize {
        self.index
    }

    fn other_child(&self, c: usize) -> Option<usize> {
        if self.is_left_child(c) { self.right } else { self.left }
    }

    fn is_left_child(&self, c: usize) -> bool {
        if let Some(left) = self.left { c == left } else { false }
    }

    unsafe fn has_red_child(&self, a: &Arena<T>) -> bool {
        if let Some(left) = self.left {
            if a.get(left).red {
                return true;
            }
        }

        if let Some(right) = self.right {
            return a.get(right).red;
        }
        false
    }

    unsafe fn sole_red_child(&self, a: &Arena<T>) -> SoleRedChild {
        match (self.left, self.right) {
            (None, None) => SoleRedChild::None,
            (None, Some(r)) => {
                if a.get(r).red {
                    SoleRedChild::Right(r)
                } else {
                    SoleRedChild::None
                }
            }
            (Some(l), None) => {
                if a.get(l).red {
                    SoleRedChild::Left(l)
                } else {
                    SoleRedChild::None
                }
            }
            (Some(l), Some(r)) => match (a.get(l).red, a.get(r).red) {
                (true, false) => SoleRedChild::Left(l),
                (false, true) => SoleRedChild::Right(r),
                _ => SoleRedChild::None,
            },
        }
    }

    unsafe fn recalculate(&mut self, a: &Arena<T>) {
        self.children = 0;
        self.max_gen = self.generation;
        self.min_gen = self.generation;

        if let Some(left) = self.left {
            let lb = a.get(left);

            self.children += 1 + lb.children;
            self.min_gen = min(self.min_gen, lb.min_gen);
            self.max_gen = max(self.max_gen, lb.max_gen);
        }

        if let Some(right) = self.right {
            let rb = a.get(right);

            self.children += 1 + rb.children;
            self.min_gen = min(self.min_gen, rb.min_gen);
            self.max_gen = max(self.max_gen, rb.max_gen);
        }
    }

    fn recalc_ancestors(node: usize, a: &Arena<T>) {
        let mut node = node;
        loop {
            unsafe {
                let nb = &mut *a.get_mut(node);
                nb.recalculate(a);
                node = match nb.parent {
                    None => break,
                    Some(p) => p,
                };
            }
        }
    }

    fn set_generation(node: usize, a: &Arena<T>, next_gen: u64) {
        let n = unsafe { &mut *a.get_mut(node) };
        if n.generation != next_gen {
            n.generation = next_gen;
            Self::recalc_ancestors(node, a);
        }
    }

    // Finds the first node with index >= i and generation <= g
    fn find_above(node: usize, a: &Arena<T>, i: usize, g: u64) -> Result<NonNull<Self>, usize> {
        let nb = a.get(node);
        if nb.min_gen > g || nb.children + 1 < i {
            return Err(nb.children + 1);
        }

        let mut left_children = 0;

        if let Some(left) = nb.left {
            match Self::find_above(left, a, i, g) {
                Ok(n) => return Ok(n),
                Err(lc) => left_children = lc,
            }
        }

        if i <= left_children && nb.generation <= g {
            return Ok(unsafe { NonNull::new_unchecked(a.vec[node].get()) });
        }

        if let Some(right) = nb.right {
            let right_r = Self::find_above(right, a, i.saturating_sub(left_children + 1), g);
            if right_r.is_ok() {
                return right_r;
            }
        }

        Err(nb.children + 1)
    }

    // fn values<'a>(&'a self, vals: &mut Vec<&'a T>) {
    //     if let Some(left) = self.left {
    //         unsafe {
    //             left.as_ref().values(vals);
    //         }
    //     }
    //     vals.push(&self.item);
    //     if let Some(right) = &self.right {
    //         unsafe {
    //             right.as_ref().values(vals);
    //         }
    //     }
    // }
    //
    // fn dump<'a>(&'a self, vals: &mut Vec<(&'a T, u64)>) {
    //     if let Some(left) = self.left {
    //         unsafe {
    //             left.as_ref().dump(vals);
    //         }
    //     }
    //     vals.push((&self.item, self.generation));
    //     if let Some(right) = &self.right {
    //         unsafe {
    //             right.as_ref().dump(vals);
    //         }
    //     }
    // }

    // UNSAFE -- All existing pointers to node except parent pointers from its children must be
    // destroyed.
    // unsafe fn destroy_tree(mut node: NonNull<Self>) {
    //     let cur = unsafe { node.as_mut() };
    //     cur.parent = None;
    //     unsafe {
    //         if let Some(left) = cur.left.take() {
    //             Self::destroy_tree(left);
    //         }
    //         if let Some(right) = cur.right.take() {
    //             Self::destroy_tree(right);
    //         }
    //     }
    //
    //     // By now, all pointers to this node have been destroyed, it's safe to drop and
    // deallocate     // it when the function returns.
    //     unsafe {
    //         drop(Box::from_raw(node.as_ptr()));
    //     }
    // }

    // UNSAFE -- All existing pointers to node except parent pointers from its children must be
    // destroyed.
    // unsafe fn into_values(mut node: NonNull<Self>, vals: &mut Vec<T>) {
    //     let cur = unsafe { node.as_mut() };
    //     cur.parent = None;
    //     unsafe {
    //         if let Some(left) = cur.left.take() {
    //             Self::into_values(left, vals);
    //         }
    //         if let Some(right) = cur.right.take() {
    //             Self::into_values(right, vals);
    //         }
    //     }
    //
    //     // By now, all pointers to this node have been destroyed, it's safe to drop and
    // deallocate     // it when the function returns.
    //     unsafe {
    //         let node = Box::from_raw(node.as_ptr());
    //         vals.push(node.item);
    //     }
    // }
}

#[derive(Debug)]
pub struct Rbtree<T, H> {
    arena: Arena<T>,
    root: Option<usize>,
    // TODO -- remove and forward to arena
    size: usize,
    hasher: H,
}

unsafe impl<T, H> Send for Rbtree<T, H>
where
    T: Send,
    H: Send,
{
}
// Implementing Sync would likely be safe but functionally probably useless.

impl<T> Default for Rbtree<T, AHasher> {
    fn default() -> Self {
        Self {
            arena: Arena::default(),
            root: None,
            size: 0,
            hasher: RandomState::new().build_hasher(),
        }
    }
}

// impl<T, H> Drop for Rbtree<T, H> {
//     fn drop(&mut self) {
//         if let Some(root) = self.root.take() {
//             unsafe { Node::destroy_tree(root) }
//         }
//     }
// }


// c - current
// p - parent
// g - grandparent
// s - sibling
impl<T, H> Rbtree<T, H>
where
    T: Item,
    H: Hasher + Clone,
{
    pub(crate) const fn new(hasher: H) -> Self {
        Self {
            arena: Arena::new(),
            root: None,
            size: 0,
            hasher,
        }
    }

    fn hash(&self, item: &T) -> u64 {
        let mut hasher = self.hasher.clone();
        item.hash(&mut hasher);
        hasher.finish()
    }

    pub(crate) fn find_node(&self, item: &T) -> Option<&Node<T>> {
        let mut n = self.arena.get(self.root?);

        let h = self.hash(item);

        loop {
            let next = match (h, item).cmp(&(n.hash, &n.item)) {
                Ordering::Equal => break,
                Ordering::Less => n.left,
                Ordering::Greater => n.right,
            };

            n = self.arena.get(next?);
        }

        Some(n)
    }

    pub fn insert(&mut self, item: T, generation: u64) -> bool {
        let h = self.hash(&item);
        self.reinsert(item, h, generation)
    }

    pub fn reinsert(&mut self, item: T, hash: u64, generation: u64) -> bool {
        let mut c = match self.root {
            None => {
                self.size += 1;
                self.root = Some(self.arena.alloc(item, hash, generation, false, None));
                return true;
            }
            Some(n) => n,
        };

        let mut p;
        unsafe {
            loop {
                p = c;
                let cn = self.arena.get(c);

                let next = match hash.cmp(&cn.hash).then_with(|| item.cmp(&cn.item)) {
                    Ordering::Equal => return false,
                    Ordering::Less => cn.left,
                    Ordering::Greater => cn.right,
                };

                match next {
                    None => break,
                    Some(next) => c = next,
                };
            }

            self.size += 1;
            let node = self.arena.alloc(item, hash, generation, true, Some(p));
            let mut pb = &mut *self.arena.get_mut(p);

            match self.arena.get(node).cmp(pb) {
                Ordering::Equal => unreachable!(),
                Ordering::Less => pb.left = Some(node),
                Ordering::Greater => pb.right = Some(node),
            }

            loop {
                let mut pb = &mut *self.arena.get_mut(p);

                pb.children += 1;

                if generation > pb.max_gen {
                    pb.max_gen = generation;
                } else if generation < pb.min_gen {
                    pb.min_gen = generation;
                }

                let next = pb.parent;

                match next {
                    None => break,
                    Some(next) => p = next,
                }
            }


            self.fix_after_insert(node);
            true
        }
    }

    pub fn delete(&mut self, item: &T) -> Option<(T, u64)> {
        unsafe {
            let n = self.find_node(item)?.index;

            self.size -= 1;

            let nb = &mut *self.arena.get_mut(n);
            // Ensure the node has only one child by replacing it with its successor
            let n = if let (Some(_), Some(right)) = (nb.left, nb.right) {
                let mut s = right;
                loop {
                    let l = match self.arena.get(s).left {
                        None => break,
                        Some(l) => l,
                    };
                    s = l;
                }

                let sb = &mut *self.arena.get_mut(s);
                // Only item, hash, and gen need to be swapped,
                // the rest will be recalculated in the next step
                swap(&mut nb.item, &mut sb.item);
                swap(&mut nb.hash, &mut sb.hash);
                swap(&mut nb.generation, &mut sb.generation);
                s
            } else {
                n
            };

            let nb = self.arena.get(n);
            let p = nb.parent;

            let p = match p {
                None => {
                    // Deleting the root
                    match (nb.left, nb.right) {
                        (None, None) => self.root = None,
                        (Some(_), Some(_)) => unreachable!(),
                        (None, Some(child)) | (Some(child), None) => {
                            self.root = Some(child);
                            let mut cb = &mut *self.arena.get_mut(child);
                            cb.parent = None;
                            cb.red = false;
                        }
                    }

                    // By now there are no pointers to n in the tree and it can be dropped.
                    let (moved_root, old) = self.arena.dealloc(n);
                    if moved_root {
                        self.root = Some(n);
                    }
                    return Some((old.item, old.hash));
                }
                Some(p) => p,
            };

            let (c, c_red) = match (nb.left, nb.right) {
                (None, None) => (None, false),
                (None, Some(child)) | (Some(child), None) => {
                    (Some(child), self.arena.get(child).red)
                }
                (Some(_), Some(_)) => unreachable!(),
            };

            if nb.red || c_red {
                if let Some(c) = c {
                    let mut cb = &mut *self.arena.get_mut(c);
                    cb.red = false;
                    cb.parent = Some(p);
                }

                let mut pb = &mut *self.arena.get_mut(p);
                if pb.is_left_child(n) {
                    pb.left = c;
                } else {
                    pb.right = c;
                }
            } else {
                self.fix_black_node_before_delete(n);

                let nb = self.arena.get(n);
                let p = nb.parent.expect("Impossible");
                let mut pb = &mut *self.arena.get_mut(p);

                if pb.is_left_child(n) {
                    pb.left = None;
                } else {
                    pb.right = None;
                }
            }

            if let Some(p) = self.arena.get(n).parent {
                Node::recalc_ancestors(p, &self.arena)
            }

            // By now there are no pointers to n in the tree and it can be dropped.
            let (moved_root, old) = self.arena.dealloc(n);
            if moved_root {
                self.root = Some(n);
            }
            Some((old.item, old.hash))
        }
    }

    unsafe fn fix_after_insert(&mut self, node: usize) {
        let mut c = node;
        let mut p = self.arena.get(c).parent;
        while let Some(mut pnd) = p {
            if !self.arena.get(pnd).red {
                return;
            }

            let g = self.arena.get(pnd).parent.expect("Impossible");
            let mut gb = &mut *self.arena.get_mut(g);

            let ps = gb.other_child(pnd);


            if let Some(ps) = ps {
                let mut psb = &mut *self.arena.get_mut(ps);
                if psb.red {
                    let mut pb = &mut *self.arena.get_mut(pnd);
                    // The parent-sibling is red, so we can continue up the tree
                    pb.red = false;
                    psb.red = false;
                    gb.red = true;
                    c = g;
                    drop(psb);
                    drop(gb);
                    drop(pb);
                    p = self.arena.get(c).parent;
                    continue;
                };
            };

            if gb.is_left_child(pnd) {
                drop(gb);
                if let Some(pright) = self.arena.get(pnd).right {
                    if c == pright {
                        self.rotate_left(pnd);
                        pnd = c;
                    }
                }

                self.rotate_right(g);
            } else {
                drop(gb);
                if let Some(pleft) = self.arena.get(pnd).left {
                    if c == pleft {
                        self.rotate_right(pnd);
                        pnd = c;
                    }
                }

                self.rotate_left(g);
            }
            (*self.arena.get_mut(pnd)).red = false;
            (*self.arena.get_mut(g)).red = true;
            return;
        }
        // We've replaced the root, and it cannot be red
        (*self.arena.get_mut(c)).red = false;
    }

    // This is only called when the node to be deleted is a non-root black node, and therefore has
    // a sibling.
    unsafe fn fix_black_node_before_delete(&mut self, mut node: usize) {
        while self.arena.get(node).parent.is_some() {
            // TODO -- we want a parent_and_other_child method
            let p = self.arena.get(node).parent.expect("Non-root black node must have parent.");
            let mut pb = &mut *self.arena.get_mut(p);
            let s = pb.other_child(node).expect("Non-root black node must have sibling");

            let mut sb = &mut *self.arena.get_mut(s);

            // The sibling is red, make it black and make it into the new parent.
            if sb.red {
                sb.red = false;
                pb.red = true;
                drop(sb);
                let left = pb.is_left_child(node);
                drop(pb);
                if left {
                    self.rotate_left(p);
                } else {
                    self.rotate_right(p);
                }
            } else {
                drop(pb);
                drop(sb);
            }

            let p = self.arena.get(node).parent.expect("Non-root black node must have parent.");
            let mut pb = &mut *self.arena.get_mut(p);
            let s = pb.other_child(node).expect("Non-root black node must have sibling");

            let mut sb = &mut *self.arena.get_mut(s);

            if !pb.red && !sb.red && !sb.has_red_child(&self.arena) {
                // All three nodes are black and the sibling has no red children.
                // Mark S as red so the subtree rooted at p meets the black-path requirement.
                // Continue up the tree.
                sb.red = true;
                drop(pb);
                node = p;
                continue;
            }

            if pb.red && !sb.red && !sb.has_red_child(&self.arena) {
                // Parent is red, S is black with no red children.
                // We can move the redness down to S and maintain the black-path requirement.
                sb.red = true;
                pb.red = false;
                return;
            }

            drop(sb);
            let sb = self.arena.get(s);

            if !sb.red {
                // All three nodes are black but S has at least one red child.
                // If there is a single red child on the inside, rotate that child onto S.


                if pb.is_left_child(node) {
                    if let SoleRedChild::Left(l) = sb.sole_red_child(&self.arena) {
                        (*self.arena.get_mut(l)).red = false;
                        drop(sb);
                        (*self.arena.get_mut(s)).red = true;
                        drop(pb);
                        self.rotate_right(s);
                    } else {
                        drop(sb);
                        drop(pb);
                    }
                } else if let SoleRedChild::Right(r) = sb.sole_red_child(&self.arena) {
                    (*self.arena.get_mut(r)).red = false;
                    drop(sb);
                    (*self.arena.get_mut(s)).red = true;
                    drop(pb);
                    self.rotate_left(s);
                } else {
                    drop(sb);
                    drop(pb);
                }
            } else {
                drop(sb);
                drop(pb);
            }

            // S is red or has two red children.
            // Rotate S onto parent and copy parent's colour, make both its children black.

            let p = self.arena.get(node).parent.expect("Non-root black node must have parent.");
            let s = self
                .arena
                .get(p)
                .other_child(node)
                .expect("Non-root black node must have sibling");

            let mut pb = &mut *self.arena.get_mut(p);
            let mut sb = &mut *self.arena.get_mut(s);

            sb.red = pb.red;
            pb.red = false;
            drop(sb);
            let sb = self.arena.get(s);

            if pb.is_left_child(node) {
                if let Some(r) = sb.right {
                    (*self.arena.get_mut(r)).red = false;
                }
                drop(sb);
                drop(pb);
                self.rotate_left(p);
            } else {
                if let Some(l) = sb.left {
                    (*self.arena.get_mut(l)).red = false;
                }
                drop(sb);
                drop(pb);
                self.rotate_right(p);
            }

            return;
        }
    }

    unsafe fn rotate_right(&mut self, parent: usize) {
        // Left child becomes the new parent
        let mut pb = &mut *self.arena.get_mut(parent);
        let l = pb.left.expect("Tried to make None child into parent");
        let mut lb = &mut *self.arena.get_mut(l);

        pb.left = lb.right.take();
        if let Some(p_left) = pb.left {
            (*self.arena.get_mut(p_left)).parent = Some(parent);
        }

        lb.right = Some(parent);
        lb.parent = pb.parent.take();
        pb.parent = Some(l);
        drop(pb);

        if let Some(l_parent) = lb.parent {
            let mut lpb = &mut *self.arena.get_mut(l_parent);
            if lpb.is_left_child(parent) {
                lpb.left = Some(l);
            } else {
                lpb.right = Some(l);
            }
            drop(lpb);
            drop(lb);
        } else {
            drop(lb);
            self.root = Some(l)
        }

        (*self.arena.get_mut(parent)).recalculate(&self.arena);
        (*self.arena.get_mut(l)).recalculate(&self.arena);
    }

    unsafe fn rotate_left(&mut self, parent: usize) {
        // Right child becomes the new parent
        let mut pb = &mut *self.arena.get_mut(parent);
        let r = pb.right.expect("Tried to make None child into parent");
        let mut rb = &mut *self.arena.get_mut(r);

        pb.right = rb.left.take();
        if let Some(p_right) = pb.right {
            (*self.arena.get_mut(p_right)).parent = Some(parent);
        }

        rb.left = Some(parent);
        rb.parent = pb.parent.take();
        pb.parent = Some(r);
        drop(pb);

        if let Some(r_parent) = rb.parent {
            let mut rpb = &mut *self.arena.get_mut(r_parent);
            if !rpb.is_left_child(parent) {
                rpb.right = Some(r);
            } else {
                rpb.left = Some(r);
            }
            drop(rpb);
            drop(rb);
        } else {
            drop(rb);
            self.root = Some(r)
        }

        (*self.arena.get_mut(parent)).recalculate(&self.arena);
        (*self.arena.get_mut(r)).recalculate(&self.arena);
    }

    // Only to be used when the generation would overflow a u64
    pub(crate) fn reset(&mut self) {
        for node in &mut self.arena.vec {
            let node = node.get_mut();
            node.generation = 0;
            node.min_gen = 0;
            node.max_gen = 0;
        }
    }

    // Finds the next item with a generation <= g after index (inclusive) from left to right in the
    // tree.
    // Wraps around to the start of the tree if one isn't found.
    #[allow(clippy::missing_panics_doc)]
    pub fn find_next(&self, index: usize, generation: u64) -> NonNull<Node<T>> {
        assert!(self.size > 0);
        assert!(index < self.size);
        let root = self.root.expect("Root cannot be None in a tree with size > 0");

        Node::find_above(root, &self.arena, index, generation)
            .or_else(|_| Node::find_above(root, &self.arena, 0, generation))
            .expect("Corrupt tree")
    }

    pub(crate) fn values(&self) -> Vec<&T> {
        unsafe { self.arena.vec.iter().map(|n| &n.get().as_ref().unwrap().item).collect() }
    }

    pub(crate) fn into_values(self) -> Vec<T> {
        self.arena.vec.into_iter().map(UnsafeCell::into_inner).map(|n| n.item).collect()
    }

    pub(crate) fn dump(&self) -> Vec<(&T, u64)> {
        unsafe {
            self.arena
                .vec
                .iter()
                .map(|n| {
                    let n = &n.get().as_ref().unwrap();
                    (&n.item, n.generation)
                })
                .collect()
        }
    }

    pub(crate) const fn size(&self) -> usize {
        self.arena.vec.len()
    }

    pub(crate) fn generations(&self) -> (u64, u64) {
        if let Some(root) = self.root {
            let root = self.arena.get(root);
            (root.min_gen, root.max_gen)
        } else {
            (0, 0)
        }
    }

    // SAFETY: Must not be actively referencing any Nodes in the tree
    pub(crate) unsafe fn set_generation(&self, node: usize, next_gen: u64) {
        Node::set_generation(node, &self.arena, next_gen);
    }
}

#[cfg(test)]
impl<T> Node<T>
where
    T: Item + std::fmt::Display + Debug,
{
    fn pprint(&self, a: &Arena<T>, prefix: String) -> String {
        let left = if let Some(left) = self.left {
            a.get(left).pprint(a, prefix.clone() + "  ")
        } else {
            String::new()
        };

        let right = if let Some(right) = self.right {
            a.get(right).pprint(a, prefix.clone() + "  ")
        } else {
            String::new()
        };

        let c = if self.red { "red" } else { "black" };

        format!(
            "{left}{prefix}{}: {} [{},{}], {c}\n{right}",
            self.item, self.generation, self.min_gen, self.max_gen
        )
    }

    fn print(&self, a: &Arena<T>) -> String {
        let left = self.left.map(|n| a.get(n).print(a)).unwrap_or_default();

        let right = self.right.map(|n| a.get(n).print(a)).unwrap_or_default();

        let c = if self.red { "r" } else { "b" };

        format!("({} {} {c} {left} {right})", self.item, self.generation)
    }

    fn verify(&self, a: &Arena<T>) -> usize {
        let mut min_gen = self.generation;
        let mut max_gen = self.generation;
        let mut children = 0;

        unsafe {
            let (l_black, l_red) = if let Some(left) = self.left {
                let lb = a.get(left);
                assert_eq!(self.index, lb.parent.unwrap());

                assert!(self.hash >= lb.hash);
                assert!(self > lb);

                children += lb.children + 1;
                min_gen = min(min_gen, lb.min_gen);
                max_gen = max(max_gen, lb.max_gen);
                (lb.verify(a), lb.red)
            } else {
                (0, false)
            };

            let (r_black, r_red) = if let Some(right) = self.right {
                let rb = a.get(right);
                assert_eq!(self.index, rb.parent.unwrap());

                assert!(self.hash <= rb.hash);
                assert!(self < rb);

                children += rb.children + 1;
                min_gen = min(min_gen, rb.min_gen);
                max_gen = max(max_gen, rb.max_gen);
                (rb.verify(a), rb.red)
            } else {
                (0, false)
            };

            // red nodes cannot have red children
            assert!(!self.red || !(l_red || r_red));

            assert_eq!(self.min_gen, min_gen);
            assert_eq!(self.max_gen, max_gen);
            assert_eq!(self.children, children);
            assert_eq!(l_black, r_black);

            if self.red { l_black } else { l_black + 1 }
        }
    }
}

#[cfg(test)]
impl<T, H> Rbtree<T, H>
where
    T: Item + std::fmt::Display + Debug,
    H: Hasher + Clone,
{
    #[allow(dead_code)]
    pub(super) fn pprint(&self) -> String {
        self.root
            .map(|r| self.arena.get(r).pprint(&self.arena, String::new()))
            .unwrap_or_default()
    }

    fn print(&self) -> String {
        self.root.map(|r| self.arena.get(r).print(&self.arena)).unwrap_or_default()
    }

    fn verify(&self) {
        match self.root {
            None => {
                assert_eq!(self.size, 0);
            }
            Some(root) => {
                let rb = self.arena.get(root);

                assert_eq!(self.size, rb.children + 1);
                assert!(rb.parent.is_none());
                assert!(!rb.red);

                rb.verify(&self.arena);
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::convert::TryInto;
    use std::hash::{BuildHasher, Hasher};
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::rc::Rc;
    use std::str::from_utf8;

    use ahash::{AHashMap, RandomState};
    use rand::prelude::SliceRandom;

    use super::{Arena, Node, Rbtree};

    #[derive(Clone)]
    pub(crate) struct DummyHasher<'a> {
        values: Rc<AHashMap<&'a str, u64>>,
        val: u64,
    }

    impl Hasher for DummyHasher<'_> {
        fn finish(&self) -> u64 {
            self.val
        }

        fn write(&mut self, bytes: &[u8]) {
            if bytes.is_empty() || bytes[0] == 0xff {
                return;
            }
            self.val = *self.values.get(from_utf8(bytes).unwrap()).unwrap_or(&0);
        }
    }

    impl<'a> Rbtree<&'a str, DummyHasher<'a>> {
        pub(crate) fn new_dummy(entries: &[(&'static str, u64)]) -> Self {
            let hashes: AHashMap<_, _> = entries.iter().copied().collect();
            Self {
                arena: super::Arena::new(),
                root: None,
                size: 0,
                hasher: DummyHasher { val: 0, values: Rc::from(hashes) },
            }
        }
    }

    fn sequential_strings(n: usize) -> Vec<String> {
        let strlen = n.to_string().len();

        (0..n).map(|i| format!("{i:0strlen$}")).collect()
    }

    #[test]
    fn basic_insert() {
        let mut rb = Rbtree::new_dummy(&[]);
        assert!(rb.insert("5", 0));
        assert!(rb.insert("4", 1));
        assert!(rb.insert("6", 2));

        rb.verify();
        assert_eq!(rb.print(), "(5 0 b (4 1 r  ) (6 2 r  ))");
    }

    #[test]
    fn insert_hasher() {
        let mut rb = Rbtree::new_dummy(&[("4", 1)]);
        assert!(rb.insert("5", 0));
        assert!(rb.insert("4", 1));
        assert!(rb.insert("6", 2));

        rb.verify();
        assert_eq!(rb.print(), "(6 2 b (5 0 r  ) (4 1 r  ))");
    }

    #[test]
    #[cfg_attr(miri, ignore)] // ahash changes under miri
    fn test_hasher() {
        // ahash may change output when updated, so this test may fail after updating dependencies
        // Can also fail in miri due to different hash output, but not UB.
        let hasher = RandomState::with_seeds(100, 200, 300, 400).build_hasher();
        let mut rb = Rbtree {
            arena: Arena::new(),
            root: None,
            size: 0,
            hasher,
        };

        assert!(rb.insert("5", 0));
        assert!(rb.insert("4", 1));
        assert!(rb.insert("6", 2));

        rb.verify();
        assert_eq!(rb.print(), "(4 1 b (5 0 r  ) (6 2 r  ))");

        let hasher = RandomState::with_seeds(400, 300, 200, 100).build_hasher();
        let mut rb = Rbtree {
            arena: Arena::new(),
            root: None,
            size: 0,
            hasher,
        };

        assert!(rb.insert("5", 0));
        assert!(rb.insert("4", 1));
        assert!(rb.insert("6", 2));

        rb.verify();
        assert_eq!(rb.print(), "(6 2 b (4 1 r  ) (5 0 r  ))");
    }

    #[test]
    fn left_insert() {
        let mut rb = Rbtree::new_dummy(&[]);
        assert!(rb.insert("5", 0));
        assert!(rb.insert("4", 1));
        assert!(rb.insert("3", 2));
        assert!(rb.insert("2", 3));
        assert!(rb.insert("1", 4));
        assert!(!rb.insert("1", 50));

        rb.verify();
        assert_eq!(rb.print(), "(4 1 b (2 3 b (1 4 r  ) (3 2 r  )) (5 0 b  ))");
    }

    #[test]
    fn right_insert() {
        let mut rb = Rbtree::new_dummy(&[]);
        assert!(rb.insert("1", 0));
        assert!(rb.insert("2", 1));
        assert!(rb.insert("3", 2));
        assert!(rb.insert("4", 3));
        assert!(rb.insert("5", 4));
        assert!(!rb.insert("5", 50));

        rb.verify();
        assert_eq!(rb.print(), "(2 1 b (1 0 b  ) (4 3 b (3 2 r  ) (5 4 r  )))");
    }

    #[test]
    fn insert_left_right() {
        let mut rb = Rbtree::new_dummy(&[]);
        assert!(rb.insert("5", 0));
        assert!(rb.insert("2", 1));
        assert!(rb.insert("3", 2));

        rb.verify();
        assert_eq!(rb.print(), "(3 2 b (2 1 r  ) (5 0 r  ))");
    }

    #[test]
    fn insert_right_left() {
        let mut rb = Rbtree::new_dummy(&[]);
        assert!(rb.insert("2", 1));
        assert!(rb.insert("5", 0));
        assert!(rb.insert("3", 2));

        rb.verify();
        assert_eq!(rb.print(), "(3 2 b (2 1 r  ) (5 0 r  ))");
    }

    #[test]
    fn reset() {
        let mut rb = Rbtree::new_dummy(&[]);
        assert!(rb.insert("5", 5));
        assert!(rb.insert("2", 2));
        assert!(rb.insert("7", 7));
        assert!(rb.insert("1", 1));
        assert!(rb.insert("3", 3));
        assert!(rb.insert("6", 6));
        assert!(rb.insert("8", 8));

        rb.verify();
        assert_eq!(rb.print(), "(5 5 b (2 2 b (1 1 r  ) (3 3 r  )) (7 7 b (6 6 r  ) (8 8 r  )))");

        rb.reset();
        rb.verify();
        assert_eq!(rb.print(), "(5 0 b (2 0 b (1 0 r  ) (3 0 r  )) (7 0 b (6 0 r  ) (8 0 r  )))");
    }


    #[test]
    fn delete_root() {
        let mut rb = Rbtree::new_dummy(&[]);
        assert!(rb.insert("5", 5));
        assert!(rb.insert("2", 2));
        assert!(rb.insert("7", 7));
        assert!(rb.insert("1", 1));
        assert!(rb.insert("3", 3));
        assert!(rb.insert("6", 6));
        assert!(rb.insert("8", 8));

        assert_eq!(rb.delete(&"5"), Some(("5", 0)));
        assert_eq!(rb.print(), "(6 6 b (2 2 b (1 1 r  ) (3 3 r  )) (7 7 b  (8 8 r  )))");
        rb.verify();

        assert_eq!(rb.delete(&"6"), Some(("6", 0)));
        assert_eq!(rb.print(), "(7 7 b (2 2 b (1 1 r  ) (3 3 r  )) (8 8 b  ))");
        rb.verify();

        println!("{}", rb.pprint());
        assert_eq!(rb.delete(&"7"), Some(("7", 0)));
        println!("{}", rb.pprint());
        assert_eq!(rb.print(), "(2 2 b (1 1 b  ) (8 8 b (3 3 r  ) ))");
        rb.verify();

        assert_eq!(rb.delete(&"2"), Some(("2", 0)));
        assert_eq!(rb.print(), "(3 3 b (1 1 b  ) (8 8 b  ))");
        rb.verify();

        assert_eq!(rb.delete(&"3"), Some(("3", 0)));
        assert_eq!(rb.print(), "(8 8 b (1 1 r  ) )");
        rb.verify();

        assert_eq!(rb.delete(&"8"), Some(("8", 0)));
        assert_eq!(rb.print(), "(1 1 b  )");
        rb.verify();

        assert_eq!(rb.delete(&"1"), Some(("1", 0)));
        assert_eq!(rb.print(), "");
        rb.verify();

        assert!(rb.insert("2", 0));
        assert_eq!(rb.delete(&"1"), None);
        assert_eq!(rb.print(), "(2 0 b  )");
        rb.verify();
    }


    #[test]
    fn delete_red_sibling() {
        let mut rb = Rbtree::new_dummy(&[]);
        assert!(rb.insert("1", 0));
        assert!(rb.insert("2", 0));
        assert!(rb.insert("3", 0));
        assert!(rb.insert("4", 0));
        assert!(rb.insert("5", 0));
        assert!(rb.insert("6", 0));

        assert_eq!(rb.print(), "(2 0 b (1 0 b  ) (4 0 r (3 0 b  ) (5 0 b  (6 0 r  ))))");
        rb.verify();

        assert_eq!(rb.delete(&"1"), Some(("1", 0)));
        assert_eq!(rb.print(), "(4 0 b (2 0 b  (3 0 r  )) (5 0 b  (6 0 r  )))");
        rb.verify();

        let mut rb = Rbtree::new_dummy(&[]);
        assert!(rb.insert("6", 0));
        assert!(rb.insert("5", 0));
        assert!(rb.insert("4", 0));
        assert!(rb.insert("3", 0));
        assert!(rb.insert("2", 0));
        assert!(rb.insert("1", 0));

        assert_eq!(rb.print(), "(5 0 b (3 0 r (2 0 b (1 0 r  ) ) (4 0 b  )) (6 0 b  ))");

        rb.verify();

        assert_eq!(rb.delete(&"6"), Some(("6", 0)));
        assert_eq!(rb.print(), "(3 0 b (2 0 b (1 0 r  ) ) (5 0 b (4 0 r  ) ))");
        rb.verify();
    }

    #[test]
    fn delete_sibling_inner_red_child() {
        let mut rb = Rbtree::new_dummy(&[]);
        assert!(rb.insert("1", 0));
        assert!(rb.insert("2", 0));
        assert!(rb.insert("4", 0));
        assert!(rb.insert("3", 0));

        assert_eq!(rb.print(), "(2 0 b (1 0 b  ) (4 0 b (3 0 r  ) ))");

        assert_eq!(rb.delete(&"1"), Some(("1", 0)));
        assert_eq!(rb.print(), "(3 0 b (2 0 b  ) (4 0 b  ))");
        rb.verify();

        let mut rb = Rbtree::new_dummy(&[]);
        assert!(rb.insert("4", 0));
        assert!(rb.insert("3", 0));
        assert!(rb.insert("1", 0));
        assert!(rb.insert("2", 0));

        assert_eq!(rb.print(), "(3 0 b (1 0 b  (2 0 r  )) (4 0 b  ))");

        assert_eq!(rb.delete(&"4"), Some(("4", 0)));
        assert_eq!(rb.print(), "(2 0 b (1 0 b  ) (3 0 b  ))");
        rb.verify();
    }


    #[test]
    fn delete_leaves() {
        let mut rb = Rbtree::new_dummy(&[]);
        assert!(rb.insert("5", 5));
        assert!(rb.insert("2", 2));
        assert!(rb.insert("7", 7));
        assert!(rb.insert("1", 1));
        assert!(rb.insert("3", 3));
        assert!(rb.insert("6", 6));
        assert!(rb.insert("8", 8));

        assert_eq!(rb.print(), "(5 5 b (2 2 b (1 1 r  ) (3 3 r  )) (7 7 b (6 6 r  ) (8 8 r  )))");
        rb.delete(&"8");
        assert_eq!(rb.print(), "(5 5 b (2 2 b (1 1 r  ) (3 3 r  )) (7 7 b (6 6 r  ) ))");
        rb.verify();

        rb.delete(&"1");
        assert_eq!(rb.print(), "(5 5 b (2 2 b  (3 3 r  )) (7 7 b (6 6 r  ) ))");
        rb.verify();

        rb.delete(&"6");
        assert_eq!(rb.print(), "(5 5 b (2 2 b  (3 3 r  )) (7 7 b  ))");
        rb.verify();

        rb.delete(&"3");
        assert_eq!(rb.print(), "(5 5 b (2 2 b  ) (7 7 b  ))");
        rb.verify();

        rb.delete(&"2");
        assert_eq!(rb.print(), "(5 5 b  (7 7 r  ))");
        rb.verify();

        rb.delete(&"7");
        assert_eq!(rb.print(), "(5 5 b  )");
        rb.verify();
    }

    #[test]
    fn delete_branches() {
        let mut rb = Rbtree::new_dummy(&[]);
        assert!(rb.insert("5", 5));
        assert!(rb.insert("2", 2));
        assert!(rb.insert("7", 7));
        assert!(rb.insert("1", 1));
        assert!(rb.insert("3", 3));
        assert!(rb.insert("6", 6));
        assert!(rb.insert("8", 8));

        rb.delete(&"2");
        assert_eq!(rb.print(), "(5 5 b (3 3 b (1 1 r  ) ) (7 7 b (6 6 r  ) (8 8 r  )))");
        rb.verify();

        rb.delete(&"3");
        assert_eq!(rb.print(), "(5 5 b (1 1 b  ) (7 7 b (6 6 r  ) (8 8 r  )))");
        rb.verify();

        rb.delete(&"7");
        assert_eq!(rb.print(), "(5 5 b (1 1 b  ) (8 8 b (6 6 r  ) ))");
        rb.verify();
    }

    #[test]
    fn delete_unbalance() {
        let mut rb = Rbtree::new_dummy(&[]);
        assert!(rb.insert("5", 5));
        assert!(rb.insert("2", 2));
        assert!(rb.insert("7", 7));
        assert!(rb.insert("1", 1));
        assert!(rb.insert("3", 3));
        assert!(rb.insert("6", 6));
        assert!(rb.insert("8", 8));

        rb.delete(&"2");
        rb.delete(&"3");
        rb.delete(&"1");
        assert_eq!(rb.print(), "(7 7 b (5 5 b  (6 6 r  )) (8 8 b  ))");
        rb.verify();
    }

    #[test]
    fn delete_noop() {
        let mut rb = Rbtree::new_dummy(&[]);

        assert_eq!(rb.delete(&"23423"), None);
        assert_eq!(rb.print(), "");
        rb.verify();

        assert!(rb.insert("5", 5));
        assert!(rb.insert("2", 2));
        assert!(rb.insert("7", 7));

        assert_eq!(rb.delete(&"8"), None);
        assert_eq!(rb.print(), "(5 5 b (2 2 r  ) (7 7 r  ))");
        rb.verify();

        assert_eq!(rb.delete(&""), None);
        assert_eq!(rb.print(), "(5 5 b (2 2 r  ) (7 7 r  ))");
        rb.verify();
    }

    // Just fuzz it with random values to sanity check that all the properties hold and borrows are
    // properly managed.
    #[test]
    fn fuzz_insert_delete() {
        #[cfg(not(miri))]
        let input = sequential_strings(100000);
        // Use a smaller set for miri since it's way too slow with large sets
        #[cfg(miri)]
        let input = sequential_strings(100);

        let mut rng = rand::rng();
        for _ in 1..10 {
            let mut rb = Rbtree::default();
            let mut shuffled = input.clone();
            shuffled.shuffle(&mut rng);
            shuffled.into_iter().enumerate().for_each(|(i, s)| {
                assert!(rb.insert(s, i.try_into().unwrap()));
                if i % 1000 == 0 {
                    rb.verify();
                }
            });
            rb.verify();

            let mut shuffled: Vec<&String> = input.iter().collect();
            shuffled.shuffle(&mut rng);
            shuffled.into_iter().enumerate().for_each(|(i, s)| {
                let (ds, _) = rb.delete(s).expect("Missing element");
                assert_eq!(&*ds, s);
                if i % 1000 == 0 {
                    rb.verify();
                }
            });
            rb.verify();
            assert_eq!(rb.size, 0);
        }
    }

    #[test]
    fn find_next() {
        let strings = sequential_strings(11);
        let mut rb = Rbtree::new_dummy(&[]);

        strings.iter().enumerate().for_each(|(i, s)| {
            assert!(rb.insert(s, (10 - i).try_into().unwrap()));
        });

        unsafe {
            assert_eq!((*(rb.find_next(0, 10).as_ref())).item, "00");
            assert_eq!((*(rb.find_next(0, 0).as_ref())).item, "10");
            assert_eq!((*(rb.find_next(0, 1).as_ref())).item, "09");
            assert_eq!((*(rb.find_next(0, 5).as_ref())).item, "05");
            assert_eq!((*(rb.find_next(8, 5).as_ref())).item, "08");
            assert_eq!((*(rb.find_next(8, 9).as_ref())).item, "08");
            assert_eq!((*(rb.find_next(8, 2).as_ref())).item, "08");
            assert_eq!((*(rb.find_next(8, 1).as_ref())).item, "09");
            assert_eq!((*(rb.find_next(10, 0).as_ref())).item, "10");
            assert_eq!((*(rb.find_next(10, 1).as_ref())).item, "10");
            assert_eq!((*(rb.find_next(10, 5).as_ref())).item, "10");
            assert_eq!((*(rb.find_next(10, 10).as_ref())).item, "10");
        }
    }

    #[test]
    fn find_next_reversed() {
        let strings = sequential_strings(11);
        let mut rb = Rbtree::new_dummy(&[]);

        strings.iter().enumerate().for_each(|(i, s)| {
            let g = if i == 0 { 5 } else { i.try_into().unwrap() };
            assert!(rb.insert(s, g));
        });

        unsafe {
            assert_eq!((*(rb.find_next(0, 10).as_ref())).item, "00");
            assert_eq!((*(rb.find_next(0, 4).as_ref())).item, "01");
            assert_eq!((*(rb.find_next(0, 1).as_ref())).item, "01");
            assert_eq!((*(rb.find_next(0, 5).as_ref())).item, "00");
            assert_eq!((*(rb.find_next(8, 5).as_ref())).item, "00");
            assert_eq!((*(rb.find_next(8, 9).as_ref())).item, "08");
            assert_eq!((*(rb.find_next(8, 2).as_ref())).item, "01");
            assert_eq!((*(rb.find_next(8, 1).as_ref())).item, "01");
            assert_eq!((*(rb.find_next(10, 1).as_ref())).item, "01");
            assert_eq!((*(rb.find_next(10, 5).as_ref())).item, "00");
            assert_eq!((*(rb.find_next(10, 10).as_ref())).item, "10");
        }
    }

    // These methods are only called from Base,
    // so any error means the shuffler is irrecoverably corrupt.
    #[test]
    fn find_next_invalid() {
        let strings = sequential_strings(10);
        let mut rb = Rbtree::new_dummy(&[]);

        strings.iter().enumerate().for_each(|(i, s)| {
            assert!(rb.insert(s, (10 - i).try_into().unwrap()));
        });

        rb.insert("10", 1);
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                rb.find_next(11, 1);
            }))
            .is_err()
        );
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                rb.find_next(5, 0);
            }))
            .is_err()
        );
    }

    #[test]
    fn values() {
        let strings = sequential_strings(10);
        let mut rb = Rbtree::new_dummy(&[("07", 1)]);

        strings.iter().enumerate().for_each(|(i, s)| {
            assert!(rb.insert(s, (10 - i).try_into().unwrap()));
        });

        let expected = sequential_strings(10);
        let mut v = rb.values();
        assert_eq!(v.len(), expected.len());

        v.sort_unstable();

        v.into_iter().zip(expected.iter()).for_each(|(a, b)| assert_eq!(a, b));
    }

    #[test]
    fn into_values() {
        let strings = sequential_strings(10);
        let mut rb = Rbtree::new_dummy(&[("07", 1)]);

        strings.iter().enumerate().for_each(|(i, s)| {
            assert!(rb.insert(s, (10 - i).try_into().unwrap()));
        });

        let expected = sequential_strings(10);
        let mut v = rb.into_values();
        assert_eq!(v.len(), expected.len());

        v.sort_unstable();

        v.into_iter().zip(expected.iter()).for_each(|(a, b)| assert_eq!(a, b));
    }

    #[test]
    fn size() {
        let mut rb = Rbtree::new_dummy(&[]);

        assert_eq!(rb.size(), 0);

        assert!(rb.insert("5", 5));
        assert!(rb.insert("2", 2));
        assert!(rb.insert("7", 7));

        assert_eq!(rb.size(), 3);

        rb.delete(&"5");

        assert_eq!(rb.size(), 2);
    }

    #[test]
    fn generations() {
        let mut rb = Rbtree::new_dummy(&[]);

        assert_eq!(rb.generations(), (0, 0));

        assert!(rb.insert("5", 5));
        assert!(rb.insert("2", 2));
        assert!(rb.insert("7", 7));

        assert_eq!(rb.generations(), (2, 7));

        rb.delete(&"7");

        assert_eq!(rb.generations(), (2, 5));

        rb.delete(&"2");

        assert_eq!(rb.generations(), (5, 5));

        rb.delete(&"5");

        assert_eq!(rb.generations(), (0, 0));
    }

    #[test]
    fn set_generation() {
        let mut rb = Rbtree::new_dummy(&[]);
        assert!(rb.insert("5", 5));
        assert!(rb.insert("2", 2));
        assert!(rb.insert("7", 7));

        assert_eq!(rb.print(), "(5 5 b (2 2 r  ) (7 7 r  ))");
        rb.verify();

        let n = rb.find_next(0, 2);

        unsafe { rb.set_generation((*n.as_ref()).index, 1000) };

        assert_eq!(rb.print(), "(5 5 b (2 1000 r  ) (7 7 r  ))");
        rb.verify();
    }
}
