#![allow(missing_docs)]

use std::cmp::{Ordering, max, min};
use std::fmt::Debug;
use std::hash::{BuildHasher, Hasher};
use std::mem::swap;
use std::ptr::NonNull;

use ahash::{AHasher, RandomState};

use crate::Item;

// This was originally written in Go, translated to a version using Rc<RefCell<>>, debugged and
// fuzzed, then converted into this code.

pub struct Node<T> {
    item: T,
    hash: u64,
    gen: u64,
    red: bool,
    children: usize,
    min_gen: u64,
    max_gen: u64,
    parent: Option<NonNull<Node<T>>>,
    left: Option<NonNull<Node<T>>>,
    right: Option<NonNull<Node<T>>>,
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
            .field("gen", &self.gen)
            .field("red", &self.red)
            .field("children", &self.children)
            .field("min_gen", &self.min_gen)
            .field("max_gen", &self.max_gen)
            .finish()
    }
}

enum SoleRedChild<T> {
    Right(NonNull<Node<T>>),
    Left(NonNull<Node<T>>),
    None,
}

impl<T> Node<T> {
    #[inline]
    pub(crate) const fn get(&self) -> &T {
        &self.item
    }

    fn other_child(&self, c: &Self) -> &Option<NonNull<Self>> {
        if self.is_left_child(c) { &self.right } else { &self.left }
    }

    fn is_left_child(&self, c: &Self) -> bool {
        if let Some(left) = self.left {
            unsafe { std::ptr::eq(c, left.as_ref()) }
        } else {
            false
        }
    }

    const fn has_red_child(&self) -> bool {
        if let Some(left) = self.left {
            if unsafe { left.as_ref() }.red {
                return true;
            }
        }

        if let Some(right) = self.right {
            return unsafe { right.as_ref() }.red;
        }
        false
    }

    const fn sole_red_child(&self) -> SoleRedChild<T> {
        match (self.left, self.right) {
            (None, None) => SoleRedChild::None,
            (None, Some(r)) => {
                if unsafe { r.as_ref() }.red {
                    SoleRedChild::Right(r)
                } else {
                    SoleRedChild::None
                }
            }
            (Some(l), None) => {
                if unsafe { l.as_ref() }.red {
                    SoleRedChild::Left(l)
                } else {
                    SoleRedChild::None
                }
            }
            (Some(l), Some(r)) => match unsafe { (l.as_ref().red, r.as_ref().red) } {
                (true, false) => SoleRedChild::Left(l),
                (false, true) => SoleRedChild::Right(r),
                _ => SoleRedChild::None,
            },
        }
    }

    fn recalculate(&mut self) {
        self.children = 0;
        self.max_gen = self.gen;
        self.min_gen = self.gen;

        if let Some(left) = self.left {
            let lb = unsafe { left.as_ref() };

            self.children += 1 + lb.children;
            self.min_gen = min(self.min_gen, lb.min_gen);
            self.max_gen = max(self.max_gen, lb.max_gen);
        }

        if let Some(right) = self.right {
            let rb = unsafe { right.as_ref() };

            self.children += 1 + rb.children;
            self.min_gen = min(self.min_gen, rb.min_gen);
            self.max_gen = max(self.max_gen, rb.max_gen);
        }
    }

    fn recalc_ancestors(mut node: NonNull<Self>) {
        let mut node = unsafe { node.as_mut() };
        loop {
            node.recalculate();
            node = match &mut node.parent {
                None => break,
                Some(p) => unsafe { p.as_mut() },
            };
        }
    }

    pub(crate) fn set_generation(mut node: NonNull<Self>, next_gen: u64) {
        let n = unsafe { node.as_mut() };
        if n.gen != next_gen {
            n.gen = next_gen;
            Self::recalc_ancestors(node);
        }
    }

    // Finds the first node with index >= i and gen <= g
    fn find_above(node: NonNull<Self>, i: usize, g: u64) -> Result<NonNull<Self>, usize> {
        let nb = unsafe { node.as_ref() };
        if nb.min_gen > g || nb.children + 1 < i {
            return Err(nb.children + 1);
        }

        let mut left_children = 0;

        if let Some(left) = nb.left {
            match Self::find_above(left, i, g) {
                Ok(n) => return Ok(n),
                Err(lc) => left_children = lc,
            }
        }

        if i <= left_children && nb.gen <= g {
            return Ok(node);
        }

        if let Some(right) = nb.right {
            let right_r = Self::find_above(right, i.saturating_sub(left_children + 1), g);
            if right_r.is_ok() {
                return right_r;
            }
        }

        Err(nb.children + 1)
    }

    fn values<'a>(&'a self, vals: &mut Vec<&'a T>) {
        if let Some(left) = self.left {
            unsafe {
                left.as_ref().values(vals);
            }
        }
        vals.push(&self.item);
        if let Some(right) = &self.right {
            unsafe {
                right.as_ref().values(vals);
            }
        }
    }

    fn dump<'a>(&'a self, vals: &mut Vec<(&'a T, u64)>) {
        if let Some(left) = self.left {
            unsafe {
                left.as_ref().dump(vals);
            }
        }
        vals.push((&self.item, self.gen));
        if let Some(right) = &self.right {
            unsafe {
                right.as_ref().dump(vals);
            }
        }
    }

    fn reset(&mut self) {
        self.gen = 0;
        self.min_gen = 0;
        self.max_gen = 0;
        unsafe {
            if let Some(mut left) = self.left {
                left.as_mut().reset();
            }
            if let Some(mut right) = self.right {
                right.as_mut().reset();
            }
        }
    }

    // UNSAFE -- All existing pointers to node except parent pointers from its children must be
    // destroyed.
    unsafe fn destroy_tree(mut node: NonNull<Self>) {
        let cur = unsafe { node.as_mut() };
        cur.parent = None;
        unsafe {
            if let Some(left) = cur.left.take() {
                Self::destroy_tree(left);
            }
            if let Some(right) = cur.right.take() {
                Self::destroy_tree(right);
            }
        }

        // By now, all pointers to this node have been destroyed, it's safe to drop and deallocate
        // it when the function returns.
        unsafe {
            drop(Box::from_raw(node.as_ptr()));
        }
    }

    // UNSAFE -- All existing pointers to node except parent pointers from its children must be
    // destroyed.
    unsafe fn into_values(mut node: NonNull<Self>, vals: &mut Vec<T>) {
        let cur = unsafe { node.as_mut() };
        cur.parent = None;
        unsafe {
            if let Some(left) = cur.left.take() {
                Self::into_values(left, vals);
            }
            if let Some(right) = cur.right.take() {
                Self::into_values(right, vals);
            }
        }

        // By now, all pointers to this node have been destroyed, it's safe to drop and deallocate
        // it when the function returns.
        unsafe {
            let node = Box::from_raw(node.as_ptr());
            vals.push(node.item);
        }
    }
}

// TODO -- it'd be possible to drop the Clone requirement here.
#[derive(Debug)]
pub struct Rbtree<T, H> {
    root: Option<NonNull<Node<T>>>,
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
            root: None,
            size: 0,
            hasher: RandomState::new().build_hasher(),
        }
    }
}

impl<T, H> Drop for Rbtree<T, H> {
    fn drop(&mut self) {
        if let Some(root) = self.root.take() {
            unsafe { Node::destroy_tree(root) }
        }
    }
}


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
        Self { root: None, size: 0, hasher }
    }

    fn hash(&self, item: &T) -> u64 {
        let mut hasher = self.hasher.clone();
        item.hash(&mut hasher);
        hasher.finish()
    }

    pub(crate) fn find_node(&self, item: &T) -> Option<NonNull<Node<T>>> {
        let mut n = self.root?;

        let h = self.hash(item);

        loop {
            let nb = unsafe { n.as_ref() };
            let next = match (h, item).cmp(&(nb.hash, &nb.item)) {
                Ordering::Equal => break,
                Ordering::Less => nb.left,
                Ordering::Greater => nb.right,
            };

            n = next?;
        }

        Some(n)
    }

    pub fn insert(&mut self, item: T, gen: u64) -> bool {
        let h = self.hash(&item);
        self.reinsert(item, h, gen)
    }

    pub fn reinsert(&mut self, item: T, hash: u64, gen: u64) -> bool {
        let mut node = Node {
            item,
            hash,
            gen,
            red: true,
            children: 0,
            min_gen: gen,
            max_gen: gen,
            parent: None,
            left: None,
            right: None,
        };

        let Some(mut c) = self.root else {
            node.red = false;
            self.size += 1;
            self.root = Some(unsafe { NonNull::new_unchecked(Box::into_raw(Box::from(node))) });
            return true;
        };

        let mut p;
        loop {
            p = c;

            let next = unsafe {
                match node.cmp(c.as_ref()) {
                    Ordering::Equal => return false,
                    Ordering::Less => c.as_ref().left,
                    Ordering::Greater => c.as_ref().right,
                }
            };

            match next {
                None => break,
                Some(next) => c = next,
            };
        }

        self.size += 1;
        node.parent = Some(p);
        let node = unsafe { NonNull::new_unchecked(Box::into_raw(Box::from(node))) };

        unsafe {
            match node.as_ref().cmp(p.as_ref()) {
                Ordering::Equal => unreachable!(),
                Ordering::Less => p.as_mut().left = Some(node),
                Ordering::Greater => p.as_mut().right = Some(node),
            }
        }

        loop {
            let pb = unsafe { p.as_mut() };

            pb.children += 1;

            if gen > pb.max_gen {
                pb.max_gen = gen;
            } else if gen < pb.min_gen {
                pb.min_gen = gen;
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

    pub fn delete(&mut self, item: &T) -> Option<(T, u64)> {
        let mut n = self.find_node(item)?;

        self.size -= 1;

        let nb = unsafe { n.as_mut() };
        // Ensure the node has only one child by replacing it with its successor
        let n = if let (Some(_), Some(right)) = (nb.left, nb.right) {
            let mut s = right;
            while let Some(l) = unsafe { s.as_ref() }.left {
                s = l;
            }

            let sb = unsafe { s.as_mut() };
            // Only item, hash, and gen need to be swapped,
            // the rest will be recalculated in the next step
            swap(&mut nb.item, &mut sb.item);
            swap(&mut nb.hash, &mut sb.hash);
            swap(&mut nb.gen, &mut sb.gen);
            s
        } else {
            n
        };

        let nb = unsafe { n.as_ref() };
        let p = nb.parent;

        let Some(mut p) = p else {
            // Deleting the root
            match (nb.left, nb.right) {
                (None, None) => self.root = None,
                (Some(_), Some(_)) => unreachable!(),
                (None, Some(mut child)) | (Some(mut child), None) => {
                    self.root = Some(child);
                    let cb = unsafe { child.as_mut() };
                    cb.parent = None;
                    cb.red = false;
                }
            }

            // By now there are no other pointers to n and it can be dropped.
            let n = unsafe { Box::from_raw(n.as_ptr()) };

            return Some((n.item, n.hash));
        };

        let (c, c_red) = match (nb.left, nb.right) {
            (None, None) => (None, false),
            (None, Some(child)) | (Some(child), None) => {
                (Some(child), unsafe { child.as_ref().red })
            }
            (Some(_), Some(_)) => unreachable!(),
        };

        if nb.red || c_red {
            if let Some(mut c) = c {
                let cb = unsafe { c.as_mut() };
                cb.red = false;
                cb.parent = Some(p);
            }

            let pb = unsafe { p.as_mut() };
            if pb.is_left_child(nb) {
                pb.left = c;
            } else {
                pb.right = c;
            }
        } else {
            self.fix_black_node_before_delete(n);

            let nb = unsafe { n.as_ref() };
            let p = nb.parent;
            let pb = unsafe { p.unwrap().as_mut() };

            if pb.is_left_child(nb) {
                pb.left = None;
            } else {
                pb.right = None;
            }
        }

        if let Some(p) = unsafe { n.as_ref().parent } {
            Node::recalc_ancestors(p)
        }

        // By now there are no other pointers to n and it can be dropped.
        let n = unsafe { Box::from_raw(n.as_ptr()) };

        Some((n.item, n.hash))
    }

    fn fix_after_insert(&mut self, node: NonNull<Node<T>>) {
        unsafe {
            let mut c = node;
            let mut p = c.as_ref().parent;
            while let Some(mut pnd) = p {
                if !pnd.as_ref().red {
                    return;
                }

                let mut g = pnd.as_ref().parent.unwrap();
                let gb = g.as_mut();

                let ps = gb.other_child(pnd.as_ref());


                if let Some(mut ps) = ps {
                    let psb = ps.as_mut();
                    if psb.red {
                        let pb = pnd.as_mut();
                        // The parent-sibling is red, so we can continue up the tree
                        pb.red = false;
                        psb.red = false;
                        gb.red = true;
                        c = g;
                        p = c.as_ref().parent;
                        continue;
                    };
                };

                if gb.is_left_child(pnd.as_ref()) {
                    if let Some(pright) = pnd.as_ref().right {
                        if std::ptr::eq(c.as_ptr(), pright.as_ptr()) {
                            self.rotate_left(pnd);
                            pnd = c;
                        }
                    }

                    self.rotate_right(g);
                } else {
                    if let Some(pleft) = pnd.as_ref().left.as_ref() {
                        if std::ptr::eq(c.as_ptr(), pleft.as_ptr()) {
                            self.rotate_right(pnd);
                            pnd = c;
                        }
                    }

                    self.rotate_left(g);
                }
                pnd.as_mut().red = false;
                g.as_mut().red = true;
                return;
            }
            // We've replaced the root, and it cannot be red
            c.as_mut().red = false;
        }
    }

    // This is only called when the node to be deleted is a non-root black node, and therefore has
    // a sibling.
    fn fix_black_node_before_delete(&mut self, mut node: NonNull<Node<T>>) {
        while unsafe { node.as_ref() }.parent.is_some() {
            unsafe {
                let mut p = node.as_ref().parent.expect("Non-root black node must have parent.");
                let pb = p.as_mut();
                let mut s =
                    pb.other_child(node.as_ref()).expect("Non-root black node must have sibling");

                let sb = s.as_mut();

                // The sibling is red, make it black and make it into the new parent.
                if sb.red {
                    sb.red = false;
                    pb.red = true;
                    let left = pb.is_left_child(node.as_ref());
                    if left {
                        self.rotate_left(p);
                    } else {
                        self.rotate_right(p);
                    }
                }
            }

            unsafe {
                let mut p = node.as_ref().parent.expect("Non-root black node must have parent.");
                let pb = p.as_mut();
                let mut s =
                    pb.other_child(node.as_ref()).expect("Non-root black node must have sibling");

                let sb = s.as_mut();

                if !pb.red && !sb.red && !sb.has_red_child() {
                    // All three nodes are black and the sibling has no red children.
                    // Mark S as red so the subtree rooted at p meets the black-path requirement.
                    // Continue up the tree.
                    sb.red = true;
                    node = p;
                    continue;
                }

                if pb.red && !sb.red && !sb.has_red_child() {
                    // Parent is red, S is black with no red children.
                    // We can move the redness down to S and maintain the black-path requirement.
                    sb.red = true;
                    pb.red = false;
                    return;
                }

                let sb = s.as_ref();

                if !sb.red {
                    // All three nodes are black but S has at least one red child.
                    // If there is a single red child on the inside, rotate that child onto S.


                    if pb.is_left_child(node.as_ref()) {
                        if let SoleRedChild::Left(mut l) = sb.sole_red_child() {
                            l.as_mut().red = false;
                            s.as_mut().red = true;
                            self.rotate_right(s);
                        }
                    } else if let SoleRedChild::Right(mut r) = sb.sole_red_child() {
                        r.as_mut().red = false;
                        s.as_mut().red = true;
                        self.rotate_left(s);
                    }
                }
            }

            // S is red or has two red children.
            // Rotate S onto parent and copy parent's colour, make both its children black.

            unsafe {
                let mut p = node.as_ref().parent.expect("Non-root black node must have parent.");
                let mut s = p
                    .as_ref()
                    .other_child(node.as_ref())
                    .expect("Non-root black node must have sibling");

                let pb = p.as_mut();
                let sb = s.as_mut();

                sb.red = pb.red;
                pb.red = false;
                let sb = s.as_ref();

                if pb.is_left_child(node.as_ref()) {
                    if let Some(mut r) = sb.right {
                        r.as_mut().red = false;
                    }
                    self.rotate_left(p);
                } else {
                    if let Some(mut l) = sb.left {
                        l.as_mut().red = false;
                    }
                    self.rotate_right(p);
                }
            }

            return;
        }
    }

    fn rotate_right(&mut self, mut parent: NonNull<Node<T>>) {
        // Left child becomes the new parent
        let pb = unsafe { parent.as_mut() };
        let mut l = pb.left.expect("Tried to make None child into parent");
        let lb = unsafe { l.as_mut() };

        pb.left = lb.right.take();
        if let Some(mut p_left) = pb.left {
            unsafe { p_left.as_mut() }.parent = Some(parent);
        }

        lb.right = Some(parent);
        lb.parent = pb.parent.take();
        pb.parent = Some(l);

        if let Some(mut l_parent) = lb.parent {
            let lpb = unsafe { l_parent.as_mut() };
            if lpb.is_left_child(pb) {
                lpb.left = Some(l);
            } else {
                lpb.right = Some(l);
            }
        } else {
            self.root = Some(l)
        }

        unsafe { parent.as_mut() }.recalculate();
        unsafe { l.as_mut() }.recalculate();
    }

    fn rotate_left(&mut self, mut parent: NonNull<Node<T>>) {
        // Right child becomes the new parent
        let pb = unsafe { parent.as_mut() };
        let mut r = pb.right.expect("Tried to make None child into parent");
        let rb = unsafe { r.as_mut() };

        pb.right = rb.left.take();
        if let Some(mut p_right) = pb.right {
            unsafe { p_right.as_mut() }.parent = Some(parent);
        }

        rb.left = Some(parent);
        rb.parent = pb.parent.take();
        pb.parent = Some(r);

        if let Some(mut r_parent) = rb.parent {
            let rpb = unsafe { r_parent.as_mut() };
            if !rpb.is_left_child(pb) {
                rpb.right = Some(r);
            } else {
                rpb.left = Some(r);
            }
        } else {
            self.root = Some(r)
        }

        unsafe { parent.as_mut() }.recalculate();
        unsafe { r.as_mut() }.recalculate();
    }

    // Only to be used when the generation would overflow a u64
    pub(crate) fn reset(&mut self) {
        if let Some(mut root) = self.root {
            unsafe { root.as_mut().reset() }
        }
    }

    // Finds the next item with a generation <= g after index (inclusive).
    // Wraps around to the start of the tree if one isn't found.
    #[allow(clippy::missing_panics_doc)]
    pub fn find_next(&self, index: usize, gen: u64) -> NonNull<Node<T>> {
        assert!(self.size > 0);
        assert!(index < self.size);
        let root = self.root.expect("Root cannot be None in a tree with size > 0");

        Node::find_above(root, index, gen)
            .or_else(|_| Node::find_above(root, 0, gen))
            .expect("Corrupt tree")
    }

    pub(crate) fn values(&self) -> Vec<&T> {
        let mut out = Vec::with_capacity(self.size);

        if let Some(root) = &self.root {
            unsafe { root.as_ref().values(&mut out) };
        }

        out
    }

    pub(crate) fn into_values(mut self) -> Vec<T> {
        let mut out = Vec::with_capacity(self.size);

        // It's safe to take() self.root as self will immediately be dropped, which does not care
        // about size being stale.
        if let Some(root) = self.root.take() {
            unsafe { Node::into_values(root, &mut out) };
        }

        out
    }

    pub(crate) fn dump(&self) -> Vec<(&T, u64)> {
        let mut out = Vec::with_capacity(self.size);

        if let Some(root) = &self.root {
            unsafe { root.as_ref().dump(&mut out) };
        }

        out
    }

    pub(crate) const fn size(&self) -> usize {
        if let Some(root) = &self.root {
            unsafe { root.as_ref().children + 1 }
        } else {
            0
        }
    }

    pub(crate) const fn generations(&self) -> (u64, u64) {
        if let Some(root) = self.root {
            let root = unsafe { root.as_ref() };
            (root.min_gen, root.max_gen)
        } else {
            (0, 0)
        }
    }
}

#[cfg(test)]
impl<T> Node<T>
where
    T: Item + std::fmt::Display + Debug,
{
    fn pprint(&self, prefix: String) -> String {
        let left = if let Some(left) = self.left {
            unsafe { left.as_ref().pprint(prefix.clone() + "  ") }
        } else {
            String::new()
        };

        let right = if let Some(right) = self.right {
            unsafe { right.as_ref().pprint(prefix.clone() + "  ") }
        } else {
            String::new()
        };

        let c = if self.red { "red" } else { "black" };

        format!(
            "{left}{prefix}{}: {} [{},{}], {c}\n{right}",
            self.item, self.gen, self.min_gen, self.max_gen
        )
    }

    fn print(&self) -> String {
        let left = if let Some(left) = self.left {
            unsafe { left.as_ref().print() }
        } else {
            String::new()
        };

        let right = if let Some(right) = self.right {
            unsafe { right.as_ref().print() }
        } else {
            String::new()
        };

        let c = if self.red { "r" } else { "b" };

        format!("({} {} {c} {left} {right})", self.item, self.gen)
    }

    fn verify(&self) -> usize {
        let mut min_gen = self.gen;
        let mut max_gen = self.gen;
        let mut children = 0;

        unsafe {
            let (l_black, l_red) = if let Some(left) = self.left {
                let lb = left.as_ref();
                assert_eq!(self, lb.parent.unwrap().as_ref());

                assert!(self.hash >= lb.hash);
                assert!(self > lb);

                children += lb.children + 1;
                min_gen = min(min_gen, lb.min_gen);
                max_gen = max(max_gen, lb.max_gen);
                (lb.verify(), lb.red)
            } else {
                (0, false)
            };

            let (r_black, r_red) = if let Some(right) = self.right {
                let rb = right.as_ref();
                assert_eq!(self, rb.parent.unwrap().as_ref());

                assert!(self.hash <= rb.hash);
                assert!(self < rb);

                children += rb.children + 1;
                min_gen = min(min_gen, rb.min_gen);
                max_gen = max(max_gen, rb.max_gen);
                (rb.verify(), rb.red)
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
        match self.root {
            Some(r) => unsafe { r.as_ref().pprint(String::new()) },
            None => String::new(),
        }
    }

    fn print(&self) -> String {
        match self.root {
            Some(r) => unsafe { r.as_ref().print() },
            None => String::new(),
        }
    }

    fn verify(&self) {
        match self.root {
            None => {
                assert_eq!(self.size, 0);
            }
            Some(root) => {
                let rb = unsafe { root.as_ref() };

                assert_eq!(self.size, rb.children + 1);
                assert!(rb.parent.is_none());
                assert!(!rb.red);

                rb.verify();
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

    use super::{Node, Rbtree};

    #[derive(Clone)]
    pub(crate) struct DummyHasher {
        values: Rc<AHashMap<&'static str, u64>>,
        val: u64,
    }

    impl Hasher for DummyHasher {
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

    impl Rbtree<&'static str, DummyHasher> {
        pub(crate) fn new_dummy(entries: &[(&'static str, u64)]) -> Self {
            let hashes: AHashMap<_, _> = entries.iter().copied().collect();
            Self {
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
    fn test_hasher() {
        // ahash may change output when updated, so this test may fail after updating dependencies
        // Can also fail in miri due to different hash output, but not UB.
        let hasher = RandomState::with_seeds(100, 200, 300, 400).build_hasher();
        let mut rb = Rbtree { root: None, size: 0, hasher };

        assert!(rb.insert("5", 0));
        assert!(rb.insert("4", 1));
        assert!(rb.insert("6", 2));

        rb.verify();
        assert_eq!(rb.print(), "(4 1 b (5 0 r  ) (6 2 r  ))");

        let hasher = RandomState::with_seeds(400, 300, 200, 100).build_hasher();
        let mut rb = Rbtree { root: None, size: 0, hasher };

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
        let input = sequential_strings(10000);
        // Use a smaller set for miri since it's way too slow with large sets
        // let input = sequential_strings(100);

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
            assert_eq!((rb.find_next(0, 10).as_ref()).item, "00");
            assert_eq!((rb.find_next(0, 0).as_ref()).item, "10");
            assert_eq!((rb.find_next(0, 1).as_ref()).item, "09");
            assert_eq!((rb.find_next(0, 5).as_ref()).item, "05");
            assert_eq!((rb.find_next(8, 5).as_ref()).item, "08");
            assert_eq!((rb.find_next(8, 9).as_ref()).item, "08");
            assert_eq!((rb.find_next(8, 2).as_ref()).item, "08");
            assert_eq!((rb.find_next(8, 1).as_ref()).item, "09");
            assert_eq!((rb.find_next(10, 0).as_ref()).item, "10");
            assert_eq!((rb.find_next(10, 1).as_ref()).item, "10");
            assert_eq!((rb.find_next(10, 5).as_ref()).item, "10");
            assert_eq!((rb.find_next(10, 10).as_ref()).item, "10");
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
            assert_eq!((rb.find_next(0, 10).as_ref()).item, "00");
            assert_eq!((rb.find_next(0, 4).as_ref()).item, "01");
            assert_eq!((rb.find_next(0, 1).as_ref()).item, "01");
            assert_eq!((rb.find_next(0, 5).as_ref()).item, "00");
            assert_eq!((rb.find_next(8, 5).as_ref()).item, "00");
            assert_eq!((rb.find_next(8, 9).as_ref()).item, "08");
            assert_eq!((rb.find_next(8, 2).as_ref()).item, "01");
            assert_eq!((rb.find_next(8, 1).as_ref()).item, "01");
            assert_eq!((rb.find_next(10, 1).as_ref()).item, "01");
            assert_eq!((rb.find_next(10, 5).as_ref()).item, "00");
            assert_eq!((rb.find_next(10, 10).as_ref()).item, "10");
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

        Node::set_generation(n, 1000);

        assert_eq!(rb.print(), "(5 5 b (2 1000 r  ) (7 7 r  ))");
        rb.verify();
    }
}
