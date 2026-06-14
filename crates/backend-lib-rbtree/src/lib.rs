#![no_std]
#![forbid(unsafe_code)]
//! Generic red-black binary tree — safe, idiomatic rewrite.
//!
//! Idiomatic port of PostgreSQL 18.3 `src/backend/lib/rbtree.c` (itself adopted
//! from Thomas Niemann's "Sorting and Searching Algorithms: a Cookbook").
//! Red-black trees are balanced binary trees in which (1) any child of a red
//! node is always black, and (2) every path from root to leaf traverses an
//! equal number of black nodes, guaranteeing O(lg n) lookups.
//!
//! # Why this is an arena, not raw links
//!
//! The C `rbtree` API is *intrusive*: the caller embeds an `RBTNode`
//! (color + `parent`/`left`/`right` links) inside its own struct and the tree
//! threads aliasing `RBTNode *` pointers through those caller-owned objects,
//! with a single shared `RBTNIL` sentinel node standing in for NULL. That is a
//! graph of shared, mutable, aliasing pointers — the one shape Rust's ownership
//! model forbids with `&mut`/`Box`. The idiomatic and memory-safe Rust
//! equivalent of an intrusive tree with stable node identity is an
//! **index-based (arena) tree**: the tree owns a slab of nodes and the
//! `parent`/`left`/`right` links are small `usize` slot indices rather than raw
//! pointers, with the constant [`SENTINEL`] index (slot 0) playing the role of
//! C's `RBTNIL`.
//!
//! This preserves every behavioural property C relies on:
//!
//! - **Stable identity**: a slot index keeps referring to the same logical node
//!   for as long as it is live, like an `RBTNode *`.
//! - **The shared sentinel**: slot 0 is the `RBTNIL` node — black, self-looping
//!   `left`/`right`, exactly as C `rbt_create` sets up `t->RBTNIL`.
//! - **O(lg n) descent / insert / delete** with the identical
//!   rotate/fixup/delete algorithm, transcribed line-for-line from `rbtree.c`
//!   with `(*p).left` reads becoming `self.nodes[p].left` and so on.
//! - **The C `rbt_copy_data` payload move**: where C `memcpy`s the successor's
//!   payload bytes into the logically-deleted node, this moves the owned `T`
//!   value between slots with a plain `Option::take`/assignment — no clone,
//!   no double-drop.
//!
//! The comparator is an ordinary Rust closure rather than an `extern "C"`
//! function pointer, and the combiner is a closure passed to [`rbt_insert`]
//! rather than a stored `combiner`/`arg` pair. There are no raw pointers, no
//! `extern "C"`, and no `unsafe` anywhere in this crate (`#![forbid(unsafe_code)]`).
//!
//! # Why the safe rewrite (vs the earlier raw-pointer transcription)
//!
//! An earlier version of this crate transcribed `rbtree.c` line-for-line through
//! `*mut RBTNode` raw pointers, `Box::into_raw`, and `extern "C"` callbacks. That
//! reproduced C's aliasing-pointer graph literally and SIGABRTed (null-deref)
//! under the concurrent stress test. This arena rewrite removes all unsafe and is
//! memory-safe by construction while keeping the algorithm identical to C.
//!
//! # Memory management
//!
//! The arena's backing slab (`nodes`) and its `free` recycle list are the only
//! heap this crate owns. The `backend-utils-mctx` charge/free model used by the
//! upstream src-idiomatic tree is dropped here (this repo's memory-context owner,
//! `mcx`, differs and is not a leaf dependency of this crate); following the same
//! decision recorded for `backend-utils-activity-waitevent`, the slabs are plain
//! `alloc::vec::Vec`s. Their growth is still surfaced as a fallible [`PgResult`]
//! via `Vec::try_reserve` — modelling C `rbt_create`'s `palloc` `allocfunc` whose
//! failure is an `elog(ERROR)` non-local exit — so the insert/alloc signatures
//! keep the same failure surface they had in C.
//!
//! Dropping the Rust handle reclaims the tree's storage and drops every
//! still-stored payload exactly once (the `Vec`s own the nodes); unlike C
//! (where the caller's memory context owns the nodes and a dropped handle leaks
//! until context reset) this is automatic and leak-free.

// `clippy::result_large_err`: the allocating operations (insert) return the
// shared `backend_utils_error::PgResult` to model the C `elog(ERROR, ...)`
// non-local exit faithfully (here, OOM from a `try_reserve`). `PgError`'s size
// is fixed by that crate, and the un-boxed `PgResult` is the project-wide error
// contract these ports must match; boxing it locally would diverge from every
// sibling crate's signatures.
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::vec::Vec;
use core::cmp::Ordering;

use backend_utils_error::elog;
use types_error::{PgResult, ERROR};

/// Node color, mirroring C's `RBTColor` (`RBTBLACK = 0`, `RBTRED = 1`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Color {
    Black,
    Red,
}

/// Iteration order, mirroring C's `RBTOrderControl`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RBTOrderControl {
    /// `LeftRightWalk`: ascending (in-order) traversal.
    LeftRightWalk,
    /// `RightLeftWalk`: descending (reverse-in-order) traversal.
    RightLeftWalk,
}

/// The arena index of the shared sentinel node (`RBTNIL`).
///
/// Slot 0 is reserved for the sentinel and never holds caller data; it stands in
/// for C's single shared `RBTNIL` node and for every NULL link.
pub const SENTINEL: usize = 0;

/// One node of the red-black tree arena: the color + intrusive links plus the
/// caller payload. `value` is `None` only for the sentinel slot and for freed
/// slots awaiting reuse.
struct Node<T> {
    color: Color,
    left: usize,
    right: usize,
    parent: usize,
    value: Option<T>,
}

/// A typed, owned red-black tree.
///
/// The arena (`nodes`) owns the elements; `free` recycles removed slots, and the
/// sentinel (`RBTNIL`) lives at slot [`SENTINEL`]. There are no raw pointers and
/// no `unsafe`; see the crate-level docs.
pub struct RBTree<T, C> {
    nodes: Vec<Node<T>>,
    free: Vec<usize>,
    root: usize,
    comparator: C,
}

impl<T: core::fmt::Debug, C: Fn(&T, &T) -> Ordering> core::fmt::Debug for RBTree<T, C> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RBTree")
            .field("values", &self.values_in_order())
            .finish_non_exhaustive()
    }
}

/// Create a red-black tree ordering its payloads by their natural [`Ord`].
///
/// Mirrors C `rbt_create` with the comparator being the payload ordering.
pub fn rbt_create<T: Ord>() -> RBTree<T, fn(&T, &T) -> Ordering> {
    rbt_create_with(|left: &T, right: &T| left.cmp(right))
}

/// Create a red-black tree with a custom comparator closure.
///
/// Mirrors C `rbt_create`: the comparator plays the role of the
/// `rbt_comparator` function pointer (the combiner is supplied per-insert to
/// [`rbt_insert`], and allocation/free is the owned arena, so there are no
/// `allocfunc`/`freefunc`/`arg` parameters).
pub fn rbt_create_with<T, C>(comparator: C) -> RBTree<T, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    // Pushing the sentinel is the only allocation in construction. Mirror C
    // `rbt_create`'s palloc of the control struct / RBTNIL: a fresh tree that
    // cannot hold its own RBTNIL is fatal, so surface OOM by panicking here.
    let mut nodes: Vec<Node<T>> = Vec::new();
    nodes
        .try_reserve(1)
        .expect("rbtree: failed to allocate sentinel node");
    nodes.push(Node {
        color: Color::Black,
        left: SENTINEL,
        right: SENTINEL,
        parent: SENTINEL,
        value: None,
    });
    RBTree {
        nodes,
        free: Vec::new(),
        root: SENTINEL,
        comparator,
    }
}

impl<T, C> RBTree<T, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    fn values_in_order(&self) -> Vec<&T> {
        let mut out = Vec::new();
        let mut iter = rbt_begin_iterate(self, RBTOrderControl::LeftRightWalk);
        while let Some(v) = rbt_iterate(&mut iter) {
            out.push(v);
        }
        out
    }

    /// Read a slot's color.
    #[inline]
    fn color(&self, x: usize) -> Color {
        self.nodes[x].color
    }
    #[inline]
    fn set_color(&mut self, x: usize, c: Color) {
        self.nodes[x].color = c;
    }
    #[inline]
    fn left(&self, x: usize) -> usize {
        self.nodes[x].left
    }
    #[inline]
    fn right(&self, x: usize) -> usize {
        self.nodes[x].right
    }
    #[inline]
    fn parent(&self, x: usize) -> usize {
        self.nodes[x].parent
    }
    #[inline]
    fn set_left(&mut self, x: usize, v: usize) {
        self.nodes[x].left = v;
    }
    #[inline]
    fn set_right(&mut self, x: usize, v: usize) {
        self.nodes[x].right = v;
    }
    #[inline]
    fn set_parent(&mut self, x: usize, v: usize) {
        self.nodes[x].parent = v;
    }
    /// The payload of a live node (panics on the sentinel/freed slots, which the
    /// algorithm never reads payloads from).
    #[inline]
    fn value(&self, x: usize) -> &T {
        self.nodes[x]
            .value
            .as_ref()
            .expect("rbtree: payload read on sentinel/free slot")
    }
    #[inline]
    fn value_mut(&mut self, x: usize) -> &mut T {
        self.nodes[x]
            .value
            .as_mut()
            .expect("rbtree: payload read on sentinel/free slot")
    }

    /* -- arena allocation --------------------------------------------------- */

    /// Allocate a node slot for `value`, reusing a freed slot when possible.
    ///
    /// Fallible: grows the slab via `try_reserve`, surfacing OOM as a `PgError`
    /// (the C `allocfunc`/`palloc` analog, whose failure is an `elog(ERROR)`).
    fn alloc_node(&mut self, value: T) -> PgResult<usize> {
        if let Some(index) = self.free.pop() {
            let n = &mut self.nodes[index];
            n.color = Color::Red;
            n.left = SENTINEL;
            n.right = SENTINEL;
            n.parent = SENTINEL;
            n.value = Some(value);
            Ok(index)
        } else {
            if self.nodes.try_reserve(1).is_err() {
                elog(ERROR, "out of memory: rbtree node allocation failed")?;
            }
            let index = self.nodes.len();
            self.nodes.push(Node {
                color: Color::Red,
                left: SENTINEL,
                right: SENTINEL,
                parent: SENTINEL,
                value: Some(value),
            });
            Ok(index)
        }
    }

    /// Free node `index` (C `freefunc`/`pfree`), returning its payload and
    /// recycling the slot. The payload is moved out, so no destructor runs on a
    /// moved-from slot — matching C's "node passed to freefunc may not contain
    /// valid data" contract.
    fn free_node(&mut self, index: usize) -> PgResult<Option<T>> {
        let value = self.nodes[index].value.take();
        let n = &mut self.nodes[index];
        n.left = SENTINEL;
        n.right = SENTINEL;
        n.parent = SENTINEL;
        n.color = Color::Black;
        if self.free.try_reserve(1).is_err() {
            elog(ERROR, "out of memory: rbtree free-list growth failed")?;
        }
        self.free.push(index);
        Ok(value)
    }

    /* -- search routines (rbt_find / rbt_find_great / rbt_find_less) -------- */

    /// C `rbt_find` descent.
    fn find_node(&self, data: &T) -> usize {
        let mut node = self.root;
        while node != SENTINEL {
            match (self.comparator)(data, self.value(node)) {
                Ordering::Equal => return node,
                Ordering::Less => node = self.left(node),
                Ordering::Greater => node = self.right(node),
            }
        }
        SENTINEL
    }

    /// C `rbt_find_great`.
    fn find_great_node(&self, data: &T, equal_match: bool) -> usize {
        let mut node = self.root;
        let mut greater = SENTINEL;
        while node != SENTINEL {
            match (self.comparator)(data, self.value(node)) {
                Ordering::Equal if equal_match => return node,
                Ordering::Less => {
                    greater = node;
                    node = self.left(node);
                }
                Ordering::Equal | Ordering::Greater => node = self.right(node),
            }
        }
        greater
    }

    /// C `rbt_find_less`.
    fn find_less_node(&self, data: &T, equal_match: bool) -> usize {
        let mut node = self.root;
        let mut lesser = SENTINEL;
        while node != SENTINEL {
            match (self.comparator)(data, self.value(node)) {
                Ordering::Equal if equal_match => return node,
                Ordering::Greater => {
                    lesser = node;
                    node = self.right(node);
                }
                Ordering::Equal | Ordering::Less => node = self.left(node),
            }
        }
        lesser
    }

    /// C `rbt_leftmost`.
    fn leftmost_node(&self) -> usize {
        let mut node = self.root;
        let mut leftmost = self.root;
        while node != SENTINEL {
            leftmost = node;
            node = self.left(node);
        }
        if leftmost != SENTINEL {
            leftmost
        } else {
            SENTINEL
        }
    }

    /* -- rotations (rbt_rotate_left / rbt_rotate_right) --------------------- */

    /// C `rbt_rotate_left`: rotate node `x` left.
    fn rotate_left(&mut self, x: usize) {
        let y = self.right(x); // y = x->right

        // establish x->right link
        self.set_right(x, self.left(y)); // x->right = y->left
        if self.left(y) != SENTINEL {
            self.set_parent(self.left(y), x); // y->left->parent = x
        }

        // establish y->parent link
        if y != SENTINEL {
            self.set_parent(y, self.parent(x)); // y->parent = x->parent
        }
        if self.parent(x) != SENTINEL {
            if x == self.left(self.parent(x)) {
                self.set_left(self.parent(x), y); // x->parent->left = y
            } else {
                self.set_right(self.parent(x), y); // x->parent->right = y
            }
        } else {
            self.root = y;
        }

        // link x and y
        self.set_left(y, x); // y->left = x
        if x != SENTINEL {
            self.set_parent(x, y); // x->parent = y
        }
    }

    /// C `rbt_rotate_right`: rotate node `x` right.
    fn rotate_right(&mut self, x: usize) {
        let y = self.left(x); // y = x->left

        // establish x->left link
        self.set_left(x, self.right(y)); // x->left = y->right
        if self.right(y) != SENTINEL {
            self.set_parent(self.right(y), x); // y->right->parent = x
        }

        // establish y->parent link
        if y != SENTINEL {
            self.set_parent(y, self.parent(x)); // y->parent = x->parent
        }
        if self.parent(x) != SENTINEL {
            if x == self.right(self.parent(x)) {
                self.set_right(self.parent(x), y); // x->parent->right = y
            } else {
                self.set_left(self.parent(x), y); // x->parent->left = y
            }
        } else {
            self.root = y;
        }

        // link x and y
        self.set_right(y, x); // y->right = x
        if x != SENTINEL {
            self.set_parent(x, y); // x->parent = y
        }
    }

    /* -- insert fixup (rbt_insert_fixup) ------------------------------------ */

    /// C `rbt_insert_fixup`: re-balance after inserting RED node `x`.
    fn insert_fixup(&mut self, mut x: usize) {
        // check RED-BLACK properties
        while x != self.root && self.color(self.parent(x)) == Color::Red {
            // we have a violation
            if self.parent(x) == self.left(self.parent(self.parent(x))) {
                let y = self.right(self.parent(self.parent(x))); // uncle
                if self.color(y) == Color::Red {
                    // uncle is RED
                    self.set_color(self.parent(x), Color::Black);
                    self.set_color(y, Color::Black);
                    self.set_color(self.parent(self.parent(x)), Color::Red);
                    x = self.parent(self.parent(x));
                } else {
                    // uncle is BLACK
                    if x == self.right(self.parent(x)) {
                        // make x a left child
                        x = self.parent(x);
                        self.rotate_left(x);
                    }
                    // recolor and rotate
                    self.set_color(self.parent(x), Color::Black);
                    self.set_color(self.parent(self.parent(x)), Color::Red);
                    self.rotate_right(self.parent(self.parent(x)));
                }
            } else {
                // mirror image of above code
                let y = self.left(self.parent(self.parent(x))); // uncle
                if self.color(y) == Color::Red {
                    // uncle is RED
                    self.set_color(self.parent(x), Color::Black);
                    self.set_color(y, Color::Black);
                    self.set_color(self.parent(self.parent(x)), Color::Red);
                    x = self.parent(self.parent(x));
                } else {
                    // uncle is BLACK
                    if x == self.left(self.parent(x)) {
                        x = self.parent(x);
                        self.rotate_right(x);
                    }
                    self.set_color(self.parent(x), Color::Black);
                    self.set_color(self.parent(self.parent(x)), Color::Red);
                    self.rotate_left(self.parent(self.parent(x)));
                }
            }
        }
        self.set_color(self.root, Color::Black);
    }

    /* -- insert (rbt_insert) ------------------------------------------------ */

    /// C `rbt_insert` body, with `combine` standing in for the
    /// `combiner(current, data, arg)` callback.
    ///
    /// Returns `Ok(true)` for a freshly-created node (`*isNew = true`), `Ok(false)`
    /// when an equal node was found and combined into.
    fn insert(&mut self, data: T, combine: &mut dyn FnMut(&mut T, T)) -> PgResult<bool> {
        let mut current = self.root;
        let mut parent = SENTINEL;
        let mut cmp = Ordering::Equal;

        // find where node belongs
        while current != SENTINEL {
            cmp = (self.comparator)(&data, self.value(current));
            if cmp == Ordering::Equal {
                // found node with equal key; combine and return it as not-new.
                combine(self.value_mut(current), data);
                return Ok(false);
            }
            parent = current;
            current = if cmp == Ordering::Less {
                self.left(current)
            } else {
                self.right(current)
            };
        }

        // setup new node
        let x = self.alloc_node(data)?;
        // alloc_node already sets color = RED, links = RBTNIL, parent = RBTNIL.
        self.set_parent(x, parent);

        // insert node in tree
        if parent != SENTINEL {
            if cmp == Ordering::Less {
                self.set_left(parent, x);
            } else {
                self.set_right(parent, x);
            }
        } else {
            self.root = x;
        }

        self.insert_fixup(x);
        Ok(true)
    }

    /* -- delete fixup (rbt_delete_fixup) ------------------------------------ */

    /// C `rbt_delete_fixup`: re-balance after deleting, with `x` the spliced-in
    /// child.
    fn delete_fixup(&mut self, mut x: usize) {
        while x != self.root && self.color(x) == Color::Black {
            if x == self.left(self.parent(x)) {
                let mut w = self.right(self.parent(x));
                if self.color(w) == Color::Red {
                    self.set_color(w, Color::Black);
                    self.set_color(self.parent(x), Color::Red);
                    self.rotate_left(self.parent(x));
                    w = self.right(self.parent(x));
                }
                if self.color(self.left(w)) == Color::Black
                    && self.color(self.right(w)) == Color::Black
                {
                    self.set_color(w, Color::Red);
                    x = self.parent(x);
                } else {
                    if self.color(self.right(w)) == Color::Black {
                        self.set_color(self.left(w), Color::Black);
                        self.set_color(w, Color::Red);
                        self.rotate_right(w);
                        w = self.right(self.parent(x));
                    }
                    self.set_color(w, self.color(self.parent(x)));
                    self.set_color(self.parent(x), Color::Black);
                    self.set_color(self.right(w), Color::Black);
                    self.rotate_left(self.parent(x));
                    x = self.root;
                }
            } else {
                // mirror image of above code
                let mut w = self.left(self.parent(x));
                if self.color(w) == Color::Red {
                    self.set_color(w, Color::Black);
                    self.set_color(self.parent(x), Color::Red);
                    self.rotate_right(self.parent(x));
                    w = self.left(self.parent(x));
                }
                if self.color(self.right(w)) == Color::Black
                    && self.color(self.left(w)) == Color::Black
                {
                    self.set_color(w, Color::Red);
                    x = self.parent(x);
                } else {
                    if self.color(self.left(w)) == Color::Black {
                        self.set_color(self.right(w), Color::Black);
                        self.set_color(w, Color::Red);
                        self.rotate_left(w);
                        w = self.left(self.parent(x));
                    }
                    self.set_color(w, self.color(self.parent(x)));
                    self.set_color(self.parent(x), Color::Black);
                    self.set_color(self.left(w), Color::Black);
                    self.rotate_right(self.parent(x));
                    x = self.root;
                }
            }
        }
        self.set_color(x, Color::Black);
    }

    /* -- delete (rbt_delete_node) ------------------------------------------- */

    /// C `rbt_delete_node`: delete node `z`, returning its payload.
    ///
    /// `y` is the node physically removed (`z` if it has fewer than two children,
    /// else `z`'s in-order successor); `x` is `y`'s only child. After splicing
    /// `y` out we read out `z`'s payload to return it; when `y != z` we move `y`'s
    /// payload into `z`'s slot (C's `rbt_copy_data` memcpy), leaving `y`'s slot
    /// moved-from so `free_node` reclaims it without running a destructor.
    fn delete_node(&mut self, z: usize) -> PgResult<Option<T>> {
        if z == SENTINEL {
            return Ok(None);
        }

        let y = if self.left(z) == SENTINEL || self.right(z) == SENTINEL {
            // y has a RBTNIL node as a child
            z
        } else {
            // find tree successor with a RBTNIL node as a child
            let mut y = self.right(z);
            while self.left(y) != SENTINEL {
                y = self.left(y);
            }
            y
        };

        // x is y's only child
        let x = if self.left(y) != SENTINEL {
            self.left(y)
        } else {
            self.right(y)
        };

        // remove y from the parent chain
        self.set_parent(x, self.parent(y));
        if self.parent(y) != SENTINEL {
            if y == self.left(self.parent(y)) {
                self.set_left(self.parent(y), x);
            } else {
                self.set_right(self.parent(y), x);
            }
        } else {
            self.root = x;
        }

        // Read out z's payload to hand back to the caller; this leaves z's slot
        // logically empty (the C `return` value lives in the deleted node).
        let removed = self.nodes[z].value.take();

        // If we removed the successor (y), move its payload into z's slot, the
        // idiomatic equivalent of C's `rbt_copy_data(rbt, z, y)` memcpy.
        if y != z {
            let moved = self.nodes[y].value.take();
            self.nodes[z].value = moved;
        }

        if self.color(y) == Color::Black {
            self.delete_fixup(x);
        }

        // free_node recycles y; y's payload slot is already moved-from.
        self.free_node(y)?;

        Ok(removed)
    }
}

/* ============================================================================
 * Public free-function API (mirrors rbtree.c's exported functions)
 * ========================================================================= */

/// C `rbt_find`: find the entry comparing equal to `data`.
pub fn rbt_find<'a, T, C>(rbt: &'a RBTree<T, C>, data: &T) -> Option<&'a T>
where
    C: Fn(&T, &T) -> Ordering,
{
    let node = rbt.find_node(data);
    (node != SENTINEL).then(|| rbt.value(node))
}

/// C `rbt_find_great`: the smallest entry greater than `data` (or, if
/// `equal_match`, an equal one).
pub fn rbt_find_great<'a, T, C>(rbt: &'a RBTree<T, C>, data: &T, equal_match: bool) -> Option<&'a T>
where
    C: Fn(&T, &T) -> Ordering,
{
    let node = rbt.find_great_node(data, equal_match);
    (node != SENTINEL).then(|| rbt.value(node))
}

/// C `rbt_find_less`: the largest entry less than `data` (or, if `equal_match`,
/// an equal one).
pub fn rbt_find_less<'a, T, C>(rbt: &'a RBTree<T, C>, data: &T, equal_match: bool) -> Option<&'a T>
where
    C: Fn(&T, &T) -> Ordering,
{
    let node = rbt.find_less_node(data, equal_match);
    (node != SENTINEL).then(|| rbt.value(node))
}

/// C `rbt_leftmost`: the leftmost (smallest) entry, or `None` if the tree is
/// empty.
pub fn rbt_leftmost<T, C>(rbt: &RBTree<T, C>) -> Option<&T>
where
    C: Fn(&T, &T) -> Ordering,
{
    let node = rbt.leftmost_node();
    (node != SENTINEL).then(|| rbt.value(node))
}

/// C `rbt_insert`: insert a value into the tree.
///
/// If no entry compares equal to `data`, a new entry is created from `data` and
/// `Ok(true)` is returned (C's `*isNew = true`). Otherwise the `combine` closure
/// is invoked with a mutable reference to the existing entry and the proposed
/// `data` (matching C's `combiner(existing, newdata, arg)`), and `Ok(false)` is
/// returned. As in C, the only allocation that can fail is the new-node
/// allocation, surfaced via [`PgResult`].
pub fn rbt_insert<T, C>(
    rbt: &mut RBTree<T, C>,
    data: T,
    mut combine: impl FnMut(&mut T, T),
) -> PgResult<bool>
where
    C: Fn(&T, &T) -> Ordering,
{
    rbt.insert(data, &mut combine)
}

/// Remove the entry comparing equal to `data`, returning its value.
///
/// The typed-wrapper analogue of C `rbt_find` + `rbt_delete`: it locates the
/// matching entry, unlinks it via the C `rbt_delete_node` algorithm (moving the
/// successor's payload exactly as C's `rbt_copy_data` does), and returns the
/// removed payload by value. Returns `Ok(None)` when no entry matches (C would
/// be undefined behaviour on a bogus node pointer).
pub fn rbt_delete<T, C>(rbt: &mut RBTree<T, C>, data: &T) -> PgResult<Option<T>>
where
    C: Fn(&T, &T) -> Ordering,
{
    let node = rbt.find_node(data);
    if node == SENTINEL {
        return Ok(None);
    }
    rbt.delete_node(node)
}

/// C `rbt_begin_iterate`: prepare a traversal in the given order.
pub fn rbt_begin_iterate<'a, T, C>(
    rbt: &'a RBTree<T, C>,
    ctrl: RBTOrderControl,
) -> RBTreeIterator<'a, T, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    RBTreeIterator {
        tree: rbt,
        last_visited: SENTINEL,
        started: false,
        is_over: rbt.root == SENTINEL,
        order: ctrl,
    }
}

/// C `rbt_iterate`: yield the next entry of the traversal, or `None` when done.
pub fn rbt_iterate<'a, T, C>(iter: &mut RBTreeIterator<'a, T, C>) -> Option<&'a T>
where
    C: Fn(&T, &T) -> Ordering,
{
    if iter.is_over {
        return None;
    }
    let tree = iter.tree;
    let node = match iter.order {
        RBTOrderControl::LeftRightWalk => left_right_iterator(tree, iter),
        RBTOrderControl::RightLeftWalk => right_left_iterator(tree, iter),
    };
    if node == SENTINEL {
        None
    } else {
        Some(tree.value(node))
    }
}

/* ============================================================================
 * Iterators (rbt_left_right_iterator / rbt_right_left_iterator)
 * ========================================================================= */

/// Traversal state for [`rbt_begin_iterate`] / [`rbt_iterate`], mirroring C's
/// `RBTreeIterator`.
///
/// `last_visited`/`is_over` mirror the C fields; an extra `started` flag
/// distinguishes "iteration not begun" from "currently sitting on the sentinel
/// slot", which the C code expresses via a NULL `last_visited` pointer (the
/// arena's sentinel is a real index, so we cannot overload it for "not begun").
pub struct RBTreeIterator<'a, T, C> {
    tree: &'a RBTree<T, C>,
    last_visited: usize,
    started: bool,
    is_over: bool,
    order: RBTOrderControl,
}

/// C `rbt_left_right_iterator`.
fn left_right_iterator<T, C>(tree: &RBTree<T, C>, iter: &mut RBTreeIterator<'_, T, C>) -> usize
where
    C: Fn(&T, &T) -> Ordering,
{
    if !iter.started {
        iter.started = true;
        iter.last_visited = tree.root;
        while tree.left(iter.last_visited) != SENTINEL {
            iter.last_visited = tree.left(iter.last_visited);
        }
        return iter.last_visited;
    }

    if tree.right(iter.last_visited) != SENTINEL {
        iter.last_visited = tree.right(iter.last_visited);
        while tree.left(iter.last_visited) != SENTINEL {
            iter.last_visited = tree.left(iter.last_visited);
        }
        return iter.last_visited;
    }

    loop {
        let came_from = iter.last_visited;
        iter.last_visited = tree.parent(iter.last_visited);
        if iter.last_visited == SENTINEL {
            iter.is_over = true;
            break;
        }
        if tree.left(iter.last_visited) == came_from {
            break;
        }
    }
    iter.last_visited
}

/// C `rbt_right_left_iterator`.
fn right_left_iterator<T, C>(tree: &RBTree<T, C>, iter: &mut RBTreeIterator<'_, T, C>) -> usize
where
    C: Fn(&T, &T) -> Ordering,
{
    if !iter.started {
        iter.started = true;
        iter.last_visited = tree.root;
        while tree.right(iter.last_visited) != SENTINEL {
            iter.last_visited = tree.right(iter.last_visited);
        }
        return iter.last_visited;
    }

    if tree.left(iter.last_visited) != SENTINEL {
        iter.last_visited = tree.left(iter.last_visited);
        while tree.right(iter.last_visited) != SENTINEL {
            iter.last_visited = tree.right(iter.last_visited);
        }
        return iter.last_visited;
    }

    loop {
        let came_from = iter.last_visited;
        iter.last_visited = tree.parent(iter.last_visited);
        if iter.last_visited == SENTINEL {
            iter.is_over = true;
            break;
        }
        if tree.right(iter.last_visited) == came_from {
            break;
        }
    }
    iter.last_visited
}

#[cfg(test)]
extern crate std;

#[cfg(test)]
mod tests;
