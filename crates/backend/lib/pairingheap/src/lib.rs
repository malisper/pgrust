#![no_std]
#![forbid(unsafe_code)]
//! A Pairing Heap implementation — idiomatic port.
//!
//! Idiomatic port of PostgreSQL 18.3 `src/backend/lib/pairingheap.c`. A pairing
//! heap is a data structure that's useful for implementing priority queues: it
//! is simple to implement, and provides amortized O(1) insert and find-min
//! operations, and amortized O(log n) delete-min.
//!
//! The pairing heap was first described in this paper:
//!
//! > Michael L. Fredman, Robert Sedgewick, Daniel D. Sleator, and Robert E.
//! > Tarjan. 1986. The pairing heap: a new form of self-adjusting heap.
//! > Algorithmica 1, 1 (January 1986), pages 111-129. DOI: 10.1007/BF01840439
//!
//! # Why this is an arena, not raw links
//!
//! The C `pairingheap` API is *intrusive*: the caller embeds a `pairingheap_node`
//! (the `first_child`/`next_sibling`/`prev_or_parent` links) inside its own
//! struct and the heap threads aliasing `pairingheap_node *` pointers through
//! those caller-owned objects, recovering the payload with the
//! `pairingheap_container()` offset-of macro. A node can have multiple children
//! forming a doubly-linked sibling list: `first_child` points to the first
//! child, the rest follow `next_sibling`, the last child has a NULL
//! `next_sibling`, and `prev_or_parent` points to the previous sibling or, for a
//! first child, up to the parent. That is a graph of shared, mutable, aliasing
//! pointers — the one shape Rust's ownership model forbids with `&mut`/`Box`.
//!
//! The idiomatic and memory-safe Rust equivalent of an intrusive heap with
//! stable node identity is an **index-based (arena) heap**: the heap owns a slab
//! of nodes and the `first_child`/`next_sibling`/`prev_or_parent` links are small
//! [`usize`] slot indices rather than raw pointers, with the constant [`NONE`]
//! index playing the role of C's `NULL`.
//!
//! This preserves every behavioural property C relies on:
//!
//! - **Stable identity**: a slot index keeps referring to the same logical node
//!   for as long as it is live, like a `pairingheap_node *`. Removed-but-not-freed
//!   slots are recycled through a free list, exactly as a caller would reuse the
//!   storage backing a removed C node.
//! - **The `NULL` link**: the [`NONE`] index stands in for every NULL pointer,
//!   including the empty-heap root (`ph_root == NULL`).
//! - **Amortized O(1) insert / find-first, O(log n) delete** with the identical
//!   two-pass `merge`/`merge_children` algorithm, transcribed line-for-line from
//!   `pairingheap.c` with `a->first_child` reads becoming `self.first_child(a)`
//!   and so on. The comparator's `<0` / swap semantics, the child-linking order,
//!   the pair-then-merge loop, and the arbitrary-remove splice are all preserved
//!   exactly.
//! - **The garbage-link contract**: C's `merge`/`merge_children`/`remove_first`
//!   deliberately leave a returned or removed node's `next_sibling`/
//!   `prev_or_parent` (and, for the removed root, `first_child`) as garbage. The
//!   raw-faithful primitives here do the same (they simply don't touch those
//!   fields); the typed wrapper clears a removed node's links before recycling so
//!   the safe API never observes that garbage.
//!
//! The comparator is an ordinary Rust closure rather than an `extern "C"`
//! function pointer, and there is no `ph_arg` opaque pointer (the closure
//! captures whatever it needs). There are no raw pointers, no `extern "C"`, and
//! no `unsafe` anywhere in this crate.
//!
//! # Allocation / `palloc` failure
//!
//! C's `pairingheap_allocate` is the only function that `palloc`s (the heap
//! control struct); the per-node storage is caller-owned and `pairingheap_free`
//! frees only the control struct, never the nodes. In this arena port the only
//! allocation is the arena spine (`nodes`) and the free-recycle list (`free`);
//! both grow via `try_reserve`, surfacing OOM as a [`PgError`] with
//! `ERRCODE_OUT_OF_MEMORY` — the faithful analogue of C's `palloc` failure
//! (`ereport(ERROR, ...)`, a non-local exit). The arena's `Vec`s free themselves
//! on [`Drop`], reclaiming the heap's storage automatically; unlike C (where the
//! caller's memory context owns the nodes and a dropped handle leaks until
//! context reset) this is automatic and leak-free.

// `clippy::result_large_err`: the allocating operation (`add`) returns the shared
// `types_error::PgResult` (== `Result<_, PgError>`) to model the C `palloc`
// failure / `elog(ERROR, ...)` non-local exit faithfully. `PgError`'s size is
// fixed by that crate, and the un-boxed `PgResult` is the project-wide error
// contract these ports must match; boxing it locally would diverge from every
// sibling crate's signatures.
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::vec::Vec;
use core::cmp::Ordering;

use types_error::{PgError, PgResult, ERRCODE_OUT_OF_MEMORY};

/// The arena index standing in for C's `NULL` link.
///
/// Every `first_child`/`next_sibling`/`prev_or_parent` link that C would set to
/// `NULL` is this sentinel index, and an empty heap has `root == NONE`
/// (`ph_root == NULL`).
pub const NONE: usize = usize::MAX;

/// Build the OOM `PgError` raised when an arena spine cannot grow — the analogue
/// of C's `palloc` failure (`ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), ...)))`.
fn oom(what: &str) -> PgError {
    let mut msg = alloc::string::String::from("out of memory: pairingheap ");
    msg.push_str(what);
    PgError::error(msg).with_sqlstate(ERRCODE_OUT_OF_MEMORY)
}

/// One node of the pairing-heap arena: the intrusive links plus the caller
/// payload.
///
/// `value` is `None` only for freed slots awaiting reuse (a node moved out by a
/// recycle). The three link fields mirror C's `pairingheap_node`
/// (`first_child`/`next_sibling`/`prev_or_parent`), each an index or [`NONE`].
struct Node<T> {
    first_child: usize,
    next_sibling: usize,
    prev_or_parent: usize,
    value: Option<T>,
    /// Bumped every time this slot is (re)allocated. A [`PairingHeapHandle`]
    /// captures the generation at `add` time so a stale handle — one whose slot
    /// was removed and recycled into a *different* logical node — is detected and
    /// rejected, rather than silently operating on the wrong payload. C has no
    /// such guard (a `pairingheap_remove` of an already-removed node is undefined
    /// behaviour); this is the safe-API analogue.
    generation: u64,
}

/// A typed, owned pairing heap.
///
/// The arena (`nodes`) owns the elements; `free` recycles removed slots, and the
/// empty-heap root is [`NONE`].
///
/// This is a max-heap with respect to the supplied comparator (the comparator
/// returns `Less` iff `a < b`, etc., exactly as C's `pairingheap_comparator`
/// returns `<0` iff `a < b`).
pub struct PairingHeap<T, C> {
    nodes: Vec<Node<T>>,
    free: Vec<usize>,
    /// C `ph_root`; [`NONE`] when the heap is empty.
    root: usize,
    comparator: C,
    /// Monotonic source for [`Node::generation`] (slot-recycling guard).
    next_generation: u64,
}

/// A handle to a node added via [`PairingHeap::add`], usable with
/// [`PairingHeap::remove`] to remove that specific node (the safe analogue of
/// passing a `pairingheap_node *` to C's `pairingheap_remove`).
///
/// The handle pairs the arena slot index with the generation stamped at `add`
/// time, so a handle to an already-removed node (whose slot may have been
/// recycled into a different logical node) is detected and rejected rather than
/// silently corrupting the heap.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PairingHeapHandle {
    index: usize,
    generation: u64,
}

/// Create a pairing heap with the given comparator closure (max-heap).
///
/// Mirrors C `pairingheap_allocate(compare, arg)`: the comparator plays the role
/// of the `pairingheap_comparator` function pointer (the closure captures
/// whatever the C code would pass through `arg`, so there are no
/// `allocfunc`/`arg` parameters), and the empty heap starts with `ph_root` ==
/// [`NONE`].
pub fn pairingheap_allocate<T, C>(comparator: C) -> PairingHeap<T, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    PairingHeap {
        nodes: Vec::new(),
        free: Vec::new(),
        root: NONE,
        comparator,
        next_generation: 0,
    }
}

/// Convenience constructor ordering payloads by their natural [`Ord`] (max-heap).
pub fn pairingheap_allocate_ord<T: Ord>() -> PairingHeap<T, fn(&T, &T) -> Ordering> {
    pairingheap_allocate(|a: &T, b: &T| a.cmp(b))
}

impl<T, C> PairingHeap<T, C>
where
    C: Fn(&T, &T) -> Ordering,
{
    /* -- link accessors (the `(*x).field` reads/writes in C) ---------------- */

    #[inline]
    fn first_child(&self, x: usize) -> usize {
        self.nodes[x].first_child
    }
    #[inline]
    fn next_sibling(&self, x: usize) -> usize {
        self.nodes[x].next_sibling
    }
    #[inline]
    fn prev_or_parent(&self, x: usize) -> usize {
        self.nodes[x].prev_or_parent
    }
    #[inline]
    fn set_first_child(&mut self, x: usize, v: usize) {
        self.nodes[x].first_child = v;
    }
    #[inline]
    fn set_next_sibling(&mut self, x: usize, v: usize) {
        self.nodes[x].next_sibling = v;
    }
    #[inline]
    fn set_prev_or_parent(&mut self, x: usize, v: usize) {
        self.nodes[x].prev_or_parent = v;
    }

    /// The payload of a live node (panics on freed slots, which the algorithm
    /// never reads payloads from).
    #[inline]
    fn value(&self, x: usize) -> &T {
        self.nodes[x]
            .value
            .as_ref()
            .expect("pairingheap: payload read on free slot")
    }

    /* -- arena allocation --------------------------------------------------- */

    /// Allocate a node slot for `value`, reusing a freed slot when possible.
    ///
    /// Fallible: grows the arena via `try_reserve`, surfacing OOM as a `PgError`
    /// (the C `palloc` analog, whose failure is an `ereport(ERROR)`). Links are
    /// NOT initialized here — each call site sets them exactly as the C code does
    /// on the freshly-storage-allocated node (e.g. `pairingheap_add` sets
    /// `node->first_child = NULL`).
    fn alloc_node(&mut self, value: T) -> PgResult<usize> {
        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1);
        if let Some(index) = self.free.pop() {
            let n = &mut self.nodes[index];
            n.first_child = NONE;
            n.next_sibling = NONE;
            n.prev_or_parent = NONE;
            n.value = Some(value);
            n.generation = generation;
            Ok(index)
        } else {
            let index = self.nodes.len();
            self.nodes
                .try_reserve(1)
                .map_err(|_| oom("node allocation failed"))?;
            self.nodes.push(Node {
                first_child: NONE,
                next_sibling: NONE,
                prev_or_parent: NONE,
                value: Some(value),
                generation,
            });
            Ok(index)
        }
    }

    /// Recycle node `index`, returning its payload. The payload is moved out, so
    /// no destructor runs on a moved-from slot, and the slot's links are cleared
    /// so a future reader never sees stale indices.
    fn free_node(&mut self, index: usize) -> PgResult<Option<T>> {
        let value = self.nodes[index].value.take();
        let n = &mut self.nodes[index];
        n.first_child = NONE;
        n.next_sibling = NONE;
        n.prev_or_parent = NONE;
        self.free
            .try_reserve(1)
            .map_err(|_| oom("free-list growth failed"))?;
        self.free.push(index);
        Ok(value)
    }

    /* -- merge (static merge() in pairingheap.c) ---------------------------- */

    /// C `merge`: merge two subheaps into one.
    ///
    /// The subheap with smaller value is put as a child of the other one (a
    /// max-heap). The `next_sibling` and `prev_or_parent` links of the inputs are
    /// ignored; on return, the returned node's `next_sibling`/`prev_or_parent` are
    /// garbage (left untouched, exactly as in C).
    fn merge(&mut self, mut a: usize, mut b: usize) -> usize {
        if a == NONE {
            return b;
        }
        if b == NONE {
            return a;
        }

        // swap 'a' and 'b' so that 'a' is the one with larger value
        if (self.comparator)(self.value(a), self.value(b)) == Ordering::Less {
            core::mem::swap(&mut a, &mut b);
        }

        // and put 'b' as a child of 'a'
        if self.first_child(a) != NONE {
            let fc = self.first_child(a);
            self.set_prev_or_parent(fc, b);
        }
        self.set_prev_or_parent(b, a);
        let fc = self.first_child(a);
        self.set_next_sibling(b, fc);
        self.set_first_child(a, b);

        a
    }

    /* -- merge_children (static merge_children() in pairingheap.c) ---------- */

    /// C `merge_children`: merge a list of subheaps into a single heap, using the
    /// basic two-pass merging strategy — first forming pairs from left to right,
    /// then merging the pairs.
    fn merge_children(&mut self, children: usize) -> usize {
        if children == NONE || self.next_sibling(children) == NONE {
            return children;
        }

        // Walk the subheaps from left to right, merging in pairs
        let mut next = children;
        let mut pairs = NONE;
        loop {
            let mut curr = next;

            if curr == NONE {
                break;
            }

            if self.next_sibling(curr) == NONE {
                // last odd node at the end of list
                self.set_next_sibling(curr, pairs);
                pairs = curr;
                break;
            }

            next = self.next_sibling(self.next_sibling(curr));

            // merge this and the next subheap, and add to 'pairs' list.
            let sib = self.next_sibling(curr);
            curr = self.merge(curr, sib);
            self.set_next_sibling(curr, pairs);
            pairs = curr;
        }

        // Merge all the pairs together to form a single heap.
        let mut newroot = pairs;
        next = self.next_sibling(pairs);
        while next != NONE {
            let curr = next;
            next = self.next_sibling(curr);

            newroot = self.merge(newroot, curr);
        }

        newroot
    }

    /* -- add body (pairingheap_add) ----------------------------------------- */

    /// C `pairingheap_add` body, operating on an already-allocated slot `node`.
    fn add_node(&mut self, node: usize) {
        self.set_first_child(node, NONE);

        // Link the new node as a new tree
        self.root = self.merge(self.root, node);
        self.set_prev_or_parent(self.root, NONE);
        self.set_next_sibling(self.root, NONE);
    }

    /* -- remove_first body (pairingheap_remove_first) ----------------------- */

    /// C `pairingheap_remove_first`: remove the root, rebalance, return the old
    /// root slot. The caller guarantees a non-empty heap (the public wrapper
    /// guards the empty case).
    fn remove_first_node(&mut self) -> usize {
        // Remove the root, and form a new heap of its children.
        let result = self.root;
        let children = self.first_child(result);

        self.root = self.merge_children(children);
        if self.root != NONE {
            self.set_prev_or_parent(self.root, NONE);
            self.set_next_sibling(self.root, NONE);
        }

        result
    }

    /* -- remove body (pairingheap_remove) ----------------------------------- */

    /// C `pairingheap_remove`: remove the given node from the heap.
    fn remove_node(&mut self, node: usize) {
        // If the removed node happens to be the root node, do it with
        // remove_first().
        if node == self.root {
            let _ = self.remove_first_node();
            return;
        }

        // Before we modify anything, remember the removed node's first_child and
        // next_sibling pointers.
        let children = self.first_child(node);
        let next_sibling = self.next_sibling(node);

        // Also find the pointer to the removed node in its previous sibling, or
        // if this is the first child of its parent, in its parent.
        //
        // C takes the address of either `prev_or_parent->first_child` or
        // `prev_or_parent->next_sibling`; we capture which field of which slot to
        // write instead (`*prev_ptr = ...`).
        let pop = self.prev_or_parent(node);
        let prev_is_first_child = self.first_child(pop) == node;
        debug_assert_eq!(
            if prev_is_first_child {
                self.first_child(pop)
            } else {
                self.next_sibling(pop)
            },
            node
        );

        // If this node has children, make a new subheap of the children and link
        // the subheap in place of the removed node. Otherwise just unlink this
        // node.
        if children != NONE {
            let replacement = self.merge_children(children);

            self.set_prev_or_parent(replacement, self.prev_or_parent(node));
            self.set_next_sibling(replacement, self.next_sibling(node));
            if prev_is_first_child {
                self.set_first_child(pop, replacement);
            } else {
                self.set_next_sibling(pop, replacement);
            }
            if next_sibling != NONE {
                self.set_prev_or_parent(next_sibling, replacement);
            }
        } else {
            if prev_is_first_child {
                self.set_first_child(pop, next_sibling);
            } else {
                self.set_next_sibling(pop, next_sibling);
            }
            if next_sibling != NONE {
                self.set_prev_or_parent(next_sibling, self.prev_or_parent(node));
            }
        }
    }

    /* ========================================================================
     * Public method API (mirrors pairingheap.c's exported functions and
     * pairingheap.h's macros)
     * ===================================================================== */

    /// C macro `pairingheap_reset(h)`: reset the heap to be empty.
    ///
    /// Drops every still-stored payload (recycling its slot) so the safe API
    /// never leaks. C's macro merely nulls `ph_root` (the caller owns the node
    /// storage and recycles it separately); here the arena owns the payloads, so
    /// resetting must free them.
    pub fn reset(&mut self) {
        for i in 0..self.nodes.len() {
            if self.nodes[i].value.is_some() {
                // free_node only grows the free list, whose worst case is one
                // slot per node; growth failure here is the C palloc-failure
                // analog and is swallowed (reset cannot fail in C).
                let _ = self.free_node(i);
            }
        }
        self.root = NONE;
    }

    /// C macro `pairingheap_is_empty(h)`.
    pub fn is_empty(&self) -> bool {
        self.root == NONE
    }

    /// C macro `pairingheap_is_singular(h)`: is there exactly one node?
    pub fn is_singular(&self) -> bool {
        self.root != NONE && self.first_child(self.root) == NONE
    }

    /// C `pairingheap_add`: add a value to the heap in O(1) time.
    ///
    /// Returns a [`PairingHeapHandle`] for the inserted node, which can later be
    /// passed to [`remove`](Self::remove) to remove that specific node (the safe
    /// analogue of holding a `pairingheap_node *`). As in C, the only operation
    /// that can fail is the node allocation, surfaced via [`PgResult`].
    pub fn add(&mut self, value: T) -> PgResult<PairingHeapHandle> {
        let node = self.alloc_node(value)?;
        let generation = self.nodes[node].generation;
        self.add_node(node);
        Ok(PairingHeapHandle {
            index: node,
            generation,
        })
    }

    /// C `pairingheap_remove`: remove a specific node (identified by the handle
    /// from [`add`](Self::add)) and return its value. O(log n) amortized.
    ///
    /// Returns `None` if the handle is stale — its node was already removed (so
    /// either the slot is free or it has been recycled into a different logical
    /// node). C has no such check (it is undefined behaviour to remove an
    /// already-removed node); this is the safe-API guard.
    pub fn remove(&mut self, handle: PairingHeapHandle) -> Option<T> {
        // Reject a stale handle: the slot must still hold a live value whose
        // generation matches the one stamped when the handle was issued.
        if handle.index >= self.nodes.len()
            || self.nodes[handle.index].value.is_none()
            || self.nodes[handle.index].generation != handle.generation
        {
            return None;
        }
        self.remove_node(handle.index);
        // remove_node() unlinked the node but left its links as garbage (faithful
        // to C); free_node recycles the slot and returns the moved-out payload.
        self.free_node(handle.index).ok().flatten()
    }

    /// C `pairingheap_first`: a reference to the first (root, topmost) node
    /// without modifying the heap, or `None` if the heap is empty.
    ///
    /// C asserts the heap is non-empty and dereferences `ph_root`; the safe
    /// analogue returns `None` for an empty heap. Always O(1).
    pub fn first(&self) -> Option<&T> {
        (self.root != NONE).then(|| self.value(self.root))
    }

    /// C `pairingheap_remove_first`: remove and return the first (root, topmost)
    /// node after rebalancing, or `None` if the heap is empty. O(log n) amortized.
    pub fn remove_first(&mut self) -> Option<T> {
        if self.root == NONE {
            return None;
        }
        let slot = self.remove_first_node();
        // free_node recycles the old root slot, moving its payload out to return.
        self.free_node(slot).ok().flatten()
    }
}

#[cfg(test)]
extern crate std;

#[cfg(test)]
mod tests;
