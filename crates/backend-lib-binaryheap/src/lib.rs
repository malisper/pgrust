//! Logic-exact port of PostgreSQL's simple binary heap
//! (`src/common/binaryheap.c`, `src/include/lib/binaryheap.h`).
//!
//! 1:1 port of the sift-up / sift-down max-heap algorithm. The array layout
//! (root at 0, children of `i` at `2*i+1` / `2*i+2`, parent at `(i-1)/2`), the
//! "hole" optimisation in [`BinaryHeap::sift_up`]/[`BinaryHeap::sift_down`], and
//! the `bh_has_heap_property` cross-check are preserved exactly.
//!
//! # Idiomatic mapping of the C ABI
//!
//! * `bh_node_type` is `Datum` in the backend and `void *` in the frontend —
//!   both opaque, comparator-interpreted payloads, so the port generalises to a
//!   type parameter `T`: callers instantiate `BinaryHeap<Datum>`, `BinaryHeap<*mut
//!   c_void>`, or any owned payload.
//! * `binaryheap_comparator` is `int (*)(bh_node_type a, bh_node_type b, void *arg)`
//!   — a function pointer plus an explicit `void *arg`. The replacement is a
//!   stored closure `C: FnMut(&T, &T) -> i32`: the `arg` user-data is captured
//!   by the closure, and the `>0 / ==0 / <0` three-way result contract is
//!   preserved exactly (this is *not* `Ordering`, so the faithful `cmp > 0` /
//!   `cmp < 0` branch logic transcribes verbatim).
//! * `palloc(offsetof(binaryheap, bh_nodes) + sizeof(bh_node_type) * capacity)`
//!   becomes a `Vec<T>` pre-reserved to `capacity` via `try_reserve_exact`,
//!   surfacing OOM as a [`PgError`] with `ERRCODE_OUT_OF_MEMORY` — the faithful
//!   analogue of C's `palloc` failure (`ereport(ERROR, ...)`, a non-local exit).
//!   The mctx charge model is dropped, matching the repo's other leaf
//!   data-structure ports (`backend-lib-rbtree`, `backend-lib-pairingheap`).
//! * The fixed `bh_space` capacity and the "out of binary heap slots" overflow
//!   condition (`elog(ERROR)` / `pg_fatal`) are preserved as a [`PgError`]:
//!   [`BinaryHeap::add`] and [`BinaryHeap::add_unordered`] return `Err` when the
//!   heap is already at capacity, mirroring C's non-local error exit.

#![no_std]
#![forbid(unsafe_code)]
#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;
use types_error::{PgError, PgResult, ERRCODE_OUT_OF_MEMORY};

/// Build the OOM `PgError` raised when the node array cannot be reserved — the
/// analogue of C's `palloc` failure (`ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), ...))`).
fn oom() -> PgError {
    PgError::error("out of memory allocating binary heap").with_sqlstate(ERRCODE_OUT_OF_MEMORY)
}

/// Build the "out of binary heap slots" error raised when an `add` would exceed
/// the fixed capacity — C's `elog(ERROR, "out of binary heap slots")` /
/// `pg_fatal("out of binary heap slots")`.
fn out_of_slots() -> PgError {
    PgError::error("out of binary heap slots").with_sqlstate(ERRCODE_OUT_OF_MEMORY)
}

/// A `binaryheap`: a fixed-capacity max-heap over payloads of type `T`, ordered
/// by the comparator `C`.
///
/// Mirrors C's `struct binaryheap`:
/// * `nodes` / `nodes.len()` ↔ `bh_nodes` / `bh_size`,
/// * `space` ↔ `bh_space` (the fixed capacity),
/// * `has_heap_property` ↔ `bh_has_heap_property`,
/// * `compare` ↔ `bh_compare` + `bh_arg` folded into a closure.
pub struct BinaryHeap<T, C: FnMut(&T, &T) -> i32> {
    /// `bh_space`: how many nodes can be stored.
    space: i32,
    /// `bh_has_heap_property`: no unordered operations since last heap build.
    has_heap_property: bool,
    /// `bh_compare` (with `bh_arg` captured): the comparison closure.
    compare: C,
    /// `bh_nodes` (and `bh_size` == `nodes.len()`): the heap array.
    nodes: Vec<T>,
}

/// The offset of the left child of the node at index `i`.
#[inline]
fn left_offset(i: i32) -> i32 {
    2 * i + 1
}

/// The offset of the right child of the node at index `i`.
#[inline]
fn right_offset(i: i32) -> i32 {
    2 * i + 2
}

/// The offset of the parent of the node at index `i`.
#[inline]
fn parent_offset(i: i32) -> i32 {
    (i - 1) / 2
}

impl<T, C: FnMut(&T, &T) -> i32> BinaryHeap<T, C> {
    /// `binaryheap_allocate`
    ///
    /// Returns a newly-allocated heap that has the capacity to store the given
    /// number of nodes, with the heap property defined by the given comparator
    /// closure (the C `compare`/`arg` pair, with `arg` captured).
    ///
    /// The node array is pre-reserved to `capacity` (C `palloc`s `sizeof(node) *
    /// capacity`). Fails with `ERRCODE_OUT_OF_MEMORY` on allocation failure,
    /// mirroring `palloc`'s out-of-memory `ERROR`.
    pub fn allocate(capacity: i32, compare: C) -> PgResult<Self> {
        let mut nodes: Vec<T> = Vec::new();
        nodes.try_reserve_exact(capacity as usize).map_err(|_| oom())?;
        Ok(BinaryHeap {
            space: capacity,
            has_heap_property: true,
            compare,
            // bh_size = 0
            nodes,
        })
    }

    /// `binaryheap_reset`
    ///
    /// Resets the heap to an empty state, losing its data content but not the
    /// parameters passed at allocation. The spine capacity is retained, exactly
    /// like C keeping its `palloc`'d slots while setting `bh_size = 0`.
    pub fn reset(&mut self) {
        self.nodes.clear();
        self.has_heap_property = true;
    }

    /// `binaryheap_free`
    ///
    /// Releases memory used by the given binaryheap (`pfree`). Consumes the heap
    /// and drops its node array.
    pub fn free(self) {
        drop(self);
    }

    /// `binaryheap_empty`
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// `binaryheap_size`
    #[inline]
    pub fn size(&self) -> i32 {
        self.nodes.len() as i32
    }

    /// `binaryheap_get_node`
    #[inline]
    pub fn get_node(&self, n: i32) -> &T {
        &self.nodes[n as usize]
    }

    /// `binaryheap_add_unordered`
    ///
    /// Adds the given datum to the end of the heap's list of nodes in O(1)
    /// without preserving the heap property. To obtain a valid heap, one must
    /// call [`build`](Self::build) afterwards.
    ///
    /// Returns `Err` ("out of binary heap slots") if the heap is already at
    /// capacity, mirroring C's `elog(ERROR)` / `pg_fatal`.
    pub fn add_unordered(&mut self, d: T) -> PgResult<()> {
        if self.size() >= self.space {
            return Err(out_of_slots());
        }
        self.has_heap_property = false;
        // bh_nodes[bh_size] = d; bh_size++  (capacity pre-reserved by allocate)
        self.nodes.push(d);
        Ok(())
    }

    /// `binaryheap_build`
    ///
    /// Assembles a valid heap in O(n) from the nodes added by
    /// [`add_unordered`](Self::add_unordered). Not needed otherwise.
    pub fn build(&mut self) {
        let mut i = parent_offset(self.size() - 1);
        while i >= 0 {
            self.sift_down(i);
            i -= 1;
        }
        self.has_heap_property = true;
    }

    /// `binaryheap_add`
    ///
    /// Adds the given datum to the heap in O(log n) time, while preserving the
    /// heap property.
    ///
    /// Returns `Err` ("out of binary heap slots") if the heap is already at
    /// capacity.
    pub fn add(&mut self, d: T) -> PgResult<()> {
        if self.size() >= self.space {
            return Err(out_of_slots());
        }
        self.nodes.push(d);
        let last = self.size() - 1;
        self.sift_up(last);
        Ok(())
    }

    /// `binaryheap_first`
    ///
    /// Returns the first (root, topmost) node in the heap without modifying the
    /// heap. The caller must ensure this is not used on an empty heap. O(1).
    pub fn first(&self) -> &T {
        debug_assert!(!self.is_empty() && self.has_heap_property);
        &self.nodes[0]
    }

    /// `binaryheap_remove_first`
    ///
    /// Removes the first (root, topmost) node in the heap and returns it after
    /// rebalancing the heap. The caller must ensure this is not used on an empty
    /// heap. O(log n) worst case.
    pub fn remove_first(&mut self) -> T {
        debug_assert!(!self.is_empty() && self.has_heap_property);

        // easy if heap contains one element: extract the root node, bh_size--
        if self.size() == 1 {
            return self.nodes.pop().expect("non-empty heap");
        }

        // Remove the last node, placing it in the vacated root entry, and sift
        // the new root node down to its correct position.
        //   result = bh_nodes[0];
        //   bh_nodes[0] = bh_nodes[--bh_size];
        //   sift_down(heap, 0);
        let last = self.nodes.pop().expect("non-empty heap");
        let result = core::mem::replace(&mut self.nodes[0], last);
        self.sift_down(0);
        result
    }

    /// `binaryheap_remove_node`
    ///
    /// Removes the nth (zero based) node from the heap. The caller must ensure
    /// that there are at least `(n + 1)` nodes in the heap. O(log n) worst case.
    pub fn remove_node(&mut self, n: i32) {
        debug_assert!(!self.is_empty() && self.has_heap_property);
        debug_assert!(n >= 0 && n < self.size());

        // compare last node to the one that is being removed
        //   cmp = bh_compare(bh_nodes[--bh_size], bh_nodes[n], bh_arg);
        // C decrements bh_size first, then reads bh_nodes[bh_size] (the last
        // node) and bh_nodes[n]. When n == size-1 these alias the same slot, so
        // cmp == 0 and the node simply drops — compare by index *before* the
        // pop so that aliasing case is preserved (popping first would put
        // bh_nodes[n] out of bounds and panic, diverging from C).
        let last_idx = (self.size() - 1) as usize;
        let cmp = (self.compare)(&self.nodes[last_idx], &self.nodes[n as usize]);

        // remove the last node, placing it in the vacated entry
        //   bh_nodes[n] = bh_nodes[bh_size];
        // When n == size-1 the removed node *is* the last node: C writes
        // bh_nodes[n] = bh_nodes[bh_size] into the just-vacated slot (a no-op),
        // and cmp == 0 means no sift. Here the pop already removed it, so skip
        // the (out-of-bounds) self-assignment.
        let last = self.nodes.pop().expect("non-empty heap");
        if (n as usize) != last_idx {
            self.nodes[n as usize] = last;
        }

        // sift as needed to preserve the heap property
        if cmp > 0 {
            self.sift_up(n);
        } else if cmp < 0 {
            self.sift_down(n);
        }
    }

    /// `binaryheap_replace_first`
    ///
    /// Replace the topmost element of a non-empty heap, preserving the heap
    /// property. O(1) in the best case, or O(log n) if it must fall back to
    /// sifting the new node down.
    pub fn replace_first(&mut self, d: T) {
        debug_assert!(!self.is_empty() && self.has_heap_property);

        self.nodes[0] = d;

        if self.size() > 1 {
            self.sift_down(0);
        }
    }

    /// Sift a node up to the highest position it can hold according to the
    /// comparator. (static `sift_up`)
    ///
    /// C keeps `node_val` in a register and shuffles a notional "hole" to avoid
    /// re-copying it; owned `T` cannot be byte-copied, so we realise the exact
    /// same shuffle with move-only swaps. This is equivalent: the node being
    /// sifted always sits in the `node_off` slot, and each step either swaps it
    /// one level up or stops — the final array is identical to the hole version.
    fn sift_up(&mut self, mut node_off: i32) {
        let compare = &mut self.compare;
        let nodes = self.nodes.as_mut_slice();
        while node_off != 0 {
            // If this node is smaller than its parent, the heap condition is
            // satisfied, and we're done.
            let parent_off = parent_offset(node_off);
            let cmp = compare(&nodes[node_off as usize], &nodes[parent_off as usize]);
            if cmp <= 0 {
                break;
            }

            // Otherwise, swap with the parent, and go on to check the node's new
            // parent.
            nodes.swap(node_off as usize, parent_off as usize);
            node_off = parent_off;
        }
    }

    /// Sift a node down from its current position to satisfy the heap property.
    /// (static `sift_down`)
    ///
    /// As in [`sift_up`](Self::sift_up), the C "hole" copy-avoidance is realised
    /// with move-only swaps (identical final array).
    fn sift_down(&mut self, mut node_off: i32) {
        let size = self.size();
        let compare = &mut self.compare;
        let nodes = self.nodes.as_mut_slice();
        loop {
            let left_off = left_offset(node_off);
            let right_off = right_offset(node_off);
            let mut swap_off = left_off;

            // Is the right child larger than the left child?
            if right_off < size
                && compare(&nodes[left_off as usize], &nodes[right_off as usize]) < 0
            {
                swap_off = right_off;
            }

            // If no children or parent is >= the larger child, heap condition is
            // satisfied, and we're done.
            if left_off >= size
                || compare(&nodes[node_off as usize], &nodes[swap_off as usize]) >= 0
            {
                break;
            }

            // Otherwise, swap with the child that violates the heap property;
            // then go on to check its children.
            nodes.swap(node_off as usize, swap_off as usize);
            node_off = swap_off;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec::Vec;

    /// Max-heap comparator over i32 (a<b => <0, a==b => 0, a>b => >0).
    fn max_cmp(a: &i32, b: &i32) -> i32 {
        if a < b {
            -1
        } else if a > b {
            1
        } else {
            0
        }
    }

    /// Pop everything off and confirm it comes out in descending order (max-heap).
    fn drain_sorted(h: &mut BinaryHeap<i32, fn(&i32, &i32) -> i32>) -> Vec<i32> {
        let mut out = Vec::new();
        while !h.is_empty() {
            out.push(h.remove_first());
        }
        out
    }

    #[test]
    fn add_then_remove_first_is_descending() {
        let mut h: BinaryHeap<i32, fn(&i32, &i32) -> i32> =
            BinaryHeap::allocate(8, max_cmp as fn(&i32, &i32) -> i32).unwrap();
        for &v in &[3, 1, 4, 1, 5, 9, 2, 6] {
            h.add(v).unwrap();
        }
        assert_eq!(h.size(), 8);
        assert_eq!(*h.first(), 9);
        assert_eq!(drain_sorted(&mut h), [9, 6, 5, 4, 3, 2, 1, 1]);
        assert!(h.is_empty());
    }

    #[test]
    fn add_unordered_then_build() {
        let mut h: BinaryHeap<i32, fn(&i32, &i32) -> i32> =
            BinaryHeap::allocate(8, max_cmp as fn(&i32, &i32) -> i32).unwrap();
        for &v in &[3, 1, 4, 1, 5, 9, 2, 6] {
            h.add_unordered(v).unwrap();
        }
        h.build();
        assert_eq!(*h.first(), 9);
        assert_eq!(drain_sorted(&mut h), [9, 6, 5, 4, 3, 2, 1, 1]);
    }

    #[test]
    fn replace_first_reheapifies() {
        let mut h: BinaryHeap<i32, fn(&i32, &i32) -> i32> =
            BinaryHeap::allocate(4, max_cmp as fn(&i32, &i32) -> i32).unwrap();
        h.add(10).unwrap();
        h.add(8).unwrap();
        h.add(6).unwrap();
        assert_eq!(*h.first(), 10);
        // Replace root 10 with 1 -> root must sift down to 8.
        h.replace_first(1);
        assert_eq!(*h.first(), 8);
        assert_eq!(drain_sorted(&mut h), [8, 6, 1]);
    }

    #[test]
    fn remove_node_middle() {
        let mut h: BinaryHeap<i32, fn(&i32, &i32) -> i32> =
            BinaryHeap::allocate(8, max_cmp as fn(&i32, &i32) -> i32).unwrap();
        for &v in &[7, 5, 6, 3, 4, 2, 1] {
            h.add(v).unwrap();
        }
        // Find node holding value 6 and remove it; rest stays a valid heap.
        let mut idx = 0;
        for i in 0..h.size() {
            if *h.get_node(i) == 6 {
                idx = i;
                break;
            }
        }
        h.remove_node(idx);
        let rest = drain_sorted(&mut h);
        assert_eq!(rest, [7, 5, 4, 3, 2, 1]);
    }

    #[test]
    fn reset_and_reuse() {
        let mut h: BinaryHeap<i32, fn(&i32, &i32) -> i32> =
            BinaryHeap::allocate(4, max_cmp as fn(&i32, &i32) -> i32).unwrap();
        h.add(1).unwrap();
        h.add(2).unwrap();
        h.reset();
        assert!(h.is_empty());
        assert_eq!(h.size(), 0);
        h.add(42).unwrap();
        assert_eq!(*h.first(), 42);
        h.free();
    }

    #[test]
    fn overflow_returns_err() {
        let mut h: BinaryHeap<i32, fn(&i32, &i32) -> i32> =
            BinaryHeap::allocate(2, max_cmp as fn(&i32, &i32) -> i32).unwrap();
        h.add(1).unwrap();
        h.add(2).unwrap();
        assert!(h.add(3).is_err()); // capacity exceeded
        assert!(h.add_unordered(3).is_err());
    }

    #[test]
    fn remove_node_last_index() {
        // Removing the last node (n == size-1): C reads bh_nodes[--bh_size]
        // and bh_nodes[n] which alias, so cmp == 0 and the node just drops.
        // The port must not panic on this valid input.
        let mut h: BinaryHeap<i32, fn(&i32, &i32) -> i32> =
            BinaryHeap::allocate(8, max_cmp as fn(&i32, &i32) -> i32).unwrap();
        for &v in &[7, 5, 6, 3, 4] {
            h.add(v).unwrap();
        }
        let last = h.size() - 1;
        let removed_val = *h.get_node(last);
        h.remove_node(last);
        assert_eq!(h.size(), 4);
        // The rest is the original multiset minus the removed last node, and
        // remains a valid max-heap (drains in descending order).
        let rest = drain_sorted(&mut h);
        let mut expected: Vec<i32> = [7, 5, 6, 3, 4].into_iter().collect();
        let pos = expected.iter().position(|&x| x == removed_val).unwrap();
        expected.remove(pos);
        expected.sort_unstable_by(|a: &i32, b: &i32| b.cmp(a));
        assert_eq!(rest, expected);
    }

    #[test]
    fn single_element_remove_first() {
        let mut h: BinaryHeap<i32, fn(&i32, &i32) -> i32> =
            BinaryHeap::allocate(1, max_cmp as fn(&i32, &i32) -> i32).unwrap();
        h.add(99).unwrap();
        assert_eq!(h.remove_first(), 99);
        assert!(h.is_empty());
    }
}
