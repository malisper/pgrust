#![no_std]
#![forbid(unsafe_code)]
// `clippy::result_large_err`: the allocating/erroring operations
// (`IntegerSet::create`, `intset_new_internal_node`, `intset_new_leaf_node`,
// `intset_flush_buffered_values`, `intset_update_upper`, `add_member`) and the
// free-function wrappers return the shared `PgResult` (== `Result<_, PgError>`)
// to model the C `elog(ERROR, ...)` non-local exit faithfully.  `PgError`'s size
// is fixed by the `types-error` crate and the un-boxed `PgResult` is the
// project-wide error contract every sibling crate matches; boxing it locally
// would diverge from those signatures.
#![allow(clippy::result_large_err)]
//! Data structure to hold a large set of 64-bit integers efficiently.
//!
//! `IntegerSet` provides an in-memory data structure to hold a set of
//! arbitrary 64-bit integers.  Internally, the values are stored in a
//! B-tree, with a special packed representation at the leaf level using
//! the Simple-8b algorithm, which can pack clusters of nearby values
//! very tightly.
//!
//! This is an idiomatic port of `src/backend/lib/integerset.c` from
//! PostgreSQL 18.3.  The Simple-8b encoder/decoder and the B-tree
//! insertion/search/iteration algorithms reproduce the C source exactly,
//! including branch order, error message text, and SQLSTATE.
//!
//! # Ownership model (the idiomatic difference from C)
//!
//! The C struct links its B-tree together with raw `intset_node *`
//! pointers (`root`, `downlinks[]`, `rightmost_nodes[]`, `next`,
//! `leftmost_leaf`, `iter_node`).  This port replaces every raw pointer
//! with an **arena index**: all internal nodes live in one `Vec`, all
//! leaf nodes in another, both owned by the [`IntegerSet`].  A link is a
//! [`NodeRef`] (a tagged arena index) or `Option<LeafIdx>`; "NULL" is
//! `None`.  This keeps the data structure `#![forbid(unsafe_code)]` with no
//! aliasing hazards, while the algorithm is unchanged: nodes are
//! append-only and never moved out of the arena (the C code likewise never
//! frees a node), so indices stay stable for the life of the set, exactly
//! mirroring the stable C pointers.
//!
//! # Memory accounting (the PILOT model)
//!
//! `intset_create()` builds the set inside a caller-provided
//! [`MemoryContext`] (the analog of C's "current memory context").
//! `mem_used` tracks the bytes the C code would have charged via
//! `GetMemoryChunkSpace` (here, the size of each node struct plus the
//! control object), and is what callers read with [`memory_usage`].  Node
//! storage grows fallibly: a `try_reserve` failure is routed through the
//! owning [`MemoryContext`] as a real out-of-memory [`PgError`], modelling
//! `palloc`'s `ereport(ERROR, ...)` on OOM.  Dropping the set frees the
//! whole tree, which is exactly how C recommends freeing an integer set
//! (destroy the dedicated memory context).
//!
//! ## Interface
//!
//! * [`IntegerSet::create`] / [`intset_create`] - Create a new, empty set
//! * [`IntegerSet::add_member`] / [`intset_add_member`] - Add an integer
//! * [`IntegerSet::is_member`] / [`intset_is_member`] - Test membership
//! * [`IntegerSet::begin_iterate`] / [`intset_begin_iterate`] - Begin a scan
//! * [`IntegerSet::iterate_next`] / [`intset_iterate_next`] - Next member
//!
//! ## Limitations
//!
//! * Values must be added in order.
//! * Values cannot be added while iteration is in progress.
//! * No support for removing values.

extern crate alloc;

use alloc::collections::TryReserveError;
use alloc::vec::Vec;

use mcx::MemoryContext;
use types_error::{PgError, PgResult};

/*
 * Maximum number of integers that can be encoded in a single Simple-8b
 * codeword.
 */
const SIMPLE8B_MAX_VALUES_PER_CODEWORD: usize = 240;

/*
 * Parameters for shape of the in-memory B-tree.
 *
 * These set the size of each internal and leaf node.  With the default 64,
 * each node is about 1 kb.  If you change these, you must recalculate
 * MAX_TREE_LEVELS, too!
 */
const MAX_INTERNAL_ITEMS: usize = 64;
const MAX_LEAF_ITEMS: usize = 64;

/*
 * Maximum height of the tree.
 *
 * In practice we'll need far fewer levels, because you will run out of memory
 * long before reaching this number, but let's be conservative.
 */
const MAX_TREE_LEVELS: usize = 11;

const MAX_VALUES_PER_LEAF_ITEM: usize = 1 + SIMPLE8B_MAX_VALUES_PER_CODEWORD;

/*
 * We buffer insertions in a simple array, before packing and inserting them
 * into the B-tree.  MAX_BUFFERED_VALUES sets the size of the buffer.  The
 * encoder assumes that it is large enough that we can always fill a leaf item
 * with buffered new items.  In other words, MAX_BUFFERED_VALUES must be larger
 * than MAX_VALUES_PER_LEAF_ITEM.  For efficiency, make it much larger.
 */
const MAX_BUFFERED_VALUES: usize = MAX_VALUES_PER_LEAF_ITEM * 2;

/*
 * Simple-8b mode table.
 *
 * In Simple-8b, each codeword consists of a 4-bit selector, which indicates
 * how many integers are encoded in the codeword, and the encoded integers are
 * packed into the remaining 60 bits.  The selector allows for 16 different
 * ways of using the remaining 60 bits, called "modes".  The number of integers
 * packed into a single codeword in each mode is listed in the table below.
 *
 * Modes 0 and 1 are a bit special; they encode a run of 240 or 120 zeroes
 * (which means 240 or 120 consecutive integers, since we're encoding the
 * deltas between integers), without using the rest of the codeword bits for
 * anything.
 */
struct Simple8bMode {
    bits_per_int: u8,
    num_ints: i32,
}

static SIMPLE8B_MODES: [Simple8bMode; 17] = [
    Simple8bMode { bits_per_int: 0, num_ints: 240 }, /* mode  0: 240 zeroes */
    Simple8bMode { bits_per_int: 0, num_ints: 120 }, /* mode  1: 120 zeroes */
    Simple8bMode { bits_per_int: 1, num_ints: 60 },  /* mode  2: sixty 1-bit integers */
    Simple8bMode { bits_per_int: 2, num_ints: 30 },  /* mode  3: thirty 2-bit integers */
    Simple8bMode { bits_per_int: 3, num_ints: 20 },  /* mode  4: twenty 3-bit integers */
    Simple8bMode { bits_per_int: 4, num_ints: 15 },  /* mode  5: fifteen 4-bit integers */
    Simple8bMode { bits_per_int: 5, num_ints: 12 },  /* mode  6: twelve 5-bit integers */
    Simple8bMode { bits_per_int: 6, num_ints: 10 },  /* mode  7: ten 6-bit integers */
    Simple8bMode { bits_per_int: 7, num_ints: 8 },   /* mode  8: eight 7-bit integers */
    Simple8bMode { bits_per_int: 8, num_ints: 7 },   /* mode  9: seven 8-bit integers */
    Simple8bMode { bits_per_int: 10, num_ints: 6 },  /* mode 10: six 10-bit integers */
    Simple8bMode { bits_per_int: 12, num_ints: 5 },  /* mode 11: five 12-bit integers */
    Simple8bMode { bits_per_int: 15, num_ints: 4 },  /* mode 12: four 15-bit integers */
    Simple8bMode { bits_per_int: 20, num_ints: 3 },  /* mode 13: three 20-bit integers */
    Simple8bMode { bits_per_int: 30, num_ints: 2 },  /* mode 14: two 30-bit integers */
    Simple8bMode { bits_per_int: 60, num_ints: 1 },  /* mode 15: one 60-bit integer */
    Simple8bMode { bits_per_int: 0, num_ints: 0 },   /* sentinel value */
];

/*
 * EMPTY_CODEWORD is a special value, used to indicate "no values".
 * It is used if the next value is too large to be encoded with Simple-8b.
 *
 * This value looks like a mode-0 codeword, but we can distinguish it because a
 * regular mode-0 codeword would have zeroes in the unused bits.
 */
const EMPTY_CODEWORD: u64 = 0x0FFF_FFFF_FFFF_FFFF;

/*
 * Node structures, for the in-memory B-tree.
 *
 * In C, an `intset_node` is the common header (`level`, `num_items`) shared by
 * `intset_internal_node` and `intset_leaf_node`, and downlinks/root point at
 * the common header so the level field can be inspected before downcasting.
 *
 * Here each node carries its own `level`/`num_items` and we never need to
 * downcast: a [`NodeRef`] already records whether it points at the internal or
 * the leaf arena.
 */

/// One leaf item: the first integer in plain form, plus a Simple-8b codeword
/// packing the deltas of any following integers.
#[derive(Clone, Copy)]
struct LeafItem {
    first: u64,    /* first integer in this item */
    codeword: u64, /* simple8b encoded differences from 'first' */
}

impl LeafItem {
    const ZERO: LeafItem = LeafItem { first: 0, codeword: 0 };
}

/// Internal node: `level >= 1`.  `values[i]` is the low key of the subtree
/// reachable via `downlinks[i]`.
struct InternalNode {
    level: u16, /* >= 1 on internal nodes */
    num_items: u16,
    values: [u64; MAX_INTERNAL_ITEMS],
    downlinks: [Option<NodeRef>; MAX_INTERNAL_ITEMS],
}

/// Leaf node: `level == 0`.  Holds packed items plus a link to the right
/// sibling leaf (for iteration).
struct LeafNode {
    num_items: u16,
    next: Option<LeafIdx>, /* right sibling, if any */
    items: [LeafItem; MAX_LEAF_ITEMS],
}

/// An index into the internal-node arena.
type InternalIdx = usize;
/// An index into the leaf-node arena.
type LeafIdx = usize;

/// A link to a B-tree node, tagged with which arena it lives in.  The idiomatic
/// replacement for C's `intset_node *` downlink (which is dereferenced after
/// checking `level == 0` to decide leaf vs internal).
#[derive(Clone, Copy, PartialEq, Eq)]
enum NodeRef {
    Internal(InternalIdx),
    Leaf(LeafIdx),
}

/*
 * IntegerSet is the top-level object representing the set.
 *
 * The integers are stored in an in-memory B-tree structure, plus an array for
 * newly-added integers.  IntegerSet also tracks information about memory usage,
 * as well as the current position when iterating the set.
 */
pub struct IntegerSet {
    /*
     * 'context' is the memory context holding this integer set and all its tree
     * nodes (the two arenas below are charged to it).
     *
     * 'mem_used' tracks the amount of memory used.  We don't do anything with it
     * in integerset itself, but callers can ask for it with memory_usage().
     */
    context: MemoryContext,
    mem_used: u64,

    /*
     * The B-tree nodes.  These two arenas hold every node ever allocated; nodes
     * are never moved or freed individually (matching C, which never frees a
     * node), so indices stored in links remain valid for the set's lifetime.
     */
    internal_nodes: Vec<InternalNode>,
    leaf_nodes: Vec<LeafNode>,

    num_entries: u64,   /* total # of values in the set */
    highest_value: u64, /* highest value stored in this set */

    /*
     * B-tree to hold the packed values.
     *
     * 'rightmost_nodes' hold links to the rightmost node on each level.
     * rightmost_nodes[0] is rightmost leaf, rightmost_nodes[1] is its parent,
     * and so forth, all the way up to the root.  These are needed when adding
     * new values.  (Currently, we require that new values are added at the end.)
     */
    num_levels: i32, /* height of the tree */
    root: Option<NodeRef>,
    rightmost_nodes: [Option<NodeRef>; MAX_TREE_LEVELS],
    leftmost_leaf: Option<LeafIdx>,

    /*
     * Holding area for new items that haven't been inserted to the tree yet.
     */
    buffered_values: [u64; MAX_BUFFERED_VALUES],
    num_buffered_values: i32,

    /*
     * Iterator support.
     *
     * 'iter_values' is an array of integers ready to be returned to the caller;
     * 'iter_num_values' is the length of that array, and 'iter_valueno' is the
     * next index.  'iter_node' and 'iter_itemno' point to the leaf node, and
     * item within the leaf node, to get the next batch of values from.
     *
     * In C, 'iter_values' normally points at 'iter_values_buf' (decoded leaf
     * items) and, once the B-tree is exhausted, is repointed at
     * 'buffered_values'.  Here we model that "which array" choice with the
     * [`IterSource`] enum rather than a pointer.
     */
    iter_active: bool, /* is iteration in progress? */

    iter_source: IterSource,
    iter_num_values: i32, /* number of elements in the active iter array */
    iter_valueno: i32,    /* next index into the active iter array */

    iter_node: Option<LeafIdx>, /* current leaf node */
    iter_itemno: i32,           /* next item in 'iter_node' to decode */

    iter_values_buf: [u64; MAX_VALUES_PER_LEAF_ITEM],
}

/// Which backing array the iterator is currently reading from.  Replaces C's
/// `iter_values` pointer that flips between `iter_values_buf` and
/// `buffered_values`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum IterSource {
    /// Reading decoded values out of `iter_values_buf`.
    DecodeBuf,
    /// Reading the trailing unbuffered values out of `buffered_values`.
    Buffered,
}

impl IntegerSet {
    /*
     * Create a new, initially empty, integer set.
     *
     * The integer set is created in the given memory context (the idiomatic
     * analog of C's "current memory context").  All subsequent allocations are
     * charged to the same context, regardless of which context is "current" when
     * new integers are added.
     *
     * In C `intset` is itself palloc'd and `mem_used = GetMemoryChunkSpace(intset)`
     * accounts for it; here the control object lives by value, so we seed
     * `mem_used` with the size of the struct to match that accounting.
     */
    pub fn create(context: MemoryContext) -> PgResult<Self> {
        let mut intset = IntegerSet {
            context,
            mem_used: 0,

            internal_nodes: Vec::new(),
            leaf_nodes: Vec::new(),

            num_entries: 0,
            highest_value: 0,

            num_levels: 0,
            root: None,
            rightmost_nodes: [None; MAX_TREE_LEVELS],
            leftmost_leaf: None,

            buffered_values: [0; MAX_BUFFERED_VALUES],
            num_buffered_values: 0,

            iter_active: false,
            iter_source: IterSource::DecodeBuf,
            iter_num_values: 0,
            iter_valueno: 0,
            iter_node: None,
            iter_itemno: 0,
            iter_values_buf: [0; MAX_VALUES_PER_LEAF_ITEM],
        };

        /* intset->mem_used = GetMemoryChunkSpace(intset); */
        intset.mem_used = core::mem::size_of::<IntegerSet>() as u64;

        Ok(intset)
    }

    /// The memory context backing this set.
    pub fn context(&self) -> &MemoryContext {
        &self.context
    }

    /*
     * Allocate a new internal node, returning its arena index.
     */
    fn intset_new_internal_node(&mut self) -> PgResult<InternalIdx> {
        let node = InternalNode {
            level: 0, /* caller must set */
            num_items: 0,
            values: [0; MAX_INTERNAL_ITEMS],
            downlinks: [None; MAX_INTERNAL_ITEMS],
        };

        self.internal_nodes
            .try_reserve(1)
            .map_err(|e| oom(&self.context, e))?;
        self.internal_nodes.push(node);
        /* intset->mem_used += GetMemoryChunkSpace(n); */
        self.mem_used += core::mem::size_of::<InternalNode>() as u64;
        Ok(self.internal_nodes.len() - 1)
    }

    fn intset_new_leaf_node(&mut self) -> PgResult<LeafIdx> {
        let node = LeafNode {
            num_items: 0,
            next: None,
            items: [LeafItem::ZERO; MAX_LEAF_ITEMS],
        };

        self.leaf_nodes
            .try_reserve(1)
            .map_err(|e| oom(&self.context, e))?;
        self.leaf_nodes.push(node);
        /* intset->mem_used += GetMemoryChunkSpace(n); */
        self.mem_used += core::mem::size_of::<LeafNode>() as u64;
        Ok(self.leaf_nodes.len() - 1)
    }

    /*
     * Take a batch of buffered values, and pack them into the B-tree.
     */
    fn intset_flush_buffered_values(&mut self) -> PgResult<()> {
        let num_values = self.num_buffered_values;
        let mut num_packed: i32 = 0;

        let mut leaf: LeafIdx = match self.rightmost_nodes[0] {
            Some(NodeRef::Leaf(idx)) => idx,
            Some(NodeRef::Internal(_)) => {
                /* rightmost_nodes[0] is always a leaf when set */
                unreachable!("rightmost_nodes[0] must be a leaf")
            }
            None => {
                /*
                 * This is the very first item in the set.
                 *
                 * Allocate root node.  It's also a leaf.
                 */
                let leaf = self.intset_new_leaf_node()?;

                self.root = Some(NodeRef::Leaf(leaf));
                self.leftmost_leaf = Some(leaf);
                self.rightmost_nodes[0] = Some(NodeRef::Leaf(leaf));
                self.num_levels = 1;
                leaf
            }
        };

        /*
         * If there are less than MAX_VALUES_PER_LEAF_ITEM values in the buffer,
         * stop.  In most cases, we cannot encode that many values in a single
         * value, but this way, the encoder doesn't have to worry about running
         * out of input.
         */
        while (num_values - num_packed) as usize >= MAX_VALUES_PER_LEAF_ITEM {
            let mut item = LeafItem::ZERO;

            /*
             * Construct the next leaf item, packing as many buffered values as
             * possible.
             */
            item.first = self.buffered_values[num_packed as usize];
            let (codeword, num_encoded) = simple8b_encode(
                &self.buffered_values[(num_packed + 1) as usize..],
                item.first,
            );
            item.codeword = codeword;

            /*
             * Add the item to the node, allocating a new node if the old one is
             * full.
             */
            if self.leaf_nodes[leaf].num_items as usize >= MAX_LEAF_ITEMS {
                /* Allocate new leaf and link it to the tree */
                let old_leaf = leaf;

                leaf = self.intset_new_leaf_node()?;
                self.leaf_nodes[old_leaf].next = Some(leaf);
                self.rightmost_nodes[0] = Some(NodeRef::Leaf(leaf));
                self.intset_update_upper(1, NodeRef::Leaf(leaf), item.first)?;
            }
            {
                let node = &mut self.leaf_nodes[leaf];
                let idx = node.num_items as usize;
                node.items[idx] = item;
                node.num_items += 1;
            }

            num_packed += 1 + num_encoded;
        }

        /*
         * Move any remaining buffered values to the beginning of the array.
         */
        if num_packed < self.num_buffered_values {
            self.buffered_values.copy_within(
                (num_packed as usize)..(self.num_buffered_values as usize),
                0,
            );
        }
        self.num_buffered_values -= num_packed;

        Ok(())
    }

    /*
     * Insert a downlink into parent node, after creating a new node.
     *
     * Recurses if the parent node is full, too.
     */
    fn intset_update_upper(
        &mut self,
        level: i32,
        child: NodeRef,
        child_key: u64,
    ) -> PgResult<()> {
        debug_assert!(level > 0);

        /*
         * Create a new root node, if necessary.
         */
        if level >= self.num_levels {
            let oldroot = self.root.ok_or_else(|| {
                PgError::error("intset_update_upper: root missing when growing the tree")
            })?;

            /* MAX_TREE_LEVELS should be more than enough, this shouldn't happen */
            if self.num_levels as usize == MAX_TREE_LEVELS {
                return Err(PgError::error(
                    "could not expand integer set, maximum number of levels reached",
                ));
            }
            self.num_levels += 1;

            /*
             * Get the first value on the old root page, to be used as the
             * downlink.
             */
            let downlink_key: u64 = match oldroot {
                NodeRef::Leaf(idx) => self.leaf_nodes[idx].items[0].first,
                NodeRef::Internal(idx) => self.internal_nodes[idx].values[0],
            };

            let parent = self.intset_new_internal_node()?;
            {
                let n = &mut self.internal_nodes[parent];
                n.level = level as u16;
                n.values[0] = downlink_key;
                n.downlinks[0] = Some(oldroot);
                n.num_items = 1;
            }

            self.root = Some(NodeRef::Internal(parent));
            self.rightmost_nodes[level as usize] = Some(NodeRef::Internal(parent));
        }

        /*
         * Place the downlink on the parent page.
         */
        let parent = match self.rightmost_nodes[level as usize] {
            Some(NodeRef::Internal(idx)) => idx,
            _ => unreachable!("rightmost_nodes[level>0] must be an internal node"),
        };

        if (self.internal_nodes[parent].num_items as usize) < MAX_INTERNAL_ITEMS {
            let n = &mut self.internal_nodes[parent];
            let idx = n.num_items as usize;
            n.values[idx] = child_key;
            n.downlinks[idx] = Some(child);
            n.num_items += 1;
        } else {
            /*
             * Doesn't fit.  Allocate new parent, with the downlink as the first
             * item on it, and recursively insert the downlink to the new parent
             * to the grandparent.
             */
            let new_parent = self.intset_new_internal_node()?;
            {
                let n = &mut self.internal_nodes[new_parent];
                n.level = level as u16;
                n.values[0] = child_key;
                n.downlinks[0] = Some(child);
                n.num_items = 1;
            }

            self.rightmost_nodes[level as usize] = Some(NodeRef::Internal(new_parent));

            self.intset_update_upper(level + 1, NodeRef::Internal(new_parent), child_key)?;
        }

        Ok(())
    }

    /*
     * Return the number of entries in the integer set.
     */
    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }

    /*
     * Return the amount of memory used by the integer set.
     */
    pub fn memory_usage(&self) -> u64 {
        self.mem_used
    }

    /*
     * Add a value to the set.
     *
     * Values must be added in order.
     */
    pub fn add_member(&mut self, x: u64) -> PgResult<()> {
        if self.iter_active {
            return Err(PgError::error(
                "cannot add new values to integer set while iteration is in progress",
            ));
        }

        if x <= self.highest_value && self.num_entries > 0 {
            return Err(PgError::error(
                "cannot add value to integer set out of order",
            ));
        }

        if self.num_buffered_values as usize >= MAX_BUFFERED_VALUES {
            /* Time to flush our buffer */
            self.intset_flush_buffered_values()?;
            debug_assert!((self.num_buffered_values as usize) < MAX_BUFFERED_VALUES);
        }

        /* Add it to the buffer of newly-added values */
        self.buffered_values[self.num_buffered_values as usize] = x;
        self.num_buffered_values += 1;
        self.num_entries += 1;
        self.highest_value = x;

        Ok(())
    }

    /*
     * Does the set contain the given value?
     */
    pub fn is_member(&self, x: u64) -> bool {
        /*
         * The value might be in the buffer of newly-added values.
         */
        if self.num_buffered_values > 0 && x >= self.buffered_values[0] {
            let itemno = intset_binsrch_uint64(
                x,
                &self.buffered_values,
                self.num_buffered_values,
                false,
            );
            if itemno >= self.num_buffered_values {
                return false;
            } else {
                return self.buffered_values[itemno as usize] == x;
            }
        }

        /*
         * Start from the root, and walk down the B-tree to find the right leaf
         * node.
         */
        let mut node = match self.root {
            Some(n) => n,
            None => return false,
        };
        let mut level = self.num_levels - 1;
        while level > 0 {
            let n = match node {
                NodeRef::Internal(idx) => &self.internal_nodes[idx],
                NodeRef::Leaf(_) => unreachable!("expected internal node above level 0"),
            };

            debug_assert_eq!(n.level as i32, level);

            let itemno =
                intset_binsrch_uint64(x, &n.values, n.num_items as i32, true);
            if itemno == 0 {
                return false;
            }
            node = n.downlinks[(itemno - 1) as usize]
                .expect("downlink must be set for an in-range item");

            level -= 1;
        }
        let leaf = match node {
            NodeRef::Leaf(idx) => &self.leaf_nodes[idx],
            NodeRef::Internal(_) => unreachable!("expected leaf node at level 0"),
        };

        /*
         * Binary search to find the right item on the leaf page.
         */
        let itemno = intset_binsrch_leaf(x, &leaf.items, leaf.num_items as i32, true);
        if itemno == 0 {
            return false;
        }
        let item = &leaf.items[(itemno - 1) as usize];

        /* Is this a match to the first value on the item? */
        if item.first == x {
            return true;
        }
        debug_assert!(x > item.first);

        /* Is it in the packed codeword? */
        if simple8b_contains(item.codeword, x, item.first) {
            return true;
        }

        false
    }

    /*
     * Begin in-order scan through all the values.
     *
     * While the iteration is in-progress, you cannot add new values to the set.
     */
    pub fn begin_iterate(&mut self) {
        /* Note that we allow an iteration to be abandoned midway */
        self.iter_active = true;
        self.iter_node = self.leftmost_leaf;
        self.iter_itemno = 0;
        self.iter_valueno = 0;
        self.iter_num_values = 0;
        self.iter_source = IterSource::DecodeBuf;
    }

    /*
     * Returns the next integer, when iterating.
     *
     * begin_iterate() must be called first.  iterate_next() returns the next
     * value in the set.  Returns Some(value) if there was another value, and
     * None otherwise.
     */
    pub fn iterate_next(&mut self) -> Option<u64> {
        debug_assert!(self.iter_active);
        loop {
            /* Return next iter_values[] entry if any */
            if self.iter_valueno < self.iter_num_values {
                let value = match self.iter_source {
                    IterSource::DecodeBuf => self.iter_values_buf[self.iter_valueno as usize],
                    IterSource::Buffered => self.buffered_values[self.iter_valueno as usize],
                };
                self.iter_valueno += 1;
                return Some(value);
            }

            /* Decode next item in current leaf node, if any */
            if let Some(leaf_idx) = self.iter_node {
                if self.iter_itemno < self.leaf_nodes[leaf_idx].num_items as i32 {
                    let item = self.leaf_nodes[leaf_idx].items[self.iter_itemno as usize];
                    self.iter_itemno += 1;

                    self.iter_values_buf[0] = item.first;
                    let num_decoded = simple8b_decode(
                        item.codeword,
                        &mut self.iter_values_buf[1..],
                        item.first,
                    );
                    self.iter_num_values = num_decoded + 1;
                    self.iter_valueno = 0;
                    self.iter_source = IterSource::DecodeBuf;
                    continue;
                }

                /* No more items on this leaf, step to next node */
                self.iter_node = self.leaf_nodes[leaf_idx].next;
                self.iter_itemno = 0;
                continue;
            }

            /*
             * We have reached the end of the B-tree.  But we might still have
             * some integers in the buffer of newly-added values.
             */
            if self.iter_source == IterSource::DecodeBuf {
                self.iter_source = IterSource::Buffered;
                self.iter_num_values = self.num_buffered_values;
                self.iter_valueno = 0;
                continue;
            }

            break;
        }

        /* No more results. */
        self.iter_active = false;
        None
    }
}

/*
 * Create a new, initially empty, integer set, in a freshly named context.
 *
 * Free-function form mirroring the C `intset_create()` entry point.  The set
 * owns a dedicated [`MemoryContext`]; dropping the returned set frees it (the C
 * recommendation to "destroy the dedicated memory context").
 */
pub fn intset_create() -> PgResult<IntegerSet> {
    IntegerSet::create(MemoryContext::new("integer set"))
}

/// Free-function form of [`IntegerSet::num_entries`].
pub fn intset_num_entries(intset: &IntegerSet) -> u64 {
    intset.num_entries()
}

/// Free-function form of [`IntegerSet::memory_usage`].
pub fn intset_memory_usage(intset: &IntegerSet) -> u64 {
    intset.memory_usage()
}

/// Free-function form of [`IntegerSet::add_member`].
pub fn intset_add_member(intset: &mut IntegerSet, x: u64) -> PgResult<()> {
    intset.add_member(x)
}

/// Free-function form of [`IntegerSet::is_member`].
pub fn intset_is_member(intset: &IntegerSet, x: u64) -> bool {
    intset.is_member(x)
}

/// Free-function form of [`IntegerSet::begin_iterate`].
pub fn intset_begin_iterate(intset: &mut IntegerSet) {
    intset.begin_iterate()
}

/// Free-function form of [`IntegerSet::iterate_next`].
///
/// Returns `true` and stores the value in `*next` if there was another value;
/// otherwise returns `false` (and leaves `*next` zeroed, like the C
/// out-parameter contract).
pub fn intset_iterate_next(intset: &mut IntegerSet, next: &mut u64) -> bool {
    match intset.iterate_next() {
        Some(v) => {
            *next = v;
            true
        }
        None => {
            *next = 0; /* prevent uninitialized-variable warnings */
            false
        }
    }
}

/// Translate a `try_reserve` failure into the project's out-of-memory error.
/// The C code relies on `palloc`'s `ereport(ERROR, ...)` for OOM; here the
/// fallible bulk allocation surfaces as a loud [`PgError`] routed through the
/// owning [`MemoryContext`] instead.
fn oom(context: &MemoryContext, _e: TryReserveError) -> PgError {
    context.oom(0)
}

/*
 * intset_binsrch_uint64() -- search a sorted array of uint64s
 *
 * Returns the first position with key equal or less than the given key.  The
 * returned position would be the "insert" location for the given key, that is,
 * the position where the new key should be inserted to.
 *
 * 'nextkey' affects the behavior on equal keys.  If true, and there is an equal
 * key in the array, this returns the position immediately after the equal key.
 * If false, this returns the position of the equal key itself.
 */
fn intset_binsrch_uint64(item: u64, arr: &[u64], arr_elems: i32, nextkey: bool) -> i32 {
    let mut low = 0;
    let mut high = arr_elems;

    while high > low {
        let mid = low + (high - low) / 2;

        if nextkey {
            if item >= arr[mid as usize] {
                low = mid + 1;
            } else {
                high = mid;
            }
        } else if item > arr[mid as usize] {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    low
}

/* same, but for an array of leaf items */
fn intset_binsrch_leaf(item: u64, arr: &[LeafItem], arr_elems: i32, nextkey: bool) -> i32 {
    let mut low = 0;
    let mut high = arr_elems;

    while high > low {
        let mid = low + (high - low) / 2;

        if nextkey {
            if item >= arr[mid as usize].first {
                low = mid + 1;
            } else {
                high = mid;
            }
        } else if item > arr[mid as usize].first {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    low
}

/*
 * Encode a number of integers into a Simple-8b codeword.
 *
 * (What we actually encode are deltas between successive integers.  "base" is
 * the value before ints[0].)
 *
 * The input array must contain at least SIMPLE8B_MAX_VALUES_PER_CODEWORD
 * elements, ensuring that we can produce a full codeword.
 *
 * Returns the encoded codeword, and the number of input integers that were
 * encoded.  That can be zero, if the first delta is too large to be encoded.
 */
fn simple8b_encode(ints: &[u64], base: u64) -> (u64, i32) {
    debug_assert!(ints.len() >= SIMPLE8B_MAX_VALUES_PER_CODEWORD);
    debug_assert!(ints[0] > base);

    /*
     * Select the "mode" to use for this codeword.
     *
     * In each iteration, check if the next value can be represented in the
     * current mode we're considering.  If it's too large, then step up the mode
     * to a wider one, and repeat.  If it fits, move on to the next integer.
     * Repeat until the codeword is full, given the current mode.
     *
     * Note that we don't have any way to represent unused slots in the codeword,
     * so we require each codeword to be "full".  It is always possible to produce
     * a full codeword unless the very first delta is too large to be encoded.
     * For example, if the first delta is small but the second is too large to be
     * encoded, we'll end up using the last "mode", which has nints == 1.
     */
    let mut selector: usize = 0;
    let mut nints = SIMPLE8B_MODES[0].num_ints;
    let mut bits = SIMPLE8B_MODES[0].bits_per_int;
    let mut diff = ints[0] - base - 1;
    let mut last_val = ints[0];
    let mut i: i32 = 0; /* number of deltas we have accepted */
    loop {
        if diff >= (1_u64 << bits) {
            /* too large, step up to next mode */
            selector += 1;
            nints = SIMPLE8B_MODES[selector].num_ints;
            bits = SIMPLE8B_MODES[selector].bits_per_int;
            /* we might already have accepted enough deltas for this mode */
            if i >= nints {
                break;
            }
        } else {
            /* accept this delta; then done if codeword is full */
            i += 1;
            if i >= nints {
                break;
            }
            /* examine next delta */
            debug_assert!(ints[i as usize] > last_val);
            diff = ints[i as usize] - last_val - 1;
            last_val = ints[i as usize];
        }
    }

    if nints == 0 {
        /*
         * The first delta is too large to be encoded with Simple-8b.
         *
         * If there is at least one not-too-large integer in the input, we will
         * encode it using mode 15 (or a more compact mode).  Hence, we can only
         * get here if the *first* delta is >= 2^60.
         */
        debug_assert_eq!(i, 0);
        return (EMPTY_CODEWORD, 0);
    }

    /*
     * Encode the integers using the selected mode.  Note that we shift them into
     * the codeword in reverse order, so that they will come out in the correct
     * order in the decoder.
     */
    let mut codeword: u64 = 0;
    if bits > 0 {
        let mut j = nints - 1;
        while j > 0 {
            diff = ints[j as usize] - ints[(j - 1) as usize] - 1;
            codeword |= diff;
            codeword <<= bits;
            j -= 1;
        }
        diff = ints[0] - base - 1;
        codeword |= diff;
    }

    /* add selector to the codeword, and return */
    codeword |= (selector as u64) << 60;

    (codeword, nints)
}

/*
 * Decode a codeword into an array of integers.
 * Returns the number of integers decoded.
 */
fn simple8b_decode(codeword: u64, decoded: &mut [u64], base: u64) -> i32 {
    let selector = (codeword >> 60) as usize;
    let nints = SIMPLE8B_MODES[selector].num_ints;
    let bits = SIMPLE8B_MODES[selector].bits_per_int;
    let mask = (1_u64 << bits).wrapping_sub(1);
    let mut curr_value;

    if codeword == EMPTY_CODEWORD {
        return 0;
    }

    let mut cw = codeword;
    curr_value = base;
    for slot in decoded.iter_mut().take(nints as usize) {
        let diff = cw & mask;

        curr_value += 1 + diff;
        *slot = curr_value;
        cw >>= bits;
    }
    nints
}

/*
 * This is very similar to simple8b_decode(), but instead of decoding all the
 * values to an array, it just checks if the given "key" is part of the
 * codeword.
 */
fn simple8b_contains(codeword: u64, key: u64, base: u64) -> bool {
    let selector = (codeword >> 60) as usize;
    let nints = SIMPLE8B_MODES[selector].num_ints;
    let bits = SIMPLE8B_MODES[selector].bits_per_int;

    if codeword == EMPTY_CODEWORD {
        return false;
    }

    if bits == 0 {
        /* Special handling for 0-bit cases. */
        (key - base) <= nints as u64
    } else {
        let mask = (1_u64 << bits) - 1;
        let mut curr_value;

        let mut cw = codeword;
        curr_value = base;
        for _ in 0..nints {
            let diff = cw & mask;

            curr_value += 1 + diff;

            if curr_value >= key {
                return curr_value == key;
            }

            cw >>= bits;
        }
        false
    }
}

#[cfg(test)]
extern crate std;

#[cfg(test)]
mod tests;
