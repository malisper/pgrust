use core::ffi::c_void;

use crate::types::Size;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct RBTNode {
    pub color: i8,
    pub left: *mut RBTNode,
    pub right: *mut RBTNode,
    pub parent: *mut RBTNode,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RBTOrderControl {
    LeftRightWalk = 0,
    RightLeftWalk = 1,
}

pub enum RBTree {}

pub type rbt_comparator =
    Option<unsafe extern "C" fn(*const RBTNode, *const RBTNode, *mut c_void) -> i32>;
pub type rbt_combiner = Option<unsafe extern "C" fn(*mut RBTNode, *const RBTNode, *mut c_void)>;
pub type rbt_allocfunc = Option<unsafe extern "C" fn(*mut c_void) -> *mut RBTNode>;
pub type rbt_freefunc = Option<unsafe extern "C" fn(*mut RBTNode, *mut c_void)>;
pub type rbt_iterate_func = Option<unsafe extern "C" fn(*mut RBTreeIterator) -> *mut RBTNode>;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct RBTreeIterator {
    pub rbt: *mut RBTree,
    pub iterate: rbt_iterate_func,
    pub last_visited: *mut RBTNode,
    pub is_over: bool,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BipartiteMatchStateData {
    pub u_size: i32,
    pub v_size: i32,
    pub adjacency: *mut *mut i16,
    pub matching: i32,
    pub pair_uv: *mut i16,
    pub pair_vu: *mut i16,
    pub distance: *mut i16,
    pub queue: *mut i16,
}

pub type BipartiteMatchState = *mut BipartiteMatchStateData;

pub enum bloom_filter {}

/*
 * On-disk / ABI layout structs for the IntegerSet in-memory B-tree.
 *
 * These mirror src/backend/lib/integerset.c exactly, so they are kept here as
 * #[repr(C)] exact-layout types with compile-time size/offset assertions.
 */

/// Maximum number of integers that can be encoded in a single Simple-8b
/// codeword.
pub const SIMPLE8B_MAX_VALUES_PER_CODEWORD: usize = 240;

/// Parameters for shape of the in-memory B-tree (see integerset.c).
pub const MAX_INTERNAL_ITEMS: usize = 64;
pub const MAX_LEAF_ITEMS: usize = 64;

/// Maximum height of the tree.
pub const MAX_TREE_LEVELS: usize = 11;

/// `1 + SIMPLE8B_MAX_VALUES_PER_CODEWORD`
pub const MAX_VALUES_PER_LEAF_ITEM: usize = 1 + SIMPLE8B_MAX_VALUES_PER_CODEWORD;

/// `MAX_VALUES_PER_LEAF_ITEM * 2`
pub const MAX_BUFFERED_VALUES: usize = MAX_VALUES_PER_LEAF_ITEM * 2;

/// Common structure of both leaf and internal nodes.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct intset_node {
    /// tree level of this node
    pub level: u16,
    /// number of items in this node
    pub num_items: u16,
}

/// Internal node. The common header (`level`, `num_items`) must match
/// `intset_node`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct intset_internal_node {
    /// >= 1 on internal nodes
    pub level: u16,
    pub num_items: u16,
    /// array of key values
    pub values: [u64; MAX_INTERNAL_ITEMS],
    /// pointers to lower-level nodes, corresponding to the key values
    pub downlinks: [*mut intset_node; MAX_INTERNAL_ITEMS],
}

/// Leaf item: the first integer in plain format, plus the Simple-8b encoded
/// differences from `first`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct leaf_item {
    /// first integer in this item
    pub first: u64,
    /// simple8b encoded differences from `first`
    pub codeword: u64,
}

/// Leaf node. The common header (`level`, `num_items`) must match
/// `intset_node`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct intset_leaf_node {
    /// 0 on leafs
    pub level: u16,
    pub num_items: u16,
    /// right sibling, if any
    pub next: *mut intset_leaf_node,
    pub items: [leaf_item; MAX_LEAF_ITEMS],
}

/// Opaque handle exposed by the Rust API (definition lives in
/// backend-lib-integerset).
pub enum IntegerSet {}

pub const RBTBLACK: i8 = 0;
pub const RBTRED: i8 = 1;

/// Comparator callback for the pairing heap.
///
/// Mirrors PostgreSQL's `pairingheap_comparator`: for a max-heap, returns `<0`
/// iff `a < b`, `0` iff `a == b`, and `>0` iff `a > b`. The `arg` pointer is the
/// opaque argument supplied to `pairingheap_allocate`. Unlike the C original,
/// this is a plain Rust function pointer (no `extern "C"`), in keeping with the
/// zero-FFI policy of the ported lib crates. It is layout-compatible with the C
/// function-pointer field it replaces (both are a single non-null pointer).
#[allow(non_camel_case_types)]
pub type pairingheap_comparator =
    Option<fn(*const pairingheap_node, *const pairingheap_node, *mut c_void) -> i32>;

/// Intrusive heap node embedded in caller-owned values.
///
/// Mirrors `pairingheap_node` from `src/include/lib/pairingheap.h`: a node can
/// have multiple children forming a doubly-linked sibling list. `first_child`
/// points to the node's first child; subsequent children follow `next_sibling`
/// (last child has `next_sibling == NULL`). `prev_or_parent` points to the
/// previous sibling, or to the parent if the node is its parent's first child.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct pairingheap_node {
    pub first_child: *mut pairingheap_node,
    pub next_sibling: *mut pairingheap_node,
    pub prev_or_parent: *mut pairingheap_node,
}

/// PostgreSQL-compatible pairing heap control object.
///
/// Mirrors `pairingheap` from `src/include/lib/pairingheap.h`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct pairingheap {
    pub ph_compare: pairingheap_comparator,
    pub ph_arg: *mut c_void,
    pub ph_root: *mut pairingheap_node,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BloomFilterHeader {
    pub k_hash_funcs: i32,
    pub seed: u64,
    pub m: u64,
}

impl BloomFilterHeader {
    pub const fn bitset_offset() -> Size {
        core::mem::size_of::<Self>()
    }
}

/// On-disk / ABI layout struct for the HyperLogLog cardinality estimator
/// (`src/include/lib/hyperloglog.h`).
///
/// Mirrors `struct hyperLogLogState` field-for-field so the in-memory layout is
/// byte-compatible with PostgreSQL. Kept here as the single canonical
/// `#[repr(C)]` exact-layout type with compile-time size/offset assertions;
/// `backend-lib-hyperloglog` operates on this type directly.
///
/// ```text
///   uint8  registerWidth;   register width, in bits ("k")
///   Size   nRegisters;      number of registers
///   double alphaMM;         alpha * m ^ 2 (see initHyperLogLog())
///   uint8 *hashesArr;       array of hashes
///   Size   arrSize;         size of hashesArr
/// ```
#[allow(non_snake_case)]
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct hyperLogLogState {
    pub registerWidth: u8,
    pub nRegisters: Size,
    pub alphaMM: f64,
    pub hashesArr: *mut u8,
    pub arrSize: Size,
}

// Compile-time exact-layout enforcement (LP64: size 40, 8-byte alignment).
const _: () = assert!(core::mem::size_of::<hyperLogLogState>() == 40);
const _: () = assert!(core::mem::align_of::<hyperLogLogState>() == core::mem::align_of::<Size>());
const _: () = assert!(core::mem::offset_of!(hyperLogLogState, registerWidth) == 0);
const _: () = assert!(core::mem::offset_of!(hyperLogLogState, nRegisters) == 8);
const _: () = assert!(core::mem::offset_of!(hyperLogLogState, alphaMM) == 16);
const _: () = assert!(core::mem::offset_of!(hyperLogLogState, hashesArr) == 24);
const _: () = assert!(core::mem::offset_of!(hyperLogLogState, arrSize) == 32);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rbtree_node_layout_matches_postgres() {
        assert_eq!(core::mem::offset_of!(RBTNode, color), 0);
        assert_eq!(core::mem::offset_of!(RBTNode, left), 8);
        assert_eq!(core::mem::offset_of!(RBTNode, right), 16);
        assert_eq!(core::mem::offset_of!(RBTNode, parent), 24);
        assert_eq!(core::mem::size_of::<RBTNode>(), 32);
    }

    #[test]
    fn rbtree_iterator_layout_matches_postgres() {
        assert_eq!(core::mem::offset_of!(RBTreeIterator, rbt), 0);
        assert_eq!(core::mem::offset_of!(RBTreeIterator, iterate), 8);
        assert_eq!(core::mem::offset_of!(RBTreeIterator, last_visited), 16);
        assert_eq!(core::mem::offset_of!(RBTreeIterator, is_over), 24);
        assert_eq!(core::mem::size_of::<RBTreeIterator>(), 32);
    }

    #[test]
    fn pairingheap_node_layout_matches_postgres() {
        assert_eq!(core::mem::offset_of!(pairingheap_node, first_child), 0);
        assert_eq!(core::mem::offset_of!(pairingheap_node, next_sibling), 8);
        assert_eq!(core::mem::offset_of!(pairingheap_node, prev_or_parent), 16);
        assert_eq!(core::mem::size_of::<pairingheap_node>(), 24);
        assert_eq!(
            core::mem::align_of::<pairingheap_node>(),
            core::mem::align_of::<usize>()
        );
    }

    #[test]
    fn pairingheap_layout_matches_postgres() {
        assert_eq!(core::mem::offset_of!(pairingheap, ph_compare), 0);
        assert_eq!(core::mem::offset_of!(pairingheap, ph_arg), 8);
        assert_eq!(core::mem::offset_of!(pairingheap, ph_root), 16);
        assert_eq!(core::mem::size_of::<pairingheap>(), 24);
        assert_eq!(
            core::mem::align_of::<pairingheap>(),
            core::mem::align_of::<usize>()
        );
    }

    #[test]
    fn bipartite_match_state_layout_matches_postgres() {
        assert_eq!(core::mem::offset_of!(BipartiteMatchStateData, u_size), 0);
        assert_eq!(core::mem::offset_of!(BipartiteMatchStateData, adjacency), 8);
        assert_eq!(core::mem::offset_of!(BipartiteMatchStateData, matching), 16);
        assert_eq!(core::mem::offset_of!(BipartiteMatchStateData, pair_uv), 24);
        assert_eq!(core::mem::size_of::<BipartiteMatchStateData>(), 56);
    }

    #[test]
    fn intset_node_layout_matches_postgres() {
        assert_eq!(core::mem::offset_of!(intset_node, level), 0);
        assert_eq!(core::mem::offset_of!(intset_node, num_items), 2);
        assert_eq!(core::mem::size_of::<intset_node>(), 4);
    }

    #[test]
    fn leaf_item_layout_matches_postgres() {
        assert_eq!(core::mem::offset_of!(leaf_item, first), 0);
        assert_eq!(core::mem::offset_of!(leaf_item, codeword), 8);
        assert_eq!(core::mem::size_of::<leaf_item>(), 16);
    }

    #[test]
    fn intset_internal_node_layout_matches_postgres() {
        assert_eq!(core::mem::offset_of!(intset_internal_node, level), 0);
        assert_eq!(core::mem::offset_of!(intset_internal_node, num_items), 2);
        assert_eq!(core::mem::offset_of!(intset_internal_node, values), 8);
        assert_eq!(core::mem::offset_of!(intset_internal_node, downlinks), 520);
        assert_eq!(core::mem::size_of::<intset_internal_node>(), 1032);
    }

    #[test]
    fn intset_leaf_node_layout_matches_postgres() {
        assert_eq!(core::mem::offset_of!(intset_leaf_node, level), 0);
        assert_eq!(core::mem::offset_of!(intset_leaf_node, num_items), 2);
        assert_eq!(core::mem::offset_of!(intset_leaf_node, next), 8);
        assert_eq!(core::mem::offset_of!(intset_leaf_node, items), 16);
        assert_eq!(core::mem::size_of::<intset_leaf_node>(), 1040);
    }

    #[test]
    fn hyperloglog_state_layout_matches_postgres() {
        assert_eq!(core::mem::offset_of!(hyperLogLogState, registerWidth), 0);
        assert_eq!(core::mem::offset_of!(hyperLogLogState, nRegisters), 8);
        assert_eq!(core::mem::offset_of!(hyperLogLogState, alphaMM), 16);
        assert_eq!(core::mem::offset_of!(hyperLogLogState, hashesArr), 24);
        assert_eq!(core::mem::offset_of!(hyperLogLogState, arrSize), 32);
        assert_eq!(core::mem::size_of::<hyperLogLogState>(), 40);
        assert_eq!(
            core::mem::align_of::<hyperLogLogState>(),
            core::mem::align_of::<Size>()
        );
    }
}
