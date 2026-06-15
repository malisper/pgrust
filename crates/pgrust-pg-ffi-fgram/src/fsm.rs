//! Free Space Map (FSM) page ABI — `src/include/storage/fsm_internals.h`.
//!
//! The FSM is stored in a dedicated relation fork (`FSM_FORKNUM`).  Each FSM
//! page holds a flat array encoding a binary max-tree of one-byte free-space
//! "categories": the first `NonLeafNodesPerPage` bytes are interior nodes and
//! the following `LeafNodesPerPage` bytes are the leaf "slots".  This module
//! provides the `#[repr(C)]` `FSMPageData` layout plus the page-geometry
//! constants, matching the C header exactly so the in-crate FSM logic
//! (`backend-storage-freespace`) manipulates the same bytes as PostgreSQL.

use crate::storage::SizeOfPageHeaderData;
use crate::types::{uint8, BLCKSZ};

/// `typedef struct { int fp_next_slot; uint8 fp_nodes[FLEXIBLE_ARRAY_MEMBER]; }
/// FSMPageData;` (fsm_internals.h).
///
/// `fp_next_slot` is the round-robin hint pointing at the next slot to hand
/// out; it is declared `int` (not `uint16`) precisely so it can be read/written
/// without an exclusive lock, as a plain word load/store.  `fp_nodes` is the
/// flexible array holding the binary tree; it is modeled here as a zero-length
/// array so `offset_of!(FSMPageData, fp_nodes)` matches the C `offsetof`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FSMPageData {
    pub fp_next_slot: core::ffi::c_int,
    pub fp_nodes: [uint8; 0],
}

/// `typedef FSMPageData *FSMPage;`
pub type FSMPage = *mut FSMPageData;

/// `MAXALIGN(LEN)` — round up to `MAXIMUM_ALIGNOF` (8 on supported platforms),
/// matching `#define MAXALIGN(LEN)` in c.h.
const fn maxalign(len: usize) -> usize {
    const MAXIMUM_ALIGNOF: usize = 8;
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `#define NodesPerPage (BLCKSZ - MAXALIGN(SizeOfPageHeaderData) -
/// offsetof(FSMPageData, fp_nodes))` — total bytes (tree nodes) available on a
/// FSM page.  With the default 8 KiB page this is `8192 - 24 - 4 = 8164`.
pub const NodesPerPage: usize =
    BLCKSZ - maxalign(SizeOfPageHeaderData) - core::mem::offset_of!(FSMPageData, fp_nodes);

/// `#define NonLeafNodesPerPage (BLCKSZ / 2 - 1)` — number of interior tree
/// nodes (`4095` on an 8 KiB page).
pub const NonLeafNodesPerPage: usize = BLCKSZ / 2 - 1;

/// `#define LeafNodesPerPage (NodesPerPage - NonLeafNodesPerPage)` — number of
/// leaf nodes, i.e. addressable slots (`4069` on an 8 KiB page).
pub const LeafNodesPerPage: usize = NodesPerPage - NonLeafNodesPerPage;

/// `#define SlotsPerFSMPage LeafNodesPerPage` — the count to use outside
/// `fsmpage.c`.
pub const SlotsPerFSMPage: usize = LeafNodesPerPage;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fsmpage_layout_matches_c() {
        // fp_next_slot is the first member (offset 0); fp_nodes follows the
        // 4-byte int (no padding before a uint8 array), so offsetof == 4.
        assert_eq!(core::mem::offset_of!(FSMPageData, fp_next_slot), 0);
        assert_eq!(core::mem::offset_of!(FSMPageData, fp_nodes), 4);
    }

    #[test]
    fn fsm_geometry_constants_match_c() {
        // Default 8 KiB page: SizeOfPageHeaderData == 24, MAXALIGN(24) == 24,
        // offsetof(FSMPageData, fp_nodes) == 4.
        assert_eq!(SizeOfPageHeaderData, 24);
        assert_eq!(NodesPerPage, 8164);
        assert_eq!(NonLeafNodesPerPage, 4095);
        assert_eq!(LeafNodesPerPage, 4069);
        assert_eq!(SlotsPerFSMPage, 4069);
        // FSM_TREE_DEPTH = (SlotsPerFSMPage >= 1626) ? 3 : 4 == 3.
        assert!(SlotsPerFSMPage >= 1626);
    }
}
