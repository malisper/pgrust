#![no_std]
#![allow(non_upper_case_globals)]
//! Free Space Map (FSM) page ABI — `src/include/storage/fsm_internals.h`.
//!
//! The FSM is stored in a dedicated relation fork (`FSM_FORKNUM`). Each FSM
//! page holds a flat array encoding a binary max-tree of one-byte free-space
//! "categories": the first `NonLeafNodesPerPage` bytes are interior nodes and
//! the following `LeafNodesPerPage` bytes are the leaf "slots". This module
//! provides the `FSMPageData` layout plus the page-geometry constants, matching
//! the C header so the in-crate FSM logic (`backend-storage-freespace`)
//! manipulates the same bytes as PostgreSQL.

extern crate alloc;

use alloc::vec::Vec;

use ::types_core::primitive::BLCKSZ;
use ::types_storage::bufpage::SizeOfPageHeaderData;

/// `MAXALIGN(LEN)` — round up to `MAXIMUM_ALIGNOF` (8 on supported platforms),
/// matching `#define MAXALIGN(LEN)` in c.h.
const fn maxalign(len: usize) -> usize {
    const MAXIMUM_ALIGNOF: usize = 8;
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `offsetof(FSMPageData, fp_nodes)` — the flexible array follows a single
/// 4-byte `int` with no intervening padding, so the C `offsetof` is `4`.
pub const OFFSET_OF_FP_NODES: usize = 4;

/// `#define NodesPerPage (BLCKSZ - MAXALIGN(SizeOfPageHeaderData) -
/// offsetof(FSMPageData, fp_nodes))` — total bytes (tree nodes) available on a
/// FSM page. With the default 8 KiB page this is `8192 - 24 - 4 = 8164`.
pub const NodesPerPage: usize = BLCKSZ - maxalign(SizeOfPageHeaderData as usize) - OFFSET_OF_FP_NODES;

/// `#define NonLeafNodesPerPage (BLCKSZ / 2 - 1)` — number of interior tree
/// nodes (`4095` on an 8 KiB page).
pub const NonLeafNodesPerPage: usize = BLCKSZ / 2 - 1;

/// `#define LeafNodesPerPage (NodesPerPage - NonLeafNodesPerPage)` — number of
/// leaf nodes, i.e. addressable slots (`4069` on an 8 KiB page).
pub const LeafNodesPerPage: usize = NodesPerPage - NonLeafNodesPerPage;

/// `#define SlotsPerFSMPage LeafNodesPerPage` — the count to use outside
/// `fsmpage.c`.
pub const SlotsPerFSMPage: usize = LeafNodesPerPage;

/// `typedef struct { int fp_next_slot; uint8 fp_nodes[FLEXIBLE_ARRAY_MEMBER]; }
/// FSMPageData;` (fsm_internals.h).
///
/// `fp_next_slot` is the round-robin hint pointing at the next slot to hand
/// out; it is declared `int` (not `uint16`) precisely so it can be read/written
/// without an exclusive lock, as a plain word load/store. `fp_nodes` is the
/// flexible array holding the binary tree (`NodesPerPage` one-byte nodes).
///
/// This is the owned value materialized from `PageGetContents(page)` of an FSM
/// buffer; the buffer-manager seam reads it out and writes it back, so the FSM
/// algorithm never touches a raw `Page` pointer.
#[derive(Clone, Debug)]
pub struct FSMPageData {
    pub fp_next_slot: i32,
    pub fp_nodes: Vec<u8>,
}
