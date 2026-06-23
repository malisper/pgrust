//! Inward seams for the heap page-pruning unit (`access/heap/pruneheap.c`).
//!
//! These are the pruneheap functions that *other* heap-AM families and the
//! heap xlog-redo path reach across the dependency cycle (heapam's `heapgetpage`
//! calls `heap_page_prune_opt`; the heap2 redo handler replays planned page
//! changes via `heap_page_prune_execute`; index build / `heap_get_latest_tid`
//! map TIDs to HOT-chain roots via `heap_get_root_tuples`). The owning
//! `backend-access-heap-pruneheap` crate installs them from its `init_seams()`;
//! until then each is the loud `seam_core::seam!` panic — nothing fabricates a
//! prune decision or a page mutation.

#![allow(non_snake_case)]

extern crate alloc;
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::primitive::OffsetNumber;
use types_error::PgResult;
use rel::Relation;
use types_storage::Buffer;

seam_core::seam!(
    /// `heap_page_prune_opt(relation, buffer)` — opportunistically prune and
    /// defragment `buffer`'s page if it heuristically looks worthwhile and a
    /// cleanup lock can be had without blocking. Caller holds a pin but no lock.
    pub fn heap_page_prune_opt<'mcx>(
        mcx: Mcx<'mcx>,
        relation: &Relation<'mcx>,
        buffer: Buffer,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `heap_page_prune_execute(buffer, lp_truncate_only, redirected,
    /// nredirected, nowdead, ndead, nowunused, nunused)` — apply the planned
    /// line-pointer changes (redirect / dead / unused) and repair fragmentation.
    /// `redirected` is the flat from/to pair array (length = 2 * nredirected).
    /// Used both by `heap_page_prune_and_freeze` and the heap2 prune/freeze
    /// redo handler.
    pub fn heap_page_prune_execute(
        buffer: Buffer,
        lp_truncate_only: bool,
        redirected: Vec<OffsetNumber>,
        nowdead: Vec<OffsetNumber>,
        nowunused: Vec<OffsetNumber>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `heap_get_root_tuples(page, root_offsets)` — for every item on
    /// `buffer`'s page, map it to the offset of its HOT-chain root. Returns the
    /// `MaxHeapTuplesPerPage`-long array (`InvalidOffsetNumber` for unmapped
    /// slots). Caller holds at least a share lock and a pin.
    pub fn heap_get_root_tuples<'mcx>(
        mcx: Mcx<'mcx>,
        buffer: Buffer,
    ) -> PgResult<Vec<OffsetNumber>>
);
