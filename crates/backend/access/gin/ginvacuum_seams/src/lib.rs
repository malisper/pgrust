//! Seam declarations the `backend-access-gin-ginvacuum` unit (`ginvacuum.c`)
//! needs — the few cross-subsystem calls whose owners are not yet ported.
//!
//! The bulk of GIN vacuum's external substrate already has seams owned by their
//! real owners (bufmgr `LockBuffer`/`ReadBufferExtended`/`MarkBufferDirty`/…,
//! lmgr `LockRelationForExtension`, freespace `RecordFreeIndexPage`/
//! `IndexFreeSpaceMapVacuum`, varsup `ReadNextTransactionId`, vacuum
//! `vacuum_delay_point`/`AmAutoVacuumWorkerProcess`/`vacuum_tid_is_dead`,
//! relcache `RelationNeedsWAL`/`RelationGetNumberOfBlocks`, hio
//! `RELATION_IS_LOCAL`). `ginvacuum` consumes those directly.
//!
//! What remains, declared here, is the substrate whose owners are still
//! unported; a call panics loudly until they land (mirror-PG-and-panic):
//!
//!  * `PredicateLockPageCombine` (predicate.c) — no owner seam reaches this
//!    crate yet (nbtree-core has the same gap).
//!  * `GlobalVisCheckRemovableXid` (procarray.c) — the GIN-page recyclability
//!    test; the `(NULL, xid)` form GIN uses (no relation/heaprel context).
//!  * `ginInsertCleanup` (ginfast.c) — flush the fast-update pending list; the
//!    `ginfast.c` owner is not yet ported.

#![allow(clippy::result_large_err)]

use types_core::primitive::BlockNumber;

// `PredicateLockPageCombine` is owned by predicate.c — re-homed to
// `backend-storage-lmgr-predicate-seams::predicate_lock_page_combine`.
// `GlobalVisCheckRemovableXid(NULL, xid)` is owned by procarray.c — re-homed to
// `backend-storage-ipc-procarray-seams::global_vis_check_removable_xid`.

seam_core::seam!(
    /// `ginInsertCleanup(ginstate, full_clean, fill_fsm, forceCleanup, stats)`
    /// (ginfast.c): flush the fast-update pending list into the main index,
    /// accumulating `stats->pages_deleted`. Reached by `ginbulkdelete` /
    /// `ginvacuumcleanup` (and the autovacuum-analyze path). C re-derives the
    /// `GinState` from the index via `initGinState(&ginstate, index)` right
    /// before the call, so the seam carries the open `Relation` (and the `Mcx`
    /// the GinState's scratch allocations live in) and the owner builds the
    /// `GinState`. Returns the number of pages deleted to fold into the running
    /// stats.
    pub fn gin_insert_cleanup<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        index: &rel::Relation<'mcx>,
        full_clean: bool,
        fill_fsm: bool,
        force_cleanup: bool,
    ) -> types_error::PgResult<BlockNumber>
);
