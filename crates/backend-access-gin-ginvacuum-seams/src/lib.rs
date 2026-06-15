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

use types_core::primitive::{BlockNumber, TransactionId};

seam_core::seam!(
    /// `PredicateLockPageCombine(relation, oldblkno, newblkno)` (predicate.c):
    /// transfer predicate locks from a page about to be unlinked to its right
    /// sibling. Reached by GIN `ginDeletePage`. Owner (predicate.c
    /// SSI-on-GIN) not yet ported — panics until then.
    pub fn predicate_lock_page_combine(
        relation: types_core::primitive::Oid,
        oldblkno: BlockNumber,
        newblkno: BlockNumber,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `GlobalVisCheckRemovableXid(NULL, xid)` (procarray.c): true if no backend
    /// could still view `xid` as in-progress (used by `GinPageIsRecyclable`).
    /// The GIN call passes `heaprel == NULL`, so the seam carries only the xid.
    pub fn global_vis_check_removable_xid(xid: TransactionId) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `ginInsertCleanup(ginstate, full_clean, fill_fsm, forceCleanup, stats)`
    /// (ginfast.c): flush the fast-update pending list into the main index,
    /// accumulating `stats->pages_deleted`. Reached by `ginbulkdelete` /
    /// `ginvacuumcleanup` (and the autovacuum-analyze path). The seam takes the
    /// index OID (the owner re-derives the `GinState`) and returns the number of
    /// pages deleted to fold into the running stats. Owner (`ginfast.c`) not yet
    /// ported — panics until then.
    pub fn gin_insert_cleanup(
        index: types_core::primitive::Oid,
        full_clean: bool,
        fill_fsm: bool,
        force_cleanup: bool,
    ) -> types_error::PgResult<BlockNumber>
);
