//! F3 — xmin/xid horizon computation + `GlobalVisState` machinery for vacuum
//! visibility (procarray.c).
//!
//! `ComputeXidHorizons`, the `GetOldestNonRemovableTransactionId` /
//! `GetOldestTransactionIdConsideredRunning` cutoffs, and the full GlobalVis
//! family (`GlobalVisTestFor`, `GlobalVisTestShouldUpdate`, `GlobalVisUpdate`,
//! `GlobalVisUpdateApply`, the removable-xid tests). Mutates the F0-owned
//! `GlobalVis{Shared,Catalog,Data,Temp}Rels` process-locals and reads
//! clog/transam latest-completed via the transam seam.
//!
//! Owns + installs the NEW inward seams `global_vis_test_for`,
//! `global_vis_test_is_removable_{xid,fullxid}`, and
//! `get_oldest_non_removable_transaction_id` — consumed by vacuumlazy + heapam
//! visibility, which today only hold a `GlobalVisStateHandle`
//! (`types_vacuum`).

use types_core::{FullTransactionId, Oid, TransactionId};
use types_error::PgResult;
use types_vacuum::vacuumlazy::GlobalVisStateHandle;

use crate::shmem_model::ComputeXidHorizonsResult;

/// `ComputeXidHorizons(ComputeXidHorizonsResult *h)` (procarray.c) — the single
/// scan that derives every xmin/removable cutoff from the ProcArray under
/// `ProcArrayLock`, also refreshing the GlobalVis statics. Reads
/// `TransamVariables->latestCompletedXid` and the replication-slot xmins.
pub fn ComputeXidHorizons() -> PgResult<ComputeXidHorizonsResult> {
    panic!("decomp: ComputeXidHorizons not yet filled")
}

/// `GlobalVisHorizonKindForRel(Relation rel)` (procarray.c, static) — classify
/// `rel` into the shared/catalog/data/temp visibility-horizon kind. Modelled on
/// the bare `Oid` identity carried across the seam.
pub fn GlobalVisHorizonKindForRel(_rel: Oid) -> i32 {
    panic!("decomp: GlobalVisHorizonKindForRel not yet filled")
}

/// `GetOldestNonRemovableTransactionId(Relation rel)` (procarray.c) — the
/// VACUUM removable cutoff for `rel`'s visibility class.
pub fn GetOldestNonRemovableTransactionId(_rel: Oid) -> PgResult<TransactionId> {
    panic!("decomp: GetOldestNonRemovableTransactionId not yet filled")
}

/// `GetOldestTransactionIdConsideredRunning(void)` (procarray.c) — the oldest
/// xid any backend might still consider running (incl. VACUUM); used to decide
/// pg_subtrans truncation.
pub fn GetOldestTransactionIdConsideredRunning() -> PgResult<TransactionId> {
    panic!("decomp: GetOldestTransactionIdConsideredRunning not yet filled")
}

/// `GlobalVisTestFor(Relation rel)` (procarray.c) — the appropriate
/// `GlobalVisState *` (handle) for `rel`, refreshing horizons if needed.
pub fn GlobalVisTestFor(_rel: Oid) -> PgResult<GlobalVisStateHandle> {
    panic!("decomp: GlobalVisTestFor not yet filled")
}

/// `GlobalVisTestShouldUpdate(GlobalVisState *state)` (procarray.c, static) —
/// whether `state` is stale enough relative to the local horizon snapshot to
/// warrant a recompute.
pub fn GlobalVisTestShouldUpdate(_state: GlobalVisStateHandle) -> bool {
    panic!("decomp: GlobalVisTestShouldUpdate not yet filled")
}

/// `GlobalVisUpdate(void)` (procarray.c) — recompute the GlobalVis statics from
/// a fresh `ComputeXidHorizons`.
pub fn GlobalVisUpdate() -> PgResult<()> {
    panic!("decomp: GlobalVisUpdate not yet filled")
}

/// `GlobalVisUpdateApply(ComputeXidHorizonsResult *horizons)` (procarray.c,
/// static) — push freshly-computed horizons into the four GlobalVis statics.
pub fn GlobalVisUpdateApply(_horizons: &ComputeXidHorizonsResult) {
    panic!("decomp: GlobalVisUpdateApply not yet filled")
}

/// `GlobalVisTestIsRemovableFullXid(GlobalVisState *state,
/// FullTransactionId fxid)` (procarray.c) — removability test (full-xid).
pub fn GlobalVisTestIsRemovableFullXid(
    _state: GlobalVisStateHandle,
    _fxid: FullTransactionId,
) -> bool {
    panic!("decomp: GlobalVisTestIsRemovableFullXid not yet filled")
}

/// `GlobalVisTestIsRemovableXid(GlobalVisState *state, TransactionId xid)`
/// (procarray.c) — removability test (32-bit xid; promoted via `FullXidRelativeTo`).
pub fn GlobalVisTestIsRemovableXid(_state: GlobalVisStateHandle, _xid: TransactionId) -> bool {
    panic!("decomp: GlobalVisTestIsRemovableXid not yet filled")
}

/// `GlobalVisCheckRemovableFullXid(GlobalVisState *state,
/// FullTransactionId fxid)` (procarray.c) — like the test variant but forces a
/// horizon update when the answer is initially "maybe needed".
pub fn GlobalVisCheckRemovableFullXid(
    _state: GlobalVisStateHandle,
    _fxid: FullTransactionId,
) -> PgResult<bool> {
    panic!("decomp: GlobalVisCheckRemovableFullXid not yet filled")
}

/// `GlobalVisCheckRemovableXid(GlobalVisState *state, TransactionId xid)`
/// (procarray.c) — the 32-bit-xid variant of the checked removability test.
pub fn GlobalVisCheckRemovableXid(
    _state: GlobalVisStateHandle,
    _xid: TransactionId,
) -> PgResult<bool> {
    panic!("decomp: GlobalVisCheckRemovableXid not yet filled")
}

/// Install the F3-owned inward seams: the NEW GlobalVis-resolution + removable
/// seams + the oldest-non-removable cutoff, consumed by vacuumlazy + heapam
/// visibility.
pub fn init_seams() {
    use backend_storage_ipc_procarray_seams as seams;

    seams::global_vis_test_for::set(GlobalVisTestFor);
    seams::global_vis_test_is_removable_xid::set(GlobalVisTestIsRemovableXid);
    seams::global_vis_test_is_removable_fullxid::set(GlobalVisTestIsRemovableFullXid);
    seams::get_oldest_non_removable_transaction_id::set(GetOldestNonRemovableTransactionId);
}
