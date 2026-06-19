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

use std::cell::RefCell;

use types_core::{
    FullTransactionId, GlobalVisStateHandle, InvalidOid, InvalidTransactionId, Oid, TransactionId,
    TransactionIdIsValid,
};
use types_error::PgResult;

use backend_access_transam_varsup_seams as varsup;
use backend_access_transam_xlog_seams as xlog;
use backend_catalog_catalog_seams as catalog;
use backend_storage_lmgr_lwlock_seams as lwlock;
use backend_storage_lmgr_proc_seams as proc;
use backend_utils_cache_relcache_seams as relcache;
use backend_utils_time_snapmgr_pc_seams as snapmgr;

use types_storage::storage::{
    PROC_AFFECTS_ALL_HORIZONS, PROC_IN_LOGICAL_DECODING, PROC_IN_VACUUM,
};
use types_storage::LWLockMode;
use types_tuple::access::{RELKIND_MATVIEW, RELKIND_RELATION};

use crate::shmem_model::{
    FullTransactionIdNewer, FullXidRelativeTo, GlobalVisState, TransactionIdOlder,
    GLOBAL_VIS_CATALOG_RELS, GLOBAL_VIS_DATA_RELS, GLOBAL_VIS_SHARED_RELS, GLOBAL_VIS_TEMP_RELS,
    PROC_ARRAY,
};
use crate::shmem_model::ComputeXidHorizonsResult;

// ---------------------------------------------------------------------------
// File-static `static TransactionId ComputeXidHorizonsResultLastXmin;`
// (procarray.c:331) — the `RecentXmin` snapshotted at the last
// `GlobalVisUpdateApply`, used by `GlobalVisTestShouldUpdate` to decide whether
// a recompute is worthwhile. Backend thread-local (forked-child convention).
// ---------------------------------------------------------------------------
thread_local! {
    static COMPUTE_XID_HORIZONS_RESULT_LAST_XMIN: RefCell<TransactionId> =
        const { RefCell::new(InvalidTransactionId) };
}

// ---------------------------------------------------------------------------
// `GlobalVisHorizonKind` (procarray.c:273) + the stable `GlobalVisStateHandle`
// ids the four process-local `GlobalVisState`s resolve to. The whole-tree
// `GlobalVisStateHandle` is the INHERITED u64 opacity: `id == 0` is C's NULL,
// and ids 1..=4 name the four `GlobalVis{Shared,Catalog,Data,Temp}Rels`
// statics (there is no other backing store — the handle is just a tag).
// ---------------------------------------------------------------------------

/// `VISHORIZON_SHARED` (== 0).
const VISHORIZON_SHARED: i32 = 0;
/// `VISHORIZON_CATALOG` (== 1).
const VISHORIZON_CATALOG: i32 = 1;
/// `VISHORIZON_DATA` (== 2).
const VISHORIZON_DATA: i32 = 2;
/// `VISHORIZON_TEMP` (== 3).
const VISHORIZON_TEMP: i32 = 3;

const HANDLE_SHARED: u64 = 1;
const HANDLE_CATALOG: u64 = 2;
const HANDLE_DATA: u64 = 3;
const HANDLE_TEMP: u64 = 4;

/// Map a `GlobalVisHorizonKind` to the stable handle naming its process-local
/// `GlobalVisState` static.
fn handle_for_kind(kind: i32) -> GlobalVisStateHandle {
    let id = match kind {
        VISHORIZON_SHARED => HANDLE_SHARED,
        VISHORIZON_CATALOG => HANDLE_CATALOG,
        VISHORIZON_DATA => HANDLE_DATA,
        VISHORIZON_TEMP => HANDLE_TEMP,
        _ => 0,
    };
    GlobalVisStateHandle::new(id)
}

/// Read the `GlobalVisState` value behind `handle` (one of the four process-local
/// statics). Panics on the C-NULL handle (`id == 0`), which the callers never
/// produce (`GlobalVisTestFor` always returns a valid kind).
fn read_state(handle: GlobalVisStateHandle) -> GlobalVisState {
    match handle.id {
        HANDLE_SHARED => GLOBAL_VIS_SHARED_RELS.with(|g| *g.borrow()),
        HANDLE_CATALOG => GLOBAL_VIS_CATALOG_RELS.with(|g| *g.borrow()),
        HANDLE_DATA => GLOBAL_VIS_DATA_RELS.with(|g| *g.borrow()),
        HANDLE_TEMP => GLOBAL_VIS_TEMP_RELS.with(|g| *g.borrow()),
        _ => panic!("GlobalVisStateHandle id {} does not name a GlobalVisState", handle.id),
    }
}

// ---------------------------------------------------------------------------
// FullTransactionId comparison helpers (`access/transam.h`) local to F3.
// ---------------------------------------------------------------------------

#[inline]
fn full_transaction_id_precedes(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value < b.value
}

#[inline]
fn full_transaction_id_follows_or_equals(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value >= b.value
}

/// `TransactionIdAdvance(dest)` (`access/transam.h`) — `dest++`, skipping the
/// special low xids on wraparound.
#[inline]
fn transaction_id_advance(dest: TransactionId) -> TransactionId {
    let mut d = dest.wrapping_add(1);
    if d < types_core::FirstNormalTransactionId {
        d = types_core::FirstNormalTransactionId;
    }
    d
}

/// `RecentXmin` (snapmgr.c) read through the pc-seam.
#[inline]
fn recent_xmin() -> TransactionId {
    snapmgr::recent_xmin::call()
}

/// `ComputeXidHorizons(ComputeXidHorizonsResult *h)` (procarray.c) — the single
/// scan that derives every xmin/removable cutoff from the ProcArray under
/// `ProcArrayLock`, also refreshing the GlobalVis statics. Reads
/// `TransamVariables->latestCompletedXid` and the replication-slot xmins.
pub fn ComputeXidHorizons() -> PgResult<ComputeXidHorizonsResult> {
    let mut h = ComputeXidHorizonsResult::default();

    let in_recovery = xlog::recovery_in_progress::call();

    // inferred after ProcArrayLock is released
    h.catalog_oldest_nonremovable = InvalidTransactionId;

    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)?;

    h.latest_completed = varsup::get_latest_completed_xid::call();

    // We initialize the MIN() calculation with latestCompletedXid + 1. This is a
    // lower bound for the XIDs that might appear in the ProcArray later, and so
    // protects us against overestimating the result due to future additions.
    let initial = {
        let initial = h.latest_completed.xid();
        debug_assert!(TransactionIdIsValid(initial));
        let initial = transaction_id_advance(initial);

        h.oldest_considered_running = initial;
        h.shared_oldest_nonremovable = initial;
        h.data_oldest_nonremovable = initial;

        // Only modifications made by this backend affect the horizon for
        // temporary relations. Initialize to the current top-level xid if any,
        // else latestCompletedXid + 1.
        let myxid = proc::my_proc_xid::call();
        if TransactionIdIsValid(myxid) {
            h.temp_oldest_nonremovable = myxid;
        } else {
            h.temp_oldest_nonremovable = initial;
        }
        initial
    };
    let _ = initial;

    // Fetch slot horizons while ProcArrayLock is held - the
    // LWLockAcquire/LWLockRelease are a barrier.
    (h.slot_xmin, h.slot_catalog_xmin) = PROC_ARRAY.with(|pa| {
        let b = pa.borrow();
        let a = b.as_ref().expect("ProcArray accessed before ProcArrayShmemInit");
        (a.replication_slot_xmin, a.replication_slot_catalog_xmin)
    });

    let my_database_id = backend_utils_init_small::globals::MyDatabaseId();
    let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);

    for index in 0..num_procs {
        let pgprocno =
            PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos()[index as usize]);
        let status_flags = proc::proc_global_status_flags::call(index);

        // Fetch xid just once - see GetNewTransactionId
        let xid = proc::proc_array_xid::call(index);
        let mut xmin = proc::proc_xmin::call(pgprocno);

        // Consider both the transaction's Xmin and its Xid.
        xmin = TransactionIdOlder(xmin, xid);

        // if neither is set, this proc doesn't influence the horizon
        if !TransactionIdIsValid(xmin) {
            continue;
        }

        // Don't ignore any procs when determining which transactions might be
        // considered running.
        h.oldest_considered_running = TransactionIdOlder(h.oldest_considered_running, xmin);

        // Skip over backends either vacuuming or doing logical decoding.
        if status_flags & (PROC_IN_VACUUM | PROC_IN_LOGICAL_DECODING) != 0 {
            continue;
        }

        // shared tables need to take backends in all databases into account
        h.shared_oldest_nonremovable = TransactionIdOlder(h.shared_oldest_nonremovable, xmin);

        // Normally sessions in other databases are ignored for anything but the
        // shared horizon. Include them when MyDatabaseId is not (yet) set, when
        // PROC_AFFECTS_ALL_HORIZONS is set, or in recovery.
        if proc::proc_database_id::call(pgprocno) == my_database_id
            || my_database_id == InvalidOid
            || (status_flags & PROC_AFFECTS_ALL_HORIZONS) != 0
            || in_recovery
        {
            h.data_oldest_nonremovable = TransactionIdOlder(h.data_oldest_nonremovable, xmin);
        }
    }

    // If in recovery fetch oldest xid in KnownAssignedXids, applied after the
    // lock is released.
    let kaxmin = if in_recovery {
        crate::knownassignedxids::KnownAssignedXidsGetOldestXmin()
    } else {
        InvalidTransactionId
    };

    // No other shared state is needed; release the lock immediately.
    lwlock::lwlock_release_proc_array::call()?;

    if in_recovery {
        h.oldest_considered_running = TransactionIdOlder(h.oldest_considered_running, kaxmin);
        h.shared_oldest_nonremovable = TransactionIdOlder(h.shared_oldest_nonremovable, kaxmin);
        h.data_oldest_nonremovable = TransactionIdOlder(h.data_oldest_nonremovable, kaxmin);
        // temp relations cannot be accessed in recovery
    }

    // Check whether there are replication slots requiring an older xmin.
    h.shared_oldest_nonremovable = TransactionIdOlder(h.shared_oldest_nonremovable, h.slot_xmin);
    h.data_oldest_nonremovable = TransactionIdOlder(h.data_oldest_nonremovable, h.slot_xmin);

    // The only difference between catalog / data horizons is that the slot's
    // catalog xmin is applied to the catalog one. Initialize with data horizon
    // and then back up further if necessary. Have to back up the shared horizon
    // as well, since that also can contain catalogs.
    h.shared_oldest_nonremovable_raw = h.shared_oldest_nonremovable;
    h.shared_oldest_nonremovable =
        TransactionIdOlder(h.shared_oldest_nonremovable, h.slot_catalog_xmin);
    h.catalog_oldest_nonremovable = h.data_oldest_nonremovable;
    h.catalog_oldest_nonremovable =
        TransactionIdOlder(h.catalog_oldest_nonremovable, h.slot_catalog_xmin);

    // It's possible that slots backed up the horizons further than
    // oldest_considered_running. Fix.
    h.oldest_considered_running =
        TransactionIdOlder(h.oldest_considered_running, h.shared_oldest_nonremovable);
    h.oldest_considered_running =
        TransactionIdOlder(h.oldest_considered_running, h.catalog_oldest_nonremovable);
    h.oldest_considered_running =
        TransactionIdOlder(h.oldest_considered_running, h.data_oldest_nonremovable);

    // update approximate horizons with the computed horizons
    GlobalVisUpdateApply(&h);

    Ok(h)
}

/// `GlobalVisHorizonKindForRel(Relation rel)` (procarray.c, static) — classify
/// `rel` into the shared/catalog/data/temp visibility-horizon kind. The seam
/// carries the bare `Oid` identity; the C reads the relation's
/// `relisshared`/relkind/catalog/temp properties, so the owner re-opens the
/// relcache entry (no lock taken — the caller already holds one, as in C) to
/// classify. `rel == InvalidOid` selects the most conservative (shared) horizon.
pub fn GlobalVisHorizonKindForRel(rel: Oid) -> PgResult<i32> {
    // rel == NULL || rel->rd_rel->relisshared || RecoveryInProgress()
    //     => VISHORIZON_SHARED
    if rel == InvalidOid || xlog::recovery_in_progress::call() {
        return Ok(VISHORIZON_SHARED);
    }

    let ctx = mcx::MemoryContext::new("GlobalVisHorizonKindForRel");
    let mcx = ctx.mcx();

    let relation = relcache::relation_id_get_relation::call(mcx, rel)?
        .ok_or_else(|| types_error::PgError::error("relation no longer exists"))?;

    // Other relkinds currently don't contain xids (C Assert).
    debug_assert!(
        relation.rd_rel.relkind == RELKIND_RELATION
            || relation.rd_rel.relkind == RELKIND_MATVIEW
            || relation.rd_rel.relkind == types_tuple::access::RELKIND_TOASTVALUE
    );

    let kind = if relation.rd_rel.relisshared {
        VISHORIZON_SHARED
    } else if catalog::is_catalog_relation::call(&relation)
        || relation_is_accessible_in_logical_decoding(&relation)
    {
        VISHORIZON_CATALOG
    } else if !relcache::relation_is_local::call(&relation) {
        VISHORIZON_DATA
    } else {
        VISHORIZON_TEMP
    };

    relcache::relation_close::call(rel)?;

    Ok(kind)
}

/// `RelationIsAccessibleInLogicalDecoding(relation)` (`utils/rel.h`):
/// `XLogLogicalInfoActive() && RelationNeedsWAL(relation) &&
///  (IsCatalogRelation(relation) || RelationIsUsedAsCatalogTable(relation))`.
fn relation_is_accessible_in_logical_decoding(
    relation: &types_rel::RelationData<'_>,
) -> bool {
    xlog::xlog_logical_info_active::call()
        && relcache::relation_needs_wal::call(relation)
        && (catalog::is_catalog_relation::call(relation)
            || relation_is_used_as_catalog_table(relation))
}

/// `RelationIsUsedAsCatalogTable(relation)` (`utils/rel.h`): `rd_options &&
/// (relkind == RELATION || relkind == MATVIEW) ?
/// ((StdRdOptions *) rd_options)->user_catalog_table : false`.
fn relation_is_used_as_catalog_table(relation: &types_rel::RelationData<'_>) -> bool {
    match &relation.rd_options {
        Some(opts)
            if relation.rd_rel.relkind == RELKIND_RELATION
                || relation.rd_rel.relkind == RELKIND_MATVIEW =>
        {
            opts.user_catalog_table
        }
        _ => false,
    }
}

/// `GetOldestNonRemovableTransactionId(Relation rel)` (procarray.c) — the
/// VACUUM removable cutoff for `rel`'s visibility class.
pub fn GetOldestNonRemovableTransactionId(rel: Oid) -> PgResult<TransactionId> {
    let horizons = ComputeXidHorizons()?;

    let cutoff = match GlobalVisHorizonKindForRel(rel)? {
        VISHORIZON_SHARED => horizons.shared_oldest_nonremovable,
        VISHORIZON_CATALOG => horizons.catalog_oldest_nonremovable,
        VISHORIZON_DATA => horizons.data_oldest_nonremovable,
        VISHORIZON_TEMP => horizons.temp_oldest_nonremovable,
        // just to prevent compiler warnings (C falls through to InvalidXid)
        _ => InvalidTransactionId,
    };
    Ok(cutoff)
}

/// `GetOldestTransactionIdConsideredRunning(void)` (procarray.c) — the oldest
/// xid any backend might still consider running (incl. VACUUM); used to decide
/// pg_subtrans truncation.
pub fn GetOldestTransactionIdConsideredRunning() -> PgResult<TransactionId> {
    let horizons = ComputeXidHorizons()?;
    Ok(horizons.oldest_considered_running)
}

/// `GlobalVisTestFor(Relation rel)` (procarray.c) — the appropriate
/// `GlobalVisState *` (handle) for `rel`, refreshing horizons if needed.
pub fn GlobalVisTestFor(rel: Oid) -> PgResult<GlobalVisStateHandle> {
    // XXX: C asserts a snapshot is pushed/registered (Assert(RecentXmin)).
    let kind = GlobalVisHorizonKindForRel(rel)?;
    let handle = handle_for_kind(kind);

    debug_assert!({
        let state = read_state(handle);
        state.definitely_needed.is_valid() && state.maybe_needed.is_valid()
    });

    Ok(handle)
}

/// `GlobalVisTestShouldUpdate(GlobalVisState *state)` (procarray.c, static) —
/// whether `state` is stale enough relative to the local horizon snapshot to
/// warrant a recompute.
pub fn GlobalVisTestShouldUpdate(state: GlobalVisStateHandle) -> bool {
    let last_xmin = COMPUTE_XID_HORIZONS_RESULT_LAST_XMIN.with(|c| *c.borrow());

    // hasn't been updated yet
    if !TransactionIdIsValid(last_xmin) {
        return true;
    }

    let s = read_state(state);

    // If the maybe_needed/definitely_needed boundaries are the same, it's
    // unlikely to be beneficial to refresh boundaries.
    if full_transaction_id_follows_or_equals(s.maybe_needed, s.definitely_needed) {
        return false;
    }

    // does the last snapshot built have a different xmin?
    recent_xmin() != last_xmin
}

/// `GlobalVisUpdate(void)` (procarray.c) — recompute the GlobalVis statics from
/// a fresh `ComputeXidHorizons`.
pub fn GlobalVisUpdate() -> PgResult<()> {
    // updates the horizons as a side-effect
    let _ = ComputeXidHorizons()?;
    Ok(())
}

/// `GlobalVisUpdateApply(ComputeXidHorizonsResult *horizons)` (procarray.c,
/// static) — push freshly-computed horizons into the four GlobalVis statics.
pub fn GlobalVisUpdateApply(horizons: &ComputeXidHorizonsResult) {
    GLOBAL_VIS_SHARED_RELS.with(|g| {
        g.borrow_mut().maybe_needed = FullXidRelativeTo(
            horizons.latest_completed,
            horizons.shared_oldest_nonremovable,
        );
    });
    GLOBAL_VIS_CATALOG_RELS.with(|g| {
        g.borrow_mut().maybe_needed = FullXidRelativeTo(
            horizons.latest_completed,
            horizons.catalog_oldest_nonremovable,
        );
    });
    GLOBAL_VIS_DATA_RELS.with(|g| {
        g.borrow_mut().maybe_needed =
            FullXidRelativeTo(horizons.latest_completed, horizons.data_oldest_nonremovable);
    });
    GLOBAL_VIS_TEMP_RELS.with(|g| {
        g.borrow_mut().maybe_needed =
            FullXidRelativeTo(horizons.latest_completed, horizons.temp_oldest_nonremovable);
    });

    // In longer running transactions it's possible that transactions we
    // previously needed to treat as running aren't around anymore. So update
    // definitely_needed to not be earlier than maybe_needed.
    GLOBAL_VIS_SHARED_RELS.with(|g| {
        let mut s = g.borrow_mut();
        s.definitely_needed = FullTransactionIdNewer(s.maybe_needed, s.definitely_needed);
    });
    GLOBAL_VIS_CATALOG_RELS.with(|g| {
        let mut s = g.borrow_mut();
        s.definitely_needed = FullTransactionIdNewer(s.maybe_needed, s.definitely_needed);
    });
    GLOBAL_VIS_DATA_RELS.with(|g| {
        let mut s = g.borrow_mut();
        s.definitely_needed = FullTransactionIdNewer(s.maybe_needed, s.definitely_needed);
    });
    GLOBAL_VIS_TEMP_RELS.with(|g| {
        let mut s = g.borrow_mut();
        s.definitely_needed = s.maybe_needed;
    });

    COMPUTE_XID_HORIZONS_RESULT_LAST_XMIN.with(|c| *c.borrow_mut() = recent_xmin());
}

/// `GlobalVisTestIsRemovableFullXid(GlobalVisState *state,
/// FullTransactionId fxid)` (procarray.c) — removability test (full-xid).
pub fn GlobalVisTestIsRemovableFullXid(
    state: GlobalVisStateHandle,
    fxid: FullTransactionId,
) -> bool {
    global_vis_test_is_removable_full_xid_impl(state, fxid)
        .expect("GlobalVisTestIsRemovableFullXid: ComputeXidHorizons")
}

/// Shared body for the full-xid removability test; `GlobalVisUpdate` can
/// `ereport(ERROR)` (carried on `Err`), so the infallible C-shaped seam variant
/// `.expect()`s it while the 32-bit `PgResult` seam propagates it.
fn global_vis_test_is_removable_full_xid_impl(
    state: GlobalVisStateHandle,
    fxid: FullTransactionId,
) -> PgResult<bool> {
    let s = read_state(state);

    // If fxid is older than maybe_needed bound, it definitely is visible to
    // everyone.
    if full_transaction_id_precedes(fxid, s.maybe_needed) {
        return Ok(true);
    }

    // If fxid is >= definitely_needed bound, it is very likely to still be
    // considered running.
    if full_transaction_id_follows_or_equals(fxid, s.definitely_needed) {
        return Ok(false);
    }

    // fxid is between maybe_needed and definitely_needed. If it makes sense,
    // update boundaries and recheck.
    if GlobalVisTestShouldUpdate(state) {
        GlobalVisUpdate()?;

        // Re-read after the update mutated the static.
        let s = read_state(state);
        debug_assert!(full_transaction_id_precedes(fxid, s.definitely_needed));

        Ok(full_transaction_id_precedes(fxid, s.maybe_needed))
    } else {
        Ok(false)
    }
}

/// `GlobalVisTestIsRemovableXid(GlobalVisState *state, TransactionId xid)`
/// (procarray.c) — removability test (32-bit xid; promoted via `FullXidRelativeTo`).
/// The installed seam is `PgResult<bool>` (the merged heapam-visibility /
/// pruneheap consumers call it with `?`); the C is infallible, so a filled body
/// returns `Ok(..)`.
pub fn GlobalVisTestIsRemovableXid(
    state: GlobalVisStateHandle,
    xid: TransactionId,
) -> PgResult<bool> {
    let s = read_state(state);

    // Convert 32 bit argument to FullTransactionId relative to
    // state->definitely_needed (which was based on values at snapshot build).
    let fxid = FullXidRelativeTo(s.definitely_needed, xid);

    global_vis_test_is_removable_full_xid_impl(state, fxid)
}

/// `GlobalVisCheckRemovableFullXid(Relation rel, FullTransactionId fxid)`
/// (procarray.c) — convenience wrapper around `GlobalVisTestFor` +
/// `GlobalVisTestIsRemovableFullXid`.
pub fn GlobalVisCheckRemovableFullXid(
    rel: Oid,
    fxid: FullTransactionId,
) -> PgResult<bool> {
    let state = GlobalVisTestFor(rel)?;
    Ok(GlobalVisTestIsRemovableFullXid(state, fxid))
}

/// `GlobalVisCheckRemovableXid(Relation rel, TransactionId xid)`
/// (procarray.c) — the 32-bit-xid variant of the checked removability test.
pub fn GlobalVisCheckRemovableXid(
    rel: Oid,
    xid: TransactionId,
) -> PgResult<bool> {
    let state = GlobalVisTestFor(rel)?;
    GlobalVisTestIsRemovableXid(state, xid)
}

/// Install the F3-owned inward seams: the NEW GlobalVis-resolution + removable
/// seams + the oldest-non-removable cutoff, consumed by vacuumlazy + heapam
/// visibility.
pub fn init_seams() {
    use backend_storage_ipc_procarray_seams as seams;

    seams::global_vis_test_for::set(GlobalVisTestFor);
    seams::global_vis_test_is_removable_xid::set(GlobalVisTestIsRemovableXid);
    // GIN's `GinPageIsRecyclable` calls `GlobalVisCheckRemovableXid(NULL, xid)`;
    // `heaprel == NULL` => resolve the shared (InvalidOid) horizon.
    seams::global_vis_check_removable_xid::set(|xid| GlobalVisCheckRemovableXid(InvalidOid, xid));
    seams::global_vis_test_is_removable_fullxid::set(GlobalVisTestIsRemovableFullXid);
    seams::get_oldest_non_removable_transaction_id::set(GetOldestNonRemovableTransactionId);

    // The consumers (heapam index_delete / pruneheap / vacuumlazy) call
    // `GlobalVisTestFor` through the `backend-access-heap-vacuumlazy-seams`
    // declaration (the seam was originally mis-homed there; the owner-homed
    // copy lives in procarray-seams). Install both so the actually-consumed
    // declaration resolves to this owner.
    backend_access_heap_vacuumlazy_seams::global_vis_test_for::set(|rel| GlobalVisTestFor(rel.rd_id));
}
