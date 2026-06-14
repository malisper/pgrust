//! PostgreSQL snapshot manager — port of PostgreSQL 18.3
//! `src/backend/utils/time/snapmgr.c`.
//!
//! This crate owns the per-backend snapshot bookkeeping: the three reusable
//! MVCC snapshots (`CurrentSnapshot`/`SecondarySnapshot`/`CatalogSnapshot`),
//! the `ActiveSnapshot` stack, the `RegisteredSnapshots` set (ordered, in C, by
//! `xmin`), the `FirstSnapshotSet`/`TransactionXmin`/`RecentXmin` globals,
//! snapshot copy/lifecycle, export/import (text file format), parallel-worker
//! serialization, and the historic (logical-decoding) snapshot. All of this is
//! per-backend state, modelled with a `thread_local!` cell (`state` module).
//!
//! `GetSnapshotData` lives in procarray, and the other genuine externals
//! (predicate.c's serializable hooks, the `MyProc->xmin` shared-memory write,
//! `ProcArrayInstall*Xmin`, `GetMaxSnapshot*Count`, and the fd.c file
//! primitives for the export/import transport) are reached through the owners'
//! seam crates, which panic until those owners land. The text *format* of
//! exported/imported snapshots, the path/rename control flow, and all the
//! refcount/stack arithmetic are implemented here.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use backend_access_transam_subtrans_seams as subtrans_seams;
use backend_access_transam_xact::{
    GetCurrentCommandId, GetCurrentTransactionNestLevel, GetTopTransactionIdIfAny, IsInParallelMode,
    IsSubTransaction, IsolationIsSerializable, IsolationUsesXactSnapshot, XactIsoLevel,
    XactReadOnly, xactGetCommittedChildren,
};
use backend_storage_file_fd_seams as fd_seams;
use backend_storage_ipc_procarray_seams as procarray_seams;
use backend_storage_lmgr_predicate_seams as predicate_seams;
use backend_storage_lmgr_proc_seams as proc_seams;
use backend_utils_cache_syscache::{RelationHasSysCache, RelationInvalidatesSnapshotsOnly};
use backend_utils_error::ereport;
use backend_utils_init_small_seams as misc_seams;
use types_core::{
    CommandId, FirstNormalTransactionId, InvalidOid, InvalidTransactionId, Oid, ProcNumber, Size,
    TransactionId, XACT_SERIALIZABLE,
};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_ACTIVE_SQL_TRANSACTION, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_UNDEFINED_OBJECT, ERROR, LOG, WARNING,
};
use types_snapshot::{SnapshotData, SnapshotType};
use types_storage::VirtualTransactionId;

mod state;

pub use state::SnapHandle;
pub use types_hash::hsearch::HTAB;
use state::{
    new_handle, new_snapshot_data, with_state, ActiveSnapshotElt, ExportedSnapshot, SnapMgrState,
};

#[cfg(test)]
mod tests;

/// `#define SNAPSHOT_EXPORT_DIR "pg_snapshots"` (snapmgr.c:202).
pub const SNAPSHOT_EXPORT_DIR: &str = "pg_snapshots";

/// `InvalidPid` (miscadmin.h:32) — `(-1)`.
const INVALID_PID: i32 = -1;

/// Error location helper, mirroring `__FILE__`/`__func__`.
fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/utils/time/snapmgr.c", 0, funcname)
}

/// `elog(ERROR, msg)` returning the never-`Ok` result type the call site needs.
fn elog_error<T>(msg: &'static str, funcname: &'static str) -> PgResult<T> {
    ereport(ERROR).errmsg_internal(msg).finish(loc(funcname))?;
    unreachable!("ereport(ERROR) returns Err")
}

/* ----------------------------------------------------------------------
 * Small transam.h / c.h predicates (file-owned macros).
 * ---------------------------------------------------------------------- */

/// `#define TransactionIdIsNormal(xid)  ((xid) >= FirstNormalTransactionId)`.
fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
}

/// `#define TransactionIdIsValid(xid)  ((xid) != InvalidTransactionId)`.
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `TransactionIdPrecedes(id1, id2)` (transam.c) — wraparound-aware "less than".
fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 < id2;
    }
    let diff = id1.wrapping_sub(id2) as i32;
    diff < 0
}

/// `TransactionIdFollowsOrEquals(id1, id2)` (transam.c).
fn TransactionIdFollowsOrEquals(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 >= id2;
    }
    let diff = id1.wrapping_sub(id2) as i32;
    diff >= 0
}

/// `OidIsValid(oid)` (c.h).
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// `pg_lfind32(value, base, nelem)` (port/pg_lfind.h) — linear membership test.
fn pg_lfind32(value: TransactionId, base: &[TransactionId]) -> bool {
    base.contains(&value)
}

/* ----------------------------------------------------------------------
 * RegisteredSnapshots set helpers.
 *
 * C orders RegisteredSnapshots in a pairing heap so it can quickly find the
 * member with the lowest xmin (to advance MyProc->xmin). The heap is otherwise
 * only mutated by add/remove, so an unordered `Vec<SnapHandle>` plus a
 * minimum-by-`xmin` scan (using the wraparound-aware `xmin_cmp`) reproduces it.
 * ---------------------------------------------------------------------- */

fn registered_add(s: &mut SnapMgrState, snap: SnapHandle) {
    s.registered.push(snap);
}

fn registered_remove(s: &mut SnapMgrState, snap: &SnapHandle) {
    if let Some(pos) = s
        .registered
        .iter()
        .position(|h| SnapHandle::ptr_eq(h, snap))
    {
        s.registered.swap_remove(pos);
    }
}

fn registered_is_empty(s: &SnapMgrState) -> bool {
    s.registered.is_empty()
}

fn registered_is_singular(s: &SnapMgrState) -> bool {
    s.registered.len() == 1
}

/// The minimum-`xmin` registered snapshot under the `xmin_cmp` ordering
/// (`pairingheap_first`: smallest xmin at the top). Returns its `xmin`.
fn registered_min_xmin(s: &SnapMgrState) -> Option<TransactionId> {
    let mut best: Option<TransactionId> = None;
    for h in &s.registered {
        let x = h.borrow().xmin;
        best = Some(match best {
            None => x,
            Some(cur) if TransactionIdPrecedes(x, cur) => x,
            Some(cur) => cur,
        });
    }
    best
}

/* ----------------------------------------------------------------------
 * Exported globals (per-backend)
 * ---------------------------------------------------------------------- */

/// `TransactionId TransactionXmin` (snapmgr.c:158).
pub fn TransactionXmin() -> TransactionId {
    with_state(|s| s.transaction_xmin)
}

/// `TransactionId RecentXmin` (snapmgr.c:159).
pub fn RecentXmin() -> TransactionId {
    with_state(|s| s.recent_xmin)
}

/// `bool FirstSnapshotSet` (snapmgr.c:192).
pub fn FirstSnapshotSet() -> bool {
    with_state(|s| s.first_snapshot_set)
}

/* ----------------------------------------------------------------------
 * GetSnapshotData glue
 *
 * GetSnapshotData itself lives in procarray (seamed). In C it also writes
 * MyProc->xmin / TransactionXmin / RecentXmin (procarray.c); the seam only
 * computes and returns the snapshot fields, so this glue replays those global
 * updates here (the MyProc->xmin write goes through the proc seam; the
 * GlobalVis maintenance stays inside procarray's seam implementation).
 * ---------------------------------------------------------------------- */

/// Which reusable snapshot struct a glue routine targets.
#[derive(Clone, Copy)]
enum StaticWhich {
    Current,
    Secondary,
    Catalog,
}

impl StaticWhich {
    fn handle(self, s: &SnapMgrState) -> SnapHandle {
        match self {
            StaticWhich::Current => s.current_data.clone(),
            StaticWhich::Secondary => s.secondary_data.clone(),
            StaticWhich::Catalog => s.catalog_data.clone(),
        }
    }
}

/// Fetch snapshot data via the procarray `get_snapshot_data` seam into the
/// reusable struct identified by `which`, replaying C's
/// `MyProc->xmin`/`TransactionXmin`/`RecentXmin` updates, and return that
/// struct's handle.
fn get_snapshot_data_into(which: StaticWhich) -> PgResult<SnapHandle> {
    let fetched = procarray_seams::get_snapshot_data::call()?;
    let xmin = fetched.xmin;

    let handle = with_state(|s| {
        let h = which.handle(s);
        {
            let mut d = h.borrow_mut();
            d.xmin = fetched.xmin;
            d.xmax = fetched.xmax;
            d.xcnt = fetched.xip.len() as u32;
            d.xip = fetched.xip;
            d.subxcnt = fetched.subxip.len() as i32;
            d.subxip = fetched.subxip;
            d.suboverflowed = fetched.suboverflowed;
            d.takenDuringRecovery = fetched.takenDuringRecovery;
            d.copied = false;
            d.curcid = 0;
            d.snapXactCompletionCount = fetched.snapXactCompletionCount;
        }
        h
    });

    // procarray.c: if (!TransactionIdIsValid(MyProc->xmin)) MyProc->xmin =
    // TransactionXmin = xmin;
    if !TransactionIdIsValid(proc_seams::my_proc_xmin::call()) {
        proc_seams::set_my_proc_xmin::call(xmin);
        with_state(|s| s.transaction_xmin = xmin);
    }
    // procarray.c: RecentXmin = xmin;
    with_state(|s| s.recent_xmin = xmin);

    Ok(handle)
}

/* ----------------------------------------------------------------------
 * Snapshot acquisition
 * ---------------------------------------------------------------------- */

/// `GetTransactionSnapshot` (snapmgr.c:270).
pub fn GetTransactionSnapshot() -> PgResult<SnapHandle> {
    // Return historic snapshot if doing logical decoding.
    if HistoricSnapshotActive() {
        debug_assert!(!FirstSnapshotSet());
        return Ok(with_state(|s| {
            s.historic.clone().expect("HistoricSnapshot != NULL")
        }));
    }

    // First call in transaction?
    if !FirstSnapshotSet() {
        // Don't allow catalog snapshot to be older than xact snapshot.
        InvalidateCatalogSnapshot()?;

        with_state(|s| {
            debug_assert!(registered_is_empty(s));
            debug_assert!(s.first_xact_snapshot.is_none());
        });

        if IsInParallelMode() {
            return elog_error(
                "cannot take query snapshot during a parallel operation",
                "GetTransactionSnapshot",
            );
        }

        // In transaction-snapshot mode, the first snapshot must live until end
        // of xact, so we copy it; in serializable mode, predicate.c wraps the
        // fetch.
        if IsolationUsesXactSnapshot() {
            let current = if IsolationIsSerializable() {
                let stat = get_snapshot_data_into(StaticWhich::Current)?;
                GetSerializableTransactionSnapshot(&stat)?;
                stat
            } else {
                get_snapshot_data_into(StaticWhich::Current)?
            };
            // Make a saved copy.
            let copy = CopySnapshot(&current);
            copy.borrow_mut().regd_count += 1;
            with_state(|s| {
                s.current = Some(copy.clone());
                s.first_xact_snapshot = Some(copy.clone());
                registered_add(s, copy.clone());
                s.first_snapshot_set = true;
            });
            return Ok(copy);
        } else {
            let current = get_snapshot_data_into(StaticWhich::Current)?;
            with_state(|s| {
                s.current = Some(current.clone());
                s.first_snapshot_set = true;
            });
            return Ok(current);
        }
    }

    if IsolationUsesXactSnapshot() {
        return Ok(with_state(|s| {
            s.current.clone().expect("CurrentSnapshot != NULL")
        }));
    }

    // Don't allow catalog snapshot to be older than xact snapshot.
    InvalidateCatalogSnapshot()?;

    let current = get_snapshot_data_into(StaticWhich::Current)?;
    with_state(|s| s.current = Some(current.clone()));
    Ok(current)
}

/// `GetLatestSnapshot` (snapmgr.c:352).
pub fn GetLatestSnapshot() -> PgResult<SnapHandle> {
    if IsInParallelMode() {
        return elog_error(
            "cannot update SecondarySnapshot during a parallel operation",
            "GetLatestSnapshot",
        );
    }

    debug_assert!(!HistoricSnapshotActive());

    // If first call in transaction, go ahead and set the xact snapshot.
    if !FirstSnapshotSet() {
        return GetTransactionSnapshot();
    }

    let secondary = get_snapshot_data_into(StaticWhich::Secondary)?;
    with_state(|s| s.secondary = Some(secondary.clone()));
    Ok(secondary)
}

/// `GetCatalogSnapshot` (snapmgr.c:383).
pub fn GetCatalogSnapshot(relid: Oid) -> PgResult<SnapHandle> {
    if HistoricSnapshotActive() {
        return Ok(with_state(|s| {
            s.historic.clone().expect("HistoricSnapshot != NULL")
        }));
    }
    GetNonHistoricCatalogSnapshot(relid)
}

/// `GetNonHistoricCatalogSnapshot` (snapmgr.c:405).
pub fn GetNonHistoricCatalogSnapshot(relid: Oid) -> PgResult<SnapHandle> {
    let have_catalog = with_state(|s| s.catalog.is_some());
    if have_catalog && !RelationInvalidatesSnapshotsOnly(relid) && !RelationHasSysCache(relid) {
        InvalidateCatalogSnapshot()?;
    }

    if with_state(|s| s.catalog.is_none()) {
        let catalog = get_snapshot_data_into(StaticWhich::Catalog)?;
        with_state(|s| {
            s.catalog = Some(catalog.clone());
            // Shove the CatalogSnapshot into the registered set manually so it
            // is accounted for in PGPROC->xmin decisions.
            registered_add(s, catalog.clone());
        });
    }

    Ok(with_state(|s| {
        s.catalog.clone().expect("CatalogSnapshot != NULL")
    }))
}

/* ----------------------------------------------------------------------
 * Catalog-snapshot invalidation
 * ---------------------------------------------------------------------- */

/// `InvalidateCatalogSnapshot` (snapmgr.c:453).
pub fn InvalidateCatalogSnapshot() -> PgResult<()> {
    let catalog = with_state(|s| s.catalog.clone());
    if let Some(catalog) = catalog {
        with_state(|s| {
            registered_remove(s, &catalog);
            s.catalog = None;
        });
        SnapshotResetXmin()?;
    }
    Ok(())
}

/// `InvalidateCatalogSnapshotConditionally` (snapmgr.c:474).
pub fn InvalidateCatalogSnapshotConditionally() -> PgResult<()> {
    let should =
        with_state(|s| s.catalog.is_some() && s.active.is_empty() && registered_is_singular(s));
    if should {
        InvalidateCatalogSnapshot()?;
    }
    Ok(())
}

/// `SnapshotSetCommandId` (snapmgr.c:487).
pub fn SnapshotSetCommandId(curcid: CommandId) {
    with_state(|s| {
        if !s.first_snapshot_set {
            return;
        }
        if let Some(current) = &s.current {
            current.borrow_mut().curcid = curcid;
        }
        if let Some(secondary) = &s.secondary {
            secondary.borrow_mut().curcid = curcid;
        }
        // Should we do the same with CatalogSnapshot? (C leaves this open.)
    });
}

/* ----------------------------------------------------------------------
 * Import: SetTransactionSnapshot
 * ---------------------------------------------------------------------- */

/// `SetTransactionSnapshot` (snapmgr.c:508) — set the transaction's snapshot
/// from an imported MVCC snapshot. Closely tied to the first-snapshot case of
/// `GetTransactionSnapshot`.
fn SetTransactionSnapshot(
    sourcesnap: &SnapshotData,
    sourcevxid: Option<&VirtualTransactionId>,
    sourcepid: i32,
    sourceproc: Option<ProcNumber>,
) -> PgResult<()> {
    debug_assert!(!FirstSnapshotSet());

    InvalidateCatalogSnapshot()?;

    with_state(|s| {
        debug_assert!(registered_is_empty(s));
        debug_assert!(s.first_xact_snapshot.is_none());
    });
    debug_assert!(!HistoricSnapshotActive());

    // Even though we are not going to use the snapshot it computes, we must
    // call GetSnapshotData, to be sure CurrentSnapshotData's XID arrays are
    // allocated and to update the state for GlobalVis*.
    let current = get_snapshot_data_into(StaticWhich::Current)?;

    // Now copy appropriate fields from the source snapshot.
    let xmin;
    {
        let mut cur = current.borrow_mut();
        cur.xmin = sourcesnap.xmin;
        cur.xmax = sourcesnap.xmax;
        debug_assert!(sourcesnap.xcnt <= procarray_seams::get_max_snapshot_xid_count::call() as u32);
        cur.xcnt = sourcesnap.xcnt;
        cur.xip = if sourcesnap.xcnt > 0 {
            sourcesnap.xip[..sourcesnap.xcnt as usize].to_vec()
        } else {
            Vec::new()
        };
        debug_assert!(sourcesnap.subxcnt <= procarray_seams::get_max_snapshot_subxid_count::call());
        cur.subxcnt = sourcesnap.subxcnt;
        cur.subxip = if sourcesnap.subxcnt > 0 {
            sourcesnap.subxip[..sourcesnap.subxcnt as usize].to_vec()
        } else {
            Vec::new()
        };
        cur.suboverflowed = sourcesnap.suboverflowed;
        cur.takenDuringRecovery = sourcesnap.takenDuringRecovery;
        // NB: curcid is NOT copied, it's a local matter.
        cur.snapXactCompletionCount = 0;
        xmin = cur.xmin;
    }

    // Fix up MyProc->xmin and TransactionXmin atomically (the source must still
    // be running). Let procarray.c do it.
    let installed = if let Some(procnum) = sourceproc {
        procarray_seams::proc_array_install_restored_xmin::call(xmin, procnum)?
    } else {
        let vxid = sourcevxid.expect("sourcevxid or sourceproc must be set");
        procarray_seams::proc_array_install_imported_xmin::call(xmin, *vxid)?
    };
    if !installed {
        if sourceproc.is_some() {
            ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("could not import the requested snapshot")
                .errdetail("The source transaction is not running anymore.")
                .finish(loc("SetTransactionSnapshot"))?;
        } else {
            ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("could not import the requested snapshot")
                .errdetail(format!(
                    "The source process with PID {sourcepid} is not running anymore."
                ))
                .finish(loc("SetTransactionSnapshot"))?;
        }
    }

    // In transaction-snapshot mode, the first snapshot must live until end of
    // xact, so make a copy.
    if IsolationUsesXactSnapshot() {
        if IsolationIsSerializable() {
            let vxid = sourcevxid
                .copied()
                .unwrap_or_else(VirtualTransactionId::invalid);
            let snap_copy = current.borrow().clone();
            predicate_seams::set_serializable_transaction_snapshot::call(snap_copy, vxid, sourcepid)?;
        }
        let copy = CopySnapshot(&current);
        copy.borrow_mut().regd_count += 1;
        with_state(|s| {
            s.current = Some(copy.clone());
            s.first_xact_snapshot = Some(copy.clone());
            registered_add(s, copy);
        });
    } else {
        with_state(|s| s.current = Some(current));
    }

    with_state(|s| s.first_snapshot_set = true);
    Ok(())
}

/* ----------------------------------------------------------------------
 * Copy / lifecycle
 * ---------------------------------------------------------------------- */

/// `CopySnapshot` (snapmgr.c:605).
pub fn CopySnapshot(snapshot: &SnapHandle) -> SnapHandle {
    let src = snapshot.borrow();

    let xip: Vec<TransactionId> = if src.xcnt > 0 {
        src.xip[..src.xcnt as usize].to_vec()
    } else {
        Vec::new()
    };

    // Don't bother to copy the subxid array if it had overflowed, unless taken
    // during recovery (top-level XIDs live in subxip then too).
    let subxip: Vec<TransactionId> =
        if src.subxcnt > 0 && (!src.suboverflowed || src.takenDuringRecovery) {
            src.subxip[..src.subxcnt as usize].to_vec()
        } else {
            Vec::new()
        };

    let mut newsnap: SnapshotData = src.clone();
    newsnap.xcnt = xip.len() as u32;
    newsnap.xip = xip;
    newsnap.subxcnt = subxip.len() as i32;
    newsnap.subxip = subxip;
    newsnap.regd_count = 0;
    newsnap.active_count = 0;
    newsnap.copied = true;
    newsnap.snapXactCompletionCount = 0;

    new_handle(newsnap)
}

/* ----------------------------------------------------------------------
 * Active snapshot stack
 * ---------------------------------------------------------------------- */

/// `PushActiveSnapshot` (snapmgr.c:679).
pub fn PushActiveSnapshot(snapshot: &SnapHandle) {
    PushActiveSnapshotWithLevel(snapshot, GetCurrentTransactionNestLevel());
}

/// `PushActiveSnapshotWithLevel` (snapmgr.c:693).
pub fn PushActiveSnapshotWithLevel(snapshot: &SnapHandle, snap_level: i32) {
    debug_assert!(with_state(|s| s
        .active
        .last()
        .map(|top| snap_level >= top.as_level)
        .unwrap_or(true)));

    let (current, secondary) = with_state(|s| (s.current.clone(), s.secondary.clone()));
    let is_current = current
        .as_ref()
        .is_some_and(|h| SnapHandle::ptr_eq(h, snapshot));
    let is_secondary = secondary
        .as_ref()
        .is_some_and(|h| SnapHandle::ptr_eq(h, snapshot));

    // Checking SecondarySnapshot is probably useless here, but be sure.
    let as_snap = if is_current || is_secondary || !snapshot.borrow().copied {
        CopySnapshot(snapshot)
    } else {
        snapshot.clone()
    };

    as_snap.borrow_mut().active_count += 1;

    with_state(|s| {
        s.active.push(ActiveSnapshotElt {
            as_snap,
            as_level: snap_level,
        });
    });
}

/// `PushCopiedSnapshot` (snapmgr.c:729).
pub fn PushCopiedSnapshot(snapshot: &SnapHandle) {
    PushActiveSnapshot(&CopySnapshot(snapshot));
}

/// `UpdateActiveSnapshotCommandId` (snapmgr.c:741).
pub fn UpdateActiveSnapshotCommandId() -> PgResult<()> {
    let top = with_state(|s| {
        s.active
            .last()
            .expect("ActiveSnapshot != NULL")
            .as_snap
            .clone()
    });
    debug_assert_eq!(top.borrow().active_count, 1);
    debug_assert_eq!(top.borrow().regd_count, 0);

    // Don't allow modification of the active snapshot during parallel operation.
    let save_curcid = top.borrow().curcid;
    let curcid = GetCurrentCommandId(false)?;
    if IsInParallelMode() && save_curcid != curcid {
        return elog_error(
            "cannot modify commandid in active snapshot during a parallel operation",
            "UpdateActiveSnapshotCommandId",
        );
    }
    top.borrow_mut().curcid = curcid;
    Ok(())
}

/// `PopActiveSnapshot` (snapmgr.c:772).
pub fn PopActiveSnapshot() -> PgResult<()> {
    let popped = with_state(|s| s.active.pop().expect("ActiveSnapshot != NULL").as_snap);

    debug_assert!(popped.borrow().active_count > 0);
    popped.borrow_mut().active_count -= 1;

    // FreeSnapshot (snapmgr.c:661): when both counts reach 0 the snapshot is no
    // longer referenced from anywhere we track, so dropping the last `Rc` here
    // reclaims it (C's pfree). The asserts mirror C's.
    {
        let sd = popped.borrow();
        if sd.active_count == 0 && sd.regd_count == 0 {
            debug_assert!(sd.copied);
        }
    }
    drop(popped);

    SnapshotResetXmin()
}

/// `InitDirtySnapshot(snapshotData)` (snapmgr.h macro) — a fresh `SnapshotData`
/// of type `SNAPSHOT_DIRTY` with all other fields zeroed, minted as an owned
/// handle.
pub fn InitDirtySnapshot() -> SnapHandle {
    new_handle(new_snapshot_data(SnapshotType::SNAPSHOT_DIRTY))
}

/// `GetActiveSnapshot` (snapmgr.c:797).
pub fn GetActiveSnapshot() -> SnapHandle {
    with_state(|s| {
        s.active
            .last()
            .expect("ActiveSnapshot != NULL")
            .as_snap
            .clone()
    })
}

/// `ActiveSnapshotSet` (snapmgr.c:809).
pub fn ActiveSnapshotSet() -> bool {
    with_state(|s| !s.active.is_empty())
}

/* ----------------------------------------------------------------------
 * Registered snapshots
 * ---------------------------------------------------------------------- */

/// `RegisterSnapshot` (snapmgr.c:821). The resource-owner bookkeeping
/// (`ResourceOwnerRemember`) sits above snapmgr per the RAII lifecycle model;
/// this owns the refcount + set membership (the `NoOwner` core). `None`
/// registers nothing.
pub fn RegisterSnapshot(snapshot: Option<&SnapHandle>) -> Option<SnapHandle> {
    let snapshot = snapshot?;
    Some(RegisterSnapshotOnOwner(snapshot))
}

/// `RegisterSnapshotOnOwner` (snapmgr.c:834).
pub fn RegisterSnapshotOnOwner(snapshot: &SnapHandle) -> SnapHandle {
    // Static snapshot? Create a persistent copy.
    let snap_ = if snapshot.borrow().copied {
        snapshot.clone()
    } else {
        CopySnapshot(snapshot)
    };

    snap_.borrow_mut().regd_count += 1;

    if snap_.borrow().regd_count == 1 {
        with_state(|s| registered_add(s, snap_.clone()));
    }

    snap_
}

/// `UnregisterSnapshot` (snapmgr.c:863). `None` unregisters nothing.
pub fn UnregisterSnapshot(snapshot: Option<&SnapHandle>) -> PgResult<()> {
    let Some(snapshot) = snapshot else {
        return Ok(());
    };
    UnregisterSnapshotFromOwner(snapshot)
}

/// `UnregisterSnapshotFromOwner` (snapmgr.c:876). The resowner `Forget` is the
/// caller's responsibility (RAII lifecycle); this is the `NoOwner` core.
pub fn UnregisterSnapshotFromOwner(snapshot: &SnapHandle) -> PgResult<()> {
    UnregisterSnapshotNoOwner(snapshot)
}

/// `UnregisterSnapshotNoOwner` (snapmgr.c:886) — also the
/// `ResOwnerReleaseSnapshot` resource-owner callback target.
pub fn UnregisterSnapshotNoOwner(snapshot: &SnapHandle) -> PgResult<()> {
    debug_assert!(snapshot.borrow().regd_count > 0);
    debug_assert!(with_state(|s| !registered_is_empty(s)));

    snapshot.borrow_mut().regd_count -= 1;
    if snapshot.borrow().regd_count == 0 {
        with_state(|s| registered_remove(s, snapshot));
    }

    let (regd, active) = {
        let sd = snapshot.borrow();
        (sd.regd_count, sd.active_count)
    };
    if regd == 0 && active == 0 {
        // FreeSnapshot: dropping the manager's last tracked reference reclaims
        // it (the caller's handle drops at its own scope end).
        SnapshotResetXmin()?;
    }
    Ok(())
}

/* ----------------------------------------------------------------------
 * Xmin tracking
 * ---------------------------------------------------------------------- */

/// `SnapshotResetXmin` (snapmgr.c:934). The computation is in-crate; the
/// `MyProc->xmin` write goes through the proc seam (a shared-memory field).
pub fn SnapshotResetXmin() -> PgResult<()> {
    if with_state(|s| !s.active.is_empty()) {
        return Ok(());
    }

    if with_state(|s| registered_is_empty(s)) {
        proc_seams::set_my_proc_xmin::call(InvalidTransactionId);
        with_state(|s| s.transaction_xmin = InvalidTransactionId);
        return Ok(());
    }

    let min_xmin = with_state(|s| registered_min_xmin(s)).expect("non-empty registered set");

    if TransactionIdPrecedes(proc_seams::my_proc_xmin::call(), min_xmin) {
        proc_seams::set_my_proc_xmin::call(min_xmin);
        with_state(|s| s.transaction_xmin = min_xmin);
    }
    Ok(())
}

/* ----------------------------------------------------------------------
 * Subtransaction handling
 * ---------------------------------------------------------------------- */

/// `AtSubCommit_Snapshot` (snapmgr.c:958).
pub fn AtSubCommit_Snapshot(level: i32) {
    with_state(|s| {
        // C walks from the top downward and stops at the first element whose
        // level is below `level`; the top is the last `Vec` element.
        for elt in s.active.iter_mut().rev() {
            if elt.as_level < level {
                break;
            }
            elt.as_level = level - 1;
        }
    });
}

/// `AtSubAbort_Snapshot` (snapmgr.c:979).
pub fn AtSubAbort_Snapshot(level: i32) -> PgResult<()> {
    loop {
        let pop = with_state(|s| matches!(s.active.last(), Some(top) if top.as_level >= level));
        if !pop {
            break;
        }

        let snapshot = with_state(|s| s.active.pop().unwrap().as_snap);

        // Decrement the snapshot's active count. If it's still registered or
        // marked active by an outer subtransaction, we can't free it yet.
        debug_assert!(snapshot.borrow().active_count >= 1);
        snapshot.borrow_mut().active_count -= 1;

        // FreeSnapshot when both counts hit 0: dropping the last reference here.
        drop(snapshot);
    }

    SnapshotResetXmin()
}

/* ----------------------------------------------------------------------
 * End of transaction
 * ---------------------------------------------------------------------- */

/// `AtEOXact_Snapshot` (snapmgr.c:1013).
pub fn AtEOXact_Snapshot(is_commit: bool, reset_xmin: bool) -> PgResult<()> {
    // Release our privately-managed reference to the transaction snapshot. We
    // must remove it from RegisteredSnapshots; we don't free it explicitly (the
    // memory goes away with TopTransactionContext, and a stacked-as-active copy
    // would be a dangling pointer in C).
    let first_xact = with_state(|s| s.first_xact_snapshot.clone());
    if let Some(first_xact) = &first_xact {
        debug_assert!(first_xact.borrow().regd_count > 0);
        debug_assert!(with_state(|s| !registered_is_empty(s)));
        with_state(|s| registered_remove(s, first_xact));
    }
    with_state(|s| s.first_xact_snapshot = None);

    // If we exported any snapshots, clean them up.
    let exported = with_state(|s| core::mem::take(&mut s.exported_snapshots));
    if !exported.is_empty() {
        for esnap in &exported {
            // Unlink failure is only a WARNING.
            if fd_seams::unlink_file::call(&esnap.snapfile) != 0 {
                ereport(WARNING)
                    .errcode_for_file_access()
                    .errmsg(format!("could not unlink file \"{}\"", esnap.snapfile))
                    .finish(loc("AtEOXact_Snapshot"))?;
            }
            // Remove the snapshot from RegisteredSnapshots to prevent a warning.
            with_state(|s| registered_remove(s, &esnap.snapshot));
        }
    }

    // Drop catalog snapshot if any.
    InvalidateCatalogSnapshot()?;

    // On commit, complain about leftover snapshots.
    if is_commit {
        if with_state(|s| !registered_is_empty(s)) {
            ereport(WARNING)
                .errmsg_internal("registered snapshots seem to remain after cleanup")
                .finish(loc("AtEOXact_Snapshot"))?;
        }
        // Complain about unpopped active snapshots. C logs `elog(WARNING,
        // "snapshot %p still active", active)` for each leftover; use the live
        // handle's address as the %p-equivalent.
        let active_addrs: Vec<usize> = with_state(|s| {
            s.active
                .iter()
                .map(|elt| SnapHandle::as_ptr(&elt.as_snap) as usize)
                .collect()
        });
        for addr in active_addrs {
            ereport(WARNING)
                .errmsg_internal(format!("snapshot {addr:#x} still active"))
                .finish(loc("AtEOXact_Snapshot"))?;
        }
    }

    // Reset our state. We don't free the memory explicitly -- the `Rc`s drop.
    with_state(|s| {
        s.active.clear();
        s.registered.clear();
        s.current = None;
        s.secondary = None;
        s.first_snapshot_set = false;
    });

    // During normal commit processing, ProcArrayEndTransaction() already reset
    // MyProc->xmin before this is called, so we need not touch xmin then.
    if reset_xmin {
        SnapshotResetXmin()?;
    }

    debug_assert!(reset_xmin || proc_seams::my_proc_xmin::call() == 0);
    Ok(())
}

/* ----------------------------------------------------------------------
 * Export
 * ---------------------------------------------------------------------- */

/// `ExportSnapshot` (snapmgr.c:1112) — export the snapshot to a file. Returns
/// the token (the basename of the file) usable with `SET TRANSACTION SNAPSHOT`.
pub fn ExportSnapshot(snapshot: &SnapHandle) -> PgResult<String> {
    // Get our transaction ID if there is one, to include in the snapshot.
    let top_xid = GetTopTransactionIdIfAny();

    // We cannot export a snapshot from a subtransaction.
    if IsSubTransaction() {
        ereport(ERROR)
            .errcode(ERRCODE_ACTIVE_SQL_TRANSACTION)
            .errmsg("cannot export a snapshot from a subtransaction")
            .finish(loc("ExportSnapshot"))?;
    }

    // Previously committed subtransactions must be seen as still running.
    let children = xactGetCommittedChildren()?;
    let nchildren = children.len();

    let vxid = proc_seams::my_proc_vxid::call();

    // Generate file path. Numbering starts at 1 within the transaction.
    let path = format!(
        "{SNAPSHOT_EXPORT_DIR}/{:08X}-{:08X}-{}",
        vxid.procNumber,
        vxid.localTransactionId,
        with_state(|s| s.exported_snapshots.len()) + 1
    );

    // Copy the snapshot, add it to exportedSnapshots, mark it pseudo-registered.
    let snapshot = CopySnapshot(snapshot);
    snapshot.borrow_mut().regd_count += 1;
    with_state(|s| {
        s.exported_snapshots.push(ExportedSnapshot {
            snapfile: path.clone(),
            snapshot: snapshot.clone(),
        });
        registered_add(s, snapshot.clone());
    });

    // Build the text serialization. The format expected by ImportSnapshot is
    // rigid: each line must be fieldname:value.
    let sd = snapshot.borrow();
    let mut buf = String::new();

    buf.push_str(&format!(
        "vxid:{}/{}\n",
        vxid.procNumber, vxid.localTransactionId
    ));
    buf.push_str(&format!("pid:{}\n", misc_seams::my_proc_pid::call()));
    buf.push_str(&format!("dbid:{}\n", misc_seams::my_database_id::call()));
    buf.push_str(&format!("iso:{}\n", XactIsoLevel()));
    buf.push_str(&format!("ro:{}\n", XactReadOnly() as i32));

    buf.push_str(&format!("xmin:{}\n", sd.xmin));
    buf.push_str(&format!("xmax:{}\n", sd.xmax));

    // Include our own top transaction ID in the top-xid data (GetSnapshotData
    // never includes our own XID), unless it's after xmax.
    let add_top_xid = TransactionIdIsValid(top_xid) && TransactionIdPrecedes(top_xid, sd.xmax);
    buf.push_str(&format!("xcnt:{}\n", sd.xcnt + add_top_xid as u32));
    for &x in &sd.xip[..sd.xcnt as usize] {
        buf.push_str(&format!("xip:{x}\n"));
    }
    if add_top_xid {
        buf.push_str(&format!("xip:{top_xid}\n"));
    }

    // Add our subcommitted child XIDs to the subxid data, coping with overflow.
    if sd.suboverflowed
        || sd.subxcnt as usize + nchildren
            > procarray_seams::get_max_snapshot_subxid_count::call() as usize
    {
        buf.push_str("sof:1\n");
    } else {
        buf.push_str("sof:0\n");
        buf.push_str(&format!("sxcnt:{}\n", sd.subxcnt as usize + nchildren));
        for &x in &sd.subxip[..sd.subxcnt as usize] {
            buf.push_str(&format!("sxp:{x}\n"));
        }
        for &x in &children {
            buf.push_str(&format!("sxp:{x}\n"));
        }
    }
    buf.push_str(&format!("rec:{}\n", sd.takenDuringRecovery as u32));
    drop(sd);

    // Write to a ".tmp" file, then rename to the final filename, so no other
    // backend can read an incomplete file.
    let pathtmp = format!("{path}.tmp");
    fd_seams::allocate_file_write::call(&pathtmp, buf.as_bytes())?;
    if fd_seams::rename_file::call(&pathtmp, &path) < 0 {
        ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!(
                "could not rename file \"{pathtmp}\" to \"{path}\""
            ))
            .finish(loc("ExportSnapshot"))?;
    }

    // The basename (after "pg_snapshots/") is what pg_export_snapshot() returns.
    Ok(path[SNAPSHOT_EXPORT_DIR.len() + 1..].to_string())
}

/// `pg_export_snapshot` (snapmgr.c:1289) — SQL-callable wrapper; returns the
/// token text. (The `PG_FUNCTION_ARGS`/text-Datum wrapping is the systemic fmgr
/// deferral; this returns the `String` token.)
pub fn pg_export_snapshot() -> PgResult<String> {
    ExportSnapshot(&GetActiveSnapshot())
}

/// `DeleteAllExportedSnapshotFiles` (snapmgr.c:1584) — clean up files left by a
/// crashed backend. Called during startup/crash recovery.
pub fn DeleteAllExportedSnapshotFiles() -> PgResult<()> {
    // fd.c reports directory-read problems at LOG and skips them; the names it
    // returns exclude `.`/`..`.
    let names = fd_seams::read_dir_names_logged::call(SNAPSHOT_EXPORT_DIR);
    for name in &names {
        let buf = format!("{SNAPSHOT_EXPORT_DIR}/{name}");
        if fd_seams::unlink_file::call(&buf) != 0 {
            ereport(LOG)
                .errcode_for_file_access()
                .errmsg(format!("could not remove file \"{buf}\""))
                .finish(loc("DeleteAllExportedSnapshotFiles"))?;
        }
    }
    Ok(())
}

/* ----------------------------------------------------------------------
 * Import: text parsing
 * ---------------------------------------------------------------------- */

/// Cursor over the import file, advanced line-by-line by the parse subroutines.
struct ImportCursor<'a> {
    rest: &'a str,
    filename: &'a str,
}

/// Common helper: verify the prefix, return the value substring of the current
/// line, and advance past the newline. Mirrors the strncmp/strchr('\n') logic
/// shared by parseIntFromText/parseXidFromText/parseVxidFromText.
fn parse_line<'a>(cur: &mut ImportCursor<'a>, prefix: &str) -> PgResult<&'a str> {
    if !cur.rest.starts_with(prefix) {
        return invalid_snapshot_data(cur.filename);
    }
    let after_prefix = &cur.rest[prefix.len()..];
    let nl = match after_prefix.find('\n') {
        Some(i) => i,
        None => return invalid_snapshot_data(cur.filename),
    };
    let value = &after_prefix[..nl];
    cur.rest = &after_prefix[nl + 1..];
    Ok(value)
}

fn invalid_snapshot_data<T>(filename: &str) -> PgResult<T> {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
        .errmsg(format!("invalid snapshot data in file \"{filename}\""))
        .finish(loc("ImportSnapshot"))?;
    unreachable!("ereport(ERROR) returns Err")
}

/// `parseIntFromText` (snapmgr.c:1304).
fn parseIntFromText(prefix: &str, cur: &mut ImportCursor) -> PgResult<i32> {
    let value = parse_line(cur, prefix)?;
    match scan_leading_i32(value) {
        Some(v) => Ok(v),
        None => invalid_snapshot_data(cur.filename),
    }
}

/// `parseXidFromText` (snapmgr.c:1329).
fn parseXidFromText(prefix: &str, cur: &mut ImportCursor) -> PgResult<TransactionId> {
    let value = parse_line(cur, prefix)?;
    match scan_leading_u32(value) {
        Some(v) => Ok(v),
        None => invalid_snapshot_data(cur.filename),
    }
}

/// `parseVxidFromText` (snapmgr.c:1354) — parse `prefix` + "%d/%u".
fn parseVxidFromText(
    prefix: &str,
    cur: &mut ImportCursor,
    vxid: &mut VirtualTransactionId,
) -> PgResult<()> {
    let value = parse_line(cur, prefix)?;
    let mut parts = value.splitn(2, '/');
    let pn = parts.next().and_then(scan_leading_i32);
    let lx = parts.next().and_then(scan_leading_u32);
    match (pn, lx) {
        (Some(pn), Some(lx)) => {
            vxid.procNumber = pn;
            vxid.localTransactionId = lx;
            Ok(())
        }
        _ => invalid_snapshot_data(cur.filename),
    }
}

/// `sscanf(s, "%d", &val)` semantics: skip leading whitespace, read an optional
/// sign and decimal digits, ignore the rest. Returns `None` if no digits match.
fn scan_leading_i32(s: &str) -> Option<i32> {
    let s = s.trim_start();
    let (neg, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let end = digits
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(digits.len());
    if end == 0 {
        return None;
    }
    let n: i64 = digits[..end].parse().ok()?;
    let n = if neg { -n } else { n };
    Some(n as i32)
}

/// `sscanf(s, "%u", &val)` semantics for an unsigned 32-bit value.
fn scan_leading_u32(s: &str) -> Option<u32> {
    let s = s.trim_start();
    let s = s.strip_prefix('+').unwrap_or(s);
    let end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    if end == 0 {
        return None;
    }
    let n: u64 = s[..end].parse().ok()?;
    Some(n as u32)
}

/// `ImportSnapshot` (snapmgr.c:1384) — import a previously exported snapshot.
/// `idstr` is a filename in `pg_snapshots`. Called by `SET TRANSACTION SNAPSHOT`.
pub fn ImportSnapshot(idstr: &str) -> PgResult<()> {
    // Must be at top level of a fresh transaction with no XID.
    if FirstSnapshotSet()
        || GetTopTransactionIdIfAny() != InvalidTransactionId
        || IsSubTransaction()
    {
        ereport(ERROR)
            .errcode(ERRCODE_ACTIVE_SQL_TRANSACTION)
            .errmsg("SET TRANSACTION SNAPSHOT must be called before any query")
            .finish(loc("ImportSnapshot"))?;
    }

    // In read committed mode the next query gets a new snapshot anyway.
    if !IsolationUsesXactSnapshot() {
        ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(
                "a snapshot-importing transaction must have isolation level SERIALIZABLE or REPEATABLE READ",
            )
            .finish(loc("ImportSnapshot"))?;
    }

    // Verify the identifier: only 0-9, A-F and hyphens are allowed (prevents
    // reading arbitrary files). An empty idstr is NOT special-cased here
    // (strspn("", ...) == strlen("")), so it passes and fails later on open.
    if !idstr
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'A'..=b'F').contains(&b) || b == b'-')
    {
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("invalid snapshot identifier: \"{idstr}\""))
            .finish(loc("ImportSnapshot"))?;
    }

    let path = format!("{SNAPSHOT_EXPORT_DIR}/{idstr}");

    // Read the file. ENOENT becomes "snapshot does not exist".
    let filebuf = match fd_seams::allocate_file_read::call(&path)? {
        Some(bytes) => bytes,
        None => {
            ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("snapshot \"{idstr}\" does not exist"))
                .finish(loc("ImportSnapshot"))?;
            unreachable!("ereport(ERROR) returns Err")
        }
    };
    // The file is text; parse over its UTF-8 view (exported files are ASCII).
    let filebuf = String::from_utf8_lossy(&filebuf);

    // Construct a snapshot struct by parsing the file content.
    let mut cur = ImportCursor {
        rest: &filebuf,
        filename: &path,
    };

    let mut src_vxid = VirtualTransactionId::invalid();
    parseVxidFromText("vxid:", &mut cur, &mut src_vxid)?;
    let src_pid = parseIntFromText("pid:", &mut cur)?;
    // We abuse parseXidFromText a bit here for the dbid.
    let src_dbid: Oid = parseXidFromText("dbid:", &mut cur)?;
    let src_isolevel = parseIntFromText("iso:", &mut cur)?;
    let src_readonly = parseIntFromText("ro:", &mut cur)? != 0;

    let xmin = parseXidFromText("xmin:", &mut cur)?;
    let xmax = parseXidFromText("xmax:", &mut cur)?;

    let xcnt = parseIntFromText("xcnt:", &mut cur)?;
    // Sanity-check the xid count before allocating.
    if xcnt < 0 || xcnt > procarray_seams::get_max_snapshot_xid_count::call() {
        return invalid_snapshot_data(&path);
    }
    let mut xip = Vec::with_capacity(xcnt as usize);
    for _ in 0..xcnt {
        xip.push(parseXidFromText("xip:", &mut cur)?);
    }

    let suboverflowed = parseIntFromText("sof:", &mut cur)? != 0;

    let subxip = if !suboverflowed {
        let subxcnt = parseIntFromText("sxcnt:", &mut cur)?;
        if subxcnt < 0 || subxcnt > procarray_seams::get_max_snapshot_subxid_count::call() {
            return invalid_snapshot_data(&path);
        }
        let mut v = Vec::with_capacity(subxcnt as usize);
        for _ in 0..subxcnt {
            v.push(parseXidFromText("sxp:", &mut cur)?);
        }
        v
    } else {
        Vec::new()
    };

    let taken_during_recovery = parseIntFromText("rec:", &mut cur)? != 0;

    // Additional sanity checking.
    if !src_vxid.is_valid()
        || !OidIsValid(src_dbid)
        || !TransactionIdIsNormal(xmin)
        || !TransactionIdIsNormal(xmax)
    {
        return invalid_snapshot_data(&path);
    }

    // Serializable-source restrictions.
    if IsolationIsSerializable() {
        if src_isolevel != XACT_SERIALIZABLE {
            ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(
                    "a serializable transaction cannot import a snapshot from a non-serializable transaction",
                )
                .finish(loc("ImportSnapshot"))?;
        }
        if src_readonly && !XactReadOnly() {
            ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(
                    "a non-read-only serializable transaction cannot import a snapshot from a read-only transaction",
                )
                .finish(loc("ImportSnapshot"))?;
        }
    }

    // Cannot import a snapshot from a different database.
    if src_dbid != misc_seams::my_database_id::call() {
        ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot import a snapshot from a different database")
            .finish(loc("ImportSnapshot"))?;
    }

    // Assemble a SnapshotData with the parsed arrays and install it.
    let mut snapshot = new_snapshot_data(SnapshotType::SNAPSHOT_MVCC);
    snapshot.xmin = xmin;
    snapshot.xmax = xmax;
    snapshot.xcnt = xip.len() as u32;
    snapshot.xip = xip;
    snapshot.subxcnt = subxip.len() as i32;
    snapshot.subxip = subxip;
    snapshot.suboverflowed = suboverflowed;
    snapshot.takenDuringRecovery = taken_during_recovery;

    // OK, install the snapshot (sourceproc = None -> use the vxid path).
    SetTransactionSnapshot(&snapshot, Some(&src_vxid), src_pid, None)
}

/// `RestoreTransactionSnapshot` (snapmgr.c:1853).
pub fn RestoreTransactionSnapshot(snapshot: &SnapHandle, source_pgproc: ProcNumber) -> PgResult<()> {
    let sd = snapshot.borrow().clone();
    SetTransactionSnapshot(&sd, None, INVALID_PID, Some(source_pgproc))
}

/* ----------------------------------------------------------------------
 * Registered-snapshot queries / historic snapshots
 * ---------------------------------------------------------------------- */

/// `XactHasExportedSnapshots` (snapmgr.c:1571).
pub fn XactHasExportedSnapshots() -> bool {
    with_state(|s| !s.exported_snapshots.is_empty())
}

/// `ThereAreNoPriorRegisteredSnapshots` (snapmgr.c:1623).
pub fn ThereAreNoPriorRegisteredSnapshots() -> bool {
    with_state(|s| registered_is_empty(s) || registered_is_singular(s))
}

/// `HaveRegisteredOrActiveSnapshot` (snapmgr.c:1641).
pub fn HaveRegisteredOrActiveSnapshot() -> bool {
    with_state(|s| {
        if !s.active.is_empty() {
            return true;
        }
        // The catalog snapshot is in RegisteredSnapshots when valid, but can be
        // removed at any time. If only the catalog snapshot is registered, it
        // doesn't count as a "registered or active" snapshot.
        if s.catalog.is_some() && registered_is_singular(s) {
            return false;
        }
        !registered_is_empty(s)
    })
}

/// `SetupHistoricSnapshot` (snapmgr.c:1666).
pub fn SetupHistoricSnapshot(historic_snapshot: SnapHandle, tuplecids: *mut HTAB) {
    with_state(|s| {
        s.historic = Some(historic_snapshot);
        s.tuplecid_data = tuplecids;
    });
}

/// `TeardownHistoricSnapshot` (snapmgr.c:1682).
pub fn TeardownHistoricSnapshot(_is_error: bool) {
    with_state(|s| {
        s.historic = None;
        s.tuplecid_data = core::ptr::null_mut();
    });
}

/// `HistoricSnapshotActive` (snapmgr.c:1689).
pub fn HistoricSnapshotActive() -> bool {
    with_state(|s| s.historic.is_some())
}

/// `HistoricSnapshotGetTupleCids` (snapmgr.c:1695).
pub fn HistoricSnapshotGetTupleCids() -> *mut HTAB {
    debug_assert!(HistoricSnapshotActive());
    with_state(|s| s.tuplecid_data)
}

/* ----------------------------------------------------------------------
 * Parallel-worker snapshot transfer
 *
 * C's `SerializedSnapshotData` is a 7-field struct; with x86-64 layout it is
 * 24 bytes (xmin,xmax,xcnt: 4 each; subxcnt: 4; suboverflowed,recovery: 1 each
 * + 2 pad; curcid: 4). The byte layout is reproduced exactly (little-endian
 * scalars, same field order, the 2-byte tail padding before `curcid`) so a
 * serialize/restore round-trip across the shared-memory segment is
 * byte-faithful with the C struct copy.
 * ---------------------------------------------------------------------- */

const SERIALIZED_HEADER_LEN: usize = 24;

/// `EstimateSnapshotSpace` (snapmgr.c:1709).
pub fn EstimateSnapshotSpace(snapshot: &SnapHandle) -> Size {
    let s = snapshot.borrow();
    debug_assert_eq!(s.snapshot_type, SnapshotType::SNAPSHOT_MVCC);

    let mut size =
        SERIALIZED_HEADER_LEN + s.xcnt as usize * core::mem::size_of::<TransactionId>();
    if s.subxcnt > 0 && (!s.suboverflowed || s.takenDuringRecovery) {
        size += s.subxcnt as usize * core::mem::size_of::<TransactionId>();
    }
    size
}

/// `SerializeSnapshot` (snapmgr.c:1733).
pub fn SerializeSnapshot(snapshot: &SnapHandle) -> Vec<u8> {
    let s = snapshot.borrow();
    debug_assert!(s.subxcnt >= 0);

    // Ignore the SubXID array if it overflowed, unless taken during recovery.
    let subxcnt = if s.suboverflowed && !s.takenDuringRecovery {
        0
    } else {
        s.subxcnt
    };

    let mut out = Vec::with_capacity(
        SERIALIZED_HEADER_LEN
            + (s.xcnt as usize + subxcnt.max(0) as usize) * core::mem::size_of::<TransactionId>(),
    );
    // Fixed header matching the C `SerializedSnapshotData` byte layout.
    out.extend_from_slice(&s.xmin.to_le_bytes());
    out.extend_from_slice(&s.xmax.to_le_bytes());
    out.extend_from_slice(&s.xcnt.to_le_bytes());
    out.extend_from_slice(&subxcnt.to_le_bytes());
    out.push(s.suboverflowed as u8);
    out.push(s.takenDuringRecovery as u8);
    out.push(0); // tail padding to align curcid
    out.push(0);
    out.extend_from_slice(&s.curcid.to_le_bytes());
    debug_assert_eq!(out.len(), SERIALIZED_HEADER_LEN);

    // Copy XID array.
    for &x in &s.xip[..s.xcnt as usize] {
        out.extend_from_slice(&x.to_le_bytes());
    }
    // Copy SubXID array (only the possibly-truncated count).
    for &x in &s.subxip[..subxcnt.max(0) as usize] {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

/// `RestoreSnapshot` (snapmgr.c:1790).
pub fn RestoreSnapshot(start_address: &[u8]) -> SnapHandle {
    let xmin = u32::from_le_bytes(start_address[0..4].try_into().unwrap());
    let xmax = u32::from_le_bytes(start_address[4..8].try_into().unwrap());
    let xcnt = u32::from_le_bytes(start_address[8..12].try_into().unwrap());
    let subxcnt = i32::from_le_bytes(start_address[12..16].try_into().unwrap());
    let suboverflowed = start_address[16] != 0;
    let taken_during_recovery = start_address[17] != 0;
    // bytes 18,19 are padding
    let curcid = u32::from_le_bytes(start_address[20..24].try_into().unwrap());

    let mut off = SERIALIZED_HEADER_LEN;
    let mut xip = Vec::with_capacity(xcnt as usize);
    for _ in 0..xcnt {
        xip.push(u32::from_le_bytes(
            start_address[off..off + 4].try_into().unwrap(),
        ));
        off += 4;
    }
    let mut subxip = Vec::with_capacity(subxcnt.max(0) as usize);
    for _ in 0..subxcnt.max(0) {
        subxip.push(u32::from_le_bytes(
            start_address[off..off + 4].try_into().unwrap(),
        ));
        off += 4;
    }

    let mut snapshot = new_snapshot_data(SnapshotType::SNAPSHOT_MVCC);
    snapshot.xmin = xmin;
    snapshot.xmax = xmax;
    snapshot.xcnt = xip.len() as u32;
    snapshot.xip = xip;
    snapshot.subxcnt = subxip.len() as i32;
    snapshot.subxip = subxip;
    snapshot.suboverflowed = suboverflowed;
    snapshot.takenDuringRecovery = taken_during_recovery;
    snapshot.curcid = curcid;
    snapshot.snapXactCompletionCount = 0;
    snapshot.regd_count = 0;
    snapshot.active_count = 0;
    snapshot.copied = true;

    new_handle(snapshot)
}

/* ----------------------------------------------------------------------
 * Visibility
 * ---------------------------------------------------------------------- */

/// `XidInMVCCSnapshot` (snapmgr.c:1869) — is the given XID still-in-progress
/// according to the snapshot?
pub fn XidInMVCCSnapshot(mut xid: TransactionId, snapshot: &SnapshotData) -> PgResult<bool> {
    // Quick range check. Any xid < xmin is not in-progress.
    if TransactionIdPrecedes(xid, snapshot.xmin) {
        return Ok(false);
    }
    // Any xid >= xmax is in-progress.
    if TransactionIdFollowsOrEquals(xid, snapshot.xmax) {
        return Ok(true);
    }

    if !snapshot.takenDuringRecovery {
        if !snapshot.suboverflowed {
            // Full data: search subxip.
            if pg_lfind32(xid, &snapshot.subxip[..snapshot.subxcnt as usize]) {
                return Ok(true);
            }
            // Not there, fall through to search xip[].
        } else {
            // Snapshot overflowed: convert xid to top-level (safe since we
            // eliminated too-old XIDs above).
            xid = subtrans_seams::sub_trans_get_topmost_transaction::call(xid)?;

            // xid might now be < xmin; recheck to avoid an array scan.
            if TransactionIdPrecedes(xid, snapshot.xmin) {
                return Ok(false);
            }
        }

        if pg_lfind32(xid, &snapshot.xip[..snapshot.xcnt as usize]) {
            return Ok(true);
        }
    } else {
        // In recovery all xids are stored in the subxip array (the bigger
        // array); the xip array is empty. Search subtrans first if overflowed.
        if snapshot.suboverflowed {
            xid = subtrans_seams::sub_trans_get_topmost_transaction::call(xid)?;

            if TransactionIdPrecedes(xid, snapshot.xmin) {
                return Ok(false);
            }
        }

        if pg_lfind32(xid, &snapshot.subxip[..snapshot.subxcnt as usize]) {
            return Ok(true);
        }
    }

    Ok(false)
}

/* ----------------------------------------------------------------------
 * predicate.c serializable-snapshot glue
 * ---------------------------------------------------------------------- */

/// Invoke predicate.c's `GetSerializableTransactionSnapshot` on a `SnapHandle`
/// in place: the seam takes/returns the snapshot fields by value, so the result
/// is written back into the handle.
fn GetSerializableTransactionSnapshot(handle: &SnapHandle) -> PgResult<()> {
    let input = handle.borrow().clone();
    let filled = predicate_seams::get_serializable_transaction_snapshot::call(input)?;
    *handle.borrow_mut() = filled;
    Ok(())
}

/* ----------------------------------------------------------------------
 * Seam installation
 * ---------------------------------------------------------------------- */

/// Install every `backend-utils-time-snapmgr-seams` declaration. The seam
/// signatures marshal owned `SnapshotData` values; here the owner converts
/// between those and its internal `SnapHandle`s.
pub fn init_seams() {
    use backend_utils_time_snapmgr_seams as seams;

    seams::get_catalog_snapshot::set(|relid| Ok(GetCatalogSnapshot(relid)?.borrow().clone()));
    seams::register_snapshot::set(|snapshot| {
        Ok(RegisterSnapshotOnOwner(&new_handle(snapshot)).borrow().clone())
    });
    seams::estimate_snapshot_space::set(|snapshot| EstimateSnapshotSpace(&new_handle(snapshot.clone())));
    seams::serialize_snapshot::set(|snapshot| Ok(SerializeSnapshot(&new_handle(snapshot.clone()))));
    seams::restore_snapshot::set(|bytes| Ok(RestoreSnapshot(bytes).borrow().clone()));
    seams::with_transaction_snapshot::set(with_transaction_snapshot);
    seams::snapshot_set_command_id::set(SnapshotSetCommandId);
    seams::at_eoxact_snapshot::set(AtEOXact_Snapshot);
    seams::at_subcommit_snapshot::set(AtSubCommit_Snapshot);
    seams::at_subabort_snapshot::set(AtSubAbort_Snapshot);
    seams::xact_has_exported_snapshots::set(XactHasExportedSnapshots);
    seams::unregister_snapshot::set(|snapshot| {
        // C `UnregisterSnapshot` is void; its only fallible step is the proc
        // seam inside `SnapshotResetXmin`, which does not `ereport` in C.
        UnregisterSnapshot(Some(&new_handle(snapshot)))
            .expect("UnregisterSnapshot: SnapshotResetXmin must not ereport");
    });
    seams::get_active_snapshot::set(|| {
        // The C `Snapshot` is a shared pointer; the seam crosses an owned copy
        // wrapped in a bare `Rc`. `GetActiveSnapshot` asserts an active
        // snapshot exists, so the result is always `Some`.
        Ok(Some(std::rc::Rc::new(GetActiveSnapshot().borrow().clone())))
    });
    seams::push_active_snapshot::set(|snapshot| {
        PushActiveSnapshot(&new_handle((*snapshot).clone()));
        Ok(())
    });
    seams::pop_active_snapshot::set(PopActiveSnapshot);
    seams::historic_snapshot_active::set(HistoricSnapshotActive);
    seams::active_snapshot_set::set(ActiveSnapshotSet);
    seams::get_latest_snapshot::set(|| Ok(GetLatestSnapshot()?.borrow().clone()));
    seams::get_transaction_snapshot::set(|| Ok(GetTransactionSnapshot()?.borrow().clone()));
    seams::invalidate_catalog_snapshot::set(|| {
        // The seam is infallible (`InvalidateCatalogSnapshot` is `void` in C);
        // its only fallible step is `SnapshotResetXmin`, which does not
        // `ereport` in C.
        InvalidateCatalogSnapshot().expect("InvalidateCatalogSnapshot: SnapshotResetXmin must not ereport");
    });
    seams::push_active_snapshot_transaction::set(|| {
        PushActiveSnapshot(&GetTransactionSnapshot()?);
        Ok(())
    });
    seams::push_copied_active_snapshot::set(|| {
        PushCopiedSnapshot(&GetActiveSnapshot());
        Ok(())
    });
    seams::update_active_snapshot_command_id::set(UpdateActiveSnapshotCommandId);
    seams::unregister_snapshot_from_owner::set(|snapshot, _owner| {
        // The C `Snapshot` is a shared pointer crossing as a bare `Rc`; the
        // resowner `Forget` is the caller's RAII responsibility, so the owner is
        // unused here (the `NoOwner` core). Cannot `ereport` in C; modeled bare.
        UnregisterSnapshotFromOwner(&new_handle((*snapshot).clone()))
            .expect("UnregisterSnapshotFromOwner: SnapshotResetXmin must not ereport");
    });

    // plancache/xact/standby/subtrans's split slice of snapmgr's contract
    // (`backend-utils-time-snapmgr-pc-seams`). Same inward seams as the base
    // crate above, redeclared in the consumers' own seam crate; the owner
    // installs both. `active_snapshot_set` carries `PgResult<bool>` here (vs the
    // base crate's bare `bool`), so it's wrapped in `Ok` — `ActiveSnapshotSet`
    // is infallible in C.
    use backend_utils_time_snapmgr_pc_seams as pc_seams;
    pc_seams::active_snapshot_set::set(|| Ok(ActiveSnapshotSet()));
    pc_seams::push_active_snapshot_transaction::set(|| {
        PushActiveSnapshot(&GetTransactionSnapshot()?);
        Ok(())
    });
    pc_seams::pop_active_snapshot::set(PopActiveSnapshot);
    // `TransactionXmin` is a backend-global the consumers read directly.
    pc_seams::transaction_xmin::set(|| Ok(TransactionXmin()));
}

/// The `RemoveTempRelationsCallback` snapshot bracket
/// (`PushActiveSnapshot(GetTransactionSnapshot()); f(); PopActiveSnapshot()`),
/// owned by snapmgr as one scope. Snapshot acquisition allocates and can
/// `ereport(ERROR)`, and `f`'s error propagates; both carried on `Err`.
fn with_transaction_snapshot(f: &mut dyn FnMut() -> PgResult<()>) -> PgResult<()> {
    let snap = GetTransactionSnapshot()?;
    PushActiveSnapshot(&snap);
    let result = f();
    PopActiveSnapshot()?;
    result
}
