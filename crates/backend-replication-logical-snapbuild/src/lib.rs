//! `replication/logical/snapbuild.c` — the logical-decoding historic-snapshot
//! builder.
//!
//! Builds catalog-decoding snapshots from the WAL stream (the
//! START → BUILDING_SNAPSHOT → FULL_SNAPSHOT → CONSISTENT state machine),
//! tracks committed catalog-modifying xids in `[xmin, xmax)`, distributes
//! snapshots/invalidations to in-progress reorderbuffer transactions, and
//! serializes/restores consistent snapshots to/from disk.
//!
//! ## Ownership model
//!
//! The real [`SnapBuild`] value lives in this crate. logical.c / slotsync hold
//! it only as an opaque [`SnapBuildHandle`] (a `usize`) they forward through the
//! inward seams; this crate is the handle's owner and keeps a backend-local
//! registry mapping each live handle to its `SnapBuild`. The `ReorderBuffer`
//! and its `ReorderBufferTXN`s are *not* owned here — they cross as
//! [`ReorderBufferHandle`]/[`TxnHandle`] and every field access / mutation goes
//! through the reorderbuffer owner's seams (which panic until reorderbuffer
//! lands).
//!
//! ## On-disk format
//!
//! The C serializes the whole `SnapBuild` struct with a raw `memcpy` plus the
//! trailing committed/catchange xid arrays, CRC-checksummed. Our `SnapBuild`
//! has owned `Vec` fields and cannot be `memcpy`'d, so [`ondisk`] writes the
//! version-dependent scalar fields explicitly in the C struct's field order and
//! sizes (the pointer fields the C NULLs are simply not written), followed by
//! the same trailing xid arrays and the same CRC discipline. The format
//! round-trips faithfully for this implementation's own restarts.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;
use core::cell::RefCell;

use types_core::primitive::{TransactionId, XLogRecPtr};
use types_core::xact::{
    CommandId, FirstCommandId, InvalidCommandId, InvalidTransactionId, InvalidXLogRecPtr,
    TransactionIdIsNormal, TransactionIdIsValid, XACT_REPEATABLE_READ,
};
use types_error::{PgError, PgResult};
use types_logical::ReorderBufferHandle;
use types_snapshot::snapshot::{SnapshotData, SnapshotType};
use types_xlog_records::heapam_xlog::xl_heap_new_cid;
use types_xlog_records::standbydefs::xl_running_xacts;

use backend_access_transam_transam::{
    TransactionIdFollows, TransactionIdFollowsOrEquals, TransactionIdPrecedes,
    TransactionIdPrecedesOrEquals,
};
use backend_access_transam_varsup::TransactionIdAdvance;

use backend_utils_error::ErrorLevel;

// Outward owner seams.
use backend_access_transam_xact_seams as xact;
use backend_access_transam_xlog_seams as xlog;
use backend_replication_logical_logical_seams as logical;
use backend_replication_logical_reorderbuffer_seams as rb;
use backend_replication_slot_seams as slot;
use backend_storage_lmgr_proc_seams as proc_seam;
use backend_utils_init_miscinit_seams as miscinit;
use backend_storage_file_fd_seams as fd;
use backend_storage_lmgr_lmgr_seams as lmgr;
use backend_utils_resowner_resowner_seams as resowner;
use backend_utils_time_snapmgr_seams as snapmgr;
use port_crc32c_seams as crc;

// Error helper SqlStates.
use types_error::error::{ERRCODE_DATA_CORRUPTED, ERRCODE_T_R_SERIALIZATION_FAILURE};

// elog levels.
use types_error::error::{DEBUG1, DEBUG2, DEBUG3, ERROR, LOG};

mod ondisk;
mod registry;

pub use registry::{with_builder, with_builder_opt};

// ===========================================================================
// Constants
// ===========================================================================

/// `SnapBuildState` (snapbuild.h) — modeled as the `i32` consts the inward
/// `SnapBuildCurrentState` seam carries.
pub type SnapBuildState = i32;
pub const SNAPBUILD_START: SnapBuildState = -1;
pub const SNAPBUILD_BUILDING_SNAPSHOT: SnapBuildState = 0;
pub const SNAPBUILD_FULL_SNAPSHOT: SnapBuildState = 1;
pub const SNAPBUILD_CONSISTENT: SnapBuildState = 2;

/// `XACT_XINFO_HAS_INVALS` (xact.h) — commit record carries invalidations.
const XACT_XINFO_HAS_INVALS: u32 = 1 << 3;

/// `PG_LOGICAL_SNAPSHOTS_DIR` (reorderbuffer.h) — where serialized snapshots
/// live, relative to the data directory.
const PG_LOGICAL_SNAPSHOTS_DIR: &str = "pg_logical/snapshots";

/// `SNAPBUILD_MAGIC` / `SNAPBUILD_VERSION` (snapbuild.c).
const SNAPBUILD_MAGIC: u32 = 0x51A1_E001;
const SNAPBUILD_VERSION: u32 = 6;

// Wait events: the IO-class events are alphabetically numbered in
// `wait_event_types.h`; SLRU_WRITE is 53, so SNAPBUILD_READ/SYNC/WRITE are
// 54/55/56 (TIMELINE_HISTORY_FILE_SYNC is 57). `PG_WAIT_IO == 0x0A000000`.
const PG_WAIT_IO: u32 = 0x0A00_0000;
const WAIT_EVENT_SNAPBUILD_READ: u32 = PG_WAIT_IO + 54;
const WAIT_EVENT_SNAPBUILD_SYNC: u32 = PG_WAIT_IO + 55;
const WAIT_EVENT_SNAPBUILD_WRITE: u32 = PG_WAIT_IO + 56;

const ENOENT: i32 = 2;

// ===========================================================================
// The SnapBuild value
// ===========================================================================

/// `struct SnapBuild` (snapbuild_internal.h). The pointer fields of the C
/// struct become owned values here: `context` is dropped (the C private
/// `MemoryContext` is the crate's allocation accountability, not a behavior),
/// `snapshot` is an owned [`SnapshotData`] with an explicit refcount mirroring
/// the C `active_count` discipline, and `reorder` is the opaque handle.
pub struct SnapBuild {
    /// `state` — how far we are along building the first full snapshot.
    pub state: SnapBuildState,

    /// `xmin` — all transactions < this have committed/aborted.
    pub xmin: TransactionId,
    /// `xmax` — all transactions >= this are uncommitted.
    pub xmax: TransactionId,

    /// `start_decoding_at` — don't replay commits from an LSN < this.
    pub start_decoding_at: XLogRecPtr,

    /// `two_phase_at` — LSN two-phase decoding was enabled / consistent point
    /// at slot creation.
    pub two_phase_at: XLogRecPtr,

    /// `initial_xmin_horizon` — don't start decoding until running xacts has no
    /// xid smaller than this.
    pub initial_xmin_horizon: TransactionId,

    /// `building_full_snapshot` — building a full snapshot vs catalog-only.
    pub building_full_snapshot: bool,

    /// `in_slot_creation` — using the builder for slot creation.
    pub in_slot_creation: bool,

    /// `snapshot` — snapshot valid for the catalog state at this moment, with
    /// its builder refcount. `None` is the C NULL.
    pub snapshot: Option<RefcountedSnapshot>,

    /// `last_serialized_snapshot` — LSN of the last serialized snapshot.
    pub last_serialized_snapshot: XLogRecPtr,

    /// `reorder` — the reorderbuffer to update with snapshots.
    pub reorder: ReorderBufferHandle,

    /// `next_phase_at` — xid at which the next phase of initial snapshot
    /// building happens.
    pub next_phase_at: TransactionId,

    /// `committed` — committed catalog-changing transactions in `[xmin, xmax)`.
    pub committed: Committed,

    /// `catchange` — running catalog-changing transactions captured at
    /// serialization.
    pub catchange: Catchange,
}

/// The C `builder->committed` sub-struct.
#[derive(Default)]
pub struct Committed {
    /// `xcnt` — number of committed transactions in `xip`.
    pub xcnt: usize,
    /// `xcnt_space` — allocated space (kept for serialization fidelity; the
    /// owned `Vec` grows on its own).
    pub xcnt_space: usize,
    /// `includes_all_transactions` — false once we drop a non-catalog xact and
    /// can no longer export a general snapshot.
    pub includes_all_transactions: bool,
    /// `xip` — the committed-xact array (unsorted; sorted at snapshot build).
    pub xip: Vec<TransactionId>,
}

/// The C `builder->catchange` sub-struct.
#[derive(Default)]
pub struct Catchange {
    /// `xcnt` — number of transactions in `xip`.
    pub xcnt: usize,
    /// `xip` — sorted (xidComparator) catalog-changing running xacts.
    pub xip: Vec<TransactionId>,
}

/// `builder->snapshot` — an owned [`SnapshotData`] plus the C `active_count`
/// refcount the builder maintains. In C the snapshot is a shared pointer handed
/// to the reorderbuffer with refcounting; here the reorderbuffer takes its own
/// clone across the seam, so this refcount only tracks the builder's own
/// holding (freeing the owned snapshot when it drops to zero, matching
/// `SnapBuildFreeSnapshot`).
pub struct RefcountedSnapshot {
    pub snap: SnapshotData,
}

// ===========================================================================
// xidComparator + Normal precedence helpers
// ===========================================================================

/// `xidComparator` (xid.c) — plain unsigned compare of two `TransactionId`s.
fn xid_cmp(a: &TransactionId, b: &TransactionId) -> core::cmp::Ordering {
    a.cmp(b)
}

/// `bsearch(&xid, arr, ..., xidComparator)` — does `arr` (sorted ascending)
/// contain `xid`?
fn xid_in_sorted(arr: &[TransactionId], xid: TransactionId) -> bool {
    arr.binary_search(&xid).is_ok()
}

/// `NormalTransactionIdPrecedes(a, b)` (transam.h) — `a` and `b` are both
/// normal and `a` precedes `b`.
fn normal_transaction_id_precedes(a: TransactionId, b: TransactionId) -> bool {
    debug_assert!(TransactionIdIsNormal(a) && TransactionIdIsNormal(b));
    TransactionIdPrecedes(a, b)
}

/// `NormalTransactionIdFollows(a, b)` (transam.h).
fn normal_transaction_id_follows(a: TransactionId, b: TransactionId) -> bool {
    debug_assert!(TransactionIdIsNormal(a) && TransactionIdIsNormal(b));
    TransactionIdFollows(a, b)
}

/// `LSN_FORMAT_ARGS(lsn)` rendered into `%X/%X`.
fn lsn_str(lsn: XLogRecPtr) -> String {
    alloc::format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

/// `elog(level, msg)` — the simple no-fields report. The snapbuild elogs are
/// all DEBUG/LOG level (no ERROR), so the Ok result is discarded.
fn elog(level: ErrorLevel, msg: String) {
    let _ = backend_utils_error::elog(level, msg);
}

// ===========================================================================
// AllocateSnapshotBuilder / FreeSnapshotBuilder
// ===========================================================================

/// `AllocateSnapshotBuilder` (snapbuild.c:184).
pub fn allocate_snapshot_builder(
    reorder: ReorderBufferHandle,
    xmin_horizon: TransactionId,
    start_lsn: XLogRecPtr,
    need_full_snapshot: bool,
    in_slot_creation: bool,
    two_phase_at: XLogRecPtr,
) -> SnapBuild {
    SnapBuild {
        state: SNAPBUILD_START,
        xmin: InvalidTransactionId,
        xmax: InvalidTransactionId,
        start_decoding_at: start_lsn,
        two_phase_at,
        initial_xmin_horizon: xmin_horizon,
        building_full_snapshot: need_full_snapshot,
        in_slot_creation,
        snapshot: None,
        last_serialized_snapshot: InvalidXLogRecPtr,
        reorder,
        next_phase_at: InvalidTransactionId,
        committed: Committed {
            xcnt: 0,
            xcnt_space: 128, // arbitrary number, as in C
            includes_all_transactions: true,
            xip: Vec::new(),
        },
        catchange: Catchange {
            xcnt: 0,
            xip: Vec::new(),
        },
    }
}

/// `FreeSnapshotBuilder` (snapbuild.c:232). The C frees the snapshot explicitly
/// (with error checking) then deletes the memory context; here dropping the
/// `SnapBuild` reclaims everything, and the explicit snapshot decref preserves
/// the C error checks.
pub fn free_snapshot_builder(builder: &mut SnapBuild) {
    if builder.snapshot.is_some() {
        snap_build_snap_dec_refcount(builder);
    }
}

/// `SnapBuildCurrentState` (snapbuild.c:276).
pub fn snap_build_current_state(builder: &SnapBuild) -> SnapBuildState {
    builder.state
}

/// `SnapBuildGetTwoPhaseAt` (snapbuild.c:285) — the LSN at which two-phase
/// decoding was first enabled.
pub fn snap_build_get_two_phase_at(builder: &SnapBuild) -> XLogRecPtr {
    builder.two_phase_at
}

/// `SnapBuildSetTwoPhaseAt` (snapbuild.c:294).
pub fn snap_build_set_two_phase_at(builder: &mut SnapBuild, ptr: XLogRecPtr) {
    builder.two_phase_at = ptr;
}

/// `SnapBuildXactNeedsSkip` (snapbuild.c:303) — should the contents of a
/// transaction ending at `ptr` be decoded?
pub fn snap_build_xact_needs_skip(builder: &SnapBuild, ptr: XLogRecPtr) -> bool {
    ptr < builder.start_decoding_at
}

// ===========================================================================
// Snapshot refcounting
// ===========================================================================

/// `SnapBuildSnapIncRefcount(builder->snapshot)` — the builder's own +1 hold.
/// Modeled implicitly: the builder holds exactly one reference whenever
/// `builder.snapshot` is `Some`. Extra incref/decref calls that in C track
/// hand-outs to the reorderbuffer are no-ops here because the reorderbuffer
/// takes its own clone across the seam.
fn snap_build_snap_inc_refcount(_builder: &mut SnapBuild) {
    // No-op: see RefcountedSnapshot. Kept as a named site to mirror the C.
}

/// `SnapBuildSnapDecRefcount(builder->snapshot)` (snapbuild.c:327) for the
/// builder's own held snapshot — drops it (the C `active_count` reaching zero ->
/// `SnapBuildFreeSnapshot`).
fn snap_build_snap_dec_refcount(builder: &mut SnapBuild) {
    if let Some(rc) = &builder.snapshot {
        // make sure we don't get passed an external snapshot
        debug_assert_eq!(rc.snap.snapshot_type, SnapshotType::SNAPSHOT_HISTORIC_MVCC);
        debug_assert_eq!(rc.snap.curcid, FirstCommandId);
        debug_assert!(!rc.snap.suboverflowed);
        debug_assert!(!rc.snap.takenDuringRecovery);
        debug_assert_eq!(rc.snap.regd_count, 0);
        debug_assert!(!rc.snap.copied);
    }
    builder.snapshot = None;
}

// ===========================================================================
// SnapBuildBuildSnapshot
// ===========================================================================

/// `SnapBuildBuildSnapshot` (snapbuild.c:359).
fn snap_build_build_snapshot(builder: &SnapBuild) -> SnapshotData {
    debug_assert!(builder.state >= SNAPBUILD_FULL_SNAPSHOT);
    debug_assert!(TransactionIdIsNormal(builder.xmin));
    debug_assert!(TransactionIdIsNormal(builder.xmax));

    let mut xip = builder.committed.xip[..builder.committed.xcnt].to_vec();
    // sort so we can bsearch()
    xip.sort_by(xid_cmp);

    SnapshotData {
        snapshot_type: SnapshotType::SNAPSHOT_HISTORIC_MVCC,
        vistest: types_snapshot::snapshot::GlobalVisStateHandle::new(0),
        xmin: builder.xmin,
        xmax: builder.xmax,
        xcnt: xip.len() as u32,
        xip,
        subxcnt: 0,
        subxip: Vec::new(),
        suboverflowed: false,
        takenDuringRecovery: false,
        copied: false,
        curcid: FirstCommandId,
        speculativeToken: 0,
        active_count: 0,
        regd_count: 0,
        snapXactCompletionCount: 0,
        reg_id: 0,
    }
}

// ===========================================================================
// SnapBuildInitialSnapshot / Export / GetOrBuild / Clear / Reset
// ===========================================================================

// File-scope export state (snapbuild.c:151-152) — backend-local.
thread_local! {
    static SAVED_RESOURCE_OWNER_DURING_EXPORT: RefCell<Option<types_nodes::parsestmt::ResourceOwnerHandle>> =
        const { RefCell::new(None) };
    static EXPORT_IN_PROGRESS: RefCell<bool> = const { RefCell::new(false) };
}

/// `SnapBuildInitialSnapshot` (snapbuild.c:439). Build a full slot snapshot and
/// convert it to a normal MVCC snapshot.
pub fn snap_build_initial_snapshot(builder: &mut SnapBuild) -> PgResult<SnapshotData> {
    debug_assert!(xact::isolation_uses_xact_snapshot::call());
    debug_assert!(builder.building_full_snapshot);

    // don't allow older snapshots; about to overwrite MyProc->xmin
    snapmgr::invalidate_catalog_snapshot::call();
    if snapmgr::have_registered_or_active_snapshot::call() {
        return Err(elog_err(
            "cannot build an initial slot snapshot when snapshots exist",
        ));
    }
    debug_assert!(!snapmgr::historic_snapshot_active::call());

    if builder.state != SNAPBUILD_CONSISTENT {
        return Err(elog_err(
            "cannot build an initial slot snapshot before reaching a consistent state",
        ));
    }

    if !builder.committed.includes_all_transactions {
        return Err(elog_err(
            "cannot build an initial slot snapshot, not all transactions are monitored anymore",
        ));
    }

    // so we don't overwrite the existing value
    if TransactionIdIsValid(proc_seam::my_proc_xmin::call()) {
        return Err(elog_err(
            "cannot build an initial slot snapshot when MyProc->xmin already is valid",
        ));
    }

    let mut snap = snap_build_build_snapshot(builder);

    // We know snap->xmin is alive, enforced by the logical xmin mechanism.
    // Always double-check the horizon is enforced (cheap insurance).
    let safe_xid = procarray_get_oldest_safe_decoding_xid_locked();

    if TransactionIdFollows(safe_xid, snap.xmin) {
        return Err(elog_err(alloc::format!(
            "cannot build an initial slot snapshot as oldest safe xid {} follows snapshot's xmin {}",
            safe_xid, snap.xmin
        )));
    }

    proc_seam::set_my_proc_xmin::call(snap.xmin);

    // snapbuild stores committed xacts in ->xip; build a classical snapshot by
    // marking all non-committed transactions as in-progress.
    let max_xid_count = backend_storage_ipc_procarray_seams::get_max_snapshot_xid_count::call();
    let mut newxip: Vec<TransactionId> = Vec::new();

    let mut xid = snap.xmin;
    while normal_transaction_id_precedes(xid, snap.xmax) {
        // committed in the decoding ->xip sense?
        if !xid_in_sorted(&snap.xip, xid) {
            if newxip.len() as i32 >= max_xid_count {
                return Err(PgError::new(ERROR, String::from("initial slot snapshot too large"))
                    .with_sqlstate(ERRCODE_T_R_SERIALIZATION_FAILURE));
            }
            newxip.push(xid);
        }
        TransactionIdAdvance(&mut xid);
    }

    // adjust remaining snapshot fields as needed
    snap.snapshot_type = SnapshotType::SNAPSHOT_MVCC;
    snap.xcnt = newxip.len() as u32;
    snap.xip = newxip;

    Ok(snap)
}

/// `SnapBuildExportSnapshot` (snapbuild.c:538).
pub fn snap_build_export_snapshot(builder: &mut SnapBuild) -> PgResult<String> {
    if xact::is_transaction_or_transaction_block::call() {
        return Err(elog_err("cannot export a snapshot from within a transaction"));
    }

    if SAVED_RESOURCE_OWNER_DURING_EXPORT.with(|c| c.borrow().is_some()) {
        return Err(elog_err("can only export one snapshot at a time"));
    }

    let cur_owner = resowner::current_resource_owner::call()?;
    SAVED_RESOURCE_OWNER_DURING_EXPORT.with(|c| *c.borrow_mut() = Some(cur_owner));
    EXPORT_IN_PROGRESS.with(|c| *c.borrow_mut() = true);

    xact::start_transaction_command::call()?;

    // There doesn't seem to be a nice API to set these.
    let _ = XACT_REPEATABLE_READ; // documents the value the seam sets
    xact::set_xact_iso_level_repeatable_read::call();
    xact::set_xact_read_only::call(true);

    let snap = snap_build_initial_snapshot(builder)?;
    let xcnt = snap.xcnt;

    // now that we've built a plain snapshot, make it active and use the normal
    // mechanisms for exporting it
    let snapname = snapmgr::export_snapshot::call(snap)?;

    elog(
        LOG,
        if xcnt == 1 {
            alloc::format!(
                "exported logical decoding snapshot: \"{snapname}\" with {xcnt} transaction ID"
            )
        } else {
            alloc::format!(
                "exported logical decoding snapshot: \"{snapname}\" with {xcnt} transaction IDs"
            )
        },
    );
    Ok(snapname)
}

/// `SnapBuildGetOrBuildSnapshot` (snapbuild.c:578).
pub fn snap_build_get_or_build_snapshot(builder: &mut SnapBuild) -> SnapshotData {
    debug_assert_eq!(builder.state, SNAPBUILD_CONSISTENT);

    if builder.snapshot.is_none() {
        let snap = snap_build_build_snapshot(builder);
        builder.snapshot = Some(RefcountedSnapshot { snap });
        snap_build_snap_inc_refcount(builder);
    }

    builder.snapshot.as_ref().unwrap().snap.clone()
}

/// `SnapBuildClearExportedSnapshot` (snapbuild.c:599).
pub fn snap_build_clear_exported_snapshot() -> PgResult<()> {
    // nothing exported, that is the usual case
    if !EXPORT_IN_PROGRESS.with(|c| *c.borrow()) {
        return Ok(());
    }

    if !xact::is_transaction_state::call() {
        return Err(elog_err("clearing exported snapshot in wrong transaction state"));
    }

    // AbortCurrentTransaction() resets snapshot state; remember the saved owner.
    let tmp_res_owner = SAVED_RESOURCE_OWNER_DURING_EXPORT.with(|c| *c.borrow());

    // make sure nothing could have ever happened
    xact::abort_current_transaction::call()?;

    match tmp_res_owner {
        Some(owner) => resowner::set_current_resource_owner::call(owner),
        None => resowner::set_current_resource_owner::call(
            types_nodes::parsestmt::ResourceOwnerHandle::NULL,
        ),
    }
    Ok(())
}

/// `SnapBuildResetExportedSnapshotState` (snapbuild.c:626).
pub fn snap_build_reset_exported_snapshot_state() {
    SAVED_RESOURCE_OWNER_DURING_EXPORT.with(|c| *c.borrow_mut() = None);
    EXPORT_IN_PROGRESS.with(|c| *c.borrow_mut() = false);
}

// ===========================================================================
// SnapBuildProcessChange / NewCid / Distribute / AddCommittedTxn
// ===========================================================================

/// `SnapBuildProcessChange` (snapbuild.c:638).
pub fn snap_build_process_change(
    builder: &mut SnapBuild,
    xid: TransactionId,
    lsn: XLogRecPtr,
) -> bool {
    // can't handle data if we haven't built a snapshot yet
    if builder.state < SNAPBUILD_FULL_SNAPSHOT {
        return false;
    }

    // no point in tracking changes in transactions we can't decode
    if builder.state < SNAPBUILD_CONSISTENT
        && TransactionIdPrecedes(xid, builder.next_phase_at)
    {
        return false;
    }

    // if the reorderbuffer doesn't yet have a snapshot, add one now
    if !rb::ReorderBufferXidHasBaseSnapshot::call(builder.reorder, xid) {
        if builder.snapshot.is_none() {
            let snap = snap_build_build_snapshot(builder);
            builder.snapshot = Some(RefcountedSnapshot { snap });
            snap_build_snap_inc_refcount(builder);
        }

        // increase refcount for the transaction we're handing the snapshot to
        snap_build_snap_inc_refcount(builder);
        let snap = builder.snapshot.as_ref().unwrap().snap.clone();
        rb::ReorderBufferSetBaseSnapshot::call(builder.reorder, xid, lsn, snap);
    }

    true
}

/// `SnapBuildProcessNewCid` (snapbuild.c:688).
pub fn snap_build_process_new_cid(
    builder: &mut SnapBuild,
    xid: TransactionId,
    lsn: XLogRecPtr,
    xlrec: &xl_heap_new_cid,
) -> PgResult<()> {
    // only logged when a catalog tuple was modified -> mark the txn
    rb::ReorderBufferXidSetCatalogChanges::call(builder.reorder, xid, lsn);

    rb::ReorderBufferAddNewTupleCids::call(
        builder.reorder,
        xlrec.top_xid,
        lsn,
        xlrec.target_locator,
        xlrec.target_tid,
        xlrec.cmin,
        xlrec.cmax,
        xlrec.combocid,
    );

    // figure out new command id
    let cid: CommandId = if xlrec.cmin != InvalidCommandId && xlrec.cmax != InvalidCommandId {
        core::cmp::max(xlrec.cmin, xlrec.cmax)
    } else if xlrec.cmax != InvalidCommandId {
        xlrec.cmax
    } else if xlrec.cmin != InvalidCommandId {
        xlrec.cmin
    } else {
        return Err(elog_err("xl_heap_new_cid record without a valid CommandId"));
    };

    rb::ReorderBufferAddNewCommandId::call(builder.reorder, xid, lsn, cid.wrapping_add(1));
    Ok(())
}

/// `SnapBuildDistributeSnapshotAndInval` (snapbuild.c:730).
fn snap_build_distribute_snapshot_and_inval(
    builder: &mut SnapBuild,
    lsn: XLogRecPtr,
    xid: TransactionId,
) {
    // Iterate through all toplevel transactions in LSN order.
    let txns = rb::reorder_buffer_toplevel_txns::call(builder.reorder);
    for txn in txns {
        let txn_xid = rb::reorder_buffer_txn_xid::call(builder.reorder, txn);
        debug_assert!(TransactionIdIsValid(txn_xid));

        // no base snapshot yet -> no changes -> no snapshot needed
        if !rb::ReorderBufferXidHasBaseSnapshot::call(builder.reorder, txn_xid) {
            continue;
        }

        // prepared transactions should not see new catalog contents
        if rb::reorder_buffer_txn_is_prepared::call(builder.reorder, txn) {
            continue;
        }

        elog(
            DEBUG2,
            alloc::format!(
                "adding a new snapshot and invalidations to {} at {}",
                txn_xid,
                lsn_str(lsn)
            ),
        );

        // increase the snapshot's refcount for the txn we're handing it to
        snap_build_snap_inc_refcount(builder);
        let snap = builder.snapshot.as_ref().unwrap().snap.clone();
        rb::ReorderBufferAddSnapshot::call(builder.reorder, txn_xid, lsn, snap);

        // distribute the current committed txn's invalidations to other
        // in-progress txns (not the committed txn itself)
        if txn_xid != xid {
            let msgs = rb::ReorderBufferGetInvalidations::call(builder.reorder, xid);
            if !msgs.is_empty() {
                rb::ReorderBufferAddDistributedInvalidations::call(
                    builder.reorder,
                    txn_xid,
                    lsn,
                    msgs,
                );
            }
        }
    }
}

/// `SnapBuildAddCommittedTxn` (snapbuild.c:828).
fn snap_build_add_committed_txn(builder: &mut SnapBuild, xid: TransactionId) {
    debug_assert!(TransactionIdIsValid(xid));

    if builder.committed.xcnt == builder.committed.xcnt_space {
        builder.committed.xcnt_space = builder.committed.xcnt_space * 2 + 1;
        elog(
            DEBUG1,
            alloc::format!(
                "increasing space for committed transactions to {}",
                builder.committed.xcnt_space as u32
            ),
        );
    }

    // The xip Vec is the source of truth; keep xcnt and the Vec length in step.
    if builder.committed.xip.len() > builder.committed.xcnt {
        builder.committed.xip.truncate(builder.committed.xcnt);
    }
    builder.committed.xip.push(xid);
    builder.committed.xcnt += 1;
}

/// `SnapBuildPurgeOlderTxn` (snapbuild.c:862).
fn snap_build_purge_older_txn(builder: &mut SnapBuild) {
    // not ready yet
    if !TransactionIdIsNormal(builder.xmin) {
        return;
    }

    // copy xids that still are interesting
    let xmin = builder.xmin;
    let before = builder.committed.xcnt;
    let mut workspace: Vec<TransactionId> = Vec::new();
    for &x in &builder.committed.xip[..builder.committed.xcnt] {
        if normal_transaction_id_precedes(x, xmin) {
            // remove
        } else {
            workspace.push(x);
        }
    }
    let surviving_xids = workspace.len();
    builder.committed.xip.truncate(0);
    builder.committed.xip.extend_from_slice(&workspace);

    elog(
        DEBUG3,
        alloc::format!(
            "purged committed transactions from {} to {}, xmin: {}, xmax: {}",
            before as u32, surviving_xids as u32, builder.xmin, builder.xmax
        ),
    );
    builder.committed.xcnt = surviving_xids;

    // Purge ->catchange as well. The purged array stays sorted.
    if builder.catchange.xcnt > 0 {
        // catchange.xip is sorted; find the lower bound of interesting xids.
        let mut off = 0usize;
        while off < builder.catchange.xcnt {
            if TransactionIdFollowsOrEquals(builder.catchange.xip[off], builder.xmin) {
                break;
            }
            off += 1;
        }

        let surviving = builder.catchange.xcnt - off;
        let before_c = builder.catchange.xcnt;
        if surviving > 0 {
            builder.catchange.xip.drain(0..off);
        } else {
            builder.catchange.xip.clear();
        }

        elog(
            DEBUG3,
            alloc::format!(
                "purged catalog modifying transactions from {} to {}, xmin: {}, xmax: {}",
                before_c as u32, surviving as u32, builder.xmin, builder.xmax
            ),
        );
        builder.catchange.xcnt = surviving;
    }
}

/// `SnapBuildXidHasCatalogChanges` (snapbuild.c:1105).
fn snap_build_xid_has_catalog_changes(
    builder: &SnapBuild,
    xid: TransactionId,
    xinfo: u32,
) -> bool {
    if rb::ReorderBufferXidHasCatalogChanges::call(builder.reorder, xid) {
        return true;
    }

    // txns that changed catalogs must have invalidation info
    if xinfo & XACT_XINFO_HAS_INVALS == 0 {
        return false;
    }

    builder.catchange.xcnt > 0
        && xid_in_sorted(&builder.catchange.xip[..builder.catchange.xcnt], xid)
}

/// `SnapBuildCommitTxn` (snapbuild.c:939).
pub fn snap_build_commit_txn(
    builder: &mut SnapBuild,
    lsn: XLogRecPtr,
    xid: TransactionId,
    subxacts: &[TransactionId],
    xinfo: u32,
) {
    let mut needs_snapshot = false;
    let mut needs_timetravel = false;
    let mut sub_needs_timetravel = false;
    let mut xmax = xid;

    // Transactions preceding BUILDING_SNAPSHOT are neither decoded nor part of
    // a snapshot.
    if builder.state == SNAPBUILD_START
        || (builder.state == SNAPBUILD_BUILDING_SNAPSHOT
            && TransactionIdPrecedes(xid, builder.next_phase_at))
    {
        if builder.start_decoding_at <= lsn {
            builder.start_decoding_at = lsn + 1;
        }
        return;
    }

    if builder.state < SNAPBUILD_CONSISTENT {
        if builder.start_decoding_at <= lsn {
            builder.start_decoding_at = lsn + 1;
        }

        // if building an exportable snapshot, force xid to be tracked
        if builder.building_full_snapshot {
            needs_timetravel = true;
        }
    }

    for &subxid in subxacts {
        if snap_build_xid_has_catalog_changes(builder, subxid, xinfo) {
            sub_needs_timetravel = true;
            needs_snapshot = true;

            elog(
                DEBUG1,
                alloc::format!("found subtransaction {}:{} with catalog changes", xid, subxid),
            );

            snap_build_add_committed_txn(builder, subxid);

            if normal_transaction_id_follows(subxid, xmax) {
                xmax = subxid;
            }
        } else if needs_timetravel {
            snap_build_add_committed_txn(builder, subxid);
            if normal_transaction_id_follows(subxid, xmax) {
                xmax = subxid;
            }
        }
    }

    // if top-level modified catalog, it'll need a snapshot
    if snap_build_xid_has_catalog_changes(builder, xid, xinfo) {
        elog(
            DEBUG2,
            alloc::format!("found top level transaction {}, with catalog changes", xid),
        );
        needs_snapshot = true;
        needs_timetravel = true;
        snap_build_add_committed_txn(builder, xid);
    } else if sub_needs_timetravel {
        elog(
            DEBUG2,
            alloc::format!(
                "forced transaction {} to do timetravel due to one of its subtransactions",
                xid
            ),
        );
        needs_timetravel = true;
        snap_build_add_committed_txn(builder, xid);
    } else if needs_timetravel {
        elog(DEBUG2, alloc::format!("forced transaction {} to do timetravel", xid));
        snap_build_add_committed_txn(builder, xid);
    }

    if !needs_timetravel {
        // record that we cannot export a general snapshot anymore
        builder.committed.includes_all_transactions = false;
    }

    debug_assert!(!needs_snapshot || needs_timetravel);

    // Adjust xmax for committed, catalog-modifying transactions.
    if needs_timetravel
        && (!TransactionIdIsValid(builder.xmax)
            || TransactionIdFollowsOrEquals(xmax, builder.xmax))
    {
        builder.xmax = xmax;
        TransactionIdAdvance(&mut builder.xmax);
    }

    // build a historic snapshot if there's any reason to
    if needs_snapshot {
        // a complete snapshot isn't useful/possible before FULL_SNAPSHOT
        if builder.state < SNAPBUILD_FULL_SNAPSHOT {
            return;
        }

        // decref the old snapshot (still used if handed out earlier)
        if builder.snapshot.is_some() {
            snap_build_snap_dec_refcount(builder);
        }

        let snap = snap_build_build_snapshot(builder);
        builder.snapshot = Some(RefcountedSnapshot { snap });

        // we might need to execute invalidations, add snapshot
        if !rb::ReorderBufferXidHasBaseSnapshot::call(builder.reorder, xid) {
            snap_build_snap_inc_refcount(builder);
            let s = builder.snapshot.as_ref().unwrap().snap.clone();
            rb::ReorderBufferSetBaseSnapshot::call(builder.reorder, xid, lsn, s);
        }

        // refcount of the snapshot builder for the new snapshot
        snap_build_snap_inc_refcount(builder);

        // add a new catalog snapshot + invalidations to all running txns
        snap_build_distribute_snapshot_and_inval(builder, lsn, xid);
    }
}

// ===========================================================================
// SnapBuildProcessRunningXacts + FindSnapshot + WaitSnapshot
// ===========================================================================

/// `SnapBuildProcessRunningXacts` (snapbuild.c:1135).
pub fn snap_build_process_running_xacts(
    builder: &mut SnapBuild,
    lsn: XLogRecPtr,
    running: &xl_running_xacts,
    running_xids: &[TransactionId],
) -> PgResult<()> {
    // not consistent yet: inspect the record to try to get closer.
    // consistent: dump our snapshot for reuse.
    if builder.state < SNAPBUILD_CONSISTENT {
        if !snap_build_find_snapshot(builder, lsn, running, running_xids)? {
            return Ok(());
        }
    } else {
        snap_build_serialize(builder, lsn)?;
    }

    // Update range of interesting xids; xmax is only advanced on catalog
    // commits (SnapBuildCommitTxn).
    builder.xmin = running.oldestRunningXid;

    // remove transactions we no longer need to track
    snap_build_purge_older_txn(builder);

    // advance the slot's xmin limit
    let mut xmin = rb::ReorderBufferGetOldestXmin::call(builder.reorder);
    if xmin == InvalidTransactionId {
        xmin = running.oldestRunningXid;
    }
    elog(
        DEBUG3,
        alloc::format!(
            "xmin: {}, xmax: {}, oldest running: {}, oldest xmin: {}",
            builder.xmin, builder.xmax, running.oldestRunningXid, xmin
        ),
    );
    logical::logical_increase_xmin_for_slot::call(lsn, xmin)?;

    // tell the slot where we can restart decoding from, but only once we're
    // consistent (we don't know a serialized snapshot's location otherwise).
    if builder.state < SNAPBUILD_CONSISTENT {
        return Ok(());
    }

    match rb::ReorderBufferGetOldestTXN::call(builder.reorder) {
        Some(txn) => {
            let restart = rb::reorder_buffer_txn_restart_decoding_lsn::call(builder.reorder, txn);
            if restart != InvalidXLogRecPtr {
                logical::logical_increase_restart_decoding_for_slot::call(lsn, restart)?;
            }
        }
        None => {
            // no in-progress txn -> reuse the last serialized snapshot if any
            if rb::reorder_buffer_current_restart_decoding_lsn::call(builder.reorder)
                != InvalidXLogRecPtr
                && builder.last_serialized_snapshot != InvalidXLogRecPtr
            {
                logical::logical_increase_restart_decoding_for_slot::call(
                    lsn,
                    builder.last_serialized_snapshot,
                )?;
            }
        }
    }

    Ok(())
}

/// `SnapBuildFindSnapshot` (snapbuild.c:1237). Returns true if there is a point
/// in performing internal maintenance/cleanup using the record.
fn snap_build_find_snapshot(
    builder: &mut SnapBuild,
    lsn: XLogRecPtr,
    running: &xl_running_xacts,
    running_xids: &[TransactionId],
) -> PgResult<bool> {
    // record older than what we can use
    if TransactionIdIsNormal(builder.initial_xmin_horizon)
        && normal_transaction_id_precedes(running.oldestRunningXid, builder.initial_xmin_horizon)
    {
        elog(
            DEBUG1,
            alloc::format!(
                "skipping snapshot at {} while building logical decoding snapshot, xmin horizon too low",
                lsn_str(lsn)
            ),
        );
        snap_build_wait_snapshot(running, running_xids, builder.initial_xmin_horizon)?;
        return Ok(true);
    }

    // a) no transactions running -> jump straight to consistent
    if running.oldestRunningXid == running.nextXid {
        if builder.start_decoding_at == InvalidXLogRecPtr || builder.start_decoding_at <= lsn {
            builder.start_decoding_at = lsn + 1;
        }

        builder.xmin = running.nextXid; // < are finished
        builder.xmax = running.nextXid; // >= are running

        debug_assert!(TransactionIdIsNormal(builder.xmin));
        debug_assert!(TransactionIdIsNormal(builder.xmax));

        builder.state = SNAPBUILD_CONSISTENT;
        builder.next_phase_at = InvalidTransactionId;

        elog(
            LOG,
            alloc::format!(
                "logical decoding found consistent point at {}\nThere are no running transactions.",
                lsn_str(lsn)
            ),
        );
        return Ok(false);
    }
    // b) valid on-disk state, and neither building full snapshot nor creating
    //    a slot.
    else if !builder.building_full_snapshot
        && !builder.in_slot_creation
        && snap_build_restore(builder, lsn)?
    {
        // there won't be any state to cleanup
        return Ok(false);
    }
    // c) START -> BUILDING_SNAPSHOT
    else if builder.state == SNAPBUILD_START {
        builder.state = SNAPBUILD_BUILDING_SNAPSHOT;
        builder.next_phase_at = running.nextXid;

        builder.xmin = running.nextXid; // < are finished
        builder.xmax = running.nextXid; // >= are running

        debug_assert!(TransactionIdIsNormal(builder.xmin));
        debug_assert!(TransactionIdIsNormal(builder.xmax));

        elog(
            LOG,
            alloc::format!(
                "logical decoding found initial starting point at {}\nWaiting for transactions (approximately {}) older than {} to end.",
                lsn_str(lsn), running.xcnt, running.nextXid
            ),
        );

        snap_build_wait_snapshot(running, running_xids, running.nextXid)?;
    }
    // c) BUILDING_SNAPSHOT -> FULL_SNAPSHOT
    else if builder.state == SNAPBUILD_BUILDING_SNAPSHOT
        && TransactionIdPrecedesOrEquals(builder.next_phase_at, running.oldestRunningXid)
    {
        builder.state = SNAPBUILD_FULL_SNAPSHOT;
        builder.next_phase_at = running.nextXid;

        elog(
            LOG,
            alloc::format!(
                "logical decoding found initial consistent point at {}\nWaiting for transactions (approximately {}) older than {} to end.",
                lsn_str(lsn), running.xcnt, running.nextXid
            ),
        );

        snap_build_wait_snapshot(running, running_xids, running.nextXid)?;
    }
    // c) FULL_SNAPSHOT -> CONSISTENT
    else if builder.state == SNAPBUILD_FULL_SNAPSHOT
        && TransactionIdPrecedesOrEquals(builder.next_phase_at, running.oldestRunningXid)
    {
        builder.state = SNAPBUILD_CONSISTENT;
        builder.next_phase_at = InvalidTransactionId;

        elog(
            LOG,
            alloc::format!(
                "logical decoding found consistent point at {}\nThere are no old transactions anymore.",
                lsn_str(lsn)
            ),
        );
    }

    // we already started tracking and need to wait for in-progress ones; fall
    // through to the normal processing so cleanup can be performed.
    Ok(true)
}

/// `SnapBuildWaitSnapshot` (snapbuild.c:1434).
fn snap_build_wait_snapshot(
    running: &xl_running_xacts,
    running_xids: &[TransactionId],
    cutoff: TransactionId,
) -> PgResult<()> {
    for off in 0..(running.xcnt as usize) {
        let xid = running_xids[off];

        // upper layers should prevent waiting on ourselves
        if xact::transaction_id_is_current_transaction_id::call(xid) {
            return Err(elog_err("waiting for ourselves"));
        }

        if TransactionIdFollows(xid, cutoff) {
            continue;
        }

        lmgr::xact_lock_table_wait::call(
            xid,
            String::new(),
            types_tuple::heaptuple::ItemPointerData::default(),
            types_storage::lock::XLTW_Oper::None,
        )?;
    }

    // try to ensure another xl_running_xacts record is logged promptly.
    if !recovery_in_progress()? {
        log_standby_snapshot()?;
    }
    Ok(())
}

// ===========================================================================
// Serialization point / Serialize / Restore / RestoreSnapshot / RestoreContents
// ===========================================================================

/// `SnapBuildSerializationPoint` (snapbuild.c:1483).
pub fn snap_build_serialization_point(builder: &mut SnapBuild, lsn: XLogRecPtr) -> PgResult<()> {
    if builder.state < SNAPBUILD_CONSISTENT {
        snap_build_restore(builder, lsn)?;
    } else {
        snap_build_serialize(builder, lsn)?;
    }
    Ok(())
}

/// `SnapBuildSerialize` (snapbuild.c:1496).
fn snap_build_serialize(builder: &mut SnapBuild, lsn: XLogRecPtr) -> PgResult<()> {
    debug_assert!(lsn != InvalidXLogRecPtr);
    debug_assert!(
        builder.last_serialized_snapshot == InvalidXLogRecPtr
            || builder.last_serialized_snapshot <= lsn
    );

    // no point serializing if we cannot continue immediately after restoring
    if builder.state < SNAPBUILD_CONSISTENT {
        return Ok(());
    }

    // consistent snapshots have no next phase
    debug_assert_eq!(builder.next_phase_at, InvalidTransactionId);

    let path = alloc::format!("{}/{}.snap", PG_LOGICAL_SNAPSHOTS_DIR, snap_file_lsn(lsn));

    // check whether some other backend already wrote the snapshot for this LSN
    let exists = fd::stat_file::call(&path, true)?;
    if exists.is_some() {
        // somebody else already serialized; repeat the fsync to be safe.
        fd::fsync_fname::call(&path, false)?;
        fd::fsync_fname::call(PG_LOGICAL_SNAPSHOTS_DIR, true)?;
        builder.last_serialized_snapshot = lsn;
        rb::ReorderBufferSetRestartPoint::call(builder.reorder, builder.last_serialized_snapshot);
        return Ok(());
    }

    elog(DEBUG1, alloc::format!("serializing snapshot to {}", path));

    // include pid so only we write to this tempfile
    let tmppath = alloc::format!(
        "{}/{}.snap.{}.tmp",
        PG_LOGICAL_SNAPSHOTS_DIR,
        snap_file_lsn(lsn),
        my_proc_pid()
    );

    // Unlink temp file if it already exists.
    let r = fd::unlink_file::call(&tmppath);
    if r != 0 && -r != ENOENT {
        return Err(file_access_err(
            alloc::format!("could not remove file \"{}\"", tmppath),
            -r,
        ));
    }

    // Get the catalog modifying transactions that are not yet committed.
    let catchange_xip = rb::ReorderBufferGetCatalogChangesXacts::call(builder.reorder);
    let catchange_xcnt = rb::reorder_buffer_catchange_count::call(builder.reorder);

    // Build the on-disk image (header + builder scalars + committed + catchange).
    let image = ondisk::serialize(builder, catchange_xcnt, &catchange_xip);

    // open tempfile, write, fsync, rename, fsync dir
    let fd_no = fd::open_transient_file::call(
        &tmppath,
        libc_flags::O_CREAT | libc_flags::O_EXCL | libc_flags::O_WRONLY | libc_flags::PG_BINARY,
    );
    if fd_no < 0 {
        return Err(file_access_err(
            alloc::format!("could not open file \"{}\"", tmppath),
            -fd_no,
        ));
    }

    waitevent_start(WAIT_EVENT_SNAPBUILD_WRITE);
    let written = fd::transient_write::call(fd_no, &image);
    waitevent_end();
    if written != image.len() as isize {
        let save_errno = if written < 0 { (-written) as i32 } else { 0 };
        fd::close_transient_file::call(fd_no);
        let errno = if save_errno != 0 { save_errno } else { 28 /* ENOSPC */ };
        return Err(file_access_err(
            alloc::format!("could not write to file \"{}\"", tmppath),
            errno,
        ));
    }

    // fsync the file before renaming
    waitevent_start(WAIT_EVENT_SNAPBUILD_SYNC);
    let fsync_ret = fd::pg_fsync::call(fd_no);
    waitevent_end();
    if fsync_ret != 0 {
        let save_errno = -fsync_ret;
        fd::close_transient_file::call(fd_no);
        return Err(file_access_err(
            alloc::format!("could not fsync file \"{}\"", tmppath),
            save_errno,
        ));
    }

    let close_ret = fd::close_transient_file::call(fd_no);
    if close_ret != 0 {
        return Err(file_access_err(
            alloc::format!("could not close file \"{}\"", tmppath),
            -close_ret,
        ));
    }

    fd::fsync_fname::call(PG_LOGICAL_SNAPSHOTS_DIR, true)?;

    // rename into place (may overwrite another backend's identical work)
    let ren = fd::rename_file::call(&tmppath, &path);
    if ren != 0 {
        return Err(file_access_err(
            alloc::format!("could not rename file \"{}\" to \"{}\"", tmppath, path),
            -ren,
        ));
    }

    // make sure we persist
    fd::fsync_fname::call(&path, false)?;
    fd::fsync_fname::call(PG_LOGICAL_SNAPSHOTS_DIR, true)?;

    builder.last_serialized_snapshot = lsn;

    rb::ReorderBufferSetRestartPoint::call(builder.reorder, builder.last_serialized_snapshot);
    Ok(())
}

/// `SnapBuildRestoreSnapshot` (snapbuild.c:1741) — read+validate the on-disk
/// snapshot for `lsn` into an owned [`ondisk::OnDisk`]. `missing_ok` mirrors C.
pub fn snap_build_restore_snapshot(
    lsn: XLogRecPtr,
    missing_ok: bool,
) -> PgResult<Option<ondisk::OnDisk>> {
    let path = alloc::format!("{}/{}.snap", PG_LOGICAL_SNAPSHOTS_DIR, snap_file_lsn(lsn));

    let fd_no = fd::open_transient_file::call(&path, libc_flags::O_RDONLY | libc_flags::PG_BINARY);
    if fd_no < 0 {
        let errno = -fd_no;
        if missing_ok && errno == ENOENT {
            return Ok(None);
        }
        return Err(file_access_err(
            alloc::format!("could not open file \"{}\"", path),
            errno,
        ));
    }

    // Make sure the snapshot had been stored safely to disk.
    fd::fsync_fname::call(&path, false)?;
    fd::fsync_fname::call(PG_LOGICAL_SNAPSHOTS_DIR, true)?;

    // Read the whole file (the C reads in sections; reading all then parsing is
    // equivalent because the file is written atomically and fully).
    let bytes = match read_all_transient(fd_no, &path) {
        Ok(b) => b,
        Err(e) => {
            fd::close_transient_file::call(fd_no);
            return Err(e);
        }
    };

    if fd::close_transient_file::call(fd_no) != 0 {
        return Err(file_access_err(
            alloc::format!("could not close file \"{}\"", path),
            fd::last_errno::call(),
        ));
    }

    ondisk::deserialize(&bytes, &path).map(Some)
}

/// `SnapBuildRestore` (snapbuild.c:1840).
fn snap_build_restore(builder: &mut SnapBuild, lsn: XLogRecPtr) -> PgResult<bool> {
    // no point loading a snapshot if we're already there
    if builder.state == SNAPBUILD_CONSISTENT {
        return Ok(false);
    }

    let ondisk = match snap_build_restore_snapshot(lsn, true)? {
        Some(o) => o,
        None => return Ok(false),
    };

    // only interested in consistent snapshots
    if ondisk.state < SNAPBUILD_CONSISTENT {
        return Ok(false);
    }

    // don't use a snapshot whose xmin we cannot guarantee
    if TransactionIdPrecedes(ondisk.xmin, builder.initial_xmin_horizon) {
        return Ok(false);
    }

    // consistent snapshots have no next phase
    debug_assert_eq!(ondisk.next_phase_at, InvalidTransactionId);
    builder.next_phase_at = InvalidTransactionId;

    // copy over everything important
    builder.xmin = ondisk.xmin;
    builder.xmax = ondisk.xmax;
    builder.state = ondisk.state;

    builder.committed.xcnt = ondisk.committed_xip.len();
    if builder.committed.xcnt > 0 {
        builder.committed.xcnt_space = ondisk.committed_xip.len();
        builder.committed.xip = ondisk.committed_xip;
    }

    builder.catchange.xcnt = ondisk.catchange_xip.len();
    builder.catchange.xip = ondisk.catchange_xip;

    // our snapshot is not interesting anymore, build a new one
    if builder.snapshot.is_some() {
        snap_build_snap_dec_refcount(builder);
    }
    let snap = snap_build_build_snapshot(builder);
    builder.snapshot = Some(RefcountedSnapshot { snap });
    snap_build_snap_inc_refcount(builder);

    rb::ReorderBufferSetRestartPoint::call(builder.reorder, lsn);

    debug_assert_eq!(builder.state, SNAPBUILD_CONSISTENT);

    elog(
        LOG,
        alloc::format!(
            "logical decoding found consistent point at {}\nLogical decoding will begin using saved snapshot.",
            lsn_str(lsn)
        ),
    );
    Ok(true)
}

// ===========================================================================
// CheckPointSnapBuild / SnapBuildSnapshotExists
// ===========================================================================

/// `CheckPointSnapBuild` (snapbuild.c:1969).
pub fn check_point_snap_build() -> PgResult<()> {
    // minimum of the last redo pointer
    let mut cutoff = slot::replication_slots_compute_logical_restart_lsn::call()?;
    let redo = xlog::get_redo_rec_ptr::call();

    if redo < cutoff {
        cutoff = redo;
    }

    const PGFILETYPE_ERROR: i32 = 0;
    const PGFILETYPE_REG: i32 = 2;

    let names = fd::read_dir_names_logged::call(PG_LOGICAL_SNAPSHOTS_DIR);
    for name in names {
        if name == "." || name == ".." {
            continue;
        }

        let path = alloc::format!("{}/{}", PG_LOGICAL_SNAPSHOTS_DIR, name);
        let de_type = fd::get_dirent_type::call(&path);

        if de_type != PGFILETYPE_ERROR && de_type != PGFILETYPE_REG {
            elog(DEBUG1, alloc::format!("only regular files expected: {}", path));
            continue;
        }

        // parse "%X-%X.snap"
        let lsn = match parse_snap_lsn(&name) {
            Some(l) => l,
            None => {
                elog(LOG, alloc::format!("could not parse file name \"{}\"", path));
                continue;
            }
        };

        if lsn < cutoff || cutoff == InvalidXLogRecPtr {
            elog(DEBUG1, alloc::format!("removing snapbuild snapshot {}", path));
            if fd::unlink_file::call(&path) < 0 {
                elog(LOG, alloc::format!("could not remove file \"{}\"", path));
                continue;
            }
        }
    }
    Ok(())
}

/// `SnapBuildSnapshotExists` (snapbuild.c:2057).
pub fn snap_build_snapshot_exists(lsn: XLogRecPtr) -> bool {
    let path = alloc::format!("{}/{}.snap", PG_LOGICAL_SNAPSHOTS_DIR, snap_file_lsn(lsn));
    // stat(); ENOENT -> not exists; other errors would ereport in C but the
    // inward seam is declared infallible, so a stat error downgrades to false.
    matches!(fd::stat_file::call(&path, true), Ok(Some(_)))
}

// ===========================================================================
// Small helpers
// ===========================================================================

/// `%X-%X` for the snapshot filename's LSN portion.
fn snap_file_lsn(lsn: XLogRecPtr) -> String {
    alloc::format!("{:X}-{:X}", (lsn >> 32) as u32, lsn as u32)
}

/// Parse `"%X-%X.snap"` into an `XLogRecPtr`, or `None` if it doesn't match.
fn parse_snap_lsn(name: &str) -> Option<XLogRecPtr> {
    let stem = name.strip_suffix(".snap")?;
    let (hi, lo) = stem.split_once('-')?;
    let hi = u32::from_str_radix(hi, 16).ok()?;
    let lo = u32::from_str_radix(lo, 16).ok()?;
    Some(((hi as u64) << 32) | lo as u64)
}

/// `elog(ERROR, msg)` as a `PgError`.
fn elog_err(msg: impl Into<String>) -> PgError {
    PgError::new(ERROR, msg.into())
}

/// `ereport(ERROR, (errcode_for_file_access(), errmsg(...)))` — file error at
/// ERROR with the saved errno rendered into `%m`.
fn file_access_err(message: String, errno: i32) -> PgError {
    let msg = alloc::format!("{}: {}", message, errno_str(errno));
    PgError::new(ERROR, msg)
        .with_sqlstate(ERRCODE_DATA_CORRUPTED)
        .with_saved_errno(errno)
}

fn errno_str(errno: i32) -> String {
    alloc::format!("errno {}", errno)
}

/// Read the entire remaining contents of a transient fd (the C reads in fixed
/// sections; we read it all and parse).
fn read_all_transient(fd_no: i32, path: &str) -> PgResult<Vec<u8>> {
    let mut out: Vec<u8> = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        waitevent_start(WAIT_EVENT_SNAPBUILD_READ);
        let n = fd::transient_read::call(fd_no, &mut buf);
        waitevent_end();
        if n < 0 {
            return Err(file_access_err(
                alloc::format!("could not read file \"{}\"", path),
                (-n) as i32,
            ));
        }
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n as usize]);
    }
    Ok(out)
}

// CRC32C wrappers mirroring the INIT/COMP/FIN/EQ macros over the seam.
fn crc32c(data: &[u8]) -> u32 {
    // The seam's comp_crc32c initializes (0xFFFFFFFF) and finalizes (XOR)
    // internally, so a single call over the whole region yields the final CRC.
    crc::comp_crc32c::call(0, data)
}

fn waitevent_start(event: u32) {
    backend_utils_activity_waitevent_seams::pgstat_report_wait_start::call(event);
}
fn waitevent_end() {
    backend_utils_activity_waitevent_seams::pgstat_report_wait_end::call();
}

fn my_proc_pid() -> i32 {
    // The pid is only used to make the tempfile name unique; the slot seam
    // exposes the backend's MyProcPid when available.
    miscinit::my_proc_pid::call()
}

fn recovery_in_progress() -> PgResult<bool> {
    backend_replication_logical_origin_extern_seams::RecoveryInProgress::call()
}

fn log_standby_snapshot() -> PgResult<()> {
    xlog::log_standby_snapshot::call().map(|_| ())
}

/// `GetOldestSafeDecodingTransactionId(false)` under `ProcArrayLock` (the
/// procarray seam takes the lock internally).
fn procarray_get_oldest_safe_decoding_xid_locked() -> TransactionId {
    backend_storage_ipc_procarray_seams::GetOldestSafeDecodingTransactionId::call(false)
}

mod libc_flags {
    pub const O_RDONLY: i32 = 0;
    pub const O_WRONLY: i32 = 1;
    pub const O_CREAT: i32 = 0o100;
    pub const O_EXCL: i32 = 0o200;
    /// `PG_BINARY` is 0 on POSIX.
    pub const PG_BINARY: i32 = 0;
}

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install the inward seams this unit owns (across the two snapbuild seam
/// crates). The handle layer ([`registry`]) resolves a [`SnapBuildHandle`] to
/// its live [`SnapBuild`].
pub fn init_seams() {
    registry::init_seams();

    // Inward seam consumed by the WAL sender's replication-command entry:
    // `exec_replication_command` clears any exported snapshot at the top.
    // (`snap_build_snapshot_exists` is already installed by `registry::init_seams`.)
    backend_replication_snapbuild_seams::snap_build_clear_exported_snapshot::set(
        snap_build_clear_exported_snapshot,
    );
}

#[cfg(test)]
mod tests;
