//! Handle layer: the backend-local registry mapping each live
//! [`SnapBuildHandle`] to its owned [`SnapBuild`], and the inward seam
//! installers.
//!
//! logical.c / slotsync hold the builder only as the opaque `SnapBuildHandle`
//! (a `usize`) they forward through the inward seams; this crate is the owner
//! and resolves it to the real value. The registry is `thread_local!` because
//! the C `SnapBuild *` is per-backend (one decoding context per backend at a
//! time, but a backend may juggle several builders, so we key by handle).

extern crate alloc;

use core::cell::RefCell;

use ::types_core::primitive::{TransactionId, XLogRecPtr};
use types_logical::{ReorderBufferHandle, SnapBuildHandle};

use crate::{
    allocate_snapshot_builder, free_snapshot_builder, snap_build_commit_txn,
    snap_build_get_or_build_snapshot, snap_build_get_two_phase_at, snap_build_process_change,
    snap_build_process_new_cid, snap_build_process_running_xacts,
    snap_build_reset_exported_snapshot_state, snap_build_serialization_point,
    snap_build_snapshot_exists, snap_build_xact_needs_skip, SnapBuild,
};

::std::thread_local! {
    /// Backend-local table of live builders. A handle is `1 + slot index`
    /// (`0` is the C `NULL`, never handed out); freed slots become `None` and
    /// are reused.
    static BUILDERS: RefCell<alloc::vec::Vec<Option<SnapBuild>>> =
        const { RefCell::new(alloc::vec::Vec::new()) };
}

fn handle_to_index(h: SnapBuildHandle) -> usize {
    debug_assert!(h.0 != 0, "NULL SnapBuildHandle");
    h.0 - 1
}
fn index_to_handle(i: usize) -> SnapBuildHandle {
    SnapBuildHandle(i + 1)
}

/// Register a freshly built [`SnapBuild`] and hand back its handle.
fn register(builder: SnapBuild) -> SnapBuildHandle {
    BUILDERS.with(|b| {
        let mut tab = b.borrow_mut();
        if let Some(i) = tab.iter().position(|s| s.is_none()) {
            tab[i] = Some(builder);
            index_to_handle(i)
        } else {
            tab.push(Some(builder));
            index_to_handle(tab.len() - 1)
        }
    })
}

/// Run `f` against the live builder behind `handle` (the C dereferences the
/// `SnapBuild *` for the same span).
pub fn with_builder<R>(handle: SnapBuildHandle, f: impl FnOnce(&mut SnapBuild) -> R) -> R {
    BUILDERS.with(|b| {
        let mut tab = b.borrow_mut();
        let slot = tab
            .get_mut(handle_to_index(handle))
            .and_then(|s| s.as_mut())
            .expect("SnapBuildHandle refers to a freed/unknown builder");
        f(slot)
    })
}

/// Like [`with_builder`] but returns `None` if the handle is unknown/freed
/// (used where the C would tolerate a NULL).
pub fn with_builder_opt<R>(
    handle: SnapBuildHandle,
    f: impl FnOnce(&mut SnapBuild) -> R,
) -> Option<R> {
    BUILDERS.with(|b| {
        let mut tab = b.borrow_mut();
        tab.get_mut(handle_to_index(handle))
            .and_then(|s| s.as_mut())
            .map(f)
    })
}

// ---------------------------------------------------------------------------
// Inward seam adapters
// ---------------------------------------------------------------------------

/// `AllocateSnapshotBuilder(...)` — build and register, returning the handle.
fn seam_allocate(
    reorder: ReorderBufferHandle,
    xmin_horizon: TransactionId,
    start_lsn: XLogRecPtr,
    need_full_snapshot: bool,
    in_create: bool,
    two_phase_at: XLogRecPtr,
) -> SnapBuildHandle {
    let builder = allocate_snapshot_builder(
        reorder,
        xmin_horizon,
        start_lsn,
        need_full_snapshot,
        in_create,
        two_phase_at,
    );
    register(builder)
}

/// `FreeSnapshotBuilder(builder)` — run the C free checks, then drop the slot.
fn seam_free(handle: SnapBuildHandle) {
    // SnapBuildSnapDecRefcount error checks happen here while the slot is live.
    with_builder_opt(handle, |b| free_snapshot_builder(b));
    BUILDERS.with(|b| {
        let mut tab = b.borrow_mut();
        if let Some(slot) = tab.get_mut(handle_to_index(handle)) {
            *slot = None;
        }
    });
}

/// `SnapBuildCurrentState(builder)`.
fn seam_current_state(handle: SnapBuildHandle) -> i32 {
    with_builder(handle, |b| b.state)
}

/// `SnapBuildSetTwoPhaseAt(builder, lsn)`.
fn seam_set_two_phase_at(handle: SnapBuildHandle, lsn: XLogRecPtr) {
    with_builder(handle, |b| b.two_phase_at = lsn);
}

// ---------------------------------------------------------------------------
// decode.c entry points (change processing / snapshot generation). Each
// resolves the handle to the live builder and forwards to the (landed) owner
// function.
// ---------------------------------------------------------------------------

fn seam_process_change(handle: SnapBuildHandle, xid: TransactionId, lsn: XLogRecPtr) -> bool {
    with_builder(handle, |b| snap_build_process_change(b, xid, lsn))
}

fn seam_process_new_cid(
    handle: SnapBuildHandle,
    xid: TransactionId,
    lsn: XLogRecPtr,
    xlrec: xlog_records::heapam_xlog::xl_heap_new_cid,
) -> types_error::PgResult<()> {
    with_builder(handle, |b| snap_build_process_new_cid(b, xid, lsn, &xlrec))
}

fn seam_commit_txn(
    handle: SnapBuildHandle,
    lsn: XLogRecPtr,
    xid: TransactionId,
    subxacts: alloc::vec::Vec<TransactionId>,
    xinfo: u32,
) {
    with_builder(handle, |b| snap_build_commit_txn(b, lsn, xid, &subxacts, xinfo));
}

fn seam_process_running_xacts(
    handle: SnapBuildHandle,
    lsn: XLogRecPtr,
    running: xlog_records::standbydefs::xl_running_xacts,
    running_xids: alloc::vec::Vec<TransactionId>,
) -> types_error::PgResult<()> {
    with_builder(handle, |b| {
        snap_build_process_running_xacts(b, lsn, &running, &running_xids)
    })
}

fn seam_get_or_build_snapshot(handle: SnapBuildHandle) -> snapshot::SnapshotData {
    with_builder(handle, |b| snap_build_get_or_build_snapshot(b))
}

fn seam_xact_needs_skip(handle: SnapBuildHandle, ptr: XLogRecPtr) -> bool {
    with_builder(handle, |b| snap_build_xact_needs_skip(b, ptr))
}

fn seam_get_two_phase_at(handle: SnapBuildHandle) -> XLogRecPtr {
    with_builder(handle, |b| snap_build_get_two_phase_at(b))
}

fn seam_serialization_point(handle: SnapBuildHandle, lsn: XLogRecPtr) -> types_error::PgResult<()> {
    with_builder(handle, |b| snap_build_serialization_point(b, lsn))
}

/// Install the six inward seams this unit owns across the two seam crates.
pub fn init_seams() {
    logical_snapbuild_seams::AllocateSnapshotBuilder::set(seam_allocate);
    logical_snapbuild_seams::FreeSnapshotBuilder::set(seam_free);
    logical_snapbuild_seams::SnapBuildCurrentState::set(seam_current_state);
    logical_snapbuild_seams::SnapBuildSetTwoPhaseAt::set(seam_set_two_phase_at);
    logical_snapbuild_seams::snap_build_reset_exported_snapshot_state::set(
        snap_build_reset_exported_snapshot_state,
    );
    replication_snapbuild_seams::snap_build_snapshot_exists::set(snap_build_snapshot_exists);

    // decode.c entry points (change processing / snapshot generation).
    use logical_snapbuild_seams as s;
    s::SnapBuildProcessChange::set(seam_process_change);
    s::SnapBuildProcessNewCid::set(seam_process_new_cid);
    s::SnapBuildCommitTxn::set(seam_commit_txn);
    s::SnapBuildProcessRunningXacts::set(seam_process_running_xacts);
    s::SnapBuildGetOrBuildSnapshot::set(seam_get_or_build_snapshot);
    s::SnapBuildXactNeedsSkip::set(seam_xact_needs_skip);
    s::SnapBuildGetTwoPhaseAt::set(seam_get_two_phase_at);
    s::SnapBuildSerializationPoint::set(seam_serialization_point);
}
