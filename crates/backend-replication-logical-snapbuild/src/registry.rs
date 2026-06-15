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

use types_core::primitive::{TransactionId, XLogRecPtr};
use types_logical::{ReorderBufferHandle, SnapBuildHandle};

use crate::{
    allocate_snapshot_builder, free_snapshot_builder, snap_build_reset_exported_snapshot_state,
    snap_build_snapshot_exists, SnapBuild,
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

/// Install the six inward seams this unit owns across the two seam crates.
pub fn init_seams() {
    backend_replication_logical_snapbuild_seams::AllocateSnapshotBuilder::set(seam_allocate);
    backend_replication_logical_snapbuild_seams::FreeSnapshotBuilder::set(seam_free);
    backend_replication_logical_snapbuild_seams::SnapBuildCurrentState::set(seam_current_state);
    backend_replication_logical_snapbuild_seams::SnapBuildSetTwoPhaseAt::set(seam_set_two_phase_at);
    backend_replication_logical_snapbuild_seams::snap_build_reset_exported_snapshot_state::set(
        snap_build_reset_exported_snapshot_state,
    );
    backend_replication_snapbuild_seams::snap_build_snapshot_exists::set(snap_build_snapshot_exists);
}
