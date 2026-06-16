//! `backend-storage-file-fileset::sharedfileset` — a faithful port of
//! `src/backend/storage/file/sharedfileset.c`.
//!
//! SharedFileSets provide a temporary namespace (think directory) so that files
//! can be discovered by name, and a shared ownership semantics so that shared
//! files survive until the last user detaches.
//!
//! This is a thin layer over the sibling [`FileSet`](super) protocol
//! (`fileset.c`, ported in this same crate) plus reference-count bookkeeping in
//! the DSM-resident [`SharedFileSet`] and the DSM detach-callback machinery
//! (`storage/ipc/dsm.c`, reached on the `backend-storage-ipc-dsm-core` sibling).
//!
//! The C registers `on_dsm_detach(seg, SharedFileSetOnDetach,
//! PointerGetDatum(fileset))`: the callback's `Datum` arg carries a pointer to
//! the DSM-resident `SharedFileSet`, recovered on detach. The
//! [`SharedFileSet`] handed to these functions is a real `&mut` into the DSM
//! chunk (the parallel keystone resolves it from the in-segment address), so
//! its address is exactly the C `&pstate->fileset` pointer and is stable for the
//! lifetime of the segment — the same model `dsa.c`'s
//! `dsa_on_dsm_detach_release_in_place` uses for its in-place control object.

use backend_storage_ipc_dsm_core::dsm;
use backend_utils_mmgr_mcxt_seams::top_memory_context;
use types_datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE};
use types_execparallel::DsmSegmentHandle;
use types_storage::fileset::SharedFileSet;

/// The `DsmSegmentHandle` (a `dsm_segment *` carried as `usize`) carries the
/// real [`dsm::DsmSegmentId`] (opacity-inherited; the handle value *is*
/// `DsmSegmentId::as_u64()`), matching the bridge in
/// `backend-access-transam-parallel`.
fn seg_id_of(seg: DsmSegmentHandle) -> dsm::DsmSegmentId {
    dsm::DsmSegmentId::from_u64(seg.0 as u64)
}

/// `PointerGetDatum(fileset)` — carry the DSM-resident `SharedFileSet` pointer
/// as the detach callback's `Datum` arg.
fn pointer_get_datum(fileset: &SharedFileSet) -> Datum {
    Datum::from_usize(fileset as *const SharedFileSet as usize)
}

/// `(SharedFileSet *) DatumGetPointer(datum)` — recover the DSM-resident
/// `SharedFileSet` from the detach callback's `Datum` arg.
///
/// # Safety
/// `datum` was produced by [`pointer_get_datum`] over a `SharedFileSet` that
/// lives in the DSM segment now detaching; it is still mapped for the duration
/// of the callback (the C comment: "we are still actually attached for the rest
/// of this function so we can safely access its data").
fn datum_get_fileset<'a>(datum: Datum) -> &'a mut SharedFileSet {
    // SAFETY: see the doc-comment above — a live, aligned `*mut SharedFileSet`.
    unsafe { &mut *(datum.as_usize() as *mut SharedFileSet) }
}

/// Initialize a space for temporary files that can be opened by other backends.
/// (`SharedFileSetInit`, sharedfileset.c:37-50.)
///
/// Other backends must attach to it before accessing it. Associate this
/// `SharedFileSet` with `seg`. Any contained files will be deleted when the last
/// backend detaches.
pub fn SharedFileSetInit(fileset: &mut SharedFileSet, seg: DsmSegmentHandle) -> PgResult<()> {
    // Initialize the shared fileset specific members.
    //   SpinLockInit(&fileset->mutex);
    backend_storage_lmgr_s_lock::s_init_lock(&fileset.mutex);
    //   fileset->refcnt = 1;
    fileset.refcnt = 1;

    // Initialize the fileset.
    //   FileSetInit(&fileset->fs);
    super::FileSetInit(&mut fileset.fs)?;

    // Register our cleanup callback.
    //   if (seg) on_dsm_detach(seg, SharedFileSetOnDetach, PointerGetDatum(fileset));
    if seg.0 != 0 {
        let arg = pointer_get_datum(fileset);
        dsm::on_dsm_detach(
            seg_id_of(seg),
            SharedFileSetOnDetach,
            arg,
            top_memory_context::call(),
        )?;
    }
    Ok(())
}

/// Attach to a set of directories that was created with [`SharedFileSetInit`].
/// (`SharedFileSetAttach`, sharedfileset.c:55-77.)
pub fn SharedFileSetAttach(fileset: &mut SharedFileSet, seg: DsmSegmentHandle) -> PgResult<()> {
    let success;

    //   SpinLockAcquire(&fileset->mutex);
    backend_storage_lmgr_s_lock::s_lock_macro(&fileset.mutex, Some(file!()), line!() as i32, None);
    //   if (fileset->refcnt == 0) success = false;
    //   else { ++fileset->refcnt; success = true; }
    if fileset.refcnt == 0 {
        success = false;
    } else {
        fileset.refcnt += 1;
        success = true;
    }
    //   SpinLockRelease(&fileset->mutex);
    backend_storage_lmgr_s_lock::s_unlock(&fileset.mutex);

    //   if (!success) ereport(ERROR, ...);
    if !success {
        return Err(PgError::error(
            "could not attach to a SharedFileSet that is already destroyed",
        )
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
    }

    // Register our cleanup callback.
    //   on_dsm_detach(seg, SharedFileSetOnDetach, PointerGetDatum(fileset));
    let arg = pointer_get_datum(fileset);
    dsm::on_dsm_detach(
        seg_id_of(seg),
        SharedFileSetOnDetach,
        arg,
        top_memory_context::call(),
    )?;
    Ok(())
}

/// Delete all files in the set. (`SharedFileSetDeleteAll`, sharedfileset.c:82-86.)
pub fn SharedFileSetDeleteAll(fileset: &mut SharedFileSet) -> PgResult<()> {
    //   FileSetDeleteAll(&fileset->fs);
    super::FileSetDeleteAll(&fileset.fs)
}

/// Callback function that will be invoked when this backend detaches from a DSM
/// segment holding a `SharedFileSet` that it has created or attached to. If we
/// are the last to detach, then try to remove the directories and everything in
/// them. We can't raise an error on failures, because this runs in error cleanup
/// paths. (`SharedFileSetOnDetach`, sharedfileset.c:95-114.)
fn SharedFileSetOnDetach(_segment: dsm::DsmSegmentId, datum: Datum) -> PgResult<()> {
    let mut unlink_all = false;
    //   SharedFileSet *fileset = (SharedFileSet *) DatumGetPointer(datum);
    let fileset = datum_get_fileset(datum);

    //   SpinLockAcquire(&fileset->mutex);
    backend_storage_lmgr_s_lock::s_lock_macro(&fileset.mutex, Some(file!()), line!() as i32, None);
    //   Assert(fileset->refcnt > 0);
    debug_assert!(fileset.refcnt > 0);
    //   if (--fileset->refcnt == 0) unlink_all = true;
    fileset.refcnt -= 1;
    if fileset.refcnt == 0 {
        unlink_all = true;
    }
    //   SpinLockRelease(&fileset->mutex);
    backend_storage_lmgr_s_lock::s_unlock(&fileset.mutex);

    // If we are the last to detach, we delete the directory in all tablespaces.
    // Note that we are still actually attached for the rest of this function so
    // we can safely access its data.
    //   if (unlink_all) FileSetDeleteAll(&fileset->fs);
    if unlink_all {
        super::FileSetDeleteAll(&fileset.fs)?;
    }
    Ok(())
}
