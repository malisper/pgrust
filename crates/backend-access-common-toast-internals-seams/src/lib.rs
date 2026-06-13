//! Seam declarations for the `backend-access-common-toast-internals` unit
//! (`access/common/toast_internals.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Relations cross as borrows of the caller's open
//! `types_rel::RelationData` carriers; the open toast indexes are held
//! by [`ToastIndexesGuard`] (AGENTS.md "Locks and held resources": held
//! resources may never cross a `?` without a `Drop` guard).

use mcx::PgVec;
use types_error::PgResult;
use types_rel::RelationData;
use types_storage::lock::LOCKMODE;

seam_core::seam!(
    /// `toast_delete_external(rel, values, isnull, is_speculative)`
    /// (toast_internals.c): check for external stored attributes and delete
    /// them from the secondary relation. `values`/`isnull` are the deformed
    /// columns of the tuple being deleted. `Err` carries the toast-table
    /// scan/delete `ereport(ERROR)`s.
    pub fn toast_delete_external(
        rel: &types_rel::RelationData<'_>,
        values: &[types_tuple::backend_access_common_heaptuple::TupleValue<'_>],
        isnull: &[bool],
        is_speculative: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `toast_open_indexes(toastrel, lock, &toastidxs, &num_indexes)`
    /// (toast_internals.c): open the toast relation's indexes with
    /// `index_open(_, lock)` (the carriers are allocated in `mcx` — C
    /// palloc's the `Relation` array in the current context) and find the
    /// valid index to use. The opened indexes come back held by a
    /// [`ToastIndexesGuard`]; C's separate `validIndex` return is the
    /// guard's [`ToastIndexesGuard::valid_index`]. `Err` carries
    /// `elog(ERROR, "no valid index found for toast relation ...")` and the
    /// index-open error surface.
    pub fn toast_open_indexes<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        toastrel: &types_rel::RelationData<'_>,
        lock: types_storage::lock::LOCKMODE,
    ) -> types_error::PgResult<ToastIndexesGuard<'mcx>>
);

seam_core::seam!(
    /// `toast_close_indexes(toastidxs, num_indexes, lock)`
    /// (toast_internals.c): close (`index_close`) the indexes opened by
    /// [`toast_open_indexes`], consuming the carriers (C's `pfree` of the
    /// array is the vector drop). Reached only through [`ToastIndexesGuard`]
    /// (`close()` or `Drop`); consumers never call it directly. `Err`
    /// carries the index-close error surface.
    pub fn toast_close_indexes<'mcx>(
        toastidxs: mcx::PgVec<'mcx, types_rel::RelationData<'mcx>>,
        lock: types_storage::lock::LOCKMODE,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `toast_compress_datum(value, cmethod)` (toast_internals.c): try to
    /// compress the varlena `value` (its verbatim on-disk bytes, header
    /// included) with the given compression method (`InvalidCompressionMethod`
    /// == -1 means use the type default). Returns `Some(compressed bytes in
    /// mcx)` on a worthwhile compression, or `None` (C's
    /// `PointerGetDatum(NULL)`) when compression did not shrink the value
    /// enough. `Err` carries OOM and the unsupported-method `elog(ERROR)`.
    pub fn toast_compress_datum<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        value: &[u8],
        cmethod: i8,
    ) -> types_error::PgResult<Option<mcx::PgVec<'mcx, u8>>>
);

seam_core::seam!(
    /// `get_toast_snapshot()` (toast_internals.c): the snapshot to use for
    /// reading toast data (`&SnapshotToastData`, crossing as an owned
    /// trimmed copy). In C this reads the per-backend snapshot stacks via
    /// `HaveRegisteredOrActiveSnapshot()`; per the no-ambient-global rule
    /// that value crosses as the explicit
    /// `have_registered_or_active_snapshot` parameter — the caller reads it
    /// off its snapshot-stack facet when the snapmgr owner lands. `Err`
    /// carries `elog(ERROR, "cannot fetch toast data without an active
    /// snapshot")`.
    pub fn get_toast_snapshot(
        have_registered_or_active_snapshot: bool,
    ) -> types_error::PgResult<types_snapshot::SnapshotData>
);

/// The held-resource token returned by [`toast_open_indexes`]: the open
/// (locked) toast indexes plus the position of the valid one. `Drop` closes
/// the indexes silently (the abort path); [`Self::close`] is the explicit
/// C `toast_close_indexes(toastidxs, num_indexes, lock)` call site,
/// surfacing its error.
#[derive(Debug)]
pub struct ToastIndexesGuard<'mcx> {
    indexes: Option<PgVec<'mcx, RelationData<'mcx>>>,
    lock: LOCKMODE,
    valid_index: usize,
}

impl<'mcx> ToastIndexesGuard<'mcx> {
    /// Wrap just-opened toast indexes. Called by the owner's installed
    /// implementation (and test fixtures); consumers only ever receive one.
    pub fn new(
        indexes: PgVec<'mcx, RelationData<'mcx>>,
        valid_index: usize,
        lock: LOCKMODE,
    ) -> Self {
        debug_assert!(valid_index < indexes.len());
        ToastIndexesGuard {
            indexes: Some(indexes),
            lock,
            valid_index,
        }
    }

    /// The open toast indexes (`*toastidxs`, length `*num_indexes`).
    pub fn indexes(&self) -> &[RelationData<'mcx>] {
        self.indexes
            .as_deref()
            .expect("ToastIndexesGuard already closed")
    }

    /// `(*toastidxs)[validIndex]` — the valid index found at open.
    pub fn valid_index(&self) -> &RelationData<'mcx> {
        &self.indexes()[self.valid_index]
    }

    /// `toast_close_indexes(toastidxs, num_indexes, lock)` at the C call
    /// site, consuming the guard.
    pub fn close(mut self) -> PgResult<()> {
        let indexes = self
            .indexes
            .take()
            .expect("ToastIndexesGuard closed twice");
        toast_close_indexes::call(indexes, self.lock)
    }
}

impl Drop for ToastIndexesGuard<'_> {
    fn drop(&mut self) {
        if let Some(indexes) = self.indexes.take() {
            // The abort path: close silently (C reaches the equivalent
            // releases through error-recovery resource cleanup).
            let _ = toast_close_indexes::call(indexes, self.lock);
        }
    }
}
