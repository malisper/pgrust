//! Seam declarations for the `backend-access-common-toast-internals` unit
//! (`access/common/toast_internals.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Relations cross as their `Oid` (the relcache
//! resolves the OID back to the live entry).

seam_core::seam!(
    /// `toast_delete_external(rel, values, isnull, is_speculative)`
    /// (toast_internals.c): check for external stored attributes and delete
    /// them from the secondary relation. `values`/`isnull` are the deformed
    /// columns of the tuple being deleted. `Err` carries the toast-table
    /// scan/delete `ereport(ERROR)`s.
    pub fn toast_delete_external(
        rel: types_core::Oid,
        values: &[types_tuple::backend_access_common_heaptuple::TupleValue<'_>],
        isnull: &[bool],
        is_speculative: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `toast_open_indexes(toastrel, lock, &toastidxs, &num_indexes)`
    /// (toast_internals.c): open the toast relation's indexes (returned as
    /// their OIDs, allocated in `mcx` — C palloc's the `Relation` array in the
    /// current context) and return `(toastidxs, validIndex)`, the position of
    /// the valid index to use. `Err` carries
    /// `elog(ERROR, "no valid index found for toast relation ...")` and the
    /// index-open error surface.
    pub fn toast_open_indexes<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        toastrel: types_core::Oid,
        lock: types_storage::lock::LOCKMODE,
    ) -> types_error::PgResult<(mcx::PgVec<'mcx, types_core::Oid>, i32)>
);

seam_core::seam!(
    /// `toast_close_indexes(toastidxs, num_indexes, lock)`
    /// (toast_internals.c): close the indexes opened by `toast_open_indexes`
    /// (the slice carries `num_indexes`; the caller drops its OID vector, C's
    /// `pfree(toastidxs)`). `Err` carries the relation-close error surface.
    pub fn toast_close_indexes(
        toastidxs: &[types_core::Oid],
        lock: types_storage::lock::LOCKMODE,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `get_toast_snapshot()` (toast_internals.c): the snapshot to use for
    /// reading toast data — the oldest registered/active snapshot. `Err`
    /// carries `elog(ERROR, "cannot fetch toast data without an active
    /// snapshot")`.
    pub fn get_toast_snapshot(
    ) -> types_error::PgResult<types_scan::snapshot::SnapshotHandle>
);
