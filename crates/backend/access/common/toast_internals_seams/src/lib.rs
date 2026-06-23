//! Seam declarations for the `backend-access-common-toast-internals` unit
//! (`access/common/toast_internals.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Relations cross as borrows of the caller's open
//! `::rel::RelationData` carriers; the open toast indexes are held
//! by [`ToastIndexesGuard`] (AGENTS.md "Locks and held resources": held
//! resources may never cross a `?` without a `Drop` guard).

use ::mcx::PgVec;
use ::types_error::PgResult;
use ::rel::{Relation, RelationData};
use ::types_storage::lock::LOCKMODE;

seam_core::seam!(
    /// `toast_delete_external(rel, values, isnull, is_speculative)`
    /// (toast_internals.c): check for external stored attributes and delete
    /// them from the secondary relation. `values`/`isnull` are the deformed
    /// columns of the tuple being deleted. `Err` carries the toast-table
    /// scan/delete `ereport(ERROR)`s.
    pub fn toast_delete_external(
        rel: &::rel::RelationData<'_>,
        values: &[types_tuple::heaptuple::Datum<'_>],
        isnull: &[bool],
        is_speculative: bool,
    ) -> ::types_error::PgResult<()>
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
        mcx: ::mcx::Mcx<'mcx>,
        toastrel: &::rel::RelationData<'_>,
        lock: ::types_storage::lock::LOCKMODE,
    ) -> ::types_error::PgResult<ToastIndexesGuard<'mcx>>
);

seam_core::seam!(
    /// `toast_close_indexes(toastidxs, num_indexes, lock)`
    /// (toast_internals.c): close (`index_close`) the indexes opened by
    /// [`toast_open_indexes`], consuming the carriers (C's `pfree` of the
    /// array is the vector drop). Reached only through [`ToastIndexesGuard`]
    /// (`close()` or `Drop`); consumers never call it directly. `Err`
    /// carries the index-close error surface.
    pub fn toast_close_indexes<'mcx>(
        toastidxs: ::mcx::PgVec<'mcx, ::rel::Relation<'mcx>>,
        lock: ::types_storage::lock::LOCKMODE,
    ) -> ::types_error::PgResult<()>
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
        mcx: ::mcx::Mcx<'mcx>,
        value: &[u8],
        cmethod: i8,
    ) -> ::types_error::PgResult<Option<::mcx::PgVec<'mcx, u8>>>
);

seam_core::seam!(
    /// `toast_save_datum(rel, value, oldexternal, options)` (toast_internals.c):
    /// move a varlena `value` (its verbatim on-disk bytes, header included) out
    /// to the relation's TOAST table, chunking it across rows, and return the
    /// new on-disk-external TOAST pointer image. `rel` is the heap relation that
    /// owns the value (crosses as its OID; the relcache resolves the live entry,
    /// from which `rd_rel->reltoastrelid` is read). `oldexternal` is the column's
    /// prior external pointer image (`tai_oldexternal`), used to reuse the same
    /// TOAST value OID when re-saving, or `None`. `Err` carries the toast-table
    /// insert / index `ereport(ERROR)` surface and OOM.
    pub fn toast_save_datum<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        rel: types_core::primitive::Oid,
        value: &[u8],
        oldexternal: Option<&[u8]>,
        options: i32,
    ) -> ::types_error::PgResult<::mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `toast_delete_datum(rel, value, is_speculative)` (toast_internals.c):
    /// delete a single on-disk-external TOAST datum's chunks from the relation's
    /// TOAST table. `rel` is the heap relation that owns the value (crosses as
    /// its OID). `value` is the verbatim external pointer image. `Err` carries
    /// the toast-table scan/delete `ereport(ERROR)` surface.
    pub fn toast_delete_datum(
        rel: types_core::primitive::Oid,
        value: &[u8],
        is_speculative: bool,
    ) -> ::types_error::PgResult<()>
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
    ) -> ::types_error::PgResult<snapshot::SnapshotData>
);

seam_core::seam!(
    /// `toast_fetch_datum(attr)` (access/common/detoast.c, static): reconstruct
    /// an in-memory datum from the chunks saved in the TOAST relation —
    /// `table_open(va_toastrelid, AccessShareLock)` +
    /// `table_relation_fetch_toast_slice(..., 0, attrsize, result)` +
    /// `table_close`. `attr` is the verbatim on-disk TOAST-pointer datum
    /// bytes; the reassembled varlena comes back in `mcx` (C palloc's it).
    /// Decompression is left to the caller. `Err` carries the toast-fetch
    /// `ereport(ERROR)`s (`missing chunk number ...`, etc.). The chunk
    /// reassembly is the TOAST-relation I/O the toast-internals subsystem
    /// owns, so detoast reaches it across the cycle through this seam.
    pub fn toast_fetch_datum<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        attr: &[u8],
    ) -> ::types_error::PgResult<::mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `toast_fetch_datum_slice(attr, sliceoffset, slicelength)`
    /// (access/common/detoast.c, static): reconstruct a segment of a datum
    /// from the chunks saved in the TOAST relation. Supports non-compressed
    /// external datums and compressed external datums (in which case the
    /// requested slice must be a prefix, i.e. `sliceoffset == 0`). The result
    /// comes back in `mcx`. `Err` carries the toast-fetch `ereport(ERROR)`s.
    pub fn toast_fetch_datum_slice<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        attr: &[u8],
        sliceoffset: i32,
        slicelength: i32,
    ) -> ::types_error::PgResult<::mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `VARATT_EXTERNAL_GET_POINTER(redirect, attr); redirect.pointer`
    /// (access/common/detoast.c): dereference a `VARATT_IS_EXTERNAL_INDIRECT`
    /// datum to the in-memory `struct varlena *` it points at, returning a
    /// `mcx` copy of that target's verbatim bytes. The indirect pointer is a
    /// raw in-memory pointer only the writer that built it can follow, so the
    /// dereference crosses this seam. `Err` carries OOM.
    pub fn indirect_pointer<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        attr: &[u8],
    ) -> ::types_error::PgResult<::mcx::PgVec<'mcx, u8>>
);

/// The held-resource token returned by [`toast_open_indexes`]: the open
/// (locked) toast index relations plus the position of the valid one.
///
/// The indexes are held as their RAII [`Relation`] handles — exactly the
/// carriers `index_open` returns — so closing (`index_close`) is the handle's
/// `Drop`, matching the repo's relation-open/auto-close pattern. C's separate
/// `toast_close_indexes(toastidxs, num_indexes, lock)` reduces to dropping the
/// vector; [`Self::close`] is the explicit C call site (surfacing the
/// close-error surface, currently infallible), `Drop` is the abort path. The
/// `indexes()` / `valid_index()` accessors deref to `&RelationData`, which is
/// what `systable_beginscan_ordered` and the consumers consume.
#[derive(Debug)]
pub struct ToastIndexesGuard<'mcx> {
    indexes: Option<PgVec<'mcx, Relation<'mcx>>>,
    lock: LOCKMODE,
    valid_index: usize,
}

impl<'mcx> ToastIndexesGuard<'mcx> {
    /// Wrap just-opened toast indexes (the `index_open` handles). Called by the
    /// owner's installed implementation (and test fixtures); consumers only
    /// ever receive one.
    pub fn new(
        indexes: PgVec<'mcx, Relation<'mcx>>,
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

    /// The open toast index relations (`*toastidxs`, length `*num_indexes`).
    pub fn indexes(&self) -> &[Relation<'mcx>] {
        self.indexes
            .as_deref()
            .expect("ToastIndexesGuard already closed")
    }

    /// `(*toastidxs)[validIndex]` — the valid index found at open, as its
    /// `RelationData` (via the handle's `Deref`).
    pub fn valid_index(&self) -> &RelationData<'mcx> {
        &self.indexes()[self.valid_index]
    }

    /// `toast_close_indexes(toastidxs, num_indexes, lock)` at the C call site,
    /// consuming the guard. The handles' `Drop` is the `index_close`.
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

// ---------------------------------------------------------------------------
// Indirect-TOAST-pointer target registry (varatt_indirect.pointer stand-in)
// ---------------------------------------------------------------------------
//
// C's `VARTAG_INDIRECT` external datum carries a raw in-memory `struct varlena
// *` (`varatt_indirect.pointer`) at the referent target, which lives in
// `TopTransactionContext`; the producer (`make_tuple_indirect`) builds it and
// the detoast `indirect_pointer` dereference follows the bare pointer. In this
// port a composite Datum crosses the fmgr boundary as a *serialized byte image*
// (`RefPayload::Composite`), so a raw process address embedded in those bytes
// is meaningless once the image is copied/reconstructed.
//
// This registry is the faithful stand-in: the producer stores the target
// varlena's verbatim bytes here and embeds a stable `u64` *token* (not an
// address) into the indirect datum's `varatt_indirect`-payload slot; the
// `indirect_pointer` dereference resolves the token back to those bytes. The
// registry is `thread_local` because, exactly like `TopTransactionContext`, it
// is per-backend; entries live for the backend's lifetime (a regression-test
// fixture produces a bounded handful), matching the "still lives later" intent
// of the C copy into `TopTransactionContext`.

use core::cell::RefCell;

thread_local! {
    /// Token -> target varlena bytes. The token is the 1-based vector index.
    static INDIRECT_TARGETS: RefCell<std::vec::Vec<std::vec::Vec<u8>>> =
        const { RefCell::new(std::vec::Vec::new()) };
}

/// Store a target varlena image and return its stable token (`>= 1`, so `0`
/// stays an obvious "unset" sentinel). C: copy the datum into
/// `TopTransactionContext` and stash its address in `varatt_indirect.pointer`.
pub fn register_indirect_target(bytes: &[u8]) -> u64 {
    INDIRECT_TARGETS.with(|t| {
        let mut t = t.borrow_mut();
        t.push(bytes.to_vec());
        t.len() as u64 // 1-based token
    })
}

/// Resolve an indirect-pointer token back to its target varlena bytes. C:
/// follow `varatt_indirect.pointer`. Returns `None` for an unknown token.
pub fn resolve_indirect_target(token: u64) -> Option<std::vec::Vec<u8>> {
    if token == 0 {
        return None;
    }
    INDIRECT_TARGETS.with(|t| t.borrow().get((token - 1) as usize).cloned())
}
