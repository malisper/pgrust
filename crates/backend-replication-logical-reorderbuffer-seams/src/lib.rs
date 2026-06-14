//! Seam declarations for the `backend-replication-logical-reorderbuffer` unit
//! (`replication/logical/reorderbuffer.c`), as consumed by logical decoding.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use types_logical::{ReorderBufferHandle, ReorderBufferStats};

seam_core::seam!(
    /// `ResolveCminCmaxDuringDecoding(tuplecid_data, snapshot, htup, buffer,
    /// &cmin, &cmax)` (reorderbuffer.c) — look up the actual cmin/cmax for a
    /// tuple seen by a historic (logical-decoding) MVCC snapshot, resolving any
    /// combo CID via the decoded tuplecid hash. `cmin`/`cmax` carry the C
    /// in/out-parameter values; the returned [`ResolveCminCmaxResult`] bundles
    /// the C `bool` return with the resolved out-parameters.
    pub fn resolve_cmin_cmax_during_decoding(
        snapshot: types_snapshot::SnapshotData,
        htup: types_tuple::heaptuple::HeapTupleData<'_>,
        buffer: types_storage::storage::Buffer,
        cmin: types_core::CommandId,
        cmax: types_core::CommandId,
    ) -> types_error::PgResult<types_snapshot::snapshot::ResolveCminCmaxResult>
);

seam_core::seam!(
    /// `ReorderBufferAllocate()`.
    pub fn ReorderBufferAllocate() -> ReorderBufferHandle
);
seam_core::seam!(
    /// `ReorderBufferFree(rb)`.
    pub fn ReorderBufferFree(rb: ReorderBufferHandle)
);
seam_core::seam!(
    /// Wire `rb->private_data = ctx` and install every `*_cb_wrapper`
    /// trampoline (the ReorderBuffer-driven callbacks logical.c owns).
    pub fn wire_reorderbuffer_callbacks(rb: ReorderBufferHandle)
);
seam_core::seam!(
    /// `rb->output_rewrites = value`.
    pub fn set_output_rewrites(rb: ReorderBufferHandle, value: bool)
);
seam_core::seam!(
    /// Read the eight `ReorderBuffer` stat counters (`UpdateDecodingStats`).
    pub fn reorderbuffer_stats(rb: ReorderBufferHandle) -> ReorderBufferStats
);
seam_core::seam!(
    /// Zero the eight `ReorderBuffer` stat counters after reporting.
    pub fn reorderbuffer_reset_stats(rb: ReorderBufferHandle)
);
