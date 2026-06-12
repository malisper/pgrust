//! Seam declarations for the `backend-access-common-detoast` unit
//! (`access/common/detoast.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. A varlena attribute crosses as its verbatim
//! datum bytes (header included), exactly what C's `struct varlena *` points
//! at.

seam_core::seam!(
    /// `detoast_external_attr(attr)` (access/common/detoast.c): fetch back an
    /// out-of-line or out-of-memory stored attribute into `mcx`, without
    /// decompressing compressed data. `Err` carries the toast-fetch
    /// `ereport(ERROR)`s (`missing chunk number ...` etc.) and OOM. (C: the
    /// result is palloc'd in the current memory context.)
    pub fn detoast_external_attr<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        attr: &[u8],
    ) -> types_error::PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `detoast_attr(attr)` (access/common/detoast.c): return a de-TOASTed
    /// (fetched back and decompressed) copy of `attr` in `mcx`. `Err` carries
    /// the toast-fetch / decompression `ereport(ERROR)`s and OOM.
    pub fn detoast_attr<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        attr: &[u8],
    ) -> types_error::PgResult<mcx::PgVec<'mcx, u8>>
);
