//! Seam declarations for the LZ4 paths of the `toast_compression.c` unit
//! (`access/common/toast_compression.c`, `backend-access-common-toast-compression`,
//! not yet ported).
//!
//! LZ4 is an optional build dependency (`#ifdef USE_LZ4`); on a non-LZ4 build
//! C raises `NO_LZ4_SUPPORT()`. The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly. The PGLZ
//! decompression path is ported directly over `common-pglz`, so only the LZ4
//! routines are seamed.

seam_core::seam!(
    /// `lz4_decompress_datum(value)` (toast_compression.c): decompress an
    /// LZ4-compressed varlena into a fresh `mcx` buffer. `value` is the
    /// verbatim compressed-header datum bytes. `Err` carries
    /// `NO_LZ4_SUPPORT()` (non-LZ4 build), the corrupt-data
    /// `ereport(ERROR, ERRCODE_DATA_CORRUPTED, "compressed lz4 data is
    /// corrupt")`, and OOM.
    pub fn lz4_decompress_datum<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        value: &[u8],
    ) -> types_error::PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `lz4_decompress_datum_slice(value, slicelength)`
    /// (toast_compression.c): decompress the front `slicelength` bytes of an
    /// LZ4-compressed varlena into a fresh `mcx` buffer. `Err` as for
    /// [`lz4_decompress_datum`].
    pub fn lz4_decompress_datum_slice<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        value: &[u8],
        slicelength: i32,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, u8>>
);
