//! Seam declarations for the `common/pg_lzcompress.c` PGLZ decompressor
//! (`common-pglz`, not yet ported).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Both routines are pure, allocation-free byte
//! transforms over caller-provided buffers — there is no memory context to
//! thread.

seam_core::seam!(
    /// `pglz_decompress(source, slen, dest, rawsize, check_complete)`
    /// (common/pg_lzcompress.c): decompress `source` directly into `dest`,
    /// returning the number of bytes written. Decompression stops when the
    /// source is exhausted or `dest` is full (a "slice" extraction). With
    /// `check_complete` set, the stream is corrupt unless it exactly fills
    /// `dest` and consumes all of `source`. C signals corruption with a
    /// negative return (`rawsize < 0`); here that is `Ok(None)` so the caller
    /// raises its own `ereport(ERROR, ERRCODE_DATA_CORRUPTED, "compressed pglz
    /// data is corrupt")`.
    pub fn pglz_decompress_to_slice(
        source: &[u8],
        dest: &mut [u8],
        check_complete: bool,
    ) -> types_error::PgResult<Option<usize>>
);

seam_core::seam!(
    /// `pglz_maximum_compressed_size(rawsize, total_compressed_size)`
    /// (common/pg_lzcompress.c): the largest compressed size that could yield
    /// at least `rawsize` decompressed bytes, capped at
    /// `total_compressed_size`. Pure arithmetic, infallible.
    pub fn pglz_maximum_compressed_size(rawsize: i32, total_compressed_size: i32) -> i32
);
