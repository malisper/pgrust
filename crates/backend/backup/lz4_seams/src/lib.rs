//! Seam declarations for the LZ4 frame compression primitives used by
//! `backend-backup-lz4` (`src/backend/backup/basebackup_lz4.c`).
//!
//! These are the genuinely external compression-library calls (`<lz4frame.h>`:
//! `LZ4F_compressBound` / `LZ4F_createCompressionContext` /
//! `LZ4F_compressBegin` / `LZ4F_compressUpdate` / `LZ4F_compressEnd` /
//! `LZ4F_freeCompressionContext`). lz4 is an out-of-tree C library with no
//! in-tree owner, so the codec crosses a seam, exactly as the cryptographic
//! primitives in `common-cryptohash-seams` do. The base-backup sink buffering
//! / flush / forward logic is the real port and lives in `backend-backup-lz4`.
//!
//! The `LZ4F_compressionContext_t` is the external library's opaque object; it
//! is held across calls by the lz4 sink as the [`Lz4CtxHandle`] returned by
//! [`lz4f_create_compression_context`] and never dereferenced by the consumer.
//! The `LZ4F_preferences_t` is reconstructed by the owner from the
//! [`Lz4Preferences`] the sink carries (the only fields the sink sets are the
//! `frameInfo.blockSizeID` (`LZ4F_max256KB`) and `compressionLevel`).
//!
//! Output/footer writes are expressed with safe slices and the lz4 functions'
//! `size_t` return convention (the produced byte count, or an error code that
//! `LZ4F_isError` recognizes), so the consumer (which `#![forbid(unsafe_code)]`)
//! never handles raw pointers.

#![forbid(unsafe_code)]

use std::string::String;

/// An opaque handle to a live `LZ4F_compressionContext_t` (the C
/// `LZ4F_cctx` created by `LZ4F_createCompressionContext`). The lz4 sink holds
/// it across calls and passes it back to the codec seams.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Lz4CtxHandle(pub u64);

/// The subset of `LZ4F_preferences_t` the lz4 sink sets (C
/// `bbsink_lz4_begin_backup` zeroes the struct and sets only these two). The
/// owner reconstructs the full `LZ4F_preferences_t` from them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Lz4Preferences {
    /// `prefs.frameInfo.blockSizeID` — always `LZ4F_max256KB` here.
    pub block_size_id: i32,
    /// `prefs.compressionLevel`.
    pub compression_level: i32,
}

/// `LZ4F_max256KB` (`<lz4frame.h>`, `= 7`): the block-size ID the sink selects.
pub const LZ4F_MAX_256KB: i32 = 7;

/// The result of an lz4 codec call: either the number of bytes produced into
/// the output buffer, or an error whose message is the
/// `LZ4F_getErrorName(code)` string.
pub type Lz4Result = std::result::Result<usize, String>;

seam_core::seam!(
    /// `LZ4F_compressBound(input_size, &prefs)` (`<lz4frame.h>`): the worst-case
    /// number of output bytes that compressing `input_size` input bytes with
    /// `prefs` could require.
    pub fn lz4f_compress_bound(input_size: usize, prefs: Lz4Preferences) -> usize
);

seam_core::seam!(
    /// `LZ4F_createCompressionContext(&ctx, LZ4F_VERSION)` (`<lz4frame.h>`):
    /// allocate a compression context. Returns the new handle, or `Err(name)`
    /// carrying the `LZ4F_getErrorName` of the failing `LZ4F_errorCode_t`.
    pub fn lz4f_create_compression_context() -> std::result::Result<Lz4CtxHandle, String>
);

seam_core::seam!(
    /// `LZ4F_compressBegin(ctx, dst, dst_capacity, &prefs)` (`<lz4frame.h>`):
    /// write the frame header into the start of `output`, returning the header
    /// byte count (or an error).
    pub fn lz4f_compress_begin(
        ctx: Lz4CtxHandle,
        output: &mut [u8],
        prefs: Lz4Preferences,
    ) -> Lz4Result
);

seam_core::seam!(
    /// `LZ4F_compressUpdate(ctx, dst, dst_capacity, src, src_size, NULL)`
    /// (`<lz4frame.h>`): compress `input` into `output`, returning the number of
    /// bytes written to `output` (or an error).
    pub fn lz4f_compress_update(
        ctx: Lz4CtxHandle,
        output: &mut [u8],
        input: &[u8],
    ) -> Lz4Result
);

seam_core::seam!(
    /// `LZ4F_compressEnd(ctx, dst, dst_capacity, NULL)` (`<lz4frame.h>`): flush
    /// whatever remains and write the frame footer into `output`, returning the
    /// number of bytes written (or an error).
    pub fn lz4f_compress_end(ctx: Lz4CtxHandle, output: &mut [u8]) -> Lz4Result
);

seam_core::seam!(
    /// `LZ4F_freeCompressionContext(ctx)` (`<lz4frame.h>`): release the context.
    pub fn lz4f_free_compression_context(ctx: Lz4CtxHandle)
);
