//! Seam declarations for the zstd streaming compression primitives used by
//! `backend-backup-zstd` (`src/backend/backup/basebackup_zstd.c`).
//!
//! These are the genuinely external compression-library calls (`<zstd.h>`:
//! `ZSTD_createCCtx` / `ZSTD_CCtx_setParameter` / `ZSTD_CCtx_reset` /
//! `ZSTD_compressBound` / `ZSTD_compressStream2` / `ZSTD_freeCCtx`). zstd is an
//! out-of-tree C library with no in-tree owner, so the codec crosses a seam,
//! exactly as the cryptographic primitives in `common-cryptohash-seams` do.
//! The base-backup sink buffering / flush / forward logic is the real port and
//! lives in `backend-backup-zstd`.
//!
//! The `ZSTD_CCtx *` is the external library's opaque object; it is held across
//! calls by the zstd sink as the [`ZstdCctxHandle`] returned by
//! [`zstd_create_cctx`] and never dereferenced by the consumer.
//!
//! The C code drives the stream with `ZSTD_inBuffer`/`ZSTD_outBuffer`
//! (`{src/dst, size, pos}`). Here [`zstd_compress_stream2`] takes the output
//! slice plus an in/out write cursor (`out_pos`) and the input slice plus an
//! in/out read cursor (`in_pos`), so the consumer (which
//! `#![forbid(unsafe_code)]`) never handles raw pointers; the call returns the
//! `yet_to_flush` hint (`Ok`) or the `ZSTD_getErrorName` string (`Err`).

#![forbid(unsafe_code)]

use std::string::String;

/// An opaque handle to a live `ZSTD_CCtx` (the C compression context created by
/// `ZSTD_createCCtx`). The zstd sink holds it across calls and passes it back
/// to the codec seams.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ZstdCctxHandle(pub u64);

/// `ZSTD_cParameter` values used by the zstd sink (`<zstd.h>`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZstdCParameter {
    /// `ZSTD_c_compressionLevel`.
    CompressionLevel,
    /// `ZSTD_c_nbWorkers`.
    NbWorkers,
    /// `ZSTD_c_enableLongDistanceMatching`.
    EnableLongDistanceMatching,
}

/// `ZSTD_ResetDirective` values used by the zstd sink (`<zstd.h>`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZstdResetDirective {
    /// `ZSTD_reset_session_only`.
    SessionOnly,
}

/// `ZSTD_EndDirective` values used by the zstd sink (`<zstd.h>`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZstdEndDirective {
    /// `ZSTD_e_continue`.
    Continue,
    /// `ZSTD_e_end`.
    End,
}

/// The result of one [`zstd_compress_stream2`] call: how far the input/output
/// cursors advanced plus the `yet_to_flush` hint (the C return value, > 0 if
/// more flushing is needed for `ZSTD_e_end`). On a library error, the variant
/// is `Err(ZSTD_getErrorName(...))`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZstdStreamOutcome {
    /// New input cursor (`inBuf.pos`).
    pub in_pos: usize,
    /// New output cursor (`outBuf.pos`).
    pub out_pos: usize,
    /// `yet_to_flush` — the call's return value.
    pub yet_to_flush: usize,
}

seam_core::seam!(
    /// `ZSTD_createCCtx()` (`<zstd.h>`): allocate a compression context.
    /// Returns the handle, or `None` if allocation failed (C `!mysink->cctx`).
    pub fn zstd_create_cctx() -> ::core::option::Option<ZstdCctxHandle>
);

seam_core::seam!(
    /// `ZSTD_CCtx_setParameter(cctx, param, value)` (`<zstd.h>`): set a
    /// compression parameter. Returns `Ok(())` on success, or
    /// `Err(ZSTD_getErrorName(ret))` (the C `ZSTD_isError(ret)` branch).
    pub fn zstd_cctx_set_parameter(
        cctx: ZstdCctxHandle,
        param: ZstdCParameter,
        value: i32,
    ) -> ::core::result::Result<(), String>
);

seam_core::seam!(
    /// `ZSTD_CCtx_reset(cctx, directive)` (`<zstd.h>`): reset the context (the
    /// sink uses `ZSTD_reset_session_only`, keeping the sticky parameters).
    pub fn zstd_cctx_reset(cctx: ZstdCctxHandle, directive: ZstdResetDirective)
);

seam_core::seam!(
    /// `ZSTD_compressBound(src_size)` (`<zstd.h>`): the worst-case compressed
    /// size for `src_size` input bytes.
    pub fn zstd_compress_bound(src_size: usize) -> usize
);

seam_core::seam!(
    /// `ZSTD_compressStream2(cctx, &outBuf, &inBuf, end_op)` (`<zstd.h>`).
    ///
    /// Compresses from `input[in_pos..]` into `output[out_pos..]`, advancing
    /// both cursors, and returns the advanced cursors plus the `yet_to_flush`
    /// hint — or `Err(ZSTD_getErrorName(...))` on a library error.
    pub fn zstd_compress_stream2(
        cctx: ZstdCctxHandle,
        output: &mut [u8],
        out_pos: usize,
        input: &[u8],
        in_pos: usize,
        end_op: ZstdEndDirective,
    ) -> ::core::result::Result<ZstdStreamOutcome, String>
);

seam_core::seam!(
    /// `ZSTD_freeCCtx(cctx)` (`<zstd.h>`): release the context.
    pub fn zstd_free_cctx(cctx: ZstdCctxHandle)
);
