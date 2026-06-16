//! Seam declarations for the zlib `deflate` streaming primitives used by
//! `backend-backup-gzip` (`src/backend/backup/basebackup_gzip.c`).
//!
//! These are the genuinely external compression-library calls (`<zlib.h>`:
//! `deflateInit2` / `deflate`). zlib is an out-of-tree C library with no
//! in-tree owner, so — exactly like the cryptographic primitives in
//! `common-cryptohash-seams` — the streaming codec crosses a seam. The base
//! backup sink *buffering* logic (allocating the working buffer, feeding bytes
//! through the compressor, flushing full output blocks to the next sink, and
//! forwarding the chain callbacks) is the real port and lives in
//! `backend-backup-gzip`; only these codec primitives are seamed.
//!
//! The `z_stream` is the external library's opaque streaming object. It is held
//! across calls by the gzip sink as the [`GzipStreamHandle`] returned by
//! [`deflate_init2`], and never dereferenced by the consumer. The owning unit
//! (the zlib FFI boundary) installs these from its `init_seams()` when it
//! lands; until then a call panics loudly. There is no silent fallback.
//!
//! `deflate`'s `next_in`/`avail_in`/`next_out`/`avail_out` accounting is
//! expressed with safe slices plus an explicit consumed/produced count
//! ([`DeflateOutcome`]), so the consumer (which `#![forbid(unsafe_code)]`)
//! never handles raw pointers.

#![forbid(unsafe_code)]

use std::string::String;

/// An opaque handle to a live zlib `z_stream` (the C `z_stream` allocated and
/// `deflateInit2`-ed by the codec owner). The gzip sink holds it across
/// `archive_contents` / `end_archive` calls and passes it back to
/// [`deflate`]; it never inspects the contents.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GzipStreamHandle(pub u64);

/// zlib `flush` argument values (`<zlib.h>`), as used by the gzip sink.
pub const Z_NO_FLUSH: i32 = 0;
pub const Z_FINISH: i32 = 4;

/// zlib return code: an unrecoverable stream error (`Z_STREAM_ERROR`, `-2`).
/// `basebackup_gzip.c` treats this as a programming-error and `elog(ERROR)`s.
pub const Z_STREAM_ERROR: i32 = -2;

/// `Z_DEFLATED` — the only legal method for `deflateInit2` (`8`).
pub const Z_DEFLATED: i32 = 8;

/// `Z_DEFAULT_STRATEGY` (`0`).
pub const Z_DEFAULT_STRATEGY: i32 = 0;

/// `Z_DEFAULT_COMPRESSION` (`-1`).
pub const Z_DEFAULT_COMPRESSION: i32 = -1;

/// `Z_OK` (`0`), the success code returned by `deflateInit2`.
pub const Z_OK: i32 = 0;

/// The result of one [`deflate`] call: the zlib return code plus how much
/// input was consumed and how much output was produced. Mirrors how `deflate`
/// advances `next_in`/`avail_in` and `next_out`/`avail_out`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeflateOutcome {
    /// The zlib return code (`Z_OK`, `Z_STREAM_END`, `Z_STREAM_ERROR`, ...).
    pub res: i32,
    /// Number of input bytes consumed from the input slice.
    pub consumed: usize,
    /// Number of output bytes produced into the output slice.
    pub produced: usize,
    /// The `z_stream->msg` string, if any (for the `elog(ERROR)` detail on
    /// `Z_STREAM_ERROR`).
    pub msg: Option<String>,
}

seam_core::seam!(
    /// `deflateInit2(zs, level, Z_DEFLATED, window_bits, mem_level, strategy)`
    /// (`<zlib.h>`): allocate a `z_stream` (using the context's allocator,
    /// matching `gzip_palloc`/`gzip_pfree`) and initialize it for the requested
    /// gzip parameters. Returns the new stream handle, or `Err(ret)` carrying
    /// the non-`Z_OK` return code on failure.
    pub fn deflate_init2(
        level: i32,
        window_bits: i32,
        mem_level: i32,
        strategy: i32,
    ) -> ::core::result::Result<GzipStreamHandle, i32>
);

seam_core::seam!(
    /// `deflate(zs, flush)` (`<zlib.h>`) over the supplied input/output slices.
    ///
    /// Consumes from `input` and produces into `output`, returning the zlib
    /// return code and the consumed/produced byte counts (the deltas applied to
    /// `next_in`/`avail_in` and `next_out`/`avail_out`).
    pub fn deflate(
        stream: GzipStreamHandle,
        input: &[u8],
        output: &mut [u8],
        flush: i32,
    ) -> DeflateOutcome
);
