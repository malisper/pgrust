//! Seam declarations for the `backend-utils-adt-encode` unit
//! (`utils/adt/encode.c`): the binary<->text encoders used by `bytea` I/O.
//!
//! The owning unit (`encode.c`) is not yet ported; the owner installs these
//! from its `init_seams()` when it lands. Until then a call panics loudly per
//! mirror-pg-and-panic.
//!
//! In C these operate on caller-provided `char *dst` buffers (the caller has
//! already sized `dst` via `hex_enc_len`/`hex_dec_len`) and return the number
//! of bytes actually written. Across a seam the destination buffer cannot be
//! borrowed, so each seam allocates and returns the exact written payload in
//! `mcx` (C: the `byteain`/`byteaout` callers palloc the destination in the
//! current memory context).

use ::mcx::{Mcx, PgVec};
use ::types_error::{PgError, PgResult};

seam_core::seam!(
    /// `hex_encode(const char *src, size_t len, char *dst)` (encode.c:181) —
    /// encode `src` as lowercase hex (2 chars per input byte). The returned
    /// buffer is `src.len() * 2` bytes, charged to `mcx`. Pure: never errors
    /// except for the charging OOM.
    pub fn hex_encode<'mcx>(mcx: Mcx<'mcx>, src: &[u8]) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `hex_decode_safe(const char *src, size_t len, char *dst, Node *escontext)`
    /// (encode.c:217) — decode hex text `src` into raw bytes, skipping ASCII
    /// whitespace between byte pairs. The returned buffer holds the decoded
    /// bytes, charged to `mcx`.
    ///
    /// `soft` mirrors C's `escontext != NULL`. The recoverable
    /// `ERRCODE_INVALID_PARAMETER_VALUE` failures (an invalid hex digit or a
    /// dangling nibble — C's `ereturn(escontext, ...)`) cross as the **inner**
    /// `Err(PgError)` of the `Ok` arm so the consumer (`byteain`) can route the
    /// complete error through its own frame escontext via `ereturn`; the inner
    /// error is only ever produced when `soft` is true (with `soft = false` the
    /// owner re-raises it hard, surfacing as the outer `Err`). The **outer**
    /// `Err` is reserved for genuine hard failures (e.g. the charging OOM).
    pub fn hex_decode_safe<'mcx>(
        mcx: Mcx<'mcx>,
        src: &[u8],
        soft: bool,
    ) -> PgResult<Result<PgVec<'mcx, u8>, PgError>>
);
