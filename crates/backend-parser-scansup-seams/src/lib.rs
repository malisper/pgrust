//! Seam declarations for `backend/parser/scansup.c` identifier-truncation
//! helpers.
//!
//! `scansup.c` is not yet ported. `base_yylex` (in the `backend-parser-driver`
//! unit) calls `truncate_identifier` on a de-escaped `UIDENT`. The owning unit
//! installs this seam from its `init_seams()` when it lands; until then a call
//! panics loudly (mirror-PG-and-panic).
//!
//! `scanner_isspace` (scansup.c) — the flex `{space}` predicate — is a small
//! pure byte test, reimplemented in place by every consuming crate (per the
//! repo precedent in `arrayfuncs`/`varlena`/`misc2`), so it is not seamed.

extern crate alloc;

use mcx::{Mcx, PgVec};
use types_error::PgResult;

seam_core::seam!(
    /// `truncate_identifier(ident, len, warn)` (scansup.c:93) — truncate an
    /// identifier to `NAMEDATALEN - 1` bytes (on a multibyte-character
    /// boundary, via `pg_mbcliplen`).
    ///
    /// C mutates `ident` in place; here the seam returns the truncated bytes
    /// (without the trailing NUL C writes) allocated in `mcx`. When `len` is
    /// already below `NAMEDATALEN` the input is returned unchanged. With `warn`
    /// set, truncation emits an `ereport(NOTICE, ERRCODE_NAME_TOO_LONG)`; the
    /// NOTICE path and palloc OOM are carried on `Err`.
    pub fn truncate_identifier<'mcx>(
        mcx: Mcx<'mcx>,
        ident: &[u8],
        warn: bool,
    ) -> PgResult<PgVec<'mcx, u8>>
);
