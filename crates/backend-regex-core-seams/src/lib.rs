//! Seam declarations for the `backend-regex-core` unit (`backend/regex/*`:
//! `regcomp.c`, `regexec.c`, `regprefix.c`, `regfree.c`, `regerror.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.
//!
//! The compiled regex crosses this boundary as the opaque
//! [`types_regex::RegexHandle`] inside [`types_regex::RegexCompiled`] — the
//! engine owns the `regex_t`/`re_guts` state behind the handle (see
//! `types-regex` for why that opacity is inherited, not introduced). One
//! consequence: in C, `regexp.c` stores each compiled regex in a dedicated
//! per-regexp memory context and frees it with `MemoryContextDelete`; with
//! engine-owned state the free maps onto the engine's own release entry
//! point, `pg_regfree`.
//!
//! Failure surface: PostgreSQL 18's engine pallocs internally and runs
//! `CHECK_FOR_INTERRUPTS`, so every operation can `ereport(ERROR)` — hence
//! `PgResult`. The engine's *regex-level* failure codes (bad pattern, etc.)
//! are not `ereport`s: they come back as return codes that the caller turns
//! into its own error, so they are mirrored as the `Failed` /
//! non-`Compiled` arms of the result enums, carrying the
//! `pg_regerror`-formatted message (`regerror.c` owns the message table).

use mcx::Mcx;
use types_core::{Oid, PgWChar};
use types_error::PgResult;
use types_regex::{RegMatch, RegcompResult, RegexHandle, RegexecResult, RegprefixResult};

seam_core::seam!(
    /// `pg_regcomp(re, string, len, flags, collation)` (regcomp.c), with the
    /// non-`REG_OKAY` arm carried as `RegcompResult::Failed` (already
    /// `pg_regerror`-formatted). `pattern` is `pg_wchar` code points (the
    /// caller has already done `pg_mb2wchar_with_len`).
    pub fn pg_regcomp(
        pattern: &[PgWChar],
        cflags: i32,
        collation: Oid,
    ) -> PgResult<RegcompResult>
);

seam_core::seam!(
    /// `pg_regexec(re, string, len, search_start, NULL, nmatch, pmatch, 0)`
    /// (regexec.c). `nmatch` is `pmatch.len()` (pass `&mut []` for C's
    /// `nmatch = 0, pmatch = NULL`); on `Matched` the slots are filled in
    /// place, exactly as C fills the caller-provided `pmatch` array.
    pub fn pg_regexec(
        handle: RegexHandle,
        data: &[PgWChar],
        search_start: i32,
        pmatch: &mut [RegMatch],
    ) -> PgResult<RegexecResult>
);

seam_core::seam!(
    /// `pg_regprefix(re, &string, &slen)` (regprefix.c). The extracted
    /// prefix (`pg_wchar` code points) is allocated in `mcx` (C: palloc in
    /// the caller's current context; the caller pfrees it after converting
    /// back to the database encoding).
    pub fn pg_regprefix<'mcx>(
        mcx: Mcx<'mcx>,
        handle: RegexHandle,
    ) -> PgResult<RegprefixResult<'mcx>>
);

seam_core::seam!(
    /// `pg_regfree(re)` (regfree.c) — release the engine-owned compiled-RE
    /// state behind `handle`. In C, `regexp.c` evicts a cache entry with
    /// `MemoryContextDelete(cre_context)`, which frees the palloc'd engine
    /// state; with engine-owned handles that maps onto `pg_regfree`.
    pub fn pg_regfree(handle: RegexHandle)
);
