//! Seam declarations for the `backend-regex-core` unit (`backend/regex/*`:
//! `regcomp.c`, `regexec.c`, `regprefix.c`, `regfree.c`, `regerror.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.
//!
//! The compiled regex crosses this boundary as [`::regex::RegexCompiled`],
//! which carries the real engine-owned `regex_t` value type-erased as an
//! `Rc<dyn Any>`; the owning unit downcasts it back to its own `regex_t` (see
//! `types-regex` for why that real-value/downcast cycle-break introduces no
//! handle or opacity). One consequence: in C, `regexp.c` stores each compiled
//! regex in a dedicated per-regexp memory context and frees it with
//! `MemoryContextDelete`; here the free maps onto dropping the carrier (the
//! `pg_regfree` seam takes it by value).
//!
//! Failure surface: PostgreSQL 18's engine pallocs internally and runs
//! `CHECK_FOR_INTERRUPTS`, so every operation can `ereport(ERROR)` — hence
//! `PgResult`. The engine's *regex-level* failure codes (bad pattern, etc.)
//! are not `ereport`s: they come back as return codes that the caller turns
//! into its own error, so they are mirrored as the `Failed` /
//! non-`Compiled` arms of the result enums, carrying the
//! `pg_regerror`-formatted message (`regerror.c` owns the message table).

use ::mcx::Mcx;
use ::types_core::{Oid, PgWChar};
use ::types_error::PgResult;
use ::regex::{RegMatch, RegcompResult, RegexCompiled, RegexecResult, RegprefixResult};

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
        re: &RegexCompiled,
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
        re: &RegexCompiled,
    ) -> PgResult<RegprefixResult<'mcx>>
);

seam_core::seam!(
    /// `pg_regfree(re)` (regfree.c) — release the engine-owned compiled-RE
    /// state. In C, `regexp.c` evicts a cache entry with
    /// `MemoryContextDelete(cre_context)`, which frees the palloc'd engine
    /// state; here the carrier is taken by value and its `Rc` dropped, freeing
    /// the engine `regex_t` when the last reference goes away.
    pub fn pg_regfree(re: RegexCompiled)
);
