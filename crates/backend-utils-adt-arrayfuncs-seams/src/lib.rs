//! Seam declarations for the `text[]` deconstruct/construct operations the
//! reloptions parser drives (`utils/adt/arrayfuncs.c`).
//!
//! `reloptions.c` reads/writes the `pg_class.reloptions` / `pg_tablespace`
//! `text[]` array with `deconstruct_array_builtin(..., TEXTOID, ...)`,
//! `accumArrayResult`, and `makeArrayResult`. Those routines `palloc` their
//! results in the current memory context, so the seams take the target
//! `Mcx<'mcx>` and their outputs carry `'mcx`. `Err` carries the C
//! `ereport(ERROR)` surface (malformed array, etc.).
//!
//! The owning unit (`backend-utils-adt-array-more`) installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use mcx::{Mcx, PgString, PgVec};
use types_datum::datum::Datum;
use types_error::PgResult;

seam_core::seam!(
    /// `deconstruct_array_builtin(DatumGetArrayTypeP(array), TEXTOID, ...)`
    /// (arrayfuncs.c): split a non-null `text[]` varlena (verbatim catalog
    /// bytes) into its element strings, in order. The C result is a palloc'd
    /// `Datum *` of `text *` payloads (no NULLs in reloptions arrays).
    pub fn deconstruct_text_array<'mcx>(
        mcx: Mcx<'mcx>,
        array: &[u8],
    ) -> PgResult<PgVec<'mcx, PgString<'mcx>>>
);

seam_core::seam!(
    /// `accumArrayResult`/`makeArrayResult` over `TEXTOID` (arrayfuncs.c):
    /// build a `text[]` array `Datum` from the given element strings. An empty
    /// input yields the C `(Datum) 0` (no array), represented as `Datum::null`.
    /// The result varlena is allocated in `mcx`; the carried `Datum` is the
    /// pointer word into it.
    pub fn construct_text_array<'mcx>(
        mcx: Mcx<'mcx>,
        elems: &[&str],
    ) -> PgResult<Datum>
);

seam_core::seam!(
    /// `construct_array_builtin(datums, n, INT4OID)` (arrayfuncs.c): build a
    /// 1-D `int4[]` array `Datum` from the given elements (duplicates kept, as
    /// the `pg_blocking_pids` / `pg_safe_snapshot_blocking_pids` callers
    /// require). An empty input still yields a valid empty array (the C
    /// behaviour). The result varlena is allocated in `mcx`. `Err` carries OOM.
    pub fn construct_int4_array<'mcx>(mcx: Mcx<'mcx>, elems: &[i32]) -> PgResult<Datum>
);
