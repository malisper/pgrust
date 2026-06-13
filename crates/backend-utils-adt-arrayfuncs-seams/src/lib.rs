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
use types_core::Oid;
use types_datum::datum::Datum;
use types_error::PgResult;

seam_core::seam!(
    /// `construct_array_builtin(elems, nelems, elmtype)` (arrayfuncs.c): build
    /// a one-dimensional array `Datum` from `nelems` pass-by-value element
    /// `Datum`s of the built-in type `elmtype` (e.g. `REGTYPEOID`). An empty
    /// input yields a zero-element array, not NULL. The result varlena is
    /// allocated in `mcx`; the carried `Datum` is its pointer word. Can
    /// `ereport(ERROR)` (unsupported element type).
    pub fn construct_array_builtin<'mcx>(
        mcx: Mcx<'mcx>,
        elems: &[Datum],
        elmtype: Oid,
    ) -> PgResult<Datum>
);

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
