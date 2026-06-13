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

use mcx::{Mcx, PgBox, PgString, PgVec};
use types_core::Oid;
use types_datum::array_build::ArrayBuildStateAny;
use types_datum::datum::Datum;
use types_error::PgResult;

/// The `ArrayBuildStateAny *` threaded between the array-accumulation seams.
/// `None` is the C `NULL` (no accumulator yet / empty result).
pub type ArrayBuildStateAnyHandle<'mcx> = Option<PgBox<'mcx, ArrayBuildStateAny>>;

seam_core::seam!(
    /// `initArrayResultAny(input_type, CurrentMemoryContext, true)`
    /// (arrayfuncs.c): create a fresh polymorphic array accumulator for
    /// elements of `input_type`, allocated in `mcx`. Fallible on OOM.
    pub fn init_array_result_any<'mcx>(
        mcx: Mcx<'mcx>,
        input_type: Oid,
    ) -> PgResult<ArrayBuildStateAnyHandle<'mcx>>
);

seam_core::seam!(
    /// `accumArrayResultAny(astate, dvalue, disnull, input_type, ctx)`
    /// (arrayfuncs.c): accumulate one value into the accumulator (creating it
    /// if `None`), in context `ctx`. Returns the (possibly newly created)
    /// accumulator. Fallible on OOM.
    pub fn accum_array_result_any<'mcx>(
        ctx: Mcx<'mcx>,
        astate: ArrayBuildStateAnyHandle<'mcx>,
        dvalue: Datum,
        disnull: bool,
        input_type: Oid,
    ) -> PgResult<ArrayBuildStateAnyHandle<'mcx>>
);

seam_core::seam!(
    /// `makeArrayResultAny(astate, ctx, true)` (arrayfuncs.c): finalize the
    /// accumulator into an array `Datum`, allocated in `ctx`. A `None`
    /// accumulator yields an empty array (not NULL). Fallible on OOM.
    pub fn make_array_result_any<'mcx>(
        ctx: Mcx<'mcx>,
        astate: ArrayBuildStateAnyHandle<'mcx>,
    ) -> PgResult<Datum>
);

seam_core::seam!(
    /// `pfree(DatumGetPointer(node->curArray))` (utils/palloc.h): free a
    /// previously built array `Datum` held in the node's `curArray`. A null
    /// `Datum` is a no-op (the C guards with `!= PointerGetDatum(NULL)`).
    /// Infallible.
    pub fn pfree_array_datum(curarray: Datum)
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
