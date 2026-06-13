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
use types_nodes::{EStateData, EcxtId};
use types_namespace::{CharArrayDatum, OidArrayDatum, TextArrayDatum};

/// The `ArrayBuildStateAny *` threaded between the array-accumulation seams.
/// `None` is the C `NULL` (no accumulator yet / empty result).
pub type ArrayBuildStateAnyHandle<'mcx> = Option<PgBox<'mcx, ArrayBuildStateAny>>;

/// Which of an `ExprContext`'s two memory contexts a polymorphic-array build
/// step allocates in. The C reaches the live memory context through the
/// ambient `CurrentMemoryContext` / `econtext->ecxt_per_query_memory`; the
/// owned model has no ambient current context, so the caller names the target
/// relative to its `econtext` (`EcxtId`) and the arrayfuncs owner resolves the
/// real `MemoryContext` (and its `'mcx`-lived handle) off the EState.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArrayBuildCtx {
    /// `CurrentMemoryContext` at entry — for a SubPlan evaluated inside
    /// expression evaluation that is `econtext->ecxt_per_tuple_memory` (the
    /// short-lived per-tuple eval context that the caller resets between outer
    /// tuples). Used by `ExecScanSubPlan`'s ARRAY_SUBLINK path.
    PerTuple,
    /// `econtext->ecxt_per_query_memory` (== the EState's `es_query_cxt`) — the
    /// per-query context that survives until query end. Used by
    /// `ExecSetParamPlan`'s ARRAY_SUBLINK path (the result is stashed in
    /// `node->curArray` for cross-call reuse).
    PerQuery,
}

seam_core::seam!(
    /// `initArrayResultAny(input_type, CurrentMemoryContext, true)`
    /// (arrayfuncs.c): create a fresh polymorphic array accumulator for
    /// elements of `input_type`, allocated in the memory context the `econtext`
    /// names with `ctx`. Fallible on OOM.
    pub fn init_array_result_any<'mcx>(
        estate: &mut EStateData<'mcx>,
        econtext: EcxtId,
        ctx: ArrayBuildCtx,
        input_type: Oid,
    ) -> PgResult<ArrayBuildStateAnyHandle<'mcx>>
);

seam_core::seam!(
    /// `accumArrayResultAny(astate, dvalue, disnull, input_type, ctx)`
    /// (arrayfuncs.c): accumulate one value into the accumulator (creating it
    /// if `None`), in the memory context the `econtext` names with `ctx`.
    /// Returns the (possibly newly created) accumulator. Fallible on OOM.
    pub fn accum_array_result_any<'mcx>(
        estate: &mut EStateData<'mcx>,
        econtext: EcxtId,
        ctx: ArrayBuildCtx,
        astate: ArrayBuildStateAnyHandle<'mcx>,
        dvalue: Datum,
        disnull: bool,
        input_type: Oid,
    ) -> PgResult<ArrayBuildStateAnyHandle<'mcx>>
);

seam_core::seam!(
    /// `makeArrayResultAny(astate, ctx, true)` (arrayfuncs.c): finalize the
    /// accumulator into an array `Datum`, allocated in the memory context the
    /// `econtext` names with `ctx`. A `None` accumulator yields an empty array
    /// (not NULL). Fallible on OOM.
    pub fn make_array_result_any<'mcx>(
        estate: &mut EStateData<'mcx>,
        econtext: EcxtId,
        ctx: ArrayBuildCtx,
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
    /// `DatumGetArrayTypeP(arraydatum)` then
    /// `deconstruct_array_builtin(itemarray, TIDOID, &ipdatums, &ipnulls,
    /// &ndatums)` (arrayfuncs.c): detoast the `tid[]` array `Datum` and split
    /// it into its per-element `(ItemPointerData, isnull)` pairs, in order
    /// (`ipdatums[i]` reinterpreted via `DatumGetPointer` as an
    /// `ItemPointer`). The C result arrays are palloc'd in the current context
    /// (and pfree'd by the caller); the owned model returns them in `mcx`.
    /// Fallible on `ereport(ERROR)` (malformed array).
    pub fn deconstruct_tid_array<'mcx>(
        mcx: Mcx<'mcx>,
        arraydatum: Datum,
    ) -> PgResult<PgVec<'mcx, (types_tuple::heaptuple::ItemPointerData, bool)>>
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

seam_core::seam!(
    /// `ARR_NDIM(DatumGetArrayTypeP(arraydatum))` (array.h): the number of
    /// dimensions of the array carried by `arraydatum`, after detoast. Thin
    /// accessor over the array header (used by the multirange constructor to
    /// reject multidimensional arrays). Fallible only on the detoast surface.
    pub fn array_get_ndim<'mcx>(mcx: Mcx<'mcx>, arraydatum: Datum) -> PgResult<i32>
);

seam_core::seam!(
    /// `ARR_ELEMTYPE(DatumGetArrayTypeP(arraydatum))` (array.h): the element
    /// type OID of the array carried by `arraydatum`, after detoast. Thin
    /// accessor over the array header. Fallible only on the detoast surface.
    pub fn array_get_elemtype<'mcx>(mcx: Mcx<'mcx>, arraydatum: Datum) -> PgResult<Oid>
);

seam_core::seam!(
    /// `deconstruct_array(DatumGetArrayTypeP(arraydatum), elmtype, elmlen,
    /// elmbyval, elmalign, &elemsp, &nullsp, &nelemsp)` (arrayfuncs.c): split a
    /// detoasted array `Datum` into its per-element `(Datum, isnull)` pairs, in
    /// order, given the element type's storage attributes. The C result arrays
    /// are palloc'd in the current context; the owned model returns them in
    /// `mcx`. Fallible on the `ereport(ERROR)` surface (malformed array).
    pub fn deconstruct_array<'mcx>(
        mcx: Mcx<'mcx>,
        arraydatum: Datum,
        elmtype: Oid,
        elmlen: i16,
        elmbyval: bool,
        elmalign: core::ffi::c_char,
    ) -> PgResult<PgVec<'mcx, (Datum, bool)>>
);

seam_core::seam!(
    /// `DatumGetArrayTypeP(arraydatum)` (detoast) then project the `ArrayType`
    /// header (`ARR_NDIM` / `ARR_DIMS[0]` / `ARR_HASNULL` / `ARR_ELEMTYPE`) and
    /// read `ARR_DATA_PTR` as a C `Oid[]` (the funcapi `build_function_result_*`
    /// path reads OID arrays directly, not via `deconstruct_array`). The
    /// shape-validity checks and the `elog(ERROR)` stay on the funcapi caller;
    /// the seam only detoasts and projects. `values` is the `dim0` raw Oids.
    pub fn oid_array_datum<'mcx>(
        mcx: Mcx<'mcx>,
        arraydatum: Datum,
    ) -> PgResult<OidArrayDatum<'mcx>>
);

seam_core::seam!(
    /// `DatumGetArrayTypeP(arraydatum)` (detoast) then project the `ArrayType`
    /// header plus `ARR_DATA_PTR` read as a C `"char"[]` (the funcapi path reads
    /// `proargmodes` directly as `char[]`). The shape-validity checks and the
    /// `elog(ERROR)` stay on the funcapi caller; the seam only detoasts and
    /// projects.
    pub fn char_array_datum<'mcx>(
        mcx: Mcx<'mcx>,
        arraydatum: Datum,
    ) -> PgResult<CharArrayDatum<'mcx>>
);

seam_core::seam!(
    /// `DatumGetArrayTypeP(arraydatum)` (detoast) then project the `ArrayType`
    /// header plus the elements deconstructed via
    /// `deconstruct_array_builtin(arr, TEXTOID, &elems, NULL, &nelems)` and each
    /// run through `TextDatumGetCString` (the funcapi path's per-element name
    /// reads). The shape-validity checks and the `elog(ERROR)` stay on the
    /// funcapi caller; the seam only detoasts, deconstructs and stringifies.
    pub fn text_array_datum<'mcx>(
        mcx: Mcx<'mcx>,
        arraydatum: Datum,
    ) -> PgResult<TextArrayDatum<'mcx>>
);

seam_core::seam!(
    /// The `stanumbers` extraction of `get_attstatsslot` (lsyscache.c): detoast
    /// + copy the `Datum` (`DatumGetArrayTypePCopy`), verify it is a 1-D
    /// no-NULLs `float4` array (`ARR_NDIM(statarray) != 1 || narrayelem <= 0 ||
    /// ARR_HASNULL(statarray) || ARR_ELEMTYPE(statarray) != FLOAT4OID` ->
    /// `elog(ERROR, "stanumbers is not a 1-D float4 array")`), and return its
    /// element values (`ARR_DATA_PTR` viewed as `float4[narrayelem]`) copied
    /// into `mcx`. In C the slot's `numbers` points directly into the detoasted
    /// array (freed by `free_attstatsslot`); the owned model returns the copy
    /// so its `Drop` subsumes the free. `Err` carries the validation
    /// `ereport(ERROR)` and detoast/OOM surface.
    pub fn array_get_float4_values<'mcx>(
        mcx: Mcx<'mcx>,
        arraydatum: Datum,
    ) -> PgResult<PgVec<'mcx, f32>>
);
