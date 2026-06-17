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
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;

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
    /// `Array_nulls` GUC (`utils/adt/arrayfuncs.c`, declared `bool Array_nulls
    /// = true`). `array_in` reads the live value of this PGC_USERSET GUC out of
    /// its slot to decide whether an unquoted `NULL` in array input text is a
    /// null element (true) or a literal string (false). The owning unit
    /// (`backend-utils-adt-arrayfuncs`) installs this reading the live value
    /// from the GUC slot (`guc_tables::vars::Array_nulls`); it is a plain GUC
    /// read, not a ControlFile field.
    pub fn array_nulls() -> bool
);

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
    /// The array half of `DecodeTextArrayToBitmapset` (evtcache.c): treat
    /// `array` as a detoasted `text[]` varlena (`DatumGetArrayTypeP`), enforce
    /// `ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != TEXTOID`
    /// (`elog(ERROR, "expected 1-D text array")`, carried on `Err`), then
    /// `deconstruct_array_builtin(arr, TEXTOID, ...)` into its element strings
    /// in order (no NULLs after the check). The `bms_add_member` accumulation
    /// over `GetCommandTagEnum` of each string stays with the evtcache caller.
    pub fn decode_text_array_to_strings<'mcx>(
        mcx: Mcx<'mcx>,
        array: &[u8],
    ) -> PgResult<PgVec<'mcx, PgString<'mcx>>>
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
    /// `deconstruct_array_builtin(DatumGetArrayTypeP(array), TEXTOID, &elems,
    /// &nulls, &nelems)` (arrayfuncs.c), preserving per-element NULLs — the
    /// null-permitting form of [`deconstruct_text_array`]. Each element comes
    /// back as `Some(payload)` (a non-null `text` element's UTF-8 string) or
    /// `None` (the C `nulls[i] == true`), in order, so a caller can apply its
    /// own object-specific null-error message (e.g.
    /// `textarray_to_strvaluelist` / `pg_get_object_address` in objectaddress.c,
    /// which `ereport(ERROR)` "name or argument lists may not contain nulls").
    /// The on-disk `text[]` byte image is detoasted (`DatumGetArrayTypeP`).
    /// Fallible on detoast / malformed array / invalid UTF-8.
    pub fn deconstruct_text_array_nullable<'mcx>(
        mcx: Mcx<'mcx>,
        array: &[u8],
    ) -> PgResult<PgVec<'mcx, Option<PgString<'mcx>>>>
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
    /// `accumArrayResult`/`makeArrayResult` over `TEXTOID` followed by
    /// `array_out` (arrayfuncs.c): build a `text[]` array from the given element
    /// strings and render its external text form — the `getTypeOutputInfo(
    /// ANYARRAYOID)` + `OidOutputFunctionCall(typoutput, makeArrayResult(...))`
    /// pair that `brin_minmax_multi_summary_out` (brin_minmax_multi.c:2998) uses
    /// to print the per-range / per-value text arrays. An empty input renders the
    /// C empty-array form `{}`. The result string is allocated in `mcx`. `Err`
    /// carries the element/array output `ereport(ERROR)` surface and OOM.
    pub fn text_array_out<'mcx>(
        mcx: Mcx<'mcx>,
        elems: &[&str],
    ) -> PgResult<PgString<'mcx>>
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
    /// `deconstruct_array(DatumGetArrayTypeP(arraydatum), elmtype, elmlen,
    /// elmbyval, elmalign, &elemsp, &nullsp, &nelemsp)` (arrayfuncs.c) over the
    /// canonical unified value type: split a detoasted array `Datum` into its
    /// per-element `(Datum<'mcx>, isnull)` pairs, in order, given the element
    /// type's storage attributes. The `compute_array_stats` (array_typanalyze.c)
    /// path needs the elements as `types_tuple::Datum<'mcx>` — they are tracked
    /// in the Lossy-Counting table and ultimately datumCopy'd into the
    /// `VacAttrStats` MCELEM slot, which is `Vec<Datum<'mcx>>`. The owner
    /// detoasts internally (`DatumGetArrayTypeP`). C result arrays are palloc'd
    /// in the current context; the owned model returns them in `mcx`. Fallible
    /// on the `ereport(ERROR)` surface (malformed array).
    pub fn deconstruct_array_v<'mcx>(
        mcx: Mcx<'mcx>,
        arraydatum: DatumV<'mcx>,
        elmtype: Oid,
        elmlen: i16,
        elmbyval: bool,
        elmalign: core::ffi::c_char,
    ) -> PgResult<PgVec<'mcx, (DatumV<'mcx>, bool)>>
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

seam_core::seam!(
    /// `ArrayGetNItems(ARR_NDIM(arrayval), ARR_DIMS(arrayval))` (array.h /
    /// arrayfuncs.c) on the array value held by a non-NULL `Const`'s
    /// `constvalue`: the total element count of the constant array. `clauses.c`
    /// uses it (`is_strict_saop`, `convert_saop_to_hashed_saop`,
    /// `estimate_array_length`) to test whether a folded `IN`-list array is
    /// non-empty / large enough for a hashed SAOP. The C reads the varlena
    /// array header directly; the owned model takes the `Const.constvalue`
    /// `Datum` and detoasts as needed. `Err` carries the `ArrayGetNItems`
    /// overflow `ereport(ERROR, ERRCODE_PROGRAM_LIMIT_EXCEEDED)` and the
    /// detoast surface.
    pub fn array_const_nitems(constvalue: Datum) -> PgResult<i32>
);

seam_core::seam!(
    /// `accumArrayResult`/`makeArrayResult` over `TEXTOID`, preserving per-element
    /// NULLs (arrayfuncs.c) — the array-build half of `text_to_array` /
    /// `text_to_array_null` (varlena.c:4771-4801). Each input element is either a
    /// non-null `text` payload (`Some(bytes)`, run through `CStringGetTextDatum`)
    /// or a SQL NULL (`None`, accumulated with `disnull = true`). An empty input
    /// yields the C `construct_empty_array(TEXTOID)` (a zero-element array, not
    /// NULL — matching the `tstate.astate == NULL` branch). The result is the
    /// array varlena's raw bytes allocated in `mcx` (so the caller can carry it
    /// on the canonical by-reference `Datum`, not a bare pointer word). `Err`
    /// carries the `MaxAllocSize` / OOM `ereport(ERROR)` surface.
    pub fn build_text_array_nullable<'mcx>(
        mcx: Mcx<'mcx>,
        elems: &[Option<&[u8]>],
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `construct_array_builtin(names, n, NAMEOID)` (arrayfuncs.c), the array
    /// half of `current_schemas` (name.c). Each input element is the
    /// `NAMEDATALEN`-byte, NUL-padded `NameData` image of one schema name (the
    /// `name` Datum the C `DirectFunctionCall1(namein, ...)` produced); the
    /// elements are pass-by-reference fixed-length `name` values. An empty
    /// input yields a zero-element array (not NULL). The result is the array
    /// varlena's raw bytes allocated in `mcx`, so the caller can carry it on
    /// the canonical by-reference `Datum`. `Err` carries the `MaxAllocSize` /
    /// OOM `ereport(ERROR)` surface.
    pub fn build_name_array<'mcx>(
        mcx: Mcx<'mcx>,
        elems: &[&[u8]],
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// The element-deconstruct + per-element `OutputFunctionCall` walk of
    /// `array_to_text_internal` (arrayfuncs.c / varlena.c:5130-5178): given the
    /// already-detoasted array varlena bytes (`DatumGetArrayTypeP(v)` — the
    /// caller resolves the by-reference payload), look up the element type's
    /// output function via `get_type_io_data(element_type, IOFunc_output, ...)`,
    /// then walk the flat array's data area element by element (consulting the
    /// null bitmap), running each non-null element through its output function.
    /// Returns one entry per array element in storage order: `Some(bytes)` for a
    /// non-null element's output-formatted cstring bytes, `None` for a NULL
    /// element. An empty array yields an empty list (the caller maps that to the
    /// empty string). `element_type` is `ARR_ELEMTYPE(v)`, supplied by the
    /// caller. `Err` carries the `ArrayGetNItems` / output-function
    /// `ereport(ERROR)` surface.
    pub fn array_to_text_elements<'mcx>(
        mcx: Mcx<'mcx>,
        array: &[u8],
        element_type: Oid,
    ) -> PgResult<PgVec<'mcx, Option<PgVec<'mcx, u8>>>>
);

/* ---------------------------------------------------------------------------
 * Byte-image array-decode seams (`&[u8]`-input).
 *
 * The canonical `Datum` model carries a by-reference catalog attribute as a
 * `Datum::ByRef(bytes)` byte image (the deformed on-disk varlena), not a bare
 * pointer word. The `Datum`-input seams above presuppose a pointer-word that
 * `DatumGetArrayTypeP` can detoast; bridging a `ByRef` image to them would need
 * an unsafe pointer-forge. These seams instead take the on-disk
 * `ArrayType`/`oidvector` byte image directly — exactly the bytes
 * `SysCacheGetAttr` returns for an array column — and perform the same decode
 * (`DatumGetArrayTypeP` == `detoast_attr` on the bytes, then the standard
 * element walk). No forge, no silent corruption.
 * ------------------------------------------------------------------------- */

seam_core::seam!(
    /// `deconstruct_array(DatumGetArrayTypeP(bytes), elmtype, elmlen, elmbyval,
    /// elmalign, &elemsp, &nullsp, &nelemsp)` (arrayfuncs.c) operating on the
    /// on-disk array byte image `bytes` (a `Datum::ByRef` attribute image)
    /// rather than a pointer-word `Datum`. The image is detoasted
    /// (`detoast_attr`) and split into per-element `(Datum, isnull)` pairs, in
    /// order, given the element type's storage attributes. Identical decode to
    /// [`deconstruct_array`], reading the bytes directly. Fallible on the
    /// `ereport(ERROR)` surface (malformed array) / detoast.
    pub fn deconstruct_array_bytes<'mcx>(
        mcx: Mcx<'mcx>,
        bytes: &[u8],
        elmtype: Oid,
        elmlen: i16,
        elmbyval: bool,
        elmalign: core::ffi::c_char,
    ) -> PgResult<PgVec<'mcx, (Datum, bool)>>
);

seam_core::seam!(
    /// `deconstruct_array(DatumGetArrayTypeP(bytes), elmtype, elmlen, elmbyval,
    /// elmalign, ...)` (arrayfuncs.c) operating on the on-disk array byte image
    /// `bytes`, like [`deconstruct_array_bytes`], but returning each element as
    /// the canonical *value-carrying* [`DatumV`] (`ByVal` word / `ByRef` bytes
    /// copied into `mcx`) rather than the bare-word [`Datum`]. The bare-word
    /// form encodes a by-reference element as a pointer into the (scratch) array
    /// buffer, which cannot be carried past the buffer's lifetime; the value
    /// form captures the element payload by value, so callers that must outlive
    /// the array buffer (e.g. `RelationBuildTupleDesc`'s `attmissingval`
    /// extraction, which stores the element into the relcache entry) read it
    /// here. Fallible on the `ereport(ERROR)` surface (malformed array) /
    /// detoast.
    pub fn deconstruct_array_values_bytes<'mcx>(
        mcx: Mcx<'mcx>,
        bytes: &[u8],
        elmtype: Oid,
        elmlen: i16,
        elmbyval: bool,
        elmalign: core::ffi::c_char,
    ) -> PgResult<PgVec<'mcx, (DatumV<'mcx>, bool)>>
);

seam_core::seam!(
    /// `(oidvector *) DatumGetPointer(datum)` then read `->values[0 ..
    /// ->dim1]` (e.g. `proargtypes`, `pg_index.indclass`) operating on the
    /// on-disk `oidvector` byte image `bytes` (a `Datum::ByRef` attribute
    /// image). An `oidvector` is stored as a 1-D `ArrayType` of `OIDOID`
    /// (4-byte pass-by-value, int-aligned, no NULLs, lower bound 0); the image
    /// is detoasted (`detoast_attr`), and `ARR_DATA_PTR` is read as the C
    /// `Oid[ARR_DIMS[0]]`, returned in `mcx`. An empty/zero-dimension vector
    /// yields an empty result (the C `dim1 == 0` case). Fallible on detoast /
    /// truncated element data.
    pub fn oidvector_to_oids_bytes<'mcx>(
        mcx: Mcx<'mcx>,
        bytes: &[u8],
    ) -> PgResult<PgVec<'mcx, Oid>>
);

seam_core::seam!(
    /// `(int2vector *) DatumGetPointer(datum)` then read `->values[0 ..
    /// ->dim1]` (e.g. `pg_index.indoption`) operating on the on-disk
    /// `int2vector` byte image `bytes` (a `Datum::ByRef` attribute image). An
    /// `int2vector` is a 1-D `ArrayType` of `INT2OID` (2-byte pass-by-value,
    /// short-aligned, no NULLs, lower bound 0 — `int2vectorin` constructs it
    /// that way); the image is detoasted (`detoast_attr`), and `ARR_DATA_PTR`
    /// is read as the C `int16[ARR_DIMS[0]]`, returned in `mcx`. An
    /// empty/zero-dimension vector yields an empty result (the C `dim1 == 0`
    /// case). Fallible on detoast / truncated element data.
    pub fn int2vector_to_i16s_bytes<'mcx>(
        mcx: Mcx<'mcx>,
        bytes: &[u8],
    ) -> PgResult<PgVec<'mcx, i16>>
);

seam_core::seam!(
    /// `deconstruct_array_builtin(DatumGetArrayTypeP(bytes), TEXTOID, &elems,
    /// NULL, &nelems)` then `TextDatumGetCString` per element (arrayfuncs.c)
    /// operating on the on-disk `text[]` byte image `bytes` (a `Datum::ByRef`
    /// attribute image, e.g. `proconfig`). The image is detoasted
    /// (`detoast_attr`), then walked element by element, each non-null `text`
    /// element's inline varlena (short or 4-byte header) projected to its UTF-8
    /// string in `bytes`-storage order. A NULL element raises the C
    /// null-not-allowed `ereport(ERROR)` (the proconfig / reloptions text arrays
    /// have no NULLs, and the C `TextDatumGetCString` would dereference NULL).
    /// Fallible on detoast / malformed array / invalid UTF-8.
    pub fn text_array_to_strings_bytes<'mcx>(
        mcx: Mcx<'mcx>,
        bytes: &[u8],
    ) -> PgResult<PgVec<'mcx, PgString<'mcx>>>
);

// ---- array subscripting exec callbacks (arraysubs.c) ----------------------
//
// The `SubscriptExecSteps` method bodies for varlena/raw arrays. The executor
// (execExprInterp EEOP_SBSREF_* steps) dispatches these by `SubscriptMethod`
// discriminant; the array logic lives in the arrayfuncs owner. The container
// and result cross as the canonical `DatumV`.

seam_core::seam!(
    /// `array_subscript_fetch` (arraysubs.c): fetch one element from a non-NULL
    /// array `container`, given the (already integer-converted) `upperindex`
    /// subscripts and the element-type storage attributes. Returns
    /// `(element, isnull)`.
    pub fn array_subscript_fetch<'mcx>(
        mcx: Mcx<'mcx>,
        container: DatumV<'mcx>,
        numupper: i32,
        upperindex: &[i32],
        refattrlength: i16,
        refelemlength: i16,
        refelembyval: bool,
        refelemalign: u8,
    ) -> PgResult<(DatumV<'mcx>, bool)>
);

seam_core::seam!(
    /// `array_subscript_fetch_slice` (arraysubs.c): fetch a slice (never NULL).
    pub fn array_subscript_fetch_slice<'mcx>(
        mcx: Mcx<'mcx>,
        container: DatumV<'mcx>,
        numupper: i32,
        upperindex: &[i32],
        lowerindex: &[i32],
        upperprovided: &[bool],
        lowerprovided: &[bool],
        refattrlength: i16,
        refelemlength: i16,
        refelembyval: bool,
        refelemalign: u8,
    ) -> PgResult<(DatumV<'mcx>, bool)>
);

seam_core::seam!(
    /// `array_subscript_assign` (arraysubs.c): assign one element, returning the
    /// new whole-array value.
    pub fn array_subscript_assign<'mcx>(
        mcx: Mcx<'mcx>,
        container: DatumV<'mcx>,
        container_null: bool,
        numupper: i32,
        upperindex: &[i32],
        replacevalue: DatumV<'mcx>,
        replacenull: bool,
        refelemtype: Oid,
        refattrlength: i16,
        refelemlength: i16,
        refelembyval: bool,
        refelemalign: u8,
    ) -> PgResult<(DatumV<'mcx>, bool)>
);

seam_core::seam!(
    /// `array_subscript_assign_slice` (arraysubs.c): assign a slice.
    pub fn array_subscript_assign_slice<'mcx>(
        mcx: Mcx<'mcx>,
        container: DatumV<'mcx>,
        container_null: bool,
        numupper: i32,
        upperindex: &[i32],
        lowerindex: &[i32],
        upperprovided: &[bool],
        lowerprovided: &[bool],
        replacevalue: DatumV<'mcx>,
        replacenull: bool,
        refelemtype: Oid,
        refattrlength: i16,
        refelemlength: i16,
        refelembyval: bool,
        refelemalign: u8,
    ) -> PgResult<(DatumV<'mcx>, bool)>
);

seam_core::seam!(
    /// `array_subscript_fetch_old` (arraysubs.c): fetch the existing element for
    /// a nested assignment (copes with a NULL container).
    pub fn array_subscript_fetch_old<'mcx>(
        mcx: Mcx<'mcx>,
        container: DatumV<'mcx>,
        container_null: bool,
        numupper: i32,
        upperindex: &[i32],
        refattrlength: i16,
        refelemlength: i16,
        refelembyval: bool,
        refelemalign: u8,
    ) -> PgResult<(DatumV<'mcx>, bool)>
);

seam_core::seam!(
    /// `array_subscript_fetch_old_slice` (arraysubs.c): fetch the existing slice
    /// for a nested assignment.
    pub fn array_subscript_fetch_old_slice<'mcx>(
        mcx: Mcx<'mcx>,
        container: DatumV<'mcx>,
        container_null: bool,
        numupper: i32,
        upperindex: &[i32],
        lowerindex: &[i32],
        upperprovided: &[bool],
        lowerprovided: &[bool],
        refattrlength: i16,
        refelemlength: i16,
        refelembyval: bool,
        refelemalign: u8,
    ) -> PgResult<(DatumV<'mcx>, bool)>
);

seam_core::seam!(
    /// `ExecEvalArrayExpr`'s array fabrication (execExprInterp.c): build an
    /// `ARRAY[...]` constructor result from the 6-arm element values the
    /// interpreter evaluated into `op->d.arrayexpr.elemvalues[]` /
    /// `elemnulls[]`. When `multidims` is false the elements are scalars and the
    /// result is a 1-D `construct_md_array`; when true the elements are
    /// sub-arrays concatenated into an (n+1)-D array (the C nested-subarray
    /// branch, including the all-empty `construct_empty_array` short-circuit and
    /// the "cannot merge incompatible arrays" / matching-dimensions checks).
    ///
    /// The result varlena image is allocated in `mcx`; the caller wraps it as a
    /// `Datum::ByRef`. `Err` carries the C `ereport(ERROR)` surface (dimension
    /// overflow, incompatible arrays, size limit).
    pub fn construct_array_expr<'mcx>(
        mcx: Mcx<'mcx>,
        elemvalues: &[DatumV<'mcx>],
        elemnulls: &[bool],
        elemtype: Oid,
        elemlength: i16,
        elembyval: bool,
        elemalign: u8,
        multidims: bool,
    ) -> PgResult<PgVec<'mcx, u8>>
);
