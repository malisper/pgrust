//! Seam declarations for the `backend-utils-adt-jsonb` unit
//! (`utils/adt/jsonb.c`) — the SQL-facing `jsonb` type.
//!
//! `jsonb.c` is the on-top layer over the `jsonb_util.c` engine
//! (`backend-utils-adt-jsonb-util`). Most of its catalog/fmgr boundary is
//! already declared by sibling seam crates and reused directly:
//!
//!   * `backend-utils-adt-jsonfuncs-seams` — `categorize_type`,
//!     `output_function_call`, `func_volatile`, `text_datum_bytes`,
//!     `deconstruct_array`, `walk_composite` (the `json_categorize_type`
//!     classification + per-type output/cast fmgr calls + array/composite
//!     catalog half),
//!   * `backend-utils-adt-timestamp-seams::json_encode_datetime` (datetime
//!     rendering),
//!
//! and the numeric float casts go straight to the landed
//! `backend-utils-adt-numeric` crate.
//!
//! This crate declares the remaining boundaries. Most are now resolved by an
//! installed body:
//!
//!   * `parse_to_jsonb` — the JSON lexer/parser (jsonapi subsystem) driving the
//!     `jsonb_in_*` semantic actions to assemble on-disk bytes. The jsonapi
//!     parser (`pg_parse_json`) is not yet ported (only `common-jsonapi-seams`
//!     exists, with no provider), so this seam stays unresolved until that
//!     subsystem lands.
//!   * `oid_function_call1` — `OidFunctionCall1` for the `JSONTYPE_CAST` arm.
//!     Installed by the `jsonb` owner's `init_seams()` by delegating to the
//!     fmgr-core `function_call1_coll_datum` seam (the real `fmgr.c` owner).
//!   * `jsonb_datum_bytes` — detoast a `jsonb` `Datum` to its varlena bytes
//!     (`DatumGetJsonbP` = `PG_DETOAST_DATUM`). Installed by the `jsonb` owner
//!     via the `detoast_attr` seam.
//!   * `numeric_int2` / `numeric_int4` / `numeric_int8` — the `numeric`→integer
//!     casts (`DirectFunctionCall1(numeric_intN, ...)`). Installed by
//!     `backend-utils-adt-numeric::init_seams()` (the real `numeric.c` owner).

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::{PgResult, SoftErrorContext};
use types_tuple::Datum;

seam_core::seam!(
    /// `jsonb_from_cstring(char *json, int len, bool unique_keys, ...)` — run
    /// the JSON lexer/parser (`pg_parse_json`) over `json`, driving the
    /// `jsonb_in_*` semantic actions, and return the assembled on-disk `jsonb`
    /// varlena bytes (length header + root container), allocated in `mcx`. The
    /// lexer/parser is the jsonapi subsystem (json's cycle partner), so the
    /// parse loop is owned by the provider. C forwards `escontext`
    /// (`fcinfo->context`) to `json_errsave_error`: with a live soft sink a
    /// parse failure yields `Ok(None)` (`ereturn(escontext, (Datum) 0, ...)`),
    /// otherwise it raises `Err` (the parse
    /// `ereport(ERROR, "invalid input syntax for type json")`).
    pub fn parse_to_jsonb<'mcx, 'e>(
        mcx: Mcx<'mcx>,
        json: &[u8],
        unique_keys: bool,
        escontext: Option<&'e mut SoftErrorContext>,
    ) -> PgResult<Option<PgVec<'mcx, u8>>>
);

seam_core::seam!(
    /// `OidFunctionCall1(outfuncoid, val)` — the `JSONTYPE_CAST` arm of
    /// `datum_to_jsonb_internal`: invoke the type's cast-to-json function
    /// through the fmgr calling convention, returning the resulting (json/jsonb
    /// text) `Datum`. `Err` carries the called function's `ereport(ERROR)`.
    pub fn oid_function_call1<'mcx>(
        mcx: Mcx<'mcx>,
        outfuncoid: Oid,
        val: &Datum<'mcx>,
    ) -> PgResult<Datum<'mcx>>
);

seam_core::seam!(
    /// `PG_GETARG_JSONB_P(val)` / `DatumGetJsonbP(val)` — detoast a `jsonb`
    /// `Datum` to its on-disk varlena bytes (length header + root container)
    /// for the `JSONTYPE_JSONB` arm of `datum_to_jsonb_internal`. The detoasted
    /// copy is allocated in `mcx` (`DatumGetJsonbP` = `PG_DETOAST_DATUM`).
    pub fn jsonb_datum_bytes<'mcx>(
        mcx: Mcx<'mcx>,
        val: &Datum<'mcx>,
    ) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `DirectFunctionCall1(numeric_int2, num)` — cast on-disk `numeric` bytes
    /// `num` to `int2`. `Err` carries "smallint out of range". Not yet ported
    /// in `backend-utils-adt-numeric`.
    pub fn numeric_int2(num: &[u8]) -> PgResult<i16>
);

seam_core::seam!(
    /// `DirectFunctionCall1(numeric_int4, num)` — cast on-disk `numeric` bytes
    /// `num` to `int4`. `Err` carries "integer out of range".
    pub fn numeric_int4(num: &[u8]) -> PgResult<i32>
);

seam_core::seam!(
    /// `DirectFunctionCall1(numeric_int8, num)` — cast on-disk `numeric` bytes
    /// `num` to `int8`. `Err` carries "bigint out of range".
    pub fn numeric_int8(num: &[u8]) -> PgResult<i64>
);

seam_core::seam!(
    /// The `variadic` branch of `extract_variadic_args` (funcapi.c) for the
    /// VARIADIC-"any" jsonb builders (`jsonb_build_object` / `jsonb_build_array`
    /// called as `f(VARIADIC arr)`): take the single trailing array argument's
    /// on-disk varlena image (the `Datum::ByRef` lane bytes) and expand it into
    /// per-element `(Datum, isnull)` pairs plus the common element type OID.
    ///
    /// Mirrors C exactly: `array_in = PG_GETARG_ARRAYTYPE_P(variadic_start);
    /// element_type = ARR_ELEMTYPE(array_in); get_typlenbyvalalign(element_type,
    /// ...); deconstruct_array(array_in, element_type, typlen, typbyval,
    /// typalign, &args_res, &nulls_res, &nargs);` — every `types_res[i]` is set
    /// to `element_type`. The element `Datum`s are materialized in the canonical
    /// header-ful by-reference form the per-type output functions consume.
    /// Installed by `backend-utils-adt-jsonfuncs` (which owns `arrayfuncs` +
    /// `lsyscache`).
    pub fn extract_variadic_array<'mcx>(
        mcx: Mcx<'mcx>,
        array_image: &Datum<'mcx>,
    ) -> PgResult<(Oid, alloc::vec::Vec<(Datum<'mcx>, bool)>)>
);

seam_core::seam!(
    /// `deconstruct_array_builtin(in_array, TEXTOID, &in_datums, &in_nulls,
    /// &in_count)` (jsonb.c `jsonb_object` / `jsonb_object_two_arg`): explode the
    /// on-disk `text[]` varlena image into `ARR_NDIM`, the full `ARR_DIMS` vector
    /// (for the `[0]` even-element / `[1]` two-column shape checks), and the
    /// per-element text payloads (`None` == SQL NULL — the C `in_nulls[i]` flag;
    /// `Some(bytes)` == the raw `text` payload, `VARDATA_ANY` with no header).
    /// Installed by `backend-utils-adt-jsonfuncs` (which owns `arrayfuncs`).
    pub fn deconstruct_text_array_with_dims(
        arr: &[u8],
    ) -> PgResult<(i32, alloc::vec::Vec<i32>, alloc::vec::Vec<Option<alloc::vec::Vec<u8>>>)>
);

extern crate alloc;
