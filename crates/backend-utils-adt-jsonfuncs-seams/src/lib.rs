//! Seam declarations for the type-classification + typed-Datum-to-JSON
//! rendering boundary anchored by `json_categorize_type` (which lives in
//! `utils/adt/jsonfuncs.c`, json's cycle partner). `json.c` renders arbitrary
//! SQL Datums to JSON, which needs catalog/syscache type classification and the
//! per-type *output / cast functions* invoked through the fmgr calling
//! convention, plus the `deconstruct_array` / composite-walk catalog work.
//!
//! These cross the json<->jsonfuncs cycle (and reach the catalog/fmgr/array
//! layers behind it), so they are seamed. The owning unit installs them from
//! its `init_seams()` when it lands; until then a call panics loudly.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;

use types_core::Oid;
use types_error::PgResult;
use types_json::{ArrayForJson, CompositeFieldForJson, JsonTypeCategory};
use types_tuple::Datum;

seam_core::seam!(
    /// `json_categorize_type(typoid, false, &tcategory, &outfuncoid)` —
    /// classify a type and yield its output (or cast) function OID. `json.c`
    /// always passes `is_jsonb = false`.
    pub fn categorize_type(typoid: Oid) -> PgResult<(JsonTypeCategory, Oid)>
);

seam_core::seam!(
    /// `func_volatile(funcid)` — the catalog volatility of a function as the
    /// `PROVOLATILE_*` byte (`'i'`/`'s'`/`'v'`). Used by `to_json_is_immutable`.
    pub fn func_volatile(funcid: Oid) -> PgResult<u8>
);

seam_core::seam!(
    /// `OidOutputFunctionCall(outfuncoid, val)` — the resolved type output
    /// function's text representation of `val` (NUL-excluded bytes). Allocates
    /// in `mcx` (the fmgr call and the result bytes), so it carries the context.
    pub fn output_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        outfuncoid: Oid,
        val: &Datum<'mcx>,
    ) -> PgResult<Vec<u8>>
);

seam_core::seam!(
    /// `OidFunctionCall1(outfuncoid, val)` for `JSONTYPE_CAST`, then
    /// `VARDATA_ANY`/`VARSIZE_ANY_EXHDR` — the explicit cast-to-json function's
    /// text result bytes (already JSON). Allocates in `mcx`.
    pub fn cast_function_call<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        outfuncoid: Oid,
        val: &Datum<'mcx>,
    ) -> PgResult<Vec<u8>>
);

seam_core::seam!(
    /// The special-case text output (`F_TEXTOUT`/`F_VARCHAROUT`/`F_BPCHAROUT`)
    /// in `datum_to_json_internal`'s default arm — the detoasted text payload
    /// bytes of `val`, which the caller escapes via `escape_json_with_len`.
    /// Detoasts into `mcx`.
    pub fn text_datum_bytes<'mcx>(mcx: mcx::Mcx<'mcx>, val: &Datum<'mcx>) -> PgResult<Vec<u8>>
);

seam_core::seam!(
    /// True if `outfuncoid` is one of `F_TEXTOUT`/`F_VARCHAROUT`/`F_BPCHAROUT`,
    /// selecting the fast text-escape path in `datum_to_json_internal`.
    pub fn is_text_output_func(outfuncoid: Oid) -> bool
);

seam_core::seam!(
    /// The catalog/`array.c` half of `array_to_json_internal` —
    /// `get_typlenbyvalalign` + `json_categorize_type(element_type, ...)` +
    /// `deconstruct_array`. Returns the element classification and the flat
    /// element/null vectors plus dimensionality; the structural `[ ... ]`
    /// assembly (`array_dim_to_json`) stays in-crate. Allocates in `mcx` (the
    /// detoasted array image and the element/null vectors).
    pub fn deconstruct_array<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        array: &Datum<'mcx>,
    ) -> PgResult<ArrayForJson<'mcx>>
);

seam_core::seam!(
    /// The catalog half of `composite_to_json` — `lookup_rowtype_tupdesc`, the
    /// per-attribute `heap_getattr`, and the per-attribute
    /// `json_categorize_type`. Returns one entry per *non-dropped* attribute
    /// (dropped attributes already skipped, matching the C `continue`); the
    /// `{ ... }` assembly stays in-crate. Allocates in `mcx` (the looked-up
    /// tuple descriptor work and the per-attribute value Datums).
    pub fn walk_composite<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        composite: &Datum<'mcx>,
    ) -> PgResult<Vec<CompositeFieldForJson<'mcx>>>
);
