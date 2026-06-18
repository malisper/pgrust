//! Installs the inward seams declared in `backend-utils-adt-jsonfuncs-seams`,
//! which the `json`/`jsonb` crates (this unit's cycle partners, below it in the
//! dep graph) call through the seam crate.
//!
//! Three are `jsonfuncs.c` proper:
//!   * `categorize_type`     — `json_categorize_type` (jsonfuncs.c:5998),
//!   * `func_volatile`        — thin `lsyscache::func_volatile` wrapper used by
//!     `to_json_is_immutable`, no allocation, declared without `Mcx`,
//!   * `is_text_output_func`  — jsonfuncs-local OID compare (categorize.rs).
//!
//! The other five are the catalog/fmgr/array/typcache *halves* of `json.c`'s
//! `datum_to_json_internal` / `array_to_json_internal` / `composite_to_json`
//! (`OidOutputFunctionCall` / `OidFunctionCall1` (fmgr.c), `TextDatumGetCString`
//! (varlena), `deconstruct_array` (arrayfuncs.c) + the json element
//! classification, and the inline composite walk over `lookup_rowtype_tupdesc`
//! + `heap_getattr` + `json_categorize_type`). They were homed in this seam
//! crate by the `json.c` porter because `json_categorize_type` is their
//! neighbour; `jsonfuncs` already depends on every one of those owners
//! (fmgr-core, arrayfuncs, typcache, the varlena/fmgr/detoast seam crates), so
//! it is their faithful provider. They are implemented in [`crate::json_render`]
//! and installed here (each now carries the `Mcx<'mcx>` the real allocating
//! calls require — the contract reconcile that retired their `Mcx`-less,
//! never-installed declarations).

use backend_utils_adt_jsonfuncs_seams as seams;

use crate::categorize::{is_text_output_func, json_categorize_type};
use crate::json_render;

/// Install the inward seams in `backend-utils-adt-jsonfuncs-seams`.
pub fn init_seams() {
    // Register the expressible scalar/operator `jsonfuncs.c` builtins into the
    // fmgr-core builtin table (C: `fmgr_builtins[]`).
    crate::fmgr_builtins::register_jsonfuncs_builtins();

    // `json_categorize_type(typoid, false, ...)` — json always passes
    // is_jsonb = false (jsonb renders through its own datum_to_jsonb).
    seams::categorize_type::set(|typoid| json_categorize_type(typoid, false));

    // `func_volatile(funcid)` — the PROVOLATILE_* byte.
    seams::func_volatile::set(backend_utils_cache_lsyscache::function::func_volatile);

    // `is_text_output_func(outfuncoid)`.
    seams::is_text_output_func::set(is_text_output_func);

    // The json-rendering catalog/fmgr/array/typcache halves (json.c / jsonb.c):
    seams::output_function_call::set(json_render::output_function_call);
    seams::cast_function_call::set(json_render::cast_function_call);
    seams::text_datum_bytes::set(json_render::text_datum_bytes);
    seams::deconstruct_array::set(json_render::deconstruct_array);
    seams::walk_composite::set(json_render::walk_composite);
}
