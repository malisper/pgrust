//! Installs the inward seams this unit owns from `jsonfuncs.c`.
//!
//! Of the seams declared in `backend-utils-adt-jsonfuncs-seams`, only three are
//! `jsonfuncs.c` functions and are this unit's to implement:
//!   * `categorize_type`     — `json_categorize_type` (jsonfuncs.c:5998),
//!   * `func_volatile`        — thin `lsyscache::func_volatile` wrapper used by
//!     `to_json_is_immutable`, no allocation, declared without `Mcx`,
//!   * `is_text_output_func`  — jsonfuncs-local OID compare (categorize.rs).
//!
//! The other five seams in that crate are NOT `jsonfuncs.c` functions:
//! `output_function_call` / `cast_function_call` are `OidOutputFunctionCall` /
//! `OidFunctionCall1` (fmgr.c), `text_datum_bytes` is a varlena detoast,
//! `deconstruct_array` is arrayfuncs.c, and `walk_composite` is the inline
//! composite-walk over `lookup_rowtype_tupdesc` + `heap_getattr`
//! (typcache/heaptuple). They were homed in this seam crate by the `json.c`
//! porter because `json_categorize_type` is their neighbour; their declarations
//! also omit the `Mcx<'mcx>` the real allocating calls require. Their faithful
//! provider is the fmgr/arrayfuncs/typcache layer with `Mcx`-carrying seams
//! (`backend-utils-fmgr-fmgr-seams::oid_output_function_call`, etc.), not this
//! unit; re-homing them (and updating the merged json/jsonb call sites) is a
//! separate change. They remain a loud panic — their state before this port —
//! rather than a fabricated result.

use backend_utils_adt_jsonfuncs_seams as seams;

use crate::categorize::{is_text_output_func, json_categorize_type};

/// Install the `jsonfuncs.c` seams in `backend-utils-adt-jsonfuncs-seams`.
pub fn init_seams() {
    // `json_categorize_type(typoid, false, ...)` — json always passes
    // is_jsonb = false (jsonb renders through its own datum_to_jsonb).
    seams::categorize_type::set(|typoid| json_categorize_type(typoid, false));

    // `func_volatile(funcid)` — the PROVOLATILE_* byte.
    seams::func_volatile::set(backend_utils_cache_lsyscache::function::func_volatile);

    // `is_text_output_func(outfuncoid)`.
    seams::is_text_output_func::set(is_text_output_func);
}
