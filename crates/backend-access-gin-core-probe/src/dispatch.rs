//! Typed by-OID dispatch for the GIN `anyarray_ops` opclass support procedures
//! (`ginarrayproc.c`).
//!
//! The GIN access method resolves each opclass support function into a
//! `GinState` / `GinScanKey` `FmgrInfo` slot (`index_getprocinfo` →
//! `fmgr_info`), and the build / scan machinery invokes them through the
//! `ginutil-seams` (`gin_extract_value` / `gin_extract_query`) and
//! `core-probe-seams` (`gin_consistent_call_bool` / `gin_consistent_call_tri`)
//! seams. Those support procs are `prolang => internal` functions whose
//! out-parameters (`*nentries`, `**nullFlags`, `*searchMode`, `*recheck`) cannot
//! cross the by-word fmgr `Datum` lane.
//!
//! The faithful re-model — exactly the proven typanalyze (`array_typanalyze`),
//! BRIN (`F_BRIN_MINMAX_*`) and GiST (`F_GIST_*`) opclass-dispatch idiom — is a
//! TYPED dispatch keyed on the resolved support-proc OID (`FmgrInfo::fn_oid` /
//! `GinScanKey::*_fmgr_oid`, the `index_getprocinfo` row). This module installs
//! the four GIN dispatch seams over the `anyarray_ops` OIDs and routes each to
//! the ported [`crate::ginarrayproc`] body, passing the real Rust references the
//! C out-parameters stand for. The element keys travel as the canonical unified
//! `types_tuple::Datum` value (via `deconstruct_array_v`), so by-reference
//! element types (`text[]`, …) carry real `ByRef` bytes, not a dangling word.
//!
//! For `fmgr_info` to resolve the `internal`-language prosrc names at all (else
//! `CREATE INDEX ... USING gin` errors `internal function "ginarrayextract" is
//! not in internal lookup table`), the `fmgr_builtins[]` rows are registered in
//! [`crate::fmgr_builtins`]; their `fn_addr` is structurally never reached (the
//! AM dispatches by `fn_oid`), mirroring the GiST opclass.
//!
//! tsvector_ops / jsonb_ops support procs (whose OIDs are not handled here)
//! bottom out loudly — those opclass bodies are the genuine residual GIN port.

use mcx::{Mcx, PgVec};
use types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_access_gin_core_probe_seams::{gin_consistent_call_bool, gin_consistent_call_tri};
use backend_access_gin_ginutil_seams::{
    gin_extract_query, gin_extract_value, GinExtractQueryResult,
};
use backend_utils_adt_arrayfuncs_seams as arrayfuncs_seams;
use backend_utils_cache_lsyscache_seams as lsyscache_seams;

use types_gin::{GinTernaryValue, GIN_CAT_NULL_KEY};

use crate::ginarrayproc::{
    self, GinContainedStrategy, GinContainsStrategy, GinEqualStrategy, GinOverlapStrategy,
};
use crate::ginlogic::GinScanKey;

// GIN search modes (gin.h) — the `*searchMode` values `ginqueryarrayextract`
// selects per strategy.
const GIN_SEARCH_MODE_DEFAULT: i32 = 0;
const GIN_SEARCH_MODE_INCLUDE_EMPTY: i32 = 1;
const GIN_SEARCH_MODE_ALL: i32 = 2;

// pg_proc OIDs (fmgroids.h / pg_proc.dat) of the `anyarray_ops` GIN support
// procedures. These are the values `index_getprocinfo` records in the resolved
// `FmgrInfo::fn_oid` for the array opclass.
/// `ginarrayextract(anyarray, internal, internal)` — `extractValue`.
pub const F_GINARRAYEXTRACT: u32 = 2743;
/// `ginarrayextract(anyarray, internal)` — legacy two-arg `extractValue`.
pub const F_GINARRAYEXTRACT_2ARGS: u32 = 3076;
/// `ginqueryarrayextract(...)` — `extractQuery`.
pub const F_GINQUERYARRAYEXTRACT: u32 = 2774;
/// `ginarrayconsistent(...)` — boolean `consistent`.
pub const F_GINARRAYCONSISTENT: u32 = 2744;
/// `ginarraytriconsistent(...)` — ternary `triConsistent`.
pub const F_GINARRAYTRICONSISTENT: u32 = 3920;

/// Deconstruct an array `Datum` into its canonical element values + null flags,
/// the shared front half of `ginarrayextract` / `ginqueryarrayextract`
/// (`get_typlenbyvalalign(ARR_ELEMTYPE(array))` + `deconstruct_array`). The
/// element type is read from the array header (`ARR_ELEMTYPE`); the elements
/// travel as the canonical `types_tuple::Datum` so by-reference keys keep their
/// bytes.
fn deconstruct_query_or_value<'mcx>(
    mcx: Mcx<'mcx>,
    array: Datum<'mcx>,
) -> PgResult<(PgVec<'mcx, Datum<'mcx>>, PgVec<'mcx, bool>)> {
    // ARR_ELEMTYPE(DatumGetArrayTypeP(array)) — the element type OID, read from
    // the detoasted array bytes.
    let arr_elemtype: Oid =
        arrayfuncs_seams::array_get_elemtype_bytes::call(mcx, array.as_ref_bytes())?;

    // get_typlenbyvalalign(ARR_ELEMTYPE(array), &elmlen, &elmbyval, &elmalign);
    let tlba = lsyscache_seams::get_typlenbyvalalign::call(arr_elemtype)?;

    // deconstruct_array(array, ARR_ELEMTYPE(array), elmlen, elmbyval, elmalign,
    //                   &elems, &nulls, &nelems);
    let pairs = arrayfuncs_seams::deconstruct_array_v::call(
        mcx,
        array,
        arr_elemtype,
        tlba.typlen,
        tlba.typbyval,
        tlba.typalign as core::ffi::c_char,
    )?;

    let mut elems: PgVec<'mcx, Datum<'mcx>> = PgVec::new_in(mcx);
    let mut nulls: PgVec<'mcx, bool> = PgVec::new_in(mcx);
    for (d, isnull) in pairs {
        elems.push(d);
        nulls.push(isnull);
    }
    Ok((elems, nulls))
}

/// `gin_extract_value` dispatch (`extractValueFn`, `FunctionCall3Coll`): route
/// `flinfo.fn_oid` to the `anyarray_ops` `ginarrayextract` body. Returns the
/// extracted element keys + per-key null flags (the seam's `*nkeys` /
/// `**nullFlags` out-params), or `None` (no keys) for the placeholder path.
fn dispatch_extract_value<'mcx>(
    mcx: Mcx<'mcx>,
    flinfo: &types_core::fmgr::FmgrInfo,
    _collation: Oid,
    value: Datum<'mcx>,
) -> PgResult<Option<(PgVec<'mcx, Datum<'mcx>>, PgVec<'mcx, bool>)>> {
    match flinfo.fn_oid {
        F_GINARRAYEXTRACT | F_GINARRAYEXTRACT_2ARGS => {
            // ginarrayextract / ginarrayextract_2args: identical extraction (the
            // legacy 2-arg wrapper just forwards). The fmgr wrapper supplies the
            // already-copied array; the canonical `value` carries the bytes.
            let (elems, nulls) = deconstruct_query_or_value(mcx, value)?;
            Ok(Some((elems, nulls)))
        }
        other => Err(unported(other, "extractValue")),
    }
}

/// `gin_extract_query` dispatch (`extractQueryFn`, `FunctionCall7Coll`): route
/// `flinfo.fn_oid` to the `anyarray_ops` `ginqueryarrayextract` body. Returns
/// the extracted query keys + null flags + search mode (the seam's out-params);
/// `partial_matches` / `extra_data` stay empty (the C `NULL` — array_ops sets
/// neither).
fn dispatch_extract_query<'mcx>(
    mcx: Mcx<'mcx>,
    flinfo: &types_core::fmgr::FmgrInfo,
    _collation: Oid,
    query: Datum<'mcx>,
    strategy: u16,
) -> PgResult<GinExtractQueryResult<'mcx>> {
    match flinfo.fn_oid {
        F_GINQUERYARRAYEXTRACT => {
            let (elems, nulls) = deconstruct_query_or_value(mcx, query)?;
            let nelems = elems.len() as i32;

            // switch (strategy) { ... *searchMode = ...; } — mirrors
            // ginqueryarrayextract (ginarrayproc.c:107) exactly.
            let search_mode: i32 = match strategy {
                GinOverlapStrategy => GIN_SEARCH_MODE_DEFAULT,
                GinContainsStrategy => {
                    if nelems > 0 {
                        GIN_SEARCH_MODE_DEFAULT
                    } else {
                        // everything contains the empty set
                        GIN_SEARCH_MODE_ALL
                    }
                }
                GinContainedStrategy => {
                    // empty set is contained in everything
                    GIN_SEARCH_MODE_INCLUDE_EMPTY
                }
                GinEqualStrategy => {
                    if nelems > 0 {
                        GIN_SEARCH_MODE_DEFAULT
                    } else {
                        GIN_SEARCH_MODE_INCLUDE_EMPTY
                    }
                }
                other => {
                    return Err(PgError::error(format!(
                        "ginqueryarrayextract: unknown strategy number: {other}"
                    )));
                }
            };

            Ok(GinExtractQueryResult {
                query_values: elems,
                null_flags: nulls,
                // array_ops returns no partial-match / extra-data arrays.
                partial_matches: PgVec::new_in(mcx),
                extra_data: PgVec::new_in(mcx),
                search_mode,
            })
        }
        other => Err(unported(other, "extractQuery")),
    }
}

/// `gin_consistent_call_bool` dispatch (`consistentFn`, `FunctionCall8Coll`):
/// route `key.consistent_fmgr_oid` to the `anyarray_ops` `ginarrayconsistent`
/// body. The C `check` (arg 0) is `key->entryRes` read as `bool[]`, `nkeys` is
/// `key->nuserentries`, `nullFlags` (arg 7) is `key->queryCategories` read as
/// `bool[]` (`GIN_CAT_NULL_KEY == 1` → null), and `recheck` (arg 5) is written
/// back into `key->recheckCurItem`.
fn dispatch_consistent_bool(key: &mut GinScanKey) -> bool {
    match key.consistent_fmgr_oid {
        F_GINARRAYCONSISTENT => {
            let nkeys = key.nuserentries as usize;
            // check[i] = (entryRes[i] != GIN_FALSE) — GinTernaryValue as bool.
            let check: Vec<bool> =
                key.entryRes[..nkeys].iter().map(|&v| v != 0).collect();
            // nullFlags[i] = (queryCategories[i] == GIN_CAT_NULL_KEY).
            let null_flags: Vec<bool> = key.queryCategories[..nkeys]
                .iter()
                .map(|&c| c == GIN_CAT_NULL_KEY)
                .collect();
            let mut recheck = key.recheckCurItem;
            let res = match ginarrayproc::ginarrayconsistent(
                &check,
                key.strategy,
                nkeys as i32,
                &null_flags,
                &mut recheck,
            ) {
                Ok(r) => r,
                Err(e) => std::panic::panic_any(e),
            };
            key.recheckCurItem = recheck;
            res
        }
        other => std::panic::panic_any(unported(other, "consistent")),
    }
}

/// `gin_consistent_call_tri` dispatch (`triConsistentFn`, `FunctionCall7Coll`):
/// route `key.tri_consistent_fmgr_oid` to the `anyarray_ops`
/// `ginarraytriconsistent` body. Same arg mapping as the boolean dispatch, but
/// `check` carries the ternary `GIN_TRUE`/`GIN_FALSE`/`GIN_MAYBE` values and
/// there is no `recheck` out-param (the ternary interface encodes recheck as
/// `GIN_MAYBE`).
fn dispatch_consistent_tri(key: &mut GinScanKey) -> GinTernaryValue {
    match key.tri_consistent_fmgr_oid {
        F_GINARRAYTRICONSISTENT => {
            let nkeys = key.nuserentries as usize;
            let check: Vec<GinTernaryValue> = key.entryRes[..nkeys].to_vec();
            let null_flags: Vec<bool> = key.queryCategories[..nkeys]
                .iter()
                .map(|&c| c == GIN_CAT_NULL_KEY)
                .collect();
            match ginarrayproc::ginarraytriconsistent(
                &check,
                key.strategy,
                nkeys as i32,
                &null_flags,
            ) {
                Ok(r) => r,
                Err(e) => std::panic::panic_any(e),
            }
        }
        other => std::panic::panic_any(unported(other, "triConsistent")),
    }
}

/// The loud bottom-out for a GIN opclass support-proc OID this dispatch does not
/// handle (a tsvector_ops / jsonb_ops support proc whose body is not yet ported,
/// or a user-defined opclass that would need a `Datum::Internal` fmgr arm).
fn unported(foid: u32, role: &str) -> PgError {
    PgError::error(format!(
        "GIN opclass {role} support function (OID {foid}) has no owned dispatch \
         (only the anyarray_ops procedures are wired through the typed by-OID \
         GIN dispatch; tsvector_ops / jsonb_ops opclass bodies remain to be ported)"
    ))
}

/// Install the four GIN opclass-dispatch seams over the `anyarray_ops`
/// support-proc OIDs. Called from this crate's `init_seams()`.
pub fn install() {
    gin_extract_value::set(dispatch_extract_value);
    gin_extract_query::set(dispatch_extract_query);
    gin_consistent_call_bool::set(dispatch_consistent_bool);
    gin_consistent_call_tri::set(dispatch_consistent_tri);
}
