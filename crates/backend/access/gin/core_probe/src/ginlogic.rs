//! `src/backend/access/gin/ginlogic.c` (PostgreSQL 18.3) — the binary- and
//! ternary-logic consistent-check routing for GIN.
//!
//! A GIN operator class can provide a boolean or ternary consistent function,
//! or both. This file provides both interfaces to the rest of GIN even if only
//! one is implemented. Providing a boolean interface over a ternary fn is
//! straightforward (map `GIN_MAYBE` to `true`+recheck); providing a ternary
//! interface over a boolean fn calls it with all MAYBE arguments set to all
//! combinations of TRUE/FALSE (up to `MAX_MAYBE_ENTRIES`).
//!
//! Every function is ported 1:1: `trueConsistentFn`, `trueTriConsistentFn`,
//! `directBoolConsistentFn`, `directTriConsistentFn`, `shimBoolConsistentFn`,
//! `shimTriConsistentFn`, and `ginInitConsistentFunction`. The C function
//! pointers `key->boolConsistentFn`/`triConsistentFn` are modeled as dispatch
//! tags chosen by `ginInitConsistentFunction` and invoked by
//! [`callBoolConsistentFn`]/[`callTriConsistentFn`].
//!
//! The four `direct*`/`shim*` helpers issue `FunctionCall8Coll` /
//! `FunctionCall7Coll` into the opclass support functions — the fmgr calling
//! convention, a genuine external — so they go through this unit's
//! `gin_consistent_call_bool` / `gin_consistent_call_tri` seams (loud-panic
//! until the fmgr GIN-call dispatcher installs them). There is no
//! `ereport`/`elog` anywhere in `ginlogic.c`, so this module raises no errors.

use ::types_core::OidIsValid;
use ::tsearch::gin::{GinTernaryValue, GIN_FALSE, GIN_MAYBE, GIN_SEARCH_MODE_EVERYTHING, GIN_TRUE};

pub use ::gin::GinState;
pub use ::tsearch::backend_access_gin_ginlogic::{
    GinBoolConsistentKind, GinScanKey, GinTriConsistentKind,
};

use ::core_probe_seams::{gin_consistent_call_bool, gin_consistent_call_tri};

/// `MAX_MAYBE_ENTRIES` (ginlogic.c:44): maximum number of MAYBE inputs that
/// [`shimTriConsistentFn`] will resolve by enumerating all combinations.
pub const MAX_MAYBE_ENTRIES: usize = 4;

/// `trueConsistentFn` (ginlogic.c:49): dummy boolean consistent function for an
/// EVERYTHING key. Just claim it matches, without recheck.
pub fn trueConsistentFn(key: &mut GinScanKey) -> bool {
    key.recheckCurItem = false;
    true
}

/// `trueTriConsistentFn` (ginlogic.c:55): dummy ternary consistent function for
/// an EVERYTHING key. Just claim it matches.
pub fn trueTriConsistentFn(_key: &mut GinScanKey) -> GinTernaryValue {
    GIN_TRUE
}

/// `directBoolConsistentFn` (ginlogic.c:64): helper for calling a regular,
/// binary-logic consistent function. Initializes `recheckCurItem` in case the
/// consistent function doesn't know it should set it (the safe assumption then
/// is to force recheck), then issues the `FunctionCall8Coll` through the seam.
pub fn directBoolConsistentFn(key: &mut GinScanKey) -> bool {
    key.recheckCurItem = true;
    gin_consistent_call_bool::call(key)
}

/// `directTriConsistentFn` (ginlogic.c:88): helper for calling a native
/// ternary-logic consistent function (the `FunctionCall7Coll`).
pub fn directTriConsistentFn(key: &mut GinScanKey) -> GinTernaryValue {
    gin_consistent_call_tri::call(key)
}

/// `shimBoolConsistentFn` (ginlogic.c:107): a binary-logic consistency check
/// implemented via the opclass's ternary consistent function. `GIN_MAYBE` is
/// interpreted as `true` with the recheck flag set.
pub fn shimBoolConsistentFn(key: &mut GinScanKey) -> bool {
    let result = gin_consistent_call_tri::call(key);
    if result == GIN_MAYBE {
        key.recheckCurItem = true;
        true
    } else {
        key.recheckCurItem = false;
        // C returns the GinTernaryValue directly as a bool; in the non-MAYBE
        // branch it is GIN_TRUE (1) or GIN_FALSE (0), so `!= 0` matches.
        result != 0
    }
}

/// `shimTriConsistentFn` (ginlogic.c:147): a tri-state consistency check
/// implemented via the opclass's boolean consistent function.
///
/// Calls the boolean consistent function with the MAYBE inputs replaced with
/// every combination of TRUE/FALSE. If the boolean function returns the same
/// value for every combination, that's the overall result; otherwise the result
/// is MAYBE. Testing every combination is O(2^n), so this is only feasible for a
/// small number of MAYBE inputs (capped at [`MAX_MAYBE_ENTRIES`]).
///
/// NB: this function modifies `key.entryRes`, but restores the entry-time
/// contents before returning.
pub fn shimTriConsistentFn(key: &mut GinScanKey) -> GinTernaryValue {
    let mut maybeEntries = [0usize; MAX_MAYBE_ENTRIES];
    let mut nmaybe: usize = 0;

    // Count how many MAYBE inputs there are, and store their indexes in
    // maybeEntries. If there are too many, give up and return MAYBE.
    for i in 0..key.nentries as usize {
        if key.entryRes[i] == GIN_MAYBE {
            if nmaybe >= MAX_MAYBE_ENTRIES {
                return GIN_MAYBE;
            }
            maybeEntries[nmaybe] = i;
            nmaybe += 1;
        }
    }

    // If none of the inputs were MAYBE, we can just call the consistent fn.
    if nmaybe == 0 {
        return bool_to_tri(directBoolConsistentFn(key));
    }

    // First call consistent function with all the maybe-inputs set FALSE.
    for &idx in maybeEntries.iter().take(nmaybe) {
        key.entryRes[idx] = GIN_FALSE;
    }
    let mut curResult = bool_to_tri(directBoolConsistentFn(key));
    let mut recheck = key.recheckCurItem;

    loop {
        // Twiddle the entries for next combination.
        let mut i: usize = 0;
        while i < nmaybe {
            if key.entryRes[maybeEntries[i]] == GIN_FALSE {
                key.entryRes[maybeEntries[i]] = GIN_TRUE;
                break;
            } else {
                key.entryRes[maybeEntries[i]] = GIN_FALSE;
            }
            i += 1;
        }
        if i == nmaybe {
            break;
        }

        let boolResult = bool_to_tri(directBoolConsistentFn(key));
        recheck |= key.recheckCurItem;

        if curResult != boolResult {
            curResult = GIN_MAYBE;
            break;
        }
    }

    // TRUE with recheck is taken to mean MAYBE.
    if curResult == GIN_TRUE && recheck {
        curResult = GIN_MAYBE;
    }

    // We must restore the original state of the entryRes array.
    for &idx in maybeEntries.iter().take(nmaybe) {
        key.entryRes[idx] = GIN_MAYBE;
    }

    curResult
}

/// C assigns a `bool` return value directly into a `GinTernaryValue`: `true`
/// becomes `GIN_TRUE` (1) and `false` becomes `GIN_FALSE` (0).
#[inline]
fn bool_to_tri(b: bool) -> GinTernaryValue {
    if b {
        GIN_TRUE
    } else {
        GIN_FALSE
    }
}

/// `ginInitConsistentFunction` (ginlogic.c:226): set up the implementation of
/// the consistent functions for a scan key.
///
/// For an EVERYTHING key both interfaces use the dummy `true*` functions.
/// Otherwise the key's consistent/triConsistent OIDs and collation are wired
/// from `ginstate` at `attnum - 1`, and each interface uses the `direct*` helper
/// when the opclass provides that function (`OidIsValid(fn_oid)`) or the `shim*`
/// helper otherwise.
pub fn ginInitConsistentFunction(ginstate: &GinState, key: &mut GinScanKey) {
    if key.searchMode == GIN_SEARCH_MODE_EVERYTHING {
        key.boolConsistentFn = GinBoolConsistentKind::True;
        key.triConsistentFn = GinTriConsistentKind::True;
    } else {
        // C: key->consistentFmgrInfo = &ginstate->consistentFn[key->attnum - 1];
        //    key->triConsistentFmgrInfo = &ginstate->triConsistentFn[key->attnum - 1];
        //    key->collation = ginstate->supportCollation[key->attnum - 1];
        let attno = (key.attnum - 1) as usize;
        let consistent_fn_oid = ginstate.consistentFn[attno].fn_oid;
        let tri_consistent_fn_oid = ginstate.triConsistentFn[attno].fn_oid;

        key.consistent_fmgr_oid = consistent_fn_oid;
        key.tri_consistent_fmgr_oid = tri_consistent_fn_oid;
        key.collation = ginstate.supportCollation[attno];

        if OidIsValid(consistent_fn_oid) {
            key.boolConsistentFn = GinBoolConsistentKind::Direct;
        } else {
            key.boolConsistentFn = GinBoolConsistentKind::Shim;
        }

        if OidIsValid(tri_consistent_fn_oid) {
            key.triConsistentFn = GinTriConsistentKind::Direct;
        } else {
            key.triConsistentFn = GinTriConsistentKind::Shim;
        }
    }
}

/// Invoke the boolean consistent implementation selected for `key`
/// (C: `key->boolConsistentFn(key)`).
pub fn callBoolConsistentFn(key: &mut GinScanKey) -> bool {
    match key.boolConsistentFn {
        GinBoolConsistentKind::True => trueConsistentFn(key),
        GinBoolConsistentKind::Direct => directBoolConsistentFn(key),
        GinBoolConsistentKind::Shim => shimBoolConsistentFn(key),
    }
}

/// Invoke the ternary consistent implementation selected for `key`
/// (C: `key->triConsistentFn(key)`).
pub fn callTriConsistentFn(key: &mut GinScanKey) -> GinTernaryValue {
    match key.triConsistentFn {
        GinTriConsistentKind::True => trueTriConsistentFn(key),
        GinTriConsistentKind::Direct => directTriConsistentFn(key),
        GinTriConsistentKind::Shim => shimTriConsistentFn(key),
    }
}
