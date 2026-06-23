//! `src/backend/access/gin/ginarrayproc.c` (PostgreSQL 18.3) — the GIN support
//! procedures for indexing any array type: the `extractValue`, `extractQuery`,
//! `consistent`, and `triConsistent` opclass support functions
//! (`anyarray_ops`).
//!
//! Every function is implemented with logic identical to the C — same control
//! flow, loop bounds, switch arms, branch order, message text, and SQLSTATE.
//!
//! The fmgr `Datum`/out-parameter layer is a separate subsystem: the cores here
//! take typed parameters and return typed results, with the
//! `*nkeys`/`*nullFlags`/`*searchMode` out-parameters carried as struct fields.
//! The two catalog/array-runtime calls (`get_typlenbyvalalign`,
//! `deconstruct_array`) reach across a subsystem boundary, so they go through
//! the installed lsyscache / arrayfuncs seams.

use ::mcx::Mcx;
use ::types_core::Oid;
use ::datum::datum::Datum;
use ::types_error::{ErrorLocation, PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use ::types_jsonb::jsonb::{
    GinTernaryValue, GIN_FALSE, GIN_MAYBE, GIN_SEARCH_MODE_ALL, GIN_SEARCH_MODE_DEFAULT,
    GIN_SEARCH_MODE_INCLUDE_EMPTY, GIN_TRUE,
};

use ::types_scan::StrategyNumber;

use arrayfuncs_seams as arrayfuncs_seams;
use lsyscache_seams as lsyscache_seams;

// GIN array-opclass strategy numbers (ginarrayproc.c:23-26).
/// `GinOverlapStrategy` — the `&&` (overlaps) strategy.
pub const GinOverlapStrategy: StrategyNumber = 1;
/// `GinContainsStrategy` — the `@>` (contains) strategy.
pub const GinContainsStrategy: StrategyNumber = 2;
/// `GinContainedStrategy` — the `<@` (contained-by) strategy.
pub const GinContainedStrategy: StrategyNumber = 3;
/// `GinEqualStrategy` — the `=` (equals) strategy.
pub const GinEqualStrategy: StrategyNumber = 4;

/// The result of [`ginarrayextract`] — the values C reports through its
/// out-parameters and return value (`*nkeys`, `*nullFlags`, and the `elems`
/// return). `nkeys == elems.len()`.
#[derive(Clone, Debug, Default)]
pub struct GinArrayExtractResult {
    /// Return value (`elems`): the extracted element keys.
    pub elems: Vec<Datum>,
    /// `*nkeys`: the number of keys (== `elems.len() == nelems`).
    pub nkeys: i32,
    /// `*nullFlags`: per-key NULL flags (length `nkeys`).
    pub null_flags: Vec<bool>,
}

/// The result of [`ginqueryarrayextract`] — like [`GinArrayExtractResult`] plus
/// the `*searchMode` out-parameter selected by the strategy.
#[derive(Clone, Debug, Default)]
pub struct GinQueryArrayExtractResult {
    /// Return value (`elems`): the extracted element keys.
    pub elems: Vec<Datum>,
    /// `*nkeys`: the number of keys.
    pub nkeys: i32,
    /// `*nullFlags`: per-key NULL flags (length `nkeys`).
    pub null_flags: Vec<bool>,
    /// `*searchMode`: the GIN search mode chosen for the strategy.
    pub search_mode: i32,
}

/// Split the deconstructed `(Datum, isnull)` pairs into the parallel `elems` /
/// `null_flags` arrays C's `deconstruct_array` fills (`&elemsp`, `&nullsp`).
fn split_pairs(pairs: &[(Datum, bool)]) -> (Vec<Datum>, Vec<bool>) {
    let mut elems = Vec::with_capacity(pairs.len());
    let mut nulls = Vec::with_capacity(pairs.len());
    for &(d, isnull) in pairs {
        elems.push(d);
        nulls.push(isnull);
    }
    (elems, nulls)
}

/// `ginarrayextract` (ginarrayproc.c:32): the `extractValue` support function.
///
/// Looks up the element type's len/byval/align, deconstructs `arraydatum` into
/// its element Datums + NULL flags, and reports them (with the count in
/// `*nkeys`). The fmgr wrapper supplies the already-copied array
/// (`PG_GETARG_ARRAYTYPE_P_COPY(0)`) as `arraydatum` and `ARR_ELEMTYPE(array)`
/// as `arr_elemtype`.
pub fn ginarrayextract(
    mcx: Mcx<'_>,
    arraydatum: Datum,
    arr_elemtype: Oid,
) -> PgResult<GinArrayExtractResult> {
    // get_typlenbyvalalign(ARR_ELEMTYPE(array), &elmlen, &elmbyval, &elmalign);
    let tlba = lsyscache_seams::get_typlenbyvalalign::call(arr_elemtype)?;

    // deconstruct_array(array, ARR_ELEMTYPE(array), elmlen, elmbyval, elmalign,
    //                   &elems, &nulls, &nelems);
    let pairs = arrayfuncs_seams::deconstruct_array::call(
        mcx,
        arraydatum,
        arr_elemtype,
        tlba.typlen,
        tlba.typbyval,
        tlba.typalign as core::ffi::c_char,
    )?;
    let (elems, nulls) = split_pairs(&pairs);
    let nelems = elems.len() as i32;

    // *nkeys = nelems; *nullFlags = nulls; PG_RETURN_POINTER(elems);
    Ok(GinArrayExtractResult {
        elems,
        nkeys: nelems,
        null_flags: nulls,
    })
}

/// `ginarrayextract_2args` (ginarrayproc.c:67): legacy two-argument
/// compatibility wrapper. Errors if called with fewer than three args (which
/// should not happen), otherwise delegates to [`ginarrayextract`].
///
/// `nargs` is C's `PG_NARGS()` (supplied by the fmgr wrapper).
pub fn ginarrayextract_2args(
    mcx: Mcx<'_>,
    nargs: i32,
    arraydatum: Datum,
    arr_elemtype: Oid,
) -> PgResult<GinArrayExtractResult> {
    // if (PG_NARGS() < 3)  /* should not happen */
    //     elog(ERROR, "ginarrayextract requires three arguments");
    if nargs < 3 {
        return Err(elog_error(
            "ginarrayextract requires three arguments".into(),
            71,
            "ginarrayextract_2args",
        ));
    }
    // return ginarrayextract(fcinfo);
    ginarrayextract(mcx, arraydatum, arr_elemtype)
}

/// `ginqueryarrayextract` (ginarrayproc.c:78): the `extractQuery` support
/// function. Deconstructs the query `arraydatum` exactly as [`ginarrayextract`]
/// does, then selects the GIN search mode (`*searchMode`) for `strategy`.
pub fn ginqueryarrayextract(
    mcx: Mcx<'_>,
    arraydatum: Datum,
    arr_elemtype: Oid,
    strategy: StrategyNumber,
) -> PgResult<GinQueryArrayExtractResult> {
    // get_typlenbyvalalign(ARR_ELEMTYPE(array), &elmlen, &elmbyval, &elmalign);
    let tlba = lsyscache_seams::get_typlenbyvalalign::call(arr_elemtype)?;

    // deconstruct_array(array, ARR_ELEMTYPE(array), elmlen, elmbyval, elmalign,
    //                   &elems, &nulls, &nelems);
    let pairs = arrayfuncs_seams::deconstruct_array::call(
        mcx,
        arraydatum,
        arr_elemtype,
        tlba.typlen,
        tlba.typbyval,
        tlba.typalign as core::ffi::c_char,
    )?;
    let (elems, nulls) = split_pairs(&pairs);
    let nelems = elems.len() as i32;

    // *nkeys = nelems; *nullFlags = nulls;
    // switch (strategy) { ... *searchMode = ...; }
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
        _ => {
            return Err(elog_error(
                format!("ginqueryarrayextract: unknown strategy number: {strategy}"),
                130,
                "ginqueryarrayextract",
            ));
        }
    };

    // PG_RETURN_POINTER(elems);
    Ok(GinQueryArrayExtractResult {
        elems,
        nkeys: nelems,
        null_flags: nulls,
        search_mode,
    })
}

/// `ginarrayconsistent` (ginarrayproc.c:141): the boolean `consistent` support
/// function.
///
/// `check` and `null_flags` are C's `check`/`nullFlags` of length `nkeys`;
/// `recheck` is the `*recheck` out-parameter. The unused query / extra_data /
/// queryKeys arguments are not represented (C comments them out).
pub fn ginarrayconsistent(
    check: &[bool],
    strategy: StrategyNumber,
    nkeys: i32,
    null_flags: &[bool],
    recheck: &mut bool,
) -> PgResult<bool> {
    let res: bool;

    match strategy {
        GinOverlapStrategy => {
            // result is not lossy
            *recheck = false;
            // must have a match for at least one non-null element
            let mut r = false;
            let mut i: i32 = 0;
            while i < nkeys {
                if check[i as usize] && !null_flags[i as usize] {
                    r = true;
                    break;
                }
                i += 1;
            }
            res = r;
        }
        GinContainsStrategy => {
            // result is not lossy
            *recheck = false;
            // must have all elements in check[] true, and no nulls
            let mut r = true;
            let mut i: i32 = 0;
            while i < nkeys {
                if !check[i as usize] || null_flags[i as usize] {
                    r = false;
                    break;
                }
                i += 1;
            }
            res = r;
        }
        GinContainedStrategy => {
            // we will need recheck
            *recheck = true;
            // can't do anything else useful here
            res = true;
        }
        GinEqualStrategy => {
            // we will need recheck
            *recheck = true;

            // Must have all elements in check[] true; no discrimination against
            // nulls here.  This is because array_contain_compare and array_eq
            // handle nulls differently ...
            let mut r = true;
            let mut i: i32 = 0;
            while i < nkeys {
                if !check[i as usize] {
                    r = false;
                    break;
                }
                i += 1;
            }
            res = r;
        }
        _ => {
            return Err(elog_error(
                format!("ginarrayconsistent: unknown strategy number: {strategy}"),
                214,
                "ginarrayconsistent",
            ));
        }
    }

    // PG_RETURN_BOOL(res);
    Ok(res)
}

/// `ginarraytriconsistent` (ginarrayproc.c:225): the ternary `triConsistent`
/// support function — the 3-valued counterpart of [`ginarrayconsistent`].
///
/// `check` holds `GIN_TRUE`/`GIN_FALSE`/`GIN_MAYBE` per key (length `nkeys`);
/// `null_flags` is `nullFlags`. There is no `recheck` out-parameter (the ternary
/// interface encodes "needs recheck" as `GIN_MAYBE`).
pub fn ginarraytriconsistent(
    check: &[GinTernaryValue],
    strategy: StrategyNumber,
    nkeys: i32,
    null_flags: &[bool],
) -> PgResult<GinTernaryValue> {
    let res: GinTernaryValue;

    match strategy {
        GinOverlapStrategy => {
            // must have a match for at least one non-null element
            let mut r: GinTernaryValue = GIN_FALSE;
            let mut i: i32 = 0;
            while i < nkeys {
                if !null_flags[i as usize] {
                    if check[i as usize] == GIN_TRUE {
                        r = GIN_TRUE;
                        break;
                    } else if check[i as usize] == GIN_MAYBE && r == GIN_FALSE {
                        r = GIN_MAYBE;
                    }
                }
                i += 1;
            }
            res = r;
        }
        GinContainsStrategy => {
            // must have all elements in check[] true, and no nulls
            let mut r: GinTernaryValue = GIN_TRUE;
            let mut i: i32 = 0;
            while i < nkeys {
                if check[i as usize] == GIN_FALSE || null_flags[i as usize] {
                    r = GIN_FALSE;
                    break;
                }
                if check[i as usize] == GIN_MAYBE {
                    r = GIN_MAYBE;
                }
                i += 1;
            }
            res = r;
        }
        GinContainedStrategy => {
            // can't do anything else useful here
            res = GIN_MAYBE;
        }
        GinEqualStrategy => {
            // Must have all elements in check[] true; no discrimination against
            // nulls here.  This is because array_contain_compare and array_eq
            // handle nulls differently ...
            let mut r: GinTernaryValue = GIN_MAYBE;
            let mut i: i32 = 0;
            while i < nkeys {
                if check[i as usize] == GIN_FALSE {
                    r = GIN_FALSE;
                    break;
                }
                i += 1;
            }
            res = r;
        }
        _ => {
            return Err(elog_error(
                format!("ginarrayconsistent: unknown strategy number: {strategy}"),
                299,
                "ginarraytriconsistent",
            ));
        }
    }

    // PG_RETURN_GIN_TERNARY_VALUE(res);
    Ok(res)
}

/// Build the `PgError` for an `elog(ERROR, ...)` in `ginarrayproc.c` (every such
/// call is an internal error — `ERRCODE_INTERNAL_ERROR`, no `_()` wrapping).
fn elog_error(message: String, line: i32, func: &'static str) -> PgError {
    PgError::error(message)
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
        .with_error_location(ErrorLocation::new(
            "../src/backend/access/gin/ginarrayproc.c",
            line,
            func,
        ))
}
