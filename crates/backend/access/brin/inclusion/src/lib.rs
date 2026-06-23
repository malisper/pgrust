//! Idiomatic port of `src/backend/access/brin/brin_inclusion.c` (PostgreSQL 18.3).
//!
//! The inclusion BRIN operator class: framework support for the types that
//! support R-Tree operations (geometric, network, range). Each page range is
//! summarized by the union of its values, plus two flags — whether it contains
//! unmergeable elements (e.g. an IPv6 address amidst IPv4) and whether it
//! contains an empty element. Four support procedures plus two procinfo caches:
//!
//!   * [`brin_inclusion_opcinfo`]               (brin_inclusion.c:88)
//!   * [`brin_inclusion_add_value`]             (brin_inclusion.c:130)
//!   * [`brin_inclusion_consistent`]            (brin_inclusion.c:241)
//!   * [`brin_inclusion_union`]                 (brin_inclusion.c:471)
//!   * [`inclusion_get_procinfo`]               (brin_inclusion.c:551)
//!   * [`inclusion_get_strategy_procinfo`]      (brin_inclusion.c:617)
//!
//! These are reached by the BRIN AM (`brin.c`, unported) through the
//! `backend-access-brin-entry-seams` opclass-dispatch seams; the single
//! installer of those seams lives in `backend-access-brin-minmax`, which
//! dispatches the built-in `brin_inclusion_*` support-procedure OIDs into the
//! public bodies here. This is the BRIN F0-opclass S2-inclusion stage; it reuses
//! the [`brin::OpaqueOpcInfo`] carrier (its [`InclusionOpaque`] variant).
//!
//! ## Carrier and fmgr-dispatch
//!
//! C's `InclusionOpaque` holds `FmgrInfo extra_procinfos[]` /
//! `bool extra_proc_missing[]` / `Oid cached_subtype` /
//! `FmgrInfo strategy_procinfos[]` in the `palloc0`'d tail of the `BrinOpcInfo`
//! (`oi_opaque`). The repo models `oi_opaque` as the typed enum
//! [`brin::OpaqueOpcInfo`]; inclusion's variant is
//! [`brin::InclusionOpaque`], whose every cached `FmgrInfo` is reduced to
//! the resolved function's `Oid` (the repo's fmgr-call seams re-resolve by OID).
//! The AM dispatches the support procs through a `&BrinDesc` (immutable), so the
//! cache slots are `Cell`s and fill lazily through the shared reference, matching
//! C's mutation through `bdesc->bd_info[]->oi_opaque`.
//!
//! Unlike minmax, the inclusion R-Tree operators take and return by-reference
//! values (box / range / inet / …). They are invoked over the canonical
//! per-attribute [`Datum`] lane via the
//! [`fmgr_seams::function_call1_coll_datum`] /
//! `function_call2_coll_datum` seams (by-reference args/result cross through the
//! fmgr by-reference side channel), so the merged union is a fresh `mcx`-owned
//! value — the C `pfree`/`datumCopy`-on-alias memory hygiene is handled by the
//! owned model and needs no explicit copy.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::format;

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use brin::{
    BrinDesc, BrinOpcInfo, BrinValues, InclusionOpaque, OpaqueOpcInfo, INCLUSION_MAX_PROCNUMS,
    RT_MAX_STRATEGY_NUMBER,
};
use types_core::catalog::BOOLOID;
use types_core::primitive::{AttrNumber, Oid, RegProcedure};
use types_error::error::{
    ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_OBJECT_DEFINITION, ERROR,
};
use types_error::PgResult;
use types_scan::scankey::ScanKeyData;
use types_tuple::heaptuple::Datum;

use utils_error::ereport;

use indexam_seams as indexam;
use scalar_seams as scalar;
use lsyscache_seams as lsyscache;
use typcache_seams as typcache;
use fmgr_seams as fmgr;

// ---------------------------------------------------------------------------
// brin_inclusion.c additional SQL-level support function numbers.
// ---------------------------------------------------------------------------

/// `PROCNUM_MERGE` (brin_inclusion.c:47): required.
const PROCNUM_MERGE: u16 = 11;
/// `PROCNUM_MERGEABLE` (brin_inclusion.c:48): optional.
const PROCNUM_MERGEABLE: u16 = 12;
/// `PROCNUM_CONTAINS` (brin_inclusion.c:49): optional.
const PROCNUM_CONTAINS: u16 = 13;
/// `PROCNUM_EMPTY` (brin_inclusion.c:50): optional.
const PROCNUM_EMPTY: u16 = 14;
/// `PROCNUM_BASE` (brin_inclusion.c:57): subtract from procnum to index the
/// `InclusionOpaque` arrays (== minimum of the private procnums).
const PROCNUM_BASE: u16 = 11;

// `bv_values[]` slot meanings (brin_inclusion.c:69-71).
/// `INCLUSION_UNION`: the union of the values in the block range.
const INCLUSION_UNION: usize = 0;
/// `INCLUSION_UNMERGEABLE`: whether the values cannot be merged.
const INCLUSION_UNMERGEABLE: usize = 1;
/// `INCLUSION_CONTAINS_EMPTY`: whether an empty value is present.
const INCLUSION_CONTAINS_EMPTY: usize = 2;

// ---------------------------------------------------------------------------
// stratnum.h — R-tree strategy numbers used by the inclusion opclass.
// ---------------------------------------------------------------------------

const RT_LEFT_STRATEGY_NUMBER: u16 = 1; // <<
const RT_OVER_LEFT_STRATEGY_NUMBER: u16 = 2; // &<
const RT_OVERLAP_STRATEGY_NUMBER: u16 = 3; // &&
const RT_OVER_RIGHT_STRATEGY_NUMBER: u16 = 4; // &>
const RT_RIGHT_STRATEGY_NUMBER: u16 = 5; // >>
const RT_SAME_STRATEGY_NUMBER: u16 = 6; // ~=
const RT_CONTAINS_STRATEGY_NUMBER: u16 = 7; // @>
const RT_CONTAINED_BY_STRATEGY_NUMBER: u16 = 8; // <@
const RT_OVER_BELOW_STRATEGY_NUMBER: u16 = 9; // &<|
const RT_BELOW_STRATEGY_NUMBER: u16 = 10; // <<|
const RT_ABOVE_STRATEGY_NUMBER: u16 = 11; // |>>
const RT_OVER_ABOVE_STRATEGY_NUMBER: u16 = 12; // |&>
const RT_CONTAINS_ELEM_STRATEGY_NUMBER: u16 = 16; // range @> elem
const RT_ADJACENT_STRATEGY_NUMBER: u16 = 17; // -|-
const RT_EQUAL_STRATEGY_NUMBER: u16 = 18; // =
const RT_LESS_STRATEGY_NUMBER: u16 = 20; // <
const RT_LESS_EQUAL_STRATEGY_NUMBER: u16 = 21; // <=
const RT_GREATER_STRATEGY_NUMBER: u16 = 22; // >
const RT_GREATER_EQUAL_STRATEGY_NUMBER: u16 = 23; // >=
const RT_SUB_STRATEGY_NUMBER: u16 = 24; // inet >>
const RT_SUB_EQUAL_STRATEGY_NUMBER: u16 = 25; // inet <<=
const RT_SUPER_STRATEGY_NUMBER: u16 = 26; // inet <<
const RT_SUPER_EQUAL_STRATEGY_NUMBER: u16 = 27; // inet >>=
/// `RTMaxStrategyNumber` (stratnum.h:82).
const RT_MAX_STRATEGY_NUMBER_U16: u16 = 30;

/// `InvalidOid` (postgres_ext.h).
const INVALID_OID: Oid = 0;

// ---------------------------------------------------------------------------
// Built-in `brin_inclusion_*` support-procedure OIDs (pg_proc.dat).
// ---------------------------------------------------------------------------

/// `brin_inclusion_opcinfo` (pg_proc.dat oid 4105).
pub const F_BRIN_INCLUSION_OPCINFO: Oid = 4105;
/// `brin_inclusion_add_value` (pg_proc.dat oid 4106).
pub const F_BRIN_INCLUSION_ADD_VALUE: Oid = 4106;
/// `brin_inclusion_consistent` (pg_proc.dat oid 4107).
pub const F_BRIN_INCLUSION_CONSISTENT: Oid = 4107;
/// `brin_inclusion_union` (pg_proc.dat oid 4108).
pub const F_BRIN_INCLUSION_UNION: Oid = 4108;

// ---------------------------------------------------------------------------
// fmgr helpers over the canonical Datum lane.
// ---------------------------------------------------------------------------

/// `FunctionCall1Coll(finfo, colloid, arg1)` returning the boolean result
/// (`DatumGetBool`). The support procedure is resolved by `function_id`.
fn call1_bool(
    mcx: Mcx<'_>,
    function_id: Oid,
    colloid: Oid,
    arg1: &Datum,
) -> PgResult<bool> {
    let r = fmgr::function_call1_coll_datum::call(mcx, function_id, colloid, arg1.clone_in(mcx)?)?;
    Ok(r.as_bool())
}

/// `FunctionCall2Coll(finfo, colloid, arg1, arg2)` returning the boolean result.
fn call2_bool(
    mcx: Mcx<'_>,
    function_id: Oid,
    colloid: Oid,
    arg1: &Datum,
    arg2: &Datum,
) -> PgResult<bool> {
    let r = fmgr::function_call2_coll_datum::call(
        mcx,
        function_id,
        colloid,
        arg1.clone_in(mcx)?,
        arg2.clone_in(mcx)?,
    )?;
    Ok(r.as_bool())
}

/// `FunctionCall2Coll(merge_fn, colloid, arg1, arg2)` returning the merged union
/// value (a fresh `mcx`-owned `Datum`). Used for `PROCNUM_MERGE`.
fn call2_datum<'mcx>(
    mcx: Mcx<'mcx>,
    function_id: Oid,
    colloid: Oid,
    arg1: &Datum,
    arg2: &Datum,
) -> PgResult<Datum<'mcx>> {
    fmgr::function_call2_coll_datum::call(
        mcx,
        function_id,
        colloid,
        arg1.clone_in(mcx)?,
        arg2.clone_in(mcx)?,
    )
}

/// Borrow the [`InclusionOpaque`] cache out of
/// `bdesc.bd_info[attno - 1].oi_opaque`. Created by [`brin_inclusion_opcinfo`];
/// any other shape is a caller/dispatch bug.
fn inclusion_opaque<'a, 'mcx>(
    bdesc: &'a BrinDesc<'mcx>,
    attno: AttrNumber,
) -> &'a InclusionOpaque {
    match bdesc.bd_info[(attno - 1) as usize].oi_opaque.as_ref() {
        Some(OpaqueOpcInfo::Inclusion(o)) => o,
        _ => panic!("brin_inclusion: oi_opaque is not an InclusionOpaque cache"),
    }
}

// ===========================================================================
// brin_inclusion_opcinfo (brin_inclusion.c:88)
// ===========================================================================

/// `brin_inclusion_opcinfo(typoid)` (brin_inclusion.c:88): build the
/// [`BrinOpcInfo`] for the inclusion opclass over `typoid` — three stored
/// columns (the union; the unmergeable flag; the contains-empty flag), regular
/// NULL handling, and a fresh (`palloc0`-zeroed) [`InclusionOpaque`]. The union
/// column's type-cache slot is `lookup_type_cache(typoid, 0)`; the two flag
/// columns use `lookup_type_cache(BOOLOID, 0)`.
pub fn brin_inclusion_opcinfo<'mcx>(
    mcx: Mcx<'mcx>,
    typoid: Oid,
) -> PgResult<PgBox<'mcx, BrinOpcInfo<'mcx>>> {
    // TypeCacheEntry *bool_typcache = lookup_type_cache(BOOLOID, 0);
    let bool_typcache = typcache::lookup_type_cache::call(BOOLOID, 0)?;
    let union_typcache = typcache::lookup_type_cache::call(typoid, 0)?;

    // result = palloc0(MAXALIGN(SizeofBrinOpcInfo(3)) + sizeof(InclusionOpaque));
    // result->oi_nstored = 3;
    // result->oi_regular_nulls = true;
    // result->oi_opaque = (InclusionOpaque *) MAXALIGN(...);  -- palloc0-zeroed.
    // result->oi_typcache[INCLUSION_UNION] = lookup_type_cache(typoid, 0);
    // result->oi_typcache[INCLUSION_UNMERGEABLE] = bool_typcache;
    // result->oi_typcache[INCLUSION_CONTAINS_EMPTY] = bool_typcache;
    let mut oi_typcache: PgVec<'mcx, _> = vec_with_capacity_in(mcx, 3)?;
    oi_typcache.push(union_typcache);
    oi_typcache.push(bool_typcache.clone());
    oi_typcache.push(bool_typcache);

    alloc_in(
        mcx,
        BrinOpcInfo {
            oi_nstored: 3,
            oi_regular_nulls: true,
            oi_opaque: Some(OpaqueOpcInfo::Inclusion(InclusionOpaque::default())),
            oi_typcache,
        },
    )
}

// ===========================================================================
// brin_inclusion_add_value (brin_inclusion.c:130)
// ===========================================================================

/// `brin_inclusion_add_value` (brin_inclusion.c:130): examine the index tuple
/// summary for indexed column `column` by comparing it to `newval` from a heap
/// tuple. If the new value is outside the union, update the summary and return
/// true; otherwise return false and do not modify.
///
/// `_isnull` is the C `PG_GETARG_BOOL(3)` (`PG_USED_FOR_ASSERTS_ONLY`).
pub fn brin_inclusion_add_value<'mcx>(
    mcx: Mcx<'mcx>,
    bdesc: &BrinDesc<'mcx>,
    column: &mut BrinValues<'mcx>,
    newval: &Datum<'mcx>,
    _isnull: bool,
    colloid: Oid,
) -> PgResult<bool> {
    // Assert(!isnull);
    debug_assert!(!_isnull);

    // attno = column->bv_attno;
    // attr = TupleDescCompactAttr(bdesc->bd_tupdesc, attno - 1);
    let attno = column.bv_attno;
    let attr = bdesc.bd_tupdesc.attr((attno - 1) as usize);
    let attbyval = attr.attbyval;
    let attlen = attr.attlen;

    let mut new = false;

    // If the recorded value is null, copy the new value (not null), and we're
    // almost done.
    if column.bv_allnulls {
        column.bv_values[INCLUSION_UNION] =
            scalar_datum_copy(mcx, newval, attbyval, attlen)?;
        column.bv_values[INCLUSION_UNMERGEABLE] = Datum::from_bool(false);
        column.bv_values[INCLUSION_CONTAINS_EMPTY] = Datum::from_bool(false);
        column.bv_allnulls = false;
        new = true;
    }

    // No need for further processing if the block range is marked as containing
    // unmergeable values.
    if column.bv_values[INCLUSION_UNMERGEABLE].as_bool() {
        return Ok(false);
    }

    // If the opclass supports empty values, test the new value for emptiness; if
    // so, set the "contains empty" flag (unless already set).
    let finfo = inclusion_get_procinfo(mcx, bdesc, attno, PROCNUM_EMPTY, true)?;
    if let Some(finfo) = finfo {
        if call1_bool(mcx, finfo, colloid, newval)? {
            if !column.bv_values[INCLUSION_CONTAINS_EMPTY].as_bool() {
                column.bv_values[INCLUSION_CONTAINS_EMPTY] = Datum::from_bool(true);
                return Ok(true);
            }
            return Ok(false);
        }
    }

    if new {
        return Ok(true);
    }

    // Check if the new value is already contained.
    let finfo = inclusion_get_procinfo(mcx, bdesc, attno, PROCNUM_CONTAINS, true)?;
    if let Some(finfo) = finfo {
        if call2_bool(mcx, finfo, colloid, &column.bv_values[INCLUSION_UNION], newval)? {
            return Ok(false);
        }
    }

    // Check if the new value is mergeable to the existing union. If not, mark the
    // value as containing unmergeable elements and get out.
    let finfo = inclusion_get_procinfo(mcx, bdesc, attno, PROCNUM_MERGEABLE, true)?;
    if let Some(finfo) = finfo {
        if !call2_bool(mcx, finfo, colloid, &column.bv_values[INCLUSION_UNION], newval)? {
            column.bv_values[INCLUSION_UNMERGEABLE] = Datum::from_bool(true);
            return Ok(true);
        }
    }

    // Finally, merge the new value to the existing union. The merge support proc
    // returns a fresh `mcx`-owned union value (the C `pfree`/`datumCopy`-on-alias
    // hygiene is intrinsic to the owned model).
    let finfo = inclusion_get_procinfo(mcx, bdesc, attno, PROCNUM_MERGE, false)?
        .expect("PROCNUM_MERGE is required (missing_ok = false)");
    let result = call2_datum(mcx, finfo, colloid, &column.bv_values[INCLUSION_UNION], newval)?;
    column.bv_values[INCLUSION_UNION] = result;

    Ok(true)
}

// ===========================================================================
// brin_inclusion_consistent (brin_inclusion.c:241)
// ===========================================================================

/// `brin_inclusion_consistent` (brin_inclusion.c:241): given an index tuple
/// summary for a page range and a scan key, return whether the scan key is
/// consistent with the range's union (and the empty/unmergeable flags). All
/// strategies are optional. This opclass uses the old 3-argument signature.
pub fn brin_inclusion_consistent<'mcx>(
    mcx: Mcx<'mcx>,
    bdesc: &BrinDesc<'mcx>,
    column: &BrinValues<'mcx>,
    key: &ScanKeyData<'mcx>,
    colloid: Oid,
) -> PgResult<bool> {
    // Assert(PG_NARGS() == 3);  -- old 3-arg signature.
    // Assert(!column->bv_allnulls);  -- should not see an all-NULL range.
    debug_assert!(!column.bv_allnulls);

    // It has to be checked if it contains elements that are not mergeable.
    if column.bv_values[INCLUSION_UNMERGEABLE].as_bool() {
        return Ok(true);
    }

    let attno = key.sk_attno;
    let subtype = key.sk_subtype;
    let query = &key.sk_argument;
    let unionval = &column.bv_values[INCLUSION_UNION];

    let matches: bool = match key.sk_strategy {
        // Placement strategies: implemented by negating the converse operator.
        // These all return false if either argument is empty, so no need to
        // check for empty elements.
        RT_LEFT_STRATEGY_NUMBER => {
            let finfo = inclusion_get_strategy_procinfo(
                mcx, bdesc, attno, subtype, RT_OVER_RIGHT_STRATEGY_NUMBER,
            )?;
            !call2_bool(mcx, finfo, colloid, unionval, query)?
        }
        RT_OVER_LEFT_STRATEGY_NUMBER => {
            let finfo = inclusion_get_strategy_procinfo(
                mcx, bdesc, attno, subtype, RT_RIGHT_STRATEGY_NUMBER,
            )?;
            !call2_bool(mcx, finfo, colloid, unionval, query)?
        }
        RT_OVER_RIGHT_STRATEGY_NUMBER => {
            let finfo = inclusion_get_strategy_procinfo(
                mcx, bdesc, attno, subtype, RT_LEFT_STRATEGY_NUMBER,
            )?;
            !call2_bool(mcx, finfo, colloid, unionval, query)?
        }
        RT_RIGHT_STRATEGY_NUMBER => {
            let finfo = inclusion_get_strategy_procinfo(
                mcx, bdesc, attno, subtype, RT_OVER_LEFT_STRATEGY_NUMBER,
            )?;
            !call2_bool(mcx, finfo, colloid, unionval, query)?
        }
        RT_BELOW_STRATEGY_NUMBER => {
            let finfo = inclusion_get_strategy_procinfo(
                mcx, bdesc, attno, subtype, RT_OVER_ABOVE_STRATEGY_NUMBER,
            )?;
            !call2_bool(mcx, finfo, colloid, unionval, query)?
        }
        RT_OVER_BELOW_STRATEGY_NUMBER => {
            let finfo = inclusion_get_strategy_procinfo(
                mcx, bdesc, attno, subtype, RT_ABOVE_STRATEGY_NUMBER,
            )?;
            !call2_bool(mcx, finfo, colloid, unionval, query)?
        }
        RT_OVER_ABOVE_STRATEGY_NUMBER => {
            let finfo = inclusion_get_strategy_procinfo(
                mcx, bdesc, attno, subtype, RT_BELOW_STRATEGY_NUMBER,
            )?;
            !call2_bool(mcx, finfo, colloid, unionval, query)?
        }
        RT_ABOVE_STRATEGY_NUMBER => {
            let finfo = inclusion_get_strategy_procinfo(
                mcx, bdesc, attno, subtype, RT_OVER_BELOW_STRATEGY_NUMBER,
            )?;
            !call2_bool(mcx, finfo, colloid, unionval, query)?
        }

        // Overlap and contains strategies: simply call the operator and return
        // its result. Empty elements don't change the result.
        RT_OVERLAP_STRATEGY_NUMBER
        | RT_CONTAINS_STRATEGY_NUMBER
        | RT_CONTAINS_ELEM_STRATEGY_NUMBER
        | RT_SUB_STRATEGY_NUMBER
        | RT_SUB_EQUAL_STRATEGY_NUMBER => {
            let finfo = inclusion_get_strategy_procinfo(
                mcx, bdesc, attno, subtype, key.sk_strategy,
            )?;
            call2_bool(mcx, finfo, colloid, unionval, query)?
        }

        // Contained by strategies: cannot call the original operator (some
        // elements can be contained even though the union is not), so use the
        // overlap operator. Empty elements are checked separately as they are
        // not merged to the union but contained by everything.
        RT_CONTAINED_BY_STRATEGY_NUMBER
        | RT_SUPER_STRATEGY_NUMBER
        | RT_SUPER_EQUAL_STRATEGY_NUMBER => {
            let finfo = inclusion_get_strategy_procinfo(
                mcx, bdesc, attno, subtype, RT_OVERLAP_STRATEGY_NUMBER,
            )?;
            if call2_bool(mcx, finfo, colloid, unionval, query)? {
                true
            } else {
                column.bv_values[INCLUSION_CONTAINS_EMPTY].as_bool()
            }
        }

        // Adjacent strategy: test for overlap first, then call the actual
        // adjacent operator. An empty element cannot be adjacent to any other.
        RT_ADJACENT_STRATEGY_NUMBER => {
            let finfo = inclusion_get_strategy_procinfo(
                mcx, bdesc, attno, subtype, RT_OVERLAP_STRATEGY_NUMBER,
            )?;
            if call2_bool(mcx, finfo, colloid, unionval, query)? {
                true
            } else {
                let finfo = inclusion_get_strategy_procinfo(
                    mcx, bdesc, attno, subtype, RT_ADJACENT_STRATEGY_NUMBER,
                )?;
                call2_bool(mcx, finfo, colloid, unionval, query)?
            }
        }

        // Basic comparison strategies. Empty elements are considered less than
        // others; we cannot use the empty support function to check the query (it
        // can be a different data type), so return true if there is a possibility
        // empty elements change the result.
        RT_LESS_STRATEGY_NUMBER | RT_LESS_EQUAL_STRATEGY_NUMBER => {
            let finfo = inclusion_get_strategy_procinfo(
                mcx, bdesc, attno, subtype, RT_RIGHT_STRATEGY_NUMBER,
            )?;
            if !call2_bool(mcx, finfo, colloid, unionval, query)? {
                true
            } else {
                column.bv_values[INCLUSION_CONTAINS_EMPTY].as_bool()
            }
        }
        RT_SAME_STRATEGY_NUMBER | RT_EQUAL_STRATEGY_NUMBER => {
            let finfo = inclusion_get_strategy_procinfo(
                mcx, bdesc, attno, subtype, RT_CONTAINS_STRATEGY_NUMBER,
            )?;
            if call2_bool(mcx, finfo, colloid, unionval, query)? {
                true
            } else {
                column.bv_values[INCLUSION_CONTAINS_EMPTY].as_bool()
            }
        }
        RT_GREATER_EQUAL_STRATEGY_NUMBER => {
            let finfo = inclusion_get_strategy_procinfo(
                mcx, bdesc, attno, subtype, RT_LEFT_STRATEGY_NUMBER,
            )?;
            if !call2_bool(mcx, finfo, colloid, unionval, query)? {
                true
            } else {
                column.bv_values[INCLUSION_CONTAINS_EMPTY].as_bool()
            }
        }
        RT_GREATER_STRATEGY_NUMBER => {
            // no need to check for empty elements
            let finfo = inclusion_get_strategy_procinfo(
                mcx, bdesc, attno, subtype, RT_LEFT_STRATEGY_NUMBER,
            )?;
            !call2_bool(mcx, finfo, colloid, unionval, query)?
        }

        // shouldn't happen
        other => {
            // elog(ERROR, "invalid strategy number %d", key->sk_strategy);
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg_internal(format!("invalid strategy number {}", other as i32))
                .into_error());
        }
    };

    Ok(matches)
}

// ===========================================================================
// brin_inclusion_union (brin_inclusion.c:471)
// ===========================================================================

/// `brin_inclusion_union` (brin_inclusion.c:471): update `col_a` so that it
/// becomes a union of the summary values contained in both `col_a` and `col_b`;
/// `col_b` is untouched.
pub fn brin_inclusion_union<'mcx>(
    mcx: Mcx<'mcx>,
    bdesc: &BrinDesc<'mcx>,
    col_a: &mut BrinValues<'mcx>,
    col_b: &BrinValues<'mcx>,
    colloid: Oid,
) -> PgResult<()> {
    // Assert(col_a->bv_attno == col_b->bv_attno);
    debug_assert_eq!(col_a.bv_attno, col_b.bv_attno);
    // Assert(!col_a->bv_allnulls && !col_b->bv_allnulls);
    debug_assert!(!col_a.bv_allnulls && !col_b.bv_allnulls);

    // attno = col_a->bv_attno;
    let attno = col_a.bv_attno;

    // If B includes empty elements, mark A similarly, if needed.
    if !col_a.bv_values[INCLUSION_CONTAINS_EMPTY].as_bool()
        && col_b.bv_values[INCLUSION_CONTAINS_EMPTY].as_bool()
    {
        col_a.bv_values[INCLUSION_CONTAINS_EMPTY] = Datum::from_bool(true);
    }

    // Check if A includes elements that are not mergeable.
    if col_a.bv_values[INCLUSION_UNMERGEABLE].as_bool() {
        return Ok(());
    }

    // If B includes elements that are not mergeable, mark A similarly.
    if col_b.bv_values[INCLUSION_UNMERGEABLE].as_bool() {
        col_a.bv_values[INCLUSION_UNMERGEABLE] = Datum::from_bool(true);
        return Ok(());
    }

    // Check if A and B are mergeable; if not, mark A unmergeable.
    let finfo = inclusion_get_procinfo(mcx, bdesc, attno, PROCNUM_MERGEABLE, true)?;
    if let Some(finfo) = finfo {
        if !call2_bool(
            mcx,
            finfo,
            colloid,
            &col_a.bv_values[INCLUSION_UNION],
            &col_b.bv_values[INCLUSION_UNION],
        )? {
            col_a.bv_values[INCLUSION_UNMERGEABLE] = Datum::from_bool(true);
            return Ok(());
        }
    }

    // Finally, merge B to A.
    let finfo = inclusion_get_procinfo(mcx, bdesc, attno, PROCNUM_MERGE, false)?
        .expect("PROCNUM_MERGE is required (missing_ok = false)");
    let result = call2_datum(
        mcx,
        finfo,
        colloid,
        &col_a.bv_values[INCLUSION_UNION],
        &col_b.bv_values[INCLUSION_UNION],
    )?;
    col_a.bv_values[INCLUSION_UNION] = result;

    Ok(())
}

// ===========================================================================
// inclusion_get_procinfo (brin_inclusion.c:551)
// ===========================================================================

/// `inclusion_get_procinfo(bdesc, attno, procnum, missing_ok)`
/// (brin_inclusion.c:551): cache and return the support-procedure OID for the
/// given support-function number, or `None` if it does not exist. If
/// `missing_ok` is false and the procedure is absent, raise an error.
///
/// The cached `FmgrInfo` is reduced to the resolved function's `Oid`. The
/// `RegProcedureIsValid(index_getprocid(...))` test + `fmgr_info_copy` /
/// `index_getprocinfo` resolution collapses to the indexam `index_getprocid`
/// (no-error missing test) + `index_getprocinfo` (the OID we cache).
fn inclusion_get_procinfo(
    _mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    attno: AttrNumber,
    procnum: u16,
    missing_ok: bool,
) -> PgResult<Option<Oid>> {
    let opaque = inclusion_opaque(bdesc, attno);
    let basenum = (procnum - PROCNUM_BASE) as usize;
    debug_assert!(basenum < INCLUSION_MAX_PROCNUMS);

    // If we already searched for this proc and didn't find it, don't bother
    // searching again.
    if opaque.extra_proc_missing[basenum].get() {
        return Ok(None);
    }

    if opaque.extra_procinfos[basenum].get() == INVALID_OID {
        // if (RegProcedureIsValid(index_getprocid(bdesc->bd_index, attno, procnum)))
        //     fmgr_info_copy(&opaque->extra_procinfos[basenum],
        //                    index_getprocinfo(bdesc->bd_index, attno, procnum), ...);
        let procid: RegProcedure = indexam::index_getprocid::call(&bdesc.bd_index, attno, procnum)?;
        if procid != INVALID_OID {
            // index_getprocinfo resolves + caches the FmgrInfo; we keep its OID.
            let finfo = indexam::index_getprocinfo::call(&bdesc.bd_index, attno, procnum)?;
            opaque.extra_procinfos[basenum].set(finfo.fn_oid);
        } else {
            if !missing_ok {
                // ereport(ERROR, errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                //   errmsg_internal("invalid opclass definition"),
                //   errdetail_internal("The operator class is missing support
                //                       function %d for column %d.", procnum, attno));
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg_internal("invalid opclass definition".to_string())
                    .errdetail_internal(format!(
                        "The operator class is missing support function {} for column {}.",
                        procnum, attno
                    ))
                    .into_error());
            }

            opaque.extra_proc_missing[basenum].set(true);
            return Ok(None);
        }
    }

    Ok(Some(opaque.extra_procinfos[basenum].get()))
}

// ===========================================================================
// inclusion_get_strategy_procinfo (brin_inclusion.c:617)
// ===========================================================================

/// `inclusion_get_strategy_procinfo(bdesc, attno, subtype, strategynum)`
/// (brin_inclusion.c:617): cache and return the comparison-procedure OID for the
/// given sub-type and strategy, invalidating the whole cache when `subtype`
/// changes. Throws an error if the `pg_amop` row does not exist.
///
/// Mirrors `minmax_get_strategy_procinfo`. The `SearchSysCache4(AMOPSTRATEGY)` +
/// `SysCacheGetAttrNotNull(amopopr)` + `get_opcode` resolution is the lsyscache
/// `get_opfamily_member` + `get_opcode` pair.
fn inclusion_get_strategy_procinfo(
    _mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    attno: AttrNumber,
    subtype: Oid,
    strategynum: u16,
) -> PgResult<Oid> {
    // Assert(strategynum >= 1 && strategynum <= RTMaxStrategyNumber);
    debug_assert!(strategynum >= 1 && strategynum <= RT_MAX_STRATEGY_NUMBER_U16);

    let opaque = inclusion_opaque(bdesc, attno);

    // We cache the procedures for the last sub-type in the opaque struct. If the
    // sub-type changed, invalidate all the cached entries.
    if opaque.cached_subtype.get() != subtype {
        for i in 1..=RT_MAX_STRATEGY_NUMBER {
            opaque.strategy_procinfos[i - 1].set(INVALID_OID);
        }
        opaque.cached_subtype.set(subtype);
    }

    if opaque.strategy_procinfos[(strategynum - 1) as usize].get() == INVALID_OID {
        // opfamily = bdesc->bd_index->rd_opfamily[attno - 1];
        // attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);
        let opfamily = bdesc.bd_index.rd_opfamily[(attno - 1) as usize];
        let atttypid = bdesc.bd_tupdesc.attr((attno - 1) as usize).atttypid;

        // tuple = SearchSysCache4(AMOPSTRATEGY, opfamily, attr->atttypid,
        //                         subtype, strategynum);
        // oprid = DatumGetObjectId(SysCacheGetAttrNotNull(..., amopopr));
        let oprid =
            lsyscache::get_opfamily_member::call(opfamily, atttypid, subtype, strategynum as i16)?;

        if oprid == INVALID_OID {
            // if (!HeapTupleIsValid(tuple))
            //     elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
            //          strategynum, attr->atttypid, subtype, opfamily);
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg_internal(format!(
                    "missing operator {}({},{}) in opfamily {}",
                    strategynum, atttypid, subtype, opfamily
                ))
                .into_error());
        }

        // Assert(RegProcedureIsValid(oprid));
        // fmgr_info_cxt(get_opcode(oprid), &opaque->strategy_procinfos[...], ...);
        let opcode = lsyscache::get_opcode::call(oprid)?;
        debug_assert!(opcode != INVALID_OID);
        opaque.strategy_procinfos[(strategynum - 1) as usize].set(opcode);
    }

    Ok(opaque.strategy_procinfos[(strategynum - 1) as usize].get())
}

// ---------------------------------------------------------------------------
// datumCopy helper (utils/adt/datum.c, via the scalar seam).
// ---------------------------------------------------------------------------

/// `datumCopy(value, typByVal, typLen)` over the canonical `Datum` lane.
fn scalar_datum_copy<'mcx>(
    mcx: Mcx<'mcx>,
    value: &Datum<'mcx>,
    typbyval: bool,
    typlen: i16,
) -> PgResult<Datum<'mcx>> {
    scalar::datum_copy::call(mcx, value, typbyval, typlen)
}

#[cfg(test)]
mod tests;
