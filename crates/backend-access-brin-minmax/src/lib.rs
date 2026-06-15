//! Idiomatic port of `src/backend/access/brin/brin_minmax.c` (PostgreSQL 18.3).
//!
//! The Min/Max BRIN operator class: it summarizes each page range by the
//! minimum and maximum value seen. Four support procedures plus the
//! per-attribute strategy-procinfo cache:
//!
//!   * [`brin_minmax_opcinfo`]          (brin_minmax.c:33)
//!   * [`brin_minmax_add_value`]        (brin_minmax.c:63)
//!   * [`brin_minmax_consistent`]       (brin_minmax.c:136)
//!   * [`brin_minmax_union`]            (brin_minmax.c:207)
//!   * [`minmax_get_strategy_procinfo`] (brin_minmax.c:260)
//!
//! These are reached by the BRIN AM (`brin.c`, unported) through the
//! `backend-access-brin-entry-seams` opclass-dispatch seams, which this crate
//! installs (resolving the column's support-procedure OID via
//! `index_getprocinfo` and dispatching the built-in `brin_minmax_*` OIDs here;
//! the inclusion opclass dispatches into `backend-access-brin-inclusion`; bloom
//! / minmax-multi panic until their stage lands). This is the
//! BRIN F0-opclass S1-minmax stage; S2-S4 (inclusion/bloom/minmax-multi) reuse
//! the same [`types_brin::OpaqueOpcInfo`] carrier and the same dispatch
//! installer pattern.
//!
//! ## Carrier decision (S1)
//!
//! C's `MinmaxOpaque { Oid cached_subtype; FmgrInfo strategy_procinfos[]; }`
//! lives in the `palloc0`'d tail of the `BrinOpcInfo` (`oi_opaque`, a `void *`).
//! The repo models `oi_opaque` as the typed enum [`types_brin::OpaqueOpcInfo`]
//! (one variant per built-in opclass); minmax's variant is
//! [`types_brin::MinmaxOpaque`]. Each cached `FmgrInfo` is reduced to the
//! resolved comparison function's `Oid` — the repo's fmgr-call seam
//! (`function_call2_coll`) re-resolves by OID, so the `Oid` is the whole
//! callable identity. The BRIN AM dispatches the support procs through a
//! `&BrinDesc` (immutable), so the cache slots are `Cell`s: the lazy fill
//! mutates them through the shared reference, matching C's mutation of
//! `bdesc->bd_info[]->oi_opaque` through a pointer.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::format;

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_brin::{BrinDesc, BrinOpcInfo, BrinValues, MinmaxOpaque, OpaqueOpcInfo};
use types_core::primitive::{AttrNumber, Oid};
use types_error::error::{ERRCODE_INTERNAL_ERROR, ERROR};
use types_error::PgResult;
use types_rel::Relation;
use types_scan::scankey::ScanKeyData;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_error::ereport;

use backend_access_brin_entry_seams as opclass;
use backend_access_brin_inclusion as inclusion;
use backend_access_index_indexam_seams as indexam;
use backend_utils_adt_scalar_seams as scalar;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_typcache_seams as typcache;
use backend_utils_fmgr_fmgr_seams as fmgr;

// ---------------------------------------------------------------------------
// stratnum.h — B-tree strategy numbers used by the minmax opclass.
// ---------------------------------------------------------------------------

/// `BTLessStrategyNumber` (stratnum.h:29).
const BT_LESS_STRATEGY_NUMBER: u16 = 1;
/// `BTLessEqualStrategyNumber` (stratnum.h:30).
const BT_LESS_EQUAL_STRATEGY_NUMBER: u16 = 2;
/// `BTEqualStrategyNumber` (stratnum.h:31).
const BT_EQUAL_STRATEGY_NUMBER: u16 = 3;
/// `BTGreaterEqualStrategyNumber` (stratnum.h:32).
const BT_GREATER_EQUAL_STRATEGY_NUMBER: u16 = 4;
/// `BTGreaterStrategyNumber` (stratnum.h:33).
const BT_GREATER_STRATEGY_NUMBER: u16 = 5;
/// `BTMaxStrategyNumber` (stratnum.h:35).
const BT_MAX_STRATEGY_NUMBER: u16 = 5;

/// `InvalidOid` (postgres_ext.h).
const INVALID_OID: Oid = 0;

// ---------------------------------------------------------------------------
// Built-in `brin_minmax_*` support-procedure OIDs (pg_proc.dat).
// ---------------------------------------------------------------------------

/// `F_BRIN_MINMAX_OPCINFO` — `brin_minmax_opcinfo` (pg_proc.dat oid 3383).
const F_BRIN_MINMAX_OPCINFO: Oid = 3383;
/// `F_BRIN_MINMAX_ADD_VALUE` — `brin_minmax_add_value` (pg_proc.dat oid 3384).
const F_BRIN_MINMAX_ADD_VALUE: Oid = 3384;
/// `F_BRIN_MINMAX_CONSISTENT` — `brin_minmax_consistent` (pg_proc.dat oid 3385).
const F_BRIN_MINMAX_CONSISTENT: Oid = 3385;
/// `F_BRIN_MINMAX_UNION` — `brin_minmax_union` (pg_proc.dat oid 3386).
const F_BRIN_MINMAX_UNION: Oid = 3386;

// `BRIN_PROCNUM_*` (brin_internal.h): the BRIN support-procedure index numbers.
const BRIN_PROCNUM_OPCINFO: u16 = 1;
const BRIN_PROCNUM_ADDVALUE: u16 = 2;
const BRIN_PROCNUM_CONSISTENT: u16 = 3;
const BRIN_PROCNUM_UNION: u16 = 4;

// ---------------------------------------------------------------------------
// Datum <-> bool / word helpers (postgres.h / fmgr seam edge).
// ---------------------------------------------------------------------------

/// `DatumGetBool(X)` == `((bool) ((X) & 1))` — the low bit of an fmgr result.
#[inline]
fn datum_get_bool(d: types_datum::Datum) -> bool {
    (d.as_usize() & 1) != 0
}

/// Convert a canonical `Datum<'mcx>` into the bare-word `types_datum::Datum`
/// the fmgr seams dispatch on (mirrors nbtree's `to_word`).
#[inline]
fn to_word(d: &Datum) -> types_datum::Datum {
    types_datum::Datum::from_usize(d.as_usize())
}

/// `FunctionCall2Coll(cmpFn, colloid, a, b)` reduced to the repo's by-OID call
/// seam, returning the boolean comparison result. `function_id` is the cached
/// comparison-procedure OID stored in the [`MinmaxOpaque`].
#[inline]
fn call_strategy2(
    function_id: Oid,
    colloid: Oid,
    a: &Datum,
    b: &Datum,
) -> PgResult<bool> {
    let r = fmgr::function_call2_coll::call(function_id, colloid, to_word(a), to_word(b))?;
    Ok(datum_get_bool(r))
}

/// Borrow the [`MinmaxOpaque`] cache out of `bdesc.bd_info[attno - 1].oi_opaque`.
/// The opaque was created by [`brin_minmax_opcinfo`]; any other shape is a
/// caller/dispatch bug.
fn minmax_opaque<'a, 'mcx>(bdesc: &'a BrinDesc<'mcx>, attno: AttrNumber) -> &'a MinmaxOpaque {
    match bdesc.bd_info[(attno - 1) as usize].oi_opaque.as_ref() {
        Some(OpaqueOpcInfo::Minmax(o)) => o,
        _ => panic!("brin_minmax: oi_opaque is not a MinmaxOpaque cache"),
    }
}

// ===========================================================================
// brin_minmax_opcinfo (brin_minmax.c:33)
// ===========================================================================

/// `brin_minmax_opcinfo(typoid)` (brin_minmax.c:33): build the [`BrinOpcInfo`]
/// for the minmax opclass over `typoid` — two stored columns (min, max),
/// regular NULL handling, a fresh (`palloc0`-zeroed) [`MinmaxOpaque`], and both
/// type-cache slots set to `lookup_type_cache(typoid, 0)`.
pub fn brin_minmax_opcinfo<'mcx>(
    mcx: Mcx<'mcx>,
    typoid: Oid,
) -> PgResult<PgBox<'mcx, BrinOpcInfo<'mcx>>> {
    // result = palloc0(MAXALIGN(SizeofBrinOpcInfo(2)) + sizeof(MinmaxOpaque));
    // result->oi_nstored = 2;
    // result->oi_regular_nulls = true;
    // result->oi_opaque = (MinmaxOpaque *) MAXALIGN(...);  -- palloc0-zeroed.
    // result->oi_typcache[0] = result->oi_typcache[1] =
    //     lookup_type_cache(typoid, 0);
    let tce = typcache::lookup_type_cache::call(typoid, 0)?;
    let mut oi_typcache: PgVec<'mcx, _> = vec_with_capacity_in(mcx, 2)?;
    oi_typcache.push(tce.clone());
    oi_typcache.push(tce);

    alloc_in(
        mcx,
        BrinOpcInfo {
            oi_nstored: 2,
            oi_regular_nulls: true,
            oi_opaque: Some(OpaqueOpcInfo::Minmax(MinmaxOpaque::default())),
            oi_typcache,
        },
    )
}

// ===========================================================================
// brin_minmax_add_value (brin_minmax.c:63)
// ===========================================================================

/// `brin_minmax_add_value` (brin_minmax.c:63): examine the index tuple summary
/// for indexed column `column` by comparing it to `newval` from a heap tuple. If
/// the new value is outside the min/max range, update the summary and return
/// true; otherwise return false and do not modify.
///
/// `_isnull` is the C `PG_GETARG_DATUM(3)` (`PG_USED_FOR_ASSERTS_ONLY`).
pub fn brin_minmax_add_value<'mcx>(
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
    // attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);
    let attno = column.bv_attno;
    let attr = bdesc.bd_tupdesc.attr((attno - 1) as usize);
    let attbyval = attr.attbyval;
    let attlen = attr.attlen;
    let atttypid = attr.atttypid;

    let mut updated = false;

    // If the recorded value is null, store the new value (which we know to be
    // not null) as both minimum and maximum, and we're done.
    if column.bv_allnulls {
        column.bv_values[0] = scalar::datum_copy::call(mcx, newval, attbyval, attlen)?;
        column.bv_values[1] = scalar::datum_copy::call(mcx, newval, attbyval, attlen)?;
        column.bv_allnulls = false;
        return Ok(true);
    }

    let opaque = minmax_opaque(bdesc, attno);

    // First check if it's less than the existing minimum.
    let cmp_fn =
        minmax_get_strategy_procinfo(bdesc, opaque, attno, atttypid, BT_LESS_STRATEGY_NUMBER)?;
    if call_strategy2(cmp_fn, colloid, newval, &column.bv_values[0])? {
        // if (!attr->attbyval) pfree(DatumGetPointer(column->bv_values[0]));
        // (the canonical Datum frees on overwrite; no explicit pfree needed.)
        column.bv_values[0] = scalar::datum_copy::call(mcx, newval, attbyval, attlen)?;
        updated = true;
    }

    // And now compare it to the existing maximum.
    let cmp_fn =
        minmax_get_strategy_procinfo(bdesc, opaque, attno, atttypid, BT_GREATER_STRATEGY_NUMBER)?;
    if call_strategy2(cmp_fn, colloid, newval, &column.bv_values[1])? {
        column.bv_values[1] = scalar::datum_copy::call(mcx, newval, attbyval, attlen)?;
        updated = true;
    }

    Ok(updated)
}

// ===========================================================================
// brin_minmax_consistent (brin_minmax.c:136)
// ===========================================================================

/// `brin_minmax_consistent` (brin_minmax.c:136): given an index tuple summary
/// for a page range and a scan key, return whether the scan key is consistent
/// with the range's min/max values.
///
/// We're no longer dealing with NULL keys here (handled by the AM code), so
/// there should be no all-NULL ranges either. Returns the C `matches` boolean
/// (`PG_RETURN_DATUM`).
pub fn brin_minmax_consistent<'mcx>(
    bdesc: &BrinDesc<'mcx>,
    column: &BrinValues<'mcx>,
    key: &ScanKeyData<'mcx>,
    colloid: Oid,
) -> PgResult<bool> {
    // Assert(PG_NARGS() == 3);  -- old 3-arg signature, fixed by the call site.
    // Assert(!column->bv_allnulls);  -- should not see an all-NULL range.
    debug_assert!(!column.bv_allnulls);

    let attno = key.sk_attno;
    let subtype = key.sk_subtype;
    let value = &key.sk_argument;

    let opaque = minmax_opaque(bdesc, attno);

    let matches: bool = match key.sk_strategy {
        BT_LESS_STRATEGY_NUMBER | BT_LESS_EQUAL_STRATEGY_NUMBER => {
            let finfo =
                minmax_get_strategy_procinfo(bdesc, opaque, attno, subtype, key.sk_strategy)?;
            call_strategy2(finfo, colloid, &column.bv_values[0], value)?
        }
        BT_EQUAL_STRATEGY_NUMBER => {
            // In the equality case (WHERE col = someval), return the current
            // page range if the minimum value in the range <= scan key, and the
            // maximum value >= scan key.
            let finfo = minmax_get_strategy_procinfo(
                bdesc,
                opaque,
                attno,
                subtype,
                BT_LESS_EQUAL_STRATEGY_NUMBER,
            )?;
            let m = call_strategy2(finfo, colloid, &column.bv_values[0], value)?;
            if !m {
                m
            } else {
                // max() >= scankey
                let finfo = minmax_get_strategy_procinfo(
                    bdesc,
                    opaque,
                    attno,
                    subtype,
                    BT_GREATER_EQUAL_STRATEGY_NUMBER,
                )?;
                call_strategy2(finfo, colloid, &column.bv_values[1], value)?
            }
        }
        BT_GREATER_EQUAL_STRATEGY_NUMBER | BT_GREATER_STRATEGY_NUMBER => {
            let finfo =
                minmax_get_strategy_procinfo(bdesc, opaque, attno, subtype, key.sk_strategy)?;
            call_strategy2(finfo, colloid, &column.bv_values[1], value)?
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
// brin_minmax_union (brin_minmax.c:207)
// ===========================================================================

/// `brin_minmax_union` (brin_minmax.c:207): update `col_a` so that it becomes a
/// union of the summary values contained in both `col_a` and `col_b`; `col_b` is
/// untouched.
pub fn brin_minmax_union<'mcx>(
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
    // attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);
    let attno = col_a.bv_attno;
    let attr = bdesc.bd_tupdesc.attr((attno - 1) as usize);
    let attbyval = attr.attbyval;
    let attlen = attr.attlen;
    let atttypid = attr.atttypid;

    let opaque = minmax_opaque(bdesc, attno);

    // Adjust minimum, if B's min is less than A's min.
    let finfo =
        minmax_get_strategy_procinfo(bdesc, opaque, attno, atttypid, BT_LESS_STRATEGY_NUMBER)?;
    if call_strategy2(finfo, colloid, &col_b.bv_values[0], &col_a.bv_values[0])? {
        // if (!attr->attbyval) pfree(DatumGetPointer(col_a->bv_values[0]));
        col_a.bv_values[0] = scalar::datum_copy::call(mcx, &col_b.bv_values[0], attbyval, attlen)?;
    }

    // Adjust maximum, if B's max is greater than A's max.
    let finfo =
        minmax_get_strategy_procinfo(bdesc, opaque, attno, atttypid, BT_GREATER_STRATEGY_NUMBER)?;
    if call_strategy2(finfo, colloid, &col_b.bv_values[1], &col_a.bv_values[1])? {
        col_a.bv_values[1] = scalar::datum_copy::call(mcx, &col_b.bv_values[1], attbyval, attlen)?;
    }

    Ok(())
}

// ===========================================================================
// minmax_get_strategy_procinfo (brin_minmax.c:260)
// ===========================================================================

/// `minmax_get_strategy_procinfo(bdesc, attno, subtype, strategynum)`
/// (brin_minmax.c:260): cache and return the comparison-procedure OID for the
/// given strategy, invalidating the whole cache when `subtype` changes.
///
/// The cached `FmgrInfo` is reduced to the resolved comparison function's `Oid`
/// (the repo's `function_call2_coll` re-resolves by OID). The
/// `SearchSysCache4(AMOPSTRATEGY)` + `SysCacheGetAttrNotNull(amopopr)` +
/// `get_opcode` + `fmgr_info_cxt` resolution is the lsyscache
/// `get_opfamily_member` + `get_opcode` pair; the `"missing operator"`
/// `elog(ERROR)`, the cache invalidation, and the `RegProcedureIsValid` assert
/// are in-crate.
///
/// Note: this function mirrors `inclusion_get_strategy_procinfo`; see notes
/// there. If changes are made here, see that function too.
fn minmax_get_strategy_procinfo(
    bdesc: &BrinDesc<'_>,
    opaque: &MinmaxOpaque,
    attno: AttrNumber,
    subtype: Oid,
    strategynum: u16,
) -> PgResult<Oid> {
    // Assert(strategynum >= 1 && strategynum <= BTMaxStrategyNumber);
    debug_assert!((1..=BT_MAX_STRATEGY_NUMBER).contains(&strategynum));

    // We cache the procedures for the previous subtype in the opaque struct, to
    // avoid repetitive syscache lookups. If the subtype changed, invalidate all
    // the cached entries.
    if opaque.cached_subtype.get() != subtype {
        for i in 1..=BT_MAX_STRATEGY_NUMBER {
            opaque.strategy_procinfos[(i - 1) as usize].set(INVALID_OID);
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
        // get_opfamily_member resolves the (opfamily, lefttype, righttype,
        // strategy) -> operator OID, the modern equivalent of the AMOPSTRATEGY
        // syscache lookup + amopopr read.
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
        // fmgr_info_cxt(get_opcode(oprid),
        //               &opaque->strategy_procinfos[strategynum - 1],
        //               bdesc->bd_context);
        let opcode = lsyscache::get_opcode::call(oprid)?;
        debug_assert!(opcode != INVALID_OID);
        opaque.strategy_procinfos[(strategynum - 1) as usize].set(opcode);
    }

    // return &opaque->strategy_procinfos[strategynum - 1];
    Ok(opaque.strategy_procinfos[(strategynum - 1) as usize].get())
}

// ===========================================================================
// Opclass-dispatch seam installers (backend-access-brin-entry-seams).
// ===========================================================================
//
// The BRIN AM (`brin.c`, unported) reaches the built-in opclasses' support
// procedures through these `(index, keyno, ...)`-keyed seams. The single
// installer resolves the column's support-procedure OID via `index_getprocinfo`
// (the same `BRIN_PROCNUM_*` lookup `brin_build_desc`/`bringetbitmap` do) and
// dispatches the built-in `brin_minmax_*` OIDs to the bodies above and the
// `brin_inclusion_*` OIDs into `backend-access-brin-inclusion`. The remaining
// built-in opclasses (bloom/minmax-multi) panic until their stage lands —
// `seam-and-panic`, never a silent stub.

/// `index_getprocinfo(index, keyno + 1, procnum).fn_oid` — the OID of the
/// opclass support procedure registered for indexed column `keyno` (0-based).
fn support_proc_oid(index: &Relation<'_>, keyno: usize, procnum: u16) -> PgResult<Oid> {
    let finfo = indexam::index_getprocinfo::call(index, (keyno + 1) as AttrNumber, procnum)?;
    Ok(finfo.fn_oid)
}

/// Panic for a built-in BRIN opclass support procedure whose stage has not
/// landed yet (bloom S3 / minmax-multi S4).
fn unported_opclass(which: &str, oid: Oid) -> ! {
    panic!("brin opclass support procedure {which} (proc oid {oid}) not yet ported");
}

fn dispatch_opcinfo<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    keyno: usize,
    atttypid: Oid,
) -> PgResult<PgBox<'mcx, BrinOpcInfo<'mcx>>> {
    let oid = support_proc_oid(index, keyno, BRIN_PROCNUM_OPCINFO)?;
    match oid {
        F_BRIN_MINMAX_OPCINFO => brin_minmax_opcinfo(mcx, atttypid),
        inclusion::F_BRIN_INCLUSION_OPCINFO => inclusion::brin_inclusion_opcinfo(mcx, atttypid),
        _ => unported_opclass("OpcInfo", oid),
    }
}

#[allow(clippy::too_many_arguments)]
fn dispatch_addvalue<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    attno: usize,
    collation: Oid,
    bdesc: &BrinDesc<'mcx>,
    bval: &mut BrinValues<'mcx>,
    value: &Datum<'mcx>,
    isnull: bool,
) -> PgResult<bool> {
    let oid = support_proc_oid(index, attno, BRIN_PROCNUM_ADDVALUE)?;
    match oid {
        F_BRIN_MINMAX_ADD_VALUE => {
            brin_minmax_add_value(mcx, bdesc, bval, value, isnull, collation)
        }
        inclusion::F_BRIN_INCLUSION_ADD_VALUE => {
            inclusion::brin_inclusion_add_value(mcx, bdesc, bval, value, isnull, collation)
        }
        _ => unported_opclass("AddValue", oid),
    }
}

fn dispatch_union<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    attno: usize,
    collation: Oid,
    bdesc: &BrinDesc<'mcx>,
    col_a: &mut BrinValues<'mcx>,
    col_b: &BrinValues<'mcx>,
) -> PgResult<()> {
    let oid = support_proc_oid(index, attno, BRIN_PROCNUM_UNION)?;
    match oid {
        F_BRIN_MINMAX_UNION => brin_minmax_union(mcx, bdesc, col_a, col_b, collation),
        inclusion::F_BRIN_INCLUSION_UNION => {
            inclusion::brin_inclusion_union(mcx, bdesc, col_a, col_b, collation)
        }
        _ => unported_opclass("Union", oid),
    }
}

fn dispatch_consistent_is_multi(index: &Relation<'_>, attno: usize) -> PgResult<bool> {
    let oid = support_proc_oid(index, attno, BRIN_PROCNUM_CONSISTENT)?;
    match oid {
        // Both minmax's and inclusion's Consistent use the old 3-arg signature
        // (fn_nargs < 4), so neither selects the multi-key form.
        F_BRIN_MINMAX_CONSISTENT | inclusion::F_BRIN_INCLUSION_CONSISTENT => Ok(false),
        _ => unported_opclass("Consistent", oid),
    }
}

#[allow(clippy::too_many_arguments)]
fn dispatch_consistent_single<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    attno: usize,
    collation: Oid,
    bdesc: &BrinDesc<'mcx>,
    bval: &BrinValues<'mcx>,
    key: &ScanKeyData<'mcx>,
) -> PgResult<bool> {
    let oid = support_proc_oid(index, attno, BRIN_PROCNUM_CONSISTENT)?;
    match oid {
        F_BRIN_MINMAX_CONSISTENT => brin_minmax_consistent(bdesc, bval, key, collation),
        inclusion::F_BRIN_INCLUSION_CONSISTENT => {
            inclusion::brin_inclusion_consistent(mcx, bdesc, bval, key, collation)
        }
        _ => unported_opclass("Consistent", oid),
    }
}

#[allow(clippy::too_many_arguments)]
fn dispatch_consistent_multi<'mcx>(
    _mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    attno: usize,
    _collation: Oid,
    _bdesc: &BrinDesc<'mcx>,
    _bval: &BrinValues<'mcx>,
    _keys: &[ScanKeyData<'mcx>],
) -> PgResult<bool> {
    // minmax never selects the multi-key form (Consistent is 3-arg), so this
    // arm is only reachable for an opclass whose stage has not landed.
    let oid = support_proc_oid(index, attno, BRIN_PROCNUM_CONSISTENT)?;
    unported_opclass("Consistent (multi)", oid)
}

/// Install the BRIN opclass-dispatch seams owned by the built-in opclasses.
/// Single installer per seam (CLAUDE.md); the built-in opclasses that have not
/// landed panic loudly on dispatch.
pub fn init_seams() {
    opclass::brin_opcinfo::set(dispatch_opcinfo);
    opclass::brin_addvalue::set(dispatch_addvalue);
    opclass::brin_union::set(dispatch_union);
    opclass::brin_consistent_is_multi::set(dispatch_consistent_is_multi);
    opclass::brin_consistent_single::set(dispatch_consistent_single);
    opclass::brin_consistent_multi::set(dispatch_consistent_multi);
}

#[cfg(test)]
mod tests;
