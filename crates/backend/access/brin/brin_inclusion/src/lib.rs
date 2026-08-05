//! brin_inclusion.c: inclusion opclasses for BRIN.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

use ::datum::Datum;
use ::fmgr::FmgrInfo;
use ::mcx::Mcx;
use ::adt_scalar::datum_ops::datum_copy;
use ::types_brin::{
    BrinColInfo, BrinDesc, BrinOpcKind, BrinValues, InclusionOpaque, MinmaxOpaque,
    INCLUSION_MAX_PROCNUMS, RT_MAX_STRATEGY,
};
use ::types_core::{Oid, BOOLOID};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION};
use ::types_scan::scankey::{
    ScanKeyData, RTAboveStrategyNumber, RTAdjacentStrategyNumber, RTBelowStrategyNumber,
    RTContainedByStrategyNumber, RTContainsElemStrategyNumber, RTContainsStrategyNumber,
    RTEqualStrategyNumber, RTGreaterEqualStrategyNumber, RTGreaterStrategyNumber,
    RTLeftStrategyNumber, RTLessEqualStrategyNumber, RTLessStrategyNumber,
    RTOverAboveStrategyNumber, RTOverBelowStrategyNumber, RTOverLeftStrategyNumber,
    RTOverRightStrategyNumber, RTOverlapStrategyNumber, RTRightStrategyNumber,
    RTSameStrategyNumber, RTSubEqualStrategyNumber, RTSubStrategyNumber,
    RTSuperEqualStrategyNumber, RTSuperStrategyNumber,
};

const PROCNUM_MERGE: u16 = 11;
const PROCNUM_MERGEABLE: u16 = 12;
const PROCNUM_CONTAINS: u16 = 13;
const PROCNUM_EMPTY: u16 = 14;
const PROCNUM_BASE: u16 = 11;

const INCLUSION_UNION: usize = 0;
const INCLUSION_UNMERGEABLE: usize = 1;
const INCLUSION_CONTAINS_EMPTY: usize = 2;

pub fn brin_inclusion_opcinfo(typoid: Oid) -> BrinColInfo {
    BrinColInfo {
        oi_opclass_options: None,
        oi_nstored: 3,
        oi_regular_nulls: true,
        kind: BrinOpcKind::Inclusion,
        oi_typids: [typoid, BOOLOID, BOOLOID],
        minmax: MinmaxOpaque::default(),
        distance_procinfo: core::cell::RefCell::new(None),
        bloom: None,
        inclusion: Some(Box::new(InclusionOpaque::default())),
    }
}

pub fn brin_inclusion_add_value(
    mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    column: &mut BrinValues,
    newval: Datum,
    isnull: bool,
    colloid: Oid,
) -> PgResult<bool> {
    debug_assert!(!isnull);
    let attno = column.bv_attno;
    let att = bdesc.bd_tupdesc.attr(attno as usize - 1);
    let (attbyval, attlen) = (att.attbyval, att.attlen);

    let mut new = false;
    if column.bv_allnulls {
        column.bv_values[INCLUSION_UNION] = datum_copy(mcx, newval, attbyval, attlen)?;
        column.bv_values[INCLUSION_UNMERGEABLE] = Datum::from_bool(false);
        column.bv_values[INCLUSION_CONTAINS_EMPTY] = Datum::from_bool(false);
        column.bv_allnulls = false;
        new = true;
    }

    if column.bv_values[INCLUSION_UNMERGEABLE].as_bool() {
        return Ok(false);
    }

    if let Some(mut finfo) = inclusion_get_procinfo(bdesc, attno, PROCNUM_EMPTY, true)? {
        if fmgr_core::function_call1_coll_in(&mut finfo, colloid, mcx, newval)?.as_bool() {
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

    if let Some(mut finfo) = inclusion_get_procinfo(bdesc, attno, PROCNUM_CONTAINS, true)? {
        if fmgr_core::function_call2_coll_in(
            &mut finfo,
            colloid,
            mcx,
            column.bv_values[INCLUSION_UNION],
            newval,
        )?
        .as_bool()
        {
            return Ok(false);
        }
    }

    if let Some(mut finfo) = inclusion_get_procinfo(bdesc, attno, PROCNUM_MERGEABLE, true)? {
        if !fmgr_core::function_call2_coll_in(
            &mut finfo,
            colloid,
            mcx,
            column.bv_values[INCLUSION_UNION],
            newval,
        )?
        .as_bool()
        {
            column.bv_values[INCLUSION_UNMERGEABLE] = Datum::from_bool(true);
            return Ok(true);
        }
    }

    let mut finfo = inclusion_get_procinfo(bdesc, attno, PROCNUM_MERGE, false)?.unwrap();
    let mut result = fmgr_core::function_call2_coll_in(
        &mut finfo,
        colloid,
        mcx,
        column.bv_values[INCLUSION_UNION],
        newval,
    )?;
    // C pfrees the replaced union (bump reset reclaims it) and datumCopies a
    // returned-newval alias out of the heap tuple's lifetime.
    if !attbyval && result == newval {
        result = datum_copy(mcx, result, attbyval, attlen)?;
    }
    column.bv_values[INCLUSION_UNION] = result;
    Ok(true)
}

pub fn brin_inclusion_consistent(
    mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    column: &BrinValues,
    key: &ScanKeyData,
) -> PgResult<bool> {
    debug_assert!(!column.bv_allnulls);

    if column.bv_values[INCLUSION_UNMERGEABLE].as_bool() {
        return Ok(true);
    }

    let attno = key.sk_attno as u16;
    let subtype = key.sk_subtype;
    let query = key.sk_argument;
    let unionval = column.bv_values[INCLUSION_UNION];
    let colloid = key.sk_collation;

    let mut call = |strategynum: u16| -> PgResult<bool> {
        let mut finfo = inclusion_get_strategy_procinfo(bdesc, attno, subtype, strategynum)?;
        Ok(fmgr_core::function_call2_coll_in(&mut finfo, colloid, mcx, unionval, query)?
            .as_bool())
    };

    match key.sk_strategy {
        // Placement strategies: negated converse placement operator.
        RTLeftStrategyNumber => Ok(!call(RTOverRightStrategyNumber)?),
        RTOverLeftStrategyNumber => Ok(!call(RTRightStrategyNumber)?),
        RTOverRightStrategyNumber => Ok(!call(RTLeftStrategyNumber)?),
        RTRightStrategyNumber => Ok(!call(RTOverLeftStrategyNumber)?),
        RTBelowStrategyNumber => Ok(!call(RTOverAboveStrategyNumber)?),
        RTOverBelowStrategyNumber => Ok(!call(RTAboveStrategyNumber)?),
        RTOverAboveStrategyNumber => Ok(!call(RTBelowStrategyNumber)?),
        RTAboveStrategyNumber => Ok(!call(RTOverBelowStrategyNumber)?),

        s @ (RTOverlapStrategyNumber
        | RTContainsStrategyNumber
        | RTContainsElemStrategyNumber
        | RTSubStrategyNumber
        | RTSubEqualStrategyNumber) => call(s),

        // Contained-by: overlap, plus empty elements which are contained by
        // everything but never merged into the union.
        RTContainedByStrategyNumber | RTSuperStrategyNumber | RTSuperEqualStrategyNumber => {
            if call(RTOverlapStrategyNumber)? {
                return Ok(true);
            }
            Ok(column.bv_values[INCLUSION_CONTAINS_EMPTY].as_bool())
        }

        RTAdjacentStrategyNumber => {
            if call(RTOverlapStrategyNumber)? {
                return Ok(true);
            }
            call(RTAdjacentStrategyNumber)
        }

        // Basic comparisons; empty elements sort below everything.
        RTLessStrategyNumber | RTLessEqualStrategyNumber => {
            if !call(RTRightStrategyNumber)? {
                return Ok(true);
            }
            Ok(column.bv_values[INCLUSION_CONTAINS_EMPTY].as_bool())
        }

        RTSameStrategyNumber | RTEqualStrategyNumber => {
            if call(RTContainsStrategyNumber)? {
                return Ok(true);
            }
            Ok(column.bv_values[INCLUSION_CONTAINS_EMPTY].as_bool())
        }

        RTGreaterEqualStrategyNumber => {
            if !call(RTLeftStrategyNumber)? {
                return Ok(true);
            }
            Ok(column.bv_values[INCLUSION_CONTAINS_EMPTY].as_bool())
        }

        RTGreaterStrategyNumber => Ok(!call(RTLeftStrategyNumber)?),

        other => panic!("invalid strategy number {other}"),
    }
}

pub fn brin_inclusion_union(
    mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    colloid: Oid,
    col_a: &mut BrinValues,
    col_b: &BrinValues,
) -> PgResult<()> {
    debug_assert!(col_a.bv_attno == col_b.bv_attno);
    debug_assert!(!col_a.bv_allnulls && !col_b.bv_allnulls);

    let attno = col_a.bv_attno;
    let att = bdesc.bd_tupdesc.attr(attno as usize - 1);
    let (attbyval, attlen) = (att.attbyval, att.attlen);

    if !col_a.bv_values[INCLUSION_CONTAINS_EMPTY].as_bool()
        && col_b.bv_values[INCLUSION_CONTAINS_EMPTY].as_bool()
    {
        col_a.bv_values[INCLUSION_CONTAINS_EMPTY] = Datum::from_bool(true);
    }

    if col_a.bv_values[INCLUSION_UNMERGEABLE].as_bool() {
        return Ok(());
    }

    if col_b.bv_values[INCLUSION_UNMERGEABLE].as_bool() {
        col_a.bv_values[INCLUSION_UNMERGEABLE] = Datum::from_bool(true);
        return Ok(());
    }

    if let Some(mut finfo) = inclusion_get_procinfo(bdesc, attno, PROCNUM_MERGEABLE, true)? {
        if !fmgr_core::function_call2_coll_in(
            &mut finfo,
            colloid,
            mcx,
            col_a.bv_values[INCLUSION_UNION],
            col_b.bv_values[INCLUSION_UNION],
        )?
        .as_bool()
        {
            col_a.bv_values[INCLUSION_UNMERGEABLE] = Datum::from_bool(true);
            return Ok(());
        }
    }

    let mut finfo = inclusion_get_procinfo(bdesc, attno, PROCNUM_MERGE, false)?.unwrap();
    let mut result = fmgr_core::function_call2_coll_in(
        &mut finfo,
        colloid,
        mcx,
        col_a.bv_values[INCLUSION_UNION],
        col_b.bv_values[INCLUSION_UNION],
    )?;
    if !attbyval && result == col_b.bv_values[INCLUSION_UNION] {
        result = datum_copy(mcx, result, attbyval, attlen)?;
    }
    col_a.bv_values[INCLUSION_UNION] = result;
    Ok(())
}

/// Missing optional procs return None and are remembered (extra_proc_missing).
fn inclusion_get_procinfo(
    bdesc: &BrinDesc<'_>,
    attno: u16,
    procnum: u16,
    missing_ok: bool,
) -> PgResult<Option<FmgrInfo>> {
    let basenum = (procnum - PROCNUM_BASE) as usize;
    debug_assert!(basenum < INCLUSION_MAX_PROCNUMS);
    let opaque = bdesc.bd_info[attno as usize - 1]
        .inclusion
        .as_ref()
        .expect("inclusion column");

    if opaque.extra_proc_missing.get()[basenum] {
        return Ok(None);
    }

    if let Some(fi) = opaque.extra_procinfos.borrow()[basenum].as_ref() {
        return Ok(Some(fi.clone()));
    }

    let opfamily = bdesc.bd_opfamily[attno as usize - 1];
    let opcintype = bdesc.bd_opcintype[attno as usize - 1];
    let proc = lsyscache::get_opfamily_proc(opfamily, opcintype, opcintype, procnum as i16)?;
    if proc == 0 {
        if !missing_ok {
            return Err(invalid_opclass(procnum, attno));
        }
        let mut missing = opaque.extra_proc_missing.get();
        missing[basenum] = true;
        opaque.extra_proc_missing.set(missing);
        return Ok(None);
    }
    let finfo = fmgr_core::fmgr_info(proc)?;
    opaque.extra_procinfos.borrow_mut()[basenum] = Some(finfo.clone());
    Ok(Some(finfo))
}

// inclusion_get_strategy_procinfo: per-subtype cache of the strategy operator
// procs, resolved through pg_amop + pg_operator (rule-5 cache).
fn inclusion_get_strategy_procinfo(
    bdesc: &BrinDesc<'_>,
    attno: u16,
    subtype: Oid,
    strategynum: u16,
) -> PgResult<FmgrInfo> {
    debug_assert!(strategynum >= 1 && strategynum as usize <= RT_MAX_STRATEGY);
    let opaque = bdesc.bd_info[attno as usize - 1]
        .inclusion
        .as_ref()
        .expect("inclusion column");

    if opaque.cached_subtype.get() != subtype {
        *opaque.strategy_procinfos.borrow_mut() = [const { None }; RT_MAX_STRATEGY];
        opaque.cached_subtype.set(subtype);
    }

    {
        let cache = opaque.strategy_procinfos.borrow();
        if let Some(fi) = &cache[strategynum as usize - 1] {
            return Ok(fi.clone());
        }
    }

    let opfamily = bdesc.bd_opfamily[attno as usize - 1];
    let atttypid = bdesc.bd_tupdesc.attr(attno as usize - 1).atttypid;
    let oprid = lsyscache::get_opfamily_member(opfamily, atttypid, subtype, strategynum as i16)?;
    if oprid == 0 {
        panic!("missing operator {strategynum}({atttypid},{subtype}) in opfamily {opfamily}");
    }
    let proc = lsyscache::get_opcode(oprid)?;
    debug_assert!(proc != 0);
    let finfo = fmgr_core::fmgr_info(proc)?;
    opaque.strategy_procinfos.borrow_mut()[strategynum as usize - 1] = Some(finfo.clone());
    Ok(finfo)
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_opclass(procnum: u16, attno: u16) -> Box<PgError> {
    Box::new(
        PgError::error("invalid opclass definition")
            .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION)
            .with_detail(format!(
                "The operator class is missing support function {procnum} for column {attno}."
            )),
    )
}
