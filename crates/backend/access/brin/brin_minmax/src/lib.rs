//! brin_minmax.c: Min/Max opclass for BRIN.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::datum::Datum;
use ::fmgr::{function_call2_coll_in, FmgrInfo};
use ::mcx::Mcx;
use ::types_brin::{BrinColInfo, BrinDesc, BrinOpcKind, BrinValues, MinmaxOpaque};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_scan::scankey::{
    ScanKeyData, BTEqualStrategyNumber, BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber,
    BTLessEqualStrategyNumber, BTLessStrategyNumber, BTMaxStrategyNumber,
};

pub fn brin_minmax_opcinfo(typoid: Oid) -> BrinColInfo {
    BrinColInfo {
        oi_opclass_options: None,
        oi_nstored: 2,
        oi_regular_nulls: true,
        kind: BrinOpcKind::MinMax,
        oi_typids: [typoid, typoid, 0],
        minmax: MinmaxOpaque::default(),
        distance_procinfo: core::cell::RefCell::new(None),
        bloom: None,
        inclusion: None,
    }
}

/// brin_minmax_add_value; true = the summary changed. By-ref copies land in
/// `mcx` (the memtuple's context); the C pfree of the replaced value is a
/// no-op there (bump reset reclaims it wholesale).
pub fn brin_minmax_add_value(
    mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    column: &mut BrinValues,
    newval: Datum,
    isnull: bool,
    colloid: Oid,
) -> PgResult<bool> {
    debug_assert!(!isnull);
    let attno = column.bv_attno as usize;
    let att = bdesc.bd_tupdesc.attr(attno - 1);
    let (attbyval, attlen, atttypid) = (att.attbyval, att.attlen, att.atttypid);

    if column.bv_allnulls {
        let copied = brin_tuple::datum_copy(mcx, newval, attbyval, attlen)?;
        column.bv_values[0] = copied;
        column.bv_values[1] = brin_tuple::datum_copy(mcx, newval, attbyval, attlen)?;
        column.bv_allnulls = false;
        return Ok(true);
    }

    let mut updated = false;

    let compar = call_strategy(
        mcx, bdesc, attno as u16, atttypid, BTLessStrategyNumber, colloid,
        newval, column.bv_values[0],
    )?;
    if compar.as_bool() {
        column.bv_values[0] = brin_tuple::datum_copy(mcx, newval, attbyval, attlen)?;
        updated = true;
    }

    let compar = call_strategy(
        mcx, bdesc, attno as u16, atttypid, BTGreaterStrategyNumber, colloid,
        newval, column.bv_values[1],
    )?;
    if compar.as_bool() {
        column.bv_values[1] = brin_tuple::datum_copy(mcx, newval, attbyval, attlen)?;
        updated = true;
    }

    Ok(updated)
}

pub fn brin_minmax_consistent(
    mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    column: &BrinValues,
    key: &ScanKeyData,
) -> PgResult<bool> {
    debug_assert!(!column.bv_allnulls);
    let attno = key.sk_attno as u16;
    let subtype = key.sk_subtype;
    let value = key.sk_argument;
    let colloid = key.sk_collation;

    let matches = match key.sk_strategy {
        s @ (BTLessStrategyNumber | BTLessEqualStrategyNumber) => call_strategy(
            mcx, bdesc, attno, subtype, s, colloid, column.bv_values[0], value,
        )?,
        BTEqualStrategyNumber => {
            let m = call_strategy(
                mcx, bdesc, attno, subtype, BTLessEqualStrategyNumber, colloid,
                column.bv_values[0], value,
            )?;
            if !m.as_bool() {
                m
            } else {
                call_strategy(
                    mcx, bdesc, attno, subtype, BTGreaterEqualStrategyNumber, colloid,
                    column.bv_values[1], value,
                )?
            }
        }
        s @ (BTGreaterEqualStrategyNumber | BTGreaterStrategyNumber) => call_strategy(
            mcx, bdesc, attno, subtype, s, colloid, column.bv_values[1], value,
        )?,
        other => panic!("invalid strategy number {other}"),
    };
    Ok(matches.as_bool())
}

pub fn brin_minmax_union(
    mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    colloid: Oid,
    col_a: &mut BrinValues,
    col_b: &BrinValues,
) -> PgResult<()> {
    debug_assert!(col_a.bv_attno == col_b.bv_attno);
    debug_assert!(!col_a.bv_allnulls && !col_b.bv_allnulls);

    let attno = col_a.bv_attno as usize;
    let att = bdesc.bd_tupdesc.attr(attno - 1);
    let (attbyval, attlen, atttypid) = (att.attbyval, att.attlen, att.atttypid);

    let needsadj = call_strategy(
        mcx, bdesc, attno as u16, atttypid, BTLessStrategyNumber, colloid,
        col_b.bv_values[0], col_a.bv_values[0],
    )?;
    if needsadj.as_bool() {
        col_a.bv_values[0] = brin_tuple::datum_copy(mcx, col_b.bv_values[0], attbyval, attlen)?;
    }

    let needsadj = call_strategy(
        mcx, bdesc, attno as u16, atttypid, BTGreaterStrategyNumber, colloid,
        col_b.bv_values[1], col_a.bv_values[1],
    )?;
    if needsadj.as_bool() {
        col_a.bv_values[1] = brin_tuple::datum_copy(mcx, col_b.bv_values[1], attbyval, attlen)?;
    }

    Ok(())
}

fn call_strategy(
    mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    attno: u16,
    subtype: Oid,
    strategynum: u16,
    colloid: Oid,
    arg1: Datum,
    arg2: Datum,
) -> PgResult<Datum> {
    let mut finfo = minmax_get_strategy_procinfo(bdesc, attno, subtype, strategynum)?;
    function_call2_coll_in(&mut finfo, colloid, mcx, arg1, arg2)
}

// minmax_get_strategy_procinfo: per-subtype cache of the btree comparison
// procs, resolved through pg_amop + pg_operator (rule-5 cache).
fn minmax_get_strategy_procinfo(
    bdesc: &BrinDesc<'_>,
    attno: u16,
    subtype: Oid,
    strategynum: u16,
) -> PgResult<FmgrInfo> {
    debug_assert!(strategynum >= 1 && strategynum <= BTMaxStrategyNumber);
    let opaque = &bdesc.bd_info[attno as usize - 1].minmax;

    if opaque.cached_subtype.get() != subtype {
        // Per-slot stores, not `= [const { None }; 5]`: the whole-array
        // assignment memcpys a const-promoted array into the heap cell, which
        // defeats the model checker's field tracking of the niche
        // discriminant (proofs/brin-minmax); same stores, same cold path.
        for slot in opaque.strategy_procinfos.borrow_mut().iter_mut() {
            *slot = None;
        }
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
        panic!(
            "missing operator {strategynum}({atttypid},{subtype}) in opfamily {opfamily}"
        );
    }
    let proc = lsyscache::get_opcode(oprid)?;
    debug_assert!(proc != 0);
    let finfo = fmgr_core::fmgr_info(proc)?;
    opaque.strategy_procinfos.borrow_mut()[strategynum as usize - 1] = Some(finfo.clone());
    Ok(finfo)
}
