//! amvalidate.c — support routines for index AM validators and
//! amadjustmembers.
#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_core::{InvalidOid, Oid, BTREE_AM_OID, INTERNALOID, VOIDOID};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};

pub const AMOP_SEARCH: i8 = b's' as i8;
pub const AMOP_ORDER: i8 = b'o' as i8;

pub use syscache_seams::{PgAmopRow, PgAmprocRow};

#[cfg(test)]
mod tests;

// amvalidate.c:54 elog(ERROR, "cannot validate operator family without ordered data").
#[track_caller]
#[cold]
#[inline(never)]
fn unordered_data() -> Box<PgError> {
    Box::new(
        PgError::error("cannot validate operator family without ordered data")
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

#[derive(Clone, Copy, Debug)]
pub struct OpFamilyOpFuncGroup {
    pub lefttype: Oid,
    pub righttype: Oid,
    pub operatorset: u64,
    pub functionset: u64,
}

// One group per lefttype/righttype pair; bit K set for strategy/procnum
// K < 64. Lists must arrive in AMOPSTRATEGY/AMPROCNUM cache order.
pub fn identify_opfamily_groups<'mcx>(
    mcx: Mcx<'mcx>,
    oprlist: &[PgAmopRow],
    oprlist_ordered: bool,
    proclist: &[PgAmprocRow],
    proclist_ordered: bool,
) -> PgResult<PgVec<'mcx, OpFamilyOpFuncGroup>> {
    // amvalidate.c:53-54: elog(ERROR), a catchable XX000.
    if !oprlist_ordered || !proclist_ordered {
        return Err(unordered_data());
    }

    let mut result: PgVec<'mcx, OpFamilyOpFuncGroup> = PgVec::new_in(mcx);
    let mut io = 0usize;
    let mut ip = 0usize;
    let mut this: Option<usize> = None;

    while io < oprlist.len() || ip < proclist.len() {
        if let (Some(g), true) = (this, io < oprlist.len()) {
            let opr = &oprlist[io];
            if opr.amoplefttype == result[g].lefttype && opr.amoprighttype == result[g].righttype {
                if opr.amopstrategy > 0 && opr.amopstrategy < 64 {
                    result[g].operatorset |= 1u64 << opr.amopstrategy;
                }
                io += 1;
                continue;
            }
        }
        if let (Some(g), true) = (this, ip < proclist.len()) {
            let proc = &proclist[ip];
            if proc.amproclefttype == result[g].lefttype
                && proc.amprocrighttype == result[g].righttype
            {
                if proc.amprocnum > 0 && proc.amprocnum < 64 {
                    result[g].functionset |= 1u64 << proc.amprocnum;
                }
                ip += 1;
                continue;
            }
        }

        // Time for a new group.
        let (lefttype, righttype) = if io < oprlist.len()
            && (ip >= proclist.len()
                || (oprlist[io].amoplefttype < proclist[ip].amproclefttype
                    || (oprlist[io].amoplefttype == proclist[ip].amproclefttype
                        && oprlist[io].amoprighttype < proclist[ip].amprocrighttype)))
        {
            (oprlist[io].amoplefttype, oprlist[io].amoprighttype)
        } else {
            (proclist[ip].amproclefttype, proclist[ip].amprocrighttype)
        };
        result.push(OpFamilyOpFuncGroup { lefttype, righttype, operatorset: 0, functionset: 0 });
        this = Some(result.len() - 1);
    }
    Ok(result)
}

// Result type must match exactly; args exactly or binary-coercibly.
pub fn check_amproc_signature(
    funcid: Oid,
    restype: Oid,
    exact: bool,
    minargs: usize,
    maxargs: usize,
    argtypes: &[Oid],
) -> PgResult<bool> {
    debug_assert_eq!(argtypes.len(), maxargs);
    let scratch = mcx::MemoryContext::new("check_amproc_signature");
    let (rettype, args) = lsyscache::get_func_signature(scratch.mcx(), funcid)?;
    let retset = lsyscache::get_func_retset(funcid)?;
    let mut result = true;
    if rettype != restype || retset || args.len() < minargs || args.len() > maxargs {
        result = false;
    }
    for (i, &argtype) in argtypes.iter().enumerate() {
        if i >= args.len() {
            continue;
        }
        let matched = if exact {
            argtype == args[i]
        } else {
            coerce::IsBinaryCoercible(argtype, args[i])?
        };
        if !matched {
            result = false;
        }
    }
    Ok(result)
}

pub fn check_amoptsproc_signature(funcid: Oid) -> PgResult<bool> {
    check_amproc_signature(funcid, VOIDOID, true, 1, 1, &[INTERNALOID])
}

// amvalidate.c:214 `elog(ERROR, "cache lookup failed for operator %u", opno)`
// on the OPEROID miss in check_amop_signature (C marks it "shouldn't happen")
// -- elog's default SQLSTATE is XX000 and the error is catchable, never a
// backend abort.
#[track_caller]
#[cold]
#[inline(never)]
fn operator_lookup_failed(opno: Oid) -> Box<types_error::PgError> {
    Box::new(types_error::PgError::error(format!(
        "cache lookup failed for operator {opno}"
    )))
}

pub fn check_amop_signature(opno: Oid, restype: Oid, lefttype: Oid, righttype: Oid) -> PgResult<bool> {
    let Some(shape) = syscache_seams::lookup_pg_operator_shape::call(opno)? else {
        return Err(operator_lookup_failed(opno));
    };
    Ok(shape.oprresult == restype
        && shape.oprleft == lefttype
        && shape.oprright == righttype
        && shape.oprleft != InvalidOid
        && shape.oprright != InvalidOid)
}

pub fn opclass_for_family_datatype(amoid: Oid, opfamilyoid: Oid, datatypeoid: Oid) -> PgResult<Oid> {
    let scratch = mcx::MemoryContext::new("opclass_for_family_datatype");
    let rows = syscache_seams::lookup_pg_opclass_rows_by_am::call(scratch.mcx(), amoid)?;
    for &(oid, opcfamily, opcintype, _opcdefault, _name) in rows.iter() {
        if opcfamily == opfamilyoid && opcintype == datatypeoid {
            return Ok(oid);
        }
    }
    Ok(InvalidOid)
}

pub fn opfamily_can_sort_type(opfamilyoid: Oid, datatypeoid: Oid) -> PgResult<bool> {
    Ok(opclass_for_family_datatype(BTREE_AM_OID, opfamilyoid, datatypeoid)? != InvalidOid)
}

// Each AM validator opens with SearchSysCache1(CLAOID) and
// `elog(ERROR, "cache lookup failed for operator class %u", opclassoid)` on a
// miss (nbtvalidate.c:61, hashvalidate.c:60, gistvalidate.c:52,
// ginvalidate.c:51, spgvalidate.c:63, brin_validate.c:58, blvalidate.c:50) —
// a catchable XX000, never a backend abort. Reached only through
// amapi::amvalidate, which now raises the same error first, so this is the
// can't-happen backstop rather than the SQL-facing arm; it stays an error so
// the whole family is abort-free.
#[track_caller]
#[cold]
#[inline(never)]
pub fn opclass_lookup_failed(opclassoid: Oid) -> Box<types_error::PgError> {
    Box::new(types_error::PgError::error(format!(
        "cache lookup failed for operator class {opclassoid}"
    )))
}
