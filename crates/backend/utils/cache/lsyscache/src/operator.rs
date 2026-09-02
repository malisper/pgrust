use crate::attribute::name_to_pgstring;
use crate::cache_lookup_error;
use crate::function::{func_strict, func_volatile};
use mcx::{Mcx, PgString};
use types_core::{InvalidOid, Oid, RegProcedure, RECORDOID};
use types_error::PgResult;

// pg_operator.dat
pub const ARRAY_EQ_OP: Oid = 1070;
pub const RECORD_EQ_OP: Oid = 2988;
// upstream 11aed8d19cd7 (18.4): Fix missed checks for hashability of container-type equality.
pub const RANGE_EQ_OP: Oid = 3882;
pub const MULTIRANGE_EQ_OP: Oid = 2860;
const _: () = assert!(RECORDOID == 2249);

#[cold]
fn operator_lookup_failed(opno: Oid) -> Box<types_error::PgError> {
    cache_lookup_error(format!("cache lookup failed for operator {opno}"))
}

pub fn get_opcode(opno: Oid) -> PgResult<RegProcedure> {
    Ok(match syscache_seams::lookup_pg_operator_shape::call(opno)? {
        Some(optup) => optup.oprcode,
        None => InvalidOid,
    })
}

pub fn get_opname<'mcx>(mcx: Mcx<'mcx>, opno: Oid) -> PgResult<Option<PgString<'mcx>>> {
    match syscache_seams::pg_operator_oprname::call(opno)? {
        Some(name) => Ok(Some(name_to_pgstring(mcx, &name)?)),
        None => Ok(None),
    }
}

pub fn get_op_rettype(opno: Oid) -> PgResult<Oid> {
    Ok(match syscache_seams::lookup_pg_operator_shape::call(opno)? {
        Some(optup) => optup.oprresult,
        None => InvalidOid,
    })
}

pub fn op_input_types(opno: Oid) -> PgResult<(Oid, Oid)> {
    match syscache_seams::lookup_pg_operator_shape::call(opno)? {
        Some(optup) => Ok((optup.oprleft, optup.oprright)),
        None => Err(operator_lookup_failed(opno)),
    }
}

const F_BTARRAYCMP: Oid = 382;
const F_BTRECORDCMP: Oid = 2987;
const F_HASH_ARRAY: Oid = 626;
const F_HASH_RECORD: Oid = 6192;
const F_HASH_RANGE: Oid = 3902;
const F_HASH_MULTIRANGE: Oid = 4278;

pub fn op_mergejoinable(opno: Oid, inputtype: Oid) -> PgResult<bool> {
    if opno == RECORD_EQ_OP {
        return Ok(typcache_seams::type_cache_cmp_proc::call(inputtype)? == F_BTRECORDCMP);
    }
    if opno == ARRAY_EQ_OP {
        return Ok(typcache_seams::type_cache_cmp_proc::call(inputtype)? == F_BTARRAYCMP);
    }
    Ok(match syscache_seams::lookup_pg_operator_shape::call(opno)? {
        Some(optup) => optup.oprcanmerge,
        None => false,
    })
}

pub fn op_hashjoinable(opno: Oid, inputtype: Oid) -> PgResult<bool> {
    if opno == RECORD_EQ_OP {
        return Ok(typcache_seams::type_cache_hash_proc::call(inputtype)? == F_HASH_RECORD);
    }
    if opno == ARRAY_EQ_OP {
        return Ok(typcache_seams::type_cache_hash_proc::call(inputtype)? == F_HASH_ARRAY);
    }
    // upstream 11aed8d19cd7 (18.4): Fix missed checks for hashability of container-type equality.
    if opno == RANGE_EQ_OP {
        return Ok(typcache_seams::type_cache_hash_proc::call(inputtype)? == F_HASH_RANGE);
    }
    if opno == MULTIRANGE_EQ_OP {
        return Ok(typcache_seams::type_cache_hash_proc::call(inputtype)? == F_HASH_MULTIRANGE);
    }
    Ok(match syscache_seams::lookup_pg_operator_shape::call(opno)? {
        Some(optup) => optup.oprcanhash,
        None => false,
    })
}

// upstream 11aed8d19cd7 (18.4): Fix missed checks for hashability of container-type equality.
// get_op_hash_functions_ext (lsyscache.c): get_op_hash_functions plus the
// container-type check the planner needs; the hash functions of array_eq,
// record_eq, range_eq and multirange_eq fail at runtime when the contained
// type is not hashable. As in op_hashjoinable, the left input type decides.
pub fn get_op_hash_functions_ext(opno: Oid, inputtype: Oid) -> PgResult<Option<(Oid, Oid)>> {
    let required = match opno {
        ARRAY_EQ_OP => F_HASH_ARRAY,
        RECORD_EQ_OP => F_HASH_RECORD,
        RANGE_EQ_OP => F_HASH_RANGE,
        MULTIRANGE_EQ_OP => F_HASH_MULTIRANGE,
        _ => InvalidOid,
    };
    if required != InvalidOid && typcache_seams::type_cache_hash_proc::call(inputtype)? != required {
        return Ok(None);
    }
    crate::amop::get_op_hash_functions(opno)
}

pub fn op_strict(opno: Oid) -> PgResult<bool> {
    let funcid = get_opcode(opno)?;
    if funcid == InvalidOid {
        return Err(cache_lookup_error(format!("operator {opno} does not exist")));
    }
    func_strict(funcid)
}

pub fn op_volatile(opno: Oid) -> PgResult<i8> {
    let funcid = get_opcode(opno)?;
    if funcid == InvalidOid {
        return Err(cache_lookup_error(format!("operator {opno} does not exist")));
    }
    func_volatile(funcid)
}

pub fn get_commutator(opno: Oid) -> PgResult<Oid> {
    Ok(match syscache_seams::lookup_pg_operator_shape::call(opno)? {
        Some(optup) => optup.oprcom,
        None => InvalidOid,
    })
}

pub fn get_negator(opno: Oid) -> PgResult<Oid> {
    Ok(match syscache_seams::lookup_pg_operator_shape::call(opno)? {
        Some(optup) => optup.oprnegate,
        None => InvalidOid,
    })
}

pub fn get_oprrest(opno: Oid) -> PgResult<RegProcedure> {
    Ok(match syscache_seams::lookup_pg_operator_shape::call(opno)? {
        Some(optup) => optup.oprrest,
        None => InvalidOid,
    })
}

pub fn get_oprjoin(opno: Oid) -> PgResult<RegProcedure> {
    Ok(match syscache_seams::lookup_pg_operator_shape::call(opno)? {
        Some(optup) => optup.oprjoin,
        None => InvalidOid,
    })
}
