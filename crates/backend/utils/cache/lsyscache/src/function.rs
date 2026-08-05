use crate::attribute::name_to_pgstring;
use crate::cache_lookup_error;
use mcx::{Mcx, PgString, PgVec};
use types_core::{InvalidOid, Oid, RegProcedure};
use types_error::PgResult;

#[cold]
fn function_lookup_failed(funcid: Oid) -> Box<types_error::PgError> {
    cache_lookup_error(format!("cache lookup failed for function {funcid}"))
}

pub fn get_func_name<'mcx>(mcx: Mcx<'mcx>, funcid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    match syscache_seams::pg_proc_proname::call(funcid)? {
        Some(name) => Ok(Some(name_to_pgstring(mcx, &name)?)),
        None => Ok(None),
    }
}

pub fn get_func_namespace(funcid: Oid) -> PgResult<Oid> {
    Ok(match syscache_seams::lookup_pg_proc_shape::call(funcid)? {
        Some(functup) => functup.pronamespace,
        None => InvalidOid,
    })
}

pub fn get_func_rettype(funcid: Oid) -> PgResult<Oid> {
    match syscache_seams::lookup_pg_proc_shape::call(funcid)? {
        Some(functup) => Ok(functup.prorettype),
        None => Err(function_lookup_failed(funcid)),
    }
}

pub fn get_func_nargs(funcid: Oid) -> PgResult<i32> {
    match syscache_seams::lookup_pg_proc_shape::call(funcid)? {
        Some(functup) => Ok(functup.pronargs as i32),
        None => Err(function_lookup_failed(funcid)),
    }
}

pub fn get_func_signature<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
) -> PgResult<(Oid, PgVec<'mcx, Oid>)> {
    match syscache_seams::lookup_pg_proc_signature::call(mcx, funcid)? {
        Some((rettype, argtypes)) => Ok((rettype, argtypes)),
        None => Err(function_lookup_failed(funcid)),
    }
}

pub fn get_func_variadictype(funcid: Oid) -> PgResult<Oid> {
    match syscache_seams::lookup_pg_proc_shape::call(funcid)? {
        Some(functup) => Ok(functup.provariadic),
        None => Err(function_lookup_failed(funcid)),
    }
}

pub fn get_func_retset(funcid: Oid) -> PgResult<bool> {
    match syscache_seams::lookup_pg_proc_shape::call(funcid)? {
        Some(functup) => Ok(functup.proretset),
        None => Err(function_lookup_failed(funcid)),
    }
}

pub fn func_strict(funcid: Oid) -> PgResult<bool> {
    match syscache_seams::lookup_pg_proc_shape::call(funcid)? {
        Some(functup) => Ok(functup.proisstrict),
        None => Err(function_lookup_failed(funcid)),
    }
}

pub fn func_volatile(funcid: Oid) -> PgResult<i8> {
    match syscache_seams::lookup_pg_proc_shape::call(funcid)? {
        Some(functup) => Ok(functup.provolatile),
        None => Err(function_lookup_failed(funcid)),
    }
}

pub fn func_parallel(funcid: Oid) -> PgResult<i8> {
    match syscache_seams::lookup_pg_proc_shape::call(funcid)? {
        Some(functup) => Ok(functup.proparallel),
        None => Err(function_lookup_failed(funcid)),
    }
}

pub fn get_func_prokind(funcid: Oid) -> PgResult<i8> {
    match syscache_seams::lookup_pg_proc_shape::call(funcid)? {
        Some(functup) => Ok(functup.prokind),
        None => Err(function_lookup_failed(funcid)),
    }
}

pub fn get_func_leakproof(funcid: Oid) -> PgResult<bool> {
    match syscache_seams::lookup_pg_proc_shape::call(funcid)? {
        Some(functup) => Ok(functup.proleakproof),
        None => Err(function_lookup_failed(funcid)),
    }
}

pub fn get_func_support(funcid: Oid) -> PgResult<RegProcedure> {
    Ok(match syscache_seams::lookup_pg_proc_shape::call(funcid)? {
        Some(functup) => functup.prosupport,
        None => InvalidOid,
    })
}
