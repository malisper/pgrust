//! `function` family — `lsyscache.c` lookups keyed on `pg_proc`
//! (`PROCOID` syscache).
//!
//! C entry points covered here: `get_func_rettype`, `get_func_signature`.
//!
//! Both bottom out in `SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid))`.
//! That probe is owned by the syscache layer, so it is routed through the
//! `backend-utils-cache-syscache-seams` `proc_row_by_oid` seam (a
//! `SearchSysCache1(PROCOID)` projected to the `pg_proc` row's fields, with the
//! C `!HeapTupleIsValid(tp)` miss surfacing as `Ok(None)`). Each entry point
//! turns that miss into the C `elog(ERROR, "cache lookup failed for function
//! %u")` and projects the field(s) the C function reads out of `GETSTRUCT(tp)`.

use mcx::{Mcx, MemoryContext, PgString, PgVec};
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult};

use syscache_seams as syscache_seam;

/// `elog(ERROR, ...)` — an internal error with the default
/// `ERRCODE_INTERNAL_ERROR` SQLSTATE.
fn elog_error<T>(message: String) -> PgResult<T> {
    Err(PgError::error(message))
}

/// `get_func_rettype(funcid)` (lsyscache.c).
///
/// ```c
/// Oid
/// get_func_rettype(Oid funcid)
/// {
///     HeapTuple   tp;
///     Oid         result;
///
///     tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
///     if (!HeapTupleIsValid(tp))
///         elog(ERROR, "cache lookup failed for function %u", funcid);
///
///     result = ((Form_pg_proc) GETSTRUCT(tp))->prorettype;
///     ReleaseSysCache(tp);
///     return result;
/// }
/// ```
pub fn get_func_rettype(funcid: Oid) -> PgResult<Oid> {
    // The C reads a single scalar field; the syscache probe / row projection
    // allocate in a transient context that is dropped on return (mirroring the
    // C `SearchSysCache1` + `ReleaseSysCache` lifetime around the field read).
    let scratch = MemoryContext::new("get_func_rettype");
    let tp = match syscache_seam::proc_row_by_oid::call(scratch.mcx(), funcid)? {
        Some(tp) => tp,
        None => return elog_error(format!("cache lookup failed for function {funcid}")),
    };

    Ok(tp.prorettype)
}

/// `get_func_signature(funcid, &argtypes, &nargs)` (lsyscache.c).
///
/// ```c
/// Oid
/// get_func_signature(Oid funcid, Oid **argtypes, int *nargs)
/// {
///     HeapTuple   tp;
///     Form_pg_proc procstruct;
///     Oid         result;
///
///     tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
///     if (!HeapTupleIsValid(tp))
///         elog(ERROR, "cache lookup failed for function %u", funcid);
///
///     procstruct = (Form_pg_proc) GETSTRUCT(tp);
///
///     result = procstruct->prorettype;
///     *nargs = (int) procstruct->pronargs;
///     Assert(*nargs == procstruct->proargtypes.dim1);
///     *argtypes = (Oid *) palloc(*nargs * sizeof(Oid));
///     memcpy(*argtypes, procstruct->proargtypes.values, *nargs * sizeof(Oid));
///
///     ReleaseSysCache(tp);
///     return result;
/// }
/// ```
///
/// The seam contract returns the argument-type array (the C `*argtypes`,
/// length `*nargs`), palloc'd in the caller's `mcx`. `proc_row_by_oid`
/// projects `procstruct->proargtypes.values` (length `proargtypes.dim1`)
/// straight into `mcx`, which is the C `palloc` + `memcpy`; the C
/// `Assert(*nargs == proargtypes.dim1)` is upheld by `proc_row_by_oid`, which
/// builds the vector from `pronargs` over `proargtypes.dim1`.
pub fn get_func_signature<'mcx>(mcx: Mcx<'mcx>, func_oid: Oid) -> PgResult<PgVec<'mcx, Oid>> {
    let procstruct = match syscache_seam::proc_row_by_oid::call(mcx, func_oid)? {
        Some(procstruct) => procstruct,
        None => return elog_error(format!("cache lookup failed for function {func_oid}")),
    };

    Ok(procstruct.proargtypes)
}

/// Fetch the fixed-width `Form_pg_proc` row for `funcid`, or `None` on a cache
/// miss. The probe / projection allocate in a short-lived scratch context that
/// drops on return (mirroring the C `SearchSysCache1` + `ReleaseSysCache`).
fn pg_proc_form(funcid: Oid) -> PgResult<Option<syscache_seam::PgProcForm>> {
    let scratch = MemoryContext::new("pg_proc_form");
    syscache_seam::pg_proc_form::call(scratch.mcx(), funcid)
}

/// `get_func_name(funcid)` (lsyscache.c): the function's name copied into
/// `mcx` (C: `pstrdup`), or `Ok(None)` if absent.
pub fn get_func_name<'mcx>(mcx: Mcx<'mcx>, funcid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    let scratch = MemoryContext::new("get_func_name");
    match syscache_seam::pg_proc_form::call(scratch.mcx(), funcid)? {
        Some(functup) => Ok(Some(PgString::from_str_in(&functup.proname, mcx)?)),
        None => Ok(None),
    }
}

/// `get_func_namespace(funcid)` (lsyscache.c): the function's schema OID, or
/// `InvalidOid` if absent.
pub fn get_func_namespace(funcid: Oid) -> PgResult<Oid> {
    match pg_proc_form(funcid)? {
        Some(functup) => Ok(functup.pronamespace),
        None => Ok(InvalidOid),
    }
}

/// `get_func_nargs(funcid)` (lsyscache.c): the number of arguments; a missing
/// function is `elog(ERROR)`.
pub fn get_func_nargs(funcid: Oid) -> PgResult<i32> {
    match pg_proc_form(funcid)? {
        Some(functup) => Ok(functup.pronargs as i32),
        None => elog_error(format!("cache lookup failed for function {funcid}")),
    }
}

/// `get_func_variadictype(funcid)` (lsyscache.c): `provariadic`; missing
/// function is `elog(ERROR)`.
pub fn get_func_variadictype(funcid: Oid) -> PgResult<Oid> {
    match pg_proc_form(funcid)? {
        Some(functup) => Ok(functup.provariadic),
        None => elog_error(format!("cache lookup failed for function {funcid}")),
    }
}

/// `get_func_retset(funcid)` (lsyscache.c): `proretset`; missing function is
/// `elog(ERROR)`.
pub fn get_func_retset(funcid: Oid) -> PgResult<bool> {
    match pg_proc_form(funcid)? {
        Some(functup) => Ok(functup.proretset),
        None => elog_error(format!("cache lookup failed for function {funcid}")),
    }
}

/// `func_strict(funcid)` (lsyscache.c): `proisstrict`; missing function is
/// `elog(ERROR)`.
pub fn func_strict(funcid: Oid) -> PgResult<bool> {
    match pg_proc_form(funcid)? {
        Some(functup) => Ok(functup.proisstrict),
        None => elog_error(format!("cache lookup failed for function {funcid}")),
    }
}

/// `func_volatile(funcid)` (lsyscache.c): `provolatile` (`i`/`s`/`v`); missing
/// function is `elog(ERROR)`.
pub fn func_volatile(funcid: Oid) -> PgResult<u8> {
    match pg_proc_form(funcid)? {
        Some(functup) => Ok(functup.provolatile as u8),
        None => elog_error(format!("cache lookup failed for function {funcid}")),
    }
}

/// `func_parallel(funcid)` (lsyscache.c): `proparallel` (`s`/`r`/`u`); missing
/// function is `elog(ERROR)`.
pub fn func_parallel(funcid: Oid) -> PgResult<u8> {
    match pg_proc_form(funcid)? {
        Some(functup) => Ok(functup.proparallel as u8),
        None => elog_error(format!("cache lookup failed for function {funcid}")),
    }
}

/// `get_func_prokind(funcid)` (lsyscache.c): `prokind` (`f`/`p`/`a`/`w`);
/// missing function is `elog(ERROR)`.
pub fn get_func_prokind(funcid: Oid) -> PgResult<u8> {
    match pg_proc_form(funcid)? {
        Some(functup) => Ok(functup.prokind as u8),
        None => elog_error(format!("cache lookup failed for function {funcid}")),
    }
}

/// `get_func_leakproof(funcid)` (lsyscache.c): `proleakproof`; missing function
/// is `elog(ERROR)`.
pub fn get_func_leakproof(funcid: Oid) -> PgResult<bool> {
    match pg_proc_form(funcid)? {
        Some(functup) => Ok(functup.proleakproof),
        None => elog_error(format!("cache lookup failed for function {funcid}")),
    }
}

/// `get_func_support(funcid)` (lsyscache.c): the planner support function OID
/// (`prosupport`), or `InvalidOid` if absent.
pub fn get_func_support(funcid: Oid) -> PgResult<Oid> {
    match pg_proc_form(funcid)? {
        Some(functup) => Ok(functup.prosupport),
        None => Ok(InvalidOid),
    }
}

/// The function's `prosrc` text copied into `mcx`, or `Ok(None)` on a cache miss
/// / SQL-null `prosrc`. Routes a dynamically-OID'd planner support function to
/// its kernel by symbol name.
pub fn get_func_prosrc<'mcx>(mcx: Mcx<'mcx>, funcid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    syscache_seam::proc_prosrc::call(mcx, funcid)
}
