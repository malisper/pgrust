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

use mcx::{Mcx, MemoryContext, PgVec};
use types_core::Oid;
use types_error::{PgError, PgResult};

use backend_utils_cache_syscache_seams as syscache_seam;

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
