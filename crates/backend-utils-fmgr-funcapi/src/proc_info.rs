//! `pg_proc`-row projection — `funcapi.c` lines 1379–1869.
//!
//! Extract argument types/names/modes, TRF types, the result column name, and
//! build the result `TupleDesc` of an OUT-parameter or RETURNS-TABLE function,
//! all from a `pg_proc` row.

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_namespace::FuncArgInfo;
use types_tuple::heaptuple::TupleDesc;

/// `get_func_arg_info(procTup, p_argtypes, p_argnames, p_argmodes)`
/// (funcapi.c:1379) — extract the all-argument type OID array, the per-argument
/// names (NULL where unnamed), and the per-argument modes from a `pg_proc` row;
/// returns the total argument count via the [`FuncArgInfo`] vectors' length.
pub fn get_func_arg_info<'mcx>(_mcx: Mcx<'mcx>, _proc_tuple_oid: Oid) -> PgResult<FuncArgInfo<'mcx>> {
    todo!("funcapi.c:1379 get_func_arg_info")
}

/// Inward-seam adapter for [`get_func_arg_info`]: matches the
/// `backend-utils-fmgr-funcapi-seams::get_func_arg_info` signature
/// (`(mcx, func_oid) -> PgResult<FuncArgInfo>`), which re-fetches the `pg_proc`
/// row by OID rather than taking the C caller's `HeapTuple`.
pub fn get_func_arg_info_seam<'mcx>(mcx: Mcx<'mcx>, func_oid: Oid) -> PgResult<FuncArgInfo<'mcx>> {
    get_func_arg_info(mcx, func_oid)
}

/// `get_func_trftypes(procTup, p_trftypes)` (funcapi.c:1475) — extract the
/// transform-function type OID array (`protrftypes`) from a `pg_proc` row;
/// empty when the function declares no transforms.
pub fn get_func_trftypes<'mcx>(_mcx: Mcx<'mcx>, _proc_tuple_oid: Oid) -> PgResult<PgVec<'mcx, Oid>> {
    todo!("funcapi.c:1475 get_func_trftypes")
}

/// `get_func_input_arg_names(proargnames, proargmodes, arg_names)`
/// (funcapi.c:1522) — derive the input-argument names array from the
/// `proargnames`/`proargmodes` arrays (skipping OUT-only modes), returning
/// `None` per unnamed input.
pub fn get_func_input_arg_names<'mcx>(
    _mcx: Mcx<'mcx>,
    _proargnames: types_datum::Datum,
    _proargmodes: types_datum::Datum,
) -> PgResult<PgVec<'mcx, Option<mcx::PgString<'mcx>>>> {
    todo!("funcapi.c:1522 get_func_input_arg_names")
}

/// `get_func_result_name(functionId)` (funcapi.c:1607) — the column name of a
/// single-OUT-parameter function's result, or `None` if the function has no
/// single named result column.
pub fn get_func_result_name<'mcx>(
    _mcx: Mcx<'mcx>,
    _function_id: Oid,
) -> PgResult<Option<mcx::PgString<'mcx>>> {
    todo!("funcapi.c:1607 get_func_result_name")
}

/// `build_function_result_tupdesc_t(procTuple)` (funcapi.c:1705) — build the
/// result `TupleDesc` for an OUT/INOUT/TABLE function from its `pg_proc` row
/// (delegating the array decoding to [`build_function_result_tupdesc_d`]);
/// `None` when the function returns no composite.
pub fn build_function_result_tupdesc_t<'mcx>(
    _mcx: Mcx<'mcx>,
    _proc_tuple_oid: Oid,
) -> PgResult<TupleDesc<'mcx>> {
    todo!("funcapi.c:1705 build_function_result_tupdesc_t")
}

/// `build_function_result_tupdesc_d(prokind, proallargtypes, proargmodes,
/// proargnames)` (funcapi.c:1751) — build the result `TupleDesc` from the
/// decoded OUT/INOUT/TABLE columns of the `pg_proc` argument arrays; `None`
/// when there is no composite result.
pub fn build_function_result_tupdesc_d<'mcx>(
    _mcx: Mcx<'mcx>,
    _prokind: u8,
    _proallargtypes: types_datum::Datum,
    _proargmodes: types_datum::Datum,
    _proargnames: types_datum::Datum,
) -> PgResult<TupleDesc<'mcx>> {
    todo!("funcapi.c:1751 build_function_result_tupdesc_d")
}
