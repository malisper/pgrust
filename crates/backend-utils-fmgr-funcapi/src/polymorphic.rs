//! Polymorphic pseudo-type resolution — `funcapi.c` lines 589–1378.
//!
//! Deduce the actual types behind `anyelement`/`anyarray`/`anyrange`/
//! `anymultirange` (and the `anycompatible*` family) from the function's actual
//! argument types, substitute them into a declared-argument array or a result
//! `TupleDesc`, and classify a type OID into a [`TypeFuncClass`].

use types_core::Oid;
use types_error::PgResult;
use types_nodes::funcapi::{PolymorphicActuals, TypeFuncClass};
use types_nodes::nodes::Node;
use types_tuple::heaptuple::TupleDesc;

/// `resolve_anyelement_from_others(actuals)` (funcapi.c:589) — derive
/// `anyelement_type` from a known `anyarray`/`anyrange`/`anymultirange` actual
/// (its element type), `ereport`-ing if none is available.
pub fn resolve_anyelement_from_others(_actuals: &mut PolymorphicActuals) -> PgResult<()> {
    todo!("funcapi.c:589 resolve_anyelement_from_others")
}

/// `resolve_anyarray_from_others(actuals)` (funcapi.c:655) — derive
/// `anyarray_type` from the known `anyelement` actual (its array type).
pub fn resolve_anyarray_from_others(_actuals: &mut PolymorphicActuals) -> PgResult<()> {
    todo!("funcapi.c:655 resolve_anyarray_from_others")
}

/// `resolve_anyrange_from_others(actuals)` (funcapi.c:681) — derive
/// `anyrange_type` from the known `anymultirange` actual (its range type).
pub fn resolve_anyrange_from_others(_actuals: &mut PolymorphicActuals) -> PgResult<()> {
    todo!("funcapi.c:681 resolve_anyrange_from_others")
}

/// `resolve_anymultirange_from_others(actuals)` (funcapi.c:710) — derive
/// `anymultirange_type` from the known `anyrange` actual (its multirange type).
pub fn resolve_anymultirange_from_others(_actuals: &mut PolymorphicActuals) -> PgResult<()> {
    todo!("funcapi.c:710 resolve_anymultirange_from_others")
}

/// `resolve_polymorphic_tupdesc(tupdesc, declared_args, call_expr)`
/// (funcapi.c:744) — substitute the resolved polymorphic actuals into each
/// polymorphic column of `tupdesc` in place; returns `false` if a substitution
/// could not be determined (C `bool`).
pub fn resolve_polymorphic_tupdesc<'mcx>(
    _tupdesc: &mut TupleDesc<'mcx>,
    _declared_args: &[Oid],
    _call_expr: Option<&Node<'mcx>>,
) -> PgResult<bool> {
    todo!("funcapi.c:744 resolve_polymorphic_tupdesc")
}

/// `resolve_polymorphic_argtypes(numargs, argtypes, argmodes, call_expr)`
/// (funcapi.c:1064) — two-pass substitution of the polymorphic entries of the
/// `argtypes` array (per `argmodes`) from the call's actual argument types;
/// returns `false` if resolution failed (C `bool`).
pub fn resolve_polymorphic_argtypes<'mcx>(
    _argtypes: &mut [Oid],
    _argmodes: Option<&[u8]>,
    _call_expr: Option<&Node<'mcx>>,
) -> PgResult<bool> {
    todo!("funcapi.c:1064 resolve_polymorphic_argtypes")
}

/// `get_type_func_class(typid, base_typeid)` (funcapi.c:1328) — classify a type
/// OID into a [`TypeFuncClass`] (the `get_typtype` switch); for a domain also
/// yields the resolved base type OID.
pub fn get_type_func_class(_typid: Oid) -> PgResult<(TypeFuncClass, Oid)> {
    todo!("funcapi.c:1328 get_type_func_class")
}

/// `get_call_expr_argtype(call_expr, argnum)` (fmgr.c:1929, hosted here while
/// `FmgrInfo.fn_expr` is still the tag-only ABI stub) — return the actual type
/// OID of argument `argnum` of the call expression, or `InvalidOid`.
pub fn get_call_expr_argtype<'mcx>(_call_expr: Option<&Node<'mcx>>, _argnum: i32) -> Oid {
    todo!("funcapi.c get_call_expr_argtype")
}
