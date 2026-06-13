//! Descriptor builders + VARIADIC unpacking — `funcapi.c` lines 1870–2256.
//!
//! Build a result `TupleDesc` from a relation name or a (possibly composite)
//! type OID, and unpack a function's VARIADIC argument run into per-element
//! `(value, type, isnull)` triples.

use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::funcapi::ExtractedVariadicArgs;
use types_tuple::heaptuple::TupleDesc;

/// `RelationNameGetTupleDesc(relname)` (funcapi.c:1870) — look up the relation
/// by (possibly qualified) name and return a copy of its row `TupleDesc`.
pub fn RelationNameGetTupleDesc<'mcx>(_mcx: Mcx<'mcx>, _relname: &str) -> PgResult<TupleDesc<'mcx>> {
    todo!("funcapi.c:1870 RelationNameGetTupleDesc")
}

/// `TypeGetTupleDesc(typeoid, colaliases)` (funcapi.c:1903) — build a
/// `TupleDesc` for `typeoid`: for a composite type its row descriptor (renamed
/// per `colaliases`), otherwise a single-column descriptor of that base type.
pub fn TypeGetTupleDesc<'mcx>(
    _mcx: Mcx<'mcx>,
    _typeoid: Oid,
    _colaliases: Option<&[PgString<'mcx>]>,
) -> PgResult<TupleDesc<'mcx>> {
    todo!("funcapi.c:1903 TypeGetTupleDesc")
}

/// `extract_variadic_args(fcinfo, variadic_start, convert_unknown, values,
/// types, nulls)` (funcapi.c:2005) — unpack the function's VARIADIC argument
/// run starting at `variadic_start` into per-element triples. A real VARIADIC
/// array argument is deconstructed; otherwise the trailing scalar args are
/// gathered (optionally converting `unknown` literals to `text`). Returns the
/// element count via the [`ExtractedVariadicArgs`] vectors' length, or `None`
/// for a NULL VARIADIC array.
pub fn extract_variadic_args<'mcx>(
    _mcx: Mcx<'mcx>,
    _fcinfo: &FunctionCallInfoBaseData<'mcx>,
    _variadic_start: i32,
    _convert_unknown: bool,
) -> PgResult<Option<ExtractedVariadicArgs<'mcx>>> {
    todo!("funcapi.c:2005 extract_variadic_args")
}
