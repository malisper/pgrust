//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! sequence functions in `sequence.c`: `nextval`/`currval`/`lastval`/`setval`.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in this crate, and writes back the
//! `int8` result word. [`register_sequence_builtins`] registers every row into
//! the fmgr-core builtin table (C: `fmgr_builtins[]`) so by-OID dispatch
//! resolves them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`: all rows are `proisstrict => 't'` (implied) and not
//! `proretset`.
//!
//! The cores (`nextval_internal`/`currval_internal`/`lastval_internal`/
//! `do_setval`) take a real `Mcx` rather than the call frame, so each adapter
//! pulls the regclass/int8/bool arguments off the `types_fmgr` frame and spins a
//! scratch context to drive them — the same shape `namespace.c`'s builtins use.

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use types_core::Oid;

/// A scratch context for cores that allocate / read through `Mcx`. The C
/// counterparts palloc their result into the caller's context; here the `int8`
/// result is a bare word that crosses by value, so the context is purely a
/// working arena for the call.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("sequence fmgr scratch")
}

/// `PG_GETARG_OID(i)`.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo.arg(i).expect("sequence fn: missing oid arg").value.as_oid()
}

/// `PG_GETARG_INT64(i)`.
#[inline]
fn arg_i64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("sequence fn: missing int8 arg").value.as_i64()
}

/// `PG_GETARG_BOOL(i)`.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).expect("sequence fn: missing bool arg").value.as_bool()
}

/// `PG_RETURN_INT64(v)`.
#[inline]
fn ret_i64(v: i64) -> Datum {
    Datum::from_i64(v)
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `nextval_oid(PG_FUNCTION_ARGS)` — SQL `nextval(regclass)`.
fn fc_nextval_oid(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let relid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    Ok(ret_i64(crate::nextval_internal(m.mcx(), relid, true)?))
}

/// `currval_oid(PG_FUNCTION_ARGS)` — SQL `currval(regclass)`.
fn fc_currval_oid(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let relid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    Ok(ret_i64(crate::currval_internal(m.mcx(), relid)?))
}

/// `lastval(PG_FUNCTION_ARGS)` — SQL `lastval()`.
fn fc_lastval(_fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    Ok(ret_i64(crate::lastval_internal(m.mcx())?))
}

/// `setval_oid(PG_FUNCTION_ARGS)` — SQL `setval(regclass, bigint)`.
fn fc_setval_oid(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let relid = arg_oid(fcinfo, 0);
    let next = arg_i64(fcinfo, 1);
    let m = scratch_mcx();
    crate::do_setval(m.mcx(), relid, next, true)?;
    Ok(ret_i64(next))
}

/// `setval3_oid(PG_FUNCTION_ARGS)` — SQL `setval(regclass, bigint, boolean)`.
fn fc_setval3_oid(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let relid = arg_oid(fcinfo, 0);
    let next = arg_i64(fcinfo, 1);
    let iscalled = arg_bool(fcinfo, 2);
    let m = scratch_mcx();
    crate::do_setval(m.mcx(), relid, next, iscalled)?;
    Ok(ret_i64(next))
}

/// `PG_RETURN_DATUM(HeapTupleGetDatum(tuple))` for the record-returning
/// sequence-info functions: the value core returns the composite as a
/// `types_tuple::Datum`; route it onto the fmgr frame's by-reference `Composite`
/// lane (mirrors `regclass.c`'s `ret_composite_datum`). A `ByVal(0)` is the
/// core's `Datum::null()` (e.g. the missing-relation path of
/// `pg_get_sequence_data`), routed to a NULL fmgr result.
fn ret_composite_datum(
    fcinfo: &mut FunctionCallInfoBaseData,
    d: types_tuple::Datum<'_>,
) -> Datum {
    use types_fmgr::boundary::RefPayload;
    match d {
        types_tuple::Datum::ByRef(bytes) => {
            fcinfo.set_ref_result(RefPayload::Composite(bytes.as_slice().to_vec()));
            Datum::from_usize(0)
        }
        types_tuple::Datum::Composite(t) => {
            fcinfo.set_ref_result(RefPayload::Composite(t.to_datum_image()));
            Datum::from_usize(0)
        }
        types_tuple::Datum::ByVal(0) => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
        _ => panic!("sequence fmgr: unexpected Datum arm from composite-returning core"),
    }
}

/// `pg_sequence_parameters(PG_FUNCTION_ARGS)` — SQL `pg_sequence_parameters(oid)`.
fn fc_pg_sequence_parameters(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let relid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let d = crate::pg_sequence_parameters_core(m.mcx(), relid)?;
    Ok(ret_composite_datum(fcinfo, d))
}

/// `pg_get_sequence_data(PG_FUNCTION_ARGS)` — SQL `pg_get_sequence_data(regclass)`.
fn fc_pg_get_sequence_data(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let relid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let d = crate::pg_get_sequence_data_core(m.mcx(), relid)?;
    Ok(ret_composite_datum(fcinfo, d))
}

/// `pg_sequence_last_value(PG_FUNCTION_ARGS)` — SQL `pg_sequence_last_value(regclass)`.
/// Returns int8 by value, or NULL when the sequence has not been called.
fn fc_pg_sequence_last_value(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let relid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    // The core returns `None` for the not-yet-called case (C: `PG_RETURN_NULL()`),
    // carried explicitly so a real last_value of 0 isn't mistaken for NULL.
    match crate::pg_sequence_last_value_core(m.mcx(), relid)? {
        Some(v) => Ok(ret_i64(v)),
        None => {
            fcinfo.set_result_null(true);
            Ok(Datum::from_usize(0))
        }
    }
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict,
            retset,
            func: None,
        },
        native,
    )
}

/// Register the sequence-function builtins into the fmgr-core builtin table.
pub fn register_sequence_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        builtin(1574, "nextval_oid", 1, true, false, fc_nextval_oid),
        builtin(1575, "currval_oid", 1, true, false, fc_currval_oid),
        builtin(1576, "setval_oid", 2, true, false, fc_setval_oid),
        builtin(1765, "setval3_oid", 3, true, false, fc_setval3_oid),
        builtin(2559, "lastval", 0, true, false, fc_lastval),
        builtin(3078, "pg_sequence_parameters", 1, true, false, fc_pg_sequence_parameters),
        builtin(4032, "pg_sequence_last_value", 1, true, false, fc_pg_sequence_last_value),
        builtin(6427, "pg_get_sequence_data", 1, true, false, fc_pg_get_sequence_data),
    ]);
}
