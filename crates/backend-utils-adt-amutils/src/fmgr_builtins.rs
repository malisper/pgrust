//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `amutils.c` functions — the four index-AM property/progress introspection
//! functions whose argument/result types are expressible at the current fmgr
//! boundary (the scalar `oid`/`regclass`/`int4`/`int8` words, the `text` by-ref
//! lane, and a `bool` or `text` result that may be SQL NULL).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in [`crate`], and writes back the
//! result word / by-reference payload. [`register_amutils_builtins`] registers
//! every row into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID
//! dispatch (and the `fmgr_isbuiltin` fast path early catalog scankeys rely on)
//! resolves them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat` (all four are strict by default, none retset).
//!
//! The C bodies unmarshal their args as: `PG_GETARG_OID(0)` (the `oid`/
//! `regclass` word), `PG_GETARG_INT32(1)` (the `int4` column number, or — for
//! `pg_indexam_progress_phasename` — the *truncation* of the `int8` phase
//! number, which the value core reproduces), and
//! `text_to_cstring(PG_GETARG_TEXT_PP(i))` (the property-name `text`, arriving
//! on the by-ref lane as its detoasted `VARDATA_ANY` bytes). The `bool`-valued
//! cores return `PgResult<Option<bool>>` — `None` is C's `PG_RETURN_NULL()`,
//! mapped to `fcinfo.set_result_null(true)`. `pg_indexam_progress_phasename`
//! returns `PgResult<Option<String>>`; a `Some` is C's
//! `CStringGetTextDatum(name)`, written back as a `Varlena` payload (the
//! boundary re-wraps it with the varlena header).

use alloc::string::{String, ToString};

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

use types_core::Oid;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_OID(i)` → `DatumGetObjectId`: the low 32 bits of arg `i`'s word.
/// (Also reads a `regclass` arg, which is an `oid` at the fmgr boundary.)
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo.arg(i).expect("amutils fn: missing arg").value.as_oid()
}

/// `PG_GETARG_INT32(i)` → `DatumGetInt32`: arg `i`'s word as a signed `int4`.
#[inline]
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("amutils fn: missing arg").value.as_i32()
}

/// A `text` arg's detoasted `VARDATA_ANY` payload bytes on the by-ref lane,
/// decoded as a `&str` (C: `text_to_cstring(PG_GETARG_TEXT_PP(i))`).
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let bytes = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("amutils fn: text arg missing from by-ref lane");
    core::str::from_utf8(bytes).expect("amutils fn: text arg not valid UTF-8")
}

/// Write a `bool` result, or set the result NULL for `None` (C:
/// `PG_RETURN_NULL()`). Returns the result word (a dummy `0` when NULL).
#[inline]
fn ret_bool_opt(fcinfo: &mut FunctionCallInfoBaseData, v: Option<bool>) -> Datum {
    match v {
        Some(b) => Datum::from_bool(b),
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
}

/// Write a `text` result on the by-ref lane (C: `CStringGetTextDatum(name)` →
/// `PG_RETURN_DATUM`), or set the result NULL for `None` (C: `PG_RETURN_NULL()`).
/// The boundary re-wraps the payload with the varlena header. Returns the dummy
/// result word.
#[inline]
fn ret_text_opt(fcinfo: &mut FunctionCallInfoBaseData, v: Option<String>) -> Datum {
    match v {
        Some(s) => {
            fcinfo.set_ref_result(RefPayload::Varlena(s.into_bytes()));
            Datum::from_usize(0)
        }
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
}

/// A scratch context for the cores that allocate (the AM `amproperty` callback's
/// `propname` round-trip) through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("amutils fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(std::format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}

/// Unwrap a `PgResult`, re-raising its error through `raise`.
#[inline]
fn ok<T>(r: types_error::PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `pg_indexam_has_property(amoid oid, prop text)` (OID 636).
fn fc_pg_indexam_has_property(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let amoid = arg_oid(fcinfo, 0);
    let prop = arg_text(fcinfo, 1).to_string();
    let m = scratch_mcx();
    let res = ok(crate::pg_indexam_has_property(m.mcx(), amoid, &prop));
    ret_bool_opt(fcinfo, res)
}

/// `pg_index_has_property(index regclass, prop text)` (OID 637).
fn fc_pg_index_has_property(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let indexoid = arg_oid(fcinfo, 0);
    let prop = arg_text(fcinfo, 1).to_string();
    let m = scratch_mcx();
    let res = ok(crate::pg_index_has_property(m.mcx(), indexoid, &prop));
    ret_bool_opt(fcinfo, res)
}

/// `pg_index_column_has_property(index regclass, column int4, prop text)`
/// (OID 638).
fn fc_pg_index_column_has_property(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let indexoid = arg_oid(fcinfo, 0);
    let attno = arg_int32(fcinfo, 1);
    let prop = arg_text(fcinfo, 2).to_string();
    let m = scratch_mcx();
    let res = ok(crate::pg_index_column_has_property(m.mcx(), indexoid, attno, &prop));
    ret_bool_opt(fcinfo, res)
}

/// `pg_indexam_progress_phasename(amoid oid, phasenum int8)` (OID 676).
///
/// The C body reads the second arg with `PG_GETARG_INT32(1)`, which truncates
/// the `int8` datum to its low 32 bits before widening back to `int64` for the
/// `ambuildphasename(int64)` callback; the value core takes the full `int64`
/// word and reproduces that truncation internally.
fn fc_pg_indexam_progress_phasename(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let amoid = arg_oid(fcinfo, 0);
    let phasenum = fcinfo
        .arg(1)
        .expect("amutils fn: missing arg")
        .value
        .as_i64();
    let res = ok(crate::pg_indexam_progress_phasename(amoid, phasenum));
    ret_text_opt(fcinfo, res)
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
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict,
        retset,
        func: Some(func),
    }
}

/// Register every SQL-callable `amutils.c` builtin (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs / nargs / strict /
/// retset transcribed exactly from `pg_proc.dat` (all proisstrict, none
/// retset).
pub fn register_amutils_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(636, "pg_indexam_has_property", 2, true, false, fc_pg_indexam_has_property),
        builtin(637, "pg_index_has_property", 2, true, false, fc_pg_index_has_property),
        builtin(
            638,
            "pg_index_column_has_property",
            3,
            true,
            false,
            fc_pg_index_column_has_property,
        ),
        builtin(
            676,
            "pg_indexam_progress_phasename",
            2,
            true,
            false,
            fc_pg_indexam_progress_phasename,
        ),
    ]);
}
