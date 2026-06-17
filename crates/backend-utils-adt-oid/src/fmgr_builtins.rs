//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for every SQL-callable
//! function in `oid.c` whose argument/result types are expressible at the
//! current fmgr boundary (the scalar `oid` I/O and comparison operators).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_oid_builtins`] registers every row into the
//! fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch (and the
//! `fmgr_isbuiltin` fast path that early catalog scankeys rely on) resolves
//! them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.
//!
//! The `oidvector` family is NOT registered here (see the crate docs): it needs
//! the array `oidvector` carrier and `array_recv`/`array_send`.

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

use types_core::Oid;
use types_stringinfo::StringInfo;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_OID(i)` → `DatumGetObjectId`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo.arg(i).expect("oid fn: missing arg").value.as_oid()
}

/// `PG_GETARG_CSTRING(i)`: the input text on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("oid fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_POINTER(i)` for a `StringInfo` (the `oidrecv` wire buffer): the
/// raw message bytes on the by-ref lane.
#[inline]
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("oid fn: by-ref arg missing from by-ref lane")
}

#[inline]
fn ret_oid(v: Oid) -> Datum {
    Datum::from_oid(v)
}
#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

/// Set a `cstring` (`_out`) result on the by-ref lane and return the dummy word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}
/// Set a `bytea` (`_send`) result on the by-ref lane.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("oid fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

fn fc_oidin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    // C: uint32in_subr(s, NULL, "oid", fcinfo->context). fcinfo->context carries
    // the soft ErrorSaveContext; at this boundary a hard parse is used (a soft
    // context is not modeled on the fmgr frame), matching every other adt _in.
    match crate::oidin(s, None) {
        Ok(o) => ret_oid(o),
        Err(e) => raise(e),
    }
}

fn fc_oidout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let o = arg_oid(fcinfo, 0);
    ret_cstring(fcinfo, crate::oidout(o))
}

fn fc_oidrecv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let src = arg_varlena(fcinfo, 0);
    let mut data = mcx::PgVec::new_in(m.mcx());
    if data.try_reserve(src.len()).is_err() {
        raise(types_error::PgError::error("out of memory"));
    }
    data.extend_from_slice(src);
    let mut buf = StringInfo::from_vec(data);
    match crate::oidrecv(&mut buf) {
        Ok(o) => ret_oid(o),
        Err(e) => raise(e),
    }
}

fn fc_oidsend(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = match crate::oidsend(m.mcx(), arg1) {
        Ok(bytea) => bytea.as_bytes().to_vec(),
        Err(e) => raise(e),
    };
    ret_varlena(fcinfo, bytes)
}

fn fc_oideq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::oideq(arg_oid(fcinfo, 0), arg_oid(fcinfo, 1)))
}
fn fc_oidne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::oidne(arg_oid(fcinfo, 0), arg_oid(fcinfo, 1)))
}
fn fc_oidlt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::oidlt(arg_oid(fcinfo, 0), arg_oid(fcinfo, 1)))
}
fn fc_oidle(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::oidle(arg_oid(fcinfo, 0), arg_oid(fcinfo, 1)))
}
fn fc_oidgt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::oidgt(arg_oid(fcinfo, 0), arg_oid(fcinfo, 1)))
}
fn fc_oidge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::oidge(arg_oid(fcinfo, 0), arg_oid(fcinfo, 1)))
}
fn fc_oidlarger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_oid(crate::oidlarger(arg_oid(fcinfo, 0), arg_oid(fcinfo, 1)))
}
fn fc_oidsmaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_oid(crate::oidsmaller(arg_oid(fcinfo, 0), arg_oid(fcinfo, 1)))
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

/// Register every scalar `oid.c` builtin (C: their `fmgr_builtins[]` rows).
/// Called from this crate's `init_seams()`. OIDs/nargs/strict from
/// `pg_proc.dat` (all are `proisstrict => 't'`, none retset).
pub fn register_oid_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // ---- I/O ----
        builtin(1798, "oidin", 1, true, false, fc_oidin),
        builtin(1799, "oidout", 1, true, false, fc_oidout),
        builtin(2418, "oidrecv", 1, true, false, fc_oidrecv),
        builtin(2419, "oidsend", 1, true, false, fc_oidsend),
        // ---- comparison operators ----
        builtin(184, "oideq", 2, true, false, fc_oideq),
        builtin(185, "oidne", 2, true, false, fc_oidne),
        builtin(716, "oidlt", 2, true, false, fc_oidlt),
        builtin(717, "oidle", 2, true, false, fc_oidle),
        builtin(1638, "oidgt", 2, true, false, fc_oidgt),
        builtin(1639, "oidge", 2, true, false, fc_oidge),
        builtin(1965, "oidlarger", 2, true, false, fc_oidlarger),
        builtin(1966, "oidsmaller", 2, true, false, fc_oidsmaller),
    ]);
}
