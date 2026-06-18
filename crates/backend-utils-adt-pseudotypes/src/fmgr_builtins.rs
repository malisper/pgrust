//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the `cstring`
//! pseudo-type's working I/O functions, whose argument/result types are
//! expressible at the current fmgr boundary.
//!
//! `cstring` is marked a pseudo-type only so people don't use it in tables, but
//! it carries a full working set of I/O functions (pseudotypes.c:100-141). Its
//! arg (`cstring`) and results (`cstring` / `bytea`) all ride the by-ref lane.
//! Each entry is a `fc_<name>` adapter that reads its argument off the fmgr call
//! frame, calls the matching value core, and writes back the by-reference
//! payload. [`register_pseudotypes_builtins`] registers every row into the
//! fmgr-core builtin table (C: `fmgr_builtins[]`). OIDs / nargs / strict /
//! retset are transcribed exactly from `pg_proc.dat`.
//!
//! `cstring_in` / `cstring_out` / `cstring_send` are registered here, along with
//! the `void` working I/O (`void_in` / `void_out` / `void_send`) and the `shell`
//! type dummies (`shell_in` / `shell_out`) — all of whose arg/result types are
//! expressible at the current fmgr boundary (`cstring` on the by-ref lane, the
//! 0-width by-value `void`, the `bytea` send result on the by-ref lane).
//!
//! The remaining pseudo-type I/O functions are either the `ereport(ERROR)`
//! dummies (no SQL-callable value), or `recv`/delegating outputs over `Datum`
//! arms (array / enum / range / multirange) whose arg/result types are not
//! expressible at the current fmgr boundary.

extern crate std;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_CSTRING(i)`: the input text on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("pseudotypes fn: cstring arg missing from by-ref lane")
}

/// Set a `cstring` result on the by-ref lane and return the dummy word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// Set a `bytea` (`_send`) result on the by-ref lane and return the dummy word.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("pseudotypes fmgr scratch")
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

/// `cstring_in` (pseudotypes.c:101): `PG_RETURN_CSTRING(pstrdup(str))`.
fn fc_cstring_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let s = arg_cstring(fcinfo, 0);
    let owned = match crate::cstring_in(m.mcx(), s) {
        Ok(out) => out.as_str().to_string(),
        Err(e) => raise(e),
    };
    ret_cstring(fcinfo, owned)
}

/// `cstring_out` (pseudotypes.c:110): `PG_RETURN_CSTRING(pstrdup(str))`.
fn fc_cstring_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let s = arg_cstring(fcinfo, 0);
    let owned = match crate::cstring_out(m.mcx(), s) {
        Ok(out) => out.as_str().to_string(),
        Err(e) => raise(e),
    };
    ret_cstring(fcinfo, owned)
}

/// `cstring_send` (pseudotypes.c:130): `PG_RETURN_BYTEA_P(pq_endtypsend(&buf))`.
fn fc_cstring_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let s = arg_cstring(fcinfo, 0);
    let bytes = match crate::cstring_send(m.mcx(), s) {
        Ok(bytea) => bytea.as_bytes().to_vec(),
        Err(e) => raise(e),
    };
    ret_varlena(fcinfo, bytes)
}

/// `void_in` (pseudotypes.c:263): `PG_RETURN_VOID()`. Accepts any cstring and
/// returns the 0-width by-value `void` word.
fn fc_void_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let _s = arg_cstring(fcinfo, 0);
    match crate::void_in(_s) {
        Ok(d) => d,
        Err(e) => raise(e),
    }
}

/// `void_out` (pseudotypes.c:269): `PG_RETURN_CSTRING(pstrdup(""))`. The `void`
/// argument is a 0-width by-value word that carries no payload.
fn fc_void_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let owned = match crate::void_out(m.mcx()) {
        Ok(out) => out.as_str().to_string(),
        Err(e) => raise(e),
    };
    ret_cstring(fcinfo, owned)
}

/// `void_send` (pseudotypes.c:285): send an empty string,
/// `PG_RETURN_BYTEA_P(pq_endtypsend(&buf))`.
fn fc_void_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let bytes = match crate::void_send(m.mcx()) {
        Ok(bytea) => bytea.as_bytes().to_vec(),
        Err(e) => raise(e),
    };
    ret_varlena(fcinfo, bytes)
}

/// `shell_in` (pseudotypes.c:303): `errmsg("cannot accept a value of a shell
/// type")` — always raises.
fn fc_shell_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    match crate::shell_in(s) {
        Ok(d) => d,
        Err(e) => raise(e),
    }
}

/// `shell_out` (pseudotypes.c:313): `errmsg("cannot display a value of a shell
/// type")` — always raises. Its `opaque` argument is an unread by-value word.
fn fc_shell_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let value = fcinfo.arg(0).map(|a| a.value).unwrap_or_else(Datum::null);
    let owned = match crate::shell_out(value) {
        Ok(out) => out.as_str().to_string(),
        Err(e) => raise(e),
    };
    ret_cstring(fcinfo, owned)
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

/// Register the `cstring` pseudo-type's working I/O builtins (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`. OIDs /
/// nargs from `pg_proc.dat`; all default `proisstrict => 't'`, none retset.
pub fn register_pseudotypes_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(2292, "cstring_in", 1, true, false, fc_cstring_in),
        builtin(2293, "cstring_out", 1, true, false, fc_cstring_out),
        builtin(2501, "cstring_send", 1, true, false, fc_cstring_send),
        // ---- void working I/O ----
        builtin(2298, "void_in", 1, true, false, fc_void_in),
        builtin(2299, "void_out", 1, true, false, fc_void_out),
        builtin(3121, "void_send", 1, true, false, fc_void_send),
        // ---- shell type dummies ----
        builtin(2398, "shell_in", 1, false, false, fc_shell_in),
        builtin(2399, "shell_out", 1, true, false, fc_shell_out),
    ]);
}
