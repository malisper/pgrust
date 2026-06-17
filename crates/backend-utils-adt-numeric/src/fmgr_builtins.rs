//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `numeric.c` functions whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_numeric_builtins`] registers every row into
//! the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch
//! resolves them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_INT32(i)`: the low 32 bits of arg `i`'s word, sign-extended.
#[inline]
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("numeric fn: missing arg").value.as_i32()
}

/// Set a `cstring` (`_out`) result on the by-ref lane and return the dummy word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("numeric fmgr scratch")
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

/// `numerictypmodout(int4) -> cstring`: the typmod output function, producing
/// "(prec,scale)" or "". The core allocates a NUL-terminated cstring byte
/// buffer through `Mcx`; we strip the trailing NUL and decode to a `String`
/// for the by-ref `cstring` lane.
fn fc_numerictypmodout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let typmod = arg_int32(fcinfo, 0);
    let m = scratch_mcx();
    let s = match crate::ops_sql::numerictypmodout(m.mcx(), typmod) {
        Ok(bytes) => {
            // Drop the trailing NUL terminator produced by PG_RETURN_CSTRING.
            let raw = bytes.as_slice();
            let body = raw.strip_suffix(&[0u8]).unwrap_or(raw);
            String::from_utf8_lossy(body).into_owned()
        }
        Err(e) => raise(e),
    };
    ret_cstring(fcinfo, s)
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

/// Register every expressible scalar `numeric.c` builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs/nargs/strict/retset transcribed exactly from `pg_proc.dat`.
pub fn register_numeric_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // numerictypmodout: oid 2918, 1 arg (int4), proisstrict default 't',
        // not retset, prorettype cstring.
        builtin(2918, "numerictypmodout", 1, true, false, fc_numerictypmodout),
    ]);
}
