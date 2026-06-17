//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the `varbit.c`
//! functions whose argument/result types are expressible at the current fmgr
//! boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_varbit_builtins`] registers every row into
//! the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch (and
//! the `fmgr_isbuiltin` fast path) resolves them. OIDs / nargs / strict / retset
//! are transcribed exactly from `pg_proc.dat`.
//!
//! Only the typmod-out pair (`bittypmodout`/`varbittypmodout`) is registered
//! here: their sole argument is the scalar `int4` typmod (by-value word) and the
//! result is a `cstring` (the by-ref lane). The `bit`/`varbit` I/O, comparison,
//! and bitwise families operate over the `varbit`/`bit` varlena carrier, whose
//! detoasted `{ bit_len, data }` shape is not yet produced at this fmgr boundary,
//! and so are NOT registered here.

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_INT32(i)`: the low 32 bits of arg `i`'s word, as a signed int4.
#[inline]
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("varbit fn: missing arg").value.as_i32()
}

/// Set a `cstring` (`_out`) result on the by-ref lane and return the dummy word.
/// The core returns a NUL-terminated `cstring` image (`PgVec<u8>`); the by-ref
/// `Cstring` lane carries owned text without the trailing NUL.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, bytes: &[u8]) -> Datum {
    // Drop the trailing NUL the C `cstring` image carries.
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    let s = String::from_utf8_lossy(&bytes[..end]).into_owned();
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("varbit fmgr scratch")
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

fn fc_bittypmodout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let typmod = arg_int32(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = match crate::bittypmodout(m.mcx(), typmod) {
        Ok(v) => v.to_vec(),
        Err(e) => raise(e),
    };
    ret_cstring(fcinfo, &bytes)
}

fn fc_varbittypmodout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let typmod = arg_int32(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = match crate::varbittypmodout(m.mcx(), typmod) {
        Ok(v) => v.to_vec(),
        Err(e) => raise(e),
    };
    ret_cstring(fcinfo, &bytes)
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

/// Register the expressible `varbit.c` fmgr builtins (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs/nargs/strict/retset
/// transcribed exactly from `pg_proc.dat` (both `proisstrict => 't'`, 1 int4
/// arg, cstring result, none retset).
pub fn register_varbit_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(2920, "bittypmodout", 1, true, false, fc_bittypmodout),
        builtin(2921, "varbittypmodout", 1, true, false, fc_varbittypmodout),
    ]);
}
