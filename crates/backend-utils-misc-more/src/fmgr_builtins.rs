//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions in `rls.c`: the two `row_security_active` overloads.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in [`crate::rls`], and writes back
//! the result word. [`register_backend_utils_misc_more_builtins`] registers
//! every row into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID
//! dispatch resolves them. OIDs / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat`.

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

use types_core::Oid;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_OID(i)` → `DatumGetObjectId`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo
        .arg(i)
        .expect("rls fn: missing arg")
        .value
        .as_oid()
}

/// `PG_GETARG_TEXT_PP(i)` → `text_to_cstring`: a `text` arg's detoasted
/// `VARDATA_ANY` payload bytes on the by-ref lane, decoded as UTF-8. C builds a
/// NUL-terminated `char *` from the text body; the by-ref lane already delivers
/// the detoasted payload, so the body bytes are decoded directly.
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let bytes = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("rls fn: text arg missing from by-ref lane");
    core::str::from_utf8(bytes).expect("rls fn: text arg not valid UTF-8")
}

#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}

/// A scratch context for the cores' transient allocations (C charges them to
/// `CurrentMemoryContext`). The `Mcx`-free established pattern: a per-call
/// working context.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("rls fmgr scratch")
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `row_security_active(oid)` (pg_proc oid 3298).
fn fc_row_security_active(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let table_oid = arg_oid(fcinfo, 0);
    let ctx = scratch_mcx();
    match crate::rls::row_security_active(ctx.mcx(), table_oid) {
        Ok(b) => ret_bool(b),
        Err(e) => raise(e),
    }
}

/// `row_security_active_name(text)` (pg_proc oid 3299).
fn fc_row_security_active_name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let name = arg_text(fcinfo, 0);
    let ctx = scratch_mcx();
    match crate::rls::row_security_active_name(ctx.mcx(), name) {
        Ok(b) => ret_bool(b),
        Err(e) => raise(e),
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

/// Register every `rls.c` builtin (C: their `fmgr_builtins[]` rows). Called from
/// this crate's `init_seams()`. OIDs/nargs/retset from `pg_proc.dat`; both are
/// strict (no `proisstrict => 'f'`), nargs = 1, not retset.
pub fn register_backend_utils_misc_more_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(3298, "row_security_active", 1, true, false, fc_row_security_active),
        builtin(
            3299,
            "row_security_active_name",
            1,
            true,
            false,
            fc_row_security_active_name,
        ),
    ]);
}
