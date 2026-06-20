//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! language-validator functions in `pg_proc.c` whose argument/result types are
//! expressible at the current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in `lib.rs`, and (since all three
//! return `void`) returns the dummy result word. [`register_pg_proc_builtins`]
//! registers every row into the fmgr-core builtin table (C: `fmgr_builtins[]`),
//! so by-OID dispatch resolves them. OIDs / nargs / strict / retset are
//! transcribed exactly from `pg_proc.dat` (all three: 1 arg `oid`, NOT strict —
//! no `proisstrict` — not retset, `prorettype => void`).
//!
//! C: each `fmgr_*_validator(PG_FUNCTION_ARGS)` reads the function-to-validate
//! OID from `PG_GETARG_OID(0)` and its own validator OID from
//! `fcinfo->flinfo->fn_oid` (the latter is forwarded to
//! `CheckFunctionValidatorAccess`).

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use types_core::primitive::Oid;
use types_error::PgResult;

// ---------------------------------------------------------------------------
// Argument readers / result writer.
// ---------------------------------------------------------------------------

/// `PG_GETARG_OID(i)` → `DatumGetObjectId`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo
        .arg(i)
        .expect("pg_proc validator: missing arg")
        .value
        .as_oid()
}

/// `fcinfo->flinfo->fn_oid`: the validator function's own OID. C dereferences
/// `flinfo` unconditionally here (the executor always populates it for a
/// builtin call); a `None` is C's NULL deref.
#[inline]
fn validator_fn_oid(fcinfo: &FunctionCallInfoBaseData) -> Oid {
    fcinfo
        .flinfo
        .as_ref()
        .expect("pg_proc validator: flinfo (fcinfo->flinfo) is NULL")
        .fn_oid
}

/// `PG_RETURN_VOID()`: the validators return `void`; emit the dummy result word.
#[inline]
fn ret_void() -> Datum {
    Datum::from_usize(0)
}

#[inline]
fn finish(r: PgResult<()>) -> PgResult<Datum> {
    r.map(|()| ret_void())
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

fn fc_fmgr_internal_validator(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let funcoid = arg_oid(fcinfo, 0);
    let valoid = validator_fn_oid(fcinfo);
    finish(crate::fmgr_internal_validator(valoid, funcoid))
}

fn fc_fmgr_c_validator(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let funcoid = arg_oid(fcinfo, 0);
    let valoid = validator_fn_oid(fcinfo);
    finish(crate::fmgr_c_validator(valoid, funcoid))
}

fn fc_fmgr_sql_validator(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let funcoid = arg_oid(fcinfo, 0);
    let valoid = validator_fn_oid(fcinfo);
    finish(crate::fmgr_sql_validator(valoid, funcoid))
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

/// Register the `pg_proc.c` language-validator builtins (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs/nargs/strict/retset transcribed from `pg_proc.dat` (none carry
/// `proisstrict`, none `proretset`; all take a single `oid` argument).
pub fn register_pg_proc_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        builtin(2246, "fmgr_internal_validator", 1, true, false, fc_fmgr_internal_validator),
        builtin(2247, "fmgr_c_validator", 1, true, false, fc_fmgr_c_validator),
        builtin(2248, "fmgr_sql_validator", 1, true, false, fc_fmgr_sql_validator),
    ]);
}
