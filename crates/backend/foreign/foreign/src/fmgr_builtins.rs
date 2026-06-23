//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `foreign.c` functions whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that creates a scratch
//! `CurrentMemoryContext`, runs the matching `(mcx, fcinfo)` worker in the
//! crate root, and lowers the result back to the fmgr `Datum` word.
//! [`register_foreign_builtins`] registers every row into the fmgr-core builtin
//! table (C: `fmgr_builtins[]`), so by-OID dispatch resolves them — in
//! particular `OidFunctionCall2(fdwvalidator, ...)` from
//! `transformGenericOptions`. OIDs / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat`.

use mcx::MemoryContext;
use types_core::primitive::Oid;
use datum::Datum;
use types_error::PgResult;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

/// A scratch `CurrentMemoryContext` for the workers that allocate through `Mcx`.
fn scratch_mcx() -> MemoryContext {
    MemoryContext::new("foreign fmgr scratch")
}

/// `PG_GETARG_OID(i)`: the low 32 bits of arg `i`'s word as an Oid.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo.arg(i).expect("foreign fn: missing arg").value.as_oid()
}

/// `PG_GETARG_*ARRAYTYPE_P(i)`: the header-ful array varlena image off the
/// by-reference lane.
#[inline]
fn arg_array_image<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("foreign fn: by-ref array arg missing from by-ref lane")
}

/// `postgresql_fdw_validator(text[], oid) -> bool` (oid 2316). The worker reads
/// its `text[]` arg off the by-reference lane (`untransformRelOptions(
/// PG_GETARG_DATUM(0))`) and the catalog OID by value; it returns the boolean
/// the SQL function `PG_RETURN_BOOL`s.
fn fc_postgresql_fdw_validator(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let mcx = m.mcx();
    // List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    let array = arg_array_image(fcinfo, 0);
    let options_list =
        common_reloptions::untransformRelOptions(mcx, Some(array))?;
    // Oid catalog = PG_GETARG_OID(1);
    let catalog = arg_oid(fcinfo, 1);
    let result = crate::postgresql_fdw_validator_core(mcx, &options_list, catalog)?;
    Ok(Datum::from_bool(result))
}

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    func: PgFnNative,
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
        func,
    )
}

/// Register the `foreign.c` fmgr builtins into the fmgr-core registry.
pub fn register_foreign_builtins() {
    fmgr_core::register_builtins_native([builtin(
        2316,
        "postgresql_fdw_validator",
        2,
        true,
        false,
        fc_postgresql_fdw_validator,
    )]);
}
