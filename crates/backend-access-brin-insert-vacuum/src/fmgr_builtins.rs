//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! BRIN range-maintenance functions ported in this crate
//! (`brin_summarize_new_values`, `brin_summarize_range`,
//! `brin_desummarize_range`).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame (all by-value scalars here: `regclass`→`oid` and `int8`), calls
//! the matching value core, and writes back the result word. The cores allocate
//! their working state through a scratch [`mcx::MemoryContext`] (the pattern of
//! `backend-utils-adt-dbsize`'s fmgr layer). [`register_brin_insert_vacuum_builtins`]
//! registers every row into the fmgr-core builtin table (C: `fmgr_builtins[]`),
//! so by-OID dispatch resolves them. OIDs / nargs / strict / retset are
//! transcribed exactly from `pg_proc.dat`.

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use types_core::primitive::Oid;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_OID(i)` (C `regclass` is an `oid`): the low 32 bits of arg `i`.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo.arg(i).expect("brin fn: missing arg").value.as_oid()
}

/// `PG_GETARG_INT64(i)`: the full 64-bit word of arg `i`.
#[inline]
fn arg_int64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("brin fn: missing arg").value.as_i64()
}

/// `PG_RETURN_INT32(v)`.
#[inline]
fn ret_int32(v: i32) -> Datum {
    Datum::from_i32(v)
}

/// `PG_RETURN_VOID()` — C returns `(Datum) 0`.
#[inline]
fn ret_void() -> Datum {
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their working state through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("brin insert/vacuum fmgr scratch")
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `brin_summarize_new_values(regclass)` (OID 3952) → int4.
fn fc_brin_summarize_new_values(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> backend_utils_error::PgResult<Datum> {
    let relation = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let n = crate::brin_summarize_new_values(m.mcx(), relation)?;
    Ok(ret_int32(n))
}

/// `brin_summarize_range(regclass, int8)` (OID 3999) → int4.
fn fc_brin_summarize_range(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> backend_utils_error::PgResult<Datum> {
    let indexoid = arg_oid(fcinfo, 0);
    let heap_blk64 = arg_int64(fcinfo, 1);
    let m = scratch_mcx();
    let n = crate::brin_summarize_range(m.mcx(), indexoid, heap_blk64)?;
    Ok(ret_int32(n))
}

/// `brin_desummarize_range(regclass, int8)` (OID 4014) → void.
fn fc_brin_desummarize_range(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> backend_utils_error::PgResult<Datum> {
    let indexoid = arg_oid(fcinfo, 0);
    let heap_blk64 = arg_int64(fcinfo, 1);
    let m = scratch_mcx();
    crate::brin_desummarize_range(m.mcx(), indexoid, heap_blk64)?;
    Ok(ret_void())
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

/// Register this crate's SQL-callable BRIN range-maintenance builtins (C: their
/// `fmgr_builtins[]` rows). Called from this crate's [`crate::init_seams`].
/// OIDs / nargs / strict / retset transcribed exactly from `pg_proc.dat`
/// (all `provolatile => 'v'`, all strict, none retset).
pub fn register_brin_insert_vacuum_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        builtin(3952, "brin_summarize_new_values", 1, true, false, fc_brin_summarize_new_values),
        builtin(3999, "brin_summarize_range", 2, true, false, fc_brin_summarize_range),
        builtin(4014, "brin_desummarize_range", 2, true, false, fc_brin_desummarize_range),
    ]);
}
