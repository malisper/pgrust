//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `jsonb_gin.c` support functions whose argument/result types are expressible
//! at the current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word.
//! [`register_jsonb_gin_builtins`] registers every row into the fmgr-core
//! builtin table (C: `fmgr_builtins[]`). OIDs / nargs / strict / retset are
//! transcribed exactly from `pg_proc.dat`.
//!
//! Scope: only `gin_compare_jsonb` (oid 3480) is registered. The eight other
//! jsonb GIN support procs (`gin_extract_jsonb`/`gin_extract_jsonb_query`/
//! `gin_consistent_jsonb`/`gin_triconsistent_jsonb` and the `_path` family) take
//! the GIN dispatch out-parameter pointers (`int32 *nentries`, `bool *recheck`,
//! `Datum **extra_data`, `Pointer **extra_data`, the `bool[]`/`GinTernaryValue[]`
//! check vectors) which are not expressible on the scalar/by-ref fmgr call frame;
//! they reach their value cores through [`backend_utils_adt_jsonb_gin_seams`]
//! (the GIN by-OID support-proc dispatcher), not the fmgr builtin table.

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// A `text` arg's by-ref payload bytes. C: `PG_GETARG_TEXT_PP(i)` then
/// `VARDATA_ANY` — the boundary delivers the detoasted varlena payload (header
/// stripped) on the by-ref lane, which is exactly what `gin_compare_jsonb`
/// compares.
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("gin_compare_jsonb: text arg missing from by-ref lane")
}

#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// C: `gin_compare_jsonb(PG_FUNCTION_ARGS)`. Two `text` GIN keys → `int32`
/// comparison result (always under the C collation, i.e. a plain unsigned byte
/// compare).
fn fc_gin_compare_jsonb(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_text(fcinfo, 0);
    let b = arg_text(fcinfo, 1);
    ret_i32(crate::gin_compare_jsonb(a, b))
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
        name: alloc::string::ToString::to_string(name),
        nargs,
        strict,
        retset,
        func: Some(func),
    }
}

/// Register the scalar `jsonb_gin.c` builtins (C: their `fmgr_builtins[]` rows).
/// Called from this crate's [`crate::init_seams`]. OIDs / nargs / strict / retset
/// transcribed from `pg_proc.dat` (`gin_compare_jsonb`: `proargtypes => 'text
/// text'`, `prorettype => 'int4'`, `proisstrict => 't'`, not retset).
pub fn register_jsonb_gin_builtins() {
    backend_utils_fmgr_core::register_builtins([builtin(
        3480,
        "gin_compare_jsonb",
        2,
        true,
        false,
        fc_gin_compare_jsonb,
    )]);
}
