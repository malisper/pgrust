//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! GIN pending-list maintenance function `gin_clean_pending_list(regclass)`
//! (`ginfast.c:1031`).
//!
//! `gin_clean_pending_list` is a real `fmgr_builtins[]` row (OID 3789, prosrc
//! `gin_clean_pending_list`) whose body opens the named GIN index, derives a
//! `GinState`, and force-flushes the fast-update pending list through the landed
//! [`crate::ginInsertCleanup`] core, returning the number of pages deleted as an
//! `int8`. Without its row registered, `fmgr_info` → `fmgr_lookupByName` errors
//! `internal function "gin_clean_pending_list" is not in internal lookup table`.
//!
//! [`register_gin_clean_pending_list_builtin`] registers the row into the
//! fmgr-core builtin table (C: `fmgr_builtins[]`); it is called from this
//! crate's [`crate::init_seams`]. OID / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat` (`provolatile => 'v'`, strict, not retset).

use datum::Datum;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

/// `gin_clean_pending_list(regclass)` (OID 3789) → int8.
fn fc_gin_clean_pending_list(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let indexoid = fcinfo
        .arg(0)
        .expect("gin_clean_pending_list: missing arg")
        .value
        .as_oid();
    let m = mcx::MemoryContext::new("gin_clean_pending_list fmgr scratch");
    let pages_deleted = crate::gin_clean_pending_list(m.mcx(), indexoid)?;
    // PG_RETURN_INT64((int64) stats.pages_deleted).
    Ok(Datum::from_i64(pages_deleted as i64))
}

fn builtin(foid: u32, name: &str, nargs: i16, native: PgFnNative) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            // pg_proc.dat: proisstrict => 't', not proretset.
            strict: true,
            retset: false,
            func: None,
        },
        native,
    )
}

/// Register the `fmgr_builtins[]` row for `gin_clean_pending_list` (C: its
/// `fmgr_builtins[]` row). Called from this crate's [`crate::init_seams`]. OID /
/// nargs from `pg_proc.dat`.
pub fn register_gin_clean_pending_list_builtin() {
    fmgr_core::register_builtins_native([builtin(
        3789,
        "gin_clean_pending_list",
        1,
        fc_gin_clean_pending_list,
    )]);
}
