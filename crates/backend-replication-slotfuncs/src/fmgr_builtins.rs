//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the `slotfuncs.c`
//! SQL-callable functions whose argument/result types are expressible at the
//! current fmgr boundary (the `void`-returning slot administration functions
//! that take only `name` arguments).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in [`crate`], and writes back the
//! result. [`register_slotfuncs_builtins`] registers every row into the
//! fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch (and the
//! `fmgr_isbuiltin` fast path) resolves them. OIDs / nargs / strict / retset are
//! transcribed exactly from `pg_proc.dat`.
//!
//! Only the two `void`-returning functions named below are registered here. The
//! remaining `slotfuncs.c` SQL functions return composite `(slot_name, lsn)`
//! rows or the 20-column SRF — shapes the scalar fmgr boundary does not carry,
//! so they are not registered.

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_NAME(i)` → `NameStr(*name)`: a `name` value's fixed
/// `NAMEDATALEN` buffer on the by-ref lane, trimmed at the first NUL (C passes
/// the whole `NameData` by pointer).
#[inline]
fn arg_name<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let bytes = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("slotfuncs fn: name arg missing from by-ref lane");
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    core::str::from_utf8(&bytes[..end]).expect("slotfuncs fn: name arg not valid UTF-8")
}

/// `PG_RETURN_VOID()`: the dummy `(Datum) 0` a `void`-returning function yields.
#[inline]
fn ret_void() -> Datum {
    Datum::from_usize(0)
}

/// A scratch context for the cores' `Mcx<'_>` argument. The cores return
/// `PgResult<()>` (no by-ref payload survives the call), so the arena is dropped
/// on return.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("slotfuncs fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `pg_drop_replication_slot(name)` (slotfuncs.c) — `void`.
fn fc_pg_drop_replication_slot(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let name = arg_name(fcinfo, 0).to_string();
    let m = scratch_mcx();
    match crate::pg_drop_replication_slot(m.mcx(), &name) {
        Ok(()) => ret_void(),
        Err(e) => raise(e),
    }
}

/// `pg_sync_replication_slots()` (slotfuncs.c) — `void`, no arguments.
fn fc_pg_sync_replication_slots(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    match crate::pg_sync_replication_slots(m.mcx()) {
        Ok(()) => ret_void(),
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

/// Register the two `void`-returning `slotfuncs.c` fmgr builtins (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
///
/// OIDs / nargs / strict / retset transcribed from `pg_proc.dat`: both default
/// to `proisstrict => 't'` (no `proisstrict => 'f'` line), neither is
/// `proretset`. `pg_drop_replication_slot` takes one `name` argument;
/// `pg_sync_replication_slots` takes none.
pub fn register_slotfuncs_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // pg_drop_replication_slot(name) -> void
        builtin(3780, "pg_drop_replication_slot", 1, true, false, fc_pg_drop_replication_slot),
        // pg_sync_replication_slots() -> void
        builtin(6344, "pg_sync_replication_slots", 0, true, false, fc_pg_sync_replication_slots),
    ]);
}
