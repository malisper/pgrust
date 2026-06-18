//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the `xlogfuncs.c`
//! recovery-control SQL-callable functions whose argument/result types are
//! expressible at the current fmgr boundary (`bool`, `int4`, `text`).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in [`crate`], and writes back the
//! result word / by-reference payload. [`register_xlogfuncs_builtins`] registers
//! every row into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID
//! dispatch (and the `fmgr_isbuiltin` fast path) resolves them. OIDs / nargs /
//! strict / retset are transcribed exactly from `pg_proc.dat`.
//!
//! Only the six recovery-control functions named below are registered here
//! (`pg_wal_replay_pause`/`pg_wal_replay_resume` return `void`,
//! `pg_is_wal_replay_paused`/`pg_is_in_recovery`/`pg_promote` return `bool`,
//! `pg_get_wal_replay_pause_state` returns `text`). The remaining
//! `xlogfuncs.c` SQL functions are NOT registered: the WAL-control /
//! file-name / backup / LSN-diff functions return `pg_lsn`, `numeric`, or
//! composite rows, and several take `pg_lsn` / `bool` defaults whose argument or
//! result types are not yet expressible (or whose cores require an `Mcx`-built
//! varlena/composite the fmgr boundary cannot yet carry for those shapes).

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_BOOL(i)` → `DatumGetBool`: any nonzero word reads back as `true`.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo
        .arg(i)
        .expect("xlogfuncs fn: missing arg")
        .value
        .as_bool()
}

/// `PG_GETARG_INT32(i)` → `DatumGetInt32`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo
        .arg(i)
        .expect("xlogfuncs fn: missing arg")
        .value
        .as_i32()
}

#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

/// Set a `text` (`PG_RETURN_TEXT_P`) result on the by-ref lane and return the
/// dummy word. `bytes` is the full varlena image produced by `cstring_to_text`.
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`. The
/// returned varlena bytes are copied out onto the by-ref lane before this
/// context is dropped, so the result outlives the scratch arena.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("xlogfuncs fmgr scratch")
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

/// `pg_is_wal_replay_paused()` (xlogfuncs.c:572).
fn fc_pg_is_wal_replay_paused(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::pg_is_wal_replay_paused() {
        Ok(b) => ret_bool(b),
        Err(e) => raise(e),
    }
}

/// `pg_get_wal_replay_pause_state()` (xlogfuncs.c:593) — a `text` result.
fn fc_pg_get_wal_replay_pause_state(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    // Copy the varlena image out of the scratch arena onto the by-ref lane
    // (C: PG_RETURN_TEXT_P over a palloc'd varlena) before the arena is dropped.
    let bytes: Vec<u8> = match crate::pg_get_wal_replay_pause_state(m.mcx()) {
        Ok(text) => text.as_slice().to_vec(),
        Err(e) => raise(e),
    };
    ret_text(fcinfo, bytes)
}

/// `pg_wal_replay_pause()` (xlogfuncs.c:518) — `void` result
/// (C: `PG_RETURN_VOID()` = Datum 0).
fn fc_pg_wal_replay_pause(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::pg_wal_replay_pause() {
        Ok(()) => Datum::from_usize(0),
        Err(e) => raise(e),
    }
}

/// `pg_wal_replay_resume()` (xlogfuncs.c:548) — `void` result
/// (C: `PG_RETURN_VOID()` = Datum 0).
fn fc_pg_wal_replay_resume(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::pg_wal_replay_resume() {
        Ok(()) => Datum::from_usize(0),
        Err(e) => raise(e),
    }
}

/// `pg_is_in_recovery()` (xlogfuncs.c:643) — infallible `bool`.
fn fc_pg_is_in_recovery(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::pg_is_in_recovery())
}

/// `pg_promote(wait bool, wait_seconds int4)` (xlogfuncs.c:670).
fn fc_pg_promote(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let wait = arg_bool(fcinfo, 0);
    let wait_seconds = arg_int32(fcinfo, 1);
    match crate::pg_promote(wait, wait_seconds) {
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

/// Register the six expressible `xlogfuncs.c` recovery-control fmgr builtins
/// (C: their `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
///
/// OIDs / nargs / strict / retset transcribed from the generated
/// `fmgrtab.c` (Gen_fmgrtab over `pg_proc.dat`): all six are `provolatile => 'v'`
/// and inherit `proisstrict BKI_DEFAULT(t)` (none overrides it, so `strict =
/// true`), and none is `proretset` (so `retset = false`).
pub fn register_xlogfuncs_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // pg_wal_replay_pause() -> void
        builtin(3071, "pg_wal_replay_pause", 0, true, false, fc_pg_wal_replay_pause),
        // pg_wal_replay_resume() -> void
        builtin(3072, "pg_wal_replay_resume", 0, true, false, fc_pg_wal_replay_resume),
        // pg_is_wal_replay_paused() -> bool
        builtin(3073, "pg_is_wal_replay_paused", 0, true, false, fc_pg_is_wal_replay_paused),
        // pg_get_wal_replay_pause_state() -> text
        builtin(
            1137,
            "pg_get_wal_replay_pause_state",
            0,
            true,
            false,
            fc_pg_get_wal_replay_pause_state,
        ),
        // pg_is_in_recovery() -> bool
        builtin(3810, "pg_is_in_recovery", 0, true, false, fc_pg_is_in_recovery),
        // pg_promote(bool, int4) -> bool
        builtin(3436, "pg_promote", 2, true, false, fc_pg_promote),
    ]);
}
