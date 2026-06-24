#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `src/test/modules/injection_points/injection_points.c` — the test extension
//! that drives the injection-point infrastructure from SQL.
//!
//! Ported as a Rust **builtin library** (mirroring `contrib/pg_prewarm` and
//! `src/test/regress/regress.c`): the SQL emitted by `injection_points--1.0.sql`
//! (`CREATE FUNCTION ... LANGUAGE C AS 'MODULE_PATHNAME','injection_points_*'`)
//! resolves through the dynamic-loader unit's ported-library registry rather
//! than the OS loader (the Rust backend exposes no C ABI). The wait/notice/error
//! callbacks are registered into the core `injection_point` crate's builtin
//! callback registry (the analogue of `injection_points.so` exporting those
//! symbols), and resolved by `InjectionPointRun` when a point fires.
//!
//! The shared wait state (`InjectionPointSharedState`: wait counters + a
//! condition variable) is co-located in the core injection-point shmem region
//! (already wired into ipci.c), so this module needs no `shmem_startup_hook` /
//! DSM of its own — pgrust does not run extension shmem hooks for a
//! non-preloaded extension.
//!
//! Statistics (`injection_stats.c`'s custom pgstat kind) are not ported; the
//! three `injection_points_stats_*` SQL functions are registered so
//! `CREATE FUNCTION`'s C-symbol validator succeeds, but a call raises a
//! documented "not supported" error. None of the target recovery/auth tests nor
//! `injection_points.sql` exercise them.

use core::cell::RefCell;

use ::datum::Datum;
use ::fmgr::{FunctionCallInfoBaseData, LoadedExternalFunc, PGFunction};
use ::types_error::{PgError, PgResult};

/// The simple (suffix-free, directory-free) library name — `$libdir/injection_points`.
const LIBRARY: &str = "injection_points";

/// Raise a structured `ereport(ERROR)` through the `PGFunction` dispatch point
/// (`invoke_pgfunction`'s `catch_unwind`), mirroring `pg_prewarm`/`regress`.
fn raise(err: PgError) -> ! {
    std::panic::panic_any(err);
}

/// Unwrap a `PgResult` in a `PGFunction` body, raising on `Err`.
fn ok<T>(r: PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}

// ===========================================================================
// InjectionPointCondition (private_data carried per attached point)
// ===========================================================================

const INJ_CONDITION_ALWAYS: u8 = 0;
const INJ_CONDITION_PID: u8 = 1;

/// Encode an `InjectionPointCondition { type, pid }` into the opaque
/// `private_data` blob: `[type: u8][pid: i32 little-endian]`.
fn encode_condition(cond_type: u8, pid: i32) -> Vec<u8> {
    let mut v = Vec::with_capacity(5);
    v.push(cond_type);
    v.extend_from_slice(&pid.to_le_bytes());
    v
}

/// `injection_point_allowed(condition)` — does the runtime condition permit the
/// point to run in this process?
fn condition_allowed(private_data: &[u8]) -> bool {
    if private_data.is_empty() {
        return true;
    }
    match private_data[0] {
        INJ_CONDITION_PID => {
            let pid = i32::from_le_bytes([
                *private_data.get(1).unwrap_or(&0),
                *private_data.get(2).unwrap_or(&0),
                *private_data.get(3).unwrap_or(&0),
                *private_data.get(4).unwrap_or(&0),
            ]);
            init_small_seams::my_proc_pid::call() == pid
        }
        _ => true, // INJ_CONDITION_ALWAYS
    }
}

// ===========================================================================
// Callbacks (injection_error / injection_notice / injection_wait)
// ===========================================================================

/// `injection_error(name, private_data, arg)` — `elog(ERROR)` when allowed.
fn injection_error(name: &str, private_data: &[u8], arg: Option<&str>) -> PgResult<()> {
    if !condition_allowed(private_data) {
        return Ok(());
    }
    match arg {
        Some(argstr) => ::utils_error::elog(::types_error::ERROR, format!("error triggered for injection point {name} ({argstr})")),
        None => ::utils_error::elog(::types_error::ERROR, format!("error triggered for injection point {name}")),
    }
}

/// `injection_notice(name, private_data, arg)` — `elog(NOTICE)` when allowed.
fn injection_notice(name: &str, private_data: &[u8], arg: Option<&str>) -> PgResult<()> {
    if !condition_allowed(private_data) {
        return Ok(());
    }
    match arg {
        Some(argstr) => ::utils_error::elog(::types_error::NOTICE, format!("notice triggered for injection point {name} ({argstr})")),
        None => ::utils_error::elog(::types_error::NOTICE, format!("notice triggered for injection point {name}")),
    }
}

/// `injection_wait(name, private_data, arg)` — sleep on the shared condition
/// variable until `injection_points_wakeup(name)` wakes us.
fn injection_wait(name: &str, private_data: &[u8], _arg: Option<&str>) -> PgResult<()> {
    if !condition_allowed(private_data) {
        return Ok(());
    }
    injection_point::injection_wait(name)
}

// ===========================================================================
// Local injection-point tracking (set_local / cleanup)
// ===========================================================================

thread_local! {
    /// `static bool injection_point_local` — whether points attached in this
    /// process should be PID-scoped (and auto-detached at exit).
    static INJECTION_POINT_LOCAL: RefCell<bool> = const { RefCell::new(false) };
    /// `static List *inj_list_local` — names of locally-attached points.
    static INJ_LIST_LOCAL: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

/// `injection_points_cleanup` — before_shmem_exit callback detaching every
/// locally-attached point. Signature matches `dsm_core::PgOnExitCallback`.
fn injection_points_cleanup(_code: i32, _arg: Datum<'static>) -> PgResult<()> {
    if !INJECTION_POINT_LOCAL.with(|f| *f.borrow()) {
        return Ok(());
    }
    let names = INJ_LIST_LOCAL.with(|l| l.borrow().clone());
    for name in names {
        let _ = injection_point::InjectionPointDetach(&name);
    }
    Ok(())
}

// ===========================================================================
// SQL functions
// ===========================================================================

/// `injection_points_attach(name text, action text)`.
fn fc_attach(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let name = arg_text_str(fcinfo, 0);
    let action = arg_text_str(fcinfo, 1);

    let function = match action.as_str() {
        "error" => "injection_error",
        "notice" => "injection_notice",
        "wait" => "injection_wait",
        _ => raise(PgError::error(format!("incorrect action \"{action}\" for injection point creation"))),
    };

    let is_local = INJECTION_POINT_LOCAL.with(|f| *f.borrow());
    let private_data = if is_local {
        encode_condition(INJ_CONDITION_PID, init_small_seams::my_proc_pid::call())
    } else {
        encode_condition(INJ_CONDITION_ALWAYS, 0)
    };

    ok(injection_point::InjectionPointAttach(&name, LIBRARY, function, &private_data));

    if is_local {
        INJ_LIST_LOCAL.with(|l| l.borrow_mut().push(name.clone()));
    }

    ret_void(fcinfo)
}

/// `injection_points_detach(name text)`.
fn fc_detach(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let name = arg_text_str(fcinfo, 0);
    if !ok(injection_point::InjectionPointDetach(&name)) {
        raise(PgError::error(format!("could not detach injection point \"{name}\"")));
    }
    INJ_LIST_LOCAL.with(|l| l.borrow_mut().retain(|n| n != &name));
    ret_void(fcinfo)
}

/// `injection_points_run(name text, arg text DEFAULT NULL)`.
fn fc_run(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    if arg_isnull(fcinfo, 0) {
        return ret_void(fcinfo);
    }
    let name = arg_text_str(fcinfo, 0);
    let arg = if arg_isnull(fcinfo, 1) { None } else { Some(arg_text_str(fcinfo, 1)) };
    ok(injection_point::InjectionPointRun(&name, arg.as_deref()));
    ret_void(fcinfo)
}

/// `injection_points_cached(name text, arg text DEFAULT NULL)`.
fn fc_cached(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    if arg_isnull(fcinfo, 0) {
        return ret_void(fcinfo);
    }
    let name = arg_text_str(fcinfo, 0);
    let arg = if arg_isnull(fcinfo, 1) { None } else { Some(arg_text_str(fcinfo, 1)) };
    ok(injection_point::InjectionPointCached(&name, arg.as_deref()));
    ret_void(fcinfo)
}

/// `injection_points_load(name text)`.
fn fc_load(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let name = arg_text_str(fcinfo, 0);
    ok(injection_point::InjectionPointLoad(&name));
    ret_void(fcinfo)
}

/// `injection_points_wakeup(name text)`.
fn fc_wakeup(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let name = arg_text_str(fcinfo, 0);
    ok(injection_point::injection_wakeup(&name));
    ret_void(fcinfo)
}

/// `injection_points_set_local()` — PID-scope future attachments and register
/// the before_shmem_exit cleanup.
fn fc_set_local(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    INJECTION_POINT_LOCAL.with(|f| *f.borrow_mut() = true);
    ok(dsm_core::ipc::before_shmem_exit(injection_points_cleanup, Datum::null()));
    ret_void(fcinfo)
}

/// `injection_points_stats_numcalls(name text) RETURNS bigint` — stats unported.
fn fc_stats_numcalls(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    raise(PgError::error("injection_points: per-point statistics (injection_stats.c custom pgstat kind) are not ported"));
}

/// `injection_points_stats_drop()` — stats unported.
fn fc_stats_drop(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    raise(PgError::error("injection_points: statistics (injection_stats.c custom pgstat kind) are not ported"));
}

/// `injection_points_stats_fixed(...) RETURNS record` — stats unported.
fn fc_stats_fixed(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    raise(PgError::error("injection_points: fixed statistics (injection_stats.c custom pgstat kind) are not ported"));
}

/// `removable_cutoff(rel regclass) RETURNS xid8` — from regress_injection.c,
/// not ported (no target test uses it).
fn fc_removable_cutoff(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    raise(PgError::error("injection_points: removable_cutoff (regress_injection.c) is not ported"));
}

// ===========================================================================
// fmgr accessors
// ===========================================================================

/// `PG_ARGISNULL(i)`.
fn arg_isnull(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).map(|a| a.isnull).unwrap_or(true)
}

/// `text_to_cstring(PG_GETARG_TEXT_PP(i))` — a `text` arg decoded to `String`.
fn arg_text_str(fcinfo: &FunctionCallInfoBaseData, i: usize) -> String {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("injection_points: text arg missing from by-ref lane");
    String::from_utf8_lossy(varlena_payload(image)).into_owned()
}

/// `VARDATA_ANY(ptr)` for an inline (non-compressed, non-external) varlena image.
fn varlena_payload(image: &[u8]) -> &[u8] {
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= ::datum::varlena::VARHDRSZ => &image[::datum::varlena::VARHDRSZ..],
        _ => &[],
    }
}

/// `PG_RETURN_VOID()`.
fn ret_void(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.isnull = false;
    Datum::from_usize(0)
}

// ===========================================================================
// Builtin-library registration
// ===========================================================================

/// Resolve a symbol of the `injection_points` module to its ported `PGFunction`.
fn lookup(function: &str) -> Option<LoadedExternalFunc> {
    let user_fn: PGFunction = match function {
        "injection_points_attach" => Some(fc_attach),
        "injection_points_detach" => Some(fc_detach),
        "injection_points_run" => Some(fc_run),
        "injection_points_cached" => Some(fc_cached),
        "injection_points_load" => Some(fc_load),
        "injection_points_wakeup" => Some(fc_wakeup),
        "injection_points_set_local" => Some(fc_set_local),
        "injection_points_stats_numcalls" => Some(fc_stats_numcalls),
        "injection_points_stats_drop" => Some(fc_stats_drop),
        "injection_points_stats_fixed" => Some(fc_stats_fixed),
        "removable_cutoff" => Some(fc_removable_cutoff),
        _ => return None,
    };
    Some(LoadedExternalFunc {
        user_fn,
        api_version: 1,
    })
}

/// Install this unit's seams: register the `injection_points` builtin library
/// and its wait/notice/error callbacks into the core callback registry.
pub fn init_seams() {
    dfmgr_seams::register_builtin_library(dfmgr_seams::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });

    // The callbacks the OS loader would resolve from injection_points.so.
    injection_point::register_callback(LIBRARY, "injection_error", injection_error);
    injection_point::register_callback(LIBRARY, "injection_notice", injection_notice);
    injection_point::register_callback(LIBRARY, "injection_wait", injection_wait);
}
