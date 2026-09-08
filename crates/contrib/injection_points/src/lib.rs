//! SQL surface of src/test/modules/injection_points: the three functions the
//! recovery TAP suite drives (injection_points_attach / _detach / _wakeup)
//! plus the backend-local cache pair (injection_points_load /
//! injection_points_cached, injection_points.c:389-399 / :430-447), over the
//! process-global registry in the `injection_point` crate.
//!
//! Trimmed vs C: _run (the SQL-side run trigger), _set_local (per-PID
//! conditions — meaningless with one server process) and the stats functions
//! (injection_stats.c / injection_stats_fixed.c, so _load/_cached skip their
//! pgstat_report_inj_fixed counters) are not ported; no recovery test uses
//! them.

#![allow(non_snake_case)]

use datum::Datum;
use types_error::{PgError, PgResult};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

const LIBRARY: &str = "injection_points";

fn arg_text_string(fcinfo: &Fcinfo, i: usize) -> PgResult<String> {
    // SAFETY: callers pass only an argument that is non-null (STRICT
    // functions, or checked with argisnull first).
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    Ok(String::from_utf8_lossy(v.data()).into_owned())
}

// These functions attach/detach/wake injection points process-wide — they
// steer server behavior and must not be callable by ordinary roles. The
// extension script also REVOKEs EXECUTE from PUBLIC, but enforce it in-function
// too so ACL misconfiguration (or a role granted the function) cannot bypass it.
fn require_superuser() -> PgResult<()> {
    if !superuser_seams::superuser::call()? {
        return Err(Box::new(
            PgError::error("must be superuser to use injection points")
                .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    Ok(())
}

fn fc_injection_points_attach(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    require_superuser()?;
    let name = arg_text_string(fcinfo, 0)?;
    let action = arg_text_string(fcinfo, 1)?;
    injection_point::attach(&name, &action)?;
    Ok(Datum::null())
}

fn fc_injection_points_detach(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    require_superuser()?;
    let name = arg_text_string(fcinfo, 0)?;
    if !injection_point::detach(&name) {
        return Err(Box::new(PgError::error(format!(
            "could not detach injection point \"{name}\""
        ))));
    }
    Ok(Datum::null())
}

/// injection_points_load (injection_points.c:389-399): INJECTION_POINT_LOAD
/// of the named point into this backend's cache. STRICT.
fn fc_injection_points_load(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    require_superuser()?;
    let name = arg_text_string(fcinfo, 0)?;
    injection_point::load(&name);
    Ok(Datum::null())
}

/// injection_points_cached (injection_points.c:430-447): INJECTION_POINT_CACHED
/// with the optional text argument. Not STRICT: a NULL name returns void
/// (:436-437) and a NULL arg means no argument (:440-441).
fn fc_injection_points_cached(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    require_superuser()?;
    if fcinfo.argisnull(0) {
        return Ok(Datum::null());
    }
    let name = arg_text_string(fcinfo, 0)?;
    let arg = if fcinfo.nargs() > 1 && !fcinfo.argisnull(1) {
        Some(arg_text_string(fcinfo, 1)?)
    } else {
        None
    };
    injection_point::injection_point_cached(&name, arg.as_deref())?;
    Ok(Datum::null())
}

fn fc_injection_points_wakeup(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    require_superuser()?;
    let name = arg_text_string(fcinfo, 0)?;
    injection_point::wakeup(&name)?;
    Ok(Datum::null())
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "injection_points_attach" => fc_injection_points_attach,
        "injection_points_detach" => fc_injection_points_detach,
        "injection_points_load" => fc_injection_points_load,
        "injection_points_cached" => fc_injection_points_cached,
        "injection_points_wakeup" => fc_injection_points_wakeup,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
}
