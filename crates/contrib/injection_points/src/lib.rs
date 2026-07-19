//! SQL surface of src/test/modules/injection_points: the three functions the
//! recovery TAP suite drives (injection_points_attach / _detach / _wakeup),
//! over the process-global registry in the `injection_point` crate.
//!
//! Trimmed vs C: _load/_run/_cached (SQL-side triggers), _set_local (per-PID
//! conditions — meaningless with one server process) and the stats functions
//! are not ported; no recovery test uses them.

#![allow(non_snake_case)]

use datum::Datum;
use types_error::{PgError, PgResult};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

const LIBRARY: &str = "injection_points";

fn arg_text_string(fcinfo: &Fcinfo, i: usize) -> PgResult<String> {
    // SAFETY: all three functions are STRICT, so arg i is non-null.
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    Ok(String::from_utf8_lossy(v.data()).into_owned())
}

fn fc_injection_points_attach(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let name = arg_text_string(fcinfo, 0)?;
    let action = arg_text_string(fcinfo, 1)?;
    injection_point::attach(&name, &action)?;
    Ok(Datum::null())
}

fn fc_injection_points_detach(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let name = arg_text_string(fcinfo, 0)?;
    if !injection_point::detach(&name) {
        return Err(Box::new(PgError::error(format!(
            "could not detach injection point \"{name}\""
        ))));
    }
    Ok(Datum::null())
}

fn fc_injection_points_wakeup(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let name = arg_text_string(fcinfo, 0)?;
    injection_point::wakeup(&name)?;
    Ok(Datum::null())
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "injection_points_attach" => fc_injection_points_attach,
        "injection_points_detach" => fc_injection_points_detach,
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
