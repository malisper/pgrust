//! wait_event_funcs.c: `pg_get_wait_events()` (OID 6318). The static table is
//! `wait_event_funcs_data.tsv` (byte parity vs the generated C data);
//! extension/injection-point rows are appended from the custom registry.

use ::datum::Datum;
use ::types_error::PgResult;
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

pub(crate) const WAIT_EVENT_FUNCS_DATA: &str = include_str!("wait_event_funcs_data.tsv");

fn text_datum(mcx: mcx::Mcx<'_>, s: &str) -> PgResult<Datum> {
    Ok(::types_fmgr::varlena_result(varlena::cstring_to_text(mcx, s.as_bytes())?))
}

fn static_rows() -> impl Iterator<Item = (&'static str, &'static str, &'static str)> {
    WAIT_EVENT_FUNCS_DATA.lines().map(|line| {
        let mut parts = line.splitn(3, '\t');
        let ty = parts.next().expect("type column");
        let name = parts.next().expect("name column");
        let desc = parts.next().expect("description column");
        (ty, name, desc)
    })
}

pub fn fc_pg_get_wait_events(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_get_wait_events: resolved FmgrInfo required");

    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    for (ty, name, desc) in static_rows() {
        let values = [text_datum(mcx, ty)?, text_datum(mcx, name)?, text_datum(mcx, desc)?];
        srf.putvalues(&values, &[false; 3])?;
    }

    for name in crate::custom::GetWaitEventCustomNames(crate::PG_WAIT_EXTENSION) {
        let desc =
            format!("Waiting for custom wait event \"{name}\" defined by extension module");
        let values =
            [text_datum(mcx, "Extension")?, text_datum(mcx, &name)?, text_datum(mcx, &desc)?];
        srf.putvalues(&values, &[false; 3])?;
    }

    for name in crate::custom::GetWaitEventCustomNames(crate::PG_WAIT_INJECTIONPOINT) {
        let desc = format!("Waiting for injection point \"{name}\"");
        let values =
            [text_datum(mcx, "InjectionPoint")?, text_datum(mcx, &name)?, text_datum(mcx, &desc)?];
        srf.putvalues(&values, &[false; 3])?;
    }

    Ok(srf.finish(fcinfo))
}

pub const WAITEVENT_BUILTINS: &[FmgrBuiltin] = &[FmgrBuiltin {
    foid: 6318,
    name: "pg_get_wait_events",
    nargs: 0,
    strict: false,
    retset: true,
    func: fc_pg_get_wait_events,
}];
