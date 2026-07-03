use datum::Datum;
use types_error::PgResult;
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

fn text_arg_or_empty<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a [u8]> {
    if fcinfo.argisnull(i) {
        Ok(b"")
    } else {
        // SAFETY: non-null text varlena per catalog signature.
        Ok(unsafe { fcinfo.arg_varlena_packed(i)? }.data())
    }
}

pub fn fc_pg_notify(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let channel = str::from_utf8(text_arg_or_empty(fcinfo, 0)?).expect("server-encoded channel");
    let payload = str::from_utf8(text_arg_or_empty(fcinfo, 1)?).expect("server-encoded payload");

    // PreventCommandDuringRecovery("NOTIFY"); the statement form is checked
    // in ProcessUtility (inlined here: a utility dep would cycle).
    if transam_xlog::RecoveryInProgress() {
        return Err(elog::ereport(types_error::ERROR)
            .errcode(types_error::ERRCODE_READ_ONLY_SQL_TRANSACTION)
            .errmsg("cannot execute NOTIFY during recovery")
            .into_error()
            .into());
    }

    crate::Async_Notify(channel, Some(payload))?;
    Ok(Datum::null())
}

pub fn fc_pg_listening_channels(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_listening_channels: NULL flinfo");
    if !flinfo.has_fn_extra() {
        funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
    }
    let cntr = funcapi::per_MultiFuncCall(flinfo).call_cntr;
    match crate::listening_channel_at(cntr as usize) {
        Some(channel) => {
            let t = varlena::cstring_to_text(fcinfo.result_mcx(), &channel)?;
            Ok(funcapi::srf_return_next(flinfo, fcinfo, types_fmgr::varlena_result(t)))
        }
        None => Ok(funcapi::srf_return_done(flinfo, fcinfo)),
    }
}

pub fn fc_pg_notification_queue_usage(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let _ = fcinfo;
    // Advance the tail first so the report isn't stale-high.
    crate::queue::advance_tail()?;
    Ok(Datum::from_f64(crate::queue::queue_usage_fraction()?))
}

pub static ASYNC_BUILTINS: &[FmgrBuiltin] = &[
    FmgrBuiltin {
        foid: 3035,
        name: "pg_listening_channels",
        nargs: 0,
        strict: true,
        retset: true,
        func: fc_pg_listening_channels,
    },
    FmgrBuiltin {
        foid: 3036,
        name: "pg_notify",
        nargs: 2,
        strict: false,
        retset: false,
        func: fc_pg_notify,
    },
    FmgrBuiltin {
        foid: 3296,
        name: "pg_notification_queue_usage",
        nargs: 0,
        strict: true,
        retset: false,
        func: fc_pg_notification_queue_usage,
    },
];
