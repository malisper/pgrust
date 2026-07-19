use ::datum::Datum;
use ::types_error::PgResult;
use ::types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use backend_status::LocalPgBackendStatus;

use crate::activity::{aux_pid_get_proc, has_pgstat_permissions, ss_family, text_datum};

fn beentry(fcinfo: &Fcinfo) -> Option<LocalPgBackendStatus> {
    backend_status::pgstat_get_beentry_by_proc_number(fcinfo.args_n::<1>()[0].value.as_i32())
}

pub fn fc_pg_stat_get_backend_pid(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    match beentry(fcinfo) {
        Some(be) => Ok(Datum::from_i32(be.st_procpid)),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_pg_stat_get_backend_dbid(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    match beentry(fcinfo) {
        Some(be) => Ok(Datum::from_oid(be.st_databaseid)),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_pg_stat_get_backend_userid(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    match beentry(fcinfo) {
        Some(be) => Ok(Datum::from_oid(be.st_userid)),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_pg_stat_get_backend_activity(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let be = beentry(fcinfo);
    let activity = match &be {
        None => "<backend information not available>",
        Some(be) if !has_pgstat_permissions(be.st_userid)? => "<insufficient privilege>",
        Some(be) if be.st_activity_raw.is_empty() => "<command string not enabled>",
        Some(be) => be.st_activity_raw.as_str(),
    };
    let clipped = backend_status::pgstat_clip_activity(activity.as_bytes());
    text_datum(fcinfo, &clipped)
}

pub fn fc_pg_stat_get_backend_wait_event_type(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let we = match beentry(fcinfo) {
        None => Some("<backend information not available>"),
        Some(be) if !has_pgstat_permissions(be.st_userid)? => Some("<insufficient privilege>"),
        Some(be) => wait_event_info(be.st_procpid)
            .and_then(waitevent::pgstat_get_wait_event_type),
    };
    match we {
        Some(t) => text_datum(fcinfo, t),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_pg_stat_get_backend_wait_event(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let we = match beentry(fcinfo) {
        None => Some("<backend information not available>"),
        Some(be) if !has_pgstat_permissions(be.st_userid)? => Some("<insufficient privilege>"),
        Some(be) => wait_event_info(be.st_procpid).and_then(waitevent::pgstat_get_wait_event),
    };
    match we {
        Some(e) => text_datum(fcinfo, e),
        None => Ok(fcinfo.return_null()),
    }
}

fn wait_event_info(pid: i32) -> Option<u32> {
    let mut proc = procarray::BackendPidGetProc(pid);
    if proc.is_none() {
        proc = aux_pid_get_proc(pid);
    }
    proc.map(|p| p.wait_event_info.load(core::sync::atomic::Ordering::Relaxed))
}

fn beentry_tstz(
    fcinfo: &mut Fcinfo,
    field: impl Fn(&LocalPgBackendStatus) -> i64,
) -> PgResult<Datum> {
    let Some(be) = beentry(fcinfo) else {
        return Ok(fcinfo.return_null());
    };
    if !has_pgstat_permissions(be.st_userid)? {
        return Ok(fcinfo.return_null());
    }
    match field(&be) {
        0 => Ok(fcinfo.return_null()),
        ts => Ok(Datum::from_i64(ts)),
    }
}

pub fn fc_pg_stat_get_backend_activity_start(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    beentry_tstz(fcinfo, |be| be.st_activity_start_timestamp)
}

pub fn fc_pg_stat_get_backend_xact_start(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    beentry_tstz(fcinfo, |be| be.st_xact_start_timestamp)
}

pub fn fc_pg_stat_get_backend_start(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    beentry_tstz(fcinfo, |be| be.st_proc_start_timestamp)
}

// C pgstatfuncs.c:919-960 pg_stat_get_backend_client_addr.
pub fn fc_pg_stat_get_backend_client_addr(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let Some(be) = beentry(fcinfo) else {
        return Ok(fcinfo.return_null());
    };
    if !has_pgstat_permissions(be.st_userid)? {
        return Ok(fcinfo.return_null());
    }
    // C:933-936 — a zeroed client addr means we don't know.
    if ::ip::sockaddr_is_all_zeros(&be.st_clientaddr) {
        return Ok(fcinfo.return_null());
    }
    // C:938-945 — every family but INET/INET6 (unix sockets included) is NULL.
    match ss_family(&be.st_clientaddr.addr) {
        f if f == crate::activity::AF_INET as i32 || f == crate::activity::AF_INET6 as i32 => {}
        _ => return Ok(fcinfo.return_null()),
    }
    // C:947-959 — numeric host via pg_getnameinfo_all (NULL on failure),
    // clean_ipv6_addr, then the inet_in datum.
    let Some((remote_host, _)) = crate::activity::client_addr_port(&be.st_clientaddr) else {
        return Ok(fcinfo.return_null());
    };
    crate::activity::inet_datum(fcinfo, &remote_host)
}

// C pgstatfuncs.c:962-1003 pg_stat_get_backend_client_port.
pub fn fc_pg_stat_get_backend_client_port(
    _fl: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let Some(be) = beentry(fcinfo) else {
        return Ok(fcinfo.return_null());
    };
    if !has_pgstat_permissions(be.st_userid)? {
        return Ok(fcinfo.return_null());
    }
    // C:976-979 — a zeroed client addr means we don't know.
    if ::ip::sockaddr_is_all_zeros(&be.st_clientaddr) {
        return Ok(fcinfo.return_null());
    }
    match ss_family(&be.st_clientaddr.addr) {
        f if f == crate::activity::AF_INET as i32 || f == crate::activity::AF_INET6 as i32 => {}
        // C:986-987 — unix sockets report -1.
        f if f == crate::activity::AF_UNIX as i32 => return Ok(Datum::from_i32(-1)),
        // C:988-989 — unknown family is NULL.
        _ => return Ok(fcinfo.return_null()),
    }
    // C:992-1002 — numeric service via pg_getnameinfo_all (NULL on failure);
    // DirectFunctionCall1(int4in): a numeric service string always parses.
    let Some((_, remote_port)) = crate::activity::client_addr_port(&be.st_clientaddr) else {
        return Ok(fcinfo.return_null());
    };
    Ok(Datum::from_i32(remote_port.parse().expect("numeric service string")))
}

pub fn fc_pg_stat_get_backend_idset(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_stat_get_backend_idset: NULL flinfo");
    if !flinfo.has_fn_extra() {
        let fctx = ::funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(0i32));
    }
    let idx = ::funcapi::per_MultiFuncCall(flinfo)
        .user_fctx
        .as_mut()
        .expect("pg_stat_get_backend_idset: user_fctx set at first call")
        .downcast_mut::<i32>()
        .expect("pg_stat_get_backend_idset: user_fctx is i32");
    *idx += 1;
    let cur = *idx;
    // C rechecks numbackends each call; refresh mid-scan may skip/duplicate.
    if cur <= backend_status::pgstat_fetch_stat_numbackends() {
        let local = backend_status::pgstat_get_local_beentry_by_index(cur)
            .expect("index within numbackends");
        Ok(::funcapi::srf_return_next(flinfo, fcinfo, Datum::from_i32(local.proc_number)))
    } else {
        Ok(::funcapi::srf_return_done(flinfo, fcinfo))
    }
}
