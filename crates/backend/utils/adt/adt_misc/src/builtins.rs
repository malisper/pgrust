use datum::array_build::ArrayBuildState;
use datum::Datum;
use types_core::{InvalidOid, Oid, TEXTOID, PG_CATALOG_NAMESPACE, RELATION_RELATION_ID};
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_fmgr::{
    byref_result, cstring_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

fn is_ident_start(c: u8) -> bool {
    c == b'_' || c.is_ascii_alphabetic() || c >= 0x80
}

fn is_ident_cont(c: u8) -> bool {
    c.is_ascii_digit() || c == b'$' || is_ident_start(c)
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_ident_err(qualname: &[u8], detail: Option<&str>) -> Box<PgError> {
    let e = PgError::error(format!(
        "string is not a valid identifier: \"{}\"",
        String::from_utf8_lossy(qualname)
    ))
    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE);
    Box::new(match detail {
        Some(d) => e.with_detail(d),
        None => e,
    })
}

pub fn fc_parse_ident(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    // SAFETY: arg 0 is a non-null text datum (strict function).
    let qualname: Vec<u8> = unsafe { fcinfo.arg_varlena_packed(0)? }.data().to_vec();
    let strict = fcinfo.arg_bool(1);
    let encoding = mbutils::GetDatabaseEncoding();

    let mut buf = qualname.clone();
    let mut pos = 0usize;
    let mut after_dot = false;
    let mut astate: Option<ArrayBuildState<'_>> = None;

    while pos < buf.len() && parser_small1::scanner_isspace(buf[pos]) {
        pos += 1;
    }

    loop {
        let mut missing_ident = true;

        if pos < buf.len() && buf[pos] == b'"' {
            let curname_start = pos + 1;
            let mut search_start = pos + 1;
            let endp;
            loop {
                match buf[search_start..].iter().position(|&b| b == b'"') {
                    None => {
                        return Err(invalid_ident_err(
                            &qualname,
                            Some("String has unclosed double quotes."),
                        ))
                    }
                    Some(off) => {
                        let q = search_start + off;
                        if q + 1 < buf.len() && buf[q + 1] == b'"' {
                            buf.remove(q);
                            search_start = q + 1;
                            continue;
                        }
                        endp = q;
                        break;
                    }
                }
            }
            if endp == curname_start {
                return Err(invalid_ident_err(
                    &qualname,
                    Some("Quoted identifier must not be empty."),
                ));
            }
            let text = varlena::cstring_to_text(mcx, &buf[curname_start..endp])?;
            astate = Some(arrayfuncs::accum_array_result(
                mcx,
                astate.take(),
                Datum::from_usize(text.as_bytes().as_ptr() as usize),
                false,
                TEXTOID,
            )?);
            pos = endp + 1;
            missing_ident = false;
        } else if pos < buf.len() && is_ident_start(buf[pos]) {
            let start = pos;
            pos += 1;
            while pos < buf.len() && is_ident_cont(buf[pos]) {
                pos += 1;
            }
            let down = parser_small1::downcase_identifier(mcx, &buf[start..pos], false, false, encoding)?;
            let text = varlena::cstring_to_text(mcx, &down)?;
            astate = Some(arrayfuncs::accum_array_result(
                mcx,
                astate.take(),
                Datum::from_usize(text.as_bytes().as_ptr() as usize),
                false,
                TEXTOID,
            )?);
            missing_ident = false;
        }

        if missing_ident {
            if pos < buf.len() && buf[pos] == b'.' {
                return Err(invalid_ident_err(&qualname, Some("No valid identifier before \".\".")));
            } else if after_dot {
                return Err(invalid_ident_err(&qualname, Some("No valid identifier after \".\".")));
            } else {
                return Err(invalid_ident_err(&qualname, None));
            }
        }

        while pos < buf.len() && parser_small1::scanner_isspace(buf[pos]) {
            pos += 1;
        }

        if pos < buf.len() && buf[pos] == b'.' {
            after_dot = true;
            pos += 1;
            while pos < buf.len() && parser_small1::scanner_isspace(buf[pos]) {
                pos += 1;
            }
        } else if pos >= buf.len() {
            break;
        } else {
            if strict {
                return Err(invalid_ident_err(&qualname, None));
            }
            break;
        }
    }

    let img = match &astate {
        None => arrayfuncs::construct_empty_array(mcx, TEXTOID)?,
        Some(st) => arrayfuncs::make_array_result(mcx, st)?,
    };
    byref_result(mcx, &img)
}

pub fn fc_version(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(varlena::cstring_to_text(
        mcx,
        crate::introspect::PG_VERSION_STR.as_bytes(),
    )?))
}

pub fn fc_current_database(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let db = crate::introspect::current_database()?;
    byref_result(fcinfo.result_mcx(), &db.data)
}

// timestamp.c pg_postmaster_start_time: C `PgStartTime` global, hosted with
// the misc slice (postmaster_seams already crosses this dep).
pub fn fc_pg_postmaster_start_time(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_i64(postmaster_seams::pg_start_time::call()))
}

fn description_result(
    fcinfo: &mut Fcinfo,
    objoid: Oid,
    classoid: Oid,
    objsubid: i32,
) -> PgResult<Datum> {
    let found = crate::introspect::get_description(fcinfo.result_mcx(), objoid, classoid, objsubid)?
        .map(varlena_result);
    match found {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

// Unknown catalog name yields NULL, not an error (the SQL body's subquery).
pub fn fc_obj_description(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let objoid = fcinfo.arg_oid(0);
    // SAFETY: catalog arg 1 of obj_description is a non-null name (strict fn).
    let catalogname = unsafe { fcinfo.arg_name(1) };
    let len = catalogname.iter().position(|&b| b == 0).unwrap_or(catalogname.len());
    let catalogname = core::str::from_utf8(&catalogname[..len])
        .expect("catalog names are valid UTF-8");
    let classoid = lsyscache::get_relname_relid(catalogname, PG_CATALOG_NAMESPACE)?;
    if classoid == InvalidOid {
        return Ok(fcinfo.return_null());
    }
    description_result(fcinfo, objoid, classoid, 0)
}

pub fn fc_col_description(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let objoid = fcinfo.arg_oid(0);
    let attnum = fcinfo.arg_i32(1);
    description_result(fcinfo, objoid, RELATION_RELATION_ID, attnum)
}

pub fn fc_shobj_description(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let objoid = fcinfo.arg_oid(0);
    // SAFETY: catalog arg 1 of shobj_description is a non-null name (strict fn).
    let catalogname = unsafe { fcinfo.arg_name(1) };
    let len = catalogname.iter().position(|&b| b == 0).unwrap_or(catalogname.len());
    let catalogname = core::str::from_utf8(&catalogname[..len])
        .expect("catalog names are valid UTF-8");
    let classoid = lsyscache::get_relname_relid(catalogname, PG_CATALOG_NAMESPACE)?;
    if classoid == InvalidOid {
        return Ok(fcinfo.return_null());
    }
    let found = crate::introspect::get_shared_description(fcinfo.result_mcx(), objoid, classoid)?
        .map(varlena_result);
    match found {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

// xlogfuncs.c WAL-name trio (2850/2851/6213): pure segment math over
// XLogSegNo (xlog_internal.h macros) + the live insert timeline.

const XLOG_FNAME_LEN: usize = 24;

fn xlog_segments_per_xlog_id(seg_size: u64) -> u64 {
    0x1_0000_0000 / seg_size
}

fn xlog_file_name(tli: u32, segno: u64, seg_size: u64) -> [u8; XLOG_FNAME_LEN] {
    let segs = xlog_segments_per_xlog_id(seg_size);
    let mut out = [0u8; XLOG_FNAME_LEN];
    let mut put = |off: usize, v: u32| {
        for (i, b) in format!("{v:08X}").bytes().enumerate() {
            out[off + i] = b;
        }
    };
    put(0, tli);
    put(8, (segno / segs) as u32);
    put(16, (segno % segs) as u32);
    out
}

fn is_xlog_file_name(name: &[u8]) -> bool {
    name.len() == XLOG_FNAME_LEN
        && name.iter().all(|b| b.is_ascii_digit() || (b'A'..=b'F').contains(b))
}

fn xlog_from_file_name(name: &[u8], seg_size: u64) -> (u32, u64) {
    let hex = |r: core::ops::Range<usize>| {
        u32::from_str_radix(core::str::from_utf8(&name[r]).unwrap(), 16).unwrap()
    };
    let tli = hex(0..8);
    let log = hex(8..16) as u64;
    let seg = hex(16..24) as u64;
    (tli, log * xlog_segments_per_xlog_id(seg_size) + seg)
}

#[cold]
#[inline(never)]
fn recovery_in_progress_err(fname: &str) -> Box<types_error::PgError> {
    Box::new(
        types_error::PgError::error("recovery is in progress")
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint(format!("{fname} cannot be executed during recovery.")),
    )
}

pub fn fc_pg_switch_wal(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    if transam_xlog::RecoveryInProgress() {
        return Err(Box::new(
            types_error::PgError::error("recovery is in progress")
                .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .with_hint("WAL control functions cannot be executed during recovery."),
        ));
    }
    let switchpoint = transam_xlog::RequestXLogSwitch(false)?;
    Ok(Datum::from_i64(switchpoint as i64))
}

pub fn fc_pg_walfile_name(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let lsn = fcinfo.arg(0).as_u64();
    if transam_xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_err("pg_walfile_name()"));
    }
    let seg_size = transam_xlog::wal_segment_size() as u64;
    let name = xlog_file_name(transam_xlog::ctl::GetWALInsertionTimeLine(), lsn / seg_size, seg_size);
    crate::text_datum(fcinfo.result_mcx(), &name)
}

fn composite_result(
    flinfo: &FmgrInfo,
    fcinfo: &mut Fcinfo,
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    if resolved.class != funcapi::TypeFuncClass::Composite {
        return Err(crate::not_row_type());
    }
    let tupdesc = resolved.result_tuple_desc.expect("composite result has tupdesc");
    let tup = heaptuple::heap_form_tuple(mcx, &tupdesc, values, isnull)?;
    let d = Datum::from_usize(tup.header_ptr() as usize);
    core::mem::forget(tup); // leak into the arming context (C palloc ownership)
    Ok(d)
}

pub fn fc_pg_walfile_name_offset(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_walfile_name_offset: NULL flinfo");
    let lsn = fcinfo.arg(0).as_u64();
    if transam_xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_err("pg_walfile_name_offset()"));
    }
    let seg_size = transam_xlog::wal_segment_size() as u64;
    let name = xlog_file_name(transam_xlog::ctl::GetWALInsertionTimeLine(), lsn / seg_size, seg_size);
    let values = [
        crate::text_datum(fcinfo.result_mcx(), &name)?,
        Datum::from_u32((lsn % seg_size) as u32),
    ];
    composite_result(flinfo, fcinfo, &values, &[false, false])
}

pub fn fc_pg_split_walfile_name(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_split_walfile_name: NULL flinfo");
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let fname = unsafe { fcinfo.arg_varlena_packed(0)? };
    let data = fname.data();
    let mut upper = [0u8; XLOG_FNAME_LEN];
    let sized = data.len() == XLOG_FNAME_LEN;
    if sized {
        for (d, b) in upper.iter_mut().zip(data) {
            *d = b.to_ascii_uppercase();
        }
    }
    if !sized || !is_xlog_file_name(&upper) {
        return Err(Box::new(
            types_error::PgError::error(format!(
                "invalid WAL file name \"{}\"",
                String::from_utf8_lossy(data)
            ))
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    let seg_size = transam_xlog::wal_segment_size() as u64;
    let (tli, segno) = xlog_from_file_name(&upper, seg_size);

    let mcx = fcinfo.result_mcx();
    let num = adt_numeric::io::numeric_in(&segno.to_string(), -1, None)?
        .expect("decimal u64 is valid numeric input");
    let values = [
        byref_result(mcx, num.as_bytes())?,
        Datum::from_i64(tli as i64),
    ];
    composite_result(flinfo, fcinfo, &values, &[false, false])
}

pub fn fc_pg_current_wal_lsn(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    if transam_xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_err("WAL control functions"));
    }
    Ok(Datum::from_i64(transam_xlog::GetXLogWriteRecPtr() as i64))
}

pub fn fc_pg_current_wal_insert_lsn(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if transam_xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_err("WAL control functions"));
    }
    Ok(Datum::from_i64(transam_xlog::GetXLogInsertRecPtr() as i64))
}

pub fn fc_pg_current_wal_flush_lsn(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if transam_xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_err("WAL control functions"));
    }
    Ok(Datum::from_i64(transam_xlog::GetFlushRecPtr(None) as i64))
}

pub fn fc_pg_wal_lsn_diff(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let lsn1 = fcinfo.arg(0).as_u64();
    let lsn2 = fcinfo.arg(1).as_u64();
    let img = adt_pg_lsn::pg_lsn_mi(lsn1, lsn2)?;
    byref_result(fcinfo.result_mcx(), img.as_bytes())
}

pub fn fc_pg_is_in_recovery(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(transam_xlog::RecoveryInProgress()))
}

pub fn fc_pg_last_wal_receive_lsn(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if !walreceiverfuncs_seams::get_wal_rcv_flush_rec_ptr::is_installed() {
        return Ok(fcinfo.return_null());
    }
    let (recptr, _latest_chunk_start, _tli) =
        walreceiverfuncs_seams::get_wal_rcv_flush_rec_ptr::call();
    if recptr == 0 {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_i64(recptr as i64))
}

pub fn fc_pg_last_wal_replay_lsn(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let (recptr, _tli) = xlogrecovery::GetXLogReplayRecPtr();
    if recptr == 0 {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_i64(recptr as i64))
}

pub fn fc_pg_last_xact_replay_timestamp(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let xtime = xlogrecovery::GetLatestXTime();
    if xtime == 0 {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_i64(xtime))
}

fn check_recovery_control() -> PgResult<()> {
    if !transam_xlog::RecoveryInProgress() {
        return Err(recovery_not_in_progress_err("Recovery control functions"));
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn promotion_ongoing_err(fname: &str) -> Box<types_error::PgError> {
    Box::new(
        types_error::PgError::error("standby promotion is ongoing")
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint(format!("{fname} cannot be executed after promotion is triggered.")),
    )
}

pub fn fc_pg_wal_replay_pause(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_recovery_control()?;
    if xlogrecovery::PromoteIsTriggered() {
        return Err(promotion_ongoing_err("pg_wal_replay_pause()"));
    }
    xlogrecovery::SetRecoveryPause(true);
    xlogrecovery::WakeupRecovery();
    Ok(Datum::null())
}

pub fn fc_pg_wal_replay_resume(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_recovery_control()?;
    if xlogrecovery::PromoteIsTriggered() {
        return Err(promotion_ongoing_err("pg_wal_replay_resume()"));
    }
    xlogrecovery::SetRecoveryPause(false);
    Ok(Datum::null())
}

pub fn fc_pg_is_wal_replay_paused(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_recovery_control()?;
    Ok(Datum::from_bool(
        xlogrecovery::GetRecoveryPauseState() != xlogrecovery::targets::RECOVERY_NOT_PAUSED,
    ))
}

pub fn fc_pg_get_wal_replay_pause_state(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    check_recovery_control()?;
    let statestr = match xlogrecovery::GetRecoveryPauseState() {
        xlogrecovery::targets::RECOVERY_PAUSE_REQUESTED => "pause requested",
        xlogrecovery::targets::RECOVERY_PAUSED => "paused",
        _ => "not paused",
    };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(varlena::cstring_to_text(
        mcx,
        statestr.as_bytes(),
    )?))
}

pub fn fc_pg_create_restore_point(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if transam_xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_err("WAL control functions"));
    }
    if !transam_xlog::XLogIsNeeded() {
        return Err(Box::new(
            types_error::PgError::error("WAL level not sufficient for creating a restore point")
                .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .with_hint(
                    "\"wal_level\" must be set to \"replica\" or \"logical\" at server start.",
                ),
        ));
    }
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let name = unsafe { fcinfo.arg_varlena_packed(0)? };
    let name = name.data();
    // text_to_cstring's strlen() stops at an embedded NUL, a divergence from the varlena length.
    let cstr_len = name.iter().position(|&b| b == 0).unwrap_or(name.len());
    if cstr_len >= transam_xlog::MAXFNAMELEN {
        return Err(Box::new(
            types_error::PgError::error(format!(
                "value too long for restore point (maximum {} characters)",
                transam_xlog::MAXFNAMELEN - 1
            ))
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    let rp_time = timestamp_seams::get_current_timestamp::call();
    let mut body = [0u8; 8 + transam_xlog::MAXFNAMELEN];
    body[..8].copy_from_slice(&rp_time.to_ne_bytes());
    body[8..8 + cstr_len].copy_from_slice(&name[..cstr_len]);
    let recptr = xloginsert_seams::xlog_insert::call(
        transam_xlog::RM_XLOG_ID,
        transam_xlog::XLOG_RESTORE_POINT,
        &[&body],
    )?;
    let _ = elog::elog(
        types_error::LOG,
        format!(
            "restore point \"{}\" created at {:X}/{:X}",
            String::from_utf8_lossy(&name[..cstr_len]),
            (recptr >> 32) as u32,
            recptr as u32
        ),
    );
    Ok(Datum::from_i64(recptr as i64))
}

pub fn fc_pg_log_standby_snapshot(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if transam_xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_err("pg_log_standby_snapshot()"));
    }
    if !transam_xlog::XLogStandbyInfoActive() {
        return Err(Box::new(
            types_error::PgError::error(
                "pg_log_standby_snapshot() can only be used if \"wal_level\" >= \"replica\"",
            )
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        ));
    }
    let recptr = standby::LogStandbySnapshot()?;
    Ok(Datum::from_i64(recptr as i64))
}

#[cold]
#[inline(never)]
fn recovery_not_in_progress_err(fname: &str) -> Box<types_error::PgError> {
    Box::new(
        types_error::PgError::error("recovery is not in progress")
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint(format!("{fname} can only be executed during recovery.")),
    )
}

const PROMOTE_SIGNAL_FILE: &str = "promote";
const WAIT_EVENT_PROMOTE: u32 = waitevent::PG_WAIT_IPC + 43;

fn saved_errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

pub fn fc_pg_promote(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    use types_storage::waiteventset::{WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_TIMEOUT};

    let wait = fcinfo.arg_bool(0);
    let wait_seconds = fcinfo.arg_i32(1);

    if !transam_xlog::RecoveryInProgress() {
        return Err(recovery_not_in_progress_err("Recovery control functions"));
    }
    if wait_seconds <= 0 {
        return Err(Box::new(
            types_error::PgError::error("\"wait_seconds\" must not be negative or zero")
                .with_sqlstate(types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
        ));
    }

    let promote_fd = fd::AllocateFile(PROMOTE_SIGNAL_FILE, "w")?;
    if promote_fd < 0 {
        return Err(elog::ereport(types_error::ERROR)
            .with_saved_errno(saved_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not create file \"{PROMOTE_SIGNAL_FILE}\": %m"))
            .into_error()
            .into());
    }
    if fd::FreeFile(promote_fd)? != 0 {
        return Err(elog::ereport(types_error::ERROR)
            .with_saved_errno(saved_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not write file \"{PROMOTE_SIGNAL_FILE}\": %m"))
            .into_error()
            .into());
    }

    postmaster_seams::signal_postmaster_sigusr1::call();

    if !wait {
        return Ok(Datum::from_bool(true));
    }

    const WAITS_PER_SECOND: i32 = 10;
    for _ in 0..(WAITS_PER_SECOND * wait_seconds) {
        latch_seams::reset_latch_my_latch::call();
        if !transam_xlog::RecoveryInProgress() {
            return Ok(Datum::from_bool(true));
        }
        postgres_seams::check_for_interrupts::call()?;
        let rc = latch_seams::wait_latch_my_latch::call(
            WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
            1000 / WAITS_PER_SECOND as i64,
            WAIT_EVENT_PROMOTE,
        );
        if rc & WL_POSTMASTER_DEATH != 0 {
            elog::ereport(types_error::FATAL)
                .errcode(types_error::ERRCODE_ADMIN_SHUTDOWN)
                .errmsg("terminating connection due to unexpected postmaster exit")
                .errcontext_msg("while waiting on promotion")
                .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "pg_promote"))?;
        }
    }

    elog::ereport(types_error::WARNING)
        .errmsg_plural(
            format!("server did not promote within {wait_seconds} second"),
            format!("server did not promote within {wait_seconds} seconds"),
            wait_seconds as u64,
        )
        .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "pg_promote"))?;
    Ok(Datum::from_bool(false))
}

// Session-level context for the SQL-callable backup functions (xlogfuncs.c's
// static backup_state / tablespace_map, kept alive across the two calls).
std::thread_local! {
    static BACKUP_SESSION: core::cell::RefCell<Option<(xlogbackup::BackupState, Vec<u8>)>> =
        const { core::cell::RefCell::new(None) };
}

// pg_backup_start(label text, fast bool) (xlogfuncs.c).
pub fn fc_pg_backup_start(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg 0 is a non-null text varlena (strict function).
    let backupid = unsafe { fcinfo.arg_varlena_packed(0)? };
    let raw = backupid.data();
    let end = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
    let backupidstr = String::from_utf8_lossy(&raw[..end]).into_owned();
    let fast = fcinfo.arg_bool(1);

    if transam_xlog::get_backup_status() == transam_xlog::SessionBackupState::Running {
        return Err(Box::new(
            PgError::error("a backup is already in progress in this session")
                .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        ));
    }

    transam_xlog::register_persistent_abort_backup_handler()?;

    let mut state = xlogbackup::BackupState::default();
    let mut tablespace_map: Vec<u8> = Vec::new();
    transam_xlog::do_pg_backup_start(&backupidstr, fast, None, &mut state, &mut tablespace_map)?;

    let startpoint = state.startpoint;
    BACKUP_SESSION.with(|c| *c.borrow_mut() = Some((state, tablespace_map)));

    Ok(Datum::from_i64(startpoint as i64))
}

// pg_backup_stop(wait_for_archive bool) → (lsn, labelfile, spcmapfile) (xlogfuncs.c).
pub fn fc_pg_backup_stop(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_backup_stop: NULL flinfo");
    let waitforarchive = fcinfo.arg_bool(0);

    if transam_xlog::get_backup_status() != transam_xlog::SessionBackupState::Running {
        return Err(Box::new(
            PgError::error("backup is not in progress")
                .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .with_hint("Did you call pg_backup_start()?"),
        ));
    }

    let (mut state, tablespace_map) = BACKUP_SESSION
        .with(|c| c.borrow_mut().take())
        .expect("backup session state present when SESSION_BACKUP_RUNNING");

    transam_xlog::do_pg_backup_stop(&mut state, waitforarchive)?;

    let wal_segment_size = transam_xlog::wal_segment_size();
    // Scope the result_mcx() borrow of fcinfo (held alive by backup_label, an
    // mcx-allocated Vec) so it ends before composite_result borrows fcinfo &mut.
    let values = {
        let mcx = fcinfo.result_mcx();
        let backup_label = xlogbackup::build_backup_content(mcx, &state, false, wal_segment_size)?;
        [
            Datum::from_i64(state.stoppoint as i64),
            crate::text_datum(mcx, &backup_label)?,
            crate::text_datum(mcx, &tablespace_map)?,
        ]
    };
    composite_result(flinfo, fcinfo, &values, &[false, false, false])
}

pub fn fc_pg_stop_making_pinned_objects(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if !superuser_seams::superuser::call()? {
        return Err(Box::new(
            PgError::error("must be superuser to call pg_stop_making_pinned_objects()")
                .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    varsup::StopGeneratingPinnedObjectIds()?;
    Ok(Datum::from_usize(0))
}

pub fn fc_system_user(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    match miscinit::GetSystemUser() {
        Some(s) => crate::text_datum(fcinfo.result_mcx(), s.as_bytes()),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_pg_client_encoding(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mut n = types_tuple::NameData::default();
    n.namestrcpy(mbutils::pg_get_client_encoding_name());
    byref_result(fcinfo.result_mcx(), &n.data)
}

pub fn fc_pg_conf_load_time(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i64(guc::pg_reload_time()))
}

// jit.c DLSUFFIX on the build platforms.
const DLSUFFIX: &str = ".so";

thread_local! {
    // jit.c provider_failed_loading: a probe failed once; don't re-stat.
    static JIT_PROVIDER_FAILED_LOADING: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

pub fn fc_pg_jit_available(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(jit_provider_init()?))
}

// jit.c provider_init() up to the pg_file_exists probe. There is no dlopen
// substrate, so a provider shlib actually present in pkglib_path is
// unreachable-loud rather than silently claimed available (the PGDG oracle
// image ships none: C answers false, and so do we).
fn jit_provider_init() -> PgResult<bool> {
    if !guc_tables::vars::jit_enabled.read() {
        return Ok(false);
    }
    if JIT_PROVIDER_FAILED_LOADING.get() {
        return Ok(false);
    }
    // jit_provider is PGC_POSTMASTER with boot_val "llvmjit"; no owner
    // installs accessors yet, so the boot default stands in until one does.
    let provider = if guc_tables::vars::jit_provider.installed() {
        guc_tables::vars::jit_provider.read().unwrap_or_default()
    } else {
        "llvmjit".to_string()
    };
    let pkglib = init_small::globals::pkglib_path();
    let len = pkglib.iter().position(|&b| b == 0).unwrap_or(pkglib.len());
    let path = format!("{}/{provider}{DLSUFFIX}", String::from_utf8_lossy(&pkglib[..len]));
    if !fd::pg_file_exists(&path)? {
        JIT_PROVIDER_FAILED_LOADING.set(true);
        return Ok(false);
    }
    // unported: JIT provider loading (jit.c provider_init). C returns false
    // when the provider fails to load; an unloadable provider is the same
    // observable state, so report JIT as unavailable (loud once via the
    // failed-loading latch, matching C's one-shot probe).
    JIT_PROVIDER_FAILED_LOADING.set(true);
    Ok(false)
}

// misc.c LOG_METAINFO_DATAFILE, relative to the data directory (backend cwd).
const LOG_METAINFO_DATAFILE: &str = "current_logfiles";

fn current_logfile(fcinfo: &mut Fcinfo, logfmt: Option<&[u8]>) -> PgResult<Datum> {
    if let Some(f) = logfmt {
        if f != b"stderr" && f != b"csvlog" && f != b"jsonlog" {
            return Err(Box::new(
                PgError::error(format!(
                    "log format \"{}\" is not supported",
                    String::from_utf8_lossy(f)
                ))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_hint(
                    "The supported log formats are \"stderr\", \"csvlog\", and \"jsonlog\".",
                ),
            ));
        }
    }
    let contents = match std::fs::read(LOG_METAINFO_DATAFILE) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(fcinfo.return_null()),
        Err(e) => {
            return Err(elog::ereport(types_error::ERROR)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not read file \"{LOG_METAINFO_DATAFILE}\": %m"))
                .into_error()
                .into())
        }
    };
    for line in contents.split_inclusive(|&b| b == b'\n') {
        let Some(sp) = line.iter().position(|&b| b == b' ') else {
            return Err(Box::new(PgError::error(format!(
                "missing space character in \"{LOG_METAINFO_DATAFILE}\""
            ))));
        };
        let rest = &line[sp + 1..];
        let Some(nl) = rest.iter().position(|&b| b == b'\n') else {
            return Err(Box::new(PgError::error(format!(
                "missing newline character in \"{LOG_METAINFO_DATAFILE}\""
            ))));
        };
        if logfmt.is_none_or(|f| f == &line[..sp]) {
            return crate::text_datum(fcinfo.result_mcx(), &rest[..nl]);
        }
    }
    Ok(fcinfo.return_null())
}

pub fn fc_pg_current_logfile(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    current_logfile(fcinfo, None)
}

pub fn fc_pg_current_logfile_1arg(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    if a.isnull {
        return current_logfile(fcinfo, None);
    }
    // SAFETY: null-checked text arg of a non-strict fn.
    let fmt = unsafe { fcinfo.arg_varlena_packed(0)? };
    let fmt: Vec<u8> = fmt.data().to_vec();
    current_logfile(fcinfo, Some(&fmt))
}

pub fn fc_pg_get_wal_summarizer_state(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_get_wal_summarizer_state: NULL flinfo");
    let (tli, summarized_lsn, pending_lsn, pid) =
        walsummarizer_seams::get_wal_summarizer_state::call()?;
    let values = [
        Datum::from_i64(tli as i64),
        Datum::from_i64(summarized_lsn as i64),
        Datum::from_i64(pending_lsn as i64),
        Datum::from_i32(pid),
    ];
    composite_result(flinfo, fcinfo, &values, &[false, false, false, pid < 0])
}

fn fc_pg_my_temp_schema(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let _ = fcinfo;
    Ok(Datum::from_oid(catalog_namespace::GetTempNamespaceState().0))
}

fn fc_pg_is_other_temp_schema(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let oid = fcinfo.arg_oid(0);
    Ok(Datum::from_bool(catalog_namespace::isOtherTempNamespace(oid)?))
}

pub fn fc_pg_trigger_depth(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let _ = fcinfo;
    Ok(Datum::from_i32(trigger_seams::my_trigger_depth::call()))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: true,
        retset: false,
        func,
    }
}

// 1215/1216 are absent from the canonical table (SQL-language in C, STRICT).

// varchar.c typmodout slice hosted here until the varchar unit lands.
fn typmod_paren(fcinfo: &mut Fcinfo, s: String) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mut v = mcx::vec_with_capacity_in(mcx, s.len() + 1)?;
    mcx::vec_append_bytes(&mut v, s.as_bytes())?;
    mcx::vec_append_bytes(&mut v, &[0])?;
    Ok(cstring_result(v))
}

// utils/mb/mbutils.c PG_encoding_to_char, hosted with the misc slice.
pub fn fc_pg_encoding_to_char(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let name = mbutils::pg_encoding_to_char(fcinfo.arg_i32(0));
    let mut n = types_tuple::NameData::default();
    n.namestrcpy(name);
    byref_result(fcinfo.result_mcx(), &n.data)
}

// utils/mb/mbutils.c PG_char_to_encoding, hosted with the misc slice.
pub fn fc_pg_char_to_encoding(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg is name; strict fn.
    let raw = unsafe { fcinfo.arg_name(0) };
    let nul = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
    // Bytes, not str: C cleans invalid-encoding bytes byte-wise; a
    // from_utf8 wholesale-reject here was divergence #10 (reachable in
    // SQL_ASCII databases; proofs/encnames witness + glibc ground truth).
    Ok(Datum::from_i32(mbutils::pg_char_to_encoding_bytes(&raw[..nul])))
}

pub fn fc_anychar_typmodout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(0);
    let out = if typmod > 4 { format!("({})", typmod - 4) } else { String::new() };
    typmod_paren(fcinfo, out)
}

pub fn fc_numerictypmodout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(0);
    let out = if adt_numeric::ops::is_valid_numeric_typmod(typmod) {
        format!(
            "({},{})",
            adt_numeric::ops::numeric_typmod_precision(typmod),
            adt_numeric::ops::numeric_typmod_scale(typmod)
        )
    } else {
        String::new()
    };
    typmod_paren(fcinfo, out)
}

struct KeywordRows {
    tuples: Vec<Vec<u8>>,
}

fn collect_keyword_rows(flinfo: &FmgrInfo, fcinfo: &Fcinfo) -> PgResult<KeywordRows> {
    let mcx = fcinfo.result_mcx();
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    if resolved.class != funcapi::TypeFuncClass::Composite {
        return Err(Box::new(types_error::PgError::error(
            "return type must be a row type",
        )));
    }
    let desc = resolved.result_tuple_desc.expect("composite result carries a tupdesc");

    let n = keywords::ScanKeywords.num_keywords as usize;
    let mut tuples = Vec::with_capacity(n);
    for i in 0..n {
        let word = keywords::GetScanKeyword(i, &keywords::ScanKeywords).expect("index < n");
        let (catcode, catdesc): (u8, &str) = match keywords::ScanKeywordCategories[i] {
            keywords::KeywordCategory::Unreserved => (b'U', "unreserved"),
            keywords::KeywordCategory::ColName => {
                (b'C', "unreserved (cannot be function or type name)")
            }
            keywords::KeywordCategory::TypeFuncName => {
                (b'T', "reserved (can be function or type name)")
            }
            keywords::KeywordCategory::Reserved => (b'R', "reserved"),
        };
        let barelabel = keywords::ScanKeywordBareLabel[i];
        let baredesc = if barelabel { "can be bare label" } else { "requires AS" };
        let values = [
            varlena_result(varlena::cstring_to_text(mcx, word)?),
            Datum::from_char(catcode as i8),
            Datum::from_bool(barelabel),
            varlena_result(varlena::cstring_to_text(mcx, catdesc.as_bytes())?),
            varlena_result(varlena::cstring_to_text(mcx, baredesc.as_bytes())?),
        ];
        let tuple = heaptuple::heap_form_tuple(mcx, &desc, &values, &[false; 5])?;
        tuples.push(tuple.image().to_vec());
    }
    Ok(KeywordRows { tuples })
}

pub fn fc_pg_get_keywords(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_get_keywords: NULL flinfo");
    if !flinfo.has_fn_extra() {
        let rows = collect_keyword_rows(flinfo, fcinfo)?;
        let fctx = funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(rows));
    }
    let fctx = funcapi::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let rows = fctx
        .user_fctx
        .as_ref()
        .expect("pg_get_keywords: rows set at first call")
        .downcast_ref::<KeywordRows>()
        .expect("pg_get_keywords: user_fctx is KeywordRows");
    match rows.tuples.get(idx) {
        Some(img) => {
            let d = byref_result(fcinfo.result_mcx(), img)?;
            Ok(funcapi::srf_return_next(flinfo, fcinfo, d))
        }
        None => Ok(funcapi::srf_return_done(flinfo, fcinfo)),
    }
}

fn text_list_array_datum(mcx: mcx::Mcx<'_>, cols: &str) -> PgResult<Datum> {
    let inner = cols.strip_prefix('{').and_then(|s| s.strip_suffix('}'))
        .expect("generated column list is brace-wrapped");
    let mut astate: Option<ArrayBuildState<'_>> = None;
    for name in inner.split(',') {
        let text = varlena::cstring_to_text(mcx, name.as_bytes())?;
        astate = Some(arrayfuncs::accum_array_result(
            mcx,
            astate.take(),
            Datum::from_usize(text.as_bytes().as_ptr() as usize),
            false,
            TEXTOID,
        )?);
    }
    let img = arrayfuncs::make_array_result(mcx, &astate.expect("column list is non-empty"))?;
    byref_result(mcx, &img)
}

struct CatalogFkRows {
    tuples: Vec<Vec<u8>>,
}

fn collect_catalog_fk_rows(flinfo: &FmgrInfo, fcinfo: &Fcinfo) -> PgResult<CatalogFkRows> {
    let mcx = fcinfo.result_mcx();
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    if resolved.class != funcapi::TypeFuncClass::Composite {
        return Err(crate::not_row_type());
    }
    let desc = resolved.result_tuple_desc.expect("composite result carries a tupdesc");

    let rows = crate::catalog_fk::SYS_FK_RELATIONSHIPS;
    let mut tuples = Vec::with_capacity(rows.len());
    for (fk_table, pk_table, fk_columns, pk_columns, is_array, is_opt) in rows {
        let values = [
            Datum::from_oid(*fk_table),
            text_list_array_datum(mcx, fk_columns)?,
            Datum::from_oid(*pk_table),
            text_list_array_datum(mcx, pk_columns)?,
            Datum::from_bool(*is_array),
            Datum::from_bool(*is_opt),
        ];
        let tuple = heaptuple::heap_form_tuple(mcx, &desc, &values, &[false; 6])?;
        tuples.push(tuple.image().to_vec());
    }
    Ok(CatalogFkRows { tuples })
}

pub fn fc_pg_get_catalog_foreign_keys(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_get_catalog_foreign_keys: NULL flinfo");
    if !flinfo.has_fn_extra() {
        let rows = collect_catalog_fk_rows(flinfo, fcinfo)?;
        let fctx = funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(rows));
    }
    let fctx = funcapi::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let rows = fctx
        .user_fctx
        .as_ref()
        .expect("pg_get_catalog_foreign_keys: rows set at first call")
        .downcast_ref::<CatalogFkRows>()
        .expect("pg_get_catalog_foreign_keys: user_fctx is CatalogFkRows");
    match rows.tuples.get(idx) {
        Some(img) => {
            let d = byref_result(fcinfo.result_mcx(), img)?;
            Ok(funcapi::srf_return_next(flinfo, fcinfo, d))
        }
        None => Ok(funcapi::srf_return_done(flinfo, fcinfo)),
    }
}

fn tablespace_warning(msg: String) -> PgResult<()> {
    elog::ereport(types_error::WARNING)
        .errmsg(msg)
        .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "pg_tablespace_databases"))
}

#[track_caller]
#[cold]
#[inline(never)]
fn open_dir_err(e: &std::io::Error, location: &str) -> Box<PgError> {
    elog::ereport(types_error::ERROR)
        .with_saved_errno(e.raw_os_error().unwrap_or(0))
        .errcode_for_file_access()
        .errmsg(format!("could not open directory \"{location}\": %m"))
        .into_error()
        .into()
}

#[track_caller]
#[cold]
#[inline(never)]
fn read_dir_err(e: &std::io::Error, location: &str) -> Box<PgError> {
    elog::ereport(types_error::ERROR)
        .with_saved_errno(e.raw_os_error().unwrap_or(0))
        .errcode_for_file_access()
        .errmsg(format!("could not read directory \"{location}\": %m"))
        .into_error()
        .into()
}

// atooid: strtoul semantics — leading digits, 0 (skipped) when non-numeric.
pub(crate) fn atooid(name: &str) -> Oid {
    let digits: &str = &name[..name.bytes().take_while(u8::is_ascii_digit).count()];
    digits.parse().unwrap_or(0)
}

fn directory_is_empty(path: &str) -> PgResult<bool> {
    let mut dir = match std::fs::read_dir(path) {
        Ok(dir) => dir,
        Err(e) => return Err(open_dir_err(&e, path)),
    };
    Ok(dir.next().is_none())
}

pub fn fc_pg_tablespace_databases(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let tablespace_oid = fcinfo.arg_oid(0);
    let flinfo = flinfo.expect("pg_tablespace_databases: NULL flinfo");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf =
        funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, funcapi::MAT_SRF_USE_EXPECTED_DESC)?;

    if tablespace_oid == GLOBALTABLESPACE_OID {
        tablespace_warning("global tablespace never has databases".to_string())?;
        return Ok(srf.finish(fcinfo));
    }

    let location = if tablespace_oid == DEFAULTTABLESPACE_OID {
        "base".to_string()
    } else {
        format!(
            "{}/{tablespace_oid}/{}",
            types_storage::PG_TBLSPC_DIR,
            types_storage::TABLESPACE_VERSION_DIRECTORY
        )
    };

    let dir = match std::fs::read_dir(&location) {
        Ok(dir) => dir,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tablespace_warning(format!("{tablespace_oid} is not a tablespace OID"))?;
            return Ok(srf.finish(fcinfo));
        }
        Err(e) => return Err(open_dir_err(&e, &location)),
    };

    for entry in dir {
        let entry = entry.map_err(|e| read_dir_err(&e, &location))?;
        let name = entry.file_name();
        let dat_oid = atooid(&name.to_string_lossy());
        if dat_oid == 0 {
            continue;
        }
        // An empty database subdir means the tablespace is not in use there.
        if directory_is_empty(&format!("{location}/{}", name.to_string_lossy()))? {
            continue;
        }
        srf.putvalues(&[Datum::from_oid(dat_oid)], &[false])?;
    }
    Ok(srf.finish(fcinfo))
}

const DEFAULTTABLESPACE_OID: Oid = 1663;
const GLOBALTABLESPACE_OID: Oid = 1664;

pub fn fc_pg_tablespace_location(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    use elog::ereport;
    use types_error::ERROR;

    let mut tablespace_oid = fcinfo.arg_oid(0);
    if tablespace_oid == InvalidOid {
        tablespace_oid = init_small::globals::MyDatabaseTableSpace();
    }
    let mcx = fcinfo.result_mcx();
    if tablespace_oid == DEFAULTTABLESPACE_OID || tablespace_oid == GLOBALTABLESPACE_OID {
        return Ok(varlena_result(varlena::cstring_to_text(mcx, b"")?));
    }
    let sourcepath = format!("pg_tblspc/{tablespace_oid}");
    let md = std::fs::symlink_metadata(&sourcepath).map_err(|e| -> Box<types_error::PgError> {
        ereport(ERROR)
            .with_saved_errno(e.raw_os_error().unwrap_or(0))
            .errcode_for_file_access()
            .errmsg(format!("could not stat file \"{sourcepath}\": %m"))
            .into_error()
            .into()
    })?;
    if !md.file_type().is_symlink() {
        return Ok(varlena_result(varlena::cstring_to_text(mcx, sourcepath.as_bytes())?));
    }
    let target = std::fs::read_link(&sourcepath).map_err(|e| -> Box<types_error::PgError> {
        ereport(ERROR)
            .with_saved_errno(e.raw_os_error().unwrap_or(0))
            .errcode_for_file_access()
            .errmsg(format!("could not read symbolic link \"{sourcepath}\": %m"))
            .into_error()
            .into()
    })?;
    let target = target.to_string_lossy();
    Ok(varlena_result(varlena::cstring_to_text(mcx, target.as_bytes())?))
}

pub const MISC_BUILTINS: &[FmgrBuiltin] = &[
    b(89, "pgsql_version", 0, fc_version),
    b(2855, "pg_is_other_temp_schema", 1, fc_pg_is_other_temp_schema),
    b(2854, "pg_my_temp_schema", 0, fc_pg_my_temp_schema),
    b(3163, "pg_trigger_depth", 0, fc_pg_trigger_depth),
    b(3778, "pg_tablespace_location", 1, fc_pg_tablespace_location),
    b(2918, "numerictypmodout", 1, fc_numerictypmodout),
    b(861, "current_database", 0, fc_current_database),
    b(1215, "obj_description", 2, fc_obj_description),
    b(1264, "PG_char_to_encoding", 1, fc_pg_char_to_encoding),
    b(1597, "PG_encoding_to_char", 1, fc_pg_encoding_to_char),
    b(1216, "col_description", 2, fc_col_description),
    b(2848, "pg_switch_wal", 0, fc_pg_switch_wal),
    b(2850, "pg_walfile_name_offset", 1, fc_pg_walfile_name_offset),
    b(2851, "pg_walfile_name", 1, fc_pg_walfile_name),
    b(6213, "pg_split_walfile_name", 1, fc_pg_split_walfile_name),
    b(2849, "pg_current_wal_lsn", 0, fc_pg_current_wal_lsn),
    b(2852, "pg_current_wal_insert_lsn", 0, fc_pg_current_wal_insert_lsn),
    b(3330, "pg_current_wal_flush_lsn", 0, fc_pg_current_wal_flush_lsn),
    b(3165, "pg_wal_lsn_diff", 2, fc_pg_wal_lsn_diff),
    b(3810, "pg_is_in_recovery", 0, fc_pg_is_in_recovery),
    b(3820, "pg_last_wal_receive_lsn", 0, fc_pg_last_wal_receive_lsn),
    b(3821, "pg_last_wal_replay_lsn", 0, fc_pg_last_wal_replay_lsn),
    b(3830, "pg_last_xact_replay_timestamp", 0, fc_pg_last_xact_replay_timestamp),
    b(3071, "pg_wal_replay_pause", 0, fc_pg_wal_replay_pause),
    b(3072, "pg_wal_replay_resume", 0, fc_pg_wal_replay_resume),
    b(3073, "pg_is_wal_replay_paused", 0, fc_pg_is_wal_replay_paused),
    b(1137, "pg_get_wal_replay_pause_state", 0, fc_pg_get_wal_replay_pause_state),
    b(3098, "pg_create_restore_point", 1, fc_pg_create_restore_point),
    b(6305, "pg_log_standby_snapshot", 0, fc_pg_log_standby_snapshot),
    b(3436, "pg_promote", 2, fc_pg_promote),
    b(2172, "pg_backup_start", 2, fc_pg_backup_start),
    b(2739, "pg_backup_stop", 1, fc_pg_backup_stop),
    b(6311, "system_user", 0, fc_system_user),
    b(810, "pg_client_encoding", 0, fc_pg_client_encoding),
    b(2034, "pg_conf_load_time", 0, fc_pg_conf_load_time),
    b(315, "pg_jit_available", 0, fc_pg_jit_available),
    b(6323, "pg_get_wal_summarizer_state", 0, fc_pg_get_wal_summarizer_state),
    b(6241, "pg_stop_making_pinned_objects", 0, fc_pg_stop_making_pinned_objects),
    FmgrBuiltin {
        foid: 3800,
        name: "pg_current_logfile",
        nargs: 0,
        strict: false,
        retset: false,
        func: fc_pg_current_logfile,
    },
    FmgrBuiltin {
        foid: 3801,
        name: "pg_current_logfile_1arg",
        nargs: 1,
        strict: false,
        retset: false,
        func: fc_pg_current_logfile_1arg,
    },
    FmgrBuiltin {
        foid: 1686,
        name: "pg_get_keywords",
        nargs: 0,
        strict: true,
        retset: true,
        func: fc_pg_get_keywords,
    },
    b(1993, "shobj_description", 2, fc_shobj_description),
    b(1268, "parse_ident", 2, fc_parse_ident),
    FmgrBuiltin {
        foid: 2556,
        name: "pg_tablespace_databases",
        nargs: 1,
        strict: true,
        retset: true,
        func: fc_pg_tablespace_databases,
    },
    FmgrBuiltin {
        foid: 6159,
        name: "pg_get_catalog_foreign_keys",
        nargs: 0,
        strict: true,
        retset: true,
        func: fc_pg_get_catalog_foreign_keys,
    },
];
