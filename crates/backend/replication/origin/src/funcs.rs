// SQL functions of origin.c (pg_replication_origin_* + status SRF); short
// wrappers over the engine, registered as late builtins.
#![allow(non_snake_case)]

use datum::Datum;
use elog::ereport;
use types_core::{InvalidRepOriginId, InvalidXLogRecPtr, XLogRecPtr};
use types_error::{PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_RESERVED_NAME, ERROR};
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData};

use crate::{
    is_reserved_origin_name, loc, replorigin_advance, replorigin_by_name, replorigin_by_oid,
    replorigin_check_prerequisites, replorigin_create, replorigin_drop_by_name,
    replorigin_get_progress, replorigin_session_get_progress, replorigin_session_is_setup,
    replorigin_session_origin, replorigin_session_reset, replorigin_session_setup,
    set_replorigin_session_origin, set_replorigin_session_origin_lsn,
    set_replorigin_session_origin_timestamp, show_status_rows,
};

fn arg_text_string(fcinfo: &mut FunctionCallInfoBaseData, i: usize) -> PgResult<String> {
    // SAFETY: strict fn — the arg is a non-null text varlena.
    let name = unsafe { fcinfo.arg_varlena_packed(i)? };
    Ok(String::from_utf8_lossy(name.data()).into_owned())
}

pub fn fc_pg_replication_origin_create(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    replorigin_check_prerequisites(false, false)?;
    let name = arg_text_string(fcinfo, 0)?;

    // "any"/"none" are reserved for subscription options, "pg_*" for internal
    // use.
    if catalog::IsReservedName(&name) || is_reserved_origin_name(&name) {
        ereport(ERROR)
            .errcode(ERRCODE_RESERVED_NAME)
            .errmsg(format!("replication origin name \"{name}\" is reserved"))
            .errdetail(
                "Origin names \"any\", \"none\", and names starting with \"pg_\" are reserved.",
            )
            .finish(loc("pg_replication_origin_create"))?;
        unreachable!();
    }

    let roident = replorigin_create(fcinfo.result_mcx(), &name)?;
    Ok(Datum::from_oid(roident as u32))
}

pub fn fc_pg_replication_origin_drop(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    replorigin_check_prerequisites(false, false)?;
    let name = arg_text_string(fcinfo, 0)?;
    replorigin_drop_by_name(fcinfo.result_mcx(), &name, false, true)?;
    Ok(Datum::from_usize(0))
}

pub fn fc_pg_replication_origin_oid(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    replorigin_check_prerequisites(false, false)?;
    let name = arg_text_string(fcinfo, 0)?;
    let roident = replorigin_by_name(&name, true)?;
    if roident != InvalidRepOriginId {
        return Ok(Datum::from_oid(roident as u32));
    }
    Ok(fcinfo.return_null())
}

pub fn fc_pg_replication_origin_session_setup(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    replorigin_check_prerequisites(true, false)?;
    let name = arg_text_string(fcinfo, 0)?;
    let origin = replorigin_by_name(&name, false)?;
    replorigin_session_setup(origin, 0)?;
    set_replorigin_session_origin(origin);
    Ok(Datum::from_usize(0))
}

pub fn fc_pg_replication_origin_session_reset(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    replorigin_check_prerequisites(true, false)?;
    replorigin_session_reset()?;
    set_replorigin_session_origin(InvalidRepOriginId);
    set_replorigin_session_origin_lsn(InvalidXLogRecPtr);
    set_replorigin_session_origin_timestamp(0);
    Ok(Datum::from_usize(0))
}

pub fn fc_pg_replication_origin_session_is_setup(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    replorigin_check_prerequisites(false, false)?;
    Ok(Datum::from_bool(replorigin_session_origin() != InvalidRepOriginId))
}

pub fn fc_pg_replication_origin_session_progress(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    replorigin_check_prerequisites(true, false)?;
    let flush = fcinfo.arg_bool(0);
    if !replorigin_session_is_setup() {
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("no replication origin is configured")
            .finish(loc("pg_replication_origin_session_progress"))?;
        unreachable!();
    }
    let remote_lsn = replorigin_session_get_progress(flush)?;
    if remote_lsn == InvalidXLogRecPtr {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_usize(remote_lsn as usize))
}

pub fn fc_pg_replication_origin_xact_setup(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    replorigin_check_prerequisites(true, false)?;
    let location = fcinfo.arg_i64(0) as XLogRecPtr;
    if !replorigin_session_is_setup() {
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("no replication origin is configured")
            .finish(loc("pg_replication_origin_xact_setup"))?;
        unreachable!();
    }
    set_replorigin_session_origin_lsn(location);
    set_replorigin_session_origin_timestamp(fcinfo.arg_i64(1));
    Ok(Datum::from_usize(0))
}

pub fn fc_pg_replication_origin_xact_reset(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    replorigin_check_prerequisites(true, false)?;
    set_replorigin_session_origin_lsn(InvalidXLogRecPtr);
    set_replorigin_session_origin_timestamp(0);
    Ok(Datum::from_usize(0))
}

pub fn fc_pg_replication_origin_advance(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    replorigin_check_prerequisites(true, false)?;

    let name = arg_text_string(fcinfo, 0)?;
    let remote_commit = fcinfo.arg_i64(1) as XLogRecPtr;

    // Lock to prevent the replication origin from vanishing.
    lmgr::LockRelationOid(catalog::ReplicationOriginRelationId, types_rel::RowExclusiveLock)?;

    let node = replorigin_by_name(&name, false)?;

    // Can't sensibly pass a local commit to be flushed at checkpoint — this
    // xact hasn't committed yet: initial-state setup only, not replay.
    replorigin_advance(node, remote_commit, InvalidXLogRecPtr, true, true)?;

    lmgr::UnlockRelationOid(catalog::ReplicationOriginRelationId, types_rel::RowExclusiveLock)?;
    Ok(Datum::from_usize(0))
}

pub fn fc_pg_replication_origin_progress(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    replorigin_check_prerequisites(true, true)?;
    let name = arg_text_string(fcinfo, 0)?;
    let flush = fcinfo.arg_bool(1);
    let roident = replorigin_by_name(&name, false)?;
    let remote_lsn = replorigin_get_progress(roident, flush)?;
    if remote_lsn == InvalidXLogRecPtr {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_usize(remote_lsn as usize))
}

pub fn fc_pg_show_replication_origin_status(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_show_replication_origin_status requires flinfo");
    let ctx = mcx::MemoryContext::new("pg_show_replication_origin_status");
    let mcx = ctx.mcx();

    // We want to return 0 rows if max_active_replication_origins is 0.
    replorigin_check_prerequisites(false, true)?;

    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    for (roident, remote_lsn, local_lsn) in show_status_rows()? {
        let mut values = [Datum::null(); 4];
        let mut nulls = [true; 4];

        values[0] = Datum::from_oid(roident as u32);
        nulls[0] = false;

        // The origin may be dropped concurrently; silently accept that.
        if let Some(roname) = replorigin_by_oid(mcx, roident, true)? {
            let img = varlena::cstring_to_text(mcx, roname.as_bytes())?.into_image().leak();
            values[1] = Datum::from_usize(img.as_ptr() as usize);
            nulls[1] = false;
        }

        values[2] = Datum::from_usize(remote_lsn as usize);
        nulls[2] = false;
        values[3] = Datum::from_usize(local_lsn as usize);
        nulls[3] = false;

        srf.putvalues(&values, &nulls)?;
    }

    Ok(srf.finish(fcinfo))
}

// pg_proc.dat oids 6003..6014.
pub const ORIGIN_BUILTINS: &[FmgrBuiltin] = &[
    FmgrBuiltin {
        foid: 6003,
        name: "pg_replication_origin_create",
        nargs: 1,
        strict: true,
        retset: false,
        func: fc_pg_replication_origin_create,
    },
    FmgrBuiltin {
        foid: 6004,
        name: "pg_replication_origin_drop",
        nargs: 1,
        strict: true,
        retset: false,
        func: fc_pg_replication_origin_drop,
    },
    FmgrBuiltin {
        foid: 6005,
        name: "pg_replication_origin_oid",
        nargs: 1,
        strict: true,
        retset: false,
        func: fc_pg_replication_origin_oid,
    },
    FmgrBuiltin {
        foid: 6006,
        name: "pg_replication_origin_session_setup",
        nargs: 1,
        strict: true,
        retset: false,
        func: fc_pg_replication_origin_session_setup,
    },
    FmgrBuiltin {
        foid: 6007,
        name: "pg_replication_origin_session_reset",
        nargs: 0,
        strict: true,
        retset: false,
        func: fc_pg_replication_origin_session_reset,
    },
    FmgrBuiltin {
        foid: 6008,
        name: "pg_replication_origin_session_is_setup",
        nargs: 0,
        strict: true,
        retset: false,
        func: fc_pg_replication_origin_session_is_setup,
    },
    FmgrBuiltin {
        foid: 6009,
        name: "pg_replication_origin_session_progress",
        nargs: 1,
        strict: true,
        retset: false,
        func: fc_pg_replication_origin_session_progress,
    },
    FmgrBuiltin {
        foid: 6010,
        name: "pg_replication_origin_xact_setup",
        nargs: 2,
        strict: true,
        retset: false,
        func: fc_pg_replication_origin_xact_setup,
    },
    FmgrBuiltin {
        foid: 6011,
        name: "pg_replication_origin_xact_reset",
        nargs: 0,
        strict: true,
        retset: false,
        func: fc_pg_replication_origin_xact_reset,
    },
    FmgrBuiltin {
        foid: 6012,
        name: "pg_replication_origin_advance",
        nargs: 2,
        strict: true,
        retset: false,
        func: fc_pg_replication_origin_advance,
    },
    FmgrBuiltin {
        foid: 6013,
        name: "pg_replication_origin_progress",
        nargs: 2,
        strict: true,
        retset: false,
        func: fc_pg_replication_origin_progress,
    },
    FmgrBuiltin {
        foid: 6014,
        name: "pg_show_replication_origin_status",
        nargs: 0,
        strict: false,
        retset: true,
        func: fc_pg_show_replication_origin_status,
    },
];
