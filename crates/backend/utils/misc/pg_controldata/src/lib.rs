//! utils/misc/pg_controldata.c — SQL access to the on-disk control file.

#![allow(non_snake_case)]

use datum::Datum;
use types_error::{PgError, PgResult};
use types_fmgr::{varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use controldata_utils::{get_controlfile, ControlFileData};
use lwlock::{LWLockAcquire, LWLockRelease, LW_SHARED};
use transam_xlog::ctl::ControlFileLock;

const SECS_PER_DAY: i64 = 86400;
const USECS_PER_SEC: i64 = 1_000_000;
const POSTGRES_EPOCH_JDATE: i64 = 2451545;
const UNIX_EPOCH_JDATE: i64 = 2440588;

fn time_t_to_timestamptz(t: i64) -> i64 {
    (t - (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY) * USECS_PER_SEC
}

// C: LWLockAcquire(ControlFileLock, LW_SHARED); get_controlfile(DataDir, &crc_ok).
fn read_controlfile() -> PgResult<ControlFileData> {
    let datadir = init_small::globals::DataDir()
        .expect("pg_controldata functions require DataDir")
        .to_string();
    LWLockAcquire(ControlFileLock(), LW_SHARED, init_small::globals::MyProcNumber())?;
    let read = get_controlfile(&datadir);
    LWLockRelease(ControlFileLock())?;
    let (control_file, crc_ok) = read?;
    if !crc_ok {
        return Err(Box::new(PgError::error(
            "calculated CRC checksum does not match value stored in file",
        )));
    }
    Ok(control_file)
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
        return Err(Box::new(PgError::error("return type must be a row type")));
    }
    let tupdesc = resolved.result_tuple_desc.expect("composite result has tupdesc");
    let tup = heaptuple::heap_form_tuple(mcx, &tupdesc, values, isnull)?;
    let d = Datum::from_usize(tup.header_ptr() as usize);
    core::mem::forget(tup); // leak into the arming context (C palloc ownership)
    Ok(d)
}

fn text_datum(fcinfo: &Fcinfo, s: &str) -> PgResult<Datum> {
    Ok(varlena_result(varlena::cstring_to_text(fcinfo.result_mcx(), s.as_bytes())?))
}

pub fn fc_pg_control_system(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_control_system: NULL flinfo");
    let cf = read_controlfile()?;
    let values = [
        Datum::from_i32(cf.pg_control_version as i32),
        Datum::from_i32(cf.catalog_version_no as i32),
        Datum::from_i64(cf.system_identifier as i64),
        Datum::from_i64(time_t_to_timestamptz(cf.time)),
    ];
    composite_result(flinfo, fcinfo, &values, &[false; 4])
}

pub fn fc_pg_control_checkpoint(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_control_checkpoint: NULL flinfo");
    let cf = read_controlfile()?;
    let cp = &cf.checkPointCopy;

    let wal_segsz = transam_xlog::wal_segment_size();
    let segno = cp.redo / wal_segsz as u64;
    let xlogfilename = transam_xlog::XLogFileName(cp.ThisTimeLineID, segno, wal_segsz);

    let values = [
        Datum::from_u64(cf.checkPoint),
        Datum::from_u64(cp.redo),
        text_datum(fcinfo, &xlogfilename)?,
        Datum::from_i32(cp.ThisTimeLineID as i32),
        Datum::from_i32(cp.PrevTimeLineID as i32),
        Datum::from_bool(cp.fullPageWrites),
        text_datum(fcinfo, &format!("{}:{}", cp.nextXid.epoch(), cp.nextXid.xid()))?,
        Datum::from_oid(cp.nextOid),
        Datum::from_transaction_id(cp.nextMulti),
        Datum::from_transaction_id(cp.nextMultiOffset),
        Datum::from_transaction_id(cp.oldestXid),
        Datum::from_oid(cp.oldestXidDB),
        Datum::from_transaction_id(cp.oldestActiveXid),
        Datum::from_transaction_id(cp.oldestMulti),
        Datum::from_oid(cp.oldestMultiDB),
        Datum::from_transaction_id(cp.oldestCommitTsXid),
        Datum::from_transaction_id(cp.newestCommitTsXid),
        Datum::from_i64(time_t_to_timestamptz(cp.time)),
    ];
    composite_result(flinfo, fcinfo, &values, &[false; 18])
}

pub fn fc_pg_control_recovery(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_control_recovery: NULL flinfo");
    let cf = read_controlfile()?;
    let values = [
        Datum::from_u64(cf.minRecoveryPoint),
        Datum::from_i32(cf.minRecoveryPointTLI as i32),
        Datum::from_u64(cf.backupStartPoint),
        Datum::from_u64(cf.backupEndPoint),
        Datum::from_bool(cf.backupEndRequired),
    ];
    composite_result(flinfo, fcinfo, &values, &[false; 5])
}

pub fn fc_pg_control_init(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_control_init: NULL flinfo");
    let cf = read_controlfile()?;
    let values = [
        Datum::from_i32(cf.maxAlign as i32),
        Datum::from_i32(cf.blcksz as i32),
        Datum::from_i32(cf.relseg_size as i32),
        Datum::from_i32(cf.xlog_blcksz as i32),
        Datum::from_i32(cf.xlog_seg_size as i32),
        Datum::from_i32(cf.nameDataLen as i32),
        Datum::from_i32(cf.indexMaxKeys as i32),
        Datum::from_i32(cf.toast_max_chunk_size as i32),
        Datum::from_i32(cf.loblksize as i32),
        Datum::from_bool(cf.float8ByVal),
        Datum::from_i32(cf.data_checksum_version as i32),
        Datum::from_bool(cf.default_char_signedness),
    ];
    composite_result(flinfo, fcinfo, &values, &[false; 12])
}

pub const PG_CONTROLDATA_BUILTINS: &[FmgrBuiltin] = &[
    FmgrBuiltin {
        foid: 3441,
        name: "pg_control_system",
        nargs: 0,
        strict: true,
        retset: false,
        func: fc_pg_control_system,
    },
    FmgrBuiltin {
        foid: 3442,
        name: "pg_control_checkpoint",
        nargs: 0,
        strict: true,
        retset: false,
        func: fc_pg_control_checkpoint,
    },
    FmgrBuiltin {
        foid: 3443,
        name: "pg_control_recovery",
        nargs: 0,
        strict: true,
        retset: false,
        func: fc_pg_control_recovery,
    },
    FmgrBuiltin {
        foid: 3444,
        name: "pg_control_init",
        nargs: 0,
        strict: true,
        retset: false,
        func: fc_pg_control_init,
    },
];
