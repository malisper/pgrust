use ::datum::Datum;
use ::types_core::BackendType;
use ::types_error::PgResult;
use ::types_fmgr::{byref_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use pgstat::io::{
    io_context_from_index, pgstat_get_io_context_name, pgstat_get_io_object_name,
    pgstat_tracks_io_bktype, pgstat_tracks_io_object, pgstat_tracks_io_op, IOObject, IOOp,
    PgStat_BktypeIO, IOCONTEXT_NUM_TYPES, IOOP_ALL,
};
use pgstat::wal::PgStat_WalCounters;

use crate::activity::{aux_pid_get_proc, text_datum};

const IO_NUM_COLUMNS: usize = 20;
const IO_COL_RESET_TIME: usize = 19;
const PG_STAT_WAL_COLS: usize = 5;
const PG_STAT_GET_SLRU_COLS: usize = 9;

const IO_OBJECTS: [IOObject; 3] = [IOObject::Relation, IOObject::TempRelation, IOObject::Wal];

fn io_op_index(op: IOOp) -> usize {
    match op {
        IOOp::Read => 3,
        IOOp::Write => 6,
        IOOp::Writeback => 9,
        IOOp::Extend => 11,
        IOOp::Hit => 14,
        IOOp::Evict => 15,
        IOOp::Reuse => 16,
        IOOp::Fsync => 17,
    }
}

fn io_byte_index(op: IOOp) -> Option<usize> {
    match op {
        IOOp::Read => Some(4),
        IOOp::Write => Some(7),
        IOOp::Extend => Some(12),
        _ => None,
    }
}

fn io_time_index(op: IOOp) -> Option<usize> {
    match op {
        IOOp::Read => Some(5),
        IOOp::Write => Some(8),
        IOOp::Writeback => Some(10),
        IOOp::Extend => Some(13),
        IOOp::Fsync => Some(18),
        _ => None,
    }
}

fn numeric_i64_datum(fcinfo: &Fcinfo, v: i64) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), adt_numeric::int64_to_numeric(v).as_bytes())
}

fn numeric_u64_datum(fcinfo: &Fcinfo, v: u64) -> PgResult<Datum> {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    let mut v = v;
    loop {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    let s = core::str::from_utf8(&buf[i..]).expect("ascii digits");
    let img = adt_numeric::numeric_in(s, -1, None)?.expect("digits always parse");
    byref_result(fcinfo.result_mcx(), img.as_bytes())
}

// C GetNumberFromPGProc: index of the entry within ProcGlobal->allProcs.
fn get_number_from_pgproc(proc: &types_storage::storage::PGPROC) -> i32 {
    let procs = lmgr_proc::ProcGlobal().allProcs;
    ((proc as *const _ as usize - procs.as_ptr() as usize)
        / core::mem::size_of::<types_storage::storage::PGPROC>()) as i32
}

pub(crate) fn resolve_backend_proc_number(pid: i32) -> Option<i32> {
    let mut proc = procarray::BackendPidGetProc(pid);
    if proc.is_none() {
        proc = aux_pid_get_proc(pid);
    }
    Some(get_number_from_pgproc(proc?))
}

fn fetch_stat_backend_by_pid(pid: i32) -> Option<(pgstat::backend::PgStat_Backend, BackendType)> {
    let proc_number = resolve_backend_proc_number(pid)?;
    let beentry = backend_status::pgstat_get_beentry_by_proc_number(proc_number)?;
    if !pgstat::backend::pgstat_tracks_backend_bktype(beentry.st_backendType) {
        return None;
    }
    if beentry.st_procpid != pid {
        return None;
    }
    let stats = pgstat::backend::pgstat_fetch_stat_backend(proc_number)?;
    Some((stats, beentry.st_backendType))
}

fn io_build_tuples(
    fcinfo: &Fcinfo,
    srf: &mut funcapi::MaterializedSRF<'_>,
    bktype_stats: &PgStat_BktypeIO,
    bktype: BackendType,
    stat_reset_timestamp: i64,
) -> PgResult<()> {
    let bktype_desc = text_datum(fcinfo, miscinit::GetBackendTypeDesc(bktype))?;

    for (o, obj) in IO_OBJECTS.into_iter().enumerate() {
        let obj_name = pgstat_get_io_object_name(obj);

        for c in 0..IOCONTEXT_NUM_TYPES {
            let ctx = io_context_from_index(c);

            if !pgstat_tracks_io_object(bktype, obj, ctx) {
                continue;
            }

            let mut values = [Datum::from_usize(0); IO_NUM_COLUMNS];
            let mut nulls = [false; IO_NUM_COLUMNS];

            values[0] = bktype_desc;
            values[1] = text_datum(fcinfo, obj_name)?;
            values[2] = text_datum(fcinfo, pgstat_get_io_context_name(ctx))?;
            if stat_reset_timestamp != 0 {
                values[IO_COL_RESET_TIME] = Datum::from_i64(stat_reset_timestamp);
            } else {
                nulls[IO_COL_RESET_TIME] = true;
            }

            for (p, op) in IOOP_ALL.into_iter().enumerate() {
                let op_idx = io_op_index(op);

                if pgstat_tracks_io_op(bktype, obj, ctx, op) {
                    values[op_idx] = Datum::from_i64(bktype_stats.counts[o][c][p]);
                } else {
                    nulls[op_idx] = true;
                }

                if !nulls[op_idx] {
                    if let Some(time_idx) = io_time_index(op) {
                        // times stored in microseconds, displayed milliseconds
                        values[time_idx] =
                            Datum::from_f64(bktype_stats.times[o][c][p] as f64 * 0.001);
                    }
                    if let Some(byte_idx) = io_byte_index(op) {
                        values[byte_idx] =
                            numeric_i64_datum(fcinfo, bktype_stats.bytes[o][c][p] as i64)?;
                    }
                } else {
                    if let Some(time_idx) = io_time_index(op) {
                        nulls[time_idx] = true;
                    }
                    if let Some(byte_idx) = io_byte_index(op) {
                        nulls[byte_idx] = true;
                    }
                }
            }

            srf.putvalues(&values, &nulls)?;
        }
    }
    Ok(())
}

pub fn fc_pg_stat_get_io(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_stat_get_io: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    let io = pgstat::io::pgstat_fetch_stat_io();

    for (i, bktype) in BackendType::ALL.into_iter().enumerate() {
        let bktype_stats = &io.stats[i];

        debug_assert!(pgstat::io::pgstat_bktype_io_stats_valid(bktype_stats, bktype));

        if !pgstat_tracks_io_bktype(bktype) {
            continue;
        }

        io_build_tuples(fcinfo, &mut srf, bktype_stats, bktype, io.stat_reset_timestamp)?;
    }

    Ok(srf.finish(fcinfo))
}

pub fn fc_pg_stat_get_backend_io(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_stat_get_backend_io: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    let pid = fcinfo.args_n::<1>()[0].value.as_i32();
    let Some((backend_stats, bktype)) = fetch_stat_backend_by_pid(pid) else {
        return Ok(srf.finish(fcinfo));
    };

    debug_assert!(pgstat::io::pgstat_bktype_io_stats_valid(&backend_stats.io_stats, bktype));

    io_build_tuples(
        fcinfo,
        &mut srf,
        &backend_stats.io_stats,
        bktype,
        backend_stats.stat_reset_timestamp,
    )?;
    Ok(srf.finish(fcinfo))
}

fn wal_build_tuple(
    flinfo: &FmgrInfo,
    fcinfo: &mut Fcinfo,
    wal_counters: PgStat_WalCounters,
    stat_reset_timestamp: i64,
) -> PgResult<Datum> {
    let mut values = [Datum::from_usize(0); PG_STAT_WAL_COLS];
    let mut nulls = [false; PG_STAT_WAL_COLS];

    values[0] = Datum::from_i64(wal_counters.wal_records);
    values[1] = Datum::from_i64(wal_counters.wal_fpi);
    values[2] = numeric_u64_datum(fcinfo, wal_counters.wal_bytes)?;
    values[3] = Datum::from_i64(wal_counters.wal_buffers_full);
    if stat_reset_timestamp != 0 {
        values[4] = Datum::from_i64(stat_reset_timestamp);
    } else {
        nulls[4] = true;
    }

    let mcx = fcinfo.result_mcx();
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    debug_assert_eq!(resolved.class, funcapi::TypeFuncClass::Composite);
    let tupdesc = resolved.result_tuple_desc.expect("composite result carries a tupdesc");
    let tup = heaptuple::heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    let d = Datum::from_usize(tup.header_ptr() as usize);
    core::mem::forget(tup); // leak into the arming context (C palloc ownership)
    Ok(d)
}

pub fn fc_pg_stat_get_wal(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_stat_get_wal: resolved FmgrInfo required");
    let wal_stats = pgstat::wal::pgstat_fetch_stat_wal();
    wal_build_tuple(flinfo, fcinfo, wal_stats.wal_counters, wal_stats.stat_reset_timestamp)
}

pub fn fc_pg_stat_get_backend_wal(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_stat_get_backend_wal: resolved FmgrInfo required");
    let pid = fcinfo.args_n::<1>()[0].value.as_i32();
    let Some((backend_stats, _)) = fetch_stat_backend_by_pid(pid) else {
        return Ok(fcinfo.return_null());
    };
    wal_build_tuple(
        flinfo,
        fcinfo,
        backend_stats.wal_counters,
        backend_stats.stat_reset_timestamp,
    )
}

pub fn fc_pg_stat_get_slru(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_stat_get_slru: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    let stats = pgstat::slru::pgstat_fetch_slru();

    let mut i = 0i32;
    while let Some(name) = pgstat::slru::pgstat_get_slru_name(i) {
        let stat = stats[i as usize];
        let values = [
            text_datum(fcinfo, name)?,
            Datum::from_i64(stat.blocks_zeroed),
            Datum::from_i64(stat.blocks_hit),
            Datum::from_i64(stat.blocks_read),
            Datum::from_i64(stat.blocks_written),
            Datum::from_i64(stat.blocks_exists),
            Datum::from_i64(stat.flush),
            Datum::from_i64(stat.truncate),
            Datum::from_i64(stat.stat_reset_timestamp),
        ];
        srf.putvalues(&values, &[false; PG_STAT_GET_SLRU_COLS])?;
        i += 1;
    }

    Ok(srf.finish(fcinfo))
}
