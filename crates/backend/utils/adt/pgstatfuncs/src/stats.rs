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

fn record_datum(
    flinfo: &FmgrInfo,
    fcinfo: &Fcinfo,
    values: &[Datum],
    nulls: &[bool],
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    debug_assert_eq!(resolved.class, funcapi::TypeFuncClass::Composite);
    let tupdesc = resolved.result_tuple_desc.expect("composite result carries a tupdesc");
    let tup = heaptuple::heap_form_tuple(mcx, &tupdesc, values, nulls)?;
    let d = Datum::from_usize(tup.header_ptr() as usize);
    core::mem::forget(tup); // leak into the arming context (C palloc ownership)
    Ok(d)
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

    record_datum(flinfo, fcinfo, &values, &nulls)
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

pub fn fc_pg_stat_get_progress_info(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    const COLS: usize = backend_status::PGSTAT_NUM_PROGRESS_PARAM + 3;
    use backend_status::{
        PROGRESS_COMMAND_ANALYZE, PROGRESS_COMMAND_BASEBACKUP, PROGRESS_COMMAND_CLUSTER,
        PROGRESS_COMMAND_COPY, PROGRESS_COMMAND_CREATE_INDEX, PROGRESS_COMMAND_VACUUM,
    };
    let flinfo = flinfo.expect("pg_stat_get_progress_info: resolved FmgrInfo required");

    let cmd = crate::arg_text_str(fcinfo, 0)?;
    let cmdtype = if cmd.eq_ignore_ascii_case("VACUUM") {
        PROGRESS_COMMAND_VACUUM
    } else if cmd.eq_ignore_ascii_case("ANALYZE") {
        PROGRESS_COMMAND_ANALYZE
    } else if cmd.eq_ignore_ascii_case("CLUSTER") {
        PROGRESS_COMMAND_CLUSTER
    } else if cmd.eq_ignore_ascii_case("CREATE INDEX") {
        PROGRESS_COMMAND_CREATE_INDEX
    } else if cmd.eq_ignore_ascii_case("BASEBACKUP") {
        PROGRESS_COMMAND_BASEBACKUP
    } else if cmd.eq_ignore_ascii_case("COPY") {
        PROGRESS_COMMAND_COPY
    } else {
        return Err(Box::new(
            ::types_error::PgError::error(format!("invalid command name: \"{cmd}\""))
                .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    };

    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    let num_backends = backend_status::pgstat_fetch_stat_numbackends();
    for curr in 1..=num_backends {
        let Some(be) = backend_status::pgstat_get_local_beentry_by_index(curr) else {
            continue;
        };
        if be.st_progress_command != cmdtype {
            continue;
        }

        let mut values = [Datum::from_usize(0); COLS];
        let mut nulls = [false; COLS];
        values[0] = Datum::from_i32(be.st_procpid);
        values[1] = Datum::from_oid(be.st_databaseid);
        if crate::activity::has_pgstat_permissions(be.st_userid)? {
            values[2] = Datum::from_oid(be.st_progress_command_target);
            for (i, p) in be.st_progress_param.into_iter().enumerate() {
                values[i + 3] = Datum::from_i64(p);
            }
        } else {
            nulls[2] = true;
            for n in nulls[3..].iter_mut() {
                *n = true;
            }
        }
        srf.putvalues(&values, &nulls)?;
    }

    Ok(srf.finish(fcinfo))
}

pub fn fc_pg_stat_get_backend_subxact(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_stat_get_backend_subxact: resolved FmgrInfo required");
    let proc_number = fcinfo.args_n::<1>()[0].value.as_i32();
    let (values, nulls) = match backend_status::pgstat_get_beentry_by_proc_number(proc_number) {
        Some(be) => (
            [
                Datum::from_i32(be.backend_subxact_count),
                Datum::from_bool(be.backend_subxact_overflowed),
            ],
            [false, false],
        ),
        None => ([Datum::from_usize(0); 2], [true, true]),
    };
    record_datum(flinfo, fcinfo, &values, &nulls)
}

fn xfn_str(buf: &[u8]) -> &str {
    let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    core::str::from_utf8(&buf[..len]).expect("WAL file names are ASCII")
}

const PG_STAT_GET_WAL_RECEIVER_COLS: usize = 16;

// walreceiver.c:pg_stat_get_wal_receiver. C PG_RETURN_NULL()s when no
// receiver is active; here that is an all-null record — the view filters
// `WHERE s.pid IS NOT NULL`, so both yield zero rows.
pub fn fc_pg_stat_get_wal_receiver(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_stat_get_wal_receiver: resolved FmgrInfo required");
    let mut values = [Datum::from_usize(0); PG_STAT_GET_WAL_RECEIVER_COLS];
    let mut nulls = [true; PG_STAT_GET_WAL_RECEIVER_COLS];

    let snap = if walreceiverfuncs_seams::pg_stat_wal_receiver_snapshot::is_installed() {
        walreceiverfuncs_seams::pg_stat_wal_receiver_snapshot::call()
    } else {
        None
    };
    let Some(snap) = snap else {
        return record_datum(flinfo, fcinfo, &values, &nulls);
    };

    values[0] = Datum::from_i32(snap.pid);
    nulls[0] = false;
    let uid = miscinit::GetUserId();
    if !acl_seams::has_privs_of_role::call(uid, crate::activity::ROLE_PG_READ_ALL_STATS)? {
        return record_datum(flinfo, fcinfo, &values, &nulls);
    }

    values[1] = crate::activity::text_datum(fcinfo, snap.state)?;
    nulls[1] = false;
    if snap.receive_start_lsn != 0 {
        values[2] = Datum::from_i64(snap.receive_start_lsn as i64);
        nulls[2] = false;
    }
    values[3] = Datum::from_i32(snap.receive_start_tli as i32);
    nulls[3] = false;
    if snap.written_lsn != 0 {
        values[4] = Datum::from_i64(snap.written_lsn as i64);
        nulls[4] = false;
    }
    if snap.flushed_lsn != 0 {
        values[5] = Datum::from_i64(snap.flushed_lsn as i64);
        nulls[5] = false;
    }
    values[6] = Datum::from_i32(snap.received_tli as i32);
    nulls[6] = false;
    if snap.last_send_time != 0 {
        values[7] = Datum::from_i64(snap.last_send_time);
        nulls[7] = false;
    }
    if snap.last_receipt_time != 0 {
        values[8] = Datum::from_i64(snap.last_receipt_time);
        nulls[8] = false;
    }
    if snap.latest_end_lsn != 0 {
        values[9] = Datum::from_i64(snap.latest_end_lsn as i64);
        nulls[9] = false;
    }
    if snap.latest_end_time != 0 {
        values[10] = Datum::from_i64(snap.latest_end_time);
        nulls[10] = false;
    }
    if !snap.slotname.is_empty() {
        values[11] = crate::activity::text_datum(fcinfo, &snap.slotname)?;
        nulls[11] = false;
    }
    if !snap.sender_host.is_empty() {
        values[12] = crate::activity::text_datum(fcinfo, &snap.sender_host)?;
        nulls[12] = false;
    }
    if snap.sender_port != 0 {
        values[13] = Datum::from_i32(snap.sender_port);
        nulls[13] = false;
    }
    if !snap.conninfo.is_empty() {
        values[14] = crate::activity::text_datum(fcinfo, &snap.conninfo)?;
        nulls[14] = false;
    }
    record_datum(flinfo, fcinfo, &values, &nulls)
}

const PG_STAT_GET_WAL_SENDERS_COLS: usize = 12;

// offset_to_interval (walsender.c:3898): TimeOffset µs → Interval datum
// (byref, layout time i64 + day i32 + month i32, typlen 16 typalign d).
fn offset_to_interval(fcinfo: &Fcinfo, diff: i64) -> PgResult<Datum> {
    let mut image = [0u8; 16];
    image[..8].copy_from_slice(&diff.to_ne_bytes());
    byref_result(fcinfo.result_mcx(), &image)
}

// walsender.c:pg_stat_get_wal_senders — one row per live WalSnd slot.
pub fn fc_pg_stat_get_wal_senders(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_stat_get_wal_senders: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    debug_assert_eq!(srf.tupdesc.natts as usize, PG_STAT_GET_WAL_SENDERS_COLS);

    let rows = if walsender_seams::pg_stat_wal_senders_snapshot::is_installed() {
        walsender_seams::pg_stat_wal_senders_snapshot::call()
    } else {
        Vec::new()
    };

    let uid = miscinit::GetUserId();
    let can_see_details =
        acl_seams::has_privs_of_role::call(uid, crate::activity::ROLE_PG_READ_ALL_STATS)?;

    for row in rows {
        let mut values = [Datum::from_usize(0); PG_STAT_GET_WAL_SENDERS_COLS];
        let mut nulls = [false; PG_STAT_GET_WAL_SENDERS_COLS];

        values[0] = Datum::from_i32(row.pid);

        if !can_see_details {
            // Only superusers and roles with privileges of pg_read_all_stats
            // can see details; others only get the pid.
            for n in nulls.iter_mut().skip(1) {
                *n = true;
            }
        } else {
            values[1] = text_datum(fcinfo, row.state)?;

            if row.sent_ptr == 0 {
                nulls[2] = true;
            }
            values[2] = Datum::from_u64(row.sent_ptr);

            if row.write == 0 {
                nulls[3] = true;
            }
            values[3] = Datum::from_u64(row.write);

            if row.flush == 0 {
                nulls[4] = true;
            }
            values[4] = Datum::from_u64(row.flush);

            if row.apply == 0 {
                nulls[5] = true;
            }
            values[5] = Datum::from_u64(row.apply);

            // A standby that never reports a flush location (e.g. a
            // pg_basebackup background process) counts as asynchronous.
            let priority = if row.flush == 0 { 0 } else { row.sync_priority };

            if row.write_lag < 0 {
                nulls[6] = true;
            } else {
                values[6] = offset_to_interval(fcinfo, row.write_lag)?;
            }
            if row.flush_lag < 0 {
                nulls[7] = true;
            } else {
                values[7] = offset_to_interval(fcinfo, row.flush_lag)?;
            }
            if row.apply_lag < 0 {
                nulls[8] = true;
            } else {
                values[8] = offset_to_interval(fcinfo, row.apply_lag)?;
            }

            values[9] = Datum::from_i32(priority);

            values[10] = text_datum(
                fcinfo,
                if priority == 0 {
                    "async"
                } else if row.is_sync_standby {
                    if row.syncrep_method_is_priority { "sync" } else { "quorum" }
                } else {
                    "potential"
                },
            )?;

            if row.reply_time == 0 {
                nulls[11] = true;
            } else {
                values[11] = Datum::from_i64(row.reply_time);
            }
        }

        srf.putvalues(&values, &nulls)?;
    }

    Ok(srf.finish(fcinfo))
}

pub fn fc_pg_stat_get_archiver(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_stat_get_archiver: resolved FmgrInfo required");
    let a = pgstat::archiver::pgstat_fetch_stat_archiver();

    let mut values = [Datum::from_usize(0); 7];
    let mut nulls = [false; 7];
    values[0] = Datum::from_i64(a.archived_count);
    match xfn_str(&a.last_archived_wal) {
        "" => nulls[1] = true,
        s => values[1] = text_datum(fcinfo, s)?,
    }
    if a.last_archived_timestamp == 0 {
        nulls[2] = true;
    } else {
        values[2] = Datum::from_i64(a.last_archived_timestamp);
    }
    values[3] = Datum::from_i64(a.failed_count);
    match xfn_str(&a.last_failed_wal) {
        "" => nulls[4] = true,
        s => values[4] = text_datum(fcinfo, s)?,
    }
    if a.last_failed_timestamp == 0 {
        nulls[5] = true;
    } else {
        values[5] = Datum::from_i64(a.last_failed_timestamp);
    }
    if a.stat_reset_timestamp == 0 {
        nulls[6] = true;
    } else {
        values[6] = Datum::from_i64(a.stat_reset_timestamp);
    }
    record_datum(flinfo, fcinfo, &values, &nulls)
}

const NAMEDATALEN: usize = 64;

// namestrcpy's byte clip, pulled back to a char boundary to stay valid UTF-8.
fn clip_slot_name(s: &str) -> &str {
    let mut end = s.len().min(NAMEDATALEN - 1);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

pub(crate) fn pgstat_fetch_replslot(
    name: &str,
) -> PgResult<Option<pgstat::replslot::PgStat_StatReplSlotEntry>> {
    pgstat::pgstat_fetch_replslot(name)
}

pub fn fc_pg_stat_get_replication_slot(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    const COLS: usize = 10;
    let flinfo = flinfo.expect("pg_stat_get_replication_slot: resolved FmgrInfo required");
    let mut namebuf = [0u8; NAMEDATALEN];
    let name_len = {
        let clipped = clip_slot_name(crate::arg_text_str(fcinfo, 0)?);
        namebuf[..clipped.len()].copy_from_slice(clipped.as_bytes());
        clipped.len()
    };
    let name = core::str::from_utf8(&namebuf[..name_len]).expect("clipped from valid UTF-8");

    // C zero-fills when the slot has no stats entry (create message lost).
    let slotent = pgstat_fetch_replslot(name)?.unwrap_or_default();

    let mut values = [Datum::from_usize(0); COLS];
    let mut nulls = [false; COLS];
    values[0] = text_datum(fcinfo, name)?;
    values[1] = Datum::from_i64(slotent.spill_txns);
    values[2] = Datum::from_i64(slotent.spill_count);
    values[3] = Datum::from_i64(slotent.spill_bytes);
    values[4] = Datum::from_i64(slotent.stream_txns);
    values[5] = Datum::from_i64(slotent.stream_count);
    values[6] = Datum::from_i64(slotent.stream_bytes);
    values[7] = Datum::from_i64(slotent.total_txns);
    values[8] = Datum::from_i64(slotent.total_bytes);
    if slotent.stat_reset_timestamp == 0 {
        nulls[9] = true;
    } else {
        values[9] = Datum::from_i64(slotent.stat_reset_timestamp);
    }
    record_datum(flinfo, fcinfo, &values, &nulls)
}

pub fn fc_pg_stat_get_subscription_stats(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    const COLS: usize = 11;
    let flinfo = flinfo.expect("pg_stat_get_subscription_stats: resolved FmgrInfo required");
    let subid = fcinfo.args_n::<1>()[0].value.as_oid();
    let subentry = pgstat::subscription::pgstat_fetch_stat_subscription(subid).unwrap_or_default();

    let mut values = [Datum::from_usize(0); COLS];
    let mut nulls = [false; COLS];
    values[0] = Datum::from_oid(subid);
    values[1] = Datum::from_i64(subentry.apply_error_count);
    values[2] = Datum::from_i64(subentry.sync_error_count);
    for (i, c) in subentry.conflict_count.into_iter().enumerate() {
        values[i + 3] = Datum::from_i64(c);
    }
    if subentry.stat_reset_timestamp == 0 {
        nulls[10] = true;
    } else {
        values[10] = Datum::from_i64(subentry.stat_reset_timestamp);
    }
    record_datum(flinfo, fcinfo, &values, &nulls)
}
