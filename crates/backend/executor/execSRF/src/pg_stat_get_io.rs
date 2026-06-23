//! `pg_stat_get_io()` (OID 6214) registered as an executor-frame
//! materialize-mode set-returning function — the `pg_stat_io` view's underlying
//! function.
//!
//! `pgstatfuncs.c`'s `pg_stat_get_io` materializes one row per
//! (BackendType, IOObject, IOContext) combination tracked for IO, fetching the
//! cumulative IO snapshot via `pgstat_fetch_stat_io()`. The 20-column
//! projection (`io_stat_col` enum + the `pgstat_get_io_{op,time,byte}_index`
//! mappings + the per-cell `pgstat_tracks_io_op` NULL gating) is the
//! pgstatfuncs.c body, ported here; the fetch substrate + the
//! `pgstat_get_io_{object,context}_name` / `pgstat_tracks_io_*` predicates are
//! the `backend-utils-activity-pgstat-io` owner's, and `GetBackendTypeDesc` is
//! miscinit's.
//!
//! `InitMaterializedSRF` with `MAT_SRF_USE_EXPECTED_DESC` takes the executor's
//! already-resolved 20-column descriptor; the rows are appended via
//! `materialized_srf_putvalues`, and the entry point returns SQL NULL.
//! Registered from [`register_pg_stat_get_io`] (called by `init_seams`); it
//! bypasses the by-OID builtin registry whose tag-only `resultinfo` cannot
//! carry the live `ReturnSetInfo` (the WONTFIX dual-home).

extern crate alloc;
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::init::{BackendType, BACKEND_NUM_TYPES};
use types_core::Oid;
use types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use ::nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_core::primitive::TimestampTz;
use types_pgstat::activity_pgstat::{
    IOContext, IOObject, IOOp, PgStat_BktypeIO, IOCONTEXT_NUM_TYPES, IOOBJECT_NUM_TYPES,
    IOOP_NUM_TYPES,
};
use types_tuple::heaptuple::Datum;

use pgstat_io as io;
use ::funcapi::srf_support::{materialized_srf_putvalues, InitMaterializedSRF};

use crate::register_srf;

/// `pg_stat_get_io()` (OID 6214).
const PG_STAT_GET_IO: Oid = 6214;
/// `pg_stat_get_backend_io(int4)` (OID 6386).
const PG_STAT_GET_BACKEND_IO: Oid = 6386;

// ---------------------------------------------------------------------------
// The `io_stat_col` enum (pgstatfuncs.c:1320) — positional column indices into
// the IO_NUM_COLUMNS-wide values/nulls arrays.
// ---------------------------------------------------------------------------

const IO_COL_INVALID: i32 = -1;
const IO_COL_BACKEND_TYPE: usize = 0;
const IO_COL_OBJECT: usize = 1;
const IO_COL_CONTEXT: usize = 2;
const IO_COL_READS: usize = 3;
const IO_COL_READ_BYTES: usize = 4;
const IO_COL_READ_TIME: usize = 5;
const IO_COL_WRITES: usize = 6;
const IO_COL_WRITE_BYTES: usize = 7;
const IO_COL_WRITE_TIME: usize = 8;
const IO_COL_WRITEBACKS: usize = 9;
const IO_COL_WRITEBACK_TIME: usize = 10;
const IO_COL_EXTENDS: usize = 11;
const IO_COL_EXTEND_BYTES: usize = 12;
const IO_COL_EXTEND_TIME: usize = 13;
const IO_COL_HITS: usize = 14;
const IO_COL_EVICTIONS: usize = 15;
const IO_COL_REUSES: usize = 16;
const IO_COL_FSYNCS: usize = 17;
const IO_COL_FSYNC_TIME: usize = 18;
const IO_COL_RESET_TIME: usize = 19;
const IO_NUM_COLUMNS: usize = 20;

/// `pgstat_get_io_op_index(io_op)` (pgstatfuncs.c:1354).
fn pgstat_get_io_op_index(io_op: IOOp) -> usize {
    match io_op {
        IOOp::IOOP_EVICT => IO_COL_EVICTIONS,
        IOOp::IOOP_EXTEND => IO_COL_EXTENDS,
        IOOp::IOOP_FSYNC => IO_COL_FSYNCS,
        IOOp::IOOP_HIT => IO_COL_HITS,
        IOOp::IOOP_READ => IO_COL_READS,
        IOOp::IOOP_REUSE => IO_COL_REUSES,
        IOOp::IOOP_WRITE => IO_COL_WRITES,
        IOOp::IOOP_WRITEBACK => IO_COL_WRITEBACKS,
    }
}

/// `pgstat_get_io_byte_index(io_op)` (pgstatfuncs.c:1385). `IO_COL_INVALID`
/// when the op is not tracked in bytes.
fn pgstat_get_io_byte_index(io_op: IOOp) -> i32 {
    match io_op {
        IOOp::IOOP_EXTEND => IO_COL_EXTEND_BYTES as i32,
        IOOp::IOOP_READ => IO_COL_READ_BYTES as i32,
        IOOp::IOOP_WRITE => IO_COL_WRITE_BYTES as i32,
        IOOp::IOOP_EVICT
        | IOOp::IOOP_FSYNC
        | IOOp::IOOP_HIT
        | IOOp::IOOP_REUSE
        | IOOp::IOOP_WRITEBACK => IO_COL_INVALID,
    }
}

/// `pgstat_get_io_time_index(io_op)` (pgstatfuncs.c:1412). `IO_COL_INVALID`
/// when the op has no associated time.
fn pgstat_get_io_time_index(io_op: IOOp) -> i32 {
    match io_op {
        IOOp::IOOP_READ => IO_COL_READ_TIME as i32,
        IOOp::IOOP_WRITE => IO_COL_WRITE_TIME as i32,
        IOOp::IOOP_WRITEBACK => IO_COL_WRITEBACK_TIME as i32,
        IOOp::IOOP_EXTEND => IO_COL_EXTEND_TIME as i32,
        IOOp::IOOP_FSYNC => IO_COL_FSYNC_TIME as i32,
        IOOp::IOOP_EVICT | IOOp::IOOP_HIT | IOOp::IOOP_REUSE => IO_COL_INVALID,
    }
}

/// `pg_stat_us_to_ms(val_ms)` (pgstatfuncs.c:1437): microseconds → milliseconds.
fn pg_stat_us_to_ms(val_ms: i64) -> f64 {
    val_ms as f64 * 0.001
}

/// `int io_object` index → `IOObject` (the loop's `io_obj` runs `0..3`).
fn io_object_from_index(i: usize) -> IOObject {
    match i {
        0 => IOObject::IOOBJECT_RELATION,
        1 => IOObject::IOOBJECT_TEMP_RELATION,
        2 => IOObject::IOOBJECT_WAL,
        _ => unreachable!("io_object index out of range"),
    }
}

/// `int io_context` index → `IOContext` (the loop's `io_context` runs `0..5`).
fn io_context_from_index(i: usize) -> IOContext {
    match i {
        0 => IOContext::IOCONTEXT_BULKREAD,
        1 => IOContext::IOCONTEXT_BULKWRITE,
        2 => IOContext::IOCONTEXT_INIT,
        3 => IOContext::IOCONTEXT_NORMAL,
        4 => IOContext::IOCONTEXT_VACUUM,
        _ => unreachable!("io_context index out of range"),
    }
}

/// `int io_op` index → `IOOp` (the loop's `io_op` runs `0..IOOP_NUM_TYPES`).
fn io_op_from_index(i: usize) -> IOOp {
    match i {
        0 => IOOp::IOOP_EVICT,
        1 => IOOp::IOOP_FSYNC,
        2 => IOOp::IOOP_HIT,
        3 => IOOp::IOOP_REUSE,
        4 => IOOp::IOOP_WRITEBACK,
        5 => IOOp::IOOP_EXTEND,
        6 => IOOp::IOOP_READ,
        7 => IOOp::IOOP_WRITE,
        _ => unreachable!("io_op index out of range"),
    }
}

/// Register `pg_stat_get_io` in the executor-frame SRF table.
pub(crate) fn register_pg_stat_get_io() {
    register_srf(PG_STAT_GET_IO, pg_stat_get_io);
    register_srf(PG_STAT_GET_BACKEND_IO, pg_stat_get_backend_io);
}

/// Build a `NUMERIC` `Datum` from a `u64` byte count. C does
/// `numeric_in(snprintf(INT64_FORMAT, byte))`; the cumulative counters are
/// non-negative, so an exact unsigned-to-numeric conversion (`int128` carrier)
/// is the faithful equivalent.
fn numeric_from_u64<'mcx>(mcx: Mcx<'mcx>, val: u64) -> PgResult<Datum<'mcx>> {
    let var = adt_numeric::convert::int128_to_numericvar(mcx, val as i128)?;
    let buf = adt_numeric::convert::make_result(mcx, &var)?;
    Ok(Datum::ByRef(buf))
}

/// `CStringGetTextDatum(s)` → a `text` varlena `Datum`.
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    varlena_seams::cstring_to_text_v::call(mcx, s)
}

/// `pg_stat_io_build_tuples(rsinfo, bktype_stats, bktype, stat_reset_timestamp)`
/// (pgstatfuncs.c:1450) — append one tuple for each (IOObject, IOContext) the
/// caller's `bktype` tracks. Shared with `pg_stat_get_backend_io`.
fn pg_stat_io_build_tuples<'mcx>(
    mcx: Mcx<'mcx>,
    rows: &mut Vec<([Datum<'mcx>; IO_NUM_COLUMNS], [bool; IO_NUM_COLUMNS])>,
    bktype_stats: &PgStat_BktypeIO,
    bktype: BackendType,
    stat_reset_timestamp: TimestampTz,
) -> PgResult<()> {
    let bktype_desc =
        text_datum(mcx, miscinit::GetBackendTypeDesc(bktype))?;

    for io_obj in 0..IOOBJECT_NUM_TYPES {
        let obj = io_object_from_index(io_obj);
        let obj_name = io::pgstat_get_io_object_name(obj);

        for io_context in 0..IOCONTEXT_NUM_TYPES {
            let ctx = io_context_from_index(io_context);
            let context_name = io::pgstat_get_io_context_name(ctx);

            // Some combinations of BackendType, IOObject, and IOContext are not
            // valid for any IOOp; omit the entire row from the view.
            if !io::pgstat_tracks_io_object(bktype, obj, ctx) {
                continue;
            }

            let mut values: [Datum<'mcx>; IO_NUM_COLUMNS] = core::array::from_fn(|_| Datum::null());
            let mut nulls = [false; IO_NUM_COLUMNS];

            values[IO_COL_BACKEND_TYPE] = bktype_desc.clone();
            values[IO_COL_CONTEXT] = text_datum(mcx, context_name)?;
            values[IO_COL_OBJECT] = text_datum(mcx, obj_name)?;
            if stat_reset_timestamp != 0 {
                values[IO_COL_RESET_TIME] = Datum::from_i64(stat_reset_timestamp);
            } else {
                nulls[IO_COL_RESET_TIME] = true;
            }

            for io_op in 0..IOOP_NUM_TYPES {
                let op = io_op_from_index(io_op);
                let op_idx = pgstat_get_io_op_index(op);
                let time_idx = pgstat_get_io_time_index(op);
                let byte_idx = pgstat_get_io_byte_index(op);

                if io::pgstat_tracks_io_op(bktype, obj, ctx, op) {
                    let count = bktype_stats.counts[io_obj][io_context][io_op];
                    values[op_idx] = Datum::from_i64(count);
                } else {
                    nulls[op_idx] = true;
                }

                if !nulls[op_idx] {
                    // not every operation is timed
                    if time_idx != IO_COL_INVALID {
                        let time = bktype_stats.times[io_obj][io_context][io_op];
                        values[time_idx as usize] = Datum::from_f64(pg_stat_us_to_ms(time));
                    }
                    // not every IO is tracked in bytes
                    if byte_idx != IO_COL_INVALID {
                        let byte = bktype_stats.bytes[io_obj][io_context][io_op];
                        values[byte_idx as usize] = numeric_from_u64(mcx, byte)?;
                    }
                } else {
                    if time_idx != IO_COL_INVALID {
                        nulls[time_idx as usize] = true;
                    }
                    if byte_idx != IO_COL_INVALID {
                        nulls[byte_idx as usize] = true;
                    }
                }
            }

            rows.push((values, nulls));
        }
    }

    Ok(())
}

/// `pg_stat_get_io(PG_FUNCTION_ARGS)` (pgstatfuncs.c:1548) over the executor
/// frame.
fn pg_stat_get_io<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_stat_get_io: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: backends_io_stats = pgstat_fetch_stat_io();
    let backends_io_stats = io::pgstat_fetch_stat_io()?;

    let mut rows: Vec<([Datum<'mcx>; IO_NUM_COLUMNS], [bool; IO_NUM_COLUMNS])> =
        Vec::new();

    for bktype_idx in 0..BACKEND_NUM_TYPES {
        let bktype = BackendType::ALL[bktype_idx];
        let bktype_stats = &backends_io_stats.stats[bktype_idx];

        // Assert(pgstat_bktype_io_stats_valid(bktype_stats, bktype)); (debug)
        debug_assert!(io::pgstat_bktype_io_stats_valid(bktype_stats, bktype));

        // For those BackendTypes without IO Operation stats, skip representing
        // them in the view altogether.
        if !io::pgstat_tracks_io_bktype(bktype) {
            continue;
        }

        pg_stat_io_build_tuples(
            mcx,
            &mut rows,
            bktype_stats,
            bktype,
            backends_io_stats.stat_reset_timestamp,
        )?;
    }

    // C: InitMaterializedSRF(fcinfo, 0). Take the executor's already-resolved
    // 20-column descriptor.
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_stat_get_io: InitMaterializedSRF establishes fcinfo->resultinfo");

    for (values, nulls) in &rows {
        materialized_srf_putvalues(rsinfo, &values[..], &nulls[..])?;
    }

    // C: return (Datum) 0 — the whole set is in the materialize tuplestore.
    fcinfo.isnull = true;
    Ok(Datum::null())
}

/// `pg_stat_get_backend_io(PG_FUNCTION_ARGS)` (pgstatfuncs.c:1589) — the IO
/// stats of one backend identified by pid, projected through the same
/// `pg_stat_io_build_tuples` body as `pg_stat_get_io`. An empty set (C
/// `return (Datum) 0` with no rows) when the pid is not a tracked backend.
fn pg_stat_get_backend_io<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_stat_get_backend_io: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: InitMaterializedSRF(fcinfo, 0); rsinfo = fcinfo->resultinfo;
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    // C: pid = PG_GETARG_INT32(0);
    //    backend_stats = pgstat_fetch_stat_backend_by_pid(pid, &bktype);
    let pid = fcinfo
        .args
        .first()
        .expect("pg_stat_get_backend_io: missing int4 arg")
        .value
        .as_i32();

    let mut bktype = BackendType::Invalid;
    let backend_stats =
        pgstat_backend::pgstat_fetch_stat_backend_by_pid(
            pid,
            Some(&mut bktype),
        )?;

    // C: if (!backend_stats) return (Datum) 0;
    let Some(backend_stats) = backend_stats else {
        fcinfo.isnull = true;
        return Ok(Datum::null());
    };

    // C: bktype_stats = &backend_stats->io_stats;
    //    Assert(pgstat_bktype_io_stats_valid(bktype_stats, bktype));
    let bktype_stats = &backend_stats.io_stats;
    debug_assert!(io::pgstat_bktype_io_stats_valid(bktype_stats, bktype));

    // C: pg_stat_io_build_tuples(rsinfo, bktype_stats, bktype,
    //                            backend_stats->stat_reset_timestamp);
    let mut rows: Vec<([Datum<'mcx>; IO_NUM_COLUMNS], [bool; IO_NUM_COLUMNS])> = Vec::new();
    pg_stat_io_build_tuples(
        mcx,
        &mut rows,
        bktype_stats,
        bktype,
        backend_stats.stat_reset_timestamp,
    )?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_stat_get_backend_io: InitMaterializedSRF establishes fcinfo->resultinfo");
    for (values, nulls) in &rows {
        materialized_srf_putvalues(rsinfo, &values[..], &nulls[..])?;
    }

    // C: return (Datum) 0.
    fcinfo.isnull = true;
    Ok(Datum::null())
}
