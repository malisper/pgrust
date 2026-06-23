//! The single-composite-row members of `pgstatfuncs.c`, registered in the
//! executor-frame SRF table (the FROM-clause / function-RTE path always consults
//! it, like the `json_to_record` / `pg_control_*` families):
//!
//!   * `pg_stat_get_wal`                 (OID 1136) — 5-column WAL stats
//!   * `pg_stat_get_archiver`            (OID 3195) — 7-column archiver stats
//!   * `pg_stat_get_replication_slot`    (OID 6169) — 10-column replslot stats
//!   * `pg_stat_get_subscription_stats`  (OID 6231) — 11-column subscription stats
//!
//! None is set-returning (`proretset => 'f'`): each returns exactly one
//! composite row. C builds its own `CreateTemplateTupleDesc` + `TupleDescInitEntry`
//! + `BlessTupleDesc`, fills `values`/`nulls`, and returns
//! `HeapTupleGetDatum(heap_form_tuple(...))`. The owned model builds the
//! composite `Datum` with `record_from_values` (the funcapi
//! `BlessTupleDesc`+`heap_form_tuple`+`HeapTupleGetDatum` pipeline) from the
//! projected fetch struct; the value-per-call loop stores the single row with
//! `isDone` left at `ExprSingleResult`.
//!
//! All read the now-ported pgstat fetch substrate
//! (`pgstat_fetch_stat_wal`/`pgstat_fetch_stat_archiver`/`pgstat_fetch_replslot`/
//! `pgstat_fetch_stat_subscription`).

extern crate alloc;
use alloc::vec::Vec;

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use ::replication::conflict::CONFLICT_NUM_TYPES;
use ::types_tuple::heaptuple::Datum;
use ::types_tuple::heaptuple::NameData;

use ::funcapi_seams::record_from_values;

use crate::register_srf;

/// `pg_stat_get_wal()` (OID 1136).
const PG_STAT_GET_WAL: Oid = 1136;
/// `pg_stat_get_archiver()` (OID 3195).
const PG_STAT_GET_ARCHIVER: Oid = 3195;
/// `pg_stat_get_replication_slot(text)` (OID 6169).
const PG_STAT_GET_REPLICATION_SLOT: Oid = 6169;
/// `pg_stat_get_subscription_stats(oid)` (OID 6231).
const PG_STAT_GET_SUBSCRIPTION_STATS: Oid = 6231;
/// `pg_stat_get_backend_wal(int4)` (OID 6313).
const PG_STAT_GET_BACKEND_WAL: Oid = 6313;

const INT8OID: Oid = 20;
const TEXTOID: Oid = 25;
const OIDOID: Oid = 26;
const NUMERICOID: Oid = 1700;
const TIMESTAMPTZOID: Oid = 1184;

/// Register the single-composite-row pgstatfuncs.c builtins in the
/// executor-frame SRF table.
pub(crate) fn register_pgstat_composite_srfs() {
    register_srf(PG_STAT_GET_WAL, pg_stat_get_wal);
    register_srf(PG_STAT_GET_ARCHIVER, pg_stat_get_archiver);
    register_srf(PG_STAT_GET_REPLICATION_SLOT, pg_stat_get_replication_slot);
    register_srf(PG_STAT_GET_SUBSCRIPTION_STATS, pg_stat_get_subscription_stats);
    register_srf(PG_STAT_GET_BACKEND_WAL, pg_stat_get_backend_wal);
}

/// The per-query memory context the SRF caller threads onto the executor frame.
fn srf_mcx<'mcx>(fcinfo: &FunctionCallInfoBaseData<'mcx>) -> Mcx<'mcx> {
    fcinfo
        .fn_mcxt
        .expect("pgstat composite SRF: fn_mcxt set by ExecMakeTableFunctionResult")
}

/// `CStringGetTextDatum(s)` → a `text` varlena `Datum`.
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    varlena_seams::cstring_to_text_v::call(mcx, s)
}

/// Build a `NUMERIC` `Datum` from a `u64`. C does `numeric_in(UINT64_FORMAT)`;
/// the counters are non-negative so an exact unsigned→numeric (`int128` carrier)
/// is faithful.
fn numeric_from_u64<'mcx>(mcx: Mcx<'mcx>, val: u64) -> PgResult<Datum<'mcx>> {
    let var = adt_numeric::convert::int128_to_numericvar(mcx, val as i128)?;
    let buf = adt_numeric::convert::make_result(mcx, &var)?;
    Ok(Datum::ByRef(buf))
}

/// Decode a NUL-padded fixed buffer (`char foo[N]`) as a C string up to the
/// first NUL. Returns `""` for an empty (first byte NUL) buffer.
fn cbuf_str(buf: &[u8]) -> &str {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    core::str::from_utf8(&buf[..end]).unwrap_or("")
}

// ===========================================================================
//  pg_stat_get_wal (pgstatfuncs.c:1700) — pg_stat_wal_build_tuple, 5 cols.
// ===========================================================================

/// `pg_stat_wal_build_tuple(wal_counters, stat_reset_timestamp)`
/// (pgstatfuncs.c:1627) — the shared helper for `pg_stat_get_wal()` and
/// `pg_stat_get_backend_wal()` returning one 5-column tuple.
fn pg_stat_wal_build_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    wc: &types_pgstat::activity_pgstat::PgStat_WalCounters,
    reset_ts: ::types_core::primitive::TimestampTz,
) -> PgResult<Datum<'mcx>> {
    let coltypes = [INT8OID, INT8OID, NUMERICOID, INT8OID, TIMESTAMPTZOID];
    let mut values: [Datum<'mcx>; 5] = [
        Datum::from_i64(wc.wal_records),
        Datum::from_i64(wc.wal_fpi),
        numeric_from_u64(mcx, wc.wal_bytes)?,
        Datum::from_i64(wc.wal_buffers_full),
        Datum::null(),
    ];
    let mut nulls = [false; 5];
    if reset_ts != 0 {
        values[4] = Datum::from_i64(reset_ts);
    } else {
        nulls[4] = true;
    }
    record_from_values::call(mcx, &coltypes, &values, &nulls)
}

fn pg_stat_get_wal<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    // C: wal_stats = pgstat_fetch_stat_wal();
    let wal_stats = pgstat_wal::pgstat_fetch_stat_wal()?;
    pg_stat_wal_build_tuple(mcx, &wal_stats.wal_counters, wal_stats.stat_reset_timestamp)
}

// ===========================================================================
//  pg_stat_get_backend_wal (pgstatfuncs.c:1678) — WAL stats for a backend by
//  pid, one composite row, or NULL when the pid is not a tracked backend.
// ===========================================================================

fn pg_stat_get_backend_wal<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    // C: pid = PG_GETARG_INT32(0);
    let pid = arg_i32(fcinfo, 0);

    // C: backend_stats = pgstat_fetch_stat_backend_by_pid(pid, NULL);
    //    if (!backend_stats) PG_RETURN_NULL();
    let backend_stats =
        match pgstat_backend::pgstat_fetch_stat_backend_by_pid(pid, None)? {
            Some(b) => b,
            None => {
                fcinfo.isnull = true;
                return Ok(Datum::null());
            }
        };

    // C: bktype_stats = backend_stats->wal_counters;
    //    return pg_stat_wal_build_tuple(bktype_stats, backend_stats->stat_reset_timestamp);
    pg_stat_wal_build_tuple(
        mcx,
        &backend_stats.wal_counters,
        backend_stats.stat_reset_timestamp,
    )
}

// ===========================================================================
//  pg_stat_get_archiver (pgstatfuncs.c:2047) — 7 cols.
// ===========================================================================

fn pg_stat_get_archiver<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    // C: archiver_stats = pgstat_fetch_stat_archiver();
    let a = activity_small::pgstat_fetch_stat_archiver()?;

    let coltypes = [
        INT8OID,
        TEXTOID,
        TIMESTAMPTZOID,
        INT8OID,
        TEXTOID,
        TIMESTAMPTZOID,
        TIMESTAMPTZOID,
    ];
    let mut values: [Datum<'mcx>; 7] = core::array::from_fn(|_| Datum::null());
    let mut nulls = [false; 7];

    values[0] = Datum::from_i64(a.archived_count);

    let last_archived_wal = cbuf_str(&a.last_archived_wal);
    if last_archived_wal.is_empty() {
        nulls[1] = true;
    } else {
        values[1] = text_datum(mcx, last_archived_wal)?;
    }

    if a.last_archived_timestamp == 0 {
        nulls[2] = true;
    } else {
        values[2] = Datum::from_i64(a.last_archived_timestamp);
    }

    values[3] = Datum::from_i64(a.failed_count);

    let last_failed_wal = cbuf_str(&a.last_failed_wal);
    if last_failed_wal.is_empty() {
        nulls[4] = true;
    } else {
        values[4] = text_datum(mcx, last_failed_wal)?;
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

    record_from_values::call(mcx, &coltypes, &values, &nulls)
}

// ===========================================================================
//  pg_stat_get_replication_slot (pgstatfuncs.c:2113) — 10 cols.
// ===========================================================================

fn pg_stat_get_replication_slot<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);

    // C: namestrcpy(&slotname, text_to_cstring(PG_GETARG_TEXT_P(0)));
    let slotname_str = arg_text(fcinfo, 0);
    let mut slotname = NameData::default();
    slotname.namestrcpy(&slotname_str);

    // C: slotent = pgstat_fetch_replslot(slotname); if (!slotent) allzero.
    let slotent = pgstat_replslot::pgstat_fetch_replslot(slotname)?
        .unwrap_or_default();

    // C: values[0] = CStringGetTextDatum(NameStr(slotname));
    let slot_name = core::str::from_utf8(slotname.name_str()).unwrap_or("");

    let coltypes = [
        TEXTOID, INT8OID, INT8OID, INT8OID, INT8OID, INT8OID, INT8OID, INT8OID, INT8OID,
        TIMESTAMPTZOID,
    ];
    let mut values: [Datum<'mcx>; 10] = [
        text_datum(mcx, slot_name)?,
        Datum::from_i64(slotent.spill_txns),
        Datum::from_i64(slotent.spill_count),
        Datum::from_i64(slotent.spill_bytes),
        Datum::from_i64(slotent.stream_txns),
        Datum::from_i64(slotent.stream_count),
        Datum::from_i64(slotent.stream_bytes),
        Datum::from_i64(slotent.total_txns),
        Datum::from_i64(slotent.total_bytes),
        Datum::null(),
    ];
    let mut nulls = [false; 10];
    if slotent.stat_reset_timestamp == 0 {
        nulls[9] = true;
    } else {
        values[9] = Datum::from_i64(slotent.stat_reset_timestamp);
    }

    record_from_values::call(mcx, &coltypes, &values, &nulls)
}

// ===========================================================================
//  pg_stat_get_subscription_stats (pgstatfuncs.c:2184) — 11 cols.
// ===========================================================================

fn pg_stat_get_subscription_stats<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = srf_mcx(fcinfo);
    let subid = arg_oid(fcinfo, 0);

    // C: subentry = pgstat_fetch_stat_subscription(subid); if (!subentry) allzero.
    let subentry =
        pgstat_subscription::pgstat_fetch_stat_subscription(subid)?
            .unwrap_or_default();

    // 11 cols: subid, apply_error_count, sync_error_count, CONFLICT_NUM_TYPES (=7)
    // conflict counters, stats_reset.
    let mut coltypes: Vec<Oid> = Vec::with_capacity(11);
    coltypes.push(OIDOID);
    coltypes.push(INT8OID);
    coltypes.push(INT8OID);
    for _ in 0..CONFLICT_NUM_TYPES {
        coltypes.push(INT8OID);
    }
    coltypes.push(TIMESTAMPTZOID);

    let mut values: Vec<Datum<'mcx>> = Vec::with_capacity(11);
    let mut nulls: Vec<bool> = Vec::with_capacity(11);

    // subid
    values.push(Datum::from_oid(subid));
    nulls.push(false);
    // apply_error_count
    values.push(Datum::from_i64(subentry.apply_error_count));
    nulls.push(false);
    // sync_error_count
    values.push(Datum::from_i64(subentry.sync_error_count));
    nulls.push(false);
    // conflict counts
    for nconflict in 0..CONFLICT_NUM_TYPES {
        values.push(Datum::from_i64(subentry.conflict_count[nconflict]));
        nulls.push(false);
    }
    // stats_reset
    if subentry.stat_reset_timestamp == 0 {
        values.push(Datum::null());
        nulls.push(true);
    } else {
        values.push(Datum::from_i64(subentry.stat_reset_timestamp));
        nulls.push(false);
    }

    debug_assert_eq!(values.len(), 11);
    record_from_values::call(mcx, &coltypes, &values, &nulls)
}

// ===========================================================================
//  Argument readers.
// ===========================================================================

/// `text_to_cstring(PG_GETARG_TEXT_P(i))`: a `text` arg's payload on the by-ref
/// lane (header-ful image; skip the 4-byte varlena header), decoded as UTF-8.
fn arg_text(fcinfo: &FunctionCallInfoBaseData, i: usize) -> alloc::string::String {
    use ::datum::varlena::VARHDRSZ;
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("pgstat composite SRF: text arg missing from by-ref lane");
    let bytes = &image[VARHDRSZ..];
    core::str::from_utf8(bytes)
        .expect("pgstat composite SRF: text arg not valid UTF-8")
        .into()
}

/// `PG_GETARG_OID(i)`.
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo
        .args
        .get(i)
        .expect("pgstat composite SRF: missing oid arg")
        .value
        .as_oid()
}

/// `PG_GETARG_INT32(i)`.
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo
        .args
        .get(i)
        .expect("pgstat composite SRF: missing int4 arg")
        .value
        .as_i32()
}
