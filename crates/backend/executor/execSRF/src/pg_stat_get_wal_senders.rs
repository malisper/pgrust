//! `pg_stat_get_wal_senders()` (OID 3099) — the `pg_stat_replication` view's
//! backing materialize-mode SRF (walsender.c:3914).
//!
//! Lists every active walsender (one row per `WalSndCtl->walsnds[]` slot whose
//! `pid != 0`) with its lag / sync state. The 12 columns are
//! `(pid, state, sent_lsn, write_lsn, flush_lsn, replay_lsn, write_lag,
//! flush_lag, replay_lag, sync_priority, sync_state, reply_time)`.
//!
//! The per-row decision logic (the privilege gate that NULLs the detail columns
//! for unprivileged callers, the invalid-flush priority adjustment, the
//! sync-state classification, and `offset_to_interval`) lives in its owner crate
//! ([`walsender::pg_stat_get_wal_senders`], which returns the assembled
//! [`WalSenderRow`]s). This is the `Datum`/tuplestore-construction adapter:
//! `InitMaterializedSRF(fcinfo, 0)` + `tuplestore_putvalues` per row, mirroring
//! the C SRF plumbing.

extern crate alloc;
use alloc::vec::Vec;

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use ::types_datetime::Interval;
use ::replication::walsender::{SyncState, WalSenderRow};
use types_tuple::heaptuple::Datum;

use ::funcapi::srf_support::{materialized_srf_putvalues, InitMaterializedSRF};

use crate::register_srf;

/// `pg_stat_get_wal_senders()` (OID 3099).
const PG_STAT_GET_WAL_SENDERS: Oid = 3099;

/// `#define PG_STAT_GET_WAL_SENDERS_COLS 12`.
const PG_STAT_GET_WAL_SENDERS_COLS: usize = 12;

/// `InvalidXLogRecPtr` (`access/xlogdefs.h`): `#define InvalidXLogRecPtr 0`.
const INVALID_XLOG_REC_PTR: u64 = 0;

/// Register `pg_stat_get_wal_senders` in the executor-frame SRF table.
pub(crate) fn register_pg_stat_get_wal_senders() {
    register_srf(PG_STAT_GET_WAL_SENDERS, pg_stat_get_wal_senders);
}

/// `CStringGetTextDatum(s)` → a `text` varlena `Datum`.
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    varlena_seams::cstring_to_text_v::call(mcx, s)
}

/// `IntervalPGetDatum(iv)` — the fixed-length 16-byte by-reference `interval`
/// image (`time:i64, day:i32, month:i32`, little-endian, no padding), interned
/// into the per-query context.
fn interval_datum<'mcx>(mcx: Mcx<'mcx>, iv: &Interval) -> PgResult<Datum<'mcx>> {
    let mut img: Vec<u8> = Vec::with_capacity(16);
    img.extend_from_slice(&iv.time.to_le_bytes());
    img.extend_from_slice(&iv.day.to_le_bytes());
    img.extend_from_slice(&iv.month.to_le_bytes());
    Ok(Datum::ByRef(::mcx::slice_in(mcx, &img)?))
}

/// `pg_stat_get_wal_senders(PG_FUNCTION_ARGS)` (walsender.c:3914) over the
/// executor frame.
fn pg_stat_get_wal_senders<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_stat_get_wal_senders: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: InitMaterializedSRF(fcinfo, 0). The descriptor is resolved from the
    // function's declared OUT params via get_call_result_type (flag 0), exactly
    // as walsender.c does.
    InitMaterializedSRF(fcinfo, 0)?;

    // C: the per-row decision logic (slot snapshot under the spinlock, the
    // SyncRepGetCandidateStandbys match, the privilege gate, the priority/sync
    // classification, offset_to_interval) lives in the walsender owner; it hands
    // back one assembled WalSenderRow per active slot.
    let rows: Vec<WalSenderRow> = walsender::pg_stat_get_wal_senders();

    let mut out: Vec<(
        [Datum<'mcx>; PG_STAT_GET_WAL_SENDERS_COLS],
        [bool; PG_STAT_GET_WAL_SENDERS_COLS],
    )> = Vec::with_capacity(rows.len());

    for row in &rows {
        let mut values: [Datum<'mcx>; PG_STAT_GET_WAL_SENDERS_COLS] =
            core::array::from_fn(|_| Datum::null());
        let mut nulls = [false; PG_STAT_GET_WAL_SENDERS_COLS];

        // values[0] = Int32GetDatum(pid);
        values[0] = Datum::from_i32(row.pid);

        if !row.has_details {
            // C: MemSet(&nulls[1], true, PG_STAT_GET_WAL_SENDERS_COLS - 1);
            // Unprivileged callers see only the pid (so they know it's a
            // walsender) and NULL detail columns.
            for n in nulls.iter_mut().skip(1) {
                *n = true;
            }
        } else {
            // values[1] = CStringGetTextDatum(WalSndGetStateString(state));
            values[1] = text_datum(mcx, row.state)?;

            // values[2..6] = LSNGetDatum(...); NULL on InvalidXLogRecPtr. C sets
            // the value unconditionally and only the null flag suppresses it; the
            // null flag wins, so we skip building the (ignored) Datum on invalid.
            set_lsn(&mut values, &mut nulls, 2, row.sent_ptr);
            set_lsn(&mut values, &mut nulls, 3, row.write);
            set_lsn(&mut values, &mut nulls, 4, row.flush);
            set_lsn(&mut values, &mut nulls, 5, row.apply);

            // values[6] = IntervalPGetDatum(offset_to_interval(writeLag)) or NULL.
            match &row.write_lag {
                Some(iv) => values[6] = interval_datum(mcx, iv)?,
                None => nulls[6] = true,
            }
            // values[7] = flush_lag.
            match &row.flush_lag {
                Some(iv) => values[7] = interval_datum(mcx, iv)?,
                None => nulls[7] = true,
            }
            // values[8] = replay_lag.
            match &row.apply_lag {
                Some(iv) => values[8] = interval_datum(mcx, iv)?,
                None => nulls[8] = true,
            }

            // values[9] = Int32GetDatum(priority);
            values[9] = Datum::from_i32(row.sync_priority);

            // values[10] = CStringGetTextDatum(<sync_state>);
            values[10] = text_datum(mcx, sync_state_str(row.sync_state))?;

            // values[11] = TimestampTzGetDatum(replyTime) or NULL when 0.
            match row.reply_time {
                Some(t) => values[11] = Datum::from_i64(t),
                None => nulls[11] = true,
            }
        }

        out.push((values, nulls));
    }

    // C: tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls).
    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_stat_get_wal_senders: InitMaterializedSRF establishes fcinfo->resultinfo");
    for (values, nulls) in &out {
        materialized_srf_putvalues(rsinfo, &values[..], &nulls[..])?;
    }

    // C: return (Datum) 0.
    fcinfo.isnull = true;
    Ok(Datum::null())
}

/// `if (XLogRecPtrIsInvalid(p)) nulls[i] = true; else values[i] = LSNGetDatum(p);`.
/// `pg_lsn` is an 8-byte pass-by-value type.
fn set_lsn<'mcx>(
    values: &mut [Datum<'mcx>; PG_STAT_GET_WAL_SENDERS_COLS],
    nulls: &mut [bool; PG_STAT_GET_WAL_SENDERS_COLS],
    i: usize,
    p: u64,
) {
    if p == INVALID_XLOG_REC_PTR {
        nulls[i] = true;
    } else {
        values[i] = Datum::from_u64(p);
    }
}

/// The textual sync-state name used in the view (not translated).
fn sync_state_str(s: SyncState) -> &'static str {
    match s {
        SyncState::Async => "async",
        SyncState::Sync => "sync",
        SyncState::Quorum => "quorum",
        SyncState::Potential => "potential",
    }
}
