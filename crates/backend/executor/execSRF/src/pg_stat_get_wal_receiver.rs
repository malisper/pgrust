//! `pg_stat_get_wal_receiver()` (OID 3317) over the executor frame — the
//! single-row (or NULL) record function backing the `pg_stat_wal_receiver`
//! system view.
//!
//! C's `pg_stat_get_wal_receiver` is a non-set function (`proretset=false`,
//! `RETURNS record` with 15 OUT params) that `PG_RETURN_NULL()`s when there is no
//! live WAL receiver and otherwise returns one composite row. In the
//! `pg_stat_wal_receiver` view's `FROM` clause the executor drives it through
//! `ExecMakeTableFunctionResult` → the executor-frame SRF table; since it is NOT
//! a set function it returns exactly one `Datum::Composite` (or SQL NULL) with
//! `isDone` left at `ExprSingleResult` — the value-per-call loop stores the one
//! row and stops (the `pg_input_error_info` template, NOT materialize mode).
//!
//! The WalRcv snapshot + the field-by-field NULL/value selection
//! ([`replication_walreceiver::pg_stat_get_wal_receiver`] →
//! `WalReceiverActivity`) is its walreceiver.c owner's; this is the
//! `Datum`/tuple-construction adapter.

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::heaptuple::Datum;
use types_walreceiver::WalReceiverActivity;

use crate::register_srf;

/// `pg_stat_get_wal_receiver()` (OID 3317).
const PG_STAT_GET_WAL_RECEIVER: Oid = 3317;

/// 15 output columns (pid + 14 detail columns).
const NUM_COLS: usize = 15;

/// Register the `pg_stat_wal_receiver` record function in the executor-frame SRF
/// table (the by-OID builtin registry's tag-only `resultinfo` cannot carry the
/// live `ReturnSetInfo`/`expectedDesc` this record function needs — WONTFIX
/// dual-home).
pub(crate) fn register_pg_stat_get_wal_receiver() {
    register_srf(PG_STAT_GET_WAL_RECEIVER, pg_stat_get_wal_receiver);
}

/// `CStringGetTextDatum(s)` over the call's per-query context.
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    varlena_seams::cstring_to_text_v::call(mcx, s)
}

/// `pg_stat_get_wal_receiver(PG_FUNCTION_ARGS)` (walreceiver.c) over the executor
/// frame.
fn pg_stat_get_wal_receiver<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_stat_get_wal_receiver: fn_mcxt set by the SRF caller");

    // C: snapshot WalRcv; PG_RETURN_NULL() when there's no receiver / not ready.
    let activity = replication_walreceiver::pg_stat_get_wal_receiver()?;
    let act = match activity {
        // C: PG_RETURN_NULL(). The single value-per-call result is SQL NULL; the
        // pg_stat_wal_receiver view's `WHERE pid IS NOT NULL` drops it (0 rows).
        None => {
            fcinfo.isnull = true;
            return Ok(Datum::null());
        }
        Some(a) => a,
    };

    // C: get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE ->
    //    elog(ERROR, "return type must be a row type"). The expected composite
    //    descriptor reaches us through the live ReturnSetInfo (resultinfo).
    let tupdesc = fcinfo
        .resultinfo
        .as_ref()
        .and_then(|rsi| rsi.expectedDesc.as_ref())
        .expect("pg_stat_get_wal_receiver: expected composite result descriptor")
        .clone_in(mcx)
        .expect("pg_stat_get_wal_receiver: clone result descriptor");

    let mut values: [Datum<'mcx>; NUM_COLS] = core::array::from_fn(|_| Datum::null());
    let mut nulls = [true; NUM_COLS];

    // [0] pid — int4 (always present)
    values[0] = Datum::from_i32(act.pid);
    nulls[0] = false;

    /// Set one column to `v` (non-null) via the typed `Datum` builder.
    macro_rules! set {
        ($idx:expr, $opt:expr, $build:expr) => {
            if let Some(v) = $opt {
                values[$idx] = $build(v)?;
                nulls[$idx] = false;
            }
        };
    }

    // [1] status — text
    set!(1, act.state.as_deref(), |s: &str| text_datum(mcx, s));
    // [2] receive_start_lsn — pg_lsn (u64, pass-by-value)
    set!(2, act.receive_start_lsn, |v: u64| Ok::<_, types_error::PgError>(Datum::from_u64(v)));
    // [3] receive_start_tli — int4
    set!(3, act.receive_start_tli, |v: u32| Ok::<_, types_error::PgError>(Datum::from_i32(v as i32)));
    // [4] written_lsn — pg_lsn
    set!(4, act.written_lsn, |v: u64| Ok::<_, types_error::PgError>(Datum::from_u64(v)));
    // [5] flushed_lsn — pg_lsn
    set!(5, act.flushed_lsn, |v: u64| Ok::<_, types_error::PgError>(Datum::from_u64(v)));
    // [6] received_tli — int4
    set!(6, act.received_tli, |v: u32| Ok::<_, types_error::PgError>(Datum::from_i32(v as i32)));
    // [7] last_msg_send_time — timestamptz (i64, pass-by-value)
    set!(7, act.last_send_time, |v: i64| Ok::<_, types_error::PgError>(Datum::from_i64(v)));
    // [8] last_msg_receipt_time — timestamptz
    set!(8, act.last_receipt_time, |v: i64| Ok::<_, types_error::PgError>(Datum::from_i64(v)));
    // [9] latest_end_lsn — pg_lsn
    set!(9, act.latest_end_lsn, |v: u64| Ok::<_, types_error::PgError>(Datum::from_u64(v)));
    // [10] latest_end_time — timestamptz
    set!(10, act.latest_end_time, |v: i64| Ok::<_, types_error::PgError>(Datum::from_i64(v)));
    // [11] slot_name — text
    set!(11, act.slotname.as_deref(), |s: &str| text_datum(mcx, s));
    // [12] sender_host — text
    set!(12, act.sender_host.as_deref(), |s: &str| text_datum(mcx, s));
    // [13] sender_port — int4
    set!(13, act.sender_port, |v: i32| Ok::<_, types_error::PgError>(Datum::from_i32(v)));
    // [14] conninfo — text
    set!(14, act.conninfo.as_deref(), |s: &str| text_datum(mcx, s));

    let formed =
        heaptuple::heap_form_tuple(mcx, &tupdesc, &values, &nulls)
            .expect("pg_stat_get_wal_receiver: heap_form_tuple");

    // C: return HeapTupleGetDatum(...). One single-result row; the value-per-call
    // loop stores it and stops (isDone stays ExprSingleResult).
    fcinfo.isnull = false;
    Ok(Datum::Composite(formed))
}
