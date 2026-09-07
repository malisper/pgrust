// datetime.c:423-431 GetCurrentTimeUsec: a transaction start timestamp that
// timestamp2tm rejects is ereport(ERROR, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE,
// "timestamp out of range") -- an ordinary error the datetime decoders carry
// out through their PgResult, never a process panic. The seam that hands the
// broken-down current time to adt_datetime (installed by adt_timestamp, whose
// GetCurrentTimeUsec raises exactly that error) is stubbed here with the
// failing arm, since no clock can be put outside the timestamp range from SQL
// (row a186-candidate-fp-adt-datetime-p1-0f3d4694dccb4b17d4e0-1).
//
// Own test binary: the seams are set-once and tests/datetime.rs never
// installs them (its inputs avoid 'now'/'today').
#![allow(non_snake_case)]

use adt_datetime::*;
use types_error::{PgError, PgResult, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE};

fn out_of_range() -> PgResult<timestamp_seams::CurrentTimeUsec> {
    Err(Box::new(
        PgError::error("timestamp out of range").with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
    ))
}

fn install_failing_clock() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        timestamp_seams::get_current_datetime::set(out_of_range);
        timestamp_seams::get_current_time_usec::set(out_of_range);
    });
}

const TS_BUFLEN: usize = MAXDATELEN + MAXDATEFIELDS;

fn decode_ts(input: &str) -> PgResult<i32> {
    let mut workbuf = [0u8; TS_BUFLEN];
    let mut field: [&[u8]; MAXDATEFIELDS] = [b""; MAXDATEFIELDS];
    let mut ftype = [0i32; MAXDATEFIELDS];
    let mut nf = 0usize;
    let rc = ParseDateTime(input.as_bytes(), &mut workbuf, &mut field, &mut ftype, MAXDATEFIELDS, &mut nf);
    assert_eq!(rc, 0, "ParseDateTime({input})");
    let mut dtype = 0;
    let mut tm = pg_tm::default();
    let mut fsec = 0;
    let mut tz = 0;
    let mut extra = DateTimeErrorExtra::default();
    DecodeDateTime(
        &field[..nf],
        &ftype[..nf],
        nf,
        &mut dtype,
        &mut tm,
        &mut fsec,
        Some(&mut tz),
        &mut extra,
    )
}

fn decode_time(input: &str) -> PgResult<i32> {
    let mut workbuf = [0u8; MAXDATELEN + 1];
    let mut field: [&[u8]; MAXDATEFIELDS] = [b""; MAXDATEFIELDS];
    let mut ftype = [0i32; MAXDATEFIELDS];
    let mut nf = 0usize;
    let rc = ParseDateTime(input.as_bytes(), &mut workbuf, &mut field, &mut ftype, MAXDATEFIELDS, &mut nf);
    assert_eq!(rc, 0, "ParseDateTime({input})");
    let mut dtype = 0;
    let mut tm = pg_tm::default();
    let mut fsec = 0;
    let mut tz = 0;
    let mut extra = DateTimeErrorExtra::default();
    DecodeTimeOnly(
        &field[..nf],
        &mut ftype[..nf],
        nf,
        &mut dtype,
        &mut tm,
        &mut fsec,
        Some(&mut tz),
        &mut extra,
    )
}

fn assert_timestamp_out_of_range(r: PgResult<i32>, what: &str) {
    let err = r.err().unwrap_or_else(|| panic!("{what}: expected the 22008 error, got a dterr/success"));
    assert_eq!(err.sqlstate(), ERRCODE_DATETIME_VALUE_OUT_OF_RANGE, "{what}");
    assert_eq!(err.message(), "timestamp out of range", "{what}");
}

#[test]
fn now_with_out_of_range_transaction_timestamp_is_22008_error() {
    install_failing_clock();
    assert_timestamp_out_of_range(decode_ts("now"), "DecodeDateTime('now')");
}

#[test]
fn today_with_out_of_range_transaction_timestamp_is_22008_error() {
    install_failing_clock();
    assert_timestamp_out_of_range(decode_ts("today"), "DecodeDateTime('today')");
    assert_timestamp_out_of_range(decode_ts("yesterday"), "DecodeDateTime('yesterday')");
    assert_timestamp_out_of_range(decode_ts("tomorrow"), "DecodeDateTime('tomorrow')");
}

#[test]
fn time_now_with_out_of_range_transaction_timestamp_is_22008_error() {
    install_failing_clock();
    assert_timestamp_out_of_range(decode_time("now"), "DecodeTimeOnly('now')");
}
