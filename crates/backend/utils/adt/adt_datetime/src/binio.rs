//! Binary (wire) protocol I/O cores for the date/time types, ported from the
//! `*_recv` / `*_send` functions in `src/backend/utils/adt/{date,timestamp}.c`
//! (idiomatic, safe Rust).
//!
//! These are the computational cores behind the SQL-callable fmgr wrappers; the
//! `Datum NAME(PG_FUNCTION_ARGS)` shims (argument unpacking / `PG_RETURN_*`)
//! stay in the deferred fmgr layer.  The C originals marshal through pqformat's
//! `StringInfo`/`bytea` (pointer-based); the idiomatic surface forbids raw
//! pointers / `bytea`, so the genuine *computation* is preserved exactly while
//! the marshalling is expressed in owned bytes:
//!
//! * `recv` takes a byte cursor ([`WireReader`]) over the wire payload and, for
//!   the typmod-bearing types, the `typmod: i32`.  It reads the SAME big-endian
//!   fields in the SAME order, applies the SAME typmod adjustment and the SAME
//!   range/validity checks that the corresponding `*_in` core applies, and
//!   returns the crate scalar/struct value.  A short buffer is a parse error
//!   (mirroring `pq_getmsgint*`'s "insufficient data" `ereport`).
//! * `send` returns the big-endian wire encoding as a `Vec<u8>` (the varlena
//!   payload the C `pq_endtypsend` produces, minus the varlena header).
//!   `send`/`recv` are mutually inverse and the wire field order matches C.
//!
//! Idiomatic surface: plain integers, owned `Vec<u8>`, `Result`, `&[u8]`.  No
//! raw pointers, `bytea`, `StringInfo`, `extern "C"`, `c_int`, or
//! `pg_ffi_fgram`.

use pgtime::pg_tm;
use types_datetime::{Interval, TimeTzADT, TZDISP_LIMIT, USECS_PER_DAY};
use types_error::{
    ERRCODE_DATETIME_VALUE_OUT_OF_RANGE, ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE,
};
use types_datetime::{fsec_t, DateADT, TimeADT, Timestamp, TimestampTz};
use types_error::{PgError, PgResult};

use crate::date::{DATE_NOT_FINITE, IS_VALID_DATE};
use crate::interval::AdjustIntervalForTypmod;
use crate::time::AdjustTimeForTypmod;
use crate::timestamp::{
    timestamp2tm, AdjustTimestampForTypmod, IS_VALID_TIMESTAMP, TIMESTAMP_NOT_FINITE,
};

// ---------------------------------------------------------------------------
// A minimal big-endian wire reader, the idiomatic analogue of pqformat's
// `StringInfo` receive cursor (`pq_getmsgint` / `pq_getmsgint64`).
// ---------------------------------------------------------------------------

/// A cursor over the bytes of a received binary value.  Reads big-endian
/// integers and advances; a short read is an "insufficient data" parse error
/// (the idiomatic analogue of `pq_getmsgint`'s out-of-bounds `ereport`).
#[derive(Clone, Debug)]
pub struct WireReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> WireReader<'a> {
    /// Build a reader over `bytes`, positioned at the start.
    pub fn new(bytes: &'a [u8]) -> Self {
        WireReader { bytes, pos: 0 }
    }

    fn take(&mut self, n: usize) -> PgResult<&[u8]> {
        let end = self.pos.checked_add(n).ok_or_else(insufficient_data)?;
        if end > self.bytes.len() {
            return Err(insufficient_data());
        }
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    /// Read a big-endian `i32` (`pq_getmsgint(buf, 4)`).
    pub fn get_i32(&mut self) -> PgResult<i32> {
        let b = self.take(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Read a big-endian `i64` (`pq_getmsgint64(buf)`).
    pub fn get_i64(&mut self) -> PgResult<i64> {
        let b = self.take(8)?;
        Ok(i64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
}

/// "insufficient data left in message" (C: pqformat `pq_getmsgint` ereport,
/// `ERRCODE_PROTOCOL_VIOLATION`; we surface as an invalid-binary parse error).
fn insufficient_data() -> PgError {
    PgError::error("insufficient data left in message")
        .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
}

// ---------------------------------------------------------------------------
// Small error helpers mirroring the C ereport() calls verbatim.
// ---------------------------------------------------------------------------

fn datetime_out_of_range(msg: &'static str) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
}

fn tz_displacement_out_of_range() -> PgError {
    PgError::error("time zone displacement out of range")
        .with_sqlstate(ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE)
}

// ---------------------------------------------------------------------------
// DATE
// ---------------------------------------------------------------------------

/// `date_recv()` CORE -- convert external binary format to a `DateADT`.
pub fn date_recv(buf: &mut WireReader<'_>) -> PgResult<DateADT> {
    let result = buf.get_i32()?;

    // Limit to the same range that date_in() accepts.
    if DATE_NOT_FINITE(result) {
        // ok
    } else if !IS_VALID_DATE(result) {
        return Err(datetime_out_of_range("date out of range"));
    }

    Ok(result)
}

/// `date_send()` CORE -- convert a `DateADT` to binary format.
pub fn date_send(date: DateADT) -> Vec<u8> {
    date.to_be_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// TIME
// ---------------------------------------------------------------------------

/// `time_recv()` CORE -- convert external binary format to a `TimeADT`.
pub fn time_recv(buf: &mut WireReader<'_>, typmod: i32) -> PgResult<TimeADT> {
    let mut result = buf.get_i64()?;

    if !(0..=USECS_PER_DAY).contains(&result) {
        return Err(datetime_out_of_range("time out of range"));
    }

    AdjustTimeForTypmod(&mut result, typmod);

    Ok(result)
}

/// `time_send()` CORE -- convert a `TimeADT` to binary format.
pub fn time_send(time: TimeADT) -> Vec<u8> {
    time.to_be_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// TIMETZ
// ---------------------------------------------------------------------------

/// `timetz_recv()` CORE -- convert external binary format to a `TimeTzADT`.
///
/// Wire field order matches C: the int64 time-of-day THEN the int32 zone.
pub fn timetz_recv(buf: &mut WireReader<'_>, typmod: i32) -> PgResult<TimeTzADT> {
    let mut time = buf.get_i64()?;

    if !(0..=USECS_PER_DAY).contains(&time) {
        return Err(datetime_out_of_range("time out of range"));
    }

    let zone = buf.get_i32()?;

    // Check for sane GMT displacement; see notes in datatype/timestamp.h
    if zone <= -TZDISP_LIMIT || zone >= TZDISP_LIMIT {
        return Err(tz_displacement_out_of_range());
    }

    AdjustTimeForTypmod(&mut time, typmod);

    Ok(TimeTzADT { time, zone })
}

/// `timetz_send()` CORE -- convert a `TimeTzADT` to binary format.
///
/// Emits the int64 time THEN the int32 zone (mutual inverse of `timetz_recv`).
pub fn timetz_send(time: &TimeTzADT) -> Vec<u8> {
    let mut buf = Vec::with_capacity(12);
    buf.extend_from_slice(&time.time.to_be_bytes());
    buf.extend_from_slice(&time.zone.to_be_bytes());
    buf
}

// ---------------------------------------------------------------------------
// TIMESTAMP (without time zone)
// ---------------------------------------------------------------------------

/// `timestamp_recv()` CORE -- convert external binary format to a `Timestamp`.
pub fn timestamp_recv(buf: &mut WireReader<'_>, typmod: i32) -> PgResult<Timestamp> {
    let mut timestamp = buf.get_i64()?;

    // range check: see if timestamp_out would like it
    if TIMESTAMP_NOT_FINITE(timestamp) {
        // ok
    } else {
        let mut tm = pg_tm::default();
        let mut fsec: fsec_t = 0;
        if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None).is_err()
            || !IS_VALID_TIMESTAMP(timestamp)
        {
            return Err(datetime_out_of_range("timestamp out of range"));
        }
    }

    AdjustTimestampForTypmod(&mut timestamp, typmod)?;

    Ok(timestamp)
}

/// `timestamp_send()` CORE -- convert a `Timestamp` to binary format.
pub fn timestamp_send(timestamp: Timestamp) -> Vec<u8> {
    timestamp.to_be_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// TIMESTAMPTZ
// ---------------------------------------------------------------------------

/// `timestamptz_recv()` CORE -- convert external binary format to a
/// `TimestampTz`.
pub fn timestamptz_recv(buf: &mut WireReader<'_>, typmod: i32) -> PgResult<TimestampTz> {
    let mut timestamp = buf.get_i64()?;

    // range check: see if timestamptz_out would like it
    if TIMESTAMP_NOT_FINITE(timestamp) {
        // ok
    } else {
        let mut tz: i32 = 0;
        let mut tm = pg_tm::default();
        let mut fsec: fsec_t = 0;
        if timestamp2tm(timestamp, Some(&mut tz), &mut tm, &mut fsec, None, None).is_err()
            || !IS_VALID_TIMESTAMP(timestamp)
        {
            return Err(datetime_out_of_range("timestamp out of range"));
        }
    }

    AdjustTimestampForTypmod(&mut timestamp, typmod)?;

    Ok(timestamp)
}

/// `timestamptz_send()` CORE -- convert a `TimestampTz` to binary format.
pub fn timestamptz_send(timestamp: TimestampTz) -> Vec<u8> {
    timestamp.to_be_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// INTERVAL
// ---------------------------------------------------------------------------

/// `interval_recv()` CORE -- convert external binary format to an `Interval`.
///
/// Wire field order matches C: the int64 time THEN the int32 day THEN the int32
/// month.
pub fn interval_recv(buf: &mut WireReader<'_>, typmod: i32) -> PgResult<Interval> {
    let time = buf.get_i64()?;
    let day = buf.get_i32()?;
    let month = buf.get_i32()?;

    let mut interval = Interval { time, day, month };

    AdjustIntervalForTypmod(&mut interval, typmod)?;

    Ok(interval)
}

/// `interval_send()` CORE -- convert an `Interval` to binary format.
///
/// Emits the int64 time THEN the int32 day THEN the int32 month (mutual inverse
/// of `interval_recv`).
pub fn interval_send(interval: &Interval) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    buf.extend_from_slice(&interval.time.to_be_bytes());
    buf.extend_from_slice(&interval.day.to_be_bytes());
    buf.extend_from_slice(&interval.month.to_be_bytes());
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::date::{DATEVAL_NOBEGIN, DATEVAL_NOEND};
    use crate::interval::{interval_nobegin, interval_noend};
    use types_datetime::{DAY, INTERVAL_FULL_PRECISION, INTERVAL_MASK, INTERVAL_TYPMOD};

    fn iso_lock() -> std::sync::MutexGuard<'static, ()> {
        let g = crate::settings::DATE_ORDER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        crate::settings::set_date_style(types_datetime::USE_ISO_DATES);
        g
    }

    // --- Exact wire bytes -------------------------------------------------

    #[test]
    fn date_send_exact_bytes() {
        assert_eq!(date_send(0), 0i32.to_be_bytes());
        assert_eq!(date_send(1), 1i32.to_be_bytes());
        assert_eq!(date_send(-1), (-1i32).to_be_bytes());
        assert_eq!(date_send(DATEVAL_NOEND), i32::MAX.to_be_bytes());
        assert_eq!(date_send(DATEVAL_NOBEGIN), i32::MIN.to_be_bytes());
    }

    #[test]
    fn timetz_send_exact_bytes_field_order() {
        let t = TimeTzADT {
            time: 36_000 * 1_000_000,
            zone: 18_000,
        };
        let mut expected = Vec::new();
        expected.extend_from_slice(&t.time.to_be_bytes());
        expected.extend_from_slice(&t.zone.to_be_bytes());
        assert_eq!(timetz_send(&t), expected);
        assert_eq!(timetz_send(&t).len(), 12);
    }

    #[test]
    fn interval_send_exact_bytes_field_order() {
        let iv = Interval {
            time: 3_600_000_000,
            day: 2,
            month: 13,
        };
        let mut expected = Vec::new();
        expected.extend_from_slice(&iv.time.to_be_bytes());
        expected.extend_from_slice(&iv.day.to_be_bytes());
        expected.extend_from_slice(&iv.month.to_be_bytes());
        assert_eq!(interval_send(&iv), expected);
        assert_eq!(interval_send(&iv).len(), 16);
    }

    // --- Round-trips ------------------------------------------------------

    #[test]
    fn date_round_trip() {
        for x in [0i32, 1, -1, 12345, -7654, DATEVAL_NOBEGIN, DATEVAL_NOEND] {
            let bytes = date_send(x);
            let mut buf = WireReader::new(&bytes);
            assert_eq!(date_recv(&mut buf).unwrap(), x, "date {x}");
        }
    }

    #[test]
    fn time_round_trip() {
        for x in [0i64, 1, 43_200_000_000, USECS_PER_DAY] {
            let bytes = time_send(x);
            let mut buf = WireReader::new(&bytes);
            assert_eq!(time_recv(&mut buf, -1).unwrap(), x, "time {x}");
        }
    }

    #[test]
    fn timetz_round_trip() {
        for (time, zone) in [
            (0i64, 0i32),
            (43_200_000_000, 18_000),
            (USECS_PER_DAY, -18_000),
            (1, 3600),
        ] {
            let t = TimeTzADT { time, zone };
            let bytes = timetz_send(&t);
            let mut buf = WireReader::new(&bytes);
            let got = timetz_recv(&mut buf, -1).unwrap();
            assert_eq!(got.time, time, "timetz time");
            assert_eq!(got.zone, zone, "timetz zone");
        }
    }

    #[test]
    fn timestamp_round_trip() {
        let _g = iso_lock();
        let normal = crate::timestamp::timestamp_in("2024-01-15 10:30:45.123456", -1).unwrap();
        for x in [0i64, normal, Timestamp::MIN, Timestamp::MAX] {
            let bytes = timestamp_send(x);
            let mut buf = WireReader::new(&bytes);
            assert_eq!(timestamp_recv(&mut buf, -1).unwrap(), x, "timestamp {x}");
        }
    }

    #[test]
    fn timestamptz_round_trip() {
        let _g = iso_lock();
        let normal = crate::timestamp::timestamptz_in("2024-01-15 10:30:45+00", -1).unwrap();
        for x in [0i64, normal, TimestampTz::MIN, TimestampTz::MAX] {
            let bytes = timestamptz_send(x);
            let mut buf = WireReader::new(&bytes);
            assert_eq!(timestamptz_recv(&mut buf, -1).unwrap(), x, "timestamptz {x}");
        }
    }

    #[test]
    fn interval_round_trip() {
        let cases = [
            Interval {
                time: 0,
                day: 0,
                month: 0,
            },
            Interval {
                time: 3_600_000_000,
                day: 2,
                month: 13,
            },
            Interval {
                time: -1_000_000,
                day: -5,
                month: -1,
            },
            interval_nobegin(),
            interval_noend(),
        ];
        for iv in cases {
            let bytes = interval_send(&iv);
            let mut buf = WireReader::new(&bytes);
            let got = interval_recv(&mut buf, -1).unwrap();
            assert_eq!(got.time, iv.time, "interval time");
            assert_eq!(got.day, iv.day, "interval day");
            assert_eq!(got.month, iv.month, "interval month");
        }
    }

    // --- typmod adjustment applied on recv --------------------------------

    #[test]
    fn time_recv_applies_typmod_rounding() {
        let bytes = time_send(123_456);
        let mut buf = WireReader::new(&bytes);
        assert_eq!(time_recv(&mut buf, 0).unwrap(), 0);

        let bytes = time_send(654_321);
        let mut buf = WireReader::new(&bytes);
        assert_eq!(time_recv(&mut buf, 0).unwrap(), 1_000_000);
    }

    #[test]
    fn timestamp_recv_applies_typmod_rounding() {
        let _g = iso_lock();
        let ts = crate::timestamp::timestamp_in("2024-01-15 10:30:45.4", -1).unwrap();
        let bytes = timestamp_send(ts);
        let mut buf = WireReader::new(&bytes);
        let got = timestamp_recv(&mut buf, 0).unwrap();
        assert_eq!(
            crate::timestamp::timestamp_out(got).unwrap(),
            "2024-01-15 10:30:45"
        );
    }

    #[test]
    fn interval_recv_applies_typmod_truncation() {
        let iv = crate::interval::interval_in("1 day 2 hours 3 minutes", -1).unwrap();
        let bytes = interval_send(&iv);
        let mut buf = WireReader::new(&bytes);
        let day_typmod = INTERVAL_TYPMOD(INTERVAL_FULL_PRECISION, INTERVAL_MASK(DAY));
        let got = interval_recv(&mut buf, day_typmod).unwrap();
        assert_eq!(got.day, 1);
        assert_eq!(got.time, 0, "sub-day time truncated by DAY range");
        assert_eq!(got.month, 0);
    }

    // --- Field-order decode checks ----------------------------------------

    #[test]
    fn timetz_recv_decodes_fields_in_order() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&36_000_000_000i64.to_be_bytes());
        bytes.extend_from_slice(&7_200i32.to_be_bytes());
        let mut buf = WireReader::new(&bytes);
        let got = timetz_recv(&mut buf, -1).unwrap();
        assert_eq!(got.time, 36_000_000_000);
        assert_eq!(got.zone, 7_200);
    }

    #[test]
    fn interval_recv_decodes_fields_in_order() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&5_000_000i64.to_be_bytes());
        bytes.extend_from_slice(&9i32.to_be_bytes());
        bytes.extend_from_slice(&11i32.to_be_bytes());
        let mut buf = WireReader::new(&bytes);
        let got = interval_recv(&mut buf, -1).unwrap();
        assert_eq!(got.time, 5_000_000);
        assert_eq!(got.day, 9);
        assert_eq!(got.month, 11);
    }

    // --- Error paths ------------------------------------------------------

    #[test]
    fn date_recv_rejects_out_of_range_but_accepts_infinity() {
        let candidate = DATEVAL_NOEND - 1;
        assert!(!DATE_NOT_FINITE(candidate));
        assert!(!IS_VALID_DATE(candidate));
        let bytes = candidate.to_be_bytes();
        let mut buf = WireReader::new(&bytes);
        let err = date_recv(&mut buf).unwrap_err();
        assert_eq!(err.message(), "date out of range");
        assert_eq!(err.sqlstate(), ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);

        let bytes = DATEVAL_NOEND.to_be_bytes();
        let mut buf = WireReader::new(&bytes);
        assert_eq!(date_recv(&mut buf).unwrap(), DATEVAL_NOEND);
    }

    #[test]
    fn time_recv_rejects_out_of_range() {
        let bytes = (-1i64).to_be_bytes();
        let mut buf = WireReader::new(&bytes);
        let err = time_recv(&mut buf, -1).unwrap_err();
        assert_eq!(err.message(), "time out of range");
        assert_eq!(err.sqlstate(), ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);

        let bytes = (USECS_PER_DAY + 1).to_be_bytes();
        let mut buf = WireReader::new(&bytes);
        let err = time_recv(&mut buf, -1).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
    }

    #[test]
    fn timetz_recv_rejects_bad_zone() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0i64.to_be_bytes());
        bytes.extend_from_slice(&TZDISP_LIMIT.to_be_bytes());
        let mut buf = WireReader::new(&bytes);
        let err = timetz_recv(&mut buf, -1).unwrap_err();
        assert_eq!(err.message(), "time zone displacement out of range");
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE);
    }

    #[test]
    fn recv_rejects_truncated_buffer() {
        let bytes = [0u8, 0];
        let mut buf = WireReader::new(&bytes);
        assert!(date_recv(&mut buf).is_err());

        let bytes = [0u8, 0, 0, 0];
        let mut buf = WireReader::new(&bytes);
        assert!(time_recv(&mut buf, -1).is_err());
    }

    #[test]
    fn timestamp_recv_rejects_out_of_range() {
        let _g = iso_lock();
        let bad = Timestamp::MAX - 1;
        assert!(!TIMESTAMP_NOT_FINITE(bad));
        let bytes = bad.to_be_bytes();
        let mut buf = WireReader::new(&bytes);
        let err = timestamp_recv(&mut buf, -1).unwrap_err();
        assert_eq!(err.message(), "timestamp out of range");
        assert_eq!(err.sqlstate(), ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
    }
}
