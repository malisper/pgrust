//! interval_engine_diff: differential fuzz driver — shipped Rust
//! `adt_datetime` interval parse/encode ENGINE vs vendored PostgreSQL 18.3
//! (Stamp-18.3, upstream sha 62d6c7d3df) C (csrc/pg_datetime_io_io.c).
//!
//! Scope note (claims): the SQL entry points interval_in/interval_out live
//! in adt_timestamp (NOT claimed by this lane; their fc wrappers ride
//! adt_date's builtins). This target therefore compares at the ENGINE
//! level, entirely inside the claimed adt_datetime crate: ParseDateTime
//! field-splitting + DecodeInterval / DecodeISO8601Interval (raw dterr
//! codes + decoded pg_itm_in fields — finer than errcode classes) and
//! EncodeInterval (text image). The C oracle's interval2itm (timestamp.c,
//! verbatim) only PREPARES the pg_itm handed to BOTH encoders from a raw
//! (time,day,month) triple — shared input construction, not a compared
//! surface.
//!
//! Environment: IntervalStyle fuzzed from a selector byte on both sides
//! (all 4 styles); no tz/clock/GUC state is reachable from these engines.
//!
//! Input layout: [selector][payload]; selector % 3 picks the arm:
//!   0 DecodeInterval        — [istyle][range][text]
//!   1 DecodeISO8601Interval — [text] (style-independent; C caller pins
//!     INTSTYLE_ISO_8601), text < 256 bytes (C staging buffer)
//!   2 EncodeInterval        — [istyle][time i64][day i32][month i32],
//!     itm derived via verbatim interval2itm and handed to both sides

use std::ffi::CString;

use adt_datetime::consts::{pg_itm, pg_itm_in, INTERVAL_FULL_RANGE, INTERVAL_MASK};
use adt_datetime::set_interval_style;
use adt_datetime::{DecodeISO8601Interval, DecodeInterval, ParseDateTime};

extern "C" {
    fn pg_diff_decode_interval(
        str_: *const std::ffi::c_char,
        range: i32,
        istyle: i32,
        usec: *mut i64,
        mday: *mut i32,
        mon: *mut i32,
        year: *mut i32,
        dtype: *mut i32,
    ) -> i32;
    fn pg_diff_decode_iso8601_interval(
        str_: *const std::ffi::c_char,
        usec: *mut i64,
        mday: *mut i32,
        mon: *mut i32,
        year: *mut i32,
        dtype: *mut i32,
    ) -> i32;
    fn pg_diff_encode_interval(
        time: i64,
        day: i32,
        month: i32,
        istyle: i32,
        buf: *mut u8,
        itm_usec: *mut i64,
        itm_hour: *mut i64,
        itm_sec: *mut i32,
        itm_min: *mut i32,
        itm_mday: *mut i32,
        itm_mon: *mut i32,
        itm_year: *mut i32,
    ) -> i32;
}

const MAXDATEFIELDS: usize = 25;
/// Real interval_in's ParseDateTime frame is `char workbuf[256]`
/// (timestamp.c:908) — NOT date.c's MAXDATELEN+1 (129) and NOT
/// MAXDATELEN+MAXDATEFIELDS (153, timestamp_in's). Both sides of this
/// target model interval_in exactly; with the 200-byte text cap +
/// MAXDATEFIELDS=25 the buffer-full arm is unreachable here (max
/// fields+NULs = 225), matching by construction.
const DATE_WORKBUF: usize = 256;

/// datetime.h unit codes for the range masks (DecodeInterval's typmod
/// range argument) — the set intervaltypmodin can actually produce.
fn range_from(b: u8) -> i32 {
    const YEAR: i32 = 25;
    const MONTH: i32 = 23;
    const DAY: i32 = 21;
    const HOUR: i32 = 26;
    const MINUTE: i32 = 27;
    const SECOND: i32 = 28;
    match b % 13 {
        0 => INTERVAL_FULL_RANGE,
        1 => INTERVAL_MASK(YEAR),
        2 => INTERVAL_MASK(MONTH),
        3 => INTERVAL_MASK(DAY),
        4 => INTERVAL_MASK(HOUR),
        5 => INTERVAL_MASK(MINUTE),
        6 => INTERVAL_MASK(SECOND),
        7 => INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH),
        8 => INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR),
        9 => INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE),
        10 => {
            INTERVAL_MASK(DAY)
                | INTERVAL_MASK(HOUR)
                | INTERVAL_MASK(MINUTE)
                | INTERVAL_MASK(SECOND)
        }
        11 => INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE),
        _ => INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND),
    }
}

fn istyle_from(b: u8) -> i32 {
    (b % 4) as i32 /* INTSTYLE_POSTGRES..INTSTYLE_ISO_8601, verbatim values */
}

fn text_payload(b: &[u8]) -> Option<(&[u8], CString)> {
    if b.len() > 200 || b.contains(&0) || std::str::from_utf8(b).is_err() {
        return None;
    }
    Some((b, CString::new(b).unwrap()))
}

pub fn interval_engine_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 3 {
        0 => decode_interval_diff(payload),
        1 => decode_iso8601_diff(payload),
        _ => encode_interval_diff(payload),
    }
}

/// strtod tininess platform carve (identical to timestamp_diff's
/// `dblmin_boundary`, which see): when any number token in the input rounds
/// to exactly +/-DBL_MIN, glibc (the oracle platform of record — docker
/// postgres:18.3 rejects such interval literals with 22007, re-verified
/// 2026-07-31) flags ERANGE via tininess-BEFORE-rounding while macOS strtod
/// flags after rounding and accepts. The shipped Rust model follows glibc,
/// so the local (macOS) C oracle diverges from BOTH real PG and pgrust on
/// exactly this band; skip the compare, run Rust for panic-safety. Fleet
/// campaigns replay the band fully (Linux libc agrees with the model).
/// Found when the 10M fleet campaign (job pgrust-fuzz-campaign-1785532939)
/// banked boundary tokens that then asserted only on laptop replay.
fn dblmin_boundary(text: &[u8]) -> bool {
    for i in 0..text.len() {
        if let Some(tok) = adt_float::scan_number(&text[i..]) {
            let t = &text[i..i + tok.len];
            let v = match tok.kind {
                adt_float::NumKind::Decimal => {
                    std::str::from_utf8(t).ok().and_then(|s| s.parse::<f64>().ok())
                }
                adt_float::NumKind::Hex => Some(adt_float::parse_hex_float(t)),
            };
            if v.is_some_and(|v| v.abs() == f64::MIN_POSITIVE) {
                return true;
            }
        }
    }
    false
}

fn decode_interval_diff(payload: &[u8]) {
    if payload.len() < 2 {
        return;
    }
    let istyle = istyle_from(payload[0]);
    let range = range_from(payload[1]);
    let Some((bytes, cs)) = text_payload(&payload[2..]) else {
        return;
    };
    set_interval_style(istyle);

    if dblmin_boundary(bytes) {
        /* strtod tininess platform carve — see dblmin_boundary; Rust side
         * still runs for panic-safety + coverage, nothing is compared. */
        let mut workbuf = [0u8; DATE_WORKBUF];
        let mut field: [&[u8]; MAXDATEFIELDS] = [b""; MAXDATEFIELDS];
        let mut ftype = [0i32; MAXDATEFIELDS];
        let mut nf = 0usize;
        if ParseDateTime(bytes, &mut workbuf, &mut field, &mut ftype, MAXDATEFIELDS, &mut nf) == 0
        {
            let mut dtype = 0i32;
            let mut itm_in = pg_itm_in::default();
            let _ = DecodeInterval(
                &field[..nf],
                &ftype[..nf],
                nf,
                range,
                &mut dtype,
                &mut itm_in,
            );
        }
        return;
    }
    let (mut cu, mut cd, mut cm, mut cy, mut cdt) = (0i64, 0i32, 0i32, 0i32, 0i32);
    let crc = unsafe {
        pg_diff_decode_interval(
            cs.as_ptr(),
            range,
            istyle,
            &mut cu,
            &mut cd,
            &mut cm,
            &mut cy,
            &mut cdt,
        )
    };

    let mut workbuf = [0u8; DATE_WORKBUF];
    let mut field: [&[u8]; MAXDATEFIELDS] = [b""; MAXDATEFIELDS];
    let mut ftype = [0i32; MAXDATEFIELDS];
    let mut nf = 0usize;
    let mut dterr =
        ParseDateTime(bytes, &mut workbuf, &mut field, &mut ftype, MAXDATEFIELDS, &mut nf);
    let mut dtype = 0i32;
    let mut itm_in = pg_itm_in::default();
    if dterr == 0 {
        dterr = DecodeInterval(&field[..nf], &ftype[..nf], nf, range, &mut dtype, &mut itm_in);
    }

    if dterr != 0 {
        assert!(
            crc == dterr,
            "DecodeInterval DIVERGENCE input={:?} istyle={istyle} range={range:#x}: C rc={crc} vs Rust dterr={dterr}",
            String::from_utf8_lossy(bytes)
        );
        return;
    }
    assert!(
        crc == 0
            && cu == itm_in.tm_usec
            && cd == itm_in.tm_mday
            && cm == itm_in.tm_mon
            && cy == itm_in.tm_year
            && cdt == dtype,
        "DecodeInterval DIVERGENCE input={:?} istyle={istyle} range={range:#x}: C=(rc {crc}, usec {cu} d {cd} m {cm} y {cy} dtype {cdt}) Rust=(usec {} d {} m {} y {} dtype {dtype})",
        String::from_utf8_lossy(bytes),
        itm_in.tm_usec,
        itm_in.tm_mday,
        itm_in.tm_mon,
        itm_in.tm_year
    );
}

fn decode_iso8601_diff(payload: &[u8]) {
    let Some((bytes, cs)) = text_payload(payload) else {
        return;
    };

    if dblmin_boundary(bytes) {
        /* strtod tininess platform carve — see dblmin_boundary */
        let mut dtype = 0i32;
        let mut itm_in = pg_itm_in::default();
        let _ = DecodeISO8601Interval(bytes, &mut dtype, &mut itm_in);
        return;
    }
    let (mut cu, mut cd, mut cm, mut cy, mut cdt) = (0i64, 0i32, 0i32, 0i32, 0i32);
    let crc = unsafe {
        pg_diff_decode_iso8601_interval(cs.as_ptr(), &mut cu, &mut cd, &mut cm, &mut cy, &mut cdt)
    };

    let mut dtype = 0i32;
    let mut itm_in = pg_itm_in::default();
    let dterr = DecodeISO8601Interval(bytes, &mut dtype, &mut itm_in);

    if dterr != 0 {
        assert!(
            crc == dterr,
            "DecodeISO8601Interval DIVERGENCE input={:?}: C rc={crc} vs Rust dterr={dterr}",
            String::from_utf8_lossy(bytes)
        );
        return;
    }
    assert!(
        crc == 0
            && cu == itm_in.tm_usec
            && cd == itm_in.tm_mday
            && cm == itm_in.tm_mon
            && cy == itm_in.tm_year
            && cdt == dtype,
        "DecodeISO8601Interval DIVERGENCE input={:?}: C=(rc {crc}, usec {cu} d {cd} m {cm} y {cy} dtype {cdt}) Rust=(usec {} d {} m {} y {} dtype {dtype})",
        String::from_utf8_lossy(bytes),
        itm_in.tm_usec,
        itm_in.tm_mday,
        itm_in.tm_mon,
        itm_in.tm_year
    );
}

fn encode_interval_diff(payload: &[u8]) {
    if payload.len() < 17 {
        return;
    }
    let istyle = istyle_from(payload[0]);
    let time = i64::from_le_bytes(payload[1..9].try_into().unwrap());
    let day = i32::from_le_bytes(payload[9..13].try_into().unwrap());
    let month = i32::from_le_bytes(payload[13..17].try_into().unwrap());

    let mut cbuf = [0u8; 512];
    let (mut iu, mut ih) = (0i64, 0i64);
    let (mut is_, mut imin, mut imd, mut imo, mut iy) = (0i32, 0i32, 0i32, 0i32, 0i32);
    let crc = unsafe {
        pg_diff_encode_interval(
            time,
            day,
            month,
            istyle,
            cbuf.as_mut_ptr(),
            &mut iu,
            &mut ih,
            &mut is_,
            &mut imin,
            &mut imd,
            &mut imo,
            &mut iy,
        )
    };
    assert!(crc == 0, "encode_interval C rc={crc} (must not error)");
    let clen = cbuf.iter().position(|&b| b == 0).unwrap();

    let itm = pg_itm {
        tm_usec: iu as i32,
        tm_sec: is_,
        tm_min: imin,
        tm_hour: ih,
        tm_mday: imd,
        tm_mon: imo,
        tm_year: iy,
    };
    let mut rbuf = [0u8; 512];
    let n = adt_datetime::EncodeInterval(&itm, istyle, &mut rbuf);
    assert!(
        &rbuf[..n] == &cbuf[..clen],
        "EncodeInterval DIVERGENCE time={time} day={day} month={month} istyle={istyle}: C={:?} Rust={:?}",
        String::from_utf8_lossy(&cbuf[..clen]),
        String::from_utf8_lossy(&rbuf[..n])
    );
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_corpus_replays_clean() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/interval_engine_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/interval_engine_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                interval_engine_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    fn arm(sel: u8, tail: &[u8]) -> Vec<u8> {
        let mut v = vec![sel];
        v.extend_from_slice(tail);
        v
    }

    #[test]
    fn arms_smoke() {
        let _serial = crate::c_oracle_serial();
        for st in 0u8..4 {
            for r in 0u8..13 {
                for text in ["1 year 2 months 3 days", "@ 1 hour ago", "-1 4:05:06",
                             "P1Y2M3DT4H5M6S", "1-2", "3 4:05:06.789", "infinity",
                             "-infinity", "ago", "1.5 weeks", "junk", "999999999 years"] {
                    let mut p = vec![0, st, r];
                    p.extend_from_slice(text.as_bytes());
                    interval_engine_diff(&p);
                }
            }
        }
        for text in ["P1Y", "PT4H5M6.7S", "P0001-02-03T04:05:06", "PT", "P", "P1W",
                     "P1.5Y", "PT1.5H", "junk", "P1Y2M3DT4H5M6S"] {
            interval_engine_diff(&arm(1, text.as_bytes()));
        }
        for st in 0u8..4 {
            for (t, d, m) in [(0i64, 0i32, 0i32), (1, 0, 0), (-1, 0, 0),
                              (3_723_000_000, 4, 5), (i64::MAX, i32::MAX, i32::MAX),
                              (i64::MIN, i32::MIN, i32::MIN), (-3_723_456_789, -4, -17)] {
                let mut p = vec![2, st];
                p.extend_from_slice(&t.to_le_bytes());
                p.extend_from_slice(&d.to_le_bytes());
                p.extend_from_slice(&m.to_le_bytes());
                interval_engine_diff(&p);
            }
        }
    }
}
