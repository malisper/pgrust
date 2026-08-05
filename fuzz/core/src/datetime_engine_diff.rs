//! datetime_engine_diff: differential fuzz driver — shipped Rust
//! `adt_datetime` timestamp-image encoder and ISO week/year calendar helpers
//! vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_datetime_io_io.c).
//!
//! Scope note (claims): the SQL entry points that reach `EncodeDateTime`
//! (timestamp_out / timestamptz_out) and the ISO week/year helpers
//! (extract/to_char) live in adt_timestamp and adt_formatting, neither of
//! which this campaign lane has claimed. Both surfaces are therefore compared
//! at the ENGINE level, entirely inside the claimed adt_datetime crate, the
//! same shape `interval_engine_diff` uses for `EncodeInterval`.
//!
//! Why a separate target rather than more arms on `interval_engine_diff`: that
//! target dispatches on `sel % 3`, and its banked corpus (11k+ entries, one
//! 10M-exec fleet campaign) is keyed to that modulus. Widening it would remap
//! every banked seed to a different arm, discarding the measured coverage the
//! bank represents. A new target keeps both corpora meaningful; the marginal
//! fleet cost is one more (parallel, ~5 min) campaign.
//!
//! Environment: DateStyle x DateOrder driven from a selector byte on both
//! sides (all 5 styles x 3 orders). No tz-database, clock or session state is
//! reachable from these engines — `EncodeDateTime` takes its zone as a
//! caller-supplied (tz, tzn) pair, so nothing here touches the zone-name cache
//! the io target has to ration.
//!
//! Domain fences (all three named, all matching a C-side contract or a C-side
//! undefined behavior — see fold_mon / fold_absable below): tm_mon folded into
//! EncodeDateTime's own Assert range, and sec / fsec / tz kept off i32::MIN
//! where the vendored code's abs() is undefined. Nothing real PostgreSQL can
//! produce is fenced out.
//!
//! Comparison planes: the full emitted image bytes (C's NUL-terminated buffer
//! vs the Rust length-returning writer, compared over the whole prefix), the
//! `tm_wday` write-back the USE_POSTGRES_DATES arm performs, and the returned
//! ISO week/year scalars. There is no error plane: none of these functions can
//! fail (no ereport reachable), which the harness asserts for the C side.
//!
//! Input layout: [selector][payload]; selector % 4 picks the arm:
//!   0 EncodeDateTime   — [style/order][flags][tm fields][fsec][tz][tzn text]
//!   1 date2isoweek + date2isoyear + date2isoyearday — [year][mon][mday]
//!   2 isoweek2j + isoweek2date                      — [year][week]
//!   3 isoweekdate2date                              — [year][isoweek][wday]

use std::ffi::CString;

use adt_datetime::{
    date2isoweek, date2isoyear, date2isoyearday, isoweek2date, isoweek2j, isoweekdate2date,
    set_date_order, set_date_style, DATEORDER_DMY, DATEORDER_MDY, DATEORDER_YMD, USE_GERMAN_DATES,
    USE_ISO_DATES, USE_POSTGRES_DATES, USE_SQL_DATES, USE_XSD_DATES,
};
use adt_datetime::{pg_tm, EncodeDateTime};

extern "C" {
    fn pg_diff_encode_datetime(
        year: i32,
        mon: i32,
        mday: i32,
        hour: i32,
        min: i32,
        sec: i32,
        isdst: i32,
        fsec: i64,
        print_tz: i32,
        tz: i32,
        tzn: *const std::ffi::c_char,
        style: i32,
        order: i32,
        buf: *mut u8,
        out_wday: *mut i32,
    ) -> i32;
    fn pg_diff_date2isoweek(year: i32, mon: i32, mday: i32) -> i32;
    fn pg_diff_date2isoyear(year: i32, mon: i32, mday: i32) -> i32;
    fn pg_diff_date2isoyearday(year: i32, mon: i32, mday: i32) -> i32;
    fn pg_diff_isoweek2j(year: i32, week: i32) -> i32;
    fn pg_diff_isoweek2date(woy: i32, year: *mut i32, mon: *mut i32, mday: *mut i32);
    fn pg_diff_isoweekdate2date(
        isoweek: i32,
        wday: i32,
        year: *mut i32,
        mon: *mut i32,
        mday: *mut i32,
    );
}

/// Both sides write into a buffer this size. Worst case is far smaller: six
/// numeric fields at their 10-digit maximum plus separators, a zone and " BC"
/// stay under 128 (real PG sizes this MAXDATELEN=128).
const IMGBUF: usize = 512;

fn style_order_from(b: u8) -> (i32, i32) {
    let style = match b % 5 {
        0 => USE_POSTGRES_DATES,
        1 => USE_ISO_DATES,
        2 => USE_SQL_DATES,
        3 => USE_GERMAN_DATES,
        _ => USE_XSD_DATES,
    };
    let order = match (b / 5) % 3 {
        0 => DATEORDER_YMD,
        1 => DATEORDER_DMY,
        _ => DATEORDER_MDY,
    };
    set_date_style(style);
    set_date_order(order);
    (style, order)
}

fn i32_at(b: &[u8], o: usize) -> i32 {
    i32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}

/// `EncodeDateTime`'s declared contract is `Assert(tm_mon >= 1 && tm_mon <=
/// MONTHS_PER_YEAR)` (datetime.c:4468). Outside it the C indexes `months[]`
/// out of bounds, so the month is folded into the contract rather than
/// compared out of it — a named domain fence, not a skipped comparison.
fn fold_mon(raw: i32) -> i32 {
    (raw.rem_euclid(12)) + 1
}

/// `abs()` on `INT_MIN` is undefined in C, and the vendored code applies it to
/// `sec` and `fsec` (`AppendSeconds`) and to `tz` (`EncodeTimezone`). Folding
/// the sentinel away keeps the oracle defined; every other value of all three
/// fields stays in the compared domain. Real PostgreSQL cannot reach the
/// sentinel on any of them — zone offsets are bounded by ±15:59:59 and
/// timestamp2tm's seconds by 60 — so nothing observable is fenced out. (Rust's
/// `unsigned_abs()` is well-defined at the sentinel and simply has no C
/// behavior to be compared against.)
fn fold_absable(raw: i32) -> i32 {
    if raw == i32::MIN {
        i32::MIN + 1
    } else {
        raw
    }
}

/// Zone-name plane: NUL-free ASCII, deliberately allowed past MAXTZLEN (10) so
/// the truncating copy on both sides is witnessed.
fn fold_tzn(b: &[u8]) -> Option<Vec<u8>> {
    if b.len() > 24 {
        return None;
    }
    let v: Vec<u8> = b
        .iter()
        .copied()
        .filter(|&c| c.is_ascii_graphic() || c == b' ')
        .collect();
    Some(v)
}

pub fn datetime_engine_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 4 {
        0 => encode_datetime_diff(payload),
        1 => date2iso_diff(payload),
        2 => isoweek2_diff(payload),
        _ => isoweekdate2date_diff(payload),
    }
}

fn encode_datetime_diff(payload: &[u8]) {
    // [0] style/order, [1] flags, [2..30] 7 i32 tm fields, [30..34] fsec,
    // [34..38] tz, [38..] tzn text
    if payload.len() < 38 {
        return;
    }
    let (style, order) = style_order_from(payload[0]);
    let flags = payload[1];
    let print_tz = flags & 1 != 0;
    let have_tzn = flags & 2 != 0;

    let year = i32_at(payload, 2);
    let mon = fold_mon(i32_at(payload, 6));
    let mday = i32_at(payload, 10);
    let hour = i32_at(payload, 14);
    let min = i32_at(payload, 18);
    let sec = fold_absable(i32_at(payload, 22));
    let isdst = i32_at(payload, 26);
    let fsec = fold_absable(i32_at(payload, 30));
    let tz = fold_absable(i32_at(payload, 34));

    let tzn_bytes = if have_tzn {
        match fold_tzn(&payload[38..]) {
            Some(v) => Some(v),
            None => return,
        }
    } else {
        None
    };
    let ctzn = tzn_bytes
        .as_ref()
        .map(|v| CString::new(v.clone()).expect("filtered NUL-free"));

    let mut cbuf = [0u8; IMGBUF];
    let mut cwday = 0i32;
    let crc = unsafe {
        pg_diff_encode_datetime(
            year,
            mon,
            mday,
            hour,
            min,
            sec,
            isdst,
            fsec as i64,
            print_tz as i32,
            tz,
            ctzn.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
            style,
            order,
            cbuf.as_mut_ptr(),
            &mut cwday,
        )
    };
    assert!(
        crc == 0,
        "EncodeDateTime C errcode={crc} (this encoder cannot ereport)"
    );
    let clen = cbuf.iter().position(|&b| b == 0).expect("C NUL-terminates");

    let mut tm = pg_tm {
        tm_sec: sec,
        tm_min: min,
        tm_hour: hour,
        tm_mday: mday,
        tm_mon: mon,
        tm_year: year,
        tm_wday: 0,
        tm_yday: 0,
        tm_isdst: isdst,
        tm_gmtoff: 0,
        tm_zone: None,
    };
    let mut rbuf = [0u8; IMGBUF];
    let n = EncodeDateTime(
        &mut tm,
        fsec,
        print_tz,
        tz,
        tzn_bytes.as_deref(),
        style,
        &mut rbuf,
    );

    assert!(
        rbuf[..n] == cbuf[..clen],
        "EncodeDateTime IMAGE DIVERGENCE style={style} order={order} \
         tm=(y {year} mon {mon} mday {mday} h {hour} min {min} s {sec} isdst {isdst}) \
         fsec={fsec} print_tz={print_tz} tz={tz} tzn={:?}: C={:?} Rust={:?}",
        tzn_bytes.as_ref().map(|v| String::from_utf8_lossy(v)),
        String::from_utf8_lossy(&cbuf[..clen]),
        String::from_utf8_lossy(&rbuf[..n])
    );
    assert!(
        cwday == tm.tm_wday,
        "EncodeDateTime tm_wday WRITE-BACK DIVERGENCE style={style} \
         tm=(y {year} mon {mon} mday {mday}): C={cwday} Rust={}",
        tm.tm_wday
    );
}

fn date2iso_diff(payload: &[u8]) {
    if payload.len() < 12 {
        return;
    }
    let year = i32_at(payload, 0);
    let mon = i32_at(payload, 4);
    let mday = i32_at(payload, 8);

    let (cw, cy, cyd) = unsafe {
        (
            pg_diff_date2isoweek(year, mon, mday),
            pg_diff_date2isoyear(year, mon, mday),
            pg_diff_date2isoyearday(year, mon, mday),
        )
    };
    let (rw, ry, ryd) = (
        date2isoweek(year, mon, mday),
        date2isoyear(year, mon, mday),
        date2isoyearday(year, mon, mday),
    );
    assert!(
        cw == rw && cy == ry && cyd == ryd,
        "ISO week/year DIVERGENCE (y {year} mon {mon} mday {mday}): \
         C=(week {cw} year {cy} yearday {cyd}) Rust=(week {rw} year {ry} yearday {ryd})"
    );
}

fn isoweek2_diff(payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let year = i32_at(payload, 0);
    let week = i32_at(payload, 4);

    let cj = unsafe { pg_diff_isoweek2j(year, week) };
    let rj = isoweek2j(year, week);
    assert!(
        cj == rj,
        "isoweek2j DIVERGENCE (year {year} week {week}): C={cj} Rust={rj}"
    );

    // isoweek2date reads *year as input and overwrites all three out params.
    let (mut cy, mut cm, mut cd) = (year, 0i32, 0i32);
    unsafe { pg_diff_isoweek2date(week, &mut cy, &mut cm, &mut cd) };
    let (mut ry, mut rm, mut rd) = (year, 0i32, 0i32);
    isoweek2date(week, &mut ry, &mut rm, &mut rd);
    assert!(
        cy == ry && cm == rm && cd == rd,
        "isoweek2date DIVERGENCE (year {year} woy {week}): \
         C=({cy},{cm},{cd}) Rust=({ry},{rm},{rd})"
    );
}

fn isoweekdate2date_diff(payload: &[u8]) {
    if payload.len() < 12 {
        return;
    }
    let year = i32_at(payload, 0);
    let isoweek = i32_at(payload, 4);
    let wday = i32_at(payload, 8);

    let (mut cy, mut cm, mut cd) = (year, 0i32, 0i32);
    unsafe { pg_diff_isoweekdate2date(isoweek, wday, &mut cy, &mut cm, &mut cd) };
    let (mut ry, mut rm, mut rd) = (year, 0i32, 0i32);
    isoweekdate2date(isoweek, wday, &mut ry, &mut rm, &mut rd);
    assert!(
        cy == ry && cm == rm && cd == rd,
        "isoweekdate2date DIVERGENCE (year {year} isoweek {isoweek} wday {wday}): \
         C=({cy},{cm},{cd}) Rust=({ry},{rm},{rd})"
    );
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_corpus_replays_clean() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/datetime_engine_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/datetime_engine_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                datetime_engine_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 60, "expected >=60 seeds, found {n}");
    }

    /// Build an EncodeDateTime arm input from explicit fields — also the
    /// generator the single-field-difference witness seeds are cut from.
    pub(crate) fn enc_arm(
        so: u8,
        flags: u8,
        tm: [i32; 7],
        fsec: i32,
        tz: i32,
        tzn: &[u8],
    ) -> Vec<u8> {
        let mut v = vec![0u8, so, flags];
        // selector byte 0 selects the EncodeDateTime arm; payload starts at v[1]
        for f in tm {
            v.extend_from_slice(&f.to_le_bytes());
        }
        v.extend_from_slice(&fsec.to_le_bytes());
        v.extend_from_slice(&tz.to_le_bytes());
        v.extend_from_slice(tzn);
        v
    }

    pub(crate) fn tri_arm(sel: u8, a: i32, b: i32, c: i32) -> Vec<u8> {
        let mut v = vec![sel];
        v.extend_from_slice(&a.to_le_bytes());
        v.extend_from_slice(&b.to_le_bytes());
        v.extend_from_slice(&c.to_le_bytes());
        v
    }

    const BASE_TM: [i32; 7] = [2026, 6, 15, 12, 30, 45, 0];

    #[test]
    fn arms_smoke_encode_datetime() {
        let _serial = crate::c_oracle_serial();
        for so in 0u8..15 {
            for flags in 0u8..4 {
                for tzn in [&b""[..], b"GMT", b"UTC", b"America/Los_Angeles", b"+05"] {
                    for tm in [
                        BASE_TM,
                        [1, 1, 1, 0, 0, 0, 0],
                        [0, 12, 31, 23, 59, 59, 0],
                        [-1, 2, 29, 0, 0, 0, -1],
                        [-4713, 11, 24, 0, 0, 0, 1],
                        [294276, 12, 31, 23, 59, 59, 0],
                        [1999, 12, 31, 23, 59, 60, 0],
                    ] {
                        for fsec in [0, 1, 999_999, -1, 1_000_000] {
                            datetime_engine_diff(&enc_arm(so, flags, tm, fsec, -28800, tzn));
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn arms_smoke_iso_family() {
        let _serial = crate::c_oracle_serial();
        let years = [
            0, 1, -1, 1970, 2000, 2026, -4713, 294276, 1, i32::MAX, i32::MIN, i32::MAX - 1,
        ];
        for y in years {
            for m in [-1, 0, 1, 2, 6, 12, 13] {
                for d in [-1, 0, 1, 4, 28, 31, 32] {
                    datetime_engine_diff(&tri_arm(1, y, m, d));
                }
            }
            for w in [-5, 0, 1, 2, 52, 53, 54, i32::MAX, i32::MIN] {
                datetime_engine_diff(&tri_arm(2, y, w, 0));
                for wd in [-4, 0, 1, 2, 7, 8, 11] {
                    datetime_engine_diff(&tri_arm(3, y, w, wd));
                }
            }
        }
    }

    /// Single-field-difference witness pairs (campaign obligation): the tm
    /// staging packs seven i32 fields into one byte string, and only pairs
    /// differing in exactly ONE field can witness that each field reaches the
    /// output slot it is supposed to. Line coverage cannot detect their
    /// absence — an earlier lane's byte-shift mutants survived a 27M-exec
    /// corpus at full line coverage for exactly this reason.
    #[test]
    fn single_field_difference_witnesses() {
        let _serial = crate::c_oracle_serial();
        for so in 0u8..15 {
            for field in 0..7 {
                for delta in [1i32, -1, 2, -2, 10, -10] {
                    let mut tm = BASE_TM;
                    tm[field] = tm[field].wrapping_add(delta);
                    // both orders: base-then-variant and variant-then-base
                    datetime_engine_diff(&enc_arm(so, 3, BASE_TM, 500_000, -28800, b"PST"));
                    datetime_engine_diff(&enc_arm(so, 3, tm, 500_000, -28800, b"PST"));
                    datetime_engine_diff(&enc_arm(so, 1, tm, 500_000, -28800, b""));
                    datetime_engine_diff(&enc_arm(so, 1, BASE_TM, 500_000, -28800, b""));
                }
            }
            // fsec, tz and tzn are the remaining single-field slots
            for d in [1i32, -1, 10, -10, 100_000, -100_000] {
                datetime_engine_diff(&enc_arm(so, 3, BASE_TM, 500_000 + d, -28800, b"PST"));
                datetime_engine_diff(&enc_arm(so, 3, BASE_TM, 500_000, -28800 + d, b"PST"));
            }
            for tzn in [&b"P"[..], b"PS", b"PST", b"PSTX", b"PACIFICSTAND", b"PACIFICSTANDARD"] {
                datetime_engine_diff(&enc_arm(so, 3, BASE_TM, 500_000, -28800, tzn));
            }
        }
    }
}

