//! Datetime SQL entry cores: timestamp/timestamptz `to_char`, and the
//! `to_timestamp` / `to_date` consumers over `do_to_timestamp`
//! (formatting.c:3942-4867).

use std::rc::Rc;

use ::datum::Varlena;
use ::mcx::{Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, SoftErrorContext, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE};

use ::adt_datetime::{
    date2j, isleap, j2date, pg_tm, DateTimeParseError, ValidateDate, DTK_M, DAY, DTERR_FIELD_OVERFLOW,
    DTERR_TZDISP_OVERFLOW, HOURS_PER_DAY, MAX_TZDISP_HOUR, MINS_PER_HOUR, MONTH, MONTHS_PER_YEAR,
    POSTGRES_EPOCH_JDATE, SECS_PER_HOUR, SECS_PER_MINUTE, Timestamp, USECS_PER_SEC, YEAR,
    IS_VALID_JULIAN,
};
use ::adt_datetime::fsec_t;
use ::adt_timestamp::{
    tm2timestamp, timestamp2tm, AdjustTimestampForTypmod, TIMESTAMP_NOT_FINITE,
};
use ::adt_date::{DateADT, IS_VALID_DATE};

use crate::cache::dch_cache_fetch;
use crate::dch::{dch_to_char, FmtTm, FmtTz, TmToChar};
use crate::dch_fromchar::{dch_datetime_type, dch_from_char, TmFromChar};
use crate::parse::parse_format;
use crate::tables::*;

const VARHDRSZ: usize = ::datum::varlena::VARHDRSZ;

fn text_result<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<Varlena<'mcx>> {
    let cap = VARHDRSZ + payload.len();
    let mut image: PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, cap)?;
    ::mcx::vec_append_bytes(&mut image, &[0u8; VARHDRSZ])?;
    ::mcx::vec_append_bytes(&mut image, payload)?;
    Ok(Varlena::from_image(image))
}

fn zero_tm() -> pg_tm {
    pg_tm {
        tm_mday: 1,
        tm_mon: 1,
        ..Default::default()
    }
}

fn copy_tm(dst: &mut FmtTm, src: &pg_tm) {
    dst.tm_sec = src.tm_sec;
    dst.tm_min = src.tm_min;
    dst.tm_hour = src.tm_hour as i64;
    dst.tm_mday = src.tm_mday;
    dst.tm_mon = src.tm_mon;
    dst.tm_year = src.tm_year;
    dst.tm_wday = src.tm_wday;
    dst.tm_yday = src.tm_yday;
    dst.tm_gmtoff = src.tm_gmtoff;
}

#[track_caller]
#[cold]
fn timestamp_out_of_range() -> Box<PgError> {
    Box::new(
        PgError::error("timestamp out of range".to_string())
            .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
    )
}

#[track_caller]
#[cold]
fn date_out_of_range(date_txt: &[u8]) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "date out of range: \"{}\"",
            String::from_utf8_lossy(date_txt)
        ))
        .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
    )
}

fn fetch_format(fmt: &[u8], std: bool) -> PgResult<Rc<[FormatNode]>> {
    if fmt.len() > DCH_CACHE_SIZE {
        Ok(parse_format(
            fmt,
            DCH_KEYWORDS,
            DCH_SUFF,
            &DCH_INDEX,
            DCH_FLAG | if std { STD_FLAG } else { 0 },
            None,
        )?
        .into())
    } else {
        dch_cache_fetch(fmt, std)
    }
}

// C datetime_format_has_tz (formatting.c:4366).
pub fn datetime_format_has_tz(fmt: &[u8]) -> PgResult<bool> {
    let format = fetch_format(fmt, false)?;
    Ok(dch_datetime_type(&format) & DCH_ZONED != 0)
}

fn datetime_to_char_body<'mcx>(
    mcx: Mcx<'mcx>,
    tmtc: &TmToChar,
    fmt: &[u8],
    is_interval: bool,
    collid: Oid,
) -> PgResult<Vec<u8>> {
    let format = fetch_format(fmt, false)?;
    dch_to_char(mcx, &format, is_interval, tmtc, collid)
}

pub fn timestamp_to_char<'mcx>(mcx: Mcx<'mcx>,
    collid: Oid, ts: i64, fmt: &[u8]) -> PgResult<Varlena<'mcx>> {
    if fmt.is_empty() || TIMESTAMP_NOT_FINITE(ts) {
        return text_result(mcx, b"");
    }

    let mut tm = zero_tm();
    let mut fsec: fsec_t = 0;
    if timestamp2tm(ts, None, &mut tm, &mut fsec, None, None).is_err() {
        return Err(timestamp_out_of_range());
    }

    let thisdate = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday);
    tm.tm_wday = (thisdate + 1) % 7;
    tm.tm_yday = thisdate - date2j(tm.tm_year, 1, 1) + 1;

    let mut tmtc = TmToChar::zero();
    copy_tm(&mut tmtc.tm, &tm);
    tmtc.fsec = fsec;

    let out = datetime_to_char_body(mcx, &tmtc, fmt, false, collid)?;
    text_result(mcx, &out)
}

pub fn timestamptz_to_char<'mcx>(mcx: Mcx<'mcx>,
    collid: Oid, ts: i64, fmt: &[u8]) -> PgResult<Varlena<'mcx>> {
    if fmt.is_empty() || TIMESTAMP_NOT_FINITE(ts) {
        return text_result(mcx, b"");
    }

    let mut tm = zero_tm();
    let mut fsec: fsec_t = 0;
    let mut tz: i32 = 0;
    let mut tzn: Option<&'static str> = None;
    if timestamp2tm(ts, Some(&mut tz), &mut tm, &mut fsec, Some(&mut tzn), None).is_err() {
        return Err(timestamp_out_of_range());
    }

    let thisdate = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday);
    tm.tm_wday = (thisdate + 1) % 7;
    tm.tm_yday = thisdate - date2j(tm.tm_year, 1, 1) + 1;

    let mut tmtc = TmToChar::zero();
    copy_tm(&mut tmtc.tm, &tm);
    tmtc.fsec = fsec;
    tmtc.tzn = tzn.map(|s| s.as_bytes().to_vec());

    let out = datetime_to_char_body(mcx, &tmtc, fmt, false, collid)?;
    text_result(mcx, &out)
}

pub fn interval_to_char<'mcx>(
    mcx: Mcx<'mcx>,
    collid: Oid,
    it: &::adt_datetime::Interval,
    fmt: &[u8],
) -> PgResult<Varlena<'mcx>> {
    let mut tt = ::adt_datetime::pg_itm::default();
    ::adt_timestamp::interval::interval2itm(*it, &mut tt);

    let mut tmtc = TmToChar::zero();
    tmtc.fsec = tt.tm_usec;
    tmtc.tm.tm_sec = tt.tm_sec;
    tmtc.tm.tm_min = tt.tm_min;
    tmtc.tm.tm_hour = tt.tm_hour;
    tmtc.tm.tm_mday = tt.tm_mday;
    tmtc.tm.tm_mon = tt.tm_mon;
    tmtc.tm.tm_year = tt.tm_year;

    // C formatting.c interval_to_char: "wday is meaningless, yday
    // approximates the total span in days" — the DDD/WW codes read tm_yday
    // (negative for negative intervals; C prints the signed value). C's
    // plain-int math is compiled with -fwrapv; wrap exactly like C on
    // extreme month counts (MONTHS_PER_YEAR = 12, DAYS_PER_MONTH = 30).
    tmtc.tm.tm_yday = tt
        .tm_year
        .wrapping_mul(12)
        .wrapping_add(tt.tm_mon)
        .wrapping_mul(30)
        .wrapping_add(tt.tm_mday);

    let out = datetime_to_char_body(mcx, &tmtc, fmt, true, collid)?;
    text_result(mcx, &out)
}

pub fn to_timestamp<'mcx>(mcx: Mcx<'mcx>,
    collid: Oid, text: &[u8], fmt: &[u8]) -> PgResult<i64> {
    let mut tm = zero_tm();
    let mut ftz = FmtTz::default();
    let mut fsec: fsec_t = 0;
    let mut fprec: i32 = 0;

    do_to_timestamp(
        mcx,
        text,
        fmt,
        collid,
        false,
        &mut tm,
        &mut fsec,
        &mut ftz,
        Some(&mut fprec),
        None,
        None,
    )?;

    let tz = if ftz.has_tz {
        ftz.gmtoffset
    } else {
        let session = ::adt_datetime::tz::session_timezone()
            .unwrap_or_else(|| panic!("to_timestamp: session_timezone not initialized"));
        ::adt_datetime::tz::DetermineTimeZoneOffset(&mut tm, session)
    };

    let mut result: Timestamp = 0;
    if tm2timestamp(&tm, fsec, Some(tz), &mut result).is_err() {
        return Err(timestamp_out_of_range());
    }

    if fprec != 0 {
        AdjustTimestampForTypmod(&mut result, fprec, None)?;
    }

    Ok(result)
}

pub fn to_date<'mcx>(mcx: Mcx<'mcx>,
    collid: Oid, text: &[u8], fmt: &[u8]) -> PgResult<DateADT> {
    let mut tm = zero_tm();
    let mut ftz = FmtTz::default();
    let mut fsec: fsec_t = 0;

    do_to_timestamp(
        mcx, text, fmt, collid, false, &mut tm, &mut fsec, &mut ftz, None, None, None,
    )?;

    if !IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday) {
        return Err(date_out_of_range(text));
    }

    let result = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;

    if !IS_VALID_DATE(result) {
        return Err(date_out_of_range(text));
    }

    Ok(result)
}

fn dtk_date_m() -> i32 {
    DTK_M(YEAR) | DTK_M(MONTH) | DTK_M(DAY)
}

fn dterr(code: i32, s: &[u8], escontext: Option<&mut SoftErrorContext>) -> PgResult<()> {
    DateTimeParseError(code, None, &String::from_utf8_lossy(s), "timestamp", escontext)
}

/// C: `do_to_timestamp` (formatting.c:4442).
#[allow(clippy::too_many_arguments)]
pub fn do_to_timestamp<'mcx>(
    mcx: Mcx<'mcx>,
    date_txt: &[u8],
    fmt: &[u8],
    collid: Oid,
    std: bool,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    tz: &mut FmtTz,
    mut fprec: Option<&mut i32>,
    mut flags: Option<&mut u32>,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    let mut tmfc = TmFromChar::default();
    *tm = zero_tm();
    *fsec = 0;
    tz.has_tz = false;
    if let Some(f) = fprec.as_deref_mut() {
        *f = 0;
    }
    if let Some(f) = flags.as_deref_mut() {
        *f = 0;
    }
    let mut fmask: i32 = 0;

    if !fmt.is_empty() {
        let format = fetch_format(fmt, std)?;
        if !dch_from_char(mcx, &format, date_txt, &mut tmfc, collid, std, escontext.as_deref_mut())?
        {
            return Ok(false);
        }
        if let Some(f) = flags {
            *f = dch_datetime_type(&format) as u32;
        }
    }

    if tmfc.ssss != 0 {
        let mut x = tmfc.ssss;
        tm.tm_hour = x / SECS_PER_HOUR;
        x %= SECS_PER_HOUR;
        tm.tm_min = x / SECS_PER_MINUTE;
        x %= SECS_PER_MINUTE;
        tm.tm_sec = x;
    }
    if tmfc.ss != 0 {
        tm.tm_sec = tmfc.ss;
    }
    if tmfc.mi != 0 {
        tm.tm_min = tmfc.mi;
    }
    if tmfc.hh != 0 {
        tm.tm_hour = tmfc.hh;
    }

    if tmfc.clock == CLOCK_12_HOUR {
        if tm.tm_hour < 1 || tm.tm_hour > HOURS_PER_DAY / 2 {
            ::types_error::ereturn(
                escontext.as_deref_mut(),
                false,
                PgError::error(format!(
                    "hour \"{}\" is invalid for the 12-hour clock",
                    tm.tm_hour
                ))
                .with_sqlstate(::types_error::ERRCODE_INVALID_DATETIME_FORMAT)
                .with_hint("Use the 24-hour clock, or give an hour between 1 and 12."),
            )?;
            return Ok(false);
        }
        if tmfc.pm != 0 && tm.tm_hour < HOURS_PER_DAY / 2 {
            tm.tm_hour += HOURS_PER_DAY / 2;
        } else if tmfc.pm == 0 && tm.tm_hour == HOURS_PER_DAY / 2 {
            tm.tm_hour = 0;
        }
    }

    if tmfc.year != 0 {
        if tmfc.cc != 0 && tmfc.yysz <= 2 {
            if tmfc.bc != 0 {
                tmfc.cc = -tmfc.cc;
            }
            tm.tm_year = tmfc.year % 100;
            if tm.tm_year != 0 {
                if tmfc.cc >= 0 {
                    match (tmfc.cc - 1).checked_mul(100).and_then(|t| tm.tm_year.checked_add(t)) {
                        Some(v) => tm.tm_year = v,
                        None => {
                            dterr(DTERR_FIELD_OVERFLOW, date_txt, escontext.as_deref_mut())?;
                            return Ok(false);
                        }
                    }
                } else {
                    match (tmfc.cc + 1)
                        .checked_mul(100)
                        .and_then(|t| t.checked_sub(tm.tm_year))
                        .and_then(|t| t.checked_add(1))
                    {
                        Some(v) => tm.tm_year = v,
                        None => {
                            dterr(DTERR_FIELD_OVERFLOW, date_txt, escontext.as_deref_mut())?;
                            return Ok(false);
                        }
                    }
                }
            } else {
                tm.tm_year = tmfc.cc * 100 + (if tmfc.cc >= 0 { 0 } else { 1 });
            }
        } else {
            tm.tm_year = tmfc.year;
            if tmfc.bc != 0 {
                tm.tm_year = -tm.tm_year;
            }
            if tm.tm_year < 0 {
                tm.tm_year += 1;
            }
        }
        fmask |= DTK_M(YEAR);
    } else if tmfc.cc != 0 {
        if tmfc.bc != 0 {
            tmfc.cc = -tmfc.cc;
        }
        if tmfc.cc >= 0 {
            match (tmfc.cc - 1).checked_mul(100).and_then(|t| t.checked_add(1)) {
                Some(v) => tm.tm_year = v,
                None => {
                    dterr(DTERR_FIELD_OVERFLOW, date_txt, escontext.as_deref_mut())?;
                    return Ok(false);
                }
            }
        } else {
            match tmfc.cc.checked_mul(100).and_then(|t| t.checked_add(1)) {
                Some(v) => tm.tm_year = v,
                None => {
                    dterr(DTERR_FIELD_OVERFLOW, date_txt, escontext.as_deref_mut())?;
                    return Ok(false);
                }
            }
        }
        fmask |= DTK_M(YEAR);
    }

    if tmfc.j != 0 {
        let (mut y, mut m, mut d) = (0, 0, 0);
        j2date(tmfc.j, &mut y, &mut m, &mut d);
        tm.tm_year = y;
        tm.tm_mon = m;
        tm.tm_mday = d;
        fmask |= dtk_date_m();
    }

    if tmfc.ww != 0 {
        if tmfc.mode == FromCharDateMode::Isoweek {
            let (mut y, mut m, mut d) = (tm.tm_year, 0, 0);
            if tmfc.d != 0 {
                crate::isoweek::isoweekdate2date(tmfc.ww, tmfc.d, &mut y, &mut m, &mut d);
            } else {
                crate::isoweek::isoweek2date(tmfc.ww, &mut y, &mut m, &mut d);
            }
            tm.tm_year = y;
            tm.tm_mon = m;
            tm.tm_mday = d;
            fmask |= dtk_date_m();
        } else {
            match tmfc
                .ww
                .checked_sub(1)
                .and_then(|t| t.checked_mul(7))
                .and_then(|t| t.checked_add(1))
            {
                Some(v) => tmfc.ddd = v,
                None => {
                    dterr(DTERR_FIELD_OVERFLOW, date_txt, escontext.as_deref_mut())?;
                    return Ok(false);
                }
            }
        }
    }

    if tmfc.w != 0 {
        match tmfc
            .w
            .checked_sub(1)
            .and_then(|t| t.checked_mul(7))
            .and_then(|t| t.checked_add(1))
        {
            Some(v) => tmfc.dd = v,
            None => {
                dterr(DTERR_FIELD_OVERFLOW, date_txt, escontext.as_deref_mut())?;
                return Ok(false);
            }
        }
    }
    if tmfc.dd != 0 {
        tm.tm_mday = tmfc.dd;
        fmask |= DTK_M(DAY);
    }
    if tmfc.mm != 0 {
        tm.tm_mon = tmfc.mm;
        fmask |= DTK_M(MONTH);
    }

    if tmfc.ddd != 0 && (tm.tm_mon <= 1 || tm.tm_mday <= 1) {
        if tm.tm_year == 0 && tmfc.bc == 0 {
            ::types_error::ereturn(
                escontext.as_deref_mut(),
                false,
                PgError::error(
                    "cannot calculate day of year without year information".to_string(),
                )
                .with_sqlstate(::types_error::ERRCODE_INVALID_DATETIME_FORMAT),
            )?;
            return Ok(false);
        }

        if tmfc.mode == FromCharDateMode::Isoweek {
            let j0 = crate::isoweek::isoweek2j(tm.tm_year, 1) - 1;
            let (mut y, mut m, mut d) = (0, 0, 0);
            j2date(j0 + tmfc.ddd, &mut y, &mut m, &mut d);
            tm.tm_year = y;
            tm.tm_mon = m;
            tm.tm_mday = d;
            fmask |= dtk_date_m();
        } else {
            const YSUM: [[i32; 13]; 2] = [
                [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365],
                [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366],
            ];
            let y = &YSUM[if isleap(tm.tm_year) { 1 } else { 0 }];
            let mut i = 1;
            while i <= MONTHS_PER_YEAR {
                if tmfc.ddd <= y[i as usize] {
                    break;
                }
                i += 1;
            }
            if tm.tm_mon <= 1 {
                tm.tm_mon = i;
            }
            if tm.tm_mday <= 1 {
                tm.tm_mday = tmfc.ddd - y[(i - 1) as usize];
            }
            fmask |= DTK_M(MONTH) | DTK_M(DAY);
        }
    }

    if tmfc.ms != 0 {
        match tmfc.ms.checked_mul(1000).and_then(|t| (*fsec).checked_add(t)) {
            Some(v) => *fsec = v,
            None => {
                dterr(DTERR_FIELD_OVERFLOW, date_txt, escontext.as_deref_mut())?;
                return Ok(false);
            }
        }
    }
    if tmfc.us != 0 {
        *fsec += tmfc.us;
    }
    if let Some(f) = fprec {
        *f = tmfc.ff;
    }

    if fmask != 0 {
        // AD/BC already applied above (C passes isjulian=true).
        let e = ValidateDate(fmask, true, false, false, tm);
        if e != 0 {
            dterr(DTERR_FIELD_OVERFLOW, date_txt, escontext.as_deref_mut())?;
            return Ok(false);
        }
    }

    if tm.tm_hour < 0
        || tm.tm_hour >= HOURS_PER_DAY
        || tm.tm_min < 0
        || tm.tm_min >= MINS_PER_HOUR
        || tm.tm_sec < 0
        || tm.tm_sec >= SECS_PER_MINUTE
        || (*fsec as i64) < 0
        || (*fsec as i64) >= USECS_PER_SEC
    {
        dterr(DTERR_FIELD_OVERFLOW, date_txt, escontext.as_deref_mut())?;
        return Ok(false);
    }

    if tmfc.tzsign != 0 {
        if tmfc.tzh < 0 || tmfc.tzh > MAX_TZDISP_HOUR || tmfc.tzm < 0 || tmfc.tzm >= MINS_PER_HOUR {
            dterr(DTERR_TZDISP_OVERFLOW, date_txt, escontext)?;
            return Ok(false);
        }
        tz.has_tz = true;
        tz.gmtoffset = (tmfc.tzh * MINS_PER_HOUR + tmfc.tzm) * SECS_PER_MINUTE;
        if tmfc.tzsign > 0 {
            tz.gmtoffset = -tz.gmtoffset;
        }
    } else if tmfc.has_tz {
        tz.has_tz = true;
        match tmfc.tzp {
            None => tz.gmtoffset = -tmfc.gmtoffset,
            Some(tzp) => {
                tz.gmtoffset = ::adt_datetime::tz::DetermineTimeZoneAbbrevOffset(
                    tm,
                    &tmfc.abbrev[..tmfc.abbrev_len as usize],
                    tzp,
                );
            }
        }
    }

    Ok(true)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParsedDatetime {
    Date(DateADT),
    Time(::adt_date::TimeADT),
    TimeTz(::adt_date::TimeTzADT),
    Timestamp(Timestamp),
    TimestampTz(Timestamp),
}

#[cold]
fn out_of_range(what: &str) -> PgError {
    PgError::error(format!("{what} out of range"))
        .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
}

/// C: `parse_datetime` (formatting.c:4217) — datetime type inferred from the
/// format's DCH_DATED/TIMED/ZONED fields. Ok(None) = soft error recorded.
pub fn parse_datetime<'mcx>(
    mcx: Mcx<'mcx>,
    date_txt: &[u8],
    fmt: &[u8],
    collid: Oid,
    strict: bool,
    typmod: &mut i32,
    tz_out: &mut i32,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<ParsedDatetime>> {
    let mut tm = zero_tm();
    let mut ftz = FmtTz::default();
    let mut fsec: fsec_t = 0;
    let mut fprec: i32 = 0;
    let mut flags: u32 = 0;

    if !do_to_timestamp(
        mcx,
        date_txt,
        fmt,
        collid,
        strict,
        &mut tm,
        &mut fsec,
        &mut ftz,
        Some(&mut fprec),
        Some(&mut flags),
        escontext.as_deref_mut(),
    )? {
        return Ok(None);
    }

    *typmod = if fprec != 0 { fprec } else { -1 };

    let flags = flags as i32;
    if flags & DCH_DATED != 0 {
        if flags & DCH_TIMED != 0 {
            if flags & DCH_ZONED != 0 {
                if ftz.has_tz {
                    *tz_out = ftz.gmtoffset;
                } else {
                    debug_assert!(!strict);
                    ::types_error::ereturn(
                        escontext.as_deref_mut(),
                        (),
                        PgError::error(
                            "missing time zone in input string for type timestamptz",
                        )
                        .with_sqlstate(::types_error::ERRCODE_INVALID_DATETIME_FORMAT),
                    )?;
                    return Ok(None);
                }
                let mut result: Timestamp = 0;
                if tm2timestamp(&tm, fsec, Some(*tz_out), &mut result).is_err() {
                    ::types_error::ereturn(
                        escontext.as_deref_mut(),
                        (),
                        out_of_range("timestamptz"),
                    )?;
                    return Ok(None);
                }
                if !AdjustTimestampForTypmod(&mut result, *typmod, escontext.as_deref_mut())? {
                    return Ok(None);
                }
                Ok(Some(ParsedDatetime::TimestampTz(result)))
            } else {
                let mut result: Timestamp = 0;
                if tm2timestamp(&tm, fsec, None, &mut result).is_err() {
                    ::types_error::ereturn(
                        escontext.as_deref_mut(),
                        (),
                        out_of_range("timestamp"),
                    )?;
                    return Ok(None);
                }
                if !AdjustTimestampForTypmod(&mut result, *typmod, escontext.as_deref_mut())? {
                    return Ok(None);
                }
                Ok(Some(ParsedDatetime::Timestamp(result)))
            }
        } else if flags & DCH_ZONED != 0 {
            ::types_error::ereturn(
                escontext.as_deref_mut(),
                (),
                PgError::error("datetime format is zoned but not timed")
                    .with_sqlstate(::types_error::ERRCODE_INVALID_DATETIME_FORMAT),
            )?;
            Ok(None)
        } else {
            if !IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday) {
                ::types_error::ereturn(
                    escontext.as_deref_mut(),
                    (),
                    *date_out_of_range(date_txt),
                )?;
                return Ok(None);
            }
            let result = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;
            if !IS_VALID_DATE(result) {
                ::types_error::ereturn(
                    escontext.as_deref_mut(),
                    (),
                    *date_out_of_range(date_txt),
                )?;
                return Ok(None);
            }
            Ok(Some(ParsedDatetime::Date(result)))
        }
    } else if flags & DCH_TIMED != 0 {
        if flags & DCH_ZONED != 0 {
            if ftz.has_tz {
                *tz_out = ftz.gmtoffset;
            } else {
                debug_assert!(!strict);
                ::types_error::ereturn(
                    escontext.as_deref_mut(),
                    (),
                    PgError::error("missing time zone in input string for type timetz")
                        .with_sqlstate(::types_error::ERRCODE_INVALID_DATETIME_FORMAT),
                )?;
                return Ok(None);
            }
            let mut result = ::adt_date::TimeTzADT::default();
            ::adt_date::tm2timetz(&tm, fsec, *tz_out, &mut result);
            ::adt_date::AdjustTimeForTypmod(&mut result.time, *typmod);
            Ok(Some(ParsedDatetime::TimeTz(result)))
        } else {
            let mut result = ::adt_date::tm2time(&tm, fsec);
            ::adt_date::AdjustTimeForTypmod(&mut result, *typmod);
            Ok(Some(ParsedDatetime::Time(result)))
        }
    } else {
        ::types_error::ereturn(
            escontext.as_deref_mut(),
            (),
            PgError::error("datetime format is not dated and not timed")
                .with_sqlstate(::types_error::ERRCODE_INVALID_DATETIME_FORMAT),
        )?;
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::mcx::MemoryContext;

    fn ts_1997() -> i64 {
        let mut tm = pg_tm::default();
        tm.tm_year = 1997;
        tm.tm_mon = 12;
        tm.tm_mday = 17;
        tm.tm_hour = 7;
        tm.tm_min = 37;
        tm.tm_sec = 16;
        let mut ts: Timestamp = 0;
        tm2timestamp(&tm, 0, None, &mut ts).unwrap();
        ts
    }

    fn to_char_str(fmt: &[u8]) -> String {
        let ctx = MemoryContext::new("dch-test");
        let v = timestamp_to_char(ctx.mcx(), ::types_core::InvalidOid, ts_1997(), fmt).unwrap();
        String::from_utf8_lossy(v.data()).into_owned()
    }

    #[test]
    fn to_char_ymd_hms() {
        assert_eq!(to_char_str(b"YYYY-MM-DD HH24:MI:SS"), "1997-12-17 07:37:16");
    }

    #[test]
    fn to_char_fm_month() {
        assert_eq!(to_char_str(b"FMMonth DD, YYYY"), "December 17, 1997");
    }

    #[test]
    fn to_char_dy() {
        assert_eq!(to_char_str(b"Dy"), "Wed");
    }

    #[test]
    fn to_char_ddd() {
        assert_eq!(to_char_str(b"DDD"), "351");
    }

    #[test]
    fn to_char_quarter() {
        assert_eq!(to_char_str(b"Q"), "4");
    }

    #[test]
    fn to_char_quoted_literal() {
        assert_eq!(to_char_str(b"YYYY\"y\""), "1997y");
    }

    // fnconf batch-1, OID 1768 (to_char(interval)) crash family: huge month
    // counts route ISO-week codes through date2j, whose int math C compiles
    // with -fwrapv (datetime.c date2j). C 18.3:
    //   to_char('2147483647 months'::interval, 'IYYY') → 178956970
    //   to_char('2147483647 months'::interval, 'MM')   → 07
    // Red at base: debug multiply-with-overflow panic in date2j.
    #[test]
    fn to_char_interval_huge_months_wraps_like_c() {
        let ctx = MemoryContext::new("dch-test");
        let it = ::adt_datetime::Interval { time: 0, day: 0, month: i32::MAX };
        let fmt = |f: &[u8]| {
            let v =
                interval_to_char(ctx.mcx(), ::types_core::InvalidOid, &it, f).unwrap();
            String::from_utf8_lossy(v.data()).into_owned()
        };
        assert_eq!(fmt(b"IYYY"), "178956970");
        assert_eq!(fmt(b"MM"), "07");
        assert_eq!(fmt(b"YYYY"), "178956970");
        // Wrapped-arithmetic pins (values from C's -fwrapv evaluation).
        assert_eq!(::adt_datetime::calendar::date2j(178956970, 7, 0), 939902916);
        assert_eq!(::adt_datetime::calendar::date2isoyear(178956970, 7, 0), 178956970);
    }

    fn interval_to_char_str(month: i32, day: i32, time: i64, fmt: &[u8]) -> String {
        let ctx = MemoryContext::new("dch-test");
        let it = ::adt_datetime::Interval { time, day, month };
        let v = interval_to_char(ctx.mcx(), ::types_core::InvalidOid, &it, fmt).unwrap();
        String::from_utf8_lossy(v.data()).into_owned()
    }

    // C formatting.c interval_to_char sets
    //   tm_yday = (tm_year * MONTHS_PER_YEAR + tm_mon) * DAYS_PER_MONTH + tm_mday
    // ("wday is meaningless, yday approximates the total span in days"), and
    // the DDD/WW arms print the signed value for negative intervals. Every
    // pin below is byte-compared to live C 18.3 (Homebrew twin, 2026-07-18).
    // Red at base: tm_yday was never set, so all intervals printed WW as the
    // yday=0 value "01" (fnconf campaign-2 OID 1768 residual diff).
    #[test]
    fn to_char_interval_yday_family_matches_c() {
        // the campaign-2 minimized repro interval: -1121 mons -1605 days -84436.746699 s
        assert_eq!(interval_to_char_str(-1121, -1605, -84436746699, b"WW"), "-5032");
        assert_eq!(interval_to_char_str(-1121, -1605, -84436746699, b"DDD"), "-35235");
        assert_eq!(interval_to_char_str(-1121, -1605, -84436746699, b"W"), "-228");
        // ... and its exact harness format string.
        assert_eq!(
            interval_to_char_str(-1121, -1605, -84436746699, b"wwsqFUKCcPapl6ba3"),
            "-5032s-1FUKCcPapl6ba3"
        );
        // positive span: 3 years 2 months 5 days.
        assert_eq!(interval_to_char_str(38, 5, 0, b"WW"), "164");
        assert_eq!(interval_to_char_str(38, 5, 0, b"DDD"), "1145");
        // zero interval: yday = 0 -> WW = (0-1)/7+1 = 1 (C trunc-toward-zero).
        assert_eq!(interval_to_char_str(0, 0, 0, b"WW"), "01");
        assert_eq!(interval_to_char_str(0, 0, 0, b"DDD"), "000");
        // 2147483647 months: (178956970*12 + 7) = INT_MAX, *30 wraps to -30
        // under C's -fwrapv; the wrapping_* chain must reproduce it exactly.
        assert_eq!(interval_to_char_str(i32::MAX, 0, 0, b"WW"), "-3");
        assert_eq!(interval_to_char_str(i32::MAX, 0, 0, b"DDD"), "-30");
    }

    #[test]
    fn to_char_hh12_ampm() {
        assert_eq!(to_char_str(b"HH12:MI AM"), "07:37 AM");
    }

    #[test]
    fn to_date_ymd() {
        let ctx = MemoryContext::new("dch-test");
        let d = to_date(ctx.mcx(), ::types_core::InvalidOid, b"2011-12-18", b"YYYY-MM-DD").unwrap();
        let mut y = 0;
        let mut m = 0;
        let mut day = 0;
        j2date(d + POSTGRES_EPOCH_JDATE, &mut y, &mut m, &mut day);
        assert_eq!((y, m, day), (2011, 12, 18));
    }

    // Expected tuples are live C PostgreSQL 18.3 output (2026-07-17);
    // internal year 0 prints as 0001 BC. Pins the t26-soak to_date
    // statement class (all-literal format => 1 BC) and its era /
    // zero-year / century neighbors.
    #[test]
    fn to_date_era_zero_year_pins_match_c() {
        let ctx = MemoryContext::new("dch-test");
        let ymd = |input: &[u8], fmt: &[u8]| {
            let d = to_date(ctx.mcx(), ::types_core::InvalidOid, input, fmt).unwrap();
            let (mut y, mut m, mut day) = (0, 0, 0);
            j2date(d + POSTGRES_EPOCH_JDATE, &mut y, &mut m, &mut day);
            (y, m, day)
        };
        assert_eq!(ymd(b"16.0", b"16.0"), (0, 1, 1));
        assert_eq!(ymd(b"", b""), (0, 1, 1));
        assert_eq!(ymd(b"0000-06-13", b"YYYY-MM-DD"), (0, 6, 13));
        assert_eq!(ymd(b"0001-06-13 BC", b"YYYY-MM-DD BC"), (0, 6, 13));
        assert_eq!(ymd(b"0001-06-13 AD", b"YYYY-MM-DD AD"), (1, 6, 13));
        assert_eq!(ymd(b"1 BC", b"Y BC"), (-2000, 1, 1));
        assert_eq!(ymd(b"0002 b.c.", b"YYYY B.C."), (-1, 1, 1));
        assert_eq!(ymd(b"-1234", b"YYYY"), (-1233, 1, 1));
        assert_eq!(ymd(b"20 00 BC", b"CC YY BC"), (-1999, 1, 1));
        assert_eq!(ymd(b"21 BC", b"CC BC"), (-2099, 1, 1));
        assert_eq!(ymd(b"4714-11-24 BC", b"YYYY-MM-DD BC"), (-4713, 11, 24));
        let err = to_date(ctx.mcx(), ::types_core::InvalidOid, b"4714-11-23 BC", b"YYYY-MM-DD BC")
            .unwrap_err();
        assert_eq!(err.message, "date out of range: \"4714-11-23 BC\"");
    }
}
