use crate::*;
use types_error::PgError;

// sizeof(union input_buffer): 2 * sizeof(struct tzhead)(44) + 2 * sizeof(struct
// state)(23440 on LP64) + 4 * TZ_MAX_TIMES. The single-read cap is part of the
// accept/reject behavior: a larger file is truncated, then fails length checks.
pub(crate) const INPUT_BUF_SIZE: usize = 2 * 44 + 2 * 23440 + 4 * TZ_MAX_TIMES;

const TZDEFRULESTRING: &[u8] = b"M3.2.0,M11.1.0";

/// C's errno result: ENOENT / EINVAL-and-short-read / escaped ereport.
#[derive(Debug)]
pub enum TzLoadError {
    NotFound,
    Invalid,
    Report(Box<PgError>),
}

pub fn tzload(
    name: &[u8],
    canonname: Option<&mut [u8; TZ_STRLEN_MAX + 1]>,
    sp: &mut TzState,
    doextend: bool,
) -> Result<(), TzLoadError> {
    let name = if name.first() == Some(&b':') {
        &name[1..]
    } else {
        name
    };
    // C mallocs local_storage per call; the read buffer here likewise.
    let mut buf = vec![0u8; INPUT_BUF_SIZE];
    let nread = match pgtz_seams::pg_open_tzfile::call(name, canonname, &mut buf) {
        Ok(Some(n)) => n,
        Ok(None) => return Err(TzLoadError::NotFound),
        Err(e) => return Err(TzLoadError::Report(e)),
    };
    if nread < 44 {
        return Err(TzLoadError::Invalid);
    }
    parse_tzif(&buf[..nread], sp, doextend)
}

fn parse_tzif(bytes: &[u8], sp: &mut TzState, doextend: bool) -> Result<(), TzLoadError> {
    // The 64-bit block re-parses over the 32-bit one; no magic check, as in C.
    let first = TzifHeader::parse_at(bytes, 0).ok_or(TzLoadError::Invalid)?;
    let mut cursor = first.data_start;
    parse_tzif_block(bytes, &first, 4, &mut cursor, sp)?;

    // C breaks on a '\0' version byte; the footer check runs from that point.
    let footer_start = if first.version == 0 {
        0
    } else {
        let second_start = cursor;
        let second = TzifHeader::parse_at(bytes, cursor).ok_or(TzLoadError::Invalid)?;
        cursor = second.data_start;
        parse_tzif_block(bytes, &second, 8, &mut cursor, sp)?;
        if second.version == 0 {
            second_start
        } else {
            cursor
        }
    };

    if doextend && sp.typecnt as usize + 2 <= TZ_MAX_TYPES {
        if let Some(posix) = parse_footer_posix(bytes, footer_start) {
            let mut ts = Box::new(TzState::new());
            if tzparse(posix, &mut ts, false) {
                extend_with_posix(sp, &mut ts);
            }
        }
    }

    if sp.typecnt == 0 {
        return Err(TzLoadError::Invalid);
    }

    if sp.timecnt > 1 {
        let timecnt = sp.timecnt as usize;
        for i in 1..timecnt {
            if typesequiv(sp, sp.types[i] as i32, sp.types[0] as i32)
                && differ_by_repeat(sp.ats[i], sp.ats[0])
            {
                sp.goback = true;
                break;
            }
        }
        for i in (0..=timecnt - 2).rev() {
            if typesequiv(sp, sp.types[timecnt - 1] as i32, sp.types[i] as i32)
                && differ_by_repeat(sp.ats[timecnt - 1], sp.ats[i])
            {
                sp.goahead = true;
                break;
            }
        }
    }

    set_default_type(sp);
    Ok(())
}

fn typesequiv(sp: &TzState, a: i32, b: i32) -> bool {
    if a < 0 || a >= sp.typecnt || b < 0 || b >= sp.typecnt {
        return false;
    }
    let ap = &sp.ttis[a as usize];
    let bp = &sp.ttis[b as usize];
    ap.tt_utoff == bp.tt_utoff
        && ap.tt_isdst == bp.tt_isdst
        && ap.tt_ttisstd == bp.tt_ttisstd
        && ap.tt_ttisut == bp.tt_ttisut
        && cstr_bytes(&sp.chars, ap.tt_desigidx as usize)
            == cstr_bytes(&sp.chars, bp.tt_desigidx as usize)
}

fn differ_by_repeat(t1: pg_time_t, t0: pg_time_t) -> bool {
    // TYPE_BIT - TYPE_SIGNED == 63 >= SECSPERREPEAT_BITS(34): no early return.
    if (pg_time_t::BITS - 1) < SECSPERREPEAT_BITS {
        return false;
    }
    t1.checked_sub(t0) == Some(SECSPERREPEAT)
}

#[derive(Clone, Copy)]
struct TzifHeader {
    version: u8,
    ttisutcnt: usize,
    ttisstdcnt: usize,
    leapcnt: usize,
    timecnt: usize,
    typecnt: usize,
    charcnt: usize,
    data_start: usize,
}

impl TzifHeader {
    // Negative int32 counts become huge usizes and fail the upper bounds,
    // matching C's `0 <= cnt` tests.
    fn parse_at(bytes: &[u8], offset: usize) -> Option<Self> {
        let header = bytes.get(offset..offset.checked_add(44)?)?;
        Some(Self {
            version: header[4],
            ttisutcnt: read_be_i32(&header[20..24])? as usize,
            ttisstdcnt: read_be_i32(&header[24..28])? as usize,
            leapcnt: read_be_i32(&header[28..32])? as usize,
            timecnt: read_be_i32(&header[32..36])? as usize,
            typecnt: read_be_i32(&header[36..40])? as usize,
            charcnt: read_be_i32(&header[40..44])? as usize,
            data_start: offset + 44,
        })
    }
}

// TIME_T range checks are vacuous for full-i64 pg_time_t; kept for fidelity.
#[allow(clippy::absurd_extreme_comparisons)]
fn parse_tzif_block(
    bytes: &[u8],
    header: &TzifHeader,
    time_size: usize,
    cursor: &mut usize,
    sp: &mut TzState,
) -> Result<(), TzLoadError> {
    if !(header.leapcnt < TZ_MAX_LEAPS
        && header.typecnt < TZ_MAX_TYPES
        && header.timecnt < TZ_MAX_TIMES
        && header.charcnt < TZ_MAX_CHARS
        && (header.ttisstdcnt == header.typecnt || header.ttisstdcnt == 0)
        && (header.ttisutcnt == header.typecnt || header.ttisutcnt == 0))
    {
        return Err(TzLoadError::Invalid);
    }

    *sp = TzState::new();
    sp.leapcnt = header.leapcnt as i32;
    sp.timecnt = header.timecnt as i32;
    sp.typecnt = header.typecnt as i32;
    sp.charcnt = header.charcnt as i32;

    // Out-of-range transitions are discarded, duplicates drop the earlier
    // entry, decreasing times reject; `keep[i]` is C's transient types[] flag.
    let mut keep = [false; TZ_MAX_TIMES];
    let mut timecnt = 0usize;
    for i in 0..header.timecnt {
        let raw = take(bytes, cursor, time_size)?;
        let at: i64 = if time_size == 8 {
            read_be_i64(raw).ok_or(TzLoadError::Invalid)?
        } else {
            read_be_i32(raw).ok_or(TzLoadError::Invalid)? as i64
        };
        keep[i] = at <= TIME_T_MAX;
        if keep[i] {
            let attime = if at < TIME_T_MIN { TIME_T_MIN } else { at };
            if timecnt != 0 && attime <= sp.ats[timecnt - 1] {
                if attime < sp.ats[timecnt - 1] {
                    return Err(TzLoadError::Invalid);
                }
                keep[i - 1] = false;
                timecnt -= 1;
            }
            sp.ats[timecnt] = attime;
            timecnt += 1;
        }
    }

    let types = take(bytes, cursor, header.timecnt)?;
    timecnt = 0;
    for (i, typ) in types.iter().copied().enumerate() {
        if typ as usize >= header.typecnt {
            return Err(TzLoadError::Invalid);
        }
        if keep[i] {
            sp.types[timecnt] = typ;
            timecnt += 1;
        }
    }
    sp.timecnt = timecnt as i32;

    for i in 0..header.typecnt {
        let raw = take(bytes, cursor, 6)?;
        let tt_utoff = read_be_i32(&raw[0..4]).ok_or(TzLoadError::Invalid)?;
        let isdst = raw[4];
        if isdst >= 2 {
            return Err(TzLoadError::Invalid);
        }
        let desigidx = raw[5];
        if desigidx as usize >= header.charcnt {
            return Err(TzLoadError::Invalid);
        }
        sp.ttis[i] = TtInfo {
            tt_utoff,
            tt_isdst: isdst != 0,
            tt_desigidx: desigidx as i32,
            tt_ttisstd: false,
            tt_ttisut: false,
        };
    }

    let chars = take(bytes, cursor, header.charcnt)?;
    sp.chars[..header.charcnt].copy_from_slice(chars);
    sp.chars[header.charcnt] = 0;

    // Leap seconds: none before the Epoch, >= 28 days - 1s apart, corrections
    // step by exactly 1.
    let mut prevtr: i64 = 0;
    let mut prevcorr: i64 = 0;
    let mut leapcnt = 0usize;
    for _ in 0..header.leapcnt {
        let tr: i64 = if time_size == 8 {
            read_be_i64(take(bytes, cursor, 8)?).ok_or(TzLoadError::Invalid)?
        } else {
            read_be_i32(take(bytes, cursor, 4)?).ok_or(TzLoadError::Invalid)? as i64
        };
        let corr = read_be_i32(take(bytes, cursor, 4)?).ok_or(TzLoadError::Invalid)? as i64;
        if tr < 0 {
            return Err(TzLoadError::Invalid);
        }
        if tr <= TIME_T_MAX {
            if tr - prevtr < 28 * SECSPERDAY - 1 || (corr != prevcorr - 1 && corr != prevcorr + 1)
            {
                return Err(TzLoadError::Invalid);
            }
            prevtr = tr;
            prevcorr = corr;
            sp.lsis[leapcnt] = LsInfo {
                ls_trans: tr,
                ls_corr: corr,
            };
            leapcnt += 1;
        }
    }
    sp.leapcnt = leapcnt as i32;

    for i in 0..header.typecnt {
        if header.ttisstdcnt == 0 {
            sp.ttis[i].tt_ttisstd = false;
        } else {
            let byte = take(bytes, cursor, 1)?[0];
            if byte > 1 {
                return Err(TzLoadError::Invalid);
            }
            sp.ttis[i].tt_ttisstd = byte != 0;
        }
    }
    for i in 0..header.typecnt {
        if header.ttisutcnt == 0 {
            sp.ttis[i].tt_ttisut = false;
        } else {
            let byte = take(bytes, cursor, 1)?[0];
            if byte > 1 {
                return Err(TzLoadError::Invalid);
            }
            sp.ttis[i].tt_ttisut = byte != 0;
        }
    }

    Ok(())
}

// Footer: more than two bytes, '\n' first and last; C passes &buf[1] as a C
// string, so the TZ string runs to the first NUL if any.
fn parse_footer_posix(bytes: &[u8], start: usize) -> Option<&[u8]> {
    let footer = bytes.get(start..)?;
    if footer.len() <= 2 || footer[0] != b'\n' || footer[footer.len() - 1] != b'\n' {
        return None;
    }
    let tz = &footer[1..footer.len() - 1];
    Some(match tz.iter().position(|&b| b == 0) {
        Some(nul) => &tz[..nul],
        None => tz,
    })
}

// The doextend graft: no change unless every ts type matched an abbreviation
// slot (C's gotabbr == ts->typecnt gate).
fn extend_with_posix(sp: &mut TzState, ts: &mut TzState) {
    let mut gotabbr = 0usize;
    let mut charcnt = sp.charcnt as usize;
    for i in 0..ts.typecnt as usize {
        let tsabbr_start = ts.ttis[i].tt_desigidx as usize;
        // C's reuse scan steps j by 1 byte, so a suffix of an existing
        // abbreviation ("KST" inside "AKST\0") also matches.
        let mut matched = None;
        let mut j = 0usize;
        while j < charcnt {
            if cstr_bytes(&sp.chars, j) == cstr_bytes(&ts.chars, tsabbr_start) {
                matched = Some(j);
                break;
            }
            j += 1;
        }
        if let Some(j) = matched {
            ts.ttis[i].tt_desigidx = j as i32;
            gotabbr += 1;
        } else {
            let tsabbrlen = cstr_bytes(&ts.chars, tsabbr_start).len();
            if j + tsabbrlen < TZ_MAX_CHARS {
                sp.chars[j..j + tsabbrlen]
                    .copy_from_slice(&ts.chars[tsabbr_start..tsabbr_start + tsabbrlen]);
                sp.chars[j + tsabbrlen] = 0;
                charcnt = j + tsabbrlen + 1;
                ts.ttis[i].tt_desigidx = j as i32;
                gotabbr += 1;
            }
        }
    }

    if gotabbr != ts.typecnt as usize {
        return;
    }
    sp.charcnt = charcnt as i32;

    // Drop trailing no-op transitions generated by zic 2016j or earlier.
    while sp.timecnt > 1 && sp.types[sp.timecnt as usize - 1] == sp.types[sp.timecnt as usize - 2]
    {
        sp.timecnt -= 1;
    }

    let mut i = 0usize;
    while i < ts.timecnt as usize {
        let corrected = ts.ats[i] + leapcorr(sp, ts.ats[i]);
        if sp.timecnt == 0 || sp.ats[sp.timecnt as usize - 1] < corrected {
            break;
        }
        i += 1;
    }
    while i < ts.timecnt as usize && (sp.timecnt as usize) < TZ_MAX_TIMES {
        let idx = sp.timecnt as usize;
        sp.ats[idx] = ts.ats[i] + leapcorr(sp, ts.ats[i]);
        sp.types[idx] = sp.typecnt as u8 + ts.types[i];
        sp.timecnt += 1;
        i += 1;
    }
    for i in 0..ts.typecnt as usize {
        sp.ttis[sp.typecnt as usize] = ts.ttis[i];
        sp.typecnt += 1;
    }
}

pub(crate) fn leapcorr(sp: &TzState, t: pg_time_t) -> i64 {
    for i in (0..sp.leapcnt as usize).rev() {
        if t >= sp.lsis[i].ls_trans {
            return sp.lsis[i].ls_corr;
        }
    }
    0
}

// defaulttype heuristics for tzdb 2018e-or-earlier data.
fn set_default_type(sp: &mut TzState) {
    let timecnt = sp.timecnt as usize;
    let typecnt = sp.typecnt;

    let mut i: i32 = if sp.types[..timecnt].contains(&0) {
        -1
    } else {
        0
    };

    if i < 0 && timecnt > 0 && sp.ttis[sp.types[0] as usize].tt_isdst {
        i = sp.types[0] as i32;
        loop {
            i -= 1;
            if i < 0 || !sp.ttis[i as usize].tt_isdst {
                break;
            }
        }
    }

    if i < 0 {
        i = 0;
        while sp.ttis[i as usize].tt_isdst {
            i += 1;
            if i >= typecnt {
                i = 0;
                break;
            }
        }
    }

    sp.defaulttype = i;
}

pub fn tzparse(name: &[u8], sp: &mut TzState, lastditch: bool) -> bool {
    let (stdname, stdoffset, rest): (&[u8], i32, &[u8]) = if lastditch {
        // Unlike IANA, don't assume name is exactly "GMT".
        (name, 0, b"")
    } else {
        let Some((stdname, rest)) = parse_zone_name(name) else {
            return false;
        };
        // Empty STD abbrev allowed (unlike IANA); a missing offset is not.
        if rest.is_empty() {
            return false;
        }
        let Some((stdoffset, rest)) = getoffset(rest) else {
            return false;
        };
        (stdname, stdoffset, rest)
    };

    let parsed_dst = if rest.is_empty() {
        None
    } else {
        let Some((dstname, rest)) = parse_zone_name(rest) else {
            return false;
        };
        if dstname.is_empty() {
            return false;
        }
        let (dstoffset, rest) = if !rest.is_empty() && rest[0] != b',' && rest[0] != b';' {
            let Some((dstoffset, rest)) = getoffset(rest) else {
                return false;
            };
            (dstoffset, rest)
        } else {
            (stdoffset - SECSPERHOUR as i32, rest)
        };
        // PG desupports TZDEFRULES (load_ok always false): a DST name with no
        // rule gets the default US rules.
        let rules: &[u8] = if rest.is_empty() {
            TZDEFRULESTRING
        } else if rest[0] == b',' || rest[0] == b';' {
            &rest[1..]
        } else {
            return false;
        };
        let Some((start, rules)) = getrule(rules) else {
            return false;
        };
        if rules.first() != Some(&b',') {
            return false;
        }
        let Some((end, rules)) = getrule(&rules[1..]) else {
            return false;
        };
        if !rules.is_empty() {
            return false;
        }
        Some((dstname, dstoffset, start, end))
    };

    let charcnt = stdname.len() + 1 + parsed_dst.map(|d| d.0.len() + 1).unwrap_or(0);
    if charcnt > CHARS_SIZE {
        return false;
    }

    *sp = TzState::new();
    sp.typecnt = if parsed_dst.is_some() { 2 } else { 1 };
    sp.charcnt = charcnt as i32;
    sp.defaulttype = 0;
    sp.ttis[0] = TtInfo {
        tt_utoff: -stdoffset,
        tt_isdst: false,
        tt_desigidx: 0,
        tt_ttisstd: false,
        tt_ttisut: false,
    };
    write_chars_at(&mut sp.chars, 0, stdname);
    if let Some((dstname, dstoffset, start, end)) = parsed_dst {
        let dst_index = stdname.len() + 1;
        sp.ttis[1] = TtInfo {
            tt_utoff: -dstoffset,
            tt_isdst: true,
            tt_desigidx: dst_index as i32,
            tt_ttisstd: false,
            tt_ttisut: false,
        };
        write_chars_at(&mut sp.chars, dst_index, dstname);
        build_posix_transitions(sp, stdoffset, dstoffset, start, end);
    }
    true
}

// A <>-quoted name needs its '>'; unquoted ends at a digit, ',', '-', '+'.
fn parse_zone_name(input: &[u8]) -> Option<(&[u8], &[u8])> {
    if input.first() == Some(&b'<') {
        let rest = &input[1..];
        let end = rest.iter().position(|&b| b == b'>')?;
        Some((&rest[..end], &rest[end + 1..]))
    } else {
        let end = input
            .iter()
            .position(|&b| b == b'+' || b == b'-' || b == b',' || b.is_ascii_digit())
            .unwrap_or(input.len());
        Some((&input[..end], &input[end..]))
    }
}

// Running value rejected the moment it exceeds max; no digit-count limit.
fn getnum(input: &[u8], min: i32, max: i32) -> Option<(i32, &[u8])> {
    if !input.first()?.is_ascii_digit() {
        return None;
    }
    let mut num: i32 = 0;
    let mut idx = 0usize;
    while let Some(&c) = input.get(idx).filter(|c| c.is_ascii_digit()) {
        num = num.checked_mul(10)?.checked_add((c - b'0') as i32)?;
        if num > max {
            return None;
        }
        idx += 1;
    }
    if num < min {
        return None;
    }
    Some((num, &input[idx..]))
}

// hh[:mm[:ss]]: hours 0..=167 (quasi-POSIX "M10.4.6/26"), seconds allow 60.
fn getsecs(input: &[u8]) -> Option<(i32, &[u8])> {
    let (num, rest) = getnum(input, 0, (HOURSPERDAY * DAYSPERWEEK as i64 - 1) as i32)?;
    let mut secs = num.checked_mul(SECSPERHOUR as i32)?;
    let mut rest = rest;
    if rest.first() == Some(&b':') {
        let (num, r) = getnum(&rest[1..], 0, (MINSPERHOUR - 1) as i32)?;
        secs = secs.checked_add(num * SECSPERMIN as i32)?;
        rest = r;
        if rest.first() == Some(&b':') {
            let (num, r) = getnum(&rest[1..], 0, SECSPERMIN as i32)?;
            secs = secs.checked_add(num)?;
            rest = r;
        }
    }
    Some((secs, rest))
}

fn getoffset(input: &[u8]) -> Option<(i32, &[u8])> {
    let (neg, rest) = match input.first() {
        Some(&b'-') => (true, &input[1..]),
        Some(&b'+') => (false, &input[1..]),
        _ => (false, input),
    };
    let (secs, rest) = getsecs(rest)?;
    Some((if neg { secs.checked_neg()? } else { secs }, rest))
}

#[derive(Clone, Copy)]
pub(crate) enum Rule {
    JulianNoLeap { day: i32 },
    ZeroBasedJulian { day: i32 },
    MonthWeekDay { month: i32, week: i32, day: i32 },
}

#[derive(Clone, Copy)]
pub(crate) struct TransitionRule {
    rule: Rule,
    time: i32,
}

fn getrule(input: &[u8]) -> Option<(TransitionRule, &[u8])> {
    let (rule, rest) = if input.first() == Some(&b'J') {
        let (day, rest) = getnum(&input[1..], 1, DAYSPERNYEAR)?;
        (Rule::JulianNoLeap { day }, rest)
    } else if input.first() == Some(&b'M') {
        let (month, rest) = getnum(&input[1..], 1, MONSPERYEAR as i32)?;
        if rest.first() != Some(&b'.') {
            return None;
        }
        let (week, rest) = getnum(&rest[1..], 1, 5)?;
        if rest.first() != Some(&b'.') {
            return None;
        }
        let (day, rest) = getnum(&rest[1..], 0, DAYSPERWEEK - 1)?;
        (Rule::MonthWeekDay { month, week, day }, rest)
    } else if input.first().is_some_and(u8::is_ascii_digit) {
        let (day, rest) = getnum(input, 0, DAYSPERLYEAR - 1)?;
        (Rule::ZeroBasedJulian { day }, rest)
    } else {
        return None;
    };

    let (time, rest) = if rest.first() == Some(&b'/') {
        getoffset(&rest[1..])?
    } else {
        (2 * SECSPERHOUR as i32, rest)
    };
    Some((TransitionRule { rule, time }, rest))
}

// Two transitions per year from a bounded look-back window before EPOCH_YEAR;
// an empty table means perpetual DST and collapses to one type.
fn build_posix_transitions(
    sp: &mut TzState,
    stdoffset: i32,
    dstoffset: i32,
    start: TransitionRule,
    end: TransitionRule,
) {
    let mut timecnt = 0usize;
    let mut janfirst: pg_time_t = 0;
    let mut janoffset: i32 = 0;
    let mut yearbeg = EPOCH_YEAR;

    loop {
        let yearsecs = year_lengths(is_leap(yearbeg - 1)) as i64 * SECSPERDAY;
        yearbeg -= 1;
        if increment_overflow_time(&mut janfirst, -yearsecs) {
            janoffset = -(yearsecs as i32);
            break;
        }
        if EPOCH_YEAR - YEARSPERREPEAT / 2 >= yearbeg {
            break;
        }
    }

    let mut yearlim = yearbeg + YEARSPERREPEAT + 1;
    let mut year = yearbeg;
    while year < yearlim {
        let mut starttime = transtime(year, start, stdoffset);
        let mut endtime = transtime(year, end, dstoffset);
        let yearsecs = year_lengths(is_leap(year)) as i64 * SECSPERDAY;
        let reversed = endtime < starttime;
        if reversed {
            core::mem::swap(&mut starttime, &mut endtime);
        }
        if reversed
            || (starttime < endtime
                && (endtime - starttime < (yearsecs + (stdoffset as i64 - dstoffset as i64))))
        {
            if TZ_MAX_TIMES - 2 < timecnt {
                break;
            }
            sp.ats[timecnt] = janfirst;
            if !increment_overflow_time(&mut sp.ats[timecnt], janoffset as i64 + starttime) {
                sp.types[timecnt] = (!reversed) as u8;
                timecnt += 1;
            }
            sp.ats[timecnt] = janfirst;
            if !increment_overflow_time(&mut sp.ats[timecnt], janoffset as i64 + endtime) {
                sp.types[timecnt] = reversed as u8;
                timecnt += 1;
                yearlim = year + YEARSPERREPEAT + 1;
            }
        }
        if increment_overflow_time(&mut janfirst, janoffset as i64 + yearsecs) {
            break;
        }
        janoffset = 0;
        year += 1;
    }

    sp.timecnt = timecnt as i32;
    if timecnt == 0 {
        sp.ttis[0] = sp.ttis[1];
        sp.typecnt = 1;
    } else if YEARSPERREPEAT < year - yearbeg {
        sp.goback = true;
        sp.goahead = true;
    }
}

// C transtime, including the Zeller's-congruence month/week/day computation.
fn transtime(year: i32, rule: TransitionRule, offset: i32) -> i64 {
    let leapyear = is_leap(year);
    let value: i64 = match rule.rule {
        Rule::JulianNoLeap { day } => {
            // Jn: 1 == Jan 1, 60 == Mar 1 even in leap years.
            let mut v = (day as i64 - 1) * SECSPERDAY;
            if leapyear && day >= 60 {
                v += SECSPERDAY;
            }
            v
        }
        Rule::ZeroBasedJulian { day } => day as i64 * SECSPERDAY,
        Rule::MonthWeekDay { month, week, day } => {
            let m1 = (month + 9) % 12 + 1;
            let yy0 = if month <= 2 { year - 1 } else { year };
            let yy1 = yy0 / 100;
            let yy2 = yy0 % 100;
            let mut dow = ((26 * m1 - 2) / 10 + 1 + yy2 + yy2 / 4 + yy1 / 4 - 2 * yy1) % 7;
            if dow < 0 {
                dow += DAYSPERWEEK;
            }
            let mut d = day - dow;
            if d < 0 {
                d += DAYSPERWEEK;
            }
            for _ in 1..week {
                if d + DAYSPERWEEK >= mon_lengths(leapyear, month as usize - 1) {
                    break;
                }
                d += DAYSPERWEEK;
            }
            let mut v = d as i64 * SECSPERDAY;
            for i in 0..month as usize - 1 {
                v += mon_lengths(leapyear, i) as i64 * SECSPERDAY;
            }
            v
        }
    };
    value + rule.time as i64 + offset as i64
}

fn write_chars_at(dst: &mut [u8], offset: usize, value: &[u8]) {
    dst[offset..offset + value.len()].copy_from_slice(value);
    dst[offset + value.len()] = 0;
}

fn take<'a>(bytes: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8], TzLoadError> {
    let end = cursor.checked_add(len).ok_or(TzLoadError::Invalid)?;
    let slice = bytes.get(*cursor..end).ok_or(TzLoadError::Invalid)?;
    *cursor = end;
    Ok(slice)
}

fn read_be_i32(bytes: &[u8]) -> Option<i32> {
    Some(i32::from_be_bytes(bytes.try_into().ok()?))
}

fn read_be_i64(bytes: &[u8]) -> Option<i64> {
    Some(i64::from_be_bytes(bytes.try_into().ok()?))
}
