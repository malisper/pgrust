use crate::backend::utils::misc::guc_datetime::{DateOrder, DateTimeConfig};
use crate::include::nodes::datetime::{
    POSTGRES_EPOCH_JDATE, SECS_PER_DAY, USECS_PER_DAY, USECS_PER_HOUR, USECS_PER_MINUTE,
    USECS_PER_SEC,
};
use std::path::Path;

use crate::backend::utils::time::system_time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateTimeKeyword {
    Epoch,
    Now,
    Today,
    Tomorrow,
    Yesterday,
    Infinity,
    NegInfinity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DateTimeParseError {
    Invalid,
    FieldOutOfRange,
    UnknownTimeZone(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeZoneSpec {
    FixedOffset(i32),
    Named(String),
}

pub fn parse_keyword(text: &str) -> Option<DateTimeKeyword> {
    match text.trim().to_ascii_lowercase().as_str() {
        "epoch" => Some(DateTimeKeyword::Epoch),
        "now" => Some(DateTimeKeyword::Now),
        "today" => Some(DateTimeKeyword::Today),
        "tomorrow" => Some(DateTimeKeyword::Tomorrow),
        "yesterday" => Some(DateTimeKeyword::Yesterday),
        "infinity" | "+infinity" => Some(DateTimeKeyword::Infinity),
        "-infinity" => Some(DateTimeKeyword::NegInfinity),
        _ => None,
    }
}

pub fn month_number(token: &str) -> Option<u32> {
    match token.trim().to_ascii_lowercase().as_str() {
        "jan" | "january" => Some(1),
        "feb" | "february" => Some(2),
        "mar" | "march" => Some(3),
        "apr" | "april" => Some(4),
        "may" => Some(5),
        "jun" | "june" => Some(6),
        "jul" | "july" => Some(7),
        "aug" | "august" => Some(8),
        "sep" | "sept" | "september" => Some(9),
        "oct" | "october" => Some(10),
        "nov" | "november" => Some(11),
        "dec" | "december" => Some(12),
        _ => None,
    }
}

pub fn is_weekday_token(token: &str) -> bool {
    matches!(
        token.trim().to_ascii_lowercase().as_str(),
        "mon"
            | "monday"
            | "tue"
            | "tues"
            | "tuesday"
            | "wed"
            | "wednesday"
            | "thu"
            | "thur"
            | "thurs"
            | "thursday"
            | "fri"
            | "friday"
            | "sat"
            | "saturday"
            | "sun"
            | "sunday"
    )
}

pub fn is_bc_token(token: &str) -> bool {
    matches!(token.trim().to_ascii_lowercase().as_str(), "bc" | "b.c.")
}

pub fn expand_two_digit_year(year: i32) -> i32 {
    if (0..=69).contains(&year) {
        year + 2000
    } else if (70..=99).contains(&year) {
        year + 1900
    } else {
        year
    }
}

pub fn normalize_timezone_name(name: &str) -> String {
    name.trim().to_string()
}

pub fn current_timezone_name(config: &DateTimeConfig) -> &str {
    config.time_zone.as_str()
}

const UNIX_EPOCH_TO_POSTGRES_EPOCH_DAYS: i64 = 10_957;

pub fn current_postgres_timestamp_usecs() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => {
            let unix_usecs =
                duration.as_secs() as i64 * USECS_PER_SEC + duration.subsec_micros() as i64;
            unix_usecs - UNIX_EPOCH_TO_POSTGRES_EPOCH_DAYS * USECS_PER_DAY
        }
        Err(err) => {
            let duration = err.duration();
            let unix_usecs =
                duration.as_secs() as i64 * USECS_PER_SEC + duration.subsec_micros() as i64;
            -unix_usecs - UNIX_EPOCH_TO_POSTGRES_EPOCH_DAYS * USECS_PER_DAY
        }
    }
}

pub fn postgres_date_from_unix_days(unix_days: i64) -> i32 {
    (unix_days - UNIX_EPOCH_TO_POSTGRES_EPOCH_DAYS) as i32
}

pub fn unix_days_from_postgres_date(pg_days: i32) -> i64 {
    pg_days as i64 + UNIX_EPOCH_TO_POSTGRES_EPOCH_DAYS
}

pub fn julian_day_from_postgres_date(pg_days: i32) -> i32 {
    pg_days + POSTGRES_EPOCH_JDATE
}

pub fn postgres_date_from_julian_day(julian_day: i32) -> i32 {
    julian_day - POSTGRES_EPOCH_JDATE
}

pub fn days_from_ymd(year: i32, month: u32, day: u32) -> Option<i32> {
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    let year_adj = year - i32::from(month <= 2);
    let era = if year_adj >= 0 {
        year_adj
    } else {
        year_adj - 399
    } / 400;
    let yoe = year_adj - era * 400;
    let month = month as i32;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day as i32 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let unix_days = era as i64 * 146_097 + doe as i64 - 719_468;
    Some(postgres_date_from_unix_days(unix_days))
}

pub fn ymd_from_days(pg_days: i32) -> (i32, u32, u32) {
    let unix_days = unix_days_from_postgres_date(pg_days);
    let z = unix_days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe as i32 + era as i32 * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = year + i32::from(month <= 2);
    (year, month as u32, day as u32)
}

pub fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

pub fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

pub fn day_of_year(year: i32, month: u32, day: u32) -> u32 {
    let mut total = 0;
    for current_month in 1..month {
        total += days_in_month(year, current_month);
    }
    total + day
}

pub fn day_of_week_from_julian_day(julian_day: i32) -> u32 {
    julian_day.saturating_add(1).rem_euclid(7) as u32
}

pub fn iso_day_of_week_from_julian_day(julian_day: i32) -> u32 {
    match day_of_week_from_julian_day(julian_day) {
        0 => 7,
        other => other,
    }
}

pub fn iso_weeks_in_year(year: i32) -> u32 {
    let jan1 = days_from_ymd(year, 1, 1).expect("january 1 must be valid");
    let jan1_isodow = iso_day_of_week_from_julian_day(julian_day_from_postgres_date(jan1));
    if jan1_isodow == 4 || (jan1_isodow == 3 && is_leap_year(year)) {
        53
    } else {
        52
    }
}

pub fn iso_week_and_year(year: i32, month: u32, day: u32) -> (i32, u32) {
    let pg_days = days_from_ymd(year, month, day).expect("validated date components required");
    let julian_day = julian_day_from_postgres_date(pg_days);
    let ordinal = day_of_year(year, month, day) as i32;
    let iso_dow = iso_day_of_week_from_julian_day(julian_day) as i32;
    let mut week = (ordinal - iso_dow + 10).div_euclid(7);
    let mut iso_year = year;
    if week < 1 {
        iso_year -= 1;
        week = iso_weeks_in_year(iso_year) as i32;
    } else {
        let max_week = iso_weeks_in_year(year) as i32;
        if week > max_week {
            iso_year += 1;
            week = 1;
        }
    }
    (iso_year, week as u32)
}

pub fn parse_date_parts(text: &str) -> Option<(i32, u32, u32)> {
    let parts = text.trim().split('-').collect::<Vec<_>>();
    if parts.len() != 3 {
        return None;
    }
    let year = parts[0].parse::<i32>().ok()?;
    let month = parts[1].parse::<u32>().ok()?;
    let day = parts[2].parse::<u32>().ok()?;
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    Some((year, month, day))
}

fn validate_date(year: i32, month: u32, day: u32) -> Result<(i32, u32, u32), DateTimeParseError> {
    if days_from_ymd(year, month, day).is_some() {
        Ok((year, month, day))
    } else {
        Err(DateTimeParseError::FieldOutOfRange)
    }
}

fn parse_year_number(text: &str, allow_two_digits: bool) -> Option<i32> {
    let year = text.parse::<i32>().ok()?;
    Some(if allow_two_digits && text.len() <= 2 {
        expand_two_digit_year(year)
    } else {
        year
    })
}

fn parse_numeric_triplet(
    first: &str,
    second: &str,
    third: &str,
    config: &DateTimeConfig,
) -> Result<(i32, u32, u32), DateTimeParseError> {
    let a = first
        .parse::<i32>()
        .map_err(|_| DateTimeParseError::Invalid)?;
    let b = second
        .parse::<u32>()
        .map_err(|_| DateTimeParseError::Invalid)?;
    let c = third
        .parse::<i32>()
        .map_err(|_| DateTimeParseError::Invalid)?;

    if first.len() >= 3 {
        return validate_date(
            parse_year_number(first, true).unwrap_or(a),
            b,
            third
                .parse::<u32>()
                .map_err(|_| DateTimeParseError::Invalid)?,
        );
    }
    if third.len() >= 4 || c > 31 {
        if matches!(config.date_order, DateOrder::Ymd) {
            return Err(DateTimeParseError::FieldOutOfRange);
        }
        let year = parse_year_number(third, true).ok_or(DateTimeParseError::Invalid)?;
        return match config.date_order {
            DateOrder::Mdy => validate_date(
                year,
                first
                    .parse::<u32>()
                    .map_err(|_| DateTimeParseError::Invalid)?,
                second
                    .parse::<u32>()
                    .map_err(|_| DateTimeParseError::Invalid)?,
            ),
            DateOrder::Dmy => validate_date(
                year,
                second
                    .parse::<u32>()
                    .map_err(|_| DateTimeParseError::Invalid)?,
                first
                    .parse::<u32>()
                    .map_err(|_| DateTimeParseError::Invalid)?,
            ),
            DateOrder::Ymd => validate_date(
                parse_year_number(first, true).unwrap_or(a),
                b,
                third
                    .parse::<u32>()
                    .map_err(|_| DateTimeParseError::Invalid)?,
            ),
        };
    }

    match config.date_order {
        DateOrder::Mdy => validate_date(
            parse_year_number(third, true).ok_or(DateTimeParseError::Invalid)?,
            first
                .parse::<u32>()
                .map_err(|_| DateTimeParseError::Invalid)?,
            second
                .parse::<u32>()
                .map_err(|_| DateTimeParseError::Invalid)?,
        ),
        DateOrder::Dmy => validate_date(
            parse_year_number(third, true).ok_or(DateTimeParseError::Invalid)?,
            second
                .parse::<u32>()
                .map_err(|_| DateTimeParseError::Invalid)?,
            first
                .parse::<u32>()
                .map_err(|_| DateTimeParseError::Invalid)?,
        ),
        DateOrder::Ymd => validate_date(
            parse_year_number(first, true).ok_or(DateTimeParseError::Invalid)?,
            second
                .parse::<u32>()
                .map_err(|_| DateTimeParseError::Invalid)?,
            third
                .parse::<u32>()
                .map_err(|_| DateTimeParseError::Invalid)?,
        ),
    }
}

pub fn parse_ordinal_date(year: i32, ordinal_day: u32) -> Option<(i32, u32, u32)> {
    if ordinal_day == 0 || ordinal_day > if is_leap_year(year) { 366 } else { 365 } {
        return None;
    }
    let mut remaining = ordinal_day;
    for month in 1..=12 {
        let days = days_in_month(year, month);
        if remaining <= days {
            return Some((year, month, remaining));
        }
        remaining -= days;
    }
    None
}

pub fn parse_date_token_with_config(
    text: &str,
    config: &DateTimeConfig,
) -> Result<Option<(i32, u32, u32)>, DateTimeParseError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let iso_parts = trimmed.split('-').collect::<Vec<_>>();
    if iso_parts.len() == 3 && iso_parts[0].len() >= 4 {
        if let Some(parts) = parse_date_parts(trimmed) {
            return Ok(Some(parts));
        }
    }
    if trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return match trimmed.len() {
            8 => {
                let year = trimmed[0..4]
                    .parse::<i32>()
                    .map_err(|_| DateTimeParseError::Invalid)?;
                let month = trimmed[4..6]
                    .parse::<u32>()
                    .map_err(|_| DateTimeParseError::Invalid)?;
                let day = trimmed[6..8]
                    .parse::<u32>()
                    .map_err(|_| DateTimeParseError::Invalid)?;
                Ok(Some(validate_date(year, month, day)?))
            }
            6 => {
                let year = expand_two_digit_year(
                    trimmed[0..2]
                        .parse::<i32>()
                        .map_err(|_| DateTimeParseError::Invalid)?,
                );
                let month = trimmed[2..4]
                    .parse::<u32>()
                    .map_err(|_| DateTimeParseError::Invalid)?;
                let day = trimmed[4..6]
                    .parse::<u32>()
                    .map_err(|_| DateTimeParseError::Invalid)?;
                Ok(Some(validate_date(year, month, day)?))
            }
            _ => Ok(None),
        };
    }

    for delim in ['-', '/', '.'] {
        if trimmed.contains(delim) {
            let parts = trimmed.split(delim).collect::<Vec<_>>();
            if delim == '.' && parts.len() == 2 && parts[0].chars().all(|ch| ch.is_ascii_digit()) {
                let year = parse_year_number(parts[0], true).ok_or(DateTimeParseError::Invalid)?;
                let ordinal = parts[1]
                    .parse::<u32>()
                    .map_err(|_| DateTimeParseError::Invalid)?;
                return parse_ordinal_date(year, ordinal)
                    .map(Some)
                    .ok_or(DateTimeParseError::FieldOutOfRange);
            }
            if parts.len() != 3 {
                return Ok(None);
            }
            if let Some(month) = month_number(parts[0]) {
                let day = parts[1]
                    .parse::<u32>()
                    .map_err(|_| DateTimeParseError::Invalid)?;
                let year = parse_year_number(parts[2], true).ok_or(DateTimeParseError::Invalid)?;
                return Ok(Some(validate_date(year, month, day)?));
            }
            if let Some(month) = month_number(parts[1]) {
                let first = parts[0];
                let third = parts[2];
                let first_value = first
                    .parse::<i32>()
                    .map_err(|_| DateTimeParseError::Invalid)?;
                let year_first = first.len() >= 3 || first_value > 31;
                let (day_text, year_text) = if year_first {
                    (third, first)
                } else {
                    (first, third)
                };
                let day = day_text
                    .parse::<u32>()
                    .map_err(|_| DateTimeParseError::Invalid)?;
                let year = parse_year_number(year_text, true).ok_or(DateTimeParseError::Invalid)?;
                return Ok(Some(validate_date(year, month, day)?));
            }
            if let Some(month) = month_number(parts[2]) {
                let day = parts[1]
                    .parse::<u32>()
                    .map_err(|_| DateTimeParseError::Invalid)?;
                let year = parse_year_number(parts[0], true).ok_or(DateTimeParseError::Invalid)?;
                return Ok(Some(validate_date(year, month, day)?));
            }
            if parts
                .iter()
                .all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_digit()))
            {
                return Ok(Some(parse_numeric_triplet(
                    parts[0], parts[1], parts[2], config,
                )?));
            }
            return Ok(None);
        }
    }

    let mut alpha_start = None;
    let mut alpha_end = None;
    for (idx, ch) in trimmed.char_indices() {
        if ch.is_ascii_alphabetic() {
            alpha_start.get_or_insert(idx);
            alpha_end = Some(idx + ch.len_utf8());
        } else if alpha_start.is_some() && alpha_end.is_some() {
            break;
        }
    }
    if let (Some(start), Some(end)) = (alpha_start, alpha_end) {
        let month = month_number(&trimmed[start..end]).ok_or(DateTimeParseError::Invalid)?;
        let prefix = &trimmed[..start];
        let suffix = &trimmed[end..];
        if !prefix.is_empty()
            && !suffix.is_empty()
            && prefix.chars().all(|ch| ch.is_ascii_digit())
            && suffix.chars().all(|ch| ch.is_ascii_digit())
        {
            let year = parse_year_number(prefix, true).ok_or(DateTimeParseError::Invalid)?;
            let day = suffix
                .parse::<u32>()
                .map_err(|_| DateTimeParseError::Invalid)?;
            return Ok(Some(validate_date(year, month, day)?));
        }
    }

    Ok(None)
}

pub fn parse_fraction_to_usecs(text: &str) -> Option<i64> {
    if text.is_empty() || !text.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let trimmed = if text.len() > 6 { &text[..6] } else { text };
    let mut micros = trimmed.parse::<i64>().ok()?;
    for _ in 0..(6usize.saturating_sub(trimmed.len())) {
        micros *= 10;
    }
    Some(micros)
}

fn parse_numeric_offset_seconds(text: &str) -> Option<i32> {
    let trimmed = text.trim();
    let sign = match trimmed.as_bytes().first().copied() {
        Some(b'+') => 1,
        Some(b'-') => -1,
        _ => return None,
    };
    let rest = &trimmed[1..];
    let (hour, minute, second) = if rest.contains(':') {
        let parts = rest.split(':').collect::<Vec<_>>();
        let hour = parts.first()?.parse::<i32>().ok()?;
        let minute = parts
            .get(1)
            .map_or(Some(0), |part| part.parse::<i32>().ok())?;
        let second = parts
            .get(2)
            .map_or(Some(0), |part| part.parse::<i32>().ok())?;
        (hour, minute, second)
    } else if rest.chars().all(|ch| ch.is_ascii_digit()) {
        match rest.len() {
            1 | 2 => (rest.parse::<i32>().ok()?, 0, 0),
            3 | 4 => (
                rest[..rest.len() - 2].parse::<i32>().ok()?,
                rest[rest.len() - 2..].parse::<i32>().ok()?,
                0,
            ),
            5 | 6 => (
                rest[..rest.len() - 4].parse::<i32>().ok()?,
                rest[rest.len() - 4..rest.len() - 2].parse::<i32>().ok()?,
                rest[rest.len() - 2..].parse::<i32>().ok()?,
            ),
            _ => return None,
        }
    } else {
        return None;
    };
    if minute > 59 || second > 59 {
        return None;
    }
    Some(sign * (hour * 3600 + minute * 60 + second))
}

pub fn named_timezone_offset_seconds(name: &str) -> Option<i32> {
    match name.trim().to_ascii_lowercase().as_str() {
        "utc" | "gmt" | "etc/utc" | "etc/gmt" | "z" | "zulu" => Some(0),
        "est" | "america/new_york" => Some(-5 * 3600),
        "edt" => Some(-4 * 3600),
        "cst" => Some(-6 * 3600),
        "cdt" => Some(-5 * 3600),
        "mst" => Some(-7 * 3600),
        "mdt" => Some(-6 * 3600),
        "pst" | "america/los_angeles" => Some(-8 * 3600),
        "pdt" => Some(-7 * 3600),
        _ => None,
    }
}

fn timezone_name_exists(name: &str) -> bool {
    [
        "/usr/share/zoneinfo",
        "/var/db/timezone/zoneinfo",
        "/usr/share/lib/zoneinfo",
    ]
    .into_iter()
    .any(|base| Path::new(base).join(name).exists())
}

fn is_timezone_name_candidate(token: &str) -> bool {
    let trimmed = token.trim();
    !trimmed.is_empty()
        && (trimmed.contains('/')
            || trimmed.eq_ignore_ascii_case("zulu")
            || trimmed
                .chars()
                .all(|ch| ch.is_ascii_alphabetic() || matches!(ch, '_' | '/' | '+' | '-' | ':')))
}

pub fn parse_timezone_spec(text: &str) -> Result<Option<TimeZoneSpec>, DateTimeParseError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if let Some(offset) = parse_numeric_offset_seconds(trimmed) {
        return Ok(Some(TimeZoneSpec::FixedOffset(offset)));
    }
    if let Some(offset) = named_timezone_offset_seconds(trimmed) {
        return Ok(Some(TimeZoneSpec::FixedOffset(offset)));
    }
    if let Some(sign_idx) = trimmed
        .char_indices()
        .skip(1)
        .find_map(|(idx, ch)| matches!(ch, '+' | '-').then_some(idx))
    {
        let prefix = &trimmed[..sign_idx];
        let suffix = &trimmed[sign_idx..];
        if named_timezone_offset_seconds(prefix).is_some()
            && parse_numeric_offset_seconds(suffix).is_some()
        {
            return Ok(Some(TimeZoneSpec::FixedOffset(
                parse_numeric_offset_seconds(suffix).expect("checked above"),
            )));
        }
    }
    if timezone_name_exists(trimmed) {
        return Ok(Some(TimeZoneSpec::Named(normalize_timezone_name(trimmed))));
    }
    if is_timezone_name_candidate(trimmed) {
        return Err(DateTimeParseError::UnknownTimeZone(
            trimmed.to_ascii_lowercase(),
        ));
    }
    Ok(None)
}

pub fn parse_offset_seconds(text: &str) -> Option<i32> {
    match parse_timezone_spec(text).ok()? {
        Some(TimeZoneSpec::FixedOffset(offset)) => Some(offset),
        _ => None,
    }
}

pub fn timezone_offset_seconds(config: &DateTimeConfig) -> i32 {
    parse_offset_seconds(current_timezone_name(config)).unwrap_or(0)
}

pub fn parse_time_components(text: &str) -> Option<(u32, u32, u32, i64)> {
    let (main, fraction) = match text.split_once('.') {
        Some((main, fraction)) => (main, Some(fraction)),
        None => (text, None),
    };
    let parts = main.split(':').collect::<Vec<_>>();
    if !(2..=3).contains(&parts.len()) {
        return None;
    }
    let hour = parts[0].parse::<u32>().ok()?;
    let minute = parts[1].parse::<u32>().ok()?;
    let second = parts
        .get(2)
        .map_or(Some(0), |part| part.parse::<u32>().ok())?;
    if hour > 24 || minute > 59 || second > 59 {
        return None;
    }
    let micros = fraction.map_or(Some(0), parse_fraction_to_usecs)?;
    Some((hour, minute, second, micros))
}

pub fn time_usecs_from_hms(hour: u32, minute: u32, second: u32, micros: i64) -> Option<i64> {
    let usecs = hour as i64 * USECS_PER_HOUR
        + minute as i64 * USECS_PER_MINUTE
        + second as i64 * USECS_PER_SEC
        + micros;
    if !(0..=USECS_PER_DAY).contains(&usecs) {
        return None;
    }
    Some(usecs)
}

pub fn split_time_and_offset(text: &str) -> (&str, Option<&str>) {
    let trimmed = text.trim();
    let bytes = trimmed.as_bytes();
    for idx in (1..bytes.len()).rev() {
        if matches!(bytes[idx], b'+' | b'-')
            && trimmed[..idx].contains(':')
            && !trimmed[..idx].contains(' ')
        {
            return (&trimmed[..idx], Some(&trimmed[idx..]));
        }
    }
    (trimmed, None)
}

pub fn format_date_ymd(pg_days: i32) -> String {
    let (year, month, day) = ymd_from_days(pg_days);
    format!("{year:04}-{month:02}-{day:02}")
}

pub fn format_time_usecs(time_usecs: i64) -> String {
    let mut remaining = time_usecs;
    let hour = remaining / USECS_PER_HOUR;
    remaining %= USECS_PER_HOUR;
    let minute = remaining / USECS_PER_MINUTE;
    remaining %= USECS_PER_MINUTE;
    let second = remaining / USECS_PER_SEC;
    let micros = remaining % USECS_PER_SEC;
    if micros == 0 {
        format!("{hour:02}:{minute:02}:{second:02}")
    } else {
        let mut fraction = format!("{micros:06}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        format!("{hour:02}:{minute:02}:{second:02}.{fraction}")
    }
}

pub fn format_offset(offset_seconds: i32) -> String {
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let mut remaining = offset_seconds.abs();
    let hour = remaining / 3600;
    remaining %= 3600;
    let minute = remaining / 60;
    let second = remaining % 60;
    if second != 0 {
        format!("{sign}{hour:02}:{minute:02}:{second:02}")
    } else if minute != 0 {
        format!("{sign}{hour:02}:{minute:02}")
    } else {
        format!("{sign}{hour:02}")
    }
}

pub fn today_pg_days(config: &DateTimeConfig) -> i32 {
    let now = current_postgres_timestamp_usecs();
    let local = now + timezone_offset_seconds(config) as i64 * USECS_PER_SEC;
    local.div_euclid(USECS_PER_DAY) as i32
}

pub fn split_timestamp_date_time(text: &str) -> Option<(&str, &str)> {
    let trimmed = text.trim();
    trimmed
        .split_once(' ')
        .or_else(|| trimmed.split_once('T'))
        .map(|(date, time)| (date.trim(), time.trim()))
}

pub fn timestamp_parts_from_usecs(timestamp_usecs: i64) -> (i32, i64) {
    let mut days = timestamp_usecs.div_euclid(USECS_PER_DAY) as i32;
    let mut time_usecs = timestamp_usecs.rem_euclid(USECS_PER_DAY);
    if time_usecs < 0 {
        time_usecs += USECS_PER_DAY;
        days -= 1;
    }
    (days, time_usecs)
}

pub fn pg_epoch_jdate() -> i32 {
    POSTGRES_EPOCH_JDATE
}

pub fn secs_per_day() -> i32 {
    SECS_PER_DAY
}
