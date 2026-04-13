use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::include::nodes::datetime::{
    POSTGRES_EPOCH_JDATE, SECS_PER_DAY, USECS_PER_DAY, USECS_PER_HOUR, USECS_PER_MINUTE,
    USECS_PER_SEC,
};
use std::time::{SystemTime, UNIX_EPOCH};

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

pub fn days_from_ymd(year: i32, month: u32, day: u32) -> Option<i32> {
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    let year_adj = year - i32::from(month <= 2);
    let era = if year_adj >= 0 { year_adj } else { year_adj - 399 } / 400;
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

pub fn parse_offset_seconds(text: &str) -> Option<i32> {
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case("z")
        || trimmed.eq_ignore_ascii_case("utc")
        || trimmed.eq_ignore_ascii_case("gmt")
    {
        return Some(0);
    }
    let sign = match trimmed.as_bytes().first().copied() {
        Some(b'+') => 1,
        Some(b'-') => -1,
        _ => return named_timezone_offset_seconds(trimmed),
    };
    let rest = &trimmed[1..];
    let parts = rest.split(':').collect::<Vec<_>>();
    let hour = parts.first()?.parse::<i32>().ok()?;
    let minute = parts.get(1).map_or(Some(0), |part| part.parse::<i32>().ok())?;
    let second = parts.get(2).map_or(Some(0), |part| part.parse::<i32>().ok())?;
    Some(sign * (hour * 3600 + minute * 60 + second))
}

pub fn named_timezone_offset_seconds(name: &str) -> Option<i32> {
    match name.trim().to_ascii_lowercase().as_str() {
        "utc" | "gmt" | "etc/utc" | "etc/gmt" | "z" => Some(0),
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
    let second = parts.get(2).map_or(Some(0), |part| part.parse::<u32>().ok())?;
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
