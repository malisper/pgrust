use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::time::datetime::{
    DateTimeKeyword, current_postgres_timestamp_usecs, format_date_ymd, format_offset,
    format_time_usecs, parse_date_parts, parse_keyword, parse_offset_seconds,
    parse_time_components, split_timestamp_date_time, time_usecs_from_hms,
    timestamp_parts_from_usecs, timezone_offset_seconds, today_pg_days,
};
use crate::include::nodes::datetime::{TimestampADT, TimestampTzADT};

pub fn parse_timestamp_text(text: &str, config: &DateTimeConfig) -> Option<TimestampADT> {
    let trimmed = text.trim();
    let keyword_text = trimmed.split_whitespace().next().unwrap_or(trimmed);
    match parse_keyword(keyword_text) {
        Some(DateTimeKeyword::Now) => return Some(TimestampADT(current_postgres_timestamp_usecs())),
        Some(DateTimeKeyword::Today) => {
            return Some(TimestampADT(today_pg_days(config) as i64
                * crate::include::nodes::datetime::USECS_PER_DAY));
        }
        Some(DateTimeKeyword::Tomorrow) => {
            return Some(TimestampADT(
                (today_pg_days(config) as i64 + 1) * crate::include::nodes::datetime::USECS_PER_DAY,
            ));
        }
        Some(DateTimeKeyword::Yesterday) => {
            return Some(TimestampADT(
                (today_pg_days(config) as i64 - 1) * crate::include::nodes::datetime::USECS_PER_DAY,
            ));
        }
        Some(DateTimeKeyword::Epoch) => {
            let (year, month, day) = (1970, 1, 1);
            let date = crate::backend::utils::time::datetime::days_from_ymd(year, month, day)?;
            return Some(TimestampADT(date as i64 * crate::include::nodes::datetime::USECS_PER_DAY));
        }
        Some(DateTimeKeyword::Infinity) => {
            return Some(TimestampADT(crate::include::nodes::datetime::TIMESTAMP_NOEND));
        }
        Some(DateTimeKeyword::NegInfinity) => {
            return Some(TimestampADT(crate::include::nodes::datetime::TIMESTAMP_NOBEGIN));
        }
        None => {}
    }
    if let Some((year, month, day)) = parse_date_parts(trimmed) {
        let pg_days = crate::backend::utils::time::datetime::days_from_ymd(year, month, day)?;
        return Some(TimestampADT(
            pg_days as i64 * crate::include::nodes::datetime::USECS_PER_DAY,
        ));
    }
    let (date_text, time_text) = split_timestamp_date_time(text)?;
    let (year, month, day) = parse_date_parts(date_text)?;
    let pg_days = crate::backend::utils::time::datetime::days_from_ymd(year, month, day)?;
    let time_text = time_text.trim();
    let (time_text, _) = if let Some((main, zone)) = time_text.rsplit_once(' ') {
        if parse_offset_seconds(zone).is_some() {
            (main.trim(), Some(zone.trim()))
        } else {
            crate::backend::utils::time::datetime::split_time_and_offset(time_text)
        }
    } else {
        crate::backend::utils::time::datetime::split_time_and_offset(time_text)
    };
    let (hour, minute, second, micros) = parse_time_components(time_text)?;
    let time_usecs = time_usecs_from_hms(hour, minute, second, micros)?;
    Some(TimestampADT(
        pg_days as i64 * crate::include::nodes::datetime::USECS_PER_DAY + time_usecs,
    ))
}

pub fn parse_timestamptz_text(text: &str, config: &DateTimeConfig) -> Option<TimestampTzADT> {
    let trimmed = text.trim();
    match parse_keyword(trimmed.split_whitespace().next().unwrap_or(trimmed)) {
        Some(DateTimeKeyword::Now) => return Some(TimestampTzADT(current_postgres_timestamp_usecs())),
        Some(DateTimeKeyword::Infinity) => {
            return Some(TimestampTzADT(crate::include::nodes::datetime::TIMESTAMP_NOEND));
        }
        Some(DateTimeKeyword::NegInfinity) => {
            return Some(TimestampTzADT(crate::include::nodes::datetime::TIMESTAMP_NOBEGIN));
        }
        _ => {}
    }
    let (date_text, time_text) = split_timestamp_date_time(text)?;
    let (year, month, day) = parse_date_parts(date_text)?;
    let pg_days = crate::backend::utils::time::datetime::days_from_ymd(year, month, day)?;
    let time_text = time_text.trim();
    let (time_main, offset_text) = if let Some((main, zone)) = time_text.rsplit_once(' ') {
        if parse_offset_seconds(zone).is_some() {
            (main.trim(), Some(zone.trim()))
        } else {
            crate::backend::utils::time::datetime::split_time_and_offset(time_text)
        }
    } else {
        crate::backend::utils::time::datetime::split_time_and_offset(time_text)
    };
    let (hour, minute, second, micros) = parse_time_components(time_main)?;
    let time_usecs = time_usecs_from_hms(hour, minute, second, micros)?;
    let offset_seconds = offset_text
        .and_then(parse_offset_seconds)
        .unwrap_or_else(|| timezone_offset_seconds(config));
    Some(TimestampTzADT(
        pg_days as i64 * crate::include::nodes::datetime::USECS_PER_DAY
            + time_usecs
            - offset_seconds as i64 * crate::include::nodes::datetime::USECS_PER_SEC,
    ))
}

pub fn format_timestamp_text(value: TimestampADT, _config: &DateTimeConfig) -> String {
    if value.0 == crate::include::nodes::datetime::TIMESTAMP_NOEND {
        return "infinity".into();
    }
    if value.0 == crate::include::nodes::datetime::TIMESTAMP_NOBEGIN {
        return "-infinity".into();
    }
    let (days, time_usecs) = timestamp_parts_from_usecs(value.0);
    format!("{} {}", format_date_ymd(days), format_time_usecs(time_usecs))
}

pub fn format_timestamptz_text(value: TimestampTzADT, config: &DateTimeConfig) -> String {
    if value.0 == crate::include::nodes::datetime::TIMESTAMP_NOEND {
        return "infinity".into();
    }
    if value.0 == crate::include::nodes::datetime::TIMESTAMP_NOBEGIN {
        return "-infinity".into();
    }
    let offset_seconds = timezone_offset_seconds(config);
    let adjusted = value.0 + offset_seconds as i64 * crate::include::nodes::datetime::USECS_PER_SEC;
    let (days, time_usecs) = timestamp_parts_from_usecs(adjusted);
    format!(
        "{} {}{}",
        format_date_ymd(days),
        format_time_usecs(time_usecs),
        format_offset(offset_seconds)
    )
}
