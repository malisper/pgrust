use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::time::datetime::{
    DateTimeKeyword, days_from_ymd, format_date_ymd, format_offset, format_time_usecs,
    parse_date_parts, parse_keyword, parse_offset_seconds, parse_time_components,
    split_time_and_offset, time_usecs_from_hms, timezone_offset_seconds, today_pg_days,
};
use crate::include::nodes::datetime::{DateADT, TimeADT, TimeTzADT};

pub fn parse_date_text(text: &str, config: &DateTimeConfig) -> Option<DateADT> {
    if let Some(keyword) = parse_keyword(text) {
        return match keyword {
            DateTimeKeyword::Today => Some(DateADT(today_pg_days(config))),
            DateTimeKeyword::Tomorrow => Some(DateADT(today_pg_days(config) + 1)),
            DateTimeKeyword::Yesterday => Some(DateADT(today_pg_days(config) - 1)),
            DateTimeKeyword::Epoch => Some(DateADT(days_from_ymd(1970, 1, 1)?)),
            DateTimeKeyword::Infinity => {
                Some(DateADT(crate::include::nodes::datetime::DATEVAL_NOEND))
            }
            DateTimeKeyword::NegInfinity => {
                Some(DateADT(crate::include::nodes::datetime::DATEVAL_NOBEGIN))
            }
            DateTimeKeyword::Now => Some(DateADT(today_pg_days(config))),
        };
    }
    let (year, month, day) = parse_date_parts(text)?;
    Some(DateADT(days_from_ymd(year, month, day)?))
}

pub fn parse_time_text(text: &str) -> Option<TimeADT> {
    let (time_text, _) = split_time_and_offset(text);
    let (hour, minute, second, micros) = parse_time_components(time_text)?;
    Some(TimeADT(time_usecs_from_hms(hour, minute, second, micros)?))
}

pub fn parse_timetz_text(text: &str, config: &DateTimeConfig) -> Option<TimeTzADT> {
    let (time_text, offset_text) = split_time_and_offset(text);
    let (hour, minute, second, micros) = parse_time_components(time_text)?;
    let time = TimeADT(time_usecs_from_hms(hour, minute, second, micros)?);
    let offset_seconds = offset_text
        .and_then(parse_offset_seconds)
        .unwrap_or_else(|| timezone_offset_seconds(config));
    Some(TimeTzADT {
        time,
        offset_seconds,
    })
}

pub fn format_date_text(value: DateADT, _config: &DateTimeConfig) -> String {
    format_date_ymd(value.0)
}

pub fn format_time_text(value: TimeADT, _config: &DateTimeConfig) -> String {
    format_time_usecs(value.0)
}

pub fn format_timetz_text(value: TimeTzADT, _config: &DateTimeConfig) -> String {
    format!(
        "{}{}",
        format_time_usecs(value.time.0),
        format_offset(value.offset_seconds)
    )
}
