use crate::backend::utils::misc::guc_datetime::{DateOrder, DateStyleFormat, DateTimeConfig};
use crate::backend::utils::time::datetime::{
    DateTimeKeyword, DateTimeParseError, TimeZoneSpec, current_postgres_timestamp_usecs,
    days_from_ymd, format_offset, format_time_usecs, month_number,
    named_timezone_offset_seconds_for_date, parse_date_token_with_config, parse_fraction_to_usecs,
    parse_keyword, parse_time_components, parse_timezone_spec, postgres_date_from_ymd_i64,
    split_time_and_offset, timezone_offset_seconds, timezone_offset_seconds_at_utc, today_pg_days,
    ymd_from_days,
};
use crate::include::nodes::datetime::{
    DATEVAL_NOBEGIN, DATEVAL_NOEND, DateADT, POSTGRES_EPOCH_JDATE, TimeADT, TimeTzADT,
    USECS_PER_DAY, USECS_PER_SEC,
};
use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DateParseError {
    Invalid,
    FieldOutOfRange { datestyle_hint: bool },
    OutOfRange,
}

fn supported_date_bounds() -> (i32, i32) {
    static BOUNDS: OnceLock<(i32, i32)> = OnceLock::new();
    *BOUNDS.get_or_init(|| {
        let min = days_from_ymd(-4713, 11, 24).expect("lower date bound must be valid");
        let max = days_from_ymd(5_874_897, 12, 31).expect("upper date bound must be valid");
        (min, max)
    })
}

fn parse_year_number(text: &str, allow_two_digits: bool) -> Result<i32, DateParseError> {
    let year = text.parse::<i32>().map_err(|_| DateParseError::Invalid)?;
    Ok(if allow_two_digits && text.len() <= 2 {
        if (0..=69).contains(&year) {
            year + 2000
        } else if (70..=99).contains(&year) {
            year + 1900
        } else {
            year
        }
    } else {
        year
    })
}

fn build_date(
    year: i32,
    month: u32,
    day: u32,
    datestyle_hint: bool,
) -> Result<DateADT, DateParseError> {
    let Some(days) = postgres_date_from_ymd_i64(year, month, day) else {
        return Err(DateParseError::FieldOutOfRange { datestyle_hint });
    };
    let (min, max) = supported_date_bounds();
    if days < i64::from(min) || days > i64::from(max) {
        return Err(DateParseError::OutOfRange);
    }
    Ok(DateADT(days as i32))
}

fn parse_numeric_tokens(
    tokens: &[&str],
    config: &DateTimeConfig,
) -> Result<DateADT, DateParseError> {
    if tokens.len() != 3
        || !tokens
            .iter()
            .all(|token| token.chars().all(|ch| ch.is_ascii_digit()))
    {
        return Err(DateParseError::Invalid);
    }
    let datestyle_hint = true;
    let first = tokens[0];
    let second = tokens[1];
    let third = tokens[2];

    let b = second.parse::<u32>().map_err(|_| DateParseError::Invalid)?;
    let c = third.parse::<i32>().map_err(|_| DateParseError::Invalid)?;

    if first.len() >= 3 {
        return build_date(
            parse_year_number(first, true)?,
            b,
            third.parse::<u32>().map_err(|_| DateParseError::Invalid)?,
            false,
        );
    }
    if third.len() >= 4 || c > 31 {
        if matches!(config.date_order, DateOrder::Ymd) {
            return Err(DateParseError::FieldOutOfRange {
                datestyle_hint: true,
            });
        }
        let year = parse_year_number(third, true)?;
        return match config.date_order {
            DateOrder::Mdy => build_date(
                year,
                first.parse::<u32>().map_err(|_| DateParseError::Invalid)?,
                second.parse::<u32>().map_err(|_| DateParseError::Invalid)?,
                datestyle_hint,
            ),
            DateOrder::Dmy => build_date(
                year,
                second.parse::<u32>().map_err(|_| DateParseError::Invalid)?,
                first.parse::<u32>().map_err(|_| DateParseError::Invalid)?,
                datestyle_hint,
            ),
            DateOrder::Ymd => build_date(
                parse_year_number(first, true)?,
                b,
                third.parse::<u32>().map_err(|_| DateParseError::Invalid)?,
                datestyle_hint,
            ),
        };
    }

    match config.date_order {
        DateOrder::Mdy => build_date(
            parse_year_number(third, true)?,
            first.parse::<u32>().map_err(|_| DateParseError::Invalid)?,
            second.parse::<u32>().map_err(|_| DateParseError::Invalid)?,
            datestyle_hint,
        ),
        DateOrder::Dmy => build_date(
            parse_year_number(third, true)?,
            second.parse::<u32>().map_err(|_| DateParseError::Invalid)?,
            first.parse::<u32>().map_err(|_| DateParseError::Invalid)?,
            datestyle_hint,
        ),
        DateOrder::Ymd => build_date(
            parse_year_number(first, true)?,
            second.parse::<u32>().map_err(|_| DateParseError::Invalid)?,
            third.parse::<u32>().map_err(|_| DateParseError::Invalid)?,
            false,
        ),
    }
}

fn parse_named_month_tokens(
    tokens: &[&str],
    config: &DateTimeConfig,
    allow_two_digit_year: bool,
) -> Result<DateADT, DateParseError> {
    let hyphenated = tokens.len() == 1 && tokens[0].contains('-');
    let parts = if hyphenated {
        tokens[0]
            .split('-')
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
    } else {
        tokens.to_vec()
    };
    if parts.len() != 3 {
        return Err(DateParseError::Invalid);
    }
    let first_month = month_number(parts[0]);
    let second_month = month_number(parts[1]);
    let third_month = month_number(parts[2]);

    match (first_month, second_month, third_month) {
        (Some(month), None, None) => {
            let day = parts[1]
                .parse::<u32>()
                .map_err(|_| DateParseError::Invalid)?;
            let year_text = parts[2];
            if year_text.len() >= 4 {
                build_date(
                    parse_year_number(year_text, allow_two_digit_year)?,
                    month,
                    day,
                    false,
                )
            } else if matches!(config.date_order, DateOrder::Ymd) {
                Err(DateParseError::FieldOutOfRange {
                    datestyle_hint: true,
                })
            } else {
                build_date(
                    parse_year_number(year_text, allow_two_digit_year)?,
                    month,
                    day,
                    false,
                )
            }
        }
        (None, Some(month), None) => {
            let first = parts[0];
            let third = parts[2];
            if first.len() >= 4 {
                build_date(
                    parse_year_number(first, allow_two_digit_year)?,
                    month,
                    third.parse::<u32>().map_err(|_| DateParseError::Invalid)?,
                    false,
                )
            } else if third.len() >= 4 {
                build_date(
                    parse_year_number(third, allow_two_digit_year)?,
                    month,
                    first.parse::<u32>().map_err(|_| DateParseError::Invalid)?,
                    false,
                )
            } else {
                match config.date_order {
                    DateOrder::Ymd => build_date(
                        parse_year_number(first, allow_two_digit_year)?,
                        month,
                        third.parse::<u32>().map_err(|_| DateParseError::Invalid)?,
                        true,
                    ),
                    DateOrder::Dmy => build_date(
                        parse_year_number(third, allow_two_digit_year)?,
                        month,
                        first.parse::<u32>().map_err(|_| DateParseError::Invalid)?,
                        true,
                    ),
                    DateOrder::Mdy => {
                        let day = first.parse::<u32>().map_err(|_| DateParseError::Invalid)?;
                        if day > 31 {
                            if hyphenated {
                                Err(DateParseError::FieldOutOfRange {
                                    datestyle_hint: true,
                                })
                            } else {
                                Err(DateParseError::Invalid)
                            }
                        } else {
                            build_date(
                                parse_year_number(third, allow_two_digit_year)?,
                                month,
                                day,
                                false,
                            )
                        }
                    }
                }
            }
        }
        (None, None, Some(month)) => {
            if hyphenated {
                return Err(DateParseError::Invalid);
            }
            let first = parts[0];
            let day = parts[1]
                .parse::<u32>()
                .map_err(|_| DateParseError::Invalid)?;
            if first.len() >= 4 || matches!(config.date_order, DateOrder::Ymd) {
                build_date(
                    parse_year_number(first, allow_two_digit_year)?,
                    month,
                    day,
                    false,
                )
            } else {
                Err(DateParseError::Invalid)
            }
        }
        _ => Err(DateParseError::Invalid),
    }
}

fn map_single_token_parse_error(text: &str, err: DateTimeParseError) -> DateParseError {
    match err {
        DateTimeParseError::Invalid => DateParseError::Invalid,
        DateTimeParseError::FieldOutOfRange => DateParseError::FieldOutOfRange {
            datestyle_hint: text.contains('/')
                || text
                    .split('-')
                    .all(|part| part.chars().all(|ch| ch.is_ascii_digit())),
        },
        DateTimeParseError::TimestampOutOfRange => DateParseError::FieldOutOfRange {
            datestyle_hint: false,
        },
        DateTimeParseError::TimeZoneDisplacementOutOfRange
        | DateTimeParseError::UnknownTimeZone(_) => DateParseError::Invalid,
    }
}

fn parse_julian_date(text: &str) -> Result<Option<DateADT>, DateParseError> {
    let Some(rest) = text.strip_prefix('J').or_else(|| text.strip_prefix('j')) else {
        return Ok(None);
    };
    if rest.is_empty() {
        return Err(DateParseError::Invalid);
    }
    if !rest.chars().all(|ch| ch.is_ascii_digit()) {
        return Ok(None);
    }
    let julian = rest.parse::<i32>().map_err(|_| DateParseError::Invalid)?;
    let days = julian - POSTGRES_EPOCH_JDATE;
    let (min, max) = supported_date_bounds();
    if days < min || days > max {
        return Err(DateParseError::OutOfRange);
    }
    Ok(Some(DateADT(days)))
}

pub fn parse_date_text(text: &str, config: &DateTimeConfig) -> Result<DateADT, DateParseError> {
    if let Some(keyword) = parse_keyword(text) {
        return match keyword {
            DateTimeKeyword::Today => Ok(DateADT(today_pg_days(config))),
            DateTimeKeyword::Tomorrow => Ok(DateADT(today_pg_days(config) + 1)),
            DateTimeKeyword::Yesterday => Ok(DateADT(today_pg_days(config) - 1)),
            DateTimeKeyword::Epoch => build_date(1970, 1, 1, false),
            DateTimeKeyword::Infinity => {
                Ok(DateADT(crate::include::nodes::datetime::DATEVAL_NOEND))
            }
            DateTimeKeyword::NegInfinity => {
                Ok(DateADT(crate::include::nodes::datetime::DATEVAL_NOBEGIN))
            }
            DateTimeKeyword::Now => Ok(DateADT(today_pg_days(config))),
        };
    }
    if let Some(value) = parse_julian_date(text)? {
        return Ok(value);
    }

    let trimmed = text.trim();
    let iso_parts = trimmed.split('-').collect::<Vec<_>>();
    if iso_parts.len() == 3
        && iso_parts[0].len() >= 4
        && iso_parts
            .iter()
            .all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_digit()))
    {
        let year = iso_parts[0]
            .parse::<i32>()
            .map_err(|_| DateParseError::Invalid)?;
        let month = iso_parts[1]
            .parse::<u32>()
            .map_err(|_| DateParseError::Invalid)?;
        let day = iso_parts[2]
            .parse::<u32>()
            .map_err(|_| DateParseError::Invalid)?;
        return build_date(year, month, day, false);
    }

    let normalized = trimmed.replace(',', " ");
    let mut tokens = normalized.split_whitespace().collect::<Vec<_>>();
    if tokens.is_empty() {
        return Err(DateParseError::Invalid);
    }

    let mut bc = false;
    if let Some(last) = tokens.last().copied() {
        let lower = last.to_ascii_lowercase();
        if matches!(lower.as_str(), "bc" | "b.c.") {
            bc = true;
            tokens.pop();
        } else if matches!(lower.as_str(), "ad" | "a.d.") {
            tokens.pop();
        }
    }

    let mut value = if tokens.len() == 1 {
        let token = tokens[0];
        if token.contains(' ') {
            return Err(DateParseError::Invalid);
        }
        if token.chars().any(|ch| ch.is_ascii_alphabetic()) {
            parse_named_month_tokens(&tokens, config, !bc)?
        } else if token.contains('-')
            || token.contains('/')
            || token.contains('.')
            || token.chars().all(|ch| ch.is_ascii_digit())
        {
            let (year, month, day) = parse_date_token_with_config(token, config)
                .map_err(|err| map_single_token_parse_error(token, err))?
                .ok_or(DateParseError::Invalid)?;
            build_date(year, month, day, false)?
        } else {
            return Err(DateParseError::Invalid);
        }
    } else {
        let alpha_tokens = tokens
            .iter()
            .filter(|token| token.chars().any(|ch| ch.is_ascii_alphabetic()))
            .count();
        if alpha_tokens > 0 {
            parse_named_month_tokens(&tokens, config, !bc)?
        } else {
            parse_numeric_tokens(&tokens, config)?
        }
    };

    if bc {
        let (year, month, day) = ymd_from_days(value.0);
        if year <= 0 {
            return Err(DateParseError::FieldOutOfRange {
                datestyle_hint: false,
            });
        }
        value = build_date(1 - year, month, day, false)?;
    }

    Ok(value)
}

fn tokenize_time_input<'a>(text: &'a str, config: &DateTimeConfig) -> Vec<&'a str> {
    let mut tokens = Vec::new();
    for token in text.split_whitespace() {
        if let Some((date, time)) = token.split_once('T') {
            if !date.is_empty()
                && !time.is_empty()
                && parse_date_token_with_config(date, config)
                    .ok()
                    .flatten()
                    .is_some()
            {
                tokens.push(date);
                tokens.push(time);
                continue;
            }
        }
        tokens.push(token);
    }
    tokens
}

fn split_meridiem_suffix(text: &str) -> (&str, Option<&str>) {
    if text.len() >= 2 {
        let suffix = &text[text.len() - 2..];
        if suffix.eq_ignore_ascii_case("am") || suffix.eq_ignore_ascii_case("pm") {
            return (&text[..text.len() - 2], Some(suffix));
        }
    }
    (text, None)
}

fn apply_meridiem(hour: u32, meridiem: Option<&str>) -> Option<u32> {
    match meridiem.map(|value| value.to_ascii_lowercase()) {
        None => Some(hour),
        Some(_) if !(1..=12).contains(&hour) => None,
        Some(value) if value == "am" => Some(if hour == 12 { 0 } else { hour }),
        Some(value) if value == "pm" => Some(if hour == 12 { 12 } else { hour + 12 }),
        _ => None,
    }
}

fn parse_fraction_to_usecs_rounded(text: &str) -> Option<(i64, bool)> {
    if text.is_empty() || !text.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let trimmed = if text.len() > 6 { &text[..6] } else { text };
    let mut micros = parse_fraction_to_usecs(trimmed)?;
    let mut carry = false;
    if text.len() > 6
        && text
            .as_bytes()
            .get(6)
            .copied()
            .is_some_and(|digit| digit >= b'5')
    {
        micros += 1;
        if micros >= 1_000_000 {
            micros = 0;
            carry = true;
        }
    }
    Some((micros, carry))
}

fn parse_compact_time_components_rounded(text: &str) -> Option<(u32, u32, u32, i64)> {
    let (main, fraction) = match text.split_once('.') {
        Some((main, fraction)) => (main, Some(fraction)),
        None => (text, None),
    };
    if !main.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let (hour, minute, second) = match main.len() {
        4 => (
            main[0..2].parse::<u32>().ok()?,
            main[2..4].parse::<u32>().ok()?,
            0,
        ),
        6 => (
            main[0..2].parse::<u32>().ok()?,
            main[2..4].parse::<u32>().ok()?,
            main[4..6].parse::<u32>().ok()?,
        ),
        _ => return None,
    };
    let (micros, carry_second) = match fraction {
        Some(fraction) => parse_fraction_to_usecs_rounded(fraction)?,
        None => (0, false),
    };
    Some((hour, minute, second + u32::from(carry_second), micros))
}

fn parse_time_fields_raw(text: &str) -> Option<(u32, u32, u32)> {
    let mut parts = text.split(':');
    let hour = parts.next()?.parse::<u32>().ok()?;
    let minute = parts.next()?.parse::<u32>().ok()?;
    let second = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((hour, minute, second))
}

fn standalone_meridiem(token: &str) -> bool {
    token.eq_ignore_ascii_case("am") || token.eq_ignore_ascii_case("pm")
}

fn timezone_spec_offset_seconds(
    zone: Option<TimeZoneSpec>,
    date: Option<i32>,
    config: &DateTimeConfig,
) -> Result<i32, DateTimeParseError> {
    match zone {
        Some(TimeZoneSpec::FixedOffset(offset)) => Ok(offset),
        Some(TimeZoneSpec::Named(name)) => {
            let date = date.ok_or(DateTimeParseError::Invalid)?;
            named_timezone_offset_seconds_for_date(&name, date).ok_or(
                DateTimeParseError::UnknownTimeZone(name.to_ascii_lowercase()),
            )
        }
        None => Ok(timezone_offset_seconds(config)),
    }
}

fn parse_time_token(text: &str) -> Result<Option<(i64, Option<TimeZoneSpec>)>, DateTimeParseError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let (main, inline_offset) = split_time_and_offset(trimmed);
    let (main, meridiem) = split_meridiem_suffix(main);
    let components = if main.contains(':') {
        let (main_time, fraction) = match main.split_once('.') {
            Some((main_time, fraction)) => (main_time, Some(fraction)),
            None => (main, None),
        };
        let (hour, minute, second) = match parse_time_components(main_time) {
            Some((hour, minute, second, _)) => (hour, minute, second),
            None => {
                let Some((hour, minute, second)) = parse_time_fields_raw(main_time) else {
                    return Ok(None);
                };
                if hour > 24 || minute > 59 || second > 60 {
                    return Err(DateTimeParseError::FieldOutOfRange);
                }
                (hour, minute, second)
            }
        };
        let (micros, carry_second) = match fraction {
            Some(fraction) => {
                parse_fraction_to_usecs_rounded(fraction).ok_or(DateTimeParseError::Invalid)?
            }
            None => (0, false),
        };
        (hour, minute, second + u32::from(carry_second), micros)
    } else {
        let Some(components) = parse_compact_time_components_rounded(main) else {
            return Ok(None);
        };
        components
    };

    let hour = apply_meridiem(components.0, meridiem).ok_or(DateTimeParseError::Invalid)?;
    let mut usecs = hour as i64 * crate::include::nodes::datetime::USECS_PER_HOUR
        + components.1 as i64 * crate::include::nodes::datetime::USECS_PER_MINUTE
        + components.2 as i64 * crate::include::nodes::datetime::USECS_PER_SEC
        + components.3;
    if components.0 == 24 && (components.1 != 0 || components.2 != 0 || components.3 != 0) {
        return Err(DateTimeParseError::FieldOutOfRange);
    }
    if components.2 == 60 && components.3 != 0 {
        return Err(DateTimeParseError::FieldOutOfRange);
    }
    if !(0..=crate::include::nodes::datetime::USECS_PER_DAY).contains(&usecs) {
        return Err(DateTimeParseError::FieldOutOfRange);
    }
    if components.2 == 60 {
        usecs = (hour as i64 * crate::include::nodes::datetime::USECS_PER_HOUR)
            + (components.1 as i64 + 1) * crate::include::nodes::datetime::USECS_PER_MINUTE;
    }
    let zone = match inline_offset {
        Some(zone_text) => parse_timezone_spec(zone_text)?,
        None => None,
    };
    Ok(Some((usecs, zone)))
}

pub fn parse_time_text(text: &str, config: &DateTimeConfig) -> Result<TimeADT, DateTimeParseError> {
    if matches!(parse_keyword(text), Some(DateTimeKeyword::Now)) {
        let (time_usecs, _) = current_local_time_and_offset(config);
        return Ok(TimeADT(time_usecs));
    }
    let (time_usecs, _, _) = parse_time_input(text, config)?;
    Ok(TimeADT(time_usecs))
}

fn current_local_time_and_offset(config: &DateTimeConfig) -> (i64, i32) {
    let utc_usecs = config
        .transaction_timestamp_usecs
        .unwrap_or_else(current_postgres_timestamp_usecs);
    let offset_seconds = timezone_offset_seconds_at_utc(config, utc_usecs);
    let local_usecs = utc_usecs + i64::from(offset_seconds) * USECS_PER_SEC;
    (local_usecs.rem_euclid(USECS_PER_DAY), offset_seconds)
}

fn parse_time_input(
    text: &str,
    config: &DateTimeConfig,
) -> Result<(i64, Option<TimeZoneSpec>, Option<i32>), DateTimeParseError> {
    let mut tokens = tokenize_time_input(text, config);
    if tokens.is_empty() {
        return Err(DateTimeParseError::Invalid);
    }

    let mut zone = None;
    let mut zone_requires_date = false;
    if tokens.len() > 1 {
        if let Some(last) = tokens.last().copied() {
            if !standalone_meridiem(last) {
                if let Some(spec) = parse_timezone_spec(last)? {
                    zone_requires_date = last.contains('/');
                    zone = Some(spec);
                    tokens.pop();
                }
            }
        }
    }

    let time_index = tokens
        .iter()
        .position(|token| {
            let trimmed = token.trim();
            trimmed.contains(':') || split_meridiem_suffix(trimmed).1.is_some()
        })
        .or_else(|| {
            (tokens.len() >= 2).then(|| {
                tokens.iter().enumerate().rev().find_map(|(index, token)| {
                    token
                        .trim()
                        .chars()
                        .all(|ch| ch.is_ascii_digit() || matches!(ch, '.' | '+' | '-'))
                        .then_some(index)
                })
            })?
        });
    let Some(mut time_index) = time_index else {
        return Err(DateTimeParseError::Invalid);
    };

    let mut owned_time = None;
    if let Some(next) = tokens.get(time_index + 1).copied() {
        if standalone_meridiem(next) {
            owned_time = Some(format!("{}{}", tokens[time_index], next));
            time_index += 1;
        }
    }
    let time_token = owned_time.as_deref().unwrap_or(tokens[time_index]);
    let (time_usecs, inline_zone) =
        parse_time_token(time_token)?.ok_or(DateTimeParseError::Invalid)?;
    if zone.is_some() && inline_zone.is_some() {
        return Err(DateTimeParseError::Invalid);
    }
    zone = zone.or(inline_zone);

    let mut remaining = Vec::with_capacity(tokens.len().saturating_sub(1));
    for (index, token) in tokens.iter().enumerate() {
        if index == time_index || (owned_time.is_some() && index + 1 == time_index) {
            continue;
        }
        remaining.push(*token);
    }

    let date = if remaining.is_empty() {
        None
    } else if remaining.len() == 1 {
        let (year, month, day) = parse_date_token_with_config(remaining[0], config)?
            .ok_or(DateTimeParseError::Invalid)?;
        Some(days_from_ymd(year, month, day).ok_or(DateTimeParseError::FieldOutOfRange)?)
    } else {
        let (year, month, day) = parse_date_token_with_config(&remaining.join("-"), config)?
            .ok_or(DateTimeParseError::Invalid)?;
        Some(days_from_ymd(year, month, day).ok_or(DateTimeParseError::FieldOutOfRange)?)
    };

    if (matches!(zone, Some(TimeZoneSpec::Named(_))) || zone_requires_date) && date.is_none() {
        return Err(DateTimeParseError::Invalid);
    }

    Ok((time_usecs, zone, date))
}

pub fn parse_timetz_text(
    text: &str,
    config: &DateTimeConfig,
) -> Result<TimeTzADT, DateTimeParseError> {
    if matches!(parse_keyword(text), Some(DateTimeKeyword::Now)) {
        let (time_usecs, offset_seconds) = current_local_time_and_offset(config);
        return Ok(TimeTzADT {
            time: TimeADT(time_usecs),
            offset_seconds,
        });
    }
    let (time_usecs, zone, date) = parse_time_input(text, config)?;
    let offset_seconds = timezone_spec_offset_seconds(zone, date, config)?;
    Ok(TimeTzADT {
        time: TimeADT(time_usecs),
        offset_seconds,
    })
}

pub fn format_date_text(value: DateADT, config: &DateTimeConfig) -> String {
    if value.0 == DATEVAL_NOEND {
        return "infinity".into();
    }
    if value.0 == DATEVAL_NOBEGIN {
        return "-infinity".into();
    }

    let (mut year, month, day) = ymd_from_days(value.0);
    let bc = year <= 0;
    if bc {
        year = 1 - year;
    }

    let rendered = match config.date_style_format {
        DateStyleFormat::Iso => format!("{year:04}-{month:02}-{day:02}"),
        DateStyleFormat::German => format!("{day:02}.{month:02}.{year:04}"),
        DateStyleFormat::Sql | DateStyleFormat::Postgres => match config.date_order {
            DateOrder::Ymd => format!("{year:04}-{month:02}-{day:02}"),
            DateOrder::Dmy => format!("{day:02}-{:02}-{year:04}", month),
            DateOrder::Mdy => format!("{month:02}-{day:02}-{year:04}"),
        },
    };

    if bc {
        format!("{rendered} BC")
    } else {
        rendered
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::utils::misc::guc_datetime::{DateOrder, DateStyleFormat};

    #[test]
    fn format_date_text_respects_datestyle_format_and_order() {
        let value = DateADT(days_from_ymd(1999, 1, 8).unwrap());

        let iso = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Mdy,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
            ..DateTimeConfig::default()
        };
        assert_eq!(format_date_text(value, &iso), "1999-01-08");

        let sql = DateTimeConfig {
            date_style_format: DateStyleFormat::Sql,
            date_order: DateOrder::Dmy,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
            ..DateTimeConfig::default()
        };
        assert_eq!(format_date_text(value, &sql), "08-01-1999");

        let german = DateTimeConfig {
            date_style_format: DateStyleFormat::German,
            date_order: DateOrder::Mdy,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
            ..DateTimeConfig::default()
        };
        assert_eq!(format_date_text(value, &german), "08.01.1999");
    }

    #[test]
    fn format_date_text_keeps_bc_suffix() {
        let value = DateADT(days_from_ymd(-98, 1, 8).unwrap());
        let config = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Mdy,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
            ..DateTimeConfig::default()
        };
        assert_eq!(format_date_text(value, &config), "0099-01-08 BC");
    }

    #[test]
    fn parse_date_text_accepts_common_date_regression_forms() {
        let ymd = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Ymd,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
            ..DateTimeConfig::default()
        };

        for input in [
            "January 8, 1999",
            "19990108",
            "990108",
            "1999.008",
            "J2451187",
            "2040-04-10 BC",
            "1999 Jan 08",
            "08 Jan 1999",
        ] {
            assert!(parse_date_text(input, &ymd).is_ok(), "{input}");
        }
    }

    #[test]
    fn parse_date_text_reports_range_classes() {
        let config = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Ymd,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
            ..DateTimeConfig::default()
        };

        assert_eq!(
            parse_date_text("1997-02-29", &config),
            Err(DateParseError::FieldOutOfRange {
                datestyle_hint: false,
            })
        );
        assert_eq!(
            parse_date_text("5874898-01-01", &config),
            Err(DateParseError::OutOfRange)
        );
    }

    #[test]
    fn parse_date_text_respects_numeric_date_order() {
        let ymd = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Ymd,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
            ..DateTimeConfig::default()
        };
        let dmy = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Dmy,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
            ..DateTimeConfig::default()
        };
        let mdy = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Mdy,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
            ..DateTimeConfig::default()
        };

        assert_eq!(
            parse_date_text("99-01-08", &ymd),
            parse_date_text("1999-01-08", &ymd)
        );
        assert_eq!(
            parse_date_text("99-08-01", &ymd),
            parse_date_text("1999-08-01", &ymd)
        );
        assert_eq!(
            parse_date_text("99-01-08", &dmy),
            Err(DateParseError::FieldOutOfRange {
                datestyle_hint: true,
            })
        );
        assert_eq!(
            parse_date_text("99-08-01", &dmy),
            Err(DateParseError::FieldOutOfRange {
                datestyle_hint: true,
            })
        );
        assert_eq!(
            parse_date_text("99-01-08", &mdy),
            Err(DateParseError::FieldOutOfRange {
                datestyle_hint: true,
            })
        );
        assert_eq!(
            parse_date_text("99-08-01", &mdy),
            Err(DateParseError::FieldOutOfRange {
                datestyle_hint: true,
            })
        );
        assert_eq!(
            parse_date_text("1/8/1999", &ymd),
            Err(DateParseError::FieldOutOfRange {
                datestyle_hint: true,
            })
        );
        assert_eq!(
            parse_date_text("1/8/1999", &dmy),
            parse_date_text("1999-08-01", &dmy)
        );
        assert_eq!(
            parse_date_text("1/8/1999", &mdy),
            parse_date_text("1999-01-08", &mdy)
        );
        assert_eq!(
            parse_date_text("01/02/03", &ymd),
            parse_date_text("2001-02-03", &ymd)
        );
        assert_eq!(
            parse_date_text("01/02/03", &dmy),
            parse_date_text("2003-02-01", &dmy)
        );
        assert_eq!(
            parse_date_text("01/02/03", &mdy),
            parse_date_text("2003-01-02", &mdy)
        );
    }

    #[test]
    fn parse_date_text_matches_postgres_for_ambiguous_numeric_forms() {
        let ymd = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Ymd,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
            ..DateTimeConfig::default()
        };
        let dmy = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Dmy,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
            ..DateTimeConfig::default()
        };
        let mdy = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Mdy,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
            ..DateTimeConfig::default()
        };

        let out_of_range = Err(DateParseError::FieldOutOfRange {
            datestyle_hint: true,
        });

        assert_eq!(parse_date_text("1/8/1999", &ymd), out_of_range);
        assert_eq!(parse_date_text("1/18/1999", &ymd), out_of_range);
        assert_eq!(parse_date_text("18/1/1999", &ymd), out_of_range);
        assert_eq!(
            parse_date_text("01/02/03", &ymd),
            parse_date_text("2001-02-03", &ymd)
        );
        assert_eq!(
            parse_date_text("99-01-08", &ymd),
            parse_date_text("1999-01-08", &ymd)
        );
        assert_eq!(parse_date_text("08-01-99", &ymd), out_of_range);
        assert_eq!(parse_date_text("01-08-99", &ymd), out_of_range);
        assert_eq!(
            parse_date_text("99-08-01", &ymd),
            parse_date_text("1999-08-01", &ymd)
        );
        assert_eq!(
            parse_date_text("99 01 08", &ymd),
            parse_date_text("1999-01-08", &ymd)
        );
        assert_eq!(parse_date_text("08 01 99", &ymd), out_of_range);
        assert_eq!(parse_date_text("01 08 99", &ymd), out_of_range);
        assert_eq!(
            parse_date_text("99 08 01", &ymd),
            parse_date_text("1999-08-01", &ymd)
        );

        assert_eq!(
            parse_date_text("1/8/1999", &dmy),
            parse_date_text("1999-08-01", &dmy)
        );
        assert_eq!(parse_date_text("1/18/1999", &dmy), out_of_range);
        assert_eq!(
            parse_date_text("18/1/1999", &dmy),
            parse_date_text("1999-01-18", &dmy)
        );
        assert_eq!(
            parse_date_text("01/02/03", &dmy),
            parse_date_text("2003-02-01", &dmy)
        );
        assert_eq!(parse_date_text("99-01-08", &dmy), out_of_range);
        assert_eq!(
            parse_date_text("08-01-99", &dmy),
            parse_date_text("1999-01-08", &dmy)
        );
        assert_eq!(
            parse_date_text("01-08-99", &dmy),
            parse_date_text("1999-08-01", &dmy)
        );
        assert_eq!(parse_date_text("99-08-01", &dmy), out_of_range);
        assert_eq!(parse_date_text("99 01 08", &dmy), out_of_range);
        assert_eq!(
            parse_date_text("08 01 99", &dmy),
            parse_date_text("1999-01-08", &dmy)
        );
        assert_eq!(
            parse_date_text("01 08 99", &dmy),
            parse_date_text("1999-08-01", &dmy)
        );
        assert_eq!(parse_date_text("99 08 01", &dmy), out_of_range);

        assert_eq!(
            parse_date_text("1/8/1999", &mdy),
            parse_date_text("1999-01-08", &mdy)
        );
        assert_eq!(
            parse_date_text("1/18/1999", &mdy),
            parse_date_text("1999-01-18", &mdy)
        );
        assert_eq!(parse_date_text("18/1/1999", &mdy), out_of_range);
        assert_eq!(
            parse_date_text("01/02/03", &mdy),
            parse_date_text("2003-01-02", &mdy)
        );
        assert_eq!(parse_date_text("99-01-08", &mdy), out_of_range);
        assert_eq!(
            parse_date_text("08-01-99", &mdy),
            parse_date_text("1999-08-01", &mdy)
        );
        assert_eq!(
            parse_date_text("01-08-99", &mdy),
            parse_date_text("1999-01-08", &mdy)
        );
        assert_eq!(parse_date_text("99-08-01", &mdy), out_of_range);
        assert_eq!(parse_date_text("99 01 08", &mdy), out_of_range);
        assert_eq!(
            parse_date_text("08 01 99", &mdy),
            parse_date_text("1999-08-01", &mdy)
        );
        assert_eq!(
            parse_date_text("01 08 99", &mdy),
            parse_date_text("1999-01-08", &mdy)
        );
        assert_eq!(parse_date_text("99 08 01", &mdy), out_of_range);
    }

    #[test]
    fn parse_date_text_matches_postgres_for_named_month_forms() {
        let ymd = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Ymd,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
            ..DateTimeConfig::default()
        };
        let dmy = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Dmy,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
            ..DateTimeConfig::default()
        };
        let mdy = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Mdy,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
            ..DateTimeConfig::default()
        };

        let out_of_range = Err(DateParseError::FieldOutOfRange {
            datestyle_hint: true,
        });
        let invalid = Err(DateParseError::Invalid);

        assert_eq!(parse_date_text("January 8, 99 BC", &ymd), out_of_range);
        assert_eq!(
            parse_date_text("January 8, 99 BC", &dmy),
            parse_date_text("0099-01-08 BC", &dmy)
        );
        assert_eq!(
            parse_date_text("January 8, 99 BC", &mdy),
            parse_date_text("0099-01-08 BC", &mdy)
        );

        assert_eq!(
            parse_date_text("99-Jan-08", &ymd),
            parse_date_text("1999-01-08", &ymd)
        );
        assert_eq!(parse_date_text("99-Jan-08", &dmy), out_of_range);
        assert_eq!(parse_date_text("99-Jan-08", &mdy), out_of_range);

        assert_eq!(parse_date_text("08-Jan-99", &ymd), out_of_range);
        assert_eq!(
            parse_date_text("08-Jan-99", &dmy),
            parse_date_text("1999-01-08", &dmy)
        );
        assert_eq!(
            parse_date_text("08-Jan-99", &mdy),
            parse_date_text("1999-01-08", &mdy)
        );

        assert_eq!(parse_date_text("Jan-08-99", &ymd), out_of_range);
        assert_eq!(
            parse_date_text("Jan-08-99", &dmy),
            parse_date_text("1999-01-08", &dmy)
        );
        assert_eq!(
            parse_date_text("Jan-08-99", &mdy),
            parse_date_text("1999-01-08", &mdy)
        );

        assert_eq!(parse_date_text("99-08-Jan", &ymd), invalid);
        assert_eq!(parse_date_text("99-08-Jan", &dmy), invalid);
        assert_eq!(parse_date_text("99-08-Jan", &mdy), invalid);

        assert_eq!(
            parse_date_text("99 Jan 08", &ymd),
            parse_date_text("1999-01-08", &ymd)
        );
        assert_eq!(parse_date_text("99 Jan 08", &dmy), out_of_range);
        assert_eq!(parse_date_text("99 Jan 08", &mdy), invalid);

        assert_eq!(parse_date_text("08 Jan 99", &ymd), out_of_range);
        assert_eq!(
            parse_date_text("08 Jan 99", &dmy),
            parse_date_text("1999-01-08", &dmy)
        );
        assert_eq!(
            parse_date_text("08 Jan 99", &mdy),
            parse_date_text("1999-01-08", &mdy)
        );

        assert_eq!(parse_date_text("Jan 08 99", &ymd), out_of_range);
        assert_eq!(
            parse_date_text("Jan 08 99", &dmy),
            parse_date_text("1999-01-08", &dmy)
        );
        assert_eq!(
            parse_date_text("Jan 08 99", &mdy),
            parse_date_text("1999-01-08", &mdy)
        );

        assert_eq!(
            parse_date_text("99 08 Jan", &ymd),
            parse_date_text("1999-01-08", &ymd)
        );
        assert_eq!(parse_date_text("99 08 Jan", &dmy), invalid);
        assert_eq!(parse_date_text("99 08 Jan", &mdy), invalid);
    }

    #[test]
    fn parse_time_text_accepts_postgres_time_forms() {
        let config = DateTimeConfig::default();
        assert_eq!(
            parse_time_text("02:03 PST", &config),
            Ok(TimeADT(2 * 60 * 60 * 1_000_000 + 3 * 60 * 1_000_000))
        );
        assert_eq!(
            parse_time_text("11:59 EDT", &config),
            Ok(TimeADT(11 * 60 * 60 * 1_000_000 + 59 * 60 * 1_000_000))
        );
        assert_eq!(
            parse_time_text("11:59:59.99 PM", &config),
            Ok(TimeADT(
                23 * 60 * 60 * 1_000_000 + 59 * 60 * 1_000_000 + 59 * 1_000_000 + 990_000
            ))
        );
        assert_eq!(
            parse_time_text("2003-03-07 15:36:39 America/New_York", &config),
            Ok(TimeADT(
                15 * 60 * 60 * 1_000_000 + 36 * 60 * 1_000_000 + 39 * 1_000_000
            ))
        );
    }

    #[test]
    fn parse_time_text_accepts_now_keyword() {
        let mut config = DateTimeConfig {
            time_zone: "UTC".into(),
            transaction_timestamp_usecs: Some(
                3 * 60 * 60 * 1_000_000 + 4 * 60 * 1_000_000 + 5 * 1_000_000,
            ),
            ..DateTimeConfig::default()
        };
        assert_eq!(
            parse_time_text("now", &config),
            Ok(TimeADT(
                3 * 60 * 60 * 1_000_000 + 4 * 60 * 1_000_000 + 5 * 1_000_000
            ))
        );
        assert_eq!(
            parse_timetz_text("now", &config),
            Ok(TimeTzADT {
                time: TimeADT(3 * 60 * 60 * 1_000_000 + 4 * 60 * 1_000_000 + 5 * 1_000_000),
                offset_seconds: 0,
            })
        );

        config.time_zone = "Europe/Berlin".into();
        config.transaction_timestamp_usecs = Some(12 * 60 * 60 * 1_000_000);
        assert_eq!(
            parse_timetz_text("now", &config).unwrap().time,
            TimeADT(13 * 60 * 60 * 1_000_000)
        );
    }

    #[test]
    fn parse_time_text_handles_rounding_and_named_timezone_rules() {
        let config = DateTimeConfig::default();
        assert_eq!(
            parse_time_text("23:59:59.9999999", &config),
            Ok(TimeADT(crate::include::nodes::datetime::USECS_PER_DAY))
        );
        assert_eq!(
            parse_time_text("23:59:60", &config),
            Ok(TimeADT(crate::include::nodes::datetime::USECS_PER_DAY))
        );
        assert_eq!(
            parse_time_text("15:36:39 America/New_York", &config),
            Err(DateTimeParseError::Invalid)
        );
        assert_eq!(
            parse_time_text("24:00:00.01", &config),
            Err(DateTimeParseError::FieldOutOfRange)
        );
        assert_eq!(
            parse_time_text("23:59:60.01", &config),
            Err(DateTimeParseError::FieldOutOfRange)
        );
    }

    #[test]
    fn parse_timetz_text_accepts_fixed_abbreviations() {
        let config = DateTimeConfig::default();
        assert_eq!(
            parse_timetz_text("10:00 BST", &config),
            Ok(TimeTzADT {
                time: TimeADT(10 * 60 * 60 * 1_000_000),
                offset_seconds: 60 * 60,
            })
        );
    }

    #[test]
    fn parse_timetz_text_accepts_date_dependent_named_timezones() {
        let config = DateTimeConfig::default();
        assert_eq!(
            parse_timetz_text("2003-03-07 15:36:39 America/New_York", &config),
            Ok(TimeTzADT {
                time: TimeADT(15 * 60 * 60 * 1_000_000 + 36 * 60 * 1_000_000 + 39 * 1_000_000),
                offset_seconds: -5 * 60 * 60,
            })
        );
        assert_eq!(
            parse_timetz_text("2003-07-07 15:36:39 America/New_York", &config),
            Ok(TimeTzADT {
                time: TimeADT(15 * 60 * 60 * 1_000_000 + 36 * 60 * 1_000_000 + 39 * 1_000_000),
                offset_seconds: -4 * 60 * 60,
            })
        );
        assert_eq!(
            parse_timetz_text("15:36:39 America/New_York", &config),
            Err(DateTimeParseError::Invalid)
        );
    }
}
