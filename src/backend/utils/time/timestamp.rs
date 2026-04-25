use crate::backend::utils::misc::guc_datetime::DateStyleFormat;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::time::datetime::{
    DateTimeKeyword, DateTimeParseError, TimeZoneSpec, current_postgres_timestamp_usecs,
    current_timezone_name, day_of_week_from_julian_day, days_from_ymd, expand_two_digit_year,
    format_date_ymd, format_offset, format_time_usecs, is_bc_token, is_weekday_token,
    julian_day_from_postgres_date, month_number, named_timezone_offset_seconds_for_date,
    parse_date_token_with_config, parse_keyword, parse_time_components, parse_timezone_spec,
    split_time_and_offset, time_usecs_from_hms, timestamp_parts_from_usecs,
    timezone_offset_seconds, today_pg_days, ymd_from_days,
};
use crate::include::nodes::datetime::{TimestampADT, TimestampTzADT, USECS_PER_DAY, USECS_PER_SEC};

fn timestamp_min_usecs() -> i64 {
    i64::from(days_from_ymd(-4713, 11, 24).expect("valid timestamp lower bound")) * USECS_PER_DAY
}

fn timestamp_max_exclusive_usecs() -> i64 {
    i64::from(days_from_ymd(294277, 1, 1).expect("valid timestamp upper bound")) * USECS_PER_DAY
}

fn checked_timestamp_usecs(days: i32, time_usecs: i64) -> Result<i64, DateTimeParseError> {
    let value = i64::from(days)
        .checked_mul(USECS_PER_DAY)
        .and_then(|days_usecs| days_usecs.checked_add(time_usecs))
        .ok_or(DateTimeParseError::TimestampOutOfRange)?;
    if value < timestamp_min_usecs() || value >= timestamp_max_exclusive_usecs() {
        return Err(DateTimeParseError::TimestampOutOfRange);
    }
    Ok(value)
}

const WEEKDAY_ABBREV: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MONTH_ABBREV: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

fn format_timestamp_date(pg_days: i32, config: &DateTimeConfig, include_weekday: bool) -> String {
    let (year, month, day) = ymd_from_days(pg_days);
    let bc = year <= 0;
    let rendered = match config.date_style_format {
        DateStyleFormat::Postgres => {
            let month_name = MONTH_ABBREV[month.saturating_sub(1) as usize];
            if include_weekday {
                let weekday = WEEKDAY_ABBREV
                    [day_of_week_from_julian_day(julian_day_from_postgres_date(pg_days)) as usize];
                format!("{weekday} {month_name} {day:02}")
            } else {
                format!("{month_name} {day:02}")
            }
        }
        _ => format_date_ymd(pg_days),
    };
    if matches!(config.date_style_format, DateStyleFormat::Postgres) {
        rendered
    } else if bc {
        format!("{rendered} BC")
    } else {
        rendered
    }
}

fn format_timestamp_year_suffix(pg_days: i32) -> String {
    let (mut year, _, _) = ymd_from_days(pg_days);
    let bc = year <= 0;
    if bc {
        year = 1 - year;
        format!("{year:04} BC")
    } else {
        format!("{year:04}")
    }
}

fn format_timestamptz_year_zone_suffix(pg_days: i32, zone: &str) -> String {
    let (mut year, _, _) = ymd_from_days(pg_days);
    let bc = year <= 0;
    if bc {
        year = 1 - year;
        format!("{year:04} {zone} BC")
    } else {
        format!("{year:04} {zone}")
    }
}

fn timezone_abbrev_for_output(config: &DateTimeConfig, pg_days: i32) -> Option<&'static str> {
    match current_timezone_name(config)
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "utc" | "gmt" | "etc/utc" | "etc/gmt" | "z" | "zulu" => Some("UTC"),
        "pst" => Some("PST"),
        "pdt" => Some("PDT"),
        "america/los_angeles" => {
            if pg_days < days_from_ymd(1884, 1, 1).expect("valid cutoff date") {
                Some("LMT")
            } else {
                Some("PST")
            }
        }
        _ => None,
    }
}

fn tokenize_timestamp(text: &str) -> Vec<&str> {
    let mut tokens = Vec::new();
    for token in text.split_whitespace() {
        if let Some((date, time)) = token.split_once('T') {
            if !date.is_empty()
                && !time.is_empty()
                && parse_date_token_with_config(date, &DateTimeConfig::default())
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

fn normalize_timestamp_input(text: &str) -> String {
    let mut normalized = text.replace(',', "");
    for meridiem in ["AM", "PM", "am", "pm"] {
        let separated = format!(" {meridiem} ");
        let attached = format!("{meridiem} ");
        normalized = normalized.replace(&separated, &attached);
        let trailing = format!(" {meridiem}");
        if normalized.ends_with(&trailing) {
            normalized.truncate(normalized.len() - trailing.len());
            normalized.push_str(meridiem);
        }
    }
    normalized
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

fn parse_compact_time_components(text: &str) -> Option<(u32, u32, u32, i64)> {
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
    let micros = match fraction {
        Some(fraction) => crate::backend::utils::time::datetime::parse_fraction_to_usecs(fraction)?,
        None => 0,
    };
    if hour > 24 || minute > 59 || second > 59 {
        return None;
    }
    Some((hour, minute, second, micros))
}

fn is_time_token_candidate(token: &str) -> bool {
    let trimmed = token.trim();
    trimmed.contains(':')
        || matches!(split_meridiem_suffix(trimmed), (_, Some(_)))
        || trimmed
            .chars()
            .all(|ch| ch.is_ascii_digit() || matches!(ch, '.' | '+' | '-'))
}

fn parse_time_token(
    token: &str,
) -> Result<Option<(i64, Option<TimeZoneSpec>)>, DateTimeParseError> {
    let trimmed = token.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let (main, inline_offset) = split_time_and_offset(trimmed);
    let (main, meridiem) = split_meridiem_suffix(main);
    let components = if main.contains(':') {
        parse_time_components(main)
    } else {
        parse_compact_time_components(main)
    };
    let Some((hour, minute, second, micros)) = components else {
        return Ok(None);
    };
    let hour = apply_meridiem(hour, meridiem).ok_or(DateTimeParseError::Invalid)?;
    let time_usecs = time_usecs_from_hms(hour, minute, second, micros)
        .ok_or(DateTimeParseError::FieldOutOfRange)?;
    let zone = match inline_offset {
        Some(zone_text) => parse_timezone_spec(zone_text)?,
        None => None,
    };
    Ok(Some((time_usecs, zone)))
}

fn parse_date_tokens(
    tokens: &[&str],
    config: &DateTimeConfig,
) -> Result<(i32, u32, u32), DateTimeParseError> {
    match tokens {
        [single] => match parse_date_token_with_config(single, config) {
            Ok(Some(date)) => Ok(date),
            Err(_) | Ok(None) => {
                parse_two_digit_year_first_timestamp_date(single).ok_or(DateTimeParseError::Invalid)
            }
        },
        [first, second, third] => {
            if let Some(month) = month_number(first) {
                let day = second
                    .parse::<u32>()
                    .map_err(|_| DateTimeParseError::Invalid)?;
                let year = third
                    .parse::<i32>()
                    .map_err(|_| DateTimeParseError::Invalid)?;
                return days_from_ymd(year, month, day)
                    .map(|_| (year, month, day))
                    .ok_or(DateTimeParseError::FieldOutOfRange);
            }
            if let Some(month) = month_number(second) {
                let day = first
                    .parse::<u32>()
                    .map_err(|_| DateTimeParseError::Invalid)?;
                let year = third
                    .parse::<i32>()
                    .map_err(|_| DateTimeParseError::Invalid)?;
                return days_from_ymd(year, month, day)
                    .map(|_| (year, month, day))
                    .ok_or(DateTimeParseError::FieldOutOfRange);
            }
            let joined = tokens.join("-");
            parse_date_token_with_config(&joined, config)?.ok_or(DateTimeParseError::Invalid)
        }
        _ => Err(DateTimeParseError::Invalid),
    }
}

fn parse_two_digit_year_first_timestamp_date(token: &str) -> Option<(i32, u32, u32)> {
    let trimmed = token.trim();
    for delim in ['-', '/'] {
        if !trimmed.contains(delim) {
            continue;
        }
        let parts = trimmed.split(delim).collect::<Vec<_>>();
        if parts.len() != 3
            || !parts
                .iter()
                .all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_digit()))
        {
            return None;
        }
        let year = parts[0].parse::<i32>().ok()?;
        if parts[0].len() > 2 || year <= 31 {
            return None;
        }
        let year = expand_two_digit_year(year);
        let month = parts[1].parse::<u32>().ok()?;
        let day = parts[2].parse::<u32>().ok()?;
        days_from_ymd(year, month, day)?;
        return Some((year, month, day));
    }
    None
}

fn is_signed_numeric_timezone_candidate(token: &str) -> bool {
    let trimmed = token.trim();
    let Some(rest) = trimmed.strip_prefix(['+', '-']) else {
        return false;
    };
    !rest.is_empty() && rest.chars().all(|ch| ch.is_ascii_digit() || ch == ':')
}

fn extract_timestamp_parts(
    text: &str,
    config: &DateTimeConfig,
) -> Result<(i32, i64, Option<TimeZoneSpec>), DateTimeParseError> {
    let normalized = normalize_timestamp_input(text);
    let mut tokens = tokenize_timestamp(&normalized);
    if tokens.is_empty() {
        return Err(DateTimeParseError::Invalid);
    }

    if let Some(keyword) = parse_keyword(tokens[0]) {
        for token in &tokens[1..] {
            if parse_timezone_spec(token)?.is_some() {
                continue;
            }
            if is_weekday_token(token) {
                continue;
            }
            return Err(DateTimeParseError::Invalid);
        }
        let days = match keyword {
            DateTimeKeyword::Today | DateTimeKeyword::Now => today_pg_days(config),
            DateTimeKeyword::Tomorrow => today_pg_days(config) + 1,
            DateTimeKeyword::Yesterday => today_pg_days(config) - 1,
            DateTimeKeyword::Epoch => {
                days_from_ymd(1970, 1, 1).ok_or(DateTimeParseError::Invalid)?
            }
            DateTimeKeyword::Infinity | DateTimeKeyword::NegInfinity => {
                return Err(DateTimeParseError::Invalid);
            }
        };
        let time_usecs = if matches!(keyword, DateTimeKeyword::Now) {
            current_postgres_timestamp_usecs().rem_euclid(USECS_PER_DAY)
        } else {
            0
        };
        return Ok((days, time_usecs, None));
    }

    tokens.retain(|token| !is_weekday_token(token));

    let mut bc = false;
    tokens.retain(|token| {
        if is_bc_token(token) {
            bc = true;
            false
        } else {
            true
        }
    });

    let mut zone = None;
    if tokens.len() > 1 {
        if let Some(last) = tokens.last().copied() {
            if let Some(spec) = parse_timezone_spec(last)? {
                zone = Some(spec);
                tokens.pop();
            } else if is_signed_numeric_timezone_candidate(last) {
                return Err(DateTimeParseError::TimeZoneDisplacementOutOfRange);
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
                tokens
                    .iter()
                    .enumerate()
                    .rev()
                    .find_map(|(index, token)| is_time_token_candidate(token).then_some(index))
            })?
        });

    let mut time_usecs = 0;
    if let Some(index) = time_index {
        let (_, token) = tokens
            .iter()
            .enumerate()
            .find(|(candidate, _)| *candidate == index)
            .ok_or(DateTimeParseError::Invalid)?;
        let (parsed_time, inline_zone) =
            parse_time_token(token)?.ok_or(DateTimeParseError::Invalid)?;
        if zone.is_some() && inline_zone.is_some() {
            return Err(DateTimeParseError::Invalid);
        }
        time_usecs = parsed_time;
        zone = zone.or(inline_zone);
        tokens.remove(index);
    }

    let (mut year, month, day) = parse_date_tokens(&tokens, config)?;
    if bc {
        if year <= 0 {
            return Err(DateTimeParseError::FieldOutOfRange);
        }
        year = 1 - year;
    }
    let days = days_from_ymd(year, month, day).ok_or(DateTimeParseError::FieldOutOfRange)?;
    Ok((days, time_usecs, zone))
}

fn timezone_spec_offset(
    spec: Option<TimeZoneSpec>,
    pg_days: i32,
    config: &DateTimeConfig,
) -> Result<i32, DateTimeParseError> {
    match spec {
        Some(TimeZoneSpec::FixedOffset(offset)) => Ok(offset),
        Some(TimeZoneSpec::Named(name)) => named_timezone_offset_seconds_for_date(&name, pg_days)
            .ok_or(DateTimeParseError::UnknownTimeZone(
                name.to_ascii_lowercase(),
            )),
        None => Ok(timezone_offset_seconds(config)),
    }
}

fn timestamp_keyword_value(
    trimmed: &str,
    config: &DateTimeConfig,
) -> Option<Result<TimestampADT, DateTimeParseError>> {
    match parse_keyword(trimmed) {
        Some(DateTimeKeyword::Now) => Some(Ok(TimestampADT(current_postgres_timestamp_usecs()))),
        Some(DateTimeKeyword::Today) => Some(Ok(TimestampADT(
            today_pg_days(config) as i64 * USECS_PER_DAY,
        ))),
        Some(DateTimeKeyword::Tomorrow) => Some(Ok(TimestampADT(
            (today_pg_days(config) as i64 + 1) * USECS_PER_DAY,
        ))),
        Some(DateTimeKeyword::Yesterday) => Some(Ok(TimestampADT(
            (today_pg_days(config) as i64 - 1) * USECS_PER_DAY,
        ))),
        Some(DateTimeKeyword::Epoch) => days_from_ymd(1970, 1, 1)
            .map(|days| TimestampADT(days as i64 * USECS_PER_DAY))
            .map(Ok),
        Some(DateTimeKeyword::Infinity) => Some(Ok(TimestampADT(
            crate::include::nodes::datetime::TIMESTAMP_NOEND,
        ))),
        Some(DateTimeKeyword::NegInfinity) => Some(Ok(TimestampADT(
            crate::include::nodes::datetime::TIMESTAMP_NOBEGIN,
        ))),
        None => None,
    }
}

pub fn parse_timestamp_text(
    text: &str,
    config: &DateTimeConfig,
) -> Result<TimestampADT, DateTimeParseError> {
    let trimmed = text.trim();
    if let Some(value) = timestamp_keyword_value(trimmed, config) {
        return value;
    }
    let (days, time_usecs, _) = extract_timestamp_parts(trimmed, config)?;
    Ok(TimestampADT(checked_timestamp_usecs(days, time_usecs)?))
}

pub fn parse_timestamptz_text(
    text: &str,
    config: &DateTimeConfig,
) -> Result<TimestampTzADT, DateTimeParseError> {
    let trimmed = text.trim();
    match parse_keyword(trimmed) {
        Some(DateTimeKeyword::Now) => {
            return Ok(TimestampTzADT(current_postgres_timestamp_usecs()));
        }
        Some(DateTimeKeyword::Infinity) => {
            return Ok(TimestampTzADT(
                crate::include::nodes::datetime::TIMESTAMP_NOEND,
            ));
        }
        Some(DateTimeKeyword::NegInfinity) => {
            return Ok(TimestampTzADT(
                crate::include::nodes::datetime::TIMESTAMP_NOBEGIN,
            ));
        }
        _ => {}
    }

    let (days, time_usecs, zone) = extract_timestamp_parts(trimmed, config)?;
    let offset_seconds = timezone_spec_offset(zone, days, config)?;
    let value = checked_timestamp_usecs(days, time_usecs)?
        .checked_sub(offset_seconds as i64 * USECS_PER_SEC)
        .ok_or(DateTimeParseError::TimestampOutOfRange)?;
    if value < timestamp_min_usecs() || value >= timestamp_max_exclusive_usecs() {
        return Err(DateTimeParseError::TimestampOutOfRange);
    }
    Ok(TimestampTzADT(value))
}

pub fn format_timestamp_text(value: TimestampADT, _config: &DateTimeConfig) -> String {
    if value.0 == crate::include::nodes::datetime::TIMESTAMP_NOEND {
        return "infinity".into();
    }
    if value.0 == crate::include::nodes::datetime::TIMESTAMP_NOBEGIN {
        return "-infinity".into();
    }
    let (days, time_usecs) = timestamp_parts_from_usecs(value.0);
    match _config.date_style_format {
        DateStyleFormat::Postgres => format!(
            "{} {} {}",
            format_timestamp_date(days, _config, true),
            format_time_usecs(time_usecs),
            format_timestamp_year_suffix(days),
        ),
        _ => format!(
            "{} {}",
            format_timestamp_date(days, _config, false),
            format_time_usecs(time_usecs)
        ),
    }
}

pub fn format_timestamptz_text(value: TimestampTzADT, config: &DateTimeConfig) -> String {
    if value.0 == crate::include::nodes::datetime::TIMESTAMP_NOEND {
        return "infinity".into();
    }
    if value.0 == crate::include::nodes::datetime::TIMESTAMP_NOBEGIN {
        return "-infinity".into();
    }
    let offset_seconds = timezone_offset_seconds(config);
    let adjusted = value.0 + offset_seconds as i64 * USECS_PER_SEC;
    let (days, time_usecs) = timestamp_parts_from_usecs(adjusted);
    match config.date_style_format {
        DateStyleFormat::Postgres => {
            let zone = timezone_abbrev_for_output(config, days)
                .map(str::to_string)
                .unwrap_or_else(|| format_offset(offset_seconds));
            format!(
                "{} {} {}",
                format_timestamp_date(days, config, true),
                format_time_usecs(time_usecs),
                format_timestamptz_year_zone_suffix(days, &zone),
            )
        }
        _ => format!(
            "{} {}{}",
            format_timestamp_date(days, config, false),
            format_time_usecs(time_usecs),
            format_offset(offset_seconds)
        ),
    }
}

#[cfg(test)]
mod format_tests {
    use super::*;
    use crate::backend::utils::misc::guc_datetime::{DateOrder, DateStyleFormat};

    #[test]
    fn formats_timestamp_in_postgres_style() {
        let config = DateTimeConfig {
            date_style_format: DateStyleFormat::Postgres,
            date_order: DateOrder::Mdy,
            time_zone: "America/Los_Angeles".into(),
            ..DateTimeConfig::default()
        };
        let ts = TimestampADT(i64::from(days_from_ymd(1001, 1, 1).unwrap()) * USECS_PER_DAY);
        assert_eq!(
            format_timestamp_text(ts, &config),
            "Thu Jan 01 00:00:00 1001"
        );
        let bc = TimestampADT(i64::from(days_from_ymd(-96, 2, 16).unwrap()) * USECS_PER_DAY);
        assert_eq!(
            format_timestamp_text(bc, &config),
            "Tue Feb 16 00:00:00 0097 BC"
        );
    }

    #[test]
    fn formats_timestamptz_in_postgres_style() {
        let config = DateTimeConfig {
            date_style_format: DateStyleFormat::Postgres,
            date_order: DateOrder::Mdy,
            time_zone: "America/Los_Angeles".into(),
            ..DateTimeConfig::default()
        };
        let ts = TimestampTzADT(
            i64::from(days_from_ymd(1901, 1, 1).unwrap()) * USECS_PER_DAY
                + 8 * 3600 * USECS_PER_SEC,
        );
        assert_eq!(
            format_timestamptz_text(ts, &config),
            "Tue Jan 01 00:00:00 1901 PST"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_verbose_and_compact_timestamp_inputs() {
        let config = DateTimeConfig::default();
        for input in [
            "Mon Feb 10 17:32:01 1997 PST",
            "Mon Feb 10 17:32:01.000001 1997 PST",
            "19970210 173201 -0800",
            "2000-03-15 08:14:01 GMT+8",
            "Feb 10 17:32:01 1997 -0800",
            "Feb 10 5:32PM 1997",
            "1997/02/10 17:32:01-0800",
            "Feb-10-1997 17:32:01 PST",
            "02-10-1997 17:32:01 PST",
            "19970210 173201 PST",
            "97FEB10 5:32:01PM UTC",
            "97/02/10 17:32:01 UTC",
            "1997.041 17:32:01 UTC",
            "19970210 173201 America/New_York",
            "Feb 16 17:32:01 0097 BC",
        ] {
            assert!(parse_timestamp_text(input, &config).is_ok(), "{input}");
        }
    }

    #[test]
    fn reports_unknown_timezone_separately() {
        let config = DateTimeConfig::default();
        assert_eq!(
            parse_timestamp_text("19970710 173201 America/Does_not_exist", &config),
            Err(DateTimeParseError::UnknownTimeZone(
                "america/does_not_exist".into()
            ))
        );
    }

    #[test]
    fn parses_timestamptz_with_date_dependent_named_timezone() {
        let config = DateTimeConfig::default();
        let march_days = days_from_ymd(2003, 3, 7).unwrap();
        let july_days = days_from_ymd(2003, 7, 7).unwrap();
        let time_usecs =
            15 * 60 * 60 * USECS_PER_SEC + 36 * 60 * USECS_PER_SEC + 39 * USECS_PER_SEC;
        assert_eq!(
            parse_timestamptz_text("2003-03-07 15:36:39 America/New_York", &config),
            Ok(TimestampTzADT(
                i64::from(march_days) * USECS_PER_DAY + time_usecs + 5 * 60 * 60 * USECS_PER_SEC
            ))
        );
        assert_eq!(
            parse_timestamptz_text("2003-07-07 15:36:39 America/New_York", &config),
            Ok(TimestampTzADT(
                i64::from(july_days) * USECS_PER_DAY + time_usecs + 4 * 60 * 60 * USECS_PER_SEC
            ))
        );
    }

    #[test]
    fn rejects_out_of_range_signed_timezone_displacement() {
        let config = DateTimeConfig::default();
        assert_eq!(
            parse_timestamp_text("Feb 16 17:32:01 -0097", &config),
            Err(DateTimeParseError::TimeZoneDisplacementOutOfRange)
        );
    }

    #[test]
    fn rejects_timestamp_values_outside_postgres_range() {
        let config = DateTimeConfig::default();
        assert_eq!(
            parse_timestamp_text("Feb 16 17:32:01 5097 BC", &config),
            Err(DateTimeParseError::TimestampOutOfRange)
        );
        assert_eq!(
            parse_timestamp_text("4714-11-23 23:59:59 BC", &config),
            Err(DateTimeParseError::TimestampOutOfRange)
        );
        assert_eq!(
            parse_timestamp_text("294277-01-01 00:00:00", &config),
            Err(DateTimeParseError::TimestampOutOfRange)
        );
    }
}
