use crate::compat::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::compat::backend::utils::misc::guc_datetime::{DateOrder, DateStyleFormat};
use crate::compat::backend::utils::time::datetime::{
    DateTimeKeyword, DateTimeParseError, TimeZoneSpec, current_postgres_timestamp_usecs,
    current_timezone_name, day_of_week_from_julian_day, days_from_ymd, expand_two_digit_year,
    format_offset, format_time_usecs, is_bc_token, is_weekday_token, julian_day_from_postgres_date,
    month_number, named_timezone_abbreviation_at_utc, named_timezone_offset_seconds,
    named_timezone_offset_seconds_at_utc, named_timezone_offset_seconds_for_local,
    parse_date_token_with_config, parse_fraction_to_usecs, parse_keyword, parse_time_components,
    parse_timezone_spec, split_time_and_offset, time_usecs_from_hms, timestamp_parts_from_usecs,
    timezone_offset_seconds, timezone_offset_seconds_at_utc, today_pg_days, ymd_from_days,
};
use crate::compat::include::nodes::datetime::{
    POSTGRES_EPOCH_JDATE, TimestampADT, TimestampTzADT, USECS_PER_DAY, USECS_PER_SEC,
};

const MIN_TIMESTAMP_USECS: i64 = -211_813_488_000_000_000;
const END_TIMESTAMP_USECS: i64 = 9_223_371_331_200_000_000;

pub fn is_valid_finite_timestamp_usecs(usecs: i64) -> bool {
    (MIN_TIMESTAMP_USECS..END_TIMESTAMP_USECS).contains(&usecs)
}

const WEEKDAY_ABBREV: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MONTH_ABBREV: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

fn format_timestamp_date(pg_days: i32, config: &DateTimeConfig, include_weekday: bool) -> String {
    let (mut year, month, day) = ymd_from_days(pg_days);
    if year <= 0 {
        year = 1 - year;
    }
    match config.date_style_format {
        DateStyleFormat::Postgres => {
            let month_name = MONTH_ABBREV[month.saturating_sub(1) as usize];
            if include_weekday {
                let weekday = WEEKDAY_ABBREV
                    [day_of_week_from_julian_day(julian_day_from_postgres_date(pg_days)) as usize];
                if matches!(config.date_order, DateOrder::Dmy) {
                    format!("{weekday} {day:02} {month_name}")
                } else {
                    format!("{weekday} {month_name} {day:02}")
                }
            } else if matches!(config.date_order, DateOrder::Dmy) {
                format!("{day:02} {month_name}")
            } else {
                format!("{month_name} {day:02}")
            }
        }
        DateStyleFormat::German => format!("{day:02}.{month:02}.{year:04}"),
        DateStyleFormat::Iso => format!("{year:04}-{month:02}-{day:02}"),
        DateStyleFormat::Sql => match config.date_order {
            DateOrder::Ymd => format!("{year:04}-{month:02}-{day:02}"),
            DateOrder::Dmy => format!("{day:02}/{month:02}/{year:04}"),
            DateOrder::Mdy => format!("{month:02}/{day:02}/{year:04}"),
        },
    }
}

fn format_sql_style_timestamp_date(pg_days: i32, config: &DateTimeConfig) -> String {
    let mut sql_config = config.clone();
    if matches!(sql_config.date_order, DateOrder::Ymd) {
        sql_config.date_order = DateOrder::Mdy;
    }
    format_timestamp_date(pg_days, &sql_config, false)
}

fn format_timestamp_year_suffix(pg_days: i32) -> String {
    let (year, bc) = format_timestamp_year_parts(pg_days);
    if bc { format!("{year} BC") } else { year }
}

fn format_timestamp_year_parts(pg_days: i32) -> (String, bool) {
    let (mut year, _, _) = ymd_from_days(pg_days);
    let bc = year <= 0;
    if bc {
        year = 1 - year;
        (format!("{year:04}"), true)
    } else {
        (format!("{year:04}"), false)
    }
}

fn timezone_abbrev_for_output(
    config: &DateTimeConfig,
    utc_usecs: i64,
    offset_seconds: i32,
) -> Option<String> {
    let normalized = current_timezone_name(config)
        .trim()
        .to_ascii_lowercase()
        .to_string();
    match normalized.as_str() {
        "utc" | "gmt" | "etc/utc" | "etc/gmt" | "z" | "zulu" => Some("UTC".into()),
        "pst" => Some("PST".into()),
        "pdt" => Some("PDT".into()),
        "america/los_angeles" => {
            let local_usecs = utc_usecs + i64::from(offset_seconds) * USECS_PER_SEC;
            let (pg_days, _) = timestamp_parts_from_usecs(local_usecs);
            if offset_seconds == -(7 * 3600 + 52 * 60 + 58)
                || pg_days < days_from_ymd(1884, 1, 1).expect("valid cutoff date")
            {
                Some("LMT".into())
            } else if la_date_is_dst(pg_days) {
                Some("PDT".into())
            } else {
                Some("PST".into())
            }
        }
        _ => named_timezone_abbreviation_at_utc(current_timezone_name(config), utc_usecs),
    }
}

fn la_date_is_dst(pg_days: i32) -> bool {
    let (year, month, day) = ymd_from_days(pg_days);
    if year <= 2006 {
        match month {
            5..=9 => true,
            1..=3 | 11 | 12 => false,
            4 => day >= nth_weekday_of_month(year, 4, 0, 1),
            10 => day < last_weekday_of_month(year, 10, 0),
            _ => false,
        }
    } else {
        match month {
            4..=10 => true,
            1 | 2 | 12 => false,
            3 => day >= nth_weekday_of_month(year, 3, 0, 2),
            11 => day < nth_weekday_of_month(year, 11, 0, 1),
            _ => false,
        }
    }
}

fn nth_weekday_of_month(year: i32, month: u32, weekday: u32, nth: u32) -> u32 {
    let first = days_from_ymd(year, month, 1).expect("valid month");
    let first_weekday = day_of_week_from_julian_day(julian_day_from_postgres_date(first));
    1 + ((7 + weekday - first_weekday) % 7) + (nth - 1) * 7
}

fn last_weekday_of_month(year: i32, month: u32, weekday: u32) -> u32 {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let last = days_from_ymd(next_year, next_month, 1).expect("valid month") - 1;
    let (_, _, last_day) = ymd_from_days(last);
    let last_weekday = day_of_week_from_julian_day(julian_day_from_postgres_date(last));
    last_day - ((7 + last_weekday - weekday) % 7)
}

fn split_date_and_offset_token<'a>(
    token: &'a str,
    config: &DateTimeConfig,
) -> Option<(&'a str, &'a str)> {
    let trimmed = token.trim();
    for (idx, ch) in trimmed.char_indices().rev() {
        if idx == 0 {
            continue;
        }
        if !matches!(ch, '+' | '-') {
            continue;
        }
        let (date, zone) = trimmed.split_at(idx);
        if parse_date_token_with_config(date, config)
            .ok()
            .flatten()
            .is_some()
            && matches!(parse_timezone_spec(zone), Ok(Some(_)))
        {
            return Some((date, zone));
        }
    }
    None
}

fn tokenize_timestamp<'a>(text: &'a str, config: &DateTimeConfig) -> Vec<&'a str> {
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
        if let Some((date, time)) = token.split_once('T') {
            if !date.is_empty() && !time.is_empty() && parse_julian_timestamp_token(date).is_some()
            {
                tokens.push(date);
                tokens.push(time);
                continue;
            }
        }
        if let Some((date, zone)) = split_date_and_offset_token(token, config) {
            tokens.push(date);
            tokens.push(zone);
            continue;
        }
        if let Some((date, zone)) = split_julian_and_offset_token(token) {
            tokens.push(date);
            tokens.push(zone);
            continue;
        }
        if let Some(time) = token
            .strip_prefix('T')
            .or_else(|| token.strip_prefix('t'))
            .filter(|time| {
                time.as_bytes()
                    .first()
                    .is_some_and(|first| first.is_ascii_digit() || *first == b':')
            })
        {
            tokens.push(time);
            continue;
        }
        tokens.push(token);
    }
    tokens
}

fn split_julian_and_offset_token(token: &str) -> Option<(&str, &str)> {
    let trimmed = token.trim();
    if !matches!(trimmed.as_bytes().first(), Some(b'J') | Some(b'j')) {
        return None;
    }
    for (idx, ch) in trimmed.char_indices().rev() {
        if idx == 0 || !matches!(ch, '+' | '-') {
            continue;
        }
        let (date, zone) = trimmed.split_at(idx);
        if parse_julian_timestamp_token(date).is_some()
            && matches!(parse_timezone_spec(zone), Ok(Some(_)))
        {
            return Some((date, zone));
        }
    }
    None
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
        Some(fraction) => {
            crate::compat::backend::utils::time::datetime::parse_fraction_to_usecs(fraction)?
        }
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
            Ok(None) => parse_two_digit_year_first_timestamp_date(single)
                .or_else(|| parse_long_compact_timestamp_date(single))
                .ok_or(DateTimeParseError::Invalid),
            Err(err) => parse_two_digit_year_first_timestamp_date(single)
                .or_else(|| parse_long_compact_timestamp_date(single))
                .ok_or(err),
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
                if first.len() > 2 {
                    let year = first
                        .parse::<i32>()
                        .map_err(|_| DateTimeParseError::Invalid)?;
                    let day = third
                        .parse::<u32>()
                        .map_err(|_| DateTimeParseError::Invalid)?;
                    return days_from_ymd(year, month, day)
                        .map(|_| (year, month, day))
                        .ok_or(DateTimeParseError::FieldOutOfRange);
                }
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

fn parse_julian_timestamp_token(text: &str) -> Option<(i32, i64)> {
    let rest = text
        .trim()
        .strip_prefix('J')
        .or_else(|| text.trim().strip_prefix('j'))?;
    if rest.is_empty() {
        return None;
    }
    let (day_text, fraction) = match rest.split_once('.') {
        Some((day_text, fraction)) => (day_text, Some(fraction)),
        None => (rest, None),
    };
    if day_text.is_empty() || !day_text.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let julian = day_text.parse::<i32>().ok()?;
    let days = julian - POSTGRES_EPOCH_JDATE;
    let time_usecs = fraction
        .map(|fraction| parse_fraction_to_usecs(fraction).map(|usecs| usecs * 86_400))
        .unwrap_or(Some(0))?;
    Some((days, time_usecs))
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

fn parse_long_compact_timestamp_date(token: &str) -> Option<(i32, u32, u32)> {
    let trimmed = token.trim();
    if trimmed.len() <= 8 || !trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let year = trimmed[..trimmed.len() - 4].parse::<i32>().ok()?;
    let month = trimmed[trimmed.len() - 4..trimmed.len() - 2]
        .parse::<u32>()
        .ok()?;
    let day = trimmed[trimmed.len() - 2..].parse::<u32>().ok()?;
    days_from_ymd(year, month, day)?;
    Some((year, month, day))
}

fn parse_removable_timezone_token(token: &str) -> Result<Option<TimeZoneSpec>, DateTimeParseError> {
    let trimmed = token.trim();
    if trimmed.is_empty()
        || trimmed.chars().all(|ch| ch.is_ascii_digit())
        || month_number(trimmed).is_some()
        || is_weekday_token(trimmed)
        || is_bc_token(trimmed)
        || trimmed.eq_ignore_ascii_case("t")
        || parse_keyword(trimmed).is_some()
    {
        return Ok(None);
    }
    let parsed = parse_timezone_spec(trimmed)?;
    if parsed.is_none()
        && matches!(trimmed.as_bytes().first(), Some(b'+') | Some(b'-'))
        && trimmed[1..].chars().all(|ch| ch.is_ascii_digit())
    {
        return Err(DateTimeParseError::TimeZoneDisplacementOutOfRange);
    }
    Ok(parsed)
}

fn has_named_month_date_shape(tokens: &[&str]) -> bool {
    matches!(
        tokens,
        [first, _, _] if month_number(first).is_some()
    ) || matches!(
        tokens,
        [_, second, _] if month_number(second).is_some()
    )
}

fn extract_timestamp_parts(
    text: &str,
    config: &DateTimeConfig,
) -> Result<(i32, i64, Option<TimeZoneSpec>), DateTimeParseError> {
    let normalized = normalize_timestamp_input(text);
    let mut tokens = tokenize_timestamp(&normalized, config);
    if tokens.is_empty() {
        return Err(DateTimeParseError::Invalid);
    }

    if let Some(keyword) = parse_keyword(tokens[0]) {
        let mut zone = None;
        let mut parsed_time = None;
        for token in &tokens[1..] {
            if let Some((time_usecs, inline_zone)) = parse_time_token(token)? {
                if parsed_time.is_some() || (zone.is_some() && inline_zone.is_some()) {
                    return Err(DateTimeParseError::Invalid);
                }
                parsed_time = Some(time_usecs);
                zone = zone.or(inline_zone);
                continue;
            }
            if parse_keyword(token).is_some() {
                return Err(DateTimeParseError::Invalid);
            }
            if let Some(spec) = parse_timezone_spec(token)? {
                if zone.is_some() {
                    return Err(DateTimeParseError::Invalid);
                }
                zone = Some(spec);
                continue;
            }
            if is_weekday_token(token) {
                continue;
            }
            return Err(DateTimeParseError::Invalid);
        }
        let days = match keyword {
            DateTimeKeyword::Allballs => today_pg_days(config),
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
        if parsed_time.is_some()
            && !matches!(
                keyword,
                DateTimeKeyword::Today | DateTimeKeyword::Tomorrow | DateTimeKeyword::Yesterday
            )
        {
            return Err(DateTimeParseError::Invalid);
        }
        let time_usecs = if let Some(parsed_time) = parsed_time {
            parsed_time
        } else if matches!(keyword, DateTimeKeyword::Now) {
            config
                .transaction_timestamp_usecs
                .unwrap_or_else(current_postgres_timestamp_usecs)
                .rem_euclid(USECS_PER_DAY)
        } else {
            0
        };
        return Ok((days, time_usecs, zone));
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
    if tokens.len() == 2 && tokens[1].eq_ignore_ascii_case("allballs") {
        if let Some((julian_days, _)) = parse_julian_timestamp_token(tokens[0]) {
            return Ok((julian_days, 0, Some(TimeZoneSpec::FixedOffset(0))));
        }
        let (mut year, month, day) = parse_date_tokens(&tokens[..1], config)?;
        if bc {
            if year <= 0 {
                return Err(DateTimeParseError::FieldOutOfRange);
            }
            year = 1 - year;
        }
        let days = days_from_ymd(year, month, day).ok_or(DateTimeParseError::FieldOutOfRange)?;
        return Ok((days, 0, Some(TimeZoneSpec::FixedOffset(0))));
    }

    let mut zone = None;
    if tokens.len() >= 2
        && tokens
            .last()
            .is_some_and(|token| token.eq_ignore_ascii_case("dst"))
    {
        let zone_index = tokens.len() - 2;
        if let Some(TimeZoneSpec::FixedOffset(offset)) = parse_timezone_spec(tokens[zone_index])? {
            zone = Some(TimeZoneSpec::FixedOffset(offset + 3600));
            tokens.truncate(zone_index);
        }
    }
    if tokens.len() > 1 {
        if let Some(last) = tokens.last().copied() {
            if last.eq_ignore_ascii_case("allballs") || parse_keyword(last).is_some() {
                // PostgreSQL special date/time tokens are not timezone names.
            } else if let Some(spec) = parse_timezone_spec(last)? {
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
            (tokens.len() >= 2 && !has_named_month_date_shape(&tokens)).then(|| {
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
    if tokens.len() == 1 && tokens[0].eq_ignore_ascii_case("allballs") {
        return Ok((today_pg_days(config), 0, zone));
    }
    if tokens.len() >= 2
        && tokens
            .iter()
            .any(|token| token.eq_ignore_ascii_case("allballs"))
    {
        time_usecs = 0;
        tokens.retain(|token| !token.eq_ignore_ascii_case("allballs"));
    }

    if zone.is_none() {
        for index in (0..tokens.len()).rev().take(tokens.len().saturating_sub(1)) {
            if let Some(spec) = parse_removable_timezone_token(tokens[index])? {
                zone = Some(spec);
                tokens.remove(index);
                break;
            }
        }
    }

    if tokens.len() == 1 {
        if let Some(keyword) = parse_keyword(tokens[0]) {
            let days = match keyword {
                DateTimeKeyword::Allballs | DateTimeKeyword::Today | DateTimeKeyword::Now => {
                    today_pg_days(config)
                }
                DateTimeKeyword::Tomorrow => today_pg_days(config) + 1,
                DateTimeKeyword::Yesterday => today_pg_days(config) - 1,
                DateTimeKeyword::Epoch => {
                    days_from_ymd(1970, 1, 1).ok_or(DateTimeParseError::Invalid)?
                }
                DateTimeKeyword::Infinity | DateTimeKeyword::NegInfinity => {
                    return Err(DateTimeParseError::Invalid);
                }
            };
            return Ok((days, time_usecs, zone));
        }
        if let Some((days, julian_time_usecs)) = parse_julian_timestamp_token(tokens[0]) {
            if time_usecs != 0 && julian_time_usecs != 0 {
                return Err(DateTimeParseError::Invalid);
            }
            return Ok((days, time_usecs + julian_time_usecs, zone));
        }
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
    config: &DateTimeConfig,
    local_usecs: i64,
) -> Result<i32, DateTimeParseError> {
    match spec {
        Some(TimeZoneSpec::FixedOffset(offset)) => Ok(offset),
        Some(TimeZoneSpec::Named(name)) => {
            let normalized = name.to_ascii_lowercase();
            if normalized == "lmt" {
                return lmt_offset_seconds_for_local(config, local_usecs)
                    .ok_or(DateTimeParseError::Invalid);
            }
            if normalized == "mmt" {
                if current_timezone_name(config).eq_ignore_ascii_case("America/Montevideo") {
                    return named_timezone_offset_seconds_for_local(
                        current_timezone_name(config),
                        local_usecs,
                    )
                    .ok_or(DateTimeParseError::UnknownTimeZone(
                        name.to_ascii_lowercase(),
                    ));
                }
                return named_timezone_offset_seconds(&name).ok_or(
                    DateTimeParseError::UnknownTimeZone(name.to_ascii_lowercase()),
                );
            }
            named_timezone_offset_seconds_for_local(&name, local_usecs)
                .or_else(|| named_timezone_offset_seconds(&name))
                .ok_or(DateTimeParseError::UnknownTimeZone(
                    name.to_ascii_lowercase(),
                ))
        }
        None => current_timezone_offset_seconds_for_local(config, local_usecs).ok_or_else(|| {
            DateTimeParseError::UnknownTimeZone(current_timezone_name(config).to_ascii_lowercase())
        }),
    }
}

fn lmt_offset_seconds_for_local(config: &DateTimeConfig, local_usecs: i64) -> Option<i32> {
    match current_timezone_name(config)
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "utc" | "gmt" | "etc/utc" | "etc/gmt" => None,
        "america/los_angeles" | "pst" | "pdt" => Some(-(7 * 3600 + 52 * 60 + 58)),
        "europe/london" => Some(-(60 + 15)),
        _ => named_timezone_offset_seconds_for_local(current_timezone_name(config), local_usecs),
    }
}

fn current_timezone_offset_seconds_for_local(
    config: &DateTimeConfig,
    local_usecs: i64,
) -> Option<i32> {
    named_timezone_offset_seconds_for_local(current_timezone_name(config), local_usecs)
        .or_else(|| Some(timezone_offset_seconds(config)))
}

fn timestamp_keyword_value(
    trimmed: &str,
    config: &DateTimeConfig,
) -> Option<Result<TimestampADT, DateTimeParseError>> {
    match parse_keyword(trimmed) {
        Some(DateTimeKeyword::Allballs) => Some(Ok(TimestampADT(
            today_pg_days(config) as i64 * USECS_PER_DAY,
        ))),
        Some(DateTimeKeyword::Now) => Some(Ok(TimestampADT(
            config
                .transaction_timestamp_usecs
                .unwrap_or_else(current_postgres_timestamp_usecs),
        ))),
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
            crate::compat::include::nodes::datetime::TIMESTAMP_NOEND,
        ))),
        Some(DateTimeKeyword::NegInfinity) => Some(Ok(TimestampADT(
            crate::compat::include::nodes::datetime::TIMESTAMP_NOBEGIN,
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
    timestamp_usecs_from_parts(days, time_usecs, 0).map(TimestampADT)
}

pub fn parse_timestamptz_text(
    text: &str,
    config: &DateTimeConfig,
) -> Result<TimestampTzADT, DateTimeParseError> {
    let trimmed = text.trim();
    match parse_keyword(trimmed) {
        Some(DateTimeKeyword::Allballs) => {
            let days = today_pg_days(config);
            let offset_seconds =
                timezone_spec_offset(None, config, i64::from(days) * USECS_PER_DAY)?;
            return timestamp_usecs_from_parts(days, 0, offset_seconds).map(TimestampTzADT);
        }
        Some(DateTimeKeyword::Now) => {
            return Ok(TimestampTzADT(
                config
                    .transaction_timestamp_usecs
                    .unwrap_or_else(current_postgres_timestamp_usecs),
            ));
        }
        Some(DateTimeKeyword::Infinity) => {
            return Ok(TimestampTzADT(
                crate::compat::include::nodes::datetime::TIMESTAMP_NOEND,
            ));
        }
        Some(DateTimeKeyword::NegInfinity) => {
            return Ok(TimestampTzADT(
                crate::compat::include::nodes::datetime::TIMESTAMP_NOBEGIN,
            ));
        }
        Some(DateTimeKeyword::Epoch) => {
            return days_from_ymd(1970, 1, 1)
                .map(|days| TimestampTzADT(days as i64 * USECS_PER_DAY))
                .ok_or(DateTimeParseError::Invalid);
        }
        _ => {}
    }

    let (days, time_usecs, zone) = extract_timestamp_parts(trimmed, config)?;
    let local_usecs = local_timestamp_usecs_from_parts(days, time_usecs)?;
    let offset_seconds = timezone_spec_offset(zone, config, local_usecs)?;
    timestamp_usecs_from_parts(days, time_usecs, offset_seconds).map(TimestampTzADT)
}

pub fn timestamptz_at_time_zone(
    value: TimestampTzADT,
    zone: &str,
) -> Result<TimestampADT, DateTimeParseError> {
    let offset = named_timezone_offset_seconds_at_utc(zone, value.0)
        .or_else(|| named_timezone_offset_seconds(zone))
        .ok_or_else(|| DateTimeParseError::UnknownTimeZone(zone.to_string()))?;
    Ok(TimestampADT(value.0 + offset as i64 * USECS_PER_SEC))
}

pub fn timestamp_at_time_zone(
    value: TimestampADT,
    zone: &str,
) -> Result<TimestampTzADT, DateTimeParseError> {
    let offset = named_timezone_offset_seconds_for_local(zone, value.0)
        .or_else(|| named_timezone_offset_seconds(zone))
        .ok_or_else(|| DateTimeParseError::UnknownTimeZone(zone.to_string()))?;
    Ok(TimestampTzADT(value.0 - offset as i64 * USECS_PER_SEC))
}

pub fn make_timestamptz_from_parts(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: f64,
    zone: &str,
    config: &DateTimeConfig,
) -> Result<TimestampTzADT, DateTimeParseError> {
    if year == 0 || !second.is_finite() {
        return Err(DateTimeParseError::FieldOutOfRange);
    }
    let astronomical_year = if year < 0 { year + 1 } else { year };
    let whole_second = second.trunc();
    if !(0.0..60.0).contains(&whole_second) {
        return Err(DateTimeParseError::FieldOutOfRange);
    }
    let micros = ((second - whole_second) * USECS_PER_SEC as f64).round() as i64;
    let mut days =
        days_from_ymd(astronomical_year, month, day).ok_or(DateTimeParseError::FieldOutOfRange)?;
    let mut time_usecs = time_usecs_from_hms(hour, minute, whole_second as u32, micros)
        .ok_or(DateTimeParseError::FieldOutOfRange)?;
    if time_usecs == USECS_PER_DAY {
        days = days
            .checked_add(1)
            .ok_or(DateTimeParseError::FieldOutOfRange)?;
        time_usecs = 0;
    }
    let local_usecs = local_timestamp_usecs_from_parts(days, time_usecs)?;
    let spec = parse_timezone_spec(zone)
        .map_err(|err| match err {
            DateTimeParseError::UnknownTimeZone(_) => {
                DateTimeParseError::UnknownTimeZone(zone.to_string())
            }
            other => other,
        })?
        .ok_or_else(|| DateTimeParseError::UnknownTimeZone(zone.to_string()))?;
    let offset = timezone_spec_offset(Some(spec), config, local_usecs)?;
    if offset.abs() > 15 * 3600 + 59 * 60 + 59 {
        return Err(DateTimeParseError::FieldOutOfRange);
    }
    timestamp_usecs_from_parts(days, time_usecs, offset).map(TimestampTzADT)
}

fn local_timestamp_usecs_from_parts(days: i32, time_usecs: i64) -> Result<i64, DateTimeParseError> {
    let usecs = days as i128 * USECS_PER_DAY as i128 + time_usecs as i128;
    i64::try_from(usecs).map_err(|_| DateTimeParseError::TimestampOutOfRange)
}

fn timestamp_usecs_from_parts(
    days: i32,
    time_usecs: i64,
    offset_seconds: i32,
) -> Result<i64, DateTimeParseError> {
    let usecs = days as i128 * USECS_PER_DAY as i128 + time_usecs as i128
        - offset_seconds as i128 * USECS_PER_SEC as i128;
    if usecs < MIN_TIMESTAMP_USECS as i128 || usecs >= END_TIMESTAMP_USECS as i128 {
        return Err(DateTimeParseError::TimestampOutOfRange);
    }
    Ok(usecs as i64)
}

pub fn format_timestamp_text(value: TimestampADT, _config: &DateTimeConfig) -> String {
    if value.0 == crate::compat::include::nodes::datetime::TIMESTAMP_NOEND {
        return "infinity".into();
    }
    if value.0 == crate::compat::include::nodes::datetime::TIMESTAMP_NOBEGIN {
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
        DateStyleFormat::Sql => {
            let (_, bc) = format_timestamp_year_parts(days);
            let rendered = format!(
                "{} {}",
                format_sql_style_timestamp_date(days, _config),
                format_time_usecs(time_usecs)
            );
            if bc {
                format!("{rendered} BC")
            } else {
                rendered
            }
        }
        _ => {
            let (_, bc) = format_timestamp_year_parts(days);
            let rendered = format!(
                "{} {}",
                format_timestamp_date(days, _config, false),
                format_time_usecs(time_usecs)
            );
            if bc {
                format!("{rendered} BC")
            } else {
                rendered
            }
        }
    }
}

pub fn format_timestamptz_text(value: TimestampTzADT, config: &DateTimeConfig) -> String {
    if value.0 == crate::compat::include::nodes::datetime::TIMESTAMP_NOEND {
        return "infinity".into();
    }
    if value.0 == crate::compat::include::nodes::datetime::TIMESTAMP_NOBEGIN {
        return "-infinity".into();
    }
    let offset_seconds = timezone_offset_seconds_at_utc(config, value.0);
    let adjusted = value.0 + offset_seconds as i64 * USECS_PER_SEC;
    let (days, time_usecs) = timestamp_parts_from_usecs(adjusted);
    match config.date_style_format {
        DateStyleFormat::Postgres => {
            let zone = timezone_abbrev_for_output(config, value.0, offset_seconds)
                .unwrap_or_else(|| format_offset(offset_seconds));
            let (year, bc) = format_timestamp_year_parts(days);
            let rendered = format!(
                "{} {} {} {}",
                format_timestamp_date(days, config, true),
                format_time_usecs(time_usecs),
                year,
                zone,
            );
            if bc {
                format!("{rendered} BC")
            } else {
                rendered
            }
        }
        DateStyleFormat::German => {
            let zone = timezone_abbrev_for_output(config, value.0, offset_seconds)
                .unwrap_or_else(|| format_offset(offset_seconds));
            let rendered = format!(
                "{} {} {}",
                format_timestamp_date(days, config, false),
                format_time_usecs(time_usecs),
                zone
            );
            let (_, bc) = format_timestamp_year_parts(days);
            if bc {
                format!("{rendered} BC")
            } else {
                rendered
            }
        }
        DateStyleFormat::Sql => {
            let zone = timezone_abbrev_for_output(config, value.0, offset_seconds)
                .unwrap_or_else(|| format_offset(offset_seconds));
            let rendered = format!(
                "{} {} {}",
                format_sql_style_timestamp_date(days, config),
                format_time_usecs(time_usecs),
                zone
            );
            let (_, bc) = format_timestamp_year_parts(days);
            if bc {
                format!("{rendered} BC")
            } else {
                rendered
            }
        }
        _ => {
            let rendered = format!(
                "{} {}{}",
                format_timestamp_date(days, config, false),
                format_time_usecs(time_usecs),
                format_offset(offset_seconds)
            );
            let (_, bc) = format_timestamp_year_parts(days);
            if bc {
                format!("{rendered} BC")
            } else {
                rendered
            }
        }
    }
}

#[cfg(test)]
mod format_tests {
    use super::*;
    use crate::compat::backend::utils::misc::guc_datetime::{DateOrder, DateStyleFormat};

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
    fn formats_timestamp_in_postgres_dmy_order() {
        let config = DateTimeConfig {
            date_style_format: DateStyleFormat::Postgres,
            date_order: DateOrder::Dmy,
            ..DateTimeConfig::default()
        };
        let ts = TimestampADT(i64::from(days_from_ymd(1996, 12, 27).unwrap()) * USECS_PER_DAY);
        assert_eq!(
            format_timestamp_text(ts, &config),
            "Fri 27 Dec 00:00:00 1996"
        );
    }

    #[test]
    fn formats_bc_timestamp_in_iso_and_sql_styles() {
        let ts = TimestampADT(
            i64::from(days_from_ymd(-96, 2, 16).unwrap()) * USECS_PER_DAY
                + (17 * 3600 + 32 * 60 + 1) * USECS_PER_SEC,
        );
        let iso = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Mdy,
            ..DateTimeConfig::default()
        };
        assert_eq!(format_timestamp_text(ts, &iso), "0097-02-16 17:32:01 BC");
        let sql = DateTimeConfig {
            date_style_format: DateStyleFormat::Sql,
            date_order: DateOrder::Mdy,
            ..DateTimeConfig::default()
        };
        assert_eq!(format_timestamp_text(ts, &sql), "02/16/0097 17:32:01 BC");
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

    #[test]
    fn formats_timestamptz_dst_abbreviation_in_postgres_style() {
        let config = DateTimeConfig {
            date_style_format: DateStyleFormat::Postgres,
            date_order: DateOrder::Mdy,
            time_zone: "America/Los_Angeles".into(),
            ..DateTimeConfig::default()
        };
        let ts =
            parse_timestamptz_text("Wed Jul 11 10:51:14 America/New_York 2001", &config).unwrap();
        assert_eq!(
            format_timestamptz_text(ts, &config),
            "Wed Jul 11 07:51:14 2001 PDT"
        );
    }

    #[test]
    fn formats_timestamptz_far_future_dst_abbreviation_in_postgres_style() {
        let config = DateTimeConfig {
            date_style_format: DateStyleFormat::Postgres,
            date_order: DateOrder::Mdy,
            time_zone: "America/Los_Angeles".into(),
            ..DateTimeConfig::default()
        };
        let ts = parse_timestamptz_text("205000-07-10 17:32:01 Europe/Helsinki", &config).unwrap();
        assert_eq!(
            format_timestamptz_text(ts, &config),
            "Thu Jul 10 07:32:01 205000 PDT"
        );
    }

    #[test]
    fn formats_timestamptz_legacy_la_dst_rule_in_postgres_style() {
        let config = DateTimeConfig {
            date_style_format: DateStyleFormat::Postgres,
            date_order: DateOrder::Mdy,
            time_zone: "America/Los_Angeles".into(),
            ..DateTimeConfig::default()
        };
        let ts = parse_timestamptz_text("2000-03-15 08:14:01 GMT+8", &config).unwrap();
        assert_eq!(
            format_timestamptz_text(ts, &config),
            "Wed Mar 15 08:14:01 2000 PST"
        );
    }

    #[test]
    fn parses_timestamptz_epoch_as_utc_instant() {
        let config = DateTimeConfig {
            date_style_format: DateStyleFormat::Postgres,
            date_order: DateOrder::Mdy,
            time_zone: "America/Los_Angeles".into(),
            ..DateTimeConfig::default()
        };
        let ts = parse_timestamptz_text("epoch", &config).unwrap();
        assert_eq!(
            format_timestamptz_text(ts, &config),
            "Wed Dec 31 16:00:00 1969 PST"
        );
    }

    #[test]
    fn parses_timestamptz_date_with_inline_zone() {
        let config = DateTimeConfig {
            time_zone: "America/Los_Angeles".into(),
            ..DateTimeConfig::default()
        };
        assert_eq!(
            parse_timestamptz_text("2001-01-01+11", &config).unwrap(),
            parse_timestamptz_text("2001-01-01 00:00 +11", &config).unwrap()
        );
    }

    #[test]
    fn parses_special_dates_with_explicit_timezones() {
        let transaction_timestamp_usecs = i64::from(days_from_ymd(2024, 1, 15).unwrap())
            * USECS_PER_DAY
            + 12 * 60 * 60 * USECS_PER_SEC;
        let config = DateTimeConfig {
            time_zone: "America/Los_Angeles".into(),
            transaction_timestamp_usecs: Some(transaction_timestamp_usecs),
            ..DateTimeConfig::default()
        };
        let tomorrow_days = today_pg_days(&config) + 1;
        assert_eq!(
            parse_timestamptz_text("tomorrow EST", &config).unwrap(),
            TimestampTzADT(timestamp_usecs_from_parts(tomorrow_days, 0, -5 * 3600).unwrap())
        );
        assert_eq!(
            parse_timestamptz_text("tomorrow zulu", &config).unwrap(),
            TimestampTzADT(timestamp_usecs_from_parts(tomorrow_days, 0, 0).unwrap())
        );
    }

    #[test]
    fn formats_timestamptz_bc_zone_before_era_in_postgres_style() {
        let config = DateTimeConfig {
            date_style_format: DateStyleFormat::Postgres,
            date_order: DateOrder::Mdy,
            time_zone: "America/Los_Angeles".into(),
            ..DateTimeConfig::default()
        };
        let ts = parse_timestamptz_text("Feb 16 17:32:01 0097 BC", &config).unwrap();
        assert_eq!(
            format_timestamptz_text(ts, &config),
            "Tue Feb 16 17:32:01 0097 LMT BC"
        );
    }

    #[test]
    fn formats_timestamptz_historic_timezone_abbreviations() {
        let london = DateTimeConfig {
            date_style_format: DateStyleFormat::Postgres,
            date_order: DateOrder::Mdy,
            time_zone: "Europe/London".into(),
            ..DateTimeConfig::default()
        };
        let lmt = parse_timestamptz_text("Jan 01 00:00:00 2024 LMT", &london).unwrap();
        assert_eq!(
            format_timestamptz_text(lmt, &london),
            "Mon Jan 01 00:01:15 2024 GMT"
        );

        let utc = DateTimeConfig {
            date_style_format: DateStyleFormat::Postgres,
            date_order: DateOrder::Mdy,
            time_zone: "UTC".into(),
            ..DateTimeConfig::default()
        };
        let mmt = parse_timestamptz_text("1912-01-01 00:00 MMT", &utc).unwrap();
        assert_eq!(
            format_timestamptz_text(mmt, &utc),
            "Sun Dec 31 17:30:00 1911 UTC"
        );

        let montevideo = DateTimeConfig {
            date_style_format: DateStyleFormat::Postgres,
            date_order: DateOrder::Mdy,
            time_zone: "America/Montevideo".into(),
            ..DateTimeConfig::default()
        };
        let mmt = parse_timestamptz_text("1912-01-01 00:00 MMT", &montevideo).unwrap();
        assert_eq!(
            format_timestamptz_text(mmt, &montevideo),
            "Mon Jan 01 00:00:00 1912 MMT"
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
            "Jan 1, 4713 BC",
            "Feb 16 17:32:01 0097 BC",
            "20011227 040506.789+08",
            "20011227T040506.789-08",
            "J2452271+08",
            "J2452271T040506.789-08",
            "2001-12-27 04:05:06.789 MET DST",
            "2001-12-27 allballs",
        ] {
            assert!(parse_timestamp_text(input, &config).is_ok(), "{input}");
        }
    }

    #[test]
    fn parses_horology_timestamptz_input_forms() {
        let config = DateTimeConfig::default();
        assert_eq!(
            parse_timestamptz_text("20011227 040506.789+08", &config),
            parse_timestamptz_text("2001-12-27 04:05:06.789+08", &config)
        );
        assert_eq!(
            parse_timestamptz_text("20011227T040506.789-08", &config),
            parse_timestamptz_text("2001-12-27 04:05:06.789-08", &config)
        );
        assert_eq!(
            parse_timestamptz_text("J2452271T040506.789-08", &config),
            parse_timestamptz_text("2001-12-27 04:05:06.789-08", &config)
        );
        assert_eq!(
            parse_timestamptz_text("2001-12-27 04:05:06.789 MET DST", &config),
            parse_timestamptz_text("2001-12-27 04:05:06.789+02", &config)
        );
        assert_eq!(
            parse_timestamptz_text("2001-12-27 allballs", &config),
            parse_timestamptz_text("2001-12-27 00:00:00+00", &config)
        );
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
