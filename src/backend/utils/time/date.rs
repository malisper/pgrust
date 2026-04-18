use crate::backend::utils::misc::guc_datetime::{DateOrder, DateStyleFormat, DateTimeConfig};
use crate::backend::utils::time::datetime::{
    days_from_ymd, format_offset, format_time_usecs, month_number, parse_date_token_with_config,
    parse_keyword, parse_offset_seconds, parse_time_components, split_time_and_offset,
    time_usecs_from_hms, timezone_offset_seconds, today_pg_days, ymd_from_days, DateTimeKeyword,
    DateTimeParseError,
};
use crate::include::nodes::datetime::{
    DateADT, TimeADT, TimeTzADT, DATEVAL_NOBEGIN, DATEVAL_NOEND, POSTGRES_EPOCH_JDATE,
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
    let Some(days) = days_from_ymd(year, month, day) else {
        return Err(DateParseError::FieldOutOfRange { datestyle_hint });
    };
    let (min, max) = supported_date_bounds();
    if days < min || days > max {
        return Err(DateParseError::OutOfRange);
    }
    Ok(DateADT(days))
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

    let a = first.parse::<i32>().map_err(|_| DateParseError::Invalid)?;
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
                            Err(DateParseError::Invalid)
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
        DateTimeParseError::UnknownTimeZone(_) => DateParseError::Invalid,
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
        };
        assert_eq!(format_date_text(value, &iso), "1999-01-08");

        let sql = DateTimeConfig {
            date_style_format: DateStyleFormat::Sql,
            date_order: DateOrder::Dmy,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
        };
        assert_eq!(format_date_text(value, &sql), "08-01-1999");

        let german = DateTimeConfig {
            date_style_format: DateStyleFormat::German,
            date_order: DateOrder::Mdy,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
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
        };
        let dmy = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Dmy,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
        };
        let mdy = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Mdy,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
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
        };
        let dmy = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Dmy,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
        };
        let mdy = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Mdy,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
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
        };
        let dmy = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Dmy,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
        };
        let mdy = DateTimeConfig {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Mdy,
            time_zone: "UTC".into(),
            max_stack_depth_kb: 100,
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
        assert_eq!(parse_date_text("99-Jan-08", &mdy), invalid);

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
}
