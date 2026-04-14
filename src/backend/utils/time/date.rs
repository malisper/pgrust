use crate::backend::utils::misc::guc_datetime::{DateOrder, DateStyleFormat, DateTimeConfig};
use crate::backend::utils::time::datetime::{
    DateTimeKeyword, days_from_ymd, format_offset, format_time_usecs, parse_date_parts,
    parse_keyword, parse_offset_seconds, parse_time_components, split_time_and_offset,
    time_usecs_from_hms, timezone_offset_seconds, today_pg_days, ymd_from_days,
};
use crate::include::nodes::datetime::{DATEVAL_NOBEGIN, DATEVAL_NOEND, DateADT, TimeADT, TimeTzADT};

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
        };
        assert_eq!(format_date_text(value, &iso), "1999-01-08");

        let sql = DateTimeConfig {
            date_style_format: DateStyleFormat::Sql,
            date_order: DateOrder::Dmy,
            time_zone: "UTC".into(),
        };
        assert_eq!(format_date_text(value, &sql), "08-01-1999");

        let german = DateTimeConfig {
            date_style_format: DateStyleFormat::German,
            date_order: DateOrder::Mdy,
            time_zone: "UTC".into(),
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
        };
        assert_eq!(format_date_text(value, &config), "0099-01-08 BC");
    }
}
