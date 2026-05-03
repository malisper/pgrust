#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateStyleFormat {
    Iso,
    Sql,
    Postgres,
    German,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateOrder {
    Mdy,
    Dmy,
    Ymd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntervalStyle {
    Postgres,
    PostgresVerbose,
    SqlStandard,
    Iso8601,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DateTimeConfig {
    pub date_style_format: DateStyleFormat,
    pub date_order: DateOrder,
    pub interval_style: IntervalStyle,
    pub time_zone: String,
    pub time_zone_display: Option<String>,
    pub transaction_timestamp_usecs: Option<i64>,
    pub statement_timestamp_usecs: Option<i64>,
    pub max_stack_depth_kb: u32,
    pub xml: XmlConfig,
}

impl Default for DateTimeConfig {
    fn default() -> Self {
        let (date_style_format, date_order) =
            parse_datestyle(&default_datestyle()).unwrap_or((DateStyleFormat::Iso, DateOrder::Mdy));
        Self {
            date_style_format,
            date_order,
            interval_style: IntervalStyle::Postgres,
            time_zone: default_timezone(),
            time_zone_display: None,
            transaction_timestamp_usecs: None,
            statement_timestamp_usecs: None,
            max_stack_depth_kb:
                crate::compat::backend::utils::misc::stack_depth::effective_default_max_stack_depth_kb(),
            xml: XmlConfig::default(),
        }
    }
}

pub fn default_datestyle() -> String {
    std::env::var("PGDATESTYLE").unwrap_or_else(|_| "ISO, MDY".into())
}

pub fn default_datetime_config() -> DateTimeConfig {
    let mut config = DateTimeConfig::default();
    if let Some((date_style_format, date_order)) = parse_datestyle(&default_datestyle()) {
        config.date_style_format = date_style_format;
        config.date_order = date_order;
    }
    if let Some(time_zone) = parse_timezone(&default_timezone()) {
        config.time_zone = time_zone;
        config.time_zone_display = None;
    }
    config
}

pub fn default_timezone() -> String {
    std::env::var("PGTZ").unwrap_or_else(|_| "UTC".into())
}

pub fn default_intervalstyle() -> &'static str {
    "postgres"
}

pub fn parse_datestyle(value: &str) -> Option<(DateStyleFormat, DateOrder)> {
    parse_datestyle_with_fallback(value, DateStyleFormat::Iso, DateOrder::Mdy)
}

pub fn parse_datestyle_with_fallback(
    value: &str,
    fallback_format: DateStyleFormat,
    fallback_order: DateOrder,
) -> Option<(DateStyleFormat, DateOrder)> {
    let mut format = None;
    let mut order = None;
    for part in value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        match part.to_ascii_lowercase().as_str() {
            "iso" => format = Some(DateStyleFormat::Iso),
            "sql" => format = Some(DateStyleFormat::Sql),
            "postgres" | "postgresql" => format = Some(DateStyleFormat::Postgres),
            "german" => format = Some(DateStyleFormat::German),
            "mdy" | "us" | "noneuro" => order = Some(DateOrder::Mdy),
            "dmy" | "euro" | "european" => order = Some(DateOrder::Dmy),
            "ymd" => order = Some(DateOrder::Ymd),
            _ => return None,
        }
    }
    let format = format.unwrap_or(fallback_format);
    let order = order.unwrap_or(if matches!(format, DateStyleFormat::German) {
        DateOrder::Dmy
    } else {
        fallback_order
    });
    Some((format, order))
}

pub fn format_datestyle(config: &DateTimeConfig) -> String {
    let format = match config.date_style_format {
        DateStyleFormat::Iso => "ISO",
        DateStyleFormat::Sql => "SQL",
        DateStyleFormat::Postgres => "Postgres",
        DateStyleFormat::German => "German",
    };
    let order = match config.date_order {
        DateOrder::Mdy => "MDY",
        DateOrder::Dmy => "DMY",
        DateOrder::Ymd => "YMD",
    };
    format!("{format}, {order}")
}

pub fn parse_intervalstyle(value: &str) -> Option<IntervalStyle> {
    match value.trim().to_ascii_lowercase().as_str() {
        "postgres" => Some(IntervalStyle::Postgres),
        "postgres_verbose" => Some(IntervalStyle::PostgresVerbose),
        "sql_standard" => Some(IntervalStyle::SqlStandard),
        "iso_8601" => Some(IntervalStyle::Iso8601),
        _ => None,
    }
}

pub fn format_intervalstyle(style: IntervalStyle) -> &'static str {
    match style {
        IntervalStyle::Postgres => "postgres",
        IntervalStyle::PostgresVerbose => "postgres_verbose",
        IntervalStyle::SqlStandard => "sql_standard",
        IntervalStyle::Iso8601 => "iso_8601",
    }
}

pub fn parse_timezone(value: &str) -> Option<String> {
    parse_timezone_with_display(value).map(|(time_zone, _)| time_zone)
}

pub fn parse_timezone_with_display(value: &str) -> Option<(String, Option<String>)> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    // :HACK: PostgreSQL accepts SET TIME ZONE '-08' as an ISO-style fixed
    // offset in its regression tests, while other signed string values use the
    // POSIX sign convention handled below.
    if trimmed.len() == 3
        && matches!(trimmed.as_bytes().first(), Some(b'+') | Some(b'-'))
        && trimmed.as_bytes().get(1) == Some(&b'0')
        && trimmed.as_bytes().get(2).is_some_and(u8::is_ascii_digit)
        && let Some(offset) = parse_offset_seconds(trimmed)
    {
        return Some((format_offset(offset), None));
    }

    if let Ok(hours) = trimmed.parse::<f64>() {
        if !hours.is_finite() {
            return None;
        }
        let offset = (hours * 3600.0).round() as i32;
        let normalized = format_offset(offset);
        // PostgreSQL shows negative fractional numeric time zones as the
        // generated POSIX zone name while still using the ISO-style offset for
        // arithmetic.
        let display = (hours < 0.0 && hours.fract() != 0.0)
            .then(|| format!("<{normalized}>{}", format_offset(-offset)));
        return Some((normalized, display));
    }

    if matches!(trimmed.as_bytes().first(), Some(b'+') | Some(b'-')) {
        if let Some(offset) = parse_offset_seconds(trimmed) {
            return Some((format_offset(-offset), None));
        }
    }

    if trimmed.contains(':') {
        if let Some(offset) = parse_offset_seconds(&format!("+{trimmed}")) {
            return Some((format_offset(-offset), None));
        }
    }

    Some((trimmed.to_string(), None))
}

pub fn format_timezone(config: &DateTimeConfig) -> &str {
    config
        .time_zone_display
        .as_deref()
        .unwrap_or(config.time_zone.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_numeric_timezones_as_fixed_offsets() {
        assert_eq!(parse_timezone("10.5"), Some("+10:30".into()));
        assert_eq!(parse_timezone("-8"), Some("-08".into()));
        assert_eq!(parse_timezone("-08"), Some("-08".into()));
        assert_eq!(parse_timezone("+9.75"), Some("+09:45".into()));
        assert_eq!(parse_timezone("+02:00"), Some("-02".into()));
        assert_eq!(parse_timezone("04:30"), Some("-04:30".into()));
    }

    #[test]
    fn preserves_postgres_display_for_negative_fractional_timezone() {
        let (time_zone, display) = parse_timezone_with_display("-1.5").unwrap();
        assert_eq!(time_zone, "-01:30");
        assert_eq!(display.as_deref(), Some("<-01:30>+01:30"));
        let config = DateTimeConfig {
            time_zone,
            time_zone_display: display,
            ..DateTimeConfig::default()
        };
        assert_eq!(format_timezone(&config), "<-01:30>+01:30");
    }

    #[test]
    fn german_datestyle_defaults_to_dmy_order() {
        assert_eq!(
            parse_datestyle("German"),
            Some((DateStyleFormat::German, DateOrder::Dmy))
        );
        assert_eq!(
            parse_datestyle_with_fallback("German, MDY", DateStyleFormat::Iso, DateOrder::Mdy),
            Some((DateStyleFormat::German, DateOrder::Mdy))
        );
    }

    #[test]
    fn european_datestyle_is_dmy_alias() {
        assert_eq!(
            parse_datestyle("European, Postgres"),
            Some((DateStyleFormat::Postgres, DateOrder::Dmy))
        );
        assert_eq!(
            parse_datestyle("European, ISO"),
            Some((DateStyleFormat::Iso, DateOrder::Dmy))
        );
    }
}
use crate::compat::backend::utils::misc::guc_xml::XmlConfig;
use crate::compat::backend::utils::time::datetime::{format_offset, parse_offset_seconds};
