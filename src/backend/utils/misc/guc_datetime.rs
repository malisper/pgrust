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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DateTimeConfig {
    pub date_style_format: DateStyleFormat,
    pub date_order: DateOrder,
    pub time_zone: String,
    pub max_stack_depth_kb: u32,
    pub xml: XmlConfig,
}

impl Default for DateTimeConfig {
    fn default() -> Self {
        Self {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Mdy,
            time_zone: "UTC".into(),
            max_stack_depth_kb:
                crate::backend::utils::misc::stack_depth::effective_default_max_stack_depth_kb(),
            xml: XmlConfig::default(),
        }
    }
}

pub fn default_datestyle() -> &'static str {
    "ISO, MDY"
}

pub fn default_datetime_config() -> DateTimeConfig {
    let mut config = DateTimeConfig::default();
    if let Ok(value) = std::env::var("PGDATESTYLE")
        && let Some((date_style_format, date_order)) = parse_datestyle(&value)
    {
        config.date_style_format = date_style_format;
        config.date_order = date_order;
    }
    if let Ok(value) = std::env::var("PGTZ")
        && let Some(time_zone) = parse_timezone(&value)
    {
        config.time_zone = time_zone;
    }
    config
}

pub fn default_timezone() -> &'static str {
    "UTC"
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
            "dmy" | "euro" => order = Some(DateOrder::Dmy),
            "ymd" => order = Some(DateOrder::Ymd),
            _ => return None,
        }
    }
    Some((
        format.unwrap_or(fallback_format),
        order.unwrap_or(fallback_order),
    ))
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

pub fn parse_timezone(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Ok(hours) = trimmed.parse::<f64>() {
        if !hours.is_finite() {
            return None;
        }
        return Some(format_offset((hours * 3600.0).round() as i32));
    }

    Some(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_numeric_timezones_as_fixed_offsets() {
        assert_eq!(parse_timezone("10.5"), Some("+10:30".into()));
        assert_eq!(parse_timezone("-8"), Some("-08".into()));
        assert_eq!(parse_timezone("+9.75"), Some("+09:45".into()));
    }
}
use crate::backend::utils::misc::guc_xml::XmlConfig;
use crate::backend::utils::time::datetime::format_offset;
