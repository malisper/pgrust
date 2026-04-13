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
}

impl Default for DateTimeConfig {
    fn default() -> Self {
        Self {
            date_style_format: DateStyleFormat::Iso,
            date_order: DateOrder::Mdy,
            time_zone: "UTC".into(),
        }
    }
}

pub fn default_datestyle() -> &'static str {
    "ISO, MDY"
}

pub fn default_timezone() -> &'static str {
    "UTC"
}

pub fn parse_datestyle(value: &str) -> Option<(DateStyleFormat, DateOrder)> {
    let mut format = None;
    let mut order = None;
    for part in value.split(',').map(str::trim).filter(|part| !part.is_empty()) {
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
        format.unwrap_or(DateStyleFormat::Iso),
        order.unwrap_or(DateOrder::Mdy),
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
        None
    } else {
        Some(trimmed.to_string())
    }
}
