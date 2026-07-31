//! \? output: psql 18's slashUsage() text, captured verbatim from PGDG
//! psql 18.4 (src/slash_usage.txt). Commands this port does not implement
//! still appear here exactly as psql lists them — invoking one produces the
//! clean "not supported" message instead of a half implementation.

const SLASH_USAGE_RAW: &str = include_str!("slash_usage.txt");

/// The \c line embeds the current database name (slashUsage's currdb arm).
/// The capture was taken while connected to "postgres"; substitute the live
/// database at print time.
pub fn slash_usage(current_db: Option<&str>) -> String {
    let needle = "connect to new database (currently \"postgres\")";
    let replacement = match current_db {
        Some(db) => format!("connect to new database (currently \"{db}\")"),
        None => "connect to new database (currently no connection)".to_string(),
    };
    SLASH_USAGE_RAW.replacen(needle, &replacement, 1)
}
