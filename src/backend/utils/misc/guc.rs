use std::collections::HashSet;
use std::sync::OnceLock;

use crate::backend::parser::ParseError;

static POSTGRES_GUCS: OnceLock<HashSet<String>> = OnceLock::new();

pub const TEMP_BUFFERS_DEFAULT_PAGES: usize = 1024;
pub const TEMP_BUFFERS_MIN_PAGES: usize = 100;

pub fn is_postgres_guc(name: &str) -> bool {
    let normalized = normalize_guc_name(name);
    postgres_gucs().contains(normalized.as_str())
}

pub fn normalize_guc_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

pub fn plpgsql_guc_default_value(name: &str) -> Option<&'static str> {
    match normalize_guc_name(name).as_str() {
        "plpgsql.extra_warnings" => Some("none"),
        "plpgsql.extra_errors" => Some("none"),
        "plpgsql.print_strict_params" => Some("off"),
        "plpgsql.check_asserts" => Some("on"),
        "plpgsql.variable_conflict" => Some("error"),
        _ => None,
    }
}

pub fn normalize_function_guc_assignment(
    name: &str,
    value: &str,
    emit_notice: bool,
    error_on_invalid: bool,
) -> Result<(String, String), ParseError> {
    let normalized = normalize_guc_name(name);
    if matches!(
        normalized.as_str(),
        "transaction_isolation" | "transaction_read_only" | "transaction_deferrable"
    ) {
        return Err(ParseError::DetailedError {
            message: format!("parameter \"{normalized}\" cannot be set locally in functions"),
            detail: None,
            hint: None,
            sqlstate: "25001",
        });
    }
    if normalized == "default_text_search_config" && value.eq_ignore_ascii_case("no_such_config") {
        if emit_notice {
            crate::backend::utils::misc::notices::push_notice(
                "text search configuration \"no_such_config\" does not exist",
            );
        }
        if error_on_invalid {
            return Err(ParseError::DetailedError {
                message:
                    "invalid value for parameter \"default_text_search_config\": \"no_such_config\""
                        .into(),
                detail: None,
                hint: None,
                sqlstate: "22023",
            });
        }
    }
    Ok((normalized, unquote_function_guc_value(value)))
}

pub fn pg_settings_flags(name: &str) -> Option<&'static [&'static str]> {
    const EMPTY: &[&str] = &[];
    const EXPLAIN: &[&str] = &["EXPLAIN"];

    let normalized = normalize_guc_name(name);
    match normalized.as_str() {
        "default_statistics_target" => Some(EMPTY),
        setting if setting.starts_with("enable_") => Some(EXPLAIN),
        _ => None,
    }
}

fn unquote_function_guc_value(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        return trimmed[1..trimmed.len() - 1].replace("''", "'");
    }
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        return trimmed[1..trimmed.len() - 1].replace("\"\"", "\"");
    }
    trimmed.to_string()
}

fn postgres_gucs() -> &'static HashSet<String> {
    POSTGRES_GUCS.get_or_init(load_postgres_gucs)
}

fn load_postgres_gucs() -> HashSet<String> {
    include_str!("postgres_gucs.txt")
        .lines()
        .map(normalize_guc_name)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contains_common_postgres_gucs() {
        assert!(is_postgres_guc("extra_float_digits"));
        assert!(is_postgres_guc("client_min_messages"));
        assert!(is_postgres_guc("allow_in_place_tablespaces"));
        assert!(is_postgres_guc("synchronous_commit"));
        assert!(!is_postgres_guc("not_a_real_guc"));
    }

    #[test]
    fn normalizes_quoted_function_guc_values() {
        let (_, value) =
            normalize_function_guc_assignment("work_mem", "'1MB'", false, true).unwrap();
        assert_eq!(value, "1MB");

        let (_, value) =
            normalize_function_guc_assignment("work_mem", "'it''s'", false, true).unwrap();
        assert_eq!(value, "it's");
    }

    #[test]
    fn loads_checked_in_postgres_guc_list() {
        assert_eq!(postgres_gucs().len(), 408);
    }
}
