pub fn backend_notice_visible_for_client_min_messages(
    severity: &str,
    client_min_messages: Option<&str>,
) -> bool {
    let Some(min_messages) = client_min_messages else {
        return true;
    };
    backend_notice_severity_rank(severity) >= client_min_messages_rank(min_messages)
}

pub fn backend_notice_severity_rank(severity: &str) -> u8 {
    match severity.to_ascii_lowercase().as_str() {
        "debug" | "debug1" | "debug2" | "debug3" | "debug4" | "debug5" => 10,
        "info" | "log" => 20,
        "notice" => 30,
        "warning" => 40,
        _ => 50,
    }
}

pub fn client_min_messages_rank(value: &str) -> u8 {
    match value.trim().to_ascii_lowercase().as_str() {
        "debug" | "debug1" | "debug2" | "debug3" | "debug4" | "debug5" => 10,
        "info" | "log" => 20,
        "notice" => 30,
        "warning" => 40,
        "error" => 50,
        _ => 30,
    }
}

pub fn suppress_duplicate_alter_missing_relation_notice(sql: &str, message: &str) -> bool {
    let compact = sql
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    compact.starts_with("alter foreign table if exists ")
        && compact.contains(',')
        && message.starts_with("relation \"")
        && message.ends_with("\" does not exist, skipping")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_min_messages_filters_by_rank() {
        assert!(backend_notice_visible_for_client_min_messages(
            "WARNING",
            Some("notice")
        ));
        assert!(!backend_notice_visible_for_client_min_messages(
            "NOTICE",
            Some("warning")
        ));
        assert!(backend_notice_visible_for_client_min_messages(
            "NOTICE", None
        ));
    }

    #[test]
    fn duplicate_alter_missing_relation_notice_is_detected() {
        assert!(suppress_duplicate_alter_missing_relation_notice(
            "ALTER FOREIGN TABLE IF EXISTS missing, other ALTER COLUMN x TYPE int",
            "relation \"missing\" does not exist, skipping"
        ));
    }
}
