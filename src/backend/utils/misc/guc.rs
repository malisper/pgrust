use std::collections::HashSet;
use std::sync::OnceLock;

static POSTGRES_GUCS: OnceLock<HashSet<String>> = OnceLock::new();

pub fn is_postgres_guc(name: &str) -> bool {
    let normalized = normalize_guc_name(name);
    postgres_gucs().contains(normalized.as_str())
}

pub fn normalize_guc_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
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
    fn loads_checked_in_postgres_guc_list() {
        assert_eq!(postgres_gucs().len(), 403);
    }
}
