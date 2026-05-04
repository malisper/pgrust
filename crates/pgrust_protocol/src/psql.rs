use std::borrow::Cow;

pub fn extract_identifier_pattern<'a>(sql: &'a str, identifier: &str) -> Option<&'a str> {
    let lower = sql.to_ascii_lowercase();
    let identifier = identifier.to_ascii_lowercase();
    let mut search_start = 0usize;
    while let Some(relative) = lower[search_start..].find(&identifier) {
        let start = search_start + relative;
        let end = start + identifier.len();
        let before = start
            .checked_sub(1)
            .and_then(|idx| lower.as_bytes().get(idx));
        let after = lower.as_bytes().get(end);
        if !before.is_some_and(|byte| is_sql_identifier_byte(*byte))
            && !after.is_some_and(|byte| is_sql_identifier_byte(*byte))
            && let Some(pattern) = extract_operator_pattern_at(sql, &lower, end)
        {
            return Some(pattern);
        }
        search_start = end;
    }
    None
}

pub fn is_permissions_query(lower: &str) -> bool {
    lower.starts_with("select n.nspname")
        && lower.contains("c.relname")
        && lower.contains("case c.relkind")
        && lower.contains("c.relacl")
        && lower.contains("from pg_catalog.pg_class c")
        && lower.contains("from pg_catalog.pg_policy pol")
        && lower.contains(" as \"policies\"")
}

pub fn permissions_relkind_name(relkind: char) -> &'static str {
    match relkind {
        'r' => "table",
        'v' => "view",
        'm' => "materialized view",
        'S' => "sequence",
        'f' => "foreign table",
        'p' => "partitioned table",
        _ => "",
    }
}

pub fn is_roles_query(lower: &str) -> bool {
    lower.starts_with("select r.rolname, r.rolsuper")
        && lower.contains("from pg_catalog.pg_roles r")
}

pub fn is_role_settings_query(lower: &str) -> bool {
    lower.starts_with("select rolname as \"role\", datname as \"database\"")
        && lower.contains("from pg_catalog.pg_db_role_setting s")
}

pub fn is_object_description_query(lower: &str) -> bool {
    lower.starts_with("select distinct tt.nspname")
        && lower.contains("join pg_catalog.pg_description d")
        && lower.contains("tt.object")
        && lower.contains("d.description")
}

pub fn is_partitioned_relations_query(lower: &str) -> bool {
    let Some(owner_pos) = lower.find("pg_catalog.pg_get_userbyid(c.relowner) as \"owner\"") else {
        return false;
    };
    let type_pos = lower.find("case c.relkind");
    lower.starts_with("select n.nspname as \"schema\"")
        && lower.contains("c.relname as \"name\"")
        && lower.contains("from pg_catalog.pg_class c")
        && lower.contains("where c.relkind in")
        && lower.contains("c.relkind in ('")
        && type_pos.is_none_or(|pos| owner_pos < pos)
}

pub fn is_list_tables_query(lower: &str) -> bool {
    lower.starts_with("select n.nspname")
        && lower.contains("c.relname")
        && lower.contains("case c.relkind")
        && lower.contains("pg_catalog.pg_get_userbyid(c.relowner)")
        && lower.contains("from pg_catalog.pg_class c")
        && lower.contains("where c.relkind in")
}

pub fn list_tables_query_includes_relkind(lower_sql: &str, relkind: char) -> bool {
    match relkind {
        'r' => lower_sql.contains("'r'"),
        'p' => lower_sql.contains("'p'"),
        'v' => lower_sql.contains("'v'"),
        'm' => lower_sql.contains("'m'"),
        'i' => lower_sql.contains("'i'"),
        'I' => lower_sql.contains("'i'"),
        'S' => lower_sql.contains("'s'"),
        't' => lower_sql.contains("'t'"),
        'f' => lower_sql.contains("'f'"),
        _ => false,
    }
}

pub fn list_tables_relkind_name(relkind: char) -> &'static str {
    match relkind {
        'r' => "table",
        'v' => "view",
        'm' => "materialized view",
        'i' => "index",
        'S' => "sequence",
        't' => "TOAST table",
        'f' => "foreign table",
        'p' => "partitioned table",
        'I' => "partitioned index",
        _ => "",
    }
}

pub fn describe_inherits_query_includes_relkind(lower_sql: &str) -> bool {
    lower_sql.contains("select c.oid::pg_catalog.regclass, c.relkind")
        || lower_sql.contains("select c.oid::regclass, c.relkind")
}

fn extract_operator_pattern_at<'a>(sql: &'a str, lower_sql: &str, start: usize) -> Option<&'a str> {
    let marker = "operator(pg_catalog.~)";
    let mut index = skip_ascii_whitespace(sql, start, sql.len());
    if !lower_sql[index..].starts_with(marker) {
        return None;
    }
    index += marker.len();
    index = skip_ascii_whitespace(sql, index, sql.len());
    if matches!(sql.as_bytes().get(index), Some(b'e' | b'E'))
        && sql.as_bytes().get(index + 1) == Some(&b'\'')
    {
        index += 1;
    }
    if sql.as_bytes().get(index) != Some(&b'\'') {
        return None;
    }
    let rest = &sql[index + 1..];
    let end = rest.find('\'')?;
    rest[..end].strip_prefix("^(")?.strip_suffix(")$")
}

pub fn extract_any_identifier_pattern<'a>(sql: &'a str, identifiers: &[&str]) -> Option<&'a str> {
    identifiers
        .iter()
        .find_map(|identifier| extract_identifier_pattern(sql, identifier))
}

pub fn pattern_regex(pattern: Option<&str>) -> Option<regex::Regex> {
    pattern
        .filter(|pattern| !describe_pattern_is_plain(pattern))
        .and_then(|pattern| {
            let pattern = pattern_literal_regex_text(pattern);
            regex::Regex::new(&format!("^(?:{pattern})$")).ok()
        })
}

fn pattern_literal_regex_text(pattern: &str) -> Cow<'_, str> {
    if pattern.contains("\\\\") {
        Cow::Owned(pattern.replace("\\\\", "\\"))
    } else {
        Cow::Borrowed(pattern)
    }
}

pub fn pattern_matches(
    value: &str,
    pattern: Option<&str>,
    pattern_regex: Option<&regex::Regex>,
) -> bool {
    let Some(pattern) = pattern else {
        return true;
    };
    if describe_pattern_is_plain(pattern) {
        value.eq_ignore_ascii_case(pattern)
    } else {
        pattern_regex.is_some_and(|regex| regex.is_match(value))
    }
}

pub fn describe_pattern_is_plain(pattern: &str) -> bool {
    !pattern.chars().any(|ch| {
        matches!(
            ch,
            '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\'
        )
    })
}

pub fn extract_pattern_name(sql: &str) -> Option<&str> {
    let lower = sql.to_ascii_lowercase();
    let start = lower.find("operator(pg_catalog.~)")?;
    extract_operator_pattern_at(sql, &lower, start)
}

pub fn extract_quoted_oid(sql: &str) -> Option<u32> {
    let lower = sql.to_ascii_lowercase();
    let marker = "where c.oid = '";
    let alt_marker = "where a.attrelid = '";
    let start = lower
        .find(marker)
        .map(|idx| idx + marker.len())
        .or_else(|| lower.find(alt_marker).map(|idx| idx + alt_marker.len()))?;
    let rest = &sql[start..];
    let end = rest.find('\'')?;
    rest[..end].parse::<u32>().ok()
}

pub fn extract_constraint_relid(sql: &str) -> Option<u32> {
    extract_quoted_oid_with_markers(
        sql,
        &[
            "where c.conrelid = '",
            "where r.conrelid = '",
            "and c.conrelid = '",
            "and r.conrelid = '",
            "where conrelid = '",
            "and conrelid = '",
            "where c.confrelid = '",
            "where r.confrelid = '",
            "and c.confrelid = '",
            "and r.confrelid = '",
            "where confrelid = '",
            "and confrelid = '",
        ],
    )
}

pub fn extract_quoted_literal_with_markers<'a>(sql: &'a str, markers: &[&str]) -> Option<&'a str> {
    let lower = sql.to_ascii_lowercase();
    let start = markers
        .iter()
        .find_map(|marker| lower.find(marker).map(|idx| idx + marker.len()))?;
    let rest = &sql[start..];
    let end = rest.find('\'')?;
    Some(&rest[..end])
}

pub fn extract_single_quoted_literal_after(sql: &str, needle: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    let start = lower.find(needle)? + needle.len();
    let tail = sql.get(start..)?.trim_start();
    let tail = tail.strip_prefix('\'')?;
    let end = tail.find('\'')?;
    Some(tail[..end].to_string())
}

pub fn extract_quoted_oid_with_markers(sql: &str, markers: &[&str]) -> Option<u32> {
    extract_quoted_literal_with_markers(sql, markers)?
        .parse::<u32>()
        .ok()
}

pub fn extract_unquoted_u32_after(sql: &str, marker: &str) -> Option<u32> {
    let lower = sql.to_ascii_lowercase();
    let start = lower.find(marker)? + marker.len();
    let rest = sql[start..].trim_start();
    let len = rest
        .as_bytes()
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    (len > 0).then(|| rest[..len].parse::<u32>().ok())?
}

pub fn extract_col_description_attnum(sql: &str) -> Option<i32> {
    let lower = sql.to_ascii_lowercase();
    let marker = lower
        .find("::pg_catalog.regclass,")
        .map(|idx| idx + "::pg_catalog.regclass,".len())
        .or_else(|| {
            lower
                .find("::regclass,")
                .map(|idx| idx + "::regclass,".len())
        })?;
    let rest = sql[marker..].trim_start();
    let end = rest.find(')')?;
    rest[..end].trim().parse::<i32>().ok()
}

fn skip_ascii_whitespace(sql: &str, mut start: usize, end: usize) -> usize {
    while start < end && sql.as_bytes()[start].is_ascii_whitespace() {
        start += 1;
    }
    start
}

fn is_sql_identifier_byte(byte: u8) -> bool {
    byte == b'_' || byte.is_ascii_alphanumeric()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_psql_regex_pattern_after_operator() {
        let sql = "WHERE c.relname OPERATOR(pg_catalog.~) '^(foo|bar)$'";
        assert_eq!(
            extract_identifier_pattern(sql, "c.relname"),
            Some("foo|bar")
        );
        assert_eq!(extract_pattern_name(sql), Some("foo|bar"));
    }

    #[test]
    fn psql_pattern_plain_matching_is_case_insensitive() {
        assert!(pattern_matches("Widget", Some("widget"), None));
        assert!(!describe_pattern_is_plain("foo.*"));
        let regex = pattern_regex(Some("foo.*"));
        assert!(pattern_matches("foobar", Some("foo.*"), regex.as_ref()));
    }

    #[test]
    fn extracts_psql_describe_literals() {
        assert_eq!(extract_quoted_oid("where c.oid = '123'::oid"), Some(123));
        assert_eq!(
            extract_constraint_relid("and r.confrelid = '456'::oid"),
            Some(456)
        );
        assert_eq!(
            extract_unquoted_u32_after("where c.oid = 789 and true", "where c.oid = "),
            Some(789)
        );
        assert_eq!(
            extract_col_description_attnum("select col_description('x'::regclass, 2)"),
            Some(2)
        );
        assert_eq!(
            extract_single_quoted_literal_after("where s.stxname = 'abc'", "where s.stxname ="),
            Some("abc".to_string())
        );
    }

    #[test]
    fn classifies_psql_describe_queries() {
        assert!(is_roles_query(
            "select r.rolname, r.rolsuper from pg_catalog.pg_roles r"
        ));
        assert!(is_role_settings_query(
            "select rolname as \"role\", datname as \"database\" from pg_catalog.pg_db_role_setting s"
        ));
        assert!(is_list_tables_query(
            "select n.nspname, c.relname, case c.relkind end, pg_catalog.pg_get_userbyid(c.relowner) from pg_catalog.pg_class c where c.relkind in ('r')"
        ));
        assert!(list_tables_query_includes_relkind(
            "where c.relkind in ('r','v')",
            'v'
        ));
        assert_eq!(list_tables_relkind_name('p'), "partitioned table");
        assert_eq!(permissions_relkind_name('S'), "sequence");
    }
}
