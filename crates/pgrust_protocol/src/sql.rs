use std::collections::BTreeSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimpleQueryTxnControl {
    Begin,
    Commit { chain: bool },
    Rollback { chain: bool },
    Savepoint,
    Release,
    RollbackTo,
    Other,
}

pub fn normalize_nonstandard_string_literals(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;

    while i < bytes.len() {
        if bytes[i] == b'\'' {
            let previous = sql[..i].chars().rev().find(|ch| !ch.is_ascii_whitespace());
            if !matches!(previous, Some('E' | 'e' | '&')) {
                out.push('E');
            }
            out.push('\'');
            i += 1;
            while i < bytes.len() {
                out.push(bytes[i] as char);
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    i += 1;
                    out.push(bytes[i] as char);
                } else if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 1;
                        out.push('\'');
                    } else {
                        i += 1;
                        break;
                    }
                }
                i += 1;
            }
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }

    out
}

pub fn simple_query_txn_control(sql: &str) -> SimpleQueryTxnControl {
    let normalized = sql
        .trim_start()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    if normalized.starts_with("begin") || normalized.starts_with("start transaction") {
        return SimpleQueryTxnControl::Begin;
    }
    if normalized.starts_with("commit") || normalized.starts_with("end") {
        return SimpleQueryTxnControl::Commit {
            chain: normalized.contains(" and chain"),
        };
    }
    if normalized.starts_with("rollback to") || normalized.starts_with("abort to") {
        return SimpleQueryTxnControl::RollbackTo;
    }
    if normalized.starts_with("rollback") || normalized.starts_with("abort") {
        return SimpleQueryTxnControl::Rollback {
            chain: normalized.contains(" and chain"),
        };
    }
    if normalized.starts_with("savepoint") {
        return SimpleQueryTxnControl::Savepoint;
    }
    if normalized.starts_with("release") {
        return SimpleQueryTxnControl::Release;
    }
    SimpleQueryTxnControl::Other
}

pub fn simple_query_starts_implicit_transaction(control: SimpleQueryTxnControl) -> bool {
    matches!(control, SimpleQueryTxnControl::Other)
}

pub fn normalized_command_prefix(sql: &str) -> String {
    sql.trim_start()
        .trim_end_matches(';')
        .split_whitespace()
        .take(4)
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

pub fn query_id_for_sql(sql: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    let hash = hasher.finish() & 0x7fff_ffff_ffff_ffff;
    i64::try_from(hash).unwrap_or(i64::MAX)
}

pub fn parameter_format_code(format_codes: &[i16], index: usize) -> i16 {
    match format_codes {
        [] => 0,
        [single] => *single,
        many => many.get(index).copied().unwrap_or(0),
    }
}

pub fn highest_sql_parameter_ref(sql: &str) -> usize {
    sql_parameter_refs(sql, true).into_iter().max().unwrap_or(0)
}

pub fn first_missing_sql_parameter_ref(
    sql: &str,
    standard_conforming_strings: bool,
) -> Option<usize> {
    let refs = sql_parameter_refs(sql, standard_conforming_strings);
    let highest = refs.iter().copied().max()?;
    (1..highest).find(|param| !refs.contains(param))
}

pub fn sql_parameter_refs(sql: &str, standard_conforming_strings: bool) -> Vec<usize> {
    let bytes = sql.as_bytes();
    let mut refs = Vec::new();
    let mut index = 0usize;
    let mut block_comment_depth = 0usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut line_comment = false;
    let mut dollar_quote: Option<String> = None;
    while index < bytes.len() {
        if line_comment {
            if bytes[index] == b'\n' {
                line_comment = false;
            }
            index += 1;
            continue;
        }
        if block_comment_depth > 0 {
            if index + 1 < bytes.len() && bytes[index] == b'/' && bytes[index + 1] == b'*' {
                block_comment_depth += 1;
                index += 2;
                continue;
            }
            if index + 1 < bytes.len() && bytes[index] == b'*' && bytes[index + 1] == b'/' {
                block_comment_depth -= 1;
                index += 2;
                continue;
            }
            index += 1;
            continue;
        }
        if let Some(tag) = &dollar_quote {
            if sql[index..].starts_with(tag) {
                index += tag.len();
                dollar_quote = None;
            } else {
                index += 1;
            }
            continue;
        }
        if single_quote {
            if !standard_conforming_strings && bytes[index] == b'\\' && index + 1 < bytes.len() {
                index += 2;
            } else if bytes[index] == b'\'' {
                if index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                    index += 2;
                } else {
                    single_quote = false;
                    index += 1;
                }
            } else {
                index += 1;
            }
            continue;
        }
        if double_quote {
            if bytes[index] == b'"' {
                if index + 1 < bytes.len() && bytes[index + 1] == b'"' {
                    index += 2;
                } else {
                    double_quote = false;
                    index += 1;
                }
            } else {
                index += 1;
            }
            continue;
        }

        if index + 1 < bytes.len() && bytes[index] == b'-' && bytes[index + 1] == b'-' {
            line_comment = true;
            index += 2;
            continue;
        }
        if index + 1 < bytes.len() && bytes[index] == b'/' && bytes[index + 1] == b'*' {
            block_comment_depth = 1;
            index += 2;
            continue;
        }
        if bytes[index] == b'\'' {
            single_quote = true;
            index += 1;
            continue;
        }
        if bytes[index] == b'"' {
            double_quote = true;
            index += 1;
            continue;
        }
        if bytes[index] != b'$' {
            index += 1;
            continue;
        }
        if let Some(tag_end) = sql[index + 1..].find('$') {
            let delimiter = &sql[index..=index + 1 + tag_end];
            if delimiter[1..delimiter.len() - 1]
                .chars()
                .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
                && !delimiter.as_bytes()[1].is_ascii_digit()
            {
                dollar_quote = Some(delimiter.to_string());
                index += delimiter.len();
                continue;
            }
        }
        let start = index + 1;
        let mut end = start;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
        if end > start {
            if let Ok(param) = sql[start..end].parse::<usize>() {
                refs.push(param);
            }
            index = end;
        } else {
            index += 1;
        }
    }
    refs
}

pub fn split_simple_query_statements(sql: &str, standard_conforming_strings: bool) -> Vec<&str> {
    let mut statements = Vec::new();
    let mut start = 0usize;
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut block_comment_depth = 0usize;
    let mut paren_depth = 0usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut line_comment = false;
    let mut dollar_quote: Option<String> = None;
    let mut sql_function_atomic_body = false;

    while i < bytes.len() {
        if line_comment {
            if bytes[i] == b'\n' {
                line_comment = false;
            }
            i += 1;
            continue;
        }
        if block_comment_depth > 0 {
            if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                block_comment_depth += 1;
                i += 2;
                continue;
            }
            if i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                block_comment_depth -= 1;
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }
        if let Some(tag) = &dollar_quote {
            if sql[i..].starts_with(tag) {
                i += tag.len();
                dollar_quote = None;
            } else {
                i += 1;
            }
            continue;
        }
        if single_quote {
            if !standard_conforming_strings && bytes[i] == b'\\' && i + 1 < bytes.len() {
                i += 2;
            } else if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                } else {
                    single_quote = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }
        if double_quote {
            if bytes[i] == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                } else {
                    double_quote = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }

        if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            line_comment = true;
            i += 2;
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            block_comment_depth = 1;
            i += 2;
            continue;
        }
        if bytes[i] == b'\'' {
            single_quote = true;
            i += 1;
            continue;
        }
        if bytes[i] == b'"' {
            double_quote = true;
            i += 1;
            continue;
        }
        if bytes[i] == b'$'
            && let Some(tag_end) = sql[i + 1..].find('$')
        {
            let delimiter = &sql[i..=i + 1 + tag_end];
            if delimiter[1..delimiter.len() - 1]
                .chars()
                .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
            {
                dollar_quote = Some(delimiter.to_string());
                i += delimiter.len();
                continue;
            }
        }
        if bytes[i] == b'(' {
            paren_depth += 1;
            i += 1;
            continue;
        }
        if bytes[i] == b')' {
            paren_depth = paren_depth.saturating_sub(1);
            i += 1;
            continue;
        }
        if paren_depth == 0
            && !sql_function_atomic_body
            && simple_query_keyword_at(sql, i, "begin").is_some()
            && simple_query_current_statement_is_create_routine(sql, start, i)
        {
            let begin_end = simple_query_keyword_at(sql, i, "begin").unwrap_or(i);
            let atomic_start = simple_query_skip_whitespace(sql, begin_end);
            if simple_query_keyword_at(sql, atomic_start, "atomic").is_some() {
                sql_function_atomic_body = true;
            }
        }
        if bytes[i] == b';' && paren_depth == 0 {
            if sql_function_atomic_body
                && (!simple_query_prefix_ends_with_keyword(&sql[start..i], "end")
                    || simple_query_next_token_is_keyword(sql, i + 1, "end"))
            {
                i += 1;
                continue;
            }
            statements.push(&sql[start..=i]);
            sql_function_atomic_body = false;
            start = i + 1;
        }
        i += 1;
    }

    if start < sql.len() {
        statements.push(&sql[start..]);
    }
    statements
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultipleExtendedQueryCommands;

pub fn normalize_extended_query_sql(
    sql: &str,
    standard_conforming_strings: bool,
) -> Result<String, MultipleExtendedQueryCommands> {
    let statements = split_simple_query_statements(sql, standard_conforming_strings)
        .into_iter()
        .filter(|stmt| !extended_query_segment_is_empty(stmt))
        .collect::<Vec<_>>();
    if statements.len() > 1 {
        return Err(MultipleExtendedQueryCommands);
    }
    let Some(statement) = statements.first() else {
        return Ok(String::new());
    };
    Ok(strip_one_terminal_semicolon(statement)
        .trim_start()
        .to_string())
}

pub fn extended_query_segment_is_empty(sql: &str) -> bool {
    let trimmed = sql.trim();
    if pgrust_parser::comments::sql_is_effectively_empty_after_comments(trimmed) {
        return true;
    }
    let without_semicolons = trimmed.trim_matches(';').trim();
    pgrust_parser::comments::sql_is_effectively_empty_after_comments(without_semicolons)
}

pub fn strip_one_terminal_semicolon(sql: &str) -> &str {
    let trimmed = sql.trim_end();
    trimmed.strip_suffix(';').unwrap_or(sql)
}

fn simple_query_current_statement_is_create_routine(
    sql: &str,
    start: usize,
    keyword_pos: usize,
) -> bool {
    let prefix = sql[start..keyword_pos].trim_start().to_ascii_lowercase();
    prefix.starts_with("create function ")
        || prefix.starts_with("create or replace function ")
        || prefix.starts_with("create procedure ")
        || prefix.starts_with("create or replace procedure ")
}

fn simple_query_keyword_at(sql: &str, pos: usize, keyword: &str) -> Option<usize> {
    let end = pos.checked_add(keyword.len())?;
    let candidate = sql.get(pos..end)?;
    if !candidate.eq_ignore_ascii_case(keyword) {
        return None;
    }
    let bytes = sql.as_bytes();
    if pos > 0 && simple_query_ident_byte(bytes[pos - 1]) {
        return None;
    }
    if end < bytes.len() && simple_query_ident_byte(bytes[end]) {
        return None;
    }
    Some(end)
}

fn simple_query_skip_whitespace(sql: &str, mut pos: usize) -> usize {
    let bytes = sql.as_bytes();
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    pos
}

fn simple_query_prefix_ends_with_keyword(prefix: &str, keyword: &str) -> bool {
    let trimmed = prefix.trim_end();
    let Some(start) = trimmed.len().checked_sub(keyword.len()) else {
        return false;
    };
    if !trimmed[start..].eq_ignore_ascii_case(keyword) {
        return false;
    }
    start == 0 || !simple_query_ident_byte(trimmed.as_bytes()[start - 1])
}

fn simple_query_next_token_is_keyword(sql: &str, pos: usize, keyword: &str) -> bool {
    let pos = simple_query_skip_whitespace(sql, pos);
    simple_query_keyword_at(sql, pos, keyword).is_some()
}

fn simple_query_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$')
}

pub fn psql_format_fdw_options(options: Option<&[String]>) -> String {
    let Some(options) = options else {
        return String::new();
    };
    if options.is_empty() {
        return String::new();
    }
    let parts = options
        .iter()
        .filter_map(|option| option.split_once('='))
        .map(|(name, value)| {
            format!(
                "{} '{}'",
                psql_quote_ident_if_needed(name),
                value.replace('\'', "''")
            )
        })
        .collect::<Vec<_>>();
    if parts.is_empty() {
        String::new()
    } else {
        format!("({})", parts.join(", "))
    }
}

pub fn psql_quote_ident_if_needed(ident: &str) -> String {
    let mut chars = ident.chars();
    let Some(first) = chars.next() else {
        return "\"\"".into();
    };
    let is_simple_start = first == '_' || first.is_ascii_lowercase();
    let is_simple_rest =
        chars.all(|ch| ch == '_' || ch.is_ascii_lowercase() || ch.is_ascii_digit());
    let is_keyword = matches!(ident, "user");
    if is_simple_start && is_simple_rest && !is_keyword {
        ident.to_string()
    } else {
        quote_identifier(ident)
    }
}

pub fn quote_identifier(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

pub fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub fn parse_nextval_relation_oid(expr_sql: &str) -> Option<u32> {
    let expr_sql = expr_sql.trim();
    let rest = expr_sql.strip_prefix("nextval(")?;
    let close = rest.find(')')?;
    let oid = rest[..close].trim().parse().ok()?;
    let trailing = rest[close + 1..].trim();
    if trailing.is_empty() || trailing.starts_with("::") {
        Some(oid)
    } else {
        None
    }
}

pub fn normalize_check_expr_operator_spacing(expr_sql: &str) -> String {
    let chars = expr_sql.chars().collect::<Vec<_>>();
    let mut out = String::with_capacity(expr_sql.len());
    let mut index = 0;
    while index < chars.len() {
        let op = if matches!(chars.get(index), Some('>' | '<' | '!' | '='))
            && matches!(chars.get(index + 1), Some('='))
        {
            Some(2)
        } else if matches!(chars.get(index), Some('<')) && matches!(chars.get(index + 1), Some('>'))
        {
            Some(2)
        } else if matches!(chars.get(index), Some('>' | '<' | '='))
            && !matches!(chars.get(index + 1), Some('>'))
        {
            Some(1)
        } else {
            None
        };
        let Some(op_len) = op else {
            out.push(chars[index]);
            index += 1;
            continue;
        };
        if !out.ends_with(' ') {
            out.push(' ');
        }
        for offset in 0..op_len {
            out.push(chars[index + offset]);
        }
        if !matches!(chars.get(index + op_len), Some(' ')) {
            out.push(' ');
        }
        index += op_len;
    }
    out
}

pub fn is_simple_sql_string_literal(value: &str) -> bool {
    value.starts_with('\'') && value.ends_with('\'') && value.len() >= 2
}

pub fn check_expr_column_matches(expr: &str, column_name: &str) -> bool {
    expr.eq_ignore_ascii_case(column_name)
        || expr == quote_identifier(column_name)
        || expr
            .strip_suffix("::double precision")
            .is_some_and(|base| check_expr_column_matches(base.trim(), column_name))
}

pub fn is_plain_numeric_literal(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    let mut chars = value.chars().peekable();
    if matches!(chars.peek(), Some('+') | Some('-')) {
        chars.next();
    }
    let mut saw_digit = false;
    let mut saw_dot = false;
    for ch in chars {
        if ch.is_ascii_digit() {
            saw_digit = true;
        } else if ch == '.' && !saw_dot {
            saw_dot = true;
        } else {
            return false;
        }
    }
    saw_digit
}

pub fn psql_acl_item_grants(
    item: &str,
    effective_names: &BTreeSet<String>,
    privilege: char,
) -> bool {
    let Some((grantee, rest)) = item.split_once('=') else {
        return false;
    };
    if !effective_names.contains(grantee) {
        return false;
    }
    let privileges = rest.split_once('/').map(|(privs, _)| privs).unwrap_or(rest);
    privileges.chars().any(|ch| ch == privilege)
}

pub fn parse_copy_from_stdin(sql: &str) -> Option<(String, Option<Vec<String>>, String)> {
    let lower = sql.to_ascii_lowercase();
    let prefix = "copy ";
    let source = " from stdin";
    if !lower.starts_with(prefix) || !lower.contains(source) {
        return None;
    }
    let end = lower.find(source)?;
    let target = sql[prefix.len()..end].trim();
    if target.is_empty() {
        return None;
    }
    let options = sql[end + source.len()..].trim();
    let null_marker = parse_copy_null_marker(options)?;
    let (table, columns) = if let Some(open_paren) = target.find('(') {
        let close_paren = target.rfind(')')?;
        if close_paren < open_paren {
            return None;
        }
        let table = target[..open_paren].trim();
        let columns = target[open_paren + 1..close_paren]
            .split(',')
            .map(|part| part.trim())
            .filter(|part| !part.is_empty())
            .map(|part| part.to_string())
            .collect::<Vec<_>>();
        if table.is_empty() || columns.is_empty() {
            return None;
        }
        (table.to_string(), Some(columns))
    } else {
        (target.to_string(), None)
    };
    Some((table, columns, null_marker))
}

fn parse_copy_null_marker(options: &str) -> Option<String> {
    let options = options.trim();
    if options.is_empty() {
        return Some("\\N".into());
    }
    let lower = options.to_ascii_lowercase();
    let rest = lower
        .strip_prefix("null")
        .and_then(|_| options.get(4..))?
        .trim_start();
    parse_single_quoted_copy_option(rest)
}

fn parse_single_quoted_copy_option(input: &str) -> Option<String> {
    let mut chars = input.char_indices();
    if chars.next()?.1 != '\'' {
        return None;
    }
    let mut out = String::new();
    let mut end = None;
    let mut iter = input[1..].char_indices().peekable();
    while let Some((idx, ch)) = iter.next() {
        if ch == '\'' {
            if matches!(iter.peek(), Some((_, '\''))) {
                iter.next();
                out.push('\'');
                continue;
            }
            end = Some(idx + 2);
            break;
        }
        out.push(ch);
    }
    let end = end?;
    input[end..].trim().is_empty().then_some(out)
}

pub fn parse_e_unicode_escape(bytes: &[u8], start: usize) -> Option<(usize, u32)> {
    if start + 2 > bytes.len() || bytes[start] != b'\\' {
        return None;
    }
    let (len, digits_start, digits_end) = match bytes[start + 1] {
        b'u' => (6, start + 2, start + 6),
        b'U' => (10, start + 2, start + 10),
        _ => return None,
    };
    let digits = std::str::from_utf8(bytes.get(digits_start..digits_end)?).ok()?;
    let code = u32::from_str_radix(digits, 16).ok()?;
    Some((len, code))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonstandard_literals_get_escape_prefixes() {
        assert_eq!(
            normalize_nonstandard_string_literals("select 'a\\\\b', E'c\\\\d'"),
            "select E'a\\\\b', E'c\\\\d'"
        );
    }

    #[test]
    fn command_prefix_normalizes_case_space_and_semicolon() {
        assert_eq!(
            normalized_command_prefix("  CREATE   TABLE  x (a int);"),
            "create table x (a"
        );
    }

    #[test]
    fn query_id_is_stable_and_positive() {
        let query_id = query_id_for_sql("select 1");
        assert_eq!(query_id, query_id_for_sql("select 1"));
        assert!(query_id >= 0);
    }

    #[test]
    fn extended_query_parameter_helpers_scan_visible_refs() {
        assert_eq!(parameter_format_code(&[], 0), 0);
        assert_eq!(parameter_format_code(&[1], 10), 1);
        assert_eq!(parameter_format_code(&[0, 1], 1), 1);
        assert_eq!(highest_sql_parameter_ref("select 1"), 0);
        assert_eq!(highest_sql_parameter_ref("select $2, $10, $1"), 10);
        assert_eq!(highest_sql_parameter_ref("select '$9', $1, $$ $8 $$"), 1);
        assert_eq!(first_missing_sql_parameter_ref("SELECT $2", true), Some(1));
        assert_eq!(
            first_missing_sql_parameter_ref("SELECT $1, $3", true),
            Some(2)
        );
        assert_eq!(
            first_missing_sql_parameter_ref("SELECT '$2', $1", true),
            None
        );
    }

    #[test]
    fn classifies_simple_query_transaction_control() {
        assert_eq!(
            simple_query_txn_control("BEGIN;"),
            SimpleQueryTxnControl::Begin
        );
        assert_eq!(
            simple_query_txn_control("commit and chain"),
            SimpleQueryTxnControl::Commit { chain: true }
        );
        assert_eq!(
            simple_query_txn_control("abort"),
            SimpleQueryTxnControl::Rollback { chain: false }
        );
        assert_eq!(
            simple_query_txn_control("rollback to s"),
            SimpleQueryTxnControl::RollbackTo
        );
        assert_eq!(
            simple_query_txn_control("savepoint s"),
            SimpleQueryTxnControl::Savepoint
        );
        assert_eq!(
            simple_query_txn_control("select 1"),
            SimpleQueryTxnControl::Other
        );
        assert!(simple_query_starts_implicit_transaction(
            SimpleQueryTxnControl::Other
        ));
    }

    #[test]
    fn simple_query_splitter_keeps_sql_standard_routine_bodies_together() {
        let sql = "create function f() returns int language sql begin atomic select 1; select 2; end; select 3;";
        assert_eq!(
            split_simple_query_statements(sql, true),
            vec![
                "create function f() returns int language sql begin atomic select 1; select 2; end;",
                " select 3;",
            ]
        );
    }

    #[test]
    fn extended_query_normalization_rejects_multiple_commands_and_strips_semicolon() {
        assert_eq!(
            normalize_extended_query_sql("SELECT 'val1';", true).unwrap(),
            "SELECT 'val1'"
        );
        assert_eq!(
            normalize_extended_query_sql("SELECT 1; -- trailing comment", true).unwrap(),
            "SELECT 1"
        );
        assert_eq!(
            normalize_extended_query_sql("SELECT $1, $2 ", true).unwrap(),
            "SELECT $1, $2 "
        );
        assert!(normalize_extended_query_sql("SELECT 1; SELECT 2", true).is_err());
        assert_eq!(
            normalize_extended_query_sql("-- only comment", true).unwrap(),
            ""
        );
    }

    #[test]
    fn psql_fdw_options_quote_identifiers_and_values() {
        assert_eq!(
            psql_format_fdw_options(Some(&["user=malis".into(), "fdw-option=a'b".into()])),
            "(\"user\" 'malis', \"fdw-option\" 'a''b')"
        );
        assert_eq!(psql_quote_ident_if_needed("simple_name"), "simple_name");
        assert_eq!(psql_quote_ident_if_needed("Camel"), "\"Camel\"");
    }

    #[test]
    fn sql_string_quote_and_nextval_oid_parse() {
        assert_eq!(quote_sql_string("a'b"), "'a''b'");
        assert_eq!(
            parse_nextval_relation_oid("nextval(123)::regclass"),
            Some(123)
        );
        assert_eq!(parse_nextval_relation_oid("nextval(abc)"), None);
    }

    #[test]
    fn check_expr_helpers_normalize_and_match() {
        assert_eq!(normalize_check_expr_operator_spacing("a>=1"), "a >= 1");
        assert!(is_simple_sql_string_literal("'abc'"));
        assert!(check_expr_column_matches(
            "amount::double precision",
            "amount"
        ));
        assert!(is_plain_numeric_literal("-1.25"));
        assert!(!is_plain_numeric_literal("1.2.3"));
    }

    #[test]
    fn psql_acl_item_grant_parser_accepts_optional_grantor() {
        let effective = BTreeSet::from(["malis".to_string()]);
        assert!(psql_acl_item_grants("malis=U/postgres", &effective, 'U'));
        assert!(psql_acl_item_grants("malis=U", &effective, 'U'));
        assert!(!psql_acl_item_grants("jason=U/postgres", &effective, 'U'));
    }

    #[test]
    fn copy_from_stdin_parser_extracts_table_columns_and_null_marker() {
        assert_eq!(
            parse_copy_from_stdin("COPY public.t (a, b) FROM STDIN NULL ''"),
            Some((
                "public.t".into(),
                Some(vec!["a".into(), "b".into()]),
                "".into()
            ))
        );
        assert_eq!(
            parse_copy_from_stdin("COPY public.t FROM STDIN"),
            Some(("public.t".into(), None, "\\N".into()))
        );
    }

    #[test]
    fn e_unicode_escape_parser_reads_short_and_long_forms() {
        assert_eq!(parse_e_unicode_escape(br"\u0041", 0), Some((6, 0x41)));
        assert_eq!(
            parse_e_unicode_escape(br"\U0001F600", 0),
            Some((10, 0x1F600))
        );
        assert_eq!(parse_e_unicode_escape(br"\u00", 0), None);
    }
}
