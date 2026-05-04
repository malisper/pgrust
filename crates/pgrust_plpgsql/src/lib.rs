mod ast;
pub mod cache;
pub mod compiled;
mod gram;
pub mod gucs;
pub mod normalize;
pub mod polymorphic;
pub mod runtime;

use std::collections::{HashMap, HashSet};

use pgrust_nodes::parsenodes::SqlExpr;
use pgrust_nodes::primnodes::{QueryColumn, TargetEntry};
use pgrust_nodes::{SqlType, SqlTypeKind, Value};
use pgrust_parser::{ParseError, parse_expr, parse_statement};

pub use ast::*;
pub use cache::{
    PlpgsqlFunctionCache, PlpgsqlFunctionCacheKey, RelationShape, TransitionTableShape,
    routine_cache_key, trigger_cache_key,
};
pub use compiled::*;
pub use gram::parse_block;
pub use gucs::*;
pub use normalize::*;
pub use polymorphic::*;
pub use runtime::*;

pub fn normalize_sql_context_text(sql: &str) -> String {
    sql.trim().trim_end_matches(';').trim_end().to_string()
}

pub fn decode_nonstandard_backslash_escapes(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        let Some(escaped) = chars.next() else {
            out.push('\\');
            break;
        };
        match escaped {
            '\\' => out.push('\\'),
            '\'' => out.push('\''),
            '0'..='7' => {
                let mut digits = String::from(escaped);
                while digits.len() < 3 {
                    match chars.peek().copied() {
                        Some(next @ '0'..='7') => {
                            digits.push(next);
                            chars.next();
                        }
                        _ => break,
                    }
                }
                if let Ok(code) = u32::from_str_radix(&digits, 8)
                    && let Some(decoded) = char::from_u32(code)
                {
                    out.push(decoded);
                }
            }
            other => {
                out.push('\\');
                out.push(other);
            }
        }
    }
    out
}

pub fn parse_proc_argtype_oids(argtypes: &str) -> Option<Vec<u32>> {
    if argtypes.trim().is_empty() {
        return Some(Vec::new());
    }
    argtypes
        .split_whitespace()
        .map(|part| part.parse::<u32>().ok())
        .collect()
}

pub fn exception_condition_name_sqlstate(name: &str) -> Option<&'static str> {
    match name.to_ascii_lowercase().as_str() {
        "assert_failure" => Some("P0004"),
        "data_corrupted" => Some("XX001"),
        "division_by_zero" => Some("22012"),
        "feature_not_supported" => Some("0A000"),
        "raise_exception" => Some("P0001"),
        "reading_sql_data_not_permitted" => Some("2F003"),
        "syntax_error" => Some("42601"),
        "no_data_found" => Some("P0002"),
        "too_many_rows" => Some("P0003"),
        "unique_violation" => Some("23505"),
        "not_null_violation" => Some("23502"),
        "check_violation" => Some("23514"),
        "foreign_key_violation" => Some("23503"),
        "undefined_file" => Some("58P01"),
        "invalid_parameter_value" => Some("22023"),
        "null_value_not_allowed" => Some("22004"),
        "wrong_object_type" => Some("42809"),
        _ => None,
    }
}

pub fn resolve_raise_sqlstate(value: &str) -> Option<&'static str> {
    static_sqlstate(value).or_else(|| exception_condition_name_sqlstate(value))
}

pub fn static_sqlstate(sqlstate: &str) -> Option<&'static str> {
    match sqlstate {
        "0A000" => Some("0A000"),
        "22012" => Some("22012"),
        "22004" => Some("22004"),
        "22023" => Some("22023"),
        "23502" => Some("23502"),
        "23503" => Some("23503"),
        "23505" => Some("23505"),
        "23514" => Some("23514"),
        "1234F" => Some("1234F"),
        "2F003" => Some("2F003"),
        "42601" => Some("42601"),
        "42804" => Some("42804"),
        "P0001" => Some("P0001"),
        "P0002" => Some("P0002"),
        "P0003" => Some("P0003"),
        "P0004" => Some("P0004"),
        "U9999" => Some("U9999"),
        "XX001" => Some("XX001"),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PlpgsqlVariableConflict {
    #[default]
    Error,
    UseVariable,
    UseColumn,
}

pub fn print_strict_params_directive(source: &str) -> Option<bool> {
    source.lines().find_map(|line| {
        let line = line.trim();
        let rest = line.strip_prefix("#print_strict_params")?.trim();
        if rest.eq_ignore_ascii_case("on") {
            Some(true)
        } else if rest.eq_ignore_ascii_case("off") {
            Some(false)
        } else {
            None
        }
    })
}

pub fn variable_conflict_mode(
    source: &str,
    gucs: Option<&HashMap<String, String>>,
) -> PlpgsqlVariableConflict {
    variable_conflict_directive(source).unwrap_or_else(|| variable_conflict_from_gucs(gucs))
}

pub fn variable_conflict_from_gucs(
    gucs: Option<&HashMap<String, String>>,
) -> PlpgsqlVariableConflict {
    gucs.and_then(|gucs| gucs.get("plpgsql.variable_conflict"))
        .and_then(|value| parse_variable_conflict_mode(value))
        .unwrap_or_default()
}

pub fn nonstandard_string_literals_from_gucs(gucs: Option<&HashMap<String, String>>) -> bool {
    gucs.and_then(|gucs| gucs.get("standard_conforming_strings"))
        .is_some_and(|value| value.eq_ignore_ascii_case("off"))
}

pub fn variable_conflict_directive(source: &str) -> Option<PlpgsqlVariableConflict> {
    source.lines().find_map(|line| {
        let line = line.trim();
        let rest = line.strip_prefix("#variable_conflict")?.trim();
        rest.split_whitespace()
            .next()
            .and_then(parse_variable_conflict_mode)
    })
}

pub fn parse_variable_conflict_mode(value: &str) -> Option<PlpgsqlVariableConflict> {
    match value.trim().to_ascii_lowercase().as_str() {
        "error" => Some(PlpgsqlVariableConflict::Error),
        "use_variable" => Some(PlpgsqlVariableConflict::UseVariable),
        "use_column" => Some(PlpgsqlVariableConflict::UseColumn),
        _ => None,
    }
}

pub fn positional_parameter_var_name(index: usize) -> String {
    format!("__pgrust_plpgsql_param_{index}")
}

pub fn plpgsql_label_alias(scope_index: usize, slot: usize, name: &str) -> String {
    let mut alias = format!("__pgrust_plpgsql_label_{scope_index}_{slot}_");
    for ch in name.chars() {
        alias.push(if is_plpgsql_identifier_char(ch) {
            ch
        } else {
            '_'
        });
    }
    alias
}

pub fn is_plpgsql_label_alias(name: &str) -> bool {
    name.starts_with("__pgrust_plpgsql_label_")
}

pub fn plpgsql_var_alias(slot: usize) -> String {
    format!("__pgrust_plpgsql_var_{slot}")
}

pub fn is_internal_plpgsql_name(name: &str) -> bool {
    name.starts_with("__pgrust_plpgsql_")
}

pub fn quote_identifier(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

pub fn quote_sql_string(value: &str) -> String {
    if value.contains('\\') {
        let escaped = value.replace('\\', "\\\\").replace('\'', "''");
        format!("E'{escaped}'")
    } else {
        format!("'{}'", value.replace('\'', "''"))
    }
}

fn is_plpgsql_identifier_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

pub fn split_select_into_target(sql: &str) -> Option<(String, String)> {
    let trimmed = sql.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("select into ") {
        return None;
    }
    let rest = trimmed[12..].trim_start();
    let (target, rest) = split_leading_select_into_target(rest)?;
    let select_sql = format!("select {}", rest.trim_start());
    Some((target, select_sql))
}

pub fn split_cte_prefixed_select_into_target(sql: &str) -> Option<(String, String)> {
    let trimmed = sql.trim_start();
    if !keyword_at(trimmed, 0, "with") {
        return None;
    }
    let select_idx = find_next_top_level_keyword(trimmed, &["select"])?;
    let after_select = trimmed[select_idx + "select".len()..].trim_start();
    if !keyword_at(after_select, 0, "into") {
        return None;
    }
    let rest = after_select["into".len()..].trim_start();
    let (target, rest) = split_leading_select_into_target(rest)?;
    let select_sql = format!(
        "{} select {}",
        trimmed[..select_idx].trim_end(),
        rest.trim_start()
    );
    Some((target, select_sql))
}

fn split_leading_select_into_target(rest: &str) -> Option<(String, &str)> {
    let mut chars = rest.char_indices();
    let end = if rest.starts_with('"') {
        let mut escaped = false;
        let mut end = None;
        for (index, ch) in rest.char_indices().skip(1) {
            if ch == '"' {
                if escaped {
                    escaped = false;
                    continue;
                }
                if rest[index + 1..].starts_with('"') {
                    escaped = true;
                    continue;
                }
                end = Some(index + 1);
                break;
            }
        }
        end?
    } else {
        chars
            .find(|(_, ch)| ch.is_whitespace())
            .map(|(index, _)| index)
            .unwrap_or(rest.len())
    };
    let target = rest[..end].trim().trim_matches('"').to_ascii_lowercase();
    Some((target, &rest[end..]))
}

pub fn split_select_with_into_targets(sql: &str) -> Option<(Vec<String>, String, bool)> {
    let trimmed = sql.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("select ") || lower.starts_with("select into ") {
        return None;
    }

    let into_idx = find_next_top_level_keyword(trimmed, &["into"])?;
    let select_sql = trimmed[..into_idx].trim_end();
    if select_sql.eq_ignore_ascii_case("select") {
        return None;
    }

    let mut after_into = trimmed[into_idx + "into".len()..].trim_start();
    let strict = if keyword_at(after_into, 0, "strict") {
        after_into = after_into["strict".len()..].trim_start();
        true
    } else {
        false
    };
    let clause_idx = find_next_top_level_keyword(
        after_into,
        &[
            "from",
            "where",
            "group",
            "having",
            "window",
            "union",
            "intersect",
            "except",
            "order",
            "limit",
            "offset",
            "fetch",
            "for",
        ],
    );
    let (targets_sql, suffix) = match clause_idx {
        Some(idx) => (&after_into[..idx], after_into[idx..].trim_start()),
        None => (after_into, ""),
    };
    let targets = split_top_level_csv(targets_sql)?;
    let rewritten = if suffix.is_empty() {
        select_sql.to_string()
    } else {
        format!("{select_sql} {suffix}")
    };
    Some((targets, rewritten, strict))
}

pub fn split_dml_returning_into_targets(sql: &str) -> Option<(String, Vec<String>)> {
    let trimmed = sql.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    if !(lower.starts_with("insert ")
        || lower.starts_with("update ")
        || lower.starts_with("delete ")
        || lower.starts_with("merge "))
    {
        return None;
    }

    let returning_idx = find_next_top_level_keyword(trimmed, &["returning"])?;
    let after_returning = trimmed[returning_idx + "returning".len()..].trim_start();
    let into_idx = find_next_top_level_keyword(after_returning, &["into"])?;
    let returning_sql = after_returning[..into_idx].trim_end();
    if returning_sql.is_empty() {
        return None;
    }
    let targets_sql = after_returning[into_idx + "into".len()..].trim();
    let targets = split_top_level_csv(targets_sql)?;
    let rewritten = format!(
        "{} {}",
        trimmed[..returning_idx + "returning".len()].trim_end(),
        returning_sql,
    );
    Some((rewritten, targets))
}

pub fn is_unsupported_plpgsql_transaction_command(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    keyword_at(trimmed, 0, "savepoint")
        || keyword_at(trimmed, 0, "release")
        || (keyword_at(trimmed, 0, "rollback")
            && find_next_top_level_keyword(trimmed, &["to"]).is_some())
}

pub fn transaction_command_name(sql: &str) -> &str {
    let trimmed = sql.trim_start();
    trimmed
        .split_whitespace()
        .next()
        .unwrap_or("transaction command")
}

pub fn find_next_top_level_keyword(sql: &str, keywords: &[&str]) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut idx = 0usize;
    while idx < bytes.len() {
        let ch = bytes[idx] as char;
        if in_single {
            if ch == '\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                    continue;
                }
                in_single = false;
            }
            idx += 1;
            continue;
        }
        if in_double {
            if ch == '"' {
                if bytes.get(idx + 1) == Some(&b'"') {
                    idx += 2;
                    continue;
                }
                in_double = false;
            }
            idx += 1;
            continue;
        }
        if let Some(tag) = dollar_quote_tag_at(sql, idx) {
            if let Some(close) = sql[idx + tag.len()..].find(tag) {
                idx += tag.len() + close + tag.len();
                continue;
            }
            idx += tag.len();
            continue;
        }

        match ch {
            '\'' => {
                in_single = true;
                idx += 1;
                continue;
            }
            '"' => {
                in_double = true;
                idx += 1;
                continue;
            }
            '[' => {
                bracket_depth += 1;
                idx += 1;
                continue;
            }
            ']' => {
                bracket_depth = bracket_depth.saturating_sub(1);
                idx += 1;
                continue;
            }
            '(' => {
                depth += 1;
                idx += 1;
                continue;
            }
            ')' => {
                depth = depth.saturating_sub(1);
                idx += 1;
                continue;
            }
            _ => {}
        }

        if depth == 0
            && bracket_depth == 0
            && keywords.iter().any(|keyword| keyword_at(sql, idx, keyword))
        {
            return Some(idx);
        }
        idx += 1;
    }
    None
}

pub fn keyword_at(sql: &str, idx: usize, keyword: &str) -> bool {
    let bytes = sql.as_bytes();
    let end = idx.saturating_add(keyword.len());
    if end > bytes.len() || !sql[idx..end].eq_ignore_ascii_case(keyword) {
        return false;
    }
    let before_ok = idx == 0 || !is_identifier_char(bytes[idx - 1] as char);
    let after_ok = end == bytes.len() || !is_identifier_char(bytes[end] as char);
    before_ok && after_ok
}

pub fn is_identifier_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

pub fn is_identifier_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

pub fn split_top_level_csv(input: &str) -> Option<Vec<String>> {
    let bytes = input.as_bytes();
    let mut depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut start = 0usize;
    let mut parts = Vec::new();
    let mut idx = 0usize;
    while idx < bytes.len() {
        let ch = bytes[idx] as char;
        if in_single {
            if ch == '\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                    continue;
                }
                in_single = false;
            }
            idx += 1;
            continue;
        }
        if in_double {
            if ch == '"' {
                if bytes.get(idx + 1) == Some(&b'"') {
                    idx += 2;
                    continue;
                }
                in_double = false;
            }
            idx += 1;
            continue;
        }
        if let Some(tag) = dollar_quote_tag_at(input, idx) {
            if let Some(close) = input[idx + tag.len()..].find(tag) {
                idx += tag.len() + close + tag.len();
                continue;
            }
            idx += tag.len();
            continue;
        }

        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            ',' if depth == 0 && bracket_depth == 0 => {
                let part = input[start..idx].trim();
                if part.is_empty() {
                    return None;
                }
                parts.push(part.to_string());
                start = idx + 1;
            }
            _ => {}
        }
        idx += 1;
    }

    let tail = input[start..].trim();
    if tail.is_empty() {
        return None;
    }
    parts.push(tail.to_string());
    Some(parts)
}

pub fn dollar_quote_tag_at(sql: &str, idx: usize) -> Option<&str> {
    let bytes = sql.as_bytes();
    if bytes.get(idx) != Some(&b'$') {
        return None;
    }
    let mut end = idx + 1;
    while let Some(byte) = bytes.get(end) {
        let ch = *byte as char;
        if ch == '$' {
            return Some(&sql[idx..=end]);
        }
        if !is_identifier_char(ch) {
            return None;
        }
        end += 1;
    }
    None
}

pub fn find_keyword_at_top_level(input: &str, keyword: &str) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let keyword_len = keyword.len();

    for (idx, ch) in input.char_indices() {
        if in_single {
            if ch == '\'' {
                in_single = false;
            }
            continue;
        }
        if in_double {
            if ch == '"' {
                in_double = false;
            }
            continue;
        }

        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ => {}
        }

        if depth != 0 {
            continue;
        }

        let tail = &input[idx..];
        if tail.len() < keyword_len {
            continue;
        }
        if !tail[..keyword_len].eq_ignore_ascii_case(keyword) {
            continue;
        }
        let prev_ok = idx == 0
            || !input[..idx]
                .chars()
                .next_back()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_');
        let next_ok = tail[keyword_len..]
            .chars()
            .next()
            .is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'));
        if prev_ok && next_ok {
            return Some(idx);
        }
    }

    None
}

pub fn find_top_level_token(input: &str, token: &str) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;

    for (idx, ch) in input.char_indices() {
        if in_single {
            if ch == '\'' {
                in_single = false;
            }
            continue;
        }
        if in_double {
            if ch == '"' {
                in_double = false;
            }
            continue;
        }

        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ => {}
        }

        if depth == 0 && input[idx..].starts_with(token) {
            return Some(idx);
        }
    }

    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryCompareOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    IsDistinctFrom,
    IsNotDistinctFrom,
}

pub struct ParsedQueryCondition<'a> {
    pub left_expr: &'a str,
    pub op: QueryCompareOp,
    pub right_expr: &'a str,
    pub from_clause: &'a str,
}

pub fn parse_plpgsql_query_condition(sql: &str) -> Option<ParsedQueryCondition<'_>> {
    let from_idx = find_keyword_at_top_level(sql, "from")?;
    let before_from = sql[..from_idx].trim();
    let after_from = sql[from_idx + "from".len()..].trim();
    if before_from.is_empty() || after_from.is_empty() {
        return None;
    }

    let (left, op, right) = split_top_level_comparison(before_from)?;
    if !looks_like_aggregate_expr(left) {
        return None;
    }

    Some(ParsedQueryCondition {
        left_expr: left,
        op: query_compare_op(op)?,
        right_expr: right,
        from_clause: after_from,
    })
}

pub fn rewrite_plpgsql_query_condition(sql: &str) -> Option<String> {
    let parsed = parse_plpgsql_query_condition(sql)?;
    Some(format!(
        "(select {} from {}) {} {}",
        parsed.left_expr,
        parsed.from_clause,
        render_query_compare_op(parsed.op),
        parsed.right_expr
    ))
}

pub fn rewrite_plpgsql_assignment_query_expr(sql: &str) -> Option<String> {
    let from_idx = find_keyword_at_top_level(sql, "from")?;
    let expr = sql[..from_idx].trim();
    let from_clause = sql[from_idx + "from".len()..].trim();
    if expr.is_empty() || from_clause.is_empty() {
        return None;
    }
    Some(format!("(select {expr} from {from_clause})"))
}

pub fn query_compare_op(op: &str) -> Option<QueryCompareOp> {
    Some(match op {
        "=" => QueryCompareOp::Eq,
        "<>" | "!=" => QueryCompareOp::NotEq,
        "<" => QueryCompareOp::Lt,
        "<=" => QueryCompareOp::LtEq,
        ">" => QueryCompareOp::Gt,
        ">=" => QueryCompareOp::GtEq,
        "is distinct from" => QueryCompareOp::IsDistinctFrom,
        "is not distinct from" => QueryCompareOp::IsNotDistinctFrom,
        _ => return None,
    })
}

pub fn render_query_compare_op(op: QueryCompareOp) -> &'static str {
    match op {
        QueryCompareOp::Eq => "=",
        QueryCompareOp::NotEq => "!=",
        QueryCompareOp::Lt => "<",
        QueryCompareOp::LtEq => "<=",
        QueryCompareOp::Gt => ">",
        QueryCompareOp::GtEq => ">=",
        QueryCompareOp::IsDistinctFrom => "is distinct from",
        QueryCompareOp::IsNotDistinctFrom => "is not distinct from",
    }
}

pub fn split_top_level_comparison(input: &str) -> Option<(&str, &'static str, &str)> {
    const OPERATORS: [&str; 8] = [
        " is not distinct from ",
        " is distinct from ",
        ">=",
        "<=",
        "<>",
        "!=",
        "=",
        ">",
    ];

    for op in OPERATORS {
        if let Some(idx) = find_top_level_token(input, op) {
            let left = input[..idx].trim();
            let right = input[idx + op.len()..].trim();
            if !left.is_empty() && !right.is_empty() {
                return Some((left, op.trim(), right));
            }
        }
    }

    if let Some(idx) = find_top_level_token(input, "<") {
        let left = input[..idx].trim();
        let right = input[idx + 1..].trim();
        if !left.is_empty() && !right.is_empty() {
            return Some((left, "<", right));
        }
    }

    None
}

pub fn looks_like_aggregate_expr(expr: &str) -> bool {
    let trimmed = expr.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    [
        "count(",
        "sum(",
        "avg(",
        "min(",
        "max(",
        "bool_and(",
        "bool_or(",
        "every(",
        "array_agg(",
        "string_agg(",
        "json_agg(",
        "jsonb_agg(",
        "json_object_agg(",
        "jsonb_object_agg(",
        "xmlagg(",
    ]
    .iter()
    .any(|prefix| lower.starts_with(prefix))
}

pub fn dynamic_sql_literal(sql_expr: &str) -> Option<String> {
    let expr = parse_expr(sql_expr).ok()?;
    match expr {
        SqlExpr::Const(value) => value.as_text().map(str::to_string),
        _ => None,
    }
}

pub fn dynamic_shape_sql(sql: &str, using_exprs: &[String]) -> String {
    if using_exprs.is_empty() {
        return normalize_sql_context_text(sql);
    }
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut idx = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    while idx < bytes.len() {
        let ch = bytes[idx] as char;
        if in_single {
            out.push(ch);
            if ch == '\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    out.push('\'');
                    idx += 2;
                    continue;
                }
                in_single = false;
            }
            idx += 1;
            continue;
        }
        if in_double {
            out.push(ch);
            if ch == '"' {
                if bytes.get(idx + 1) == Some(&b'"') {
                    out.push('"');
                    idx += 2;
                    continue;
                }
                in_double = false;
            }
            idx += 1;
            continue;
        }
        if ch == '\'' {
            in_single = true;
            out.push(ch);
            idx += 1;
            continue;
        }
        if ch == '"' {
            in_double = true;
            out.push(ch);
            idx += 1;
            continue;
        }
        if ch == '$' {
            let start = idx + 1;
            let mut end = start;
            while end < bytes.len() && (bytes[end] as char).is_ascii_digit() {
                end += 1;
            }
            if end > start
                && let Ok(param_index) = sql[start..end].parse::<usize>()
                && let Some(expr) = using_exprs.get(param_index - 1)
            {
                out.push('(');
                out.push_str(expr);
                out.push(')');
                idx = end;
                continue;
            }
        }
        out.push(ch);
        idx += 1;
    }
    normalize_sql_context_text(&out)
}

pub fn target_entry_query_column(target: &TargetEntry) -> QueryColumn {
    QueryColumn {
        name: target.name.clone(),
        sql_type: target.sql_type,
        wire_type_oid: None,
    }
}

pub fn static_query_source_known_columns(sql: &str) -> Option<Vec<QueryColumn>> {
    let normalized = sql
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    if !matches!(
        normalized.as_str(),
        "select * from pg_get_catalog_foreign_keys()"
            | "select * from pg_catalog.pg_get_catalog_foreign_keys()"
    ) {
        return None;
    }

    Some(vec![
        plpgsql_query_column("fktable", SqlType::new(SqlTypeKind::Text)),
        plpgsql_query_column("fkcols", SqlType::array_of(SqlType::new(SqlTypeKind::Text))),
        plpgsql_query_column("pktable", SqlType::new(SqlTypeKind::Text)),
        plpgsql_query_column("pkcols", SqlType::array_of(SqlType::new(SqlTypeKind::Text))),
        plpgsql_query_column("is_array", SqlType::new(SqlTypeKind::Bool)),
        plpgsql_query_column("is_opt", SqlType::new(SqlTypeKind::Bool)),
    ])
}

pub fn should_fallback_to_runtime_sql(err: &ParseError) -> bool {
    !matches!(
        err.unpositioned(),
        ParseError::AmbiguousColumn(_)
            | ParseError::DetailedError {
                sqlstate: "42702",
                ..
            }
    )
}

pub fn should_defer_plpgsql_sql_to_runtime(err: &ParseError) -> bool {
    should_fallback_to_runtime_sql(err)
        && matches!(
            err.unpositioned(),
            ParseError::UnknownTable(_)
                | ParseError::TableDoesNotExist(_)
                | ParseError::MissingFromClauseEntry(_)
        )
}

pub fn persistent_object_transition_table_reference_name<'a>(
    sql: &str,
    transition_table_names: impl IntoIterator<Item = &'a str>,
) -> Option<String> {
    let lower = sql.trim_start().to_ascii_lowercase();
    let is_persistent_create = lower.starts_with("create view ")
        || lower.starts_with("create materialized view ")
        || (lower.starts_with("create table ")
            && !lower.starts_with("create table pg_temp.")
            && !lower.starts_with("create table temp ")
            && !lower.starts_with("create table temporary "))
        || (lower.starts_with("create unlogged table ")
            && !lower.starts_with("create unlogged table pg_temp."));
    if !is_persistent_create {
        return None;
    }
    transition_table_names
        .into_iter()
        .find(|name| sql_references_relation_name(&lower, name))
        .map(str::to_string)
}

pub fn sql_references_relation_name(lower_sql: &str, name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    [
        format!(" from {name}"),
        format!(" join {name}"),
        format!(" update {name}"),
        format!(" into {name}"),
        format!(" from \"{name}\""),
        format!(" join \"{name}\""),
        format!(" update \"{name}\""),
        format!(" into \"{name}\""),
    ]
    .iter()
    .any(|needle| lower_sql.contains(needle))
}

pub const PLPGSQL_RUNTIME_PARAM_BASE: usize = 1_000_000_000;

pub fn runtime_sql_param_id(slot: usize) -> usize {
    PLPGSQL_RUNTIME_PARAM_BASE + slot
}

pub fn declared_cursor_args_context(
    assigned: &[Option<String>],
    param_names: &[String],
) -> Option<String> {
    if assigned.is_empty() {
        return None;
    }
    Some(
        assigned
            .iter()
            .zip(param_names)
            .map(|(expr, param_name)| {
                format!(
                    "{} AS {}",
                    expr.as_deref().expect("cursor args checked").trim(),
                    param_name
                )
            })
            .collect::<Vec<_>>()
            .join(", "),
    )
}

pub fn plpgsql_query_column(name: &str, sql_type: SqlType) -> QueryColumn {
    QueryColumn {
        name: name.into(),
        sql_type,
        wire_type_oid: None,
    }
}

pub fn is_catalog_foreign_key_query_sql(sql: &str) -> bool {
    let normalized = sql
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "select * from pg_get_catalog_foreign_keys()"
            | "select * from pg_catalog.pg_get_catalog_foreign_keys()"
    )
}

pub fn is_catalog_foreign_key_check_sql(sql: &str) -> bool {
    let normalized = sql
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    normalized.starts_with("select ctid")
        && normalized.contains(" from pg_")
        && normalized.contains(" not exists(select 1 from pg_")
}

pub fn catalog_foreign_key_column_array(cols: &str) -> Value {
    Value::Array(
        cols.split(',')
            .map(|col| Value::Text(col.trim().into()))
            .collect(),
    )
}

pub fn catalog_foreign_key_is_array(fktable: &str, fkcols: &str) -> bool {
    matches!(
        (fktable, fkcols),
        ("pg_proc", "proargtypes")
            | ("pg_proc", "proallargtypes")
            | ("pg_proc", "protrftypes")
            | ("pg_constraint", "conpfeqop")
            | ("pg_constraint", "conppeqop")
            | ("pg_constraint", "conffeqop")
            | ("pg_constraint", "conexclop")
            | ("pg_constraint", "conrelid,conkey")
            | ("pg_constraint", "confrelid,confkey")
            | ("pg_index", "indcollation")
            | ("pg_index", "indclass")
            | ("pg_index", "indrelid,indkey")
            | ("pg_statistic_ext", "stxrelid,stxkeys")
            | ("pg_trigger", "tgrelid,tgattr")
            | ("pg_extension", "extconfig")
            | ("pg_policy", "polroles")
            | ("pg_partitioned_table", "partclass")
            | ("pg_partitioned_table", "partcollation")
            | ("pg_partitioned_table", "partrelid,partattrs")
    )
}

pub fn catalog_foreign_key_is_optional(fktable: &str, fkcols: &str) -> bool {
    matches!(
        (fktable, fkcols),
        ("pg_proc", "provariadic")
            | ("pg_proc", "prosupport")
            | ("pg_type", "typrelid")
            | ("pg_type", "typsubscript")
            | ("pg_type", "typelem")
            | ("pg_type", "typarray")
            | ("pg_type", "typreceive")
            | ("pg_type", "typsend")
            | ("pg_type", "typmodin")
            | ("pg_type", "typmodout")
            | ("pg_type", "typanalyze")
            | ("pg_type", "typbasetype")
            | ("pg_type", "typcollation")
            | ("pg_attribute", "atttypid")
            | ("pg_attribute", "attcollation")
            | ("pg_class", "reltype")
            | ("pg_class", "reloftype")
            | ("pg_class", "relam")
            | ("pg_class", "reltablespace")
            | ("pg_class", "reltoastrelid")
            | ("pg_class", "relrewrite")
            | ("pg_constraint", "conrelid")
            | ("pg_constraint", "contypid")
            | ("pg_constraint", "conindid")
            | ("pg_constraint", "conparentid")
            | ("pg_constraint", "confrelid")
            | ("pg_constraint", "conrelid,conkey")
            | ("pg_index", "indcollation")
            | ("pg_index", "indrelid,indkey")
            | ("pg_operator", "oprleft")
            | ("pg_operator", "oprresult")
            | ("pg_operator", "oprcom")
            | ("pg_operator", "oprnegate")
            | ("pg_operator", "oprcode")
            | ("pg_operator", "oprrest")
            | ("pg_operator", "oprjoin")
            | ("pg_opclass", "opckeytype")
            | ("pg_amop", "amopsortfamily")
            | ("pg_language", "lanplcallfoid")
            | ("pg_language", "laninline")
            | ("pg_language", "lanvalidator")
            | ("pg_aggregate", "aggfinalfn")
            | ("pg_aggregate", "aggcombinefn")
            | ("pg_aggregate", "aggserialfn")
            | ("pg_aggregate", "aggdeserialfn")
            | ("pg_aggregate", "aggmtransfn")
            | ("pg_aggregate", "aggminvtransfn")
            | ("pg_aggregate", "aggmfinalfn")
            | ("pg_aggregate", "aggsortop")
            | ("pg_aggregate", "aggmtranstype")
            | ("pg_statistic", "staop1")
            | ("pg_statistic", "staop2")
            | ("pg_statistic", "staop3")
            | ("pg_statistic", "staop4")
            | ("pg_statistic", "staop5")
            | ("pg_statistic", "stacoll1")
            | ("pg_statistic", "stacoll2")
            | ("pg_statistic", "stacoll3")
            | ("pg_statistic", "stacoll4")
            | ("pg_statistic", "stacoll5")
            | ("pg_cast", "castfunc")
            | ("pg_database", "dattablespace")
            | ("pg_db_role_setting", "setdatabase")
            | ("pg_db_role_setting", "setrole")
            | ("pg_shdepend", "dbid")
            | ("pg_default_acl", "defaclnamespace")
            | ("pg_partitioned_table", "partdefid")
            | ("pg_partitioned_table", "partrelid,partattrs")
            | ("pg_range", "rngcollation")
            | ("pg_range", "rngcanonical")
            | ("pg_range", "rngsubdiff")
            | ("pg_transform", "trffromsql")
            | ("pg_transform", "trftosql")
    )
}

pub fn parse_select_into_assign_target(target: &str) -> Result<AssignTarget, ParseError> {
    let trimmed = target.trim();
    match parse_expr(trimmed)? {
        SqlExpr::Column(name) => {
            if let Some((relation, field)) = name.rsplit_once('.') {
                Ok(AssignTarget::Field {
                    relation: relation.to_string(),
                    field: field.to_string(),
                })
            } else {
                Ok(AssignTarget::Name(name))
            }
        }
        SqlExpr::FieldSelect { expr, field } => match *expr {
            SqlExpr::Column(relation) => Ok(AssignTarget::Field { relation, field }),
            _ => Err(ParseError::UnexpectedToken {
                expected: "PL/pgSQL SELECT INTO target",
                actual: trimmed.into(),
            }),
        },
        _ => Err(ParseError::UnexpectedToken {
            expected: "PL/pgSQL SELECT INTO target",
            actual: trimmed.into(),
        }),
    }
}

pub fn parse_select_into_assign_targets(
    targets_sql: &str,
) -> Result<Vec<AssignTarget>, ParseError> {
    split_top_level_csv(targets_sql)
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "PL/pgSQL SELECT INTO target [, ...]",
            actual: targets_sql.into(),
        })?
        .iter()
        .map(|target| parse_select_into_assign_target(target))
        .collect()
}

pub fn identifier_position(sql: &str, ident: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let ident_len = ident.len();
    let mut offset = 0usize;
    while offset + ident_len <= bytes.len() {
        let rest = &sql[offset..];
        let Some(found) = rest.to_ascii_lowercase().find(&ident.to_ascii_lowercase()) else {
            break;
        };
        let start = offset + found;
        let end = start + ident_len;
        let before_ok =
            start == 0 || !is_sql_ident_char(sql.as_bytes()[start.saturating_sub(1)] as char);
        let after_ok = end == sql.len() || !is_sql_ident_char(sql.as_bytes()[end] as char);
        if before_ok && after_ok {
            return Some(start);
        }
        offset = end;
    }
    None
}

fn is_sql_ident_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlpgsqlNotice {
    pub level: RaiseLevel,
    pub sqlstate: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

impl PlpgsqlNotice {
    pub fn new(level: RaiseLevel, message: impl Into<String>) -> Self {
        let sqlstate = match &level {
            RaiseLevel::Warning => "01000",
            _ => "00000",
        };
        Self {
            level,
            sqlstate: sqlstate.into(),
            message: message.into(),
            detail: None,
            hint: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlpgsqlValidationNotice {
    pub severity: &'static str,
    pub sqlstate: &'static str,
    pub message: String,
}

pub fn validate_create_function_body(body: &str, has_output_args: bool) -> Result<(), ParseError> {
    validate_create_function_body_with_options(
        body,
        has_output_args,
        false,
        false,
        false,
        &[],
        &[],
        None,
    )
    .map(|_| ())
}

pub fn validate_create_function_body_with_options(
    body: &str,
    has_output_args: bool,
    returns_void: bool,
    returns_set: bool,
    allows_bare_return: bool,
    arg_names: &[String],
    arg_types: &[(String, SqlType)],
    gucs: Option<&HashMap<String, String>>,
) -> Result<Vec<PlpgsqlValidationNotice>, ParseError> {
    let block = crate::parse_block(body)?;
    validate_declared_cursor_arguments(&block)?;
    validate_raise_placeholders(&block)?;
    validate_return_statements(
        &block,
        has_output_args,
        returns_void,
        returns_set,
        allows_bare_return,
    )?;
    validate_get_diagnostics_targets(&block, arg_types)?;
    validate_static_sql(&block)?;
    let mut notices = Vec::new();
    validate_shadowed_variables(&block, arg_names, gucs, &mut notices)?;
    Ok(notices)
}

fn validate_get_diagnostics_targets(
    block: &Block,
    arg_types: &[(String, SqlType)],
) -> Result<(), ParseError> {
    let hidden_names = block
        .declarations
        .iter()
        .map(|decl| match decl {
            Decl::Var(decl) => &decl.name,
            Decl::Cursor(decl) => &decl.name,
            Decl::Alias(decl) => &decl.name,
        })
        .map(|name| name.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let visible_arg_types = arg_types
        .iter()
        .filter(|(name, _)| !hidden_names.contains(&name.to_ascii_lowercase()))
        .cloned()
        .collect::<Vec<_>>();
    for stmt in &block.statements {
        validate_get_diagnostics_targets_in_stmt(stmt, &visible_arg_types)?;
    }
    for handler in &block.exception_handlers {
        for stmt in &handler.statements {
            validate_get_diagnostics_targets_in_stmt(stmt, &visible_arg_types)?;
        }
    }
    Ok(())
}

fn validate_get_diagnostics_targets_in_stmt(
    stmt: &Stmt,
    arg_types: &[(String, SqlType)],
) -> Result<(), ParseError> {
    match stmt {
        Stmt::WithLine { stmt, .. } => validate_get_diagnostics_targets_in_stmt(stmt, arg_types),
        Stmt::GetDiagnostics { items, .. } => {
            for (target, _) in items {
                validate_get_diagnostics_target(target, arg_types)?;
            }
            Ok(())
        }
        Stmt::Block(block) => validate_get_diagnostics_targets(block, arg_types),
        Stmt::If {
            branches,
            else_branch,
        } => {
            for (_, body) in branches {
                for stmt in body {
                    validate_get_diagnostics_targets_in_stmt(stmt, arg_types)?;
                }
            }
            for stmt in else_branch {
                validate_get_diagnostics_targets_in_stmt(stmt, arg_types)?;
            }
            Ok(())
        }
        Stmt::While { body, .. }
        | Stmt::Loop { body }
        | Stmt::ForInt { body, .. }
        | Stmt::ForQuery { body, .. }
        | Stmt::ForEach { body, .. } => {
            for stmt in body {
                validate_get_diagnostics_targets_in_stmt(stmt, arg_types)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_get_diagnostics_target(
    target: &AssignTarget,
    arg_types: &[(String, SqlType)],
) -> Result<(), ParseError> {
    let AssignTarget::Name(name) = target else {
        return Ok(());
    };
    let Some((_, ty)) = arg_types
        .iter()
        .find(|(arg_name, _)| arg_name.eq_ignore_ascii_case(name))
    else {
        return Ok(());
    };
    if matches!(ty.kind, SqlTypeKind::Composite | SqlTypeKind::Record) {
        return Err(ParseError::DetailedError {
            message: format!("\"{name}\" is not a scalar variable"),
            detail: None,
            hint: None,
            sqlstate: "42804",
        });
    }
    Ok(())
}

fn validate_raise_placeholders(block: &Block) -> Result<(), ParseError> {
    for stmt in &block.statements {
        validate_raise_placeholders_in_stmt(stmt)?;
    }
    for handler in &block.exception_handlers {
        for stmt in &handler.statements {
            validate_raise_placeholders_in_stmt(stmt)?;
        }
    }
    Ok(())
}

fn validate_raise_placeholders_in_stmt(stmt: &Stmt) -> Result<(), ParseError> {
    match stmt {
        Stmt::WithLine { stmt, .. } => validate_raise_placeholders_in_stmt(stmt),
        Stmt::Block(block) => validate_raise_placeholders(block),
        Stmt::If {
            branches,
            else_branch,
        } => {
            for (_, body) in branches {
                for stmt in body {
                    validate_raise_placeholders_in_stmt(stmt)?;
                }
            }
            for stmt in else_branch {
                validate_raise_placeholders_in_stmt(stmt)?;
            }
            Ok(())
        }
        Stmt::While { body, .. }
        | Stmt::Loop { body }
        | Stmt::ForInt { body, .. }
        | Stmt::ForQuery { body, .. }
        | Stmt::ForEach { body, .. } => {
            for stmt in body {
                validate_raise_placeholders_in_stmt(stmt)?;
            }
            Ok(())
        }
        Stmt::Raise {
            message: Some(message),
            params,
            ..
        } => {
            let placeholder_count = count_raise_placeholders(message);
            if placeholder_count < params.len() {
                return Err(raise_placeholder_error(
                    "too many parameters specified for RAISE",
                ));
            }
            if placeholder_count > params.len() {
                return Err(raise_placeholder_error(
                    "too few parameters specified for RAISE",
                ));
            }
            Ok(())
        }
        Stmt::Raise {
            message: None,
            params,
            ..
        } if !params.is_empty() => Err(raise_placeholder_error(
            "too many parameters specified for RAISE",
        )),
        _ => Ok(()),
    }
}

pub fn count_raise_placeholders(message: &str) -> usize {
    let mut count = 0usize;
    let mut chars = message.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '%' {
            if chars.peek() == Some(&'%') {
                chars.next();
            } else {
                count += 1;
            }
        }
    }
    count
}

fn raise_placeholder_error(message: &str) -> ParseError {
    ParseError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "42601",
    }
}

fn validate_declared_cursor_arguments(block: &Block) -> Result<(), ParseError> {
    validate_declared_cursor_arguments_in_block(block, &mut Vec::new())
}

fn validate_declared_cursor_arguments_in_block(
    block: &Block,
    scopes: &mut Vec<HashMap<String, Vec<String>>>,
) -> Result<(), ParseError> {
    scopes.push(
        block
            .declarations
            .iter()
            .filter_map(|decl| match decl {
                Decl::Cursor(cursor) => Some((
                    cursor.name.to_ascii_lowercase(),
                    cursor
                        .params
                        .iter()
                        .map(|param| param.name.clone())
                        .collect(),
                )),
                _ => None,
            })
            .collect(),
    );
    for stmt in &block.statements {
        validate_declared_cursor_arguments_in_stmt(stmt, scopes)?;
    }
    for handler in &block.exception_handlers {
        for stmt in &handler.statements {
            validate_declared_cursor_arguments_in_stmt(stmt, scopes)?;
        }
    }
    scopes.pop();
    Ok(())
}

fn validate_declared_cursor_arguments_in_stmt(
    stmt: &Stmt,
    scopes: &mut Vec<HashMap<String, Vec<String>>>,
) -> Result<(), ParseError> {
    match stmt {
        Stmt::WithLine { stmt, .. } => validate_declared_cursor_arguments_in_stmt(stmt, scopes),
        Stmt::Block(block) => validate_declared_cursor_arguments_in_block(block, scopes),
        Stmt::If {
            branches,
            else_branch,
        } => {
            for (_, body) in branches {
                for stmt in body {
                    validate_declared_cursor_arguments_in_stmt(stmt, scopes)?;
                }
            }
            for stmt in else_branch {
                validate_declared_cursor_arguments_in_stmt(stmt, scopes)?;
            }
            Ok(())
        }
        Stmt::While { body, .. }
        | Stmt::Loop { body }
        | Stmt::ForInt { body, .. }
        | Stmt::ForEach { body, .. } => {
            for stmt in body {
                validate_declared_cursor_arguments_in_stmt(stmt, scopes)?;
            }
            Ok(())
        }
        Stmt::ForQuery { source, body, .. } => {
            if let ForQuerySource::Cursor { name, args } = source {
                let params = visible_declared_cursor_params(name, scopes).ok_or_else(|| {
                    ParseError::DetailedError {
                        message: "cursor FOR loop must use a bound cursor variable".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "42601",
                    }
                })?;
                validate_cursor_arg_list(name, args, params)?;
            }
            for stmt in body {
                validate_declared_cursor_arguments_in_stmt(stmt, scopes)?;
            }
            Ok(())
        }
        Stmt::OpenCursor { name, source } => {
            if let OpenCursorSource::Declared { args } = source
                && let Some(params) = visible_declared_cursor_params(name, scopes)
            {
                validate_cursor_arg_list(name, args, params)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn visible_declared_cursor_params<'a>(
    name: &str,
    scopes: &'a [HashMap<String, Vec<String>>],
) -> Option<&'a [String]> {
    scopes
        .iter()
        .rev()
        .find_map(|scope| scope.get(&name.to_ascii_lowercase()))
        .map(Vec::as_slice)
}

fn validate_cursor_arg_list(
    cursor_name: &str,
    args: &[CursorArg],
    params: &[String],
) -> Result<(), ParseError> {
    let mut assigned = vec![false; params.len()];
    for (arg_index, arg) in args.iter().enumerate() {
        match arg {
            CursorArg::Positional(_) => {
                let Some(param_name) = params.get(arg_index) else {
                    return Err(cursor_arg_error(format!(
                        "too many arguments for cursor \"{cursor_name}\""
                    )));
                };
                if assigned[arg_index] {
                    return Err(duplicate_cursor_param_error(cursor_name, param_name));
                }
                assigned[arg_index] = true;
            }
            CursorArg::Named { name, .. } => {
                let Some(index) = params
                    .iter()
                    .position(|param| param.eq_ignore_ascii_case(name))
                else {
                    return Err(cursor_arg_error(format!(
                        "cursor \"{cursor_name}\" has no argument named \"{name}\""
                    )));
                };
                if assigned[index] {
                    return Err(duplicate_cursor_param_error(cursor_name, &params[index]));
                }
                assigned[index] = true;
            }
        }
    }
    if assigned.iter().any(|assigned| !assigned) {
        return Err(cursor_arg_error(format!(
            "not enough arguments for cursor \"{cursor_name}\""
        )));
    }
    Ok(())
}

fn duplicate_cursor_param_error(cursor_name: &str, param_name: &str) -> ParseError {
    cursor_arg_error(format!(
        "value for parameter \"{param_name}\" of cursor \"{cursor_name}\" specified more than once"
    ))
}

fn cursor_arg_error(message: String) -> ParseError {
    ParseError::DetailedError {
        message,
        detail: None,
        hint: None,
        sqlstate: "42601",
    }
}

fn validate_return_statements(
    block: &Block,
    has_output_args: bool,
    returns_void: bool,
    returns_set: bool,
    allows_bare_return: bool,
) -> Result<(), ParseError> {
    for stmt in &block.statements {
        validate_return_stmt_in_stmt(
            stmt,
            has_output_args,
            returns_void,
            returns_set,
            allows_bare_return,
        )?;
    }
    for handler in &block.exception_handlers {
        for stmt in &handler.statements {
            validate_return_stmt_in_stmt(
                stmt,
                has_output_args,
                returns_void,
                returns_set,
                allows_bare_return,
            )?;
        }
    }
    Ok(())
}

fn validate_return_stmt_in_stmt(
    stmt: &Stmt,
    has_output_args: bool,
    returns_void: bool,
    returns_set: bool,
    allows_bare_return: bool,
) -> Result<(), ParseError> {
    match stmt {
        Stmt::WithLine { stmt, .. } => validate_return_stmt_in_stmt(
            stmt,
            has_output_args,
            returns_void,
            returns_set,
            allows_bare_return,
        ),
        Stmt::Return { expr: Some(_), .. } if has_output_args => Err(ParseError::DetailedError {
            message: "RETURN cannot have a parameter in function with OUT parameters".into(),
            detail: None,
            hint: None,
            sqlstate: "42804",
        }),
        Stmt::Return { expr: Some(_), .. } if returns_void => Err(ParseError::DetailedError {
            message: "RETURN cannot have a parameter in function returning void".into(),
            detail: None,
            hint: None,
            sqlstate: "42804",
        }),
        Stmt::Return { expr: None, .. }
            if !has_output_args && !returns_void && !returns_set && !allows_bare_return =>
        {
            Err(ParseError::DetailedError {
                message: "missing expression at or near \";\"".into(),
                detail: None,
                hint: None,
                sqlstate: "42601",
            })
        }
        Stmt::Block(block) => validate_return_statements(
            block,
            has_output_args,
            returns_void,
            returns_set,
            allows_bare_return,
        ),
        Stmt::Continue { .. } => Ok(()),
        Stmt::If {
            branches,
            else_branch,
        } => {
            for (_, body) in branches {
                for stmt in body {
                    validate_return_stmt_in_stmt(
                        stmt,
                        has_output_args,
                        returns_void,
                        returns_set,
                        allows_bare_return,
                    )?;
                }
            }
            for stmt in else_branch {
                validate_return_stmt_in_stmt(
                    stmt,
                    has_output_args,
                    returns_void,
                    returns_set,
                    allows_bare_return,
                )?;
            }
            Ok(())
        }
        Stmt::While { body, .. }
        | Stmt::Loop { body }
        | Stmt::ForInt { body, .. }
        | Stmt::ForQuery { body, .. }
        | Stmt::ForEach { body, .. } => {
            for stmt in body {
                validate_return_stmt_in_stmt(
                    stmt,
                    has_output_args,
                    returns_void,
                    returns_set,
                    allows_bare_return,
                )?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_static_sql(block: &Block) -> Result<(), ParseError> {
    for decl in &block.declarations {
        if let Decl::Cursor(cursor) = decl {
            validate_static_select_sql(&cursor.query)?;
        }
    }
    for stmt in &block.statements {
        validate_static_sql_in_stmt(stmt)?;
    }
    for handler in &block.exception_handlers {
        for stmt in &handler.statements {
            validate_static_sql_in_stmt(stmt)?;
        }
    }
    Ok(())
}

fn validate_static_sql_in_stmt(stmt: &Stmt) -> Result<(), ParseError> {
    match stmt {
        Stmt::WithLine { stmt, .. } => validate_static_sql_in_stmt(stmt),
        Stmt::Block(block) => validate_static_sql(block),
        Stmt::If {
            branches,
            else_branch,
        } => {
            for (_, body) in branches {
                for stmt in body {
                    validate_static_sql_in_stmt(stmt)?;
                }
            }
            for stmt in else_branch {
                validate_static_sql_in_stmt(stmt)?;
            }
            Ok(())
        }
        Stmt::While { body, .. }
        | Stmt::Loop { body }
        | Stmt::ForInt { body, .. }
        | Stmt::ForEach { body, .. } => {
            for stmt in body {
                validate_static_sql_in_stmt(stmt)?;
            }
            Ok(())
        }
        Stmt::ForQuery { source, body, .. } => {
            if let ForQuerySource::Static(sql) = source {
                validate_static_select_sql(sql)?;
            }
            for stmt in body {
                validate_static_sql_in_stmt(stmt)?;
            }
            Ok(())
        }
        Stmt::ReturnQuery { source } => {
            if let ForQuerySource::Static(sql) = source {
                validate_static_select_sql(sql)?;
            }
            Ok(())
        }
        Stmt::OpenCursor {
            source: OpenCursorSource::Static(sql),
            ..
        } => validate_static_select_sql(sql),
        Stmt::Perform { sql, .. } => validate_static_sql_text(&format!("select {sql}")),
        Stmt::ExecSql { sql } if should_validate_exec_sql(sql) => validate_static_sql_text(sql),
        _ => Ok(()),
    }
}

fn validate_static_select_sql(sql: &str) -> Result<(), ParseError> {
    validate_static_sql_text(sql)
}

fn should_validate_exec_sql(sql: &str) -> bool {
    let lowered = sql.to_ascii_lowercase();
    let words = lowered.split_whitespace().collect::<Vec<_>>();
    !sql.contains('$') && !words.iter().any(|word| *word == "into")
}

fn validate_static_sql_text(sql: &str) -> Result<(), ParseError> {
    if let Some(token) = malformed_select_alias_token(sql) {
        return Err(ParseError::UnexpectedToken {
            expected: "statement",
            actual: format!("syntax error at or near \"{token}\""),
        });
    }
    if should_defer_static_sql_validation(sql) {
        return Ok(());
    }
    match parse_statement(sql) {
        Ok(_) => Ok(()),
        Err(err) if is_static_sql_syntax_error(err.unpositioned()) => {
            Err(err.unpositioned().clone())
        }
        Err(_) => Ok(()),
    }
}

fn is_static_sql_syntax_error(err: &ParseError) -> bool {
    match err {
        ParseError::UnexpectedToken { actual, .. } => {
            actual.starts_with("syntax error at or near ")
        }
        ParseError::UnexpectedEof => true,
        _ => false,
    }
}

fn should_defer_static_sql_validation(sql: &str) -> bool {
    let Some(first_word) = sql.split_whitespace().next() else {
        return true;
    };
    if sql.contains(":=") || sql.contains('[') {
        return true;
    }
    matches!(
        first_word.to_ascii_lowercase().as_str(),
        "alter"
            | "call"
            | "close"
            | "comment"
            | "create"
            | "delete"
            | "drop"
            | "execute"
            | "fetch"
            | "insert"
            | "move"
            | "open"
            | "reset"
            | "select"
            | "set"
            | "truncate"
            | "update"
            | "values"
            | "with"
    )
}

fn malformed_select_alias_token(sql: &str) -> Option<String> {
    let trimmed = sql.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("select ") {
        return None;
    }
    let after_select = trimmed.get("select".len()..).unwrap_or_default();
    let after_select_lower = lower.get("select".len()..).unwrap_or_default();
    let select_list = after_select_lower
        .find(" from ")
        .and_then(|index| after_select.get(..index))
        .unwrap_or(after_select);
    for item in select_list.split(',') {
        let words = item.split_whitespace().take(3).collect::<Vec<_>>();
        if words.len() < 3 {
            continue;
        }
        if words.iter().all(|word| is_bare_identifier(word))
            && !words[1..].iter().any(|word| is_select_expr_keyword(word))
        {
            return Some(words[2].trim_matches('"').to_string());
        }
    }
    None
}

fn is_bare_identifier(word: &str) -> bool {
    let mut chars = word.chars();
    matches!(chars.next(), Some(ch) if ch == '_' || ch.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn is_select_expr_keyword(word: &str) -> bool {
    matches!(
        word.to_ascii_lowercase().as_str(),
        "and"
            | "as"
            | "between"
            | "case"
            | "collate"
            | "else"
            | "end"
            | "from"
            | "full"
            | "cross"
            | "inner"
            | "join"
            | "in"
            | "is"
            | "left"
            | "like"
            | "not"
            | "null"
            | "on"
            | "or"
            | "right"
            | "then"
            | "when"
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValidationCheckLevel {
    Warning,
    Error,
}

fn validate_shadowed_variables(
    block: &Block,
    arg_names: &[String],
    gucs: Option<&HashMap<String, String>>,
    notices: &mut Vec<PlpgsqlValidationNotice>,
) -> Result<(), ParseError> {
    let Some(level) = validation_extra_check_level(gucs, "shadowed_variables") else {
        return Ok(());
    };
    let mut scopes = vec![
        arg_names
            .iter()
            .filter(|name| !name.is_empty())
            .map(|name| name.to_ascii_lowercase())
            .collect::<std::collections::HashSet<_>>(),
    ];
    validate_shadowed_variables_in_block(block, level, &mut scopes, notices)
}

fn validate_shadowed_variables_in_block(
    block: &Block,
    level: ValidationCheckLevel,
    scopes: &mut Vec<std::collections::HashSet<String>>,
    notices: &mut Vec<PlpgsqlValidationNotice>,
) -> Result<(), ParseError> {
    scopes.push(std::collections::HashSet::new());
    for decl in &block.declarations {
        match decl {
            Decl::Var(decl) => validate_decl_name_shadow(&decl.name, level, scopes, notices)?,
            Decl::Alias(decl) => validate_decl_name_shadow(&decl.name, level, scopes, notices)?,
            Decl::Cursor(decl) => {
                validate_decl_name_shadow(&decl.name, level, scopes, notices)?;
                for param in &decl.params {
                    validate_decl_name_shadow(&param.name, level, scopes, notices)?;
                }
            }
        }
    }
    for stmt in &block.statements {
        validate_shadowed_variables_in_stmt(stmt, level, scopes, notices)?;
    }
    for handler in &block.exception_handlers {
        for stmt in &handler.statements {
            validate_shadowed_variables_in_stmt(stmt, level, scopes, notices)?;
        }
    }
    scopes.pop();
    Ok(())
}

fn validate_shadowed_variables_in_stmt(
    stmt: &Stmt,
    level: ValidationCheckLevel,
    scopes: &mut Vec<std::collections::HashSet<String>>,
    notices: &mut Vec<PlpgsqlValidationNotice>,
) -> Result<(), ParseError> {
    match stmt {
        Stmt::WithLine { stmt, .. } => {
            validate_shadowed_variables_in_stmt(stmt, level, scopes, notices)
        }
        Stmt::Block(block) => validate_shadowed_variables_in_block(block, level, scopes, notices),
        Stmt::If {
            branches,
            else_branch,
        } => {
            for (_, body) in branches {
                for stmt in body {
                    validate_shadowed_variables_in_stmt(stmt, level, scopes, notices)?;
                }
            }
            for stmt in else_branch {
                validate_shadowed_variables_in_stmt(stmt, level, scopes, notices)?;
            }
            Ok(())
        }
        Stmt::While { body, .. }
        | Stmt::Loop { body }
        | Stmt::ForInt { body, .. }
        | Stmt::ForQuery { body, .. }
        | Stmt::ForEach { body, .. } => {
            for stmt in body {
                validate_shadowed_variables_in_stmt(stmt, level, scopes, notices)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_decl_name_shadow(
    name: &str,
    level: ValidationCheckLevel,
    scopes: &mut [std::collections::HashSet<String>],
    notices: &mut Vec<PlpgsqlValidationNotice>,
) -> Result<(), ParseError> {
    let normalized = name.to_ascii_lowercase();
    if scopes.iter().rev().any(|scope| scope.contains(&normalized)) {
        let message = format!("variable \"{name}\" shadows a previously defined variable");
        match level {
            ValidationCheckLevel::Warning => notices.push(PlpgsqlValidationNotice {
                severity: "WARNING",
                sqlstate: "01000",
                message,
            }),
            ValidationCheckLevel::Error => {
                return Err(ParseError::DetailedError {
                    message,
                    detail: None,
                    hint: None,
                    sqlstate: "42712",
                });
            }
        }
    }
    if let Some(scope) = scopes.last_mut() {
        scope.insert(normalized);
    }
    Ok(())
}

fn validation_extra_check_level(
    gucs: Option<&HashMap<String, String>>,
    check: &str,
) -> Option<ValidationCheckLevel> {
    let gucs = gucs?;
    if validation_extra_check_enabled(gucs.get("plpgsql.extra_errors"), check) {
        Some(ValidationCheckLevel::Error)
    } else if validation_extra_check_enabled(gucs.get("plpgsql.extra_warnings"), check) {
        Some(ValidationCheckLevel::Warning)
    } else {
        None
    }
}

fn validation_extra_check_enabled(value: Option<&String>, check: &str) -> bool {
    value.is_some_and(|value| {
        value.eq_ignore_ascii_case("all")
            || value
                .split(',')
                .any(|item| item.trim().eq_ignore_ascii_case(check))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_context_text_trims_trailing_semicolon() {
        assert_eq!(normalize_sql_context_text(" select 1;  "), "select 1");
    }

    #[test]
    fn nonstandard_backslash_escapes_decode_sql_escapes() {
        assert_eq!(
            decode_nonstandard_backslash_escapes(r"a\'b\\c\141"),
            "a'b\\ca"
        );
    }

    #[test]
    fn proc_argtype_oids_parse_pg_proc_vector_text() {
        assert_eq!(parse_proc_argtype_oids(""), Some(Vec::new()));
        assert_eq!(parse_proc_argtype_oids("23 25"), Some(vec![23, 25]));
        assert_eq!(parse_proc_argtype_oids("23 nope"), None);
    }

    #[test]
    fn exception_conditions_and_raise_placeholders_match_postgres_names() {
        assert_eq!(
            exception_condition_name_sqlstate("division_by_zero"),
            Some("22012")
        );
        assert_eq!(
            exception_condition_name_sqlstate("wrong_object_type"),
            Some("42809")
        );
        assert_eq!(resolve_raise_sqlstate("unique_violation"), Some("23505"));
        assert_eq!(resolve_raise_sqlstate("U9999"), Some("U9999"));
        assert_eq!(exception_condition_name_sqlstate("no_such_condition"), None);
        assert_eq!(count_raise_placeholders("x % y %% z %"), 2);
    }

    #[test]
    fn plpgsql_directives_parse_first_matching_line() {
        assert_eq!(
            print_strict_params_directive("begin\n#print_strict_params on\nend"),
            Some(true)
        );
        assert_eq!(
            variable_conflict_directive("#variable_conflict use_column trailing"),
            Some(PlpgsqlVariableConflict::UseColumn)
        );
        assert_eq!(
            parse_variable_conflict_mode("use_variable"),
            Some(PlpgsqlVariableConflict::UseVariable)
        );
        assert_eq!(parse_variable_conflict_mode("bogus"), None);
    }

    #[test]
    fn plpgsql_internal_names_are_stable() {
        assert_eq!(positional_parameter_var_name(2), "__pgrust_plpgsql_param_2");
        assert_eq!(
            plpgsql_label_alias(1, 7, "a-b"),
            "__pgrust_plpgsql_label_1_7_a_b"
        );
        assert!(is_plpgsql_label_alias("__pgrust_plpgsql_label_1_7_a_b"));
        assert_eq!(plpgsql_var_alias(3), "__pgrust_plpgsql_var_3");
        assert!(is_internal_plpgsql_name("__pgrust_plpgsql_var_3"));
    }

    #[test]
    fn split_plpgsql_select_and_returning_into_clauses() {
        assert_eq!(
            split_select_into_target(" select into dst a from t "),
            Some(("dst".into(), "select a from t ".into()))
        );
        assert_eq!(
            split_select_with_into_targets("select a, b into strict x, y from t"),
            Some((
                vec!["x".into(), "y".into()],
                "select a, b from t".into(),
                true
            ))
        );
        assert_eq!(
            split_dml_returning_into_targets("update t set a = 1 returning a, b into x, y"),
            Some((
                "update t set a = 1 returning a, b".into(),
                vec!["x".into(), "y".into()]
            ))
        );
    }

    #[test]
    fn top_level_scanners_ignore_nested_and_quoted_text() {
        assert_eq!(
            find_next_top_level_keyword("select ' into ', f(a into b) into x", &["into"]),
            Some(29)
        );
        assert_eq!(
            split_top_level_csv("a, f(b, c), 'd,e'"),
            Some(vec!["a".into(), "f(b, c)".into(), "'d,e'".into(),])
        );
        assert_eq!(dollar_quote_tag_at("$tag$body$tag$", 0), Some("$tag$"));
        assert!(is_unsupported_plpgsql_transaction_command(
            "rollback to savepoint s"
        ));
        assert_eq!(transaction_command_name("  savepoint s"), "savepoint");
    }

    #[test]
    fn plpgsql_query_condition_rewrites_aggregate_comparison() {
        let parsed = parse_plpgsql_query_condition("count(*) = 0 from room").unwrap();
        assert_eq!(parsed.left_expr, "count(*)");
        assert_eq!(parsed.op, QueryCompareOp::Eq);
        assert_eq!(parsed.right_expr, "0");
        assert_eq!(parsed.from_clause, "room");
        assert_eq!(
            rewrite_plpgsql_query_condition("count(*) = 0 from room"),
            Some("(select count(*) from room) = 0".into())
        );
        assert_eq!(
            rewrite_plpgsql_assignment_query_expr("count(*) from room"),
            Some("(select count(*) from room)".into())
        );
        assert_eq!(
            split_top_level_comparison("count(*) is not distinct from 0"),
            Some(("count(*)", "is not distinct from", "0"))
        );
        assert!(looks_like_aggregate_expr("sum(x)"));
        assert!(!looks_like_aggregate_expr("x + 1"));
    }

    #[test]
    fn dynamic_sql_literals_and_shape_substitute_using_params() {
        assert_eq!(
            dynamic_sql_literal("'select $1, ''$2'''"),
            Some("select $1, '$2'".into())
        );
        assert_eq!(
            dynamic_shape_sql(
                "select $1, '$2', \"$3\";",
                &["a + b".into(), "ignored".into()]
            ),
            "select (a + b), '$2', \"$3\""
        );
        assert_eq!(dynamic_sql_literal("1 + 1"), None);
    }

    #[test]
    fn select_into_targets_and_identifier_positions_parse() {
        assert_eq!(
            parse_select_into_assign_target("rec.field").unwrap(),
            AssignTarget::Field {
                relation: "rec".into(),
                field: "field".into(),
            }
        );
        assert_eq!(
            parse_select_into_assign_targets("a, rec.field").unwrap(),
            vec![
                AssignTarget::Name("a".into()),
                AssignTarget::Field {
                    relation: "rec".into(),
                    field: "field".into(),
                },
            ]
        );
        assert_eq!(identifier_position("select foo, foobar", "foo"), Some(7));
        assert_eq!(identifier_position("select foobar", "foo"), None);
    }

    #[test]
    fn static_query_source_known_columns_handles_catalog_foreign_keys() {
        let columns = static_query_source_known_columns(
            " select * from pg_catalog.pg_get_catalog_foreign_keys(); ",
        )
        .unwrap();
        assert_eq!(
            columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "fktable", "fkcols", "pktable", "pkcols", "is_array", "is_opt"
            ]
        );
        assert_eq!(static_query_source_known_columns("select 1"), None);
    }

    #[test]
    fn runtime_sql_fallback_policies_match_parse_error_shape() {
        assert!(!should_fallback_to_runtime_sql(
            &ParseError::AmbiguousColumn("x".into())
        ));
        assert!(should_fallback_to_runtime_sql(&ParseError::UnknownTable(
            "t".into()
        )));
        assert!(should_defer_plpgsql_sql_to_runtime(
            &ParseError::UnknownTable("t".into())
        ));
        assert!(!should_defer_plpgsql_sql_to_runtime(
            &ParseError::UnknownColumn("c".into())
        ));
    }

    #[test]
    fn persistent_object_transition_table_references_are_detected() {
        assert_eq!(
            persistent_object_transition_table_reference_name(
                "create view v as select * from old_table",
                ["old_table"].into_iter()
            ),
            Some("old_table".into())
        );
        assert_eq!(
            persistent_object_transition_table_reference_name(
                "create temp table t as select * from old_table",
                ["old_table"].into_iter()
            ),
            None
        );
        assert!(sql_references_relation_name(
            "create view v as select * from \"old_table\"",
            "old_table"
        ));
    }

    #[test]
    fn runtime_sql_param_ids_use_reserved_high_range() {
        assert_eq!(PLPGSQL_RUNTIME_PARAM_BASE, 1_000_000_000);
        assert_eq!(runtime_sql_param_id(42), 1_000_000_042);
    }

    #[test]
    fn declared_cursor_args_context_formats_bound_arguments() {
        assert_eq!(
            declared_cursor_args_context(
                &[Some(" 1 + 2 ".into()), Some("name".into())],
                &["a".into(), "b".into()],
            ),
            Some("1 + 2 AS a, name AS b".into())
        );
        assert_eq!(declared_cursor_args_context(&[], &[]), None);
    }

    #[test]
    fn catalog_foreign_key_helpers_match_oidjoins_shapes() {
        assert!(is_catalog_foreign_key_query_sql(
            " select * from pg_catalog.pg_get_catalog_foreign_keys(); "
        ));
        assert!(is_catalog_foreign_key_check_sql(
            "select ctid from pg_class where not exists(select 1 from pg_type)"
        ));
        assert!(catalog_foreign_key_is_array("pg_proc", "proargtypes"));
        assert!(catalog_foreign_key_is_optional("pg_type", "typelem"));
        let Value::Array(values) = catalog_foreign_key_column_array("a,b") else {
            panic!("expected array");
        };
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].as_text(), Some("a"));
        assert_eq!(values[1].as_text(), Some("b"));
    }
}
