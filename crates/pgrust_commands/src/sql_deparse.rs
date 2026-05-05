use pgrust_nodes::SqlTypeKind;
use pgrust_nodes::primnodes::RelationDesc;

pub fn normalize_index_expression_sql(expr_sql: &str) -> String {
    let normalized = normalize_infix_spacing(expr_sql.trim());
    normalize_expression_text_collation(&normalized).unwrap_or(normalized)
}

pub fn normalize_stored_expr_sql(expr_sql: &str) -> String {
    normalize_keywords(&normalize_infix_spacing(strip_outer_parens_once(
        expr_sql.trim(),
    )))
}

pub fn normalize_check_expr_sql(expr_sql: &str) -> String {
    format!("({})", normalize_stored_expr_sql(expr_sql))
}

pub fn normalize_index_predicate_sql(
    predicate_sql: &str,
    relation_desc: Option<&RelationDesc>,
) -> String {
    let normalized = normalize_keywords(&normalize_infix_spacing(strip_outer_parens_once(
        predicate_sql.trim(),
    )));
    let normalized =
        normalize_predicate_text_collation(&normalized, relation_desc).unwrap_or(normalized);
    relation_desc
        .and_then(|desc| normalize_simple_text_comparison(&normalized, desc))
        .unwrap_or(normalized)
}

fn normalize_simple_text_comparison(sql: &str, desc: &RelationDesc) -> Option<String> {
    let (left, operator, right) = split_top_level_comparison(sql)?;
    let operator = match operator {
        "=" => "=",
        "!=" | "<>" => "<>",
        _ => return None,
    };
    let left = left.trim();
    let right = right.trim();
    let column = desc
        .columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(left))?;
    if column.sql_type.kind != SqlTypeKind::Text || column.sql_type.is_array {
        return None;
    }
    if !is_simple_string_literal(right) || right.contains("::") {
        return None;
    }
    Some(format!("{left} {operator} {right}::text"))
}

fn is_simple_string_literal(sql: &str) -> bool {
    sql.len() >= 2 && sql.starts_with('\'') && sql.ends_with('\'')
}

fn normalize_expression_text_collation(sql: &str) -> Option<String> {
    let (expr, collation) = split_top_level_collate(strip_outer_parens_once(sql))?;
    let cast = normalize_text_cast_operand(expr, None)?;
    Some(format!("({}) COLLATE {}", cast.sql, collation.trim()))
}

fn normalize_predicate_text_collation(sql: &str, desc: Option<&RelationDesc>) -> Option<String> {
    let (left, operator, right) = split_top_level_comparison(sql)?;
    let mut left = normalize_predicate_operand(left, desc);
    let right = normalize_predicate_operand(right, desc);
    if right.textish && is_parenthesized_string_literal(left.original) {
        left = NormalizedSql {
            sql: format!("{}::text", strip_outer_parens_once(left.original)),
            textish: true,
            changed: true,
            original: left.original,
        };
    }
    if !left.changed && !right.changed {
        return None;
    }
    let operator = if operator == "!=" { "<>" } else { operator };
    Some(format!("{} {} {}", left.sql, operator, right.sql))
}

#[derive(Debug, Clone)]
struct NormalizedSql<'a> {
    sql: String,
    textish: bool,
    changed: bool,
    original: &'a str,
}

fn normalize_predicate_operand<'a>(sql: &'a str, desc: Option<&RelationDesc>) -> NormalizedSql<'a> {
    let trimmed = sql.trim();
    if let Some((expr, collation)) = split_top_level_collate(trimmed)
        && let Some(cast) = normalize_text_cast_operand(expr, desc)
    {
        return NormalizedSql {
            sql: format!("({} COLLATE {})", cast.sql, collation.trim()),
            textish: true,
            changed: true,
            original: trimmed,
        };
    }
    if let Some(cast) = normalize_text_cast_operand(trimmed, desc) {
        return NormalizedSql {
            sql: cast.sql,
            textish: true,
            changed: true,
            original: trimmed,
        };
    }
    NormalizedSql {
        sql: trimmed.to_string(),
        textish: false,
        changed: false,
        original: trimmed,
    }
}

struct NormalizedTextCast {
    sql: String,
}

fn normalize_text_cast_operand(
    sql: &str,
    desc: Option<&RelationDesc>,
) -> Option<NormalizedTextCast> {
    let trimmed = strip_outer_parens_once(sql.trim());
    let (base, target_type) = split_top_level_cast(trimmed)?;
    if !is_text_type_name(target_type) {
        return None;
    }
    let base = strip_outer_parens_once(base.trim());
    if desc
        .and_then(|relation_desc| column_sql_type_kind(relation_desc, base))
        .is_some_and(|kind| kind == SqlTypeKind::Text)
    {
        return Some(NormalizedTextCast {
            sql: base.to_string(),
        });
    }
    Some(NormalizedTextCast {
        sql: pg_style_text_cast(base),
    })
}

fn pg_style_text_cast(base: &str) -> String {
    let base = strip_outer_parens_once(base.trim());
    if is_simple_string_literal(base) {
        format!("{base}::text")
    } else {
        format!("({base})::text")
    }
}

fn is_parenthesized_string_literal(sql: &str) -> bool {
    let trimmed = sql.trim();
    trimmed.starts_with('(')
        && trimmed.ends_with(')')
        && is_simple_string_literal(strip_outer_parens_once(trimmed))
}

fn column_sql_type_kind(desc: &RelationDesc, candidate: &str) -> Option<SqlTypeKind> {
    let name = unquote_simple_identifier(candidate.trim());
    desc.columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(name))
        .map(|column| column.sql_type.kind)
}

fn unquote_simple_identifier(input: &str) -> &str {
    input
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
        .unwrap_or(input)
}

fn is_text_type_name(input: &str) -> bool {
    let normalized = input.trim().trim_matches('"').to_ascii_lowercase();
    matches!(normalized.as_str(), "text" | "pg_catalog.text")
}

fn split_top_level_comparison(input: &str) -> Option<(&str, &'static str, &str)> {
    for (idx, op) in top_level_operator_positions(
        input,
        &[" >= ", " <= ", " <> ", " != ", " = ", " > ", " < "],
    ) {
        return Some((&input[..idx], op.trim(), &input[idx + op.len()..]));
    }
    None
}

fn split_top_level_collate(input: &str) -> Option<(&str, &str)> {
    let idx = top_level_keyword_position(input, " collate ")?;
    Some((&input[..idx], &input[idx + " collate ".len()..]))
}

fn split_top_level_cast(input: &str) -> Option<(&str, &str)> {
    let idx = top_level_token_position(input, "::")?;
    Some((&input[..idx], &input[idx + "::".len()..]))
}

fn top_level_operator_positions<'a>(
    input: &'a str,
    operators: &'static [&'static str],
) -> Vec<(usize, &'static str)> {
    let mut out = Vec::new();
    scan_top_level(input, |idx| {
        for operator in operators {
            if input[idx..].starts_with(operator) {
                out.push((idx, *operator));
                return true;
            }
        }
        false
    });
    out
}

fn top_level_keyword_position(input: &str, keyword: &str) -> Option<usize> {
    let lower = input.to_ascii_lowercase();
    let mut found = None;
    scan_top_level(input, |idx| {
        if lower[idx..].starts_with(keyword) {
            found = Some(idx);
            true
        } else {
            false
        }
    });
    found
}

fn top_level_token_position(input: &str, token: &str) -> Option<usize> {
    let mut found = None;
    scan_top_level(input, |idx| {
        if input[idx..].starts_with(token) {
            found = Some(idx);
            true
        } else {
            false
        }
    });
    found
}

fn scan_top_level(mut input: &str, mut at_top_level: impl FnMut(usize) -> bool) {
    let original = input;
    let mut depth = 0i32;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    while let Some((relative_idx, ch)) = input.char_indices().next() {
        let idx = original.len() - input.len() + relative_idx;
        if in_single_quote {
            input = &input[ch.len_utf8()..];
            if ch == '\'' {
                if input.starts_with('\'') {
                    input = &input[1..];
                    continue;
                }
                in_single_quote = false;
            }
            continue;
        }
        if in_double_quote {
            input = &input[ch.len_utf8()..];
            if ch == '"' {
                in_double_quote = false;
            }
            continue;
        }
        match ch {
            '\'' => in_single_quote = true,
            '"' => in_double_quote = true,
            '(' => depth += 1,
            ')' => depth -= 1,
            _ if depth == 0 && at_top_level(idx) => return,
            _ => {}
        }
        input = &input[ch.len_utf8()..];
    }
}

fn normalize_infix_spacing(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 8);
    let mut chars = input.char_indices().peekable();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    while let Some((idx, ch)) = chars.next() {
        if in_single_quote {
            out.push(ch);
            if ch == '\'' {
                if matches!(chars.peek(), Some((_, '\''))) {
                    if let Some((_, escaped)) = chars.next() {
                        out.push(escaped);
                    }
                } else {
                    in_single_quote = false;
                }
            }
            continue;
        }
        if in_double_quote {
            out.push(ch);
            if ch == '"' {
                in_double_quote = false;
            }
            continue;
        }
        match ch {
            '\'' => {
                in_single_quote = true;
                out.push(ch);
            }
            '"' => {
                in_double_quote = true;
                out.push(ch);
            }
            '|' if input[idx + ch.len_utf8()..].starts_with('|') => {
                let _ = chars.next();
                push_spaced_operator(&mut out, "||");
            }
            '>' | '<' | '!' => {
                let rest = &input[idx + ch.len_utf8()..];
                if rest.starts_with('=') || (ch == '<' && rest.starts_with('>')) {
                    let _ = chars.next();
                    let mut operator = String::with_capacity(2);
                    operator.push(ch);
                    operator.push(if rest.starts_with('=') { '=' } else { '>' });
                    push_spaced_operator(&mut out, &operator);
                } else {
                    let mut operator = String::with_capacity(1);
                    operator.push(ch);
                    push_spaced_operator(&mut out, &operator);
                }
            }
            '=' => push_spaced_operator(&mut out, "="),
            '+' => push_spaced_operator(&mut out, "+"),
            _ => out.push(ch),
        }
    }
    collapse_spaces(&out)
}

fn normalize_keywords(input: &str) -> String {
    let mut out = input.to_string();
    for (from, to) in [
        (" is not null", " IS NOT NULL"),
        (" is null", " IS NULL"),
        (" and ", " AND "),
        (" or ", " OR "),
        (" not ", " NOT "),
    ] {
        out = replace_ascii_case_insensitive(&out, from, to);
    }
    out
}

fn replace_ascii_case_insensitive(input: &str, from: &str, to: &str) -> String {
    let lower = input.to_ascii_lowercase();
    let needle = from.to_ascii_lowercase();
    let mut out = String::with_capacity(input.len());
    let mut pos = 0usize;
    while let Some(relative) = lower[pos..].find(&needle) {
        let start = pos + relative;
        out.push_str(&input[pos..start]);
        out.push_str(to);
        pos = start + from.len();
    }
    out.push_str(&input[pos..]);
    out
}

fn push_spaced_operator(out: &mut String, operator: &str) {
    while out.ends_with(' ') {
        out.pop();
    }
    if !out.is_empty() && !out.ends_with('(') {
        out.push(' ');
    }
    out.push_str(operator);
    out.push(' ');
}

fn collapse_spaces(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut previous_space = false;
    for ch in input.chars() {
        if ch.is_whitespace() {
            if !previous_space {
                out.push(' ');
                previous_space = true;
            }
        } else {
            out.push(ch);
            previous_space = false;
        }
    }
    out.trim().to_string()
}

fn strip_outer_parens_once(input: &str) -> &str {
    let trimmed = input.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return trimmed;
    }
    let mut depth = 0i32;
    let mut in_single_quote = false;
    for (idx, ch) in trimmed.char_indices() {
        if in_single_quote {
            if ch == '\'' {
                in_single_quote = false;
            }
            continue;
        }
        match ch {
            '\'' => in_single_quote = true,
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 && idx + ch.len_utf8() < trimmed.len() {
                    return trimmed;
                }
            }
            _ => {}
        }
    }
    trimmed[1..trimmed.len().saturating_sub(1)].trim()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_catalog_store::catalog::column_desc;
    use pgrust_nodes::SqlType;
    use pgrust_nodes::primnodes::RelationDesc;

    #[test]
    fn normalizes_index_expression_operator_spacing() {
        assert_eq!(normalize_index_expression_sql("f2||f1"), "f2 || f1");
        assert_eq!(normalize_index_expression_sql("(f2||f1)"), "(f2 || f1)");
    }

    #[test]
    fn normalizes_check_expression_sql() {
        assert_eq!(
            normalize_check_expr_sql("aa is not null"),
            "(aa IS NOT NULL)"
        );
        assert_eq!(normalize_check_expr_sql("f2>0"), "(f2 > 0)");
        assert_eq!(normalize_check_expr_sql("a<>0"), "(a <> 0)");
    }

    #[test]
    fn normalizes_simple_text_predicates() {
        let desc = RelationDesc {
            columns: vec![column_desc("f1", SqlType::new(SqlTypeKind::Text), false)],
        };
        assert_eq!(
            normalize_index_predicate_sql("(f1='a')", Some(&desc)),
            "f1 = 'a'::text"
        );
        assert_eq!(
            normalize_index_predicate_sql("f1>='a'", Some(&desc)),
            "f1 >= 'a'"
        );
        assert_eq!(
            normalize_index_predicate_sql("f1!='a'", Some(&desc)),
            "f1 <> 'a'::text"
        );
    }

    #[test]
    fn normalizes_text_collation_index_expression() {
        assert_eq!(
            normalize_index_expression_sql("c1::text COLLATE \"C\""),
            "((c1)::text) COLLATE \"C\""
        );
    }

    #[test]
    fn normalizes_text_collation_index_predicates() {
        let desc = RelationDesc {
            columns: vec![
                column_desc("c1", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("c2", SqlType::new(SqlTypeKind::Bool), false),
            ],
        };
        assert_eq!(
            normalize_index_predicate_sql("c1::text > 500000000::text COLLATE \"C\"", Some(&desc)),
            "(c1)::text > ((500000000)::text COLLATE \"C\")"
        );
        assert_eq!(
            normalize_index_predicate_sql("('-H') >= (c2::TEXT) COLLATE \"C\"", Some(&desc)),
            "'-H'::text >= ((c2)::text COLLATE \"C\")"
        );

        let text_desc = RelationDesc {
            columns: vec![column_desc("c2", SqlType::new(SqlTypeKind::Text), false)],
        };
        assert_eq!(
            normalize_index_predicate_sql("('-H') >= (c2::TEXT) COLLATE \"C\"", Some(&text_desc)),
            "'-H'::text >= (c2 COLLATE \"C\")"
        );
    }
}
