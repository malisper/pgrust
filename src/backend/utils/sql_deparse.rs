use crate::backend::parser::SqlTypeKind;
use crate::include::nodes::primnodes::RelationDesc;

pub(crate) fn normalize_index_expression_sql(expr_sql: &str) -> String {
    normalize_infix_spacing(expr_sql.trim())
}

pub(crate) fn normalize_stored_expr_sql(expr_sql: &str) -> String {
    normalize_keywords(&normalize_infix_spacing(strip_outer_parens_once(
        expr_sql.trim(),
    )))
}

pub(crate) fn normalize_check_expr_sql(expr_sql: &str) -> String {
    format!("({})", normalize_stored_expr_sql(expr_sql))
}

pub(crate) fn normalize_index_predicate_sql(
    predicate_sql: &str,
    relation_desc: Option<&RelationDesc>,
) -> String {
    let normalized = normalize_infix_spacing(strip_outer_parens_once(predicate_sql.trim()));
    relation_desc
        .and_then(|desc| normalize_simple_text_equality(&normalized, desc))
        .unwrap_or(normalized)
}

fn normalize_simple_text_equality(sql: &str, desc: &RelationDesc) -> Option<String> {
    let (left, right) = sql.split_once(" = ")?;
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
    Some(format!("{left} = {right}::text"))
}

fn is_simple_string_literal(sql: &str) -> bool {
    sql.len() >= 2 && sql.starts_with('\'') && sql.ends_with('\'')
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
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::include::nodes::primnodes::RelationDesc;

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
            columns: vec![crate::backend::catalog::catalog::column_desc(
                "f1",
                SqlType::new(SqlTypeKind::Text),
                false,
            )],
        };
        assert_eq!(
            normalize_index_predicate_sql("(f1='a')", Some(&desc)),
            "f1 = 'a'::text"
        );
        assert_eq!(
            normalize_index_predicate_sql("f1>='a'", Some(&desc)),
            "f1 >= 'a'"
        );
    }
}
