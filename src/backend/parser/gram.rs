use pest::Parser as _;
use pest::iterators::Pair;
use pest_derive::Parser;

use super::comments::{
    normalize_position_syntax_preserving_layout, normalize_string_continuation_preserving_layout,
    strip_sql_comments_preserving_layout,
};
use super::parsenodes::*;
use crate::backend::executor::{AggFunc, Value};
use crate::include::nodes::datum::BitString;

#[derive(Parser)]
#[grammar = "backend/parser/gram.pest"]
struct SqlParser;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseOptions {
    pub standard_conforming_strings: bool,
}

impl Default for ParseOptions {
    fn default() -> Self {
        Self {
            standard_conforming_strings: true,
        }
    }
}

pub fn parse_statement(sql: &str) -> Result<Statement, ParseError> {
    parse_statement_with_options(sql, ParseOptions::default())
}

pub fn parse_statement_with_options(
    sql: &str,
    options: ParseOptions,
) -> Result<Statement, ParseError> {
    // :HACK: Some parser paths currently recurse deeply enough to overflow the
    // default Rust test-thread stack on modest statements (for example certain
    // `unnest(...)` forms). Run parsing on a dedicated larger stack until the
    // underlying recursion is flattened.
    run_with_parser_stack({
        let sql = sql.to_string();
        move || parse_statement_with_options_inner(sql, options)
    })
}

fn parse_statement_with_options_inner(
    sql: String,
    options: ParseOptions,
) -> Result<Statement, ParseError> {
    let sql = normalize_string_continuation_preserving_layout(&sql);
    let sql = strip_sql_comments_preserving_layout(&sql);
    validate_unicode_string_literals(&sql, options)?;
    let sql = normalize_position_syntax_preserving_layout(&sql);
    if let Some(stmt) = try_parse_domain_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_create_function_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_unsupported_statement(&sql) {
        if matches!(
            stmt,
            Statement::Unsupported(UnsupportedStatement {
                feature: "ROLE management",
                ..
            })
        ) {
            return Ok(stmt);
        }
    }
    match SqlParser::parse(Rule::statement, &sql) {
        Ok(mut pairs) => build_statement(pairs.next().ok_or(ParseError::UnexpectedEof)?),
        Err(err) => {
            try_parse_unsupported_statement(&sql).ok_or_else(|| map_pest_error("statement", err))
        }
    }
}

pub fn parse_expr(sql: &str) -> Result<SqlExpr, ParseError> {
    let sql = strip_sql_comments_preserving_layout(sql);
    SqlParser::parse(Rule::expr, &sql)
        .map_err(|e| map_pest_error("expression", e))
        .and_then(|mut pairs| {
            let pair = pairs.next().ok_or(ParseError::UnexpectedEof)?;
            if pairs.next().is_some() {
                return Err(ParseError::UnexpectedToken {
                    expected: "expression",
                    actual: sql.clone(),
                });
            }
            build_expr(pair)
        })
}

pub fn parse_type_name(sql: &str) -> Result<RawTypeName, ParseError> {
    let sql = strip_sql_comments_preserving_layout(sql);
    let lowered = sql.trim().to_ascii_lowercase();
    match lowered.as_str() {
        "int2vector" => return Ok(RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int2Vector))),
        "oidvector" => return Ok(RawTypeName::Builtin(SqlType::new(SqlTypeKind::OidVector))),
        "name" => return Ok(RawTypeName::Builtin(SqlType::new(SqlTypeKind::Name))),
        "pg_node_tree" => return Ok(RawTypeName::Builtin(SqlType::new(SqlTypeKind::PgNodeTree))),
        _ => {}
    }
    SqlParser::parse(Rule::type_name, &sql)
        .map_err(|e| map_pest_error("type name", e))
        .and_then(|mut pairs| {
            let pair = pairs.next().ok_or(ParseError::UnexpectedEof)?;
            if pairs.next().is_some() {
                return Err(ParseError::UnexpectedToken {
                    expected: "type name",
                    actual: sql.clone(),
                });
            }
            Ok(build_type_name(pair))
        })
}

#[cfg(not(target_arch = "wasm32"))]
fn run_with_parser_stack<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    std::thread::Builder::new()
        .name("pgrust-parser".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(f)
        .expect("spawn parser thread")
        .join()
        .expect("parser thread panicked")
}

#[cfg(target_arch = "wasm32")]
fn run_with_parser_stack<F, T>(f: F) -> T
where
    F: FnOnce() -> T,
{
    // Browser wasm cannot spawn threads without a different runtime model.
    // Run inline there and keep the larger parser stack workaround only on
    // native targets.
    f()
}

fn try_parse_unsupported_statement(sql: &str) -> Option<Statement> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    let feature = if lowered.starts_with("alter table ") {
        Some("ALTER TABLE form")
    } else if lowered.starts_with("alter index ") {
        Some("ALTER INDEX")
    } else if lowered.starts_with("alter view ") {
        Some("ALTER VIEW")
    } else if lowered.starts_with("set role ") || lowered == "reset role" {
        Some("ROLE management")
    } else if lowered.starts_with("drop index ") {
        Some("DROP INDEX")
    } else if lowered.starts_with("drop table ") {
        Some("DROP TABLE form")
    } else if lowered.starts_with("drop domain ") {
        Some("DROP DOMAIN")
    } else if lowered.starts_with("drop rule ") {
        Some("DROP RULE")
    } else if lowered.starts_with("comment on column ") {
        Some("COMMENT ON COLUMN")
    } else if lowered.starts_with("comment on constraint ") {
        Some("COMMENT ON CONSTRAINT")
    } else if lowered.starts_with("comment on index ") {
        Some("COMMENT ON INDEX")
    } else if lowered.starts_with("create domain ") {
        Some("CREATE DOMAIN")
    } else if lowered.starts_with("create rule ") {
        Some("CREATE RULE")
    } else if lowered.starts_with("copy ") && lowered.contains(" to ") {
        Some("COPY TO")
    } else if lowered.starts_with("create unique index ") {
        Some("CREATE UNIQUE INDEX")
    } else if lowered.starts_with("create index ") {
        Some("CREATE INDEX form")
    } else if lowered.starts_with("create view ") {
        Some("CREATE VIEW form")
    } else if lowered.starts_with("create temp table ") {
        Some("CREATE TEMP TABLE form")
    } else if lowered.starts_with("create table ") {
        Some("CREATE TABLE form")
    } else if lowered.starts_with("select ") || lowered.starts_with("with ") {
        Some("SELECT form")
    } else if lowered.starts_with("delete from ") {
        Some("DELETE form")
    } else {
        None
    }?;

    Some(Statement::Unsupported(UnsupportedStatement {
        sql: trimmed.into(),
        feature,
    }))
}

fn try_parse_domain_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if lowered.starts_with("create domain ") {
        return build_create_domain_statement(trimmed)
            .map(|stmt| Some(Statement::CreateDomain(stmt)));
    }
    if lowered.starts_with("drop domain ") {
        return build_drop_domain_statement(trimmed).map(|stmt| Some(Statement::DropDomain(stmt)));
    }
    if lowered.starts_with("comment on domain ") {
        return build_comment_on_domain_statement(trimmed)
            .map(|stmt| Some(Statement::CommentOnDomain(stmt)));
    }
    Ok(None)
}

fn try_parse_create_function_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if lowered.starts_with("create or replace function ") {
        return Err(ParseError::FeatureNotSupported(
            "CREATE OR REPLACE FUNCTION is not supported yet".into(),
        ));
    }
    if !lowered.starts_with("create function ") {
        return Ok(None);
    }
    Ok(Some(Statement::CreateFunction(
        build_create_function_statement(trimmed)?,
    )))
}

fn build_create_function_statement(sql: &str) -> Result<CreateFunctionStatement, ParseError> {
    let prefix = "create function";
    let Some(rest) = sql.get(prefix.len()..) else {
        return Err(ParseError::UnexpectedToken {
            expected: "CREATE FUNCTION name(args) ...",
            actual: sql.into(),
        });
    };
    let rest = rest.trim_start();
    let ((schema_name, function_name), rest) = parse_create_function_name(rest)?;
    let (arg_list, mut rest) = take_parenthesized_segment(rest)?;
    let args = parse_create_function_args(&arg_list)?;

    let mut return_spec = None;
    let mut language = None;
    let mut body = None;

    while !rest.trim_start().is_empty() {
        rest = rest.trim_start();
        if keyword_at_start(rest, "returns") {
            if return_spec.is_some() {
                return Err(ParseError::UnexpectedToken {
                    expected: "single RETURNS clause",
                    actual: rest.into(),
                });
            }
            let (parsed, next_rest) = parse_create_function_returns(rest)?;
            return_spec = Some(parsed);
            rest = next_rest;
            continue;
        }
        if keyword_at_start(rest, "language") {
            if language.is_some() {
                return Err(ParseError::UnexpectedToken {
                    expected: "single LANGUAGE clause",
                    actual: rest.into(),
                });
            }
            let (parsed, next_rest) = parse_create_function_language(rest)?;
            language = Some(parsed);
            rest = next_rest;
            continue;
        }
        if keyword_at_start(rest, "as") {
            if body.is_some() {
                return Err(ParseError::UnexpectedToken {
                    expected: "single AS clause",
                    actual: rest.into(),
                });
            }
            let (parsed, next_rest) = parse_create_function_body(rest)?;
            body = Some(parsed);
            rest = next_rest;
            continue;
        }
        return Err(ParseError::FeatureNotSupported(format!(
            "unsupported CREATE FUNCTION clause: {}",
            rest.split_whitespace().next().unwrap_or(rest)
        )));
    }

    let has_out_args = args
        .iter()
        .any(|arg| matches!(arg.mode, FunctionArgMode::Out | FunctionArgMode::InOut));
    let return_spec = match return_spec {
        Some(CreateFunctionReturnSpec::Type {
            ty: RawTypeName::Record,
            setof,
        }) if has_out_args => CreateFunctionReturnSpec::DerivedFromOutArgs {
            setof_record: setof,
        },
        Some(spec) => spec,
        None if has_out_args => CreateFunctionReturnSpec::DerivedFromOutArgs {
            setof_record: false,
        },
        None => {
            return Err(ParseError::UnexpectedToken {
                expected: "RETURNS clause or OUT/INOUT arguments",
                actual: sql.into(),
            });
        }
    };

    Ok(CreateFunctionStatement {
        schema_name,
        function_name,
        args,
        return_spec,
        language: language.ok_or(ParseError::UnexpectedEof)?,
        body: body.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn parse_create_function_name(input: &str) -> Result<((Option<String>, String), &str), ParseError> {
    let (first, mut rest) = parse_sql_identifier(input)?;
    rest = rest.trim_start();
    if let Some(after_dot) = rest.strip_prefix('.') {
        let (second, rest) = parse_sql_identifier(after_dot.trim_start())?;
        return Ok(((Some(first), second), rest));
    }
    Ok(((None, first), rest))
}

fn parse_create_function_args(input: &str) -> Result<Vec<CreateFunctionArg>, ParseError> {
    let items = split_top_level_items(input, ',')?;
    if items.len() == 1 && items[0].trim().is_empty() {
        return Ok(Vec::new());
    }
    items
        .into_iter()
        .filter(|item| !item.trim().is_empty())
        .map(|item| parse_create_function_arg(&item))
        .collect()
}

fn parse_create_function_arg(input: &str) -> Result<CreateFunctionArg, ParseError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "function argument",
            actual: input.into(),
        });
    }
    let lowered = trimmed.to_ascii_lowercase();
    if lowered.contains(" default ")
        || lowered.contains(":=")
        || lowered.starts_with("variadic ")
        || lowered.contains(" variadic ")
    {
        return Err(ParseError::FeatureNotSupported(
            "default arguments and VARIADIC are not supported in CREATE FUNCTION".into(),
        ));
    }

    let (mode, rest) = if keyword_at_start(trimmed, "inout") {
        (FunctionArgMode::InOut, consume_keyword(trimmed, "inout"))
    } else if keyword_at_start(trimmed, "out") {
        (FunctionArgMode::Out, consume_keyword(trimmed, "out"))
    } else if keyword_at_start(trimmed, "in") {
        (FunctionArgMode::In, consume_keyword(trimmed, "in"))
    } else {
        (FunctionArgMode::In, trimmed)
    };
    let (name, rest) = parse_sql_identifier(rest.trim_start())?;
    let type_sql = rest.trim();
    if type_sql.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "argument type name",
            actual: input.into(),
        });
    }
    Ok(CreateFunctionArg {
        mode,
        name,
        ty: parse_type_name(type_sql)?,
    })
}

fn parse_create_function_returns(
    input: &str,
) -> Result<(CreateFunctionReturnSpec, &str), ParseError> {
    let rest = consume_keyword(input.trim_start(), "returns").trim_start();
    if keyword_at_start(rest, "table") {
        let rest = consume_keyword(rest, "table").trim_start();
        let (columns_sql, rest) = take_parenthesized_segment(rest)?;
        return Ok((
            CreateFunctionReturnSpec::Table(parse_create_function_table_columns(&columns_sql)?),
            rest,
        ));
    }

    let setof = keyword_at_start(rest, "setof");
    let type_rest = if setof {
        consume_keyword(rest, "setof").trim_start()
    } else {
        rest
    };
    let boundary =
        find_next_top_level_keyword(type_rest, &["language", "as"]).unwrap_or(type_rest.len());
    let type_sql = type_rest[..boundary].trim();
    if type_sql.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "return type name",
            actual: input.into(),
        });
    }
    Ok((
        CreateFunctionReturnSpec::Type {
            ty: parse_type_name(type_sql)?,
            setof,
        },
        &type_rest[boundary..],
    ))
}

fn parse_create_function_table_columns(
    input: &str,
) -> Result<Vec<CreateFunctionTableColumn>, ParseError> {
    split_top_level_items(input, ',')?
        .into_iter()
        .filter(|item| !item.trim().is_empty())
        .map(|item| {
            let (name, rest) = parse_sql_identifier(item.trim())?;
            let type_sql = rest.trim();
            if type_sql.is_empty() {
                return Err(ParseError::UnexpectedToken {
                    expected: "TABLE column type",
                    actual: item.to_string(),
                });
            }
            Ok(CreateFunctionTableColumn {
                name,
                ty: parse_type_name(type_sql)?,
            })
        })
        .collect()
}

fn parse_create_function_language(input: &str) -> Result<(String, &str), ParseError> {
    let rest = consume_keyword(input.trim_start(), "language").trim_start();
    let (language, rest) = parse_sql_identifier(rest)?;
    Ok((language, rest))
}

fn parse_create_function_body(input: &str) -> Result<(String, &str), ParseError> {
    let rest = consume_keyword(input.trim_start(), "as").trim_start();
    let token_len = scan_string_literal_token_len(rest).ok_or(ParseError::UnexpectedToken {
        expected: "function body string literal",
        actual: rest.into(),
    })?;
    Ok((
        decode_string_literal(&rest[..token_len])?,
        &rest[token_len..],
    ))
}

fn parse_sql_identifier(input: &str) -> Result<(String, &str), ParseError> {
    let input = input.trim_start();
    let Some(first) = input.chars().next() else {
        return Err(ParseError::UnexpectedEof);
    };
    if first == '"' {
        let mut i = 1usize;
        let bytes = input.as_bytes();
        while i < bytes.len() {
            if bytes[i] == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                } else {
                    return Ok((input[1..i].replace("\"\"", "\""), &input[i + 1..]));
                }
            } else {
                i += 1;
            }
        }
        return Err(ParseError::UnexpectedEof);
    }
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(ParseError::UnexpectedToken {
            expected: "SQL identifier",
            actual: input.into(),
        });
    }
    let mut end = first.len_utf8();
    for ch in input[end..].chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            end += ch.len_utf8();
        } else {
            break;
        }
    }
    Ok((input[..end].to_ascii_lowercase(), &input[end..]))
}

fn take_parenthesized_segment(input: &str) -> Result<(String, &str), ParseError> {
    let input = input.trim_start();
    if !input.starts_with('(') {
        return Err(ParseError::UnexpectedToken {
            expected: "(",
            actual: input.into(),
        });
    }
    let bytes = input.as_bytes();
    let mut depth = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i = parse_delimited_token_end(bytes, i, b'\'');
                continue;
            }
            b'"' => {
                i = parse_delimited_token_end(bytes, i, b'"');
                continue;
            }
            b'$' => {
                if let Some(end) = scan_dollar_string_token_end(input, i) {
                    i = end;
                    continue;
                }
            }
            b'(' => depth += 1,
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Ok((input[1..i].to_string(), &input[i + 1..]));
                }
            }
            _ => {}
        }
        i += 1;
    }
    Err(ParseError::UnexpectedEof)
}

fn split_top_level_items(input: &str, separator: char) -> Result<Vec<String>, ParseError> {
    let mut items = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let bytes = input.as_bytes();
    let sep = separator as u8;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i = parse_delimited_token_end(bytes, i, b'\'');
                continue;
            }
            b'"' => {
                i = parse_delimited_token_end(bytes, i, b'"');
                continue;
            }
            b'$' => {
                if let Some(end) = scan_dollar_string_token_end(input, i) {
                    i = end;
                    continue;
                }
            }
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth = depth.saturating_sub(1),
            byte if byte == sep && depth == 0 => {
                items.push(input[start..i].trim().to_string());
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    items.push(input[start..].trim().to_string());
    Ok(items)
}

fn find_next_top_level_keyword(input: &str, keywords: &[&str]) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut depth = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i = parse_delimited_token_end(bytes, i, b'\'');
                continue;
            }
            b'"' => {
                i = parse_delimited_token_end(bytes, i, b'"');
                continue;
            }
            b'$' => {
                if let Some(end) = scan_dollar_string_token_end(input, i) {
                    i = end;
                    continue;
                }
            }
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth = depth.saturating_sub(1),
            _ if depth == 0 => {
                if keywords
                    .iter()
                    .any(|keyword| keyword_at_boundary(input, i, keyword))
                {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn scan_string_literal_token_len(input: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    match bytes.first().copied()? {
        b'\'' => Some(parse_delimited_token_end(bytes, 0, b'\'')),
        b'e' | b'E' if bytes.get(1) == Some(&b'\'') => {
            Some(parse_delimited_token_end(bytes, 1, b'\''))
        }
        b'$' => scan_dollar_string_token_end(input, 0),
        _ => None,
    }
}

fn scan_dollar_string_token_end(input: &str, start: usize) -> Option<usize> {
    let suffix = &input[start..];
    if !suffix.starts_with('$') {
        return None;
    }
    let tag_end = suffix[1..].find('$')? + 1;
    let tag = &suffix[..=tag_end];
    let rest = &suffix[tag_end + 1..];
    let closing = rest.find(tag)?;
    Some(start + tag_end + 1 + closing + tag.len())
}

fn keyword_at_start(input: &str, keyword: &str) -> bool {
    keyword_at_boundary(input.trim_start(), 0, keyword)
}

fn keyword_at_boundary(input: &str, start: usize, keyword: &str) -> bool {
    let end = start.saturating_add(keyword.len());
    input
        .get(start..end)
        .is_some_and(|slice| slice.eq_ignore_ascii_case(keyword))
        && input
            .get(end..)
            .and_then(|slice| slice.chars().next())
            .is_none_or(|ch| !(ch.is_ascii_alphanumeric() || ch == '_'))
}

fn consume_keyword<'a>(input: &'a str, keyword: &str) -> &'a str {
    &input[keyword.len()..]
}

fn build_create_domain_statement(sql: &str) -> Result<CreateDomainStatement, ParseError> {
    let prefix = "create domain ";
    let Some(rest) = sql.get(prefix.len()..) else {
        return Err(ParseError::UnexpectedToken {
            expected: "CREATE DOMAIN name [AS] type",
            actual: sql.into(),
        });
    };
    let rest = rest.trim_start();
    let domain_name_end =
        rest.find(char::is_whitespace)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "domain base type",
                actual: sql.into(),
            })?;
    let domain_name = &rest[..domain_name_end];
    let mut type_sql = rest[domain_name_end..].trim_start();
    if type_sql
        .get(..2)
        .is_some_and(|s| s.eq_ignore_ascii_case("as"))
        && type_sql
            .get(2..3)
            .is_none_or(|s| s.chars().all(char::is_whitespace))
    {
        type_sql = type_sql[2..].trim_start();
    }
    if type_sql.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "domain base type",
            actual: sql.into(),
        });
    }
    let normalized_type_sql = normalize_domain_type_sql(type_sql);
    if normalized_type_sql.split_whitespace().any(|tok| {
        matches!(
            tok.to_ascii_lowercase().as_str(),
            "constraint"
                | "default"
                | "check"
                | "not"
                | "null"
                | "collate"
                | "references"
                | "unique"
                | "primary"
                | "generated"
                | "deferrable"
                | "no"
        )
    }) {
        return Err(ParseError::FeatureNotSupported(
            "CREATE DOMAIN constraints/defaults are not supported yet".into(),
        ));
    }
    Ok(CreateDomainStatement {
        domain_name: domain_name.to_string(),
        ty: parse_type_name(&normalized_type_sql)?,
    })
}

fn normalize_domain_type_sql(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'[' {
            out.push('[');
            i += 1;
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i < bytes.len() && bytes[i] == b']' {
                out.push(']');
                i += 1;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn build_drop_domain_statement(sql: &str) -> Result<DropDomainStatement, ParseError> {
    let tokens = sql.split_whitespace().collect::<Vec<_>>();
    let mut index = 2usize;
    let mut if_exists = false;
    if tokens
        .get(index)
        .is_some_and(|tok| tok.eq_ignore_ascii_case("if"))
    {
        if !tokens
            .get(index + 1)
            .zip(tokens.get(index + 2))
            .is_some_and(|(a, b)| a.eq_ignore_ascii_case("exists") && !b.is_empty())
        {
            return Err(ParseError::UnexpectedToken {
                expected: "DROP DOMAIN [IF EXISTS] name",
                actual: sql.into(),
            });
        }
        if_exists = true;
        index += 2;
    }
    let Some(name) = tokens.get(index) else {
        return Err(ParseError::UnexpectedToken {
            expected: "domain name",
            actual: sql.into(),
        });
    };
    let cascade = tokens
        .get(index + 1)
        .is_some_and(|tok| tok.eq_ignore_ascii_case("cascade"));
    if tokens.len() > index + 1 && !cascade && !tokens[index + 1].eq_ignore_ascii_case("restrict") {
        return Err(ParseError::UnexpectedToken {
            expected: "CASCADE, RESTRICT, or end of statement",
            actual: tokens[index + 1].into(),
        });
    }
    if tokens.len() > index + 2 {
        return Err(ParseError::UnexpectedToken {
            expected: "end of statement",
            actual: sql.into(),
        });
    }
    Ok(DropDomainStatement {
        if_exists,
        domain_name: (*name).to_string(),
        cascade,
    })
}

fn build_comment_on_domain_statement(sql: &str) -> Result<CommentOnDomainStatement, ParseError> {
    let lower = sql.to_ascii_lowercase();
    let Some(is_offset) = lower.find(" is ") else {
        return Err(ParseError::UnexpectedToken {
            expected: "COMMENT ON DOMAIN name IS ...",
            actual: sql.into(),
        });
    };
    let object = sql["comment on domain ".len()..is_offset].trim();
    let value = sql[is_offset + 4..].trim();
    let comment = if value.eq_ignore_ascii_case("null") {
        None
    } else if value.starts_with('\'') && value.ends_with('\'') && value.len() >= 2 {
        Some(value[1..value.len() - 1].replace("''", "'"))
    } else {
        return Err(ParseError::UnexpectedToken {
            expected: "quoted string or NULL",
            actual: value.into(),
        });
    };
    Ok(CommentOnDomainStatement {
        domain_name: object.to_string(),
        comment,
    })
}

#[cfg(test)]
pub(crate) fn pest_parse_keyword(rule: Rule, input: &str) -> Result<String, ParseError> {
    let mut pairs = SqlParser::parse(rule, input).map_err(|e| map_pest_error("keyword", e))?;
    Ok(pairs
        .next()
        .ok_or(ParseError::UnexpectedEof)?
        .as_str()
        .to_string())
}

fn map_pest_error(expected: &'static str, err: pest::error::Error<Rule>) -> ParseError {
    use pest::error::ErrorVariant;

    match err.variant {
        ErrorVariant::ParsingError { .. } => ParseError::UnexpectedToken {
            expected,
            actual: err.to_string(),
        },
        ErrorVariant::CustomError { message } => ParseError::UnexpectedToken {
            expected,
            actual: message,
        },
    }
}

fn validate_unicode_string_literals(sql: &str, options: ParseOptions) -> Result<(), ParseError> {
    let bytes = sql.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_unicode_string_token(bytes, i) {
            if !options.standard_conforming_strings {
                return Err(ParseError::UnexpectedToken {
                    expected: "string literal without Unicode escapes",
                    actual: "unsafe use of string constant with Unicode escapes".into(),
                });
            }

            let literal_end = parse_delimited_token_end(bytes, i + 2, b'\'');
            i = validate_unicode_uescape_clause(sql, literal_end)?;
            continue;
        }

        let ch = sql[i..].chars().next().expect("valid utf-8");
        i += ch.len_utf8();
    }

    Ok(())
}

fn validate_unicode_uescape_clause(sql: &str, mut i: usize) -> Result<usize, ParseError> {
    let bytes = sql.as_bytes();
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if !starts_uescape_keyword(bytes, i) {
        return Ok(i);
    }

    i += "UESCAPE".len();
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }

    if i >= bytes.len() || bytes[i] != b'\'' {
        let token_end = scan_unicode_clause_token_end(bytes, i);
        let actual = if i < bytes.len() {
            &sql[i..token_end]
        } else {
            "end of input"
        };
        return Err(ParseError::UnexpectedToken {
            expected: "UESCAPE string literal",
            actual: format!(
                "UESCAPE must be followed by a simple string literal at or near \"{actual}\""
            ),
        });
    }

    let escape_end = parse_delimited_token_end(bytes, i, b'\'');
    let escape_raw = &sql[i..escape_end];
    let escape = decode_string_literal(escape_raw)?;
    let mut chars = escape.chars();
    let Some(ch) = chars.next() else {
        return Err(ParseError::UnexpectedToken {
            expected: "non-empty UESCAPE character",
            actual: "invalid Unicode escape character".into(),
        });
    };
    if chars.next().is_some() || matches!(ch, '+' | '"' | '\'' | ' ' | '\t' | '\n' | '\r') {
        return Err(ParseError::UnexpectedToken {
            expected: "valid UESCAPE character",
            actual: format!("invalid Unicode escape character at or near \"{escape_raw}\""),
        });
    }

    Ok(escape_end)
}

fn starts_unicode_string_token(bytes: &[u8], i: usize) -> bool {
    i + 2 < bytes.len()
        && matches!(bytes[i], b'u' | b'U')
        && bytes[i + 1] == b'&'
        && bytes[i + 2] == b'\''
}

fn starts_uescape_keyword(bytes: &[u8], i: usize) -> bool {
    let keyword = b"uescape";
    if i + keyword.len() > bytes.len() {
        return false;
    }
    if !bytes[i..i + keyword.len()].eq_ignore_ascii_case(keyword) {
        return false;
    }
    let before_ok = i == 0 || !is_identifier_continuation(bytes[i - 1] as char);
    let after_ok = i + keyword.len() == bytes.len()
        || !is_identifier_continuation(bytes[i + keyword.len()] as char);
    before_ok && after_ok
}

fn scan_unicode_clause_token_end(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len()
        && !bytes[i].is_ascii_whitespace()
        && !matches!(bytes[i], b';' | b',' | b')')
    {
        i += 1;
    }
    i
}

fn is_identifier_continuation(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn parse_delimited_token_end(bytes: &[u8], start: usize, delimiter: u8) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == delimiter {
            if i + 1 < bytes.len() && bytes[i + 1] == delimiter {
                i += 2;
            } else {
                return i + 1;
            }
        } else {
            i += 1;
        }
    }
    bytes.len()
}

fn build_statement(pair: Pair<'_, Rule>) -> Result<Statement, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match inner.as_rule() {
        Rule::do_stmt => Ok(Statement::Do(build_do(inner)?)),
        Rule::explain_stmt => Ok(Statement::Explain(build_explain(inner)?)),
        Rule::table_stmt => Ok(Statement::Select(build_table_select(inner)?)),
        Rule::select_stmt => Ok(Statement::Select(build_select(inner)?)),
        Rule::values_stmt => Ok(Statement::Values(build_values_statement(inner)?)),
        Rule::copy_stmt => Ok(Statement::CopyFrom(build_copy_from(inner)?)),
        Rule::analyze_stmt => Ok(Statement::Analyze(build_analyze(inner)?)),
        Rule::show_stmt => Ok(Statement::Show(build_show(inner)?)),
        Rule::set_session_authorization_stmt => Ok(Statement::SetSessionAuthorization(
            build_set_session_authorization(inner)?,
        )),
        Rule::reset_session_authorization_stmt => Ok(Statement::ResetSessionAuthorization(
            build_reset_session_authorization(inner)?,
        )),
        Rule::set_stmt => Ok(Statement::Set(build_set(inner)?)),
        Rule::reset_stmt => Ok(Statement::Reset(build_reset(inner)?)),
        Rule::create_role_stmt => Ok(Statement::CreateRole(build_create_role(inner)?)),
        Rule::alter_role_stmt => Ok(Statement::AlterRole(build_alter_role(inner)?)),
        Rule::create_index_stmt => Ok(Statement::CreateIndex(build_create_index(inner)?)),
        Rule::alter_table_add_column_stmt => Ok(Statement::AlterTableAddColumn(
            build_alter_table_add_column(inner)?,
        )),
        Rule::alter_table_add_constraint_stmt => Ok(Statement::AlterTableAddConstraint(
            build_alter_table_add_constraint(inner)?,
        )),
        Rule::alter_table_drop_constraint_stmt => Ok(Statement::AlterTableDropConstraint(
            build_alter_table_drop_constraint(inner)?,
        )),
        Rule::alter_table_drop_column_stmt => Ok(Statement::AlterTableDropColumn(
            build_alter_table_drop_column(inner)?,
        )),
        Rule::alter_table_alter_column_type_stmt => Ok(Statement::AlterTableAlterColumnType(
            build_alter_table_alter_column_type(inner)?,
        )),
        Rule::alter_table_owner_stmt => Ok(Statement::AlterTableOwner(build_alter_relation_owner(
            inner,
        )?)),
        Rule::alter_table_rename_column_stmt => Ok(Statement::AlterTableRenameColumn(
            build_alter_table_rename_column(inner)?,
        )),
        Rule::alter_table_rename_stmt => Ok(Statement::AlterTableRename(build_alter_table_rename(
            inner,
        )?)),
        Rule::alter_view_owner_stmt => Ok(Statement::AlterViewOwner(build_alter_relation_owner(
            inner,
        )?)),
        Rule::alter_table_set_stmt => Ok(Statement::AlterTableSet(build_alter_table_set(inner)?)),
        Rule::alter_table_set_not_null_stmt => Ok(Statement::AlterTableSetNotNull(
            build_alter_table_set_not_null(inner)?,
        )),
        Rule::alter_table_drop_not_null_stmt => Ok(Statement::AlterTableDropNotNull(
            build_alter_table_drop_not_null(inner)?,
        )),
        Rule::alter_table_validate_constraint_stmt => Ok(Statement::AlterTableValidateConstraint(
            build_alter_table_validate_constraint(inner)?,
        )),
        Rule::comment_on_role_stmt => Ok(Statement::CommentOnRole(build_comment_on_role(inner)?)),
        Rule::comment_on_table_stmt => {
            Ok(Statement::CommentOnTable(build_comment_on_table(inner)?))
        }
        Rule::create_table_stmt => build_create_table(inner),
        Rule::create_view_stmt => Ok(Statement::CreateView(build_create_view(inner)?)),
        Rule::drop_role_stmt => Ok(Statement::DropRole(build_drop_role(inner)?)),
        Rule::drop_table_stmt => Ok(Statement::DropTable(build_drop_table(inner)?)),
        Rule::drop_view_stmt => Ok(Statement::DropView(build_drop_view(inner)?)),
        Rule::reassign_owned_stmt => Ok(Statement::ReassignOwned(build_reassign_owned(inner)?)),
        Rule::truncate_table_stmt => Ok(Statement::TruncateTable(build_truncate_table(inner)?)),
        Rule::vacuum_stmt => Ok(Statement::Vacuum(build_vacuum(inner)?)),
        Rule::insert_stmt => Ok(Statement::Insert(build_insert(inner)?)),
        Rule::update_stmt => Ok(Statement::Update(build_update(inner)?)),
        Rule::delete_stmt => Ok(Statement::Delete(build_delete(inner)?)),
        Rule::begin_stmt => Ok(Statement::Begin),
        Rule::commit_stmt => Ok(Statement::Commit),
        Rule::rollback_stmt => Ok(Statement::Rollback),
        _ => Err(ParseError::UnexpectedToken {
            expected: "statement",
            actual: inner.as_str().into(),
        }),
    }
}

fn build_table_select(pair: Pair<'_, Rule>) -> Result<SelectStatement, ParseError> {
    let name = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::identifier)
        .map(build_identifier)
        .ok_or(ParseError::UnexpectedEof)?;
    Ok(SelectStatement {
        with: Vec::new(),
        with_recursive: false,
        from: Some(FromItem::Table { name, only: false }),
        targets: vec![SelectItem {
            expr: SqlExpr::Column("*".into()),
            output_name: "*".into(),
        }],
        where_clause: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        limit: None,
        offset: None,
    })
}

fn build_do(pair: Pair<'_, Rule>) -> Result<DoStatement, ParseError> {
    let mut language = None;
    let mut code = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::do_body => code = Some(decode_string_literal_pair(part)?),
            Rule::do_language_clause => {
                let ident = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::identifier)
                    .ok_or(ParseError::UnexpectedEof)?;
                language = Some(build_identifier(ident));
            }
            _ => {}
        }
    }
    Ok(DoStatement {
        language,
        code: code.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_analyze(pair: Pair<'_, Rule>) -> Result<AnalyzeStatement, ParseError> {
    let mut targets = Vec::new();
    let mut verbose = false;
    let mut skip_locked = false;
    let mut buffer_usage_limit = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::analyze_option_block => {
                let opts = build_analyze_options(part)?;
                verbose = opts.verbose;
                skip_locked = opts.skip_locked;
                buffer_usage_limit = opts.buffer_usage_limit;
            }
            Rule::maintenance_target_list => targets = build_maintenance_target_list(part)?,
            _ => {}
        }
    }
    Ok(AnalyzeStatement {
        targets,
        verbose,
        skip_locked,
        buffer_usage_limit,
    })
}

#[derive(Default)]
struct AnalyzeOptionsBuilder {
    verbose: bool,
    skip_locked: bool,
    buffer_usage_limit: Option<String>,
}

fn build_analyze_options(pair: Pair<'_, Rule>) -> Result<AnalyzeOptionsBuilder, ParseError> {
    let mut options = AnalyzeOptionsBuilder::default();
    for part in pair.into_inner() {
        let part = if part.as_rule() == Rule::analyze_option {
            part.into_inner().next().ok_or(ParseError::UnexpectedEof)?
        } else {
            part
        };
        match part.as_rule() {
            Rule::analyze_verbose_option => {
                options.verbose = parse_option_bool(part)?;
            }
            Rule::analyze_skip_locked_option => {
                options.skip_locked = parse_option_bool(part)?;
            }
            Rule::analyze_buffer_usage_limit_option => {
                options.buffer_usage_limit = Some(parse_option_scalar(part)?);
            }
            _ => {}
        }
    }
    Ok(options)
}

fn parse_option_bool(pair: Pair<'_, Rule>) -> Result<bool, ParseError> {
    let mut inner = pair.into_inner();
    match inner.next() {
        None => Ok(true),
        Some(part) if part.as_rule() == Rule::option_bool_value => {
            let value = part.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
            Ok(!matches!(value.as_rule(), Rule::kw_false | Rule::kw_off))
        }
        Some(_) => Ok(true),
    }
}

fn parse_option_scalar(pair: Pair<'_, Rule>) -> Result<String, ParseError> {
    let scalar = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::option_scalar_value)
        .ok_or(ParseError::UnexpectedEof)?;
    build_option_scalar_value(scalar)
}

fn build_option_scalar_value(pair: Pair<'_, Rule>) -> Result<String, ParseError> {
    let pair = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    Ok(match pair.as_rule() {
        Rule::quoted_string_literal
        | Rule::string_literal
        | Rule::unicode_string_literal
        | Rule::escape_string_literal
        | Rule::dollar_string_literal => decode_string_literal_pair(pair)?,
        Rule::option_bool_value => {
            let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
            inner.as_str().to_string()
        }
        _ => pair.as_str().to_string(),
    })
}

fn build_set(pair: Pair<'_, Rule>) -> Result<SetStatement, ParseError> {
    let mut is_local = false;
    let mut name = None;
    let mut value = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::set_scope => is_local = part.as_str().eq_ignore_ascii_case("local"),
            Rule::identifier if name.is_none() => name = Some(build_identifier(part)),
            Rule::set_value_list => value = Some(build_set_value_list(part)),
            _ => {}
        }
    }
    Ok(SetStatement {
        name: name.ok_or(ParseError::UnexpectedEof)?,
        value: value.ok_or(ParseError::UnexpectedEof)?,
        is_local,
    })
}

fn build_show(pair: Pair<'_, Rule>) -> Result<ShowStatement, ParseError> {
    let mut name = None;
    for part in pair.into_inner() {
        if part.as_rule() == Rule::identifier {
            name = Some(build_identifier(part));
        }
    }
    let name = name.ok_or(ParseError::UnexpectedEof)?;
    if name.eq_ignore_ascii_case("tables") {
        return Err(ParseError::UnexpectedToken {
            expected: "configuration parameter",
            actual: name,
        });
    }
    Ok(ShowStatement { name })
}

fn build_set_session_authorization(
    pair: Pair<'_, Rule>,
) -> Result<SetSessionAuthorizationStatement, ParseError> {
    let role_name = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::identifier)
        .map(build_identifier)
        .ok_or(ParseError::UnexpectedEof)?;
    Ok(SetSessionAuthorizationStatement { role_name })
}

fn build_reset_session_authorization(
    _pair: Pair<'_, Rule>,
) -> Result<ResetSessionAuthorizationStatement, ParseError> {
    Ok(ResetSessionAuthorizationStatement)
}

fn build_reset(pair: Pair<'_, Rule>) -> Result<ResetStatement, ParseError> {
    let mut name = None;
    for part in pair.into_inner() {
        if part.as_rule() == Rule::identifier {
            name = Some(build_identifier(part));
        }
    }
    Ok(ResetStatement { name })
}

fn build_create_role(pair: Pair<'_, Rule>) -> Result<CreateRoleStatement, ParseError> {
    let is_user = pair
        .as_str()
        .to_ascii_lowercase()
        .starts_with("create user ");
    let mut role_name = None;
    let mut options = Vec::new();

    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if role_name.is_none() => role_name = Some(build_identifier(part)),
            Rule::role_option => options.push(build_role_option(part)?),
            _ => {}
        }
    }

    Ok(CreateRoleStatement {
        role_name: role_name.ok_or(ParseError::UnexpectedEof)?,
        is_user,
        options,
    })
}

fn build_alter_role(pair: Pair<'_, Rule>) -> Result<AlterRoleStatement, ParseError> {
    let mut role_name = None;
    let mut rename_to = None;
    let mut options = Vec::new();

    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if role_name.is_none() => role_name = Some(build_identifier(part)),
            Rule::alter_role_rename_clause => {
                rename_to = Some(build_alter_role_rename(part)?);
            }
            Rule::alter_role_option => options.push(build_role_option(part)?),
            _ => {}
        }
    }

    let action = if let Some(new_name) = rename_to {
        AlterRoleAction::Rename { new_name }
    } else {
        AlterRoleAction::Options(options)
    };

    Ok(AlterRoleStatement {
        role_name: role_name.ok_or(ParseError::UnexpectedEof)?,
        action,
    })
}

fn build_alter_role_rename(pair: Pair<'_, Rule>) -> Result<String, ParseError> {
    pair.into_inner()
        .find(|part| part.as_rule() == Rule::identifier)
        .map(build_identifier)
        .ok_or(ParseError::UnexpectedEof)
}

fn build_role_option(pair: Pair<'_, Rule>) -> Result<RoleOption, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match inner.as_rule() {
        Rule::alter_role_option => {
            let nested = inner.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
            build_role_option_from_rule(nested)
        }
        _ => build_role_option_from_rule(inner),
    }
}

fn build_role_option_from_rule(pair: Pair<'_, Rule>) -> Result<RoleOption, ParseError> {
    match pair.as_rule() {
        Rule::role_attr_option => build_role_attr_option(pair),
        Rule::role_connection_limit_option => {
            let limit = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::signed_integer)
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(RoleOption::ConnectionLimit(parse_i32(limit)?))
        }
        Rule::role_password_option => {
            let mut value = None;
            for part in pair.into_inner() {
                match part.as_rule() {
                    Rule::quoted_string_literal
                    | Rule::string_literal
                    | Rule::unicode_string_literal
                    | Rule::escape_string_literal
                    | Rule::dollar_string_literal => {
                        value = Some(Some(decode_string_literal_pair(part)?))
                    }
                    Rule::kw_null => value = Some(None),
                    _ => {}
                }
            }
            Ok(RoleOption::Password(
                value.ok_or(ParseError::UnexpectedEof)?,
            ))
        }
        Rule::role_encrypted_password_option => {
            let value = pair
                .into_inner()
                .find(|part| {
                    matches!(
                        part.as_rule(),
                        Rule::quoted_string_literal
                            | Rule::string_literal
                            | Rule::unicode_string_literal
                            | Rule::escape_string_literal
                            | Rule::dollar_string_literal
                    )
                })
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(RoleOption::EncryptedPassword(decode_string_literal_pair(
                value,
            )?))
        }
        Rule::role_membership_option => build_role_membership_option(pair),
        Rule::role_sysid_option => {
            let value = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(RoleOption::Sysid(parse_i32(value)?))
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "role option",
            actual: pair.as_str().to_string(),
        }),
    }
}

fn build_role_attr_option(pair: Pair<'_, Rule>) -> Result<RoleOption, ParseError> {
    let attr = pair.as_str().to_ascii_lowercase();
    Ok(match attr.as_str() {
        "superuser" => RoleOption::Superuser(true),
        "nosuperuser" => RoleOption::Superuser(false),
        "createdb" => RoleOption::CreateDb(true),
        "nocreatedb" => RoleOption::CreateDb(false),
        "createrole" => RoleOption::CreateRole(true),
        "nocreaterole" => RoleOption::CreateRole(false),
        "inherit" => RoleOption::Inherit(true),
        "noinherit" => RoleOption::Inherit(false),
        "login" => RoleOption::Login(true),
        "nologin" => RoleOption::Login(false),
        "replication" => RoleOption::Replication(true),
        "noreplication" => RoleOption::Replication(false),
        "bypassrls" => RoleOption::BypassRls(true),
        "nobypassrls" => RoleOption::BypassRls(false),
        _ => {
            return Err(ParseError::UnexpectedToken {
                expected: "role attribute option",
                actual: attr,
            });
        }
    })
}

fn build_role_membership_option(pair: Pair<'_, Rule>) -> Result<RoleOption, ParseError> {
    let lowered = pair.as_str().to_ascii_lowercase();
    let option_name = if lowered.starts_with("in role ") {
        "in role"
    } else if lowered.starts_with("role ") {
        "role"
    } else if lowered.starts_with("admin ") {
        "admin"
    } else {
        return Err(ParseError::UnexpectedToken {
            expected: "role membership option",
            actual: pair.as_str().to_string(),
        });
    };
    let mut roles = Vec::new();

    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::ident_list => roles.extend(part.into_inner().map(build_identifier)),
            Rule::identifier => roles.push(build_identifier(part)),
            _ => {}
        }
    }

    Ok(match option_name {
        "role" => RoleOption::Role(roles),
        "admin" => RoleOption::Admin(roles),
        "in role" => RoleOption::InRole(roles),
        _ => unreachable!(),
    })
}

fn build_set_value_list(pair: Pair<'_, Rule>) -> String {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::set_value_atom)
        .map(build_simple_set_value_atom)
        .collect::<Vec<_>>()
        .join(", ")
}

fn build_simple_set_value_atom(pair: Pair<'_, Rule>) -> String {
    let pair = pair.clone().into_inner().next().unwrap_or(pair);
    match pair.as_rule() {
        Rule::signed_set_value => pair.as_str().to_string(),
        Rule::quoted_string_literal
        | Rule::string_literal
        | Rule::unicode_string_literal
        | Rule::escape_string_literal
        | Rule::dollar_string_literal => decode_string_literal_pair(pair).unwrap_or_default(),
        Rule::kw_true => "true".to_string(),
        Rule::kw_false => "false".to_string(),
        Rule::kw_on_value => "on".to_string(),
        Rule::kw_off => "off".to_string(),
        Rule::kw_default => "default".to_string(),
        Rule::identifier | Rule::numeric_literal | Rule::integer => pair.as_str().to_string(),
        _ => pair.as_str().to_string(),
    }
}

fn build_explain(pair: Pair<'_, Rule>) -> Result<ExplainStatement, ParseError> {
    let mut analyze = false;
    let mut buffers = false;
    let mut timing = true;
    let mut statement = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::kw_analyze => analyze = true,
            Rule::explain_option => {
                let mut name_rule = None;
                let mut bool_val = true;
                for child in part.into_inner() {
                    match child.as_rule() {
                        Rule::explain_option_name => {
                            name_rule = child.into_inner().next().map(|r| r.as_rule());
                        }
                        Rule::explain_option_value => {
                            let val = child.into_inner().next();
                            if let Some(v) = val {
                                match v.as_rule() {
                                    Rule::kw_off | Rule::kw_false => bool_val = false,
                                    _ => bool_val = true,
                                }
                            }
                        }
                        _ => {}
                    }
                }
                match name_rule {
                    Some(Rule::kw_analyze) => analyze = bool_val,
                    Some(Rule::kw_buffers) => buffers = bool_val,
                    Some(Rule::kw_timing) => timing = bool_val,
                    _ => {} // COSTS, VERBOSE, SUMMARY, FORMAT: parsed but ignored
                }
            }
            Rule::select_stmt => statement = Some(Statement::Select(build_select(part)?)),
            _ => {}
        }
    }
    Ok(ExplainStatement {
        analyze,
        buffers,
        timing,
        statement: Box::new(statement.ok_or(ParseError::UnexpectedEof)?),
    })
}

pub(crate) fn build_select(pair: Pair<'_, Rule>) -> Result<SelectStatement, ParseError> {
    let mut with_recursive = false;
    let mut with = Vec::new();
    let mut targets = None;
    let mut from = None;
    let mut where_clause = None;
    let mut group_by = Vec::new();
    let mut having = None;
    let mut order_by = Vec::new();
    let mut limit = None;
    let mut offset = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::cte_clause => {
                let (recursive, ctes) = build_cte_clause(part)?;
                with_recursive = recursive;
                with = ctes;
            }
            Rule::select_list => targets = Some(build_select_list(part)?),
            Rule::from_item => from = Some(build_from_item(part)?),
            Rule::expr => where_clause = Some(build_expr(part)?),
            Rule::group_by_clause => group_by = build_group_by_clause(part)?,
            Rule::having_clause => having = Some(build_having_clause(part)?),
            Rule::order_by_clause => order_by = build_order_by_clause(part)?,
            Rule::limit_clause => limit = Some(build_limit_clause(part)?),
            Rule::offset_clause => offset = Some(build_offset_clause(part)?),
            _ => {}
        }
    }
    Ok(SelectStatement {
        with_recursive,
        with,
        from,
        targets: targets.unwrap_or_default(),
        where_clause,
        group_by,
        having,
        order_by,
        limit,
        offset,
    })
}

fn build_values_statement(pair: Pair<'_, Rule>) -> Result<ValuesStatement, ParseError> {
    let mut with_recursive = false;
    let mut with = Vec::new();
    let mut rows = Vec::new();
    let mut order_by = Vec::new();
    let mut limit = None;
    let mut offset = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::cte_clause => {
                let (recursive, ctes) = build_cte_clause(part)?;
                with_recursive = recursive;
                with = ctes;
            }
            Rule::values_row => rows.push(build_values_row(part)?),
            Rule::order_by_clause => order_by = build_order_by_clause(part)?,
            Rule::limit_clause => limit = Some(build_limit_clause(part)?),
            Rule::offset_clause => offset = Some(build_offset_clause(part)?),
            _ => {}
        }
    }
    Ok(ValuesStatement {
        with_recursive,
        with,
        rows,
        order_by,
        limit,
        offset,
    })
}

fn build_cte_clause(pair: Pair<'_, Rule>) -> Result<(bool, Vec<CommonTableExpr>), ParseError> {
    let recursive = pair
        .as_str()
        .trim_start()
        .to_ascii_lowercase()
        .starts_with("with recursive");
    let mut ctes = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::common_table_expr => ctes.push(build_common_table_expr(part)?),
            _ => {}
        }
    }
    Ok((recursive, ctes))
}

fn build_common_table_expr(pair: Pair<'_, Rule>) -> Result<CommonTableExpr, ParseError> {
    let mut name = None;
    let mut column_names = Vec::new();
    let mut body = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if name.is_none() => name = Some(build_identifier(part)),
            Rule::cte_column_list => {
                if let Some(list) = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::ident_list)
                {
                    column_names = list.into_inner().map(build_identifier).collect();
                }
            }
            Rule::cte_body => {
                body = Some(build_cte_body(
                    part.into_inner().next().ok_or(ParseError::UnexpectedEof)?,
                )?)
            }
            _ => {}
        }
    }
    Ok(CommonTableExpr {
        name: name.ok_or(ParseError::UnexpectedEof)?,
        column_names,
        body: body.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_cte_body(pair: Pair<'_, Rule>) -> Result<CteBody, ParseError> {
    match pair.as_rule() {
        Rule::select_stmt => Ok(CteBody::Select(Box::new(build_select(pair)?))),
        Rule::values_stmt => Ok(CteBody::Values(build_values_statement(pair)?)),
        Rule::recursive_union_cte_body => {
            let all = contains_union_all(pair.as_str());
            let mut inner = pair.into_inner();
            let anchor = build_cte_body(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            let mut recursive = None;
            for part in inner {
                match part.as_rule() {
                    Rule::select_stmt => recursive = Some(build_select(part)?),
                    _ => {}
                }
            }
            Ok(CteBody::RecursiveUnion {
                all,
                anchor: Box::new(anchor),
                recursive: Box::new(recursive.ok_or(ParseError::UnexpectedEof)?),
            })
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "SELECT or VALUES CTE body",
            actual: pair.as_str().into(),
        }),
    }
}

fn contains_union_all(sql: &str) -> bool {
    let mut prev_union = false;
    for token in sql.split_whitespace() {
        if prev_union && token.eq_ignore_ascii_case("all") {
            return true;
        }
        prev_union = token.eq_ignore_ascii_case("union");
    }
    false
}

fn build_group_by_clause(pair: Pair<'_, Rule>) -> Result<Vec<SqlExpr>, ParseError> {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::expr)
        .map(build_expr)
        .collect()
}

fn build_having_clause(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let expr = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::expr)
        .ok_or(ParseError::UnexpectedEof)?;
    build_expr(expr)
}

fn build_order_by_clause(pair: Pair<'_, Rule>) -> Result<Vec<OrderByItem>, ParseError> {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::order_by_item)
        .map(build_order_by_item)
        .collect()
}

fn build_order_by_item(pair: Pair<'_, Rule>) -> Result<OrderByItem, ParseError> {
    let mut expr = None;
    let mut descending = false;
    let mut nulls_first = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::expr => expr = Some(build_expr(part)?),
            Rule::kw_desc => descending = true,
            Rule::kw_asc => descending = false,
            Rule::nulls_ordering => {
                for item in part.into_inner() {
                    match item.as_rule() {
                        Rule::kw_first => nulls_first = Some(true),
                        Rule::kw_last => nulls_first = Some(false),
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(OrderByItem {
        expr: expr.ok_or(ParseError::UnexpectedEof)?,
        descending,
        nulls_first,
    })
}

fn build_limit_clause(pair: Pair<'_, Rule>) -> Result<usize, ParseError> {
    build_usize_clause(pair, "LIMIT")
}

fn build_offset_clause(pair: Pair<'_, Rule>) -> Result<usize, ParseError> {
    build_usize_clause(pair, "OFFSET")
}

fn build_usize_clause(pair: Pair<'_, Rule>, expected: &'static str) -> Result<usize, ParseError> {
    let integer = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::integer)
        .ok_or(ParseError::UnexpectedEof)?;
    integer
        .as_str()
        .parse::<usize>()
        .map_err(|_| ParseError::UnexpectedToken {
            expected,
            actual: integer.as_str().into(),
        })
}

fn build_from_item(pair: Pair<'_, Rule>) -> Result<FromItem, ParseError> {
    let raw = pair.as_str().to_string();
    match pair.as_rule() {
        Rule::from_item | Rule::from_primary | Rule::parenthesized_from_item => {
            build_from_item(pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?)
        }
        Rule::lateral_from_item => {
            let source = pair
                .into_inner()
                .find(|part| {
                    matches!(
                        part.as_rule(),
                        Rule::values_from_item
                            | Rule::srf_from_item
                            | Rule::derived_from_item
                            | Rule::parenthesized_from_item
                    )
                })
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(FromItem::Lateral(Box::new(build_from_item(source)?)))
        }
        Rule::from_list_item => {
            let mut source = None;
            let mut alias = None;
            let mut column_aliases = AliasColumnSpec::None;
            for part in pair.into_inner() {
                match part.as_rule() {
                    Rule::joined_from_item => source = Some(build_from_item(part)?),
                    Rule::relation_alias => {
                        let (parsed_alias, parsed_column_aliases) = build_relation_alias(part)?;
                        alias = Some(parsed_alias);
                        column_aliases = parsed_column_aliases;
                    }
                    _ => {}
                }
            }
            let item = source.ok_or(ParseError::UnexpectedEof)?;
            if let Some(alias) = alias {
                Ok(FromItem::Alias {
                    source: Box::new(item),
                    alias,
                    column_aliases,
                    preserve_source_names: true,
                })
            } else {
                Ok(item)
            }
        }
        Rule::from_list => {
            let mut items = pair
                .into_inner()
                .filter(|part| part.as_rule() == Rule::from_list_item)
                .map(build_from_item);
            let mut item = items.next().ok_or(ParseError::UnexpectedEof)??;
            for next in items {
                item = FromItem::Join {
                    left: Box::new(item),
                    right: Box::new(next?),
                    kind: JoinKind::Cross,
                    constraint: JoinConstraint::None,
                };
            }
            Ok(item)
        }
        Rule::joined_from_item => {
            let mut parts = pair.into_inner();
            let mut item = build_from_item(parts.next().ok_or(ParseError::UnexpectedEof)?)?;
            for join_clause in parts {
                let (kind, right, constraint) = build_join_clause(join_clause)?;
                item = FromItem::Join {
                    left: Box::new(item),
                    right: Box::new(right),
                    kind,
                    constraint,
                };
            }
            Ok(item)
        }
        Rule::aliased_from_item => {
            let mut source = None;
            let mut alias = None;
            let mut column_aliases = AliasColumnSpec::None;
            for part in pair.into_inner() {
                match part.as_rule() {
                    Rule::only_table_from_item
                    | Rule::table_from_item
                    | Rule::lateral_from_item
                    | Rule::values_from_item
                    | Rule::parenthesized_table_from_item
                    | Rule::srf_from_item
                    | Rule::derived_from_item
                    | Rule::parenthesized_from_item
                    | Rule::from_primary => source = Some(build_from_item(part)?),
                    Rule::relation_alias => {
                        let (parsed_alias, parsed_column_aliases) = build_relation_alias(part)?;
                        alias = Some(parsed_alias);
                        column_aliases = parsed_column_aliases;
                    }
                    _ => {}
                }
            }
            let item = source.ok_or(ParseError::UnexpectedEof)?;
            if let Some(alias) = alias {
                Ok(FromItem::Alias {
                    source: Box::new(item),
                    alias,
                    column_aliases,
                    preserve_source_names: false,
                })
            } else {
                Ok(item)
            }
        }
        Rule::only_table_from_item => Ok(FromItem::Table {
            name: build_identifier(
                pair.into_inner()
                    .find(|part| part.as_rule() == Rule::identifier)
                    .ok_or(ParseError::UnexpectedEof)?,
            ),
            only: true,
        }),
        Rule::table_from_item | Rule::parenthesized_table_from_item => Ok(FromItem::Table {
            name: build_identifier(
                pair.into_inner()
                    .find(|part| part.as_rule() == Rule::identifier)
                    .ok_or(ParseError::UnexpectedEof)?,
            ),
            only: false,
        }),
        Rule::values_from_item => Ok(FromItem::Values {
            rows: pair
                .into_inner()
                .filter(|part| part.as_rule() == Rule::values_row)
                .map(build_values_row)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        Rule::srf_from_item => {
            let mut name = None;
            let mut parsed_args = ParsedFunctionArgs::default();
            for part in pair.into_inner() {
                match part.as_rule() {
                    Rule::identifier if name.is_none() => name = Some(build_identifier(part)),
                    Rule::function_arg_list => {
                        parsed_args = build_function_arg_list(part)?;
                    }
                    _ => {}
                }
            }
            Ok(FromItem::FunctionCall {
                name: name.ok_or(ParseError::UnexpectedEof)?,
                args: parsed_args.args,
                func_variadic: parsed_args.func_variadic,
            })
        }
        Rule::derived_from_item => {
            let select = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::select_stmt)
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(FromItem::DerivedTable(Box::new(build_select(select)?)))
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "from clause",
            actual: raw,
        }),
    }
}

fn build_join_clause(
    pair: Pair<'_, Rule>,
) -> Result<(JoinKind, FromItem, JoinConstraint), ParseError> {
    let raw = pair.as_str().to_string();
    let mut kind = JoinKind::Inner;
    let mut right = None;
    let mut constraint = JoinConstraint::None;
    let mut natural = false;

    for part in pair.into_inner() {
        consume_join_part(part, &mut kind, &mut right, &mut constraint, &mut natural)?;
    }

    if natural {
        constraint = JoinConstraint::Natural;
        if matches!(kind, JoinKind::Cross) {
            return Err(ParseError::UnexpectedToken {
                expected: "NATURAL join without CROSS",
                actual: raw,
            });
        }
    }

    match kind {
        JoinKind::Cross => {
            if !matches!(constraint, JoinConstraint::None) {
                return Err(ParseError::UnexpectedToken {
                    expected: "CROSS JOIN without ON or USING",
                    actual: raw,
                });
            }
        }
        JoinKind::Inner | JoinKind::Left | JoinKind::Right | JoinKind::Full => {
            if matches!(constraint, JoinConstraint::None) {
                return Err(ParseError::UnexpectedToken {
                    expected: "join qualifier",
                    actual: raw,
                });
            }
        }
    }

    Ok((kind, right.ok_or(ParseError::UnexpectedEof)?, constraint))
}

fn consume_join_part(
    part: Pair<'_, Rule>,
    kind: &mut JoinKind,
    right: &mut Option<FromItem>,
    constraint: &mut JoinConstraint,
    natural: &mut bool,
) -> Result<(), ParseError> {
    match part.as_rule() {
        Rule::aliased_from_item => *right = Some(build_from_item(part)?),
        Rule::expr => *constraint = JoinConstraint::On(build_expr(part)?),
        Rule::join_using_clause => {
            let mut columns = Vec::new();
            collect_identifiers(part, &mut columns);
            *constraint = JoinConstraint::Using(columns);
        }
        Rule::cross_join_type => *kind = JoinKind::Cross,
        Rule::kw_left | Rule::left_join_type => *kind = JoinKind::Left,
        Rule::kw_right | Rule::right_join_type => *kind = JoinKind::Right,
        Rule::kw_full | Rule::full_join_type => *kind = JoinKind::Full,
        Rule::natural_marker => *natural = true,
        _ => {
            for inner in part.into_inner() {
                consume_join_part(inner, kind, right, constraint, natural)?;
            }
        }
    }
    Ok(())
}

fn collect_identifiers(pair: Pair<'_, Rule>, out: &mut Vec<String>) {
    match pair.as_rule() {
        Rule::identifier => out.push(build_identifier(pair)),
        _ => {
            for part in pair.into_inner() {
                collect_identifiers(part, out);
            }
        }
    }
}

fn build_relation_alias(pair: Pair<'_, Rule>) -> Result<(String, AliasColumnSpec), ParseError> {
    let mut alias = None;
    let mut column_aliases = AliasColumnSpec::None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if alias.is_none() => alias = Some(build_identifier(part)),
            Rule::bare_relation_alias if alias.is_none() => {
                alias = Some(build_identifier(
                    part.into_inner().next().ok_or(ParseError::UnexpectedEof)?,
                ));
            }
            Rule::alias_column_spec => column_aliases = build_alias_column_spec(part)?,
            _ => {}
        }
    }
    Ok((alias.ok_or(ParseError::UnexpectedEof)?, column_aliases))
}

fn build_alias_column_spec(pair: Pair<'_, Rule>) -> Result<AliasColumnSpec, ParseError> {
    let mut defs = Vec::new();
    let mut names = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alias_column_def => defs.push(build_alias_column_def(part)?),
            Rule::ident_list => collect_identifiers(part, &mut names),
            _ => {}
        }
    }
    if !defs.is_empty() {
        return Ok(AliasColumnSpec::Definitions(defs));
    }
    if !names.is_empty() {
        return Ok(AliasColumnSpec::Names(names));
    }
    Ok(AliasColumnSpec::None)
}

fn build_alias_column_def(pair: Pair<'_, Rule>) -> Result<AliasColumnDef, ParseError> {
    let mut inner = pair.into_inner();
    let name = build_identifier(inner.next().ok_or(ParseError::UnexpectedEof)?);
    let ty = build_type_name(inner.next().ok_or(ParseError::UnexpectedEof)?);
    Ok(AliasColumnDef { name, ty })
}

fn build_insert(pair: Pair<'_, Rule>) -> Result<InsertStatement, ParseError> {
    let mut with_recursive = false;
    let mut with = Vec::new();
    let mut table_name = None;
    let mut columns = None;
    let mut source = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::cte_clause => {
                let (recursive, ctes) = build_cte_clause(part)?;
                with_recursive = recursive;
                with = ctes;
            }
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::assignment_target_list => columns = Some(build_assignment_target_list(part)?),
            Rule::insert_values_source => {
                source = Some(InsertSource::Values(
                    part.into_inner()
                        .filter(|inner| inner.as_rule() == Rule::values_row)
                        .map(build_values_row)
                        .collect::<Result<Vec<_>, _>>()?,
                ))
            }
            Rule::insert_default_values_source => source = Some(InsertSource::DefaultValues),
            Rule::select_stmt => source = Some(InsertSource::Select(Box::new(build_select(part)?))),
            _ => {}
        }
    }
    Ok(InsertStatement {
        with_recursive,
        with,
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        columns,
        source: source.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_create_table(pair: Pair<'_, Rule>) -> Result<Statement, ParseError> {
    let mut relation_name = None;
    let mut persistence = TablePersistence::Permanent;
    let mut on_commit = OnCommitAction::PreserveRows;
    let mut elements = Vec::new();
    let mut inherits = Vec::new();
    let mut ctas_columns = Vec::new();
    let mut query = None;
    let mut is_ctas = false;
    let mut if_not_exists = false;
    for part in pair.into_inner() {
        let part = if part.as_rule() == Rule::create_table_tail {
            part.into_inner().next().ok_or(ParseError::UnexpectedEof)?
        } else {
            part
        };
        match part.as_rule() {
            Rule::temp_clause => persistence = TablePersistence::Temporary,
            Rule::if_not_exists_clause => if_not_exists = true,
            Rule::identifier if relation_name.is_none() => {
                relation_name = Some(build_relation_name(part))
            }
            Rule::create_table_column_form => {
                for inner in part.into_inner() {
                    match inner.as_rule() {
                        Rule::create_table_element => {
                            elements.push(build_create_table_element(inner)?)
                        }
                        Rule::on_commit_clause => on_commit = build_on_commit_action(inner)?,
                        _ => {}
                    }
                }
            }
            Rule::create_table_as_form => {
                is_ctas = true;
                for inner in part.into_inner() {
                    match inner.as_rule() {
                        Rule::ctas_column_list => {
                            ctas_columns = inner
                                .into_inner()
                                .find(|p| p.as_rule() == Rule::ident_list)
                                .map(|p| p.into_inner().map(build_identifier).collect())
                                .unwrap_or_default();
                        }
                        Rule::on_commit_clause => on_commit = build_on_commit_action(inner)?,
                        Rule::select_stmt => query = Some(build_select(inner)?),
                        _ => {}
                    }
                }
            }
            Rule::inherits_clause => {
                inherits = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::ident_list)
                    .map(|inner| inner.into_inner().map(build_identifier).collect())
                    .unwrap_or_default();
            }
            Rule::table_storage_clause => validate_table_storage_clause(part)?,
            _ => {}
        }
    }
    let (schema_name, table_name) = relation_name.ok_or(ParseError::UnexpectedEof)?;
    if is_ctas && !inherits.is_empty() {
        return Err(ParseError::FeatureNotSupported(
            "CREATE TABLE AS ... INHERITS".into(),
        ));
    }
    if is_ctas {
        Ok(Statement::CreateTableAs(CreateTableAsStatement {
            schema_name,
            table_name,
            persistence,
            on_commit,
            column_names: ctas_columns,
            query: query.ok_or(ParseError::UnexpectedEof)?,
            if_not_exists,
        }))
    } else {
        Ok(Statement::CreateTable(CreateTableStatement {
            schema_name,
            table_name,
            persistence,
            on_commit,
            elements,
            inherits,
            if_not_exists,
        }))
    }
}

fn build_create_view(pair: Pair<'_, Rule>) -> Result<CreateViewStatement, ParseError> {
    let mut relation_name = None;
    let mut query = None;
    let mut query_sql = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if relation_name.is_none() => {
                relation_name = Some(build_relation_name(part))
            }
            Rule::select_stmt => {
                query_sql = Some(part.as_str().trim().to_string());
                query = Some(build_select(part)?);
            }
            _ => {}
        }
    }
    let (schema_name, view_name) = relation_name.ok_or(ParseError::UnexpectedEof)?;
    Ok(CreateViewStatement {
        schema_name,
        view_name,
        query: query.ok_or(ParseError::UnexpectedEof)?,
        query_sql: query_sql.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_create_table_element(pair: Pair<'_, Rule>) -> Result<CreateTableElement, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match inner.as_rule() {
        Rule::column_def => Ok(CreateTableElement::Column(build_column_def(inner)?)),
        Rule::table_constraint => Ok(CreateTableElement::Constraint(build_table_constraint(
            inner,
        )?)),
        _ => Err(ParseError::UnexpectedToken {
            expected: "column definition or table constraint",
            actual: inner.as_str().to_string(),
        }),
    }
}

fn build_table_constraint(pair: Pair<'_, Rule>) -> Result<TableConstraint, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    build_table_constraint_inner(inner)
}

fn build_table_constraint_inner(pair: Pair<'_, Rule>) -> Result<TableConstraint, ParseError> {
    let rule = pair.as_rule();
    if rule == Rule::named_table_constraint {
        let mut name = None;
        let mut constraint = None;
        for part in pair.into_inner() {
            match part.as_rule() {
                Rule::identifier if name.is_none() => name = Some(build_identifier(part)),
                Rule::primary_key_table_constraint
                | Rule::unique_table_constraint
                | Rule::check_table_constraint
                | Rule::not_null_table_constraint => {
                    constraint = Some(build_table_constraint_inner(part)?)
                }
                _ => {}
            }
        }
        let mut constraint = constraint.ok_or(ParseError::UnexpectedEof)?;
        set_table_constraint_name(&mut constraint, name.ok_or(ParseError::UnexpectedEof)?);
        return Ok(constraint);
    }

    let attributes = build_constraint_attributes(pair.clone());
    match rule {
        Rule::primary_key_table_constraint => {
            let body = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::primary_key_table_constraint_body)
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(TableConstraint::PrimaryKey {
                attributes,
                columns: body
                    .into_inner()
                    .find(|part| part.as_rule() == Rule::ident_list)
                    .map(|part| part.into_inner().map(build_identifier).collect())
                    .unwrap_or_default(),
            })
        }
        Rule::unique_table_constraint => {
            let body = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::unique_table_constraint_body)
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(TableConstraint::Unique {
                attributes,
                columns: body
                    .into_inner()
                    .find(|part| part.as_rule() == Rule::ident_list)
                    .map(|part| part.into_inner().map(build_identifier).collect())
                    .unwrap_or_default(),
            })
        }
        Rule::check_table_constraint => {
            let body = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::check_table_constraint_body)
                .ok_or(ParseError::UnexpectedEof)?;
            let expr_sql = body
                .into_inner()
                .find(|part| part.as_rule() == Rule::expr)
                .map(|part| part.as_str().trim().to_string())
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(TableConstraint::Check {
                attributes,
                expr_sql,
            })
        }
        Rule::not_null_table_constraint => {
            let body = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::not_null_table_constraint_body)
                .ok_or(ParseError::UnexpectedEof)?;
            let column = body
                .into_inner()
                .find(|part| part.as_rule() == Rule::identifier)
                .map(build_identifier)
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(TableConstraint::NotNull { attributes, column })
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "table constraint",
            actual: pair.as_str().to_string(),
        }),
    }
}

fn build_constraint_attributes(pair: Pair<'_, Rule>) -> ConstraintAttributes {
    let mut attributes = ConstraintAttributes::default();
    for part in pair.into_inner() {
        if part.as_rule() != Rule::constraint_attribute {
            continue;
        }
        let attr = part
            .into_inner()
            .next()
            .expect("constraint attribute inner");
        match attr.as_rule() {
            Rule::not_valid_constraint_attribute => attributes.not_valid = true,
            Rule::deferrable_constraint_attribute => attributes.deferrable = Some(true),
            Rule::not_deferrable_constraint_attribute => attributes.deferrable = Some(false),
            Rule::initially_deferred_constraint_attribute => {
                attributes.initially_deferred = Some(true)
            }
            Rule::initially_immediate_constraint_attribute => {
                attributes.initially_deferred = Some(false)
            }
            Rule::enforced_constraint_attribute => attributes.enforced = Some(true),
            Rule::not_enforced_constraint_attribute => attributes.enforced = Some(false),
            _ => {}
        }
    }
    attributes
}

fn set_table_constraint_name(constraint: &mut TableConstraint, name: String) {
    match constraint {
        TableConstraint::NotNull { attributes, .. }
        | TableConstraint::Check { attributes, .. }
        | TableConstraint::PrimaryKey { attributes, .. }
        | TableConstraint::Unique { attributes, .. } => attributes.name = Some(name),
    }
}

fn build_column_constraint(pair: Pair<'_, Rule>) -> Result<ColumnConstraint, ParseError> {
    let rule = pair.as_rule();
    if rule == Rule::named_column_constraint {
        let mut name = None;
        let mut constraint = None;
        for part in pair.into_inner() {
            match part.as_rule() {
                Rule::identifier if name.is_none() => name = Some(build_identifier(part)),
                Rule::not_null_column_constraint
                | Rule::check_column_constraint
                | Rule::primary_key_column_constraint
                | Rule::unique_column_constraint => {
                    constraint = Some(build_column_constraint(part)?)
                }
                _ => {}
            }
        }
        let mut constraint = constraint.ok_or(ParseError::UnexpectedEof)?;
        set_column_constraint_name(&mut constraint, name.ok_or(ParseError::UnexpectedEof)?);
        return Ok(constraint);
    }

    let attributes = build_constraint_attributes(pair.clone());
    match rule {
        Rule::not_null_column_constraint => Ok(ColumnConstraint::NotNull { attributes }),
        Rule::check_column_constraint => {
            let body = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::check_column_constraint_body)
                .ok_or(ParseError::UnexpectedEof)?;
            let expr_sql = body
                .into_inner()
                .find(|part| part.as_rule() == Rule::expr)
                .map(|part| part.as_str().trim().to_string())
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(ColumnConstraint::Check {
                attributes,
                expr_sql,
            })
        }
        Rule::primary_key_column_constraint => Ok(ColumnConstraint::PrimaryKey { attributes }),
        Rule::unique_column_constraint => Ok(ColumnConstraint::Unique { attributes }),
        _ => Err(ParseError::UnexpectedToken {
            expected: "column constraint",
            actual: pair.as_str().to_string(),
        }),
    }
}

fn set_column_constraint_name(constraint: &mut ColumnConstraint, name: String) {
    match constraint {
        ColumnConstraint::NotNull { attributes }
        | ColumnConstraint::Check { attributes, .. }
        | ColumnConstraint::PrimaryKey { attributes }
        | ColumnConstraint::Unique { attributes } => attributes.name = Some(name),
    }
}

fn build_create_index(pair: Pair<'_, Rule>) -> Result<CreateIndexStatement, ParseError> {
    let raw = pair.as_str().to_ascii_lowercase();
    let unique = raw.starts_with("create unique index");
    let mut if_not_exists = false;
    let mut index_name = None;
    let mut table_name = None;
    let mut using_method = None;
    let mut columns = Vec::new();
    let mut include_columns = Vec::new();
    let mut predicate = None;
    let mut options = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::if_not_exists_clause => if_not_exists = true,
            Rule::create_index_name if index_name.is_none() => {
                index_name = Some(build_identifier(
                    part.into_inner().next().ok_or(ParseError::UnexpectedEof)?,
                ))
            }
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::create_index_using_clause => {
                using_method = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::identifier)
                    .map(build_identifier);
            }
            Rule::create_index_item => columns.push(build_create_index_item(part)?),
            Rule::create_index_include_clause => {
                include_columns.extend(
                    part.into_inner()
                        .filter(|inner| inner.as_rule() == Rule::ident_list)
                        .flat_map(|inner| inner.into_inner().map(build_identifier)),
                );
            }
            Rule::create_index_where_clause => {
                let expr = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::expr)
                    .ok_or(ParseError::UnexpectedEof)?;
                predicate = Some(build_expr(expr)?);
            }
            Rule::create_index_with_clause => {
                for option in part
                    .into_inner()
                    .filter(|inner| inner.as_rule() == Rule::reloption)
                {
                    options.push(build_reloption(option)?);
                }
            }
            _ => {}
        }
    }
    if columns.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    if if_not_exists && index_name.is_none() {
        return Err(ParseError::UnexpectedToken {
            expected: "index name after IF NOT EXISTS",
            actual: "syntax error at or near \"ON\"".into(),
        });
    }
    Ok(CreateIndexStatement {
        unique,
        if_not_exists,
        index_name: index_name.unwrap_or_default(),
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        using_method,
        columns,
        include_columns,
        predicate,
        options,
    })
}

fn build_create_index_item(pair: Pair<'_, Rule>) -> Result<IndexColumnDef, ParseError> {
    let mut name = None;
    let mut opclass = None;
    let mut descending = false;
    let mut nulls_first = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if name.is_none() => name = Some(build_identifier(part)),
            Rule::create_index_opclass if opclass.is_none() => {
                opclass = Some(build_identifier(
                    part.into_inner().next().ok_or(ParseError::UnexpectedEof)?,
                ))
            }
            Rule::kw_desc => descending = true,
            Rule::nulls_ordering => {
                let text = part.as_str().to_ascii_lowercase();
                nulls_first = Some(text.contains("first"));
            }
            _ => {}
        }
    }
    Ok(IndexColumnDef {
        name: name.ok_or(ParseError::UnexpectedEof)?,
        collation: None,
        opclass,
        descending,
        nulls_first,
    })
}

fn build_alter_table_set(pair: Pair<'_, Rule>) -> Result<AlterTableSetStatement, ParseError> {
    let mut table_name = None;
    let mut options = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::reloption => options.push(build_reloption(part)?),
            _ => {}
        }
    }
    Ok(AlterTableSetStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        options,
    })
}

fn build_comment_on_table(pair: Pair<'_, Rule>) -> Result<CommentOnTableStatement, ParseError> {
    let mut table_name = None;
    let mut comment = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier => table_name = Some(build_identifier(part)),
            Rule::quoted_string_literal
            | Rule::string_literal
            | Rule::unicode_string_literal
            | Rule::escape_string_literal
            | Rule::dollar_string_literal => {
                comment = Some(Some(decode_string_literal_pair(part)?))
            }
            Rule::kw_null => comment = Some(None),
            _ => {}
        }
    }
    Ok(CommentOnTableStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        comment: comment.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_comment_on_role(pair: Pair<'_, Rule>) -> Result<CommentOnRoleStatement, ParseError> {
    let mut role_name = None;
    let mut comment = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier => role_name = Some(build_identifier(part)),
            Rule::quoted_string_literal
            | Rule::string_literal
            | Rule::unicode_string_literal
            | Rule::escape_string_literal
            | Rule::dollar_string_literal => {
                comment = Some(Some(decode_string_literal_pair(part)?))
            }
            Rule::kw_null => comment = Some(None),
            _ => {}
        }
    }
    Ok(CommentOnRoleStatement {
        role_name: role_name.ok_or(ParseError::UnexpectedEof)?,
        comment: comment.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_reloption(pair: Pair<'_, Rule>) -> Result<RelOption, ParseError> {
    let mut name = None;
    let mut value = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if name.is_none() => name = Some(build_identifier(part)),
            Rule::set_value_atom => value = Some(build_set_value_atom(part)?),
            _ => {}
        }
    }
    Ok(RelOption {
        name: name.ok_or(ParseError::UnexpectedEof)?,
        value: value.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_set_value_atom(pair: Pair<'_, Rule>) -> Result<String, ParseError> {
    let mut inner = pair.into_inner();
    let part = inner.next().ok_or(ParseError::UnexpectedEof)?;
    match part.as_rule() {
        Rule::signed_set_value => Ok(part.as_str().to_string()),
        Rule::quoted_string_literal
        | Rule::string_literal
        | Rule::unicode_string_literal
        | Rule::escape_string_literal
        | Rule::dollar_string_literal => decode_string_literal_pair(part),
        Rule::identifier | Rule::numeric_literal | Rule::integer => Ok(part.as_str().to_string()),
        Rule::kw_default | Rule::kw_true | Rule::kw_false | Rule::kw_on_value | Rule::kw_off => {
            Ok(part.as_str().to_ascii_lowercase())
        }
        _ => Ok(part.as_str().to_string()),
    }
}

fn validate_table_storage_clause(pair: Pair<'_, Rule>) -> Result<(), ParseError> {
    let part = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match part.as_rule() {
        Rule::without_oids_clause => Ok(()),
        Rule::table_with_clause => {
            for item in part
                .into_inner()
                .filter(|inner| inner.as_rule() == Rule::table_with_item)
            {
                let mut item_parts = item.into_inner();
                let name = build_identifier(item_parts.next().ok_or(ParseError::UnexpectedEof)?);
                let value = item_parts
                    .next()
                    .map(|value| value.as_str().to_ascii_lowercase());
                if name != name.to_ascii_lowercase() {
                    return Err(ParseError::UnrecognizedParameter(name));
                }
                if name.eq_ignore_ascii_case("oids")
                    && value
                        .as_deref()
                        .map_or(true, |value| matches!(value, "true" | "on" | "1"))
                {
                    return Err(ParseError::TablesDeclaredWithOidsNotSupported);
                }
            }
            Ok(())
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "table storage clause",
            actual: part.as_str().to_string(),
        }),
    }
}

fn build_relation_name(pair: Pair<'_, Rule>) -> (Option<String>, String) {
    let name = build_identifier(pair);
    if let Some((schema, rel)) = name.split_once('.') {
        (Some(schema.to_string()), rel.to_string())
    } else {
        (None, name)
    }
}

fn build_on_commit_action(pair: Pair<'_, Rule>) -> Result<OnCommitAction, ParseError> {
    let action = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::on_commit_action)
        .ok_or(ParseError::UnexpectedEof)?;
    let text = action.as_str();
    if text.eq_ignore_ascii_case("drop") {
        Ok(OnCommitAction::Drop)
    } else if text.eq_ignore_ascii_case("delete rows") {
        Ok(OnCommitAction::DeleteRows)
    } else {
        Ok(OnCommitAction::PreserveRows)
    }
}

fn build_drop_table(pair: Pair<'_, Rule>) -> Result<DropTableStatement, ParseError> {
    let mut if_exists = false;
    let mut table_names = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::if_exists_clause => if_exists = true,
            Rule::ident_list => {
                table_names.extend(part.into_inner().map(build_identifier));
            }
            Rule::identifier => table_names.push(build_identifier(part)),
            _ => {}
        }
    }
    if table_names.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    Ok(DropTableStatement {
        if_exists,
        table_names,
    })
}

fn build_drop_role(pair: Pair<'_, Rule>) -> Result<DropRoleStatement, ParseError> {
    let mut if_exists = false;
    let mut role_names = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::if_exists_clause => if_exists = true,
            Rule::ident_list => role_names.extend(part.into_inner().map(build_identifier)),
            Rule::identifier => role_names.push(build_identifier(part)),
            _ => {}
        }
    }
    if role_names.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    Ok(DropRoleStatement {
        if_exists,
        role_names,
    })
}

fn build_drop_view(pair: Pair<'_, Rule>) -> Result<DropViewStatement, ParseError> {
    let mut if_exists = false;
    let mut view_names = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::if_exists_clause => if_exists = true,
            Rule::ident_list => {
                view_names.extend(part.into_inner().map(build_identifier));
            }
            Rule::identifier => view_names.push(build_identifier(part)),
            _ => {}
        }
    }
    if view_names.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    Ok(DropViewStatement {
        if_exists,
        view_names,
    })
}

fn build_reassign_owned(pair: Pair<'_, Rule>) -> Result<ReassignOwnedStatement, ParseError> {
    let mut old_roles = Vec::new();
    let mut new_role = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::ident_list if old_roles.is_empty() => {
                old_roles.extend(part.into_inner().map(build_identifier));
            }
            Rule::identifier if new_role.is_none() && !old_roles.is_empty() => {
                new_role = Some(build_identifier(part));
            }
            Rule::identifier => old_roles.push(build_identifier(part)),
            _ => {}
        }
    }
    Ok(ReassignOwnedStatement {
        old_roles,
        new_role: new_role.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_truncate_table(pair: Pair<'_, Rule>) -> Result<TruncateTableStatement, ParseError> {
    let table_names = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::ident_list)
        .map(|part| part.into_inner().map(build_identifier).collect::<Vec<_>>())
        .ok_or(ParseError::UnexpectedEof)?;
    Ok(TruncateTableStatement { table_names })
}

fn build_vacuum(pair: Pair<'_, Rule>) -> Result<VacuumStatement, ParseError> {
    let mut targets = Vec::new();
    let mut analyze = false;
    let mut full = false;
    let mut verbose = false;
    let mut skip_locked = false;
    let mut buffer_usage_limit = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::kw_analyze => analyze = true,
            Rule::vacuum_option_block => {
                for opt in part.into_inner() {
                    let opt = if opt.as_rule() == Rule::vacuum_option {
                        opt.into_inner().next().ok_or(ParseError::UnexpectedEof)?
                    } else {
                        opt
                    };
                    match opt.as_rule() {
                        Rule::vacuum_analyze_option => analyze = parse_option_bool(opt)?,
                        Rule::vacuum_full_option => full = parse_option_bool(opt)?,
                        Rule::analyze_verbose_option => verbose = parse_option_bool(opt)?,
                        Rule::analyze_skip_locked_option => skip_locked = parse_option_bool(opt)?,
                        Rule::analyze_buffer_usage_limit_option => {
                            buffer_usage_limit = Some(parse_option_scalar(opt)?)
                        }
                        _ => {}
                    }
                }
            }
            Rule::maintenance_target_list => targets = build_maintenance_target_list(part)?,
            _ => {}
        }
    }
    Ok(VacuumStatement {
        targets,
        analyze,
        full,
        verbose,
        skip_locked,
        buffer_usage_limit,
    })
}

fn build_maintenance_target_list(
    pair: Pair<'_, Rule>,
) -> Result<Vec<MaintenanceTarget>, ParseError> {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::maintenance_target)
        .map(build_maintenance_target)
        .collect()
}

fn build_maintenance_target(pair: Pair<'_, Rule>) -> Result<MaintenanceTarget, ParseError> {
    let mut only = false;
    let mut table_name = None;
    let mut columns = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::only_clause => only = true,
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::maintenance_column_list => {
                columns = part
                    .into_inner()
                    .find(|p| p.as_rule() == Rule::ident_list)
                    .map(|p| p.into_inner().map(build_identifier).collect())
                    .unwrap_or_default();
            }
            _ => {}
        }
    }
    Ok(MaintenanceTarget {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        columns,
        only,
    })
}

fn build_update(pair: Pair<'_, Rule>) -> Result<UpdateStatement, ParseError> {
    let mut with_recursive = false;
    let mut with = Vec::new();
    let mut table_name = None;
    let mut only = false;
    let mut assignments = Vec::new();
    let mut where_clause = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::cte_clause => {
                let (recursive, ctes) = build_cte_clause(part)?;
                with_recursive = recursive;
                with = ctes;
            }
            Rule::only_clause => only = true,
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::assignment => assignments.push(build_assignment(part)?),
            Rule::expr => where_clause = Some(build_expr(part)?),
            _ => {}
        }
    }
    Ok(UpdateStatement {
        with_recursive,
        with,
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        only,
        assignments,
        where_clause,
    })
}

fn build_delete(pair: Pair<'_, Rule>) -> Result<DeleteStatement, ParseError> {
    let mut with_recursive = false;
    let mut with = Vec::new();
    let mut table_name = None;
    let mut only = false;
    let mut where_clause = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::cte_clause => {
                let (recursive, ctes) = build_cte_clause(part)?;
                with_recursive = recursive;
                with = ctes;
            }
            Rule::only_clause => only = true,
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::expr => where_clause = Some(build_expr(part)?),
            _ => {}
        }
    }
    Ok(DeleteStatement {
        with_recursive,
        with,
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        only,
        where_clause,
    })
}

fn build_select_list(pair: Pair<'_, Rule>) -> Result<Vec<SelectItem>, ParseError> {
    let mut inner = pair.into_inner();
    let first = inner.next().ok_or(ParseError::EmptySelectList)?;
    if first.as_rule() == Rule::star {
        return Ok(vec![SelectItem {
            output_name: "*".into(),
            expr: SqlExpr::Column("*".into()),
        }]);
    }

    let mut items = Vec::new();
    for (index, item_pair) in std::iter::once(first).chain(inner).enumerate() {
        let mut preview_inner = item_pair.clone().into_inner();
        if let Some(first_part) = preview_inner.next() {
            if first_part.as_rule() == Rule::star {
                items.push(SelectItem {
                    output_name: "*".into(),
                    expr: SqlExpr::Column("*".into()),
                });
                continue;
            }
            if first_part.as_rule() == Rule::qualified_star {
                let relation = first_part
                    .as_str()
                    .strip_suffix(".*")
                    .ok_or(ParseError::UnexpectedEof)?
                    .to_string();
                items.push(SelectItem {
                    output_name: "*".into(),
                    expr: SqlExpr::Column(format!("{relation}.*")),
                });
                continue;
            }
        }

        let mut item_inner = item_pair.into_inner();
        let expr = build_expr(item_inner.next().ok_or(ParseError::UnexpectedEof)?)?;
        let output_name = if let Some(alias_pair) = item_inner.next() {
            let alias = alias_pair
                .into_inner()
                .last()
                .ok_or(ParseError::UnexpectedEof)?;
            build_identifier(alias)
        } else {
            select_item_name(&expr, index)
        };
        items.push(SelectItem { output_name, expr });
    }

    Ok(items)
}

fn select_item_name(expr: &SqlExpr, index: usize) -> String {
    let _ = index;
    match expr {
        SqlExpr::Column(name) => name.rsplit('.').next().unwrap_or(name).to_string(),
        SqlExpr::Cast(inner, ty) => match inner.as_ref() {
            SqlExpr::Column(_) => select_item_name(inner, index),
            SqlExpr::Cast(grand_inner, _) if matches!(grand_inner.as_ref(), SqlExpr::Column(_)) => {
                select_item_name(inner, index)
            }
            _ => raw_type_output_name(ty).to_string(),
        },
        SqlExpr::Row(_) => "row".to_string(),
        SqlExpr::AggCall { func, .. } => func.name().to_string(),
        SqlExpr::Random => "random".to_string(),
        SqlExpr::FuncCall { name, .. } => name.clone(),
        _ => "?column?".to_string(),
    }
}

fn sql_type_output_name(ty: SqlType) -> &'static str {
    match ty.kind {
        SqlTypeKind::Record => "record",
        SqlTypeKind::Composite => "record",
        SqlTypeKind::Int2 => "int2",
        SqlTypeKind::Int2Vector => "int2vector",
        SqlTypeKind::Int4 => "int4",
        SqlTypeKind::Int8 => "int8",
        SqlTypeKind::Name => "name",
        SqlTypeKind::Oid => "oid",
        SqlTypeKind::OidVector => "oidvector",
        SqlTypeKind::Bit => "bit",
        SqlTypeKind::VarBit => "varbit",
        SqlTypeKind::Float4 => "float4",
        SqlTypeKind::Float8 => "float8",
        SqlTypeKind::Money => "money",
        SqlTypeKind::Numeric => "numeric",
        SqlTypeKind::Json => "json",
        SqlTypeKind::Jsonb => "jsonb",
        SqlTypeKind::JsonPath => "jsonpath",
        SqlTypeKind::Date => "date",
        SqlTypeKind::Time => "time without time zone",
        SqlTypeKind::TimeTz => "time with time zone",
        SqlTypeKind::TsVector => "tsvector",
        SqlTypeKind::TsQuery => "tsquery",
        SqlTypeKind::RegConfig => "regconfig",
        SqlTypeKind::RegDictionary => "regdictionary",
        SqlTypeKind::AnyArray => "anyarray",
        SqlTypeKind::Text => "text",
        SqlTypeKind::Bytea => "bytea",
        SqlTypeKind::Bool => "bool",
        SqlTypeKind::Point => "point",
        SqlTypeKind::Lseg => "lseg",
        SqlTypeKind::Path => "path",
        SqlTypeKind::Box => "box",
        SqlTypeKind::Polygon => "polygon",
        SqlTypeKind::Line => "line",
        SqlTypeKind::Circle => "circle",
        SqlTypeKind::Timestamp => "timestamp without time zone",
        SqlTypeKind::TimestampTz => "timestamp with time zone",
        SqlTypeKind::PgNodeTree => "pg_node_tree",
        SqlTypeKind::InternalChar => "char",
        SqlTypeKind::Char => "bpchar",
        SqlTypeKind::Varchar => "varchar",
    }
}

fn raw_type_output_name(ty: &RawTypeName) -> &str {
    match ty {
        RawTypeName::Builtin(sql_type) => sql_type_output_name(*sql_type),
        RawTypeName::Named { name, .. } => name.as_str(),
        RawTypeName::Record => "record",
    }
}

fn build_values_row(pair: Pair<'_, Rule>) -> Result<Vec<SqlExpr>, ParseError> {
    pair.into_inner()
        .next()
        .ok_or(ParseError::UnexpectedEof)?
        .into_inner()
        .map(build_expr)
        .collect()
}

fn build_assignment(pair: Pair<'_, Rule>) -> Result<Assignment, ParseError> {
    let mut inner = pair.into_inner();
    Ok(Assignment {
        target: build_assignment_target(inner.next().ok_or(ParseError::UnexpectedEof)?)?,
        expr: build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?,
    })
}

fn build_assignment_target_list(pair: Pair<'_, Rule>) -> Result<Vec<AssignmentTarget>, ParseError> {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::assignment_target)
        .map(build_assignment_target)
        .collect()
}

fn build_assignment_target(pair: Pair<'_, Rule>) -> Result<AssignmentTarget, ParseError> {
    let mut inner = pair.into_inner();
    let column = build_identifier(inner.next().ok_or(ParseError::UnexpectedEof)?);
    let mut subscripts = Vec::new();
    for part in inner {
        if part.as_rule() == Rule::subscript_suffix {
            subscripts.push(build_array_subscript(part)?);
        }
    }
    Ok(AssignmentTarget { column, subscripts })
}

fn build_array_subscript(pair: Pair<'_, Rule>) -> Result<ArraySubscript, ParseError> {
    let raw = pair.as_str().to_string();
    let mut bounds = pair
        .into_inner()
        .filter(|part| part.as_rule() == Rule::subscript_bound)
        .map(|bound| {
            let expr = bound.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
            build_expr(expr).map(Box::new)
        });
    let has_slice = raw.contains(':');
    let lower = bounds.next().transpose()?;
    let upper = if has_slice {
        bounds.next().transpose()?
    } else {
        None
    };
    Ok(ArraySubscript {
        is_slice: has_slice,
        lower,
        upper,
    })
}

fn build_column_def(pair: Pair<'_, Rule>) -> Result<ColumnDef, ParseError> {
    let mut inner = pair.into_inner();
    let name = build_identifier(inner.next().ok_or(ParseError::UnexpectedEof)?);
    let ty = build_type_name(inner.next().ok_or(ParseError::UnexpectedEof)?);
    let mut default_expr = None;
    let mut constraints = Vec::new();
    for flag in inner {
        let Some(flag) = (match flag.as_rule() {
            Rule::column_modifier => flag.into_inner().next(),
            _ => Some(flag),
        }) else {
            continue;
        };
        match flag.as_rule() {
            Rule::column_default => {
                default_expr = flag
                    .into_inner()
                    .find(|part| part.as_rule() == Rule::expr)
                    .map(|expr| expr.as_str().to_string());
            }
            Rule::nullable => {}
            Rule::named_column_constraint
            | Rule::not_null_column_constraint
            | Rule::check_column_constraint
            | Rule::primary_key_column_constraint
            | Rule::unique_column_constraint => constraints.push(build_column_constraint(flag)?),
            _ => {}
        }
    }
    Ok(ColumnDef {
        name,
        ty,
        default_expr,
        constraints,
    })
}

fn build_alter_table_add_column(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableAddColumnStatement, ParseError> {
    let mut table_name = None;
    let mut column = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::column_def => column = Some(build_column_def(part)?),
            _ => {}
        }
    }
    Ok(AlterTableAddColumnStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        column: column.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_add_constraint(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableAddConstraintStatement, ParseError> {
    let mut table_name = None;
    let mut constraint = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::named_table_constraint => constraint = Some(build_table_constraint_inner(part)?),
            _ => {}
        }
    }
    Ok(AlterTableAddConstraintStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        constraint: constraint.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_drop_column(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableDropColumnStatement, ParseError> {
    let mut parts = pair
        .into_inner()
        .filter(|part| part.as_rule() == Rule::identifier)
        .map(build_identifier);
    Ok(AlterTableDropColumnStatement {
        table_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        column_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_drop_constraint(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableDropConstraintStatement, ParseError> {
    let mut parts = pair
        .into_inner()
        .filter(|part| part.as_rule() == Rule::identifier)
        .map(build_identifier);
    Ok(AlterTableDropConstraintStatement {
        table_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        constraint_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_alter_column_type(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableAlterColumnTypeStatement, ParseError> {
    let mut table_name = None;
    let mut column_name = None;
    let mut ty = None;
    let mut using_expr = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::identifier if column_name.is_none() => column_name = Some(build_identifier(part)),
            Rule::alter_table_column_type_action => {
                for inner in part.into_inner() {
                    match inner.as_rule() {
                        Rule::type_name => ty = Some(build_type_name(inner)),
                        Rule::alter_table_using_clause => {
                            let expr = inner
                                .into_inner()
                                .find(|item| item.as_rule() == Rule::expr)
                                .ok_or(ParseError::UnexpectedEof)?;
                            using_expr = Some(build_expr(expr)?);
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(AlterTableAlterColumnTypeStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        column_name: column_name.ok_or(ParseError::UnexpectedEof)?,
        ty: ty.ok_or(ParseError::UnexpectedEof)?,
        using_expr,
    })
}

fn build_alter_table_set_not_null(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableSetNotNullStatement, ParseError> {
    let mut parts = pair
        .into_inner()
        .filter(|part| part.as_rule() == Rule::identifier)
        .map(build_identifier);
    Ok(AlterTableSetNotNullStatement {
        table_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        column_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_drop_not_null(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableDropNotNullStatement, ParseError> {
    let mut parts = pair
        .into_inner()
        .filter(|part| part.as_rule() == Rule::identifier)
        .map(build_identifier);
    Ok(AlterTableDropNotNullStatement {
        table_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        column_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_validate_constraint(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableValidateConstraintStatement, ParseError> {
    let mut parts = pair
        .into_inner()
        .filter(|part| part.as_rule() == Rule::identifier)
        .map(build_identifier);
    Ok(AlterTableValidateConstraintStatement {
        table_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        constraint_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_relation_owner(
    pair: Pair<'_, Rule>,
) -> Result<AlterRelationOwnerStatement, ParseError> {
    let mut parts = pair
        .into_inner()
        .filter(|part| part.as_rule() == Rule::identifier)
        .map(build_identifier);
    Ok(AlterRelationOwnerStatement {
        relation_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        new_owner: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_rename(pair: Pair<'_, Rule>) -> Result<AlterTableRenameStatement, ParseError> {
    let mut parts = pair
        .into_inner()
        .filter(|part| part.as_rule() == Rule::identifier)
        .map(build_identifier);
    Ok(AlterTableRenameStatement {
        table_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        new_table_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_rename_column(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableRenameColumnStatement, ParseError> {
    let mut parts = pair
        .into_inner()
        .filter(|part| part.as_rule() == Rule::identifier)
        .map(build_identifier);
    Ok(AlterTableRenameColumnStatement {
        table_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        column_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        new_column_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_type_name(pair: Pair<'_, Rule>) -> RawTypeName {
    match pair.as_rule() {
        Rule::type_name | Rule::known_type_name => {
            let mut inner = pair.into_inner();
            let mut ty = build_type_name(inner.next().expect("type_name base"));
            for _ in inner {
                ty = match ty {
                    RawTypeName::Builtin(inner_ty) => {
                        RawTypeName::Builtin(SqlType::array_of(inner_ty))
                    }
                    RawTypeName::Named { name, array_bounds } => RawTypeName::Named {
                        name,
                        array_bounds: array_bounds.saturating_add(1),
                    },
                    other => other,
                };
            }
            ty
        }
        Rule::known_base_type_name => {
            build_type_name(pair.into_inner().next().expect("base_type_name inner"))
        }
        Rule::qualified_known_base_type_name => {
            let mut inner = pair.into_inner();
            inner.next().expect("qualified_type_name schema");
            build_type_name(inner.next().expect("qualified_type_name base"))
        }
        Rule::array_type_alias => {
            let base = match pair
                .as_str()
                .trim_start_matches('_')
                .to_ascii_lowercase()
                .as_str()
            {
                "int2" | "smallint" => SqlType::new(SqlTypeKind::Int2),
                "int4" | "int" | "integer" => SqlType::new(SqlTypeKind::Int4),
                "int8" | "bigint" => SqlType::new(SqlTypeKind::Int8),
                "oid" => SqlType::new(SqlTypeKind::Oid),
                "name" => SqlType::new(SqlTypeKind::Name),
                "text" => SqlType::new(SqlTypeKind::Text),
                "bool" | "boolean" => SqlType::new(SqlTypeKind::Bool),
                "bytea" => SqlType::new(SqlTypeKind::Bytea),
                "money" => SqlType::new(SqlTypeKind::Money),
                "float4" | "real" => SqlType::new(SqlTypeKind::Float4),
                "float8" => SqlType::new(SqlTypeKind::Float8),
                "timestamp" => SqlType::new(SqlTypeKind::Timestamp),
                "json" => SqlType::new(SqlTypeKind::Json),
                "jsonb" => SqlType::new(SqlTypeKind::Jsonb),
                "jsonpath" => SqlType::new(SqlTypeKind::JsonPath),
                other => panic!("unsupported array type alias: {other}"),
            };
            RawTypeName::Builtin(SqlType::array_of(base))
        }
        Rule::kw_record => RawTypeName::Record,
        Rule::kw_int2 | Rule::kw_smallint => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int2)),
        Rule::kw_int4 | Rule::kw_int | Rule::kw_integer => {
            RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4))
        }
        Rule::kw_int8 | Rule::kw_bigint => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int8)),
        Rule::kw_name => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Name)),
        Rule::kw_oid => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Oid)),
        Rule::bit_type => {
            let len = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()
                .expect("bit length");
            match len {
                Some(len) => RawTypeName::Builtin(SqlType::with_bit_len(SqlTypeKind::Bit, len)),
                None => RawTypeName::Builtin(SqlType::with_bit_len(SqlTypeKind::Bit, 1)),
            }
        }
        Rule::varbit_type => {
            let len = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()
                .expect("varbit length");
            match len {
                Some(len) => RawTypeName::Builtin(SqlType::with_bit_len(SqlTypeKind::VarBit, len)),
                None => RawTypeName::Builtin(SqlType::new(SqlTypeKind::VarBit)),
            }
        }
        Rule::kw_bytea => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Bytea)),
        Rule::kw_money => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Money)),
        Rule::kw_float4 | Rule::kw_real => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Float4)),
        Rule::kw_float8 | Rule::double_precision_type => {
            RawTypeName::Builtin(SqlType::new(SqlTypeKind::Float8))
        }
        Rule::numeric_type => {
            let dims = pair
                .into_inner()
                .filter(|part| matches!(part.as_rule(), Rule::integer | Rule::signed_integer))
                .map(build_numeric_typemod_component)
                .collect::<Result<Vec<_>, _>>()
                .expect("numeric precision/scale");
            match dims.as_slice() {
                [] => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Numeric)),
                [precision] => {
                    RawTypeName::Builtin(SqlType::with_numeric_precision_scale(*precision, 0))
                }
                [precision, scale] => {
                    RawTypeName::Builtin(SqlType::with_numeric_precision_scale(*precision, *scale))
                }
                _ => unreachable!("unexpected numeric typmod arity"),
            }
        }
        Rule::kw_text => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Text)),
        Rule::kw_json => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Json)),
        Rule::kw_jsonb => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Jsonb)),
        Rule::kw_jsonpath => RawTypeName::Builtin(SqlType::new(SqlTypeKind::JsonPath)),
        Rule::kw_tsvector => RawTypeName::Builtin(SqlType::new(SqlTypeKind::TsVector)),
        Rule::kw_tsquery => RawTypeName::Builtin(SqlType::new(SqlTypeKind::TsQuery)),
        Rule::kw_regclass => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Oid)),
        Rule::kw_regconfig => RawTypeName::Builtin(SqlType::new(SqlTypeKind::RegConfig)),
        Rule::kw_regdictionary => RawTypeName::Builtin(SqlType::new(SqlTypeKind::RegDictionary)),
        Rule::kw_bool | Rule::kw_boolean => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Bool)),
        Rule::date_type | Rule::kw_date => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Date)),
        Rule::time_type => {
            let precision = pair
                .clone()
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()
                .expect("time precision");
            let kind = if pair.as_str().eq_ignore_ascii_case("timetz")
                || pair
                    .as_str()
                    .to_ascii_lowercase()
                    .contains("with time zone")
            {
                SqlTypeKind::TimeTz
            } else {
                SqlTypeKind::Time
            };
            RawTypeName::Builtin(
                precision
                    .map(|precision| SqlType::with_time_precision(kind, precision))
                    .unwrap_or_else(|| SqlType::new(kind)),
            )
        }
        Rule::timestamp_type | Rule::kw_timestamp => {
            let precision = pair
                .clone()
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()
                .expect("timestamp precision");
            let kind = if pair.as_str().eq_ignore_ascii_case("timestamptz")
                || pair
                    .as_str()
                    .to_ascii_lowercase()
                    .contains("with time zone")
            {
                SqlTypeKind::TimestampTz
            } else {
                SqlTypeKind::Timestamp
            };
            RawTypeName::Builtin(
                precision
                    .map(|precision| SqlType::with_time_precision(kind, precision))
                    .unwrap_or_else(|| SqlType::new(kind)),
            )
        }
        Rule::kw_point => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Point)),
        Rule::kw_lseg => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Lseg)),
        Rule::kw_path => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Path)),
        Rule::kw_box => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Box)),
        Rule::kw_polygon => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Polygon)),
        Rule::kw_line => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Line)),
        Rule::kw_circle => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Circle)),
        Rule::internal_char_type => RawTypeName::Builtin(SqlType::new(SqlTypeKind::InternalChar)),
        Rule::char_type => {
            let len = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()
                .expect("char length");
            match len {
                Some(len) => RawTypeName::Builtin(SqlType::with_char_len(SqlTypeKind::Char, len)),
                None => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Char)),
            }
        }
        Rule::varchar_type | Rule::character_varying_type => {
            let len = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()
                .expect("varchar length");
            match len {
                Some(len) => {
                    RawTypeName::Builtin(SqlType::with_char_len(SqlTypeKind::Varchar, len))
                }
                None => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Varchar)),
            }
        }
        Rule::named_type_name => RawTypeName::Named {
            name: build_identifier(pair.into_inner().next().expect("named_type_name inner")),
            array_bounds: 0,
        },
        _ => unreachable!("unexpected type rule {:?}", pair.as_rule()),
    }
}

fn build_type_len(pair: Pair<'_, Rule>) -> Result<i32, ParseError> {
    pair.as_str()
        .parse::<i32>()
        .map_err(|_| ParseError::InvalidInteger(pair.as_str().to_string()))
}

fn build_numeric_typemod_component(pair: Pair<'_, Rule>) -> Result<i32, ParseError> {
    pair.as_str()
        .parse::<i32>()
        .map_err(|_| ParseError::InvalidInteger(pair.as_str().to_string()))
}

fn parse_i32(pair: Pair<'_, Rule>) -> Result<i32, ParseError> {
    pair.as_str()
        .parse::<i32>()
        .map_err(|_| ParseError::InvalidInteger(pair.as_str().to_string()))
}

fn build_identifier(pair: Pair<'_, Rule>) -> String {
    if pair.as_rule() == Rule::identifier {
        if let Some(inner) = pair.clone().into_inner().next() {
            return build_identifier(inner);
        }
    }
    let raw = pair.as_str();
    if pair.as_rule() == Rule::unicode_quoted_identifier {
        return decode_unicode_quoted_identifier(raw).unwrap_or_else(|_| raw.to_string());
    }
    if raw.starts_with('"') && raw.ends_with('"') {
        raw[1..raw.len() - 1].replace("\"\"", "\"")
    } else {
        raw.to_string()
    }
}

pub(crate) fn build_expr(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    match pair.as_rule() {
        Rule::expr
        | Rule::or_expr
        | Rule::and_expr
        | Rule::concat_expr
        | Rule::add_expr
        | Rule::bit_expr
        | Rule::shift_expr
        | Rule::pow_expr
        | Rule::mul_expr => {
            let mut inner = pair.into_inner();
            let first = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            fold_infix(first, inner)
        }
        Rule::postfix_expr => {
            let mut inner = pair.into_inner();
            let mut expr = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            for suffix in inner {
                match suffix.as_rule() {
                    Rule::cast_suffix => {
                        let ty = build_type_name(
                            suffix
                                .into_inner()
                                .find(|part| part.as_rule() == Rule::type_name)
                                .ok_or(ParseError::UnexpectedEof)?,
                        );
                        expr = SqlExpr::Cast(Box::new(expr), ty);
                    }
                    Rule::subscript_suffix => {
                        let subscript = build_array_subscript(suffix)?;
                        expr = match expr {
                            SqlExpr::ArraySubscript {
                                array,
                                mut subscripts,
                            } => {
                                subscripts.push(subscript);
                                SqlExpr::ArraySubscript { array, subscripts }
                            }
                            other => SqlExpr::ArraySubscript {
                                array: Box::new(other),
                                subscripts: vec![subscript],
                            },
                        };
                    }
                    Rule::field_select_suffix => {
                        let field = suffix
                            .into_inner()
                            .find(|part| part.as_rule() == Rule::identifier)
                            .map(build_identifier)
                            .ok_or(ParseError::UnexpectedEof)?;
                        expr = SqlExpr::FieldSelect {
                            expr: Box::new(expr),
                            field,
                        };
                    }
                    _ => {}
                }
            }
            Ok(expr)
        }
        Rule::unary_expr => build_expr(pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?),
        Rule::positive_expr => Ok(SqlExpr::UnaryPlus(Box::new(build_expr(
            pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?,
        )?))),
        Rule::negated_expr => {
            let raw = pair.as_str().trim_start();
            let expr = build_expr(pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?)?;
            if raw.starts_with("@-@") {
                Ok(SqlExpr::GeometryUnaryOp {
                    op: GeometryUnaryOp::Length,
                    expr: Box::new(expr),
                })
            } else if raw.starts_with('#') {
                Ok(SqlExpr::GeometryUnaryOp {
                    op: GeometryUnaryOp::Npoints,
                    expr: Box::new(expr),
                })
            } else if raw.starts_with("@@") {
                Ok(SqlExpr::GeometryUnaryOp {
                    op: GeometryUnaryOp::Center,
                    expr: Box::new(expr),
                })
            } else if raw.starts_with("?|") {
                Ok(SqlExpr::GeometryUnaryOp {
                    op: GeometryUnaryOp::IsVertical,
                    expr: Box::new(expr),
                })
            } else if raw.starts_with("?-") {
                Ok(SqlExpr::GeometryUnaryOp {
                    op: GeometryUnaryOp::IsHorizontal,
                    expr: Box::new(expr),
                })
            } else if raw.starts_with("||/") {
                Ok(SqlExpr::FuncCall {
                    name: "cbrt".into(),
                    args: vec![SqlFunctionArg::positional(expr)],
                    func_variadic: false,
                })
            } else if raw.starts_with("!!") {
                Ok(SqlExpr::PrefixOperator {
                    op: "!!".into(),
                    expr: Box::new(expr),
                })
            } else if raw.starts_with("|/") {
                Ok(SqlExpr::FuncCall {
                    name: "sqrt".into(),
                    args: vec![SqlFunctionArg::positional(expr)],
                    func_variadic: false,
                })
            } else if raw.starts_with('@') {
                Ok(SqlExpr::FuncCall {
                    name: "abs".into(),
                    args: vec![SqlFunctionArg::positional(expr)],
                    func_variadic: false,
                })
            } else if raw.starts_with('~') {
                Ok(SqlExpr::BitNot(Box::new(expr)))
            } else {
                Ok(SqlExpr::Negate(Box::new(expr)))
            }
        }
        Rule::not_expr => {
            let mut inner = pair.into_inner();
            let first = inner.next().ok_or(ParseError::UnexpectedEof)?;
            if first.as_rule() == Rule::kw_not {
                Ok(SqlExpr::Not(Box::new(build_expr(
                    inner.next().ok_or(ParseError::UnexpectedEof)?,
                )?)))
            } else {
                build_expr(first)
            }
        }
        Rule::cmp_expr => {
            let mut inner = pair.into_inner();
            let left = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            let Some(next) = inner.next() else {
                return Ok(left);
            };

            match next.as_rule() {
                Rule::null_predicate_suffix => build_null_predicate(left, next),
                Rule::between_suffix => {
                    let mut negated = false;
                    let mut bounds = Vec::new();
                    for part in next.into_inner() {
                        match part.as_rule() {
                            Rule::kw_not => negated = true,
                            Rule::concat_expr => bounds.push(build_expr(part)?),
                            _ => {}
                        }
                    }
                    let low = bounds.first().cloned().ok_or(ParseError::UnexpectedEof)?;
                    let high = bounds.get(1).cloned().ok_or(ParseError::UnexpectedEof)?;
                    let between = SqlExpr::And(
                        Box::new(SqlExpr::GtEq(Box::new(left.clone()), Box::new(low))),
                        Box::new(SqlExpr::LtEq(Box::new(left), Box::new(high))),
                    );
                    Ok(if negated {
                        SqlExpr::Not(Box::new(between))
                    } else {
                        between
                    })
                }
                Rule::in_expr_list_suffix => {
                    let mut negated = false;
                    let mut values = Vec::new();
                    for part in next.into_inner() {
                        match part.as_rule() {
                            Rule::kw_not => negated = true,
                            Rule::expr_list => {
                                values = part
                                    .into_inner()
                                    .filter(|part| part.as_rule() == Rule::expr)
                                    .map(build_expr)
                                    .collect::<Result<Vec<_>, _>>()?;
                            }
                            _ => {}
                        }
                    }
                    Ok(SqlExpr::QuantifiedArray {
                        left: Box::new(left),
                        op: SubqueryComparisonOp::Eq,
                        is_all: negated,
                        array: Box::new(SqlExpr::ArrayLiteral(values)),
                    })
                }

                Rule::in_subquery_suffix => {
                    let mut negated = false;
                    let mut subquery = None;
                    for part in next.into_inner() {
                        match part.as_rule() {
                            Rule::kw_not => negated = true,
                            Rule::select_stmt => {
                                subquery = Some(build_select(part)?);
                            }
                            _ => {}
                        }
                    }
                    Ok(SqlExpr::InSubquery {
                        expr: Box::new(left),
                        subquery: Box::new(subquery.ok_or(ParseError::UnexpectedEof)?),
                        negated,
                    })
                }
                Rule::quantified_suffix => {
                    let mut parts = next.into_inner();
                    let op = match parts.next().ok_or(ParseError::UnexpectedEof)?.as_str() {
                        "@@" => SubqueryComparisonOp::Match,
                        "&&" => {
                            return Err(ParseError::UnexpectedToken {
                                expected: "comparison operator for ANY/ALL",
                                actual: "&&".into(),
                            });
                        }
                        "=" => SubqueryComparisonOp::Eq,
                        "<>" | "!=" => SubqueryComparisonOp::NotEq,
                        "<" => SubqueryComparisonOp::Lt,
                        "<=" => SubqueryComparisonOp::LtEq,
                        ">" => SubqueryComparisonOp::Gt,
                        ">=" => SubqueryComparisonOp::GtEq,
                        other => {
                            return Err(ParseError::UnexpectedToken {
                                expected: "subquery comparison operator",
                                actual: other.into(),
                            });
                        }
                    };
                    let quantifier = parts.next().ok_or(ParseError::UnexpectedEof)?;
                    let is_all = match quantifier.as_str().to_ascii_lowercase().as_str() {
                        "any" => false,
                        "all" => true,
                        _ => {
                            return Err(ParseError::UnexpectedToken {
                                expected: "ANY or ALL",
                                actual: quantifier.as_str().into(),
                            });
                        }
                    };
                    let rhs = parts.next().ok_or(ParseError::UnexpectedEof)?;
                    Ok(match rhs.as_rule() {
                        Rule::select_stmt => SqlExpr::QuantifiedSubquery {
                            left: Box::new(left),
                            op,
                            is_all,
                            subquery: Box::new(build_select(rhs)?),
                        },
                        Rule::expr => SqlExpr::QuantifiedArray {
                            left: Box::new(left),
                            op,
                            is_all,
                            array: Box::new(build_expr(rhs)?),
                        },
                        _ => {
                            return Err(ParseError::UnexpectedToken {
                                expected: "subquery or array expression",
                                actual: rhs.as_str().into(),
                            });
                        }
                    })
                }
                Rule::like_suffix => build_like_predicate(left, next),
                Rule::similar_suffix => build_similar_predicate(left, next),
                Rule::comp_op => {
                    let right = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
                    Ok(match next.as_str() {
                        "<->" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::Distance,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "##" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::ClosestPoint,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "?#" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::Intersects,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "?||" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::Parallel,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "?-|" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::Perpendicular,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "~=" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::Same,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "&<" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::OverLeft,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "&>" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::OverRight,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "<<|" | "<^" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::Below,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "|>>" | ">^" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::Above,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "&<|" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::OverBelow,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "|&>" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::OverAbove,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "?-" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::IsHorizontal,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "@>" => SqlExpr::JsonbContains(Box::new(left), Box::new(right)),
                        "<@" => SqlExpr::JsonbContained(Box::new(left), Box::new(right)),
                        "@?" => SqlExpr::JsonbPathExists(Box::new(left), Box::new(right)),
                        "@@" if expr_is_jsonb_syntax(&left) && expr_is_jsonpath_syntax(&right) => {
                            SqlExpr::JsonbPathMatch(Box::new(left), Box::new(right))
                        }
                        "@@" => SqlExpr::BinaryOperator {
                            op: "@@".into(),
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "?" => SqlExpr::JsonbExists(Box::new(left), Box::new(right)),
                        "?|" => SqlExpr::JsonbExistsAny(Box::new(left), Box::new(right)),
                        "?&" => SqlExpr::JsonbExistsAll(Box::new(left), Box::new(right)),
                        "&&" if expr_is_array_syntax(&left) && expr_is_array_syntax(&right) => {
                            SqlExpr::ArrayOverlap(Box::new(left), Box::new(right))
                        }
                        "&&" => SqlExpr::BinaryOperator {
                            op: "&&".into(),
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "->" => SqlExpr::JsonGet(Box::new(left), Box::new(right)),
                        "->>" => SqlExpr::JsonGetText(Box::new(left), Box::new(right)),
                        "#>" => SqlExpr::JsonPath(Box::new(left), Box::new(right)),
                        "#>>" => SqlExpr::JsonPathText(Box::new(left), Box::new(right)),
                        "=" => SqlExpr::Eq(Box::new(left), Box::new(right)),
                        "<>" | "!=" => SqlExpr::NotEq(Box::new(left), Box::new(right)),
                        "<" => SqlExpr::Lt(Box::new(left), Box::new(right)),
                        "<=" => SqlExpr::LtEq(Box::new(left), Box::new(right)),
                        ">" => SqlExpr::Gt(Box::new(left), Box::new(right)),
                        ">=" => SqlExpr::GtEq(Box::new(left), Box::new(right)),
                        "~" => SqlExpr::RegexMatch(Box::new(left), Box::new(right)),
                        _ => unreachable!(),
                    })
                }
                _ => Err(ParseError::UnexpectedToken {
                    expected: "comparison",
                    actual: next.as_str().into(),
                }),
            }
        }
        Rule::primary_expr => {
            build_expr(pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?)
        }
        Rule::scalar_subquery_expr => {
            let subquery = build_select(
                pair.into_inner()
                    .find(|part| part.as_rule() == Rule::select_stmt)
                    .ok_or(ParseError::UnexpectedEof)?,
            )?;
            Ok(SqlExpr::ScalarSubquery(Box::new(subquery)))
        }
        Rule::exists_expr => {
            let subquery = build_select(
                pair.into_inner()
                    .find(|part| part.as_rule() == Rule::select_stmt)
                    .ok_or(ParseError::UnexpectedEof)?,
            )?;
            Ok(SqlExpr::Exists(Box::new(subquery)))
        }
        Rule::case_expr => build_case_expr(pair),
        Rule::array_expr => Ok(SqlExpr::ArrayLiteral(
            pair.into_inner()
                .find(|part| part.as_rule() == Rule::expr_list)
                .map(|list| {
                    list.into_inner()
                        .filter(|part| part.as_rule() == Rule::expr)
                        .map(build_expr)
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?
                .unwrap_or_default(),
        )),
        Rule::cast_expr => {
            let mut expr = None;
            let mut ty = None;
            for part in pair.into_inner() {
                match part.as_rule() {
                    Rule::expr => expr = Some(build_expr(part)?),
                    Rule::type_name => ty = Some(build_type_name(part)),
                    _ => {}
                }
            }
            Ok(SqlExpr::Cast(
                Box::new(expr.ok_or(ParseError::UnexpectedEof)?),
                ty.ok_or(ParseError::UnexpectedEof)?,
            ))
        }
        Rule::row_expr => Ok(SqlExpr::Row(
            pair.into_inner()
                .find(|part| part.as_rule() == Rule::expr_list)
                .map(|list| {
                    list.into_inner()
                        .filter(|part| part.as_rule() == Rule::expr)
                        .map(build_expr)
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?
                .unwrap_or_default(),
        )),
        Rule::agg_call => build_agg_call(pair),
        Rule::func_call => {
            let mut inner = pair.into_inner();
            let name = build_identifier(inner.next().ok_or(ParseError::UnexpectedEof)?);
            let parsed_args = inner
                .find(|part| part.as_rule() == Rule::function_arg_list)
                .map(build_function_arg_list)
                .transpose()?
                .unwrap_or_default();
            let args = parsed_args.args;
            if name.eq_ignore_ascii_case("random") && args.is_empty() {
                Ok(SqlExpr::Random)
            } else {
                Ok(SqlExpr::FuncCall {
                    name,
                    args,
                    func_variadic: parsed_args.func_variadic,
                })
            }
        }
        Rule::position_expr => {
            let mut args = pair
                .into_inner()
                .filter(|part| part.as_rule() != Rule::kw_in);
            let needle = build_expr(args.next().ok_or(ParseError::UnexpectedEof)?)?;
            let haystack = build_expr(args.next().ok_or(ParseError::UnexpectedEof)?)?;
            Ok(SqlExpr::FuncCall {
                name: "position".into(),
                args: vec![
                    SqlFunctionArg {
                        name: None,
                        value: needle,
                    },
                    SqlFunctionArg {
                        name: None,
                        value: haystack,
                    },
                ],
                func_variadic: false,
            })
        }
        Rule::substring_expr => {
            let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
            match inner.as_rule() {
                Rule::substring_from_expr => {
                    let mut inner = inner.into_inner().filter(|part| {
                        !matches!(
                            part.as_rule(),
                            Rule::kw_from | Rule::kw_from_atom | Rule::kw_for | Rule::kw_for_atom
                        )
                    });
                    let value = parse_expr(
                        inner
                            .next()
                            .ok_or(ParseError::UnexpectedEof)?
                            .as_str()
                            .trim(),
                    )?;
                    let start = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
                    let mut args = vec![value, start];
                    if let Some(len) = inner.next() {
                        args.push(build_expr(len)?);
                    }
                    Ok(SqlExpr::FuncCall {
                        name: "substring".into(),
                        args: args.into_iter().map(SqlFunctionArg::positional).collect(),
                        func_variadic: false,
                    })
                }
                Rule::substring_for_expr => {
                    let mut inner = inner
                        .into_inner()
                        .filter(|part| !matches!(part.as_rule(), Rule::kw_for | Rule::kw_for_atom));
                    let value = parse_expr(
                        inner
                            .next()
                            .ok_or(ParseError::UnexpectedEof)?
                            .as_str()
                            .trim(),
                    )?;
                    let len = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
                    Ok(SqlExpr::FuncCall {
                        name: "substring".into(),
                        args: vec![value, SqlExpr::IntegerLiteral("1".into()), len]
                            .into_iter()
                            .map(SqlFunctionArg::positional)
                            .collect(),
                        func_variadic: false,
                    })
                }
                Rule::substring_similar_expr => {
                    let mut inner = inner.into_inner().filter(|part| {
                        !matches!(
                            part.as_rule(),
                            Rule::kw_similar
                                | Rule::kw_similar_atom
                                | Rule::kw_to
                                | Rule::kw_to_atom
                        )
                    });
                    let value = parse_expr(
                        inner
                            .next()
                            .ok_or(ParseError::UnexpectedEof)?
                            .as_str()
                            .trim(),
                    )?;
                    let pattern = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
                    let mut args = vec![
                        SqlFunctionArg::positional(value),
                        SqlFunctionArg::positional(pattern),
                    ];
                    if let Some(escape_clause) = inner.next() {
                        let expr = escape_clause
                            .into_inner()
                            .find(|inner| inner.as_rule() == Rule::concat_expr)
                            .ok_or(ParseError::UnexpectedEof)?;
                        args.push(SqlFunctionArg::positional(build_expr(expr)?));
                    }
                    Ok(SqlExpr::FuncCall {
                        name: "similar_substring".into(),
                        args,
                        func_variadic: false,
                    })
                }
                _ => Err(ParseError::UnexpectedToken {
                    expected: "substring expression",
                    actual: inner.as_str().into(),
                }),
            }
        }
        Rule::extract_expr => {
            let mut field = None;
            let mut value = None;
            for part in pair.into_inner() {
                match part.as_rule() {
                    Rule::extract_field => {
                        let inner = part.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
                        field = Some(match inner.as_rule() {
                            Rule::identifier => {
                                SqlExpr::Const(Value::Text(build_identifier(inner).into()))
                            }
                            Rule::quoted_string_literal => SqlExpr::Const(Value::Text(
                                decode_string_literal_pair(inner)?.into(),
                            )),
                            _ => {
                                return Err(ParseError::UnexpectedToken {
                                    expected: "extract field",
                                    actual: inner.as_str().into(),
                                });
                            }
                        });
                    }
                    Rule::expr => value = Some(build_expr(part)?),
                    _ => {}
                }
            }
            Ok(SqlExpr::FuncCall {
                name: "date_part".into(),
                args: vec![
                    SqlFunctionArg::positional(field.ok_or(ParseError::UnexpectedEof)?),
                    SqlFunctionArg::positional(value.ok_or(ParseError::UnexpectedEof)?),
                ],
                func_variadic: false,
            })
        }
        Rule::overlay_expr => {
            let mut inner = pair.into_inner().filter(|part| {
                !matches!(
                    part.as_rule(),
                    Rule::kw_placing
                        | Rule::kw_placing_atom
                        | Rule::kw_from
                        | Rule::kw_from_atom
                        | Rule::kw_for
                        | Rule::kw_for_atom
                )
            });
            let value = parse_expr(
                inner
                    .next()
                    .ok_or(ParseError::UnexpectedEof)?
                    .as_str()
                    .trim(),
            )?;
            let placing = parse_expr(
                inner
                    .next()
                    .ok_or(ParseError::UnexpectedEof)?
                    .as_str()
                    .trim(),
            )?;
            let start = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            let mut args = vec![value, placing, start];
            if let Some(len) = inner.next() {
                args.push(build_expr(len)?);
            }
            Ok(SqlExpr::FuncCall {
                name: "overlay".into(),
                args: args.into_iter().map(SqlFunctionArg::positional).collect(),
                func_variadic: false,
            })
        }
        Rule::trim_expr => build_trim_expr(pair),
        Rule::typed_string_literal => {
            let mut inner = pair.into_inner();
            let ty = build_type_name(inner.next().ok_or(ParseError::UnexpectedEof)?);
            let literal =
                decode_string_literal_pair(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            Ok(SqlExpr::Cast(
                Box::new(SqlExpr::Const(Value::Text(literal.into()))),
                ty,
            ))
        }
        Rule::bit_string_literal | Rule::binary_bit_literal | Rule::hex_bit_literal => Ok(
            SqlExpr::Const(Value::Bit(parse_bit_string_literal(pair.as_str())?)),
        ),
        Rule::identifier => Ok(SqlExpr::Column(build_identifier(pair))),
        Rule::kw_default => Ok(SqlExpr::Default),
        Rule::numeric_literal => Ok(SqlExpr::NumericLiteral(pair.as_str().to_string())),
        Rule::integer => Ok(SqlExpr::IntegerLiteral(pair.as_str().to_string())),
        Rule::quoted_string_literal
        | Rule::string_literal
        | Rule::unicode_string_literal
        | Rule::escape_string_literal
        | Rule::dollar_string_literal => Ok(SqlExpr::Const(Value::Text(
            decode_string_literal_pair(pair)?.into(),
        ))),
        Rule::kw_null => Ok(SqlExpr::Const(Value::Null)),
        Rule::kw_true => Ok(SqlExpr::Const(Value::Bool(true))),
        Rule::kw_false => Ok(SqlExpr::Const(Value::Bool(false))),
        Rule::kw_current_date => Ok(SqlExpr::CurrentDate),
        Rule::kw_current_time => Ok(SqlExpr::CurrentTime {
            precision: pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()?,
        }),
        Rule::kw_current_timestamp => Ok(SqlExpr::CurrentTimestamp {
            precision: pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()?,
        }),
        Rule::kw_localtime => Ok(SqlExpr::LocalTime {
            precision: pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()?,
        }),
        Rule::kw_localtimestamp => Ok(SqlExpr::LocalTimestamp {
            precision: pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()?,
        }),
        _ => Err(ParseError::UnexpectedToken {
            expected: "expression",
            actual: pair.as_str().into(),
        }),
    }
}

fn build_agg_call(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let mut func = None;
    let mut parsed_args = ParsedFunctionArgs::default();
    let mut is_star = false;
    let mut distinct = false;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::agg_func => {
                let inner = part.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
                func = Some(match inner.as_rule() {
                    Rule::kw_count => AggFunc::Count,
                    Rule::kw_sum => AggFunc::Sum,
                    Rule::kw_avg => AggFunc::Avg,
                    Rule::kw_variance => AggFunc::Variance,
                    Rule::kw_stddev => AggFunc::Stddev,
                    Rule::kw_min => AggFunc::Min,
                    Rule::kw_max => AggFunc::Max,
                    Rule::kw_string_agg => AggFunc::StringAgg,
                    Rule::kw_array_agg => AggFunc::ArrayAgg,
                    Rule::kw_json_agg => AggFunc::JsonAgg,
                    Rule::kw_jsonb_agg => AggFunc::JsonbAgg,
                    Rule::kw_json_object_agg => AggFunc::JsonObjectAgg,
                    Rule::kw_jsonb_object_agg => AggFunc::JsonbObjectAgg,
                    _ => {
                        return Err(ParseError::UnexpectedToken {
                            expected: "aggregate function",
                            actual: inner.as_str().into(),
                        });
                    }
                });
            }
            Rule::agg_distinct => distinct = true,
            Rule::star => is_star = true,
            Rule::function_arg_list => {
                parsed_args = build_function_arg_list(part)?;
            }
            _ => {}
        }
    }
    Ok(SqlExpr::AggCall {
        func: func.ok_or(ParseError::UnexpectedEof)?,
        args: if is_star {
            Vec::new()
        } else {
            parsed_args.args
        },
        distinct,
        func_variadic: !is_star && parsed_args.func_variadic,
    })
}

#[derive(Default)]
struct ParsedFunctionArgs {
    args: Vec<SqlFunctionArg>,
    func_variadic: bool,
}

fn build_function_arg_list(pair: Pair<'_, Rule>) -> Result<ParsedFunctionArgs, ParseError> {
    let mut parsed = ParsedFunctionArgs::default();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::function_arg => parsed.args.push(build_function_arg(part)?),
            Rule::variadic_function_arg => {
                let expr = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::expr)
                    .ok_or(ParseError::UnexpectedEof)?;
                parsed
                    .args
                    .push(SqlFunctionArg::positional(build_expr(expr)?));
                parsed.func_variadic = true;
            }
            _ => {}
        }
    }
    Ok(parsed)
}

fn build_function_arg(pair: Pair<'_, Rule>) -> Result<SqlFunctionArg, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match inner.as_rule() {
        Rule::named_function_arg => {
            let mut inner = inner.into_inner();
            let name = build_identifier(inner.next().ok_or(ParseError::UnexpectedEof)?);
            let value = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            Ok(SqlFunctionArg {
                name: Some(name),
                value,
            })
        }
        Rule::positional_function_arg => {
            let expr = inner.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
            Ok(SqlFunctionArg {
                name: None,
                value: build_expr(expr)?,
            })
        }
        Rule::expr => Ok(SqlFunctionArg {
            name: None,
            value: build_expr(inner)?,
        }),
        _ => Err(ParseError::UnexpectedToken {
            expected: "function argument",
            actual: inner.as_str().into(),
        }),
    }
}

fn build_copy_from(pair: Pair<'_, Rule>) -> Result<CopyFromStatement, ParseError> {
    let mut table_name = None;
    let mut columns = None;
    let mut source = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if table_name.is_none() => {
                table_name = Some(build_identifier(part));
            }
            Rule::ident_list => {
                columns = Some(part.into_inner().map(build_identifier).collect());
            }
            Rule::quoted_string_literal
            | Rule::string_literal
            | Rule::unicode_string_literal
            | Rule::escape_string_literal
            | Rule::dollar_string_literal => {
                source = Some(CopySource::File(decode_string_literal_pair(part)?));
            }
            _ => {}
        }
    }
    Ok(CopyFromStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        columns,
        source: source.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_null_predicate(left: SqlExpr, pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let pair = if pair.as_rule() == Rule::null_predicate_suffix {
        pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?
    } else {
        pair
    };
    let raw = pair.as_str().to_ascii_lowercase();
    if raw == "is null" {
        return Ok(SqlExpr::IsNull(Box::new(left)));
    }
    if raw == "is not null" {
        return Ok(SqlExpr::IsNotNull(Box::new(left)));
    }
    if raw == "is true" {
        return Ok(SqlExpr::IsNotDistinctFrom(
            Box::new(left),
            Box::new(SqlExpr::Const(Value::Bool(true))),
        ));
    }
    if raw == "is not true" {
        return Ok(SqlExpr::IsDistinctFrom(
            Box::new(left),
            Box::new(SqlExpr::Const(Value::Bool(true))),
        ));
    }
    if raw == "is false" {
        return Ok(SqlExpr::IsNotDistinctFrom(
            Box::new(left),
            Box::new(SqlExpr::Const(Value::Bool(false))),
        ));
    }
    if raw == "is not false" {
        return Ok(SqlExpr::IsDistinctFrom(
            Box::new(left),
            Box::new(SqlExpr::Const(Value::Bool(false))),
        ));
    }
    if raw == "is unknown" {
        return Ok(SqlExpr::IsNull(Box::new(left)));
    }
    if raw == "is not unknown" {
        return Ok(SqlExpr::IsNotNull(Box::new(left)));
    }

    let mut right = None;
    let mut saw_not = false;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::expr | Rule::add_expr | Rule::primary_expr | Rule::cmp_expr => {
                right = Some(build_expr(part)?)
            }
            Rule::kw_not => saw_not = true,
            _ => {}
        }
    }

    let right = right.ok_or(ParseError::UnexpectedEof)?;
    Ok(if saw_not {
        SqlExpr::IsNotDistinctFrom(Box::new(left), Box::new(right))
    } else {
        SqlExpr::IsDistinctFrom(Box::new(left), Box::new(right))
    })
}

fn build_like_predicate(left: SqlExpr, pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let mut negated = false;
    let mut case_insensitive = false;
    let mut pattern = None;
    let mut escape = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::kw_not => negated = true,
            Rule::kw_like => case_insensitive = false,
            Rule::kw_ilike => case_insensitive = true,
            Rule::concat_expr => {
                if pattern.is_none() {
                    pattern = Some(build_expr(part)?);
                }
            }
            Rule::escape_clause => {
                let expr = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::concat_expr)
                    .ok_or(ParseError::UnexpectedEof)?;
                escape = Some(Box::new(build_expr(expr)?));
            }
            _ => {}
        }
    }
    Ok(SqlExpr::Like {
        expr: Box::new(left),
        pattern: Box::new(pattern.ok_or(ParseError::UnexpectedEof)?),
        escape,
        case_insensitive,
        negated,
    })
}

fn build_similar_predicate(left: SqlExpr, pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let mut negated = false;
    let mut pattern = None;
    let mut escape = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::kw_not => negated = true,
            Rule::concat_expr => {
                if pattern.is_none() {
                    pattern = Some(build_expr(part)?);
                }
            }
            Rule::escape_clause => {
                let expr = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::concat_expr)
                    .ok_or(ParseError::UnexpectedEof)?;
                escape = Some(Box::new(build_expr(expr)?));
            }
            _ => {}
        }
    }
    Ok(SqlExpr::Similar {
        expr: Box::new(left),
        pattern: Box::new(pattern.ok_or(ParseError::UnexpectedEof)?),
        escape,
        negated,
    })
}

fn build_trim_expr(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let trim_variant = pair
        .into_inner()
        .find(|inner| inner.as_rule() == Rule::trim_variant)
        .ok_or(ParseError::UnexpectedEof)?;
    let trim_variant = trim_variant
        .into_inner()
        .next()
        .ok_or(ParseError::UnexpectedEof)?;
    let trim_variant_rule = trim_variant.as_rule();
    let mut direction = "both";
    let mut trim_source = None;
    let mut trim_chars = None;
    for part in trim_variant.into_inner() {
        match part.as_rule() {
            Rule::trim_spec => direction = part.as_str(),
            Rule::expr => {
                let expr = build_expr(part)?;
                if trim_chars.is_none()
                    && matches!(
                        trim_variant_rule,
                        Rule::trim_spec_chars_from | Rule::trim_chars_from
                    )
                    && trim_source.is_none()
                {
                    trim_chars = Some(expr);
                } else {
                    trim_source = Some(expr);
                }
            }
            _ => {}
        }
    }
    let mut args = vec![SqlFunctionArg::positional(
        trim_source.ok_or(ParseError::UnexpectedEof)?,
    )];
    if let Some(chars) = trim_chars {
        args.push(SqlFunctionArg::positional(chars));
    }
    Ok(SqlExpr::FuncCall {
        name: match direction.to_ascii_lowercase().as_str() {
            "leading" => "ltrim",
            "trailing" => "rtrim",
            _ => "btrim",
        }
        .into(),
        args,
        func_variadic: false,
    })
}

fn build_case_when(pair: Pair<'_, Rule>) -> Result<SqlCaseWhen, ParseError> {
    let mut expr = None;
    let mut result = None;
    for part in pair.into_inner() {
        if part.as_rule() == Rule::expr {
            if expr.is_none() {
                expr = Some(build_expr(part)?);
            } else {
                result = Some(build_expr(part)?);
            }
        }
    }
    Ok(SqlCaseWhen {
        expr: expr.ok_or(ParseError::UnexpectedEof)?,
        result: result.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_case_expr(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let body = pair
        .into_inner()
        .find(|part| {
            matches!(
                part.as_rule(),
                Rule::searched_case_body | Rule::simple_case_body
            )
        })
        .ok_or(ParseError::UnexpectedEof)?;
    let mut arg = None;
    let mut args = Vec::new();
    let mut defresult = None;
    match body.as_rule() {
        Rule::searched_case_body => {
            for part in body.into_inner() {
                match part.as_rule() {
                    Rule::when_clause => args.push(build_case_when(part)?),
                    Rule::else_clause => {
                        let expr = part
                            .into_inner()
                            .find(|inner| inner.as_rule() == Rule::expr)
                            .ok_or(ParseError::UnexpectedEof)?;
                        defresult = Some(Box::new(build_expr(expr)?));
                    }
                    _ => {}
                }
            }
        }
        Rule::simple_case_body => {
            for part in body.into_inner() {
                match part.as_rule() {
                    Rule::expr if arg.is_none() => arg = Some(Box::new(build_expr(part)?)),
                    Rule::when_clause => args.push(build_case_when(part)?),
                    Rule::else_clause => {
                        let expr = part
                            .into_inner()
                            .find(|inner| inner.as_rule() == Rule::expr)
                            .ok_or(ParseError::UnexpectedEof)?;
                        defresult = Some(Box::new(build_expr(expr)?));
                    }
                    _ => {}
                }
            }
        }
        _ => {
            return Err(ParseError::UnexpectedToken {
                expected: "CASE body",
                actual: body.as_str().into(),
            });
        }
    }
    Ok(SqlExpr::Case {
        arg,
        args,
        defresult,
    })
}

fn fold_infix(
    first: SqlExpr,
    mut tail: pest::iterators::Pairs<'_, Rule>,
) -> Result<SqlExpr, ParseError> {
    let mut expr = first;
    while let Some(op) = tail.next() {
        let rhs = build_expr(tail.next().ok_or(ParseError::UnexpectedEof)?)?;
        expr = match op.as_rule() {
            Rule::kw_or => SqlExpr::Or(Box::new(expr), Box::new(rhs)),
            Rule::kw_and => SqlExpr::And(Box::new(expr), Box::new(rhs)),
            Rule::add_op => match op.as_str() {
                "+" => SqlExpr::Add(Box::new(expr), Box::new(rhs)),
                "-" => SqlExpr::Sub(Box::new(expr), Box::new(rhs)),
                _ => unreachable!(),
            },
            Rule::bit_op => match op.as_str().trim() {
                "&" => SqlExpr::BitAnd(Box::new(expr), Box::new(rhs)),
                "|" => SqlExpr::BitOr(Box::new(expr), Box::new(rhs)),
                "#" => SqlExpr::BitXor(Box::new(expr), Box::new(rhs)),
                _ => unreachable!(),
            },
            Rule::pow_op => SqlExpr::FuncCall {
                name: "power".into(),
                args: vec![
                    SqlFunctionArg::positional(expr),
                    SqlFunctionArg::positional(rhs),
                ],
                func_variadic: false,
            },
            Rule::shift_op => match op.as_str() {
                "<<" => SqlExpr::Shl(Box::new(expr), Box::new(rhs)),
                ">>" => SqlExpr::Shr(Box::new(expr), Box::new(rhs)),
                _ => unreachable!(),
            },
            Rule::concat_op => SqlExpr::Concat(Box::new(expr), Box::new(rhs)),
            Rule::mul_op => match op.as_str() {
                "*" => SqlExpr::Mul(Box::new(expr), Box::new(rhs)),
                "/" => SqlExpr::Div(Box::new(expr), Box::new(rhs)),
                "%" => SqlExpr::Mod(Box::new(expr), Box::new(rhs)),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        };
    }
    Ok(expr)
}

fn decode_string_literal(raw: &str) -> Result<String, ParseError> {
    if raw.len() >= 2 && matches!(raw.as_bytes()[0], b'u' | b'U') && raw.as_bytes()[1] == b'&' {
        return decode_unicode_string_literal(raw);
    }

    if raw.starts_with('\'') {
        return Ok(raw[1..raw.len() - 1].replace("''", "'"));
    }

    if raw.len() >= 2 && matches!(raw.as_bytes()[0], b'e' | b'E') && raw.as_bytes()[1] == b'\'' {
        return decode_escape_string(&raw[1..]);
    }

    if raw.starts_with('$') {
        return decode_dollar_string(raw);
    }

    Err(ParseError::UnexpectedToken {
        expected: "string literal",
        actual: raw.into(),
    })
}

fn expr_is_array_syntax(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::ArrayLiteral(_) => true,
        SqlExpr::Cast(_, RawTypeName::Builtin(ty)) => ty.is_array,
        SqlExpr::Const(Value::Array(_) | Value::PgArray(_)) => true,
        _ => false,
    }
}

fn expr_is_jsonb_syntax(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::Cast(_, RawTypeName::Builtin(ty)) => {
            !ty.is_array && matches!(ty.kind, SqlTypeKind::Jsonb)
        }
        SqlExpr::Const(Value::Jsonb(_)) => true,
        _ => false,
    }
}

fn expr_is_jsonpath_syntax(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::Cast(_, RawTypeName::Builtin(ty)) => {
            !ty.is_array && matches!(ty.kind, SqlTypeKind::JsonPath)
        }
        SqlExpr::Const(Value::JsonPath(_))
        | SqlExpr::Const(Value::Text(_))
        | SqlExpr::Const(Value::TextRef(_, _)) => true,
        _ => false,
    }
}

fn decode_string_literal_pair(pair: Pair<'_, Rule>) -> Result<String, ParseError> {
    match pair.as_rule() {
        Rule::unicode_string_literal => decode_unicode_string_literal(pair.as_str()),
        Rule::quoted_string_literal => {
            let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
            decode_string_literal_pair(inner)
        }
        _ => decode_string_literal(pair.as_str()),
    }
}

fn decode_unicode_quoted_identifier(raw: &str) -> Result<String, ParseError> {
    let (literal, escape_char) = split_unicode_literal_parts(raw)?;
    let text = literal
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .ok_or(ParseError::UnexpectedToken {
            expected: "unicode quoted identifier",
            actual: raw.into(),
        })?
        .replace("\"\"", "\"");
    decode_unicode_escapes(&text, escape_char)
}

fn decode_unicode_string_literal(raw: &str) -> Result<String, ParseError> {
    let (literal, escape_char) = split_unicode_literal_parts(raw)?;
    let text = decode_string_literal(literal)?;
    decode_unicode_escapes(&text, escape_char)
}

fn split_unicode_literal_parts(raw: &str) -> Result<(&str, char), ParseError> {
    let lower = raw.to_ascii_lowercase();
    let Some(prefix_stripped) = lower.strip_prefix("u&") else {
        return Err(ParseError::UnexpectedToken {
            expected: "unicode string literal",
            actual: raw.into(),
        });
    };
    if let Some(idx) = prefix_stripped.find("uescape") {
        let literal_end = 2 + idx;
        let literal = raw[2..literal_end].trim_end();
        let clause = raw[literal_end..].trim_start();
        let clause_lower = clause.to_ascii_lowercase();
        if !clause_lower.starts_with("uescape") {
            return Err(ParseError::UnexpectedToken {
                expected: "UESCAPE clause",
                actual: clause.into(),
            });
        }
        let escape_raw = clause["UESCAPE".len()..].trim();
        let escape = decode_string_literal(escape_raw)?;
        let mut chars = escape.chars();
        let Some(ch) = chars.next() else {
            return Err(ParseError::UnexpectedToken {
                expected: "non-empty UESCAPE character",
                actual: raw.into(),
            });
        };
        if chars.next().is_some() || matches!(ch, '+' | '"' | '\'' | ' ' | '\t' | '\n' | '\r') {
            return Err(ParseError::UnexpectedToken {
                expected: "valid UESCAPE character",
                actual: raw.into(),
            });
        }
        Ok((literal.trim(), ch))
    } else {
        Ok((raw[2..].trim(), '\\'))
    }
}

fn decode_unicode_escapes(text: &str, escape_char: char) -> Result<String, ParseError> {
    let mut out = String::new();
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0usize;
    while i < chars.len() {
        let ch = chars[i];
        if ch != escape_char {
            out.push(ch);
            i += 1;
            continue;
        }
        if i + 1 < chars.len() && chars[i + 1] == escape_char {
            out.push(escape_char);
            i += 2;
            continue;
        }
        if i + 1 >= chars.len() {
            return Err(ParseError::UnexpectedToken {
                expected: "valid Unicode escape sequence",
                actual: text.into(),
            });
        }
        let (digits, next) = if chars[i + 1] == '+' {
            if i + 8 > chars.len() {
                return Err(unicode_error("invalid Unicode escape"));
            }
            (chars[i + 2..i + 8].iter().collect::<String>(), i + 8)
        } else {
            if i + 5 > chars.len() {
                return Err(unicode_error("invalid Unicode escape"));
            }
            (chars[i + 1..i + 5].iter().collect::<String>(), i + 5)
        };
        let code = u32::from_str_radix(&digits, 16)
            .map_err(|_| unicode_error("invalid Unicode escape"))?;
        let (decoded, consumed_next) =
            decode_unicode_codepoint_with_surrogates(&chars, i, next, code, escape_char)?;
        out.push(decoded);
        i = consumed_next;
    }
    Ok(out)
}

fn parse_bit_string_literal(raw: &str) -> Result<BitString, ParseError> {
    let (prefix, literal) = raw.split_at(1);
    let decoded = decode_string_literal(literal)?;
    let bytes = decoded.as_bytes();
    match prefix.as_bytes()[0] {
        b'b' | b'B' => {
            let mut out = vec![0u8; BitString::byte_len(bytes.len() as i32)];
            for (idx, byte) in bytes.iter().enumerate() {
                match byte {
                    b'0' => {}
                    b'1' => out[idx / 8] |= 1 << (7 - (idx % 8)),
                    other => {
                        return Err(ParseError::UnexpectedToken {
                            expected: "valid binary digit",
                            actual: format!(
                                "\"{}\" is not a valid binary digit",
                                char::from(*other)
                            ),
                        });
                    }
                }
            }
            Ok(BitString::new(bytes.len() as i32, out))
        }
        b'x' | b'X' => {
            let mut out = Vec::with_capacity(bytes.len().div_ceil(2));
            let mut pending = None::<u8>;
            for byte in bytes {
                let nibble = match byte {
                    b'0'..=b'9' => *byte - b'0',
                    b'a'..=b'f' => *byte - b'a' + 10,
                    b'A'..=b'F' => *byte - b'A' + 10,
                    other => {
                        return Err(ParseError::UnexpectedToken {
                            expected: "valid hexadecimal digit",
                            actual: format!(
                                "\"{}\" is not a valid hexadecimal digit",
                                char::from(*other)
                            ),
                        });
                    }
                };
                if let Some(high) = pending.take() {
                    out.push((high << 4) | nibble);
                } else {
                    pending = Some(nibble);
                }
            }
            if let Some(high) = pending {
                out.push(high << 4);
            }
            Ok(BitString::new((bytes.len() * 4) as i32, out))
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "bit string literal",
            actual: raw.into(),
        }),
    }
}

fn decode_escape_string(raw: &str) -> Result<String, ParseError> {
    let text = raw[1..raw.len() - 1].replace("''", "'");
    let mut out = String::new();
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        let escaped = chars.next().ok_or(ParseError::UnexpectedEof)?;
        match escaped {
            '\'' => out.push('\''),
            '"' => out.push('"'),
            '\\' => out.push('\\'),
            'b' => out.push('\u{0008}'),
            'f' => out.push('\u{000c}'),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            't' => out.push('\t'),
            'v' => out.push('\u{000b}'),
            'a' => out.push('\u{0007}'),
            'x' => {
                let hi = chars.next().ok_or(ParseError::UnexpectedEof)?;
                let lo = chars.next().ok_or(ParseError::UnexpectedEof)?;
                let value = u8::from_str_radix(&format!("{hi}{lo}"), 16).map_err(|_| {
                    ParseError::UnexpectedToken {
                        expected: "valid hex escape",
                        actual: raw.into(),
                    }
                })?;
                out.push(value as char);
            }
            'u' => {
                let code = collect_escape_digits(&mut chars, 4)?;
                let ch = decode_escape_codepoint(&mut chars, code)?;
                out.push(ch);
            }
            'U' => {
                let code = collect_escape_digits(&mut chars, 8)?;
                let ch = decode_escape_codepoint(&mut chars, code)?;
                out.push(ch);
            }
            '0'..='7' => {
                let mut digits = String::from(escaped);
                for _ in 0..2 {
                    if let Some(next) = chars.peek().copied() {
                        if ('0'..='7').contains(&next) {
                            digits.push(chars.next().unwrap());
                        } else {
                            break;
                        }
                    }
                }
                let value =
                    u8::from_str_radix(&digits, 8).map_err(|_| ParseError::UnexpectedToken {
                        expected: "valid octal escape",
                        actual: raw.into(),
                    })?;
                out.push(value as char);
            }
            other => out.push(other),
        }
    }
    Ok(out)
}

fn collect_escape_digits(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    len: usize,
) -> Result<u32, ParseError> {
    let mut digits = String::with_capacity(len);
    for _ in 0..len {
        let Some(ch) = chars.next() else {
            return Err(unicode_error("invalid Unicode escape"));
        };
        digits.push(ch);
    }
    u32::from_str_radix(&digits, 16).map_err(|_| unicode_error("invalid Unicode escape"))
}

fn decode_dollar_string(raw: &str) -> Result<String, ParseError> {
    let end_tag_start = raw[1..]
        .find('$')
        .map(|idx| idx + 1)
        .ok_or(ParseError::UnexpectedEof)?;
    let tag = &raw[..=end_tag_start];
    let suffix = &raw[end_tag_start + 1..];
    let closing = suffix.rfind(tag).ok_or(ParseError::UnexpectedToken {
        expected: "matching dollar-quote terminator",
        actual: raw.into(),
    })?;
    Ok(suffix[..closing].to_string())
}

fn decode_escape_codepoint(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    code: u32,
) -> Result<char, ParseError> {
    if let Some(high) = as_high_surrogate(code) {
        let low_prefix = chars
            .next()
            .ok_or_else(|| unicode_error("invalid Unicode surrogate pair"))?;
        let expected_len = match low_prefix {
            '\\' => match chars
                .next()
                .ok_or_else(|| unicode_error("invalid Unicode surrogate pair"))?
            {
                'u' => 4,
                'U' => 8,
                _ => return Err(unicode_error("invalid Unicode surrogate pair")),
            },
            _ => return Err(unicode_error("invalid Unicode surrogate pair")),
        };
        let low = collect_escape_digits(chars, expected_len)?;
        let Some(low) = as_low_surrogate(low) else {
            return Err(unicode_error("invalid Unicode surrogate pair"));
        };
        let codepoint = 0x10000 + (((high as u32) - 0xD800) << 10) + ((low as u32) - 0xDC00);
        char::from_u32(codepoint).ok_or_else(|| unicode_error("invalid Unicode escape value"))
    } else if as_low_surrogate(code).is_some() {
        Err(unicode_error("invalid Unicode surrogate pair"))
    } else {
        char::from_u32(code).ok_or_else(|| unicode_error("invalid Unicode escape value"))
    }
}

fn decode_unicode_codepoint_with_surrogates(
    chars: &[char],
    start: usize,
    next: usize,
    code: u32,
    escape_char: char,
) -> Result<(char, usize), ParseError> {
    if let Some(high) = as_high_surrogate(code) {
        let Some((low, consumed)) = parse_next_unicode_escape(chars, next, escape_char)? else {
            return Err(unicode_error("invalid Unicode surrogate pair"));
        };
        let Some(low) = as_low_surrogate(low) else {
            return Err(unicode_error("invalid Unicode surrogate pair"));
        };
        let codepoint = 0x10000 + (((high as u32) - 0xD800) << 10) + ((low as u32) - 0xDC00);
        let decoded = char::from_u32(codepoint)
            .ok_or_else(|| unicode_error("invalid Unicode escape value"))?;
        Ok((decoded, consumed))
    } else if as_low_surrogate(code).is_some() {
        let _ = start;
        Err(unicode_error("invalid Unicode surrogate pair"))
    } else {
        let decoded =
            char::from_u32(code).ok_or_else(|| unicode_error("invalid Unicode escape value"))?;
        Ok((decoded, next))
    }
}

fn parse_next_unicode_escape(
    chars: &[char],
    start: usize,
    escape_char: char,
) -> Result<Option<(u32, usize)>, ParseError> {
    if start >= chars.len() {
        return Ok(None);
    }
    if chars[start] != escape_char {
        return Ok(None);
    }
    if start + 1 >= chars.len() {
        return Err(unicode_error("invalid Unicode surrogate pair"));
    }
    let (digits, next) = if chars[start + 1] == '+' {
        if start + 8 > chars.len() {
            return Err(unicode_error("invalid Unicode surrogate pair"));
        }
        (
            chars[start + 2..start + 8].iter().collect::<String>(),
            start + 8,
        )
    } else {
        if start + 5 > chars.len() {
            return Err(unicode_error("invalid Unicode surrogate pair"));
        }
        (
            chars[start + 1..start + 5].iter().collect::<String>(),
            start + 5,
        )
    };
    let code = u32::from_str_radix(&digits, 16)
        .map_err(|_| unicode_error("invalid Unicode surrogate pair"))?;
    Ok(Some((code, next)))
}

fn unicode_error(message: &'static str) -> ParseError {
    ParseError::UnexpectedToken {
        expected: "valid Unicode string literal",
        actual: message.into(),
    }
}

fn as_high_surrogate(code: u32) -> Option<u16> {
    (0xD800..=0xDBFF).contains(&code).then_some(code as u16)
}

fn as_low_surrogate(code: u32) -> Option<u16> {
    (0xDC00..=0xDFFF).contains(&code).then_some(code as u16)
}
