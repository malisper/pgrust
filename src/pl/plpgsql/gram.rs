use pest::Parser as _;
use pest::iterators::Pair;
use pest_derive::Parser;

use crate::backend::executor::Value;
use crate::backend::parser::{ParseError, SqlExpr, SqlType, parse_expr, parse_type_name};
use crate::include::catalog::RECORD_TYPE_OID;

use super::ast::{
    AliasDecl, AliasTarget, AssignTarget, Block, Decl, ForQuerySource, ForTarget, RaiseLevel,
    ReturnQueryKind, Stmt, VarDecl,
};

#[derive(Parser)]
#[grammar = "pl/plpgsql/gram.pest"]
struct PlpgsqlParser;

pub fn parse_block(sql: &str) -> Result<Block, ParseError> {
    reject_unsupported_exception_handler(sql)?;
    PlpgsqlParser::parse(Rule::pl_block, sql)
        .map_err(|e| map_pest_error("plpgsql block", e))
        .and_then(|mut pairs| build_pl_block(pairs.next().ok_or(ParseError::UnexpectedEof)?))
}

fn reject_unsupported_exception_handler(sql: &str) -> Result<(), ParseError> {
    if find_exception_handler(sql).is_some() {
        return Err(ParseError::FeatureNotSupported(
            "PL/pgSQL EXCEPTION handlers".into(),
        ));
    }
    Ok(())
}

fn find_exception_handler(sql: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut idx = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut dollar_tag: Option<&str> = None;
    while idx < bytes.len() {
        if let Some(tag) = dollar_tag {
            if sql[idx..].starts_with(tag) {
                idx += tag.len();
                dollar_tag = None;
            } else {
                idx += 1;
            }
            continue;
        }

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

        if bytes.get(idx) == Some(&b'-') && bytes.get(idx + 1) == Some(&b'-') {
            idx += 2;
            while idx < bytes.len() && bytes[idx] != b'\n' {
                idx += 1;
            }
            continue;
        }
        if bytes.get(idx) == Some(&b'/') && bytes.get(idx + 1) == Some(&b'*') {
            idx += 2;
            while idx + 1 < bytes.len()
                && !(bytes[idx] == b'*' && bytes.get(idx + 1) == Some(&b'/'))
            {
                idx += 1;
            }
            idx = (idx + 2).min(bytes.len());
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
            '$' => {
                if let Some(tag) = dollar_quote_tag_at(sql, idx) {
                    idx += tag.len();
                    dollar_tag = Some(tag);
                    continue;
                }
            }
            _ => {}
        }

        if keyword_at(sql, idx, "exception") {
            let mut next = idx + "exception".len();
            while next < bytes.len() && (bytes[next] as char).is_ascii_whitespace() {
                next += 1;
            }
            if keyword_at(sql, next, "when") {
                return Some(idx);
            }
        }
        idx += 1;
    }
    None
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

fn build_pl_block(pair: Pair<'_, Rule>) -> Result<Block, ParseError> {
    let block = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::block)
        .ok_or(ParseError::UnexpectedEof)?;
    build_block(block)
}

fn build_block(pair: Pair<'_, Rule>) -> Result<Block, ParseError> {
    let mut declarations = Vec::new();
    let mut statements = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::declare_section => declarations = build_declare_section(part)?,
            Rule::stmt => statements.push(build_stmt(part)?),
            _ => {}
        }
    }
    Ok(Block {
        declarations,
        statements,
    })
}

fn build_declare_section(pair: Pair<'_, Rule>) -> Result<Vec<Decl>, ParseError> {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::decl_stmt)
        .map(build_decl_stmt)
        .collect()
}

fn build_decl_stmt(pair: Pair<'_, Rule>) -> Result<Decl, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match inner.as_rule() {
        Rule::var_decl => Ok(Decl::Var(build_var_decl(inner)?)),
        Rule::alias_decl => Ok(Decl::Alias(build_alias_decl(inner)?)),
        _ => Err(ParseError::UnexpectedToken {
            expected: "plpgsql declaration",
            actual: inner.as_str().into(),
        }),
    }
}

fn build_var_decl(pair: Pair<'_, Rule>) -> Result<VarDecl, ParseError> {
    let mut name = None;
    let mut ty = None;
    let mut default_expr = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::ident => name = Some(build_ident(part)),
            Rule::type_name_text => {
                let parsed = parse_type_name(part.as_str().trim())?;
                ty = Some(match parsed {
                    crate::backend::parser::RawTypeName::Builtin(sql_type) => sql_type,
                    crate::backend::parser::RawTypeName::Serial(kind) => {
                        return Err(ParseError::FeatureNotSupported(format!(
                            "{} is only allowed in CREATE TABLE / ALTER TABLE ADD COLUMN",
                            match kind {
                                crate::backend::parser::SerialKind::Small => "smallserial",
                                crate::backend::parser::SerialKind::Regular => "serial",
                                crate::backend::parser::SerialKind::Big => "bigserial",
                            }
                        )));
                    }
                    crate::backend::parser::RawTypeName::Record => SqlType::record(RECORD_TYPE_OID),
                    crate::backend::parser::RawTypeName::Named { name, .. } => {
                        return Err(ParseError::UnsupportedType(name));
                    }
                });
            }
            Rule::default_clause => {
                default_expr = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::expr_until_semi)
                    .map(|expr| expr.as_str().trim().to_string());
            }
            _ => {}
        }
    }
    Ok(VarDecl {
        name: name.ok_or(ParseError::UnexpectedEof)?,
        ty: ty.ok_or(ParseError::UnexpectedEof)?,
        default_expr,
    })
}

fn build_alias_decl(pair: Pair<'_, Rule>) -> Result<AliasDecl, ParseError> {
    let mut name = None;
    let mut target = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::ident => name = Some(build_ident(part)),
            Rule::alias_target => target = Some(build_alias_target(part)?),
            _ => {}
        }
    }
    Ok(AliasDecl {
        name: name.ok_or(ParseError::UnexpectedEof)?,
        target: target.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alias_target(pair: Pair<'_, Rule>) -> Result<AliasTarget, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match inner.as_rule() {
        Rule::positional_param => {
            let raw = inner.as_str();
            Ok(AliasTarget::Parameter(raw[1..].parse::<usize>().map_err(
                |_| ParseError::UnexpectedToken {
                    expected: "valid positional parameter reference",
                    actual: raw.into(),
                },
            )?))
        }
        Rule::ident => match build_ident(inner).as_str() {
            target if target.eq_ignore_ascii_case("new") => Ok(AliasTarget::New),
            target if target.eq_ignore_ascii_case("old") => Ok(AliasTarget::Old),
            target => Err(ParseError::UnexpectedToken {
                expected: "ALIAS FOR target $n, NEW, or OLD",
                actual: target.into(),
            }),
        },
        _ => Err(ParseError::UnexpectedToken {
            expected: "PL/pgSQL alias target",
            actual: inner.as_str().into(),
        }),
    }
}

fn build_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match inner.as_rule() {
        Rule::nested_block_stmt => {
            let block = inner
                .into_inner()
                .find(|part| part.as_rule() == Rule::block)
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(Stmt::Block(build_block(block)?))
        }
        Rule::null_stmt => Ok(Stmt::Null),
        Rule::assign_stmt => build_assign_stmt(inner),
        Rule::if_stmt => build_if_stmt(inner),
        Rule::while_stmt => build_while_stmt(inner),
        Rule::for_stmt => build_for_stmt(inner),
        Rule::raise_stmt => build_raise_stmt(inner),
        Rule::return_stmt => build_return_stmt(inner),
        Rule::return_next_stmt => build_return_next_stmt(inner),
        Rule::return_query_stmt => build_return_query_stmt(inner),
        Rule::perform_stmt => build_perform_stmt(inner),
        Rule::exec_sql_stmt => build_exec_sql_stmt(inner),
        _ => Err(ParseError::UnexpectedToken {
            expected: "plpgsql statement",
            actual: inner.as_str().into(),
        }),
    }
}

fn build_assign_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let mut target = None;
    let mut expr = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::assign_target => target = Some(build_assign_target(part)?),
            Rule::expr_until_semi => expr = Some(part.as_str().trim().to_string()),
            _ => {}
        }
    }
    Ok(Stmt::Assign {
        target: target.ok_or(ParseError::UnexpectedEof)?,
        expr: expr.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_assign_target(pair: Pair<'_, Rule>) -> Result<AssignTarget, ParseError> {
    let raw = pair.as_str().to_string();
    let parts = pair
        .into_inner()
        .filter(|part| part.as_rule() == Rule::ident)
        .map(build_ident)
        .collect::<Vec<_>>();
    match parts.as_slice() {
        [name] => Ok(AssignTarget::Name(name.clone())),
        [relation, field] => Ok(AssignTarget::Field {
            relation: relation.clone(),
            field: field.clone(),
        }),
        _ => Err(ParseError::UnexpectedToken {
            expected: "assignment target",
            actual: raw,
        }),
    }
}

fn build_if_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let mut branches = Vec::new();
    let mut else_branch = Vec::new();
    let mut current_condition: Option<String> = None;
    let mut current_body: Vec<Stmt> = Vec::new();
    let mut in_else = false;

    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::expr_until_then => {
                if in_else {
                    continue;
                }
                if let Some(condition) = current_condition.take() {
                    branches.push((condition, std::mem::take(&mut current_body)));
                }
                current_condition = Some(part.as_str().trim().to_string());
            }
            Rule::stmt => {
                let stmt = build_stmt(part)?;
                if in_else {
                    else_branch.push(stmt);
                } else {
                    current_body.push(stmt);
                }
            }
            Rule::elsif_clause => {
                if let Some(condition) = current_condition.take() {
                    branches.push((condition, std::mem::take(&mut current_body)));
                }
                for inner in part.into_inner() {
                    match inner.as_rule() {
                        Rule::expr_until_then => {
                            current_condition = Some(inner.as_str().trim().to_string());
                        }
                        Rule::stmt => current_body.push(build_stmt(inner)?),
                        _ => {}
                    }
                }
            }
            Rule::else_clause => {
                if let Some(condition) = current_condition.take() {
                    branches.push((condition, std::mem::take(&mut current_body)));
                }
                in_else = true;
                for inner in part.into_inner() {
                    if inner.as_rule() == Rule::stmt {
                        else_branch.push(build_stmt(inner)?);
                    }
                }
            }
            _ => {}
        }
    }

    if let Some(condition) = current_condition {
        branches.push((condition, current_body));
    }

    Ok(Stmt::If {
        branches,
        else_branch,
    })
}

fn build_for_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let mut targets = Vec::new();
    let mut source = None;
    let mut body = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::for_target_list => {
                targets.extend(
                    part.into_inner()
                        .filter(|inner| inner.as_rule() == Rule::assign_target)
                        .map(build_assign_target)
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
            Rule::expr_until_loop if source.is_none() => {
                source = Some(part.as_str().trim().to_string());
            }
            Rule::stmt => body.push(build_stmt(part)?),
            _ => {}
        }
    }
    let source = source.ok_or(ParseError::UnexpectedEof)?;

    if source_starts_with_execute(&source) {
        let (sql_expr, using_exprs) = split_execute_query_source(&source)?;
        return Ok(Stmt::ForQuery {
            target: build_for_target(targets)?,
            source: ForQuerySource::Execute {
                sql_expr,
                using_exprs,
            },
            body,
        });
    }

    if let Some(range_index) = find_top_level_range_op(&source) {
        let [AssignTarget::Name(var_name)] = targets.as_slice() else {
            return Err(ParseError::UnexpectedToken {
                expected: "single loop variable for integer FOR loop",
                actual: format!("{targets:?}"),
            });
        };
        let start_expr = source[..range_index].trim();
        let end_expr = source[range_index + 2..].trim();
        if start_expr.is_empty() || end_expr.is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "FOR start_expr .. end_expr",
                actual: source,
            });
        }
        return Ok(Stmt::ForInt {
            var_name: var_name.clone(),
            start_expr: start_expr.to_string(),
            end_expr: end_expr.to_string(),
            body,
        });
    }

    Ok(Stmt::ForQuery {
        target: build_for_target(targets)?,
        source: ForQuerySource::Static(source),
        body,
    })
}

fn build_while_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let mut condition = None;
    let mut body = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::expr_until_loop if condition.is_none() => {
                condition = Some(part.as_str().trim().to_string());
            }
            Rule::stmt => body.push(build_stmt(part)?),
            _ => {}
        }
    }
    Ok(Stmt::While {
        condition: condition.ok_or(ParseError::UnexpectedEof)?,
        body,
    })
}

fn build_raise_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let raw = pair.as_str().to_string();
    let mut level = RaiseLevel::Exception;
    let mut message = None;
    let mut message_sql = None::<String>;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::raise_level => {
                let token = part.as_str();
                level = if token.eq_ignore_ascii_case("notice") {
                    RaiseLevel::Notice
                } else if token.eq_ignore_ascii_case("warning") {
                    RaiseLevel::Warning
                } else {
                    RaiseLevel::Exception
                };
            }
            Rule::sql_string => {
                message_sql = Some(part.as_str().to_string());
                let expr = parse_expr(part.as_str())?;
                let text = match expr {
                    SqlExpr::Const(Value::Text(text)) => text.to_string(),
                    other => {
                        return Err(ParseError::UnexpectedToken {
                            expected: "RAISE format string literal",
                            actual: format!("{other:?}"),
                        });
                    }
                };
                message = Some(text);
            }
            _ => {}
        }
    }
    let params = raise_params_from_raw_sql(&raw, message_sql.as_deref())?;
    Ok(Stmt::Raise {
        level,
        message: message.ok_or(ParseError::UnexpectedEof)?,
        params,
    })
}

fn raise_params_from_raw_sql(
    raw: &str,
    message_sql: Option<&str>,
) -> Result<Vec<String>, ParseError> {
    let Some(message_sql) = message_sql else {
        return Ok(Vec::new());
    };
    let Some(message_start) = raw.find(message_sql) else {
        return Ok(Vec::new());
    };
    let mut rest = raw[message_start + message_sql.len()..].trim();
    if let Some(stripped) = rest.strip_suffix(';') {
        rest = stripped.trim_end();
    }
    let Some(stripped) = rest.strip_prefix(',') else {
        return Ok(Vec::new());
    };
    let rest = stripped.trim();
    if rest.is_empty() {
        return Ok(Vec::new());
    }
    split_top_level_csv(rest).ok_or_else(|| ParseError::UnexpectedToken {
        expected: "RAISE parameter list",
        actual: rest.to_string(),
    })
}

fn build_return_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let expr = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::expr_until_semi)
        .map(|part| part.as_str().trim().to_string())
        .filter(|text| !text.is_empty());
    Ok(Stmt::Return { expr })
}

fn build_return_next_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let expr = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::expr_until_semi)
        .map(|part| part.as_str().trim().to_string())
        .filter(|text| !text.is_empty());
    Ok(Stmt::ReturnNext { expr })
}

fn build_return_query_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let sql = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::return_query_sql)
        .map(|part| part.as_str().trim().to_string())
        .ok_or(ParseError::UnexpectedEof)?;
    let lowered = sql.trim_start().to_ascii_lowercase();
    let kind = if lowered.starts_with("select") || lowered.starts_with("with") {
        ReturnQueryKind::Select
    } else if lowered.starts_with("values") {
        ReturnQueryKind::Values
    } else {
        return Err(ParseError::UnexpectedToken {
            expected: "RETURN QUERY SELECT ... or RETURN QUERY VALUES (...)",
            actual: sql,
        });
    };
    Ok(Stmt::ReturnQuery { sql, kind })
}

fn build_perform_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let sql = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::exec_sql_text)
        .map(|part| part.as_str().trim().to_string())
        .filter(|text| !text.is_empty())
        .ok_or(ParseError::UnexpectedEof)?;
    Ok(Stmt::Perform { sql })
}

fn build_exec_sql_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let sql = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::exec_sql_text)
        .map(|part| part.as_str().trim().to_string())
        .filter(|text| !text.is_empty())
        .ok_or(ParseError::UnexpectedEof)?;
    Ok(Stmt::ExecSql { sql })
}

fn build_ident(pair: Pair<'_, Rule>) -> String {
    let raw = pair.as_str();
    if raw.starts_with('"') && raw.ends_with('"') {
        raw[1..raw.len() - 1].replace("\"\"", "\"")
    } else {
        raw.to_ascii_lowercase()
    }
}

fn build_for_target(targets: Vec<AssignTarget>) -> Result<ForTarget, ParseError> {
    match targets.as_slice() {
        [] => Err(ParseError::UnexpectedEof),
        [target] => Ok(ForTarget::Single(target.clone())),
        _ => Ok(ForTarget::List(targets)),
    }
}

fn source_starts_with_execute(source: &str) -> bool {
    let trimmed = source.trim_start();
    keyword_at(trimmed, 0, "execute")
}

fn split_execute_query_source(source: &str) -> Result<(String, Vec<String>), ParseError> {
    let trimmed = source.trim_start();
    let rest = trimmed["execute".len()..].trim_start();
    if rest.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "FOR ... IN EXECUTE <query>",
            actual: source.to_string(),
        });
    }

    let Some(using_index) = find_next_top_level_keyword(rest, &["using"]) else {
        return Ok((rest.to_string(), Vec::new()));
    };
    let sql_expr = rest[..using_index].trim();
    let using_sql = rest[using_index + "using".len()..].trim();
    if sql_expr.is_empty() || using_sql.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "FOR ... IN EXECUTE <query> USING expr [, ...]",
            actual: source.to_string(),
        });
    }
    let using_exprs =
        split_top_level_csv(using_sql).ok_or_else(|| ParseError::UnexpectedToken {
            expected: "FOR ... IN EXECUTE <query> USING expr [, ...]",
            actual: source.to_string(),
        })?;
    Ok((sql_expr.to_string(), using_exprs))
}

fn find_next_top_level_keyword(sql: &str, keywords: &[&str]) -> Option<usize> {
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

fn find_top_level_range_op(sql: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut idx = 0usize;
    while idx + 1 < bytes.len() {
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
            '.' if depth == 0 && bracket_depth == 0 && bytes[idx + 1] == b'.' => {
                return Some(idx);
            }
            _ => {}
        }
        idx += 1;
    }
    None
}

fn split_top_level_csv(input: &str) -> Option<Vec<String>> {
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

fn dollar_quote_tag_at(sql: &str, idx: usize) -> Option<&str> {
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

fn keyword_at(sql: &str, idx: usize, keyword: &str) -> bool {
    let bytes = sql.as_bytes();
    let end = idx.saturating_add(keyword.len());
    if end > bytes.len() || !sql[idx..end].eq_ignore_ascii_case(keyword) {
        return false;
    }
    let before_ok = idx == 0 || !is_identifier_char(bytes[idx - 1] as char);
    let after_ok = end == bytes.len() || !is_identifier_char(bytes[end] as char);
    before_ok && after_ok
}

fn is_identifier_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::parser::SqlTypeKind;

    #[test]
    fn parse_basic_block_with_declare_if_for_and_raise() {
        let block = parse_block(
            "
            declare
                total int4 := 0;
            begin
                total := total + 1;
                if total > 0 then
                    raise notice 'value %', total;
                elsif total < 0 then
                    null;
                else
                    total := 1;
                end if;
                for i in 1..3 loop
                    total := total + i;
                end loop;
            end
            ",
        )
        .unwrap();
        assert_eq!(block.declarations.len(), 1);
        let Decl::Var(total_decl) = &block.declarations[0] else {
            panic!("expected variable declaration");
        };
        assert_eq!(total_decl.name, "total");
        assert_eq!(total_decl.ty.kind, SqlTypeKind::Int4);
        assert_eq!(block.statements.len(), 3);
    }

    #[test]
    fn parse_raise_params_ignore_commas_inside_nested_sql() {
        let block = parse_block(
            "
            begin
                raise notice 'trigger = %, new table = %',
                    TG_NAME,
                    (select string_agg(new_table::text, ', ' order by a) from new_table);
            end
            ",
        )
        .unwrap();

        let Stmt::Raise {
            message, params, ..
        } = &block.statements[0]
        else {
            panic!("expected raise statement");
        };
        assert_eq!(message, "trigger = %, new table = %");
        assert_eq!(
            params,
            &vec![
                "TG_NAME".to_string(),
                "(select string_agg(new_table::text, ', ' order by a) from new_table)".to_string(),
            ]
        );
    }

    #[test]
    fn parse_raise_accepts_dollar_quoted_message() {
        let block = parse_block(
            r#"
            begin
                raise exception $$Patchfield "%" does not exist$$, ps.pfname;
                raise exception $q$system "%" does not exist$q$, new.sysname;
            end
            "#,
        )
        .unwrap();

        let Stmt::Raise {
            level,
            message,
            params,
        } = &block.statements[0]
        else {
            panic!("expected first RAISE statement");
        };
        assert!(matches!(level, RaiseLevel::Exception));
        assert_eq!(message, "Patchfield \"%\" does not exist");
        assert_eq!(params, &vec!["ps.pfname".to_string()]);

        let Stmt::Raise {
            message, params, ..
        } = &block.statements[1]
        else {
            panic!("expected second RAISE statement");
        };
        assert_eq!(message, "system \"%\" does not exist");
        assert_eq!(params, &vec!["new.sysname".to_string()]);
    }

    #[test]
    fn parse_while_stmt() {
        let block = parse_block(
            "
            begin
                while current_value is not null loop
                    null;
                end loop;
            end
            ",
        )
        .unwrap();

        let Stmt::While { condition, body } = &block.statements[0] else {
            panic!("expected top-level while statement");
        };
        assert_eq!(condition, "current_value is not null");
        assert_eq!(body.len(), 1);
    }

    #[test]
    fn parse_if_stmt_preserves_elsif_branches() {
        let block = parse_block(
            "
            begin
                if first_condition then
                    null;
                elsif second_condition then
                    null;
                elsif third_condition then
                    null;
                else
                    null;
                end if;
            end
            ",
        )
        .unwrap();

        let Stmt::If {
            branches,
            else_branch,
        } = &block.statements[0]
        else {
            panic!("expected top-level if statement");
        };

        assert_eq!(branches.len(), 3);
        assert_eq!(branches[0].0, "first_condition");
        assert_eq!(branches[1].0, "second_condition");
        assert_eq!(branches[2].0, "third_condition");
        assert_eq!(else_branch.len(), 1);
    }

    #[test]
    fn parse_nested_block_statement() {
        let block = parse_block(
            "
            begin
                begin
                    null;
                end;
            end
            ",
        )
        .unwrap();
        assert!(matches!(block.statements[0], Stmt::Block(_)));
    }

    #[test]
    fn parse_rejects_unsupported_exception_handler() {
        let err = parse_block(
            "
            begin
                begin
                    null;
                exception when others then
                    null;
                end;
            end
            ",
        )
        .unwrap_err();

        assert!(
            format!("{err:?}")
                .to_ascii_lowercase()
                .contains("exception")
        );
    }

    #[test]
    fn parse_alias_and_exec_sql_statements() {
        let block = parse_block(
            "
            declare
                myname alias for $1;
                rec record;
            begin
                select into rec * from slots where slotname = myname;
                update slots set backlink = 'x' where slotname = myname;
                perform 1 + 1;
            end
            ",
        )
        .unwrap();
        assert_eq!(block.declarations.len(), 2);
        assert!(matches!(block.declarations[0], Decl::Alias(_)));
        assert!(matches!(block.declarations[1], Decl::Var(_)));
        assert!(matches!(block.statements[0], Stmt::ExecSql { .. }));
        assert!(matches!(block.statements[1], Stmt::ExecSql { .. }));
        assert!(matches!(block.statements[2], Stmt::Perform { .. }));
    }

    #[test]
    fn parse_alias_for_trigger_rows() {
        let block = parse_block(
            "
            declare
                ps alias for new;
                prior alias for old;
            begin
                return ps;
            end
            ",
        )
        .unwrap();

        assert_eq!(
            block.declarations,
            vec![
                Decl::Alias(AliasDecl {
                    name: "ps".into(),
                    target: AliasTarget::New,
                }),
                Decl::Alias(AliasDecl {
                    name: "prior".into(),
                    target: AliasTarget::Old,
                }),
            ]
        );
    }

    #[test]
    fn parse_static_query_for_loop() {
        let block = parse_block(
            "
            begin
                for objtype in values
                    ('table'), ('index'), ('sequence'), ('view')
                loop
                    null;
                end loop;
            end
            ",
        )
        .unwrap();

        let Stmt::ForQuery {
            target,
            source,
            body,
        } = &block.statements[0]
        else {
            panic!("expected query FOR loop");
        };
        assert_eq!(
            target,
            &ForTarget::Single(AssignTarget::Name("objtype".into()))
        );
        assert_eq!(
            source,
            &ForQuerySource::Static(
                "values\n                    ('table'), ('index'), ('sequence'), ('view')".into()
            )
        );
        assert_eq!(body.len(), 1);
    }

    #[test]
    fn parse_dynamic_execute_query_for_loop_with_using() {
        let block = parse_block(
            "
            begin
                for ln in execute format('select %s', $1) using current_value, current_value + 1 loop
                    null;
                end loop;
            end
            ",
        )
        .unwrap();

        let Stmt::ForQuery { target, source, .. } = &block.statements[0] else {
            panic!("expected query FOR loop");
        };
        assert_eq!(target, &ForTarget::Single(AssignTarget::Name("ln".into())));
        assert_eq!(
            source,
            &ForQuerySource::Execute {
                sql_expr: "format('select %s', $1)".into(),
                using_exprs: vec!["current_value".into(), "current_value + 1".into()],
            }
        );
    }

    #[test]
    fn parse_query_for_loop_with_scalar_target_list() {
        let block = parse_block(
            "
            begin
                for a, b in values (1, 'x') loop
                    null;
                end loop;
            end
            ",
        )
        .unwrap();

        let Stmt::ForQuery { target, .. } = &block.statements[0] else {
            panic!("expected query FOR loop");
        };
        assert_eq!(
            target,
            &ForTarget::List(vec![
                AssignTarget::Name("a".into()),
                AssignTarget::Name("b".into()),
            ])
        );
    }

    #[test]
    fn reject_multi_target_integer_for_loops() {
        let err = parse_block(
            "
            begin
                for a, b in 1..3 loop
                    null;
                end loop;
            end
            ",
        )
        .unwrap_err();
        assert!(matches!(err, ParseError::UnexpectedToken { .. }));
    }
}
