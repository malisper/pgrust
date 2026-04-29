use pest::iterators::Pair;
use pgrust_plpgsql_grammar::Rule;

use crate::backend::executor::Value;
use crate::backend::parser::{
    ParseError, RawTypeName, SerialKind, SqlExpr, SqlType, SqlTypeKind, parse_expr, parse_type_name,
};
use crate::include::catalog::RECORD_TYPE_OID;

use super::ast::{
    AliasDecl, AliasTarget, AssignTarget, Block, CursorArg, CursorDecl, CursorDirection,
    CursorParamDecl, Decl, ExceptionCondition, ExceptionHandler, ForQuerySource, ForTarget,
    OpenCursorSource, RaiseCondition, RaiseLevel, RaiseUsingOption, Stmt, VarDecl,
};

pub fn parse_block(sql: &str) -> Result<Block, ParseError> {
    pgrust_plpgsql_grammar::parse_rule(Rule::pl_block, sql)
        .map_err(|e| map_pest_error("plpgsql block", e))
        .and_then(|mut pairs| build_pl_block(pairs.next().ok_or(ParseError::UnexpectedEof)?))
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
    let mut label = None;
    let mut declarations = Vec::new();
    let mut statements = Vec::new();
    let mut exception_handlers = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::block_label => label = Some(build_block_label(part)?),
            Rule::declare_section => declarations = build_declare_section(part)?,
            Rule::stmt => statements.push(build_stmt(part)?),
            Rule::exception_section => exception_handlers = build_exception_section(part)?,
            _ => {}
        }
    }
    Ok(Block {
        label,
        declarations,
        statements,
        exception_handlers,
    })
}

fn build_exception_section(pair: Pair<'_, Rule>) -> Result<Vec<ExceptionHandler>, ParseError> {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::exception_clause)
        .map(build_exception_clause)
        .collect()
}

fn build_exception_clause(pair: Pair<'_, Rule>) -> Result<ExceptionHandler, ParseError> {
    let mut conditions = Vec::new();
    let mut statements = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::exception_condition => conditions.push(build_exception_condition(part)?),
            Rule::stmt => statements.push(build_stmt(part)?),
            _ => {}
        }
    }
    if conditions.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    Ok(ExceptionHandler {
        conditions,
        statements,
    })
}

fn build_exception_condition(pair: Pair<'_, Rule>) -> Result<ExceptionCondition, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match inner.as_rule() {
        Rule::others_condition => Ok(ExceptionCondition::Others),
        Rule::sqlstate_condition => {
            let sql = inner
                .into_inner()
                .find(|part| part.as_rule() == Rule::sql_string)
                .ok_or(ParseError::UnexpectedEof)?;
            let expr = parse_expr(sql.as_str())?;
            let SqlExpr::Const(Value::Text(sqlstate)) = expr else {
                return Err(ParseError::UnexpectedToken {
                    expected: "SQLSTATE string literal",
                    actual: sql.as_str().into(),
                });
            };
            Ok(ExceptionCondition::SqlState(sqlstate.to_string()))
        }
        Rule::ident => Ok(ExceptionCondition::ConditionName(build_ident(inner))),
        _ => Err(ParseError::UnexpectedToken {
            expected: "exception condition",
            actual: inner.as_str().into(),
        }),
    }
}

fn build_block_label(pair: Pair<'_, Rule>) -> Result<String, ParseError> {
    pair.into_inner()
        .find(|part| part.as_rule() == Rule::ident)
        .map(build_ident)
        .ok_or(ParseError::UnexpectedEof)
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
        Rule::cursor_decl => Ok(Decl::Cursor(build_cursor_decl(inner)?)),
        Rule::alias_decl => Ok(Decl::Alias(build_alias_decl(inner)?)),
        _ => Err(ParseError::UnexpectedToken {
            expected: "plpgsql declaration",
            actual: inner.as_str().into(),
        }),
    }
}

fn build_var_decl(pair: Pair<'_, Rule>) -> Result<VarDecl, ParseError> {
    let line = pair.as_span().start_pos().line_col().0;
    let mut name = None;
    let mut ty = None;
    let mut default_expr = None;
    let mut constant = false;
    let mut strict = false;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::ident => name = Some(build_ident(part)),
            Rule::type_name_text => {
                let mut type_name = part.as_str().trim();
                if let Some(suffix) = strip_leading_keyword(type_name, "constant") {
                    type_name = suffix.trim_start();
                    constant = true;
                }
                if let Some(prefix) = strip_trailing_keyword(type_name, "strict") {
                    type_name = prefix.trim_end();
                    strict = true;
                }
                if let Some(prefix) = strip_trailing_not_null(type_name) {
                    type_name = prefix.trim_end();
                    strict = true;
                }
                ty = Some((type_name.to_string(), decl_type_hint(type_name)?));
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
        type_name: ty
            .as_ref()
            .map(|(type_name, _)| type_name.clone())
            .ok_or(ParseError::UnexpectedEof)?,
        ty: ty.map(|(_, ty)| ty).ok_or(ParseError::UnexpectedEof)?,
        default_expr,
        constant,
        strict,
        line,
    })
}

fn build_cursor_decl(pair: Pair<'_, Rule>) -> Result<CursorDecl, ParseError> {
    let raw = pair.as_str().to_string();
    let mut name = None;
    let mut query = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::ident => name = Some(build_ident(part)),
            Rule::exec_sql_text => query = Some(part.as_str().trim().to_string()),
            _ => {}
        }
    }
    Ok(CursorDecl {
        name: name.ok_or(ParseError::UnexpectedEof)?,
        scrollable: cursor_decl_scrollable(&raw),
        params: cursor_decl_params(&raw)?,
        query: query.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn cursor_decl_scrollable(raw: &str) -> bool {
    let Some(cursor_idx) = find_next_top_level_keyword(raw, &["cursor"]) else {
        return true;
    };
    !raw[..cursor_idx].to_ascii_lowercase().contains("no scroll")
}

fn cursor_decl_params(raw: &str) -> Result<Vec<CursorParamDecl>, ParseError> {
    let Some(cursor_idx) = find_next_top_level_keyword(raw, &["cursor"]) else {
        return Ok(Vec::new());
    };
    let after_cursor_start = cursor_idx + "cursor".len();
    let Some(for_idx) = find_next_top_level_keyword(&raw[after_cursor_start..], &["for"]) else {
        return Ok(Vec::new());
    };
    let after_cursor = raw[after_cursor_start..after_cursor_start + for_idx].trim();
    if !after_cursor.starts_with('(') {
        return Ok(Vec::new());
    }
    let Some(close_idx) = matching_paren_index(after_cursor) else {
        return Err(ParseError::UnexpectedToken {
            expected: "cursor parameter list",
            actual: raw.to_string(),
        });
    };
    let params = &after_cursor[1..close_idx];
    split_top_level_csv(params)
        .unwrap_or_default()
        .into_iter()
        .map(|param| {
            let mut parts = param.splitn(2, char::is_whitespace);
            let name = parts.next().unwrap_or_default().trim();
            let type_name = parts.next().unwrap_or_default().trim();
            if name.is_empty() || type_name.is_empty() {
                return Err(ParseError::UnexpectedToken {
                    expected: "cursor parameter name and type",
                    actual: param,
                });
            }
            let name = parse_expr(name).and_then(|expr| match expr {
                SqlExpr::Column(name) => Ok(name),
                _ => Err(ParseError::UnexpectedToken {
                    expected: "cursor parameter name",
                    actual: param.clone(),
                }),
            })?;
            let ty = match parse_type_name(type_name)? {
                RawTypeName::Builtin(ty) => ty,
                RawTypeName::Named { name, .. } => {
                    return Err(ParseError::UnsupportedType(name));
                }
                RawTypeName::Serial(kind) => {
                    return Err(ParseError::FeatureNotSupported(format!(
                        "{} is only allowed in CREATE TABLE / ALTER TABLE ADD COLUMN",
                        match kind {
                            SerialKind::Small => "smallserial",
                            SerialKind::Regular => "serial",
                            SerialKind::Big => "bigserial",
                        }
                    )));
                }
                RawTypeName::Record => SqlType::record(RECORD_TYPE_OID),
            };
            Ok(CursorParamDecl {
                name,
                type_name: type_name.to_string(),
                ty,
            })
        })
        .collect()
}

fn matching_paren_index(input: &str) -> Option<usize> {
    let mut depth = 0usize;
    for (index, ch) in input.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

fn top_level_open_paren_index(input: &str) -> Option<usize> {
    let bytes = input.as_bytes();
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
        if let Some(tag) = dollar_quote_tag_at(input, idx) {
            if let Some(close) = input[idx + tag.len()..].find(tag) {
                idx += tag.len() + close + tag.len();
                continue;
            }
            return None;
        }
        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            '(' => return Some(idx),
            _ => {}
        }
        idx += 1;
    }
    None
}

fn strip_trailing_keyword<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = input.trim_end();
    if trimmed.len() < keyword.len() {
        return None;
    }
    let start = trimmed.len() - keyword.len();
    if !trimmed[start..].eq_ignore_ascii_case(keyword) {
        return None;
    }
    if start > 0 && is_identifier_char(trimmed.as_bytes()[start - 1] as char) {
        return None;
    }
    Some(&trimmed[..start])
}

fn strip_trailing_not_null(input: &str) -> Option<&str> {
    let without_null = strip_trailing_keyword(input, "null")?;
    strip_trailing_keyword(without_null, "not")
}

fn strip_leading_keyword<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = input.trim_start();
    if trimmed.len() < keyword.len() {
        return None;
    }
    if !trimmed[..keyword.len()].eq_ignore_ascii_case(keyword) {
        return None;
    }
    if trimmed[keyword.len()..]
        .chars()
        .next()
        .is_some_and(is_identifier_char)
    {
        return None;
    }
    Some(&trimmed[keyword.len()..])
}

fn decl_type_hint(type_name: &str) -> Result<SqlType, ParseError> {
    if type_name.trim_end().to_ascii_lowercase().ends_with("%type")
        || type_name
            .trim_end()
            .to_ascii_lowercase()
            .ends_with("%rowtype")
    {
        return Ok(SqlType::record(RECORD_TYPE_OID));
    }
    match parse_type_name(type_name)? {
        RawTypeName::Builtin(sql_type) => Ok(sql_type),
        RawTypeName::Serial(kind) => Err(ParseError::FeatureNotSupported(format!(
            "{} is only allowed in CREATE TABLE / ALTER TABLE ADD COLUMN",
            match kind {
                SerialKind::Small => "smallserial",
                SerialKind::Regular => "serial",
                SerialKind::Big => "bigserial",
            }
        ))),
        RawTypeName::Record => Ok(SqlType::record(RECORD_TYPE_OID)),
        RawTypeName::Named { array_bounds, .. } => {
            let mut ty = SqlType::new(SqlTypeKind::Composite);
            for _ in 0..array_bounds {
                ty = SqlType::array_of(ty);
            }
            Ok(ty)
        }
    }
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
    let line = inner.as_span().start_pos().line_col().0;
    let stmt = match inner.as_rule() {
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
        Rule::loop_stmt => build_loop_stmt(inner),
        Rule::exit_stmt => build_exit_stmt(inner),
        Rule::for_stmt => build_for_stmt(inner),
        Rule::foreach_stmt => build_foreach_stmt(inner),
        Rule::raise_stmt => build_raise_stmt(inner),
        Rule::assert_stmt => build_assert_stmt(inner),
        Rule::continue_stmt => Ok(Stmt::Continue),
        Rule::return_stmt => build_return_stmt(inner),
        Rule::return_next_stmt => build_return_next_stmt(inner),
        Rule::return_query_stmt => build_return_query_stmt(inner),
        Rule::perform_stmt => build_perform_stmt(inner),
        Rule::dynamic_execute_stmt => build_dynamic_execute_stmt(inner),
        Rule::get_diagnostics_stmt => build_get_diagnostics_stmt(inner),
        Rule::open_cursor_stmt => build_open_cursor_stmt(inner),
        Rule::fetch_cursor_stmt => build_fetch_cursor_stmt(inner),
        Rule::move_cursor_stmt => build_move_cursor_stmt(inner),
        Rule::close_cursor_stmt => build_close_cursor_stmt(inner),
        Rule::exec_sql_stmt => build_exec_sql_stmt(inner),
        _ => Err(ParseError::UnexpectedToken {
            expected: "plpgsql statement",
            actual: inner.as_str().into(),
        }),
    }?;
    Ok(Stmt::WithLine {
        line,
        stmt: Box::new(stmt),
    })
}

fn build_assign_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let line = pair.as_span().start_pos().line_col().0;
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
        line,
    })
}

fn build_assign_target(pair: Pair<'_, Rule>) -> Result<AssignTarget, ParseError> {
    let raw = pair.as_str().to_string();
    let mut parts = Vec::new();
    let mut subscripts = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::ident => parts.push(build_ident(part)),
            Rule::assign_target_subscript => {
                let expr = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::assign_subscript_expr)
                    .map(|inner| inner.as_str().trim().to_string())
                    .unwrap_or_default();
                subscripts.push(expr);
            }
            _ => {}
        }
    }
    match (parts.as_slice(), subscripts.is_empty()) {
        ([name], true) => Ok(AssignTarget::Name(name.clone())),
        ([name], false) => Ok(AssignTarget::Subscript {
            name: name.clone(),
            subscripts,
        }),
        ([relation, field], true) => Ok(AssignTarget::Field {
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

    if source_can_be_cursor_reference(&source)
        && let Ok((name, args)) = parse_cursor_name_and_args(&source)
    {
        return Ok(Stmt::ForQuery {
            target: build_for_target(targets)?,
            source: ForQuerySource::Cursor { name, args },
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

fn build_loop_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let body = pair
        .into_inner()
        .filter(|part| part.as_rule() == Rule::stmt)
        .map(build_stmt)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Stmt::Loop { body })
}

fn build_exit_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let condition = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::expr_until_semi)
        .map(|part| part.as_str().trim().to_string())
        .filter(|text| !text.is_empty());
    Ok(Stmt::Exit { condition })
}

fn build_foreach_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let mut targets = Vec::new();
    let mut slice = 0usize;
    let mut array_expr = None;
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
            Rule::foreach_slice => {
                let raw = part
                    .as_str()
                    .split_whitespace()
                    .last()
                    .ok_or(ParseError::UnexpectedEof)?;
                slice = raw
                    .parse::<usize>()
                    .map_err(|_| ParseError::UnexpectedToken {
                        expected: "FOREACH SLICE integer literal",
                        actual: part.as_str().into(),
                    })?;
            }
            Rule::expr_until_loop if array_expr.is_none() => {
                array_expr = Some(part.as_str().trim().to_string());
            }
            Rule::stmt => body.push(build_stmt(part)?),
            _ => {}
        }
    }
    Ok(Stmt::ForEach {
        target: build_for_target(targets)?,
        slice,
        array_expr: array_expr.ok_or(ParseError::UnexpectedEof)?,
        body,
    })
}

fn build_raise_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let raw = pair.as_str().to_string();
    let mut level = RaiseLevel::Exception;
    let mut message = None::<String>;
    let mut message_sql = None::<String>;
    let mut condition = None::<RaiseCondition>;
    let mut using_options = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::raise_level => {
                let token = part.as_str();
                level = if token.eq_ignore_ascii_case("info") {
                    RaiseLevel::Info
                } else if token.eq_ignore_ascii_case("log") {
                    RaiseLevel::Log
                } else if token.eq_ignore_ascii_case("notice") {
                    RaiseLevel::Notice
                } else if token.eq_ignore_ascii_case("warning") {
                    RaiseLevel::Warning
                } else if token.eq_ignore_ascii_case("log") {
                    RaiseLevel::Log
                } else {
                    RaiseLevel::Exception
                };
            }
            Rule::raise_message_clause => {
                for inner in part.into_inner() {
                    match inner.as_rule() {
                        Rule::sql_string => {
                            message_sql = Some(inner.as_str().to_string());
                            message = Some(raise_string_literal_text(inner.as_str())?);
                        }
                        Rule::raise_using_clause => {
                            using_options.extend(build_raise_using_clause(inner)?);
                        }
                        _ => {}
                    }
                }
            }
            Rule::raise_condition_clause => {
                let mut condition_name = None;
                let mut explicit_sqlstate = None;
                for inner in part.into_inner() {
                    match inner.as_rule() {
                        Rule::ident => condition_name = Some(build_ident(inner)),
                        Rule::sqlstate_condition => {
                            explicit_sqlstate = Some(sqlstate_condition_literal(inner)?);
                        }
                        Rule::raise_using_clause => {
                            using_options.extend(build_raise_using_clause(inner)?);
                        }
                        _ => {}
                    }
                }
                if let Some(value) = explicit_sqlstate {
                    condition = Some(RaiseCondition::SqlState(value));
                } else {
                    let condition_name = condition_name.ok_or(ParseError::UnexpectedEof)?;
                    condition = Some(RaiseCondition::ConditionName(condition_name));
                }
            }
            Rule::raise_using_clause => using_options.extend(build_raise_using_clause(part)?),
            _ => {}
        }
    }
    let params = raise_params_from_raw_sql(&raw, message_sql.as_deref())?;
    Ok(Stmt::Raise {
        level,
        condition,
        message,
        params,
        using_options,
    })
}

fn build_raise_using_clause(pair: Pair<'_, Rule>) -> Result<Vec<RaiseUsingOption>, ParseError> {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::raise_using_item)
        .map(|item| {
            let mut name = None;
            let mut expr = None;
            for part in item.into_inner() {
                match part.as_rule() {
                    Rule::ident => name = Some(build_ident(part)),
                    Rule::expr_until_comma_or_semi => {
                        expr = Some(part.as_str().trim().to_string());
                    }
                    _ => {}
                }
            }
            Ok(RaiseUsingOption {
                name: name.ok_or(ParseError::UnexpectedEof)?,
                expr: expr.ok_or(ParseError::UnexpectedEof)?,
            })
        })
        .collect()
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
    rest = stripped.trim();
    if let Some(using_idx) = find_next_top_level_keyword(rest, &["using"]) {
        rest = rest[..using_idx].trim_end();
    }
    if rest.is_empty() {
        return Ok(Vec::new());
    }
    split_top_level_csv(rest).ok_or_else(|| ParseError::UnexpectedToken {
        expected: "RAISE parameter list",
        actual: rest.to_string(),
    })
}

fn sqlstate_condition_literal(pair: Pair<'_, Rule>) -> Result<String, ParseError> {
    let sql = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::sql_string)
        .ok_or(ParseError::UnexpectedEof)?;
    let expr = parse_expr(sql.as_str())?;
    let SqlExpr::Const(Value::Text(sqlstate)) = expr else {
        return Err(ParseError::UnexpectedToken {
            expected: "SQLSTATE string literal",
            actual: sql.as_str().into(),
        });
    };
    Ok(sqlstate.to_string())
}

fn raise_string_literal_text(sql: &str) -> Result<String, ParseError> {
    let expr = parse_expr(sql)?;
    match expr {
        SqlExpr::Const(Value::Text(text)) => Ok(text.to_string()),
        other => Err(ParseError::UnexpectedToken {
            expected: "RAISE format string literal",
            actual: format!("{other:?}"),
        }),
    }
}

fn exception_condition_name_sqlstate(name: &str) -> Option<&'static str> {
    match name.to_ascii_lowercase().as_str() {
        "assert_failure" => Some("P0004"),
        "data_corrupted" => Some("XX001"),
        "division_by_zero" => Some("22012"),
        "feature_not_supported" => Some("0A000"),
        "raise_exception" => Some("P0001"),
        "reading_sql_data_not_permitted" => Some("2F003"),
        _ => None,
    }
}

fn build_assert_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let mut condition = None;
    let mut message = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::expr_until_comma_or_semi => condition = Some(part.as_str().trim().to_string()),
            Rule::expr_until_semi => message = Some(part.as_str().trim().to_string()),
            _ => {}
        }
    }
    Ok(Stmt::Assert {
        condition: condition
            .filter(|text| !text.is_empty())
            .ok_or(ParseError::UnexpectedEof)?,
        message: message.filter(|text| !text.is_empty()),
    })
}

fn build_return_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let line = pair.as_span().start_pos().line_col().0;
    let expr = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::expr_until_semi)
        .map(|part| part.as_str().trim().to_string())
        .filter(|text| !text.is_empty());
    Ok(Stmt::Return { expr, line })
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
    let source = if source_starts_with_execute(&sql) {
        let (sql_expr, using_exprs) = split_execute_query_source(&sql)?;
        ForQuerySource::Execute {
            sql_expr,
            using_exprs,
        }
    } else if sql_starts_static_query(&sql) {
        ForQuerySource::Static(sql)
    } else {
        return Err(ParseError::UnexpectedToken {
            expected: "RETURN QUERY SELECT ..., RETURN QUERY VALUES (...), or RETURN QUERY EXECUTE ...",
            actual: sql,
        });
    };
    Ok(Stmt::ReturnQuery { source })
}

fn build_perform_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let line = pair.as_span().start_pos().line_col().0;
    let sql = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::exec_sql_text)
        .map(|part| part.as_str().trim().to_string())
        .filter(|text| !text.is_empty())
        .ok_or(ParseError::UnexpectedEof)?;
    Ok(Stmt::Perform { sql, line })
}

fn build_dynamic_execute_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let line = pair.as_span().start_pos().line_col().0;
    let raw = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::exec_sql_text)
        .map(|part| part.as_str().trim().to_string())
        .filter(|text| !text.is_empty())
        .ok_or(ParseError::UnexpectedEof)?;
    let (sql_expr, strict, into_targets, using_exprs) = split_dynamic_execute_stmt(&raw)?;
    Ok(Stmt::DynamicExecute {
        sql_expr,
        strict,
        into_targets,
        using_exprs,
        line,
    })
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

fn build_get_diagnostics_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let stacked = pair
        .as_str()
        .trim_start()
        .to_ascii_lowercase()
        .starts_with("get stacked");
    let mut items = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::get_diagnostics_item => {
                let mut target = None;
                let mut item = None;
                for inner in part.into_inner() {
                    match inner.as_rule() {
                        Rule::assign_target => target = Some(build_assign_target(inner)?),
                        Rule::ident => item = Some(build_ident(inner)),
                        _ => {}
                    }
                }
                items.push((
                    target.ok_or(ParseError::UnexpectedEof)?,
                    item.ok_or(ParseError::UnexpectedEof)?,
                ));
            }
            _ => {}
        }
    }
    Ok(Stmt::GetDiagnostics { stacked, items })
}

fn build_open_cursor_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let text = pair
        .into_inner()
        .find(|inner| inner.as_rule() == Rule::exec_sql_text)
        .map(|inner| inner.as_str().trim().to_string())
        .ok_or(ParseError::UnexpectedEof)?;
    let Some(for_idx) = find_next_top_level_keyword(&text, &["for"]) else {
        let (name, args) = parse_cursor_name_and_args(&text)?;
        return Ok(Stmt::OpenCursor {
            name,
            source: OpenCursorSource::Declared { args },
        });
    };
    let name = cursor_name_from_text(&text[..for_idx])?;
    let sql = text[for_idx + "for".len()..].trim();
    if sql.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "OPEN cursor FOR query",
            actual: text,
        });
    }
    if source_starts_with_execute(sql) {
        let (sql_expr, using_exprs) = split_execute_query_source(sql)?;
        return Ok(Stmt::OpenCursor {
            name,
            source: OpenCursorSource::Dynamic {
                sql_expr,
                using_exprs,
            },
        });
    }
    Ok(Stmt::OpenCursor {
        name,
        source: OpenCursorSource::Static(sql.to_string()),
    })
}

fn build_fetch_cursor_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let text = pair
        .into_inner()
        .find(|inner| inner.as_rule() == Rule::exec_sql_text)
        .map(|inner| inner.as_str().trim().to_string())
        .ok_or(ParseError::UnexpectedEof)?;
    let Some(into_idx) = find_next_top_level_keyword(&text, &["into"]) else {
        return Err(ParseError::UnexpectedToken {
            expected: "FETCH cursor INTO target",
            actual: text,
        });
    };
    let cursor_sql = text[..into_idx].trim();
    let targets_sql = text[into_idx + "into".len()..].trim();
    let (direction, name) = parse_cursor_direction_and_name(cursor_sql)?;
    let targets = split_top_level_csv(targets_sql)
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "FETCH cursor INTO target [, ...]",
            actual: text.clone(),
        })?
        .iter()
        .map(|target| parse_dynamic_execute_into_target(target))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Stmt::FetchCursor {
        name,
        direction,
        targets,
    })
}

fn build_move_cursor_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let text = pair
        .into_inner()
        .find(|inner| inner.as_rule() == Rule::exec_sql_text)
        .map(|inner| inner.as_str().trim().to_string())
        .ok_or(ParseError::UnexpectedEof)?;
    let (direction, name) = parse_cursor_direction_and_name(&text)?;
    Ok(Stmt::MoveCursor { name, direction })
}

fn build_close_cursor_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let text = pair
        .into_inner()
        .find(|inner| inner.as_rule() == Rule::exec_sql_text)
        .map(|inner| inner.as_str().trim().to_string())
        .ok_or(ParseError::UnexpectedEof)?;
    Ok(Stmt::CloseCursor {
        name: cursor_name_from_text(&text)?,
    })
}

fn cursor_name_from_text(text: &str) -> Result<String, ParseError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "cursor name",
            actual: text.to_string(),
        });
    }
    parse_dynamic_execute_into_target(trimmed).and_then(|target| match target {
        AssignTarget::Name(name) => Ok(name),
        _ => Err(ParseError::UnexpectedToken {
            expected: "cursor variable name",
            actual: text.to_string(),
        }),
    })
}

fn parse_cursor_name_and_args(text: &str) -> Result<(String, Vec<CursorArg>), ParseError> {
    let trimmed = text.trim();
    let Some(open_idx) = top_level_open_paren_index(trimmed) else {
        return Ok((cursor_name_from_text(trimmed)?, Vec::new()));
    };
    let Some(close_idx) = matching_paren_index(&trimmed[open_idx..]) else {
        return Err(ParseError::UnexpectedToken {
            expected: "cursor argument list",
            actual: text.to_string(),
        });
    };
    if !trimmed[open_idx + close_idx + 1..].trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "cursor argument list",
            actual: text.to_string(),
        });
    }
    let name = cursor_name_from_text(&trimmed[..open_idx])?;
    let arg_sql = trimmed[open_idx + 1..open_idx + close_idx].trim();
    let args = if arg_sql.is_empty() {
        Vec::new()
    } else {
        split_top_level_csv(arg_sql)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "cursor argument list",
                actual: text.to_string(),
            })?
            .into_iter()
            .map(parse_cursor_arg)
            .collect::<Result<Vec<_>, _>>()?
    };
    Ok((name, args))
}

fn parse_cursor_arg(arg: String) -> Result<CursorArg, ParseError> {
    if let Some((op_idx, op_len)) = find_top_level_cursor_arg_assignment(&arg) {
        let name_sql = arg[..op_idx].trim();
        let expr = arg[op_idx + op_len..].trim();
        if expr.is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "cursor named argument expression",
                actual: arg,
            });
        }
        let name = parse_expr(name_sql).and_then(|expr| match expr {
            SqlExpr::Column(name) => Ok(name),
            _ => Err(ParseError::UnexpectedToken {
                expected: "cursor argument name",
                actual: name_sql.to_string(),
            }),
        })?;
        return Ok(CursorArg::Named {
            name,
            expr: expr.to_string(),
        });
    }
    Ok(CursorArg::Positional(arg))
}

fn source_can_be_cursor_reference(source: &str) -> bool {
    let trimmed = source.trim_start();
    !sql_starts_static_query(trimmed) && !keyword_at(trimmed, 0, "execute")
}

fn sql_starts_static_query(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    keyword_at(trimmed, 0, "select")
        || keyword_at(trimmed, 0, "with")
        || keyword_at(trimmed, 0, "values")
}

fn parse_cursor_direction_and_name(text: &str) -> Result<(CursorDirection, String), ParseError> {
    let words = text
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>();
    if words.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "cursor direction and name",
            actual: text.to_string(),
        });
    }

    let mut idx = 0usize;
    let direction = match words[idx].to_ascii_lowercase().as_str() {
        "from" | "in" => CursorDirection::Next,
        "next" => {
            idx += 1;
            CursorDirection::Next
        }
        "prior" => {
            idx += 1;
            CursorDirection::Prior
        }
        "first" => {
            idx += 1;
            CursorDirection::First
        }
        "last" => {
            idx += 1;
            CursorDirection::Last
        }
        "forward" => {
            idx += 1;
            parse_cursor_count_direction(&words, &mut idx, true)?
        }
        "backward" => {
            idx += 1;
            parse_cursor_count_direction(&words, &mut idx, false)?
        }
        "absolute" => {
            idx += 1;
            CursorDirection::Absolute(parse_cursor_direction_count(&words, &mut idx, text)?)
        }
        "relative" => {
            idx += 1;
            CursorDirection::Relative(parse_cursor_direction_count(&words, &mut idx, text)?)
        }
        _ => CursorDirection::Next,
    };

    if words
        .get(idx)
        .is_some_and(|word| word.eq_ignore_ascii_case("from") || word.eq_ignore_ascii_case("in"))
    {
        idx += 1;
    }
    let name = words.get(idx).ok_or_else(|| ParseError::UnexpectedToken {
        expected: "cursor name",
        actual: text.to_string(),
    })?;
    if words.get(idx + 1).is_some() {
        return Err(ParseError::UnexpectedToken {
            expected: "cursor name",
            actual: text.to_string(),
        });
    }
    Ok((direction, cursor_name_from_text(name)?))
}

fn parse_cursor_count_direction(
    words: &[String],
    idx: &mut usize,
    forward: bool,
) -> Result<CursorDirection, ParseError> {
    let Some(word) = words.get(*idx) else {
        return Ok(if forward {
            CursorDirection::Forward(1)
        } else {
            CursorDirection::Backward(1)
        });
    };
    if word.eq_ignore_ascii_case("all") {
        *idx += 1;
        return Ok(if forward {
            CursorDirection::ForwardAll
        } else {
            CursorDirection::BackwardAll
        });
    }
    if let Ok(count) = word.parse::<i64>() {
        *idx += 1;
        return Ok(if forward {
            CursorDirection::Forward(count)
        } else {
            CursorDirection::Backward(count)
        });
    }
    Ok(if forward {
        CursorDirection::Forward(1)
    } else {
        CursorDirection::Backward(1)
    })
}

fn parse_cursor_direction_count(
    words: &[String],
    idx: &mut usize,
    text: &str,
) -> Result<i64, ParseError> {
    let word = words.get(*idx).ok_or_else(|| ParseError::UnexpectedToken {
        expected: "cursor direction count",
        actual: text.to_string(),
    })?;
    *idx += 1;
    word.parse::<i64>()
        .map_err(|_| ParseError::UnexpectedToken {
            expected: "cursor direction count",
            actual: text.to_string(),
        })
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

fn split_dynamic_execute_stmt(
    source: &str,
) -> Result<(String, bool, Vec<AssignTarget>, Vec<String>), ParseError> {
    let first_clause = find_next_top_level_keyword(source, &["into", "using"]);
    let sql_expr = first_clause
        .map(|index| source[..index].trim())
        .unwrap_or_else(|| source.trim());
    if sql_expr.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "EXECUTE <query>",
            actual: source.to_string(),
        });
    }

    let mut strict = false;
    let mut into_targets = Vec::new();
    let mut using_exprs = Vec::new();
    let mut rest = first_clause
        .map(|index| source[index..].trim_start())
        .unwrap_or("");
    while !rest.is_empty() {
        if keyword_at(rest, 0, "into") {
            if !into_targets.is_empty() {
                return Err(ParseError::UnexpectedToken {
                    expected: "EXECUTE <query> INTO target [, ...]",
                    actual: source.to_string(),
                });
            }
            rest = rest["into".len()..].trim_start();
            let next_clause = find_next_top_level_keyword(rest, &["into", "using"]);
            let mut targets_sql = next_clause
                .map(|index| rest[..index].trim())
                .unwrap_or_else(|| rest.trim());
            if keyword_at(targets_sql, 0, "strict") {
                strict = true;
                targets_sql = targets_sql["strict".len()..].trim_start();
            }
            if targets_sql.is_empty() {
                return Err(ParseError::UnexpectedToken {
                    expected: "EXECUTE <query> INTO target [, ...]",
                    actual: source.to_string(),
                });
            }
            into_targets = split_top_level_csv(targets_sql)
                .ok_or_else(|| ParseError::UnexpectedToken {
                    expected: "EXECUTE <query> INTO target [, ...]",
                    actual: source.to_string(),
                })?
                .iter()
                .map(|target| parse_dynamic_execute_into_target(target))
                .collect::<Result<Vec<_>, _>>()?;
            rest = next_clause
                .map(|index| rest[index..].trim_start())
                .unwrap_or("");
        } else if keyword_at(rest, 0, "using") {
            if !using_exprs.is_empty() {
                return Err(ParseError::UnexpectedToken {
                    expected: "EXECUTE <query> USING expr [, ...]",
                    actual: source.to_string(),
                });
            }
            rest = rest["using".len()..].trim_start();
            let next_clause = find_next_top_level_keyword(rest, &["into", "using"]);
            let using_sql = next_clause
                .map(|index| rest[..index].trim())
                .unwrap_or_else(|| rest.trim());
            if using_sql.is_empty() {
                return Err(ParseError::UnexpectedToken {
                    expected: "EXECUTE <query> USING expr [, ...]",
                    actual: source.to_string(),
                });
            }
            using_exprs =
                split_top_level_csv(using_sql).ok_or_else(|| ParseError::UnexpectedToken {
                    expected: "EXECUTE <query> USING expr [, ...]",
                    actual: source.to_string(),
                })?;
            rest = next_clause
                .map(|index| rest[index..].trim_start())
                .unwrap_or("");
        } else {
            return Err(ParseError::UnexpectedToken {
                expected: "EXECUTE <query> INTO target [, ...] USING expr [, ...]",
                actual: source.to_string(),
            });
        }
    }

    Ok((sql_expr.to_string(), strict, into_targets, using_exprs))
}

fn parse_dynamic_execute_into_target(target: &str) -> Result<AssignTarget, ParseError> {
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
                expected: "PL/pgSQL EXECUTE INTO target",
                actual: trimmed.into(),
            }),
        },
        _ => Err(ParseError::UnexpectedToken {
            expected: "PL/pgSQL EXECUTE INTO target",
            actual: trimmed.into(),
        }),
    }
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

fn find_top_level_cursor_arg_assignment(sql: &str) -> Option<(usize, usize)> {
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
            return None;
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
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            ':' if depth == 0 && bracket_depth == 0 && bytes.get(idx + 1) == Some(&b'=') => {
                return Some((idx, 2));
            }
            '=' if depth == 0 && bracket_depth == 0 && bytes.get(idx + 1) == Some(&b'>') => {
                return Some((idx, 2));
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

    fn unline(stmt: &Stmt) -> &Stmt {
        match stmt {
            Stmt::WithLine { stmt, .. } => stmt,
            stmt => stmt,
        }
    }

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
        assert_eq!(block.label, None);
        let Decl::Var(total_decl) = &block.declarations[0] else {
            panic!("expected variable declaration");
        };
        assert_eq!(total_decl.name, "total");
        assert_eq!(total_decl.ty.kind, SqlTypeKind::Int4);
        assert_eq!(block.statements.len(), 3);
    }

    #[test]
    fn parse_block_accepts_empty_declare_section() {
        let block = parse_block(
            "
            declare
            begin
                null;
            end
            ",
        )
        .unwrap();

        assert!(block.declarations.is_empty());
        assert_eq!(block.statements.len(), 1);
        assert!(matches!(unline(&block.statements[0]), Stmt::Null));
    }

    #[test]
    fn parse_block_accepts_label_and_percent_declarations() {
        let block = parse_block(
            "
            <<outer>>
            declare
                xname HSlot.slotname%TYPE;
                syrow System%ROWTYPE;
            begin
                null;
            end
            ",
        )
        .unwrap();

        assert_eq!(block.label.as_deref(), Some("outer"));
        assert_eq!(block.declarations.len(), 2);
        let Decl::Var(type_decl) = &block.declarations[0] else {
            panic!("expected variable declaration");
        };
        assert_eq!(type_decl.type_name, "HSlot.slotname%TYPE");
        let Decl::Var(row_decl) = &block.declarations[1] else {
            panic!("expected variable declaration");
        };
        assert_eq!(row_decl.type_name, "System%ROWTYPE");
    }

    #[test]
    fn parse_constant_var_declaration() {
        let block = parse_block(
            "
            declare
                rc constant refcursor := 'my_cursor_name';
            begin
                null;
            end
            ",
        )
        .unwrap();

        let Decl::Var(decl) = &block.declarations[0] else {
            panic!("expected variable declaration");
        };
        assert_eq!(decl.name, "rc");
        assert!(decl.constant);
        assert_eq!(decl.type_name, "refcursor");
        assert_eq!(decl.default_expr.as_deref(), Some("'my_cursor_name'"));
    }

    #[test]
    fn parse_not_null_var_declaration() {
        let block = parse_block(
            "
            declare
                i integer not null := 0;
            begin
                null;
            end
            ",
        )
        .unwrap();

        let Decl::Var(decl) = &block.declarations[0] else {
            panic!("expected variable declaration");
        };
        assert_eq!(decl.name, "i");
        assert_eq!(decl.type_name, "integer");
        assert!(decl.strict);
        assert_eq!(decl.default_expr.as_deref(), Some("0"));
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
        } = unline(&block.statements[0])
        else {
            panic!("expected raise statement");
        };
        assert_eq!(message.as_deref(), Some("trigger = %, new table = %"));
        assert_eq!(
            params,
            &vec![
                "TG_NAME".to_string(),
                "(select string_agg(new_table::text, ', ' order by a) from new_table)".to_string(),
            ]
        );
    }

    #[test]
    fn parse_exception_sqlstate_condition() {
        let block = parse_block(
            "
            begin
                perform 1/0;
            exception
                when sqlstate '22012' then
                    null;
            end
            ",
        )
        .unwrap();

        assert_eq!(block.exception_handlers.len(), 1);
        assert_eq!(
            block.exception_handlers[0].conditions,
            vec![ExceptionCondition::SqlState("22012".into())]
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
            ..
        } = unline(&block.statements[0])
        else {
            panic!("expected first RAISE statement");
        };
        assert!(matches!(level, RaiseLevel::Exception));
        assert_eq!(message.as_deref(), Some("Patchfield \"%\" does not exist"));
        assert_eq!(params, &vec!["ps.pfname".to_string()]);

        let Stmt::Raise {
            message, params, ..
        } = unline(&block.statements[1])
        else {
            panic!("expected second RAISE statement");
        };
        assert_eq!(message.as_deref(), Some("system \"%\" does not exist"));
        assert_eq!(params, &vec!["new.sysname".to_string()]);
    }

    #[test]
    fn parse_raise_info_level() {
        let block = parse_block(
            r#"
            begin
                raise info 'r = %', true;
            end
            "#,
        )
        .unwrap();

        let Stmt::Raise {
            level,
            message,
            params,
            ..
        } = unline(&block.statements[0])
        else {
            panic!("expected RAISE statement");
        };
        assert!(matches!(level, RaiseLevel::Info));
        assert_eq!(message.as_deref(), Some("r = %"));
        assert_eq!(params, &vec!["true".to_string()]);
    }

    #[test]
    fn parse_raise_using_forms_and_reraise() {
        let block = parse_block(
            r#"
            begin
                raise 'check me'
                    using errcode = '1234F', detail = 'some detail';
                raise notice 'value %', v using hint = 'look here';
                raise using message = 'custom' || ' message', errcode = '22012';
                raise exception using
                    column = 'c',
                    constraint = 'k',
                    datatype = 't',
                    table = 'r',
                    schema = 's';
                raise;
            end
            "#,
        )
        .unwrap();

        assert!(matches!(
            unline(&block.statements[0]),
            Stmt::Raise {
                message: Some(message),
                using_options,
                ..
            } if message == "check me" && using_options.len() == 2
        ));
        assert!(matches!(
            unline(&block.statements[1]),
            Stmt::Raise {
                message: Some(message),
                params,
                using_options,
                ..
            } if message == "value %" && params == &vec!["v".to_string()] && using_options.len() == 1
        ));
        assert!(matches!(
            unline(&block.statements[2]),
            Stmt::Raise {
                message: None,
                using_options,
                ..
            } if using_options.len() == 2
        ));
        assert!(matches!(
            unline(&block.statements[3]),
            Stmt::Raise {
                using_options, ..
            } if using_options.len() == 5
        ));
        assert!(matches!(
            unline(&block.statements[4]),
            Stmt::Raise {
                message: None,
                using_options,
                params,
                ..
            } if using_options.is_empty() && params.is_empty()
        ));
    }

    #[test]
    fn parse_compiler_directives_before_block() {
        let block = parse_block(
            "
            #variable_conflict use_variable
            declare
                x int := 1;
            begin
                null;
            end
            ",
        )
        .unwrap();

        assert_eq!(block.declarations.len(), 1);
        assert!(matches!(unline(&block.statements[0]), Stmt::Null));
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

        let Stmt::While { condition, body } = unline(&block.statements[0]) else {
            panic!("expected top-level while statement");
        };
        assert_eq!(condition, "current_value is not null");
        assert_eq!(body.len(), 1);
    }

    #[test]
    fn parse_loop_exit_and_foreach() {
        let block = parse_block(
            "
            begin
                loop
                    exit when not found;
                end loop;
                foreach x, y slice 1 in array vals loop
                    null;
                end loop;
            end
            ",
        )
        .unwrap();

        assert!(matches!(
            unline(&block.statements[0]),
            Stmt::Loop { body } if body.len() == 1
                && matches!(unline(&body[0]), Stmt::Exit { condition: Some(condition) } if condition == "not found")
        ));
        assert!(matches!(
            unline(&block.statements[1]),
            Stmt::ForEach {
                target: ForTarget::List(targets),
                slice: 1,
                array_expr,
                body,
            } if targets.len() == 2 && array_expr == "vals" && body.len() == 1
        ));
    }

    #[test]
    fn parse_continue_stmt() {
        let block = parse_block(
            "
            begin
                for item in values (1), (2) loop
                    continue;
                end loop;
            end
            ",
        )
        .unwrap();

        let Stmt::ForQuery { body, .. } = unline(&block.statements[0]) else {
            panic!("expected query FOR loop");
        };
        assert!(matches!(unline(&body[0]), Stmt::Continue));
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
        } = unline(&block.statements[0])
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
    fn parse_subscripted_assignment_target() {
        let block = parse_block(
            "
            begin
                x[1] := $1;
            end
            ",
        )
        .unwrap();

        let Stmt::Assign { target, expr, line } = unline(&block.statements[0]) else {
            panic!("expected assignment statement");
        };
        assert_eq!(
            target,
            &AssignTarget::Subscript {
                name: "x".into(),
                subscripts: vec!["1".into()]
            }
        );
        assert_eq!(expr, "$1");
        assert_eq!(*line, 3);
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
        assert!(matches!(unline(&block.statements[0]), Stmt::Block(_)));
    }

    #[test]
    fn parse_exception_handler() {
        let block = parse_block(
            "
            begin
                begin
                    null;
                exception when others then
                    raise notice 'handled';
                end;
            end
            ",
        )
        .unwrap();

        let Stmt::Block(nested) = unline(&block.statements[0]) else {
            panic!("expected nested block statement");
        };
        assert_eq!(nested.exception_handlers.len(), 1);
        assert_eq!(
            nested.exception_handlers[0].conditions,
            vec![ExceptionCondition::Others]
        );
        assert_eq!(nested.exception_handlers[0].statements.len(), 1);
    }

    #[test]
    fn parse_multiple_exception_handlers() {
        let block = parse_block(
            "
            begin
                begin
                    null;
                exception
                    when substring_error then
                        raise notice 'wrong';
                    when division_by_zero then
                        raise notice 'right';
                end;
            end
            ",
        )
        .unwrap();

        let Stmt::Block(nested) = unline(&block.statements[0]) else {
            panic!("expected nested block statement");
        };
        assert_eq!(nested.exception_handlers.len(), 2);
        assert_eq!(
            nested.exception_handlers[0].conditions,
            vec![ExceptionCondition::ConditionName("substring_error".into())]
        );
        assert_eq!(
            nested.exception_handlers[1].conditions,
            vec![ExceptionCondition::ConditionName("division_by_zero".into())]
        );
    }

    #[test]
    fn parse_assert_and_dynamic_execute() {
        let block = parse_block(
            "
            begin
                assert x > 0, 'x must be positive';
                execute format('select %s', '1') into y using x;
                execute 'select * from foo where f1 = $1' using 1 into strict rec;
            end
            ",
        )
        .unwrap();

        assert!(matches!(unline(&block.statements[0]), Stmt::Assert { .. }));
        let Stmt::DynamicExecute {
            sql_expr,
            strict,
            into_targets,
            using_exprs,
            ..
        } = unline(&block.statements[1])
        else {
            panic!("expected dynamic EXECUTE statement");
        };
        assert_eq!(sql_expr, "format('select %s', '1')");
        assert!(!strict);
        assert_eq!(into_targets, &vec![AssignTarget::Name("y".into())]);
        assert_eq!(using_exprs, &vec!["x".to_string()]);
        let Stmt::DynamicExecute {
            sql_expr,
            strict,
            into_targets,
            using_exprs,
            ..
        } = unline(&block.statements[2])
        else {
            panic!("expected dynamic EXECUTE statement");
        };
        assert_eq!(sql_expr, "'select * from foo where f1 = $1'");
        assert!(*strict);
        assert_eq!(into_targets, &vec![AssignTarget::Name("rec".into())]);
        assert_eq!(using_exprs, &vec!["1".to_string()]);
    }

    #[test]
    fn parse_keyword_named_assignment_target() {
        let block = parse_block(
            "
            begin
                return := return + 1;
                return return;
            end
            ",
        )
        .unwrap();

        let Stmt::Assign { target, expr, .. } = unline(&block.statements[0]) else {
            panic!("expected assignment statement");
        };
        assert_eq!(target, &AssignTarget::Name("return".into()));
        assert_eq!(expr, "return + 1");
        assert!(matches!(unline(&block.statements[1]), Stmt::Return { .. }));
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
        assert!(matches!(unline(&block.statements[0]), Stmt::ExecSql { .. }));
        assert!(matches!(unline(&block.statements[1]), Stmt::ExecSql { .. }));
        assert!(matches!(unline(&block.statements[2]), Stmt::Perform { .. }));
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
        } = unline(&block.statements[0])
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
    fn parse_static_query_for_loop_with_many_parenthesized_values() {
        let block = parse_block(
            "
            begin
                for objtype in values
                    ('table'), ('index'), ('sequence'), ('view'),
                    ('materialized view'), ('foreign table'),
                    ('table column'), ('foreign table column'),
                    ('aggregate'), ('function'), ('procedure'), ('type'), ('cast'),
                    ('table constraint'), ('domain constraint'), ('conversion'), ('default value'),
                    ('operator'), ('operator class'), ('operator family'), ('rule'), ('trigger'),
                    ('text search parser'), ('text search dictionary'),
                    ('text search template'), ('text search configuration'),
                    ('policy'), ('user mapping'), ('default acl'), ('transform'),
                    ('operator of access method'), ('function of access method'),
                    ('publication namespace'), ('publication relation')
                loop
                    null;
                end loop;
            end
            ",
        )
        .unwrap();

        let Stmt::ForQuery { source, body, .. } = unline(&block.statements[0]) else {
            panic!("expected query FOR loop");
        };
        let ForQuerySource::Static(source) = source else {
            panic!("expected static query FOR loop");
        };
        assert!(source.contains("('publication relation')"));
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

        let Stmt::ForQuery { target, source, .. } = unline(&block.statements[0]) else {
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

        let Stmt::ForQuery { target, .. } = unline(&block.statements[0]) else {
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
