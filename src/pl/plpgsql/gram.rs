use pest::Parser as _;
use pest::iterators::Pair;
use pest_derive::Parser;

use crate::backend::executor::Value;
use crate::backend::parser::{ParseError, SqlExpr, parse_expr, parse_type_name};

use super::ast::{Block, RaiseLevel, ReturnQueryKind, Stmt, VarDecl};

#[derive(Parser)]
#[grammar = "pl/plpgsql/gram.pest"]
struct PlpgsqlParser;

pub fn parse_block(sql: &str) -> Result<Block, ParseError> {
    PlpgsqlParser::parse(Rule::pl_block, sql)
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

fn build_declare_section(pair: Pair<'_, Rule>) -> Result<Vec<VarDecl>, ParseError> {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::var_decl)
        .map(build_var_decl)
        .collect()
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
                    crate::backend::parser::RawTypeName::Record => {
                        return Err(ParseError::UnsupportedType("record".into()));
                    }
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
        Rule::for_int_stmt => build_for_stmt(inner),
        Rule::raise_stmt => build_raise_stmt(inner),
        Rule::return_stmt => build_return_stmt(inner),
        Rule::return_next_stmt => build_return_next_stmt(inner),
        Rule::return_query_stmt => build_return_query_stmt(inner),
        _ => Err(ParseError::UnexpectedToken {
            expected: "plpgsql statement",
            actual: inner.as_str().into(),
        }),
    }
}

fn build_assign_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let mut name = None;
    let mut expr = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::ident => name = Some(build_ident(part)),
            Rule::expr_until_semi => expr = Some(part.as_str().trim().to_string()),
            _ => {}
        }
    }
    Ok(Stmt::Assign {
        name: name.ok_or(ParseError::UnexpectedEof)?,
        expr: expr.ok_or(ParseError::UnexpectedEof)?,
    })
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
    let mut var_name = None;
    let mut start_expr = None;
    let mut end_expr = None;
    let mut body = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::ident if var_name.is_none() => var_name = Some(build_ident(part)),
            Rule::expr_until_range => start_expr = Some(part.as_str().trim().to_string()),
            Rule::expr_until_loop => end_expr = Some(part.as_str().trim().to_string()),
            Rule::stmt => body.push(build_stmt(part)?),
            _ => {}
        }
    }
    Ok(Stmt::ForInt {
        var_name: var_name.ok_or(ParseError::UnexpectedEof)?,
        start_expr: start_expr.ok_or(ParseError::UnexpectedEof)?,
        end_expr: end_expr.ok_or(ParseError::UnexpectedEof)?,
        body,
    })
}

fn build_raise_stmt(pair: Pair<'_, Rule>) -> Result<Stmt, ParseError> {
    let mut level = RaiseLevel::Exception;
    let mut message = None;
    let mut params = Vec::new();
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
            Rule::expr_until_comma_or_semi => params.push(part.as_str().trim().to_string()),
            _ => {}
        }
    }
    Ok(Stmt::Raise {
        level,
        message: message.ok_or(ParseError::UnexpectedEof)?,
        params,
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

fn build_ident(pair: Pair<'_, Rule>) -> String {
    let raw = pair.as_str();
    if raw.starts_with('"') && raw.ends_with('"') {
        raw[1..raw.len() - 1].replace("\"\"", "\"")
    } else {
        raw.to_ascii_lowercase()
    }
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
        assert_eq!(block.declarations[0].name, "total");
        assert_eq!(block.declarations[0].ty.kind, SqlTypeKind::Int4);
        assert_eq!(block.statements.len(), 3);
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
    fn reject_query_style_for_loops_quickly() {
        let err = parse_block(
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
        .unwrap_err();
        assert!(matches!(err, ParseError::UnexpectedToken { .. }));
    }
}
