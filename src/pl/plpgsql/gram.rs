use pest::Parser as _;
use pest::iterators::Pair;
use pest_derive::Parser;

use crate::backend::executor::Value;
use crate::backend::parser::{ParseError, SqlExpr, SqlType, parse_expr, parse_type_name};
use crate::include::catalog::RECORD_TYPE_OID;

use super::ast::{
    AliasDecl, AssignTarget, Block, Decl, RaiseLevel, ReturnQueryKind, Stmt, VarDecl,
};

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
    let mut param_index = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::ident => name = Some(build_ident(part)),
            Rule::positional_param => {
                let raw = part.as_str();
                param_index =
                    Some(
                        raw[1..]
                            .parse::<usize>()
                            .map_err(|_| ParseError::UnexpectedToken {
                                expected: "valid positional parameter reference",
                                actual: raw.into(),
                            })?,
                    );
            }
            _ => {}
        }
    }
    Ok(AliasDecl {
        name: name.ok_or(ParseError::UnexpectedEof)?,
        param_index: param_index.ok_or(ParseError::UnexpectedEof)?,
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
        Rule::while_stmt => build_while_stmt(inner),
        Rule::for_int_stmt => build_for_stmt(inner),
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
