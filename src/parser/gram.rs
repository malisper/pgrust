use pest::Parser as _;
use pest::iterators::Pair;
use pest_derive::Parser;

use crate::executor::{AggFunc, Value};
use super::parsenodes::*;

#[derive(Parser)]
#[grammar = "parser/sql.pest"]
struct SqlParser;

pub fn parse_statement(sql: &str) -> Result<Statement, ParseError> {
    SqlParser::parse(Rule::statement, sql)
        .map_err(|e| map_pest_error("statement", e))
        .and_then(|mut pairs| build_statement(pairs.next().ok_or(ParseError::UnexpectedEof)?))
}

#[cfg(test)]
pub(crate) fn pest_parse_keyword(rule: Rule, input: &str) -> Result<String, ParseError> {
    let mut pairs = SqlParser::parse(rule, input)
        .map_err(|e| map_pest_error("keyword", e))?;
    Ok(pairs.next().ok_or(ParseError::UnexpectedEof)?.as_str().to_string())
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

fn build_statement(pair: Pair<'_, Rule>) -> Result<Statement, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match inner.as_rule() {
        Rule::explain_stmt => Ok(Statement::Explain(build_explain(inner)?)),
        Rule::select_stmt => Ok(Statement::Select(build_select(inner)?)),
        Rule::show_tables_stmt => Ok(Statement::ShowTables),
        Rule::create_table_stmt => Ok(Statement::CreateTable(build_create_table(inner)?)),
        Rule::drop_table_stmt => Ok(Statement::DropTable(build_drop_table(inner)?)),
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

fn build_explain(pair: Pair<'_, Rule>) -> Result<ExplainStatement, ParseError> {
    let mut analyze = false;
    let mut buffers = false;
    let mut timing = true; // default on, like PostgreSQL
    let mut statement = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::kw_analyze => analyze = true,
            Rule::explain_option => {
                let mut inner = part.into_inner();
                let opt = inner.next().ok_or(ParseError::UnexpectedEof)?;
                match opt.as_rule() {
                    Rule::kw_analyze => analyze = true,
                    Rule::kw_buffers => buffers = true,
                    Rule::kw_timing => timing = false, // TIMING OFF
                    _ => {}
                }
            },
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
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match inner.as_rule() {
        Rule::table_from_item => Ok(FromItem::Table(
            inner
                .into_inner()
                .find(|part| part.as_rule() == Rule::identifier)
                .map(build_identifier)
                .ok_or(ParseError::UnexpectedEof)?,
        )),
        Rule::cross_from_item => {
            let identifiers = inner
                .into_inner()
                .filter(|part| part.as_rule() == Rule::identifier)
                .map(build_identifier)
                .collect::<Vec<_>>();
            match identifiers.as_slice() {
                [left_table, right_table] => Ok(FromItem::CrossJoin {
                    left_table: left_table.clone(),
                    right_table: right_table.clone(),
                }),
                _ => Err(ParseError::UnexpectedToken {
                    expected: "cross join from clause",
                    actual: raw,
                }),
            }
        }
        Rule::joined_from_item => {
            let mut identifiers = Vec::new();
            let mut on = None;
            for part in inner.into_inner() {
                match part.as_rule() {
                    Rule::identifier => identifiers.push(build_identifier(part)),
                    Rule::expr => on = Some(build_expr(part)?),
                    _ => {}
                }
            }
            match identifiers.as_slice() {
                [left_table, right_table] => Ok(FromItem::InnerJoin {
                    left_table: left_table.clone(),
                    right_table: right_table.clone(),
                    on: on.ok_or(ParseError::UnexpectedEof)?,
                }),
                _ => Err(ParseError::UnexpectedToken {
                    expected: "joined from clause",
                    actual: raw,
                }),
            }
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "from clause",
            actual: raw,
        }),
    }
}

fn build_insert(pair: Pair<'_, Rule>) -> Result<InsertStatement, ParseError> {
    let mut table_name = None;
    let mut columns = None;
    let mut values = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::ident_list => {
                columns = Some(part.into_inner().map(build_identifier).collect::<Vec<_>>())
            }
            Rule::values_row => values.push(build_values_row(part)?),
            _ => {}
        }
    }
    Ok(InsertStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        columns,
        values,
    })
}

fn build_create_table(pair: Pair<'_, Rule>) -> Result<CreateTableStatement, ParseError> {
    let mut table_name = None;
    let mut columns = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::column_def => columns.push(build_column_def(part)?),
            _ => {}
        }
    }
    Ok(CreateTableStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        columns,
    })
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

fn build_truncate_table(pair: Pair<'_, Rule>) -> Result<TruncateTableStatement, ParseError> {
    let table_names = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::ident_list)
        .map(|part| part.into_inner().map(build_identifier).collect::<Vec<_>>())
        .ok_or(ParseError::UnexpectedEof)?;
    Ok(TruncateTableStatement { table_names })
}

fn build_vacuum(pair: Pair<'_, Rule>) -> Result<VacuumStatement, ParseError> {
    let table_names = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::ident_list)
        .map(|part| part.into_inner().map(build_identifier).collect::<Vec<_>>())
        .ok_or(ParseError::UnexpectedEof)?;
    Ok(VacuumStatement { table_names })
}

fn build_update(pair: Pair<'_, Rule>) -> Result<UpdateStatement, ParseError> {
    let mut table_name = None;
    let mut assignments = Vec::new();
    let mut where_clause = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::assignment => assignments.push(build_assignment(part)?),
            Rule::expr => where_clause = Some(build_expr(part)?),
            _ => {}
        }
    }
    Ok(UpdateStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        assignments,
        where_clause,
    })
}

fn build_delete(pair: Pair<'_, Rule>) -> Result<DeleteStatement, ParseError> {
    let mut table_name = None;
    let mut where_clause = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::expr => where_clause = Some(build_expr(part)?),
            _ => {}
        }
    }
    Ok(DeleteStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
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
        let mut item_inner = item_pair.into_inner();
        let expr = build_expr(item_inner.next().ok_or(ParseError::UnexpectedEof)?)?;
        let output_name = if let Some(alias_pair) = item_inner.next() {
            alias_pair.into_inner().last().ok_or(ParseError::UnexpectedEof)?.as_str().to_string()
        } else {
            select_item_name(&expr, index)
        };
        items.push(SelectItem { output_name, expr });
    }

    Ok(items)
}

fn select_item_name(expr: &SqlExpr, index: usize) -> String {
    match expr {
        SqlExpr::Column(name) => name.clone(),
        SqlExpr::AggCall { func, .. } => func.name().to_string(),
        SqlExpr::Random => "random".to_string(),
        _ => format!("expr{}", index + 1),
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
        column: build_identifier(inner.next().ok_or(ParseError::UnexpectedEof)?),
        expr: build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?,
    })
}

fn build_column_def(pair: Pair<'_, Rule>) -> Result<ColumnDef, ParseError> {
    let mut inner = pair.into_inner();
    let name = build_identifier(inner.next().ok_or(ParseError::UnexpectedEof)?);
    let ty = build_type(inner.next().ok_or(ParseError::UnexpectedEof)?);
    let nullable = match inner.next() {
        Some(flag) => flag.as_rule() == Rule::nullable,
        None => true,
    };
    Ok(ColumnDef { name, ty, nullable })
}

fn build_type(pair: Pair<'_, Rule>) -> SqlType {
    match pair.as_str().to_ascii_lowercase().as_str() {
        "int4" | "int" | "integer" => SqlType::Int4,
        "text" => SqlType::Text,
        "bool" | "boolean" => SqlType::Bool,
        "timestamp" => SqlType::Timestamp,
        ty if ty.starts_with("char(") => SqlType::Char,
        _ => unreachable!(),
    }
}

fn build_identifier(pair: Pair<'_, Rule>) -> String {
    pair.as_str().to_string()
}

pub(crate) fn build_expr(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    match pair.as_rule() {
        Rule::expr | Rule::or_expr | Rule::and_expr | Rule::add_expr => {
            let mut inner = pair.into_inner();
            let first = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            fold_infix(first, inner)
        }
        Rule::unary_expr => build_expr(pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?),
        Rule::negated_expr => Ok(SqlExpr::Negate(Box::new(build_expr(
            pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?,
        )?))),
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
                Rule::comp_op => {
                    let right = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
                    Ok(match next.as_str() {
                        "=" => SqlExpr::Eq(Box::new(left), Box::new(right)),
                        "<" => SqlExpr::Lt(Box::new(left), Box::new(right)),
                        ">" => SqlExpr::Gt(Box::new(left), Box::new(right)),
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
        Rule::primary_expr => build_expr(pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?),
        Rule::agg_call => build_agg_call(pair),
        Rule::func_call => Ok(SqlExpr::Random),
        Rule::identifier => Ok(SqlExpr::Column(pair.as_str().to_string())),
        Rule::integer => pair
            .as_str()
            .parse::<i32>()
            .map(|value| SqlExpr::Const(Value::Int32(value)))
            .map_err(|_| ParseError::InvalidInteger(pair.as_str().into())),
        Rule::string_literal => Ok(SqlExpr::Const(Value::Text(unescape_string(pair.as_str()).into()))),
        Rule::kw_null => Ok(SqlExpr::Const(Value::Null)),
        Rule::kw_true => Ok(SqlExpr::Const(Value::Bool(true))),
        Rule::kw_false => Ok(SqlExpr::Const(Value::Bool(false))),
        Rule::kw_current_timestamp => Ok(SqlExpr::CurrentTimestamp),
        _ => Err(ParseError::UnexpectedToken {
            expected: "expression",
            actual: pair.as_str().into(),
        }),
    }
}

fn build_agg_call(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let mut func = None;
    let mut arg = None;
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
                    Rule::kw_min => AggFunc::Min,
                    Rule::kw_max => AggFunc::Max,
                    _ => {
                        return Err(ParseError::UnexpectedToken {
                            expected: "aggregate function",
                            actual: inner.as_str().into(),
                        })
                    }
                });
            }
            Rule::agg_distinct => distinct = true,
            Rule::star => is_star = true,
            Rule::expr => arg = Some(build_expr(part)?),
            _ => {}
        }
    }
    Ok(SqlExpr::AggCall {
        func: func.ok_or(ParseError::UnexpectedEof)?,
        arg: if is_star {
            None
        } else {
            Some(Box::new(arg.ok_or(ParseError::UnexpectedEof)?))
        },
        distinct,
    })
}

fn build_null_predicate(
    left: SqlExpr,
    pair: Pair<'_, Rule>,
) -> Result<SqlExpr, ParseError> {
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
            Rule::add_op => SqlExpr::Add(Box::new(expr), Box::new(rhs)),
            _ => unreachable!(),
        };
    }
    Ok(expr)
}

fn unescape_string(raw: &str) -> String {
    raw[1..raw.len() - 1].replace("''", "'")
}
