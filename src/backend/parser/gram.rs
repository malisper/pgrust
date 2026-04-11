use pest::Parser as _;
use pest::iterators::Pair;
use pest_derive::Parser;

use super::parsenodes::*;
use crate::backend::executor::{AggFunc, Value};

#[derive(Parser)]
#[grammar = "backend/parser/gram.pest"]
struct SqlParser;

pub fn parse_statement(sql: &str) -> Result<Statement, ParseError> {
    SqlParser::parse(Rule::statement, sql)
        .map_err(|e| map_pest_error("statement", e))
        .and_then(|mut pairs| build_statement(pairs.next().ok_or(ParseError::UnexpectedEof)?))
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

fn build_statement(pair: Pair<'_, Rule>) -> Result<Statement, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match inner.as_rule() {
        Rule::explain_stmt => Ok(Statement::Explain(build_explain(inner)?)),
        Rule::select_stmt => Ok(Statement::Select(build_select(inner)?)),
        Rule::analyze_stmt => Ok(Statement::Analyze(build_analyze(inner)?)),
        Rule::set_stmt => Ok(Statement::Set(build_set(inner)?)),
        Rule::reset_stmt => Ok(Statement::Reset(build_reset(inner)?)),
        Rule::show_tables_stmt => Ok(Statement::ShowTables),
        Rule::create_table_stmt => build_create_table(inner),
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
        | Rule::escape_string_literal
        | Rule::dollar_string_literal => decode_string_literal(pair.as_str())?,
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

fn build_reset(pair: Pair<'_, Rule>) -> Result<ResetStatement, ParseError> {
    let mut name = None;
    for part in pair.into_inner() {
        if part.as_rule() == Rule::identifier {
            name = Some(build_identifier(part));
        }
    }
    Ok(ResetStatement { name })
}

fn build_set_value_list(pair: Pair<'_, Rule>) -> String {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::set_value_atom)
        .map(build_set_value_atom)
        .collect::<Vec<_>>()
        .join(", ")
}

fn build_set_value_atom(pair: Pair<'_, Rule>) -> String {
    let pair = pair.clone().into_inner().next().unwrap_or(pair);
    match pair.as_rule() {
        Rule::signed_set_value => pair.as_str().to_string(),
        Rule::quoted_string_literal
        | Rule::string_literal
        | Rule::escape_string_literal
        | Rule::dollar_string_literal => decode_string_literal(pair.as_str()).unwrap_or_default(),
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
    match pair.as_rule() {
        Rule::from_item | Rule::from_primary | Rule::parenthesized_from_item => {
            build_from_item(pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?)
        }
        Rule::from_list => {
            let mut items = pair
                .into_inner()
                .filter(|part| part.as_rule() == Rule::joined_from_item)
                .map(build_from_item);
            let mut item = items.next().ok_or(ParseError::UnexpectedEof)??;
            for next in items {
                item = FromItem::Join {
                    left: Box::new(item),
                    right: Box::new(next?),
                    kind: JoinKind::Cross,
                    on: None,
                };
            }
            Ok(item)
        }
        Rule::joined_from_item => {
            let mut parts = pair.into_inner();
            let mut item = build_from_item(parts.next().ok_or(ParseError::UnexpectedEof)?)?;
            for join_clause in parts {
                let mut right = None;
                let mut on = None;
                for part in join_clause.into_inner() {
                    match part.as_rule() {
                        Rule::aliased_from_item => right = Some(build_from_item(part)?),
                        Rule::expr => on = Some(build_expr(part)?),
                        _ => {}
                    }
                }
                item = FromItem::Join {
                    left: Box::new(item),
                    right: Box::new(right.ok_or(ParseError::UnexpectedEof)?),
                    kind: JoinKind::Inner,
                    on: Some(on.ok_or(ParseError::UnexpectedEof)?),
                };
            }
            Ok(item)
        }
        Rule::aliased_from_item => {
            let mut source = None;
            let mut alias = None;
            let mut column_aliases = Vec::new();
            for part in pair.into_inner() {
                match part.as_rule() {
                    Rule::table_from_item
                    | Rule::values_from_item
                    | Rule::parenthesized_table_from_item
                    | Rule::srf_from_item
                    | Rule::derived_from_item
                    | Rule::parenthesized_from_item
                    | Rule::from_primary => source = Some(build_from_item(part)?),
                    Rule::relation_alias => {
                        let mut identifiers = Vec::new();
                        collect_identifiers(part, &mut identifiers);
                        alias = identifiers.first().cloned();
                        column_aliases = identifiers.into_iter().skip(1).collect();
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
                })
            } else {
                Ok(item)
            }
        }
        Rule::table_from_item | Rule::parenthesized_table_from_item => Ok(FromItem::Table {
            name: build_identifier(
                pair.into_inner()
                    .find(|part| part.as_rule() == Rule::identifier)
                    .ok_or(ParseError::UnexpectedEof)?,
            ),
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
            let mut args = Vec::new();
            for part in pair.into_inner() {
                match part.as_rule() {
                    Rule::identifier if name.is_none() => name = Some(build_identifier(part)),
                    Rule::expr_list => {
                        for expr_pair in part.into_inner() {
                            args.push(build_expr(expr_pair)?);
                        }
                    }
                    _ => {}
                }
            }
            Ok(FromItem::FunctionCall {
                name: name.ok_or(ParseError::UnexpectedEof)?,
                args,
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

fn build_create_table(pair: Pair<'_, Rule>) -> Result<Statement, ParseError> {
    let mut relation_name = None;
    let mut persistence = TablePersistence::Permanent;
    let mut on_commit = OnCommitAction::PreserveRows;
    let mut columns = Vec::new();
    let mut ctas_columns = Vec::new();
    let mut query = None;
    let mut is_ctas = false;
    for part in pair.into_inner() {
        let part = if part.as_rule() == Rule::create_table_tail {
            part.into_inner().next().ok_or(ParseError::UnexpectedEof)?
        } else {
            part
        };
        match part.as_rule() {
            Rule::temp_clause => persistence = TablePersistence::Temporary,
            Rule::identifier if relation_name.is_none() => {
                relation_name = Some(build_relation_name(part))
            }
            Rule::create_table_column_form => {
                for inner in part.into_inner() {
                    match inner.as_rule() {
                        Rule::column_def => columns.push(build_column_def(inner)?),
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
            _ => {}
        }
    }
    let (schema_name, table_name) = relation_name.ok_or(ParseError::UnexpectedEof)?;
    if is_ctas {
        Ok(Statement::CreateTableAs(CreateTableAsStatement {
            schema_name,
            table_name,
            persistence,
            on_commit,
            column_names: ctas_columns,
            query: query.ok_or(ParseError::UnexpectedEof)?,
        }))
    } else {
        Ok(Statement::CreateTable(CreateTableStatement {
            schema_name,
            table_name,
            persistence,
            on_commit,
            columns,
        }))
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
            alias_pair
                .into_inner()
                .last()
                .ok_or(ParseError::UnexpectedEof)?
                .as_str()
                .to_string()
        } else {
            select_item_name(&expr, index)
        };
        items.push(SelectItem { output_name, expr });
    }

    Ok(items)
}

fn select_item_name(expr: &SqlExpr, index: usize) -> String {
    match expr {
        SqlExpr::Column(name) => name.rsplit('.').next().unwrap_or(name).to_string(),
        SqlExpr::Cast(inner, _) => select_item_name(inner, index),
        SqlExpr::AggCall { func, .. } => func.name().to_string(),
        SqlExpr::Random => "random".to_string(),
        SqlExpr::FuncCall { name, .. } => name.clone(),
        SqlExpr::IntegerLiteral(_) | SqlExpr::NumericLiteral(_) => format!("expr{}", index + 1),
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
    match pair.as_rule() {
        Rule::type_name => {
            let mut inner = pair.into_inner();
            let base = build_type(inner.next().expect("type_name base"));
            if inner.next().is_some() {
                SqlType::array_of(base)
            } else {
                base
            }
        }
        Rule::base_type_name => build_type(pair.into_inner().next().expect("base_type_name inner")),
        Rule::kw_int2 | Rule::kw_smallint => SqlType::new(SqlTypeKind::Int2),
        Rule::kw_int4 | Rule::kw_int | Rule::kw_integer => SqlType::new(SqlTypeKind::Int4),
        Rule::kw_int8 | Rule::kw_bigint => SqlType::new(SqlTypeKind::Int8),
        Rule::kw_float4 | Rule::kw_real => SqlType::new(SqlTypeKind::Float4),
        Rule::kw_float8 | Rule::double_precision_type => SqlType::new(SqlTypeKind::Float8),
        Rule::numeric_type => {
            let dims = pair
                .into_inner()
                .filter(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .collect::<Result<Vec<_>, _>>()
                .expect("numeric precision/scale");
            match dims.as_slice() {
                [] => SqlType::new(SqlTypeKind::Numeric),
                [precision] => SqlType::with_numeric_precision_scale(*precision, 0),
                [precision, scale] => SqlType::with_numeric_precision_scale(*precision, *scale),
                _ => unreachable!("unexpected numeric typmod arity"),
            }
        }
        Rule::kw_text => SqlType::new(SqlTypeKind::Text),
        Rule::kw_json => SqlType::new(SqlTypeKind::Json),
        Rule::kw_jsonb => SqlType::new(SqlTypeKind::Jsonb),
        Rule::kw_jsonpath => SqlType::new(SqlTypeKind::JsonPath),
        Rule::kw_bool | Rule::kw_boolean => SqlType::new(SqlTypeKind::Bool),
        Rule::kw_timestamp => SqlType::new(SqlTypeKind::Timestamp),
        Rule::char_type => {
            let len = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()
                .expect("char length");
            match len {
                Some(len) => SqlType::with_char_len(SqlTypeKind::Char, len),
                None => SqlType::new(SqlTypeKind::Char),
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
                Some(len) => SqlType::with_char_len(SqlTypeKind::Varchar, len),
                None => SqlType::new(SqlTypeKind::Varchar),
            }
        }
        _ => unreachable!("unexpected type rule {:?}", pair.as_rule()),
    }
}

fn build_type_len(pair: Pair<'_, Rule>) -> Result<i32, ParseError> {
    pair.as_str()
        .parse::<i32>()
        .map_err(|_| ParseError::InvalidInteger(pair.as_str().to_string()))
}

fn build_identifier(pair: Pair<'_, Rule>) -> String {
    pair.as_str().to_string()
}

pub(crate) fn build_expr(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    match pair.as_rule() {
        Rule::expr
        | Rule::or_expr
        | Rule::and_expr
        | Rule::concat_expr
        | Rule::add_expr
        | Rule::mul_expr => {
            let mut inner = pair.into_inner();
            let first = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            fold_infix(first, inner)
        }
        Rule::postfix_expr => {
            let mut inner = pair.into_inner();
            let mut expr = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            for suffix in inner {
                if suffix.as_rule() == Rule::cast_suffix {
                    let ty = build_type(
                        suffix
                            .into_inner()
                            .find(|part| part.as_rule() == Rule::type_name)
                            .ok_or(ParseError::UnexpectedEof)?,
                    );
                    expr = SqlExpr::Cast(Box::new(expr), ty);
                }
            }
            Ok(expr)
        }
        Rule::unary_expr => build_expr(pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?),
        Rule::positive_expr => Ok(SqlExpr::UnaryPlus(Box::new(build_expr(
            pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?,
        )?))),
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
                Rule::comp_op => {
                    let right = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
                    Ok(match next.as_str() {
                        "@>" => SqlExpr::JsonbContains(Box::new(left), Box::new(right)),
                        "<@" => SqlExpr::JsonbContained(Box::new(left), Box::new(right)),
                        "@?" => SqlExpr::JsonbPathExists(Box::new(left), Box::new(right)),
                        "@@" => SqlExpr::JsonbPathMatch(Box::new(left), Box::new(right)),
                        "?" => SqlExpr::JsonbExists(Box::new(left), Box::new(right)),
                        "?|" => SqlExpr::JsonbExistsAny(Box::new(left), Box::new(right)),
                        "?&" => SqlExpr::JsonbExistsAll(Box::new(left), Box::new(right)),
                        "&&" => SqlExpr::ArrayOverlap(Box::new(left), Box::new(right)),
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
        Rule::agg_call => build_agg_call(pair),
        Rule::func_call => {
            let mut inner = pair.into_inner();
            let name = build_identifier(inner.next().ok_or(ParseError::UnexpectedEof)?);
            let args = inner
                .find(|part| part.as_rule() == Rule::expr_list)
                .map(|list| {
                    list.into_inner()
                        .filter(|part| part.as_rule() == Rule::expr)
                        .map(build_expr)
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?
                .unwrap_or_default();
            if name.eq_ignore_ascii_case("random") && args.is_empty() {
                Ok(SqlExpr::Random)
            } else {
                Ok(SqlExpr::FuncCall { name, args })
            }
        }
        Rule::typed_string_literal => {
            let mut inner = pair.into_inner();
            let ty = build_type(inner.next().ok_or(ParseError::UnexpectedEof)?);
            let literal = decode_string_literal(
                inner
                    .next()
                    .ok_or(ParseError::UnexpectedEof)?
                    .as_str(),
            )?;
            Ok(SqlExpr::Cast(
                Box::new(SqlExpr::Const(Value::Text(literal.into()))),
                ty,
            ))
        }
        Rule::identifier => Ok(SqlExpr::Column(pair.as_str().to_string())),
        Rule::numeric_literal => Ok(SqlExpr::NumericLiteral(pair.as_str().to_string())),
        Rule::integer => Ok(SqlExpr::IntegerLiteral(pair.as_str().to_string())),
        Rule::quoted_string_literal
        | Rule::string_literal
        | Rule::escape_string_literal
        | Rule::dollar_string_literal => Ok(SqlExpr::Const(Value::Text(
            decode_string_literal(pair.as_str())?.into(),
        ))),
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
    let mut args = Vec::new();
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
            Rule::expr_list => {
                args = part
                    .into_inner()
                    .filter(|part| part.as_rule() == Rule::expr)
                    .map(build_expr)
                    .collect::<Result<Vec<_>, _>>()?;
            }
            _ => {}
        }
    }
    Ok(SqlExpr::AggCall {
        func: func.ok_or(ParseError::UnexpectedEof)?,
        args: if is_star { Vec::new() } else { args },
        distinct,
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
            Rule::add_op => match op.as_str() {
                "+" => SqlExpr::Add(Box::new(expr), Box::new(rhs)),
                "-" => SqlExpr::Sub(Box::new(expr), Box::new(rhs)),
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
                let code = collect_escape_digits(&mut chars, 4, raw)?;
                let ch = char::from_u32(code).ok_or(ParseError::UnexpectedToken {
                    expected: "valid unicode escape",
                    actual: raw.into(),
                })?;
                out.push(ch);
            }
            'U' => {
                let code = collect_escape_digits(&mut chars, 8, raw)?;
                let ch = char::from_u32(code).ok_or(ParseError::UnexpectedToken {
                    expected: "valid unicode escape",
                    actual: raw.into(),
                })?;
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
                let value = u8::from_str_radix(&digits, 8).map_err(|_| ParseError::UnexpectedToken {
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
    raw: &str,
) -> Result<u32, ParseError> {
    let mut digits = String::with_capacity(len);
    for _ in 0..len {
        digits.push(chars.next().ok_or(ParseError::UnexpectedEof)?);
    }
    u32::from_str_radix(&digits, 16).map_err(|_| ParseError::UnexpectedToken {
        expected: "valid unicode escape",
        actual: raw.into(),
    })
}

fn decode_dollar_string(raw: &str) -> Result<String, ParseError> {
    let end_tag_start = raw[1..]
        .find('$')
        .map(|idx| idx + 1)
        .ok_or(ParseError::UnexpectedEof)?;
    let tag = &raw[..=end_tag_start];
    let suffix = &raw[end_tag_start + 1..];
    let closing = suffix
        .rfind(tag)
        .ok_or(ParseError::UnexpectedToken {
            expected: "matching dollar-quote terminator",
            actual: raw.into(),
        })?;
    Ok(suffix[..closing].to_string())
}
