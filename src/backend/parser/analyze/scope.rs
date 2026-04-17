use super::query::{AnalyzedFrom, JoinAliasInfo, shift_expr_rtindexes};
use super::*;
use crate::backend::storage::smgr::RelFileLocator;
use crate::include::nodes::primnodes::{JoinType, Var, user_attrno};

#[derive(Debug, Clone)]
pub(crate) struct BoundScope {
    pub(crate) desc: RelationDesc,
    pub(crate) output_exprs: Vec<Expr>,
    pub(crate) columns: Vec<ScopeColumn>,
    pub(crate) relations: Vec<ScopeRelation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScopeColumn {
    pub(crate) output_name: String,
    pub(crate) hidden: bool,
    pub(crate) relation_names: Vec<String>,
    pub(crate) hidden_invalid_relation_names: Vec<String>,
    pub(crate) hidden_missing_relation_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScopeRelation {
    pub(crate) relation_names: Vec<String>,
    pub(crate) hidden_invalid_relation_names: Vec<String>,
    pub(crate) hidden_missing_relation_names: Vec<String>,
    pub(crate) system_varno: Option<usize>,
}

#[derive(Debug, Clone)]
pub(crate) struct GroupedOuterScope {
    pub(crate) scope: BoundScope,
    pub(crate) group_by_exprs: Vec<SqlExpr>,
}

#[derive(Debug, Clone)]
pub(crate) struct BoundCte {
    pub(crate) name: String,
    pub(crate) cte_id: usize,
    pub(crate) plan: Query,
    pub(crate) desc: RelationDesc,
    pub(crate) self_reference: bool,
    pub(crate) worktable_id: usize,
}

#[derive(Debug, Clone)]
pub struct BoundRelation {
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub toast: Option<ToastRelationRef>,
    pub namespace_oid: u32,
    pub owner_oid: u32,
    pub relpersistence: char,
    pub relkind: char,
    pub desc: RelationDesc,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum ResolvedColumn {
    Local(usize),
    Outer { depth: usize, index: usize },
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ResolvedSystemColumn {
    pub(super) varno: usize,
    pub(super) varlevelsup: usize,
    pub(super) sql_type: SqlType,
}

pub(super) fn empty_scope() -> BoundScope {
    BoundScope {
        desc: RelationDesc {
            columns: Vec::new(),
        },
        output_exprs: Vec::new(),
        columns: Vec::new(),
        relations: Vec::new(),
    }
}

fn default_scope_output_exprs(varno: usize, desc: &RelationDesc) -> Vec<Expr> {
    desc.columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            Expr::Var(Var {
                varno,
                varattno: user_attrno(index),
                varlevelsup: 0,
                vartype: column.sql_type,
            })
        })
        .collect()
}

pub(super) fn bind_values_rows(
    rows: &[Vec<SqlExpr>],
    column_names: Option<&[String]>,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(AnalyzedFrom, BoundScope), ParseError> {
    let width = rows.first().map(Vec::len).unwrap_or(0);
    for row in rows {
        if row.len() != width {
            return Err(ParseError::UnexpectedToken {
                expected: "VALUES rows with consistent column counts",
                actual: format!("VALUES row has {} columns, expected {width}", row.len()),
            });
        }
    }

    if let Some(column_names) = column_names
        && column_names.len() != width
    {
        return Err(ParseError::UnexpectedToken {
            expected: "VALUES column alias count matching VALUES width",
            actual: format!(
                "VALUES has {width} columns but {} column aliases were specified",
                column_names.len()
            ),
        });
    }

    let empty = empty_scope();
    let mut column_types = Vec::with_capacity(width);
    for col_idx in 0..width {
        let mut common = None;
        let mut common_expr: Option<&SqlExpr> = None;
        for row in rows {
            let inferred = infer_sql_expr_type_with_ctes(
                &row[col_idx],
                &empty,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            common = Some(match common {
                None => {
                    common_expr = Some(&row[col_idx]);
                    inferred.element_type()
                }
                Some(existing) => {
                    let existing = coerce_unknown_string_literal_type(
                        common_expr.expect("common expr"),
                        existing,
                        inferred,
                    );
                    let adjusted =
                        coerce_unknown_string_literal_type(&row[col_idx], inferred, existing);
                    let resolved =
                        resolve_common_scalar_type(existing, adjusted).ok_or_else(|| {
                            ParseError::UnexpectedToken {
                                expected: "VALUES columns with a common type",
                                actual: format!(
                                    "VALUES column {} cannot reconcile {} and {}",
                                    col_idx + 1,
                                    sql_type_name(existing),
                                    sql_type_name(adjusted)
                                ),
                            }
                        })?;
                    common_expr = Some(&row[col_idx]);
                    resolved
                }
            });
        }
        column_types.push(common.unwrap_or(SqlType::new(SqlTypeKind::Text)));
    }

    let bound_rows = rows
        .iter()
        .map(|row| {
            row.iter()
                .zip(column_types.iter())
                .map(|(expr, ty)| {
                    let from = infer_sql_expr_type_with_ctes(
                        expr,
                        &empty,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                    Ok(coerce_bound_expr(
                        bind_expr_with_outer_and_ctes(
                            expr,
                            &empty,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        )?,
                        from,
                        *ty,
                    ))
                })
                .collect::<Result<Vec<_>, ParseError>>()
        })
        .collect::<Result<Vec<_>, _>>()?;

    let output_columns = column_types
        .iter()
        .enumerate()
        .map(|(idx, ty)| QueryColumn {
            name: column_names
                .and_then(|names| names.get(idx))
                .cloned()
                .unwrap_or_else(|| format!("column{}", idx + 1)),
            sql_type: *ty,
        })
        .collect::<Vec<_>>();
    let desc = RelationDesc {
        columns: output_columns
            .iter()
            .map(|col| column_desc(col.name.clone(), col.sql_type, true))
            .collect(),
    };
    Ok((
        AnalyzedFrom::values(bound_rows, output_columns),
        scope_for_relation(None, &desc),
    ))
}

pub(super) fn resolve_column(scope: &BoundScope, name: &str) -> Result<usize, ParseError> {
    if name == "*" {
        return Err(ParseError::UnexpectedToken {
            expected: "named column",
            actual: "*".into(),
        });
    }
    if let Some((relation, column_name)) = name.rsplit_once('.') {
        let mut matches = scope.columns.iter().enumerate().filter(|(_, column)| {
            !column.hidden
                && column
                    .relation_names
                    .iter()
                    .any(|visible_relation| visible_relation.eq_ignore_ascii_case(relation))
                && column.output_name.eq_ignore_ascii_case(column_name)
        });
        if let Some(first) = matches.next() {
            if matches.next().is_some() {
                return Err(ParseError::AmbiguousColumn(name.to_string()));
            }
            return Ok(first.0);
        }
        let normalized_relation = relation.to_ascii_lowercase();
        if scope.columns.iter().any(|column| {
            column
                .hidden_invalid_relation_names
                .iter()
                .any(|hidden| hidden.eq_ignore_ascii_case(relation))
                && column.output_name.eq_ignore_ascii_case(column_name)
        }) {
            return Err(ParseError::InvalidFromClauseReference(normalized_relation));
        }
        if scope.columns.iter().any(|column| {
            column
                .hidden_missing_relation_names
                .iter()
                .any(|hidden| hidden.eq_ignore_ascii_case(relation))
                && column.output_name.eq_ignore_ascii_case(column_name)
        }) {
            return Err(ParseError::MissingFromClauseEntry(normalized_relation));
        }
        if scope.columns.iter().any(|column| {
            column
                .relation_names
                .iter()
                .chain(column.hidden_invalid_relation_names.iter())
                .chain(column.hidden_missing_relation_names.iter())
                .any(|known| known.eq_ignore_ascii_case(relation))
        }) {
            return Err(ParseError::UnknownColumn(name.to_string()));
        }
        return Err(ParseError::UnknownColumn(name.to_string()));
    }

    let mut matches = scope
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| !column.hidden && column.output_name.eq_ignore_ascii_case(name));
    let Some(first) = matches.next() else {
        let mut relation_matches = scope.columns.iter().enumerate().filter(|(_, column)| {
            !column.hidden
                && column
                    .relation_names
                    .iter()
                    .any(|relation| relation.eq_ignore_ascii_case(name))
        });
        let Some((index, _)) = relation_matches.next() else {
            return Err(ParseError::UnknownColumn(name.to_string()));
        };
        if relation_matches.next().is_some() || scope.columns.len() != 1 {
            return Err(ParseError::UnknownColumn(name.to_string()));
        }
        return Ok(index);
    };
    if matches.next().is_some() {
        return Err(ParseError::AmbiguousColumn(name.to_string()));
    }
    Ok(first.0)
}

fn resolve_system_column_in_scope(
    scope: &BoundScope,
    name: &str,
    varlevelsup: usize,
) -> Result<Option<ResolvedSystemColumn>, ParseError> {
    let (relation, column_name) = match name.rsplit_once('.') {
        Some((relation, column_name)) => (Some(relation), column_name),
        None => (None, name),
    };
    if !column_name.eq_ignore_ascii_case("tableoid") {
        return Ok(None);
    }

    let system_type = SqlType::new(SqlTypeKind::Oid);
    if let Some(relation) = relation {
        let mut matches = scope.relations.iter().filter(|entry| {
            entry
                .relation_names
                .iter()
                .any(|visible_relation| visible_relation.eq_ignore_ascii_case(relation))
        });
        if let Some(first) = matches.next() {
            if matches.next().is_some() {
                return Err(ParseError::AmbiguousColumn(name.to_string()));
            }
            return Ok(first.system_varno.map(|varno| ResolvedSystemColumn {
                varno,
                varlevelsup,
                sql_type: system_type,
            }));
        }
        let normalized_relation = relation.to_ascii_lowercase();
        if scope.relations.iter().any(|entry| {
            entry
                .hidden_invalid_relation_names
                .iter()
                .any(|hidden| hidden.eq_ignore_ascii_case(relation))
        }) {
            return Err(ParseError::InvalidFromClauseReference(normalized_relation));
        }
        if scope.relations.iter().any(|entry| {
            entry
                .hidden_missing_relation_names
                .iter()
                .any(|hidden| hidden.eq_ignore_ascii_case(relation))
        }) {
            return Err(ParseError::MissingFromClauseEntry(normalized_relation));
        }
        return Ok(None);
    }

    let mut matches = scope
        .relations
        .iter()
        .filter_map(|entry| entry.system_varno)
        .map(|varno| ResolvedSystemColumn {
            varno,
            varlevelsup,
            sql_type: system_type,
        });
    let Some(first) = matches.next() else {
        return Ok(None);
    };
    if matches.next().is_some() {
        return Err(ParseError::AmbiguousColumn(name.to_string()));
    }
    Ok(Some(first))
}

pub(super) fn resolve_system_column_with_outer(
    scope: &BoundScope,
    outer_scopes: &[BoundScope],
    name: &str,
) -> Result<Option<ResolvedSystemColumn>, ParseError> {
    if let Some(resolved) = resolve_system_column_in_scope(scope, name, 0)? {
        return Ok(Some(resolved));
    }

    for (depth, outer_scope) in outer_scopes.iter().enumerate() {
        if let Some(resolved) = resolve_system_column_in_scope(outer_scope, name, depth + 1)? {
            return Ok(Some(resolved));
        }
    }

    Ok(None)
}

pub(super) fn resolve_column_with_outer(
    scope: &BoundScope,
    outer_scopes: &[BoundScope],
    name: &str,
    grouped_outer: Option<&GroupedOuterScope>,
) -> Result<ResolvedColumn, ParseError> {
    match resolve_column(scope, name) {
        Ok(index) => return Ok(ResolvedColumn::Local(index)),
        Err(ParseError::AmbiguousColumn(name)) => return Err(ParseError::AmbiguousColumn(name)),
        Err(ParseError::UnknownColumn(_)) => {}
        Err(other) => return Err(other),
    }

    for (depth, outer_scope) in outer_scopes.iter().enumerate() {
        match resolve_column(outer_scope, name) {
            Ok(index) => {
                if depth == 0
                    && let Some(grouped) = grouped_outer
                    && scopes_match(&grouped.scope, outer_scope)
                    && !outer_column_is_grouped(index, &grouped.scope, &grouped.group_by_exprs)
                {
                    let column = &outer_scope.columns[index];
                    let display_name = column
                        .relation_names
                        .first()
                        .map(|relation_name| format!("{relation_name}.{}", column.output_name))
                        .unwrap_or_else(|| column.output_name.clone());
                    return Err(ParseError::UngroupedColumn {
                        display_name,
                        token: name.to_string(),
                        clause: UngroupedColumnClause::Other,
                    });
                }
                return Ok(ResolvedColumn::Outer { depth, index });
            }
            Err(ParseError::AmbiguousColumn(name)) => {
                return Err(ParseError::AmbiguousColumn(name));
            }
            Err(ParseError::UnknownColumn(_)) => {}
            Err(other) => return Err(other),
        }
    }

    Err(ParseError::UnknownColumn(name.to_string()))
}

pub(super) fn resolve_relation_row_expr_with_outer(
    scope: &BoundScope,
    outer_scopes: &[BoundScope],
    name: &str,
) -> Option<Vec<(String, Expr)>> {
    resolve_relation_row_expr_in_scope(scope, name).or_else(|| {
        outer_scopes.iter().enumerate().find_map(|(depth, scope)| {
            let exprs = resolve_relation_row_expr_in_scope(scope, name)?;
            Some(
                exprs
                    .into_iter()
                    .map(|(field, expr)| (field, raise_expr_varlevels(expr, depth + 1)))
                    .collect(),
            )
        })
    })
}

fn resolve_relation_row_expr_in_scope(scope: &BoundScope, name: &str) -> Option<Vec<(String, Expr)>> {
    let mut matched = false;
    let fields = scope
        .columns
        .iter()
        .zip(scope.output_exprs.iter())
        .filter_map(|(column, expr)| {
            if column.hidden
                || !column
                    .relation_names
                    .iter()
                    .any(|relation| relation.eq_ignore_ascii_case(name))
            {
                return None;
            }
            matched = true;
            Some((column.output_name.clone(), expr.clone()))
        })
        .collect::<Vec<_>>();
    matched.then_some(fields)
}

fn from_item_is_lateral(item: &FromItem) -> bool {
    match item {
        FromItem::Lateral(_) => true,
        FromItem::FunctionCall { .. } => true,
        FromItem::Alias { source, .. } => from_item_is_lateral(source),
        _ => false,
    }
}

fn scopes_match(left: &BoundScope, right: &BoundScope) -> bool {
    left.columns == right.columns && left.desc == right.desc
}

fn outer_column_is_grouped(index: usize, scope: &BoundScope, group_by_exprs: &[SqlExpr]) -> bool {
    group_by_exprs.iter().any(|expr| match expr {
        SqlExpr::Column(name) => resolve_column(scope, name)
            .ok()
            .is_some_and(|group_idx| group_idx == index),
        _ => false,
    })
}

fn scope_with_output_exprs(mut scope: BoundScope, output_exprs: &[Expr]) -> BoundScope {
    scope.output_exprs = output_exprs.to_vec();
    scope
}

pub(super) fn bind_from_item_with_ctes(
    stmt: &FromItem,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<(AnalyzedFrom, BoundScope), ParseError> {
    match stmt {
        FromItem::Table { name, only } => {
            if let Some(cte) = ctes.iter().find(|cte| cte.name.eq_ignore_ascii_case(name)) {
                if cte.self_reference {
                    let output_columns = cte
                        .desc
                        .columns
                        .iter()
                        .map(|column| QueryColumn {
                            name: column.name.clone(),
                            sql_type: column.sql_type,
                        })
                        .collect::<Vec<_>>();
                    let plan = AnalyzedFrom::worktable(cte.worktable_id, output_columns);
                    return Ok((
                        plan.clone(),
                        scope_with_output_exprs(
                            scope_for_relation(Some(name), &cte.desc),
                            &plan.output_exprs,
                        ),
                    ));
                }
                let plan = AnalyzedFrom::cte_scan(cte.cte_id, cte.plan.clone());
                return Ok((
                    plan.clone(),
                    scope_with_output_exprs(
                        scope_for_relation(Some(name), &cte.desc),
                        &plan.output_exprs,
                    ),
                ));
            }
            if let Some(bound) = bind_builtin_system_view(name, catalog) {
                return Ok(bound);
            }
            let entry = catalog
                .lookup_any_relation(name)
                .ok_or_else(|| ParseError::UnknownTable(name.to_string()))?;
            if !matches!(entry.relkind, 'r' | 'v' | 'S') {
                return Err(ParseError::WrongObjectType {
                    name: name.to_string(),
                    expected: "table, view, or sequence",
                });
            }
            let desc = entry.desc.clone();
            Ok((
                AnalyzedFrom::relation(
                    name.clone(),
                    entry.rel,
                    entry.relation_oid,
                    entry.relkind,
                    entry.toast,
                    !*only && entry.relkind == 'r',
                    desc.clone(),
                ),
                scope_for_base_relation(name, &desc),
            ))
        }
        FromItem::Values { rows } => {
            bind_values_rows(rows, None, catalog, outer_scopes, grouped_outer, ctes)
        }
        FromItem::FunctionCall {
            name,
            args,
            func_variadic,
        } => {
            let (plan, scope, _) = bind_function_from_item_with_ctes(
                name,
                args,
                *func_variadic,
                None,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            Ok((plan, scope))
        }
        FromItem::DerivedTable(select) => {
            let (plan, _) =
                analyze_select_query_with_outer(select, catalog, &[], None, ctes, expanded_views)?;
            let bound = AnalyzedFrom::subquery(plan);
            let desc = synthetic_desc_from_analyzed_from(&bound);
            Ok((
                bound.clone(),
                scope_with_output_exprs(scope_for_relation(None, &desc), &bound.output_exprs),
            ))
        }
        FromItem::Lateral(source) => match source.as_ref() {
            FromItem::DerivedTable(select) => {
                let (plan, _) = analyze_select_query_with_outer(
                    select,
                    catalog,
                    outer_scopes,
                    grouped_outer.cloned(),
                    ctes,
                    expanded_views,
                )?;
                let bound = AnalyzedFrom::subquery(plan);
                let desc = synthetic_desc_from_analyzed_from(&bound);
                Ok((
                    bound.clone(),
                    scope_with_output_exprs(scope_for_relation(None, &desc), &bound.output_exprs),
                ))
            }
            other => bind_from_item_with_ctes(
                other,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
                expanded_views,
            ),
        },
        FromItem::Join {
            left,
            right,
            kind,
            constraint,
        } => {
            let (left_plan, left_scope) = bind_from_item_with_ctes(
                left,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
                expanded_views,
            )?;
            let mut right_outer_scopes = outer_scopes.to_vec();
            if from_item_is_lateral(right) {
                right_outer_scopes.insert(0, left_scope.clone());
            }
            let (right_plan, right_scope) = bind_from_item_with_ctes(
                right,
                catalog,
                &right_outer_scopes,
                grouped_outer,
                ctes,
                expanded_views,
            )?;
            let right_scope = shift_scope_rtindexes(right_scope, left_plan.rtable.len());
            let raw_scope = combine_scopes(&left_scope, &right_scope);
            let (on, alias_info, scope) = bind_join_constraint_with_ctes(
                kind,
                constraint,
                &left_scope,
                &right_scope,
                &raw_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            let plan =
                AnalyzedFrom::join(left_plan, right_plan, plan_join_type(*kind), on, alias_info);
            let scope = scope_with_output_exprs(scope.unwrap_or(raw_scope), &plan.output_exprs);
            Ok((plan, scope))
        }
        FromItem::Alias {
            source,
            alias,
            column_aliases,
            preserve_source_names,
        } => {
            let function_source = match source.as_ref() {
                FromItem::FunctionCall {
                    name,
                    args,
                    func_variadic,
                } => Some((name.as_str(), args.as_slice(), *func_variadic)),
                FromItem::Lateral(inner) => match inner.as_ref() {
                    FromItem::FunctionCall {
                        name,
                        args,
                        func_variadic,
                    } => Some((name.as_str(), args.as_slice(), *func_variadic)),
                    _ => None,
                },
                _ => None,
            };
            let (plan, scope, alias_single_function_output) =
                if let Some((name, args, func_variadic)) = function_source {
                let typed_defs = match column_aliases {
                    AliasColumnSpec::Definitions(defs) => Some(defs.as_slice()),
                    AliasColumnSpec::None | AliasColumnSpec::Names(_) => None,
                };
                bind_function_from_item_with_ctes(
                    name,
                    args,
                    func_variadic,
                    typed_defs,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?
            } else {
                let (plan, scope) = bind_from_item_with_ctes(
                    source,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                    expanded_views,
                )?;
                (plan, scope, false)
            };
            let alias_columns = match column_aliases {
                AliasColumnSpec::Definitions(_) => &AliasColumnSpec::None,
                _ => column_aliases,
            };
            apply_relation_alias(
                plan,
                scope,
                alias,
                alias_columns,
                alias_single_function_output,
                *preserve_source_names,
                matches!(source.as_ref(), FromItem::Alias { .. }),
            )
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn bind_function_from_item_with_ctes(
    name: &str,
    args: &[SqlFunctionArg],
    func_variadic: bool,
    column_definitions: Option<&[AliasColumnDef]>,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(AnalyzedFrom, BoundScope, bool), ParseError> {
    let args = lower_named_table_function_args(name, args)?;
    if name.eq_ignore_ascii_case("json_populate_record")
        || name.eq_ignore_ascii_case("json_populate_recordset")
    {
        let bound = bind_json_populate_record_from_item(
            name,
            &args,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?;
        return Ok(bound);
    }
    let call_scope = empty_scope();
    let actual_types = args
        .iter()
        .map(|arg| {
            infer_sql_expr_type_with_ctes(
                arg,
                &call_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        })
        .collect::<Vec<_>>();
    let resolved = resolve_function_call(catalog, name, &actual_types, func_variadic).ok();
    let resolved_proc_oid = resolved.as_ref().map(|call| call.proc_oid).unwrap_or(0);
    let resolved_func_variadic = resolved
        .as_ref()
        .map(|call| call.func_variadic)
        .unwrap_or(func_variadic);
    let resolved_row_columns =
        resolve_function_row_columns(catalog, resolved.as_ref(), column_definitions)?;

    match name {
        "generate_series" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(ParseError::UnexpectedToken {
                    expected: "generate_series(start, stop[, step])",
                    actual: format!("generate_series with {} arguments", args.len()),
                });
            }
            let start = bind_expr_with_outer_and_ctes(
                &args[0],
                &call_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            let stop = bind_expr_with_outer_and_ctes(
                &args[1],
                &call_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            let start_type = infer_sql_expr_type_with_ctes(
                &args[0],
                &call_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let stop_type = infer_sql_expr_type_with_ctes(
                &args[1],
                &call_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let step_type = if args.len() == 3 {
                Some(infer_sql_expr_type_with_ctes(
                    &args[2],
                    &call_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                ))
            } else {
                None
            };
            let common = resolve_generate_series_common_type(start_type, stop_type, step_type)?;
            let step = if args.len() == 3 {
                let step_expr = bind_expr_with_outer_and_ctes(
                    &args[2],
                    &call_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let step_type = step_type.expect("generate_series step type");
                coerce_bound_expr(step_expr, step_type, common)
            } else {
                match common.kind {
                    SqlTypeKind::Int8 => Expr::Const(Value::Int64(1)),
                    SqlTypeKind::Numeric => Expr::Const(Value::Numeric(
                        crate::include::nodes::datum::NumericValue::from_i64(1),
                    )),
                    _ => Expr::Const(Value::Int32(1)),
                }
            };
            let desc = RelationDesc {
                columns: vec![column_desc("generate_series", common, false)],
            };
            let scope = scope_for_relation(Some(name), &desc);
            Ok((
                AnalyzedFrom::function(SetReturningCall::GenerateSeries {
                    func_oid: resolved_proc_oid,
                    func_variadic: resolved_func_variadic,
                    start: coerce_bound_expr(start, start_type, common),
                    stop: coerce_bound_expr(stop, stop_type, common),
                    step,
                    output: QueryColumn {
                        name: "generate_series".to_string(),
                        sql_type: common,
                    },
                }),
                scope,
                true,
            ))
        }
        "unnest" => {
            if args.is_empty() {
                return Err(ParseError::UnexpectedToken {
                    expected: "unnest(array_expr [, array_expr ...])",
                    actual: "unnest()".into(),
                });
            }
            if func_variadic && args.len() > 1 {
                return Err(ParseError::UnexpectedToken {
                    expected: "ordinary multi-argument unnest() in FROM without VARIADIC decoration",
                    actual: format!("unnest with {} arguments and VARIADIC", args.len()),
                });
            }
            let mut bound_args = Vec::with_capacity(args.len());
            let mut output_columns = Vec::with_capacity(args.len());
            let mut desc_columns = Vec::with_capacity(args.len());
            for (idx, arg) in args.iter().enumerate() {
                let arg_type = infer_sql_expr_type_with_ctes(
                    arg,
                    &call_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                if !arg_type.is_array {
                    return Err(ParseError::UnexpectedToken {
                        expected: "array argument to unnest",
                        actual: format!("{arg:?}"),
                    });
                }
                let element_type = arg_type.element_type();
                let column_name = if idx == 0 {
                    "unnest".to_string()
                } else {
                    format!("unnest_{}", idx + 1)
                };
                bound_args.push(bind_expr_with_outer_and_ctes(
                    arg,
                    &call_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?);
                output_columns.push(QueryColumn {
                    name: column_name.clone(),
                    sql_type: element_type,
                });
                desc_columns.push(column_desc(column_name, element_type, true));
            }
            let desc = RelationDesc {
                columns: desc_columns,
            };
            let scope = scope_for_relation(Some(name), &desc);
            let alias_single_function_output = output_columns.len() == 1;
            Ok((
                AnalyzedFrom::function(SetReturningCall::Unnest {
                    func_oid: resolved_proc_oid,
                    func_variadic: resolved_func_variadic,
                    args: bound_args,
                    output_columns,
                }),
                scope,
                alias_single_function_output,
            ))
        }
        "pg_input_error_info" => {
            if args.len() != 2 {
                return Err(ParseError::UnexpectedToken {
                    expected: "pg_input_error_info(text, text)",
                    actual: format!("pg_input_error_info with {} arguments", args.len()),
                });
            }
            let empty_scope = empty_scope();
            let text_type = SqlType::new(SqlTypeKind::Text);
            let left_type = infer_sql_expr_type_with_ctes(
                &args[0],
                &empty_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let right_type = infer_sql_expr_type_with_ctes(
                &args[1],
                &empty_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let left = coerce_bound_expr(
                bind_expr_with_outer_and_ctes(
                    &args[0],
                    &empty_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
                left_type,
                text_type,
            );
            let right = coerce_bound_expr(
                bind_expr_with_outer_and_ctes(
                    &args[1],
                    &empty_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?,
                right_type,
                text_type,
            );
            let output_columns = vec![
                QueryColumn::text("message"),
                QueryColumn::text("detail"),
                QueryColumn::text("hint"),
                QueryColumn::text("sql_error_code"),
            ];
            let desc = RelationDesc {
                columns: output_columns
                    .iter()
                    .map(|col| column_desc(col.name.clone(), col.sql_type, true))
                    .collect(),
            };
            let scope = scope_for_relation(Some(name), &desc);
            Ok((
                AnalyzedFrom::result().with_projection(vec![
                    TargetEntry::new(
                        "message",
                        Expr::builtin_func(
                            BuiltinScalarFunction::PgInputErrorMessage,
                            Some(text_type),
                            false,
                            vec![left.clone(), right.clone()],
                        ),
                        text_type,
                        1,
                    ),
                    TargetEntry::new(
                        "detail",
                        Expr::builtin_func(
                            BuiltinScalarFunction::PgInputErrorDetail,
                            Some(text_type),
                            false,
                            vec![left.clone(), right.clone()],
                        ),
                        text_type,
                        2,
                    ),
                    TargetEntry::new(
                        "hint",
                        Expr::builtin_func(
                            BuiltinScalarFunction::PgInputErrorHint,
                            Some(text_type),
                            false,
                            vec![left.clone(), right.clone()],
                        ),
                        text_type,
                        3,
                    ),
                    TargetEntry::new(
                        "sql_error_code",
                        Expr::builtin_func(
                            BuiltinScalarFunction::PgInputErrorSqlState,
                            Some(text_type),
                            false,
                            vec![left, right],
                        ),
                        text_type,
                        4,
                    ),
                ]),
                scope,
                false,
            ))
        }
        other => {
            if let Some(kind) = resolve_json_table_function(other) {
                let empty_scope = empty_scope();
                let bound_args = args
                    .iter()
                    .map(|arg| {
                        bind_expr_with_outer_and_ctes(
                            arg,
                            &empty_scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let output_columns = resolved_row_columns.clone().unwrap_or_else(|| match kind {
                    JsonTableFunction::ObjectKeys => vec![QueryColumn::text("json_object_keys")],
                    JsonTableFunction::Each => vec![
                        QueryColumn::text("key"),
                        QueryColumn {
                            name: "value".into(),
                            sql_type: SqlType::new(SqlTypeKind::Json),
                        },
                    ],
                    JsonTableFunction::EachText => {
                        vec![QueryColumn::text("key"), QueryColumn::text("value")]
                    }
                    JsonTableFunction::ArrayElements => vec![QueryColumn {
                        name: "json_array_elements".into(),
                        sql_type: SqlType::new(SqlTypeKind::Json),
                    }],
                    JsonTableFunction::ArrayElementsText => {
                        vec![QueryColumn::text("json_array_elements_text")]
                    }
                    JsonTableFunction::JsonbPathQuery => vec![QueryColumn {
                        name: "jsonb_path_query".into(),
                        sql_type: SqlType::new(SqlTypeKind::Jsonb),
                    }],
                    JsonTableFunction::JsonbObjectKeys => {
                        vec![QueryColumn::text("jsonb_object_keys")]
                    }
                    JsonTableFunction::JsonbEach => vec![
                        QueryColumn::text("key"),
                        QueryColumn {
                            name: "value".into(),
                            sql_type: SqlType::new(SqlTypeKind::Jsonb),
                        },
                    ],
                    JsonTableFunction::JsonbEachText => {
                        vec![QueryColumn::text("key"), QueryColumn::text("value")]
                    }
                    JsonTableFunction::JsonbArrayElements => vec![QueryColumn {
                        name: "jsonb_array_elements".into(),
                        sql_type: SqlType::new(SqlTypeKind::Jsonb),
                    }],
                    JsonTableFunction::JsonbArrayElementsText => {
                        vec![QueryColumn::text("jsonb_array_elements_text")]
                    }
                });
                let desc = RelationDesc {
                    columns: output_columns
                        .iter()
                        .map(|col| column_desc(col.name.clone(), col.sql_type, true))
                        .collect(),
                };
                let scope = scope_for_relation(Some(name), &desc);
                let alias_single_function_output = output_columns.len() == 1;
                Ok((
                    AnalyzedFrom::function(SetReturningCall::JsonTableFunction {
                        func_oid: resolved_proc_oid,
                        func_variadic: resolved_func_variadic,
                        kind,
                        args: bound_args,
                        output_columns,
                    }),
                    scope,
                    alias_single_function_output,
                ))
            } else if let Some(kind) = resolve_regex_table_function(other) {
                let empty_scope = empty_scope();
                let bound_args = args
                    .iter()
                    .map(|arg| {
                        bind_expr_with_outer_and_ctes(
                            arg,
                            &empty_scope,
                            catalog,
                            outer_scopes,
                            grouped_outer,
                            ctes,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let output_columns = match kind {
                    crate::include::nodes::primnodes::RegexTableFunction::Matches => {
                        vec![QueryColumn {
                            name: "regexp_matches".into(),
                            sql_type: SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                        }]
                    }
                    crate::include::nodes::primnodes::RegexTableFunction::SplitToTable => {
                        vec![QueryColumn::text("regexp_split_to_table")]
                    }
                };
                let desc = RelationDesc {
                    columns: output_columns
                        .iter()
                        .map(|col| column_desc(col.name.clone(), col.sql_type, true))
                        .collect(),
                };
                let scope = scope_for_relation(Some(name), &desc);
                let alias_single_function_output = output_columns.len() == 1;
                Ok((
                    AnalyzedFrom::function(SetReturningCall::RegexTableFunction {
                        func_oid: resolved_proc_oid,
                        func_variadic: resolved_func_variadic,
                        kind,
                        args: bound_args,
                        output_columns,
                    }),
                    scope,
                    alias_single_function_output,
                ))
            } else if let Some(resolved) = resolved.as_ref() {
                if resolved.prokind != 'f' || !resolved.proretset {
                    return Err(ParseError::UnknownTable(other.to_string()));
                }
                let bound_args = bind_user_defined_table_function_args(
                    &args,
                    &call_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    &resolved.declared_arg_types,
                )?;
                let alias_single_function_output = resolved_row_columns.is_none();
                let output_columns = resolved_row_columns.unwrap_or_else(|| {
                    vec![QueryColumn {
                        name: other.to_string(),
                        sql_type: resolved.result_type,
                    }]
                });
                let desc = RelationDesc {
                    columns: output_columns
                        .iter()
                        .map(|col| column_desc(col.name.clone(), col.sql_type, true))
                        .collect(),
                };
                let scope = scope_for_relation(Some(name), &desc);
                Ok((
                    AnalyzedFrom::function(SetReturningCall::UserDefined {
                        proc_oid: resolved.proc_oid,
                        func_variadic: resolved.func_variadic,
                        args: bound_args,
                        output_columns,
                    }),
                    scope,
                    alias_single_function_output,
                ))
            } else {
                Err(ParseError::UnknownTable(other.to_string()))
            }
        }
    }
}

fn bind_json_populate_record_from_item(
    name: &str,
    args: &[SqlExpr],
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(AnalyzedFrom, BoundScope, bool), ParseError> {
    if args.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "json_populate_record(record, json) or json_populate_recordset(record, json)",
            actual: format!("{name}()"),
        });
    }
    let call_scope = empty_scope();
    let row_type = infer_sql_expr_type_with_ctes(
        &args[0],
        &call_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    );
    if row_type.kind != SqlTypeKind::Composite || row_type.typrelid == 0 {
        return Err(ParseError::UnknownTable(name.to_string()));
    }
    let relation = catalog
        .lookup_relation_by_oid(row_type.typrelid)
        .ok_or_else(|| ParseError::UnknownTable(name.to_string()))?;
    let output_columns = relation
        .desc
        .columns
        .iter()
        .filter(|column| !column.dropped)
        .map(|column| QueryColumn {
            name: column.name.clone(),
            sql_type: column.sql_type,
        })
        .collect::<Vec<_>>();
    let bound_args = args
        .iter()
        .map(|arg| {
            bind_expr_with_outer_and_ctes(
                arg,
                &call_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let desc = RelationDesc {
        columns: output_columns
            .iter()
            .map(|col| column_desc(col.name.clone(), col.sql_type, true))
            .collect(),
    };
    let scope = scope_for_relation(Some(name), &desc);
    Ok((
        AnalyzedFrom::function(SetReturningCall::UserDefined {
            proc_oid: 0,
            func_variadic: false,
            args: bound_args,
            output_columns,
        }),
        scope,
        false,
    ))
}

fn bind_user_defined_table_function_args(
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    declared_arg_types: &[SqlType],
) -> Result<Vec<Expr>, ParseError> {
    let arg_types = args
        .iter()
        .map(|arg| infer_sql_expr_type(arg, scope, catalog, outer_scopes, grouped_outer))
        .collect::<Vec<_>>();
    let bound_args = args
        .iter()
        .map(|arg| bind_expr_with_outer(arg, scope, catalog, outer_scopes, grouped_outer))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(bound_args
        .into_iter()
        .zip(arg_types)
        .zip(declared_arg_types.iter().copied())
        .map(|((arg, actual_type), declared_type)| {
            coerce_bound_expr(arg, actual_type, declared_type)
        })
        .collect())
}

fn resolve_function_row_columns(
    catalog: &dyn CatalogLookup,
    resolved: Option<&ResolvedFunctionCall>,
    column_definitions: Option<&[AliasColumnDef]>,
) -> Result<Option<Vec<QueryColumn>>, ParseError> {
    match column_definitions {
        Some(definitions) => {
            let columns = query_columns_from_alias_definitions(definitions, catalog)?;
            match resolved.map(|call| &call.row_shape) {
                Some(ResolvedFunctionRowShape::AnonymousRecord) => Ok(Some(columns)),
                Some(ResolvedFunctionRowShape::OutParameters(_)) => Err(function_coldeflist_error(
                    "a column definition list is redundant for a function with OUT parameters",
                )),
                Some(ResolvedFunctionRowShape::NamedComposite { .. }) => {
                    Err(function_coldeflist_error(
                        "a column definition list is redundant for a function returning a named composite type",
                    ))
                }
                Some(ResolvedFunctionRowShape::None) | None => Err(function_coldeflist_error(
                    "a column definition list is only allowed for functions returning \"record\"",
                )),
            }
        }
        None => match resolved.map(|call| &call.row_shape) {
            Some(ResolvedFunctionRowShape::AnonymousRecord) => Err(function_coldeflist_error(
                "a column definition list is required for functions returning \"record\"",
            )),
            Some(ResolvedFunctionRowShape::OutParameters(columns)) => Ok(Some(columns.clone())),
            Some(ResolvedFunctionRowShape::NamedComposite { columns, .. }) => {
                Ok(Some(columns.clone()))
            }
            Some(ResolvedFunctionRowShape::None) | None => Ok(None),
        },
    }
}

fn query_columns_from_alias_definitions(
    definitions: &[AliasColumnDef],
    catalog: &dyn CatalogLookup,
) -> Result<Vec<QueryColumn>, ParseError> {
    definitions
        .iter()
        .map(|definition| {
            Ok(QueryColumn {
                name: definition.name.clone(),
                sql_type: match &definition.ty {
                    RawTypeName::Builtin(sql_type) => *sql_type,
                    RawTypeName::Serial(kind) => {
                        return Err(ParseError::FeatureNotSupported(format!(
                            "{} is only allowed in CREATE TABLE / ALTER TABLE ADD COLUMN",
                            match kind {
                                crate::backend::parser::SerialKind::Small => "smallserial",
                                crate::backend::parser::SerialKind::Regular => "serial",
                                crate::backend::parser::SerialKind::Big => "bigserial",
                            }
                        )));
                    }
                    RawTypeName::Record => SqlType::record(RECORD_TYPE_OID),
                    RawTypeName::Named { name, .. } => catalog
                        .type_rows()
                        .into_iter()
                        .find(|row| row.typname.eq_ignore_ascii_case(name))
                        .map(|row| row.sql_type)
                        .ok_or_else(|| ParseError::UnsupportedType(name.clone()))?,
                },
            })
        })
        .collect()
}

fn function_coldeflist_error(message: &str) -> ParseError {
    ParseError::UnexpectedToken {
        expected: "function row description in FROM",
        actual: message.into(),
    }
}

pub(super) fn lookup_relation(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Result<BoundRelation, ParseError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if entry.relkind == 'r' => Ok(entry),
        Some(_) => Err(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table",
        }),
        None => Err(ParseError::UnknownTable(name.to_string())),
    }
}

pub(super) fn scope_for_relation(relation_name: Option<&str>, desc: &RelationDesc) -> BoundScope {
    BoundScope {
        desc: desc.clone(),
        output_exprs: default_scope_output_exprs(1, desc),
        columns: desc
            .columns
            .iter()
            .map(|column| ScopeColumn {
                output_name: column.name.clone(),
                hidden: column.dropped,
                relation_names: relation_name.into_iter().map(str::to_string).collect(),
                hidden_invalid_relation_names: vec![],
                hidden_missing_relation_names: vec![],
            })
            .collect(),
        relations: relation_name
            .map(|name| {
                vec![ScopeRelation {
                    relation_names: vec![name.to_string()],
                    hidden_invalid_relation_names: vec![],
                    hidden_missing_relation_names: vec![],
                    system_varno: None,
                }]
            })
            .unwrap_or_default(),
    }
}

pub(super) fn scope_for_base_relation(relation_name: &str, desc: &RelationDesc) -> BoundScope {
    let mut scope = scope_for_relation(Some(relation_name), desc);
    scope.output_exprs = default_scope_output_exprs(1, desc);
    scope.relations = vec![ScopeRelation {
        relation_names: vec![relation_name.to_string()],
        hidden_invalid_relation_names: vec![],
        hidden_missing_relation_names: vec![],
        system_varno: Some(1),
    }];
    scope
}

fn shift_scope_rtindexes(mut scope: BoundScope, offset: usize) -> BoundScope {
    if offset == 0 {
        return scope;
    }
    scope.output_exprs = scope
        .output_exprs
        .into_iter()
        .map(|expr| shift_expr_rtindexes(expr, offset))
        .collect();
    for relation in &mut scope.relations {
        if let Some(varno) = relation.system_varno.as_mut() {
            *varno += offset;
        }
    }
    scope
}

pub(super) fn combine_scopes(left: &BoundScope, right: &BoundScope) -> BoundScope {
    let mut desc = left.desc.clone();
    desc.columns.extend(right.desc.columns.clone());
    let mut output_exprs = left.output_exprs.clone();
    output_exprs.extend(right.output_exprs.clone());
    let mut columns = left.columns.clone();
    columns.extend(right.columns.clone());
    let mut relations = left.relations.clone();
    relations.extend(right.relations.clone());
    BoundScope {
        desc,
        output_exprs,
        columns,
        relations,
    }
}

fn plan_join_type(kind: JoinKind) -> JoinType {
    match kind {
        JoinKind::Inner => JoinType::Inner,
        JoinKind::Cross => JoinType::Cross,
        JoinKind::Left => JoinType::Left,
        JoinKind::Right => JoinType::Right,
        JoinKind::Full => JoinType::Full,
    }
}

type JoinBinding = (Expr, Option<JoinAliasInfo>, Option<BoundScope>);

#[allow(clippy::too_many_arguments)]
fn bind_join_constraint_with_ctes(
    kind: &JoinKind,
    constraint: &JoinConstraint,
    left_scope: &BoundScope,
    right_scope: &BoundScope,
    raw_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<JoinBinding, ParseError> {
    match constraint {
        JoinConstraint::None => {
            if !matches!(kind, JoinKind::Cross) {
                return Err(ParseError::UnexpectedToken {
                    expected: "valid join clause",
                    actual: format!("{kind:?}"),
                });
            }
            Ok((Expr::Const(Value::Bool(true)), None, None))
        }
        JoinConstraint::On(on) => Ok((
            bind_expr_with_outer_and_ctes(
                on,
                raw_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            None,
            None,
        )),
        JoinConstraint::Using(columns) => {
            bind_join_using_projection(columns, left_scope, right_scope)
        }
        JoinConstraint::Natural => {
            let columns = natural_join_columns(left_scope, right_scope);
            bind_join_using_projection(&columns, left_scope, right_scope)
        }
    }
}

fn natural_join_columns(left_scope: &BoundScope, right_scope: &BoundScope) -> Vec<String> {
    let mut out = Vec::new();
    for left in &left_scope.columns {
        if left.hidden {
            continue;
        }
        if right_scope
            .columns
            .iter()
            .any(|right| !right.hidden && right.output_name.eq_ignore_ascii_case(&left.output_name))
            && !out
                .iter()
                .any(|name: &String| name.eq_ignore_ascii_case(&left.output_name))
        {
            out.push(left.output_name.clone());
        }
    }
    out
}

fn bind_join_using_projection(
    columns: &[String],
    left_scope: &BoundScope,
    right_scope: &BoundScope,
) -> Result<JoinBinding, ParseError> {
    let mut using_pairs = Vec::with_capacity(columns.len());
    for name in columns {
        let left_index = resolve_column(left_scope, name)?;
        let right_index = resolve_column(right_scope, name)?;
        using_pairs.push((name.clone(), left_index, right_index));
    }

    let on = using_pairs
        .iter()
        .fold(Expr::Const(Value::Bool(true)), |expr, (_, left, right)| {
            let predicate = Expr::op_auto(
                crate::include::nodes::primnodes::OpExprKind::Eq,
                vec![
                    left_scope.output_exprs[*left].clone(),
                    right_scope.output_exprs[*right].clone(),
                ],
            );
            match expr {
                Expr::Const(Value::Bool(true)) => predicate,
                other => Expr::bool_expr(
                    crate::include::nodes::primnodes::BoolExprType::And,
                    vec![other, predicate],
                ),
            }
        });

    let mut alias_exprs = Vec::new();
    let mut output_columns = Vec::new();
    let mut desc_columns = Vec::new();
    let mut scope_columns = Vec::new();
    let mut used_left = vec![false; left_scope.columns.len()];
    let mut used_right = vec![false; right_scope.columns.len()];
    let mut joinleftcols = Vec::new();
    let mut joinrightcols = Vec::new();

    for (name, left_index, right_index) in &using_pairs {
        used_left[*left_index] = true;
        used_right[*right_index] = true;
        let left_ty = left_scope.desc.columns[*left_index].sql_type;
        let left_expr = left_scope.output_exprs[*left_index].clone();
        let right_expr = right_scope.output_exprs[*right_index].clone();
        alias_exprs.push(Expr::Coalesce(Box::new(left_expr), Box::new(right_expr)));
        output_columns.push(QueryColumn {
            name: name.clone(),
            sql_type: left_ty,
        });
        joinleftcols.push(*left_index + 1);
        joinrightcols.push(*right_index + 1);
        desc_columns.push(column_desc(name.clone(), left_ty, true));
        scope_columns.push(ScopeColumn {
            output_name: name.clone(),
            hidden: false,
            relation_names: vec![],
            hidden_invalid_relation_names: vec![],
            hidden_missing_relation_names: vec![],
        });
    }

    for (index, column) in left_scope.columns.iter().enumerate() {
        if used_left[index] || column.hidden {
            continue;
        }
        alias_exprs.push(left_scope.output_exprs[index].clone());
        output_columns.push(QueryColumn {
            name: column.output_name.clone(),
            sql_type: left_scope.desc.columns[index].sql_type,
        });
        joinleftcols.push(index + 1);
        desc_columns.push(left_scope.desc.columns[index].clone());
        scope_columns.push(column.clone());
    }

    for (index, column) in right_scope.columns.iter().enumerate() {
        if used_right[index] || column.hidden {
            continue;
        }
        alias_exprs.push(right_scope.output_exprs[index].clone());
        output_columns.push(QueryColumn {
            name: column.output_name.clone(),
            sql_type: right_scope.desc.columns[index].sql_type,
        });
        joinrightcols.push(index + 1);
        desc_columns.push(right_scope.desc.columns[index].clone());
        scope_columns.push(column.clone());
    }

    let scope = BoundScope {
        desc: RelationDesc {
            columns: desc_columns,
        },
        output_exprs: alias_exprs.clone(),
        columns: scope_columns,
        relations: combine_scopes(left_scope, right_scope).relations,
    };
    Ok((
        on,
        Some(JoinAliasInfo {
            output_columns,
            output_exprs: alias_exprs,
            joinmergedcols: using_pairs.len(),
            joinleftcols,
            joinrightcols,
        }),
        Some(scope),
    ))
}

fn synthetic_desc_from_analyzed_from(plan: &AnalyzedFrom) -> RelationDesc {
    plan.desc()
}

fn apply_relation_alias(
    mut plan: AnalyzedFrom,
    scope: BoundScope,
    alias: &str,
    column_aliases: &AliasColumnSpec,
    alias_single_function_output: bool,
    preserve_source_names: bool,
    source_is_alias: bool,
) -> Result<(AnalyzedFrom, BoundScope), ParseError> {
    let column_aliases = match column_aliases {
        AliasColumnSpec::None => &[][..],
        AliasColumnSpec::Names(names) => names.as_slice(),
        AliasColumnSpec::Definitions(_) => {
            return Err(ParseError::UnexpectedToken {
                expected: "column alias names",
                actual: "column definition list".into(),
            });
        }
    };
    let visible_positions = scope
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| (!column.hidden).then_some(index))
        .collect::<Vec<_>>();
    if column_aliases.len() > visible_positions.len() {
        return Err(ParseError::UnexpectedToken {
            expected: "table alias column count to match source columns",
            actual: format!(
                "table \"{alias}\" has {} columns available but {} columns specified",
                visible_positions.len(),
                column_aliases.len(),
            ),
        });
    }

    let mut desc = scope.desc.clone();
    let mut columns = scope.columns.clone();
    let mut relations = scope.relations.clone();
    let mut renamed = false;

    if alias_single_function_output && column_aliases.is_empty() && visible_positions.len() == 1 {
        let column_index = visible_positions[0];
        let column = &mut columns[column_index];
        renamed |= column.output_name != alias;
        column.output_name = alias.to_string();
        desc.columns[column_index].name = alias.to_string();
        desc.columns[column_index].storage.name = alias.to_string();
    }

    if relations.iter().any(|relation| {
        relation
            .relation_names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(alias))
    }) || columns.iter().any(|column| {
        column
            .relation_names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(alias))
    }) {
        return Err(ParseError::DuplicateTableName(alias.to_string()));
    }

    for (alias_index, column_index) in visible_positions.iter().copied().enumerate() {
        if let Some(new_name) = column_aliases.get(alias_index) {
            let column = &mut columns[column_index];
            renamed |= column.output_name != *new_name;
            column.output_name = new_name.clone();
            desc.columns[column_index].name = new_name.clone();
            desc.columns[column_index].storage.name = new_name.clone();
        }
    }

    if preserve_source_names {
        let alias_only_anonymous = columns
            .iter()
            .any(|column| column.relation_names.is_empty());
        for column in &mut columns {
            if alias_only_anonymous && !column.relation_names.is_empty() {
                continue;
            }
            if !column
                .relation_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(alias))
            {
                column.relation_names.push(alias.to_string());
            }
        }
        let relation_alias_only_anonymous = relations
            .iter()
            .any(|relation| relation.relation_names.is_empty());
        if relations.is_empty() {
            relations.push(ScopeRelation {
                relation_names: vec![alias.to_string()],
                hidden_invalid_relation_names: vec![],
                hidden_missing_relation_names: vec![],
                system_varno: None,
            });
        } else {
            for relation in &mut relations {
                if relation_alias_only_anonymous && !relation.relation_names.is_empty() {
                    continue;
                }
                if !relation
                    .relation_names
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(alias))
                {
                    relation.relation_names.push(alias.to_string());
                }
            }
        }
    } else {
        for column in &mut columns {
            if !source_is_alias {
                for hidden in column.relation_names.drain(..) {
                    if !column
                        .hidden_invalid_relation_names
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(&hidden))
                    {
                        column.hidden_invalid_relation_names.push(hidden);
                    }
                }
            } else {
                for hidden in column.relation_names.drain(..) {
                    if !column
                        .hidden_missing_relation_names
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(&hidden))
                    {
                        column.hidden_missing_relation_names.push(hidden);
                    }
                }
                column.relation_names.clear();
            }
            column.relation_names = vec![alias.to_string()];
        }
        if relations.len() == 1 {
            let relation = &mut relations[0];
            if !source_is_alias {
                for hidden in relation.relation_names.drain(..) {
                    if !relation
                        .hidden_invalid_relation_names
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(&hidden))
                    {
                        relation.hidden_invalid_relation_names.push(hidden);
                    }
                }
            } else {
                for hidden in relation.relation_names.drain(..) {
                    if !relation
                        .hidden_missing_relation_names
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(&hidden))
                    {
                        relation.hidden_missing_relation_names.push(hidden);
                    }
                }
            }
            relation.relation_names = vec![alias.to_string()];
        } else {
            let mut hidden_invalid_relation_names = Vec::new();
            let mut hidden_missing_relation_names = Vec::new();
            for relation in relations {
                let hidden_names = if source_is_alias {
                    &mut hidden_missing_relation_names
                } else {
                    &mut hidden_invalid_relation_names
                };
                for hidden in relation.relation_names {
                    if !hidden_names
                        .iter()
                        .any(|name: &String| name.eq_ignore_ascii_case(&hidden))
                    {
                        hidden_names.push(hidden);
                    }
                }
                for hidden in relation.hidden_invalid_relation_names {
                    if !hidden_invalid_relation_names
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(&hidden))
                    {
                        hidden_invalid_relation_names.push(hidden);
                    }
                }
                for hidden in relation.hidden_missing_relation_names {
                    if !hidden_missing_relation_names
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(&hidden))
                    {
                        hidden_missing_relation_names.push(hidden);
                    }
                }
            }
            relations = vec![ScopeRelation {
                relation_names: vec![alias.to_string()],
                hidden_invalid_relation_names,
                hidden_missing_relation_names,
                system_varno: None,
            }];
        }
    }

    if renamed {
        plan = plan.with_projection(
            columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    TargetEntry::new(
                        column.output_name.clone(),
                        scope.output_exprs[index].clone(),
                        desc.columns[index].sql_type,
                        index + 1,
                    )
                    .with_input_resno(index + 1)
                })
                .collect(),
        );
    }

    let output_exprs = if renamed {
        plan.output_exprs.clone()
    } else {
        scope.output_exprs
    };

    Ok((
        plan,
        BoundScope {
            desc,
            output_exprs,
            columns,
            relations,
        },
    ))
}
