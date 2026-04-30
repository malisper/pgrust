use super::expr::bind_legacy_scalar_function_call;
use super::query::{AnalyzedFrom, JoinAliasInfo, shift_expr_rtindexes};
use super::*;
use crate::backend::parser::parse_statement;
use crate::backend::storage::smgr::RelFileLocator;
use crate::backend::utils::record::lookup_anonymous_record_descriptor;
use crate::include::catalog::{
    ANYOID, PG_CATALOG_NAMESPACE_OID, PG_LANGUAGE_SQL_OID, PG_TYPE_RELATION_OID,
    PgPartitionedTableRow, PgProcRow, VOID_TYPE_OID,
};
use crate::include::nodes::datum::RecordDescriptor;
use crate::include::nodes::primnodes::{
    AttrNumber, BoolExpr, BuiltinScalarFunction, CaseExpr, CaseWhen, ColumnDesc, FuncExpr,
    JoinType, JsonRecordFunction, RULE_NEW_VAR, RULE_OLD_VAR, RowsFromItem, RowsFromSource,
    SELF_ITEM_POINTER_ATTR_NO, ScalarFunctionImpl, SqlJsonTable, SqlJsonTableBehavior,
    SqlJsonTableColumn, SqlJsonTableColumnKind, SqlJsonTablePassingArg, SqlJsonTablePlan,
    SqlJsonTableQuotes, SqlJsonTableWrapper, SqlXmlTable, SqlXmlTableColumn, SqlXmlTableColumnKind,
    SqlXmlTableNamespace, TABLE_OID_ATTR_NO, Var, XMAX_ATTR_NO, XMIN_ATTR_NO, expr_sql_type_hint,
    user_attrno,
};

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
    pub(crate) qualified_only: bool,
    pub(crate) relation_names: Vec<String>,
    pub(crate) hidden_invalid_relation_names: Vec<String>,
    pub(crate) hidden_missing_relation_names: Vec<String>,
    pub(crate) source_relation_oid: Option<u32>,
    pub(crate) source_attno: Option<AttrNumber>,
    pub(crate) source_columns: Vec<(u32, AttrNumber)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScopeRelation {
    pub(crate) relation_names: Vec<String>,
    pub(crate) hidden_invalid_relation_names: Vec<String>,
    pub(crate) hidden_missing_relation_names: Vec<String>,
    pub(crate) system_varno: Option<usize>,
    pub(crate) relation_oid: Option<u32>,
}

pub(super) struct ResolvedRelationRowExpr {
    pub(super) fields: Vec<(String, Expr)>,
    pub(super) relation_oid: Option<u32>,
}

#[derive(Debug, Clone)]
pub(crate) struct GroupedOuterScope {
    pub(crate) scope: BoundScope,
    pub(crate) group_by_exprs: Vec<SqlExpr>,
}

pub(super) fn matches_grouped_outer_expr(
    expr: &SqlExpr,
    grouped_outer: Option<&GroupedOuterScope>,
) -> bool {
    grouped_outer.is_some_and(|grouped| {
        grouped
            .group_by_exprs
            .iter()
            .any(|group_expr| group_expr == expr)
    })
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
    pub of_type_oid: u32,
    pub relpersistence: char,
    pub relkind: char,
    pub relispopulated: bool,
    pub relispartition: bool,
    pub relpartbound: Option<String>,
    pub desc: RelationDesc,
    pub partitioned_table: Option<PgPartitionedTableRow>,
    pub partition_spec: Option<LoweredPartitionSpec>,
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
    pub(super) varattno: AttrNumber,
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
    let empty = empty_scope();
    let rows = rows
        .iter()
        .map(|row| expand_values_row_exprs(row, &empty, outer_scopes))
        .collect::<Result<Vec<_>, _>>()?;
    let width = rows.first().map(Vec::len).unwrap_or(0);
    for row in &rows {
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

    let mut column_types = Vec::with_capacity(width);
    for col_idx in 0..width {
        let mut common = None;
        let mut common_expr: Option<&SqlExpr> = None;
        for row in &rows {
            if row[col_idx].is_null_const() {
                continue;
            }
            let inferred =
                row[col_idx].infer_type(&empty, catalog, outer_scopes, grouped_outer, ctes);
            common = Some(match common {
                None => {
                    common_expr = row[col_idx].raw_expr();
                    inferred
                }
                Some(existing) => {
                    let existing = if is_text_like_type(existing) {
                        common_expr
                            .map(|expr| {
                                coerce_unknown_string_literal_type(expr, existing, inferred)
                            })
                            .unwrap_or(existing)
                    } else {
                        existing
                    };
                    let adjusted = row[col_idx]
                        .raw_expr()
                        .map(|expr| coerce_unknown_string_literal_type(expr, inferred, existing))
                        .unwrap_or(inferred);
                    let resolved =
                        resolve_common_values_type(existing, adjusted).ok_or_else(|| {
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
                    common_expr = row[col_idx].raw_expr();
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
                .map(|(cell, ty)| {
                    let from = cell.infer_type(&empty, catalog, outer_scopes, grouped_outer, ctes);
                    Ok(coerce_bound_expr(
                        cell.bind(&empty, catalog, outer_scopes, grouped_outer, ctes)?,
                        from,
                        *ty,
                    ))
                })
                .collect::<Result<Vec<_>, ParseError>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    if bound_rows.iter().flatten().any(expr_contains_set_returning) {
        return Err(ParseError::FeatureNotSupportedMessage(
            "set-returning functions are not allowed in VALUES".into(),
        ));
    }

    let output_columns = column_types
        .iter()
        .enumerate()
        .map(|(idx, ty)| QueryColumn {
            name: column_names
                .and_then(|names| names.get(idx))
                .cloned()
                .unwrap_or_else(|| format!("column{}", idx + 1)),
            sql_type: *ty,
            wire_type_oid: None,
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

fn bind_scalar_expression_from_item_with_ctes(
    expr: &SqlExpr,
    display_sql: Option<&str>,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(AnalyzedFrom, BoundScope), ParseError> {
    let call_scope = empty_scope();
    let bound = bind_expr_with_outer_and_ctes(
        expr,
        &call_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    let sql_type = expr_sql_type_hint(&bound).unwrap_or(SqlType::new(SqlTypeKind::Text));
    let column_name = scalar_expression_rte_column_name(expr);
    let output_columns = vec![QueryColumn {
        name: column_name.clone(),
        sql_type,
        wire_type_oid: None,
    }];
    let plan = AnalyzedFrom::project_function(
        vec![bound],
        output_columns,
        display_sql.map(str::to_string),
        Some(column_name.clone()),
    );
    let scope = scope_with_output_exprs(
        scope_for_relation(Some(&column_name), &plan.desc()),
        &plan.output_exprs,
    );
    Ok((plan, scope))
}

fn scalar_expression_rte_column_name(expr: &SqlExpr) -> String {
    match expr {
        SqlExpr::CurrentDate => "current_date",
        SqlExpr::CurrentCatalog => "current_catalog",
        SqlExpr::CurrentSchema => "current_schema",
        SqlExpr::CurrentUser => "current_user",
        SqlExpr::SessionUser => "session_user",
        SqlExpr::CurrentRole => "current_role",
        SqlExpr::CurrentTime { .. } => "current_time",
        SqlExpr::CurrentTimestamp { .. } => "current_timestamp",
        SqlExpr::LocalTime { .. } => "localtime",
        SqlExpr::LocalTimestamp { .. } => "localtimestamp",
        _ => "?column?",
    }
    .to_string()
}

#[derive(Debug, Clone)]
enum ValuesCell<'a> {
    Raw(&'a SqlExpr),
    Bound(Expr),
}

impl<'a> ValuesCell<'a> {
    fn raw_expr(&self) -> Option<&'a SqlExpr> {
        match self {
            ValuesCell::Raw(expr) => Some(expr),
            ValuesCell::Bound(_) => None,
        }
    }

    fn is_null_const(&self) -> bool {
        matches!(self, ValuesCell::Raw(SqlExpr::Const(Value::Null)))
    }

    fn infer_type(
        &self,
        scope: &BoundScope,
        catalog: &dyn CatalogLookup,
        outer_scopes: &[BoundScope],
        grouped_outer: Option<&GroupedOuterScope>,
        ctes: &[BoundCte],
    ) -> SqlType {
        match self {
            ValuesCell::Raw(expr) => infer_sql_expr_type_with_ctes(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
            ValuesCell::Bound(expr) => {
                expr_sql_type_hint(expr).unwrap_or(SqlType::new(SqlTypeKind::Text))
            }
        }
    }

    fn bind(
        &self,
        scope: &BoundScope,
        catalog: &dyn CatalogLookup,
        outer_scopes: &[BoundScope],
        grouped_outer: Option<&GroupedOuterScope>,
        ctes: &[BoundCte],
    ) -> Result<Expr, ParseError> {
        match self {
            ValuesCell::Raw(expr) => bind_expr_with_outer_and_ctes(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ),
            ValuesCell::Bound(expr) => Ok(expr.clone()),
        }
    }
}

fn expand_values_row_exprs<'a>(
    row: &'a [SqlExpr],
    scope: &BoundScope,
    outer_scopes: &[BoundScope],
) -> Result<Vec<ValuesCell<'a>>, ParseError> {
    let mut expanded = Vec::new();
    for expr in row {
        if let Some(relation_name) = relation_row_star_name(expr) {
            let fields = resolve_relation_row_expr_with_outer(scope, outer_scopes, relation_name)
                .ok_or_else(|| ParseError::UnknownColumn(format!("{relation_name}.*")))?;
            expanded.extend(fields.into_iter().map(|(_, expr)| ValuesCell::Bound(expr)));
            continue;
        }
        expanded.push(ValuesCell::Raw(expr));
    }
    Ok(expanded)
}

fn relation_row_star_name(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Column(name) => name.strip_suffix(".*"),
        SqlExpr::FieldSelect { expr, field } if field == "*" => {
            if let SqlExpr::Column(name) = expr.as_ref() {
                Some(name.as_str())
            } else {
                None
            }
        }
        _ => None,
    }
}

fn resolve_common_values_type(left: SqlType, right: SqlType) -> Option<SqlType> {
    if left.is_array && right.is_array {
        return resolve_common_scalar_type(left.element_type(), right.element_type())
            .map(SqlType::array_of);
    }
    resolve_common_scalar_type(left, right)
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
            (!column.hidden || column.qualified_only)
                && column
                    .relation_names
                    .iter()
                    .any(|visible_relation| relation_name_matches(visible_relation, relation))
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
        if scope.columns.iter().any(|column| {
            !column.hidden
                && !column.qualified_only
                && column.output_name.eq_ignore_ascii_case(relation)
        }) {
            return Err(ParseError::MissingFromClauseEntry(normalized_relation));
        }
        return Err(ParseError::UnknownColumn(name.to_string()));
    }

    let mut matches = scope.columns.iter().enumerate().filter(|(_, column)| {
        !column.hidden && !column.qualified_only && column.output_name.eq_ignore_ascii_case(name)
    });
    let Some(first) = matches.next() else {
        return Err(ParseError::UnknownColumn(name.to_string()));
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
    let (varattno, system_type) = if column_name.eq_ignore_ascii_case("tableoid") {
        (TABLE_OID_ATTR_NO, SqlType::new(SqlTypeKind::Oid))
    } else if column_name.eq_ignore_ascii_case("ctid") {
        (SELF_ITEM_POINTER_ATTR_NO, SqlType::new(SqlTypeKind::Tid))
    } else if column_name.eq_ignore_ascii_case("xmin") {
        (XMIN_ATTR_NO, SqlType::new(SqlTypeKind::Xid))
    } else if column_name.eq_ignore_ascii_case("xmax") {
        (XMAX_ATTR_NO, SqlType::new(SqlTypeKind::Xid))
    } else {
        return Ok(None);
    };
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
                varattno,
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
            varattno,
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
    let mut hidden_invalid_error = None;
    match resolve_column(scope, name) {
        Ok(index) => return Ok(ResolvedColumn::Local(index)),
        Err(ParseError::AmbiguousColumn(name)) => return Err(ParseError::AmbiguousColumn(name)),
        Err(ParseError::UnknownColumn(_)) => {}
        Err(err @ ParseError::InvalidFromClauseReference(_)) => {
            hidden_invalid_error = Some(err);
        }
        Err(other) => return Err(other),
    }

    if let Some((relation, _)) = name.rsplit_once('.') {
        let visible_outer_matches = outer_scopes
            .iter()
            .filter(|outer_scope| scope_has_visible_relation(outer_scope, relation))
            .count();
        if visible_outer_matches > 1 {
            return Err(ParseError::DetailedError {
                message: format!("table reference \"{relation}\" is ambiguous"),
                detail: None,
                hint: None,
                sqlstate: "42P09",
            });
        }
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

    Err(hidden_invalid_error.unwrap_or_else(|| ParseError::UnknownColumn(name.to_string())))
}

fn scope_has_visible_relation(scope: &BoundScope, name: &str) -> bool {
    scope.relations.iter().any(|relation| {
        relation
            .relation_names
            .iter()
            .any(|relation_name| relation_name.eq_ignore_ascii_case(name))
    }) || scope.columns.iter().any(|column| {
        column
            .relation_names
            .iter()
            .any(|relation_name| relation_name.eq_ignore_ascii_case(name))
    })
}

pub(super) fn resolve_relation_row_expr_with_outer(
    scope: &BoundScope,
    outer_scopes: &[BoundScope],
    name: &str,
) -> Option<Vec<(String, Expr)>> {
    resolve_relation_row_expr_ref_with_outer(scope, outer_scopes, name)
        .map(|resolved| resolved.fields)
}

pub(super) fn resolve_relation_row_expr_ref_with_outer(
    scope: &BoundScope,
    outer_scopes: &[BoundScope],
    name: &str,
) -> Option<ResolvedRelationRowExpr> {
    if rule_pseudo_varno_for_name(name).is_some()
        && let Some(resolved) = resolve_rule_relation_row_expr_in_outer(outer_scopes, name)
    {
        return Some(resolved);
    }
    resolve_relation_row_expr_in_scope(scope, name).or_else(|| {
        outer_scopes.iter().enumerate().find_map(|(depth, scope)| {
            let resolved = resolve_relation_row_expr_in_scope(scope, name)?;
            let is_rule_pseudo = scope_has_rule_pseudo_relation(scope, name);
            Some(ResolvedRelationRowExpr {
                fields: resolved
                    .fields
                    .into_iter()
                    .map(|(field, expr)| {
                        let expr = if is_rule_pseudo {
                            expr
                        } else {
                            raise_expr_varlevels(expr, depth + 1)
                        };
                        (field, expr)
                    })
                    .collect(),
                relation_oid: resolved.relation_oid,
            })
        })
    })
}

fn resolve_rule_relation_row_expr_in_outer(
    outer_scopes: &[BoundScope],
    name: &str,
) -> Option<ResolvedRelationRowExpr> {
    outer_scopes.iter().find_map(|scope| {
        if scope_has_rule_pseudo_relation(scope, name) {
            resolve_relation_row_expr_in_scope(scope, name)
        } else {
            None
        }
    })
}

fn scope_has_rule_pseudo_relation(scope: &BoundScope, name: &str) -> bool {
    let Some(varno) = rule_pseudo_varno_for_name(name) else {
        return false;
    };
    scope.relations.iter().any(|relation| {
        relation.system_varno == Some(varno)
            && relation
                .relation_names
                .iter()
                .any(|relation_name| relation_name_matches(relation_name, name))
    })
}

fn rule_pseudo_varno_for_name(name: &str) -> Option<usize> {
    if name.eq_ignore_ascii_case("old") {
        Some(RULE_OLD_VAR)
    } else if name.eq_ignore_ascii_case("new") {
        Some(RULE_NEW_VAR)
    } else {
        None
    }
}

pub(super) fn relation_row_reference_level(
    scope: &BoundScope,
    outer_scopes: &[BoundScope],
    name: &str,
) -> Option<usize> {
    if resolve_relation_row_expr_in_scope(scope, name).is_some() {
        return Some(0);
    }
    outer_scopes
        .iter()
        .position(|outer_scope| resolve_relation_row_expr_in_scope(outer_scope, name).is_some())
        .map(|depth| depth + 1)
}

fn resolve_relation_row_expr_in_scope(
    scope: &BoundScope,
    name: &str,
) -> Option<ResolvedRelationRowExpr> {
    let matched_relation = scope.relations.iter().find(|relation| {
        relation
            .relation_names
            .iter()
            .any(|relation_name| relation_name_matches(relation_name, name))
    });
    let relation_exists = matched_relation.is_some();
    let mut matched = false;
    let fields = scope
        .columns
        .iter()
        .zip(scope.output_exprs.iter())
        .filter_map(|(column, expr)| {
            if (column.hidden && !column.qualified_only)
                || !column
                    .relation_names
                    .iter()
                    .any(|relation| relation_name_matches(relation, name))
            {
                return None;
            }
            matched = true;
            Some((column.output_name.clone(), expr.clone()))
        })
        .collect::<Vec<_>>();
    (matched || relation_exists).then_some(ResolvedRelationRowExpr {
        fields,
        relation_oid: matched_relation.and_then(|relation| relation.relation_oid),
    })
}

fn relation_name_matches(visible_relation: &str, requested_relation: &str) -> bool {
    if visible_relation.eq_ignore_ascii_case(requested_relation) {
        return true;
    }
    requested_relation
        .rsplit_once('.')
        .is_some_and(|(_, requested_base)| visible_relation.eq_ignore_ascii_case(requested_base))
}

fn from_item_is_lateral(item: &FromItem) -> bool {
    match item {
        FromItem::Lateral(_) => true,
        FromItem::Expression { .. } => true,
        FromItem::FunctionCall { .. } => true,
        FromItem::RowsFrom { .. } => true,
        FromItem::JsonTable(_) => true,
        FromItem::XmlTable(_) => true,
        FromItem::Alias { source, .. } => from_item_is_lateral(source),
        FromItem::TableSample { source, .. } => from_item_is_lateral(source),
        _ => false,
    }
}

fn from_item_contains_lateral(item: &FromItem) -> bool {
    if from_item_is_lateral(item) {
        return true;
    }
    match item {
        FromItem::Join { left, right, .. } => {
            from_item_contains_lateral(left) || from_item_contains_lateral(right)
        }
        _ => false,
    }
}

fn invalid_lateral_outer_scope(mut scope: BoundScope) -> BoundScope {
    for column in &mut scope.columns {
        for name in std::mem::take(&mut column.relation_names) {
            if !column
                .hidden_invalid_relation_names
                .iter()
                .any(|hidden| hidden.eq_ignore_ascii_case(&name))
            {
                column.hidden_invalid_relation_names.push(name);
            }
        }
    }
    for relation in &mut scope.relations {
        for name in std::mem::take(&mut relation.relation_names) {
            if !relation
                .hidden_invalid_relation_names
                .iter()
                .any(|hidden| hidden.eq_ignore_ascii_case(&name))
            {
                relation.hidden_invalid_relation_names.push(name);
            }
        }
        relation.system_varno = None;
    }
    scope
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

fn bind_relation_from_entry(
    name: &str,
    only: bool,
    catalog: &dyn CatalogLookup,
    entry: BoundRelation,
) -> Result<(AnalyzedFrom, BoundScope), ParseError> {
    if !matches!(entry.relkind, 'r' | 'p' | 'v' | 'm' | 'S' | 't' | 'f') {
        return Err(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table, view, materialized view, sequence, or TOAST table",
        });
    }
    if entry.relkind == 'f' {
        validate_foreign_table_scan_handler(catalog, entry.relation_oid)?;
    }
    let desc = entry.desc.clone();
    let mut plan = AnalyzedFrom::relation(
        name.to_string(),
        entry.rel,
        entry.relation_oid,
        entry.relkind,
        entry.relispopulated,
        entry.toast,
        !only && matches!(entry.relkind, 'r' | 'p'),
        desc.clone(),
    );
    plan.output_exprs = generated_relation_output_exprs(&desc, catalog)?;
    Ok((
        plan,
        scope_for_base_relation_with_generated(name, &desc, Some(entry.relation_oid), catalog)?,
    ))
}

fn is_physical_pg_type_relation_name(name: &str) -> bool {
    // :HACK: Dynamic type DDL is still exposed through the synthetic pg_type
    // alias; qualified catalog references can use physical storage for indexed
    // catalog joins.
    name.eq_ignore_ascii_case("pg_catalog.pg_type")
}

fn passthrough_cte_target<'a>(cte: &'a BoundCte, ctes: &'a [BoundCte]) -> Option<&'a BoundCte> {
    let [rte] = cte.plan.rtable.as_slice() else {
        return None;
    };
    let crate::include::nodes::parsenodes::RangeTblEntryKind::Cte { cte_id, .. } = rte.kind else {
        return None;
    };
    if !matches!(
        cte.plan.jointree,
        Some(crate::include::nodes::parsenodes::JoinTreeNode::RangeTblRef(1))
    ) {
        return None;
    }
    if cte.plan.target_list.len() != cte.desc.columns.len() {
        return None;
    }
    for (index, target) in cte.plan.target_list.iter().enumerate() {
        let Expr::Var(var) = &target.expr else {
            return None;
        };
        if var.varno != 1 || var.varattno != user_attrno(index) || target.resjunk {
            return None;
        }
    }
    ctes.iter().find(|candidate| candidate.cte_id == cte_id)
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
                            wire_type_oid: None,
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
                let scan_cte = passthrough_cte_target(cte, ctes).unwrap_or(cte);
                let mut plan = AnalyzedFrom::cte_scan(
                    scan_cte.name.clone(),
                    scan_cte.cte_id,
                    scan_cte.plan.clone(),
                );
                if scan_cte.cte_id != cte.cte_id
                    && let Some(rte) = plan.rtable.last_mut()
                {
                    rte.alias_preserves_source_names = true;
                }
                return Ok((
                    plan.clone(),
                    scope_with_output_exprs(
                        scope_for_relation(Some(name), &cte.desc),
                        &plan.output_exprs,
                    ),
                ));
            }
            if is_physical_pg_type_relation_name(name)
                && let Some(entry) = catalog.lookup_relation_by_oid(PG_TYPE_RELATION_OID)
            {
                return bind_relation_from_entry(name, *only, catalog, entry);
            }
            if let Some(bound) = bind_builtin_system_view(name, catalog) {
                return Ok(bound);
            }
            let entry = catalog
                .lookup_any_relation(name)
                .ok_or_else(|| ParseError::UnknownTable(name.to_string()))?;
            bind_relation_from_entry(name, *only, catalog, entry)
        }
        FromItem::Values { rows } => {
            bind_values_rows(rows, None, catalog, outer_scopes, grouped_outer, ctes)
        }
        FromItem::Expression { expr, display_sql } => bind_scalar_expression_from_item_with_ctes(
            expr,
            display_sql.as_deref(),
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        ),
        FromItem::FunctionCall {
            name,
            args,
            func_variadic,
            with_ordinality,
        } => {
            let (plan, scope, _) = bind_function_from_item_with_ctes(
                name,
                args,
                *func_variadic,
                *with_ordinality,
                None,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            Ok((plan, scope))
        }
        FromItem::RowsFrom {
            functions,
            with_ordinality,
        } => bind_rows_from_item_with_ctes(
            functions,
            *with_ordinality,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        ),
        FromItem::JsonTable(table) => bind_sql_json_table_from_item_with_ctes(
            table,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        ),
        FromItem::XmlTable(table) => bind_sql_xml_table_from_item_with_ctes(
            table,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        ),
        FromItem::DerivedTable(select) => {
            if select
                .with
                .iter()
                .any(|cte| super::cte_body_is_modifying(&cte.body))
            {
                return Err(ParseError::FeatureNotSupportedMessage(
                    "WITH clause containing a data-modifying statement must be at the top level"
                        .into(),
                ));
            }
            reject_from_subselect_outer_aggregates(
                select,
                catalog,
                outer_scopes,
                grouped_outer.cloned(),
                ctes,
                expanded_views,
            )?;
            let visible_agg_scope = current_visible_aggregate_scope();
            let (plan, _) = if select.set_operation.is_some() {
                analyze_select_query_with_outer(
                    select,
                    catalog,
                    outer_scopes,
                    grouped_outer.cloned(),
                    visible_agg_scope.as_ref(),
                    ctes,
                    expanded_views,
                )?
            } else {
                analyze_select_query_with_outer(
                    select,
                    catalog,
                    outer_scopes,
                    None,
                    visible_agg_scope.as_ref(),
                    ctes,
                    expanded_views,
                )?
            };
            let bound = AnalyzedFrom::subquery(plan);
            let desc = synthetic_desc_from_analyzed_from(&bound);
            Ok((
                bound.clone(),
                scope_with_output_exprs(scope_for_relation(None, &desc), &bound.output_exprs),
            ))
        }
        FromItem::Lateral(source) => match source.as_ref() {
            FromItem::DerivedTable(select) => {
                if select
                    .with
                    .iter()
                    .any(|cte| super::cte_body_is_modifying(&cte.body))
                {
                    return Err(ParseError::FeatureNotSupportedMessage(
                        "WITH clause containing a data-modifying statement must be at the top level"
                            .into(),
                    ));
                }
                reject_from_subselect_outer_aggregates(
                    select,
                    catalog,
                    outer_scopes,
                    grouped_outer.cloned(),
                    ctes,
                    expanded_views,
                )?;
                let visible_agg_scope = current_visible_aggregate_scope();
                let (plan, _) = analyze_select_query_with_outer(
                    select,
                    catalog,
                    outer_scopes,
                    grouped_outer.cloned(),
                    visible_agg_scope.as_ref(),
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
            if from_item_contains_lateral(right) {
                let lateral_scope = if matches!(kind, JoinKind::Right | JoinKind::Full) {
                    invalid_lateral_outer_scope(left_scope.clone())
                } else {
                    left_scope.clone()
                };
                right_outer_scopes.insert(0, lateral_scope);
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
        FromItem::TableSample { source, sample } => {
            let (mut plan, scope) = bind_from_item_with_ctes(
                source,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
                expanded_views,
            )?;
            let call_scope = empty_scope();
            let args = sample
                .args
                .iter()
                .map(|expr| {
                    bind_expr_with_outer_and_ctes(
                        expr,
                        &call_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let repeatable = sample
                .repeatable
                .as_ref()
                .map(|expr| {
                    bind_expr_with_outer_and_ctes(
                        expr,
                        &call_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                })
                .transpose()?;
            let sample_qual = table_sample_qual(&sample.method, &args, repeatable.as_ref())?;
            attach_table_sample(
                &mut plan,
                TableSampleClause {
                    method: sample.method.clone(),
                    args,
                    repeatable,
                },
                sample_qual,
            )?;
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
                    with_ordinality,
                } => Some((
                    name.as_str(),
                    args.as_slice(),
                    *func_variadic,
                    *with_ordinality,
                )),
                FromItem::Lateral(inner) => match inner.as_ref() {
                    FromItem::FunctionCall {
                        name,
                        args,
                        func_variadic,
                        with_ordinality,
                    } => Some((
                        name.as_str(),
                        args.as_slice(),
                        *func_variadic,
                        *with_ordinality,
                    )),
                    _ => None,
                },
                _ => None,
            };
            let (plan, scope, alias_single_function_output) =
                if let Some((name, args, func_variadic, with_ordinality)) = function_source {
                    let typed_defs = match column_aliases {
                        AliasColumnSpec::Definitions(defs) => Some(defs.as_slice()),
                        AliasColumnSpec::None | AliasColumnSpec::Names(_) => None,
                    };
                    bind_function_from_item_with_ctes(
                        name,
                        args,
                        func_variadic,
                        with_ordinality,
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
                    let scalar_expression_source = match source.as_ref() {
                        FromItem::Expression { .. } => true,
                        FromItem::Lateral(inner) => {
                            matches!(inner.as_ref(), FromItem::Expression { .. })
                        }
                        _ => false,
                    };
                    (plan, scope, scalar_expression_source)
                };
            if *preserve_source_names
                && column_aliases.is_empty()
                && let FromItem::Join {
                    constraint: JoinConstraint::Using(columns),
                    ..
                } = source.as_ref()
            {
                return apply_join_using_alias(plan, scope, alias, columns.len());
            }
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

fn attach_table_sample(
    plan: &mut AnalyzedFrom,
    sample: TableSampleClause,
    sample_qual: Expr,
) -> Result<(), ParseError> {
    let relation_indexes = plan
        .rtable
        .iter()
        .enumerate()
        .filter_map(|(index, rte)| {
            matches!(rte.kind, RangeTblEntryKind::Relation { .. }).then_some(index)
        })
        .collect::<Vec<_>>();
    if relation_indexes.len() != 1 {
        return Err(ParseError::FeatureNotSupported(
            "TABLESAMPLE on non-table relation".into(),
        ));
    }
    let rte = &mut plan.rtable[relation_indexes[0]];
    let RangeTblEntryKind::Relation {
        relkind,
        tablesample,
        ..
    } = &mut rte.kind
    else {
        unreachable!();
    };
    if !matches!(*relkind, 'r' | 'm' | 'p') {
        return Err(ParseError::DetailedError {
            message: "TABLESAMPLE clause can only be applied to tables and materialized views"
                .into(),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    }
    *tablesample = Some(sample);
    // :HACK: Store TABLESAMPLE as a relation security qual so it is evaluated
    // before normal WHERE predicates, matching the rowsecurity regression path
    // without introducing a full SampleScan plan node yet.
    rte.security_quals.insert(0, sample_qual);
    Ok(())
}

fn table_sample_qual(
    method: &str,
    args: &[Expr],
    repeatable: Option<&Expr>,
) -> Result<Expr, ParseError> {
    let normalized_method = method.to_ascii_lowercase();
    if !matches!(normalized_method.as_str(), "bernoulli" | "system") {
        return Err(ParseError::FeatureNotSupported(format!(
            "TABLESAMPLE method {method}"
        )));
    }
    let [percent_arg] = args else {
        return Err(ParseError::DetailedError {
            message: format!(
                "tablesample method {normalized_method} requires 1 argument, not {}",
                args.len()
            ),
            detail: None,
            hint: None,
            sqlstate: "42804",
        });
    };
    // :HACK: Lower SYSTEM to the same deterministic row-level predicate as
    // BERNOULLI until pgrust has block-level sample scan support.
    let percent = table_sample_float_expr(percent_arg.clone());
    let seed = repeatable
        .cloned()
        .map(table_sample_float_expr)
        .unwrap_or(Expr::Const(Value::Float64(0.0)));
    Ok(Expr::Func(Box::new(FuncExpr {
        funcid: 0,
        funcname: Some("pgrust_tablesample_bernoulli".into()),
        funcresulttype: Some(SqlType::new(SqlTypeKind::Bool)),
        funcvariadic: false,
        implementation: ScalarFunctionImpl::Builtin(
            BuiltinScalarFunction::PgRustTablesampleBernoulli,
        ),
        display_args: None,
        args: vec![
            Expr::Var(Var {
                varno: 1,
                varattno: SELF_ITEM_POINTER_ATTR_NO,
                varlevelsup: 0,
                vartype: SqlType::new(SqlTypeKind::Tid),
            }),
            percent,
            seed,
        ],
    })))
}

fn table_sample_float_expr(expr: Expr) -> Expr {
    match expr {
        Expr::Const(Value::Int16(value)) => Expr::Const(Value::Float64(f64::from(value))),
        Expr::Const(Value::Int32(value)) => Expr::Const(Value::Float64(f64::from(value))),
        Expr::Const(Value::Int64(value)) => Expr::Const(Value::Float64(value as f64)),
        Expr::Const(Value::Float64(_)) => expr,
        other => Expr::Cast(Box::new(other), SqlType::new(SqlTypeKind::Float8)),
    }
}

fn bind_rows_from_item_with_ctes(
    functions: &[RowsFromFunction],
    with_ordinality: bool,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(AnalyzedFrom, BoundScope), ParseError> {
    let mut items = Vec::with_capacity(functions.len());
    let mut output_columns = Vec::new();
    for function in functions {
        let typed_defs = (!function.column_definitions.is_empty())
            .then_some(function.column_definitions.as_slice());
        let (plan, _, _) = bind_function_from_item_with_ctes(
            &function.name,
            &function.args,
            function.func_variadic,
            false,
            typed_defs,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?;
        let source = match plan.rtable.as_slice() {
            [rte] => match &rte.kind {
                RangeTblEntryKind::Function { call } => RowsFromSource::Function(call.clone()),
                RangeTblEntryKind::Subquery { query } => RowsFromSource::Project {
                    output_exprs: query
                        .target_list
                        .iter()
                        .map(|target| target.expr.clone())
                        .collect(),
                    output_columns: plan.output_columns.clone(),
                    display_sql: None,
                },
                _ => {
                    return Err(ParseError::UnexpectedToken {
                        expected: "function scan or single-row function projection",
                        actual: "ROWS FROM item".into(),
                    });
                }
            },
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "function scan or single-row function projection",
                    actual: "ROWS FROM item".into(),
                });
            }
        };
        output_columns.extend(plan.output_columns.iter().cloned());
        items.push(RowsFromItem {
            source,
            column_definitions: !function.column_definitions.is_empty(),
        });
    }
    if with_ordinality {
        output_columns.push(QueryColumn {
            name: "ordinality".to_string(),
            sql_type: SqlType::new(SqlTypeKind::Int8),
            wire_type_oid: None,
        });
    }
    let plan = AnalyzedFrom::function(SetReturningCall::RowsFrom {
        items,
        output_columns,
        with_ordinality,
    });
    let desc = synthetic_desc_from_analyzed_from(&plan);
    Ok((
        plan.clone(),
        scope_with_output_exprs(scope_for_relation(None, &desc), &plan.output_exprs),
    ))
}

struct SqlJsonTableBindState {
    columns: Vec<SqlJsonTableColumn>,
    output_columns: Vec<QueryColumn>,
    seen_names: std::collections::BTreeSet<String>,
    next_path_id: usize,
    error_on_error: bool,
}

impl SqlJsonTableBindState {
    fn new(error_on_error: bool) -> Self {
        Self {
            columns: Vec::new(),
            output_columns: Vec::new(),
            seen_names: std::collections::BTreeSet::new(),
            next_path_id: 0,
            error_on_error,
        }
    }

    fn remember_name(&mut self, name: &str) -> Result<(), ParseError> {
        if self.seen_names.insert(name.to_ascii_lowercase()) {
            Ok(())
        } else {
            Err(ParseError::DetailedError {
                message: format!("duplicate JSON_TABLE column or path name: {name}"),
                detail: None,
                hint: None,
                sqlstate: "42710",
            })
        }
    }

    fn next_path_name(&mut self) -> String {
        let name = format!("json_table_path_{}", self.next_path_id);
        self.next_path_id += 1;
        name
    }

    fn push_column(
        &mut self,
        name: String,
        sql_type: SqlType,
        kind: SqlJsonTableColumnKind,
    ) -> Result<usize, ParseError> {
        self.remember_name(&name)?;
        let index = self.columns.len();
        self.output_columns.push(QueryColumn {
            name: name.clone(),
            sql_type,
            wire_type_oid: None,
        });
        self.columns.push(SqlJsonTableColumn {
            name,
            sql_type,
            kind,
        });
        Ok(index)
    }
}

fn bind_sql_xml_table_from_item_with_ctes(
    table: &XmlTableExpr,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(AnalyzedFrom, BoundScope), ParseError> {
    let empty_scope = empty_scope();
    let text_type = SqlType::new(SqlTypeKind::Text);
    let xml_type = SqlType::new(SqlTypeKind::Xml);
    let bind_as = |expr: &SqlExpr, target: SqlType| -> Result<Expr, ParseError> {
        let source = infer_sql_expr_type_with_ctes(
            expr,
            &empty_scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        );
        Ok(coerce_bound_expr(
            bind_expr_with_outer_and_ctes(
                expr,
                &empty_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?,
            source,
            target,
        ))
    };

    let namespaces = table
        .namespaces
        .iter()
        .map(|namespace| {
            if namespace.name.is_none() {
                return Err(ParseError::DetailedError {
                    message: "DEFAULT namespace is not supported".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                });
            }
            Ok(SqlXmlTableNamespace {
                name: namespace.name.clone(),
                uri: bind_as(&namespace.uri, text_type)?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let row_path = bind_as(&table.row_path, text_type)?;
    let document = bind_as(&table.document, xml_type)?;
    let mut seen_names = std::collections::BTreeSet::new();
    let mut ordinality_found = false;
    let mut columns = Vec::new();
    let mut output_columns = Vec::new();
    for column in &table.columns {
        match column {
            XmlTableColumn::Ordinality { name } => {
                if ordinality_found {
                    return Err(ParseError::DetailedError {
                        message: "only one FOR ORDINALITY column is allowed".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "42601",
                    });
                }
                ordinality_found = true;
                push_xml_table_column_name(name, &mut seen_names)?;
                let sql_type = SqlType::new(SqlTypeKind::Int4);
                output_columns.push(QueryColumn {
                    name: name.clone(),
                    sql_type,
                    wire_type_oid: None,
                });
                columns.push(SqlXmlTableColumn {
                    name: name.clone(),
                    sql_type,
                    kind: SqlXmlTableColumnKind::Ordinality,
                });
            }
            XmlTableColumn::Regular {
                name,
                type_name,
                path,
                default,
                not_null,
            } => {
                push_xml_table_column_name(name, &mut seen_names)?;
                let sql_type = resolve_raw_type_name(type_name, catalog)?;
                let path = path
                    .as_ref()
                    .map(|expr| bind_as(expr, text_type))
                    .transpose()?;
                let default = default
                    .as_ref()
                    .map(|expr| bind_as(expr, sql_type))
                    .transpose()?;
                output_columns.push(QueryColumn {
                    name: name.clone(),
                    sql_type,
                    wire_type_oid: None,
                });
                columns.push(SqlXmlTableColumn {
                    name: name.clone(),
                    sql_type,
                    kind: SqlXmlTableColumnKind::Regular {
                        path,
                        default,
                        not_null: *not_null,
                    },
                });
            }
        }
    }

    let desc = RelationDesc {
        columns: output_columns
            .iter()
            .map(|col| column_desc(col.name.clone(), col.sql_type, true))
            .collect(),
    };
    let scope = scope_for_relation(Some("xmltable"), &desc);
    Ok((
        AnalyzedFrom::function(SetReturningCall::SqlXmlTable(SqlXmlTable {
            namespaces,
            row_path,
            document,
            columns,
            output_columns,
        })),
        scope,
    ))
}

fn push_xml_table_column_name(
    name: &str,
    seen_names: &mut std::collections::BTreeSet<String>,
) -> Result<(), ParseError> {
    if seen_names.insert(name.to_string()) {
        Ok(())
    } else {
        Err(ParseError::DetailedError {
            message: format!("column name \"{name}\" is not unique"),
            detail: None,
            hint: None,
            sqlstate: "42601",
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn bind_sql_json_table_from_item_with_ctes(
    table: &JsonTableExpr,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(AnalyzedFrom, BoundScope), ParseError> {
    let empty_scope = empty_scope();
    let context = bind_expr_with_outer_and_ctes(
        &table.context,
        &empty_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    let passing = table
        .passing
        .iter()
        .map(|arg| {
            bind_expr_with_outer_and_ctes(
                &arg.expr,
                &empty_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
            .map(|expr| SqlJsonTablePassingArg {
                name: arg.name.clone(),
                expr,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let on_error = bind_sql_json_table_top_behavior(table.on_error.as_ref())?;
    let error_on_error = matches!(on_error, SqlJsonTableBehavior::Error);
    let mut state = SqlJsonTableBindState::new(error_on_error);
    let root_path_name = table
        .root_path
        .name
        .clone()
        .unwrap_or_else(|| state.next_path_name());
    state.remember_name(&root_path_name)?;
    let plan = bind_sql_json_table_column_group(
        &table.columns,
        table.root_path.path.clone(),
        root_path_name.clone(),
        &mut state,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    let sql_table = SqlJsonTable {
        context,
        root_path: table.root_path.path.clone(),
        root_path_name,
        passing,
        columns: state.columns,
        plan,
        output_columns: state.output_columns.clone(),
        on_error,
    };
    let desc = RelationDesc {
        columns: state
            .output_columns
            .iter()
            .map(|col| column_desc(col.name.clone(), col.sql_type, true))
            .collect(),
    };
    let scope = scope_for_relation(Some("json_table"), &desc);
    Ok((
        AnalyzedFrom::function(SetReturningCall::SqlJsonTable(sql_table)),
        scope,
    ))
}

#[allow(clippy::too_many_arguments)]
fn bind_sql_json_table_column_group(
    columns: &[JsonTableColumn],
    path: String,
    path_name: String,
    state: &mut SqlJsonTableBindState,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<SqlJsonTablePlan, ParseError> {
    let mut column_indexes = Vec::new();
    let mut nested_plans = Vec::new();
    let mut ordinality_found = false;
    for column in columns {
        match column {
            JsonTableColumn::Ordinality { name } => {
                if ordinality_found {
                    return Err(ParseError::DetailedError {
                        message: "only one FOR ORDINALITY column is allowed".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "42601",
                    });
                }
                ordinality_found = true;
                let index = state.push_column(
                    name.clone(),
                    SqlType::new(SqlTypeKind::Int4),
                    SqlJsonTableColumnKind::Ordinality,
                )?;
                column_indexes.push(index);
            }
            JsonTableColumn::Regular {
                name,
                type_name,
                path,
                format_json,
                wrapper,
                quotes,
                on_empty,
                on_error,
            } => {
                let sql_type = resolve_raw_type_name(type_name, catalog)?;
                let path_text = bind_sql_json_table_column_path(name, path, state)?;
                let on_empty = bind_sql_json_table_behavior(
                    on_empty.as_ref().unwrap_or(&JsonTableBehavior::Null),
                    sql_type,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let on_error = bind_sql_json_table_behavior(
                    on_error.as_ref().unwrap_or(&JsonTableBehavior::Null),
                    sql_type,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let planned_wrapper = sql_json_table_wrapper(*wrapper);
                let planned_quotes = sql_json_table_quotes(*quotes);
                if matches!(
                    planned_wrapper,
                    SqlJsonTableWrapper::Conditional | SqlJsonTableWrapper::Unconditional
                ) && matches!(planned_quotes, SqlJsonTableQuotes::Omit)
                {
                    return Err(ParseError::DetailedError {
                        message: "SQL/JSON QUOTES behavior must not be specified when WITH WRAPPER is used".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "42601",
                    });
                }
                let formatted_column = *format_json
                    || sql_json_table_column_is_formatted(sql_type)
                    || !matches!(planned_wrapper, SqlJsonTableWrapper::Unspecified)
                    || !matches!(planned_quotes, SqlJsonTableQuotes::Unspecified);
                validate_sql_json_table_regular_behavior(
                    name,
                    &on_empty,
                    "EMPTY",
                    formatted_column,
                )?;
                validate_sql_json_table_regular_behavior(
                    name,
                    &on_error,
                    "ERROR",
                    formatted_column,
                )?;
                let kind = if formatted_column {
                    SqlJsonTableColumnKind::Formatted {
                        path: path_text,
                        format_json: *format_json,
                        wrapper: planned_wrapper,
                        quotes: planned_quotes,
                        on_empty,
                        on_error,
                    }
                } else {
                    SqlJsonTableColumnKind::Scalar {
                        path: path_text,
                        on_empty,
                        on_error,
                    }
                };
                let index = state.push_column(name.clone(), sql_type, kind)?;
                column_indexes.push(index);
            }
            JsonTableColumn::Exists {
                name,
                type_name,
                path,
                on_error,
            } => {
                let sql_type = resolve_raw_type_name(type_name, catalog)?;
                let path_text = bind_sql_json_table_column_path(name, path, state)?;
                let on_error = bind_sql_json_table_behavior(
                    on_error.as_ref().unwrap_or(&JsonTableBehavior::False),
                    sql_type,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                validate_sql_json_table_exists_behavior(name, &on_error, "ERROR")?;
                let index = state.push_column(
                    name.clone(),
                    sql_type,
                    SqlJsonTableColumnKind::Exists {
                        path: path_text,
                        on_error,
                    },
                )?;
                column_indexes.push(index);
            }
            JsonTableColumn::Nested { .. } => {}
        }
    }
    for column in columns {
        if let JsonTableColumn::Nested { path, columns } = column {
            let path_name = path.name.clone().unwrap_or_else(|| state.next_path_name());
            state.remember_name(&path_name)?;
            nested_plans.push(bind_sql_json_table_column_group(
                columns,
                path.path.clone(),
                path_name,
                state,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?);
        }
    }
    Ok(SqlJsonTablePlan::PathScan {
        path,
        path_name,
        column_indexes,
        error_on_error: state.error_on_error,
        child: fold_sql_json_table_siblings(nested_plans).map(Box::new),
    })
}

fn fold_sql_json_table_siblings(mut plans: Vec<SqlJsonTablePlan>) -> Option<SqlJsonTablePlan> {
    if plans.is_empty() {
        return None;
    }
    let mut plan = plans.remove(0);
    for next in plans {
        plan = SqlJsonTablePlan::SiblingJoin {
            left: Box::new(plan),
            right: Box::new(next),
        };
    }
    Some(plan)
}

fn bind_sql_json_table_column_path(
    column_name: &str,
    path: &Option<JsonTablePathSpec>,
    state: &mut SqlJsonTableBindState,
) -> Result<String, ParseError> {
    if let Some(path) = path {
        if let Some(name) = &path.name {
            state.remember_name(name)?;
        }
        Ok(path.path.clone())
    } else {
        Ok(format!("$.{}", serde_json::to_string(column_name).unwrap()))
    }
}

fn bind_sql_json_table_top_behavior(
    behavior: Option<&JsonTableBehavior>,
) -> Result<SqlJsonTableBehavior, ParseError> {
    match behavior.unwrap_or(&JsonTableBehavior::Empty) {
        JsonTableBehavior::Empty => Ok(SqlJsonTableBehavior::Empty),
        JsonTableBehavior::EmptyArray => Ok(SqlJsonTableBehavior::EmptyArray),
        JsonTableBehavior::Error => Ok(SqlJsonTableBehavior::Error),
        _ => Err(ParseError::DetailedError {
            message: "invalid ON ERROR behavior".into(),
            detail: Some(
                "Only EMPTY [ ARRAY ] or ERROR is allowed in the top-level ON ERROR clause.".into(),
            ),
            hint: None,
            sqlstate: "42601",
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn bind_sql_json_table_behavior(
    behavior: &JsonTableBehavior,
    target_type: SqlType,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<SqlJsonTableBehavior, ParseError> {
    Ok(match behavior {
        JsonTableBehavior::Null => SqlJsonTableBehavior::Null,
        JsonTableBehavior::Error => SqlJsonTableBehavior::Error,
        JsonTableBehavior::Empty => SqlJsonTableBehavior::Empty,
        JsonTableBehavior::EmptyArray => SqlJsonTableBehavior::EmptyArray,
        JsonTableBehavior::EmptyObject => SqlJsonTableBehavior::EmptyObject,
        JsonTableBehavior::True => SqlJsonTableBehavior::True,
        JsonTableBehavior::False => SqlJsonTableBehavior::False,
        JsonTableBehavior::Unknown => SqlJsonTableBehavior::Unknown,
        JsonTableBehavior::Default(expr) => {
            let scope = empty_scope();
            let raw_type = infer_sql_expr_type_with_ctes(
                expr,
                &scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let bound = bind_expr_with_outer_and_ctes(
                expr,
                &scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            SqlJsonTableBehavior::Default(coerce_bound_expr(bound, raw_type, target_type))
        }
    })
}

fn sql_json_table_column_is_formatted(sql_type: SqlType) -> bool {
    sql_type.is_array
        || matches!(
            sql_type.kind,
            SqlTypeKind::Json | SqlTypeKind::Jsonb | SqlTypeKind::Record | SqlTypeKind::Composite
        )
}

fn validate_sql_json_table_regular_behavior(
    column_name: &str,
    behavior: &SqlJsonTableBehavior,
    target: &'static str,
    formatted: bool,
) -> Result<(), ParseError> {
    let valid = if formatted {
        matches!(
            behavior,
            SqlJsonTableBehavior::Error
                | SqlJsonTableBehavior::Null
                | SqlJsonTableBehavior::EmptyArray
                | SqlJsonTableBehavior::EmptyObject
                | SqlJsonTableBehavior::Default(_)
        )
    } else {
        matches!(
            behavior,
            SqlJsonTableBehavior::Error
                | SqlJsonTableBehavior::Null
                | SqlJsonTableBehavior::Default(_)
        )
    };
    if valid {
        return Ok(());
    }
    let detail = if formatted {
        format!(
            "Only ERROR, NULL, EMPTY ARRAY, EMPTY OBJECT, or DEFAULT expression is allowed in ON {target} for formatted columns."
        )
    } else {
        format!(
            "Only ERROR, NULL, or DEFAULT expression is allowed in ON {target} for scalar columns."
        )
    };
    Err(ParseError::DetailedError {
        message: format!("invalid ON {target} behavior for column \"{column_name}\""),
        detail: Some(detail),
        hint: None,
        sqlstate: "42601",
    })
}

fn validate_sql_json_table_exists_behavior(
    column_name: &str,
    behavior: &SqlJsonTableBehavior,
    target: &'static str,
) -> Result<(), ParseError> {
    if matches!(
        behavior,
        SqlJsonTableBehavior::Error
            | SqlJsonTableBehavior::True
            | SqlJsonTableBehavior::False
            | SqlJsonTableBehavior::Unknown
    ) {
        return Ok(());
    }
    Err(ParseError::DetailedError {
        message: format!("invalid ON {target} behavior for column \"{column_name}\""),
        detail: Some(format!(
            "Only ERROR, TRUE, FALSE, or UNKNOWN is allowed in ON {target} for EXISTS columns."
        )),
        hint: None,
        sqlstate: "42601",
    })
}

fn sql_json_table_wrapper(wrapper: JsonTableWrapper) -> SqlJsonTableWrapper {
    match wrapper {
        JsonTableWrapper::Unspecified => SqlJsonTableWrapper::Unspecified,
        JsonTableWrapper::Without => SqlJsonTableWrapper::Without,
        JsonTableWrapper::Conditional => SqlJsonTableWrapper::Conditional,
        JsonTableWrapper::Unconditional => SqlJsonTableWrapper::Unconditional,
    }
}

fn sql_json_table_quotes(quotes: JsonTableQuotes) -> SqlJsonTableQuotes {
    match quotes {
        JsonTableQuotes::Unspecified => SqlJsonTableQuotes::Unspecified,
        JsonTableQuotes::Keep => SqlJsonTableQuotes::Keep,
        JsonTableQuotes::Omit => SqlJsonTableQuotes::Omit,
    }
}

fn validate_foreign_table_scan_handler(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Result<(), ParseError> {
    let foreign_table = catalog
        .foreign_table_rows()
        .into_iter()
        .find(|row| row.ftrelid == relation_oid)
        .ok_or_else(|| ParseError::DetailedError {
            message: format!("cache lookup failed for foreign table {relation_oid}"),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })?;
    let server = catalog
        .foreign_server_rows()
        .into_iter()
        .find(|row| row.oid == foreign_table.ftserver)
        .ok_or_else(|| ParseError::DetailedError {
            message: format!(
                "cache lookup failed for foreign server {}",
                foreign_table.ftserver
            ),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })?;
    let fdw = catalog
        .foreign_data_wrapper_rows()
        .into_iter()
        .find(|row| row.oid == server.srvfdw)
        .ok_or_else(|| ParseError::DetailedError {
            message: format!(
                "cache lookup failed for foreign-data wrapper {}",
                server.srvfdw
            ),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })?;
    if fdw.fdwhandler == 0 {
        return Err(ParseError::DetailedError {
            message: format!("foreign-data wrapper \"{}\" has no handler", fdw.fdwname),
            detail: None,
            hint: None,
            sqlstate: "HV00N",
        });
    }

    Err(ParseError::FeatureNotSupportedMessage(
        "foreign table scans".into(),
    ))
}

fn nested_from_function_srf_error() -> ParseError {
    ParseError::FeatureNotSupportedMessage(
        "set-returning functions must appear at top level of FROM".into(),
    )
}

fn reject_nested_from_function_srfs(
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(), ParseError> {
    if args.iter().any(|arg| {
        sql_expr_contains_set_returning_call(arg, scope, catalog, outer_scopes, grouped_outer, ctes)
    }) {
        return Err(nested_from_function_srf_error());
    }
    Ok(())
}

fn sql_expr_contains_set_returning_call(
    expr: &SqlExpr,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> bool {
    match expr {
        SqlExpr::Column(_)
        | SqlExpr::Parameter(_)
        | SqlExpr::ParamRef(_)
        | SqlExpr::Default
        | SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::ScalarSubquery(_)
        | SqlExpr::ArraySubquery(_)
        | SqlExpr::Exists(_)
        | SqlExpr::Random
        | SqlExpr::CurrentDate
        | SqlExpr::CurrentCatalog
        | SqlExpr::CurrentSchema
        | SqlExpr::CurrentUser
        | SqlExpr::SessionUser
        | SqlExpr::CurrentRole
        | SqlExpr::CurrentTime { .. }
        | SqlExpr::CurrentTimestamp { .. }
        | SqlExpr::LocalTime { .. }
        | SqlExpr::LocalTimestamp { .. } => false,
        SqlExpr::FuncCall {
            name,
            args,
            order_by,
            within_group,
            func_variadic,
            filter,
            over,
            ..
        } => {
            root_call_returns_set(
                name,
                args.args(),
                *func_variadic,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) || args.args().iter().any(|arg| {
                sql_expr_contains_set_returning_call(
                    &arg.value,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            }) || order_by.iter().any(|item| {
                sql_expr_contains_set_returning_call(
                    &item.expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            }) || within_group.as_deref().is_some_and(|items| {
                items.iter().any(|item| {
                    sql_expr_contains_set_returning_call(
                        &item.expr,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                })
            }) || filter.as_deref().is_some_and(|expr| {
                sql_expr_contains_set_returning_call(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            }) || over.as_ref().is_some_and(|spec| {
                raw_window_spec_contains_set_returning_call(
                    spec,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            })
        }
        SqlExpr::InSubquery { expr, .. } => sql_expr_contains_set_returning_call(
            expr,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        ),
        SqlExpr::QuantifiedSubquery { left, .. } => sql_expr_contains_set_returning_call(
            left,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        ),
        SqlExpr::ArrayLiteral(elements) | SqlExpr::Row(elements) => elements.iter().any(|expr| {
            sql_expr_contains_set_returning_call(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        }),
        SqlExpr::BinaryOperator { left, right, .. }
        | SqlExpr::ArrayOverlap(left, right)
        | SqlExpr::ArrayContains(left, right)
        | SqlExpr::ArrayContained(left, right)
        | SqlExpr::QuantifiedArray {
            left, array: right, ..
        }
        | SqlExpr::JsonGet(left, right)
        | SqlExpr::JsonGetText(left, right)
        | SqlExpr::JsonPath(left, right)
        | SqlExpr::JsonPathText(left, right)
        | SqlExpr::JsonbContains(left, right)
        | SqlExpr::JsonbContained(left, right)
        | SqlExpr::JsonbExists(left, right)
        | SqlExpr::JsonbExistsAny(left, right)
        | SqlExpr::JsonbExistsAll(left, right)
        | SqlExpr::JsonbPathExists(left, right)
        | SqlExpr::JsonbPathMatch(left, right)
        | SqlExpr::Add(left, right)
        | SqlExpr::Sub(left, right)
        | SqlExpr::BitAnd(left, right)
        | SqlExpr::BitOr(left, right)
        | SqlExpr::BitXor(left, right)
        | SqlExpr::Shl(left, right)
        | SqlExpr::Shr(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right)
        | SqlExpr::Mod(left, right)
        | SqlExpr::Concat(left, right)
        | SqlExpr::Eq(left, right)
        | SqlExpr::NotEq(left, right)
        | SqlExpr::Lt(left, right)
        | SqlExpr::LtEq(left, right)
        | SqlExpr::Gt(left, right)
        | SqlExpr::GtEq(left, right)
        | SqlExpr::RegexMatch(left, right)
        | SqlExpr::And(left, right)
        | SqlExpr::Or(left, right)
        | SqlExpr::IsDistinctFrom(left, right)
        | SqlExpr::IsNotDistinctFrom(left, right)
        | SqlExpr::Overlaps(left, right) => {
            sql_expr_contains_set_returning_call(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) || sql_expr_contains_set_returning_call(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        }
        SqlExpr::AtTimeZone { expr, zone } => {
            sql_expr_contains_set_returning_call(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) || sql_expr_contains_set_returning_call(
                zone,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        }
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            sql_expr_contains_set_returning_call(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) || sql_expr_contains_set_returning_call(
                pattern,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) || escape.as_deref().is_some_and(|expr| {
                sql_expr_contains_set_returning_call(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            })
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            arg.as_deref().is_some_and(|expr| {
                sql_expr_contains_set_returning_call(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            }) || args.iter().any(|arm| {
                sql_expr_contains_set_returning_call(
                    &arm.expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                ) || sql_expr_contains_set_returning_call(
                    &arm.result,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            }) || defresult.as_deref().is_some_and(|expr| {
                sql_expr_contains_set_returning_call(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )
            })
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            sql_expr_contains_set_returning_call(
                array,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) || subscripts.iter().any(|subscript| {
                subscript.lower.as_deref().is_some_and(|expr| {
                    sql_expr_contains_set_returning_call(
                        expr,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                }) || subscript.upper.as_deref().is_some_and(|expr| {
                    sql_expr_contains_set_returning_call(
                        expr,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )
                })
            })
        }
        SqlExpr::Cast(inner, _)
        | SqlExpr::Collate { expr: inner, .. }
        | SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::BitNot(inner)
        | SqlExpr::Not(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::GeometryUnaryOp { expr: inner, .. }
        | SqlExpr::PrefixOperator { expr: inner, .. }
        | SqlExpr::FieldSelect { expr: inner, .. }
        | SqlExpr::Subscript { expr: inner, .. } => sql_expr_contains_set_returning_call(
            inner,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        ),
        SqlExpr::GeometryBinaryOp { left, right, .. } => {
            sql_expr_contains_set_returning_call(
                left,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            ) || sql_expr_contains_set_returning_call(
                right,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        }
        SqlExpr::Xml(xml) => xml.child_exprs().any(|expr| {
            sql_expr_contains_set_returning_call(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        }),
        SqlExpr::JsonQueryFunction(func) => func.child_exprs().iter().any(|expr| {
            sql_expr_contains_set_returning_call(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        }),
    }
}

fn raw_window_spec_contains_set_returning_call(
    spec: &RawWindowSpec,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> bool {
    spec.partition_by.iter().any(|expr| {
        sql_expr_contains_set_returning_call(
            expr,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )
    }) || spec.order_by.iter().any(|item| {
        sql_expr_contains_set_returning_call(
            &item.expr,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )
    }) || spec.frame.as_deref().is_some_and(|frame| {
        raw_window_frame_bound_contains_set_returning_call(
            &frame.start_bound,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        ) || raw_window_frame_bound_contains_set_returning_call(
            &frame.end_bound,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )
    })
}

fn raw_window_frame_bound_contains_set_returning_call(
    bound: &RawWindowFrameBound,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> bool {
    match bound {
        RawWindowFrameBound::OffsetPreceding(expr) | RawWindowFrameBound::OffsetFollowing(expr) => {
            sql_expr_contains_set_returning_call(
                expr,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        }
        RawWindowFrameBound::UnboundedPreceding
        | RawWindowFrameBound::CurrentRow
        | RawWindowFrameBound::UnboundedFollowing => false,
    }
}

#[allow(clippy::too_many_arguments)]
fn bind_function_from_item_with_ctes(
    name: &str,
    args: &[SqlFunctionArg],
    func_variadic: bool,
    with_ordinality: bool,
    column_definitions: Option<&[AliasColumnDef]>,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(AnalyzedFrom, BoundScope, bool), ParseError> {
    let raw_args = args;
    let has_named_args = raw_args.iter().any(|arg| arg.name.is_some());
    let args = match lower_named_table_function_args(name, raw_args) {
        Ok(args) => args,
        Err(err) if has_named_args => {
            if let Some(bound) = try_bind_user_defined_function_from_item_with_arg_defaults(
                name,
                raw_args,
                func_variadic,
                with_ordinality,
                column_definitions,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )? {
                return Ok(bound);
            }
            return Err(err);
        }
        Err(err) => return Err(err),
    };
    let call_scope = empty_scope();
    reject_nested_from_function_srfs(
        &args,
        &call_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    if resolve_json_record_function(name).is_some() {
        let bound = bind_json_record_from_item(
            name,
            &args,
            column_definitions,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?;
        return Ok(bound);
    }
    let actual_types = args
        .iter()
        .map(|arg| {
            super::infer::infer_sql_expr_function_arg_type_with_ctes(
                arg,
                &call_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        })
        .collect::<Vec<_>>();
    let resolved_result = resolve_function_call(catalog, name, &actual_types, func_variadic);
    if resolved_result.is_err() {
        let positional_args = args
            .iter()
            .cloned()
            .map(|value| SqlFunctionArg { name: None, value })
            .collect::<Vec<_>>();
        if let Some(bound) = try_bind_user_defined_function_from_item_with_arg_defaults(
            name,
            &positional_args,
            func_variadic,
            with_ordinality,
            column_definitions,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )? {
            return Ok(bound);
        }
    }
    let resolved_error = resolved_result.as_ref().err().cloned();
    let resolved = resolved_result.ok();
    let resolved_proc_oid = resolved.as_ref().map(|call| call.proc_oid).unwrap_or(0);
    let resolved_func_variadic = resolved
        .as_ref()
        .map(|call| call.func_variadic)
        .unwrap_or(func_variadic);
    let resolved_row_columns =
        resolve_function_row_columns(catalog, resolved.as_ref(), column_definitions)?;
    let lowered_name = name.to_ascii_lowercase();
    let builtin_name = normalize_builtin_function_name(&lowered_name);

    match builtin_name {
        "generate_series" => {
            if args.len() < 2 || args.len() > 4 {
                return Err(ParseError::UnexpectedToken {
                    expected: "generate_series(start, stop, step[, timezone])",
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
            let raw_step_type = if args.len() >= 3 {
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
            let step_type = raw_step_type.map(|inferred| {
                let has_timestamp_bound = matches!(
                    start_type.kind,
                    SqlTypeKind::Timestamp | SqlTypeKind::TimestampTz
                ) || matches!(
                    stop_type.kind,
                    SqlTypeKind::Timestamp | SqlTypeKind::TimestampTz
                );
                if has_timestamp_bound {
                    coerce_unknown_string_literal_type(
                        &args[2],
                        inferred,
                        SqlType::new(SqlTypeKind::Interval),
                    )
                } else {
                    inferred
                }
            });
            let timezone_type = if args.len() == 4 {
                Some(infer_sql_expr_type_with_ctes(
                    &args[3],
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
            if timezone_type.is_some() && !matches!(common.kind, SqlTypeKind::TimestampTz) {
                return Err(ParseError::UnexpectedToken {
                    expected: "generate_series timestamptz arguments with timezone",
                    actual: sql_type_name(common),
                });
            }
            let step = if args.len() >= 3 {
                let step_expr = bind_expr_with_outer_and_ctes(
                    &args[2],
                    &call_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let step_type = raw_step_type.expect("generate_series step type");
                let step_target = if matches!(
                    common.kind,
                    SqlTypeKind::Timestamp | SqlTypeKind::TimestampTz
                ) {
                    SqlType::new(SqlTypeKind::Interval)
                } else {
                    common
                };
                coerce_bound_expr(step_expr, step_type, step_target)
            } else {
                match common.kind {
                    SqlTypeKind::Int8 => Expr::Const(Value::Int64(1)),
                    SqlTypeKind::Numeric => Expr::Const(Value::Numeric(
                        crate::include::nodes::datum::NumericValue::from_i64(1),
                    )),
                    _ => Expr::Const(Value::Int32(1)),
                }
            };
            let timezone = if args.len() == 4 {
                let timezone_expr = bind_expr_with_outer_and_ctes(
                    &args[3],
                    &call_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                Some(coerce_bound_expr(
                    timezone_expr,
                    timezone_type.expect("generate_series timezone type"),
                    SqlType::new(SqlTypeKind::Text),
                ))
            } else {
                None
            };
            let mut output_columns = vec![QueryColumn {
                name: "generate_series".to_string(),
                sql_type: common,
                wire_type_oid: None,
            }];
            let mut desc_columns = vec![column_desc("generate_series", common, false)];
            maybe_append_function_ordinality(
                with_ordinality,
                &mut output_columns,
                &mut desc_columns,
            );
            let desc = RelationDesc {
                columns: desc_columns,
            };
            let scope = scope_for_relation(Some(name), &desc);
            Ok((
                AnalyzedFrom::function(SetReturningCall::GenerateSeries {
                    func_oid: resolved_proc_oid,
                    func_variadic: resolved_func_variadic,
                    start: coerce_bound_expr(start, start_type, common),
                    stop: coerce_bound_expr(stop, stop_type, common),
                    step,
                    timezone,
                    output_columns,
                    with_ordinality,
                }),
                scope,
                true,
            ))
        }
        "generate_subscripts" => {
            if !(2..=3).contains(&args.len()) {
                return Err(ParseError::UnexpectedToken {
                    expected: "generate_subscripts(array, dimension [, reverse])",
                    actual: format!("generate_subscripts with {} arguments", args.len()),
                });
            }
            let array_type = infer_sql_expr_type_with_ctes(
                &args[0],
                &call_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            if !array_type.is_array
                && !matches!(
                    array_type.kind,
                    SqlTypeKind::Int2Vector | SqlTypeKind::OidVector
                )
            {
                return Err(ParseError::UnexpectedToken {
                    expected: "array argument to generate_subscripts",
                    actual: sql_type_name(array_type),
                });
            }
            let dimension_type = infer_sql_expr_type_with_ctes(
                &args[1],
                &call_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let array = bind_expr_with_outer_and_ctes(
                &args[0],
                &call_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            let dimension = bind_expr_with_outer_and_ctes(
                &args[1],
                &call_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            let reverse = if args.len() == 3 {
                let reverse_type = infer_sql_expr_type_with_ctes(
                    &args[2],
                    &call_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                Some(coerce_bound_expr(
                    bind_expr_with_outer_and_ctes(
                        &args[2],
                        &call_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?,
                    reverse_type,
                    SqlType::new(SqlTypeKind::Bool),
                ))
            } else {
                None
            };
            let mut output_columns = vec![QueryColumn {
                name: "generate_subscripts".to_string(),
                sql_type: SqlType::new(SqlTypeKind::Int4),
                wire_type_oid: None,
            }];
            let mut desc_columns = vec![column_desc(
                "generate_subscripts",
                SqlType::new(SqlTypeKind::Int4),
                false,
            )];
            maybe_append_function_ordinality(
                with_ordinality,
                &mut output_columns,
                &mut desc_columns,
            );
            let desc = RelationDesc {
                columns: desc_columns,
            };
            let scope = scope_for_relation(Some(name), &desc);
            Ok((
                AnalyzedFrom::function(SetReturningCall::GenerateSubscripts {
                    func_oid: resolved_proc_oid,
                    func_variadic: resolved_func_variadic,
                    array,
                    dimension: coerce_bound_expr(
                        dimension,
                        dimension_type,
                        SqlType::new(SqlTypeKind::Int4),
                    ),
                    reverse,
                    output_columns,
                    with_ordinality,
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
            for (_idx, arg) in args.iter().enumerate() {
                let arg_type = infer_sql_expr_type_with_ctes(
                    arg,
                    &call_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                );
                if !arg_type.is_array && matches!(arg_type.kind, SqlTypeKind::TsVector) {
                    if args.len() != 1 {
                        return Err(ParseError::UnexpectedToken {
                            expected: "single tsvector argument to unnest",
                            actual: format!("unnest with {} arguments", args.len()),
                        });
                    }
                    bound_args.push(bind_expr_with_outer_and_ctes(
                        arg,
                        &call_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?);
                    for (name, sql_type) in [
                        ("lexeme", SqlType::new(SqlTypeKind::Text)),
                        (
                            "positions",
                            SqlType::array_of(SqlType::new(SqlTypeKind::Int2)),
                        ),
                        (
                            "weights",
                            SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                        ),
                    ] {
                        output_columns.push(QueryColumn {
                            name: name.into(),
                            sql_type,
                            wire_type_oid: None,
                        });
                        desc_columns.push(column_desc(name, sql_type, true));
                    }
                    continue;
                }
                let Some(element_type) = unnest_element_type(arg_type) else {
                    return Err(ParseError::UnexpectedToken {
                        expected: "array or multirange argument to unnest",
                        actual: format!("{arg:?}"),
                    });
                };
                bound_args.push(bind_expr_with_outer_and_ctes(
                    arg,
                    &call_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?);
                if args.len() == 1
                    && let Some(columns) = resolved_row_columns.clone()
                {
                    for column in columns {
                        desc_columns.push(column_desc(column.name.clone(), column.sql_type, true));
                        output_columns.push(column);
                    }
                    continue;
                }
                if args.len() == 1
                    && let Some(columns) =
                        output_columns_for_unnest_composite_element(element_type, catalog)?
                {
                    for column in columns {
                        desc_columns.push(column_desc(column.name.clone(), column.sql_type, true));
                        output_columns.push(column);
                    }
                    continue;
                }
                let column_name = "unnest".to_string();
                output_columns.push(QueryColumn {
                    name: column_name.clone(),
                    sql_type: element_type,
                    wire_type_oid: None,
                });
                desc_columns.push(column_desc(column_name, element_type, true));
            }
            maybe_append_function_ordinality(
                with_ordinality,
                &mut output_columns,
                &mut desc_columns,
            );
            let desc = RelationDesc {
                columns: desc_columns,
            };
            let scope = scope_for_relation(Some(name), &desc);
            let alias_single_function_output =
                output_columns.len() == 1 || (with_ordinality && output_columns.len() == 2);
            Ok((
                AnalyzedFrom::function(SetReturningCall::Unnest {
                    func_oid: resolved_proc_oid,
                    func_variadic: resolved_func_variadic,
                    args: bound_args,
                    output_columns,
                    with_ordinality,
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
                let bound_args = bind_json_table_function_args(
                    kind,
                    &args,
                    &empty_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                )?;
                let output_columns = resolved_row_columns.clone().unwrap_or_else(|| match kind {
                    JsonTableFunction::ObjectKeys => vec![QueryColumn::text("json_object_keys")],
                    JsonTableFunction::Each => vec![
                        QueryColumn::text("key"),
                        QueryColumn {
                            name: "value".into(),
                            sql_type: SqlType::new(SqlTypeKind::Json),
                            wire_type_oid: None,
                        },
                    ],
                    JsonTableFunction::EachText => {
                        vec![QueryColumn::text("key"), QueryColumn::text("value")]
                    }
                    JsonTableFunction::ArrayElements => vec![QueryColumn {
                        name: "value".into(),
                        sql_type: SqlType::new(SqlTypeKind::Json),
                        wire_type_oid: None,
                    }],
                    JsonTableFunction::ArrayElementsText => {
                        vec![QueryColumn::text("value")]
                    }
                    JsonTableFunction::JsonbPathQuery | JsonTableFunction::JsonbPathQueryTz => {
                        vec![QueryColumn {
                            name: "jsonb_path_query".into(),
                            sql_type: SqlType::new(SqlTypeKind::Jsonb),
                            wire_type_oid: None,
                        }]
                    }
                    JsonTableFunction::JsonbObjectKeys => {
                        vec![QueryColumn::text("jsonb_object_keys")]
                    }
                    JsonTableFunction::JsonbEach => vec![
                        QueryColumn::text("key"),
                        QueryColumn {
                            name: "value".into(),
                            sql_type: SqlType::new(SqlTypeKind::Jsonb),
                            wire_type_oid: None,
                        },
                    ],
                    JsonTableFunction::JsonbEachText => {
                        vec![QueryColumn::text("key"), QueryColumn::text("value")]
                    }
                    JsonTableFunction::JsonbArrayElements => vec![QueryColumn {
                        name: "value".into(),
                        sql_type: SqlType::new(SqlTypeKind::Jsonb),
                        wire_type_oid: None,
                    }],
                    JsonTableFunction::JsonbArrayElementsText => {
                        vec![QueryColumn::text("value")]
                    }
                });
                let mut output_columns = output_columns;
                let mut desc_columns = output_columns
                    .iter()
                    .map(|col| column_desc(col.name.clone(), col.sql_type, true))
                    .collect::<Vec<_>>();
                maybe_append_function_ordinality(
                    with_ordinality,
                    &mut output_columns,
                    &mut desc_columns,
                );
                let desc = RelationDesc {
                    columns: desc_columns,
                };
                let scope = scope_for_relation(Some(name), &desc);
                let alias_single_function_output = output_columns.len() == 1
                    && !matches!(
                        kind,
                        JsonTableFunction::ArrayElements
                            | JsonTableFunction::ArrayElementsText
                            | JsonTableFunction::JsonbArrayElements
                            | JsonTableFunction::JsonbArrayElementsText
                    );
                Ok((
                    AnalyzedFrom::function(SetReturningCall::JsonTableFunction {
                        func_oid: resolved_proc_oid,
                        func_variadic: resolved_func_variadic,
                        kind,
                        args: bound_args,
                        output_columns,
                        with_ordinality,
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
                            wire_type_oid: None,
                        }]
                    }
                    crate::include::nodes::primnodes::RegexTableFunction::SplitToTable => {
                        vec![QueryColumn::text("regexp_split_to_table")]
                    }
                };
                let mut output_columns = output_columns;
                let mut desc_columns = output_columns
                    .iter()
                    .map(|col| column_desc(col.name.clone(), col.sql_type, true))
                    .collect::<Vec<_>>();
                maybe_append_function_ordinality(
                    with_ordinality,
                    &mut output_columns,
                    &mut desc_columns,
                );
                let desc = RelationDesc {
                    columns: desc_columns,
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
                        with_ordinality,
                    }),
                    scope,
                    alias_single_function_output,
                ))
            } else if let Some(kind) = resolve_string_table_function(other) {
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
                let mut output_columns = vec![QueryColumn::text("string_to_table")];
                let mut desc_columns = output_columns
                    .iter()
                    .map(|col| column_desc(col.name.clone(), col.sql_type, true))
                    .collect::<Vec<_>>();
                maybe_append_function_ordinality(
                    with_ordinality,
                    &mut output_columns,
                    &mut desc_columns,
                );
                let desc = RelationDesc {
                    columns: desc_columns,
                };
                let scope = scope_for_relation(Some(name), &desc);
                let alias_single_function_output = output_columns.len() == 1;
                Ok((
                    AnalyzedFrom::function(SetReturningCall::StringTableFunction {
                        func_oid: resolved_proc_oid,
                        func_variadic: resolved_func_variadic,
                        kind,
                        args: bound_args,
                        output_columns,
                        with_ordinality,
                    }),
                    scope,
                    alias_single_function_output,
                ))
            } else if let Some(kind) = resolve_text_search_table_function(other) {
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
                let mut output_columns = text_search_table_function_columns(kind);
                let mut desc_columns = output_columns
                    .iter()
                    .map(|col| column_desc(col.name.clone(), col.sql_type, true))
                    .collect::<Vec<_>>();
                maybe_append_function_ordinality(
                    with_ordinality,
                    &mut output_columns,
                    &mut desc_columns,
                );
                let desc = RelationDesc {
                    columns: desc_columns,
                };
                let scope = scope_for_relation(Some(name), &desc);
                Ok((
                    AnalyzedFrom::function(SetReturningCall::TextSearchTableFunction {
                        kind,
                        args: bound_args,
                        output_columns,
                        with_ordinality,
                    }),
                    scope,
                    false,
                ))
            } else if let Some(resolved) = resolved.as_ref() {
                if resolved.prokind != 'f' {
                    return Err(ParseError::UnknownTable(other.to_string()));
                }
                if matches!(resolved.srf_impl, Some(ResolvedSrfImpl::PgLockStatus)) {
                    if !args.is_empty() {
                        return Err(ParseError::UnexpectedToken {
                            expected: "pg_lock_status()",
                            actual: format!("pg_lock_status with {} arguments", args.len()),
                        });
                    }
                    let mut output_columns = resolved_row_columns.clone().ok_or_else(|| {
                        ParseError::UnexpectedToken {
                            expected: "pg_lock_status OUT parameter metadata",
                            actual: other.to_string(),
                        }
                    })?;
                    let mut desc_columns = output_columns
                        .iter()
                        .map(|col| column_desc(col.name.clone(), col.sql_type, true))
                        .collect::<Vec<_>>();
                    maybe_append_function_ordinality(
                        with_ordinality,
                        &mut output_columns,
                        &mut desc_columns,
                    );
                    let desc = RelationDesc {
                        columns: desc_columns,
                    };
                    let scope = scope_for_relation(Some(name), &desc);
                    return Ok((
                        AnalyzedFrom::function(SetReturningCall::PgLockStatus {
                            func_oid: resolved.proc_oid,
                            func_variadic: resolved.func_variadic,
                            output_columns,
                            with_ordinality,
                        }),
                        scope,
                        false,
                    ));
                }
                if matches!(resolved.srf_impl, Some(ResolvedSrfImpl::TxidSnapshotXip)) {
                    let bound_args = bind_user_defined_table_function_args(
                        &args,
                        &call_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        &resolved.declared_arg_types,
                    )?;
                    let arg = bound_args.into_iter().next().ok_or_else(|| {
                        ParseError::UnexpectedToken {
                            expected: "single txid_snapshot argument",
                            actual: other.to_string(),
                        }
                    })?;
                    let mut output_columns = resolved_row_columns.clone().unwrap_or_else(|| {
                        vec![QueryColumn {
                            name: other.to_string(),
                            sql_type: resolved.result_type,
                            wire_type_oid: None,
                        }]
                    });
                    let mut desc_columns = output_columns
                        .iter()
                        .map(|col| column_desc(col.name.clone(), col.sql_type, true))
                        .collect::<Vec<_>>();
                    maybe_append_function_ordinality(
                        with_ordinality,
                        &mut output_columns,
                        &mut desc_columns,
                    );
                    let desc = RelationDesc {
                        columns: desc_columns,
                    };
                    let scope = scope_for_relation(Some(name), &desc);
                    let alias_single_function_output = output_columns.len() == 1;
                    return Ok((
                        AnalyzedFrom::function(SetReturningCall::TxidSnapshotXip {
                            func_oid: resolved.proc_oid,
                            func_variadic: resolved.func_variadic,
                            arg,
                            output_columns,
                            with_ordinality,
                        }),
                        scope,
                        alias_single_function_output,
                    ));
                }
                if let Some(ResolvedSrfImpl::TextSearch(kind)) = resolved.srf_impl {
                    let bound_args = bind_user_defined_table_function_args(
                        &args,
                        &call_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        &resolved.declared_arg_types,
                    )?;
                    let mut output_columns = resolved_row_columns.clone().unwrap_or_else(|| {
                        vec![QueryColumn {
                            name: other.to_string(),
                            sql_type: resolved.result_type,
                            wire_type_oid: None,
                        }]
                    });
                    let mut desc_columns = output_columns
                        .iter()
                        .map(|col| column_desc(col.name.clone(), col.sql_type, true))
                        .collect::<Vec<_>>();
                    maybe_append_function_ordinality(
                        with_ordinality,
                        &mut output_columns,
                        &mut desc_columns,
                    );
                    let desc = RelationDesc {
                        columns: desc_columns,
                    };
                    let scope = scope_for_relation(Some(name), &desc);
                    let alias_single_function_output = output_columns.len() == 1;
                    return Ok((
                        AnalyzedFrom::function(SetReturningCall::TextSearchTableFunction {
                            kind,
                            args: bound_args,
                            output_columns,
                            with_ordinality,
                        }),
                        scope,
                        alias_single_function_output,
                    ));
                }
                if let Some(
                    srf_impl @ (ResolvedSrfImpl::PartitionTree
                    | ResolvedSrfImpl::PartitionAncestors),
                ) = resolved.srf_impl
                {
                    let bound_args = bind_user_defined_table_function_args(
                        &args,
                        &call_scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        &resolved.declared_arg_types,
                    )?;
                    let output_columns =
                        resolved_row_columns
                            .clone()
                            .unwrap_or_else(|| match srf_impl {
                                ResolvedSrfImpl::PartitionTree => vec![
                                    QueryColumn {
                                        name: "relid".into(),
                                        sql_type: SqlType::new(SqlTypeKind::RegClass),
                                        wire_type_oid: None,
                                    },
                                    QueryColumn {
                                        name: "parentrelid".into(),
                                        sql_type: SqlType::new(SqlTypeKind::RegClass),
                                        wire_type_oid: None,
                                    },
                                    QueryColumn {
                                        name: "isleaf".into(),
                                        sql_type: SqlType::new(SqlTypeKind::Bool),
                                        wire_type_oid: None,
                                    },
                                    QueryColumn {
                                        name: "level".into(),
                                        sql_type: SqlType::new(SqlTypeKind::Int4),
                                        wire_type_oid: None,
                                    },
                                ],
                                ResolvedSrfImpl::PartitionAncestors => vec![QueryColumn {
                                    name: "relid".into(),
                                    sql_type: SqlType::new(SqlTypeKind::RegClass),
                                    wire_type_oid: None,
                                }],
                                _ => unreachable!(
                                    "partition SRF branch only handles partition builtins"
                                ),
                            });
                    let mut output_columns = output_columns;
                    let mut desc_columns = output_columns
                        .iter()
                        .map(|col| column_desc(col.name.clone(), col.sql_type, true))
                        .collect::<Vec<_>>();
                    maybe_append_function_ordinality(
                        with_ordinality,
                        &mut output_columns,
                        &mut desc_columns,
                    );
                    let desc = RelationDesc {
                        columns: desc_columns,
                    };
                    let scope = scope_for_relation(Some(name), &desc);
                    let relid = bound_args.into_iter().next().ok_or_else(|| {
                        ParseError::UnexpectedToken {
                            expected: "single regclass argument",
                            actual: other.to_string(),
                        }
                    })?;
                    let call = match srf_impl {
                        ResolvedSrfImpl::PartitionTree => SetReturningCall::PartitionTree {
                            func_oid: resolved.proc_oid,
                            func_variadic: resolved.func_variadic,
                            relid,
                            output_columns,
                            with_ordinality,
                        },
                        ResolvedSrfImpl::PartitionAncestors => {
                            SetReturningCall::PartitionAncestors {
                                func_oid: resolved.proc_oid,
                                func_variadic: resolved.func_variadic,
                                relid,
                                output_columns,
                                with_ordinality,
                            }
                        }
                        _ => unreachable!("partition SRF branch only handles partition builtins"),
                    };
                    let alias_single_function_output = call.output_columns().len() == 1;
                    return Ok((
                        AnalyzedFrom::function(call),
                        scope,
                        alias_single_function_output,
                    ));
                }
                if !resolved.proretset {
                    return bind_single_row_function_from_item_with_ctes(
                        name,
                        &args,
                        resolved,
                        resolved_row_columns,
                        with_ordinality,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    );
                }
                let bound_args = bind_resolved_user_defined_table_function_args(
                    &args,
                    &call_scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    &resolved,
                )?;
                let alias_single_function_output = resolved_row_columns.is_none();
                let output_columns = resolved_row_columns.unwrap_or_else(|| {
                    vec![QueryColumn {
                        name: resolved.proname.clone(),
                        sql_type: resolved.result_type,
                        wire_type_oid: None,
                    }]
                });
                let mut output_columns = output_columns;
                let mut desc_columns = output_columns
                    .iter()
                    .map(|col| column_desc(col.name.clone(), col.sql_type, true))
                    .collect::<Vec<_>>();
                maybe_append_function_ordinality(
                    with_ordinality,
                    &mut output_columns,
                    &mut desc_columns,
                );
                if let Some((plan, scope, alias_single_function_output)) =
                    try_inline_sql_set_function_from_item(
                        name,
                        &resolved,
                        &bound_args,
                        output_columns.clone(),
                        with_ordinality,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                    )?
                {
                    return Ok((plan, scope, alias_single_function_output));
                }
                let desc = RelationDesc {
                    columns: desc_columns,
                };
                let scope = scope_for_relation(Some(&resolved.proname), &desc);
                Ok((
                    AnalyzedFrom::function(SetReturningCall::UserDefined {
                        proc_oid: resolved.proc_oid,
                        function_name: resolved.proname.clone(),
                        func_variadic: resolved.func_variadic,
                        args: bound_args,
                        inlined_expr: None,
                        output_columns,
                        with_ordinality,
                    }),
                    scope,
                    alias_single_function_output,
                ))
            } else if let Some(typed) = bind_legacy_scalar_function_call(
                other,
                &args,
                func_variadic,
                &call_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )? {
                if let Some(mut output_columns) = resolved_row_columns.clone() {
                    let returned_columns =
                        output_columns_for_unnest_composite_element(typed.sql_type, catalog)?
                            .ok_or_else(|| {
                                function_coldeflist_error(
                                    "a column definition list is only allowed for functions returning \"record\"",
                                )
                            })?;
                    validate_json_record_coldef_compatibility(&returned_columns, &output_columns)?;
                    let mut targets =
                        Vec::with_capacity(output_columns.len() + usize::from(with_ordinality));
                    for (index, (returned, expected)) in returned_columns
                        .iter()
                        .zip(output_columns.iter())
                        .enumerate()
                    {
                        let field_expr = Expr::FieldSelect {
                            expr: Box::new(typed.expr.clone()),
                            field: returned.name.clone(),
                            field_type: returned.sql_type,
                        };
                        targets.push(TargetEntry::new(
                            expected.name.clone(),
                            coerce_bound_expr(field_expr, returned.sql_type, expected.sql_type),
                            expected.sql_type,
                            index + 1,
                        ));
                    }
                    if with_ordinality {
                        let ordinality_type = SqlType::new(SqlTypeKind::Int8);
                        output_columns.push(QueryColumn {
                            name: "ordinality".into(),
                            sql_type: ordinality_type,
                            wire_type_oid: None,
                        });
                        targets.push(TargetEntry::new(
                            "ordinality",
                            Expr::Const(Value::Int64(1)),
                            ordinality_type,
                            output_columns.len(),
                        ));
                    }
                    let plan = AnalyzedFrom::project_function(
                        targets.iter().map(|target| target.expr.clone()).collect(),
                        output_columns.clone(),
                        None,
                        Some(other.to_string()),
                    );
                    let desc = RelationDesc {
                        columns: output_columns
                            .iter()
                            .map(|col| column_desc(col.name.clone(), col.sql_type, true))
                            .collect(),
                    };
                    let scope = scope_with_output_exprs(
                        scope_for_relation(Some(other), &desc),
                        &plan.output_exprs,
                    );
                    return Ok((plan, scope, false));
                }
                let mut output_columns = vec![QueryColumn {
                    name: other.to_string(),
                    sql_type: typed.sql_type,
                    wire_type_oid: None,
                }];
                let mut output_exprs = vec![typed.expr];
                let alias_single_function_output = true;
                if with_ordinality {
                    let ordinality_type = SqlType::new(SqlTypeKind::Int8);
                    output_columns.push(QueryColumn {
                        name: "ordinality".into(),
                        sql_type: ordinality_type,
                        wire_type_oid: None,
                    });
                    output_exprs.push(Expr::Const(Value::Int64(1)));
                }
                let plan = AnalyzedFrom::project_function(
                    output_exprs,
                    output_columns,
                    None,
                    Some(other.to_string()),
                );
                let desc = plan.desc();
                let scope = scope_with_output_exprs(
                    scope_for_relation(Some(other), &desc),
                    &plan.output_exprs,
                );
                Ok((plan, scope, alias_single_function_output))
            } else {
                Err(resolved_error.unwrap_or_else(|| ParseError::UnknownTable(other.to_string())))
            }
        }
    }
}

fn unnest_element_type(arg_type: SqlType) -> Option<SqlType> {
    if arg_type.is_array {
        return Some(arg_type.element_type());
    }
    if arg_type.is_multirange() {
        return Some(
            crate::include::catalog::range_type_ref_for_multirange_sql_type(arg_type)
                .map(|range_type| range_type.sql_type)
                .unwrap_or(SqlType::new(SqlTypeKind::Text)),
        );
    }
    match arg_type.kind {
        SqlTypeKind::Int2Vector => Some(SqlType::new(SqlTypeKind::Int2)),
        SqlTypeKind::OidVector => Some(SqlType::new(SqlTypeKind::Oid)),
        _ => None,
    }
}

fn output_columns_for_unnest_composite_element(
    element_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Vec<QueryColumn>>, ParseError> {
    match element_type.kind {
        SqlTypeKind::Composite if element_type.typrelid != 0 => {
            let relation = catalog
                .lookup_relation_by_oid(element_type.typrelid)
                .ok_or_else(|| ParseError::UnknownTable(element_type.typrelid.to_string()))?;
            Ok(Some(
                relation
                    .desc
                    .columns
                    .into_iter()
                    .filter(|column| !column.dropped)
                    .map(|column| QueryColumn {
                        name: column.name,
                        sql_type: column.sql_type,
                        wire_type_oid: None,
                    })
                    .collect(),
            ))
        }
        SqlTypeKind::Record => Ok(lookup_anonymous_record_descriptor(element_type.typmod).map(
            |descriptor| {
                descriptor
                    .fields
                    .into_iter()
                    .map(|field| QueryColumn {
                        name: field.name,
                        sql_type: field.sql_type,
                        wire_type_oid: None,
                    })
                    .collect()
            },
        )),
        _ => Ok(None),
    }
}

#[allow(clippy::too_many_arguments)]
fn try_bind_user_defined_function_from_item_with_arg_defaults(
    name: &str,
    args: &[SqlFunctionArg],
    func_variadic: bool,
    with_ordinality: bool,
    column_definitions: Option<&[AliasColumnDef]>,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Option<(AnalyzedFrom, BoundScope, bool)>, ParseError> {
    let call_scope = empty_scope();
    let actual_types = args
        .iter()
        .map(|arg| {
            super::infer::infer_sql_expr_function_arg_type_with_ctes(
                &arg.value,
                &call_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        })
        .collect::<Vec<_>>();
    let normalized = match resolve_function_call_with_arg_defaults(
        catalog,
        name,
        args,
        &actual_types,
        func_variadic,
    ) {
        Ok(normalized) => normalized,
        Err(err)
            if is_undefined_function_error(&err)
                && !user_defined_function_name_exists(catalog, name) =>
        {
            return Ok(None);
        }
        Err(err) => return Err(err),
    };
    if !resolved_call_uses_user_defined_binding(&normalized.resolved) {
        return Ok(None);
    }
    let normalized_args = normalized.args;
    let resolved = normalized.resolved;
    reject_nested_from_function_srfs(
        &normalized_args,
        &call_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
    )?;
    let resolved_row_columns =
        resolve_function_row_columns(catalog, Some(&resolved), column_definitions)?;
    Ok(Some(
        bind_resolved_user_defined_function_from_item_with_ctes(
            name,
            &normalized_args,
            resolved,
            resolved_row_columns,
            with_ordinality,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?,
    ))
}

fn is_undefined_function_error(err: &ParseError) -> bool {
    matches!(
        err,
        ParseError::DetailedError {
            sqlstate: "42883",
            ..
        }
    )
}

fn user_defined_function_name_exists(catalog: &dyn CatalogLookup, name: &str) -> bool {
    let (lookup_name, namespace_oid) = match name.rsplit_once('.') {
        Some((schema_name, base_name)) => {
            let namespace_oid = catalog
                .namespace_rows()
                .into_iter()
                .find(|row| row.nspname.eq_ignore_ascii_case(schema_name))
                .map(|row| row.oid);
            (base_name, namespace_oid)
        }
        None => (name, None),
    };
    catalog
        .proc_rows_by_name(lookup_name)
        .into_iter()
        .any(|row| {
            if let Some(namespace_oid) = namespace_oid {
                row.pronamespace == namespace_oid
            } else {
                row.pronamespace != PG_CATALOG_NAMESPACE_OID
            }
        })
}

fn resolved_call_uses_user_defined_binding(resolved: &ResolvedFunctionCall) -> bool {
    resolved.prokind == 'f'
        && resolved.scalar_impl.is_none()
        && resolved.srf_impl.is_none()
        && resolved.agg_impl.is_none()
        && resolved.hypothetical_agg_impl.is_none()
        && resolved.window_impl.is_none()
}

#[allow(clippy::too_many_arguments)]
fn bind_resolved_user_defined_function_from_item_with_ctes(
    name: &str,
    args: &[SqlExpr],
    resolved: ResolvedFunctionCall,
    resolved_row_columns: Option<Vec<QueryColumn>>,
    with_ordinality: bool,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(AnalyzedFrom, BoundScope, bool), ParseError> {
    if !resolved.proretset {
        return bind_single_row_function_from_item_with_ctes(
            name,
            args,
            &resolved,
            resolved_row_columns,
            with_ordinality,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        );
    }

    let call_scope = empty_scope();
    let bound_args = bind_resolved_user_defined_table_function_args(
        args,
        &call_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        &resolved,
    )?;
    let alias_single_function_output = resolved_row_columns.is_none();
    let output_columns = resolved_row_columns.unwrap_or_else(|| {
        vec![QueryColumn {
            name: resolved.proname.clone(),
            sql_type: resolved.result_type,
            wire_type_oid: None,
        }]
    });
    let mut output_columns = output_columns;
    let mut desc_columns = output_columns
        .iter()
        .map(|col| column_desc(col.name.clone(), col.sql_type, true))
        .collect::<Vec<_>>();
    maybe_append_function_ordinality(with_ordinality, &mut output_columns, &mut desc_columns);
    if let Some((plan, scope, alias_single_function_output)) =
        try_inline_sql_set_function_from_item(
            name,
            &resolved,
            &bound_args,
            output_columns.clone(),
            with_ordinality,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )?
    {
        return Ok((plan, scope, alias_single_function_output));
    }
    let desc = RelationDesc {
        columns: desc_columns,
    };
    let scope = scope_for_relation(Some(&resolved.proname), &desc);
    Ok((
        AnalyzedFrom::function(SetReturningCall::UserDefined {
            proc_oid: resolved.proc_oid,
            function_name: resolved.proname.clone(),
            func_variadic: resolved.func_variadic,
            args: bound_args,
            inlined_expr: None,
            output_columns,
            with_ordinality,
        }),
        scope,
        alias_single_function_output,
    ))
}

fn bind_single_row_function_from_item_with_ctes(
    name: &str,
    args: &[SqlExpr],
    resolved: &ResolvedFunctionCall,
    resolved_row_columns: Option<Vec<QueryColumn>>,
    with_ordinality: bool,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    _ctes: &[BoundCte],
) -> Result<(AnalyzedFrom, BoundScope, bool), ParseError> {
    if resolved_row_columns.is_none() && matches!(resolved.result_type.kind, SqlTypeKind::AnyArray)
    {
        return Err(ParseError::DetailedError {
            message: format!("function \"{name}\" in FROM has unsupported return type anyarray"),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    if let Some(mut output_columns) = resolved_row_columns {
        let bound_args = bind_resolved_user_defined_table_function_args(
            args,
            &empty_scope(),
            catalog,
            outer_scopes,
            grouped_outer,
            resolved,
        )?;
        let mut desc_columns = output_columns
            .iter()
            .map(|col| column_desc(col.name.clone(), col.sql_type, true))
            .collect::<Vec<_>>();
        maybe_append_function_ordinality(with_ordinality, &mut output_columns, &mut desc_columns);
        let desc = RelationDesc {
            columns: desc_columns,
        };
        let scope = scope_for_relation(Some(&resolved.proname), &desc);
        let alias_single_function_output = output_columns.len() == 1;
        let inlined_expr =
            try_inline_sql_scalar_function_scan_expr(resolved, &bound_args, catalog)?;
        return Ok((
            AnalyzedFrom::function(SetReturningCall::UserDefined {
                proc_oid: resolved.proc_oid,
                function_name: resolved.proname.clone(),
                func_variadic: resolved.func_variadic,
                args: bound_args,
                inlined_expr: inlined_expr.map(Box::new),
                output_columns,
                with_ordinality,
            }),
            scope,
            alias_single_function_output,
        ));
    }

    let bound_args = bind_resolved_user_defined_table_function_args(
        args,
        &empty_scope(),
        catalog,
        outer_scopes,
        grouped_outer,
        resolved,
    )?;
    let mut output_columns = vec![QueryColumn {
        name: resolved.proname.clone(),
        sql_type: resolved.result_type,
        wire_type_oid: None,
    }];
    let mut desc_columns = output_columns
        .iter()
        .map(|col| column_desc(col.name.clone(), col.sql_type, true))
        .collect::<Vec<_>>();
    maybe_append_function_ordinality(with_ordinality, &mut output_columns, &mut desc_columns);
    let desc = RelationDesc {
        columns: desc_columns,
    };
    let scope = scope_for_relation(Some(&resolved.proname), &desc);
    let inlined_expr = try_inline_sql_scalar_function_scan_expr(resolved, &bound_args, catalog)?;
    Ok((
        AnalyzedFrom::function(SetReturningCall::UserDefined {
            proc_oid: resolved.proc_oid,
            function_name: resolved.proname.clone(),
            func_variadic: resolved.func_variadic,
            args: bound_args,
            inlined_expr: inlined_expr.map(Box::new),
            output_columns,
            with_ordinality,
        }),
        scope,
        true,
    ))
}

#[allow(clippy::too_many_arguments)]
fn try_inline_sql_set_function_from_item(
    _name: &str,
    resolved: &ResolvedFunctionCall,
    bound_args: &[Expr],
    output_columns: Vec<QueryColumn>,
    with_ordinality: bool,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Option<(AnalyzedFrom, BoundScope, bool)>, ParseError> {
    let Some(row) = catalog.proc_row_by_oid(resolved.proc_oid) else {
        return Ok(None);
    };
    if !sql_function_is_set_inline_candidate(&row, with_ordinality) {
        return Ok(None);
    }
    let Some(stmt) = parse_sql_function_select_body(&row.prosrc)? else {
        return Ok(None);
    };
    if matches!(resolved.result_type.kind, SqlTypeKind::Composite) && stmt.from.is_some() {
        return Ok(None);
    }
    let has_target_function_call = stmt
        .targets
        .iter()
        .any(|target| matches!(&target.expr, SqlExpr::FuncCall { over: None, .. }));
    if has_target_function_call && !sql_function_target_only_body_can_inline(&stmt) {
        return Ok(None);
    }
    let inline_args = sql_function_inline_args(&row, bound_args, &resolved.declared_arg_types);
    let (query, _) = with_sql_function_inline_args(inline_args, || {
        if has_target_function_call
            && let Some(query) = analyze_sql_function_target_only_body(&stmt, catalog)?
        {
            Ok((query, empty_scope()))
        } else {
            analyze_select_query_with_outer(
                &stmt,
                catalog,
                outer_scopes,
                grouped_outer.cloned(),
                None,
                ctes,
                &[],
            )
        }
    })?;
    if has_target_function_call
        && stmt.offset.is_none()
        && query.rtable.is_empty()
        && query.jointree.is_none()
        && query.target_list.len() == output_columns.len()
    {
        let plan = AnalyzedFrom {
            rtable: Vec::new(),
            jointree: None,
            output_exprs: query
                .target_list
                .iter()
                .zip(output_columns.iter())
                .map(|(target, column)| {
                    let expr = decrement_inline_expr_varlevels(target.expr.clone());
                    coerce_bound_expr(expr, target.sql_type, column.sql_type)
                })
                .collect(),
            output_columns,
        };
        let desc = plan.desc();
        let scope = scope_with_output_exprs(
            scope_for_relation(Some(&row.proname), &desc),
            &plan.output_exprs,
        );
        return Ok(Some((plan, scope, false)));
    }
    let preserve_body_names = query.set_operation.is_some() && !query.sort_clause.is_empty();
    let mut plan = AnalyzedFrom::subquery(query);
    if output_columns.len() == plan.output_columns.len() {
        let coerce_targets = matches!(resolved.result_type.kind, SqlTypeKind::Composite)
            && resolved.result_type.typrelid != 0
            || matches!(resolved.result_type.kind, SqlTypeKind::Record);
        if coerce_targets {
            validate_sql_function_inline_output_assignments(
                &row.proname,
                &plan.output_columns,
                &output_columns,
                catalog,
            )?;
        }
        retarget_analyzed_from_output_columns(
            &mut plan,
            output_columns,
            coerce_targets,
            preserve_body_names,
        );
    }
    if preserve_body_names && let Some(rte) = plan.rtable.last_mut() {
        rte.alias = Some("*SELECT*".into());
        rte.eref.aliasname = "*SELECT*".into();
    }
    let desc = plan.desc();
    let scope = scope_with_output_exprs(
        scope_for_relation(Some(&row.proname), &desc),
        &plan.output_exprs,
    );
    Ok(Some((plan, scope, false)))
}

fn validate_sql_function_inline_output_assignments(
    function_name: &str,
    actual_columns: &[QueryColumn],
    output_columns: &[QueryColumn],
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    if actual_columns.len() != output_columns.len() {
        return Ok(());
    }
    for (index, (actual, expected)) in actual_columns.iter().zip(output_columns.iter()).enumerate()
    {
        if sql_function_output_assignment_is_allowed(actual.sql_type, expected.sql_type, catalog) {
            continue;
        }
        return Err(ParseError::DetailedError {
            message: "return type mismatch in function declared to return record".into(),
            detail: Some(format!(
                "Final statement returns {} instead of {} at column {}.",
                sql_type_name(actual.sql_type),
                sql_type_name(expected.sql_type),
                index + 1
            )),
            hint: None,
            sqlstate: "42804",
        }
        .with_context(format!("SQL function \"{function_name}\" during inlining")));
    }
    Ok(())
}

fn sql_function_target_only_body_can_inline(stmt: &SelectStatement) -> bool {
    !stmt.with_recursive
        && stmt.with.is_empty()
        && stmt.from.is_none()
        && stmt.where_clause.is_none()
        && stmt.group_by.is_empty()
        && stmt.having.is_none()
        && stmt.window_clauses.is_empty()
        && stmt.order_by.is_empty()
        && stmt.limit.is_none()
        && stmt.locking_clause.is_none()
        && stmt.set_operation.is_none()
}

fn analyze_sql_function_target_only_body(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Query>, ParseError> {
    if !sql_function_target_only_body_can_inline(stmt) {
        return Ok(None);
    }
    let empty = empty_scope();
    let bound_targets = bind_select_targets(&stmt.targets, &empty, catalog, &[], None, &[])?;
    let BoundSelectTargets::Plain(targets) = bound_targets;
    Ok(Some(Query {
        command_type: crate::include::executor::execdesc::CommandType::Select,
        depends_on_row_security: false,
        rtable: Vec::new(),
        jointree: None,
        target_list: normalize_target_list(targets),
        distinct: false,
        distinct_on: Vec::new(),
        where_qual: None,
        group_by: Vec::new(),
        group_by_refs: Vec::new(),
        grouping_sets: Vec::new(),
        accumulators: Vec::new(),
        window_clauses: Vec::new(),
        having_qual: None,
        sort_clause: Vec::new(),
        constraint_deps: Vec::new(),
        limit_count: stmt.limit,
        limit_offset: stmt.offset,
        locking_clause: None,
        locking_targets: Vec::new(),
        locking_nowait: false,
        row_marks: Vec::new(),
        has_target_srfs: false,
        recursive_union: None,
        set_operation: None,
    }))
}

fn decrement_inline_expr_varlevels(expr: Expr) -> Expr {
    match expr {
        Expr::Var(mut var) if var.varlevelsup > 0 => {
            var.varlevelsup -= 1;
            Expr::Var(var)
        }
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(decrement_inline_expr_varlevels)
                .collect(),
            ..*bool_expr
        })),
        Expr::Case(case_expr) => Expr::Case(Box::new(CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| Box::new(decrement_inline_expr_varlevels(*arg))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| CaseWhen {
                    expr: decrement_inline_expr_varlevels(arm.expr),
                    result: decrement_inline_expr_varlevels(arm.result),
                })
                .collect(),
            defresult: Box::new(decrement_inline_expr_varlevels(*case_expr.defresult)),
            ..*case_expr
        })),
        Expr::Cast(inner, ty) => Expr::Cast(Box::new(decrement_inline_expr_varlevels(*inner)), ty),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(decrement_inline_expr_varlevels(*inner))),
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(decrement_inline_expr_varlevels(*inner)))
        }
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Expr::FieldSelect {
            expr: Box::new(decrement_inline_expr_varlevels(*expr)),
            field,
            field_type,
        },
        Expr::Row { descriptor, fields } => Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| (name, decrement_inline_expr_varlevels(expr)))
                .collect(),
        },
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(decrement_inline_expr_varlevels)
                .collect(),
            ..*func
        })),
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(decrement_inline_expr_varlevels(*left)),
            Box::new(decrement_inline_expr_varlevels(*right)),
        ),
        other => other,
    }
}

fn sql_function_output_assignment_is_allowed(
    actual: SqlType,
    expected: SqlType,
    catalog: &dyn CatalogLookup,
) -> bool {
    if actual == expected {
        return true;
    }
    let Some(actual_oid) = catalog.type_oid_for_sql_type(actual) else {
        return false;
    };
    let Some(expected_oid) = catalog.type_oid_for_sql_type(expected) else {
        return false;
    };
    if actual_oid == expected_oid {
        return true;
    }
    catalog
        .cast_by_source_target(actual_oid, expected_oid)
        .is_some_and(|row| matches!(row.castcontext, 'i' | 'a'))
}

fn try_inline_sql_scalar_function_scan_expr(
    resolved: &ResolvedFunctionCall,
    bound_args: &[Expr],
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    let Some(row) = catalog.proc_row_by_oid(resolved.proc_oid) else {
        return Ok(None);
    };
    if !sql_function_is_scalar_inline_candidate(&row) {
        return Ok(None);
    }
    let Some(stmt) = parse_sql_function_select_body(&row.prosrc)? else {
        return Ok(None);
    };
    if !select_body_is_simple_scalar_inline(&stmt) {
        return Ok(None);
    }
    let inline_args = sql_function_inline_args(&row, bound_args, &resolved.declared_arg_types);
    let empty = empty_scope();
    let bound_targets = with_sql_function_inline_args(inline_args, || {
        bind_select_targets(&stmt.targets, &empty, catalog, &[], None, &[])
    })?;
    let BoundSelectTargets::Plain(targets) = bound_targets;
    if matches!(
        resolved.result_type.kind,
        SqlTypeKind::Composite | SqlTypeKind::Record
    ) && targets.len() != 1
    {
        let Some((descriptor, columns)) =
            sql_function_record_result_descriptor(&row, resolved.result_type, catalog)
        else {
            return Ok(None);
        };
        if targets.len() != columns.len() {
            return Ok(None);
        }
        let fields = targets
            .into_iter()
            .zip(columns.iter())
            .map(|(target, column)| {
                (
                    column.name.clone(),
                    coerce_bound_expr(target.expr, target.sql_type, column.sql_type),
                )
            })
            .collect();
        return Ok(Some(Expr::Row { descriptor, fields }));
    }
    let Ok([target]) = <Vec<TargetEntry> as TryInto<[TargetEntry; 1]>>::try_into(targets) else {
        return Ok(None);
    };
    if matches!(resolved.result_type.kind, SqlTypeKind::Void)
        && !matches!(target.expr, Expr::Func(_))
    {
        return Ok(None);
    }
    Ok(Some(coerce_bound_expr(
        target.expr,
        target.sql_type,
        resolved.result_type,
    )))
}

fn sql_function_is_set_inline_candidate(row: &PgProcRow, with_ordinality: bool) -> bool {
    row.prolang == PG_LANGUAGE_SQL_OID
        && row.prokind == 'f'
        && row.proretset
        && row.prorettype != VOID_TYPE_OID
        && !with_ordinality
        && !row.proisstrict
        && row.provolatile != 'v'
        && !row.prosecdef
        && row.proconfig.is_none()
}

fn sql_function_is_scalar_inline_candidate(row: &PgProcRow) -> bool {
    row.prolang == PG_LANGUAGE_SQL_OID
        && row.prokind == 'f'
        && !row.proretset
        && !row.prosecdef
        && row.proconfig.is_none()
        && (row.provolatile == 'i' || row.prorettype == VOID_TYPE_OID)
}

fn parse_sql_function_select_body(source: &str) -> Result<Option<SelectStatement>, ParseError> {
    let Some(body) = sql_function_select_body_source(source) else {
        return Ok(None);
    };
    match parse_statement(body.as_ref())? {
        Statement::Select(stmt) => Ok(Some(stmt)),
        Statement::Values(values) => {
            Ok(Some(crate::backend::parser::wrap_values_as_select(values)))
        }
        _ => Ok(None),
    }
}

fn sql_function_select_body_source(source: &str) -> Option<std::borrow::Cow<'_, str>> {
    let body = source.trim().trim_end_matches(';').trim();
    if body
        .get(.."return".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("return"))
        && body
            .get("return".len()..)
            .and_then(|rest| rest.chars().next())
            .is_none_or(|ch| ch.is_whitespace())
    {
        return Some(std::borrow::Cow::Owned(format!(
            "select {}",
            body["return".len()..].trim()
        )));
    }
    let lower = body.to_ascii_lowercase();
    if lower.starts_with("begin atomic") {
        let without_trailing_semicolon = body.trim_end_matches(';').trim_end();
        let lowered_without_semicolon = without_trailing_semicolon.to_ascii_lowercase();
        let end = if lowered_without_semicolon.ends_with("end") {
            without_trailing_semicolon.len().saturating_sub("end".len())
        } else {
            body.len()
        };
        let inner = body.get("begin atomic".len()..end)?.trim();
        let statements = inner
            .split(';')
            .map(str::trim)
            .filter(|statement| !statement.is_empty() && !statement.eq_ignore_ascii_case("end"))
            .collect::<Vec<_>>();
        if statements.len() == 1 {
            return Some(std::borrow::Cow::Owned(statements[0].to_string()));
        }
        return None;
    }
    sql_function_quoted_body_can_inline(body).then_some(std::borrow::Cow::Borrowed(body))
}

fn sql_function_quoted_body_can_inline(body: &str) -> bool {
    let lower = body.to_ascii_lowercase();
    lower.strip_prefix("select").is_some_and(|rest| {
        rest.chars()
            .next()
            .is_none_or(|ch| ch.is_whitespace() || ch == '(')
    }) || lower.strip_prefix("values").is_some_and(|rest| {
        rest.chars()
            .next()
            .is_none_or(|ch| ch.is_whitespace() || ch == '(')
    })
}

fn select_body_is_simple_scalar_inline(stmt: &SelectStatement) -> bool {
    !stmt.with_recursive
        && stmt.with.is_empty()
        && stmt.from.is_none()
        && stmt.where_clause.is_none()
        && stmt.group_by.is_empty()
        && stmt.having.is_none()
        && stmt.window_clauses.is_empty()
        && stmt.order_by.is_empty()
        && stmt.limit.is_none()
        && stmt.offset.is_none()
        && stmt.locking_clause.is_none()
        && stmt.set_operation.is_none()
}

fn sql_function_record_result_descriptor(
    row: &PgProcRow,
    result_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Option<(RecordDescriptor, Vec<QueryColumn>)> {
    if matches!(result_type.kind, SqlTypeKind::Composite) && result_type.typrelid != 0 {
        let relation = catalog.lookup_relation_by_oid(result_type.typrelid)?;
        let columns = relation
            .desc
            .columns
            .into_iter()
            .filter(|column| !column.dropped)
            .map(|column| QueryColumn {
                name: column.name,
                sql_type: column.sql_type,
                wire_type_oid: None,
            })
            .collect::<Vec<_>>();
        let descriptor = RecordDescriptor::named(
            row.prorettype,
            result_type.typrelid,
            result_type.typmod,
            columns
                .iter()
                .map(|column| (column.name.clone(), column.sql_type))
                .collect(),
        );
        return Some((descriptor, columns));
    }
    None
}

fn sql_function_inline_args(
    row: &PgProcRow,
    coerced_args: &[Expr],
    declared_arg_types: &[SqlType],
) -> Vec<SqlFunctionInlineArg> {
    let names = sql_function_input_arg_names(row);
    coerced_args
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, expr)| SqlFunctionInlineArg {
            name: names.get(index).cloned().flatten(),
            sql_type: declared_arg_types
                .get(index)
                .copied()
                .or_else(|| expr_sql_type_hint(&expr))
                .unwrap_or(SqlType::new(SqlTypeKind::Text)),
            expr,
        })
        .collect()
}

fn sql_function_input_arg_names(row: &PgProcRow) -> Vec<Option<String>> {
    let names = row.proargnames.as_deref().unwrap_or(&[]);
    let Some(modes) = row.proargmodes.as_ref() else {
        return (0..row.pronargs.max(0) as usize)
            .map(|index| names.get(index).filter(|name| !name.is_empty()).cloned())
            .collect();
    };
    modes
        .iter()
        .copied()
        .enumerate()
        .filter_map(|(index, mode)| {
            matches!(mode, b'i' | b'b' | b'v')
                .then(|| names.get(index).filter(|name| !name.is_empty()).cloned())
        })
        .collect()
}

fn retarget_analyzed_from_output_columns(
    plan: &mut AnalyzedFrom,
    output_columns: Vec<QueryColumn>,
    coerce_targets: bool,
    preserve_rte_names: bool,
) {
    let preserved_names = plan
        .rtable
        .last()
        .map(|rte| {
            rte.desc
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let preserved_names = if preserve_rte_names {
        uniquify_preserved_output_names(preserved_names)
    } else {
        preserved_names
    };
    let desc = RelationDesc {
        columns: output_columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                let name = if preserve_rte_names {
                    preserved_names
                        .get(index)
                        .cloned()
                        .unwrap_or_else(|| column.name.clone())
                } else {
                    column.name.clone()
                };
                column_desc(name, column.sql_type, true)
            })
            .collect(),
    };
    let rtindex = plan.rtable.len();
    if let Some(rte) = plan.rtable.last_mut() {
        if let RangeTblEntryKind::Subquery { query } = &mut rte.kind {
            retarget_query_output_columns(
                query,
                &output_columns,
                coerce_targets,
                preserve_rte_names,
            );
        }
        rte.desc = desc;
        rte.eref.colnames = rte
            .desc
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect();
    }
    plan.output_exprs = output_columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            Expr::Var(Var {
                varno: rtindex,
                varattno: user_attrno(index),
                varlevelsup: 0,
                vartype: column.sql_type,
            })
        })
        .collect();
    plan.output_columns = output_columns;
}

fn uniquify_preserved_output_names(names: Vec<String>) -> Vec<String> {
    let mut output = Vec::with_capacity(names.len());
    for name in names {
        if output.iter().any(|existing| existing == &name) {
            let mut suffix = 1usize;
            loop {
                let candidate = format!("{name}_{suffix}");
                if !output.iter().any(|existing| existing == &candidate) {
                    output.push(candidate);
                    break;
                }
                suffix += 1;
            }
        } else {
            output.push(name);
        }
    }
    output
}

fn retarget_query_output_columns(
    query: &mut Query,
    output_columns: &[QueryColumn],
    coerce_targets: bool,
    preserve_names: bool,
) {
    if coerce_targets && query.target_list.len() == output_columns.len() {
        let can_retarget_output_vars = query.rtable.is_empty() && query.jointree.is_none();
        for (target, column) in query.target_list.iter_mut().zip(output_columns.iter()) {
            let expr = if can_retarget_output_vars {
                retarget_output_var_type(target.expr.clone(), output_columns)
            } else {
                target.expr.clone()
            };
            target.expr = if expr_sql_type_hint(&expr) == Some(column.sql_type) {
                expr
            } else {
                coerce_bound_expr(expr, target.sql_type, column.sql_type)
            };
            if !preserve_names {
                target.name = column.name.clone();
            }
            target.sql_type = column.sql_type;
        }
    }
    if query.rtable.is_empty() && query.jointree.is_none() {
        for item in &mut query.sort_clause {
            item.expr = retarget_output_var_type(item.expr.clone(), output_columns);
        }
    }
    if let Some(set_operation) = &mut query.set_operation {
        if coerce_targets {
            for (index, column) in output_columns.iter().enumerate() {
                let Some(desc_column) = set_operation.output_desc.columns.get_mut(index) else {
                    continue;
                };
                if !preserve_names {
                    desc_column.name = column.name.clone();
                }
                desc_column.sql_type = column.sql_type;
            }
            for input in &mut set_operation.inputs {
                retarget_query_output_columns(
                    input,
                    output_columns,
                    coerce_targets,
                    preserve_names,
                );
            }
        }
    }
}

fn retarget_output_var_type(expr: Expr, output_columns: &[QueryColumn]) -> Expr {
    match expr {
        Expr::Var(mut var)
            if var.varno == 1
                && var.varlevelsup == 0
                && let Some(index) =
                    crate::include::nodes::primnodes::attrno_index(var.varattno)
                && let Some(column) = output_columns.get(index) =>
        {
            var.vartype = column.sql_type;
            Expr::Var(var)
        }
        other => other,
    }
}

fn maybe_append_function_ordinality(
    with_ordinality: bool,
    output_columns: &mut Vec<QueryColumn>,
    desc_columns: &mut Vec<ColumnDesc>,
) {
    if !with_ordinality {
        return;
    }
    let ordinality_type = SqlType::new(SqlTypeKind::Int8);
    output_columns.push(QueryColumn {
        name: "ordinality".to_string(),
        sql_type: ordinality_type,
        wire_type_oid: None,
    });
    desc_columns.push(column_desc("ordinality", ordinality_type, false));
}

fn bind_json_table_function_args(
    kind: JsonTableFunction,
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<Vec<Expr>, ParseError> {
    args.iter()
        .enumerate()
        .map(|(index, arg)| {
            let target_type = match (kind, index) {
                (
                    JsonTableFunction::JsonbPathQuery | JsonTableFunction::JsonbPathQueryTz,
                    0 | 2,
                )
                | (JsonTableFunction::JsonbObjectKeys, 0)
                | (JsonTableFunction::JsonbEach, 0)
                | (JsonTableFunction::JsonbEachText, 0)
                | (JsonTableFunction::JsonbArrayElements, 0)
                | (JsonTableFunction::JsonbArrayElementsText, 0) => {
                    Some(SqlType::new(SqlTypeKind::Jsonb))
                }
                (JsonTableFunction::JsonbPathQuery | JsonTableFunction::JsonbPathQueryTz, 1) => {
                    Some(SqlType::new(SqlTypeKind::JsonPath))
                }
                (JsonTableFunction::JsonbPathQuery | JsonTableFunction::JsonbPathQueryTz, 3) => {
                    Some(SqlType::new(SqlTypeKind::Bool))
                }
                _ => None,
            };
            let raw_arg_type = infer_sql_expr_type_with_ctes(
                arg,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            );
            let resolved_arg_type = target_type
                .map(|target| coerce_unknown_string_literal_type(arg, raw_arg_type, target))
                .unwrap_or(raw_arg_type);
            let bound = bind_expr_with_outer_and_ctes(
                arg,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )?;
            Ok(match target_type {
                Some(target) if resolved_arg_type == target && raw_arg_type != target => {
                    coerce_bound_expr(bound, raw_arg_type, target)
                }
                None => bound,
                Some(_) => bound,
            })
        })
        .collect()
}

fn bind_json_record_from_item(
    name: &str,
    args: &[SqlExpr],
    column_definitions: Option<&[AliasColumnDef]>,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
) -> Result<(AnalyzedFrom, BoundScope, bool), ParseError> {
    let Some(kind) = resolve_json_record_function(name) else {
        return Err(ParseError::UnknownTable(name.to_string()));
    };
    if args.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "json/jsonb record-expansion function call",
            actual: format!("{name}()"),
        });
    }
    let call_scope = empty_scope();
    let actual_types = args
        .iter()
        .map(|arg| {
            super::infer::infer_sql_expr_function_arg_type_with_ctes(
                arg,
                &call_scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
            )
        })
        .collect::<Vec<_>>();
    let resolved = resolve_function_call(catalog, name, &actual_types, false)?;
    let output_columns = match kind {
        JsonRecordFunction::ToRecord
        | JsonRecordFunction::ToRecordSet
        | JsonRecordFunction::JsonbToRecord
        | JsonRecordFunction::JsonbToRecordSet => column_definitions
            .map(|definitions| query_columns_from_alias_definitions(definitions, catalog))
            .transpose()?
            .ok_or_else(|| {
                function_coldeflist_error(
                    "a column definition list is required for functions returning \"record\"",
                )
            })?,
        JsonRecordFunction::PopulateRecord
        | JsonRecordFunction::PopulateRecordSet
        | JsonRecordFunction::JsonbPopulateRecord
        | JsonRecordFunction::JsonbPopulateRecordSet => {
            let row_type = *actual_types
                .first()
                .expect("json populate record row arg type");
            output_columns_for_json_populate_record(name, row_type, column_definitions, catalog)?
        }
    };
    let bound_args = bind_user_defined_table_function_args(
        args,
        &call_scope,
        catalog,
        outer_scopes,
        grouped_outer,
        &resolved.declared_arg_types,
    )?;
    let desc = RelationDesc {
        columns: output_columns
            .iter()
            .map(|col| column_desc(col.name.clone(), col.sql_type, true))
            .collect(),
    };
    let scope = scope_for_relation(Some(name), &desc);
    Ok((
        AnalyzedFrom::function(SetReturningCall::JsonRecordFunction {
            func_oid: resolved.proc_oid,
            func_variadic: resolved.func_variadic,
            kind,
            args: bound_args,
            output_columns,
            record_type: match kind {
                JsonRecordFunction::PopulateRecord
                | JsonRecordFunction::PopulateRecordSet
                | JsonRecordFunction::JsonbPopulateRecord
                | JsonRecordFunction::JsonbPopulateRecordSet => actual_types.first().copied(),
                JsonRecordFunction::ToRecord
                | JsonRecordFunction::ToRecordSet
                | JsonRecordFunction::JsonbToRecord
                | JsonRecordFunction::JsonbToRecordSet => None,
            },
            with_ordinality: false,
        }),
        scope,
        false,
    ))
}

fn output_columns_for_json_populate_record(
    name: &str,
    row_type: SqlType,
    column_definitions: Option<&[AliasColumnDef]>,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<QueryColumn>, ParseError> {
    match row_type.kind {
        SqlTypeKind::Composite if row_type.typrelid != 0 => {
            if column_definitions.is_some() {
                return Err(function_coldeflist_error(
                    "a column definition list is redundant for a function returning a named composite type",
                ));
            }
            let relation = catalog
                .lookup_relation_by_oid(row_type.typrelid)
                .ok_or_else(|| ParseError::UnknownTable(name.to_string()))?;
            Ok(relation
                .desc
                .columns
                .into_iter()
                .filter(|column| !column.dropped)
                .map(|column| QueryColumn {
                    name: column.name,
                    sql_type: column.sql_type,
                    wire_type_oid: None,
                })
                .collect())
        }
        SqlTypeKind::Record => {
            let inferred_columns =
                lookup_anonymous_record_descriptor(row_type.typmod).map(|descriptor| {
                    descriptor
                        .fields
                        .into_iter()
                        .map(|field| QueryColumn {
                            name: field.name,
                            sql_type: field.sql_type,
                            wire_type_oid: None,
                        })
                        .collect::<Vec<_>>()
                });
            match (inferred_columns, column_definitions) {
                (Some(columns), Some(definitions)) => {
                    let query_columns = query_columns_from_alias_definitions(definitions, catalog)?;
                    validate_json_record_coldef_compatibility(&columns, &query_columns)?;
                    Ok(query_columns)
                }
                (Some(columns), None) => Ok(columns),
                (None, Some(definitions)) => {
                    query_columns_from_alias_definitions(definitions, catalog)
                }
                (None, None) => Err(function_coldeflist_error(
                    "a column definition list is required for functions returning \"record\"",
                )),
            }
        }
        _ => Err(ParseError::UnknownTable(name.to_string())),
    }
}

fn validate_json_record_coldef_compatibility(
    returned: &[QueryColumn],
    expected: &[QueryColumn],
) -> Result<(), ParseError> {
    if returned.len() != expected.len() {
        return Err(function_coldeflist_mismatch_error(format!(
            "Returned row contains {} attribute{}, but query expects {}.",
            returned.len(),
            if returned.len() == 1 { "" } else { "s" },
            expected.len()
        )));
    }
    for (index, (returned, expected)) in returned.iter().zip(expected.iter()).enumerate() {
        if returned.sql_type != expected.sql_type {
            return Err(function_coldeflist_mismatch_error(format!(
                "Returned type {} at ordinal position {}, but query expects {}.",
                sql_type_name(returned.sql_type),
                index + 1,
                sql_type_name(expected.sql_type)
            )));
        }
    }
    Ok(())
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

fn bind_resolved_user_defined_table_function_args(
    args: &[SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    resolved: &ResolvedFunctionCall,
) -> Result<Vec<Expr>, ParseError> {
    let arg_types = args
        .iter()
        .map(|arg| infer_sql_expr_type(arg, scope, catalog, outer_scopes, grouped_outer))
        .collect::<Vec<_>>();
    let bound_args = args
        .iter()
        .map(|arg| bind_expr_with_outer(arg, scope, catalog, outer_scopes, grouped_outer))
        .collect::<Result<Vec<_>, _>>()?;
    if !resolved.func_variadic {
        return Ok(bound_args
            .into_iter()
            .zip(arg_types)
            .zip(resolved.declared_arg_types.iter().copied())
            .map(|((arg, actual_type), declared_type)| {
                coerce_bound_expr(arg, actual_type, declared_type)
            })
            .collect());
    }
    if resolved.vatype_oid == ANYOID {
        return Ok(bound_args);
    }

    let element_type = catalog
        .type_by_oid(resolved.vatype_oid)
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "known variadic element type",
            actual: resolved.vatype_oid.to_string(),
        })?
        .sql_type;
    let array_type = SqlType::array_of(element_type);

    if resolved.nvargs > 0 {
        let fixed_prefix_len = bound_args.len().saturating_sub(resolved.nvargs);
        let mut rewritten = bound_args
            .iter()
            .take(fixed_prefix_len)
            .cloned()
            .zip(arg_types.iter().take(fixed_prefix_len).copied())
            .zip(
                resolved
                    .declared_arg_types
                    .iter()
                    .take(fixed_prefix_len)
                    .copied(),
            )
            .map(|((arg, actual_type), declared_type)| {
                coerce_bound_expr(arg, actual_type, declared_type)
            })
            .collect::<Vec<_>>();
        let elements = bound_args[fixed_prefix_len..]
            .iter()
            .zip(arg_types[fixed_prefix_len..].iter().copied())
            .map(|(expr, sql_type)| coerce_bound_expr(expr.clone(), sql_type, element_type))
            .collect();
        rewritten.push(Expr::ArrayLiteral {
            elements,
            array_type,
        });
        return Ok(rewritten);
    }

    Ok(bound_args
        .into_iter()
        .zip(arg_types)
        .zip(resolved.declared_arg_types.iter().copied())
        .enumerate()
        .map(|(index, ((arg, actual_type), declared_type))| {
            if index + 1 == resolved.declared_arg_types.len() {
                coerce_bound_expr(arg, actual_type, array_type)
            } else {
                coerce_bound_expr(arg, actual_type, declared_type)
            }
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
                Some(ResolvedFunctionRowShape::None) => Err(function_coldeflist_error(
                    "a column definition list is only allowed for functions returning \"record\"",
                )),
                None => Ok(Some(columns)),
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
                        .type_by_name(name)
                        .map(|row| row.sql_type)
                        .ok_or_else(|| ParseError::UnsupportedType(name.clone()))?,
                },
                wire_type_oid: None,
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

fn function_coldeflist_mismatch_error(detail: String) -> ParseError {
    ParseError::DetailedError {
        message: "function return row and query-specified return row do not match".into(),
        detail: Some(detail),
        hint: None,
        sqlstate: "42804",
    }
}

pub(super) fn lookup_relation(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Result<BoundRelation, ParseError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'r' | 'p') => Ok(entry),
        Some(entry) if entry.relkind == 'm' => Err(ParseError::FeatureNotSupportedMessage(
            format!("cannot change materialized view \"{name}\""),
        )),
        Some(_) => Err(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table",
        }),
        None => Err(ParseError::UnknownTable(name.to_string())),
    }
}

pub(crate) fn scope_for_relation(relation_name: Option<&str>, desc: &RelationDesc) -> BoundScope {
    BoundScope {
        desc: desc.clone(),
        output_exprs: default_scope_output_exprs(1, desc),
        columns: desc
            .columns
            .iter()
            .map(|column| ScopeColumn {
                output_name: column.name.clone(),
                hidden: column.dropped,
                qualified_only: false,
                relation_names: relation_name.into_iter().map(str::to_string).collect(),
                hidden_invalid_relation_names: vec![],
                hidden_missing_relation_names: vec![],
                source_relation_oid: None,
                source_attno: None,
                source_columns: Vec::new(),
            })
            .collect(),
        relations: relation_name
            .map(|name| {
                vec![ScopeRelation {
                    relation_names: vec![name.to_string()],
                    hidden_invalid_relation_names: vec![],
                    hidden_missing_relation_names: vec![],
                    system_varno: None,
                    relation_oid: None,
                }]
            })
            .unwrap_or_default(),
    }
}

pub(super) fn scope_for_base_relation(
    relation_name: &str,
    desc: &RelationDesc,
    relation_oid: Option<u32>,
) -> BoundScope {
    let mut scope = scope_for_relation(Some(relation_name), desc);
    for (index, column) in scope.columns.iter_mut().enumerate() {
        let attno = user_attrno(index);
        column.source_relation_oid = relation_oid;
        column.source_attno = Some(attno);
        column.source_columns = relation_oid
            .map(|relation_oid| vec![(relation_oid, attno)])
            .unwrap_or_default();
    }
    scope.output_exprs = default_scope_output_exprs(1, desc);
    scope.relations = vec![ScopeRelation {
        relation_names: vec![relation_name.to_string()],
        hidden_invalid_relation_names: vec![],
        hidden_missing_relation_names: vec![],
        system_varno: Some(1),
        relation_oid,
    }];
    scope
}

pub(crate) fn scope_for_base_relation_with_optional_name(
    relation_name: Option<&str>,
    desc: &RelationDesc,
) -> BoundScope {
    let mut scope = scope_for_relation(relation_name, desc);
    scope.output_exprs = default_scope_output_exprs(1, desc);
    scope.relations = vec![ScopeRelation {
        relation_names: relation_name.into_iter().map(str::to_string).collect(),
        hidden_invalid_relation_names: vec![],
        hidden_missing_relation_names: vec![],
        system_varno: Some(1),
        relation_oid: None,
    }];
    scope
}

pub(crate) fn shift_scope_rtindexes(mut scope: BoundScope, offset: usize) -> BoundScope {
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
            bind_join_using_projection(kind, columns, left_scope, right_scope)
        }
        JoinConstraint::Natural => {
            let columns = natural_join_columns(left_scope, right_scope);
            bind_join_using_projection(kind, &columns, left_scope, right_scope)
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

fn scope_column_sources(column: &ScopeColumn) -> Vec<(u32, AttrNumber)> {
    let mut sources = column.source_columns.clone();
    if let (Some(relation_oid), Some(attno)) = (column.source_relation_oid, column.source_attno)
        && !sources.contains(&(relation_oid, attno))
    {
        sources.push((relation_oid, attno));
    }
    sources
}

fn join_using_source_columns(left: &ScopeColumn, right: &ScopeColumn) -> Vec<(u32, AttrNumber)> {
    let mut sources = scope_column_sources(left);
    for source in scope_column_sources(right) {
        if !sources.contains(&source) {
            sources.push(source);
        }
    }
    sources
}

fn join_using_relation_names(left: &ScopeColumn, right: &ScopeColumn) -> Vec<String> {
    let mut names = left.relation_names.clone();
    for name in &right.relation_names {
        if !names
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(name))
        {
            names.push(name.clone());
        }
    }
    names
}

fn bind_join_using_projection(
    kind: &JoinKind,
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
        alias_exprs.push(match kind {
            JoinKind::Full => Expr::Coalesce(Box::new(left_expr), Box::new(right_expr)),
            JoinKind::Right => right_expr,
            _ => left_expr,
        });
        output_columns.push(QueryColumn {
            name: name.clone(),
            sql_type: left_ty,
            wire_type_oid: None,
        });
        joinleftcols.push(*left_index + 1);
        joinrightcols.push(*right_index + 1);
        desc_columns.push(column_desc(name.clone(), left_ty, true));
        scope_columns.push(ScopeColumn {
            output_name: name.clone(),
            hidden: false,
            qualified_only: false,
            relation_names: join_using_relation_names(
                &left_scope.columns[*left_index],
                &right_scope.columns[*right_index],
            ),
            hidden_invalid_relation_names: vec![],
            hidden_missing_relation_names: vec![],
            source_relation_oid: None,
            source_attno: None,
            source_columns: join_using_source_columns(
                &left_scope.columns[*left_index],
                &right_scope.columns[*right_index],
            ),
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
            wire_type_oid: None,
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
            wire_type_oid: None,
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

fn apply_function_rte_alias(plan: &mut AnalyzedFrom, alias: &str, desc: &RelationDesc) -> bool {
    let [rte] = plan.rtable.as_mut_slice() else {
        return false;
    };
    let RangeTblEntryKind::Function { call } = &mut rte.kind else {
        return false;
    };
    let output_columns = desc
        .columns
        .iter()
        .map(|column| QueryColumn {
            name: column.name.clone(),
            sql_type: column.sql_type,
            wire_type_oid: None,
        })
        .collect::<Vec<_>>();
    rte.alias = Some(alias.to_string());
    rte.alias_preserves_source_names = false;
    rte.eref.aliasname = alias.to_string();
    rte.eref.colnames = output_columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    rte.desc = desc.clone();
    call.set_output_columns(output_columns.clone());
    plan.output_columns = output_columns;
    true
}

fn apply_join_using_alias(
    mut plan: AnalyzedFrom,
    mut scope: BoundScope,
    alias: &str,
    using_column_count: usize,
) -> Result<(AnalyzedFrom, BoundScope), ParseError> {
    if scope.relations.iter().any(|relation| {
        relation
            .relation_names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(alias))
    }) || scope.columns.iter().any(|column| {
        column
            .relation_names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(alias))
    }) {
        return Err(ParseError::DuplicateTableName(alias.to_string()));
    }

    for column in scope.columns.iter_mut().take(using_column_count) {
        if !column
            .relation_names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(alias))
        {
            column.relation_names.push(alias.to_string());
        }
    }
    if let Some(rte) = plan.rtable.last_mut()
        && matches!(rte.kind, RangeTblEntryKind::Join { .. })
    {
        rte.alias = Some(alias.to_string());
        rte.alias_preserves_source_names = true;
        rte.eref.aliasname = alias.to_string();
    }
    Ok((plan, scope))
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
        let table_function_name = plan.rtable.last().and_then(|rte| match &rte.kind {
            RangeTblEntryKind::Function {
                call: SetReturningCall::SqlJsonTable(_),
                ..
            } => Some("JSON_TABLE"),
            RangeTblEntryKind::Function {
                call: SetReturningCall::SqlXmlTable(_),
                ..
            } => Some("XMLTABLE"),
            _ => None,
        });
        let actual = if let Some(name) = table_function_name {
            format!(
                "{name} function has {} columns available but {} columns specified",
                visible_positions.len(),
                column_aliases.len(),
            )
        } else {
            format!(
                "table \"{alias}\" has {} columns available but {} columns specified",
                visible_positions.len(),
                column_aliases.len(),
            )
        };
        return Err(ParseError::UnexpectedToken {
            expected: "table alias column count to match source columns",
            actual,
        });
    }

    let mut desc = scope.desc.clone();
    let mut columns = scope.columns.clone();
    let mut relations = scope.relations.clone();
    let mut renamed = false;
    if let Some(rte) = plan.rtable.last_mut() {
        let preserve_rte_name = rte.alias_preserves_source_names;
        rte.alias = Some(alias.to_string());
        rte.alias_preserves_source_names = preserve_source_names;
        if !preserve_rte_name {
            rte.eref.aliasname = alias.to_string();
        }
    }

    let aliasable_single_function_column =
        if alias_single_function_output && column_aliases.is_empty() {
            if visible_positions.len() == 1 {
                Some(visible_positions[0])
            } else if visible_positions.len() == 2
                && desc.columns[visible_positions[1]]
                    .name
                    .eq_ignore_ascii_case("ordinality")
            {
                Some(visible_positions[0])
            } else {
                None
            }
        } else {
            None
        };
    if let Some(column_index) = aliasable_single_function_column {
        let column = &mut columns[column_index];
        renamed |= column.output_name != alias;
        column.output_name = alias.to_string();
        desc.columns[column_index].name = alias.to_string();
        desc.columns[column_index].storage.name = alias.to_string();
    }

    let alias_is_source_name = !source_is_alias
        && relations.len() == 1
        && relations[0]
            .relation_names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(alias));
    if !alias_is_source_name
        && (relations.iter().any(|relation| {
            relation
                .relation_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(alias))
        }) || columns.iter().any(|column| {
            column
                .relation_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(alias))
        }))
    {
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
        let join_using_alias_merged_cols = plan.rtable.last().and_then(|rte| {
            if let RangeTblEntryKind::Join { joinmergedcols, .. } = &rte.kind
                && *joinmergedcols > 0
            {
                Some(*joinmergedcols)
            } else {
                None
            }
        });
        let alias_only_anonymous = columns
            .iter()
            .any(|column| column.relation_names.is_empty());
        for (index, column) in columns.iter_mut().enumerate() {
            if join_using_alias_merged_cols.is_some_and(|merged_cols| index >= merged_cols) {
                continue;
            }
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
        if join_using_alias_merged_cols.is_none() {
            let relation_alias_only_anonymous = relations
                .iter()
                .any(|relation| relation.relation_names.is_empty());
            if relations.is_empty() {
                relations.push(ScopeRelation {
                    relation_names: vec![alias.to_string()],
                    hidden_invalid_relation_names: vec![],
                    hidden_missing_relation_names: vec![],
                    system_varno: None,
                    relation_oid: None,
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
                relation_oid: None,
            }];
        }
    }

    let function_alias_applied = apply_function_rte_alias(&mut plan, alias, &desc);

    if !function_alias_applied {
        let output_columns = desc
            .columns
            .iter()
            .map(|column| QueryColumn {
                name: column.name.clone(),
                sql_type: column.sql_type,
                wire_type_oid: None,
            })
            .collect::<Vec<_>>();
        if let Some(rte) = plan.rtable.last_mut() {
            rte.desc = desc.clone();
            rte.eref.colnames = output_columns
                .iter()
                .map(|column| column.name.clone())
                .collect();
        }
        plan.output_columns = output_columns;
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
