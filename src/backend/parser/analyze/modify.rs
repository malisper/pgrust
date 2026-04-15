use super::paths::choose_modify_row_source;
use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundInsertStatement {
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub toast: Option<ToastRelationRef>,
    pub toast_index: Option<BoundIndexRelation>,
    pub desc: RelationDesc,
    pub indexes: Vec<BoundIndexRelation>,
    pub column_defaults: Vec<Expr>,
    pub target_columns: Vec<BoundAssignmentTarget>,
    pub source: BoundInsertSource,
    pub subplans: Vec<Plan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundInsertSource {
    Values(Vec<Vec<Expr>>),
    DefaultValues(Vec<Expr>),
    Select(Box<Query>),
}

/// A pre-bound insert plan that can be executed repeatedly with different
/// parameter values, avoiding re-parsing and re-binding on each call.
#[derive(Debug, Clone)]
pub struct PreparedInsert {
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub toast: Option<ToastRelationRef>,
    pub toast_index: Option<BoundIndexRelation>,
    pub desc: RelationDesc,
    pub indexes: Vec<BoundIndexRelation>,
    pub column_defaults: Vec<Expr>,
    pub target_columns: Vec<usize>,
    pub num_params: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundUpdateStatement {
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub toast: Option<ToastRelationRef>,
    pub toast_index: Option<BoundIndexRelation>,
    pub desc: RelationDesc,
    pub row_source: BoundModifyRowSource,
    pub indexes: Vec<BoundIndexRelation>,
    pub assignments: Vec<BoundAssignment>,
    pub predicate: Option<Expr>,
    pub subplans: Vec<Plan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundDeleteStatement {
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub toast: Option<ToastRelationRef>,
    pub desc: RelationDesc,
    pub row_source: BoundModifyRowSource,
    pub predicate: Option<Expr>,
    pub subplans: Vec<Plan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundAssignment {
    pub column_index: usize,
    pub subscripts: Vec<BoundArraySubscript>,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundAssignmentTarget {
    pub column_index: usize,
    pub subscripts: Vec<BoundArraySubscript>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundArraySubscript {
    pub is_slice: bool,
    pub lower: Option<Expr>,
    pub upper: Option<Expr>,
}

fn first_toast_index(
    catalog: &dyn CatalogLookup,
    toast: Option<ToastRelationRef>,
) -> Option<BoundIndexRelation> {
    let toast = toast?;
    catalog
        .index_relations_for_heap(toast.relation_oid)
        .into_iter()
        .next()
}

fn bind_insert_column_defaults(
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
    local_ctes: &[BoundCte],
) -> Result<Vec<Expr>, ParseError> {
    desc.columns
        .iter()
        .map(|column| {
            column
                .default_expr
                .as_ref()
                .map(|sql| {
                    let expr = crate::backend::parser::parse_expr(sql)?;
                    bind_expr_with_outer_and_ctes(
                        &expr,
                        &empty_scope(),
                        catalog,
                        &[],
                        None,
                        local_ctes,
                    )
                })
                .transpose()
                .map(|expr| expr.unwrap_or(Expr::Const(Value::Null)))
        })
        .collect()
}

fn visible_assignment_targets(desc: &RelationDesc) -> Vec<BoundAssignmentTarget> {
    desc.visible_column_indexes()
        .into_iter()
        .map(|column_index| BoundAssignmentTarget {
            column_index,
            subscripts: Vec::new(),
        })
        .collect()
}

pub fn bind_insert_prepared(
    table_name: &str,
    columns: Option<&[String]>,
    num_params: usize,
    catalog: &dyn CatalogLookup,
) -> Result<PreparedInsert, ParseError> {
    let entry = lookup_relation(catalog, table_name)?;
    let column_defaults = bind_insert_column_defaults(&entry.desc, catalog, &[])?;

    let target_columns = if let Some(columns) = columns {
        let scope = scope_for_relation(Some(table_name), &entry.desc);
        columns
            .iter()
            .map(|column| resolve_column(&scope, column))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        let visible_indexes = entry.desc.visible_column_indexes();
        if num_params > visible_indexes.len() {
            return Err(ParseError::InvalidInsertTargetCount {
                expected: visible_indexes.len(),
                actual: num_params,
            });
        }
        visible_indexes.into_iter().take(num_params).collect()
    };

    if target_columns.len() != num_params {
        return Err(ParseError::InvalidInsertTargetCount {
            expected: target_columns.len(),
            actual: num_params,
        });
    }

    Ok(PreparedInsert {
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        toast: entry.toast,
        toast_index: first_toast_index(catalog, entry.toast),
        desc: entry.desc.clone(),
        indexes: catalog.index_relations_for_heap(entry.relation_oid),
        column_defaults,
        target_columns,
        num_params,
    })
}

pub fn bind_insert(
    stmt: &InsertStatement,
    catalog: &dyn CatalogLookup,
) -> Result<BoundInsertStatement, ParseError> {
    let local_ctes = bind_ctes(&stmt.with, catalog, &[], None, &[], &[])?;
    let entry = lookup_relation(catalog, &stmt.table_name)?;
    let column_defaults = bind_insert_column_defaults(&entry.desc, catalog, &local_ctes)?;
    let scope = scope_for_relation(Some(&stmt.table_name), &entry.desc);

    let source = match &stmt.source {
        InsertSource::Values(rows) => {
            let target_columns = if let Some(columns) = &stmt.columns {
                columns
                    .iter()
                    .map(|column| bind_assignment_target(column, &scope, catalog, &local_ctes))
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                let visible_targets = visible_assignment_targets(&entry.desc);
                let width = rows.first().map(Vec::len).unwrap_or(0);
                if width > visible_targets.len() {
                    return Err(ParseError::InvalidInsertTargetCount {
                        expected: visible_targets.len(),
                        actual: width,
                    });
                }
                visible_targets.into_iter().take(width).collect()
            };
            for row in rows {
                if target_columns.len() != row.len() {
                    return Err(ParseError::InvalidInsertTargetCount {
                        expected: target_columns.len(),
                        actual: row.len(),
                    });
                }
            }
            let bound_rows = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .zip(target_columns.iter())
                        .map(|(expr, target)| match expr {
                            SqlExpr::Default => Ok(column_defaults[target.column_index].clone()),
                            _ => bind_expr_with_outer_and_ctes(
                                expr,
                                &scope,
                                catalog,
                                &[],
                                None,
                                &local_ctes,
                            ),
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()?;
            (target_columns, BoundInsertSource::Values(bound_rows))
        }
        InsertSource::DefaultValues => (
            visible_assignment_targets(&entry.desc),
            BoundInsertSource::DefaultValues(
                entry
                    .desc
                    .visible_column_indexes()
                    .into_iter()
                    .map(|column_index| column_defaults[column_index].clone())
                    .collect(),
            ),
        ),
        InsertSource::Select(select) => {
            let (query, _) =
                analyze_select_query_with_outer(select, catalog, &[], None, &local_ctes, &[])?;
            let actual = query.columns().len();
            let target_columns = if let Some(columns) = &stmt.columns {
                columns
                    .iter()
                    .map(|column| bind_assignment_target(column, &scope, catalog, &local_ctes))
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                let visible_targets = visible_assignment_targets(&entry.desc);
                if actual > visible_targets.len() {
                    return Err(ParseError::InvalidInsertTargetCount {
                        expected: visible_targets.len(),
                        actual,
                    });
                }
                visible_targets.into_iter().take(actual).collect()
            };
            if target_columns.len() != actual {
                return Err(ParseError::InvalidInsertTargetCount {
                    expected: target_columns.len(),
                    actual,
                });
            }
            (target_columns, BoundInsertSource::Select(Box::new(query)))
        }
    };
    let (target_columns, source) = source;

    Ok(BoundInsertStatement {
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        toast: entry.toast,
        toast_index: first_toast_index(catalog, entry.toast),
        desc: entry.desc.clone(),
        indexes: catalog.index_relations_for_heap(entry.relation_oid),
        column_defaults,
        target_columns,
        source,
        subplans: Vec::new(),
    })
}

pub fn bind_update(
    stmt: &UpdateStatement,
    catalog: &dyn CatalogLookup,
) -> Result<BoundUpdateStatement, ParseError> {
    let local_ctes = bind_ctes(&stmt.with, catalog, &[], None, &[], &[])?;
    let entry = lookup_relation(catalog, &stmt.table_name)?;
    if catalog.has_subclass(entry.relation_oid) {
        return Err(ParseError::FeatureNotSupported(
            "UPDATE on inherited parents is not supported yet".into(),
        ));
    }
    let scope = scope_for_relation(Some(&stmt.table_name), &entry.desc);
    let indexes = catalog.index_relations_for_heap(entry.relation_oid);
    let predicate = stmt
        .where_clause
        .as_ref()
        .map(|expr| bind_expr_with_outer_and_ctes(expr, &scope, catalog, &[], None, &local_ctes))
        .transpose()?;

    Ok(BoundUpdateStatement {
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        toast: entry.toast,
        toast_index: first_toast_index(catalog, entry.toast),
        desc: entry.desc.clone(),
        row_source: choose_modify_row_source(predicate.as_ref(), &indexes),
        indexes,
        assignments: stmt
            .assignments
            .iter()
            .map(|assignment| {
                Ok(BoundAssignment {
                    column_index: resolve_column(&scope, &assignment.target.column)?,
                    subscripts: bind_assignment_subscripts(
                        &assignment.target.subscripts,
                        &scope,
                        catalog,
                        &local_ctes,
                    )?,
                    expr: bind_expr_with_outer_and_ctes(
                        &assignment.expr,
                        &scope,
                        catalog,
                        &[],
                        None,
                        &local_ctes,
                    )?,
                })
            })
            .collect::<Result<Vec<_>, ParseError>>()?,
        predicate,
        subplans: Vec::new(),
    })
}

fn bind_assignment_target(
    target: &crate::include::nodes::parsenodes::AssignmentTarget,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    local_ctes: &[BoundCte],
) -> Result<BoundAssignmentTarget, ParseError> {
    Ok(BoundAssignmentTarget {
        column_index: resolve_column(scope, &target.column)?,
        subscripts: bind_assignment_subscripts(&target.subscripts, scope, catalog, local_ctes)?,
    })
}

fn bind_assignment_subscripts(
    subscripts: &[crate::include::nodes::parsenodes::ArraySubscript],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    local_ctes: &[BoundCte],
) -> Result<Vec<BoundArraySubscript>, ParseError> {
    subscripts
        .iter()
        .map(|subscript| {
            Ok(BoundArraySubscript {
                is_slice: subscript.is_slice,
                lower: subscript
                    .lower
                    .as_deref()
                    .map(|expr| {
                        bind_expr_with_outer_and_ctes(expr, scope, catalog, &[], None, local_ctes)
                    })
                    .transpose()?,
                upper: subscript
                    .upper
                    .as_deref()
                    .map(|expr| {
                        bind_expr_with_outer_and_ctes(expr, scope, catalog, &[], None, local_ctes)
                    })
                    .transpose()?,
            })
        })
        .collect()
}

pub fn bind_delete(
    stmt: &DeleteStatement,
    catalog: &dyn CatalogLookup,
) -> Result<BoundDeleteStatement, ParseError> {
    let local_ctes = bind_ctes(&stmt.with, catalog, &[], None, &[], &[])?;
    let entry = lookup_relation(catalog, &stmt.table_name)?;
    if catalog.has_subclass(entry.relation_oid) {
        return Err(ParseError::FeatureNotSupported(
            "DELETE on inherited parents is not supported yet".into(),
        ));
    }
    let scope = scope_for_relation(Some(&stmt.table_name), &entry.desc);
    let predicate = stmt
        .where_clause
        .as_ref()
        .map(|expr| bind_expr_with_outer_and_ctes(expr, &scope, catalog, &[], None, &local_ctes))
        .transpose()?;
    let indexes = catalog.index_relations_for_heap(entry.relation_oid);

    Ok(BoundDeleteStatement {
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        toast: entry.toast,
        desc: entry.desc.clone(),
        row_source: choose_modify_row_source(predicate.as_ref(), &indexes),
        predicate,
        subplans: Vec::new(),
    })
}
