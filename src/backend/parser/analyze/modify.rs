use super::paths::choose_modify_row_source;
use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundInsertStatement {
    pub relation_name: String,
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub toast: Option<ToastRelationRef>,
    pub toast_index: Option<BoundIndexRelation>,
    pub desc: RelationDesc,
    pub relation_constraints: BoundRelationConstraints,
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
    pub relation_name: String,
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub toast: Option<ToastRelationRef>,
    pub toast_index: Option<BoundIndexRelation>,
    pub desc: RelationDesc,
    pub relation_constraints: BoundRelationConstraints,
    pub indexes: Vec<BoundIndexRelation>,
    pub column_defaults: Vec<Expr>,
    pub target_columns: Vec<usize>,
    pub num_params: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundUpdateTarget {
    pub relation_name: String,
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub toast: Option<ToastRelationRef>,
    pub toast_index: Option<BoundIndexRelation>,
    pub desc: RelationDesc,
    pub relation_constraints: BoundRelationConstraints,
    pub row_source: BoundModifyRowSource,
    pub indexes: Vec<BoundIndexRelation>,
    pub assignments: Vec<BoundAssignment>,
    pub predicate: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundUpdateStatement {
    pub targets: Vec<BoundUpdateTarget>,
    pub subplans: Vec<Plan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundDeleteTarget {
    pub relation_name: String,
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub toast: Option<ToastRelationRef>,
    pub desc: RelationDesc,
    pub row_source: BoundModifyRowSource,
    pub predicate: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundDeleteStatement {
    pub targets: Vec<BoundDeleteTarget>,
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

fn relation_display_name(catalog: &dyn CatalogLookup, relation_oid: u32, fallback: &str) -> String {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| fallback.to_string())
}

fn inheritance_translation_indexes(
    parent_desc: &RelationDesc,
    child_desc: &RelationDesc,
) -> Vec<Option<usize>> {
    parent_desc
        .columns
        .iter()
        .map(|parent_column| {
            child_desc
                .columns
                .iter()
                .enumerate()
                .find(|(_, child_column)| {
                    !child_column.dropped
                        && child_column.name.eq_ignore_ascii_case(&parent_column.name)
                        && child_column.sql_type == parent_column.sql_type
                })
                .map(|(index, _)| index)
        })
        .collect()
}

fn inheritance_translation_layout(indexes: &[Option<usize>]) -> Vec<Expr> {
    indexes
        .iter()
        .map(|index| match index {
            Some(index) => Expr::Column(*index),
            None => Expr::Const(Value::Null),
        })
        .collect()
}

fn translated_child_column_index(
    parent_index: usize,
    indexes: &[Option<usize>],
    relation_name: &str,
) -> Result<usize, ParseError> {
    match indexes.get(parent_index).copied().flatten() {
        Some(index) => Ok(index),
        _ => Err(ParseError::UnexpectedToken {
            expected: "inherited target column present in child relation",
            actual: format!(
                "column {} has no compatible inherited mapping in relation \"{}\"",
                parent_index + 1,
                relation_name
            ),
        }),
    }
}

fn build_update_target(
    base_relation_name: &str,
    parent_desc: &RelationDesc,
    parent_assignments: &[BoundAssignment],
    parent_predicate: Option<&Expr>,
    child: &BoundRelation,
    catalog: &dyn CatalogLookup,
) -> Result<BoundUpdateTarget, ParseError> {
    let relation_name = relation_display_name(catalog, child.relation_oid, base_relation_name);
    let translation_indexes = inheritance_translation_indexes(parent_desc, &child.desc);
    let layout = inheritance_translation_layout(&translation_indexes);
    let indexes = catalog.index_relations_for_heap(child.relation_oid);
    let predicate = parent_predicate.map(|expr| rewrite_expr_columns(expr.clone(), &layout));
    let assignments = parent_assignments
        .iter()
        .map(|assignment| {
            Ok(BoundAssignment {
                column_index: translated_child_column_index(
                    assignment.column_index,
                    &translation_indexes,
                    &relation_name,
                )?,
                subscripts: assignment.subscripts.clone(),
                expr: rewrite_expr_columns(assignment.expr.clone(), &layout),
            })
        })
        .collect::<Result<Vec<_>, ParseError>>()?;

    Ok(BoundUpdateTarget {
        relation_name: relation_name.clone(),
        rel: child.rel,
        relation_oid: child.relation_oid,
        toast: child.toast,
        toast_index: first_toast_index(catalog, child.toast),
        desc: child.desc.clone(),
        relation_constraints: bind_relation_constraints(
            Some(&relation_name),
            child.relation_oid,
            &child.desc,
            catalog,
        )?,
        row_source: choose_modify_row_source(predicate.as_ref(), &indexes),
        indexes,
        assignments,
        predicate,
    })
}

fn build_delete_target(
    base_relation_name: &str,
    parent_desc: &RelationDesc,
    parent_predicate: Option<&Expr>,
    child: &BoundRelation,
    catalog: &dyn CatalogLookup,
) -> BoundDeleteTarget {
    let relation_name = relation_display_name(catalog, child.relation_oid, base_relation_name);
    let layout = inheritance_translation_layout(&inheritance_translation_indexes(
        parent_desc,
        &child.desc,
    ));
    let predicate = parent_predicate.map(|expr| rewrite_expr_columns(expr.clone(), &layout));
    let indexes = catalog.index_relations_for_heap(child.relation_oid);

    BoundDeleteTarget {
        relation_name,
        rel: child.rel,
        relation_oid: child.relation_oid,
        toast: child.toast,
        desc: child.desc.clone(),
        row_source: choose_modify_row_source(predicate.as_ref(), &indexes),
        predicate,
    }
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
        relation_name: table_name.to_string(),
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        toast: entry.toast,
        toast_index: first_toast_index(catalog, entry.toast),
        desc: entry.desc.clone(),
        relation_constraints: bind_relation_constraints(
            Some(table_name),
            entry.relation_oid,
            &entry.desc,
            catalog,
        )?,
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
    let local_ctes = bind_ctes(
        stmt.with_recursive,
        &stmt.with,
        catalog,
        &[],
        None,
        &[],
        &[],
    )?;
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
        relation_name: stmt.table_name.clone(),
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        toast: entry.toast,
        toast_index: first_toast_index(catalog, entry.toast),
        desc: entry.desc.clone(),
        relation_constraints: bind_relation_constraints(
            Some(&stmt.table_name),
            entry.relation_oid,
            &entry.desc,
            catalog,
        )?,
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
    let local_ctes = bind_ctes(
        stmt.with_recursive,
        &stmt.with,
        catalog,
        &[],
        None,
        &[],
        &[],
    )?;
    let entry = lookup_relation(catalog, &stmt.table_name)?;
    let scope = scope_for_relation(Some(&stmt.table_name), &entry.desc);
    let predicate = stmt
        .where_clause
        .as_ref()
        .map(|expr| bind_expr_with_outer_and_ctes(expr, &scope, catalog, &[], None, &local_ctes))
        .transpose()?;
    let assignments = stmt
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
        .collect::<Result<Vec<_>, ParseError>>()?;

    let targets = if stmt.only {
        vec![entry.relation_oid]
    } else {
        catalog.find_all_inheritors(entry.relation_oid)
    }
    .into_iter()
    .map(|relation_oid| {
        let child = catalog
            .relation_by_oid(relation_oid)
            .ok_or_else(|| ParseError::UnknownTable(stmt.table_name.clone()))?;
        build_update_target(
            &stmt.table_name,
            &entry.desc,
            &assignments,
            predicate.as_ref(),
            &child,
            catalog,
        )
    })
    .collect::<Result<Vec<_>, ParseError>>()?;

    Ok(BoundUpdateStatement {
        targets,
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
    let local_ctes = bind_ctes(
        stmt.with_recursive,
        &stmt.with,
        catalog,
        &[],
        None,
        &[],
        &[],
    )?;
    let entry = lookup_relation(catalog, &stmt.table_name)?;
    let scope = scope_for_relation(Some(&stmt.table_name), &entry.desc);
    let predicate = stmt
        .where_clause
        .as_ref()
        .map(|expr| bind_expr_with_outer_and_ctes(expr, &scope, catalog, &[], None, &local_ctes))
        .transpose()?;

    let targets = if stmt.only {
        vec![entry.relation_oid]
    } else {
        catalog.find_all_inheritors(entry.relation_oid)
    }
    .into_iter()
    .map(|relation_oid| {
        let child = catalog
            .relation_by_oid(relation_oid)
            .ok_or_else(|| ParseError::UnknownTable(stmt.table_name.clone()))?;
        Ok(build_delete_target(
            &stmt.table_name,
            &entry.desc,
            predicate.as_ref(),
            &child,
            catalog,
        ))
    })
    .collect::<Result<Vec<_>, ParseError>>()?;

    Ok(BoundDeleteStatement {
        targets,
        subplans: Vec::new(),
    })
}
