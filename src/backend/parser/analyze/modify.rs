use super::paths::choose_modify_row_source;
use super::query::rewrite_local_vars_for_output_exprs;
use super::*;
use crate::backend::rewrite::{
    RlsWriteCheck, ViewDmlEvent, ViewDmlRewriteError, build_target_relation_row_security,
    relation_has_row_security, resolve_auto_updatable_view_target,
};
use crate::include::catalog::PolicyCommand;
use crate::include::nodes::primnodes::JoinType;
use crate::include::nodes::primnodes::{SELF_ITEM_POINTER_ATTR_NO, TargetEntry, Var};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundInsertStatement {
    pub relation_name: String,
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub relkind: char,
    pub toast: Option<ToastRelationRef>,
    pub toast_index: Option<BoundIndexRelation>,
    pub desc: RelationDesc,
    pub relation_constraints: BoundRelationConstraints,
    pub referenced_by_foreign_keys: Vec<BoundReferencedByForeignKey>,
    pub indexes: Vec<BoundIndexRelation>,
    pub column_defaults: Vec<Expr>,
    pub target_columns: Vec<BoundAssignmentTarget>,
    pub source: BoundInsertSource,
    pub on_conflict: Option<BoundOnConflictClause>,
    pub returning: Vec<TargetEntry>,
    pub(crate) rls_write_checks: Vec<RlsWriteCheck>,
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
    pub relkind: char,
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
    pub relkind: char,
    pub toast: Option<ToastRelationRef>,
    pub toast_index: Option<BoundIndexRelation>,
    pub desc: RelationDesc,
    pub relation_constraints: BoundRelationConstraints,
    pub referenced_by_foreign_keys: Vec<BoundReferencedByForeignKey>,
    pub row_source: BoundModifyRowSource,
    pub indexes: Vec<BoundIndexRelation>,
    pub assignments: Vec<BoundAssignment>,
    pub predicate: Option<Expr>,
    pub(crate) rls_write_checks: Vec<RlsWriteCheck>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundUpdateStatement {
    pub targets: Vec<BoundUpdateTarget>,
    pub returning: Vec<TargetEntry>,
    pub subplans: Vec<Plan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundDeleteTarget {
    pub relation_name: String,
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub relkind: char,
    pub toast: Option<ToastRelationRef>,
    pub desc: RelationDesc,
    pub referenced_by_foreign_keys: Vec<BoundReferencedByForeignKey>,
    pub row_source: BoundModifyRowSource,
    pub predicate: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundDeleteStatement {
    pub targets: Vec<BoundDeleteTarget>,
    pub returning: Vec<TargetEntry>,
    pub subplans: Vec<Plan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundMergeStatement {
    pub relation_name: String,
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub toast: Option<ToastRelationRef>,
    pub toast_index: Option<BoundIndexRelation>,
    pub desc: RelationDesc,
    pub relation_constraints: BoundRelationConstraints,
    pub referenced_by_foreign_keys: Vec<BoundReferencedByForeignKey>,
    pub indexes: Vec<BoundIndexRelation>,
    pub column_defaults: Vec<Expr>,
    pub target_relation_name: String,
    pub explain_target_name: String,
    pub visible_column_count: usize,
    pub target_ctid_index: usize,
    pub source_present_index: usize,
    pub when_clauses: Vec<BoundMergeWhenClause>,
    pub input_plan: crate::include::nodes::plannodes::PlannedStmt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundMergeWhenClause {
    pub match_kind: MergeMatchKind,
    pub condition: Option<Expr>,
    pub action: BoundMergeAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundMergeAction {
    DoNothing,
    Delete,
    Update {
        assignments: Vec<BoundAssignment>,
    },
    Insert {
        target_columns: Vec<BoundAssignmentTarget>,
        values: Option<Vec<Expr>>,
    },
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

fn merge_target_relation_name(stmt: &MergeStatement) -> String {
    stmt.target_alias
        .clone()
        .unwrap_or_else(|| stmt.target_table.clone())
}

fn merge_explain_target_name(stmt: &MergeStatement) -> String {
    stmt.target_alias
        .as_ref()
        .map(|alias| format!("{} {}", stmt.target_table, alias))
        .unwrap_or_else(|| stmt.target_table.clone())
}

fn merge_hidden_ctid_name() -> String {
    "__merge_target_ctid".into()
}

fn merge_hidden_source_present_name() -> String {
    "__merge_source_present".into()
}

fn merge_join_type(clauses: &[MergeWhenClause]) -> JoinType {
    let mut need_target_rows = false;
    let mut need_source_rows = false;
    for clause in clauses {
        match clause.match_kind {
            MergeMatchKind::Matched => {}
            MergeMatchKind::NotMatchedBySource => need_target_rows = true,
            MergeMatchKind::NotMatchedByTarget => need_source_rows = true,
        }
    }
    match (need_target_rows, need_source_rows) {
        (false, false) => JoinType::Inner,
        (true, false) => JoinType::Left,
        (false, true) => JoinType::Right,
        (true, true) => JoinType::Full,
    }
}

fn unsupported_with_row_security(feature: &str) -> ParseError {
    ParseError::FeatureNotSupportedMessage(format!(
        "{feature} is not yet supported on tables with row-level security"
    ))
}

fn merge_visible_insert_targets(
    desc: &RelationDesc,
    width: usize,
) -> Result<Vec<BoundAssignmentTarget>, ParseError> {
    let visible_targets = visible_assignment_targets(desc);
    if width > visible_targets.len() {
        return Err(ParseError::InvalidInsertTargetCount {
            expected: visible_targets.len(),
            actual: width,
        });
    }
    Ok(visible_targets.into_iter().take(width).collect())
}

fn bind_merge_when_clause(
    clause: &MergeWhenClause,
    target_scope: &BoundScope,
    source_scope: &BoundScope,
    merged_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    local_ctes: &[BoundCte],
    target_desc: &RelationDesc,
) -> Result<BoundMergeWhenClause, ParseError> {
    let action_scope = match clause.match_kind {
        MergeMatchKind::Matched => merged_scope,
        MergeMatchKind::NotMatchedBySource => target_scope,
        MergeMatchKind::NotMatchedByTarget => source_scope,
    };
    let condition = clause
        .condition
        .as_ref()
        .map(|condition| {
            bind_expr_with_outer_and_ctes(condition, action_scope, catalog, &[], None, local_ctes)
        })
        .transpose()?;
    let action = match &clause.action {
        MergeAction::DoNothing => BoundMergeAction::DoNothing,
        MergeAction::Delete => BoundMergeAction::Delete,
        MergeAction::Update { assignments } => {
            let assignments = assignments
                .iter()
                .map(|assignment| {
                    Ok(BoundAssignment {
                        column_index: resolve_column(target_scope, &assignment.target.column)?,
                        subscripts: bind_assignment_subscripts(
                            &assignment.target.subscripts,
                            target_scope,
                            catalog,
                            local_ctes,
                            &[],
                        )?,
                        expr: bind_expr_with_outer_and_ctes(
                            &assignment.expr,
                            action_scope,
                            catalog,
                            &[],
                            None,
                            local_ctes,
                        )?,
                    })
                })
                .collect::<Result<Vec<_>, ParseError>>()?;
            BoundMergeAction::Update { assignments }
        }
        MergeAction::Insert { columns, source } => {
            let target_columns = if let Some(columns) = columns {
                columns
                    .iter()
                    .map(|column| {
                        Ok(BoundAssignmentTarget {
                            column_index: resolve_column(target_scope, column)?,
                            subscripts: Vec::new(),
                        })
                    })
                    .collect::<Result<Vec<_>, ParseError>>()?
            } else {
                let width = match source {
                    MergeInsertSource::Values(values) => values.len(),
                    MergeInsertSource::DefaultValues => target_desc.visible_column_indexes().len(),
                };
                merge_visible_insert_targets(target_desc, width)?
            };
            let values = match source {
                MergeInsertSource::Values(values) => {
                    if values.len() != target_columns.len() {
                        return Err(ParseError::InvalidInsertTargetCount {
                            expected: target_columns.len(),
                            actual: values.len(),
                        });
                    }
                    Some(
                        values
                            .iter()
                            .map(|expr| {
                                bind_expr_with_outer_and_ctes(
                                    expr,
                                    action_scope,
                                    catalog,
                                    &[],
                                    None,
                                    local_ctes,
                                )
                            })
                            .collect::<Result<Vec<_>, ParseError>>()?,
                    )
                }
                MergeInsertSource::DefaultValues => None,
            };
            BoundMergeAction::Insert {
                target_columns,
                values,
            }
        }
    };
    Ok(BoundMergeWhenClause {
        match_kind: clause.match_kind,
        condition,
        action,
    })
}

fn merge_projection_targets(columns: &[QueryColumn], output_exprs: &[Expr]) -> Vec<TargetEntry> {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            TargetEntry::new(
                column.name.clone(),
                output_exprs[index].clone(),
                column.sql_type,
                index + 1,
            )
            .with_input_resno(index + 1)
        })
        .collect()
}

fn bind_returning_targets(
    targets: &[crate::include::nodes::parsenodes::SelectItem],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    local_ctes: &[BoundCte],
) -> Result<Vec<TargetEntry>, ParseError> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }
    match bind_select_targets(targets, scope, catalog, outer_scopes, None, local_ctes)? {
        BoundSelectTargets::Plain(targets) => Ok(targets),
        BoundSelectTargets::WithProjectSet { .. } => Err(ParseError::FeatureNotSupported(
            "set-returning functions are not allowed in RETURNING".into(),
        )),
    }
}

fn with_merge_target_ctid(from: AnalyzedFrom, target_desc: &RelationDesc) -> (AnalyzedFrom, usize) {
    let mut targets = merge_projection_targets(&from.output_columns, &from.output_exprs);
    let ctid_resno = targets.len() + 1;
    targets.push(
        TargetEntry::new(
            merge_hidden_ctid_name(),
            Expr::Var(Var {
                varno: 1,
                varattno: SELF_ITEM_POINTER_ATTR_NO,
                varlevelsup: 0,
                vartype: SqlType::new(SqlTypeKind::Text),
            }),
            SqlType::new(SqlTypeKind::Text),
            ctid_resno,
        )
        .with_input_resno(ctid_resno),
    );
    let projected = from.with_projection(targets);
    (projected, target_desc.columns.len())
}

fn with_merge_source_present(from: AnalyzedFrom) -> (AnalyzedFrom, usize) {
    let source_visible_count = from.output_columns.len();
    let mut targets = merge_projection_targets(&from.output_columns, &from.output_exprs);
    let marker_resno = targets.len() + 1;
    targets.push(
        TargetEntry::new(
            merge_hidden_source_present_name(),
            Expr::Const(Value::Bool(true)),
            SqlType::new(SqlTypeKind::Bool),
            marker_resno,
        )
        .with_input_resno(marker_resno),
    );
    let projected = from.with_projection(targets);
    (projected, source_visible_count)
}

pub fn plan_merge(
    stmt: &MergeStatement,
    catalog: &dyn CatalogLookup,
) -> Result<BoundMergeStatement, ParseError> {
    let local_ctes = bind_ctes(
        stmt.with_recursive,
        &stmt.with,
        catalog,
        &[],
        None,
        &[],
        &[],
    )?;
    let entry = lookup_relation(catalog, &stmt.target_table)?;
    if relation_has_row_security(entry.relation_oid, catalog) {
        return Err(unsupported_with_row_security("MERGE"));
    }
    let column_defaults = bind_insert_column_defaults(&entry.desc, catalog, &local_ctes)?;
    let target_relation_name = merge_target_relation_name(stmt);
    let explain_target_name = merge_explain_target_name(stmt);
    let target_base = AnalyzedFrom::relation(
        target_relation_name.clone(),
        entry.rel,
        entry.relation_oid,
        entry.relkind,
        entry.toast,
        !stmt.target_only,
        entry.desc.clone(),
    );
    let (target_from, target_visible_count) = with_merge_target_ctid(target_base, &entry.desc);
    let target_scope = scope_for_base_relation(&target_relation_name, &entry.desc);
    let (source_base, source_scope_raw) =
        bind_from_item_with_ctes(&stmt.source, catalog, &[], None, &local_ctes, &[])?;
    let (source_from, source_visible_count) = with_merge_source_present(source_base);

    if source_scope_raw.relations.iter().any(|relation| {
        relation
            .relation_names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&target_relation_name))
    }) {
        return Err(ParseError::DuplicateTableName(target_relation_name));
    }

    let source_scope = shift_scope_rtindexes(source_scope_raw, target_from.rtable.len());
    let merged_scope = combine_scopes(&target_scope, &source_scope);
    let join_condition = bind_expr_with_outer_and_ctes(
        &stmt.join_condition,
        &merged_scope,
        catalog,
        &[],
        None,
        &local_ctes,
    )?;

    let when_clauses = stmt
        .when_clauses
        .iter()
        .map(|clause| {
            bind_merge_when_clause(
                clause,
                &target_scope,
                &source_scope,
                &merged_scope,
                catalog,
                &local_ctes,
                &entry.desc,
            )
        })
        .collect::<Result<Vec<_>, ParseError>>()?;

    let joined = AnalyzedFrom::join(
        target_from,
        source_from,
        merge_join_type(&stmt.when_clauses),
        join_condition,
        None,
    );
    let visible_column_count = entry.desc.columns.len() + source_visible_count;
    let target_ctid_index = visible_column_count;
    let source_present_index = visible_column_count + 1;
    let joined_target_columns = joined.output_columns.clone();
    let joined_output_exprs = joined.output_exprs.clone();
    let mut projection_targets = Vec::with_capacity(visible_column_count + 2);
    for index in 0..entry.desc.columns.len() {
        projection_targets.push(
            TargetEntry::new(
                joined_target_columns[index].name.clone(),
                joined_output_exprs[index].clone(),
                joined_target_columns[index].sql_type,
                projection_targets.len() + 1,
            )
            .with_input_resno(index + 1),
        );
    }
    let source_start = target_visible_count + 1;
    for source_index in 0..source_visible_count {
        let input_index = source_start + source_index;
        projection_targets.push(
            TargetEntry::new(
                joined_target_columns[input_index - 1].name.clone(),
                joined_output_exprs[input_index - 1].clone(),
                joined_target_columns[input_index - 1].sql_type,
                projection_targets.len() + 1,
            )
            .with_input_resno(input_index),
        );
    }
    projection_targets.push(
        TargetEntry::new(
            merge_hidden_ctid_name(),
            joined_output_exprs[target_visible_count].clone(),
            SqlType::new(SqlTypeKind::Text),
            projection_targets.len() + 1,
        )
        .with_input_resno(target_visible_count + 1),
    );
    let source_marker_input = target_visible_count + 1 + source_visible_count;
    projection_targets.push(
        TargetEntry::new(
            merge_hidden_source_present_name(),
            joined_output_exprs[source_marker_input - 1].clone(),
            SqlType::new(SqlTypeKind::Bool),
            projection_targets.len() + 1,
        )
        .with_input_resno(source_marker_input),
    );
    let query = query_from_from_projection(joined, projection_targets);

    Ok(BoundMergeStatement {
        relation_name: stmt.target_table.clone(),
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        toast: entry.toast,
        toast_index: first_toast_index(catalog, entry.toast),
        desc: entry.desc.clone(),
        relation_constraints: bind_relation_constraints(
            Some(&stmt.target_table),
            entry.relation_oid,
            &entry.desc,
            catalog,
        )?,
        referenced_by_foreign_keys: bind_referenced_by_foreign_keys(
            entry.relation_oid,
            &entry.desc,
            catalog,
        )?,
        indexes: catalog.index_relations_for_heap(entry.relation_oid),
        column_defaults,
        target_relation_name,
        explain_target_name,
        visible_column_count,
        target_ctid_index,
        source_present_index,
        when_clauses,
        input_plan: crate::backend::optimizer::fold_query_constants(query)
            .map(|query| planner(query, catalog))?,
    })
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

fn lookup_modify_relation(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Result<BoundRelation, ParseError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'r' | 'v') => Ok(entry),
        Some(_) => Err(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table or view",
        }),
        None => Err(ParseError::UnknownTable(name.to_string())),
    }
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

fn inheritance_translation_exprs(
    child_desc: &RelationDesc,
    indexes: &[Option<usize>],
) -> Vec<Expr> {
    let child_output_exprs = scope_for_relation(None, child_desc).output_exprs;
    indexes
        .iter()
        .map(|index| match index {
            Some(index) => child_output_exprs.get(*index).cloned().unwrap_or_else(|| {
                panic!(
                    "missing inherited child output expr for column {}",
                    index + 1
                )
            }),
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
    parent_rls_write_checks: &[RlsWriteCheck],
    child: &BoundRelation,
    catalog: &dyn CatalogLookup,
) -> Result<BoundUpdateTarget, ParseError> {
    let relation_name = relation_display_name(catalog, child.relation_oid, base_relation_name);
    let translation_indexes = inheritance_translation_indexes(parent_desc, &child.desc);
    let translation_exprs = inheritance_translation_exprs(&child.desc, &translation_indexes);
    let indexes = catalog.index_relations_for_heap(child.relation_oid);
    let predicate = parent_predicate
        .map(|expr| rewrite_local_vars_for_output_exprs(expr.clone(), 1, &translation_exprs));
    let assignments = parent_assignments
        .iter()
        .map(|assignment| {
            Ok(BoundAssignment {
                column_index: translated_child_column_index(
                    assignment.column_index,
                    &translation_indexes,
                    &relation_name,
                )?,
                subscripts: rewrite_assignment_subscripts(
                    &assignment.subscripts,
                    &translation_exprs,
                ),
                expr: rewrite_local_vars_for_output_exprs(
                    assignment.expr.clone(),
                    1,
                    &translation_exprs,
                ),
            })
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    let rls_write_checks = parent_rls_write_checks
        .iter()
        .map(|check| RlsWriteCheck {
            expr: rewrite_local_vars_for_output_exprs(check.expr.clone(), 1, &translation_exprs),
            policy_name: check.policy_name.clone(),
            source: check.source.clone(),
        })
        .collect();

    Ok(BoundUpdateTarget {
        relation_name: relation_name.clone(),
        rel: child.rel,
        relation_oid: child.relation_oid,
        relkind: child.relkind,
        toast: child.toast,
        toast_index: first_toast_index(catalog, child.toast),
        desc: child.desc.clone(),
        relation_constraints: bind_relation_constraints(
            Some(&relation_name),
            child.relation_oid,
            &child.desc,
            catalog,
        )?,
        referenced_by_foreign_keys: bind_referenced_by_foreign_keys(
            child.relation_oid,
            &child.desc,
            catalog,
        )?,
        row_source: choose_modify_row_source(predicate.as_ref(), &indexes),
        indexes,
        assignments,
        predicate,
        rls_write_checks,
    })
}

fn rewrite_assignment_subscripts(
    subscripts: &[BoundArraySubscript],
    output_exprs: &[Expr],
) -> Vec<BoundArraySubscript> {
    subscripts
        .iter()
        .map(|subscript| BoundArraySubscript {
            is_slice: subscript.is_slice,
            lower: subscript
                .lower
                .as_ref()
                .map(|expr| rewrite_local_vars_for_output_exprs(expr.clone(), 1, output_exprs)),
            upper: subscript
                .upper
                .as_ref()
                .map(|expr| rewrite_local_vars_for_output_exprs(expr.clone(), 1, output_exprs)),
        })
        .collect()
}

fn map_auto_view_column_index(
    view_desc: &RelationDesc,
    updatable_column_map: &[Option<usize>],
    column_index: usize,
) -> Result<usize, ViewDmlRewriteError> {
    updatable_column_map
        .get(column_index)
        .copied()
        .flatten()
        .ok_or_else(|| {
            let column_name = view_desc
                .columns
                .get(column_index)
                .map(|column| column.name.as_str())
                .unwrap_or("<unknown>");
            ViewDmlRewriteError::UnsupportedViewShape(format!(
                "View column \"{}\" is not automatically updatable.",
                column_name
            ))
        })
}

fn rewrite_auto_view_returning_targets(
    targets: Vec<TargetEntry>,
    output_exprs: &[Expr],
) -> Vec<TargetEntry> {
    targets
        .into_iter()
        .map(|target| TargetEntry {
            expr: rewrite_local_vars_for_output_exprs(target.expr, 1, output_exprs),
            ..target
        })
        .collect()
}

pub(crate) fn rewrite_bound_insert_auto_view_target(
    stmt: BoundInsertStatement,
    catalog: &dyn CatalogLookup,
) -> Result<BoundInsertStatement, ViewDmlRewriteError> {
    if stmt.relkind != 'v' {
        return Ok(stmt);
    }

    let resolved = resolve_auto_updatable_view_target(
        stmt.relation_oid,
        &stmt.desc,
        ViewDmlEvent::Insert,
        catalog,
        &[],
    )?;
    if stmt.on_conflict.is_some() {
        return Err(ViewDmlRewriteError::DeferredFeature(
            "INSERT ... ON CONFLICT on automatically updatable views is not supported yet.".into(),
        ));
    }

    let relation_name = relation_display_name(
        catalog,
        resolved.base_relation.relation_oid,
        &stmt.relation_name,
    );
    let target_columns = stmt
        .target_columns
        .iter()
        .map(|target| {
            Ok(BoundAssignmentTarget {
                column_index: map_auto_view_column_index(
                    &stmt.desc,
                    &resolved.updatable_column_map,
                    target.column_index,
                )?,
                subscripts: rewrite_assignment_subscripts(
                    &target.subscripts,
                    &resolved.visible_output_exprs,
                ),
            })
        })
        .collect::<Result<Vec<_>, ViewDmlRewriteError>>()?;

    Ok(BoundInsertStatement {
        relation_name: relation_name.clone(),
        rel: resolved.base_relation.rel,
        relation_oid: resolved.base_relation.relation_oid,
        relkind: resolved.base_relation.relkind,
        toast: resolved.base_relation.toast,
        toast_index: first_toast_index(catalog, resolved.base_relation.toast),
        desc: resolved.base_relation.desc.clone(),
        relation_constraints: bind_relation_constraints(
            Some(&relation_name),
            resolved.base_relation.relation_oid,
            &resolved.base_relation.desc,
            catalog,
        )
        .map_err(|err| ViewDmlRewriteError::UnsupportedViewShape(err.to_string()))?,
        referenced_by_foreign_keys: bind_referenced_by_foreign_keys(
            resolved.base_relation.relation_oid,
            &resolved.base_relation.desc,
            catalog,
        )
        .map_err(|err| ViewDmlRewriteError::UnsupportedViewShape(err.to_string()))?,
        indexes: catalog.index_relations_for_heap(resolved.base_relation.relation_oid),
        column_defaults: bind_insert_column_defaults(&resolved.base_relation.desc, catalog, &[])
            .map_err(|err| ViewDmlRewriteError::UnsupportedViewShape(err.to_string()))?,
        target_columns,
        source: stmt.source,
        on_conflict: None,
        returning: rewrite_auto_view_returning_targets(
            stmt.returning,
            &resolved.visible_output_exprs,
        ),
        rls_write_checks: stmt
            .rls_write_checks
            .into_iter()
            .chain(
                resolved
                    .view_check_options
                    .iter()
                    .cloned()
                    .map(|check| RlsWriteCheck {
                        expr: check.expr,
                        policy_name: None,
                        source: crate::backend::rewrite::RlsWriteCheckSource::ViewCheckOption(
                            check.view_name,
                        ),
                    }),
            )
            .collect(),
        subplans: stmt.subplans,
    })
}

pub(crate) fn rewrite_bound_update_auto_view_target(
    stmt: BoundUpdateStatement,
    catalog: &dyn CatalogLookup,
) -> Result<BoundUpdateStatement, ViewDmlRewriteError> {
    if !stmt.targets.iter().any(|target| target.relkind == 'v') {
        return Ok(stmt);
    }

    let [target] = stmt.targets.as_slice() else {
        return Err(ViewDmlRewriteError::UnsupportedViewShape(
            "Views with multiple update targets are not automatically updatable.".into(),
        ));
    };
    if target.relkind != 'v' {
        return Ok(stmt);
    }

    let resolved = resolve_auto_updatable_view_target(
        target.relation_oid,
        &target.desc,
        ViewDmlEvent::Update,
        catalog,
        &[],
    )?;
    let relation_name = relation_display_name(
        catalog,
        resolved.base_relation.relation_oid,
        &target.relation_name,
    );
    let assignments = target
        .assignments
        .iter()
        .map(|assignment| {
            Ok(BoundAssignment {
                column_index: map_auto_view_column_index(
                    &target.desc,
                    &resolved.updatable_column_map,
                    assignment.column_index,
                )?,
                subscripts: rewrite_assignment_subscripts(
                    &assignment.subscripts,
                    &resolved.visible_output_exprs,
                ),
                expr: rewrite_local_vars_for_output_exprs(
                    assignment.expr.clone(),
                    1,
                    &resolved.visible_output_exprs,
                ),
            })
        })
        .collect::<Result<Vec<_>, ViewDmlRewriteError>>()?;
    let predicate = and_predicates(
        target.predicate.as_ref().map(|expr| {
            rewrite_local_vars_for_output_exprs(expr.clone(), 1, &resolved.visible_output_exprs)
        }),
        resolved.combined_predicate.clone(),
    );

    let targets = auto_view_base_children(&resolved, catalog)?
        .into_iter()
        .map(|child| {
            build_update_target(
                &relation_name,
                &resolved.base_relation.desc,
                &assignments,
                predicate.as_ref(),
                &target.rls_write_checks,
                &child,
                catalog,
            )
            .map_err(|err| ViewDmlRewriteError::UnsupportedViewShape(err.to_string()))
        })
        .collect::<Result<Vec<_>, ViewDmlRewriteError>>()?;

    let targets =
        targets
            .into_iter()
            .map(|mut target| {
                target
                    .rls_write_checks
                    .extend(resolved.view_check_options.iter().cloned().map(|check| {
                        RlsWriteCheck {
                            expr: check.expr,
                            policy_name: None,
                            source: crate::backend::rewrite::RlsWriteCheckSource::ViewCheckOption(
                                check.view_name,
                            ),
                        }
                    }));
                target
            })
            .collect();

    Ok(BoundUpdateStatement {
        targets,
        returning: rewrite_auto_view_returning_targets(
            stmt.returning,
            &resolved.visible_output_exprs,
        ),
        subplans: stmt.subplans,
    })
}

pub(crate) fn rewrite_bound_delete_auto_view_target(
    stmt: BoundDeleteStatement,
    catalog: &dyn CatalogLookup,
) -> Result<BoundDeleteStatement, ViewDmlRewriteError> {
    if !stmt.targets.iter().any(|target| target.relkind == 'v') {
        return Ok(stmt);
    }

    let [target] = stmt.targets.as_slice() else {
        return Err(ViewDmlRewriteError::UnsupportedViewShape(
            "Views with multiple delete targets are not automatically updatable.".into(),
        ));
    };
    if target.relkind != 'v' {
        return Ok(stmt);
    }

    let resolved = resolve_auto_updatable_view_target(
        target.relation_oid,
        &target.desc,
        ViewDmlEvent::Delete,
        catalog,
        &[],
    )?;
    let relation_name = relation_display_name(
        catalog,
        resolved.base_relation.relation_oid,
        &target.relation_name,
    );
    let predicate = and_predicates(
        target.predicate.as_ref().map(|expr| {
            rewrite_local_vars_for_output_exprs(expr.clone(), 1, &resolved.visible_output_exprs)
        }),
        resolved.combined_predicate.clone(),
    );

    let targets = auto_view_base_children(&resolved, catalog)?
        .into_iter()
        .map(|child| {
            build_delete_target(
                &relation_name,
                &resolved.base_relation.desc,
                predicate.as_ref(),
                &child,
                catalog,
            )
            .map_err(|err| ViewDmlRewriteError::UnsupportedViewShape(err.to_string()))
        })
        .collect::<Result<Vec<_>, ViewDmlRewriteError>>()?;

    Ok(BoundDeleteStatement {
        targets,
        returning: rewrite_auto_view_returning_targets(
            stmt.returning,
            &resolved.visible_output_exprs,
        ),
        subplans: stmt.subplans,
    })
}

fn auto_view_base_children(
    resolved: &crate::backend::rewrite::ResolvedAutoViewTarget,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<BoundRelation>, ViewDmlRewriteError> {
    let relation_oids = if resolved.base_inh {
        catalog.find_all_inheritors(resolved.base_relation.relation_oid)
    } else {
        vec![resolved.base_relation.relation_oid]
    };
    relation_oids
        .into_iter()
        .map(|relation_oid| {
            catalog.relation_by_oid(relation_oid).ok_or_else(|| {
                ViewDmlRewriteError::UnsupportedViewShape(format!(
                    "missing inherited child relation {relation_oid}"
                ))
            })
        })
        .collect()
}

fn and_predicates(left: Option<Expr>, right: Option<Expr>) -> Option<Expr> {
    match (left, right) {
        (Some(left), Some(right)) => Some(Expr::and(left, right)),
        (Some(expr), None) | (None, Some(expr)) => Some(expr),
        (None, None) => None,
    }
}

fn build_delete_target(
    base_relation_name: &str,
    parent_desc: &RelationDesc,
    parent_predicate: Option<&Expr>,
    child: &BoundRelation,
    catalog: &dyn CatalogLookup,
) -> Result<BoundDeleteTarget, ParseError> {
    let relation_name = relation_display_name(catalog, child.relation_oid, base_relation_name);
    let translation_exprs = inheritance_translation_exprs(
        &child.desc,
        &inheritance_translation_indexes(parent_desc, &child.desc),
    );
    let predicate = parent_predicate
        .map(|expr| rewrite_local_vars_for_output_exprs(expr.clone(), 1, &translation_exprs));
    let indexes = catalog.index_relations_for_heap(child.relation_oid);

    Ok(BoundDeleteTarget {
        relation_name,
        rel: child.rel,
        relation_oid: child.relation_oid,
        relkind: child.relkind,
        toast: child.toast,
        desc: child.desc.clone(),
        referenced_by_foreign_keys: bind_referenced_by_foreign_keys(
            child.relation_oid,
            &child.desc,
            catalog,
        )?,
        row_source: choose_modify_row_source(predicate.as_ref(), &indexes),
        predicate,
    })
}

fn bind_insert_column_defaults(
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
    local_ctes: &[BoundCte],
) -> Result<Vec<Expr>, ParseError> {
    desc.columns
        .iter()
        .map(|column| {
            if let Some(sequence_oid) = column.default_sequence_oid {
                let expr = Expr::builtin_func(
                    BuiltinScalarFunction::NextVal,
                    Some(SqlType::new(SqlTypeKind::Int8)),
                    false,
                    vec![Expr::Const(Value::Int64(i64::from(sequence_oid)))],
                );
                let expr = if column.sql_type.kind == SqlTypeKind::Int8 {
                    expr
                } else {
                    Expr::Cast(Box::new(expr), column.sql_type)
                };
                return Ok(expr);
            }
            if let Some(value) = column.missing_default_value.clone() {
                return Ok(Expr::Const(value));
            }
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
    if relation_has_row_security(entry.relation_oid, catalog) {
        return Err(unsupported_with_row_security("prepared INSERT"));
    }
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
        relkind: entry.relkind,
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
    bind_insert_with_outer_scopes(stmt, catalog, &[])
}

pub(crate) fn bind_insert_with_outer_scopes(
    stmt: &InsertStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
) -> Result<BoundInsertStatement, ParseError> {
    let local_ctes = bind_ctes(
        stmt.with_recursive,
        &stmt.with,
        catalog,
        outer_scopes,
        None,
        &[],
        &[],
    )?;
    let entry = lookup_modify_relation(catalog, &stmt.table_name)?;
    if stmt.on_conflict.as_ref().is_some_and(|clause| {
        clause.action == crate::include::nodes::parsenodes::OnConflictAction::Update
    }) && relation_has_row_security(entry.relation_oid, catalog)
    {
        return Err(unsupported_with_row_security(
            "INSERT ... ON CONFLICT DO UPDATE",
        ));
    }
    let column_defaults = bind_insert_column_defaults(&entry.desc, catalog, &local_ctes)?;
    let target_rls = build_target_relation_row_security(
        &stmt.table_name,
        entry.relation_oid,
        &entry.desc,
        PolicyCommand::Insert,
        false,
        false,
        catalog,
    )?;
    let visible_target_name = stmt.table_alias.as_deref().unwrap_or(&stmt.table_name);
    let target_scope = scope_for_relation(Some(visible_target_name), &entry.desc);
    let expr_scope = empty_scope();
    let returning = bind_returning_targets(
        &stmt.returning,
        &target_scope,
        catalog,
        outer_scopes,
        &local_ctes,
    )?;

    let source = match &stmt.source {
        InsertSource::Values(rows) => {
            let target_columns = if let Some(columns) = &stmt.columns {
                columns
                    .iter()
                    .map(|column| {
                        bind_assignment_target(column, &target_scope, catalog, &local_ctes)
                    })
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
                                &expr_scope,
                                catalog,
                                outer_scopes,
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
            let (query, _) = analyze_select_query_with_outer(
                select,
                catalog,
                outer_scopes,
                None,
                &local_ctes,
                &[],
            )?;
            let actual = query.columns().len();
            let target_columns = if let Some(columns) = &stmt.columns {
                columns
                    .iter()
                    .map(|column| {
                        bind_assignment_target(column, &target_scope, catalog, &local_ctes)
                    })
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
        relkind: entry.relkind,
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
        referenced_by_foreign_keys: bind_referenced_by_foreign_keys(
            entry.relation_oid,
            &entry.desc,
            catalog,
        )?,
        on_conflict: stmt
            .on_conflict
            .as_ref()
            .map(|clause| {
                super::on_conflict::bind_on_conflict_clause(
                    clause,
                    visible_target_name,
                    entry.relation_oid,
                    &entry.desc,
                    catalog,
                    &local_ctes,
                )
            })
            .transpose()?,
        returning,
        rls_write_checks: target_rls.write_checks,
        subplans: Vec::new(),
    })
}

pub fn bind_update(
    stmt: &UpdateStatement,
    catalog: &dyn CatalogLookup,
) -> Result<BoundUpdateStatement, ParseError> {
    bind_update_with_outer_scopes(stmt, catalog, &[])
}

pub(crate) fn bind_update_with_outer_scopes(
    stmt: &UpdateStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
) -> Result<BoundUpdateStatement, ParseError> {
    let local_ctes = bind_ctes(
        stmt.with_recursive,
        &stmt.with,
        catalog,
        outer_scopes,
        None,
        &[],
        &[],
    )?;
    let entry = lookup_modify_relation(catalog, &stmt.table_name)?;
    let scope = scope_for_relation(Some(&stmt.table_name), &entry.desc);
    let predicate = stmt
        .where_clause
        .as_ref()
        .map(|expr| {
            bind_expr_with_outer_and_ctes(expr, &scope, catalog, outer_scopes, None, &local_ctes)
        })
        .transpose()?;
    let returning =
        bind_returning_targets(&stmt.returning, &scope, catalog, outer_scopes, &local_ctes)?;
    let target_rls = build_target_relation_row_security(
        &stmt.table_name,
        entry.relation_oid,
        &entry.desc,
        PolicyCommand::Update,
        // :HACK: pgrust always materializes old target rows through one path today,
        // so first-pass UPDATE RLS also requires SELECT visibility on the target.
        true,
        false,
        catalog,
    )?;
    let predicate = match (predicate, target_rls.visibility_qual) {
        (Some(predicate), Some(visibility_qual)) => Some(Expr::and(predicate, visibility_qual)),
        (Some(predicate), None) => Some(predicate),
        (None, Some(visibility_qual)) => Some(visibility_qual),
        (None, None) => None,
    };
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
                    outer_scopes,
                )?,
                expr: bind_expr_with_outer_and_ctes(
                    &assignment.expr,
                    &scope,
                    catalog,
                    outer_scopes,
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
            &target_rls.write_checks,
            &child,
            catalog,
        )
    })
    .collect::<Result<Vec<_>, ParseError>>()?;

    Ok(BoundUpdateStatement {
        targets,
        returning,
        subplans: Vec::new(),
    })
}

pub(super) fn bind_assignment_target(
    target: &crate::include::nodes::parsenodes::AssignmentTarget,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    local_ctes: &[BoundCte],
) -> Result<BoundAssignmentTarget, ParseError> {
    Ok(BoundAssignmentTarget {
        column_index: resolve_column(scope, &target.column)?,
        subscripts: bind_assignment_subscripts(
            &target.subscripts,
            scope,
            catalog,
            local_ctes,
            &[],
        )?,
    })
}

pub(super) fn bind_assignment_subscripts(
    subscripts: &[crate::include::nodes::parsenodes::ArraySubscript],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    local_ctes: &[BoundCte],
    outer_scopes: &[BoundScope],
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
                        bind_expr_with_outer_and_ctes(
                            expr,
                            scope,
                            catalog,
                            outer_scopes,
                            None,
                            local_ctes,
                        )
                    })
                    .transpose()?,
                upper: subscript
                    .upper
                    .as_deref()
                    .map(|expr| {
                        bind_expr_with_outer_and_ctes(
                            expr,
                            scope,
                            catalog,
                            outer_scopes,
                            None,
                            local_ctes,
                        )
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
    bind_delete_with_outer_scopes(stmt, catalog, &[])
}

pub(crate) fn bind_delete_with_outer_scopes(
    stmt: &DeleteStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
) -> Result<BoundDeleteStatement, ParseError> {
    let local_ctes = bind_ctes(
        stmt.with_recursive,
        &stmt.with,
        catalog,
        outer_scopes,
        None,
        &[],
        &[],
    )?;
    let entry = lookup_modify_relation(catalog, &stmt.table_name)?;
    let scope = scope_for_relation(Some(&stmt.table_name), &entry.desc);
    let predicate = stmt
        .where_clause
        .as_ref()
        .map(|expr| {
            bind_expr_with_outer_and_ctes(expr, &scope, catalog, outer_scopes, None, &local_ctes)
        })
        .transpose()?;
    let returning =
        bind_returning_targets(&stmt.returning, &scope, catalog, outer_scopes, &local_ctes)?;
    let target_rls = build_target_relation_row_security(
        &stmt.table_name,
        entry.relation_oid,
        &entry.desc,
        PolicyCommand::Delete,
        // :HACK: pgrust always materializes old target rows through one path today,
        // so first-pass DELETE RLS also requires SELECT visibility on the target.
        true,
        false,
        catalog,
    )?;
    let predicate = match (predicate, target_rls.visibility_qual) {
        (Some(predicate), Some(visibility_qual)) => Some(Expr::and(predicate, visibility_qual)),
        (Some(predicate), None) => Some(predicate),
        (None, Some(visibility_qual)) => Some(visibility_qual),
        (None, None) => None,
    };

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
        build_delete_target(
            &stmt.table_name,
            &entry.desc,
            predicate.as_ref(),
            &child,
            catalog,
        )
    })
    .collect::<Result<Vec<_>, ParseError>>()?;

    Ok(BoundDeleteStatement {
        targets,
        returning,
        subplans: Vec::new(),
    })
}
