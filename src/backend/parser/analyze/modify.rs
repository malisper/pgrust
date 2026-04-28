use super::paths::choose_modify_row_source;
use super::query::rewrite_local_vars_for_output_exprs;
use super::*;
use crate::backend::rewrite::{
    RlsWriteCheck, ViewDmlEvent, ViewDmlRewriteError, build_target_relation_row_security,
    relation_has_row_security, relation_has_security_invoker, resolve_auto_updatable_view_target,
};
use crate::backend::utils::record::lookup_anonymous_record_descriptor;
use crate::include::catalog::PolicyCommand;
use crate::include::executor::execdesc::CommandType;
use crate::include::nodes::plannodes::PlannedStmt;
use crate::include::nodes::primnodes::{
    INNER_VAR, OUTER_VAR, SELF_ITEM_POINTER_ATTR_NO, TABLE_OID_ATTR_NO, TargetEntry, Var,
    expr_contains_set_returning,
};
use crate::include::nodes::primnodes::{
    JoinType, RelationPrivilegeMask, RelationPrivilegeRequirement,
};

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
    pub overriding: Option<OverridingKind>,
    pub source: BoundInsertSource,
    pub on_conflict: Option<BoundOnConflictClause>,
    pub raw_on_conflict: Option<crate::include::nodes::parsenodes::OnConflictClause>,
    pub returning: Vec<TargetEntry>,
    pub(crate) rls_write_checks: Vec<RlsWriteCheck>,
    pub required_privileges: Vec<RelationPrivilegeRequirement>,
    pub subplans: Vec<Plan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundInsertSource {
    Values(Vec<Vec<Expr>>),
    ProjectSetValues(Vec<Vec<Expr>>),
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
    pub partition_update_root_oid: Option<u32>,
    pub toast: Option<ToastRelationRef>,
    pub toast_index: Option<BoundIndexRelation>,
    pub desc: RelationDesc,
    pub relation_constraints: BoundRelationConstraints,
    pub referenced_by_foreign_keys: Vec<BoundReferencedByForeignKey>,
    pub row_source: BoundModifyRowSource,
    pub indexes: Vec<BoundIndexRelation>,
    pub assignments: Vec<BoundAssignment>,
    pub parent_visible_exprs: Vec<Expr>,
    pub predicate: Option<Expr>,
    pub(crate) rls_write_checks: Vec<RlsWriteCheck>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundUpdateStatement {
    pub target_relation_name: String,
    pub explain_target_name: String,
    pub targets: Vec<BoundUpdateTarget>,
    pub returning: Vec<TargetEntry>,
    pub input_plan: Option<PlannedStmt>,
    pub target_visible_count: usize,
    pub visible_column_count: usize,
    pub target_ctid_index: usize,
    pub target_tableoid_index: usize,
    pub required_privileges: Vec<RelationPrivilegeRequirement>,
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
    pub required_privileges: Vec<RelationPrivilegeRequirement>,
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
    pub returning: Vec<TargetEntry>,
    pub required_privileges: Vec<RelationPrivilegeRequirement>,
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

fn relation_privilege_requirement(
    relation: &BoundRelation,
    relation_name: impl Into<String>,
    required: RelationPrivilegeMask,
) -> RelationPrivilegeRequirement {
    RelationPrivilegeRequirement::new(
        relation.relation_oid,
        relation_name,
        relation.relkind,
        required,
    )
}

fn view_base_privilege_requirement(
    view_oid: u32,
    relation_name: impl Into<String>,
    base_relation: &BoundRelation,
    required: RelationPrivilegeMask,
    catalog: &dyn CatalogLookup,
) -> RelationPrivilegeRequirement {
    let check_as_user_oid = view_check_as_user_oid(view_oid, catalog);
    let mut requirement = relation_privilege_requirement(base_relation, relation_name, required)
        .checked_as(check_as_user_oid);
    requirement.selected_columns = base_relation.desc.visible_column_indexes();
    requirement
}

fn view_check_as_user_oid(view_oid: u32, catalog: &dyn CatalogLookup) -> Option<u32> {
    if relation_has_security_invoker(catalog, view_oid) {
        None
    } else {
        catalog.class_row_by_oid(view_oid).map(|row| row.relowner)
    }
}

fn map_view_privilege_columns(
    columns: &[usize],
    updatable_column_map: &[Option<usize>],
) -> Vec<usize> {
    let mut mapped = columns
        .iter()
        .filter_map(|column| updatable_column_map.get(*column).copied().flatten())
        .collect::<Vec<_>>();
    mapped.sort_unstable();
    mapped.dedup();
    mapped
}

fn view_context_privilege_requirement(
    context: &crate::backend::rewrite::ViewPrivilegeContext,
    relation_name: impl Into<String>,
    required: RelationPrivilegeMask,
) -> RelationPrivilegeRequirement {
    let mut requirement =
        relation_privilege_requirement(&context.relation, relation_name, required)
            .checked_as(context.check_as_user_oid);
    requirement.selected_columns = context.relation.desc.visible_column_indexes();
    requirement
}

fn view_context_insert_privilege_requirement(
    context: &crate::backend::rewrite::ViewPrivilegeContext,
    relation_name: impl Into<String>,
    target_columns: &[BoundAssignmentTarget],
) -> RelationPrivilegeRequirement {
    let mut requirement = view_context_privilege_requirement(
        context,
        relation_name,
        RelationPrivilegeMask {
            select: true,
            insert: true,
            ..RelationPrivilegeMask::default()
        },
    );
    requirement.inserted_columns = map_view_privilege_columns(
        &target_columns
            .iter()
            .map(|target| target.column_index)
            .collect::<Vec<_>>(),
        &context.column_map,
    );
    requirement
}

fn view_context_update_privilege_requirement(
    context: &crate::backend::rewrite::ViewPrivilegeContext,
    relation_name: impl Into<String>,
    assignments: &[BoundAssignment],
) -> RelationPrivilegeRequirement {
    let mut requirement = view_context_privilege_requirement(
        context,
        relation_name,
        RelationPrivilegeMask {
            select: true,
            update: true,
            ..RelationPrivilegeMask::default()
        },
    );
    requirement.updated_columns = map_view_privilege_columns(
        &assignments
            .iter()
            .map(|assignment| assignment.column_index)
            .collect::<Vec<_>>(),
        &context.column_map,
    );
    requirement
}

fn view_context_delete_privilege_requirement(
    context: &crate::backend::rewrite::ViewPrivilegeContext,
    relation_name: impl Into<String>,
) -> RelationPrivilegeRequirement {
    view_context_privilege_requirement(
        context,
        relation_name,
        RelationPrivilegeMask {
            select: true,
            delete: true,
            ..RelationPrivilegeMask::default()
        },
    )
}

fn view_context_merge_privilege_requirement(
    context: &crate::backend::rewrite::ViewPrivilegeContext,
    relation_name: impl Into<String>,
    clauses: &[BoundMergeWhenClause],
) -> RelationPrivilegeRequirement {
    let mut requirement = merge_privilege_requirement(&context.relation, relation_name, clauses)
        .checked_as(context.check_as_user_oid);
    let inserted_columns = requirement.inserted_columns.clone();
    let updated_columns = requirement.updated_columns.clone();
    requirement.inserted_columns =
        map_view_privilege_columns(&inserted_columns, &context.column_map);
    requirement.updated_columns = map_view_privilege_columns(&updated_columns, &context.column_map);
    requirement
}

fn insert_privilege_requirement(
    relation: &BoundRelation,
    relation_name: impl Into<String>,
    target_columns: &[BoundAssignmentTarget],
) -> RelationPrivilegeRequirement {
    let mut requirement =
        relation_privilege_requirement(relation, relation_name, RelationPrivilegeMask::insert());
    requirement.inserted_columns = target_columns
        .iter()
        .map(|target| target.column_index)
        .collect();
    requirement
}

fn update_privilege_requirement(
    relation: &BoundRelation,
    relation_name: impl Into<String>,
    assignments: &[BoundAssignment],
) -> RelationPrivilegeRequirement {
    let mut requirement =
        relation_privilege_requirement(relation, relation_name, RelationPrivilegeMask::update());
    requirement.updated_columns = assignments
        .iter()
        .map(|assignment| assignment.column_index)
        .collect();
    requirement
}

fn delete_privilege_requirement(
    relation: &BoundRelation,
    relation_name: impl Into<String>,
) -> RelationPrivilegeRequirement {
    relation_privilege_requirement(relation, relation_name, RelationPrivilegeMask::delete())
}

fn merge_privilege_requirement(
    relation: &BoundRelation,
    relation_name: impl Into<String>,
    clauses: &[BoundMergeWhenClause],
) -> RelationPrivilegeRequirement {
    let mut needs_insert = false;
    let mut needs_update = false;
    let mut needs_delete = false;
    let mut inserted_columns = Vec::new();
    let mut updated_columns = Vec::new();
    for clause in clauses {
        match &clause.action {
            BoundMergeAction::DoNothing => {}
            BoundMergeAction::Delete => needs_delete = true,
            BoundMergeAction::Update { assignments } => {
                needs_update = true;
                updated_columns
                    .extend(assignments.iter().map(|assignment| assignment.column_index));
            }
            BoundMergeAction::Insert { target_columns, .. } => {
                needs_insert = true;
                inserted_columns.extend(target_columns.iter().map(|target| target.column_index));
            }
        }
    }
    inserted_columns.sort_unstable();
    inserted_columns.dedup();
    updated_columns.sort_unstable();
    updated_columns.dedup();

    let mut requirement = relation_privilege_requirement(
        relation,
        relation_name,
        RelationPrivilegeMask::merge_actions(needs_insert, needs_update, needs_delete),
    );
    requirement.selected_columns = relation.desc.visible_column_indexes();
    requirement.inserted_columns = inserted_columns;
    requirement.updated_columns = updated_columns;
    requirement
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundAssignment {
    pub column_index: usize,
    pub subscripts: Vec<BoundArraySubscript>,
    pub field_path: Vec<String>,
    pub indirection: Vec<BoundAssignmentTargetIndirection>,
    pub target_sql_type: SqlType,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundAssignmentTarget {
    pub column_index: usize,
    pub subscripts: Vec<BoundArraySubscript>,
    pub field_path: Vec<String>,
    pub indirection: Vec<BoundAssignmentTargetIndirection>,
    pub target_sql_type: SqlType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundArraySubscript {
    pub is_slice: bool,
    pub lower: Option<Expr>,
    pub upper: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundAssignmentTargetIndirection {
    Subscript(BoundArraySubscript),
    Field(String),
}

fn merge_target_relation_name(stmt: &MergeStatement) -> String {
    stmt.target_alias
        .clone()
        .unwrap_or_else(|| stmt.target_table.clone())
}

fn update_target_relation_name(stmt: &UpdateStatement) -> String {
    stmt.target_alias
        .clone()
        .unwrap_or_else(|| stmt.table_name.clone())
}

fn update_explain_target_name(stmt: &UpdateStatement) -> String {
    stmt.target_alias
        .as_ref()
        .map(|alias| format!("{} {}", stmt.table_name, alias))
        .unwrap_or_else(|| stmt.table_name.clone())
}

fn assignment_navigation_sql_type(sql_type: SqlType, catalog: &dyn CatalogLookup) -> SqlType {
    let Some(domain) = catalog.domain_by_type_oid(sql_type.type_oid) else {
        return sql_type;
    };
    if sql_type.is_array && !domain.sql_type.is_array {
        SqlType::array_of(domain.sql_type)
    } else {
        domain.sql_type
    }
}

fn resolve_assignment_field_type(
    row_type: SqlType,
    field: &str,
    catalog: &dyn CatalogLookup,
) -> Result<SqlType, ParseError> {
    let row_type = assignment_navigation_sql_type(row_type, catalog);
    if matches!(row_type.kind, SqlTypeKind::Composite) && row_type.typrelid != 0 {
        let relation = catalog
            .lookup_relation_by_oid(row_type.typrelid)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "named composite type",
                actual: format!("type relation {} not found", row_type.typrelid),
            })?;
        if let Some(found) = relation
            .desc
            .columns
            .iter()
            .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(field))
        {
            return Ok(found.sql_type);
        }
    }

    if matches!(row_type.kind, SqlTypeKind::Record)
        && row_type.typmod > 0
        && let Some(descriptor) = lookup_anonymous_record_descriptor(row_type.typmod)
        && let Some(found) = descriptor
            .fields
            .iter()
            .find(|candidate| candidate.name.eq_ignore_ascii_case(field))
    {
        return Ok(found.sql_type);
    }

    Err(ParseError::UnexpectedToken {
        expected: "record field",
        actual: format!("field selection .{field}"),
    })
}

fn resolve_assignment_target_sql_type(
    column_type: SqlType,
    subscripts: &[crate::include::nodes::parsenodes::ArraySubscript],
    field_path: &[String],
    catalog: &dyn CatalogLookup,
) -> Result<SqlType, ParseError> {
    let mut current = column_type;
    for subscript in subscripts {
        current = assignment_navigation_sql_type(current, catalog);
        if current.kind == SqlTypeKind::Jsonb && !current.is_array {
            if subscript.is_slice {
                return Err(ParseError::DetailedError {
                    message: "jsonb subscript does not support slices".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                });
            }
            current = SqlType::new(SqlTypeKind::Jsonb);
            continue;
        }
        if current.kind == SqlTypeKind::Point && !current.is_array {
            current = SqlType::new(SqlTypeKind::Float8);
            continue;
        }
        if !current.is_array {
            return Err(ParseError::DetailedError {
                message: format!(
                    "cannot subscript type {} because it does not support subscripting",
                    sql_type_name(current)
                ),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        }
        current = if subscript.is_slice {
            SqlType::array_of(current.element_type())
        } else {
            current.element_type()
        };
    }

    if !field_path.is_empty() && subscripts.iter().any(|subscript| subscript.is_slice) {
        return Err(ParseError::UnexpectedToken {
            expected: "record field selection on a scalar composite value",
            actual: format!("assignment target {}", field_path.join(".")),
        });
    }

    for field in field_path {
        current = assignment_navigation_sql_type(current, catalog);
        current = resolve_assignment_field_type(current, field, catalog)?;
    }
    Ok(current)
}

fn resolve_assignment_indirection_sql_type(
    column_type: SqlType,
    indirection: &[crate::include::nodes::parsenodes::AssignmentTargetIndirection],
    catalog: &dyn CatalogLookup,
) -> Result<SqlType, ParseError> {
    let mut current = column_type;
    for step in indirection {
        current = assignment_navigation_sql_type(current, catalog);
        match step {
            crate::include::nodes::parsenodes::AssignmentTargetIndirection::Subscript(
                subscript,
            ) => {
                if current.kind == SqlTypeKind::Jsonb && !current.is_array {
                    if subscript.is_slice {
                        return Err(ParseError::DetailedError {
                            message: "jsonb subscript does not support slices".into(),
                            detail: None,
                            hint: None,
                            sqlstate: "0A000",
                        });
                    }
                    current = SqlType::new(SqlTypeKind::Jsonb);
                    continue;
                }
                if current.kind == SqlTypeKind::Point && !current.is_array {
                    current = SqlType::new(SqlTypeKind::Float8);
                    continue;
                }
                if !current.is_array {
                    return Err(ParseError::DetailedError {
                        message: format!(
                            "cannot subscript type {} because it does not support subscripting",
                            sql_type_name(current)
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42804",
                    });
                }
                current = if subscript.is_slice {
                    SqlType::array_of(current.element_type())
                } else {
                    current.element_type()
                };
            }
            crate::include::nodes::parsenodes::AssignmentTargetIndirection::Field(field) => {
                current = resolve_assignment_field_type(current, field, catalog)?;
            }
        }
    }
    Ok(current)
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

fn update_hidden_ctid_name() -> String {
    "__update_target_ctid".into()
}

fn update_hidden_tableoid_name() -> String {
    "__update_target_tableoid".into()
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

fn reject_default_indirection_assignment(target: &BoundAssignmentTarget) -> Result<(), ParseError> {
    if target.indirection.is_empty() {
        return Ok(());
    }
    let message = if target
        .indirection
        .iter()
        .any(|step| matches!(step, BoundAssignmentTargetIndirection::Field(_)))
    {
        "cannot set a subfield to DEFAULT"
    } else {
        "cannot set an array element to DEFAULT"
    };
    Err(ParseError::FeatureNotSupportedMessage(message.into()))
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

fn bound_indirection_from_parts(
    subscripts: &[BoundArraySubscript],
    field_path: &[String],
) -> Vec<BoundAssignmentTargetIndirection> {
    subscripts
        .iter()
        .cloned()
        .map(BoundAssignmentTargetIndirection::Subscript)
        .chain(
            field_path
                .iter()
                .cloned()
                .map(BoundAssignmentTargetIndirection::Field),
        )
        .collect()
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
                    let column_index = resolve_column(target_scope, &assignment.target.column)?;
                    let subscripts = bind_assignment_subscripts(
                        &assignment.target.subscripts,
                        target_scope,
                        catalog,
                        local_ctes,
                        &[],
                    )?;
                    let field_path = assignment.target.field_path.clone();
                    let indirection = bind_assignment_indirection(
                        &assignment.target.indirection,
                        target_scope,
                        catalog,
                        local_ctes,
                        &[],
                    )?;
                    let target = BoundAssignmentTarget {
                        column_index,
                        subscripts,
                        field_path,
                        indirection,
                        target_sql_type: resolve_assignment_indirection_sql_type(
                            target_desc.columns[column_index].sql_type,
                            &assignment.target.indirection,
                            catalog,
                        )?,
                    };
                    ensure_generated_assignment_allowed(
                        target_desc,
                        &target,
                        Some(&assignment.expr),
                    )?;
                    ensure_identity_update_assignment_allowed(
                        target_desc,
                        &target,
                        &assignment.expr,
                    )?;
                    Ok(BoundAssignment {
                        column_index,
                        subscripts: target.subscripts,
                        field_path: target.field_path,
                        indirection: target.indirection,
                        target_sql_type: target.target_sql_type,
                        expr: if matches!(assignment.expr, SqlExpr::Default)
                            && target_desc.columns[column_index].generated.is_some()
                        {
                            Expr::Const(Value::Null)
                        } else {
                            bind_expr_with_outer_and_ctes(
                                &assignment.expr,
                                action_scope,
                                catalog,
                                &[],
                                None,
                                local_ctes,
                            )?
                        },
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
                        let column_index = resolve_column(target_scope, column)?;
                        Ok(BoundAssignmentTarget {
                            column_index,
                            subscripts: Vec::new(),
                            field_path: Vec::new(),
                            indirection: Vec::new(),
                            target_sql_type: target_desc.columns[column_index].sql_type,
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
                            .zip(target_columns.iter())
                            .map(|(expr, target)| {
                                ensure_generated_assignment_allowed(
                                    target_desc,
                                    target,
                                    Some(expr),
                                )?;
                                let normalized = normalize_identity_insert_expr(
                                    target_desc,
                                    target,
                                    expr,
                                    None,
                                )?;
                                if matches!(normalized, NormalizedInsertExpr::Default) {
                                    return Ok(target_desc.columns[target.column_index]
                                        .default_sequence_oid
                                        .map(|sequence_oid| {
                                            let expr = Expr::builtin_func(
                                                BuiltinScalarFunction::NextVal,
                                                Some(SqlType::new(SqlTypeKind::Int8)),
                                                false,
                                                vec![Expr::Const(Value::Int64(i64::from(
                                                    sequence_oid,
                                                )))],
                                            );
                                            if target_desc.columns[target.column_index]
                                                .sql_type
                                                .kind
                                                == SqlTypeKind::Int8
                                            {
                                                expr
                                            } else {
                                                Expr::Cast(
                                                    Box::new(expr),
                                                    target_desc.columns[target.column_index]
                                                        .sql_type,
                                                )
                                            }
                                        })
                                        .unwrap_or(Expr::Const(Value::Null)));
                                }
                                if matches!(expr, SqlExpr::Default)
                                    && target_desc.columns[target.column_index].generated.is_some()
                                {
                                    Ok(Expr::Const(Value::Null))
                                } else {
                                    bind_expr_with_outer_and_ctes(
                                        expr,
                                        action_scope,
                                        catalog,
                                        &[],
                                        None,
                                        local_ctes,
                                    )
                                }
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
        BoundSelectTargets::Plain(targets)
            if targets
                .iter()
                .any(|target| expr_contains_set_returning(&target.expr)) =>
        {
            Err(ParseError::FeatureNotSupported(
                "set-returning functions are not allowed in RETURNING".into(),
            ))
        }
        BoundSelectTargets::Plain(targets) => Ok(targets),
    }
}

fn returning_pseudo_output_exprs(desc: &RelationDesc, varno: usize) -> Vec<Expr> {
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

fn scope_with_returning_pseudo_rows(scope: BoundScope, desc: &RelationDesc) -> BoundScope {
    scope_with_returning_pseudo_row_exprs(
        scope,
        desc,
        returning_pseudo_output_exprs(desc, OUTER_VAR),
        returning_pseudo_output_exprs(desc, INNER_VAR),
    )
}

fn scope_with_returning_pseudo_row_exprs(
    mut scope: BoundScope,
    desc: &RelationDesc,
    old_output_exprs: Vec<Expr>,
    new_output_exprs: Vec<Expr>,
) -> BoundScope {
    for (relation_name, varno) in [("old", OUTER_VAR), ("new", INNER_VAR)] {
        scope.desc.columns.extend(desc.columns.iter().cloned());
        scope.output_exprs.extend(if varno == OUTER_VAR {
            old_output_exprs.clone()
        } else {
            new_output_exprs.clone()
        });
        scope
            .columns
            .extend(desc.columns.iter().map(|column| ScopeColumn {
                output_name: column.name.clone(),
                hidden: true,
                qualified_only: true,
                relation_names: vec![relation_name.to_string()],
                hidden_invalid_relation_names: vec![],
                hidden_missing_relation_names: vec![],
                source_relation_oid: None,
                source_attno: None,
                source_columns: Vec::new(),
            }));
        scope.relations.push(ScopeRelation {
            relation_names: vec![relation_name.to_string()],
            hidden_invalid_relation_names: vec![],
            hidden_missing_relation_names: vec![],
            system_varno: None,
            relation_oid: None,
        });
    }
    scope
}

fn projected_output_exprs(desc: &RelationDesc, start_index: usize) -> Vec<Expr> {
    desc.columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            Expr::Var(Var {
                varno: 1,
                varattno: user_attrno(start_index + index),
                varlevelsup: 0,
                vartype: column.sql_type,
            })
        })
        .collect()
}

fn projected_output_exprs_with_width(
    desc: &RelationDesc,
    start_index: usize,
    width: usize,
) -> Vec<Expr> {
    desc.columns
        .iter()
        .take(width)
        .enumerate()
        .map(|(index, column)| {
            Expr::Var(Var {
                varno: 1,
                varattno: user_attrno(start_index + index),
                varlevelsup: 0,
                vartype: column.sql_type,
            })
        })
        .collect()
}

fn scope_with_output_exprs(mut scope: BoundScope, output_exprs: Vec<Expr>) -> BoundScope {
    scope.output_exprs = output_exprs;
    scope
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

fn with_update_target_identity(
    from: AnalyzedFrom,
    target_desc: &RelationDesc,
) -> (AnalyzedFrom, usize, usize) {
    let mut targets = merge_projection_targets(&from.output_columns, &from.output_exprs);
    let ctid_resno = targets.len() + 1;
    targets.push(
        TargetEntry::new(
            update_hidden_ctid_name(),
            Expr::Var(Var {
                varno: 1,
                varattno: SELF_ITEM_POINTER_ATTR_NO,
                varlevelsup: 0,
                vartype: SqlType::new(SqlTypeKind::Tid),
            }),
            SqlType::new(SqlTypeKind::Tid),
            ctid_resno,
        )
        .with_input_resno(ctid_resno),
    );
    let tableoid_resno = targets.len() + 1;
    targets.push(
        TargetEntry::new(
            update_hidden_tableoid_name(),
            Expr::Var(Var {
                varno: 1,
                varattno: TABLE_OID_ATTR_NO,
                varlevelsup: 0,
                vartype: SqlType::new(SqlTypeKind::Oid),
            }),
            SqlType::new(SqlTypeKind::Oid),
            tableoid_resno,
        )
        .with_input_resno(tableoid_resno),
    );
    let projected = from.with_projection(targets);
    (
        projected,
        target_desc.columns.len(),
        target_desc.columns.len() + 1,
    )
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

fn update_from_projection_targets(
    from: &AnalyzedFrom,
    target_visible_count: usize,
    source_visible_count: usize,
) -> Vec<TargetEntry> {
    let ctid_index = target_visible_count;
    let tableoid_index = target_visible_count + 1;
    let source_start = target_visible_count + 2;
    let mut targets = Vec::with_capacity(target_visible_count + source_visible_count + 2);
    for index in 0..target_visible_count {
        targets.push(
            TargetEntry::new(
                from.output_columns[index].name.clone(),
                from.output_exprs[index].clone(),
                from.output_columns[index].sql_type,
                targets.len() + 1,
            )
            .with_input_resno(index + 1),
        );
    }
    for source_index in 0..source_visible_count {
        let input_index = source_start + source_index;
        targets.push(
            TargetEntry::new(
                from.output_columns[input_index].name.clone(),
                from.output_exprs[input_index].clone(),
                from.output_columns[input_index].sql_type,
                targets.len() + 1,
            )
            .with_input_resno(input_index + 1),
        );
    }
    targets.push(
        TargetEntry::new(
            update_hidden_ctid_name(),
            from.output_exprs[ctid_index].clone(),
            SqlType::new(SqlTypeKind::Tid),
            targets.len() + 1,
        )
        .with_input_resno(ctid_index + 1),
    );
    targets.push(
        TargetEntry::new(
            update_hidden_tableoid_name(),
            from.output_exprs[tableoid_index].clone(),
            SqlType::new(SqlTypeKind::Oid),
            targets.len() + 1,
        )
        .with_input_resno(tableoid_index + 1),
    );
    targets
}

fn query_from_projection_with_qual(input: AnalyzedFrom, where_qual: Option<Expr>) -> Query {
    let AnalyzedFrom {
        rtable,
        jointree,
        output_columns,
        output_exprs,
    } = input;
    Query {
        command_type: CommandType::Select,
        depends_on_row_security: false,
        rtable,
        jointree,
        target_list: normalize_target_list(identity_target_list(&output_columns, &output_exprs)),
        distinct: false,
        distinct_on: Vec::new(),
        where_qual,
        group_by: Vec::new(),
        accumulators: Vec::new(),
        window_clauses: Vec::new(),
        having_qual: None,
        sort_clause: Vec::new(),
        constraint_deps: Vec::new(),
        limit_count: None,
        limit_offset: 0,
        locking_clause: None,
        row_marks: Vec::new(),
        has_target_srfs: false,
        recursive_union: None,
        set_operation: None,
    }
}

fn merge_mutating_event(stmt: &MergeStatement) -> Option<ViewDmlEvent> {
    stmt.when_clauses
        .iter()
        .find_map(|clause| match clause.action {
            MergeAction::Update { .. } => Some(ViewDmlEvent::Update),
            MergeAction::Delete => Some(ViewDmlEvent::Delete),
            MergeAction::Insert { .. } => Some(ViewDmlEvent::Insert),
            MergeAction::DoNothing => None,
        })
}

fn map_merge_view_rewrite_error(relation_name: &str, err: ViewDmlRewriteError) -> ParseError {
    match err {
        ViewDmlRewriteError::DeferredFeature(detail) => ParseError::FeatureNotSupported(detail),
        ViewDmlRewriteError::NonUpdatableColumn {
            column_name,
            reason,
        } => ParseError::DetailedError {
            message: format!(
                "cannot merge into column \"{}\" of view \"{}\"",
                column_name, relation_name
            ),
            detail: Some(reason.detail().into()),
            hint: None,
            sqlstate: "55000",
        },
        ViewDmlRewriteError::MultipleAssignments(column_name) => {
            ParseError::UnexpectedToken {
                expected: "single assignment per base column",
                actual: format!("multiple assignments to same column \"{}\"", column_name),
            }
        }
        other => ParseError::DetailedError {
            message: format!("cannot execute MERGE on view \"{}\"", relation_name),
            detail: Some(other.detail()),
            hint: Some(
                "To enable MERGE on the view, provide suitable INSTEAD OF triggers or unconditional DO INSTEAD rules."
                    .into(),
            ),
            sqlstate: "55000",
        },
    }
}

fn rewrite_merge_when_clause_auto_view(
    clause: BoundMergeWhenClause,
    view_desc: &RelationDesc,
    resolved: &crate::backend::rewrite::ResolvedAutoViewTarget,
) -> Result<BoundMergeWhenClause, ViewDmlRewriteError> {
    let action = match clause.action {
        BoundMergeAction::Update { assignments } => {
            let assignments = assignments
                .into_iter()
                .map(|assignment| {
                    let column_index = map_auto_view_column_index(
                        view_desc,
                        &resolved.updatable_column_map,
                        &resolved.non_updatable_column_reasons,
                        assignment.column_index,
                    )?;
                    Ok(BoundAssignment {
                        column_index,
                        target_sql_type: resolved.base_relation.desc.columns[column_index].sql_type,
                        ..assignment
                    })
                })
                .collect::<Result<Vec<_>, ViewDmlRewriteError>>()?;
            reject_duplicate_auto_view_targets(
                &resolved.base_relation.desc,
                assignments.iter().map(|assignment| assignment.column_index),
            )?;
            BoundMergeAction::Update { assignments }
        }
        BoundMergeAction::Insert {
            target_columns,
            values,
        } => {
            let target_columns = target_columns
                .into_iter()
                .map(|target| {
                    let column_index = map_auto_view_column_index(
                        view_desc,
                        &resolved.updatable_column_map,
                        &resolved.non_updatable_column_reasons,
                        target.column_index,
                    )?;
                    Ok(BoundAssignmentTarget {
                        column_index,
                        target_sql_type: resolved.base_relation.desc.columns[column_index].sql_type,
                        ..target
                    })
                })
                .collect::<Result<Vec<_>, ViewDmlRewriteError>>()?;
            reject_duplicate_auto_view_targets(
                &resolved.base_relation.desc,
                target_columns.iter().map(|target| target.column_index),
            )?;
            BoundMergeAction::Insert {
                target_columns,
                values,
            }
        }
        other => other,
    };
    Ok(BoundMergeWhenClause { action, ..clause })
}

fn is_merge_action_returning_call(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::FuncCall {
            name,
            args,
            order_by,
            within_group,
            distinct,
            func_variadic,
            filter,
            null_treatment,
            over,
        } => {
            name.eq_ignore_ascii_case("merge_action")
                && matches!(args, SqlCallArgs::Args(args) if args.is_empty())
                && order_by.is_empty()
                && within_group.is_none()
                && !distinct
                && !func_variadic
                && filter.is_none()
                && null_treatment.is_none()
                && over.is_none()
        }
        _ => false,
    }
}

fn bind_merge_returning_targets(
    targets: &[crate::include::nodes::parsenodes::SelectItem],
    scope: &BoundScope,
    merge_action_index: usize,
    catalog: &dyn CatalogLookup,
    local_ctes: &[BoundCte],
) -> Result<Vec<TargetEntry>, ParseError> {
    let mut entries = Vec::new();
    for item in targets {
        if is_merge_action_returning_call(&item.expr) {
            entries.push(
                TargetEntry::new(
                    item.output_name.clone(),
                    Expr::Var(Var {
                        varno: 1,
                        varattno: user_attrno(merge_action_index),
                        varlevelsup: 0,
                        vartype: SqlType::new(SqlTypeKind::Text),
                    }),
                    SqlType::new(SqlTypeKind::Text),
                    entries.len() + 1,
                )
                .with_input_resno(merge_action_index + 1),
            );
            continue;
        }
        let BoundSelectTargets::Plain(bound) = bind_select_targets(
            std::slice::from_ref(item),
            scope,
            catalog,
            &[],
            None,
            local_ctes,
        )?;
        for mut target in bound {
            target.resno = entries.len() + 1;
            entries.push(target);
        }
    }
    if entries
        .iter()
        .any(|target| expr_contains_set_returning(&target.expr))
    {
        return Err(ParseError::FeatureNotSupported(
            "set-returning functions are not allowed in RETURNING".into(),
        ));
    }
    Ok(entries)
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
    let entry = lookup_modify_relation(catalog, &stmt.target_table)?;
    let auto_view_target = if entry.relkind == 'v'
        && let Some(event) = merge_mutating_event(stmt)
    {
        Some(
            resolve_auto_updatable_view_target(
                entry.relation_oid,
                &entry.desc,
                event,
                catalog,
                &[],
            )
            .map_err(|err| map_merge_view_rewrite_error(&stmt.target_table, err))?,
        )
    } else {
        None
    };
    let execution_relation = auto_view_target
        .as_ref()
        .map(|target| target.base_relation.clone())
        .unwrap_or_else(|| entry.clone());
    if relation_has_row_security(execution_relation.relation_oid, catalog) {
        return Err(unsupported_with_row_security("MERGE"));
    }
    let column_defaults =
        bind_insert_column_defaults(&execution_relation.desc, catalog, &local_ctes)?;
    let target_relation_name = merge_target_relation_name(stmt);
    let explain_target_name = merge_explain_target_name(stmt);
    let mut target_base = AnalyzedFrom::relation(
        target_relation_name.clone(),
        execution_relation.rel,
        execution_relation.relation_oid,
        execution_relation.relkind,
        execution_relation.relispopulated,
        execution_relation.toast,
        !stmt.target_only && execution_relation.relkind == 'r',
        execution_relation.desc.clone(),
    );
    if auto_view_target.is_some()
        && let Some(permission) = target_base
            .rtable
            .get_mut(0)
            .and_then(|rte| rte.permission.as_mut())
    {
        permission.check_as_user_oid = view_check_as_user_oid(entry.relation_oid, catalog);
    }
    target_base.output_exprs = generated_relation_output_exprs(&execution_relation.desc, catalog)?;
    let (target_from, target_visible_count) =
        with_merge_target_ctid(target_base, &execution_relation.desc);
    let mut target_scope = scope_for_base_relation_with_generated(
        &target_relation_name,
        &entry.desc,
        Some(entry.relation_oid),
        catalog,
    )?;
    if let Some(resolved) = auto_view_target.as_ref() {
        target_scope.output_exprs = resolved.visible_output_exprs.clone();
    }
    let (source_base, source_scope_raw) =
        bind_from_item_with_ctes(&stmt.source, catalog, &[], None, &local_ctes, &[])?;
    let (source_from, source_visible_count) = with_merge_source_present(source_base);

    if source_scope_raw.relations.iter().any(|relation| {
        relation
            .relation_names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&target_relation_name))
    }) {
        return Err(ParseError::DuplicateTableName(target_relation_name.clone()));
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
    let join_condition = and_predicates(
        Some(join_condition),
        auto_view_target
            .as_ref()
            .and_then(|target| target.combined_predicate.clone()),
    )
    .unwrap_or(Expr::Const(Value::Bool(true)));

    let projected_target_output_exprs = projected_output_exprs(&execution_relation.desc, 0);
    let action_target_output_exprs = if let Some(resolved) = auto_view_target.as_ref() {
        resolved
            .visible_output_exprs
            .iter()
            .cloned()
            .map(|expr| {
                rewrite_local_vars_for_output_exprs(expr, 1, &projected_target_output_exprs)
            })
            .collect()
    } else {
        projected_target_output_exprs.clone()
    };
    let action_target_scope =
        scope_with_output_exprs(target_scope.clone(), action_target_output_exprs);
    let action_source_scope = scope_with_output_exprs(
        source_scope.clone(),
        projected_output_exprs_with_width(
            &source_scope.desc,
            execution_relation.desc.columns.len(),
            source_visible_count,
        ),
    );
    let action_merged_scope = combine_scopes(&action_target_scope, &action_source_scope);

    let returning_visible_column_count =
        execution_relation.desc.columns.len() + source_visible_count;
    let returning_scope = if let Some(resolved) = auto_view_target.as_ref() {
        scope_with_returning_pseudo_row_exprs(
            action_merged_scope.clone(),
            &entry.desc,
            view_returning_pseudo_output_exprs(
                &resolved.visible_output_exprs,
                &resolved.base_relation.desc,
                OUTER_VAR,
            ),
            view_returning_pseudo_output_exprs(
                &resolved.visible_output_exprs,
                &resolved.base_relation.desc,
                INNER_VAR,
            ),
        )
    } else {
        scope_with_returning_pseudo_rows(action_merged_scope.clone(), &execution_relation.desc)
    };
    let returning = bind_merge_returning_targets(
        &stmt.returning,
        &returning_scope,
        returning_visible_column_count,
        catalog,
        &local_ctes,
    )?;

    let mut when_clauses = stmt
        .when_clauses
        .iter()
        .map(|clause| {
            bind_merge_when_clause(
                clause,
                &action_target_scope,
                &action_source_scope,
                &action_merged_scope,
                catalog,
                &local_ctes,
                &entry.desc,
            )
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    let mut required_privileges = vec![merge_privilege_requirement(
        &entry,
        &stmt.target_table,
        &when_clauses,
    )];
    if let Some(resolved) = auto_view_target.as_ref() {
        for context in &resolved.privilege_contexts {
            let context_name =
                relation_display_name(catalog, context.relation.relation_oid, &stmt.target_table);
            required_privileges.push(view_context_merge_privilege_requirement(
                context,
                context_name,
                &when_clauses,
            ));
        }
        when_clauses = when_clauses
            .into_iter()
            .map(|clause| rewrite_merge_when_clause_auto_view(clause, &entry.desc, resolved))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| map_merge_view_rewrite_error(&stmt.target_table, err))?;
    }

    let joined = AnalyzedFrom::join(
        target_from,
        source_from,
        merge_join_type(&stmt.when_clauses),
        join_condition,
        None,
    );
    let visible_column_count = returning_visible_column_count;
    let target_ctid_index = visible_column_count;
    let source_present_index = visible_column_count + 1;
    let joined_target_columns = joined.output_columns.clone();
    let joined_output_exprs = joined.output_exprs.clone();
    let mut projection_targets = Vec::with_capacity(visible_column_count + 2);
    for index in 0..execution_relation.desc.columns.len() {
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
    let source_start = target_visible_count + 2;
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
    let source_marker_input = target_visible_count + 2 + source_visible_count;
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
        rel: execution_relation.rel,
        relation_oid: execution_relation.relation_oid,
        toast: execution_relation.toast,
        toast_index: first_toast_index(catalog, execution_relation.toast),
        desc: execution_relation.desc.clone(),
        relation_constraints: bind_relation_constraints(
            Some(&stmt.target_table),
            execution_relation.relation_oid,
            &execution_relation.desc,
            catalog,
        )?,
        referenced_by_foreign_keys: bind_referenced_by_foreign_keys(
            execution_relation.relation_oid,
            &execution_relation.desc,
            catalog,
        )?,
        indexes: catalog.index_relations_for_heap(execution_relation.relation_oid),
        column_defaults,
        target_relation_name,
        explain_target_name,
        visible_column_count,
        target_ctid_index,
        source_present_index,
        when_clauses,
        returning,
        required_privileges,
        input_plan: crate::backend::optimizer::fold_query_constants(query)
            .map(|query| crate::backend::optimizer::planner(query, catalog))??,
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
        Some(entry) if matches!(entry.relkind, 'r' | 'p' | 'v' | 't') => Ok(entry),
        Some(entry) if entry.relkind == 'm' => Err(ParseError::FeatureNotSupportedMessage(
            format!("cannot change materialized view \"{name}\""),
        )),
        Some(_) => Err(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table or view",
        }),
        None => Err(ParseError::UnknownTable(name.to_string())),
    }
}

fn partitioned_update_target_oids(
    catalog: &dyn CatalogLookup,
    entry: &BoundRelation,
    only: bool,
) -> Vec<u32> {
    if entry.relkind == 'p' {
        if only {
            return Vec::new();
        }
        return catalog
            .find_all_inheritors(entry.relation_oid)
            .into_iter()
            .filter(|oid| {
                catalog
                    .relation_by_oid(*oid)
                    .is_some_and(|child| child.relkind == 'r')
            })
            .collect();
    }
    if only {
        vec![entry.relation_oid]
    } else {
        catalog.find_all_inheritors(entry.relation_oid)
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
    catalog: &dyn CatalogLookup,
) -> Result<Vec<Expr>, ParseError> {
    let child_output_exprs = generated_relation_output_exprs(child_desc, catalog)?;
    indexes
        .iter()
        .map(|index| match index {
            Some(index) => Ok(child_output_exprs.get(*index).cloned().unwrap_or_else(|| {
                panic!(
                    "missing inherited child output expr for column {}",
                    index + 1
                )
            })),
            None => Ok(Expr::Const(Value::Null)),
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
    partition_update_root_oid: Option<u32>,
    child: &BoundRelation,
    catalog: &dyn CatalogLookup,
) -> Result<BoundUpdateTarget, ParseError> {
    let relation_name = relation_display_name(catalog, child.relation_oid, base_relation_name);
    let translation_indexes = inheritance_translation_indexes(parent_desc, &child.desc);
    let translation_exprs =
        inheritance_translation_exprs(&child.desc, &translation_indexes, catalog)?;
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
                field_path: assignment.field_path.clone(),
                indirection: rewrite_assignment_indirection(
                    &assignment.indirection,
                    &translation_exprs,
                ),
                target_sql_type: assignment.target_sql_type,
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
        partition_update_root_oid,
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
        parent_visible_exprs: translation_exprs,
        predicate,
        rls_write_checks,
    })
}

fn build_update_target_from_joined_input(
    base_relation_name: &str,
    parent_desc: &RelationDesc,
    parent_assignments: &[BoundAssignment],
    parent_predicate: Option<&Expr>,
    parent_rls_write_checks: &[RlsWriteCheck],
    partition_update_root_oid: Option<u32>,
    child: &BoundRelation,
    catalog: &dyn CatalogLookup,
) -> Result<BoundUpdateTarget, ParseError> {
    let relation_name = relation_display_name(catalog, child.relation_oid, base_relation_name);
    let translation_indexes = inheritance_translation_indexes(parent_desc, &child.desc);
    let parent_visible_exprs =
        inheritance_translation_exprs(&child.desc, &translation_indexes, catalog)?;
    let indexes = catalog.index_relations_for_heap(child.relation_oid);
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
                field_path: assignment.field_path.clone(),
                indirection: assignment.indirection.clone(),
                target_sql_type: assignment.target_sql_type,
                expr: assignment.expr.clone(),
            })
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    let rls_write_checks = parent_rls_write_checks
        .iter()
        .map(|check| RlsWriteCheck {
            expr: rewrite_local_vars_for_output_exprs(check.expr.clone(), 1, &parent_visible_exprs),
            policy_name: check.policy_name.clone(),
            source: check.source.clone(),
        })
        .collect();

    Ok(BoundUpdateTarget {
        relation_name: relation_name.clone(),
        rel: child.rel,
        relation_oid: child.relation_oid,
        relkind: child.relkind,
        partition_update_root_oid,
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
        row_source: BoundModifyRowSource::Heap,
        indexes,
        assignments,
        parent_visible_exprs,
        predicate: parent_predicate.cloned(),
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

fn rewrite_assignment_indirection(
    indirection: &[BoundAssignmentTargetIndirection],
    output_exprs: &[Expr],
) -> Vec<BoundAssignmentTargetIndirection> {
    indirection
        .iter()
        .map(|step| match step {
            BoundAssignmentTargetIndirection::Field(field) => {
                BoundAssignmentTargetIndirection::Field(field.clone())
            }
            BoundAssignmentTargetIndirection::Subscript(subscript) => {
                BoundAssignmentTargetIndirection::Subscript(BoundArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript.lower.as_ref().map(|expr| {
                        rewrite_local_vars_for_output_exprs(expr.clone(), 1, output_exprs)
                    }),
                    upper: subscript.upper.as_ref().map(|expr| {
                        rewrite_local_vars_for_output_exprs(expr.clone(), 1, output_exprs)
                    }),
                })
            }
        })
        .collect()
}

fn map_auto_view_column_index(
    view_desc: &RelationDesc,
    updatable_column_map: &[Option<usize>],
    non_updatable_column_reasons: &[Option<
        crate::backend::rewrite::NonUpdatableViewColumnReason,
    >],
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
            let reason = non_updatable_column_reasons
                .get(column_index)
                .copied()
                .flatten()
                .unwrap_or(
                    crate::backend::rewrite::NonUpdatableViewColumnReason::NotBaseRelationColumn,
                );
            ViewDmlRewriteError::NonUpdatableColumn {
                column_name: column_name.to_string(),
                reason,
            }
        })
}

fn reject_duplicate_auto_view_targets(
    desc: &RelationDesc,
    column_indexes: impl IntoIterator<Item = usize>,
) -> Result<(), ViewDmlRewriteError> {
    let mut seen = std::collections::BTreeSet::new();
    for column_index in column_indexes {
        if !seen.insert(column_index) {
            let column_name = desc
                .columns
                .get(column_index)
                .map(|column| column.name.clone())
                .unwrap_or_else(|| "<unknown>".into());
            return Err(ViewDmlRewriteError::MultipleAssignments(column_name));
        }
    }
    Ok(())
}

fn rewrite_auto_view_returning_targets(
    targets: Vec<TargetEntry>,
    output_exprs: &[Expr],
    base_desc: &RelationDesc,
) -> Vec<TargetEntry> {
    let old_view_output_exprs =
        view_returning_pseudo_output_exprs(output_exprs, base_desc, OUTER_VAR);
    let new_view_output_exprs =
        view_returning_pseudo_output_exprs(output_exprs, base_desc, INNER_VAR);
    targets
        .into_iter()
        .map(|target| TargetEntry {
            expr: rewrite_local_vars_for_output_exprs(
                rewrite_local_vars_for_output_exprs(
                    rewrite_local_vars_for_output_exprs(target.expr, 1, output_exprs),
                    OUTER_VAR,
                    &old_view_output_exprs,
                ),
                INNER_VAR,
                &new_view_output_exprs,
            ),
            ..target
        })
        .collect()
}

fn view_returning_pseudo_output_exprs(
    output_exprs: &[Expr],
    base_desc: &RelationDesc,
    varno: usize,
) -> Vec<Expr> {
    let base_output_exprs = returning_pseudo_output_exprs(base_desc, varno);
    output_exprs
        .iter()
        .cloned()
        .map(|expr| rewrite_local_vars_for_output_exprs(expr, 1, &base_output_exprs))
        .collect()
}

fn scope_for_pseudo_relation(relation_name: &str, desc: &RelationDesc, varno: usize) -> BoundScope {
    BoundScope {
        desc: desc.clone(),
        output_exprs: returning_pseudo_output_exprs(desc, varno),
        columns: desc
            .columns
            .iter()
            .map(|column| ScopeColumn {
                output_name: column.name.clone(),
                hidden: column.dropped,
                qualified_only: false,
                relation_names: vec![relation_name.to_string()],
                hidden_invalid_relation_names: vec![],
                hidden_missing_relation_names: vec![],
                source_relation_oid: None,
                source_attno: None,
                source_columns: Vec::new(),
            })
            .collect(),
        relations: vec![ScopeRelation {
            relation_names: vec![relation_name.to_string()],
            hidden_invalid_relation_names: vec![],
            hidden_missing_relation_names: vec![],
            system_varno: None,
            relation_oid: None,
        }],
    }
}

fn bind_auto_view_on_conflict_clause(
    clause: &crate::include::nodes::parsenodes::OnConflictClause,
    view_relation_name: &str,
    base_relation_name: &str,
    view_desc: &RelationDesc,
    resolved: &crate::backend::rewrite::ResolvedAutoViewTarget,
    catalog: &dyn CatalogLookup,
) -> Result<BoundOnConflictClause, ViewDmlRewriteError> {
    let arbiters = super::on_conflict::resolve_arbiters(
        clause,
        base_relation_name,
        resolved.base_relation.relation_oid,
        &resolved.base_relation.desc,
        catalog,
    )
    .map_err(|err| ViewDmlRewriteError::UnsupportedViewShape(err.to_string()))?;
    let action = match clause.action {
        crate::include::nodes::parsenodes::OnConflictAction::Nothing => {
            BoundOnConflictAction::Nothing
        }
        crate::include::nodes::parsenodes::OnConflictAction::Update => {
            if !arbiters.temporal_constraints.is_empty() {
                return Err(ViewDmlRewriteError::UnsupportedViewShape(
                    ParseError::DetailedError {
                        message: "ON CONFLICT DO UPDATE not supported with exclusion constraints"
                            .into(),
                        detail: None,
                        hint: None,
                        sqlstate: "0A000",
                    }
                    .to_string(),
                ));
            }
            if clause.target.is_none() {
                return Err(ViewDmlRewriteError::UnsupportedViewShape(
                    ParseError::UnexpectedToken {
                        expected: "ON CONFLICT inference specification or constraint name",
                        actual:
                            "ON CONFLICT DO UPDATE requires inference specification or constraint name"
                                .into(),
                    }
                    .to_string(),
                ));
            }
            let target_scope = scope_for_relation(Some(view_relation_name), view_desc);
            let excluded_scope = scope_for_pseudo_relation("excluded", view_desc, 2);
            let raw_scope = combine_scopes(&target_scope, &excluded_scope);
            let base_old_output_exprs =
                returning_pseudo_output_exprs(&resolved.base_relation.desc, OUTER_VAR);
            let base_new_output_exprs =
                returning_pseudo_output_exprs(&resolved.base_relation.desc, INNER_VAR);
            let old_view_output_exprs = resolved
                .visible_output_exprs
                .iter()
                .cloned()
                .map(|expr| rewrite_local_vars_for_output_exprs(expr, 1, &base_old_output_exprs))
                .collect::<Vec<_>>();
            let new_view_output_exprs = resolved
                .visible_output_exprs
                .iter()
                .cloned()
                .map(|expr| rewrite_local_vars_for_output_exprs(expr, 1, &base_new_output_exprs))
                .collect::<Vec<_>>();
            let assignments = clause
                .assignments
                .iter()
                .map(|assignment| {
                    let target =
                        bind_assignment_target(&assignment.target, &target_scope, catalog, &[])
                            .map_err(|err| {
                                ViewDmlRewriteError::UnsupportedViewShape(err.to_string())
                            })?;
                    ensure_generated_assignment_allowed(view_desc, &target, Some(&assignment.expr))
                        .map_err(|err| {
                            ViewDmlRewriteError::UnsupportedViewShape(err.to_string())
                        })?;
                    let column_index = map_auto_view_column_index(
                        view_desc,
                        &resolved.updatable_column_map,
                        &resolved.non_updatable_column_reasons,
                        target.column_index,
                    )?;
                    let target = BoundAssignmentTarget {
                        column_index,
                        subscripts: rewrite_assignment_subscripts(
                            &target.subscripts,
                            &resolved.visible_output_exprs,
                        ),
                        field_path: target.field_path,
                        indirection: rewrite_assignment_indirection(
                            &target.indirection,
                            &resolved.visible_output_exprs,
                        ),
                        target_sql_type: resolved.base_relation.desc.columns[column_index].sql_type,
                    };
                    ensure_generated_assignment_allowed(
                        &resolved.base_relation.desc,
                        &target,
                        Some(&assignment.expr),
                    )
                    .map_err(|err| ViewDmlRewriteError::UnsupportedViewShape(err.to_string()))?;
                    let expr = if matches!(assignment.expr, SqlExpr::Default)
                        && resolved.base_relation.desc.columns[column_index]
                            .generated
                            .is_some()
                    {
                        Expr::Const(Value::Null)
                    } else {
                        bind_expr_with_outer_and_ctes(
                            &assignment.expr,
                            &raw_scope,
                            catalog,
                            &[],
                            None,
                            &[],
                        )
                        .map_err(|err| ViewDmlRewriteError::UnsupportedViewShape(err.to_string()))?
                    };
                    let expr = rewrite_local_vars_for_output_exprs(expr, 1, &old_view_output_exprs);
                    let expr = rewrite_local_vars_for_output_exprs(expr, 2, &new_view_output_exprs);
                    Ok(BoundAssignment {
                        column_index: target.column_index,
                        subscripts: target.subscripts,
                        field_path: target.field_path,
                        indirection: target.indirection,
                        target_sql_type: target.target_sql_type,
                        expr,
                    })
                })
                .collect::<Result<Vec<_>, ViewDmlRewriteError>>()?;
            reject_duplicate_auto_view_targets(
                &resolved.base_relation.desc,
                assignments.iter().map(|assignment| assignment.column_index),
            )?;
            let predicate = clause
                .where_clause
                .as_ref()
                .map(|expr| {
                    let expr =
                        bind_expr_with_outer_and_ctes(expr, &raw_scope, catalog, &[], None, &[])
                            .map_err(|err| {
                                ViewDmlRewriteError::UnsupportedViewShape(err.to_string())
                            })?;
                    let expr = rewrite_local_vars_for_output_exprs(expr, 1, &old_view_output_exprs);
                    Ok(rewrite_local_vars_for_output_exprs(
                        expr,
                        2,
                        &new_view_output_exprs,
                    ))
                })
                .transpose()?;
            BoundOnConflictAction::Update {
                assignments,
                predicate,
            }
        }
    };
    Ok(BoundOnConflictClause {
        arbiter_indexes: arbiters.indexes,
        arbiter_temporal_constraints: arbiters.temporal_constraints,
        action,
    })
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
                    &resolved.non_updatable_column_reasons,
                    target.column_index,
                )?,
                subscripts: rewrite_assignment_subscripts(
                    &target.subscripts,
                    &resolved.visible_output_exprs,
                ),
                field_path: target.field_path.clone(),
                indirection: rewrite_assignment_indirection(
                    &target.indirection,
                    &resolved.visible_output_exprs,
                ),
                target_sql_type: target.target_sql_type,
            })
        })
        .collect::<Result<Vec<_>, ViewDmlRewriteError>>()?;
    reject_duplicate_auto_view_targets(
        &resolved.base_relation.desc,
        target_columns.iter().map(|target| target.column_index),
    )?;
    let mut required_privileges = stmt.required_privileges.clone();
    for context in &resolved.privilege_contexts {
        let context_name =
            relation_display_name(catalog, context.relation.relation_oid, &stmt.relation_name);
        required_privileges.push(view_context_insert_privilege_requirement(
            context,
            context_name,
            &stmt.target_columns,
        ));
    }
    let on_conflict = match stmt.raw_on_conflict.as_ref() {
        Some(clause) => Some(bind_auto_view_on_conflict_clause(
            clause,
            &stmt.relation_name,
            &relation_name,
            &stmt.desc,
            &resolved,
            catalog,
        )?),
        None => stmt.on_conflict,
    };

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
        overriding: stmt.overriding,
        source: stmt.source,
        on_conflict,
        raw_on_conflict: None,
        returning: rewrite_auto_view_returning_targets(
            stmt.returning,
            &resolved.visible_output_exprs,
            &resolved.base_relation.desc,
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
        required_privileges,
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
                    &resolved.non_updatable_column_reasons,
                    assignment.column_index,
                )?,
                subscripts: rewrite_assignment_subscripts(
                    &assignment.subscripts,
                    &resolved.visible_output_exprs,
                ),
                field_path: assignment.field_path.clone(),
                indirection: rewrite_assignment_indirection(
                    &assignment.indirection,
                    &resolved.visible_output_exprs,
                ),
                target_sql_type: assignment.target_sql_type,
                expr: rewrite_local_vars_for_output_exprs(
                    assignment.expr.clone(),
                    1,
                    &resolved.visible_output_exprs,
                ),
            })
        })
        .collect::<Result<Vec<_>, ViewDmlRewriteError>>()?;
    reject_duplicate_auto_view_targets(
        &resolved.base_relation.desc,
        assignments.iter().map(|assignment| assignment.column_index),
    )?;
    let mut required_privileges = stmt.required_privileges.clone();
    for context in &resolved.privilege_contexts {
        let context_name = relation_display_name(
            catalog,
            context.relation.relation_oid,
            &target.relation_name,
        );
        required_privileges.push(view_context_update_privilege_requirement(
            context,
            context_name,
            &target.assignments,
        ));
    }
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
                None,
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
            &resolved.base_relation.desc,
        ),
        required_privileges,
        ..stmt
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
    let mut required_privileges = stmt.required_privileges.clone();
    for context in &resolved.privilege_contexts {
        let context_name = relation_display_name(
            catalog,
            context.relation.relation_oid,
            &target.relation_name,
        );
        required_privileges.push(view_context_delete_privilege_requirement(
            context,
            context_name,
        ));
    }
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
            &resolved.base_relation.desc,
        ),
        required_privileges,
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
        catalog,
    )?;
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
            if column.generated.is_some() {
                return Ok(Expr::Const(Value::Null));
            }
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
            field_path: Vec::new(),
            indirection: Vec::new(),
            target_sql_type: desc.columns[column_index].sql_type,
        })
        .collect()
}

fn bind_insert_assignment_expr(
    expr: &SqlExpr,
    target: &BoundAssignmentTarget,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    local_ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    if let SqlExpr::ArrayLiteral(elements) = expr {
        let target_type = assignment_navigation_sql_type(target.target_sql_type, catalog);
        if target_type.is_array {
            return Ok(Expr::ArrayLiteral {
                elements: elements
                    .iter()
                    .map(|element| {
                        bind_expr_with_outer_and_ctes(
                            element,
                            scope,
                            catalog,
                            outer_scopes,
                            None,
                            local_ctes,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                array_type: target_type,
            });
        }
    }

    bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, None, local_ctes)
}

pub(super) fn ensure_generated_assignment_allowed(
    desc: &RelationDesc,
    target: &BoundAssignmentTarget,
    expr: Option<&SqlExpr>,
) -> Result<(), ParseError> {
    let Some(column) = desc.columns.get(target.column_index) else {
        return Ok(());
    };
    if column.generated.is_none() {
        return Ok(());
    }
    if !target.subscripts.is_empty() || !target.field_path.is_empty() {
        return Err(ParseError::DetailedError {
            message: format!(
                "column \"{}\" of relation is a generated column",
                column.name
            ),
            detail: Some(
                "Generated columns cannot be assigned through fields or subscripts.".into(),
            ),
            hint: None,
            sqlstate: "428C9",
        });
    }
    if expr.is_some_and(|expr| !matches!(expr, SqlExpr::Default)) {
        return Err(ParseError::DetailedError {
            message: format!(
                "column \"{}\" of relation is a generated column",
                column.name
            ),
            detail: Some("Generated columns can only be assigned DEFAULT.".into()),
            hint: None,
            sqlstate: "428C9",
        });
    }
    Ok(())
}

enum NormalizedInsertExpr<'a> {
    Default,
    Expr(&'a SqlExpr),
}

fn identity_insert_error(column_name: &str) -> ParseError {
    ParseError::DetailedError {
        message: format!("cannot insert a non-DEFAULT value into column \"{column_name}\""),
        detail: Some(format!(
            "Column \"{column_name}\" is an identity column defined as GENERATED ALWAYS."
        )),
        hint: Some("Use OVERRIDING SYSTEM VALUE to override.".into()),
        sqlstate: "428C9",
    }
}

fn identity_update_error(column_name: &str) -> ParseError {
    ParseError::DetailedError {
        message: format!("column \"{column_name}\" can only be updated to DEFAULT"),
        detail: Some(format!(
            "Column \"{column_name}\" is an identity column defined as GENERATED ALWAYS."
        )),
        hint: None,
        sqlstate: "428C9",
    }
}

fn normalize_identity_insert_expr<'a>(
    desc: &RelationDesc,
    target: &BoundAssignmentTarget,
    expr: &'a SqlExpr,
    overriding: Option<OverridingKind>,
) -> Result<NormalizedInsertExpr<'a>, ParseError> {
    let Some(column) = desc.columns.get(target.column_index) else {
        return Ok(NormalizedInsertExpr::Expr(expr));
    };
    let Some(identity) = column.identity else {
        return Ok(NormalizedInsertExpr::Expr(expr));
    };
    if !target.subscripts.is_empty() || !target.field_path.is_empty() {
        return Err(identity_insert_error(&column.name));
    }
    if matches!(expr, SqlExpr::Default) || matches!(overriding, Some(OverridingKind::User)) {
        return Ok(NormalizedInsertExpr::Default);
    }
    if identity == ColumnIdentityKind::Always && !matches!(overriding, Some(OverridingKind::System))
    {
        return Err(identity_insert_error(&column.name));
    }
    Ok(NormalizedInsertExpr::Expr(expr))
}

fn ensure_identity_select_insert_allowed(
    desc: &RelationDesc,
    target: &BoundAssignmentTarget,
    overriding: Option<OverridingKind>,
) -> Result<(), ParseError> {
    let Some(column) = desc.columns.get(target.column_index) else {
        return Ok(());
    };
    if column.identity == Some(ColumnIdentityKind::Always)
        && !matches!(
            overriding,
            Some(OverridingKind::System | OverridingKind::User)
        )
    {
        return Err(identity_insert_error(&column.name));
    }
    Ok(())
}

fn ensure_identity_update_assignment_allowed(
    desc: &RelationDesc,
    target: &BoundAssignmentTarget,
    expr: &SqlExpr,
) -> Result<(), ParseError> {
    let Some(column) = desc.columns.get(target.column_index) else {
        return Ok(());
    };
    if column.identity != Some(ColumnIdentityKind::Always) {
        return Ok(());
    }
    if !target.subscripts.is_empty()
        || !target.field_path.is_empty()
        || !matches!(expr, SqlExpr::Default)
    {
        return Err(identity_update_error(&column.name));
    }
    Ok(())
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
        let target_columns = columns
            .iter()
            .map(|column| resolve_column(&scope, column))
            .collect::<Result<Vec<_>, _>>()?;
        for column_index in &target_columns {
            if entry.desc.columns[*column_index].generated.is_some() {
                ensure_generated_assignment_allowed(
                    &entry.desc,
                    &BoundAssignmentTarget {
                        column_index: *column_index,
                        subscripts: Vec::new(),
                        field_path: Vec::new(),
                        indirection: Vec::new(),
                        target_sql_type: entry.desc.columns[*column_index].sql_type,
                    },
                    Some(&SqlExpr::Const(Value::Null)),
                )?;
            }
            ensure_identity_select_insert_allowed(
                &entry.desc,
                &BoundAssignmentTarget {
                    column_index: *column_index,
                    subscripts: Vec::new(),
                    field_path: Vec::new(),
                    indirection: Vec::new(),
                    target_sql_type: entry.desc.columns[*column_index].sql_type,
                },
                None,
            )?;
        }
        target_columns
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

    for column_index in &target_columns {
        if entry.desc.columns[*column_index].generated.is_some() {
            ensure_generated_assignment_allowed(
                &entry.desc,
                &BoundAssignmentTarget {
                    column_index: *column_index,
                    subscripts: Vec::new(),
                    field_path: Vec::new(),
                    indirection: Vec::new(),
                    target_sql_type: entry.desc.columns[*column_index].sql_type,
                },
                Some(&SqlExpr::Const(Value::Null)),
            )?;
        }
        ensure_identity_select_insert_allowed(
            &entry.desc,
            &BoundAssignmentTarget {
                column_index: *column_index,
                subscripts: Vec::new(),
                field_path: Vec::new(),
                indirection: Vec::new(),
                target_sql_type: entry.desc.columns[*column_index].sql_type,
            },
            None,
        )?;
    }

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
    bind_insert_with_outer_scopes_and_ctes(stmt, catalog, outer_scopes, &[])
}

pub(crate) fn bind_insert_with_outer_scopes_and_ctes(
    stmt: &InsertStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    outer_ctes: &[BoundCte],
) -> Result<BoundInsertStatement, ParseError> {
    let local_ctes = bind_ctes(
        stmt.with_recursive,
        &stmt.with,
        catalog,
        outer_scopes,
        None,
        outer_ctes,
        &[],
    )?;
    let mut visible_ctes = local_ctes.clone();
    visible_ctes.extend_from_slice(outer_ctes);
    let entry = lookup_modify_relation(catalog, &stmt.table_name)?;
    if stmt.on_conflict.as_ref().is_some_and(|clause| {
        clause.action == crate::include::nodes::parsenodes::OnConflictAction::Update
    }) && relation_has_row_security(entry.relation_oid, catalog)
    {
        return Err(unsupported_with_row_security(
            "INSERT ... ON CONFLICT DO UPDATE",
        ));
    }
    let column_defaults = bind_insert_column_defaults(&entry.desc, catalog, &visible_ctes)?;
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
    let target_scope = scope_for_base_relation_with_generated(
        visible_target_name,
        &entry.desc,
        Some(entry.relation_oid),
        catalog,
    )?;
    let expr_scope = empty_scope();
    let returning_scope = scope_with_returning_pseudo_rows(target_scope.clone(), &entry.desc);
    let returning = bind_returning_targets(
        &stmt.returning,
        &returning_scope,
        catalog,
        outer_scopes,
        &visible_ctes,
    )?;

    let source = match &stmt.source {
        InsertSource::Values(rows) => {
            let target_columns = if let Some(columns) = &stmt.columns {
                columns
                    .iter()
                    .map(|column| {
                        bind_assignment_target(column, &target_scope, catalog, &visible_ctes)
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
                        .map(|(expr, target)| {
                            ensure_generated_assignment_allowed(&entry.desc, target, Some(expr))?;
                            if matches!(expr, SqlExpr::Default) {
                                reject_default_indirection_assignment(target)?;
                                return Ok(column_defaults[target.column_index].clone());
                            }
                            match normalize_identity_insert_expr(
                                &entry.desc,
                                target,
                                expr,
                                stmt.overriding,
                            )? {
                                NormalizedInsertExpr::Default => {
                                    Ok(column_defaults[target.column_index].clone())
                                }
                                NormalizedInsertExpr::Expr(expr) => bind_insert_assignment_expr(
                                    expr,
                                    target,
                                    &expr_scope,
                                    catalog,
                                    outer_scopes,
                                    &visible_ctes,
                                ),
                            }
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()?;
            let source = if bound_rows.iter().flatten().any(expr_contains_set_returning) {
                BoundInsertSource::ProjectSetValues(bound_rows)
            } else {
                BoundInsertSource::Values(bound_rows)
            };
            (target_columns, source)
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
            let (mut query, _) = analyze_select_query_with_outer(
                select,
                catalog,
                outer_scopes,
                None,
                None,
                &visible_ctes,
                &[],
            )?;
            let actual = query.columns().len();
            let target_columns = if let Some(columns) = &stmt.columns {
                columns
                    .iter()
                    .map(|column| {
                        bind_assignment_target(column, &target_scope, catalog, &visible_ctes)
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
            for target in &target_columns {
                if entry.desc.columns[target.column_index].generated.is_some() {
                    ensure_generated_assignment_allowed(
                        &entry.desc,
                        target,
                        Some(&SqlExpr::Const(Value::Null)),
                    )?;
                }
                ensure_identity_select_insert_allowed(&entry.desc, target, stmt.overriding)?;
            }
            for (target_entry, target_column) in
                query.target_list.iter_mut().zip(target_columns.iter())
            {
                let source_type = target_entry.sql_type;
                if source_type != target_column.target_sql_type {
                    target_entry.expr = coerce_bound_expr(
                        target_entry.expr.clone(),
                        source_type,
                        target_column.target_sql_type,
                    );
                    target_entry.sql_type = target_column.target_sql_type;
                }
            }
            (target_columns, BoundInsertSource::Select(Box::new(query)))
        }
    };
    let (target_columns, source) = source;
    let required_privileges = vec![insert_privilege_requirement(
        &entry,
        &stmt.table_name,
        &target_columns,
    )];

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
        overriding: stmt.overriding,
        source,
        referenced_by_foreign_keys: bind_referenced_by_foreign_keys(
            entry.relation_oid,
            &entry.desc,
            catalog,
        )?,
        on_conflict: stmt
            .on_conflict
            .as_ref()
            .filter(|_| entry.relkind != 'v')
            .map(|clause| {
                super::on_conflict::bind_on_conflict_clause(
                    clause,
                    visible_target_name,
                    entry.relation_oid,
                    &entry.desc,
                    catalog,
                    &visible_ctes,
                )
            })
            .transpose()?,
        raw_on_conflict: (entry.relkind == 'v')
            .then(|| stmt.on_conflict.clone())
            .flatten(),
        returning,
        rls_write_checks: target_rls.write_checks,
        required_privileges,
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
    if stmt.from.is_some() {
        return bind_update_from(stmt, catalog, outer_scopes, &local_ctes, &entry);
    }
    bind_simple_update(stmt, catalog, outer_scopes, &local_ctes, &entry)
}

fn bind_simple_update(
    stmt: &UpdateStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    local_ctes: &[BoundCte],
    entry: &BoundRelation,
) -> Result<BoundUpdateStatement, ParseError> {
    let target_relation_name = update_target_relation_name(stmt);
    let explain_target_name = update_explain_target_name(stmt);
    let scope = scope_for_base_relation_with_generated(
        &target_relation_name,
        &entry.desc,
        Some(entry.relation_oid),
        catalog,
    )?;
    let returning_scope = scope_with_returning_pseudo_rows(scope.clone(), &entry.desc);
    let column_defaults = bind_insert_column_defaults(&entry.desc, catalog, local_ctes)?;
    let predicate = stmt
        .where_clause
        .as_ref()
        .map(|expr| {
            bind_expr_with_outer_and_ctes(expr, &scope, catalog, outer_scopes, None, local_ctes)
        })
        .transpose()?;
    let returning = bind_returning_targets(
        &stmt.returning,
        &returning_scope,
        catalog,
        outer_scopes,
        local_ctes,
    )?;
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
            let column_index = resolve_column(&scope, &assignment.target.column)?;
            let subscripts = bind_assignment_subscripts(
                &assignment.target.subscripts,
                &scope,
                catalog,
                local_ctes,
                outer_scopes,
            )?;
            let indirection = bind_assignment_indirection(
                &assignment.target.indirection,
                &scope,
                catalog,
                local_ctes,
                outer_scopes,
            )?;
            let target = BoundAssignmentTarget {
                column_index,
                subscripts,
                field_path: assignment.target.field_path.clone(),
                indirection,
                target_sql_type: resolve_assignment_indirection_sql_type(
                    entry.desc.columns[column_index].sql_type,
                    &assignment.target.indirection,
                    catalog,
                )?,
            };
            ensure_generated_assignment_allowed(&entry.desc, &target, Some(&assignment.expr))?;
            ensure_identity_update_assignment_allowed(&entry.desc, &target, &assignment.expr)?;
            Ok(BoundAssignment {
                column_index,
                subscripts: target.subscripts,
                field_path: target.field_path,
                indirection: target.indirection,
                target_sql_type: target.target_sql_type,
                expr: if matches!(assignment.expr, SqlExpr::Default) {
                    column_defaults[column_index].clone()
                } else {
                    bind_expr_with_outer_and_ctes(
                        &assignment.expr,
                        &scope,
                        catalog,
                        outer_scopes,
                        None,
                        local_ctes,
                    )?
                },
            })
        })
        .collect::<Result<Vec<_>, ParseError>>()?;

    let partition_update_root_oid =
        (entry.relkind == 'p' && !stmt.only).then_some(entry.relation_oid);
    let targets = partitioned_update_target_oids(catalog, &entry, stmt.only)
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
                partition_update_root_oid,
                &child,
                catalog,
            )
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    let required_privileges = vec![update_privilege_requirement(
        &entry,
        &stmt.table_name,
        &assignments,
    )];

    Ok(BoundUpdateStatement {
        target_relation_name,
        explain_target_name,
        targets,
        returning,
        input_plan: None,
        target_visible_count: entry.desc.columns.len(),
        visible_column_count: entry.desc.columns.len(),
        target_ctid_index: entry.desc.columns.len(),
        target_tableoid_index: entry.desc.columns.len() + 1,
        required_privileges,
        subplans: Vec::new(),
    })
}

fn bind_update_from(
    stmt: &UpdateStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    local_ctes: &[BoundCte],
    entry: &BoundRelation,
) -> Result<BoundUpdateStatement, ParseError> {
    let target_relation_name = update_target_relation_name(stmt);
    let explain_target_name = update_explain_target_name(stmt);
    let target_scope = scope_for_base_relation_with_generated(
        &target_relation_name,
        &entry.desc,
        Some(entry.relation_oid),
        catalog,
    )?;
    let column_defaults = bind_insert_column_defaults(&entry.desc, catalog, local_ctes)?;
    let source_stmt = stmt.from.as_ref().expect("checked above");
    let (source_from, source_scope_raw) =
        bind_from_item_with_ctes(source_stmt, catalog, outer_scopes, None, local_ctes, &[])?;
    if source_scope_raw.relations.iter().any(|relation| {
        relation
            .relation_names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&target_relation_name))
    }) {
        return Err(ParseError::DuplicateTableName(target_relation_name));
    }

    let mut target_base = AnalyzedFrom::relation(
        target_relation_name.clone(),
        entry.rel,
        entry.relation_oid,
        entry.relkind,
        entry.relispopulated,
        entry.toast,
        !stmt.only && entry.relkind == 'r',
        entry.desc.clone(),
    );
    target_base.output_exprs = generated_relation_output_exprs(&entry.desc, catalog)?;
    let (target_from, _, _) = with_update_target_identity(target_base, &entry.desc);
    let source_scope = shift_scope_rtindexes(source_scope_raw, target_from.rtable.len());
    let source_visible_count = source_scope.desc.columns.len();
    let joined = AnalyzedFrom::join(
        target_from,
        source_from,
        JoinType::Cross,
        Expr::Const(Value::Bool(true)),
        None,
    );
    let target_visible_count = entry.desc.columns.len();
    let visible_column_count = target_visible_count + source_visible_count;
    let projection =
        update_from_projection_targets(&joined, target_visible_count, source_visible_count);
    let projected = joined.with_projection(projection);
    let mut eval_scope = combine_scopes(&target_scope, &source_scope);
    eval_scope.output_exprs = projected.output_exprs[..visible_column_count].to_vec();
    let returning_scope = scope_with_returning_pseudo_rows(eval_scope.clone(), &entry.desc);

    let target_rls = build_target_relation_row_security(
        &stmt.table_name,
        entry.relation_oid,
        &entry.desc,
        PolicyCommand::Update,
        true,
        false,
        catalog,
    )?;
    let predicate = stmt
        .where_clause
        .as_ref()
        .map(|expr| {
            bind_expr_with_outer_and_ctes(
                expr,
                &eval_scope,
                catalog,
                outer_scopes,
                None,
                local_ctes,
            )
        })
        .transpose()?;
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
            let column_index = resolve_column(&target_scope, &assignment.target.column)?;
            let subscripts = bind_assignment_subscripts(
                &assignment.target.subscripts,
                &eval_scope,
                catalog,
                local_ctes,
                outer_scopes,
            )?;
            let indirection = bind_assignment_indirection(
                &assignment.target.indirection,
                &eval_scope,
                catalog,
                local_ctes,
                outer_scopes,
            )?;
            let target = BoundAssignmentTarget {
                column_index,
                subscripts,
                field_path: assignment.target.field_path.clone(),
                indirection,
                target_sql_type: resolve_assignment_indirection_sql_type(
                    entry.desc.columns[column_index].sql_type,
                    &assignment.target.indirection,
                    catalog,
                )?,
            };
            ensure_generated_assignment_allowed(&entry.desc, &target, Some(&assignment.expr))?;
            ensure_identity_update_assignment_allowed(&entry.desc, &target, &assignment.expr)?;
            Ok(BoundAssignment {
                column_index,
                subscripts: target.subscripts,
                field_path: target.field_path,
                indirection: target.indirection,
                target_sql_type: target.target_sql_type,
                expr: if matches!(assignment.expr, SqlExpr::Default) {
                    column_defaults[column_index].clone()
                } else {
                    bind_expr_with_outer_and_ctes(
                        &assignment.expr,
                        &eval_scope,
                        catalog,
                        outer_scopes,
                        None,
                        local_ctes,
                    )?
                },
            })
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    let returning = bind_returning_targets(
        &stmt.returning,
        &returning_scope,
        catalog,
        outer_scopes,
        local_ctes,
    )?;
    let query = query_from_projection_with_qual(projected, predicate.clone());
    let input_plan = crate::backend::optimizer::fold_query_constants(query)
        .map(|query| crate::backend::optimizer::planner(query, catalog))??;

    let partition_update_root_oid =
        (entry.relkind == 'p' && !stmt.only).then_some(entry.relation_oid);
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
        build_update_target_from_joined_input(
            &stmt.table_name,
            &entry.desc,
            &assignments,
            predicate.as_ref(),
            &target_rls.write_checks,
            partition_update_root_oid,
            &child,
            catalog,
        )
    })
    .collect::<Result<Vec<_>, ParseError>>()?;
    let required_privileges = vec![update_privilege_requirement(
        &entry,
        &stmt.table_name,
        &assignments,
    )];

    Ok(BoundUpdateStatement {
        target_relation_name,
        explain_target_name,
        targets,
        returning,
        input_plan: Some(input_plan),
        target_visible_count,
        visible_column_count,
        target_ctid_index: visible_column_count,
        target_tableoid_index: visible_column_count + 1,
        required_privileges,
        subplans: Vec::new(),
    })
}

pub(super) fn bind_assignment_target(
    target: &crate::include::nodes::parsenodes::AssignmentTarget,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    local_ctes: &[BoundCte],
) -> Result<BoundAssignmentTarget, ParseError> {
    let column_index = resolve_column(scope, &target.column)?;
    let indirection =
        bind_assignment_indirection(&target.indirection, scope, catalog, local_ctes, &[])?;
    Ok(BoundAssignmentTarget {
        column_index,
        subscripts: bind_assignment_subscripts(
            &target.subscripts,
            scope,
            catalog,
            local_ctes,
            &[],
        )?,
        field_path: target.field_path.clone(),
        indirection,
        target_sql_type: resolve_assignment_indirection_sql_type(
            scope.desc.columns[column_index].sql_type,
            &target.indirection,
            catalog,
        )?,
    })
}

fn bind_assignment_indirection(
    indirection: &[crate::include::nodes::parsenodes::AssignmentTargetIndirection],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    local_ctes: &[BoundCte],
    outer_scopes: &[BoundScope],
) -> Result<Vec<BoundAssignmentTargetIndirection>, ParseError> {
    indirection
        .iter()
        .map(|step| match step {
            crate::include::nodes::parsenodes::AssignmentTargetIndirection::Field(field) => {
                Ok(BoundAssignmentTargetIndirection::Field(field.clone()))
            }
            crate::include::nodes::parsenodes::AssignmentTargetIndirection::Subscript(
                subscript,
            ) => Ok(BoundAssignmentTargetIndirection::Subscript(
                bind_assignment_subscripts(
                    std::slice::from_ref(subscript),
                    scope,
                    catalog,
                    local_ctes,
                    outer_scopes,
                )?
                .into_iter()
                .next()
                .expect("single subscript should bind to one subscript"),
            )),
        })
        .collect()
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
    let scope = scope_for_relation_with_generated(Some(&stmt.table_name), &entry.desc, catalog)?;
    let returning_scope = scope_with_returning_pseudo_rows(scope.clone(), &entry.desc);
    let predicate = stmt
        .where_clause
        .as_ref()
        .map(|expr| {
            bind_expr_with_outer_and_ctes(expr, &scope, catalog, outer_scopes, None, &local_ctes)
        })
        .transpose()?;
    let returning = bind_returning_targets(
        &stmt.returning,
        &returning_scope,
        catalog,
        outer_scopes,
        &local_ctes,
    )?;
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

    let targets = partitioned_update_target_oids(catalog, &entry, stmt.only)
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
        required_privileges: vec![delete_privilege_requirement(&entry, &stmt.table_name)],
        subplans: Vec::new(),
    })
}
