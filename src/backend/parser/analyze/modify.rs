use super::paths::choose_modify_row_source;
use super::query::rewrite_local_vars_for_output_exprs;
use super::*;
use crate::backend::rewrite::{
    RlsWriteCheck, ViewDmlEvent, ViewDmlRewriteError, apply_query_row_security,
    build_target_relation_row_security, build_target_relation_row_security_for_user,
    pg_rewrite_query, relation_has_row_security, relation_has_security_invoker,
    resolve_auto_updatable_view_target,
};
use crate::backend::utils::record::lookup_anonymous_record_descriptor;
use crate::include::catalog::PolicyCommand;
use crate::include::executor::execdesc::CommandType;
use crate::include::nodes::plannodes::{Plan, PlannedStmt};
use crate::include::nodes::primnodes::{
    INNER_VAR, OUTER_VAR, SELF_ITEM_POINTER_ATTR_NO, TABLE_OID_ATTR_NO, TargetEntry, Var,
    attrno_index, expr_contains_set_returning, expr_sql_type_hint,
};
use crate::include::nodes::primnodes::{
    JoinType, QueryColumn, RelationPrivilegeMask, RelationPrivilegeRequirement,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundInsertStatement {
    pub relation_name: String,
    pub target_alias: Option<String>,
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
    pub allow_partition_routing: bool,
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
    pub parent_desc: Option<RelationDesc>,
    pub(crate) parent_rls_write_checks: Vec<RlsWriteCheck>,
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
    pub current_of: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundDeleteTarget {
    pub relation_name: String,
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub relkind: char,
    pub partition_delete_root_oid: Option<u32>,
    pub relpersistence: char,
    pub toast: Option<ToastRelationRef>,
    pub desc: RelationDesc,
    pub referenced_by_foreign_keys: Vec<BoundReferencedByForeignKey>,
    pub row_source: BoundModifyRowSource,
    pub parent_visible_exprs: Vec<Expr>,
    pub predicate: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundDeleteStatement {
    pub targets: Vec<BoundDeleteTarget>,
    pub returning: Vec<TargetEntry>,
    pub input_plan: Option<PlannedStmt>,
    pub target_visible_count: usize,
    pub visible_column_count: usize,
    pub target_ctid_index: usize,
    pub target_tableoid_index: usize,
    pub required_privileges: Vec<RelationPrivilegeRequirement>,
    pub subplans: Vec<Plan>,
    pub current_of: Option<String>,
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
    pub target_tableoid_index: usize,
    pub source_present_index: usize,
    pub merge_update_visibility_checks: Vec<RlsWriteCheck>,
    pub merge_delete_visibility_checks: Vec<RlsWriteCheck>,
    pub merge_update_write_checks: Vec<RlsWriteCheck>,
    pub merge_insert_write_checks: Vec<RlsWriteCheck>,
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
    predicate: Option<&Expr>,
    returning: &[TargetEntry],
) -> RelationPrivilegeRequirement {
    let mut requirement =
        relation_privilege_requirement(relation, relation_name, RelationPrivilegeMask::update());
    requirement.updated_columns = assignments
        .iter()
        .map(|assignment| assignment.column_index)
        .collect();
    requirement.selected_columns = assignment_expr_selected_columns(assignments);
    if let Some(predicate) = predicate {
        collect_local_selected_columns(predicate, &mut requirement.selected_columns);
    }
    for target in returning {
        collect_local_selected_columns(&target.expr, &mut requirement.selected_columns);
    }
    requirement.selected_columns.sort_unstable();
    requirement.selected_columns.dedup();
    if !requirement.selected_columns.is_empty() {
        requirement.required.select = true;
    }
    requirement
}

fn assignment_expr_selected_columns(assignments: &[BoundAssignment]) -> Vec<usize> {
    let mut selected = Vec::new();
    for assignment in assignments {
        collect_local_selected_columns(&assignment.expr, &mut selected);
        for subscript in &assignment.subscripts {
            if let Some(lower) = &subscript.lower {
                collect_local_selected_columns(lower, &mut selected);
            }
            if let Some(upper) = &subscript.upper {
                collect_local_selected_columns(upper, &mut selected);
            }
        }
        for step in &assignment.indirection {
            if let BoundAssignmentTargetIndirection::Subscript(subscript) = step {
                if let Some(lower) = &subscript.lower {
                    collect_local_selected_columns(lower, &mut selected);
                }
                if let Some(upper) = &subscript.upper {
                    collect_local_selected_columns(upper, &mut selected);
                }
            }
        }
    }
    selected.sort_unstable();
    selected.dedup();
    selected
}

fn collect_local_selected_columns(expr: &Expr, selected: &mut Vec<usize>) {
    match expr {
        Expr::Var(var) => {
            if let Some(index) = local_modify_var_column_index(var) {
                selected.push(index);
            }
        }
        Expr::Param(_)
        | Expr::Const(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::User
        | Expr::SessionUser
        | Expr::SystemUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. }
        | Expr::CaseTest(_) => {}
        Expr::GroupingKey(grouping_key) => {
            collect_local_selected_columns(&grouping_key.expr, selected)
        }
        Expr::GroupingFunc(grouping_func) => {
            for arg in &grouping_func.args {
                collect_local_selected_columns(arg, selected);
            }
        }
        Expr::Aggref(aggref) => {
            for arg in &aggref.direct_args {
                collect_local_selected_columns(arg, selected);
            }
            for arg in &aggref.args {
                collect_local_selected_columns(arg, selected);
            }
            for item in &aggref.aggorder {
                collect_local_selected_columns(&item.expr, selected);
            }
            if let Some(filter) = &aggref.aggfilter {
                collect_local_selected_columns(filter, selected);
            }
        }
        Expr::WindowFunc(func) => {
            for arg in &func.args {
                collect_local_selected_columns(arg, selected);
            }
        }
        Expr::Op(op) => {
            for arg in &op.args {
                collect_local_selected_columns(arg, selected);
            }
        }
        Expr::Bool(bool_expr) => {
            for arg in &bool_expr.args {
                collect_local_selected_columns(arg, selected);
            }
        }
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                collect_local_selected_columns(arg, selected);
            }
            for arm in &case_expr.args {
                collect_local_selected_columns(&arm.expr, selected);
                collect_local_selected_columns(&arm.result, selected);
            }
            collect_local_selected_columns(&case_expr.defresult, selected);
        }
        Expr::Func(func) => {
            for arg in &func.args {
                collect_local_selected_columns(arg, selected);
            }
        }
        Expr::SqlJsonQueryFunction(func) => {
            for child in func.child_exprs() {
                collect_local_selected_columns(child, selected);
            }
        }
        Expr::SetReturning(srf) => {
            for arg in crate::include::nodes::primnodes::set_returning_call_exprs(&srf.call) {
                collect_local_selected_columns(arg, selected);
            }
        }
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = &sublink.testexpr {
                collect_local_selected_columns(testexpr, selected);
            }
        }
        Expr::SubPlan(subplan) => {
            if let Some(testexpr) = &subplan.testexpr {
                collect_local_selected_columns(testexpr, selected);
            }
            for arg in &subplan.args {
                collect_local_selected_columns(arg, selected);
            }
        }
        Expr::ScalarArrayOp(saop) => {
            collect_local_selected_columns(&saop.left, selected);
            collect_local_selected_columns(&saop.right, selected);
        }
        Expr::Xml(xml) => {
            for child in xml.child_exprs() {
                collect_local_selected_columns(child, selected);
            }
        }
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => {
            collect_local_selected_columns(inner, selected);
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_local_selected_columns(expr, selected);
            collect_local_selected_columns(pattern, selected);
            if let Some(escape) = escape {
                collect_local_selected_columns(escape, selected);
            }
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            collect_local_selected_columns(inner, selected);
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            collect_local_selected_columns(left, selected);
            collect_local_selected_columns(right, selected);
        }
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                collect_local_selected_columns(element, selected);
            }
        }
        Expr::Row { fields, .. } => {
            for (_, expr) in fields {
                collect_local_selected_columns(expr, selected);
            }
        }
        Expr::FieldSelect { expr, .. } => collect_local_selected_columns(expr, selected),
        Expr::ArraySubscript { array, subscripts } => {
            collect_local_selected_columns(array, selected);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_local_selected_columns(lower, selected);
                }
                if let Some(upper) = &subscript.upper {
                    collect_local_selected_columns(upper, selected);
                }
            }
        }
    }
}

fn local_modify_var_column_index(var: &Var) -> Option<usize> {
    if var.varlevelsup != 0 {
        return None;
    }
    if var.varno == 1 || matches!(var.varno, OUTER_VAR | INNER_VAR) {
        attrno_index(var.varattno)
    } else {
        None
    }
}

fn on_conflict_update_privilege_requirement(
    relation: &BoundRelation,
    relation_name: impl Into<String>,
    conflict: &BoundOnConflictClause,
    assignments: &[BoundAssignment],
    predicate: Option<&Expr>,
    returning: &[TargetEntry],
) -> RelationPrivilegeRequirement {
    let mut requirement =
        relation_privilege_requirement(relation, relation_name, RelationPrivilegeMask::update());
    requirement.selected_columns = on_conflict_selected_columns(conflict, assignments, predicate);
    for target in returning {
        collect_local_selected_columns(&target.expr, &mut requirement.selected_columns);
    }
    requirement.selected_columns.sort_unstable();
    requirement.selected_columns.dedup();
    if !requirement.selected_columns.is_empty() {
        requirement.required.select = true;
    }
    requirement.updated_columns = assignments
        .iter()
        .map(|assignment| assignment.column_index)
        .collect();
    requirement
}

fn on_conflict_selected_columns(
    conflict: &BoundOnConflictClause,
    assignments: &[BoundAssignment],
    predicate: Option<&Expr>,
) -> Vec<usize> {
    let mut selected = assignment_expr_selected_columns(assignments);
    for index in &conflict.arbiter_indexes {
        for attnum in index
            .index_meta
            .indkey
            .iter()
            .take(index.index_meta.indnkeyatts.max(0) as usize)
        {
            if let Some(column_index) = attrno_index(i32::from(*attnum)) {
                selected.push(column_index);
            }
        }
        for expr in &index.index_exprs {
            collect_local_selected_columns(expr, &mut selected);
        }
        if let Some(predicate) = &index.index_predicate {
            collect_local_selected_columns(predicate, &mut selected);
        }
    }
    if let Some(predicate) = predicate {
        collect_local_selected_columns(predicate, &mut selected);
    }
    selected.sort_unstable();
    selected.dedup();
    selected
}

fn delete_privilege_requirement(
    relation: &BoundRelation,
    relation_name: impl Into<String>,
    predicate: Option<&Expr>,
    returning: &[TargetEntry],
) -> RelationPrivilegeRequirement {
    let mut requirement =
        relation_privilege_requirement(relation, relation_name, RelationPrivilegeMask::delete());
    if let Some(predicate) = predicate {
        collect_local_selected_columns(predicate, &mut requirement.selected_columns);
    }
    for target in returning {
        collect_local_selected_columns(&target.expr, &mut requirement.selected_columns);
    }
    requirement.selected_columns.sort_unstable();
    requirement.selected_columns.dedup();
    if !requirement.selected_columns.is_empty() {
        requirement.required.select = true;
    }
    requirement
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

fn resolve_update_assignment_column(
    scope: &BoundScope,
    target: &crate::include::nodes::parsenodes::AssignmentTarget,
    relation_name: &str,
    visible_target_name: &str,
) -> Result<usize, ParseError> {
    match resolve_column(scope, &target.column) {
        Ok(column_index) => Ok(column_index),
        Err(ParseError::UnknownColumn(name))
            if !target.indirection.is_empty() && name.eq_ignore_ascii_case(visible_target_name) =>
        {
            Err(ParseError::DetailedError {
                message: format!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    target.column, relation_name
                ),
                detail: None,
                hint: Some("SET target columns cannot be qualified with the relation name.".into()),
                sqlstate: "42703",
            })
        }
        Err(err) => Err(err),
    }
}

fn assignment_navigation_sql_type(sql_type: SqlType, catalog: &dyn CatalogLookup) -> SqlType {
    let sql_type = if let Some(domain) = catalog.domain_by_type_oid(sql_type.type_oid) {
        if sql_type.is_array && !domain.sql_type.is_array {
            SqlType::array_of(domain.sql_type)
        } else {
            domain.sql_type
        }
    } else {
        sql_type
    };

    if !sql_type.is_array
        && matches!(sql_type.kind, SqlTypeKind::Composite)
        && sql_type.typrelid == 0
        && let Some(row) = catalog.type_by_oid(sql_type.type_oid)
        && row.typrelid != 0
    {
        return sql_type.with_identity(row.oid, row.typrelid);
    }
    sql_type
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

fn validate_jsonb_assignment_target_subscripts(
    column_type: SqlType,
    target: &BoundAssignmentTarget,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    let mut current = column_type;
    for subscript in &target.subscripts {
        current = validate_jsonb_assignment_subscript_step(current, subscript, catalog)?;
    }
    for step in &target.indirection {
        current = assignment_navigation_sql_type(current, catalog);
        match step {
            BoundAssignmentTargetIndirection::Subscript(subscript) => {
                current = validate_jsonb_assignment_subscript_step(current, subscript, catalog)?;
            }
            BoundAssignmentTargetIndirection::Field(field) => {
                current = resolve_assignment_field_type(current, field, catalog)?;
            }
        }
    }
    Ok(())
}

fn validate_jsonb_assignment_subscript_step(
    current: SqlType,
    subscript: &BoundArraySubscript,
    catalog: &dyn CatalogLookup,
) -> Result<SqlType, ParseError> {
    let current = assignment_navigation_sql_type(current, catalog);
    if current.kind == SqlTypeKind::Jsonb && !current.is_array {
        if subscript.is_slice {
            return Err(ParseError::DetailedError {
                message: "jsonb subscript does not support slices".into(),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
        if let Some(sql_type) = subscript.lower.as_ref().and_then(expr_sql_type_hint) {
            validate_jsonb_subscript_sql_type(sql_type)?;
        }
        return Ok(SqlType::new(SqlTypeKind::Jsonb));
    }
    if current.kind == SqlTypeKind::Point && !current.is_array {
        return Ok(SqlType::new(SqlTypeKind::Float8));
    }
    if current.is_array {
        return Ok(if subscript.is_slice {
            SqlType::array_of(current.element_type())
        } else {
            current.element_type()
        });
    }
    Ok(current)
}

fn validate_jsonb_subscript_sql_type(sql_type: SqlType) -> Result<(), ParseError> {
    if !sql_type.is_array && (is_integer_family(sql_type) || is_text_like_type(sql_type)) {
        return Ok(());
    }
    Err(ParseError::DetailedError {
        message: format!(
            "subscript type {} is not supported",
            sql_type_name(sql_type)
        ),
        detail: None,
        hint: Some("jsonb subscript must be coercible to either integer or text.".into()),
        sqlstate: "42804",
    })
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

fn merge_hidden_tableoid_name() -> String {
    "__merge_target_tableoid".into()
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

fn validate_merge_when_clauses(clauses: &[MergeWhenClause]) -> Result<(), ParseError> {
    let mut matched_terminal = false;
    let mut not_matched_by_source_terminal = false;
    let mut not_matched_by_target_terminal = false;

    for clause in clauses {
        let terminal_seen = match clause.match_kind {
            MergeMatchKind::Matched => &mut matched_terminal,
            MergeMatchKind::NotMatchedBySource => &mut not_matched_by_source_terminal,
            MergeMatchKind::NotMatchedByTarget => &mut not_matched_by_target_terminal,
        };
        if *terminal_seen {
            return Err(ParseError::DetailedError {
                message: "unreachable WHEN clause specified after unconditional WHEN clause".into(),
                detail: None,
                hint: None,
                sqlstate: "42601",
            });
        }
        if clause.condition.is_none() {
            *terminal_seen = true;
        }
    }

    Ok(())
}

fn first_scope_relation_name(scope: &BoundScope) -> Option<&str> {
    scope
        .relations
        .iter()
        .flat_map(|relation| relation.relation_names.iter())
        .find(|name| !name.is_empty())
        .map(String::as_str)
}

fn merge_when_system_column_name(expr: &SqlExpr) -> Option<&str> {
    fn system_name(column: &str) -> Option<&str> {
        let name = column.rsplit('.').next().unwrap_or(column);
        if matches!(
            name.to_ascii_lowercase().as_str(),
            "ctid" | "xmin" | "xmax" | "cmin" | "cmax"
        ) {
            Some(name)
        } else {
            None
        }
    }

    fn first<'a>(left: &'a SqlExpr, right: &'a SqlExpr) -> Option<&'a str> {
        merge_when_system_column_name(left).or_else(|| merge_when_system_column_name(right))
    }

    match expr {
        SqlExpr::Column(name) => system_name(name),
        SqlExpr::Add(left, right)
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
        | SqlExpr::IsDistinctFrom(left, right)
        | SqlExpr::IsNotDistinctFrom(left, right)
        | SqlExpr::Overlaps(left, right)
        | SqlExpr::And(left, right)
        | SqlExpr::Or(left, right)
        | SqlExpr::ArrayOverlap(left, right)
        | SqlExpr::ArrayContains(left, right)
        | SqlExpr::ArrayContained(left, right)
        | SqlExpr::JsonbContains(left, right)
        | SqlExpr::JsonbContained(left, right)
        | SqlExpr::JsonbExists(left, right)
        | SqlExpr::JsonbExistsAny(left, right)
        | SqlExpr::JsonbExistsAll(left, right)
        | SqlExpr::JsonbPathExists(left, right)
        | SqlExpr::JsonbPathMatch(left, right)
        | SqlExpr::JsonGet(left, right)
        | SqlExpr::JsonGetText(left, right)
        | SqlExpr::JsonPath(left, right)
        | SqlExpr::JsonPathText(left, right) => first(left, right),
        SqlExpr::BinaryOperator { left, right, .. }
        | SqlExpr::GeometryBinaryOp { left, right, .. } => first(left, right),
        SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::BitNot(inner)
        | SqlExpr::PrefixOperator { expr: inner, .. }
        | SqlExpr::Cast(inner, _)
        | SqlExpr::Not(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::FieldSelect { expr: inner, .. }
        | SqlExpr::GeometryUnaryOp { expr: inner, .. }
        | SqlExpr::Subscript { expr: inner, .. } => merge_when_system_column_name(inner),
        SqlExpr::Collate { expr, .. } => merge_when_system_column_name(expr),
        SqlExpr::AtTimeZone { expr, zone } => first(expr, zone),
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
        } => merge_when_system_column_name(expr)
            .or_else(|| merge_when_system_column_name(pattern))
            .or_else(|| escape.as_deref().and_then(merge_when_system_column_name)),
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => arg
            .as_deref()
            .and_then(merge_when_system_column_name)
            .or_else(|| {
                args.iter().find_map(|case_when| {
                    merge_when_system_column_name(&case_when.expr)
                        .or_else(|| merge_when_system_column_name(&case_when.result))
                })
            })
            .or_else(|| defresult.as_deref().and_then(merge_when_system_column_name)),
        SqlExpr::ArrayLiteral(exprs) | SqlExpr::Row(exprs) => {
            exprs.iter().find_map(merge_when_system_column_name)
        }
        SqlExpr::InSubquery { expr, .. } => merge_when_system_column_name(expr),
        SqlExpr::QuantifiedSubquery { left, .. } => merge_when_system_column_name(left),
        SqlExpr::QuantifiedArray { left, array, .. } => first(left, array),
        SqlExpr::ArraySubscript { array, subscripts } => merge_when_system_column_name(array)
            .or_else(|| {
                subscripts.iter().find_map(|subscript| {
                    subscript
                        .lower
                        .as_deref()
                        .and_then(merge_when_system_column_name)
                        .or_else(|| {
                            subscript
                                .upper
                                .as_deref()
                                .and_then(merge_when_system_column_name)
                        })
                })
            }),
        SqlExpr::FuncCall {
            args,
            order_by,
            within_group,
            filter,
            ..
        } => match args {
            SqlCallArgs::Star => None,
            SqlCallArgs::Args(args) => args
                .iter()
                .find_map(|arg| merge_when_system_column_name(&arg.value))
                .or_else(|| {
                    order_by
                        .iter()
                        .find_map(|item| merge_when_system_column_name(&item.expr))
                })
                .or_else(|| {
                    within_group.as_ref().and_then(|items| {
                        items
                            .iter()
                            .find_map(|item| merge_when_system_column_name(&item.expr))
                    })
                })
                .or_else(|| filter.as_deref().and_then(merge_when_system_column_name)),
        },
        SqlExpr::Xml(xml) => xml.child_exprs().find_map(merge_when_system_column_name),
        SqlExpr::JsonQueryFunction(json) => json
            .child_exprs()
            .into_iter()
            .find_map(merge_when_system_column_name),
        _ => None,
    }
}

fn reject_merge_when_system_columns(expr: &SqlExpr) -> Result<(), ParseError> {
    if let Some(name) = merge_when_system_column_name(expr) {
        return Err(ParseError::DetailedError {
            message: format!("cannot use system column \"{name}\" in MERGE WHEN condition"),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    Ok(())
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
    outer_scopes: &[BoundScope],
    local_ctes: &[BoundCte],
    target_desc: &RelationDesc,
    column_defaults: &[Expr],
    target_relation_name: &str,
    source_relation_name: Option<&str>,
) -> Result<BoundMergeWhenClause, ParseError> {
    let action_scope = match clause.match_kind {
        MergeMatchKind::Matched => merged_scope.clone(),
        MergeMatchKind::NotMatchedBySource => source_relation_name.map_or_else(
            || target_scope.clone(),
            |relation_name| {
                scope_with_hidden_invalid_relation(
                    target_scope.clone(),
                    relation_name,
                    &source_scope.desc,
                )
            },
        ),
        MergeMatchKind::NotMatchedByTarget => scope_with_hidden_invalid_relation(
            source_scope.clone(),
            target_relation_name,
            target_desc,
        ),
    };
    let condition = clause
        .condition
        .as_ref()
        .map(|condition| {
            reject_merge_when_system_columns(condition)?;
            bind_expr_with_outer_and_ctes(
                condition,
                &action_scope,
                catalog,
                outer_scopes,
                None,
                local_ctes,
            )
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
                    validate_jsonb_assignment_target_subscripts(
                        target_desc.columns[column_index].sql_type,
                        &target,
                        catalog,
                    )?;
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
                                &action_scope,
                                catalog,
                                outer_scopes,
                                None,
                                local_ctes,
                            )?
                        },
                    })
                })
                .collect::<Result<Vec<_>, ParseError>>()?;
            BoundMergeAction::Update { assignments }
        }
        MergeAction::Insert {
            columns,
            overriding,
            source,
        } => {
            let expanded_values = match source {
                MergeInsertSource::Values(values) => Some(expand_insert_values_row_exprs(
                    values,
                    &action_scope,
                    outer_scopes,
                )?),
                MergeInsertSource::DefaultValues => None,
            };
            let target_columns = if let Some(columns) = columns {
                columns
                    .iter()
                    .map(|column| bind_assignment_target(column, target_scope, catalog, local_ctes))
                    .collect::<Result<Vec<_>, ParseError>>()?
            } else {
                let width = match source {
                    MergeInsertSource::Values(_) => {
                        expanded_values.as_ref().map(Vec::len).unwrap_or(0)
                    }
                    MergeInsertSource::DefaultValues => target_desc.visible_column_indexes().len(),
                };
                merge_visible_insert_targets(target_desc, width)?
            };
            let values = match source {
                MergeInsertSource::Values(_) => {
                    let expanded_values = expanded_values.expect("MERGE VALUES row was expanded");
                    if expanded_values.len() != target_columns.len() {
                        return Err(ParseError::InvalidInsertTargetCount {
                            expected: target_columns.len(),
                            actual: expanded_values.len(),
                        });
                    }
                    Some(
                        expanded_values
                            .iter()
                            .zip(target_columns.iter())
                            .map(|(cell, target)| match cell {
                                InsertValuesCell::Raw(expr) => {
                                    ensure_generated_assignment_allowed(
                                        target_desc,
                                        target,
                                        Some(expr),
                                    )?;
                                    if matches!(expr, SqlExpr::Default) {
                                        reject_default_indirection_assignment(target)?;
                                        return Ok(column_defaults[target.column_index].clone());
                                    }
                                    match normalize_identity_insert_expr(
                                        target_desc,
                                        target,
                                        expr,
                                        *overriding,
                                    )? {
                                        NormalizedInsertExpr::Default => {
                                            reject_default_indirection_assignment(target)?;
                                            Ok(column_defaults[target.column_index].clone())
                                        }
                                        NormalizedInsertExpr::Expr(expr) => {
                                            bind_insert_assignment_expr(
                                                expr,
                                                target_desc,
                                                target,
                                                &action_scope,
                                                catalog,
                                                outer_scopes,
                                                local_ctes,
                                            )
                                        }
                                    }
                                }
                                InsertValuesCell::Bound(expr) => {
                                    ensure_generated_assignment_allowed(
                                        target_desc,
                                        target,
                                        Some(&SqlExpr::Const(Value::Null)),
                                    )?;
                                    ensure_identity_select_insert_allowed(
                                        target_desc,
                                        target,
                                        *overriding,
                                    )?;
                                    let source_type = expr_sql_type_hint(expr)
                                        .unwrap_or(SqlType::new(SqlTypeKind::Text));
                                    reject_invalid_domain_array_assignment(
                                        target_desc,
                                        target,
                                        source_type,
                                        catalog,
                                    )?;
                                    Ok(coerce_bound_expr(
                                        expr.clone(),
                                        source_type,
                                        target.target_sql_type,
                                    ))
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
    for target in targets {
        reject_window_clause(&target.expr, "RETURNING")?;
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
                collation_oid: None,
            })
        })
        .collect()
}

fn returning_pseudo_output_exprs_with_generated(
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
    varno: usize,
) -> Result<Vec<Expr>, ParseError> {
    let base_output_exprs = returning_pseudo_output_exprs(desc, varno);
    generated_relation_output_exprs(desc, catalog).map(|output_exprs| {
        output_exprs
            .into_iter()
            .map(|expr| rewrite_local_vars_for_output_exprs(expr, 1, &base_output_exprs))
            .collect()
    })
}

fn scope_with_returning_pseudo_rows_with_generated(
    scope: BoundScope,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
    relation_oid: Option<u32>,
) -> Result<BoundScope, ParseError> {
    Ok(scope_with_returning_pseudo_row_exprs(
        scope,
        desc,
        returning_pseudo_output_exprs_with_generated(desc, catalog, OUTER_VAR)?,
        returning_pseudo_output_exprs_with_generated(desc, catalog, INNER_VAR)?,
        relation_oid,
    ))
}

fn scope_with_hidden_invalid_relation(
    mut scope: BoundScope,
    relation_name: &str,
    desc: &RelationDesc,
) -> BoundScope {
    scope.desc.columns.extend(desc.columns.iter().cloned());
    scope.output_exprs.extend(std::iter::repeat_n(
        Expr::Const(Value::Null),
        desc.columns.len(),
    ));
    scope
        .columns
        .extend(desc.columns.iter().map(|column| ScopeColumn {
            output_name: column.name.clone(),
            hidden: true,
            qualified_only: true,
            relation_names: Vec::new(),
            hidden_invalid_relation_names: vec![relation_name.to_string()],
            hidden_missing_relation_names: Vec::new(),
            source_relation_oid: None,
            source_attno: None,
            source_columns: Vec::new(),
        }));
    scope.relations.push(ScopeRelation {
        relation_names: Vec::new(),
        hidden_invalid_relation_names: vec![relation_name.to_string()],
        hidden_missing_relation_names: Vec::new(),
        system_varno: None,
        relation_oid: None,
    });
    scope
}

fn mark_scope_hidden_invalid_relation(mut scope: BoundScope, relation_name: &str) -> BoundScope {
    for column in &mut scope.columns {
        if !column
            .hidden_invalid_relation_names
            .iter()
            .any(|hidden| hidden.eq_ignore_ascii_case(relation_name))
        {
            column
                .hidden_invalid_relation_names
                .push(relation_name.to_string());
        }
    }
    for relation in &mut scope.relations {
        if !relation
            .hidden_invalid_relation_names
            .iter()
            .any(|hidden| hidden.eq_ignore_ascii_case(relation_name))
        {
            relation
                .hidden_invalid_relation_names
                .push(relation_name.to_string());
        }
    }
    scope
}

fn scope_with_returning_pseudo_row_exprs(
    mut scope: BoundScope,
    desc: &RelationDesc,
    old_output_exprs: Vec<Expr>,
    new_output_exprs: Vec<Expr>,
    relation_oid: Option<u32>,
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
                source_relation_oid: relation_oid,
                source_attno: None,
                source_columns: Vec::new(),
            }));
        scope.relations.push(ScopeRelation {
            relation_names: vec![relation_name.to_string()],
            hidden_invalid_relation_names: vec![],
            hidden_missing_relation_names: vec![],
            system_varno: None,
            relation_oid,
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
                collation_oid: None,
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
                collation_oid: None,
            })
        })
        .collect()
}

fn scope_with_output_exprs(mut scope: BoundScope, output_exprs: Vec<Expr>) -> BoundScope {
    scope.output_exprs = output_exprs;
    scope
}

fn with_merge_target_identity(
    from: AnalyzedFrom,
    target_desc: &RelationDesc,
) -> (AnalyzedFrom, usize, usize) {
    let mut targets = merge_projection_targets(&from.output_columns, &from.output_exprs);
    let ctid_resno = targets.len() + 1;
    targets.push(
        TargetEntry::new(
            merge_hidden_ctid_name(),
            Expr::Var(Var {
                varno: 1,
                varattno: SELF_ITEM_POINTER_ATTR_NO,
                varlevelsup: 0,
                vartype: SqlType::new(SqlTypeKind::Tid),
                collation_oid: None,
            }),
            SqlType::new(SqlTypeKind::Tid),
            ctid_resno,
        )
        .with_input_resno(ctid_resno),
    );
    let tableoid_resno = targets.len() + 1;
    targets.push(
        TargetEntry::new(
            merge_hidden_tableoid_name(),
            Expr::Var(Var {
                varno: 1,
                varattno: TABLE_OID_ATTR_NO,
                varlevelsup: 0,
                vartype: SqlType::new(SqlTypeKind::Oid),
                collation_oid: None,
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
                collation_oid: None,
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
                collation_oid: None,
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
        group_by_refs: Vec::new(),
        grouping_sets: Vec::new(),
        accumulators: Vec::new(),
        window_clauses: Vec::new(),
        having_qual: None,
        sort_clause: Vec::new(),
        constraint_deps: Vec::new(),
        limit_count: None,
        limit_offset: None,
        locking_clause: None,
        locking_targets: Vec::new(),
        locking_nowait: false,
        row_marks: Vec::new(),
        has_target_srfs: false,
        recursive_union: None,
        set_operation: None,
    }
}

fn query_from_projection_with_target_security_quals(
    mut input: AnalyzedFrom,
    security_quals: Vec<Expr>,
    where_qual: Option<Expr>,
    catalog: &dyn CatalogLookup,
) -> Result<Query, ParseError> {
    if let Some(rte) = input.rtable.first_mut() {
        match &mut rte.kind {
            crate::include::nodes::parsenodes::RangeTblEntryKind::Subquery { query } => {
                if let Some(target_rte) = query.rtable.first_mut() {
                    target_rte.security_quals.extend(security_quals);
                }
                apply_query_row_security(query, catalog)?;
            }
            _ => {
                rte.security_quals.extend(security_quals);
                let mut query = query_from_projection_with_qual(input, where_qual);
                apply_query_row_security(&mut query, catalog)?;
                return Ok(query);
            }
        }
    }
    Ok(query_from_projection_with_qual(input, where_qual))
}

fn prepend_visibility_quals(
    mut visibility_quals: Vec<Expr>,
    predicate: Option<Expr>,
) -> Option<Expr> {
    if let Some(predicate) = predicate {
        visibility_quals.push(predicate);
    }
    let first = visibility_quals.first().cloned()?;
    Some(visibility_quals.into_iter().skip(1).fold(first, Expr::and))
}

fn build_visibility_write_checks(
    visibility_quals: Vec<Expr>,
    source: crate::backend::rewrite::RlsWriteCheckSource,
) -> Vec<RlsWriteCheck> {
    visibility_quals
        .into_iter()
        .map(|expr| RlsWriteCheck {
            expr,
            display_exprs: Vec::new(),
            policy_name: None,
            source: source.clone(),
        })
        .collect()
}

fn build_conflict_visibility_checks(visibility_quals: Vec<Expr>) -> Vec<RlsWriteCheck> {
    build_visibility_write_checks(
        visibility_quals,
        crate::backend::rewrite::RlsWriteCheckSource::ConflictUpdateVisibility,
    )
}

fn auto_view_base_rls_user_oid(
    resolved: &crate::backend::rewrite::ResolvedAutoViewTarget,
    catalog: &dyn CatalogLookup,
) -> u32 {
    resolved
        .privilege_contexts
        .iter()
        .rev()
        .find(|context| context.relation.relation_oid == resolved.base_relation.relation_oid)
        .and_then(|context| context.check_as_user_oid)
        .unwrap_or_else(|| catalog.current_user_oid())
}

fn build_auto_view_base_row_security(
    relation_name: &str,
    resolved: &crate::backend::rewrite::ResolvedAutoViewTarget,
    command: PolicyCommand,
    include_select_visibility: bool,
    include_select_check: bool,
    catalog: &dyn CatalogLookup,
) -> Result<crate::backend::rewrite::TargetRlsState, ViewDmlRewriteError> {
    build_target_relation_row_security_for_user(
        relation_name,
        resolved.base_relation.relation_oid,
        &resolved.base_relation.desc,
        command,
        include_select_visibility,
        include_select_check,
        auto_view_base_rls_user_oid(resolved, catalog),
        catalog,
    )
    .map_err(|err| ViewDmlRewriteError::UnsupportedViewShape(err.to_string()))
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

const MERGE_ACTION_RETURNING_COLUMN: &str = "__pgrust_merge_action";

fn scope_with_merge_action_column(mut scope: BoundScope, merge_action_index: usize) -> BoundScope {
    let sql_type = SqlType::new(SqlTypeKind::Text);
    let merge_action_column = column_desc(MERGE_ACTION_RETURNING_COLUMN, sql_type, true);
    let collation_oid = Some(merge_action_column.collation_oid);
    scope.desc.columns.push(merge_action_column);
    scope.output_exprs.push(Expr::Var(Var {
        varno: 1,
        varattno: user_attrno(merge_action_index),
        varlevelsup: 0,
        vartype: sql_type,
        collation_oid,
    }));
    scope.columns.push(ScopeColumn {
        output_name: MERGE_ACTION_RETURNING_COLUMN.into(),
        hidden: false,
        qualified_only: false,
        relation_names: Vec::new(),
        hidden_invalid_relation_names: Vec::new(),
        hidden_missing_relation_names: Vec::new(),
        source_relation_oid: None,
        source_attno: None,
        source_columns: Vec::new(),
    });
    scope
}

fn rewrite_merge_action_select_item(
    item: &crate::include::nodes::parsenodes::SelectItem,
) -> (crate::include::nodes::parsenodes::SelectItem, bool) {
    let (expr, changed) = rewrite_merge_action_expr(&item.expr);
    (
        crate::include::nodes::parsenodes::SelectItem {
            output_name: item.output_name.clone(),
            expr,
        },
        changed,
    )
}

fn rewrite_merge_action_order_by(item: &OrderByItem) -> (OrderByItem, bool) {
    let (expr, changed) = rewrite_merge_action_expr(&item.expr);
    (
        OrderByItem {
            expr,
            descending: item.descending,
            nulls_first: item.nulls_first,
            using_operator: item.using_operator.clone(),
        },
        changed,
    )
}

fn rewrite_merge_action_select(stmt: &SelectStatement) -> (SelectStatement, bool) {
    let mut changed = false;
    let targets = stmt
        .targets
        .iter()
        .map(|item| {
            let (item, item_changed) = rewrite_merge_action_select_item(item);
            changed |= item_changed;
            item
        })
        .collect();
    let where_clause = stmt.where_clause.as_ref().map(|expr| {
        let (expr, expr_changed) = rewrite_merge_action_expr(expr);
        changed |= expr_changed;
        expr
    });
    let having = stmt.having.as_ref().map(|expr| {
        let (expr, expr_changed) = rewrite_merge_action_expr(expr);
        changed |= expr_changed;
        expr
    });
    let distinct_on = stmt
        .distinct_on
        .iter()
        .map(|expr| {
            let (expr, expr_changed) = rewrite_merge_action_expr(expr);
            changed |= expr_changed;
            expr
        })
        .collect();
    let order_by = stmt
        .order_by
        .iter()
        .map(|item| {
            let (item, item_changed) = rewrite_merge_action_order_by(item);
            changed |= item_changed;
            item
        })
        .collect();

    (
        SelectStatement {
            with_recursive: stmt.with_recursive,
            with: stmt.with.clone(),
            distinct: stmt.distinct,
            distinct_on,
            from: stmt.from.clone(),
            targets,
            where_clause,
            group_by: stmt.group_by.clone(),
            group_by_distinct: stmt.group_by_distinct,
            having,
            window_clauses: stmt.window_clauses.clone(),
            order_by,
            limit: stmt.limit,
            offset: stmt.offset,
            locking_clause: stmt.locking_clause,
            locking_nowait: stmt.locking_nowait,
            locking_targets: stmt.locking_targets.clone(),
            set_operation: stmt.set_operation.clone(),
        },
        changed,
    )
}

fn rewrite_merge_action_expr(expr: &SqlExpr) -> (SqlExpr, bool) {
    if is_merge_action_returning_call(expr) {
        return (SqlExpr::Column(MERGE_ACTION_RETURNING_COLUMN.into()), true);
    }

    fn boxed(expr: &SqlExpr) -> (Box<SqlExpr>, bool) {
        let (expr, changed) = rewrite_merge_action_expr(expr);
        (Box::new(expr), changed)
    }

    fn binary(
        left: &SqlExpr,
        right: &SqlExpr,
        make: impl FnOnce(Box<SqlExpr>, Box<SqlExpr>) -> SqlExpr,
    ) -> (SqlExpr, bool) {
        let (left, left_changed) = boxed(left);
        let (right, right_changed) = boxed(right);
        (make(left, right), left_changed || right_changed)
    }

    match expr {
        SqlExpr::Add(left, right) => binary(left, right, SqlExpr::Add),
        SqlExpr::Sub(left, right) => binary(left, right, SqlExpr::Sub),
        SqlExpr::BitAnd(left, right) => binary(left, right, SqlExpr::BitAnd),
        SqlExpr::BitOr(left, right) => binary(left, right, SqlExpr::BitOr),
        SqlExpr::BitXor(left, right) => binary(left, right, SqlExpr::BitXor),
        SqlExpr::Shl(left, right) => binary(left, right, SqlExpr::Shl),
        SqlExpr::Shr(left, right) => binary(left, right, SqlExpr::Shr),
        SqlExpr::Mul(left, right) => binary(left, right, SqlExpr::Mul),
        SqlExpr::Div(left, right) => binary(left, right, SqlExpr::Div),
        SqlExpr::Mod(left, right) => binary(left, right, SqlExpr::Mod),
        SqlExpr::Concat(left, right) => binary(left, right, SqlExpr::Concat),
        SqlExpr::Eq(left, right) => binary(left, right, SqlExpr::Eq),
        SqlExpr::NotEq(left, right) => binary(left, right, SqlExpr::NotEq),
        SqlExpr::Lt(left, right) => binary(left, right, SqlExpr::Lt),
        SqlExpr::LtEq(left, right) => binary(left, right, SqlExpr::LtEq),
        SqlExpr::Gt(left, right) => binary(left, right, SqlExpr::Gt),
        SqlExpr::GtEq(left, right) => binary(left, right, SqlExpr::GtEq),
        SqlExpr::And(left, right) => binary(left, right, SqlExpr::And),
        SqlExpr::Or(left, right) => binary(left, right, SqlExpr::Or),
        SqlExpr::BinaryOperator { op, left, right } => {
            binary(left, right, |left, right| SqlExpr::BinaryOperator {
                op: op.clone(),
                left,
                right,
            })
        }
        SqlExpr::UnaryPlus(inner) => {
            let (inner, changed) = boxed(inner);
            (SqlExpr::UnaryPlus(inner), changed)
        }
        SqlExpr::Negate(inner) => {
            let (inner, changed) = boxed(inner);
            (SqlExpr::Negate(inner), changed)
        }
        SqlExpr::Not(inner) => {
            let (inner, changed) = boxed(inner);
            (SqlExpr::Not(inner), changed)
        }
        SqlExpr::Cast(inner, ty) => {
            let (inner, changed) = boxed(inner);
            (SqlExpr::Cast(inner, ty.clone()), changed)
        }
        SqlExpr::FieldSelect { expr, field } => {
            let (expr, changed) = boxed(expr);
            (
                SqlExpr::FieldSelect {
                    expr,
                    field: field.clone(),
                },
                changed,
            )
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            let mut changed = false;
            let arg = arg.as_ref().map(|expr| {
                let (expr, expr_changed) = boxed(expr);
                changed |= expr_changed;
                expr
            });
            let args = args
                .iter()
                .map(|case_when| {
                    let (expr, expr_changed) = rewrite_merge_action_expr(&case_when.expr);
                    let (result, result_changed) = rewrite_merge_action_expr(&case_when.result);
                    changed |= expr_changed || result_changed;
                    SqlCaseWhen { expr, result }
                })
                .collect();
            let defresult = defresult.as_ref().map(|expr| {
                let (expr, expr_changed) = boxed(expr);
                changed |= expr_changed;
                expr
            });
            (
                SqlExpr::Case {
                    arg,
                    args,
                    defresult,
                },
                changed,
            )
        }
        SqlExpr::ScalarSubquery(select) => {
            let (select, changed) = rewrite_merge_action_select(select);
            (SqlExpr::ScalarSubquery(Box::new(select)), changed)
        }
        SqlExpr::ArraySubquery(select) => {
            let (select, changed) = rewrite_merge_action_select(select);
            (SqlExpr::ArraySubquery(Box::new(select)), changed)
        }
        SqlExpr::Exists(select) => {
            let (select, changed) = rewrite_merge_action_select(select);
            (SqlExpr::Exists(Box::new(select)), changed)
        }
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let (expr, expr_changed) = boxed(expr);
            let (subquery, query_changed) = rewrite_merge_action_select(subquery);
            (
                SqlExpr::InSubquery {
                    expr,
                    subquery: Box::new(subquery),
                    negated: *negated,
                },
                expr_changed || query_changed,
            )
        }
        SqlExpr::ArrayLiteral(exprs) => {
            let mut changed = false;
            let exprs = exprs
                .iter()
                .map(|expr| {
                    let (expr, expr_changed) = rewrite_merge_action_expr(expr);
                    changed |= expr_changed;
                    expr
                })
                .collect();
            (SqlExpr::ArrayLiteral(exprs), changed)
        }
        SqlExpr::Row(exprs) => {
            let mut changed = false;
            let exprs = exprs
                .iter()
                .map(|expr| {
                    let (expr, expr_changed) = rewrite_merge_action_expr(expr);
                    changed |= expr_changed;
                    expr
                })
                .collect();
            (SqlExpr::Row(exprs), changed)
        }
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
            let mut changed = false;
            let args = match args {
                SqlCallArgs::Star => SqlCallArgs::Star,
                SqlCallArgs::Args(args) => SqlCallArgs::Args(
                    args.iter()
                        .map(|arg| {
                            let (value, arg_changed) = rewrite_merge_action_expr(&arg.value);
                            changed |= arg_changed;
                            SqlFunctionArg {
                                name: arg.name.clone(),
                                value,
                            }
                        })
                        .collect(),
                ),
            };
            let order_by = order_by
                .iter()
                .map(|item| {
                    let (item, item_changed) = rewrite_merge_action_order_by(item);
                    changed |= item_changed;
                    item
                })
                .collect();
            let filter = filter.as_ref().map(|expr| {
                let (expr, expr_changed) = boxed(expr);
                changed |= expr_changed;
                expr
            });
            (
                SqlExpr::FuncCall {
                    name: name.clone(),
                    args,
                    order_by,
                    within_group: within_group.clone(),
                    distinct: *distinct,
                    func_variadic: *func_variadic,
                    filter,
                    null_treatment: *null_treatment,
                    over: over.clone(),
                },
                changed,
            )
        }
        _ => (expr.clone(), false),
    }
}

fn bind_merge_returning_targets(
    targets: &[crate::include::nodes::parsenodes::SelectItem],
    scope: &BoundScope,
    merge_action_index: usize,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    local_ctes: &[BoundCte],
) -> Result<Vec<TargetEntry>, ParseError> {
    let mut entries = Vec::new();
    let scope_with_merge_action = scope_with_merge_action_column(scope.clone(), merge_action_index);
    for item in targets {
        let (rewritten_item, rewritten) = rewrite_merge_action_select_item(item);
        if is_merge_action_returning_call(&item.expr) {
            entries.push(
                TargetEntry::new(
                    item.output_name.clone(),
                    Expr::Var(Var {
                        varno: 1,
                        varattno: user_attrno(merge_action_index),
                        varlevelsup: 0,
                        vartype: SqlType::new(SqlTypeKind::Text),
                        collation_oid: None,
                    }),
                    SqlType::new(SqlTypeKind::Text),
                    entries.len() + 1,
                )
                .with_input_resno(merge_action_index + 1),
            );
            continue;
        }
        let BoundSelectTargets::Plain(bound) = bind_select_targets(
            std::slice::from_ref(if rewritten { &rewritten_item } else { item }),
            if rewritten {
                &scope_with_merge_action
            } else {
                scope
            },
            catalog,
            outer_scopes,
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
    if stmt.with_recursive {
        return Err(ParseError::FeatureNotSupportedMessage(
            "WITH RECURSIVE is not supported for MERGE statement".into(),
        ));
    }
    plan_merge_with_outer_ctes(stmt, catalog, &[])
}

pub(crate) fn plan_merge_with_outer_ctes(
    stmt: &MergeStatement,
    catalog: &dyn CatalogLookup,
    outer_ctes: &[BoundCte],
) -> Result<BoundMergeStatement, ParseError> {
    plan_merge_with_outer_scopes_and_ctes(stmt, catalog, &[], outer_ctes)
}

pub(crate) fn plan_merge_with_outer_scopes_and_ctes(
    stmt: &MergeStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    outer_ctes: &[BoundCte],
) -> Result<BoundMergeStatement, ParseError> {
    validate_merge_when_clauses(&stmt.when_clauses)?;
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
    let entry = lookup_merge_relation(catalog, &stmt.target_table)?;
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
    let execution_relation_name = auto_view_target
        .as_ref()
        .map(|_| {
            relation_display_name(catalog, execution_relation.relation_oid, &stmt.target_table)
        })
        .unwrap_or_else(|| stmt.target_table.clone());
    let column_defaults =
        bind_insert_column_defaults(&execution_relation.desc, catalog, &visible_ctes)?;
    let target_relation_name = merge_target_relation_name(stmt);
    let explain_target_name = auto_view_target
        .as_ref()
        .map(|_| execution_relation_name.clone())
        .unwrap_or_else(|| merge_explain_target_name(stmt));
    let mut target_base = AnalyzedFrom::relation(
        execution_relation_name.clone(),
        execution_relation.rel,
        execution_relation.relation_oid,
        execution_relation.relkind,
        execution_relation.relispopulated,
        execution_relation.toast,
        !stmt.target_only && matches!(execution_relation.relkind, 'r' | 'p'),
        execution_relation.desc.clone(),
    );
    if let Some(alias) = &stmt.target_alias
        && let Some(rte) = target_base.rtable.get_mut(0)
    {
        rte.alias = Some(alias.clone());
        rte.eref.aliasname = alias.clone();
    }
    if auto_view_target.is_some()
        && let Some(permission) = target_base
            .rtable
            .get_mut(0)
            .and_then(|rte| rte.permission.as_mut())
    {
        permission.check_as_user_oid = view_check_as_user_oid(entry.relation_oid, catalog);
    }
    target_base.output_exprs = generated_relation_output_exprs(&execution_relation.desc, catalog)?;
    let (target_from, target_visible_count, target_tableoid_input_index) =
        with_merge_target_identity(target_base, &execution_relation.desc);
    let mut target_scope = scope_for_base_relation_with_generated(
        &target_relation_name,
        &entry.desc,
        Some(entry.relation_oid),
        catalog,
    )?;
    if let Some(resolved) = auto_view_target.as_ref() {
        target_scope.output_exprs = resolved.visible_output_exprs.clone();
    }
    let invalid_target_outer_scope =
        scope_with_hidden_invalid_relation(empty_scope(), &target_relation_name, &entry.desc);
    let mut source_outer_scopes = Vec::with_capacity(outer_scopes.len() + 1);
    source_outer_scopes.push(invalid_target_outer_scope);
    source_outer_scopes.extend_from_slice(outer_scopes);
    let (source_base, source_scope_raw) = bind_from_item_with_ctes(
        &stmt.source,
        catalog,
        &source_outer_scopes,
        None,
        &visible_ctes,
        &[],
    )?;
    let (source_from, source_visible_count) = with_merge_source_present(source_base);

    if source_scope_raw.relations.iter().any(|relation| {
        relation
            .relation_names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&target_relation_name))
    }) {
        return Err(merge_duplicate_source_target_error(&target_relation_name));
    }

    // PostgreSQL's setrefs.c handles MERGE actions by rewriting source Vars
    // against the MERGE subplan targetlist while leaving target-table Vars as
    // scan-tuple references. pgrust represents the source side as a projected
    // subquery, so bind source references to that projection before building
    // the target/source join.
    let source_scope = shift_scope_rtindexes(
        scope_with_output_exprs(
            source_scope_raw,
            source_from
                .output_exprs
                .iter()
                .take(source_visible_count)
                .cloned()
                .collect(),
        ),
        target_from.rtable.len(),
    );
    let merged_scope = combine_scopes(&target_scope, &source_scope);
    let join_condition = bind_expr_with_outer_and_ctes(
        &stmt.join_condition,
        &merged_scope,
        catalog,
        outer_scopes,
        None,
        &visible_ctes,
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
    let returning_merged_scope = combine_scopes(&action_source_scope, &action_target_scope);
    let source_relation_name = first_scope_relation_name(&source_scope).map(str::to_string);

    let returning_visible_column_count =
        execution_relation.desc.columns.len() + source_visible_count;
    let returning_scope = if let Some(resolved) = auto_view_target.as_ref() {
        scope_with_returning_pseudo_row_exprs(
            returning_merged_scope,
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
            Some(entry.relation_oid),
        )
    } else {
        scope_with_returning_pseudo_rows_with_generated(
            returning_merged_scope,
            &execution_relation.desc,
            catalog,
            Some(execution_relation.relation_oid),
        )?
    };
    let returning = bind_merge_returning_targets(
        &stmt.returning,
        &returning_scope,
        returning_visible_column_count,
        catalog,
        outer_scopes,
        &visible_ctes,
    )?;
    let merge_update_rls = build_target_relation_row_security(
        &stmt.target_table,
        execution_relation.relation_oid,
        &execution_relation.desc,
        PolicyCommand::Update,
        false,
        true,
        catalog,
    )?;
    let merge_delete_rls = build_target_relation_row_security(
        &stmt.target_table,
        execution_relation.relation_oid,
        &execution_relation.desc,
        PolicyCommand::Delete,
        false,
        false,
        catalog,
    )?;
    let merge_insert_rls = build_target_relation_row_security(
        &stmt.target_table,
        execution_relation.relation_oid,
        &execution_relation.desc,
        PolicyCommand::Insert,
        false,
        !returning.is_empty(),
        catalog,
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
                outer_scopes,
                &visible_ctes,
                &entry.desc,
                &column_defaults,
                &target_relation_name,
                source_relation_name.as_deref(),
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
        false,
        join_condition,
        None,
    );
    let visible_column_count = returning_visible_column_count;
    let target_ctid_index = visible_column_count;
    let target_tableoid_index = visible_column_count + 1;
    let source_present_index = visible_column_count + 2;
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
    let source_start = target_visible_count + 3;
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
            SqlType::new(SqlTypeKind::Tid),
            projection_targets.len() + 1,
        )
        .with_input_resno(target_visible_count + 1),
    );
    projection_targets.push(
        TargetEntry::new(
            merge_hidden_tableoid_name(),
            joined_output_exprs[target_tableoid_input_index].clone(),
            SqlType::new(SqlTypeKind::Oid),
            projection_targets.len() + 1,
        )
        .with_input_resno(target_tableoid_input_index + 1),
    );
    let source_marker_input = target_visible_count + 3 + source_visible_count;
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
    let [query] = pg_rewrite_query(query, catalog)?
        .try_into()
        .expect("MERGE input rewrite should return a single query");

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
        target_tableoid_index,
        source_present_index,
        merge_update_visibility_checks: build_visibility_write_checks(
            merge_update_rls.visibility_quals,
            crate::backend::rewrite::RlsWriteCheckSource::MergeUpdateVisibility,
        ),
        merge_delete_visibility_checks: build_visibility_write_checks(
            merge_delete_rls.visibility_quals,
            crate::backend::rewrite::RlsWriteCheckSource::MergeDeleteVisibility,
        ),
        merge_update_write_checks: merge_update_rls.write_checks,
        merge_insert_write_checks: merge_insert_rls.write_checks,
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

fn merge_duplicate_source_target_error(name: &str) -> ParseError {
    ParseError::DetailedError {
        message: format!("name \"{name}\" specified more than once"),
        detail: Some("The name is used both as MERGE target table and data source.".into()),
        hint: None,
        sqlstate: "42712",
    }
}

fn lookup_merge_relation(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Result<BoundRelation, ParseError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'r' | 'p' | 'v') => Ok(entry),
        Some(entry) if entry.relkind == 'm' => Err(ParseError::DetailedError {
            message: format!("cannot execute MERGE on relation \"{name}\""),
            detail: Some("This operation is not supported for materialized views.".into()),
            hint: None,
            sqlstate: "42809",
        }),
        Some(_) => Err(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table or view",
        }),
        None => Err(ParseError::UnknownTable(name.to_string())),
    }
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
        return ordered_partition_leaf_oids(catalog, entry.relation_oid);
    }
    if only {
        vec![entry.relation_oid]
    } else {
        catalog.find_all_inheritors(entry.relation_oid)
    }
}

fn ordered_partition_leaf_oids(catalog: &dyn CatalogLookup, relation_oid: u32) -> Vec<u32> {
    let mut children = catalog
        .inheritance_children(relation_oid)
        .into_iter()
        .filter(|row| !row.inhdetachpending)
        .filter_map(|row| catalog.relation_by_oid(row.inhrelid))
        .collect::<Vec<_>>();
    children.sort_by(|left, right| {
        partition_relation_order_key(left)
            .cmp(&partition_relation_order_key(right))
            .then_with(|| left.relation_oid.cmp(&right.relation_oid))
    });

    let mut leaves = Vec::new();
    for child in children {
        if child.relkind == 'p' {
            leaves.extend(ordered_partition_leaf_oids(catalog, child.relation_oid));
        } else if child.relkind == 'r' {
            leaves.push(child.relation_oid);
        }
    }
    leaves
}

fn partition_relation_order_key(relation: &BoundRelation) -> (bool, String) {
    let Some(bound_text) = relation.relpartbound.as_deref() else {
        return (false, format!("rel:{:020}", relation.relation_oid));
    };
    match deserialize_partition_bound(bound_text) {
        Ok(bound) => (bound.is_default(), partition_bound_order_key(&bound)),
        Err(_) => (
            partition_bound_text_is_default(bound_text),
            bound_text.to_string(),
        ),
    }
}

fn partition_bound_text_is_default(bound: &str) -> bool {
    let lower_bound = bound.to_ascii_lowercase();
    lower_bound.contains("\"is_default\":true")
        || lower_bound.contains("\"is_default\": true")
        || lower_bound.contains("default")
}

fn partition_bound_order_key(bound: &PartitionBoundSpec) -> String {
    match bound {
        PartitionBoundSpec::List { values, .. } => {
            let mut keys = values
                .iter()
                .map(serialized_partition_value_order_key)
                .collect::<Vec<_>>();
            keys.sort();
            format!("list:{}", keys.join(","))
        }
        PartitionBoundSpec::Range { from, to, .. } => {
            let from = from
                .iter()
                .map(partition_range_value_order_key)
                .collect::<Vec<_>>()
                .join(",");
            let to = to
                .iter()
                .map(partition_range_value_order_key)
                .collect::<Vec<_>>()
                .join(",");
            format!("range:{from}:{to}")
        }
        PartitionBoundSpec::Hash { modulus, remainder } => {
            format!("hash:{modulus:020}:{remainder:020}")
        }
    }
}

fn partition_range_value_order_key(value: &PartitionRangeDatumValue) -> String {
    match value {
        PartitionRangeDatumValue::MinValue => "0:min".into(),
        PartitionRangeDatumValue::Value(value) => {
            format!("1:{}", serialized_partition_value_order_key(value))
        }
        PartitionRangeDatumValue::MaxValue => "2:max".into(),
    }
}

fn serialized_partition_value_order_key(value: &SerializedPartitionValue) -> String {
    match value {
        SerializedPartitionValue::Null => "00:null".into(),
        SerializedPartitionValue::Bool(value) => format!("01:{}", u8::from(*value)),
        SerializedPartitionValue::Int16(value) => signed_i128_order_key(*value as i128),
        SerializedPartitionValue::Int32(value) => signed_i128_order_key(*value as i128),
        SerializedPartitionValue::Int64(value) => signed_i128_order_key(*value as i128),
        SerializedPartitionValue::Money(value) => signed_i128_order_key(*value as i128),
        SerializedPartitionValue::Numeric(value) => numeric_text_order_key(value),
        SerializedPartitionValue::Text(value)
        | SerializedPartitionValue::Json(value)
        | SerializedPartitionValue::JsonPath(value)
        | SerializedPartitionValue::Xml(value)
        | SerializedPartitionValue::Float64(value) => format!("20:{value}"),
        SerializedPartitionValue::Date(value) => signed_i128_order_key(*value as i128),
        SerializedPartitionValue::Time(value)
        | SerializedPartitionValue::Timestamp(value)
        | SerializedPartitionValue::TimestampTz(value) => signed_i128_order_key(*value as i128),
        SerializedPartitionValue::TimeTz {
            time,
            offset_seconds,
        } => format!(
            "12:{}:{}",
            signed_i128_order_key(*time as i128),
            signed_i128_order_key(*offset_seconds as i128)
        ),
        SerializedPartitionValue::InternalChar(value) => format!("13:{value:03}"),
        SerializedPartitionValue::EnumOid(value) => format!("14:{value:020}"),
        SerializedPartitionValue::Bytea(value) | SerializedPartitionValue::Jsonb(value) => {
            format!("21:{value:?}")
        }
        SerializedPartitionValue::Array(_)
        | SerializedPartitionValue::Record(_)
        | SerializedPartitionValue::Range(_)
        | SerializedPartitionValue::Multirange(_) => format!("99:{value:?}"),
    }
}

fn signed_i128_order_key(value: i128) -> String {
    if value < 0 {
        format!("10:0:{:039}", i128::MAX + value)
    } else {
        format!("10:1:{value:039}")
    }
}

fn numeric_text_order_key(value: &str) -> String {
    let trimmed = value.trim();
    let (sign, unsigned) = trimmed
        .strip_prefix('-')
        .map(|rest| ("0", rest))
        .unwrap_or(("1", trimmed.strip_prefix('+').unwrap_or(trimmed)));
    let integer_len = unsigned
        .split_once('.')
        .map(|(integer, _)| integer.len())
        .unwrap_or(unsigned.len());
    format!("11:{sign}:{integer_len:020}:{unsigned}")
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
    allow_partition_routing: bool,
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
            display_exprs: check
                .display_exprs
                .iter()
                .cloned()
                .map(|expr| rewrite_local_vars_for_output_exprs(expr, 1, &translation_exprs))
                .collect(),
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
        allow_partition_routing,
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
        parent_desc: partition_update_root_oid.map(|_| parent_desc.clone()),
        parent_rls_write_checks: if partition_update_root_oid.is_some() {
            parent_rls_write_checks.to_vec()
        } else {
            Vec::new()
        },
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
    allow_partition_routing: bool,
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
            display_exprs: check
                .display_exprs
                .iter()
                .cloned()
                .map(|expr| rewrite_local_vars_for_output_exprs(expr, 1, &parent_visible_exprs))
                .collect(),
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
        allow_partition_routing,
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
        parent_desc: partition_update_root_oid.map(|_| parent_desc.clone()),
        parent_rls_write_checks: if partition_update_root_oid.is_some() {
            parent_rls_write_checks.to_vec()
        } else {
            Vec::new()
        },
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

fn reject_duplicate_whole_column_assignments(
    desc: &RelationDesc,
    assignments: &[BoundAssignment],
) -> Result<(), ParseError> {
    let mut seen = std::collections::BTreeSet::new();
    for assignment in assignments {
        if !assignment.subscripts.is_empty()
            || !assignment.field_path.is_empty()
            || !assignment.indirection.is_empty()
        {
            continue;
        }
        if !seen.insert(assignment.column_index) {
            let column_name = desc
                .columns
                .get(assignment.column_index)
                .map(|column| column.name.clone())
                .unwrap_or_else(|| "<unknown>".into());
            return Err(ParseError::UnexpectedToken {
                expected: "single assignment per column",
                actual: format!("multiple assignments to same column \"{}\"", column_name),
            });
        }
    }
    Ok(())
}

fn rewrite_auto_view_returning_targets(
    targets: Vec<TargetEntry>,
    local_output_exprs: &[Expr],
    view_output_exprs: &[Expr],
    base_desc: &RelationDesc,
) -> Vec<TargetEntry> {
    let old_view_output_exprs =
        view_returning_pseudo_output_exprs(view_output_exprs, base_desc, OUTER_VAR);
    let new_view_output_exprs =
        view_returning_pseudo_output_exprs(view_output_exprs, base_desc, INNER_VAR);
    targets
        .into_iter()
        .map(|target| TargetEntry {
            expr: rewrite_local_vars_for_output_exprs(
                rewrite_local_vars_for_output_exprs(
                    rewrite_local_vars_for_output_exprs(target.expr, 1, local_output_exprs),
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

fn update_auto_view_input_output_exprs(
    stmt: &BoundUpdateStatement,
    view_output_exprs: &[Expr],
) -> Vec<Expr> {
    let input_columns = stmt
        .input_plan
        .as_ref()
        .map(|plan| plan.columns())
        .unwrap_or_default();
    let width = stmt
        .visible_column_count
        .max(stmt.target_ctid_index.saturating_add(1))
        .max(stmt.target_tableoid_index.saturating_add(1));
    (0..width)
        .map(|index| {
            if index < stmt.target_visible_count
                && let Some(expr) = view_output_exprs.get(index)
            {
                expr.clone()
            } else {
                update_input_identity_expr(index, &input_columns)
            }
        })
        .collect()
}

fn update_input_identity_expr(index: usize, input_columns: &[QueryColumn]) -> Expr {
    Expr::Var(Var {
        varno: 1,
        varattno: user_attrno(index),
        varlevelsup: 0,
        vartype: input_columns
            .get(index)
            .map(|column| column.sql_type)
            .unwrap_or_else(|| SqlType::new(SqlTypeKind::AnyElement)),
        collation_oid: None,
    })
}

fn rewrite_auto_view_update_input_plan(
    input_plan: Option<PlannedStmt>,
    view_relation_oid: u32,
    view_desc: &RelationDesc,
    base_relation_name: &str,
    resolved: &crate::backend::rewrite::ResolvedAutoViewTarget,
) -> Option<PlannedStmt> {
    input_plan.map(|mut planned| {
        planned.plan_tree = rewrite_auto_view_scan_plan(
            planned.plan_tree,
            view_relation_oid,
            view_desc,
            base_relation_name,
            resolved,
        );
        planned
    })
}

fn rewrite_auto_view_scan_plan(
    plan: Plan,
    view_relation_oid: u32,
    view_desc: &RelationDesc,
    base_relation_name: &str,
    resolved: &crate::backend::rewrite::ResolvedAutoViewTarget,
) -> Plan {
    match plan {
        Plan::SeqScan {
            plan_info,
            source_id,
            relation_oid,
            disabled,
            ..
        } if relation_oid == view_relation_oid => {
            let base_scan = Plan::SeqScan {
                plan_info,
                source_id,
                rel: resolved.base_relation.rel,
                relation_name: base_relation_name.to_string(),
                relation_oid: resolved.base_relation.relation_oid,
                relkind: resolved.base_relation.relkind,
                relispopulated: resolved.base_relation.relispopulated,
                toast: resolved.base_relation.toast,
                tablesample: None,
                desc: resolved.base_relation.desc.clone(),
                disabled,
            };
            if view_output_is_base_identity(
                &resolved.visible_output_exprs,
                &resolved.base_relation.desc,
            ) {
                base_scan
            } else {
                Plan::Projection {
                    plan_info,
                    input: Box::new(base_scan),
                    targets: view_projection_targets(view_desc, &resolved.visible_output_exprs),
                }
            }
        }
        Plan::Append {
            plan_info,
            source_id,
            desc,
            partition_prune,
            children,
        } => Plan::Append {
            plan_info,
            source_id,
            desc,
            partition_prune,
            children: children
                .into_iter()
                .map(|child| {
                    rewrite_auto_view_scan_plan(
                        child,
                        view_relation_oid,
                        view_desc,
                        base_relation_name,
                        resolved,
                    )
                })
                .collect(),
        },
        Plan::MergeAppend {
            plan_info,
            source_id,
            desc,
            items,
            partition_prune,
            children,
        } => Plan::MergeAppend {
            plan_info,
            source_id,
            desc,
            items,
            partition_prune,
            children: children
                .into_iter()
                .map(|child| {
                    rewrite_auto_view_scan_plan(
                        child,
                        view_relation_oid,
                        view_desc,
                        base_relation_name,
                        resolved,
                    )
                })
                .collect(),
        },
        Plan::Unique {
            plan_info,
            key_indices,
            input,
        } => Plan::Unique {
            plan_info,
            key_indices,
            input: Box::new(rewrite_auto_view_scan_plan(
                *input,
                view_relation_oid,
                view_desc,
                base_relation_name,
                resolved,
            )),
        },
        Plan::Hash {
            plan_info,
            input,
            hash_keys,
        } => Plan::Hash {
            plan_info,
            input: Box::new(rewrite_auto_view_scan_plan(
                *input,
                view_relation_oid,
                view_desc,
                base_relation_name,
                resolved,
            )),
            hash_keys,
        },
        Plan::NestedLoopJoin {
            plan_info,
            left,
            right,
            kind,
            nest_params,
            join_qual,
            qual,
        } => Plan::NestedLoopJoin {
            plan_info,
            left: Box::new(rewrite_auto_view_scan_plan(
                *left,
                view_relation_oid,
                view_desc,
                base_relation_name,
                resolved,
            )),
            right: Box::new(rewrite_auto_view_scan_plan(
                *right,
                view_relation_oid,
                view_desc,
                base_relation_name,
                resolved,
            )),
            kind,
            nest_params,
            join_qual,
            qual,
        },
        Plan::HashJoin {
            plan_info,
            left,
            right,
            kind,
            hash_clauses,
            hash_keys,
            join_qual,
            qual,
        } => Plan::HashJoin {
            plan_info,
            left: Box::new(rewrite_auto_view_scan_plan(
                *left,
                view_relation_oid,
                view_desc,
                base_relation_name,
                resolved,
            )),
            right: Box::new(rewrite_auto_view_scan_plan(
                *right,
                view_relation_oid,
                view_desc,
                base_relation_name,
                resolved,
            )),
            kind,
            hash_clauses,
            hash_keys,
            join_qual,
            qual,
        },
        Plan::MergeJoin {
            plan_info,
            left,
            right,
            kind,
            merge_clauses,
            outer_merge_keys,
            inner_merge_keys,
            merge_key_descending,
            join_qual,
            qual,
        } => Plan::MergeJoin {
            plan_info,
            left: Box::new(rewrite_auto_view_scan_plan(
                *left,
                view_relation_oid,
                view_desc,
                base_relation_name,
                resolved,
            )),
            right: Box::new(rewrite_auto_view_scan_plan(
                *right,
                view_relation_oid,
                view_desc,
                base_relation_name,
                resolved,
            )),
            kind,
            merge_clauses,
            outer_merge_keys,
            inner_merge_keys,
            merge_key_descending,
            join_qual,
            qual,
        },
        Plan::Filter {
            plan_info,
            input,
            predicate,
        } => Plan::Filter {
            plan_info,
            input: Box::new(rewrite_auto_view_scan_plan(
                *input,
                view_relation_oid,
                view_desc,
                base_relation_name,
                resolved,
            )),
            predicate,
        },
        Plan::OrderBy {
            plan_info,
            input,
            items,
            display_items,
        } => Plan::OrderBy {
            plan_info,
            input: Box::new(rewrite_auto_view_scan_plan(
                *input,
                view_relation_oid,
                view_desc,
                base_relation_name,
                resolved,
            )),
            items,
            display_items,
        },
        Plan::IncrementalSort {
            plan_info,
            input,
            items,
            presorted_count,
            display_items,
            presorted_display_items,
        } => Plan::IncrementalSort {
            plan_info,
            input: Box::new(rewrite_auto_view_scan_plan(
                *input,
                view_relation_oid,
                view_desc,
                base_relation_name,
                resolved,
            )),
            items,
            presorted_count,
            display_items,
            presorted_display_items,
        },
        Plan::Limit {
            plan_info,
            input,
            limit,
            offset,
        } => Plan::Limit {
            plan_info,
            input: Box::new(rewrite_auto_view_scan_plan(
                *input,
                view_relation_oid,
                view_desc,
                base_relation_name,
                resolved,
            )),
            limit,
            offset,
        },
        Plan::LockRows {
            plan_info,
            input,
            row_marks,
        } => Plan::LockRows {
            plan_info,
            input: Box::new(rewrite_auto_view_scan_plan(
                *input,
                view_relation_oid,
                view_desc,
                base_relation_name,
                resolved,
            )),
            row_marks,
        },
        Plan::Projection {
            plan_info,
            input,
            targets,
        } => Plan::Projection {
            plan_info,
            input: Box::new(rewrite_auto_view_scan_plan(
                *input,
                view_relation_oid,
                view_desc,
                base_relation_name,
                resolved,
            )),
            targets,
        },
        Plan::Aggregate {
            plan_info,
            strategy,
            phase,
            disabled,
            input,
            group_by,
            group_by_refs,
            grouping_sets,
            passthrough_exprs,
            accumulators,
            semantic_accumulators,
            semantic_output_names,
            having,
            output_columns,
        } => Plan::Aggregate {
            plan_info,
            strategy,
            phase,
            disabled,
            input: Box::new(rewrite_auto_view_scan_plan(
                *input,
                view_relation_oid,
                view_desc,
                base_relation_name,
                resolved,
            )),
            group_by,
            group_by_refs,
            grouping_sets,
            passthrough_exprs,
            accumulators,
            semantic_accumulators,
            semantic_output_names,
            having,
            output_columns,
        },
        Plan::WindowAgg {
            plan_info,
            input,
            clause,
            run_condition,
            top_qual,
            output_columns,
        } => Plan::WindowAgg {
            plan_info,
            input: Box::new(rewrite_auto_view_scan_plan(
                *input,
                view_relation_oid,
                view_desc,
                base_relation_name,
                resolved,
            )),
            clause,
            run_condition,
            top_qual,
            output_columns,
        },
        Plan::SubqueryScan {
            plan_info,
            input,
            scan_name,
            filter,
            output_columns,
        } => Plan::SubqueryScan {
            plan_info,
            input: Box::new(rewrite_auto_view_scan_plan(
                *input,
                view_relation_oid,
                view_desc,
                base_relation_name,
                resolved,
            )),
            scan_name,
            filter,
            output_columns,
        },
        Plan::ProjectSet {
            plan_info,
            input,
            targets,
        } => Plan::ProjectSet {
            plan_info,
            input: Box::new(rewrite_auto_view_scan_plan(
                *input,
                view_relation_oid,
                view_desc,
                base_relation_name,
                resolved,
            )),
            targets,
        },
        other => other,
    }
}

fn view_output_is_base_identity(output_exprs: &[Expr], base_desc: &RelationDesc) -> bool {
    output_exprs.len() == base_desc.columns.len()
        && output_exprs.iter().enumerate().all(|(index, expr)| {
            matches!(
                expr,
                Expr::Var(var)
                    if var.varno == 1
                        && var.varlevelsup == 0
                        && var.varattno == user_attrno(index)
            )
        })
}

fn view_projection_targets(view_desc: &RelationDesc, output_exprs: &[Expr]) -> Vec<TargetEntry> {
    view_desc
        .columns
        .iter()
        .zip(output_exprs.iter())
        .enumerate()
        .map(|(index, (column, expr))| {
            TargetEntry::new(
                column.name.clone(),
                expr.clone(),
                column.sql_type,
                index + 1,
            )
            .with_input_resno(index + 1)
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
    _base_relation_name: &str,
    view_desc: &RelationDesc,
    resolved: &crate::backend::rewrite::ResolvedAutoViewTarget,
    catalog: &dyn CatalogLookup,
) -> Result<BoundOnConflictClause, ViewDmlRewriteError> {
    let arbiters = super::on_conflict::resolve_arbiters(
        clause,
        view_relation_name,
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
            if !arbiters.temporal_constraints.is_empty()
                || !arbiters.exclusion_constraints.is_empty()
            {
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
                conflict_visibility_checks: Vec::new(),
                update_write_checks: Vec::new(),
            }
        }
    };
    Ok(BoundOnConflictClause {
        arbiter_indexes: arbiters.indexes,
        arbiter_exclusion_constraints: arbiters.exclusion_constraints,
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
            let column_index = map_auto_view_column_index(
                &stmt.desc,
                &resolved.updatable_column_map,
                &resolved.non_updatable_column_reasons,
                target.column_index,
            )?;
            Ok(BoundAssignmentTarget {
                column_index,
                subscripts: rewrite_assignment_subscripts(
                    &target.subscripts,
                    &resolved.visible_output_exprs,
                ),
                field_path: target.field_path.clone(),
                indirection: rewrite_assignment_indirection(
                    &target.indirection,
                    &resolved.visible_output_exprs,
                ),
                target_sql_type: resolved.base_relation.desc.columns[column_index].sql_type,
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
            stmt.target_alias.as_deref().unwrap_or(&stmt.relation_name),
            &relation_name,
            &stmt.desc,
            &resolved,
            catalog,
        )?),
        None => stmt.on_conflict,
    };
    let base_rls = build_auto_view_base_row_security(
        &relation_name,
        &resolved,
        PolicyCommand::Insert,
        false,
        !stmt.returning.is_empty(),
        catalog,
    )?;
    let base_column_defaults =
        bind_insert_column_defaults(&resolved.base_relation.desc, catalog, &[])
            .map_err(|err| ViewDmlRewriteError::UnsupportedViewShape(err.to_string()))?;
    let source = match stmt.source {
        BoundInsertSource::DefaultValues(_) => BoundInsertSource::DefaultValues(
            target_columns
                .iter()
                .map(|target| base_column_defaults[target.column_index].clone())
                .collect(),
        ),
        other => other,
    };

    Ok(BoundInsertStatement {
        relation_name: relation_name.clone(),
        target_alias: stmt.target_alias.clone(),
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
        column_defaults: base_column_defaults,
        target_columns,
        overriding: stmt.overriding,
        source,
        on_conflict,
        raw_on_conflict: None,
        returning: rewrite_auto_view_returning_targets(
            stmt.returning,
            &resolved.visible_output_exprs,
            &resolved.visible_output_exprs,
            &resolved.base_relation.desc,
        ),
        rls_write_checks: base_rls
            .write_checks
            .into_iter()
            .chain(stmt.rls_write_checks)
            .chain(
                resolved
                    .view_check_options
                    .iter()
                    .cloned()
                    .map(|check| RlsWriteCheck {
                        expr: check.expr,
                        display_exprs: Vec::new(),
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

struct AutoViewInsertContext {
    base_desc: RelationDesc,
    base_column_defaults: Vec<Expr>,
    updatable_column_map: Vec<Option<usize>>,
}

fn build_auto_view_insert_context(
    entry: &BoundRelation,
    catalog: &dyn CatalogLookup,
    local_ctes: &[BoundCte],
) -> Result<Option<AutoViewInsertContext>, ParseError> {
    if entry.relkind != 'v' {
        return Ok(None);
    }
    let resolved = match resolve_auto_updatable_view_target(
        entry.relation_oid,
        &entry.desc,
        ViewDmlEvent::Insert,
        catalog,
        &[],
    ) {
        Ok(resolved) => resolved,
        Err(_) => return Ok(None),
    };
    let base_column_defaults =
        bind_insert_column_defaults(&resolved.base_relation.desc, catalog, local_ctes)?;
    Ok(Some(AutoViewInsertContext {
        base_desc: resolved.base_relation.desc,
        base_column_defaults,
        updatable_column_map: resolved.updatable_column_map,
    }))
}

fn auto_view_base_target(
    ctx: &AutoViewInsertContext,
    target: &BoundAssignmentTarget,
) -> Option<BoundAssignmentTarget> {
    let column_index = ctx
        .updatable_column_map
        .get(target.column_index)
        .copied()
        .flatten()?;
    Some(BoundAssignmentTarget {
        column_index,
        subscripts: target.subscripts.clone(),
        field_path: target.field_path.clone(),
        indirection: target.indirection.clone(),
        target_sql_type: ctx.base_desc.columns[column_index].sql_type,
    })
}

fn ensure_auto_view_insert_generated_assignment_allowed(
    ctx: Option<&AutoViewInsertContext>,
    target: &BoundAssignmentTarget,
    expr: Option<&SqlExpr>,
) -> Result<(), ParseError> {
    let Some(ctx) = ctx else {
        return Ok(());
    };
    let Some(base_target) = auto_view_base_target(ctx, target) else {
        return Ok(());
    };
    ensure_generated_assignment_allowed(&ctx.base_desc, &base_target, expr)
}

fn insert_default_expr_for_target(
    column_defaults: &[Expr],
    auto_view_ctx: Option<&AutoViewInsertContext>,
    target: &BoundAssignmentTarget,
) -> Expr {
    auto_view_ctx
        .and_then(|ctx| {
            auto_view_base_target(ctx, target)
                .map(|base_target| ctx.base_column_defaults[base_target.column_index].clone())
        })
        .unwrap_or_else(|| column_defaults[target.column_index].clone())
}

fn normalize_auto_view_identity_values(
    view_oid: u32,
    view_desc: &RelationDesc,
    target_columns: &[BoundAssignmentTarget],
    overriding: Option<OverridingKind>,
    source: BoundInsertSource,
    catalog: &dyn CatalogLookup,
) -> Result<BoundInsertSource, ParseError> {
    let resolved = match resolve_auto_updatable_view_target(
        view_oid,
        view_desc,
        ViewDmlEvent::Insert,
        catalog,
        &[],
    ) {
        Ok(resolved) => resolved,
        Err(_) => return Ok(source),
    };
    let base_column_defaults =
        bind_insert_column_defaults(&resolved.base_relation.desc, catalog, &[])?;
    let mut base_indexes = Vec::with_capacity(target_columns.len());
    for target in target_columns {
        let Ok(base_index) = map_auto_view_column_index(
            view_desc,
            &resolved.updatable_column_map,
            &resolved.non_updatable_column_reasons,
            target.column_index,
        ) else {
            return Ok(source);
        };
        base_indexes.push(base_index);
    }

    let normalize_row = |mut row: Vec<Expr>| -> Result<Vec<Expr>, ParseError> {
        for (expr, base_index) in row.iter_mut().zip(base_indexes.iter().copied()) {
            let column = &resolved.base_relation.desc.columns[base_index];
            let Some(identity) = column.identity else {
                continue;
            };
            if matches!(overriding, Some(OverridingKind::User)) {
                *expr = base_column_defaults[base_index].clone();
            } else if identity == ColumnIdentityKind::Always
                && !matches!(overriding, Some(OverridingKind::System))
            {
                return Err(identity_insert_error(&column.name));
            }
        }
        Ok(row)
    };

    match source {
        BoundInsertSource::Values(rows) => Ok(BoundInsertSource::Values(
            rows.into_iter()
                .map(normalize_row)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        BoundInsertSource::ProjectSetValues(rows) => Ok(BoundInsertSource::ProjectSetValues(
            rows.into_iter()
                .map(normalize_row)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        other => Ok(other),
    }
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
    let input_output_exprs =
        update_auto_view_input_output_exprs(&stmt, &resolved.visible_output_exprs);
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
                    &input_output_exprs,
                ),
                field_path: assignment.field_path.clone(),
                indirection: rewrite_assignment_indirection(
                    &assignment.indirection,
                    &input_output_exprs,
                ),
                target_sql_type: assignment.target_sql_type,
                expr: rewrite_local_vars_for_output_exprs(
                    assignment.expr.clone(),
                    1,
                    &input_output_exprs,
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
    let base_rls = build_auto_view_base_row_security(
        &relation_name,
        &resolved,
        PolicyCommand::Update,
        true,
        !stmt.returning.is_empty(),
        catalog,
    )?;
    let predicate = and_predicates(
        resolved.combined_predicate.clone(),
        target
            .predicate
            .as_ref()
            .map(|expr| rewrite_local_vars_for_output_exprs(expr.clone(), 1, &input_output_exprs)),
    );
    let predicate = prepend_visibility_quals(base_rls.visibility_quals.clone(), predicate);
    let rls_write_checks = base_rls
        .write_checks
        .into_iter()
        .chain(target.rls_write_checks.clone())
        .collect::<Vec<_>>();

    let targets = auto_view_base_children(&resolved, catalog)?
        .into_iter()
        .map(|child| {
            let allow_partition_routing = resolved.base_relation.relkind == 'p';
            let partition_update_root_oid =
                allow_partition_routing.then_some(resolved.base_relation.relation_oid);
            if stmt.input_plan.is_some() {
                build_update_target_from_joined_input(
                    &relation_name,
                    &resolved.base_relation.desc,
                    &assignments,
                    predicate.as_ref(),
                    &rls_write_checks,
                    partition_update_root_oid,
                    allow_partition_routing,
                    &child,
                    catalog,
                )
            } else {
                build_update_target(
                    &relation_name,
                    &resolved.base_relation.desc,
                    &assignments,
                    predicate.as_ref(),
                    &rls_write_checks,
                    partition_update_root_oid,
                    allow_partition_routing,
                    &child,
                    catalog,
                )
            }
            .map_err(|err| ViewDmlRewriteError::UnsupportedViewShape(err.to_string()))
        })
        .collect::<Result<Vec<_>, ViewDmlRewriteError>>()?;

    let targets =
        targets
            .into_iter()
            .map(|mut target| {
                let parent_visible_exprs = target.parent_visible_exprs.clone();
                target
                    .rls_write_checks
                    .extend(resolved.view_check_options.iter().cloned().map(|check| {
                        RlsWriteCheck {
                            expr: rewrite_local_vars_for_output_exprs(
                                check.expr,
                                1,
                                &parent_visible_exprs,
                            ),
                            display_exprs: parent_visible_exprs.clone(),
                            policy_name: None,
                            source: crate::backend::rewrite::RlsWriteCheckSource::ViewCheckOption(
                                check.view_name,
                            ),
                        }
                    }));
                target.parent_rls_write_checks.extend(
                    resolved
                        .view_check_options
                        .iter()
                        .cloned()
                        .map(|check| RlsWriteCheck {
                            expr: check.expr,
                            display_exprs: Vec::new(),
                            policy_name: None,
                            source: crate::backend::rewrite::RlsWriteCheckSource::ViewCheckOption(
                                check.view_name,
                            ),
                        }),
                );
                target
            })
            .collect();

    Ok(BoundUpdateStatement {
        targets,
        target_relation_name: relation_name.clone(),
        explain_target_name: relation_name.clone(),
        returning: rewrite_auto_view_returning_targets(
            stmt.returning,
            &input_output_exprs,
            &resolved.visible_output_exprs,
            &resolved.base_relation.desc,
        ),
        input_plan: rewrite_auto_view_update_input_plan(
            stmt.input_plan,
            target.relation_oid,
            &target.desc,
            &relation_name,
            &resolved,
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
    let base_rls = build_auto_view_base_row_security(
        &relation_name,
        &resolved,
        PolicyCommand::Delete,
        true,
        false,
        catalog,
    )?;
    let predicate = and_predicates(
        resolved.combined_predicate.clone(),
        target.predicate.as_ref().map(|expr| {
            rewrite_local_vars_for_output_exprs(expr.clone(), 1, &resolved.visible_output_exprs)
        }),
    );
    let predicate = prepend_visibility_quals(base_rls.visibility_quals.clone(), predicate);

    let targets = auto_view_base_children(&resolved, catalog)?
        .into_iter()
        .map(|child| {
            build_delete_target(
                &relation_name,
                &resolved.base_relation.desc,
                predicate.as_ref(),
                (resolved.base_relation.relkind == 'p')
                    .then_some(resolved.base_relation.relation_oid),
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
            &resolved.visible_output_exprs,
            &resolved.base_relation.desc,
        ),
        input_plan: stmt.input_plan,
        target_visible_count: stmt.target_visible_count,
        visible_column_count: stmt.visible_column_count,
        target_ctid_index: stmt.target_ctid_index,
        target_tableoid_index: stmt.target_tableoid_index,
        required_privileges,
        subplans: stmt.subplans,
        current_of: stmt.current_of,
    })
}

fn auto_view_base_children(
    resolved: &crate::backend::rewrite::ResolvedAutoViewTarget,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<BoundRelation>, ViewDmlRewriteError> {
    let relation_oids = if resolved.base_relation.relkind == 'p' {
        catalog
            .find_all_inheritors(resolved.base_relation.relation_oid)
            .into_iter()
            .filter(|oid| {
                catalog
                    .relation_by_oid(*oid)
                    .is_some_and(|relation| relation.relkind == 'r')
            })
            .collect()
    } else if resolved.base_inh {
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
    partition_delete_root_oid: Option<u32>,
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
        partition_delete_root_oid,
        relpersistence: child.relpersistence,
        toast: child.toast,
        desc: child.desc.clone(),
        referenced_by_foreign_keys: bind_referenced_by_foreign_keys(
            child.relation_oid,
            &child.desc,
            catalog,
        )?,
        row_source: choose_modify_row_source(predicate.as_ref(), &indexes),
        parent_visible_exprs: translation_exprs,
        predicate,
    })
}

fn build_delete_target_from_joined_input(
    base_relation_name: &str,
    parent_desc: &RelationDesc,
    parent_predicate: Option<&Expr>,
    partition_delete_root_oid: Option<u32>,
    child: &BoundRelation,
    catalog: &dyn CatalogLookup,
) -> Result<BoundDeleteTarget, ParseError> {
    let relation_name = relation_display_name(catalog, child.relation_oid, base_relation_name);
    let translation_exprs = inheritance_translation_exprs(
        &child.desc,
        &inheritance_translation_indexes(parent_desc, &child.desc),
        catalog,
    )?;

    Ok(BoundDeleteTarget {
        relation_name,
        rel: child.rel,
        relation_oid: child.relation_oid,
        relkind: child.relkind,
        partition_delete_root_oid,
        relpersistence: child.relpersistence,
        toast: child.toast,
        desc: child.desc.clone(),
        referenced_by_foreign_keys: bind_referenced_by_foreign_keys(
            child.relation_oid,
            &child.desc,
            catalog,
        )?,
        row_source: BoundModifyRowSource::Heap,
        parent_visible_exprs: translation_exprs,
        predicate: parent_predicate.cloned(),
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
                    if column.identity.is_some() {
                        BuiltinScalarFunction::IdentityNextVal
                    } else {
                        BuiltinScalarFunction::NextVal
                    },
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
            let default_expr = column.default_expr.clone().or_else(|| {
                catalog
                    .type_oid_for_sql_type(column.sql_type)
                    .and_then(|type_oid| catalog.type_default_sql(type_oid))
            });
            default_expr
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
    desc: &RelationDesc,
    target: &BoundAssignmentTarget,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    local_ctes: &[BoundCte],
) -> Result<Expr, ParseError> {
    let expr_type =
        infer_sql_expr_type_with_ctes(expr, scope, catalog, outer_scopes, None, local_ctes);
    reject_invalid_domain_array_assignment(desc, target, expr_type, catalog)?;
    if let SqlExpr::ArrayLiteral(elements) = expr {
        let target_type = assignment_navigation_sql_type(target.target_sql_type, catalog);
        let target_is_domain_array = target.target_sql_type.is_array
            && catalog
                .domain_by_type_oid(target.target_sql_type.type_oid)
                .is_some();
        if target_type.is_array && !target_is_domain_array {
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

fn whole_row_star_relation_name(expr: &SqlExpr) -> Option<&str> {
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

enum InsertValuesCell<'a> {
    Raw(&'a SqlExpr),
    Bound(Expr),
}

fn expand_insert_values_row_exprs<'a>(
    row: &'a [SqlExpr],
    scope: &BoundScope,
    outer_scopes: &[BoundScope],
) -> Result<Vec<InsertValuesCell<'a>>, ParseError> {
    let mut expanded = Vec::new();
    for expr in row {
        if let Some(name) = whole_row_star_relation_name(expr) {
            let fields = resolve_relation_row_expr_with_outer(scope, outer_scopes, name)
                .ok_or_else(|| ParseError::UnknownColumn(format!("{name}.*")))?;
            expanded.extend(
                fields
                    .into_iter()
                    .map(|(_, expr)| InsertValuesCell::Bound(expr)),
            );
        } else {
            expanded.push(InsertValuesCell::Raw(expr));
        }
    }
    Ok(expanded)
}

fn reject_invalid_domain_array_assignment(
    desc: &RelationDesc,
    target: &BoundAssignmentTarget,
    source_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    if source_type == target.target_sql_type {
        return Ok(());
    }
    if !is_array_of_domain_over_array_type(target.target_sql_type, catalog) {
        return Ok(());
    }
    if !source_type.is_array {
        return Ok(());
    }
    let column = &desc.columns[target.column_index];
    Err(ParseError::DetailedError {
        message: format!(
            "column \"{}\" is of type {} but expression is of type {}",
            column.name,
            sql_type_name_with_domains(target.target_sql_type, catalog),
            sql_type_name_with_domains(source_type, catalog)
        ),
        detail: None,
        hint: Some("You will need to rewrite or cast the expression.".into()),
        sqlstate: "42804",
    })
}

fn sql_type_name_with_domains(sql_type: SqlType, catalog: &dyn CatalogLookup) -> String {
    if sql_type.type_oid != 0
        && let Some(domain) = catalog.domain_by_type_oid(sql_type.type_oid)
    {
        return if sql_type.is_array
            && (!domain.sql_type.is_array || sql_type.typrelid == domain.array_oid)
        {
            format!("{}[]", domain.name)
        } else {
            domain.name
        };
    }
    sql_type_name(sql_type)
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
    let column_defaults = bind_insert_column_defaults(&entry.desc, catalog, &visible_ctes)?;
    let target_rls = build_target_relation_row_security(
        &stmt.table_name,
        entry.relation_oid,
        &entry.desc,
        PolicyCommand::Insert,
        false,
        !stmt.returning.is_empty(),
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
    let mut returning_scope = scope_with_returning_pseudo_rows_with_generated(
        target_scope.clone(),
        &entry.desc,
        catalog,
        Some(entry.relation_oid),
    )?;
    if stmt.on_conflict.is_some() {
        returning_scope =
            scope_with_hidden_invalid_relation(returning_scope, "excluded", &entry.desc);
    }
    let auto_view_insert_context = build_auto_view_insert_context(&entry, catalog, &visible_ctes)?;
    let returning = bind_returning_targets(
        &stmt.returning,
        &returning_scope,
        catalog,
        outer_scopes,
        &visible_ctes,
    )?;

    let source = match &stmt.source {
        InsertSource::Values(rows) => {
            let expanded_rows = rows
                .iter()
                .map(|row| expand_insert_values_row_exprs(row, &expr_scope, outer_scopes))
                .collect::<Result<Vec<_>, _>>()?;
            let target_columns = if let Some(columns) = &stmt.columns {
                columns
                    .iter()
                    .map(|column| {
                        bind_assignment_target(column, &target_scope, catalog, &visible_ctes)
                    })
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                let visible_targets = visible_assignment_targets(&entry.desc);
                let width = expanded_rows.first().map(Vec::len).unwrap_or(0);
                if width > visible_targets.len() {
                    return Err(ParseError::InvalidInsertTargetCount {
                        expected: visible_targets.len(),
                        actual: width,
                    });
                }
                visible_targets.into_iter().take(width).collect()
            };
            for row in &expanded_rows {
                if target_columns.len() != row.len() {
                    return Err(ParseError::InvalidInsertTargetCount {
                        expected: target_columns.len(),
                        actual: row.len(),
                    });
                }
            }
            let bound_rows = expanded_rows
                .iter()
                .map(|row| {
                    row.iter()
                        .zip(target_columns.iter())
                        .map(|(cell, target)| match cell {
                            InsertValuesCell::Raw(expr) => {
                                ensure_auto_view_insert_generated_assignment_allowed(
                                    auto_view_insert_context.as_ref(),
                                    target,
                                    Some(expr),
                                )?;
                                ensure_generated_assignment_allowed(
                                    &entry.desc,
                                    target,
                                    Some(expr),
                                )?;
                                if matches!(expr, SqlExpr::Default) {
                                    reject_default_indirection_assignment(target)?;
                                    return Ok(insert_default_expr_for_target(
                                        &column_defaults,
                                        auto_view_insert_context.as_ref(),
                                        target,
                                    ));
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
                                    NormalizedInsertExpr::Expr(expr) => {
                                        bind_insert_assignment_expr(
                                            expr,
                                            &entry.desc,
                                            target,
                                            &expr_scope,
                                            catalog,
                                            outer_scopes,
                                            &visible_ctes,
                                        )
                                    }
                                }
                            }
                            InsertValuesCell::Bound(expr) => {
                                ensure_auto_view_insert_generated_assignment_allowed(
                                    auto_view_insert_context.as_ref(),
                                    target,
                                    Some(&SqlExpr::Const(Value::Null)),
                                )?;
                                ensure_generated_assignment_allowed(
                                    &entry.desc,
                                    target,
                                    Some(&SqlExpr::Const(Value::Null)),
                                )?;
                                ensure_identity_select_insert_allowed(
                                    &entry.desc,
                                    target,
                                    stmt.overriding,
                                )?;
                                let source_type = expr_sql_type_hint(expr)
                                    .unwrap_or(SqlType::new(SqlTypeKind::Text));
                                reject_invalid_domain_array_assignment(
                                    &entry.desc,
                                    target,
                                    source_type,
                                    catalog,
                                )?;
                                Ok(coerce_bound_expr(
                                    expr.clone(),
                                    source_type,
                                    target.target_sql_type,
                                ))
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
                ensure_auto_view_insert_generated_assignment_allowed(
                    auto_view_insert_context.as_ref(),
                    target,
                    Some(&SqlExpr::Const(Value::Null)),
                )?;
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
                reject_invalid_domain_array_assignment(
                    &entry.desc,
                    target_column,
                    source_type,
                    catalog,
                )?;
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
    let source = if entry.relkind == 'v' {
        normalize_auto_view_identity_values(
            entry.relation_oid,
            &entry.desc,
            &target_columns,
            stmt.overriding,
            source,
            catalog,
        )?
    } else {
        source
    };
    let mut on_conflict = stmt
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
        .transpose()?;
    if let Some(BoundOnConflictClause {
        action:
            BoundOnConflictAction::Update {
                conflict_visibility_checks,
                update_write_checks,
                ..
            },
        ..
    }) = &mut on_conflict
    {
        let update_rls = build_target_relation_row_security(
            &stmt.table_name,
            entry.relation_oid,
            &entry.desc,
            PolicyCommand::Update,
            false,
            true,
            catalog,
        )?;
        let conflict_quals = update_rls
            .visibility_quals
            .into_iter()
            .filter(|qual| {
                !update_rls.write_checks.iter().any(|check| {
                    matches!(
                        check.source,
                        crate::backend::rewrite::RlsWriteCheckSource::Update
                    ) && check.expr == *qual
                })
            })
            .collect();
        *conflict_visibility_checks = build_conflict_visibility_checks(conflict_quals);
        *update_write_checks = update_rls.write_checks;
    }
    let raw_on_conflict = (entry.relkind == 'v')
        .then(|| stmt.on_conflict.clone())
        .flatten();
    let mut required_privileges = vec![insert_privilege_requirement(
        &entry,
        &stmt.table_name,
        &target_columns,
    )];
    if let Some(
        conflict @ BoundOnConflictClause {
            action:
                BoundOnConflictAction::Update {
                    assignments,
                    predicate,
                    ..
                },
            ..
        },
    ) = &on_conflict
    {
        required_privileges.push(on_conflict_update_privilege_requirement(
            &entry,
            &stmt.table_name,
            conflict,
            assignments,
            predicate.as_ref(),
            &returning,
        ));
    }

    Ok(BoundInsertStatement {
        relation_name: stmt.table_name.clone(),
        target_alias: stmt.table_alias.clone(),
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
        on_conflict,
        raw_on_conflict,
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
    bind_update_with_outer_scopes_and_ctes(stmt, catalog, outer_scopes, &[])
}

pub(crate) fn bind_update_with_outer_scopes_and_ctes(
    stmt: &UpdateStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    outer_ctes: &[BoundCte],
) -> Result<BoundUpdateStatement, ParseError> {
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
    if stmt.from.is_some() {
        return bind_update_from(stmt, catalog, outer_scopes, &visible_ctes, &entry);
    }
    bind_simple_update(stmt, catalog, outer_scopes, &visible_ctes, &entry)
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
    let mut scope = scope_for_base_relation_with_generated(
        &target_relation_name,
        &entry.desc,
        Some(entry.relation_oid),
        catalog,
    )?;
    if stmt.target_alias.is_some() {
        scope = mark_scope_hidden_invalid_relation(scope, &stmt.table_name);
    }
    let returning_scope = scope_with_returning_pseudo_rows_with_generated(
        scope.clone(),
        &entry.desc,
        catalog,
        Some(entry.relation_oid),
    )?;
    let column_defaults = bind_insert_column_defaults(&entry.desc, catalog, local_ctes)?;
    let predicate = stmt
        .where_clause
        .as_ref()
        .map(|expr| {
            reject_window_clause(expr, "WHERE")?;
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
        !stmt.returning.is_empty(),
        catalog,
    )?;
    let predicate = prepend_visibility_quals(target_rls.visibility_quals.clone(), predicate);
    let assignments = stmt
        .assignments
        .iter()
        .map(|assignment| {
            let column_index = resolve_update_assignment_column(
                &scope,
                &assignment.target,
                &stmt.table_name,
                &target_relation_name,
            )?;
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
            validate_jsonb_assignment_target_subscripts(
                entry.desc.columns[column_index].sql_type,
                &target,
                catalog,
            )?;
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
    reject_duplicate_whole_column_assignments(&entry.desc, &assignments)?;

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
                entry.relkind == 'p' && !stmt.only,
                &child,
                catalog,
            )
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    let required_privileges = vec![update_privilege_requirement(
        &entry,
        &stmt.table_name,
        &assignments,
        predicate.as_ref(),
        &returning,
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
        current_of: stmt.current_of.clone(),
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
    let mut target_scope = scope_for_base_relation_with_generated(
        &target_relation_name,
        &entry.desc,
        Some(entry.relation_oid),
        catalog,
    )?;
    if stmt.target_alias.is_some() {
        target_scope = mark_scope_hidden_invalid_relation(target_scope, &stmt.table_name);
    }
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
        !stmt.only && matches!(entry.relkind, 'r' | 'p'),
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
        false,
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
    let returning_scope = scope_with_returning_pseudo_rows_with_generated(
        eval_scope.clone(),
        &entry.desc,
        catalog,
        Some(entry.relation_oid),
    )?;

    let target_rls = build_target_relation_row_security(
        &stmt.table_name,
        entry.relation_oid,
        &entry.desc,
        PolicyCommand::Update,
        true,
        !stmt.returning.is_empty(),
        catalog,
    )?;
    let user_predicate = stmt
        .where_clause
        .as_ref()
        .map(|expr| {
            reject_window_clause(expr, "WHERE")?;
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
    let predicate =
        prepend_visibility_quals(target_rls.visibility_quals.clone(), user_predicate.clone());
    let assignments = stmt
        .assignments
        .iter()
        .map(|assignment| {
            let column_index = resolve_update_assignment_column(
                &target_scope,
                &assignment.target,
                &stmt.table_name,
                &target_relation_name,
            )?;
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
            validate_jsonb_assignment_target_subscripts(
                entry.desc.columns[column_index].sql_type,
                &target,
                catalog,
            )?;
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
    reject_duplicate_whole_column_assignments(&entry.desc, &assignments)?;
    let returning = bind_returning_targets(
        &stmt.returning,
        &returning_scope,
        catalog,
        outer_scopes,
        local_ctes,
    )?;
    let query = query_from_projection_with_target_security_quals(
        projected,
        Vec::new(),
        predicate.clone(),
        catalog,
    )?;
    let [query] = crate::backend::rewrite::pg_rewrite_query(query, catalog)?
        .try_into()
        .expect("UPDATE FROM input rewrite should return a single query");
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
            entry.relkind == 'p' && !stmt.only,
            &child,
            catalog,
        )
    })
    .collect::<Result<Vec<_>, ParseError>>()?;
    let required_privileges = vec![update_privilege_requirement(
        &entry,
        &stmt.table_name,
        &assignments,
        predicate.as_ref(),
        &returning,
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
        current_of: stmt.current_of.clone(),
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
    let bound = BoundAssignmentTarget {
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
    };
    validate_jsonb_assignment_target_subscripts(
        scope.desc.columns[column_index].sql_type,
        &bound,
        catalog,
    )?;
    Ok(bound)
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
    bind_delete_with_outer_scopes_and_ctes(stmt, catalog, outer_scopes, &[])
}

pub(crate) fn bind_delete_with_outer_scopes_and_ctes(
    stmt: &DeleteStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    outer_ctes: &[BoundCte],
) -> Result<BoundDeleteStatement, ParseError> {
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
    if stmt.using.is_some() {
        return bind_delete_using(stmt, catalog, outer_scopes, &visible_ctes, &entry);
    }
    let visible_target_name = stmt.target_alias.as_deref().unwrap_or(&stmt.table_name);
    let scope = scope_for_base_relation_with_generated(
        visible_target_name,
        &entry.desc,
        Some(entry.relation_oid),
        catalog,
    )?;
    if stmt.target_alias.is_some()
        && stmt.where_clause.as_ref().is_some_and(|expr| {
            format!("{expr:?}").contains(&format!("Column(\"{}.", stmt.table_name))
        })
    {
        return Err(ParseError::InvalidFromClauseReference(
            stmt.table_name.clone(),
        ));
    }
    let returning_scope = scope_with_returning_pseudo_rows_with_generated(
        scope.clone(),
        &entry.desc,
        catalog,
        Some(entry.relation_oid),
    )?;
    let predicate = stmt
        .where_clause
        .as_ref()
        .map(|expr| {
            reject_window_clause(expr, "WHERE")?;
            bind_expr_with_outer_and_ctes(expr, &scope, catalog, outer_scopes, None, &visible_ctes)
        })
        .transpose()?;
    let returning = bind_returning_targets(
        &stmt.returning,
        &returning_scope,
        catalog,
        outer_scopes,
        &visible_ctes,
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
    let predicate = prepend_visibility_quals(target_rls.visibility_quals.clone(), predicate);

    let partition_delete_root_oid =
        (entry.relkind == 'p' && !stmt.only).then_some(entry.relation_oid);
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
                partition_delete_root_oid,
                &child,
                catalog,
            )
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    let required_privileges = vec![delete_privilege_requirement(
        &entry,
        &stmt.table_name,
        predicate.as_ref(),
        &returning,
    )];

    Ok(BoundDeleteStatement {
        targets,
        returning,
        input_plan: None,
        target_visible_count: entry.desc.columns.len(),
        visible_column_count: entry.desc.columns.len(),
        target_ctid_index: entry.desc.columns.len(),
        target_tableoid_index: entry.desc.columns.len() + 1,
        required_privileges,
        subplans: Vec::new(),
        current_of: stmt.current_of.clone(),
    })
}

fn bind_delete_using(
    stmt: &DeleteStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    local_ctes: &[BoundCte],
    entry: &BoundRelation,
) -> Result<BoundDeleteStatement, ParseError> {
    let target_relation_name = stmt
        .target_alias
        .clone()
        .unwrap_or_else(|| stmt.table_name.clone());
    let target_scope = scope_for_base_relation_with_generated(
        &target_relation_name,
        &entry.desc,
        Some(entry.relation_oid),
        catalog,
    )?;
    let source_stmt = stmt.using.as_ref().expect("checked above");
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
        false,
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
    let returning_scope = scope_with_returning_pseudo_rows_with_generated(
        eval_scope.clone(),
        &entry.desc,
        catalog,
        Some(entry.relation_oid),
    )?;

    let target_rls = build_target_relation_row_security(
        &stmt.table_name,
        entry.relation_oid,
        &entry.desc,
        PolicyCommand::Delete,
        true,
        false,
        catalog,
    )?;
    let user_predicate = stmt
        .where_clause
        .as_ref()
        .map(|expr| {
            reject_window_clause(expr, "WHERE")?;
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
    let predicate =
        prepend_visibility_quals(target_rls.visibility_quals.clone(), user_predicate.clone());
    let returning = bind_returning_targets(
        &stmt.returning,
        &returning_scope,
        catalog,
        outer_scopes,
        local_ctes,
    )?;
    let query = query_from_projection_with_target_security_quals(
        projected,
        Vec::new(),
        predicate.clone(),
        catalog,
    )?;
    let input_plan = crate::backend::optimizer::fold_query_constants(query)
        .map(|query| crate::backend::optimizer::planner(query, catalog))??;

    let partition_delete_root_oid =
        (entry.relkind == 'p' && !stmt.only).then_some(entry.relation_oid);
    let targets = partitioned_update_target_oids(catalog, &entry, stmt.only)
        .into_iter()
        .map(|relation_oid| {
            let child = catalog
                .relation_by_oid(relation_oid)
                .ok_or_else(|| ParseError::UnknownTable(stmt.table_name.clone()))?;
            build_delete_target_from_joined_input(
                &stmt.table_name,
                &entry.desc,
                predicate.as_ref(),
                partition_delete_root_oid,
                &child,
                catalog,
            )
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    let required_privileges = vec![delete_privilege_requirement(
        &entry,
        &stmt.table_name,
        predicate.as_ref(),
        &returning,
    )];

    Ok(BoundDeleteStatement {
        targets,
        returning,
        input_plan: Some(input_plan),
        target_visible_count,
        visible_column_count,
        target_ctid_index: visible_column_count,
        target_tableoid_index: visible_column_count + 1,
        required_privileges,
        subplans: Vec::new(),
        current_of: stmt.current_of.clone(),
    })
}
