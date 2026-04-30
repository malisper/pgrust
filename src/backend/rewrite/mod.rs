mod row_security;
mod rules;
mod view_dml;
mod views;

use std::cell::Cell;

use row_security::apply_query_row_security_with_active_relations;
pub(crate) use row_security::{
    RlsWriteCheck, RlsWriteCheckSource, TargetRlsState, apply_query_row_security,
    build_target_relation_row_security, build_target_relation_row_security_for_user,
    relation_has_row_security, relation_row_security_is_enabled_for_user,
};
pub(crate) use rules::{
    format_stored_rule_definition, format_stored_rule_definition_with_catalog,
    split_stored_rule_action_sql,
};
pub(crate) use view_dml::{
    NonUpdatableViewColumnReason, ResolvedAutoViewTarget, ViewDmlEvent, ViewDmlRewriteError,
    ViewPrivilegeContext, resolve_auto_updatable_view_target,
};
pub(crate) use views::{
    format_view_definition, has_stored_view_query, load_view_return_query, load_view_return_select,
    refresh_query_relation_descriptors, register_stored_view_query, render_relation_expr_sql,
    render_relation_expr_sql_for_information_schema, render_view_query_sql,
    split_stored_view_definition_sql, stored_view_query_for_rule,
};

use crate::backend::parser::{CatalogLookup, ParseError};
use crate::include::nodes::parsenodes::{JoinTreeNode, Query, RangeTblEntry, RangeTblEntryKind};
use crate::include::nodes::primnodes::{
    AggAccum, Expr, ExprArraySubscript, RelationPrivilegeRequirement, RowsFromItem, RowsFromSource,
    SetReturningCall, SetReturningExpr, SortGroupClause, SqlJsonQueryFunction,
    SqlJsonTableBehavior, SqlJsonTablePassingArg, SubLink, TargetEntry, WindowClause, WindowFrame,
    WindowFrameBound, WindowFuncExpr, WindowFuncKind, WindowSpec, set_returning_call_exprs,
};
use views::rewrite_view_relation_query;

thread_local! {
    static RESTRICT_NONSYSTEM_VIEW_EXPANSION: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn with_restrict_nonsystem_view_expansion<T>(enabled: bool, f: impl FnOnce() -> T) -> T {
    RESTRICT_NONSYSTEM_VIEW_EXPANSION.with(|flag| {
        let previous = flag.replace(enabled);
        let result = f();
        flag.set(previous);
        result
    })
}

fn restrict_nonsystem_view_expansion_enabled() -> bool {
    RESTRICT_NONSYSTEM_VIEW_EXPANSION.with(Cell::get)
}

pub(crate) fn pg_rewrite_query(
    query: Query,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<Query>, ParseError> {
    let mut active_policy_relations = Vec::new();
    Ok(vec![rewrite_query(
        query,
        catalog,
        &[],
        &mut active_policy_relations,
    )?])
}

fn rewrite_query(
    query: Query,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<Query, ParseError> {
    let mut rewritten = Query {
        rtable: query
            .rtable
            .into_iter()
            .map(|rte| rewrite_rte(rte, catalog, expanded_views, active_policy_relations))
            .collect::<Result<Vec<_>, _>>()?,
        where_qual: query
            .where_qual
            .map(|expr| {
                rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
            })
            .transpose()?,
        group_by: query
            .group_by
            .into_iter()
            .map(|expr| {
                rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
            })
            .collect::<Result<Vec<_>, _>>()?,
        grouping_sets: query
            .grouping_sets
            .into_iter()
            .map(|set| {
                set.into_iter()
                    .map(|expr| {
                        rewrite_semantic_expr(
                            expr,
                            catalog,
                            expanded_views,
                            active_policy_relations,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?,
        accumulators: query
            .accumulators
            .into_iter()
            .map(|accum| rewrite_agg_accum(accum, catalog, expanded_views, active_policy_relations))
            .collect::<Result<Vec<_>, _>>()?,
        window_clauses: query
            .window_clauses
            .into_iter()
            .map(|clause| {
                rewrite_window_clause(clause, catalog, expanded_views, active_policy_relations)
            })
            .collect::<Result<Vec<_>, _>>()?,
        having_qual: query
            .having_qual
            .map(|expr| {
                rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
            })
            .transpose()?,
        target_list: query
            .target_list
            .into_iter()
            .map(|target| {
                rewrite_target_entry(target, catalog, expanded_views, active_policy_relations)
            })
            .collect::<Result<Vec<_>, _>>()?,
        sort_clause: query
            .sort_clause
            .into_iter()
            .map(|clause| {
                rewrite_sort_group_clause(clause, catalog, expanded_views, active_policy_relations)
            })
            .collect::<Result<Vec<_>, _>>()?,
        distinct_on: query
            .distinct_on
            .into_iter()
            .map(|clause| {
                rewrite_sort_group_clause(clause, catalog, expanded_views, active_policy_relations)
            })
            .collect::<Result<Vec<_>, _>>()?,
        has_target_srfs: query.has_target_srfs,
        recursive_union: query
            .recursive_union
            .map(|recursive_union| {
                Ok(Box::new(
                    crate::include::nodes::parsenodes::RecursiveUnionQuery {
                        output_desc: recursive_union.output_desc,
                        anchor: rewrite_query(
                            recursive_union.anchor,
                            catalog,
                            expanded_views,
                            active_policy_relations,
                        )?,
                        recursive: rewrite_query(
                            recursive_union.recursive,
                            catalog,
                            expanded_views,
                            active_policy_relations,
                        )?,
                        distinct: recursive_union.distinct,
                        recursive_references_worktable: recursive_union
                            .recursive_references_worktable,
                        worktable_id: recursive_union.worktable_id,
                    },
                ))
            })
            .transpose()?,
        set_operation: query
            .set_operation
            .map(|set_operation| {
                Ok(Box::new(
                    crate::include::nodes::parsenodes::SetOperationQuery {
                        output_desc: set_operation.output_desc,
                        op: set_operation.op,
                        inputs: set_operation
                            .inputs
                            .into_iter()
                            .map(|input| {
                                rewrite_query(
                                    input,
                                    catalog,
                                    expanded_views,
                                    active_policy_relations,
                                )
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    },
                ))
            })
            .transpose()?,
        ..query
    };
    apply_query_row_security_with_active_relations(
        &mut rewritten,
        catalog,
        active_policy_relations,
    )?;
    Ok(rewritten)
}

pub(crate) fn relation_has_security_invoker(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> bool {
    catalog
        .class_row_by_oid(relation_oid)
        .and_then(|row| row.reloptions)
        .is_some_and(|options| {
            options.iter().any(|option| {
                let (name, value) = option
                    .split_once('=')
                    .map(|(name, value)| (name, value))
                    .unwrap_or((option.as_str(), "true"));
                name.eq_ignore_ascii_case("security_invoker")
                    && matches!(value.to_ascii_lowercase().as_str(), "true" | "on")
            })
        })
}

fn apply_view_permission_context(query: &mut Query, view_owner_oid: u32, security_invoker: bool) {
    for rte in &mut query.rtable {
        if let Some(permission) = rte.permission.as_mut() {
            permission.check_as_user_oid = (!security_invoker).then_some(view_owner_oid);
        }
        match &mut rte.kind {
            RangeTblEntryKind::Subquery { query } | RangeTblEntryKind::Cte { query, .. } => {
                apply_view_permission_context(query, view_owner_oid, security_invoker);
            }
            _ => {}
        }
    }
    if let Some(recursive_union) = &mut query.recursive_union {
        apply_view_permission_context(
            &mut recursive_union.anchor,
            view_owner_oid,
            security_invoker,
        );
        apply_view_permission_context(
            &mut recursive_union.recursive,
            view_owner_oid,
            security_invoker,
        );
    }
    if let Some(set_operation) = &mut query.set_operation {
        for input in &mut set_operation.inputs {
            apply_view_permission_context(input, view_owner_oid, security_invoker);
        }
    }
}

pub(crate) fn collect_query_relation_privileges(
    query: &Query,
) -> Vec<RelationPrivilegeRequirement> {
    let mut privileges = Vec::new();
    collect_query_relation_privileges_into(query, &mut privileges);
    privileges
}

fn collect_query_relation_privileges_into(
    query: &Query,
    privileges: &mut Vec<RelationPrivilegeRequirement>,
) {
    for rte in &query.rtable {
        if let Some(permission) = &rte.permission {
            privileges.push(permission.clone());
        }
        for qual in &rte.security_quals {
            collect_expr_relation_privileges(qual, privileges);
        }
        match &rte.kind {
            RangeTblEntryKind::Join { joinaliasvars, .. } => {
                for expr in joinaliasvars {
                    collect_expr_relation_privileges(expr, privileges);
                }
            }
            RangeTblEntryKind::Values { rows, .. } => {
                for expr in rows.iter().flatten() {
                    collect_expr_relation_privileges(expr, privileges);
                }
            }
            RangeTblEntryKind::Function { call } => {
                for expr in set_returning_call_exprs(call) {
                    collect_expr_relation_privileges(expr, privileges);
                }
            }
            RangeTblEntryKind::Subquery { query } | RangeTblEntryKind::Cte { query, .. } => {
                collect_query_relation_privileges_into(query, privileges);
            }
            RangeTblEntryKind::Result
            | RangeTblEntryKind::Relation { .. }
            | RangeTblEntryKind::WorkTable { .. } => {}
        }
    }
    if let Some(jointree) = &query.jointree {
        collect_join_tree_relation_privileges(jointree, privileges);
    }
    for target in &query.target_list {
        collect_expr_relation_privileges(&target.expr, privileges);
    }
    for clause in &query.distinct_on {
        collect_expr_relation_privileges(&clause.expr, privileges);
    }
    if let Some(where_qual) = &query.where_qual {
        collect_expr_relation_privileges(where_qual, privileges);
    }
    for expr in &query.group_by {
        collect_expr_relation_privileges(expr, privileges);
    }
    for accum in &query.accumulators {
        collect_agg_accum_relation_privileges(accum, privileges);
    }
    for clause in &query.window_clauses {
        collect_window_clause_relation_privileges(clause, privileges);
    }
    if let Some(having_qual) = &query.having_qual {
        collect_expr_relation_privileges(having_qual, privileges);
    }
    for clause in &query.sort_clause {
        collect_expr_relation_privileges(&clause.expr, privileges);
    }
    if let Some(recursive_union) = &query.recursive_union {
        collect_query_relation_privileges_into(&recursive_union.anchor, privileges);
        collect_query_relation_privileges_into(&recursive_union.recursive, privileges);
    }
    if let Some(set_operation) = &query.set_operation {
        for input in &set_operation.inputs {
            collect_query_relation_privileges_into(input, privileges);
        }
    }
}

fn collect_join_tree_relation_privileges(
    node: &JoinTreeNode,
    privileges: &mut Vec<RelationPrivilegeRequirement>,
) {
    match node {
        JoinTreeNode::RangeTblRef(_) => {}
        JoinTreeNode::JoinExpr {
            left, right, quals, ..
        } => {
            collect_join_tree_relation_privileges(left, privileges);
            collect_join_tree_relation_privileges(right, privileges);
            collect_expr_relation_privileges(quals, privileges);
        }
    }
}

fn collect_agg_accum_relation_privileges(
    accum: &AggAccum,
    privileges: &mut Vec<RelationPrivilegeRequirement>,
) {
    for expr in &accum.args {
        collect_expr_relation_privileges(expr, privileges);
    }
    for item in &accum.order_by {
        collect_expr_relation_privileges(&item.expr, privileges);
    }
    if let Some(filter) = &accum.filter {
        collect_expr_relation_privileges(filter, privileges);
    }
}

fn collect_window_clause_relation_privileges(
    clause: &WindowClause,
    privileges: &mut Vec<RelationPrivilegeRequirement>,
) {
    for expr in &clause.spec.partition_by {
        collect_expr_relation_privileges(expr, privileges);
    }
    for item in &clause.spec.order_by {
        collect_expr_relation_privileges(&item.expr, privileges);
    }
    collect_window_frame_bound_relation_privileges(&clause.spec.frame.start_bound, privileges);
    collect_window_frame_bound_relation_privileges(&clause.spec.frame.end_bound, privileges);
    for func in &clause.functions {
        collect_window_func_relation_privileges(func, privileges);
    }
}

fn collect_window_frame_bound_relation_privileges(
    bound: &WindowFrameBound,
    privileges: &mut Vec<RelationPrivilegeRequirement>,
) {
    match bound {
        WindowFrameBound::OffsetPreceding(offset) | WindowFrameBound::OffsetFollowing(offset) => {
            collect_expr_relation_privileges(&offset.expr, privileges);
        }
        WindowFrameBound::UnboundedPreceding
        | WindowFrameBound::CurrentRow
        | WindowFrameBound::UnboundedFollowing => {}
    }
}

fn collect_window_func_relation_privileges(
    func: &WindowFuncExpr,
    privileges: &mut Vec<RelationPrivilegeRequirement>,
) {
    if let WindowFuncKind::Aggregate(aggref) = &func.kind {
        collect_aggref_relation_privileges(aggref, privileges);
    }
    for arg in &func.args {
        collect_expr_relation_privileges(arg, privileges);
    }
}

fn collect_aggref_relation_privileges(
    aggref: &crate::include::nodes::primnodes::Aggref,
    privileges: &mut Vec<RelationPrivilegeRequirement>,
) {
    for arg in &aggref.direct_args {
        collect_expr_relation_privileges(arg, privileges);
    }
    for arg in &aggref.args {
        collect_expr_relation_privileges(arg, privileges);
    }
    for item in &aggref.aggorder {
        collect_expr_relation_privileges(&item.expr, privileges);
    }
    if let Some(filter) = &aggref.aggfilter {
        collect_expr_relation_privileges(filter, privileges);
    }
}

fn collect_expr_relation_privileges(
    expr: &Expr,
    privileges: &mut Vec<RelationPrivilegeRequirement>,
) {
    match expr {
        Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. }
        | Expr::CaseTest(_) => {}
        Expr::Aggref(aggref) => collect_aggref_relation_privileges(aggref, privileges),
        Expr::WindowFunc(func) => collect_window_func_relation_privileges(func, privileges),
        Expr::Op(op) => {
            for arg in &op.args {
                collect_expr_relation_privileges(arg, privileges);
            }
        }
        Expr::Bool(bool_expr) => {
            for arg in &bool_expr.args {
                collect_expr_relation_privileges(arg, privileges);
            }
        }
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                collect_expr_relation_privileges(arg, privileges);
            }
            for arm in &case_expr.args {
                collect_expr_relation_privileges(&arm.expr, privileges);
                collect_expr_relation_privileges(&arm.result, privileges);
            }
            collect_expr_relation_privileges(&case_expr.defresult, privileges);
        }
        Expr::Func(func) => {
            for arg in &func.args {
                collect_expr_relation_privileges(arg, privileges);
            }
        }
        Expr::SqlJsonQueryFunction(func) => {
            for child in func.child_exprs() {
                collect_expr_relation_privileges(child, privileges);
            }
        }
        Expr::SetReturning(srf) => {
            for arg in set_returning_call_exprs(&srf.call) {
                collect_expr_relation_privileges(arg, privileges);
            }
        }
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = &sublink.testexpr {
                collect_expr_relation_privileges(testexpr, privileges);
            }
            collect_query_relation_privileges_into(&sublink.subselect, privileges);
        }
        Expr::SubPlan(subplan) => {
            if let Some(testexpr) = &subplan.testexpr {
                collect_expr_relation_privileges(testexpr, privileges);
            }
            for arg in &subplan.args {
                collect_expr_relation_privileges(arg, privileges);
            }
        }
        Expr::ScalarArrayOp(saop) => {
            collect_expr_relation_privileges(&saop.left, privileges);
            collect_expr_relation_privileges(&saop.right, privileges);
        }
        Expr::Xml(xml) => {
            for child in xml.child_exprs() {
                collect_expr_relation_privileges(child, privileges);
            }
        }
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => {
            collect_expr_relation_privileges(inner, privileges);
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
            collect_expr_relation_privileges(expr, privileges);
            collect_expr_relation_privileges(pattern, privileges);
            if let Some(escape) = escape {
                collect_expr_relation_privileges(escape, privileges);
            }
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            collect_expr_relation_privileges(inner, privileges);
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            collect_expr_relation_privileges(left, privileges);
            collect_expr_relation_privileges(right, privileges);
        }
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                collect_expr_relation_privileges(element, privileges);
            }
        }
        Expr::Row { fields, .. } => {
            for (_, expr) in fields {
                collect_expr_relation_privileges(expr, privileges);
            }
        }
        Expr::FieldSelect { expr, .. } => {
            collect_expr_relation_privileges(expr, privileges);
        }
        Expr::ArraySubscript { array, subscripts } => {
            collect_expr_relation_privileges(array, privileges);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_expr_relation_privileges(lower, privileges);
                }
                if let Some(upper) = &subscript.upper {
                    collect_expr_relation_privileges(upper, privileges);
                }
            }
        }
    }
}

pub(super) fn rewrite_policy_expr(
    expr: Expr,
    catalog: &dyn CatalogLookup,
    effective_user_oid: u32,
    active_policy_relations: &mut Vec<u32>,
) -> Result<Expr, ParseError> {
    let mut expr = rewrite_semantic_expr(expr, catalog, &[], active_policy_relations)?;
    apply_policy_expr_permission_context(&mut expr, effective_user_oid);
    Ok(expr)
}

fn apply_policy_expr_permission_context(expr: &mut Expr, effective_user_oid: u32) {
    match expr {
        Expr::Op(op) => {
            for arg in &mut op.args {
                apply_policy_expr_permission_context(arg, effective_user_oid);
            }
        }
        Expr::Bool(bool_expr) => {
            for arg in &mut bool_expr.args {
                apply_policy_expr_permission_context(arg, effective_user_oid);
            }
        }
        Expr::Func(func) => {
            for arg in &mut func.args {
                apply_policy_expr_permission_context(arg, effective_user_oid);
            }
        }
        Expr::Case(case_expr) => {
            if let Some(arg) = case_expr.arg.as_mut() {
                apply_policy_expr_permission_context(arg, effective_user_oid);
            }
            for arm in &mut case_expr.args {
                apply_policy_expr_permission_context(&mut arm.expr, effective_user_oid);
                apply_policy_expr_permission_context(&mut arm.result, effective_user_oid);
            }
            apply_policy_expr_permission_context(&mut case_expr.defresult, effective_user_oid);
        }
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = sublink.testexpr.as_mut() {
                apply_policy_expr_permission_context(testexpr, effective_user_oid);
            }
            apply_view_permission_context(&mut sublink.subselect, effective_user_oid, false);
        }
        Expr::ScalarArrayOp(saop) => {
            apply_policy_expr_permission_context(&mut saop.left, effective_user_oid);
            apply_policy_expr_permission_context(&mut saop.right, effective_user_oid);
        }
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => {
            apply_policy_expr_permission_context(inner, effective_user_oid);
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
            apply_policy_expr_permission_context(expr, effective_user_oid);
            apply_policy_expr_permission_context(pattern, effective_user_oid);
            if let Some(escape) = escape.as_mut() {
                apply_policy_expr_permission_context(escape, effective_user_oid);
            }
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            apply_policy_expr_permission_context(inner, effective_user_oid);
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            apply_policy_expr_permission_context(left, effective_user_oid);
            apply_policy_expr_permission_context(right, effective_user_oid);
        }
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                apply_policy_expr_permission_context(element, effective_user_oid);
            }
        }
        Expr::Row { fields, .. } => {
            for (_, expr) in fields {
                apply_policy_expr_permission_context(expr, effective_user_oid);
            }
        }
        Expr::FieldSelect { expr, .. } => {
            apply_policy_expr_permission_context(expr, effective_user_oid);
        }
        Expr::ArraySubscript { array, subscripts } => {
            apply_policy_expr_permission_context(array, effective_user_oid);
            for subscript in subscripts {
                if let Some(lower) = subscript.lower.as_mut() {
                    apply_policy_expr_permission_context(lower, effective_user_oid);
                }
                if let Some(upper) = subscript.upper.as_mut() {
                    apply_policy_expr_permission_context(upper, effective_user_oid);
                }
            }
        }
        Expr::Aggref(_)
        | Expr::WindowFunc(_)
        | Expr::SqlJsonQueryFunction(_)
        | Expr::SetReturning(_)
        | Expr::SubPlan(_)
        | Expr::Xml(_)
        | Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. }
        | Expr::CaseTest(_) => {}
    }
}

fn rewrite_rte(
    rte: RangeTblEntry,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<RangeTblEntry, ParseError> {
    let kind = match rte.kind {
        RangeTblEntryKind::Relation {
            rel: _,
            relation_oid,
            relkind,
            relispopulated: _,
            toast: _,
            tablesample: _,
        } if relkind == 'v' => {
            if restrict_nonsystem_view_expansion_enabled() {
                let class_row = catalog
                    .class_row_by_oid(relation_oid)
                    .ok_or_else(|| ParseError::UnknownTable(relation_oid.to_string()))?;
                if class_row.relnamespace != crate::include::catalog::PG_CATALOG_NAMESPACE_OID {
                    return Err(ParseError::DetailedError {
                        message: format!(
                            "access to non-system view \"{}\" is restricted",
                            class_row.relname
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42501",
                    });
                }
            }
            let mut analyzed = rewrite_view_relation_query(
                relation_oid,
                &rte.desc,
                rte.alias.as_deref(),
                catalog,
                expanded_views,
            )?;
            let class_row = catalog
                .class_row_by_oid(relation_oid)
                .ok_or_else(|| ParseError::UnknownTable(relation_oid.to_string()))?;
            apply_view_permission_context(
                &mut analyzed,
                class_row.relowner,
                relation_has_security_invoker(catalog, relation_oid),
            );
            let mut next_views = expanded_views.to_vec();
            next_views.push(relation_oid);
            RangeTblEntryKind::Subquery {
                query: Box::new(rewrite_query(
                    analyzed,
                    catalog,
                    &next_views,
                    active_policy_relations,
                )?),
            }
        }
        RangeTblEntryKind::Relation {
            rel,
            relation_oid,
            relkind,
            relispopulated,
            toast,
            tablesample,
        } => RangeTblEntryKind::Relation {
            rel,
            relation_oid,
            relkind,
            relispopulated,
            toast,
            tablesample,
        },
        RangeTblEntryKind::Join {
            jointype,
            joinmergedcols,
            joinaliasvars,
            joinleftcols,
            joinrightcols,
        } => RangeTblEntryKind::Join {
            jointype,
            joinmergedcols,
            joinaliasvars: joinaliasvars
                .into_iter()
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            joinleftcols,
            joinrightcols,
        },
        RangeTblEntryKind::Values {
            rows,
            output_columns,
        } => RangeTblEntryKind::Values {
            rows: rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|expr| {
                            rewrite_semantic_expr(
                                expr,
                                catalog,
                                expanded_views,
                                active_policy_relations,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
        },
        RangeTblEntryKind::Function { call } => RangeTblEntryKind::Function {
            call: rewrite_set_returning_call(
                call,
                catalog,
                expanded_views,
                active_policy_relations,
            )?,
        },
        RangeTblEntryKind::Cte { cte_id, query } => RangeTblEntryKind::Cte {
            cte_id,
            query: Box::new(rewrite_query(
                *query,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
        },
        RangeTblEntryKind::Subquery { query } => RangeTblEntryKind::Subquery {
            query: Box::new(rewrite_query(
                *query,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
        },
        RangeTblEntryKind::WorkTable { worktable_id } => {
            RangeTblEntryKind::WorkTable { worktable_id }
        }
        RangeTblEntryKind::Result => RangeTblEntryKind::Result,
    };
    Ok(RangeTblEntry { kind, ..rte })
}

fn rewrite_target_entry(
    target: TargetEntry,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<TargetEntry, ParseError> {
    Ok(TargetEntry {
        expr: rewrite_semantic_expr(
            target.expr,
            catalog,
            expanded_views,
            active_policy_relations,
        )?,
        ..target
    })
}

fn rewrite_sort_group_clause(
    clause: SortGroupClause,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<SortGroupClause, ParseError> {
    Ok(SortGroupClause {
        expr: rewrite_semantic_expr(
            clause.expr,
            catalog,
            expanded_views,
            active_policy_relations,
        )?,
        ..clause
    })
}

fn rewrite_window_clause(
    clause: WindowClause,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<WindowClause, ParseError> {
    Ok(WindowClause {
        spec: WindowSpec {
            partition_by: clause
                .spec
                .partition_by
                .into_iter()
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            order_by: clause
                .spec
                .order_by
                .into_iter()
                .map(|item| {
                    Ok(crate::include::nodes::primnodes::OrderByEntry {
                        expr: rewrite_semantic_expr(
                            item.expr,
                            catalog,
                            expanded_views,
                            active_policy_relations,
                        )?,
                        ..item
                    })
                })
                .collect::<Result<Vec<_>, ParseError>>()?,
            frame: WindowFrame {
                mode: clause.spec.frame.mode,
                start_bound: rewrite_window_frame_bound(
                    clause.spec.frame.start_bound,
                    catalog,
                    expanded_views,
                    active_policy_relations,
                )?,
                end_bound: rewrite_window_frame_bound(
                    clause.spec.frame.end_bound,
                    catalog,
                    expanded_views,
                    active_policy_relations,
                )?,
                exclusion: clause.spec.frame.exclusion,
            },
        },
        functions: clause
            .functions
            .into_iter()
            .map(|func| {
                rewrite_window_func_expr(func, catalog, expanded_views, active_policy_relations)
            })
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn rewrite_window_frame_bound(
    bound: WindowFrameBound,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<WindowFrameBound, ParseError> {
    Ok(match bound {
        WindowFrameBound::OffsetPreceding(offset) => {
            let expr = rewrite_semantic_expr(
                offset.expr.clone(),
                catalog,
                expanded_views,
                active_policy_relations,
            )?;
            WindowFrameBound::OffsetPreceding(offset.with_expr(expr))
        }
        WindowFrameBound::OffsetFollowing(offset) => {
            let expr = rewrite_semantic_expr(
                offset.expr.clone(),
                catalog,
                expanded_views,
                active_policy_relations,
            )?;
            WindowFrameBound::OffsetFollowing(offset.with_expr(expr))
        }
        other => other,
    })
}

fn rewrite_window_func_expr(
    func: WindowFuncExpr,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<WindowFuncExpr, ParseError> {
    Ok(WindowFuncExpr {
        kind: match func.kind {
            WindowFuncKind::Aggregate(aggref) => WindowFuncKind::Aggregate(
                match rewrite_semantic_expr(
                    Expr::Aggref(Box::new(aggref)),
                    catalog,
                    expanded_views,
                    active_policy_relations,
                )? {
                    Expr::Aggref(aggref) => *aggref,
                    other => unreachable!("aggregate rewrite returned non-Aggref: {other:?}"),
                },
            ),
            WindowFuncKind::Builtin(kind) => WindowFuncKind::Builtin(kind),
        },
        args: func
            .args
            .into_iter()
            .map(|arg| rewrite_semantic_expr(arg, catalog, expanded_views, active_policy_relations))
            .collect::<Result<Vec<_>, _>>()?,
        ..func
    })
}

fn rewrite_agg_accum(
    accum: AggAccum,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<AggAccum, ParseError> {
    Ok(AggAccum {
        args: accum
            .args
            .into_iter()
            .map(|expr| {
                rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
            })
            .collect::<Result<Vec<_>, _>>()?,
        order_by: accum
            .order_by
            .into_iter()
            .map(|item| {
                Ok(crate::include::nodes::primnodes::OrderByEntry {
                    expr: rewrite_semantic_expr(
                        item.expr,
                        catalog,
                        expanded_views,
                        active_policy_relations,
                    )?,
                    ..item
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
        filter: accum
            .filter
            .map(|expr| {
                rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
            })
            .transpose()?,
        ..accum
    })
}

fn rewrite_set_returning_call(
    call: SetReturningCall,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<SetReturningCall, ParseError> {
    Ok(match call {
        SetReturningCall::RowsFrom {
            items,
            output_columns,
            with_ordinality,
        } => SetReturningCall::RowsFrom {
            items: items
                .into_iter()
                .map(|item| {
                    Ok(RowsFromItem {
                        source: match item.source {
                            RowsFromSource::Function(call) => {
                                RowsFromSource::Function(rewrite_set_returning_call(
                                    call,
                                    catalog,
                                    expanded_views,
                                    active_policy_relations,
                                )?)
                            }
                            RowsFromSource::Project {
                                output_exprs,
                                output_columns,
                            } => RowsFromSource::Project {
                                output_exprs: output_exprs
                                    .into_iter()
                                    .map(|expr| {
                                        rewrite_semantic_expr(
                                            expr,
                                            catalog,
                                            expanded_views,
                                            active_policy_relations,
                                        )
                                    })
                                    .collect::<Result<Vec<_>, ParseError>>()?,
                                output_columns,
                            },
                        },
                        column_definitions: item.column_definitions,
                    })
                })
                .collect::<Result<Vec<_>, ParseError>>()?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::GenerateSeries {
            func_oid,
            func_variadic,
            start,
            stop,
            step,
            timezone,
            output_columns,
            with_ordinality,
        } => SetReturningCall::GenerateSeries {
            func_oid,
            func_variadic,
            start: rewrite_semantic_expr(start, catalog, expanded_views, active_policy_relations)?,
            stop: rewrite_semantic_expr(stop, catalog, expanded_views, active_policy_relations)?,
            step: rewrite_semantic_expr(step, catalog, expanded_views, active_policy_relations)?,
            timezone: timezone
                .map(|timezone| {
                    rewrite_semantic_expr(
                        timezone,
                        catalog,
                        expanded_views,
                        active_policy_relations,
                    )
                })
                .transpose()?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::GenerateSubscripts {
            func_oid,
            func_variadic,
            array,
            dimension,
            reverse,
            output_columns,
            with_ordinality,
        } => SetReturningCall::GenerateSubscripts {
            func_oid,
            func_variadic,
            array: rewrite_semantic_expr(array, catalog, expanded_views, active_policy_relations)?,
            dimension: rewrite_semantic_expr(
                dimension,
                catalog,
                expanded_views,
                active_policy_relations,
            )?,
            reverse: reverse
                .map(|reverse| {
                    rewrite_semantic_expr(reverse, catalog, expanded_views, active_policy_relations)
                })
                .transpose()?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::PartitionTree {
            func_oid,
            func_variadic,
            relid,
            output_columns,
            with_ordinality,
        } => SetReturningCall::PartitionTree {
            func_oid,
            func_variadic,
            relid: rewrite_semantic_expr(relid, catalog, expanded_views, active_policy_relations)?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::PartitionAncestors {
            func_oid,
            func_variadic,
            relid,
            output_columns,
            with_ordinality,
        } => SetReturningCall::PartitionAncestors {
            func_oid,
            func_variadic,
            relid: rewrite_semantic_expr(relid, catalog, expanded_views, active_policy_relations)?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::PgLockStatus {
            func_oid,
            func_variadic,
            output_columns,
            with_ordinality,
        } => SetReturningCall::PgLockStatus {
            func_oid,
            func_variadic,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::PgSequences {
            output_columns,
            with_ordinality,
        } => SetReturningCall::PgSequences {
            output_columns,
            with_ordinality,
        },
        SetReturningCall::InformationSchemaSequences {
            output_columns,
            with_ordinality,
        } => SetReturningCall::InformationSchemaSequences {
            output_columns,
            with_ordinality,
        },
        SetReturningCall::TxidSnapshotXip {
            func_oid,
            func_variadic,
            arg,
            output_columns,
            with_ordinality,
        } => SetReturningCall::TxidSnapshotXip {
            func_oid,
            func_variadic,
            arg: rewrite_semantic_expr(arg, catalog, expanded_views, active_policy_relations)?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::Unnest {
            func_oid,
            func_variadic,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::Unnest {
            func_oid,
            func_variadic,
            args: args
                .into_iter()
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::JsonTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::JsonTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::JsonRecordFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
            record_type,
            with_ordinality,
        } => SetReturningCall::JsonRecordFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
            record_type,
            with_ordinality,
        },
        SetReturningCall::RegexTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::RegexTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::StringTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::StringTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::TextSearchTableFunction {
            kind,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::TextSearchTableFunction {
            kind,
            args: args
                .into_iter()
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::UserDefined {
            proc_oid,
            function_name,
            func_variadic,
            args,
            inlined_expr,
            output_columns,
            with_ordinality,
        } => SetReturningCall::UserDefined {
            proc_oid,
            function_name,
            func_variadic,
            args: args
                .into_iter()
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            inlined_expr: inlined_expr
                .map(|expr| {
                    rewrite_semantic_expr(*expr, catalog, expanded_views, active_policy_relations)
                        .map(Box::new)
                })
                .transpose()?,
            output_columns,
            with_ordinality,
        },
        sql @ (SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)) => sql
            .try_map_exprs(|expr| {
                rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
            })?,
    })
}

fn rewrite_sql_json_behavior(
    behavior: SqlJsonTableBehavior,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<SqlJsonTableBehavior, ParseError> {
    match behavior {
        SqlJsonTableBehavior::Default(expr) => Ok(SqlJsonTableBehavior::Default(
            rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)?,
        )),
        other => Ok(other),
    }
}

fn rewrite_semantic_expr(
    expr: Expr,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
    active_policy_relations: &mut Vec<u32>,
) -> Result<Expr, ParseError> {
    Ok(match expr {
        other @ (Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. }) => other,
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| {
                    rewrite_semantic_expr(arg, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| {
                    rewrite_semantic_expr(arg, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(crate::include::nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| {
                    rewrite_semantic_expr(arg, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            ..*func
        })),
        Expr::SqlJsonQueryFunction(func) => {
            Expr::SqlJsonQueryFunction(Box::new(SqlJsonQueryFunction {
                context: rewrite_semantic_expr(
                    func.context,
                    catalog,
                    expanded_views,
                    active_policy_relations,
                )?,
                path: rewrite_semantic_expr(
                    func.path,
                    catalog,
                    expanded_views,
                    active_policy_relations,
                )?,
                passing: func
                    .passing
                    .into_iter()
                    .map(|arg| {
                        Ok(SqlJsonTablePassingArg {
                            name: arg.name,
                            expr: rewrite_semantic_expr(
                                arg.expr,
                                catalog,
                                expanded_views,
                                active_policy_relations,
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>, ParseError>>()?,
                on_empty: rewrite_sql_json_behavior(
                    func.on_empty,
                    catalog,
                    expanded_views,
                    active_policy_relations,
                )?,
                on_error: rewrite_sql_json_behavior(
                    func.on_error,
                    catalog,
                    expanded_views,
                    active_policy_relations,
                )?,
                ..*func
            }))
        }
        Expr::SetReturning(srf) => Expr::SetReturning(Box::new(SetReturningExpr {
            call: rewrite_set_returning_call(
                srf.call,
                catalog,
                expanded_views,
                active_policy_relations,
            )?,
            ..*srf
        })),
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(crate::include::nodes::primnodes::Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| {
                    rewrite_semantic_expr(arg, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            aggorder: aggref
                .aggorder
                .into_iter()
                .map(|item| {
                    Ok(crate::include::nodes::primnodes::OrderByEntry {
                        expr: rewrite_semantic_expr(
                            item.expr,
                            catalog,
                            expanded_views,
                            active_policy_relations,
                        )?,
                        ..item
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
            aggfilter: aggref
                .aggfilter
                .map(|expr| {
                    rewrite_semantic_expr(expr, catalog, expanded_views, active_policy_relations)
                })
                .transpose()?,
            ..*aggref
        })),
        Expr::WindowFunc(window_func) => Expr::WindowFunc(Box::new(rewrite_window_func_expr(
            *window_func,
            catalog,
            expanded_views,
            active_policy_relations,
        )?)),
        Expr::SubLink(sublink) => Expr::SubLink(Box::new(SubLink {
            testexpr: sublink
                .testexpr
                .map(|expr| {
                    rewrite_semantic_expr(*expr, catalog, expanded_views, active_policy_relations)
                })
                .transpose()?
                .map(Box::new),
            subselect: Box::new(rewrite_query(
                *sublink.subselect,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            ..*sublink
        })),
        Expr::SubPlan(_) => {
            return Err(ParseError::UnexpectedToken {
                expected: "semantic query expression before planning",
                actual: "SubPlan".into(),
            });
        }
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(
            crate::include::nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(rewrite_semantic_expr(
                    *saop.left,
                    catalog,
                    expanded_views,
                    active_policy_relations,
                )?),
                right: Box::new(rewrite_semantic_expr(
                    *saop.right,
                    catalog,
                    expanded_views,
                    active_policy_relations,
                )?),
                ..*saop
            },
        )),
        Expr::Xml(xml_expr) => {
            let crate::include::nodes::primnodes::XmlExpr {
                op,
                name,
                named_args,
                arg_names,
                args,
                xml_option,
                indent,
                target_type,
                standalone,
                root_version,
            } = *xml_expr;
            Expr::Xml(Box::new(crate::include::nodes::primnodes::XmlExpr {
                op,
                name,
                named_args: named_args
                    .into_iter()
                    .map(|arg| {
                        rewrite_semantic_expr(arg, catalog, expanded_views, active_policy_relations)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                arg_names,
                args: args
                    .into_iter()
                    .map(|arg| {
                        rewrite_semantic_expr(arg, catalog, expanded_views, active_policy_relations)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                xml_option,
                indent,
                target_type,
                standalone,
                root_version,
            }))
        }
        Expr::Cast(inner, ty) => Expr::Cast(
            Box::new(rewrite_semantic_expr(
                *inner,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            ty,
        ),
        Expr::Collate {
            expr,
            collation_oid,
        } => Expr::Collate {
            expr: Box::new(rewrite_semantic_expr(
                *expr,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            collation_oid,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(rewrite_semantic_expr(
            *inner,
            catalog,
            expanded_views,
            active_policy_relations,
        )?)),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(rewrite_semantic_expr(
            *inner,
            catalog,
            expanded_views,
            active_policy_relations,
        )?)),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(rewrite_semantic_expr(
                *left,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            Box::new(rewrite_semantic_expr(
                *right,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(rewrite_semantic_expr(
                *left,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            Box::new(rewrite_semantic_expr(
                *right,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
        ),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            collation_oid,
        } => Expr::Like {
            expr: Box::new(rewrite_semantic_expr(
                *expr,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            pattern: Box::new(rewrite_semantic_expr(
                *pattern,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            escape: escape
                .map(|expr| {
                    rewrite_semantic_expr(*expr, catalog, expanded_views, active_policy_relations)
                })
                .transpose()?
                .map(Box::new),
            case_insensitive,
            negated,
            collation_oid,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
            collation_oid,
        } => Expr::Similar {
            expr: Box::new(rewrite_semantic_expr(
                *expr,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            pattern: Box::new(rewrite_semantic_expr(
                *pattern,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            escape: escape
                .map(|expr| {
                    rewrite_semantic_expr(*expr, catalog, expanded_views, active_policy_relations)
                })
                .transpose()?
                .map(Box::new),
            negated,
            collation_oid,
        },
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| {
                    rewrite_semantic_expr(element, catalog, expanded_views, active_policy_relations)
                })
                .collect::<Result<Vec<_>, _>>()?,
            array_type,
        },
        Expr::Row { descriptor, fields } => Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| {
                    Ok((
                        name,
                        rewrite_semantic_expr(
                            expr,
                            catalog,
                            expanded_views,
                            active_policy_relations,
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>, ParseError>>()?,
        },
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Expr::FieldSelect {
            expr: Box::new(rewrite_semantic_expr(
                *expr,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            field,
            field_type,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(rewrite_semantic_expr(
                *left,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            Box::new(rewrite_semantic_expr(
                *right,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
        ),
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| {
                    rewrite_semantic_expr(*arg, catalog, expanded_views, active_policy_relations)
                        .map(Box::new)
                })
                .transpose()?,
            args: case_expr
                .args
                .into_iter()
                .map(|arm| {
                    Ok(crate::include::nodes::primnodes::CaseWhen {
                        expr: rewrite_semantic_expr(
                            arm.expr,
                            catalog,
                            expanded_views,
                            active_policy_relations,
                        )?,
                        result: rewrite_semantic_expr(
                            arm.result,
                            catalog,
                            expanded_views,
                            active_policy_relations,
                        )?,
                    })
                })
                .collect::<Result<Vec<_>, ParseError>>()?,
            defresult: Box::new(rewrite_semantic_expr(
                *case_expr.defresult,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            ..*case_expr
        })),
        Expr::CaseTest(case_test) => Expr::CaseTest(case_test),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(rewrite_semantic_expr(
                *array,
                catalog,
                expanded_views,
                active_policy_relations,
            )?),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| {
                    Ok(ExprArraySubscript {
                        is_slice: subscript.is_slice,
                        lower: subscript
                            .lower
                            .map(|expr| {
                                rewrite_semantic_expr(
                                    expr,
                                    catalog,
                                    expanded_views,
                                    active_policy_relations,
                                )
                            })
                            .transpose()?,
                        upper: subscript
                            .upper
                            .map(|expr| {
                                rewrite_semantic_expr(
                                    expr,
                                    catalog,
                                    expanded_views,
                                    active_policy_relations,
                                )
                            })
                            .transpose()?,
                    })
                })
                .collect::<Result<Vec<_>, ParseError>>()?,
        },
    })
}
