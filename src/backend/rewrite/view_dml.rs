use crate::backend::parser::{
    BoundRelation, CatalogLookup, ParseError, bind_generated_expr,
    rewrite_local_vars_for_output_exprs,
};
use crate::include::nodes::parsenodes::{
    JoinTreeNode, Query, RangeTblEntryKind, SelectStatement, ViewCheckOption,
};
use crate::include::nodes::primnodes::{
    Expr, RelationDesc, attrno_index, is_system_attr, set_returning_call_exprs,
};

use super::views::{load_view_return_query, load_view_return_select};

const DISTINCT_DETAIL: &str = "Views containing DISTINCT are not automatically updatable.";
const SINGLE_RELATION_DETAIL: &str =
    "Views that do not select from a single table or view are not automatically updatable.";
const GROUP_BY_DETAIL: &str = "Views containing GROUP BY are not automatically updatable.";
const HAVING_DETAIL: &str = "Views containing HAVING are not automatically updatable.";
const SET_OPERATION_DETAIL: &str =
    "Views containing UNION, INTERSECT, or EXCEPT are not automatically updatable.";
const WITH_DETAIL: &str = "Views containing WITH are not automatically updatable.";
const LIMIT_OFFSET_DETAIL: &str =
    "Views containing LIMIT or OFFSET are not automatically updatable.";
const AGGREGATE_DETAIL: &str =
    "Views that return aggregate functions are not automatically updatable.";
const WINDOW_DETAIL: &str = "Views that return window functions are not automatically updatable.";
const PROJECT_SET_DETAIL: &str =
    "Views that return set-returning functions are not automatically updatable.";
const TABLESAMPLE_DETAIL: &str = "Views containing TABLESAMPLE are not automatically updatable.";
const TARGET_LIST_DETAIL: &str =
    "Views that do not select simple base table columns are not automatically updatable.";
const RECURSIVE_DETAIL: &str =
    "Views that directly or indirectly reference themselves are not automatically updatable.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NonUpdatableViewColumnReason {
    SystemColumn,
    NotBaseRelationColumn,
}

impl NonUpdatableViewColumnReason {
    pub(crate) fn detail(self) -> &'static str {
        match self {
            NonUpdatableViewColumnReason::SystemColumn => {
                "View columns that refer to system columns are not updatable."
            }
            NonUpdatableViewColumnReason::NotBaseRelationColumn => {
                "View columns that are not columns of their base relation are not updatable."
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ViewDmlEvent {
    Insert,
    Update,
    Delete,
}

impl ViewDmlEvent {
    fn rule_event_code(self) -> char {
        match self {
            ViewDmlEvent::Update => '2',
            ViewDmlEvent::Insert => '3',
            ViewDmlEvent::Delete => '4',
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedAutoViewTarget {
    pub(crate) base_relation: BoundRelation,
    pub(crate) base_inh: bool,
    pub(crate) visible_output_exprs: Vec<Expr>,
    pub(crate) combined_predicate: Option<Expr>,
    pub(crate) updatable_column_map: Vec<Option<usize>>,
    pub(crate) non_updatable_column_reasons: Vec<Option<NonUpdatableViewColumnReason>>,
    pub(crate) privilege_contexts: Vec<ViewPrivilegeContext>,
    pub(crate) all_view_predicates: Vec<ViewCheck>,
    pub(crate) view_check_options: Vec<ViewCheck>,
}

#[derive(Debug, Clone)]
pub(crate) struct ViewPrivilegeContext {
    pub(crate) relation: BoundRelation,
    pub(crate) check_as_user_oid: Option<u32>,
    pub(crate) column_map: Vec<Option<usize>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ViewCheck {
    pub(crate) view_name: String,
    pub(crate) expr: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ViewDmlRewriteError {
    UnsupportedViewShape(String),
    NestedUserRuleMix(String),
    RecursiveView(String),
    DeferredFeature(String),
    NonUpdatableColumn {
        column_name: String,
        reason: NonUpdatableViewColumnReason,
    },
    MultipleAssignments(String),
}

fn view_check_as_user_oid(catalog: &dyn CatalogLookup, view_oid: u32) -> Option<u32> {
    let class_row = catalog.class_row_by_oid(view_oid)?;
    let security_invoker = class_row.reloptions.as_ref().is_some_and(|options| {
        options.iter().any(|option| {
            let (name, value) = option
                .split_once('=')
                .map(|(name, value)| (name, value))
                .unwrap_or((option.as_str(), "true"));
            name.eq_ignore_ascii_case("security_invoker")
                && matches!(value.to_ascii_lowercase().as_str(), "true" | "on")
        })
    });
    (!security_invoker).then_some(class_row.relowner)
}

fn compose_column_maps(
    outer_to_inner: &[Option<usize>],
    inner_to_relation: &[Option<usize>],
) -> Vec<Option<usize>> {
    outer_to_inner
        .iter()
        .map(|inner| inner.and_then(|index| inner_to_relation.get(index).copied().flatten()))
        .collect()
}

impl ViewDmlRewriteError {
    pub(crate) fn detail(&self) -> String {
        match self {
            ViewDmlRewriteError::UnsupportedViewShape(detail)
            | ViewDmlRewriteError::NestedUserRuleMix(detail)
            | ViewDmlRewriteError::DeferredFeature(detail) => detail.clone(),
            ViewDmlRewriteError::RecursiveView(_) => RECURSIVE_DETAIL.into(),
            ViewDmlRewriteError::NonUpdatableColumn { reason, .. } => reason.detail().into(),
            ViewDmlRewriteError::MultipleAssignments(_) => String::new(),
        }
    }
}

pub(crate) fn resolve_auto_updatable_view_target(
    relation_oid: u32,
    relation_desc: &RelationDesc,
    event: ViewDmlEvent,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
) -> Result<ResolvedAutoViewTarget, ViewDmlRewriteError> {
    let display_name = relation_display_name(catalog, relation_oid);
    if expanded_views.contains(&relation_oid) {
        return Err(ViewDmlRewriteError::RecursiveView(display_name));
    }
    if !expanded_views.is_empty() && has_user_dml_rules(relation_oid, event, catalog) {
        return Err(ViewDmlRewriteError::NestedUserRuleMix(format!(
            "Views with user-defined {} rules on nested view \"{}\" are not automatically updatable.",
            event_name(event),
            display_name
        )));
    }

    let select = load_view_return_select(relation_oid, None, catalog, expanded_views)
        .map_err(map_parse_error)?;
    let query = load_view_return_query(relation_oid, relation_desc, None, catalog, expanded_views)
        .map_err(map_parse_error)?;
    let mut analyzed = analyze_simple_view_query(&select, &query, relation_desc)?;
    let Some(base_relation) = catalog
        .lookup_relation_by_oid(analyzed.base_relation_oid)
        .or_else(|| catalog.relation_by_oid(analyzed.base_relation_oid))
    else {
        return Err(ViewDmlRewriteError::UnsupportedViewShape(format!(
            "view \"{}\" references missing relation {}",
            display_name, analyzed.base_relation_oid
        )));
    };
    map_virtual_generated_view_columns(&mut analyzed, &base_relation.desc, catalog)?;

    if matches!(analyzed.base_relkind, 'r' | 'p') {
        let all_view_predicates = query
            .where_qual
            .clone()
            .map(|expr| {
                vec![ViewCheck {
                    view_name: display_name.clone(),
                    expr,
                }]
            })
            .unwrap_or_default();
        let mut view_check_options = Vec::new();
        if matches!(
            view_check_option(catalog, relation_oid),
            ViewCheckOption::Local | ViewCheckOption::Cascaded
        ) && let Some(predicate) = query.where_qual.clone()
        {
            view_check_options.push(ViewCheck {
                view_name: display_name.clone(),
                expr: predicate,
            });
        }
        return Ok(ResolvedAutoViewTarget {
            base_relation: base_relation.clone(),
            base_inh: analyzed.base_inh,
            visible_output_exprs: analyzed.output_exprs,
            combined_predicate: query.where_qual.clone(),
            updatable_column_map: analyzed.updatable_column_map.clone(),
            non_updatable_column_reasons: analyzed.non_updatable_column_reasons,
            privilege_contexts: vec![ViewPrivilegeContext {
                relation: base_relation,
                check_as_user_oid: view_check_as_user_oid(catalog, relation_oid),
                column_map: analyzed.updatable_column_map,
            }],
            all_view_predicates,
            view_check_options,
        });
    }
    if analyzed.base_relkind != 'v' {
        return Err(ViewDmlRewriteError::UnsupportedViewShape(
            SINGLE_RELATION_DETAIL.into(),
        ));
    }

    let mut next_views = expanded_views.to_vec();
    next_views.push(relation_oid);
    let nested_view_relation = base_relation.clone();
    let nested = resolve_auto_updatable_view_target(
        analyzed.base_relation_oid,
        &base_relation.desc,
        event,
        catalog,
        &next_views,
    )?;
    let ResolvedAutoViewTarget {
        base_relation,
        base_inh,
        visible_output_exprs: nested_visible_output_exprs,
        combined_predicate: nested_combined_predicate,
        updatable_column_map: nested_updatable_column_map,
        non_updatable_column_reasons: nested_non_updatable_column_reasons,
        privilege_contexts: nested_privilege_contexts,
        all_view_predicates: nested_all_view_predicates,
        view_check_options: nested_view_check_options,
    } = nested;
    let output_exprs = analyzed
        .output_exprs
        .into_iter()
        .map(|expr| {
            rewrite_local_vars_for_output_exprs(
                expr,
                analyzed.base_rtindex,
                &nested_visible_output_exprs,
            )
        })
        .collect::<Vec<_>>();
    let local_predicate = query.where_qual.clone().map(|expr| {
        rewrite_local_vars_for_output_exprs(
            expr,
            analyzed.base_rtindex,
            &nested_visible_output_exprs,
        )
    });
    let current_view_name = display_name.clone();
    let combined_predicate = and_predicates(local_predicate.clone(), nested_combined_predicate);
    let local_view_check = combined_predicate
        .as_ref()
        .and_then(|_| query.where_qual.as_ref())
        .map(|expr| ViewCheck {
            view_name: current_view_name,
            expr: rewrite_local_vars_for_output_exprs(
                expr.clone(),
                analyzed.base_rtindex,
                &nested_visible_output_exprs,
            ),
        });
    let current_updatable_column_map = analyzed.updatable_column_map.clone();
    let mut updatable_column_map = Vec::with_capacity(current_updatable_column_map.len());
    let mut non_updatable_column_reasons =
        Vec::with_capacity(analyzed.non_updatable_column_reasons.len());
    for (column, reason) in current_updatable_column_map
        .iter()
        .copied()
        .into_iter()
        .zip(analyzed.non_updatable_column_reasons.into_iter())
    {
        match column {
            Some(index) => {
                let nested_column = nested_updatable_column_map.get(index).copied().flatten();
                let nested_reason = nested_non_updatable_column_reasons
                    .get(index)
                    .copied()
                    .flatten();
                updatable_column_map.push(nested_column);
                non_updatable_column_reasons.push(if nested_column.is_some() {
                    None
                } else {
                    nested_reason.or(reason)
                });
            }
            None => {
                updatable_column_map.push(None);
                non_updatable_column_reasons.push(reason);
            }
        }
    }
    let mut all_view_predicates = nested_all_view_predicates;
    if let Some(local_check) = local_view_check.clone() {
        all_view_predicates.push(local_check);
    }
    let view_check_options = combine_view_checks(
        nested_view_check_options,
        &all_view_predicates,
        local_view_check,
        view_check_option(catalog, relation_oid),
    );
    let mut privilege_contexts = vec![ViewPrivilegeContext {
        relation: nested_view_relation,
        check_as_user_oid: view_check_as_user_oid(catalog, relation_oid),
        column_map: current_updatable_column_map.clone(),
    }];
    privilege_contexts.extend(nested_privilege_contexts.into_iter().map(|context| {
        ViewPrivilegeContext {
            column_map: compose_column_maps(&current_updatable_column_map, &context.column_map),
            ..context
        }
    }));

    Ok(ResolvedAutoViewTarget {
        base_relation,
        base_inh,
        visible_output_exprs: output_exprs,
        combined_predicate,
        updatable_column_map,
        non_updatable_column_reasons,
        privilege_contexts,
        all_view_predicates,
        view_check_options,
    })
}

fn combine_view_checks(
    nested_checks: Vec<ViewCheck>,
    all_view_predicates: &[ViewCheck],
    local_check: Option<ViewCheck>,
    check_option: ViewCheckOption,
) -> Vec<ViewCheck> {
    let mut checks = nested_checks;
    if matches!(check_option, ViewCheckOption::Cascaded) {
        for predicate in all_view_predicates {
            if !checks
                .iter()
                .any(|check| check.view_name == predicate.view_name)
            {
                checks.push(predicate.clone());
            }
        }
    } else if let Some(local_check) = local_check {
        if !checks
            .iter()
            .any(|check| check.view_name == local_check.view_name)
        {
            checks.push(local_check);
        }
    }
    checks
}

fn view_check_option(catalog: &dyn CatalogLookup, relation_oid: u32) -> ViewCheckOption {
    let sql = catalog
        .rewrite_rows_for_relation(relation_oid)
        .into_iter()
        .find(|row| row.rulename == "_RETURN")
        .map(|row| row.ev_action)
        .unwrap_or_default();
    crate::backend::rewrite::split_stored_view_definition_sql(&sql).1
}

fn map_virtual_generated_view_columns(
    analyzed: &mut SimpleViewAnalysis,
    base_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<(), ViewDmlRewriteError> {
    for (view_index, expr) in analyzed.output_exprs.iter().enumerate() {
        if analyzed
            .updatable_column_map
            .get(view_index)
            .copied()
            .flatten()
            .is_some()
        {
            continue;
        }
        let Some(base_index) = matching_virtual_generated_column(expr, base_desc, catalog)? else {
            continue;
        };
        analyzed.updatable_column_map[view_index] = Some(base_index);
        analyzed.non_updatable_column_reasons[view_index] = None;
    }
    Ok(())
}

fn matching_virtual_generated_column(
    expr: &Expr,
    base_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<Option<usize>, ViewDmlRewriteError> {
    for (column_index, column) in base_desc.columns.iter().enumerate() {
        if column.generated != Some(crate::backend::parser::ColumnGeneratedKind::Virtual) {
            continue;
        }
        let Some(generated_expr) =
            bind_generated_expr(base_desc, column_index, catalog).map_err(map_parse_error)?
        else {
            continue;
        };
        if expr == &generated_expr {
            return Ok(Some(column_index));
        }
    }
    Ok(None)
}

struct SimpleViewAnalysis {
    base_rtindex: usize,
    base_relation_oid: u32,
    base_relkind: char,
    base_inh: bool,
    output_exprs: Vec<Expr>,
    updatable_column_map: Vec<Option<usize>>,
    non_updatable_column_reasons: Vec<Option<NonUpdatableViewColumnReason>>,
}

fn analyze_simple_view_query(
    raw_select: &SelectStatement,
    query: &Query,
    relation_desc: &RelationDesc,
) -> Result<SimpleViewAnalysis, ViewDmlRewriteError> {
    if raw_select.distinct {
        return Err(unsupported(DISTINCT_DETAIL));
    }
    if !query.group_by.is_empty() {
        return Err(unsupported(GROUP_BY_DETAIL));
    }
    if query.having_qual.is_some() {
        return Err(unsupported(HAVING_DETAIL));
    }
    if raw_select.set_operation.is_some() || query.recursive_union.is_some() {
        return Err(unsupported(SET_OPERATION_DETAIL));
    }
    if !raw_select.with.is_empty() {
        return Err(unsupported(WITH_DETAIL));
    }
    if query.limit_count.is_some() || query.limit_offset.is_some() {
        return Err(unsupported(LIMIT_OFFSET_DETAIL));
    }
    if !query.accumulators.is_empty() {
        return Err(unsupported(AGGREGATE_DETAIL));
    }
    if !query.window_clauses.is_empty() {
        return Err(unsupported(WINDOW_DETAIL));
    }
    if query.has_target_srfs {
        return Err(unsupported(PROJECT_SET_DETAIL));
    }

    let Some(JoinTreeNode::RangeTblRef(base_rtindex)) = query.jointree.as_ref() else {
        return Err(unsupported(SINGLE_RELATION_DETAIL));
    };
    let Some(base_rte) = query.rtable.get(base_rtindex - 1) else {
        return Err(unsupported(SINGLE_RELATION_DETAIL));
    };
    let RangeTblEntryKind::Relation {
        relation_oid,
        relkind,
        tablesample,
        ..
    } = &base_rte.kind
    else {
        return Err(unsupported(SINGLE_RELATION_DETAIL));
    };
    if tablesample.is_some() {
        return Err(unsupported(TABLESAMPLE_DETAIL));
    }

    if query.target_list.len() != relation_desc.columns.len() {
        return Err(unsupported(TARGET_LIST_DETAIL));
    }

    let mut output_exprs = Vec::with_capacity(query.target_list.len());
    let mut updatable_column_map = Vec::with_capacity(query.target_list.len());
    let mut non_updatable_column_reasons = Vec::with_capacity(query.target_list.len());
    for target in &query.target_list {
        if target.resjunk {
            return Err(unsupported(TARGET_LIST_DETAIL));
        }
        output_exprs.push(target.expr.clone());
        match &target.expr {
            Expr::Var(var) if var.varlevelsup == 0 && var.varno == *base_rtindex => {
                if is_system_attr(var.varattno) {
                    updatable_column_map.push(None);
                    non_updatable_column_reasons
                        .push(Some(NonUpdatableViewColumnReason::SystemColumn));
                    continue;
                }
                let Some(column_index) = attrno_index(var.varattno) else {
                    updatable_column_map.push(None);
                    non_updatable_column_reasons
                        .push(Some(NonUpdatableViewColumnReason::NotBaseRelationColumn));
                    continue;
                };
                updatable_column_map.push(Some(column_index));
                non_updatable_column_reasons.push(None);
            }
            _ => {
                updatable_column_map.push(None);
                non_updatable_column_reasons
                    .push(Some(NonUpdatableViewColumnReason::NotBaseRelationColumn));
            }
        }
    }

    Ok(SimpleViewAnalysis {
        base_rtindex: *base_rtindex,
        base_relation_oid: *relation_oid,
        base_relkind: *relkind,
        base_inh: base_rte.inh,
        output_exprs,
        updatable_column_map,
        non_updatable_column_reasons,
    })
}

fn expr_contains_sublink(expr: &Expr) -> bool {
    match expr {
        Expr::SubLink(_) | Expr::SubPlan(_) => true,
        Expr::GroupingKey(grouping_key) => expr_contains_sublink(&grouping_key.expr),
        Expr::GroupingFunc(grouping_func) => grouping_func.args.iter().any(expr_contains_sublink),
        Expr::Op(op) => op.args.iter().any(expr_contains_sublink),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_contains_sublink),
        Expr::Func(func) => func.args.iter().any(expr_contains_sublink),
        Expr::SqlJsonQueryFunction(func) => {
            func.child_exprs().into_iter().any(expr_contains_sublink)
        }
        Expr::SetReturning(srf) => set_returning_call_exprs(&srf.call)
            .into_iter()
            .any(expr_contains_sublink),
        Expr::Aggref(aggref) => {
            aggref.args.iter().any(expr_contains_sublink)
                || aggref
                    .aggorder
                    .iter()
                    .any(|item| expr_contains_sublink(&item.expr))
                || aggref.aggfilter.as_ref().is_some_and(expr_contains_sublink)
        }
        Expr::WindowFunc(window) => window.args.iter().any(expr_contains_sublink),
        Expr::ScalarArrayOp(saop) => {
            expr_contains_sublink(&saop.left) || expr_contains_sublink(&saop.right)
        }
        Expr::Xml(xml) => xml.child_exprs().any(expr_contains_sublink),
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => expr_contains_sublink(inner),
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
            expr_contains_sublink(expr)
                || expr_contains_sublink(pattern)
                || escape
                    .as_ref()
                    .is_some_and(|expr| expr_contains_sublink(expr))
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => expr_contains_sublink(inner),
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_contains_sublink(left) || expr_contains_sublink(right)
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().any(expr_contains_sublink),
        Expr::Row { fields, .. } => fields.iter().any(|(_, expr)| expr_contains_sublink(expr)),
        Expr::FieldSelect { expr, .. } => expr_contains_sublink(expr),
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_sublink(array)
                || subscripts.iter().any(|subscript| {
                    subscript.lower.as_ref().is_some_and(expr_contains_sublink)
                        || subscript.upper.as_ref().is_some_and(expr_contains_sublink)
                })
        }
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_ref()
                .is_some_and(|expr| expr_contains_sublink(expr))
                || case_expr.args.iter().any(|arm| {
                    expr_contains_sublink(&arm.expr) || expr_contains_sublink(&arm.result)
                })
                || expr_contains_sublink(&case_expr.defresult)
        }
        Expr::Param(_)
        | Expr::Var(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::User
        | Expr::SystemUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}

fn has_user_dml_rules(relation_oid: u32, event: ViewDmlEvent, catalog: &dyn CatalogLookup) -> bool {
    catalog
        .rewrite_rows_for_relation(relation_oid)
        .into_iter()
        .any(|row| row.rulename != "_RETURN" && row.ev_type == event.rule_event_code())
}

fn relation_display_name(catalog: &dyn CatalogLookup, relation_oid: u32) -> String {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| format!("view {relation_oid}"))
}

fn unsupported(detail: &str) -> ViewDmlRewriteError {
    ViewDmlRewriteError::UnsupportedViewShape(detail.into())
}

fn map_parse_error(err: ParseError) -> ViewDmlRewriteError {
    match err {
        ParseError::RecursiveView(name) => ViewDmlRewriteError::RecursiveView(name),
        other => ViewDmlRewriteError::UnsupportedViewShape(other.to_string()),
    }
}

fn event_name(event: ViewDmlEvent) -> &'static str {
    match event {
        ViewDmlEvent::Insert => "INSERT",
        ViewDmlEvent::Update => "UPDATE",
        ViewDmlEvent::Delete => "DELETE",
    }
}

fn and_predicates(left: Option<Expr>, right: Option<Expr>) -> Option<Expr> {
    match (left, right) {
        (Some(left), Some(right)) => Some(Expr::and(left, right)),
        (Some(expr), None) | (None, Some(expr)) => Some(expr),
        (None, None) => None,
    }
}
