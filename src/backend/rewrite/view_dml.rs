use std::collections::BTreeSet;

use crate::backend::parser::{
    BoundRelation, CatalogLookup, ParseError, rewrite_local_vars_for_output_exprs,
};
use crate::include::nodes::parsenodes::{JoinTreeNode, Query, RangeTblEntryKind};
use crate::include::nodes::primnodes::{Expr, RelationDesc, attrno_index, is_system_attr};

use super::views::load_view_return_query;

const SINGLE_RELATION_DETAIL: &str =
    "Views that do not select from a single table or view are not automatically updatable.";
const GROUP_BY_DETAIL: &str = "Views containing GROUP BY are not automatically updatable.";
const HAVING_DETAIL: &str = "Views containing HAVING are not automatically updatable.";
const SET_OPERATION_DETAIL: &str =
    "Views containing UNION, INTERSECT, or EXCEPT are not automatically updatable.";
const LIMIT_OFFSET_DETAIL: &str =
    "Views containing LIMIT or OFFSET are not automatically updatable.";
const AGGREGATE_DETAIL: &str =
    "Views that return aggregate functions are not automatically updatable.";
const WINDOW_DETAIL: &str = "Views that return window functions are not automatically updatable.";
const PROJECT_SET_DETAIL: &str =
    "Views that return set-returning functions are not automatically updatable.";
const TARGET_LIST_DETAIL: &str =
    "Views that do not select simple base table columns are not automatically updatable.";
const RECURSIVE_DETAIL: &str =
    "Views that directly or indirectly reference themselves are not automatically updatable.";

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ViewDmlRewriteError {
    UnsupportedViewShape(String),
    NestedUserRuleMix(String),
    RecursiveView(String),
    DeferredFeature(String),
}

impl ViewDmlRewriteError {
    pub(crate) fn detail(&self) -> String {
        match self {
            ViewDmlRewriteError::UnsupportedViewShape(detail)
            | ViewDmlRewriteError::NestedUserRuleMix(detail)
            | ViewDmlRewriteError::DeferredFeature(detail) => detail.clone(),
            ViewDmlRewriteError::RecursiveView(_) => RECURSIVE_DETAIL.into(),
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

    let query = load_view_return_query(relation_oid, relation_desc, None, catalog, expanded_views)
        .map_err(map_parse_error)?;
    let analyzed = analyze_simple_view_query(&query, relation_desc)?;
    let Some(base_relation) = catalog
        .lookup_relation_by_oid(analyzed.base_relation_oid)
        .or_else(|| catalog.relation_by_oid(analyzed.base_relation_oid))
    else {
        return Err(ViewDmlRewriteError::UnsupportedViewShape(format!(
            "view \"{}\" references missing relation {}",
            display_name, analyzed.base_relation_oid
        )));
    };

    if analyzed.base_relkind == 'r' {
        return Ok(ResolvedAutoViewTarget {
            base_relation,
            base_inh: analyzed.base_inh,
            visible_output_exprs: analyzed.output_exprs,
            combined_predicate: query.where_qual.clone(),
            updatable_column_map: analyzed.updatable_column_map,
        });
    }
    if analyzed.base_relkind != 'v' {
        return Err(ViewDmlRewriteError::UnsupportedViewShape(
            SINGLE_RELATION_DETAIL.into(),
        ));
    }

    let mut next_views = expanded_views.to_vec();
    next_views.push(relation_oid);
    let nested = resolve_auto_updatable_view_target(
        analyzed.base_relation_oid,
        &base_relation.desc,
        event,
        catalog,
        &next_views,
    )?;
    let output_exprs = analyzed
        .output_exprs
        .into_iter()
        .map(|expr| {
            rewrite_local_vars_for_output_exprs(
                expr,
                analyzed.base_rtindex,
                &nested.visible_output_exprs,
            )
        })
        .collect::<Vec<_>>();
    let local_predicate = query.where_qual.map(|expr| {
        rewrite_local_vars_for_output_exprs(
            expr,
            analyzed.base_rtindex,
            &nested.visible_output_exprs,
        )
    });
    let combined_predicate = and_predicates(local_predicate, nested.combined_predicate);
    let updatable_column_map = analyzed
        .updatable_column_map
        .into_iter()
        .map(|column| {
            column.and_then(|index| nested.updatable_column_map.get(index).copied().flatten())
        })
        .collect();

    Ok(ResolvedAutoViewTarget {
        base_relation: nested.base_relation,
        base_inh: nested.base_inh,
        visible_output_exprs: output_exprs,
        combined_predicate,
        updatable_column_map,
    })
}

struct SimpleViewAnalysis {
    base_rtindex: usize,
    base_relation_oid: u32,
    base_relkind: char,
    base_inh: bool,
    output_exprs: Vec<Expr>,
    updatable_column_map: Vec<Option<usize>>,
}

fn analyze_simple_view_query(
    query: &Query,
    relation_desc: &RelationDesc,
) -> Result<SimpleViewAnalysis, ViewDmlRewriteError> {
    if !query.group_by.is_empty() {
        return Err(unsupported(GROUP_BY_DETAIL));
    }
    if query.having_qual.is_some() {
        return Err(unsupported(HAVING_DETAIL));
    }
    if query.set_operation.is_some() || query.recursive_union.is_some() {
        return Err(unsupported(SET_OPERATION_DETAIL));
    }
    if query.limit_count.is_some() || query.limit_offset != 0 {
        return Err(unsupported(LIMIT_OFFSET_DETAIL));
    }
    if !query.accumulators.is_empty() {
        return Err(unsupported(AGGREGATE_DETAIL));
    }
    if !query.window_clauses.is_empty() {
        return Err(unsupported(WINDOW_DETAIL));
    }
    if query.project_set.is_some() {
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
        ..
    } = &base_rte.kind
    else {
        return Err(unsupported(SINGLE_RELATION_DETAIL));
    };

    if query.target_list.len() != relation_desc.columns.len() {
        return Err(unsupported(TARGET_LIST_DETAIL));
    }

    let mut seen_columns = BTreeSet::new();
    let mut output_exprs = Vec::with_capacity(query.target_list.len());
    let mut updatable_column_map = Vec::with_capacity(query.target_list.len());
    for target in &query.target_list {
        if target.resjunk || expr_contains_sublink(&target.expr) {
            return Err(unsupported(TARGET_LIST_DETAIL));
        }
        let Expr::Var(var) = &target.expr else {
            return Err(unsupported(TARGET_LIST_DETAIL));
        };
        if var.varlevelsup != 0 || var.varno != *base_rtindex || is_system_attr(var.varattno) {
            return Err(unsupported(TARGET_LIST_DETAIL));
        }
        let Some(column_index) = attrno_index(var.varattno) else {
            return Err(unsupported(TARGET_LIST_DETAIL));
        };
        if !seen_columns.insert(column_index) {
            return Err(unsupported(TARGET_LIST_DETAIL));
        }
        output_exprs.push(target.expr.clone());
        updatable_column_map.push(Some(column_index));
    }

    if query.where_qual.as_ref().is_some_and(expr_contains_sublink) {
        return Err(unsupported(SINGLE_RELATION_DETAIL));
    }

    Ok(SimpleViewAnalysis {
        base_rtindex: *base_rtindex,
        base_relation_oid: *relation_oid,
        base_relkind: *relkind,
        base_inh: base_rte.inh,
        output_exprs,
        updatable_column_map,
    })
}

fn expr_contains_sublink(expr: &Expr) -> bool {
    match expr {
        Expr::SubLink(_) | Expr::SubPlan(_) => true,
        Expr::Op(op) => op.args.iter().any(expr_contains_sublink),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_contains_sublink),
        Expr::Func(func) => func.args.iter().any(expr_contains_sublink),
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
        Expr::Cast(inner, _) => expr_contains_sublink(inner),
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
        | Expr::CurrentUser
        | Expr::SessionUser
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
