use std::collections::BTreeSet;

use crate::RelFileLocator;
use crate::backend::executor::{Expr, Plan};
use crate::backend::parser::{CatalogLookup, FromItem, JoinConstraint, SqlExpr};
use crate::include::nodes::parsenodes::{JoinTreeNode, Query, RangeTblEntryKind};
use crate::include::nodes::plannodes::{BoundFromPlan, BoundSelectPlan, DeferredSelectPlan};

pub(super) fn collect_rels_from_expr(expr: &Expr, rels: &mut BTreeSet<RelFileLocator>) {
    match expr {
        Expr::Var(_)
        | Expr::Column(_)
        | Expr::OuterColumn { .. }
        | Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => {}
        Expr::UnaryPlus(inner)
        | Expr::Negate(inner)
        | Expr::BitNot(inner)
        | Expr::Cast(inner, _)
        | Expr::Not(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => collect_rels_from_expr(inner, rels),
        Expr::Add(left, right)
        | Expr::Coalesce(left, right)
        | Expr::Sub(left, right)
        | Expr::BitAnd(left, right)
        | Expr::BitOr(left, right)
        | Expr::BitXor(left, right)
        | Expr::Shl(left, right)
        | Expr::Shr(left, right)
        | Expr::Mul(left, right)
        | Expr::Div(left, right)
        | Expr::Mod(left, right)
        | Expr::Concat(left, right)
        | Expr::Eq(left, right)
        | Expr::NotEq(left, right)
        | Expr::Lt(left, right)
        | Expr::LtEq(left, right)
        | Expr::Gt(left, right)
        | Expr::GtEq(left, right)
        | Expr::RegexMatch(left, right)
        | Expr::And(left, right)
        | Expr::Or(left, right)
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::JsonGet(left, right)
        | Expr::JsonGetText(left, right)
        | Expr::JsonPath(left, right)
        | Expr::JsonPathText(left, right)
        | Expr::JsonbContains(left, right)
        | Expr::JsonbContained(left, right)
        | Expr::JsonbExists(left, right)
        | Expr::JsonbExistsAny(left, right)
        | Expr::JsonbExistsAll(left, right)
        | Expr::JsonbPathExists(left, right)
        | Expr::JsonbPathMatch(left, right) => {
            collect_rels_from_expr(left, rels);
            collect_rels_from_expr(right, rels);
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_rels_from_expr(expr, rels);
            collect_rels_from_expr(pattern, rels);
            if let Some(escape) = escape {
                collect_rels_from_expr(escape, rels);
            }
        }
        Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_rels_from_expr(expr, rels);
            collect_rels_from_expr(pattern, rels);
            if let Some(escape) = escape {
                collect_rels_from_expr(escape, rels);
            }
        }
        Expr::FuncCall { args, .. } => {
            for arg in args {
                collect_rels_from_expr(arg, rels);
            }
        }
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                collect_rels_from_expr(element, rels);
            }
        }
        Expr::ArrayOverlap(left, right) => {
            collect_rels_from_expr(left, rels);
            collect_rels_from_expr(right, rels);
        }
        Expr::ScalarSubquery(plan) | Expr::ExistsSubquery(plan) => {
            collect_rels_from_deferred_select_plan(plan, rels);
        }
        Expr::AnySubquery { left, subquery, .. } | Expr::AllSubquery { left, subquery, .. } => {
            collect_rels_from_expr(left, rels);
            collect_rels_from_deferred_select_plan(subquery, rels);
        }
        Expr::AnyArray { left, right, .. } | Expr::AllArray { left, right, .. } => {
            collect_rels_from_expr(left, rels);
            collect_rels_from_expr(right, rels);
        }
        Expr::ArraySubscript { array, subscripts } => {
            collect_rels_from_expr(array, rels);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_rels_from_expr(lower, rels);
                }
                if let Some(upper) = &subscript.upper {
                    collect_rels_from_expr(upper, rels);
                }
            }
        }
    }
}

fn collect_rels_from_deferred_select_plan(
    plan: &DeferredSelectPlan,
    rels: &mut BTreeSet<RelFileLocator>,
) {
    match plan {
        DeferredSelectPlan::Bound(plan) => collect_rels_from_query(plan, rels),
        DeferredSelectPlan::Planned(plan) => collect_rels_from_plan(plan, rels),
    }
}

fn collect_rels_from_query(query: &Query, rels: &mut BTreeSet<RelFileLocator>) {
    for rte in &query.rtable {
        match &rte.kind {
            RangeTblEntryKind::Result => {}
            RangeTblEntryKind::Relation { rel, .. } => {
                rels.insert(*rel);
            }
            RangeTblEntryKind::Values { rows, .. } => {
                for row in rows {
                    for expr in row {
                        collect_rels_from_expr(expr, rels);
                    }
                }
            }
            RangeTblEntryKind::Function { call } => collect_rels_from_set_returning_call(call, rels),
            RangeTblEntryKind::Subquery { query } => collect_rels_from_query(query, rels),
        }
    }
    if let Some(expr) = &query.where_qual {
        collect_rels_from_expr(expr, rels);
    }
    for expr in &query.group_by {
        collect_rels_from_expr(expr, rels);
    }
    if let Some(expr) = &query.having_qual {
        collect_rels_from_expr(expr, rels);
    }
    for item in &query.sort_clause {
        collect_rels_from_expr(&item.expr, rels);
    }
    for target in &query.target_list {
        collect_rels_from_expr(&target.expr, rels);
    }
    if let Some(targets) = &query.project_set {
        for target in targets {
            match target {
                crate::include::nodes::plannodes::ProjectSetTarget::Scalar(entry) => {
                    collect_rels_from_expr(&entry.expr, rels);
                }
                crate::include::nodes::plannodes::ProjectSetTarget::Set { call, .. } => {
                    collect_rels_from_set_returning_call(call, rels);
                }
            }
        }
    }
    if let Some(jointree) = &query.jointree {
        collect_rels_from_jointree(jointree, rels);
    }
}

fn collect_rels_from_jointree(jointree: &JoinTreeNode, rels: &mut BTreeSet<RelFileLocator>) {
    match jointree {
        JoinTreeNode::RangeTblRef(_) => {}
        JoinTreeNode::JoinExpr { left, right, quals, .. } => {
            collect_rels_from_jointree(left, rels);
            collect_rels_from_jointree(right, rels);
            collect_rels_from_expr(quals, rels);
        }
    }
}

fn collect_rels_from_bound_select_plan(
    plan: &BoundSelectPlan,
    rels: &mut BTreeSet<RelFileLocator>,
) {
    match plan {
        BoundSelectPlan::From(plan) => collect_rels_from_bound_from_plan(plan, rels),
        BoundSelectPlan::Filter { input, predicate } => {
            collect_rels_from_bound_select_plan(input, rels);
            collect_rels_from_expr(predicate, rels);
        }
        BoundSelectPlan::OrderBy { input, items } => {
            collect_rels_from_bound_select_plan(input, rels);
            for item in items {
                collect_rels_from_expr(&item.expr, rels);
            }
        }
        BoundSelectPlan::Limit { input, .. } => collect_rels_from_bound_select_plan(input, rels),
        BoundSelectPlan::Aggregate {
            input,
            group_by,
            accumulators,
            having,
            ..
        } => {
            collect_rels_from_bound_select_plan(input, rels);
            for expr in group_by {
                collect_rels_from_expr(expr, rels);
            }
            for accum in accumulators {
                for arg in &accum.args {
                    collect_rels_from_expr(arg, rels);
                }
            }
            if let Some(expr) = having {
                collect_rels_from_expr(expr, rels);
            }
        }
        BoundSelectPlan::Projection { input, targets } => {
            collect_rels_from_bound_select_plan(input, rels);
            for target in targets {
                collect_rels_from_expr(&target.expr, rels);
            }
        }
        BoundSelectPlan::ProjectSet { input, targets } => {
            collect_rels_from_bound_select_plan(input, rels);
            for target in targets {
                match target {
                    crate::include::nodes::plannodes::ProjectSetTarget::Scalar(entry) => {
                        collect_rels_from_expr(&entry.expr, rels);
                    }
                    crate::include::nodes::plannodes::ProjectSetTarget::Set { call, .. } => {
                        collect_rels_from_set_returning_call(call, rels);
                    }
                }
            }
        }
    }
}

fn collect_rels_from_bound_from_plan(plan: &BoundFromPlan, rels: &mut BTreeSet<RelFileLocator>) {
    match plan {
        BoundFromPlan::Result => {}
        BoundFromPlan::SeqScan { rel, .. } => {
            rels.insert(*rel);
        }
        BoundFromPlan::Values { rows, .. } => {
            for row in rows {
                for expr in row {
                    collect_rels_from_expr(expr, rels);
                }
            }
        }
        BoundFromPlan::FunctionScan { call } => collect_rels_from_set_returning_call(call, rels),
        BoundFromPlan::NestedLoopJoin {
            left, right, on, ..
        } => {
            collect_rels_from_bound_from_plan(left, rels);
            collect_rels_from_bound_from_plan(right, rels);
            collect_rels_from_expr(on, rels);
        }
        BoundFromPlan::Projection { input, targets } => {
            collect_rels_from_bound_from_plan(input, rels);
            for target in targets {
                collect_rels_from_expr(&target.expr, rels);
            }
        }
        BoundFromPlan::Subquery(plan) => collect_rels_from_query(plan, rels),
    }
}

fn collect_rels_from_set_returning_call(
    call: &crate::include::nodes::plannodes::SetReturningCall,
    rels: &mut BTreeSet<RelFileLocator>,
) {
    match call {
        crate::include::nodes::plannodes::SetReturningCall::GenerateSeries {
            start,
            stop,
            step,
            ..
        } => {
            collect_rels_from_expr(start, rels);
            collect_rels_from_expr(stop, rels);
            collect_rels_from_expr(step, rels);
        }
        crate::include::nodes::plannodes::SetReturningCall::Unnest { args, .. }
        | crate::include::nodes::plannodes::SetReturningCall::JsonTableFunction { args, .. }
        | crate::include::nodes::plannodes::SetReturningCall::RegexTableFunction { args, .. }
        | crate::include::nodes::plannodes::SetReturningCall::TextSearchTableFunction {
            args,
            ..
        } => {
            for arg in args {
                collect_rels_from_expr(arg, rels);
            }
        }
    }
}

pub(super) fn collect_rels_from_plan(plan: &Plan, rels: &mut BTreeSet<RelFileLocator>) {
    match plan {
        Plan::Result { .. } => {}
        Plan::SeqScan { rel, .. } | Plan::IndexScan { rel, .. } => {
            rels.insert(*rel);
        }
        Plan::NestedLoopJoin {
            left, right, on, ..
        } => {
            collect_rels_from_plan(left, rels);
            collect_rels_from_plan(right, rels);
            collect_rels_from_expr(on, rels);
        }
        Plan::Filter {
            input, predicate, ..
        } => {
            collect_rels_from_plan(input, rels);
            collect_rels_from_expr(predicate, rels);
        }
        Plan::OrderBy { input, items, .. } => {
            collect_rels_from_plan(input, rels);
            for item in items {
                collect_rels_from_expr(&item.expr, rels);
            }
        }
        Plan::Limit { input, .. } => collect_rels_from_plan(input, rels),
        Plan::Projection { input, targets, .. } => {
            collect_rels_from_plan(input, rels);
            for target in targets {
                collect_rels_from_expr(&target.expr, rels);
            }
        }
        Plan::Aggregate {
            input,
            group_by,
            accumulators,
            having,
            ..
        } => {
            collect_rels_from_plan(input, rels);
            for expr in group_by {
                collect_rels_from_expr(expr, rels);
            }
            for accum in accumulators {
                for arg in &accum.args {
                    collect_rels_from_expr(arg, rels);
                }
            }
            if let Some(expr) = having {
                collect_rels_from_expr(expr, rels);
            }
        }
        Plan::FunctionScan { call, .. } => match call {
            crate::include::nodes::plannodes::SetReturningCall::GenerateSeries {
                start,
                stop,
                step,
                ..
            } => {
                collect_rels_from_expr(start, rels);
                collect_rels_from_expr(stop, rels);
                collect_rels_from_expr(step, rels);
            }
            crate::include::nodes::plannodes::SetReturningCall::Unnest { args, .. }
            | crate::include::nodes::plannodes::SetReturningCall::JsonTableFunction {
                args, ..
            }
            | crate::include::nodes::plannodes::SetReturningCall::RegexTableFunction {
                args, ..
            }
            | crate::include::nodes::plannodes::SetReturningCall::TextSearchTableFunction {
                args,
                ..
            } => {
                for arg in args {
                    collect_rels_from_expr(arg, rels);
                }
            }
        },
        Plan::Values { rows, .. } => {
            for row in rows {
                for expr in row {
                    collect_rels_from_expr(expr, rels);
                }
            }
        }
        Plan::ProjectSet { input, targets, .. } => {
            collect_rels_from_plan(input, rels);
            for target in targets {
                match target {
                    crate::include::nodes::plannodes::ProjectSetTarget::Scalar(entry) => {
                        collect_rels_from_expr(&entry.expr, rels);
                    }
                    crate::include::nodes::plannodes::ProjectSetTarget::Set { call, .. } => {
                        match call {
                            crate::include::nodes::plannodes::SetReturningCall::GenerateSeries {
                                start,
                                stop,
                                step,
                                ..
                            } => {
                                collect_rels_from_expr(start, rels);
                                collect_rels_from_expr(stop, rels);
                                collect_rels_from_expr(step, rels);
                            }
                            crate::include::nodes::plannodes::SetReturningCall::Unnest {
                                args, ..
                            }
                            | crate::include::nodes::plannodes::SetReturningCall::JsonTableFunction {
                                args, ..
                            }
                            | crate::include::nodes::plannodes::SetReturningCall::RegexTableFunction {
                                args, ..
                            }
                            | crate::include::nodes::plannodes::SetReturningCall::TextSearchTableFunction {
                                args, ..
                            } => {
                                for arg in args {
                                    collect_rels_from_expr(arg, rels);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

pub(super) fn collect_direct_relation_oids_from_select(
    select: &crate::backend::parser::SelectStatement,
    catalog: &dyn CatalogLookup,
    visible_ctes: &mut Vec<String>,
    rels: &mut BTreeSet<u32>,
) {
    let cte_base = visible_ctes.len();
    for cte in &select.with {
        match &cte.body {
            crate::backend::parser::CteBody::Select(subquery) => {
                collect_direct_relation_oids_from_select(subquery, catalog, visible_ctes, rels);
            }
            crate::backend::parser::CteBody::Values(values) => {
                collect_direct_relation_oids_from_values(values, catalog, visible_ctes, rels);
            }
        }
        visible_ctes.push(cte.name.to_ascii_lowercase());
    }

    if let Some(from) = &select.from {
        collect_direct_relation_oids_from_from_item(from, catalog, visible_ctes, rels);
    }
    for target in &select.targets {
        collect_direct_relation_oids_from_sql_expr(&target.expr, catalog, visible_ctes, rels);
    }
    if let Some(expr) = &select.where_clause {
        collect_direct_relation_oids_from_sql_expr(expr, catalog, visible_ctes, rels);
    }
    for expr in &select.group_by {
        collect_direct_relation_oids_from_sql_expr(expr, catalog, visible_ctes, rels);
    }
    if let Some(expr) = &select.having {
        collect_direct_relation_oids_from_sql_expr(expr, catalog, visible_ctes, rels);
    }
    for item in &select.order_by {
        collect_direct_relation_oids_from_sql_expr(&item.expr, catalog, visible_ctes, rels);
    }

    visible_ctes.truncate(cte_base);
}

fn collect_direct_relation_oids_from_values(
    values: &crate::backend::parser::ValuesStatement,
    catalog: &dyn CatalogLookup,
    visible_ctes: &mut Vec<String>,
    rels: &mut BTreeSet<u32>,
) {
    let cte_base = visible_ctes.len();
    for cte in &values.with {
        match &cte.body {
            crate::backend::parser::CteBody::Select(subquery) => {
                collect_direct_relation_oids_from_select(subquery, catalog, visible_ctes, rels);
            }
            crate::backend::parser::CteBody::Values(inner) => {
                collect_direct_relation_oids_from_values(inner, catalog, visible_ctes, rels);
            }
        }
        visible_ctes.push(cte.name.to_ascii_lowercase());
    }
    for row in &values.rows {
        for expr in row {
            collect_direct_relation_oids_from_sql_expr(expr, catalog, visible_ctes, rels);
        }
    }
    for item in &values.order_by {
        collect_direct_relation_oids_from_sql_expr(&item.expr, catalog, visible_ctes, rels);
    }
    visible_ctes.truncate(cte_base);
}

fn collect_direct_relation_oids_from_from_item(
    from: &FromItem,
    catalog: &dyn CatalogLookup,
    visible_ctes: &mut Vec<String>,
    rels: &mut BTreeSet<u32>,
) {
    match from {
        FromItem::Table { name } => {
            if !name.contains('.')
                && visible_ctes
                    .iter()
                    .any(|cte| cte == &name.to_ascii_lowercase())
            {
                return;
            }
            if let Some(entry) = catalog.lookup_any_relation(name) {
                rels.insert(entry.relation_oid);
            }
        }
        FromItem::Values { rows } => {
            for row in rows {
                for expr in row {
                    collect_direct_relation_oids_from_sql_expr(expr, catalog, visible_ctes, rels);
                }
            }
        }
        FromItem::FunctionCall { args, .. } => {
            for arg in args {
                collect_direct_relation_oids_from_sql_expr(&arg.value, catalog, visible_ctes, rels);
            }
        }
        FromItem::DerivedTable(select) => {
            collect_direct_relation_oids_from_select(select, catalog, visible_ctes, rels);
        }
        FromItem::Join {
            left,
            right,
            constraint,
            ..
        } => {
            collect_direct_relation_oids_from_from_item(left, catalog, visible_ctes, rels);
            collect_direct_relation_oids_from_from_item(right, catalog, visible_ctes, rels);
            if let JoinConstraint::On(expr) = constraint {
                collect_direct_relation_oids_from_sql_expr(expr, catalog, visible_ctes, rels);
            }
        }
        FromItem::Alias { source, .. } => {
            collect_direct_relation_oids_from_from_item(source, catalog, visible_ctes, rels);
        }
    }
}

fn collect_direct_relation_oids_from_sql_expr(
    expr: &SqlExpr,
    catalog: &dyn CatalogLookup,
    visible_ctes: &mut Vec<String>,
    rels: &mut BTreeSet<u32>,
) {
    match expr {
        SqlExpr::Column(_)
        | SqlExpr::Default
        | SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::Random
        | SqlExpr::CurrentDate
        | SqlExpr::CurrentTime { .. }
        | SqlExpr::CurrentTimestamp { .. }
        | SqlExpr::LocalTime { .. }
        | SqlExpr::LocalTimestamp { .. } => {}
        SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::BitNot(inner)
        | SqlExpr::Subscript { expr: inner, .. }
        | SqlExpr::PrefixOperator { expr: inner, .. }
        | SqlExpr::Cast(inner, _)
        | SqlExpr::Not(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::FieldSelect { expr: inner, .. } => {
            collect_direct_relation_oids_from_sql_expr(inner, catalog, visible_ctes, rels);
        }
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
        | SqlExpr::BinaryOperator { left, right, .. }
        | SqlExpr::GeometryBinaryOp { left, right, .. }
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
        | SqlExpr::ArrayOverlap(left, right)
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
        | SqlExpr::JsonPathText(left, right) => {
            collect_direct_relation_oids_from_sql_expr(left, catalog, visible_ctes, rels);
            collect_direct_relation_oids_from_sql_expr(right, catalog, visible_ctes, rels);
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
            collect_direct_relation_oids_from_sql_expr(expr, catalog, visible_ctes, rels);
            collect_direct_relation_oids_from_sql_expr(pattern, catalog, visible_ctes, rels);
            if let Some(escape) = escape {
                collect_direct_relation_oids_from_sql_expr(escape, catalog, visible_ctes, rels);
            }
        }
        SqlExpr::ArrayLiteral(elements) => {
            for element in elements {
                collect_direct_relation_oids_from_sql_expr(element, catalog, visible_ctes, rels);
            }
        }
        SqlExpr::AggCall { args, .. } | SqlExpr::FuncCall { args, .. } => {
            for arg in args {
                collect_direct_relation_oids_from_sql_expr(&arg.value, catalog, visible_ctes, rels);
            }
        }
        SqlExpr::ScalarSubquery(select) | SqlExpr::Exists(select) => {
            collect_direct_relation_oids_from_select(select, catalog, visible_ctes, rels);
        }
        SqlExpr::InSubquery { expr, subquery, .. } => {
            collect_direct_relation_oids_from_sql_expr(expr, catalog, visible_ctes, rels);
            collect_direct_relation_oids_from_select(subquery, catalog, visible_ctes, rels);
        }
        SqlExpr::QuantifiedSubquery { left, subquery, .. } => {
            collect_direct_relation_oids_from_sql_expr(left, catalog, visible_ctes, rels);
            collect_direct_relation_oids_from_select(subquery, catalog, visible_ctes, rels);
        }
        SqlExpr::QuantifiedArray { left, array, .. } => {
            collect_direct_relation_oids_from_sql_expr(left, catalog, visible_ctes, rels);
            collect_direct_relation_oids_from_sql_expr(array, catalog, visible_ctes, rels);
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            collect_direct_relation_oids_from_sql_expr(array, catalog, visible_ctes, rels);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_direct_relation_oids_from_sql_expr(lower, catalog, visible_ctes, rels);
                }
                if let Some(upper) = &subscript.upper {
                    collect_direct_relation_oids_from_sql_expr(upper, catalog, visible_ctes, rels);
                }
            }
        }
        SqlExpr::GeometryUnaryOp { expr, .. } => {
            collect_direct_relation_oids_from_sql_expr(expr, catalog, visible_ctes, rels);
        }
    }
}
