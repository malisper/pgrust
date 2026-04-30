use std::collections::BTreeSet;

use crate::RelFileLocator;
use crate::backend::executor::{Expr, Plan};
use crate::backend::parser::{CatalogLookup, FromItem, JoinConstraint, SqlExpr};
use crate::include::nodes::parsenodes::{JoinTreeNode, Query, RangeTblEntryKind};
use crate::include::nodes::plannodes::PlannedStmt;
use crate::include::nodes::primnodes::{RowsFromSource, set_returning_call_exprs};

pub(super) fn collect_rels_from_expr(expr: &Expr, rels: &mut BTreeSet<RelFileLocator>) {
    match expr {
        Expr::Aggref(aggref) => {
            for arg in &aggref.args {
                collect_rels_from_expr(arg, rels);
            }
            for item in &aggref.aggorder {
                collect_rels_from_expr(&item.expr, rels);
            }
            if let Some(filter) = aggref.aggfilter.as_ref() {
                collect_rels_from_expr(filter, rels);
            }
        }
        Expr::WindowFunc(window_func) => {
            for arg in &window_func.args {
                collect_rels_from_expr(arg, rels);
            }
            if let crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) =
                &window_func.kind
            {
                for item in &aggref.aggorder {
                    collect_rels_from_expr(&item.expr, rels);
                }
                if let Some(filter) = aggref.aggfilter.as_ref() {
                    collect_rels_from_expr(filter, rels);
                }
            }
        }
        Expr::Op(op) => {
            for arg in &op.args {
                collect_rels_from_expr(arg, rels);
            }
        }
        Expr::Bool(bool_expr) => {
            for arg in &bool_expr.args {
                collect_rels_from_expr(arg, rels);
            }
        }
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                collect_rels_from_expr(arg, rels);
            }
            for arm in &case_expr.args {
                collect_rels_from_expr(&arm.expr, rels);
                collect_rels_from_expr(&arm.result, rels);
            }
            collect_rels_from_expr(&case_expr.defresult, rels);
        }
        Expr::CaseTest(_) => {}
        Expr::Func(func) => {
            for arg in &func.args {
                collect_rels_from_expr(arg, rels);
            }
        }
        Expr::SetReturning(srf) => {
            for arg in set_returning_call_exprs(&srf.call) {
                collect_rels_from_expr(arg, rels);
            }
        }
        Expr::ScalarArrayOp(saop) => {
            collect_rels_from_expr(&saop.left, rels);
            collect_rels_from_expr(&saop.right, rels);
        }
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = &sublink.testexpr {
                collect_rels_from_expr(testexpr, rels);
            }
            collect_rels_from_query(&sublink.subselect, rels);
        }
        Expr::SubPlan(subplan) => {
            if let Some(testexpr) = &subplan.testexpr {
                collect_rels_from_expr(testexpr, rels);
            }
        }
        Expr::Var(_)
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
        | Expr::LocalTimestamp { .. } => {}
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => collect_rels_from_expr(inner, rels),
        Expr::Coalesce(left, right)
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
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
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                collect_rels_from_expr(element, rels);
            }
        }
        Expr::Row { fields, .. } => {
            for (_, expr) in fields {
                collect_rels_from_expr(expr, rels);
            }
        }
        Expr::FieldSelect { expr, .. } => collect_rels_from_expr(expr, rels),
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
        Expr::Xml(xml) => {
            for child in xml.child_exprs() {
                collect_rels_from_expr(child, rels);
            }
        }
        Expr::SqlJsonQueryFunction(func) => {
            for child in func.child_exprs() {
                collect_rels_from_expr(child, rels);
            }
        }
    }
}

fn collect_rels_from_query(query: &Query, rels: &mut BTreeSet<RelFileLocator>) {
    for rte in &query.rtable {
        match &rte.kind {
            RangeTblEntryKind::Result => {}
            RangeTblEntryKind::Relation { rel, .. } => {
                rels.insert(*rel);
            }
            RangeTblEntryKind::Join { joinaliasvars, .. } => {
                for expr in joinaliasvars {
                    collect_rels_from_expr(expr, rels);
                }
            }
            RangeTblEntryKind::Values { rows, .. } => {
                for row in rows {
                    for expr in row {
                        collect_rels_from_expr(expr, rels);
                    }
                }
            }
            RangeTblEntryKind::Function { call } => {
                collect_rels_from_set_returning_call(call, rels)
            }
            RangeTblEntryKind::WorkTable { .. } => {}
            RangeTblEntryKind::Cte { query, .. } => collect_rels_from_query(query, rels),
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
    if let Some(recursive_union) = &query.recursive_union {
        collect_rels_from_query(&recursive_union.anchor, rels);
        collect_rels_from_query(&recursive_union.recursive, rels);
    }
    if let Some(jointree) = &query.jointree {
        collect_rels_from_jointree(jointree, rels);
    }
}

fn collect_rels_from_jointree(jointree: &JoinTreeNode, rels: &mut BTreeSet<RelFileLocator>) {
    match jointree {
        JoinTreeNode::RangeTblRef(_) => {}
        JoinTreeNode::JoinExpr {
            left, right, quals, ..
        } => {
            collect_rels_from_jointree(left, rels);
            collect_rels_from_jointree(right, rels);
            collect_rels_from_expr(quals, rels);
        }
    }
}

fn collect_rels_from_set_returning_call(
    call: &crate::include::nodes::primnodes::SetReturningCall,
    rels: &mut BTreeSet<RelFileLocator>,
) {
    match call {
        crate::include::nodes::primnodes::SetReturningCall::RowsFrom { items, .. } => {
            for item in items {
                match &item.source {
                    RowsFromSource::Function(call) => {
                        collect_rels_from_set_returning_call(call, rels)
                    }
                    RowsFromSource::Project { output_exprs, .. } => {
                        for expr in output_exprs {
                            collect_rels_from_expr(expr, rels);
                        }
                    }
                }
            }
        }
        crate::include::nodes::primnodes::SetReturningCall::GenerateSeries {
            start,
            stop,
            step,
            timezone,
            ..
        } => {
            collect_rels_from_expr(start, rels);
            collect_rels_from_expr(stop, rels);
            collect_rels_from_expr(step, rels);
            if let Some(timezone) = timezone {
                collect_rels_from_expr(timezone, rels);
            }
        }
        crate::include::nodes::primnodes::SetReturningCall::GenerateSubscripts {
            array,
            dimension,
            reverse,
            ..
        } => {
            collect_rels_from_expr(array, rels);
            collect_rels_from_expr(dimension, rels);
            if let Some(reverse) = reverse {
                collect_rels_from_expr(reverse, rels);
            }
        }
        crate::include::nodes::primnodes::SetReturningCall::PartitionTree { relid, .. }
        | crate::include::nodes::primnodes::SetReturningCall::PartitionAncestors {
            relid, ..
        } => {
            collect_rels_from_expr(relid, rels);
        }
        crate::include::nodes::primnodes::SetReturningCall::PgLockStatus { .. }
        | crate::include::nodes::primnodes::SetReturningCall::PgSequences { .. }
        | crate::include::nodes::primnodes::SetReturningCall::InformationSchemaSequences {
            ..
        } => {}
        crate::include::nodes::primnodes::SetReturningCall::TxidSnapshotXip { arg, .. } => {
            collect_rels_from_expr(arg, rels);
        }
        crate::include::nodes::primnodes::SetReturningCall::Unnest { args, .. }
        | crate::include::nodes::primnodes::SetReturningCall::JsonTableFunction { args, .. }
        | crate::include::nodes::primnodes::SetReturningCall::JsonRecordFunction { args, .. }
        | crate::include::nodes::primnodes::SetReturningCall::RegexTableFunction { args, .. }
        | crate::include::nodes::primnodes::SetReturningCall::StringTableFunction {
            args, ..
        }
        | crate::include::nodes::primnodes::SetReturningCall::TextSearchTableFunction {
            args,
            ..
        }
        | crate::include::nodes::primnodes::SetReturningCall::UserDefined { args, .. } => {
            for arg in args {
                collect_rels_from_expr(arg, rels);
            }
        }
        crate::include::nodes::primnodes::SetReturningCall::SqlJsonTable(_)
        | crate::include::nodes::primnodes::SetReturningCall::SqlXmlTable(_) => {
            for arg in set_returning_call_exprs(call) {
                collect_rels_from_expr(arg, rels);
            }
        }
    }
}

pub(super) fn collect_rels_from_plan(plan: &Plan, rels: &mut BTreeSet<RelFileLocator>) {
    match plan {
        Plan::Result { .. } | Plan::WorkTableScan { .. } => {}
        Plan::Append { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::SetOp { children, .. } => {
            for child in children {
                collect_rels_from_plan(child, rels);
            }
        }
        Plan::Unique { input, .. } => collect_rels_from_plan(input, rels),
        Plan::SeqScan { rel, .. }
        | Plan::IndexOnlyScan { rel, .. }
        | Plan::IndexScan { rel, .. }
        | Plan::BitmapIndexScan { rel, .. } => {
            rels.insert(*rel);
        }
        Plan::BitmapHeapScan {
            rel, bitmapqual, ..
        } => {
            rels.insert(*rel);
            collect_rels_from_plan(bitmapqual, rels);
        }
        Plan::Hash {
            input, hash_keys, ..
        } => {
            collect_rels_from_plan(input, rels);
            for expr in hash_keys {
                collect_rels_from_expr(expr, rels);
            }
        }
        Plan::Materialize { input, .. } => collect_rels_from_plan(input, rels),
        Plan::Memoize {
            input, cache_keys, ..
        } => {
            collect_rels_from_plan(input, rels);
            for expr in cache_keys {
                collect_rels_from_expr(expr, rels);
            }
        }
        Plan::Gather { input, .. } => collect_rels_from_plan(input, rels),
        Plan::NestedLoopJoin {
            left,
            right,
            join_qual,
            qual,
            ..
        } => {
            collect_rels_from_plan(left, rels);
            collect_rels_from_plan(right, rels);
            for expr in join_qual {
                collect_rels_from_expr(expr, rels);
            }
            for expr in qual {
                collect_rels_from_expr(expr, rels);
            }
        }
        Plan::HashJoin {
            left,
            right,
            hash_clauses,
            hash_keys,
            join_qual,
            qual,
            ..
        } => {
            collect_rels_from_plan(left, rels);
            collect_rels_from_plan(right, rels);
            for expr in hash_clauses {
                collect_rels_from_expr(expr, rels);
            }
            for expr in hash_keys {
                collect_rels_from_expr(expr, rels);
            }
            for expr in join_qual {
                collect_rels_from_expr(expr, rels);
            }
            for expr in qual {
                collect_rels_from_expr(expr, rels);
            }
        }
        Plan::MergeJoin {
            left,
            right,
            merge_clauses,
            outer_merge_keys,
            inner_merge_keys,
            join_qual,
            qual,
            ..
        } => {
            collect_rels_from_plan(left, rels);
            collect_rels_from_plan(right, rels);
            for expr in merge_clauses {
                collect_rels_from_expr(expr, rels);
            }
            for expr in outer_merge_keys {
                collect_rels_from_expr(expr, rels);
            }
            for expr in inner_merge_keys {
                collect_rels_from_expr(expr, rels);
            }
            for expr in join_qual {
                collect_rels_from_expr(expr, rels);
            }
            for expr in qual {
                collect_rels_from_expr(expr, rels);
            }
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
        Plan::IncrementalSort { input, items, .. } => {
            collect_rels_from_plan(input, rels);
            for item in items {
                collect_rels_from_expr(&item.expr, rels);
            }
        }
        Plan::WindowAgg { input, clause, .. } => {
            collect_rels_from_plan(input, rels);
            for expr in &clause.spec.partition_by {
                collect_rels_from_expr(expr, rels);
            }
            for item in &clause.spec.order_by {
                collect_rels_from_expr(&item.expr, rels);
            }
            for func in &clause.functions {
                for arg in &func.args {
                    collect_rels_from_expr(arg, rels);
                }
                if let crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) =
                    &func.kind
                {
                    for item in &aggref.aggorder {
                        collect_rels_from_expr(&item.expr, rels);
                    }
                    if let Some(filter) = aggref.aggfilter.as_ref() {
                        collect_rels_from_expr(filter, rels);
                    }
                }
            }
        }
        Plan::Limit { input, .. } | Plan::LockRows { input, .. } => {
            collect_rels_from_plan(input, rels)
        }
        Plan::Projection { input, targets, .. } => {
            collect_rels_from_plan(input, rels);
            for target in targets {
                collect_rels_from_expr(&target.expr, rels);
            }
        }
        Plan::Aggregate {
            input,
            group_by,
            passthrough_exprs,
            accumulators,
            having,
            ..
        } => {
            collect_rels_from_plan(input, rels);
            for expr in group_by {
                collect_rels_from_expr(expr, rels);
            }
            for expr in passthrough_exprs {
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
        Plan::FunctionScan { call, .. } => collect_rels_from_set_returning_call(call, rels),
        Plan::SubqueryScan { input, .. } => {
            collect_rels_from_plan(input, rels);
        }
        Plan::CteScan { cte_plan, .. } => {
            collect_rels_from_plan(cte_plan, rels);
        }
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => {
            collect_rels_from_plan(anchor, rels);
            collect_rels_from_plan(recursive, rels);
        }
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
                    crate::include::nodes::primnodes::ProjectSetTarget::Scalar(entry) => {
                        collect_rels_from_expr(&entry.expr, rels);
                    }
                    crate::include::nodes::primnodes::ProjectSetTarget::Set { call, .. } => {
                        collect_rels_from_set_returning_call(call, rels);
                    }
                }
            }
        }
    }
}

pub(super) fn collect_rels_from_planned_stmt(
    planned_stmt: &PlannedStmt,
    rels: &mut BTreeSet<RelFileLocator>,
) {
    collect_rels_from_plan(&planned_stmt.plan_tree, rels);
    for subplan in &planned_stmt.subplans {
        collect_rels_from_plan(subplan, rels);
    }
}

pub(crate) fn collect_direct_relation_oids_from_sql_exprs<'a>(
    exprs: impl IntoIterator<Item = &'a SqlExpr>,
    catalog: &dyn CatalogLookup,
) -> BTreeSet<u32> {
    let mut rels = BTreeSet::new();
    let mut visible_ctes = Vec::new();
    for expr in exprs {
        collect_direct_relation_oids_from_sql_expr(expr, catalog, &mut visible_ctes, &mut rels);
    }
    rels
}

pub(super) fn collect_direct_relation_oids_from_select(
    select: &crate::backend::parser::SelectStatement,
    catalog: &dyn CatalogLookup,
    visible_ctes: &mut Vec<String>,
    rels: &mut BTreeSet<u32>,
) {
    let cte_base = visible_ctes.len();
    for cte in &select.with {
        collect_direct_relation_oids_from_cte_body(&cte.body, catalog, visible_ctes, rels);
        visible_ctes.push(cte.name.to_ascii_lowercase());
    }

    if let Some(set_operation) = &select.set_operation {
        for input in &set_operation.inputs {
            collect_direct_relation_oids_from_select(input, catalog, visible_ctes, rels);
        }
        for item in &select.order_by {
            collect_direct_relation_oids_from_sql_expr(&item.expr, catalog, visible_ctes, rels);
        }
        visible_ctes.truncate(cte_base);
        return;
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
        collect_direct_relation_oids_from_cte_body(&cte.body, catalog, visible_ctes, rels);
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

fn collect_direct_relation_oids_from_cte_body(
    body: &crate::backend::parser::CteBody,
    catalog: &dyn CatalogLookup,
    visible_ctes: &mut Vec<String>,
    rels: &mut BTreeSet<u32>,
) {
    match body {
        crate::backend::parser::CteBody::Select(subquery) => {
            collect_direct_relation_oids_from_select(subquery, catalog, visible_ctes, rels);
        }
        crate::backend::parser::CteBody::Values(values) => {
            collect_direct_relation_oids_from_values(values, catalog, visible_ctes, rels);
        }
        crate::backend::parser::CteBody::Insert(insert) => {
            collect_direct_relation_oids_from_insert(insert, catalog, visible_ctes, rels);
        }
        crate::backend::parser::CteBody::Update(update) => {
            collect_direct_relation_oids_from_update(update, catalog, visible_ctes, rels);
        }
        crate::backend::parser::CteBody::Merge(merge) => {
            collect_direct_relation_oids_from_merge(merge, catalog, visible_ctes, rels);
        }
        crate::backend::parser::CteBody::RecursiveUnion {
            anchor, recursive, ..
        } => {
            collect_direct_relation_oids_from_cte_body(anchor, catalog, visible_ctes, rels);
            collect_direct_relation_oids_from_select(recursive, catalog, visible_ctes, rels);
        }
    }
}

fn collect_direct_relation_oids_from_merge(
    merge: &crate::backend::parser::MergeStatement,
    catalog: &dyn CatalogLookup,
    visible_ctes: &mut Vec<String>,
    rels: &mut BTreeSet<u32>,
) {
    let cte_base = visible_ctes.len();
    for cte in &merge.with {
        collect_direct_relation_oids_from_cte_body(&cte.body, catalog, visible_ctes, rels);
        visible_ctes.push(cte.name.to_ascii_lowercase());
    }
    if let Some(entry) = catalog.lookup_any_relation(&merge.target_table) {
        rels.insert(entry.relation_oid);
    }
    collect_direct_relation_oids_from_from_item(&merge.source, catalog, visible_ctes, rels);
    collect_direct_relation_oids_from_sql_expr(&merge.join_condition, catalog, visible_ctes, rels);
    for clause in &merge.when_clauses {
        if let Some(condition) = &clause.condition {
            collect_direct_relation_oids_from_sql_expr(condition, catalog, visible_ctes, rels);
        }
        match &clause.action {
            crate::backend::parser::MergeAction::Update { assignments } => {
                for assignment in assignments {
                    collect_direct_relation_oids_from_sql_expr(
                        &assignment.expr,
                        catalog,
                        visible_ctes,
                        rels,
                    );
                }
            }
            crate::backend::parser::MergeAction::Insert { source, .. } => {
                if let crate::backend::parser::MergeInsertSource::Values(values) = source {
                    for expr in values {
                        collect_direct_relation_oids_from_sql_expr(
                            expr,
                            catalog,
                            visible_ctes,
                            rels,
                        );
                    }
                }
            }
            crate::backend::parser::MergeAction::Delete
            | crate::backend::parser::MergeAction::DoNothing => {}
        }
    }
    for item in &merge.returning {
        collect_direct_relation_oids_from_sql_expr(&item.expr, catalog, visible_ctes, rels);
    }
    visible_ctes.truncate(cte_base);
}

fn collect_direct_relation_oids_from_insert(
    insert: &crate::backend::parser::InsertStatement,
    catalog: &dyn CatalogLookup,
    visible_ctes: &mut Vec<String>,
    rels: &mut BTreeSet<u32>,
) {
    let cte_base = visible_ctes.len();
    for cte in &insert.with {
        collect_direct_relation_oids_from_cte_body(&cte.body, catalog, visible_ctes, rels);
        visible_ctes.push(cte.name.to_ascii_lowercase());
    }
    if let Some(entry) = catalog.lookup_any_relation(&insert.table_name) {
        rels.insert(entry.relation_oid);
    }
    match &insert.source {
        crate::backend::parser::InsertSource::Values(rows) => {
            for expr in rows.iter().flatten() {
                collect_direct_relation_oids_from_sql_expr(expr, catalog, visible_ctes, rels);
            }
        }
        crate::backend::parser::InsertSource::DefaultValues => {}
        crate::backend::parser::InsertSource::Select(select) => {
            collect_direct_relation_oids_from_select(select, catalog, visible_ctes, rels);
        }
    }
    if let Some(on_conflict) = &insert.on_conflict {
        if let Some(crate::backend::parser::OnConflictTarget::Inference(spec)) = &on_conflict.target
        {
            for elem in &spec.elements {
                collect_direct_relation_oids_from_sql_expr(&elem.expr, catalog, visible_ctes, rels);
            }
            if let Some(predicate) = &spec.predicate {
                collect_direct_relation_oids_from_sql_expr(predicate, catalog, visible_ctes, rels);
            }
        }
        for assignment in &on_conflict.assignments {
            collect_direct_relation_oids_from_sql_expr(
                &assignment.expr,
                catalog,
                visible_ctes,
                rels,
            );
        }
        if let Some(where_clause) = &on_conflict.where_clause {
            collect_direct_relation_oids_from_sql_expr(where_clause, catalog, visible_ctes, rels);
        }
    }
    for item in &insert.returning {
        collect_direct_relation_oids_from_sql_expr(&item.expr, catalog, visible_ctes, rels);
    }
    visible_ctes.truncate(cte_base);
}

fn collect_direct_relation_oids_from_update(
    update: &crate::backend::parser::UpdateStatement,
    catalog: &dyn CatalogLookup,
    visible_ctes: &mut Vec<String>,
    rels: &mut BTreeSet<u32>,
) {
    let cte_base = visible_ctes.len();
    for cte in &update.with {
        collect_direct_relation_oids_from_cte_body(&cte.body, catalog, visible_ctes, rels);
        visible_ctes.push(cte.name.to_ascii_lowercase());
    }
    if let Some(entry) = catalog.lookup_any_relation(&update.table_name) {
        rels.insert(entry.relation_oid);
    }
    if let Some(from) = &update.from {
        collect_direct_relation_oids_from_from_item(from, catalog, visible_ctes, rels);
    }
    for assignment in &update.assignments {
        collect_direct_relation_oids_from_sql_expr(&assignment.expr, catalog, visible_ctes, rels);
    }
    if let Some(where_clause) = &update.where_clause {
        collect_direct_relation_oids_from_sql_expr(where_clause, catalog, visible_ctes, rels);
    }
    for item in &update.returning {
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
        FromItem::Table { name, .. } => {
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
        FromItem::TableSample { source, sample } => {
            collect_direct_relation_oids_from_from_item(source, catalog, visible_ctes, rels);
            for arg in &sample.args {
                collect_direct_relation_oids_from_sql_expr(arg, catalog, visible_ctes, rels);
            }
            if let Some(repeatable) = &sample.repeatable {
                collect_direct_relation_oids_from_sql_expr(repeatable, catalog, visible_ctes, rels);
            }
        }
        FromItem::RowsFrom { functions, .. } => {
            for function in functions {
                for arg in &function.args {
                    collect_direct_relation_oids_from_sql_expr(
                        &arg.value,
                        catalog,
                        visible_ctes,
                        rels,
                    );
                }
            }
        }
        FromItem::JsonTable(table) => {
            collect_direct_relation_oids_from_json_table(table, catalog, visible_ctes, rels);
        }
        FromItem::XmlTable(table) => {
            collect_direct_relation_oids_from_xml_table(table, catalog, visible_ctes, rels);
        }
        FromItem::Lateral(source) => {
            collect_direct_relation_oids_from_from_item(source, catalog, visible_ctes, rels);
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

fn collect_direct_relation_oids_from_json_table(
    table: &crate::include::nodes::parsenodes::JsonTableExpr,
    catalog: &dyn CatalogLookup,
    visible_ctes: &mut Vec<String>,
    rels: &mut BTreeSet<u32>,
) {
    collect_direct_relation_oids_from_sql_expr(&table.context, catalog, visible_ctes, rels);
    for passing in &table.passing {
        collect_direct_relation_oids_from_sql_expr(&passing.expr, catalog, visible_ctes, rels);
    }
    if let Some(behavior) = &table.on_error {
        collect_direct_relation_oids_from_json_table_behavior(
            behavior,
            catalog,
            visible_ctes,
            rels,
        );
    }
    for column in &table.columns {
        collect_direct_relation_oids_from_json_table_column(column, catalog, visible_ctes, rels);
    }
}

fn collect_direct_relation_oids_from_xml_table(
    table: &crate::include::nodes::parsenodes::XmlTableExpr,
    catalog: &dyn CatalogLookup,
    visible_ctes: &mut Vec<String>,
    rels: &mut BTreeSet<u32>,
) {
    collect_direct_relation_oids_from_sql_expr(&table.row_path, catalog, visible_ctes, rels);
    collect_direct_relation_oids_from_sql_expr(&table.document, catalog, visible_ctes, rels);
    for namespace in &table.namespaces {
        collect_direct_relation_oids_from_sql_expr(&namespace.uri, catalog, visible_ctes, rels);
    }
    for column in &table.columns {
        if let crate::include::nodes::parsenodes::XmlTableColumn::Regular {
            path, default, ..
        } = column
        {
            if let Some(path) = path {
                collect_direct_relation_oids_from_sql_expr(path, catalog, visible_ctes, rels);
            }
            if let Some(default) = default {
                collect_direct_relation_oids_from_sql_expr(default, catalog, visible_ctes, rels);
            }
        }
    }
}

fn collect_direct_relation_oids_from_json_table_column(
    column: &crate::include::nodes::parsenodes::JsonTableColumn,
    catalog: &dyn CatalogLookup,
    visible_ctes: &mut Vec<String>,
    rels: &mut BTreeSet<u32>,
) {
    match column {
        crate::include::nodes::parsenodes::JsonTableColumn::Ordinality { .. } => {}
        crate::include::nodes::parsenodes::JsonTableColumn::Regular {
            on_empty, on_error, ..
        } => {
            if let Some(behavior) = on_empty {
                collect_direct_relation_oids_from_json_table_behavior(
                    behavior,
                    catalog,
                    visible_ctes,
                    rels,
                );
            }
            if let Some(behavior) = on_error {
                collect_direct_relation_oids_from_json_table_behavior(
                    behavior,
                    catalog,
                    visible_ctes,
                    rels,
                );
            }
        }
        crate::include::nodes::parsenodes::JsonTableColumn::Exists { on_error, .. } => {
            if let Some(behavior) = on_error {
                collect_direct_relation_oids_from_json_table_behavior(
                    behavior,
                    catalog,
                    visible_ctes,
                    rels,
                );
            }
        }
        crate::include::nodes::parsenodes::JsonTableColumn::Nested { columns, .. } => {
            for child in columns {
                collect_direct_relation_oids_from_json_table_column(
                    child,
                    catalog,
                    visible_ctes,
                    rels,
                );
            }
        }
    }
}

fn collect_direct_relation_oids_from_json_table_behavior(
    behavior: &crate::include::nodes::parsenodes::JsonTableBehavior,
    catalog: &dyn CatalogLookup,
    visible_ctes: &mut Vec<String>,
    rels: &mut BTreeSet<u32>,
) {
    if let crate::include::nodes::parsenodes::JsonTableBehavior::Default(expr) = behavior {
        collect_direct_relation_oids_from_sql_expr(expr, catalog, visible_ctes, rels);
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
        | SqlExpr::Parameter(_)
        | SqlExpr::ParamRef(_)
        | SqlExpr::Default
        | SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
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
        | SqlExpr::LocalTimestamp { .. } => {}
        SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::BitNot(inner)
        | SqlExpr::Subscript { expr: inner, .. }
        | SqlExpr::PrefixOperator { expr: inner, .. }
        | SqlExpr::Collate { expr: inner, .. }
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
        | SqlExpr::Overlaps(left, right)
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
        | SqlExpr::JsonPathText(left, right)
        | SqlExpr::AtTimeZone {
            expr: left,
            zone: right,
        } => {
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
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            if let Some(arg) = arg {
                collect_direct_relation_oids_from_sql_expr(arg, catalog, visible_ctes, rels);
            }
            for arm in args {
                collect_direct_relation_oids_from_sql_expr(&arm.expr, catalog, visible_ctes, rels);
                collect_direct_relation_oids_from_sql_expr(
                    &arm.result,
                    catalog,
                    visible_ctes,
                    rels,
                );
            }
            if let Some(defresult) = defresult {
                collect_direct_relation_oids_from_sql_expr(defresult, catalog, visible_ctes, rels);
            }
        }
        SqlExpr::ArrayLiteral(elements) | SqlExpr::Row(elements) => {
            for element in elements {
                collect_direct_relation_oids_from_sql_expr(element, catalog, visible_ctes, rels);
            }
        }
        SqlExpr::FuncCall { args, order_by, .. } => {
            for arg in args.args() {
                collect_direct_relation_oids_from_sql_expr(&arg.value, catalog, visible_ctes, rels);
            }
            for item in order_by {
                collect_direct_relation_oids_from_sql_expr(&item.expr, catalog, visible_ctes, rels);
            }
        }
        SqlExpr::ScalarSubquery(select)
        | SqlExpr::ArraySubquery(select)
        | SqlExpr::Exists(select) => {
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
        SqlExpr::Xml(xml) => {
            for child in xml.child_exprs() {
                collect_direct_relation_oids_from_sql_expr(child, catalog, visible_ctes, rels);
            }
        }
        SqlExpr::JsonQueryFunction(func) => {
            for child in func.child_exprs() {
                collect_direct_relation_oids_from_sql_expr(child, catalog, visible_ctes, rels);
            }
        }
        SqlExpr::GeometryUnaryOp { expr, .. } => {
            collect_direct_relation_oids_from_sql_expr(expr, catalog, visible_ctes, rels);
        }
    }
}
