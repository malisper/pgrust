use std::collections::BTreeSet;

use pgrust_access::common::toast_compression::{
    ToastCompressionError, ensure_attribute_compression_supported,
};
use pgrust_analyze::{
    BoundAssignment, BoundIndexRelation, BoundNotNullConstraint, BoundReferencedByForeignKey,
    BoundRelation, BoundRelationConstraints, BoundTemporalConstraint, CatalogLookup,
    bind_referenced_by_foreign_keys, is_system_column_name, sql_type_name,
};
use pgrust_catalog_data::desc::column_desc;
use pgrust_core::{AttributeCompression, AttributeStorage, ItemPointerData};
use pgrust_nodes::datum::{ArrayDimension, ArrayValue, Value, array_value_from_value};
use pgrust_nodes::parsenodes::ParseError;
use pgrust_nodes::plannodes::{Plan, PlannedStmt};
use pgrust_nodes::primnodes::{BoolExprType, Expr, OpExprKind, SELF_ITEM_POINTER_ATTR_NO};
use pgrust_nodes::primnodes::{
    QueryColumn, RelationDesc, TargetEntry, ToastRelationRef, XMAX_ATTR_NO, XMIN_ATTR_NO,
};
use pgrust_nodes::relcache::RelCacheEntry;
use pgrust_nodes::result::{SessionReplicationRole, StatementResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableCmdsError {
    Parse(ParseError),
    Detailed {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
}

pub struct RelationWriteState {
    pub constraints: BoundRelationConstraints,
    pub referenced_by: Vec<BoundReferencedByForeignKey>,
    pub indexes: Vec<BoundIndexRelation>,
    pub toast_index: Option<BoundIndexRelation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NormalizedStatisticsTarget {
    pub value: i16,
    pub warning: Option<&'static str>,
}

pub fn expression_detail_name(expr_sql: &str) -> String {
    let trimmed = expr_sql.trim();
    if let Some(function_call) = normalized_function_call_expression(trimmed) {
        return function_call;
    }
    if (trimmed.starts_with('(') && trimmed.ends_with(')')) || looks_like_function_call(trimmed) {
        trimmed.to_string()
    } else {
        format!("({trimmed})")
    }
}

pub fn current_of_tidscan_display_cursor(predicate: Option<&Expr>) -> Option<String> {
    let predicate = predicate?;
    let conjuncts = current_of_flatten_and(predicate);
    let mut cursor = None;
    let mut has_ctid_eq = false;
    for conjunct in conjuncts {
        if let Some(marker_cursor) = current_of_marker_cursor(conjunct) {
            cursor = Some(marker_cursor);
        } else if current_of_ctid_equality(conjunct) {
            has_ctid_eq = true;
        } else {
            return None;
        }
    }
    if has_ctid_eq { cursor } else { None }
}

pub fn lookup_relation_for_alter_column_compression(
    catalog: &dyn CatalogLookup,
    name: &str,
    if_exists: bool,
) -> Result<Option<BoundRelation>, ParseError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'r' | 'f' | 'm') => Ok(Some(entry)),
        Some(_) => Err(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table or materialized view",
        }),
        None if if_exists => Ok(None),
        None => Err(ParseError::UnknownTable(name.to_string())),
    }
}

pub fn validate_alter_table_alter_column_storage(
    desc: &RelationDesc,
    column_name: &str,
    storage: AttributeStorage,
) -> Result<(String, AttributeStorage), TableCmdsError> {
    if is_system_column_name(column_name) {
        return Err(TableCmdsError::Parse(ParseError::UnexpectedToken {
            expected: "user column name for ALTER COLUMN SET STORAGE",
            actual: column_name.to_string(),
        }));
    }
    let column = desc
        .columns
        .iter()
        .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| TableCmdsError::Parse(ParseError::UnknownColumn(column_name.to_string())))?;

    let type_default = column_desc("attstorage_check", column.sql_type, column.storage.nullable)
        .storage
        .attstorage;
    if storage != AttributeStorage::Plain && type_default == AttributeStorage::Plain {
        return Err(TableCmdsError::Detailed {
            message: format!(
                "column data type {} can only have storage PLAIN",
                sql_type_name(column.sql_type)
            ),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }

    Ok((column.name.clone(), storage))
}

pub fn validate_alter_table_alter_column_options(
    desc: &RelationDesc,
    column_name: &str,
) -> Result<String, TableCmdsError> {
    if is_system_column_name(column_name) {
        return Err(TableCmdsError::Detailed {
            message: format!("cannot alter system column \"{column_name}\""),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    let column = desc
        .columns
        .iter()
        .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| TableCmdsError::Parse(ParseError::UnknownColumn(column_name.to_string())))?;
    Ok(column.name.clone())
}

pub fn validate_alter_table_alter_column_compression(
    desc: &RelationDesc,
    column_name: &str,
    compression: AttributeCompression,
) -> Result<(String, AttributeCompression), TableCmdsError> {
    if is_system_column_name(column_name) {
        return Err(TableCmdsError::Parse(ParseError::UnexpectedToken {
            expected: "user column name for ALTER COLUMN SET COMPRESSION",
            actual: column_name.to_string(),
        }));
    }
    let column = desc
        .columns
        .iter()
        .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| TableCmdsError::Parse(ParseError::UnknownColumn(column_name.to_string())))?;

    ensure_attribute_compression_supported(compression).map_err(compression_error_to_tablecmds)?;

    let type_default = column_desc(
        "attcompression_check",
        column.sql_type,
        column.storage.nullable,
    )
    .storage
    .attstorage;
    if compression != AttributeCompression::Default && type_default == AttributeStorage::Plain {
        return Err(TableCmdsError::Detailed {
            message: format!(
                "column data type {} does not support compression",
                sql_type_name(column.sql_type)
            ),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }

    Ok((column.name.clone(), compression))
}

fn compression_error_to_tablecmds(error: ToastCompressionError) -> TableCmdsError {
    match error {
        ToastCompressionError::Lz4NotSupported => TableCmdsError::Detailed {
            message: "compression method lz4 not supported".into(),
            detail: Some(
                "This functionality requires the server to be built with lz4 support.".into(),
            ),
            hint: None,
            sqlstate: "0A000",
        },
        ToastCompressionError::InvalidCompressionMethod(value) => TableCmdsError::Detailed {
            message: format!("invalid compression method \"{value}\""),
            detail: None,
            hint: None,
            sqlstate: "22023",
        },
        ToastCompressionError::InvalidStorageValue { details } => TableCmdsError::Detailed {
            message: "invalid storage value".into(),
            detail: Some(details.into()),
            hint: None,
            sqlstate: "22023",
        },
    }
}

pub fn normalize_statistics_target(
    statistics_target: i32,
) -> Result<NormalizedStatisticsTarget, TableCmdsError> {
    if statistics_target == -1 {
        return Ok(NormalizedStatisticsTarget {
            value: -1,
            warning: None,
        });
    }
    if statistics_target < 0 {
        return Err(TableCmdsError::Detailed {
            message: format!("statistics target {} is too low", statistics_target),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    if statistics_target > 10000 {
        return Ok(NormalizedStatisticsTarget {
            value: 10000,
            warning: Some("lowering statistics target to 10000"),
        });
    }
    Ok(NormalizedStatisticsTarget {
        value: i16::try_from(statistics_target).map_err(|_| {
            TableCmdsError::Parse(ParseError::InvalidInteger(statistics_target.to_string()))
        })?,
        warning: None,
    })
}

pub fn validate_alter_table_alter_column_statistics(
    desc: &RelationDesc,
    column_name: &str,
    statistics_target: i32,
) -> Result<(String, NormalizedStatisticsTarget), TableCmdsError> {
    if is_system_column_name(column_name) {
        return Err(TableCmdsError::Parse(ParseError::UnexpectedToken {
            expected: "user column name for ALTER COLUMN SET STATISTICS",
            actual: column_name.to_string(),
        }));
    }
    let column = desc
        .columns
        .iter()
        .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| TableCmdsError::Parse(ParseError::UnknownColumn(column_name.to_string())))?;
    Ok((
        column.name.clone(),
        normalize_statistics_target(statistics_target)?,
    ))
}

pub fn validate_alter_index_alter_column_statistics(
    entry: &RelCacheEntry,
    index_name: &str,
    column_number: i16,
    statistics_target: i32,
) -> Result<(String, NormalizedStatisticsTarget), TableCmdsError> {
    let index_meta = entry
        .index
        .as_ref()
        .ok_or_else(|| TableCmdsError::Detailed {
            message: format!("relation \"{index_name}\" is not an index"),
            detail: None,
            hint: None,
            sqlstate: "42809",
        })?;
    if column_number < 1 {
        return Err(TableCmdsError::Detailed {
            message: "column number must be in range from 1 to 32767".into(),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    let column_index = usize::try_from(column_number - 1).unwrap_or(usize::MAX);
    let column = entry
        .desc
        .columns
        .get(column_index)
        .ok_or_else(|| TableCmdsError::Detailed {
            message: format!(
                "column number {} of relation \"{}\" does not exist",
                column_number, index_name
            ),
            detail: None,
            hint: None,
            sqlstate: "42703",
        })?;
    if column_number > index_meta.indnkeyatts {
        return Err(TableCmdsError::Detailed {
            message: format!(
                "cannot alter statistics on included column \"{}\" of index \"{}\"",
                column.name, index_name
            ),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    if index_meta
        .indkey
        .get(column_index)
        .copied()
        .is_none_or(|attnum| attnum != 0)
    {
        return Err(TableCmdsError::Detailed {
            message: format!(
                "cannot alter statistics on non-expression column \"{}\" of index \"{}\"",
                column.name, index_name
            ),
            detail: None,
            hint: Some("Alter statistics on table column instead.".into()),
            sqlstate: "0A000",
        });
    }
    Ok((
        column.name.clone(),
        normalize_statistics_target(statistics_target)?,
    ))
}

fn current_of_flatten_and(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::Bool(bool_expr) if matches!(bool_expr.boolop, BoolExprType::And) => bool_expr
            .args
            .iter()
            .flat_map(current_of_flatten_and)
            .collect(),
        _ => vec![expr],
    }
}

fn current_of_marker_cursor(predicate: &Expr) -> Option<String> {
    match predicate {
        Expr::Op(op) if matches!(op.op, OpExprKind::Eq) && op.args.len() == 2 => {
            let left = current_of_marker_text(&op.args[0])?;
            let right = current_of_marker_text(&op.args[1])?;
            (left == right).then(|| left.trim_start_matches("__pgrust_current_of:").to_string())
        }
        _ => None,
    }
}

fn current_of_ctid_equality(predicate: &Expr) -> bool {
    let Expr::Op(op) = predicate else {
        return false;
    };
    matches!(op.op, OpExprKind::Eq) && op.args.len() == 2 && op.args.iter().any(expr_is_ctid_var)
}

fn expr_is_ctid_var(expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) => var.varattno == SELF_ITEM_POINTER_ATTR_NO,
        Expr::Cast(inner, _) => expr_is_ctid_var(inner),
        _ => false,
    }
}

fn current_of_marker_text(expr: &Expr) -> Option<&str> {
    let Expr::Const(value) = expr else {
        return None;
    };
    let text = value.as_text()?;
    text.strip_prefix("__pgrust_current_of:")?;
    Some(text)
}

pub fn normalized_function_call_expression(expr_sql: &str) -> Option<String> {
    let trimmed = strip_outer_parens_once(expr_sql.trim());
    if !looks_like_function_call(trimmed) {
        return None;
    }
    let open = trimmed.find('(')?;
    let name = trimmed[..open].trim();
    let args = trimmed[open + 1..trimmed.len().saturating_sub(1)]
        .split(',')
        .map(str::trim)
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!("{name}({args})"))
}

pub fn strip_outer_parens_once(input: &str) -> &str {
    let trimmed = input.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return trimmed;
    }
    let mut depth = 0i32;
    for (idx, ch) in trimmed.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 && idx + ch.len_utf8() < trimmed.len() {
                    return trimmed;
                }
            }
            _ => {}
        }
    }
    trimmed[1..trimmed.len().saturating_sub(1)].trim()
}

fn looks_like_function_call(expr_sql: &str) -> bool {
    let Some(open_paren) = expr_sql.find('(') else {
        return false;
    };
    expr_sql.ends_with(')')
        && expr_sql[..open_paren].chars().enumerate().all(|(idx, ch)| {
            if idx == 0 {
                ch == '_' || ch.is_ascii_alphabetic()
            } else {
                ch == '_' || ch.is_ascii_alphanumeric()
            }
        })
}

pub fn relation_has_active_user_rules(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    session_replication_role: SessionReplicationRole,
) -> bool {
    catalog
        .rewrite_rows_for_relation(relation_oid)
        .into_iter()
        .any(|row| {
            row.rulename != "_RETURN"
                && match row.ev_enabled {
                    'D' => false,
                    'A' => true,
                    'R' => session_replication_role == SessionReplicationRole::Replica,
                    'O' => session_replication_role != SessionReplicationRole::Replica,
                    _ => true,
                }
        })
}

pub fn modified_attnums_for_update(assignments: &[BoundAssignment]) -> Vec<i16> {
    assignments
        .iter()
        .map(|assignment| assignment.column_index as i16 + 1)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub fn returning_result_columns(targets: &[TargetEntry]) -> Vec<QueryColumn> {
    targets
        .iter()
        .map(|target| QueryColumn {
            name: target.name.clone(),
            sql_type: target.sql_type,
            wire_type_oid: None,
        })
        .collect()
}

pub fn build_returning_result(columns: Vec<QueryColumn>, rows: Vec<Vec<Value>>) -> StatementResult {
    let column_names = columns.iter().map(|column| column.name.clone()).collect();
    StatementResult::Query {
        columns,
        column_names,
        rows,
    }
}

pub fn returning_contains_transaction_system_var(returning: &[TargetEntry]) -> bool {
    returning
        .iter()
        .any(|target| expr_contains_transaction_system_var(&target.expr))
}

pub fn expr_contains_transaction_system_var(expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) => matches!(var.varattno, XMIN_ATTR_NO | XMAX_ATTR_NO),
        Expr::Cast(inner, _)
        | Expr::FieldSelect { expr: inner, .. }
        | Expr::Collate { expr: inner, .. } => expr_contains_transaction_system_var(inner),
        Expr::Func(func) => func.args.iter().any(expr_contains_transaction_system_var),
        Expr::Op(op) => op.args.iter().any(expr_contains_transaction_system_var),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(expr_contains_transaction_system_var),
        Expr::Coalesce(left, right) => {
            expr_contains_transaction_system_var(left)
                || expr_contains_transaction_system_var(right)
        }
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_deref()
                .is_some_and(expr_contains_transaction_system_var)
                || case_expr.args.iter().any(|arm| {
                    expr_contains_transaction_system_var(&arm.expr)
                        || expr_contains_transaction_system_var(&arm.result)
                })
                || expr_contains_transaction_system_var(&case_expr.defresult)
        }
        Expr::ArrayLiteral { elements, .. } => {
            elements.iter().any(expr_contains_transaction_system_var)
        }
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| expr_contains_transaction_system_var(expr)),
        _ => false,
    }
}

pub fn partition_tree_has_nonmatching_user_layout(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    parent_desc: &RelationDesc,
) -> bool {
    catalog
        .find_all_inheritors(relation_oid)
        .into_iter()
        .filter(|oid| *oid != relation_oid)
        .filter_map(|oid| catalog.relation_by_oid(oid))
        .filter(|relation| relation.relkind == 'r')
        .any(|relation| !relation_user_layout_matches(parent_desc, &relation.desc))
}

pub fn relation_user_layout_matches(parent_desc: &RelationDesc, child_desc: &RelationDesc) -> bool {
    let parent_columns = &parent_desc.columns;
    let child_columns = &child_desc.columns;
    parent_columns.len() == child_columns.len()
        && parent_columns
            .iter()
            .zip(child_columns.iter())
            .all(|(parent, child)| {
                parent.dropped == child.dropped
                    && parent.name.eq_ignore_ascii_case(&child.name)
                    && parent.sql_type == child.sql_type
            })
}

pub fn plan_relation_oids(plan: &Plan) -> BTreeSet<u32> {
    let mut oids = BTreeSet::new();
    collect_plan_relation_oids(plan, &mut oids);
    oids
}

pub fn planned_stmt_relation_oids(planned_stmt: &PlannedStmt) -> BTreeSet<u32> {
    let mut oids = BTreeSet::new();
    collect_plan_relation_oids(&planned_stmt.plan_tree, &mut oids);
    for subplan in &planned_stmt.subplans {
        collect_plan_relation_oids(subplan, &mut oids);
    }
    oids
}

fn collect_plan_relation_oids(plan: &Plan, oids: &mut BTreeSet<u32>) {
    match plan {
        Plan::SeqScan { relation_oid, .. }
        | Plan::TidScan { relation_oid, .. }
        | Plan::IndexOnlyScan { relation_oid, .. }
        | Plan::IndexScan { relation_oid, .. }
        | Plan::BitmapHeapScan { relation_oid, .. }
        | Plan::BitmapIndexScan { relation_oid, .. } => {
            oids.insert(*relation_oid);
        }
        Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. } => {
            for child in children {
                collect_plan_relation_oids(child, oids);
            }
        }
        Plan::Unique { input, .. }
        | Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::ProjectSet { input, .. } => collect_plan_relation_oids(input, oids),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            collect_plan_relation_oids(left, oids);
            collect_plan_relation_oids(right, oids);
        }
        Plan::CteScan { cte_plan, .. } => collect_plan_relation_oids(cte_plan, oids),
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => {
            collect_plan_relation_oids(anchor, oids);
            collect_plan_relation_oids(recursive, oids);
        }
        Plan::SetOp { children, .. } => {
            for child in children {
                collect_plan_relation_oids(child, oids);
            }
        }
        Plan::Result { .. }
        | Plan::Values { .. }
        | Plan::FunctionScan { .. }
        | Plan::WorkTableScan { .. } => {}
    }
}

pub fn plan_contains_lock_rows(plan: &Plan) -> bool {
    match plan {
        Plan::LockRows { .. } => true,
        Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. } => children.iter().any(plan_contains_lock_rows),
        Plan::Unique { input, .. }
        | Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Limit { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::ProjectSet { input, .. } => plan_contains_lock_rows(input),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            plan_contains_lock_rows(left) || plan_contains_lock_rows(right)
        }
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => plan_contains_lock_rows(anchor) || plan_contains_lock_rows(recursive),
        Plan::SetOp { children, .. } => children.iter().any(plan_contains_lock_rows),
        Plan::CteScan { cte_plan, .. } => plan_contains_lock_rows(cte_plan),
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::TidScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::BitmapHeapScan { .. }
        | Plan::Values { .. }
        | Plan::FunctionScan { .. }
        | Plan::WorkTableScan { .. } => false,
    }
}

pub fn constraint_key_values(constraint: &BoundTemporalConstraint, values: &[Value]) -> Vec<Value> {
    values_at_indexes(values, &constraint.column_indexes)
}

pub fn constraint_columns(
    desc: &RelationDesc,
    constraint: &BoundTemporalConstraint,
) -> Vec<pgrust_nodes::primnodes::ColumnDesc> {
    constraint
        .column_indexes
        .iter()
        .filter_map(|index| desc.columns.get(*index).cloned())
        .collect()
}

pub fn foreign_key_key_values(values: &[Value], indexes: &[usize]) -> Vec<Value> {
    values_at_indexes(values, indexes)
}

fn values_at_indexes(values: &[Value], indexes: &[usize]) -> Vec<Value> {
    indexes
        .iter()
        .map(|index| values.get(*index).cloned().unwrap_or(Value::Null))
        .collect()
}

pub fn key_columns_changed(previous_values: &[Value], values: &[Value], indexes: &[usize]) -> bool {
    indexes.iter().any(|index| {
        let previous = previous_values.get(*index).unwrap_or(&Value::Null);
        let current = values.get(*index).unwrap_or(&Value::Null);
        previous != current
    })
}

pub fn foreign_key_constraint_ancestor_oids(
    catalog: &dyn CatalogLookup,
    constraint_oid: u32,
) -> BTreeSet<u32> {
    let mut oids = BTreeSet::from([constraint_oid]);
    let mut current_oid = constraint_oid;
    while let Some(row) = catalog.constraint_row_by_oid(current_oid) {
        if row.conparentid == 0 {
            break;
        }
        if !oids.insert(row.conparentid) {
            break;
        }
        current_oid = row.conparentid;
    }
    oids
}

pub fn remap_optional_column_indexes_by_name(
    parent_desc: &RelationDesc,
    child_desc: &RelationDesc,
    parent_indexes: Option<&[usize]>,
) -> Result<Option<Vec<usize>>, TableCmdsError> {
    parent_indexes
        .map(|indexes| map_column_indexes_by_name(parent_desc, child_desc, indexes))
        .transpose()
}

pub fn map_column_indexes_by_name(
    parent_desc: &RelationDesc,
    child_desc: &RelationDesc,
    parent_indexes: &[usize],
) -> Result<Vec<usize>, TableCmdsError> {
    parent_indexes
        .iter()
        .map(|parent_index| {
            let parent_column =
                parent_desc
                    .columns
                    .get(*parent_index)
                    .ok_or_else(|| TableCmdsError::Detailed {
                        message: "foreign key validation failed".into(),
                        detail: Some("invalid parent column index".into()),
                        hint: None,
                        sqlstate: "XX000",
                    })?;
            child_desc
                .columns
                .iter()
                .enumerate()
                .find(|(_, column)| {
                    !column.dropped && column.name.eq_ignore_ascii_case(&parent_column.name)
                })
                .map(|(index, _)| index)
                .ok_or_else(|| TableCmdsError::Detailed {
                    message: "foreign key validation failed".into(),
                    detail: Some("missing partition foreign key column".into()),
                    hint: None,
                    sqlstate: "XX000",
                })
        })
        .collect()
}

pub fn first_toast_index(
    catalog: &dyn CatalogLookup,
    toast: Option<ToastRelationRef>,
) -> Option<BoundIndexRelation> {
    let toast = toast?;
    catalog
        .index_relations_for_heap(toast.relation_oid)
        .into_iter()
        .next()
}

pub fn relation_write_state_for_relation(
    relation: &BoundRelation,
    catalog: &dyn CatalogLookup,
) -> Result<RelationWriteState, ParseError> {
    let constraints = BoundRelationConstraints {
        relation_oid: Some(relation.relation_oid),
        not_nulls: relation
            .desc
            .columns
            .iter()
            .enumerate()
            .filter_map(|(column_index, column)| {
                column
                    .not_null_constraint_name
                    .as_ref()
                    .map(|constraint_name| BoundNotNullConstraint {
                        column_index,
                        constraint_name: constraint_name.clone(),
                    })
            })
            .collect(),
        checks: Vec::new(),
        foreign_keys: Vec::new(),
        temporal: Vec::new(),
        exclusions: Vec::new(),
    };
    let referenced_by =
        bind_referenced_by_foreign_keys(relation.relation_oid, &relation.desc, catalog)?;
    Ok(RelationWriteState {
        constraints,
        referenced_by,
        indexes: catalog.index_relations_for_heap(relation.relation_oid),
        toast_index: first_toast_index(catalog, relation.toast),
    })
}

pub fn parse_tid_text(value: &Value) -> Result<Option<ItemPointerData>, TableCmdsError> {
    let text = match value {
        Value::Null => return Ok(None),
        Value::Tid(tid) => return Ok(Some(*tid)),
        Value::Text(text) => text.as_str(),
        Value::TextRef(_, _) => {
            return Err(internal_marker_error(
                "row ctid marker must be materialized".into(),
            ));
        }
        other => {
            return Err(internal_marker_error(format!(
                "row ctid marker has unexpected value {:?}",
                other
            )));
        }
    };
    let inner = text
        .strip_prefix('(')
        .and_then(|rest| rest.strip_suffix(')'))
        .ok_or_else(|| invalid_row_ctid_marker(text))?;
    let (block, offset) = inner
        .split_once(',')
        .ok_or_else(|| invalid_row_ctid_marker(text))?;
    Ok(Some(ItemPointerData {
        block_number: block.parse().map_err(|_| invalid_row_ctid_marker(text))?,
        offset_number: offset.parse().map_err(|_| invalid_row_ctid_marker(text))?,
    }))
}

pub fn parse_update_tableoid(value: &Value) -> Result<u32, TableCmdsError> {
    match value {
        Value::Int32(value) => u32::try_from(*value)
            .map_err(|_| internal_marker_error(format!("invalid update tableoid marker: {value}"))),
        Value::Int64(value) => u32::try_from(*value)
            .map_err(|_| internal_marker_error(format!("invalid update tableoid marker: {value}"))),
        Value::Null => Err(internal_marker_error(
            "update input row is missing target tableoid marker".into(),
        )),
        other => Err(internal_marker_error(format!(
            "update tableoid marker has unexpected value {:?}",
            other
        ))),
    }
}

pub fn merge_source_present(value: &Value) -> Result<bool, TableCmdsError> {
    match value {
        Value::Bool(value) => Ok(*value),
        Value::Null => Ok(false),
        other => Err(internal_marker_error(format!(
            "merge source marker has unexpected value {:?}",
            other
        ))),
    }
}

fn invalid_row_ctid_marker(text: &str) -> TableCmdsError {
    internal_marker_error(format!("invalid row ctid marker: {text}"))
}

fn internal_marker_error(message: String) -> TableCmdsError {
    TableCmdsError::Detailed {
        message,
        detail: None,
        hint: None,
        sqlstate: "XX000",
    }
}

pub fn assignment_current_array(current: Value) -> Result<ArrayValue, TableCmdsError> {
    match current {
        Value::Null => Ok(ArrayValue::empty()),
        other => array_value_from_value(&other).ok_or_else(|| TableCmdsError::Detailed {
            message: "array assignment requires array value".into(),
            detail: Some(format!("received value {:?}", other)),
            hint: None,
            sqlstate: "42804",
        }),
    }
}

pub fn assignment_source_array(replacement: Value) -> Result<ArrayValue, TableCmdsError> {
    array_value_from_value(&replacement).ok_or_else(|| TableCmdsError::Detailed {
        message: "array slice assignment requires array value".into(),
        detail: Some(format!("received value {:?}", replacement)),
        hint: None,
        sqlstate: "42804",
    })
}

pub fn checked_array_item_count(count: usize) -> Result<usize, TableCmdsError> {
    i32::try_from(count)
        .map(|_| count)
        .map_err(|_| array_assignment_limit_error())
}

pub fn checked_array_upper_bound(lower_bound: i32, length: usize) -> Result<i32, TableCmdsError> {
    let length = i32::try_from(length).map_err(|_| array_assignment_limit_error())?;
    lower_bound
        .checked_add(length)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(array_assignment_limit_error)
}

pub fn checked_array_span_length(lower: i32, upper: i32) -> Result<usize, TableCmdsError> {
    if upper < lower {
        return Ok(0);
    }
    let span = i64::from(upper) - i64::from(lower) + 1;
    usize::try_from(span).map_err(|_| array_assignment_limit_error())
}

pub fn array_with_element_type(mut array: ArrayValue, element_type_oid: Option<u32>) -> ArrayValue {
    array.element_type_oid = element_type_oid;
    array
}

pub fn linear_index_to_assignment_coords(
    mut offset: usize,
    lower_bounds: &[i32],
    lengths: &[usize],
) -> Vec<i32> {
    let mut coords = vec![0; lengths.len()];
    for dim_idx in 0..lengths.len() {
        let stride = lengths[dim_idx + 1..]
            .iter()
            .fold(1usize, |product, length| product.saturating_mul(*length));
        let axis_offset = if stride == 0 { 0 } else { offset / stride };
        if stride != 0 {
            offset %= stride;
        }
        coords[dim_idx] = lower_bounds[dim_idx] + axis_offset as i32;
    }
    coords
}

pub fn assignment_coords_to_linear_index(coords: &[i32], dimensions: &[ArrayDimension]) -> usize {
    let mut offset = 0usize;
    for (dim_idx, coord) in coords.iter().enumerate() {
        let stride = dimensions[dim_idx + 1..]
            .iter()
            .fold(1usize, |product, dim| product.saturating_mul(dim.length));
        offset += (*coord - dimensions[dim_idx].lower_bound) as usize * stride;
    }
    offset
}

pub fn assignment_top_level(current: Value) -> Result<(i32, Vec<Value>), TableCmdsError> {
    match current {
        Value::Null => Ok((1, Vec::new())),
        Value::Array(items) => Ok((1, items)),
        Value::PgArray(array) => Ok((
            array.lower_bound(0).unwrap_or(1),
            assignment_top_level_items(&array),
        )),
        other => Err(TableCmdsError::Detailed {
            message: "array assignment requires array value".into(),
            detail: Some(format!("received value {:?}", other)),
            hint: None,
            sqlstate: "42804",
        }),
    }
}

pub fn assignment_top_level_items(array: &ArrayValue) -> Vec<Value> {
    if array.dimensions.len() <= 1 {
        return array.elements.clone();
    }
    let child_dims = array.dimensions[1..].to_vec();
    let child_width = child_dims
        .iter()
        .fold(1usize, |acc, dim| acc.saturating_mul(dim.length));
    let mut out = Vec::with_capacity(array.dimensions[0].length);
    for idx in 0..array.dimensions[0].length {
        let start = idx * child_width;
        out.push(Value::PgArray(ArrayValue::from_dimensions(
            child_dims.clone(),
            array.elements[start..start + child_width].to_vec(),
        )));
    }
    out
}

pub fn assignment_replacement_items(replacement: Value) -> Result<Vec<Value>, TableCmdsError> {
    match replacement {
        Value::Array(items) => Ok(items),
        Value::PgArray(array) => Ok(assignment_top_level_items(&array)),
        other => Err(TableCmdsError::Detailed {
            message: "array slice assignment requires array value".into(),
            detail: Some(format!("received value {:?}", other)),
            hint: None,
            sqlstate: "42804",
        }),
    }
}

pub fn extend_assignment_items(
    lower_bound: &mut i32,
    items: &mut Vec<Value>,
    start: i32,
    end: i32,
) -> Result<(), TableCmdsError> {
    if items.is_empty() {
        *lower_bound = start;
    }
    if start < *lower_bound {
        let prepend = i64::from(*lower_bound)
            .checked_sub(i64::from(start))
            .and_then(|delta| usize::try_from(delta).ok())
            .ok_or_else(array_assignment_limit_error)?;
        items.splice(0..0, std::iter::repeat_n(Value::Null, prepend));
        *lower_bound = start;
    }
    let upper_bound = i64::from(*lower_bound)
        .checked_add(i64::try_from(items.len()).map_err(|_| array_assignment_limit_error())?)
        .and_then(|bound| bound.checked_sub(1))
        .ok_or_else(array_assignment_limit_error)?;
    if i64::from(end) > upper_bound {
        let append = i64::from(end)
            .checked_sub(upper_bound)
            .and_then(|delta| usize::try_from(delta).ok())
            .ok_or_else(array_assignment_limit_error)?;
        let new_len = items
            .len()
            .checked_add(append)
            .ok_or_else(array_assignment_limit_error)?;
        items.resize(checked_array_item_count(new_len)?, Value::Null);
    }
    Ok(())
}

pub fn build_assignment_array_value(
    lower_bound: i32,
    items: Vec<Value>,
) -> Result<Value, TableCmdsError> {
    if items.is_empty() {
        return Ok(Value::PgArray(ArrayValue::empty()));
    }
    let child_arrays = items
        .iter()
        .filter_map(|item| match item {
            Value::PgArray(array) => Some(Some(array.clone())),
            Value::Array(values) => {
                Some(ArrayValue::from_nested_values(values.clone(), vec![1]).ok())
            }
            Value::Null => Some(None),
            _ => None,
        })
        .collect::<Vec<_>>();
    if child_arrays.len() != items.len() {
        return Ok(Value::PgArray(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound,
                length: items.len(),
            }],
            items,
        )));
    }
    let Some(template) = child_arrays.iter().find_map(|entry| entry.clone()) else {
        return Ok(Value::PgArray(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound,
                length: items.len(),
            }],
            items,
        )));
    };
    let child_width = template.elements.len();
    let mut elements = Vec::with_capacity(items.len() * child_width);
    for entry in child_arrays {
        match entry {
            Some(array) => elements.extend(array.elements),
            None => elements.extend(std::iter::repeat_n(Value::Null, child_width)),
        }
    }
    let mut dimensions = vec![ArrayDimension {
        lower_bound,
        length: items.len(),
    }];
    dimensions.extend(template.dimensions);
    Ok(Value::PgArray(ArrayValue::from_dimensions(
        dimensions, elements,
    )))
}

pub fn assignment_subscript_index(value: Option<&Value>) -> Result<Option<i32>, TableCmdsError> {
    match value {
        None => Ok(Some(1)),
        Some(Value::Null) => Ok(None),
        Some(Value::Int16(v)) => Ok(Some(*v as i32)),
        Some(Value::Int32(v)) => Ok(Some(*v)),
        Some(Value::Int64(v)) => {
            i32::try_from(*v)
                .map(Some)
                .map_err(|_| TableCmdsError::Detailed {
                    message: "integer out of range".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "22003",
                })
        }
        Some(other) => Err(TableCmdsError::Detailed {
            message: "array assignment requires integer subscript".into(),
            detail: Some(format!("received value {:?}", other)),
            hint: None,
            sqlstate: "42804",
        }),
    }
}

pub fn array_assignment_limit_error() -> TableCmdsError {
    TableCmdsError::Detailed {
        message: "array size exceeds the maximum allowed".into(),
        detail: None,
        hint: None,
        sqlstate: "54000",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_catalog_data::desc::column_desc;
    use pgrust_catalog_data::{CONSTRAINT_FOREIGN, PgConstraintRow, PgRewriteRow};
    use pgrust_core::{PgInheritsRow, RelFileLocator};
    use pgrust_nodes::CommandType;
    use pgrust_nodes::datum::Value;
    use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
    use pgrust_nodes::plannodes::{PlanEstimate, PlannedStmt};
    use pgrust_nodes::primnodes::{BoolExpr, OpExpr, RelationDesc, Var};

    #[derive(Default)]
    struct TestCatalog {
        relations: Vec<pgrust_analyze::BoundRelation>,
        inherits: Vec<PgInheritsRow>,
        rewrites: Vec<PgRewriteRow>,
        constraints: Vec<PgConstraintRow>,
    }

    impl CatalogLookup for TestCatalog {
        fn lookup_any_relation(&self, _name: &str) -> Option<pgrust_analyze::BoundRelation> {
            None
        }

        fn relation_by_oid(&self, relation_oid: u32) -> Option<pgrust_analyze::BoundRelation> {
            self.relations
                .iter()
                .find(|relation| relation.relation_oid == relation_oid)
                .cloned()
        }

        fn find_all_inheritors(&self, relation_oid: u32) -> Vec<u32> {
            let mut result = vec![relation_oid];
            result.extend(
                self.inherits
                    .iter()
                    .filter(|row| row.inhparent == relation_oid)
                    .map(|row| row.inhrelid),
            );
            result
        }

        fn rewrite_rows_for_relation(&self, relation_oid: u32) -> Vec<PgRewriteRow> {
            self.rewrites
                .iter()
                .filter(|row| row.ev_class == relation_oid)
                .cloned()
                .collect()
        }

        fn constraint_rows(&self) -> Vec<PgConstraintRow> {
            self.constraints.clone()
        }
    }

    #[test]
    fn expression_detail_wraps_non_calls() {
        assert_eq!(expression_detail_name("a + b"), "(a + b)");
        assert_eq!(expression_detail_name("(a + b)"), "(a + b)");
    }

    #[test]
    fn function_call_expression_normalizes_argument_spacing() {
        assert_eq!(
            normalized_function_call_expression("(lower( name , 'x' ))"),
            Some("lower(name, 'x')".into())
        );
        assert_eq!(normalized_function_call_expression("1 + x"), None);
    }

    #[test]
    fn relation_rules_respect_session_replication_role() {
        let catalog = TestCatalog {
            rewrites: vec![
                rewrite(10, "disabled", 'D'),
                rewrite(10, "always", 'A'),
                rewrite(11, "replica", 'R'),
                rewrite(12, "origin", 'O'),
                rewrite(13, "_RETURN", 'A'),
            ],
            ..Default::default()
        };

        assert!(relation_has_active_user_rules(
            &catalog,
            10,
            SessionReplicationRole::Origin
        ));
        assert!(relation_has_active_user_rules(
            &catalog,
            11,
            SessionReplicationRole::Replica
        ));
        assert!(!relation_has_active_user_rules(
            &catalog,
            11,
            SessionReplicationRole::Origin
        ));
        assert!(relation_has_active_user_rules(
            &catalog,
            12,
            SessionReplicationRole::Origin
        ));
        assert!(!relation_has_active_user_rules(
            &catalog,
            12,
            SessionReplicationRole::Replica
        ));
        assert!(!relation_has_active_user_rules(
            &catalog,
            13,
            SessionReplicationRole::Origin
        ));
    }

    #[test]
    fn current_of_tidscan_display_cursor_requires_marker_and_ctid_equality() {
        let marker = eq(text_marker("portal_a"), text_marker("portal_a"));
        let ctid = eq(
            Expr::Var(Var {
                varno: 1,
                varattno: SELF_ITEM_POINTER_ATTR_NO,
                varlevelsup: 0,
                vartype: SqlType::new(SqlTypeKind::Tid),
                collation_oid: None,
            }),
            Expr::Const(Value::Null),
        );
        let predicate = Expr::Bool(Box::new(BoolExpr {
            boolop: BoolExprType::And,
            args: vec![marker, ctid],
        }));

        assert_eq!(
            current_of_tidscan_display_cursor(Some(&predicate)),
            Some("portal_a".into())
        );
        assert_eq!(current_of_tidscan_display_cursor(None), None);
        assert_eq!(
            current_of_tidscan_display_cursor(Some(&text_marker("portal_a"))),
            None
        );
    }

    #[test]
    fn returning_helpers_build_columns_and_find_transaction_system_vars() {
        let target = TargetEntry::new(
            "xmin",
            Expr::Var(Var {
                varno: 1,
                varattno: XMIN_ATTR_NO,
                varlevelsup: 0,
                vartype: SqlType::new(SqlTypeKind::Xid),
                collation_oid: None,
            }),
            SqlType::new(SqlTypeKind::Xid),
            1,
        );

        assert_eq!(
            returning_result_columns(std::slice::from_ref(&target)),
            vec![QueryColumn {
                name: "xmin".into(),
                sql_type: SqlType::new(SqlTypeKind::Xid),
                wire_type_oid: None,
            }]
        );
        assert!(returning_contains_transaction_system_var(&[target]));

        let result = build_returning_result(
            vec![QueryColumn::text("value")],
            vec![vec![Value::Text("ok".into())]],
        );
        assert!(matches!(result, StatementResult::Query { .. }));
    }

    #[test]
    fn update_attnums_are_unique_and_sorted() {
        let assignments = vec![assignment(2), assignment(0), assignment(2)];
        assert_eq!(modified_attnums_for_update(&assignments), vec![1, 3]);
    }

    #[test]
    fn partition_tree_layout_checks_user_columns() {
        let parent_desc = relation_desc(&["id", "tenant"]);
        let matching_child = relation(2, 'r', relation_desc(&["ID", "tenant"]));
        let mismatched_child = relation(3, 'r', relation_desc(&["id", "other"]));
        let catalog = TestCatalog {
            relations: vec![matching_child],
            inherits: vec![PgInheritsRow {
                inhrelid: 2,
                inhparent: 1,
                inhseqno: 1,
                inhdetachpending: false,
            }],
            ..Default::default()
        };

        assert!(!partition_tree_has_nonmatching_user_layout(
            &catalog,
            1,
            &parent_desc
        ));
        assert!(!relation_user_layout_matches(
            &parent_desc,
            &mismatched_child.desc
        ));
    }

    #[test]
    fn plan_helpers_collect_relation_oids_and_lock_rows() {
        let scan = seq_scan(10);
        let plan = Plan::Append {
            plan_info: PlanEstimate::default(),
            source_id: 0,
            desc: RelationDesc {
                columns: Vec::new(),
            },
            parallel_aware: false,
            partition_prune: None,
            children: vec![
                scan.clone(),
                Plan::LockRows {
                    plan_info: PlanEstimate::default(),
                    input: Box::new(seq_scan(20)),
                    row_marks: Vec::new(),
                },
            ],
        };
        let planned = PlannedStmt {
            command_type: CommandType::Select,
            depends_on_row_security: false,
            relation_privileges: Vec::new(),
            plan_tree: plan.clone(),
            subplans: vec![seq_scan(30)],
            ext_params: Vec::new(),
        };

        assert_eq!(plan_relation_oids(&plan), BTreeSet::from([10, 20]));
        assert_eq!(
            planned_stmt_relation_oids(&planned),
            BTreeSet::from([10, 20, 30])
        );
        assert!(plan_contains_lock_rows(&plan));
        assert!(!plan_contains_lock_rows(&scan));
    }

    #[test]
    fn constraint_and_foreign_key_helpers_select_values_and_columns() {
        let desc = relation_desc(&["id", "tenant", "period"]);
        let temporal = BoundTemporalConstraint {
            constraint_oid: 1,
            constraint_name: "temporal_fk".into(),
            column_names: vec!["id".into(), "period".into()],
            column_indexes: vec![0, 2],
            period_column_index: 2,
            primary: false,
            enforced: true,
        };
        let values = vec![Value::Int32(7), Value::Text("a".into()), Value::Int32(9)];

        assert_eq!(
            constraint_key_values(&temporal, &values),
            vec![Value::Int32(7), Value::Int32(9)]
        );
        assert_eq!(
            constraint_columns(&desc, &temporal)
                .into_iter()
                .map(|column| column.name)
                .collect::<Vec<_>>(),
            vec!["id".to_string(), "period".to_string()]
        );
        assert_eq!(
            foreign_key_key_values(&values, &[1, 9]),
            vec![Value::Text("a".into()), Value::Null]
        );
        assert!(key_columns_changed(
            &[Value::Int32(1), Value::Text("old".into())],
            &[Value::Int32(1), Value::Text("new".into())],
            &[1],
        ));
    }

    #[test]
    fn foreign_key_helpers_follow_constraint_ancestors_and_remap_columns() {
        let parent_desc = relation_desc(&["id", "tenant"]);
        let child_desc = relation_desc(&["tenant", "ID"]);
        let catalog = TestCatalog {
            constraints: vec![constraint(10, 0), constraint(11, 10), constraint(12, 11)],
            ..Default::default()
        };

        assert_eq!(
            foreign_key_constraint_ancestor_oids(&catalog, 12),
            BTreeSet::from([10, 11, 12])
        );
        assert_eq!(
            map_column_indexes_by_name(&parent_desc, &child_desc, &[0, 1]).unwrap(),
            vec![1, 0]
        );
        assert_eq!(
            remap_optional_column_indexes_by_name(&parent_desc, &child_desc, Some(&[1])).unwrap(),
            Some(vec![0])
        );
        assert!(map_column_indexes_by_name(&parent_desc, &child_desc, &[9]).is_err());
    }

    #[test]
    fn relation_write_state_collects_local_not_null_metadata() {
        let mut relation = relation(42, 'r', relation_desc(&["id", "tenant"]));
        relation.desc.columns[1].not_null_constraint_name = Some("tenant_nn".into());
        let state = relation_write_state_for_relation(&relation, &TestCatalog::default()).unwrap();

        assert_eq!(state.constraints.relation_oid, Some(42));
        assert_eq!(state.constraints.not_nulls.len(), 1);
        assert_eq!(state.constraints.not_nulls[0].column_index, 1);
        assert_eq!(state.constraints.not_nulls[0].constraint_name, "tenant_nn");
        assert!(state.referenced_by.is_empty());
        assert!(state.indexes.is_empty());
        assert!(state.toast_index.is_none());
    }

    fn text_marker(cursor: &str) -> Expr {
        Expr::Const(Value::Text(format!("__pgrust_current_of:{cursor}").into()))
    }

    fn eq(left: Expr, right: Expr) -> Expr {
        Expr::Op(Box::new(OpExpr {
            opno: 0,
            opfuncid: 0,
            op: OpExprKind::Eq,
            opresulttype: SqlType::new(SqlTypeKind::Bool),
            args: vec![left, right],
            collation_oid: None,
        }))
    }

    fn assignment(column_index: usize) -> BoundAssignment {
        BoundAssignment {
            column_index,
            subscripts: Vec::new(),
            field_path: Vec::new(),
            indirection: Vec::new(),
            target_sql_type: SqlType::new(SqlTypeKind::Int4),
            expr: Expr::Const(Value::Int32(1)),
        }
    }

    fn relation_desc(columns: &[&str]) -> RelationDesc {
        RelationDesc {
            columns: columns
                .iter()
                .map(|name| column_desc(*name, SqlType::new(SqlTypeKind::Int4), false))
                .collect(),
        }
    }

    fn relation(oid: u32, relkind: char, desc: RelationDesc) -> pgrust_analyze::BoundRelation {
        pgrust_analyze::BoundRelation {
            rel: RelFileLocator {
                spc_oid: 0,
                db_oid: 0,
                rel_number: oid,
            },
            relation_oid: oid,
            toast: None,
            namespace_oid: 0,
            owner_oid: 10,
            of_type_oid: 0,
            relpersistence: 'p',
            relkind,
            relispopulated: true,
            relispartition: false,
            relpartbound: None,
            desc,
            partitioned_table: None,
            partition_spec: None,
        }
    }

    fn rewrite(relation_oid: u32, name: &str, enabled: char) -> PgRewriteRow {
        PgRewriteRow {
            oid: relation_oid + enabled as u32,
            rulename: name.into(),
            ev_class: relation_oid,
            ev_type: '1',
            ev_enabled: enabled,
            is_instead: false,
            ev_qual: String::new(),
            ev_action: String::new(),
        }
    }

    fn constraint(oid: u32, conparentid: u32) -> PgConstraintRow {
        PgConstraintRow {
            oid,
            conname: format!("fk_{oid}"),
            connamespace: 0,
            contype: CONSTRAINT_FOREIGN,
            condeferrable: false,
            condeferred: false,
            conenforced: true,
            convalidated: true,
            conrelid: 0,
            contypid: 0,
            conindid: 0,
            conparentid,
            confrelid: 0,
            confupdtype: ' ',
            confdeltype: ' ',
            confmatchtype: ' ',
            conkey: None,
            confkey: None,
            conpfeqop: None,
            conppeqop: None,
            conffeqop: None,
            confdelsetcols: None,
            conexclop: None,
            conbin: None,
            conislocal: true,
            coninhcount: 0,
            connoinherit: false,
            conperiod: false,
        }
    }

    fn seq_scan(relation_oid: u32) -> Plan {
        Plan::SeqScan {
            plan_info: PlanEstimate::default(),
            source_id: 0,
            parallel_scan_id: None,
            rel: RelFileLocator {
                spc_oid: 0,
                db_oid: 0,
                rel_number: relation_oid,
            },
            relation_name: format!("r{relation_oid}"),
            relation_oid,
            relkind: 'r',
            relispopulated: true,
            toast: None,
            tablesample: None,
            desc: RelationDesc {
                columns: Vec::new(),
            },
            disabled: false,
            parallel_aware: false,
        }
    }
}
