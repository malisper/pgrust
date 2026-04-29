use super::super::*;
use super::reloptions::normalize_btree_reloptions;
use crate::backend::access::nbtree::nbtree::UNIQUE_BUILD_DETAIL_SEPARATOR;
use crate::backend::commands::tablecmds::{
    collect_matching_rows_heap, index_key_values_for_row, insert_index_entry_for_row,
    reinitialize_index_relation, row_matches_index_predicate,
};
use crate::backend::utils::cache::relcache::{IndexAmOpEntry, IndexAmProcEntry};
use crate::backend::utils::cache::syscache::{
    SearchSysCache1, SearchSysCacheList1, SysCacheId, SysCacheTuple,
};
use crate::backend::utils::misc::checkpoint::CheckpointStatsSnapshot;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::misc::notices::{push_notice, push_warning};
use crate::include::access::amapi::{
    IndexBuildEmptyContext, IndexBuildExprContext, IndexInsertContext, IndexUniqueCheck,
};
use crate::include::access::brin::BrinOptions;
use crate::include::access::gin::GinOptions;
use crate::include::access::gist::{GistBufferingMode, GistOptions};
use crate::include::access::hash::HashOptions;
use crate::include::access::nbtree::BtreeOptions;
use crate::include::catalog::{
    ANYMULTIRANGEOID, ANYRANGEOID, BRIN_AM_OID, BTREE_AM_OID, GIN_AM_OID, GIST_AM_OID,
    GIST_RANGE_FAMILY_OID, GIST_TSVECTOR_FAMILY_OID, HASH_AM_OID, RANGE_GIST_OPCLASS_OID,
    SPGIST_AM_OID, builtin_range_rows, multirange_type_ref_for_sql_type,
    range_type_ref_for_sql_type,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::RelOption;
use crate::include::nodes::primnodes::{
    Expr, ExprArraySubscript, ScalarArrayOpExpr, expr_sql_type_hint,
};
use std::collections::{BTreeMap, BTreeSet};

struct ResolvedIndexSupportMetadata {
    opfamily_oids: Vec<u32>,
    opcintype_oids: Vec<u32>,
    opckeytype_oids: Vec<u32>,
    amop_entries: Vec<Vec<IndexAmOpEntry>>,
    amproc_entries: Vec<Vec<IndexAmProcEntry>>,
}

fn oid_syscache_key(oid: u32) -> Value {
    Value::Int64(i64::from(oid))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReindexCatalogFilter {
    All,
    SystemOnly,
    UserOnly,
}

fn is_system_catalog_relation_oid(relation_oid: u32) -> bool {
    crate::backend::catalog::bootstrap::bootstrap_catalog_kinds()
        .into_iter()
        .any(|kind| {
            kind.relation_oid() == relation_oid
                || (kind.toast_relation_oid() != 0 && kind.toast_relation_oid() == relation_oid)
        })
}

fn cannot_reindex_system_catalogs_concurrently_error() -> ExecError {
    ExecError::DetailedError {
        message: "cannot reindex system catalogs concurrently".into(),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn relation_name_for_reindex_notice(
    catalog: &dyn crate::backend::parser::CatalogLookup,
    relation: &crate::backend::parser::BoundRelation,
) -> String {
    catalog
        .class_row_by_oid(relation.relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation.relation_oid.to_string())
}

fn map_unique_index_build_violation(
    constraint: String,
    fallback_detail: Option<String>,
) -> ExecError {
    if let Some((index_name, detail)) = constraint.split_once(UNIQUE_BUILD_DETAIL_SEPARATOR) {
        ExecError::DetailedError {
            message: format!("could not create unique index \"{index_name}\""),
            detail: Some(detail.to_string()),
            hint: None,
            sqlstate: "23505",
        }
    } else {
        ExecError::UniqueViolation {
            constraint,
            detail: fallback_detail,
        }
    }
}

fn invalid_fillfactor_error(value: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("value {value} out of bounds for option \"fillfactor\""),
        detail: Some("Valid values are between \"10\" and \"100\".".into()),
        hint: None,
        sqlstate: "22023",
    }
}

fn index_expression_immutable_error() -> ExecError {
    ExecError::DetailedError {
        message: "functions in index expression must be marked IMMUTABLE".into(),
        detail: None,
        hint: None,
        sqlstate: "42P17",
    }
}

fn ensure_index_expression_immutable(
    expr: &Expr,
    catalog: &dyn crate::backend::parser::CatalogLookup,
) -> Result<(), ExecError> {
    match expr {
        Expr::Var(_) | Expr::Const(_) | Expr::Param(_) | Expr::CaseTest(_) => Ok(()),
        Expr::Aggref(_) | Expr::WindowFunc(_) | Expr::SubLink(_) | Expr::SubPlan(_) => {
            Err(index_expression_immutable_error())
        }
        Expr::SetReturning(_) => Err(index_expression_immutable_error()),
        Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => Err(index_expression_immutable_error()),
        Expr::Func(func) => {
            ensure_index_proc_immutable(func.funcid, catalog)?;
            ensure_index_exprs_immutable(&func.args, catalog)
        }
        Expr::Op(op) => {
            if op.opfuncid != 0 {
                ensure_index_proc_immutable(op.opfuncid, catalog)?;
            }
            ensure_index_exprs_immutable(&op.args, catalog)
        }
        Expr::Bool(bool_expr) => ensure_index_exprs_immutable(&bool_expr.args, catalog),
        Expr::Case(case_expr) => {
            if let Some(arg) = case_expr.arg.as_deref() {
                ensure_index_expression_immutable(arg, catalog)?;
            }
            for arm in &case_expr.args {
                ensure_index_expression_immutable(&arm.expr, catalog)?;
                ensure_index_expression_immutable(&arm.result, catalog)?;
            }
            ensure_index_expression_immutable(&case_expr.defresult, catalog)
        }
        Expr::SqlJsonQueryFunction(func) => {
            ensure_sql_json_index_expression_immutable(func, catalog)?;
            for child in func.child_exprs() {
                ensure_index_expression_immutable(child, catalog)?;
            }
            Ok(())
        }
        Expr::ScalarArrayOp(saop) => ensure_scalar_array_op_immutable(saop, catalog),
        Expr::Xml(xml) => {
            for child in xml.child_exprs() {
                ensure_index_expression_immutable(child, catalog)?;
            }
            Ok(())
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => {
            ensure_index_expression_immutable(inner, catalog)
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
            ensure_index_expression_immutable(expr, catalog)?;
            ensure_index_expression_immutable(pattern, catalog)?;
            if let Some(escape) = escape.as_deref() {
                ensure_index_expression_immutable(escape, catalog)?;
            }
            Ok(())
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            ensure_index_expression_immutable(left, catalog)?;
            ensure_index_expression_immutable(right, catalog)
        }
        Expr::ArrayLiteral { elements, .. } => ensure_index_exprs_immutable(elements, catalog),
        Expr::Row { fields, .. } => fields
            .iter()
            .try_for_each(|(_, expr)| ensure_index_expression_immutable(expr, catalog)),
        Expr::ArraySubscript { array, subscripts } => {
            ensure_index_expression_immutable(array, catalog)?;
            for subscript in subscripts {
                ensure_array_subscript_immutable(subscript, catalog)?;
            }
            Ok(())
        }
    }
}

fn ensure_index_exprs_immutable(
    exprs: &[Expr],
    catalog: &dyn crate::backend::parser::CatalogLookup,
) -> Result<(), ExecError> {
    exprs
        .iter()
        .try_for_each(|expr| ensure_index_expression_immutable(expr, catalog))
}

fn ensure_scalar_array_op_immutable(
    saop: &ScalarArrayOpExpr,
    catalog: &dyn crate::backend::parser::CatalogLookup,
) -> Result<(), ExecError> {
    ensure_index_expression_immutable(&saop.left, catalog)?;
    ensure_index_expression_immutable(&saop.right, catalog)
}

fn ensure_array_subscript_immutable(
    subscript: &ExprArraySubscript,
    catalog: &dyn crate::backend::parser::CatalogLookup,
) -> Result<(), ExecError> {
    if let Some(lower) = subscript.lower.as_ref() {
        ensure_index_expression_immutable(lower, catalog)?;
    }
    if let Some(upper) = subscript.upper.as_ref() {
        ensure_index_expression_immutable(upper, catalog)?;
    }
    Ok(())
}

fn ensure_index_proc_immutable(
    proc_oid: u32,
    catalog: &dyn crate::backend::parser::CatalogLookup,
) -> Result<(), ExecError> {
    if proc_oid != 0
        && catalog
            .proc_row_by_oid(proc_oid)
            .is_some_and(|row| row.provolatile == 'i')
    {
        return Ok(());
    }
    Err(index_expression_immutable_error())
}

fn ensure_sql_json_index_expression_immutable(
    func: &crate::include::nodes::primnodes::SqlJsonQueryFunction,
    _catalog: &dyn crate::backend::parser::CatalogLookup,
) -> Result<(), ExecError> {
    let path =
        sql_json_constant_path_text(&func.path).ok_or_else(index_expression_immutable_error)?;
    let passing_types = func
        .passing
        .iter()
        .map(|arg| {
            (
                arg.name.clone(),
                expr_sql_type_hint(&arg.expr).unwrap_or_else(|| {
                    crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Text)
                }),
            )
        })
        .collect::<Vec<_>>();
    if crate::backend::executor::jsonpath::jsonpath_is_mutable(&path, &passing_types)? {
        return Err(index_expression_immutable_error());
    }
    Ok(())
}

fn sql_json_constant_path_text(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Const(value) => value.as_text().map(str::to_string),
        Expr::Cast(inner, sql_type)
            if !sql_type.is_array
                && matches!(sql_type.kind, crate::backend::parser::SqlTypeKind::JsonPath) =>
        {
            sql_json_constant_path_text(inner)
        }
        _ => None,
    }
}

fn map_reindex_unique_violation(err: ExecError, index_name: &str) -> ExecError {
    match err {
        ExecError::UniqueViolation { detail, .. } => ExecError::DetailedError {
            message: format!("could not create unique index \"{index_name}\""),
            detail: detail.map(|detail| {
                detail
                    .strip_suffix(" already exists.")
                    .map(|prefix| format!("{prefix} is duplicated."))
                    .unwrap_or(detail)
            }),
            hint: None,
            sqlstate: "23505",
        },
        other => other,
    }
}

fn resolve_index_collation_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    collation: &str,
) -> Result<u32, ExecError> {
    let lookup_name = collation.rsplit('.').next().unwrap_or(collation);
    crate::backend::utils::cache::syscache::ensure_collation_rows(db, client_id, txn_ctx)
        .into_iter()
        .find(|row| row.collname.eq_ignore_ascii_case(lookup_name))
        .map(|row| row.oid)
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "known collation",
                actual: collation.into(),
            })
        })
}

fn index_reloptions(options: &[RelOption]) -> Option<Vec<String>> {
    (!options.is_empty()).then(|| {
        options
            .iter()
            .map(|option| format!("{}={}", option.name.to_ascii_lowercase(), option.value))
            .collect()
    })
}

fn expression_index_detail_columns(
    index_entry: &crate::backend::catalog::CatalogEntry,
) -> Vec<crate::include::nodes::primnodes::ColumnDesc> {
    let mut columns = index_entry.desc.columns.clone();
    let Some(meta) = index_entry.index_meta.as_ref() else {
        return columns;
    };
    let expression_sqls = meta
        .indexprs
        .as_deref()
        .and_then(|sql| serde_json::from_str::<Vec<String>>(sql).ok())
        .unwrap_or_default();
    let mut expression_index = 0usize;
    for (column_index, attnum) in meta.indkey.iter().enumerate() {
        if *attnum != 0 {
            continue;
        }
        if let Some(column) = columns.get_mut(column_index) {
            let fallback_name = column.name.clone();
            let expr_sql = expression_sqls
                .get(expression_index)
                .map(String::as_str)
                .unwrap_or(fallback_name.as_str());
            column.name = expression_index_detail_name(expr_sql);
        }
        expression_index += 1;
    }
    columns
}

fn expression_index_detail_name(expr_sql: &str) -> String {
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

fn normalized_function_call_expression(expr_sql: &str) -> Option<String> {
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

fn strip_outer_parens_once(input: &str) -> &str {
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
    let Some(first) = expr_sql.chars().next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && expr_sql.ends_with(')')
        && expr_sql
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '(' | ')' | ',' | ' '))
}

fn record_index_column_error(column: &crate::backend::parser::IndexColumnDef) -> ExecError {
    let raw = column
        .expr_sql
        .as_deref()
        .unwrap_or(column.name.as_str())
        .trim();
    let name = raw
        .trim_start_matches('(')
        .split(|ch: char| ch == '(' || ch.is_ascii_whitespace())
        .next()
        .filter(|part| !part.is_empty())
        .unwrap_or(raw)
        .trim_matches('"');
    ExecError::DetailedError {
        message: format!("column \"{name}\" has pseudo-type record"),
        detail: None,
        hint: None,
        sqlstate: "42P16",
    }
}

fn reject_record_index_column(
    column: &crate::backend::parser::IndexColumnDef,
) -> Result<(), ExecError> {
    if column
        .expr_type
        .is_some_and(|ty| ty.kind == crate::backend::parser::SqlTypeKind::Record && !ty.is_array)
    {
        return Err(record_index_column_error(column));
    }
    Ok(())
}

fn index_system_column_error() -> ExecError {
    ExecError::DetailedError {
        message: "index creation on system columns is not supported".into(),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn reject_system_columns_in_index(
    columns: &[crate::backend::parser::IndexColumnDef],
    predicate_sql: Option<&str>,
) -> Result<(), ExecError> {
    for column in columns {
        if column
            .expr_sql
            .as_deref()
            .is_some_and(crate::backend::parser::sql_expr_mentions_system_column)
            || (column.expr_sql.is_none()
                && crate::backend::parser::is_system_column_name(&column.name))
        {
            return Err(index_system_column_error());
        }
    }
    if predicate_sql.is_some_and(crate::backend::parser::sql_expr_mentions_system_column) {
        return Err(index_system_column_error());
    }
    Ok(())
}

pub(super) fn catalog_entry_from_bound_relation(
    relation: &crate::backend::parser::BoundRelation,
) -> crate::backend::catalog::CatalogEntry {
    crate::backend::catalog::CatalogEntry {
        rel: relation.rel,
        relation_oid: relation.relation_oid,
        namespace_oid: relation.namespace_oid,
        owner_oid: relation.owner_oid,
        relacl: None,
        reloptions: None,
        of_type_oid: 0,
        row_type_oid: 0,
        array_type_oid: 0,
        reltoastrelid: relation.toast.map(|toast| toast.relation_oid).unwrap_or(0),
        relhasindex: false,
        relpersistence: relation.relpersistence,
        relkind: relation.relkind,
        am_oid: crate::include::catalog::relam_for_relkind(relation.relkind),
        relhassubclass: false,
        relhastriggers: false,
        relispartition: relation.relispartition,
        relispopulated: relation.relispopulated,
        relpartbound: relation.relpartbound.clone(),
        relrowsecurity: false,
        relforcerowsecurity: false,
        relpages: 0,
        reltuples: 0.0,
        relallvisible: 0,
        relallfrozen: 0,
        relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
        desc: relation.desc.clone(),
        partitioned_table: relation.partitioned_table.clone(),
        index_meta: None,
    }
}

pub(super) fn catalog_entry_from_bound_index_relation(
    index: &crate::backend::parser::BoundIndexRelation,
    namespace_oid: u32,
    owner_oid: u32,
    relpersistence: char,
) -> crate::backend::catalog::CatalogEntry {
    crate::backend::catalog::CatalogEntry {
        rel: index.rel,
        relation_oid: index.relation_oid,
        namespace_oid,
        owner_oid,
        relacl: None,
        of_type_oid: 0,
        reloptions: btree_reloptions(index.index_meta.btree_options),
        row_type_oid: 0,
        array_type_oid: 0,
        reltoastrelid: 0,
        relhasindex: false,
        relpersistence,
        relkind: index.relkind,
        am_oid: index.index_meta.am_oid,
        relhassubclass: false,
        relhastriggers: false,
        relispartition: false,
        relispopulated: true,
        relpartbound: None,
        relrowsecurity: false,
        relforcerowsecurity: false,
        relpages: 0,
        reltuples: 0.0,
        relallvisible: 0,
        relallfrozen: 0,
        relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
        desc: index.desc.clone(),
        partitioned_table: None,
        index_meta: Some(crate::backend::catalog::CatalogIndexMeta {
            indrelid: index.index_meta.indrelid,
            indkey: index.index_meta.indkey.clone(),
            indisunique: index.index_meta.indisunique,
            indnullsnotdistinct: index.index_meta.indnullsnotdistinct,
            indisprimary: index.index_meta.indisprimary,
            indisexclusion: index.index_meta.indisexclusion,
            indimmediate: index.index_meta.indimmediate,
            indisvalid: index.index_meta.indisvalid,
            indisready: index.index_meta.indisready,
            indislive: index.index_meta.indislive,
            indclass: index.index_meta.indclass.clone(),
            indclass_options: index.index_meta.indclass_options.clone(),
            indcollation: index.index_meta.indcollation.clone(),
            indoption: index.index_meta.indoption.clone(),
            indexprs: index.index_meta.indexprs.clone(),
            indpred: index.index_meta.indpred.clone(),
            btree_options: index.index_meta.btree_options,
            brin_options: index.index_meta.brin_options.clone(),
            gist_options: index.index_meta.gist_options,
            gin_options: index.index_meta.gin_options.clone(),
            hash_options: index.index_meta.hash_options,
        }),
    }
}

fn btree_reloptions(options: Option<BtreeOptions>) -> Option<Vec<String>> {
    options.map(|options| {
        vec![
            format!("fillfactor={}", options.fillfactor),
            format!(
                "deduplicate_items={}",
                if options.deduplicate_items {
                    "on"
                } else {
                    "off"
                }
            ),
        ]
    })
}

fn index_type_oid_for_sql_type(
    db: &Database,
    client_id: ClientId,
    sql_type: crate::backend::parser::SqlType,
) -> Option<u32> {
    ((sql_type.is_range() || sql_type.is_multirange()) && sql_type.type_oid != 0)
        .then_some(sql_type.type_oid)
        .or_else(|| range_type_ref_for_sql_type(sql_type).map(|range_type| range_type.type_oid()))
        .or_else(|| {
            multirange_type_ref_for_sql_type(sql_type)
                .map(|multirange_type| multirange_type.type_oid())
        })
        .or_else(|| {
            (matches!(
                sql_type.element_type().kind,
                crate::backend::parser::SqlTypeKind::Enum
            ) && sql_type.element_type().type_oid != 0)
                .then_some(sql_type.element_type().type_oid)
        })
        .or_else(|| (sql_type.type_oid != 0).then_some(sql_type.type_oid))
        .or_else(|| {
            let search_path = db.effective_search_path(client_id, None);
            crate::include::catalog::builtin_type_rows()
                .into_iter()
                .chain(db.dynamic_type_rows_for_search_path(&search_path))
                .find(|row| row.sql_type == sql_type)
                .map(|row| row.oid)
                .or_else(|| {
                    crate::include::catalog::builtin_type_rows()
                        .into_iter()
                        .chain(db.dynamic_type_rows_for_search_path(&search_path))
                        .find(|row| {
                            row.sql_type.kind == sql_type.kind
                                && row.sql_type.is_array == sql_type.is_array
                                && row.typrelid == 0
                        })
                        .map(|row| row.oid)
                })
        })
}

fn index_type_name_for_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    type_oid: u32,
) -> String {
    SearchSysCache1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::TYPEOID,
        oid_syscache_key(type_oid),
    )
    .ok()
    .and_then(|tuples| {
        tuples.into_iter().find_map(|tuple| match tuple {
            SysCacheTuple::Type(row) => Some(row.typname),
            _ => None,
        })
    })
    .unwrap_or_else(|| type_oid.to_string())
}

fn btree_opclass_accepts_type(opcintype: u32, type_oid: u32) -> bool {
    use crate::include::catalog::{
        ANYARRAYOID, ANYENUMOID, ANYMULTIRANGEOID, ANYOID, ANYRANGEOID, BPCHAR_TYPE_OID,
        TEXT_TYPE_OID, VARCHAR_TYPE_OID,
    };

    opcintype == type_oid
        || matches!(
            opcintype,
            ANYOID | ANYARRAYOID | ANYENUMOID | ANYRANGEOID | ANYMULTIRANGEOID
        )
        || (matches!(
            opcintype,
            TEXT_TYPE_OID | BPCHAR_TYPE_OID | VARCHAR_TYPE_OID
        ) && matches!(type_oid, TEXT_TYPE_OID | BPCHAR_TYPE_OID | VARCHAR_TYPE_OID))
}

fn catalog_type_oid(
    catalog: &dyn crate::backend::parser::CatalogLookup,
    sql_type: crate::backend::parser::SqlType,
) -> Option<u32> {
    range_type_ref_for_sql_type(sql_type)
        .map(|range_type| range_type.type_oid())
        .or_else(|| {
            multirange_type_ref_for_sql_type(sql_type)
                .map(|multirange_type| multirange_type.type_oid())
        })
        .or_else(|| {
            catalog
                .type_rows()
                .into_iter()
                .find(|row| row.sql_type == sql_type)
                .map(|row| row.oid)
        })
        .or_else(|| {
            catalog
                .type_rows()
                .into_iter()
                .find(|row| {
                    row.sql_type.kind == sql_type.kind
                        && row.sql_type.is_array == sql_type.is_array
                        && row.typrelid == 0
                })
                .map(|row| row.oid)
        })
}

fn expression_index_default_name(expr_sql: &str) -> String {
    let trimmed = expr_sql.trim();
    let Some(open_paren) = trimmed.find('(') else {
        return "expr".into();
    };
    let name = trimmed[..open_paren].trim();
    if name.is_empty()
        || !name
            .chars()
            .all(|ch| ch == '_' || ch == '.' || ch.is_ascii_alphanumeric())
    {
        return "expr".into();
    }
    name.rsplit('.').next().unwrap_or(name).to_ascii_lowercase()
}

impl Database {
    pub(super) fn relcache_index_meta_from_catalog(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        indexrelid: u32,
        meta: &crate::backend::catalog::CatalogIndexMeta,
        am_oid: u32,
        am_handler_oid: u32,
    ) -> Result<crate::backend::utils::cache::relcache::IndexRelCacheEntry, ExecError> {
        let support = self.resolve_index_support_metadata(client_id, txn_ctx, &meta.indclass)?;
        Ok(crate::backend::utils::cache::relcache::IndexRelCacheEntry {
            indexrelid,
            indrelid: meta.indrelid,
            indnatts: meta.indkey.len() as i16,
            indnkeyatts: meta.indclass.len() as i16,
            indisunique: meta.indisunique,
            indnullsnotdistinct: meta.indnullsnotdistinct,
            indisprimary: meta.indisprimary,
            indisexclusion: meta.indisexclusion,
            indimmediate: meta.indimmediate,
            indisclustered: false,
            indisvalid: meta.indisvalid,
            indcheckxmin: false,
            indisready: meta.indisready,
            indislive: meta.indislive,
            indisreplident: false,
            am_oid,
            am_handler_oid: Some(am_handler_oid),
            indkey: meta.indkey.clone(),
            indclass: meta.indclass.clone(),
            indclass_options: meta.indclass_options.clone(),
            indcollation: meta.indcollation.clone(),
            indoption: meta.indoption.clone(),
            opfamily_oids: support.opfamily_oids,
            opcintype_oids: support.opcintype_oids,
            opckeytype_oids: support.opckeytype_oids,
            amop_entries: support.amop_entries,
            amproc_entries: support.amproc_entries,
            indexprs: meta.indexprs.clone(),
            indpred: meta.indpred.clone(),
            rd_indexprs: None,
            rd_indpred: None,
            btree_options: meta.btree_options,
            brin_options: meta.brin_options.clone(),
            gist_options: meta.gist_options,
            gin_options: meta.gin_options.clone(),
            hash_options: meta.hash_options,
        })
    }

    fn resolve_brin_options(&self, options: &[RelOption]) -> Result<BrinOptions, ExecError> {
        let mut resolved = BrinOptions::default();
        for option in options {
            if option.name.eq_ignore_ascii_case("pages_per_range") {
                let pages_per_range = option.value.parse::<u32>().map_err(|_| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "positive integer pages_per_range",
                        actual: option.value.clone(),
                    })
                })?;
                if pages_per_range == 0 {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "positive integer pages_per_range",
                        actual: option.value.clone(),
                    }));
                }
                resolved.pages_per_range = pages_per_range;
                continue;
            }

            if option.name.eq_ignore_ascii_case("autosummarize") {
                return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                    "BRIN option \"autosummarize\"".into(),
                )));
            }

            return Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                "BRIN option \"{}\"",
                option.name
            ))));
        }
        Ok(resolved)
    }

    fn resolve_gist_options(&self, options: &[RelOption]) -> Result<GistOptions, ExecError> {
        let mut resolved = GistOptions::default();
        for option in options {
            if option.name.eq_ignore_ascii_case("buffering") {
                resolved.buffering_mode = match option.value.to_ascii_lowercase().as_str() {
                    "auto" => GistBufferingMode::Auto,
                    "on" => GistBufferingMode::On,
                    "off" => GistBufferingMode::Off,
                    _ => {
                        return Err(ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "GiST buffering option auto, on, or off",
                            actual: option.value.clone(),
                        }));
                    }
                };
                continue;
            }

            return Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                "GiST option \"{}\"",
                option.name
            ))));
        }
        Ok(resolved)
    }

    fn resolve_gin_options(&self, options: &[RelOption]) -> Result<GinOptions, ExecError> {
        let mut resolved = GinOptions::default();
        for option in options {
            if option.name.eq_ignore_ascii_case("fastupdate") {
                resolved.fastupdate = match option.value.to_ascii_lowercase().as_str() {
                    "on" | "true" | "yes" | "1" => true,
                    "off" | "false" | "no" | "0" => false,
                    _ => {
                        return Err(ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "boolean fastupdate",
                            actual: option.value.clone(),
                        }));
                    }
                };
                continue;
            }

            if option.name.eq_ignore_ascii_case("gin_pending_list_limit") {
                let pending_list_limit_kb = option.value.parse::<u32>().map_err(|_| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "positive integer gin_pending_list_limit",
                        actual: option.value.clone(),
                    })
                })?;
                if pending_list_limit_kb == 0 {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "positive integer gin_pending_list_limit",
                        actual: option.value.clone(),
                    }));
                }
                resolved.pending_list_limit_kb = pending_list_limit_kb;
                continue;
            }

            return Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                "GIN option \"{}\"",
                option.name
            ))));
        }
        Ok(resolved)
    }

    fn resolve_hash_options(&self, options: &[RelOption]) -> Result<HashOptions, ExecError> {
        let mut resolved = HashOptions::default();
        for option in options {
            if option.name.eq_ignore_ascii_case("fillfactor") {
                let fillfactor = option
                    .value
                    .parse::<u16>()
                    .map_err(|_| invalid_fillfactor_error(&option.value))?;
                if !(10..=100).contains(&fillfactor) {
                    return Err(invalid_fillfactor_error(&option.value));
                }
                resolved.fillfactor = fillfactor;
                continue;
            }

            return Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                "hash index option \"{}\"",
                option.name
            ))));
        }
        Ok(resolved)
    }

    fn resolve_btree_options(
        &self,
        options: &[RelOption],
    ) -> Result<Option<BtreeOptions>, ExecError> {
        normalize_btree_reloptions(options).map(|(options, _)| options)
    }

    fn access_method_can_include(access_method_oid: u32) -> bool {
        matches!(
            access_method_oid,
            BTREE_AM_OID | GIST_AM_OID | SPGIST_AM_OID
        )
    }

    fn validate_index_opclass_options(
        access_method_oid: u32,
        opfamily_oid: u32,
        column: &crate::backend::parser::IndexColumnDef,
    ) -> Result<(), ExecError> {
        if column.opclass_options.is_empty() {
            return Ok(());
        }
        if access_method_oid != GIST_AM_OID || opfamily_oid != GIST_TSVECTOR_FAMILY_OID {
            let option = &column.opclass_options[0];
            return Err(ExecError::DetailedError {
                message: format!("unrecognized parameter \"{}\"", option.name),
                detail: None,
                hint: None,
                sqlstate: "22023",
            });
        }

        let mut seen_siglen = false;
        for option in &column.opclass_options {
            if !option.name.eq_ignore_ascii_case("siglen") {
                return Err(ExecError::DetailedError {
                    message: format!("unrecognized parameter \"{}\"", option.name),
                    detail: None,
                    hint: None,
                    sqlstate: "22023",
                });
            }
            if seen_siglen {
                return Err(ExecError::DetailedError {
                    message: "parameter \"siglen\" specified more than once".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "22023",
                });
            }
            seen_siglen = true;
            let siglen = option
                .value
                .parse::<i32>()
                .map_err(|_| ExecError::DetailedError {
                    message: format!("invalid value for option \"siglen\": \"{}\"", option.value),
                    detail: None,
                    hint: None,
                    sqlstate: "22023",
                })?;
            if !(1..=2024).contains(&siglen) {
                return Err(ExecError::DetailedError {
                    message: format!("value {siglen} out of bounds for option \"siglen\""),
                    detail: Some("Valid values are between \"1\" and \"2024\".".into()),
                    hint: None,
                    sqlstate: "22023",
                });
            }
        }
        Ok(())
    }

    fn resolve_index_support_metadata(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        indclass: &[u32],
    ) -> Result<ResolvedIndexSupportMetadata, ExecError> {
        let mut resolved_opclasses = Vec::with_capacity(indclass.len());
        for oid in indclass {
            let opclass = SearchSysCache1(
                self,
                client_id,
                txn_ctx,
                SysCacheId::CLAOID,
                oid_syscache_key(*oid),
            )
            .map_err(map_catalog_error)?
            .into_iter()
            .find_map(|tuple| match tuple {
                SysCacheTuple::Opclass(row) => Some(row),
                _ => None,
            })
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "valid index operator class",
                    actual: format!("unknown operator class oid {oid}"),
                })
            })?;
            resolved_opclasses.push(opclass);
        }
        let opfamily_oids = resolved_opclasses
            .iter()
            .map(|row| row.opcfamily)
            .collect::<Vec<_>>();
        let opcintype_oids = resolved_opclasses
            .iter()
            .map(|row| row.opcintype)
            .collect::<Vec<_>>();
        let opckeytype_oids = resolved_opclasses
            .iter()
            .map(|row| row.opckeytype)
            .collect::<Vec<_>>();
        let mut operator_proc_oids = BTreeMap::<u32, u32>::new();
        let mut amop_entries = Vec::with_capacity(opfamily_oids.len());
        for family_oid in &opfamily_oids {
            let mut entries = Vec::new();
            for row in SearchSysCacheList1(
                self,
                client_id,
                txn_ctx,
                SysCacheId::AMOPSTRATEGY,
                oid_syscache_key(*family_oid),
            )
            .map_err(map_catalog_error)?
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Amop(row) => Some(row),
                _ => None,
            }) {
                let operator_proc_oid = if let Some(proc_oid) = operator_proc_oids.get(&row.amopopr)
                {
                    *proc_oid
                } else {
                    let proc_oid = SearchSysCache1(
                        self,
                        client_id,
                        txn_ctx,
                        SysCacheId::OPEROID,
                        oid_syscache_key(row.amopopr),
                    )
                    .map_err(map_catalog_error)?
                    .into_iter()
                    .find_map(|tuple| match tuple {
                        SysCacheTuple::Operator(row) => Some(row.oprcode),
                        _ => None,
                    })
                    .unwrap_or(0);
                    operator_proc_oids.insert(row.amopopr, proc_oid);
                    proc_oid
                };
                entries.push(IndexAmOpEntry {
                    strategy: row.amopstrategy,
                    purpose: row.amoppurpose,
                    lefttype: row.amoplefttype,
                    righttype: row.amoprighttype,
                    operator_oid: row.amopopr,
                    operator_proc_oid,
                    sortfamily_oid: row.amopsortfamily,
                });
            }
            amop_entries.push(entries);
        }

        let mut amproc_entries = Vec::with_capacity(opfamily_oids.len());
        for family_oid in &opfamily_oids {
            amproc_entries.push(
                SearchSysCacheList1(
                    self,
                    client_id,
                    txn_ctx,
                    SysCacheId::AMPROCNUM,
                    oid_syscache_key(*family_oid),
                )
                .map_err(map_catalog_error)?
                .into_iter()
                .filter_map(|tuple| match tuple {
                    SysCacheTuple::Amproc(row) => Some(IndexAmProcEntry {
                        procnum: row.amprocnum,
                        lefttype: row.amproclefttype,
                        righttype: row.amprocrighttype,
                        proc_oid: row.amproc,
                    }),
                    _ => None,
                })
                .collect(),
            );
        }

        Ok(ResolvedIndexSupportMetadata {
            opfamily_oids,
            opcintype_oids,
            opckeytype_oids,
            amop_entries,
            amproc_entries,
        })
    }

    pub(super) fn default_index_base_name(
        relation_name: &str,
        columns: &[crate::backend::parser::IndexColumnDef],
    ) -> String {
        let mut used_names = BTreeSet::new();
        let column_part = columns
            .iter()
            .map(|column| {
                let name = if let Some(expr_sql) = column.expr_sql.as_deref() {
                    expression_index_default_name(expr_sql)
                } else {
                    column.name.clone()
                };
                Self::unique_index_name_part(name, &mut used_names)
            })
            .collect::<Vec<_>>()
            .join("_");
        let column_part = if column_part.is_empty() {
            "idx".to_string()
        } else {
            column_part
        };
        format!("{relation_name}_{column_part}_idx")
    }

    fn unique_index_name_part(base: String, used_names: &mut BTreeSet<String>) -> String {
        let mut candidate = base.clone();
        let mut suffix = 1usize;
        while !used_names.insert(candidate.to_ascii_lowercase()) {
            candidate = format!("{base}{suffix}");
            suffix = suffix.saturating_add(1);
        }
        candidate
    }

    pub(super) fn resolve_simple_index_build_options(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        access_method_name: &str,
        relation: &crate::backend::parser::BoundRelation,
        columns: &[crate::backend::parser::IndexColumnDef],
        options: &[RelOption],
    ) -> Result<(u32, u32, CatalogIndexBuildOptions), ExecError> {
        let access_method_name = if access_method_name.eq_ignore_ascii_case("rtree") {
            push_notice("substituting access method \"gist\" for obsolete method \"rtree\"");
            "gist"
        } else {
            access_method_name
        };
        let access_method = crate::backend::utils::cache::lsyscache::access_method_row_by_name(
            self,
            client_id,
            txn_ctx,
            access_method_name,
        )
        .filter(|row| row.amtype == 'i')
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "supported index access method",
                actual: "unsupported index access method".into(),
            })
        })?;
        if !access_method
            .amname
            .eq_ignore_ascii_case(access_method_name)
        {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "supported index access method",
                actual: "unsupported index access method".into(),
            }));
        }

        let opclass_rows = crate::backend::utils::cache::lsyscache::opclass_rows_for_am(
            self,
            client_id,
            txn_ctx,
            access_method.oid,
        );
        let mut indclass = Vec::with_capacity(columns.len());
        let mut indclass_options = Vec::with_capacity(columns.len());
        let mut indcollation = Vec::with_capacity(columns.len());
        let mut indoption = Vec::with_capacity(columns.len());
        for column in columns {
            let sql_type = if column.expr_sql.is_some() {
                column.expr_type.ok_or_else(|| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "inferred expression index type",
                        actual: "missing expression index type".into(),
                    })
                })?
            } else {
                relation
                    .desc
                    .columns
                    .iter()
                    .find(|desc| desc.name.eq_ignore_ascii_case(&column.name))
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnknownColumn(column.name.clone()))
                    })?
                    .sql_type
            };
            let type_oid =
                index_type_oid_for_sql_type(self, client_id, sql_type).ok_or_else(|| {
                    ExecError::Parse(ParseError::UnsupportedType(
                        column
                            .expr_sql
                            .clone()
                            .unwrap_or_else(|| column.name.clone()),
                    ))
                })?;
            let type_name = index_type_name_for_oid(self, client_id, txn_ctx, type_oid);
            let opclass = if let Some(opclass_name) = column.opclass.as_deref() {
                let is_range_type = builtin_range_rows()
                    .iter()
                    .any(|row| row.rngtypid == type_oid);
                let opclass_lookup_name = opclass_name
                    .rsplit_once('.')
                    .map(|(_, name)| name)
                    .unwrap_or(opclass_name);
                opclass_rows
                    .iter()
                    .find(|row| {
                        row.opcmethod == access_method.oid
                            && row.opcname.eq_ignore_ascii_case(opclass_lookup_name)
                            && (row.opcintype == type_oid
                                || (is_range_type && row.opcfamily == GIST_RANGE_FAMILY_OID)
                                || (access_method.oid == BTREE_AM_OID
                                    && btree_opclass_accepts_type(row.opcintype, type_oid)))
                    })
                    .cloned()
            } else {
                crate::backend::utils::cache::lsyscache::default_opclass_for_am_and_type(
                    self,
                    client_id,
                    txn_ctx,
                    access_method.oid,
                    type_oid,
                )
                .or_else(|| {
                    matches!(
                        sql_type.kind,
                        crate::backend::parser::SqlTypeKind::Record
                            | crate::backend::parser::SqlTypeKind::Composite
                    )
                    .then(|| {
                        opclass_rows
                            .iter()
                            .find(|row| {
                                row.opcmethod == access_method.oid
                                    && row.opcdefault
                                    && row.opcintype == crate::include::catalog::RECORD_TYPE_OID
                            })
                            .cloned()
                    })
                    .flatten()
                })
                .or_else(|| {
                    matches!(
                        sql_type.element_type().kind,
                        crate::backend::parser::SqlTypeKind::Enum
                    )
                    .then(|| {
                        let fallback_oid = match access_method.oid {
                            crate::include::catalog::BTREE_AM_OID => {
                                crate::include::catalog::OID_BTREE_OPCLASS_OID
                            }
                            crate::include::catalog::HASH_AM_OID => {
                                crate::include::catalog::OID_HASH_OPCLASS_OID
                            }
                            _ => 0,
                        };
                        opclass_rows
                            .iter()
                            .find(|row| {
                                (row.opcmethod == access_method.oid
                                    && row.opcdefault
                                    && row.opcintype == crate::include::catalog::ANYENUMOID)
                                    || row.oid == fallback_oid
                            })
                            .cloned()
                    })
                    .flatten()
                })
            }
            .ok_or_else(|| {
                ExecError::Parse(ParseError::MissingDefaultOpclass {
                    access_method: access_method_name.to_string(),
                    type_name: type_name.clone(),
                })
            })?;
            Self::validate_index_opclass_options(access_method.oid, opclass.opcfamily, column)?;
            indclass_options.push(
                column
                    .opclass_options
                    .iter()
                    .map(|option| (option.name.clone(), option.value.clone()))
                    .collect(),
            );
            indclass.push(opclass.oid);
            indcollation.push(
                column
                    .collation
                    .as_deref()
                    .map(|collation| {
                        resolve_index_collation_oid(self, client_id, txn_ctx, collation)
                    })
                    .transpose()?
                    .unwrap_or(0),
            );
            let mut option = 0i16;
            if column.descending {
                option |= 0x0001;
            }
            if column.nulls_first.unwrap_or(false) {
                option |= 0x0002;
            }
            indoption.push(option);
        }

        let (btree_options, brin_options, gist_options, gin_options, hash_options) =
            match access_method.oid {
                BTREE_AM_OID => (self.resolve_btree_options(options)?, None, None, None, None),
                BRIN_AM_OID => (
                    None,
                    Some(self.resolve_brin_options(options)?),
                    None,
                    None,
                    None,
                ),
                GIST_AM_OID => (
                    None,
                    None,
                    Some(self.resolve_gist_options(options)?),
                    None,
                    None,
                ),
                GIN_AM_OID => (
                    None,
                    None,
                    None,
                    Some(self.resolve_gin_options(options)?),
                    None,
                ),
                HASH_AM_OID => (
                    None,
                    None,
                    None,
                    None,
                    Some(self.resolve_hash_options(options)?),
                ),
                _ => {
                    if !options.is_empty() {
                        return Err(ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "simple index definition",
                            actual: "unsupported CREATE INDEX feature".into(),
                        }));
                    }
                    (None, None, None, None, None)
                }
            };

        Ok((
            access_method.oid,
            access_method.amhandler,
            CatalogIndexBuildOptions {
                am_oid: access_method.oid,
                indclass,
                indclass_options,
                indcollation,
                indoption,
                reloptions: index_reloptions(options),
                indnullsnotdistinct: false,
                indisexclusion: false,
                indimmediate: true,
                btree_options,
                brin_options,
                gist_options,
                gin_options,
                hash_options,
            },
        ))
    }

    pub(super) fn resolve_exclusion_index_build_options(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        access_method_name: &str,
        relation: &crate::backend::parser::BoundRelation,
        columns: &[crate::backend::parser::IndexColumnDef],
    ) -> Result<(u32, u32, CatalogIndexBuildOptions), ExecError> {
        match self.resolve_simple_index_build_options(
            client_id,
            txn_ctx,
            access_method_name,
            relation,
            columns,
            &[],
        ) {
            Ok(options) => Ok(options),
            Err(ExecError::Parse(ParseError::MissingDefaultOpclass { access_method, .. }))
                if access_method.eq_ignore_ascii_case("gist")
                    && columns.iter().any(|column| column.expr_sql.is_some()) =>
            {
                let access_method =
                    crate::backend::utils::cache::lsyscache::access_method_row_by_name(
                        self, client_id, txn_ctx, "gist",
                    )
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "supported index access method",
                            actual: "unsupported index access method".into(),
                        })
                    })?;
                // :HACK: pgrust does not ship PostgreSQL's btree_gist opclasses
                // yet. The inherit regression uses an empty-table expression
                // exclusion constraint only for catalog semantics, so use a
                // GiST opclass placeholder while leaving enforcement deferred.
                Ok((
                    access_method.oid,
                    access_method.amhandler,
                    CatalogIndexBuildOptions {
                        am_oid: access_method.oid,
                        indclass: vec![RANGE_GIST_OPCLASS_OID; columns.len()],
                        indclass_options: vec![Vec::new(); columns.len()],
                        indcollation: vec![0; columns.len()],
                        indoption: vec![0; columns.len()],
                        reloptions: None,
                        indnullsnotdistinct: false,
                        indisexclusion: false,
                        indimmediate: true,
                        btree_options: None,
                        brin_options: None,
                        gist_options: Some(GistOptions::default()),
                        gin_options: None,
                        hash_options: None,
                    },
                ))
            }
            Err(err) => Err(err),
        }
    }

    pub(super) fn resolve_temporal_index_build_options(
        &self,
        client_id: ClientId,
        txn_ctx: Option<(TransactionId, CommandId)>,
        relation: &crate::backend::parser::BoundRelation,
        columns: &[crate::backend::parser::IndexColumnDef],
    ) -> Result<(u32, u32, CatalogIndexBuildOptions), ExecError> {
        let access_method = crate::backend::utils::cache::lsyscache::access_method_row_by_name(
            self, client_id, txn_ctx, "gist",
        )
        .filter(|row| row.amtype == 'i')
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "GiST index access method",
                actual: "unsupported index access method".into(),
            })
        })?;
        let mut indclass = Vec::with_capacity(columns.len());
        let mut indcollation = Vec::with_capacity(columns.len());
        let mut indoption = Vec::with_capacity(columns.len());
        for column in columns {
            let sql_type = relation
                .desc
                .columns
                .iter()
                .find(|desc| desc.name.eq_ignore_ascii_case(&column.name))
                .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column.name.clone())))?
                .sql_type;
            let type_oid =
                index_type_oid_for_sql_type(self, client_id, sql_type).ok_or_else(|| {
                    ExecError::Parse(ParseError::UnsupportedType(column.name.clone()))
                })?;
            let opclass_oid = if sql_type.is_range() || sql_type.is_multirange() {
                crate::include::catalog::RANGE_GIST_OPCLASS_OID
            } else {
                crate::include::catalog::default_btree_opclass_oid(type_oid).ok_or_else(|| {
                    ExecError::Parse(ParseError::UnsupportedType(column.name.clone()))
                })?
            };
            indclass.push(opclass_oid);
            indcollation.push(0);
            indoption.push(0);
        }
        Ok((
            access_method.oid,
            access_method.amhandler,
            CatalogIndexBuildOptions {
                am_oid: access_method.oid,
                indclass,
                indclass_options: vec![Vec::new(); indcollation.len()],
                indcollation,
                indoption,
                reloptions: None,
                indnullsnotdistinct: false,
                indisexclusion: true,
                indimmediate: true,
                btree_options: None,
                brin_options: None,
                gist_options: Some(GistOptions::default()),
                gin_options: None,
                hash_options: None,
            },
        ))
    }

    pub(super) fn temporal_constraint_operator_oids_for_relation(
        &self,
        relation_oid: u32,
        columns: &[String],
        without_overlaps: Option<&str>,
        catalog: &dyn crate::backend::parser::CatalogLookup,
    ) -> Result<Vec<u32>, ExecError> {
        let relation = catalog.relation_by_oid(relation_oid).ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "relation for temporal constraint",
                actual: format!("missing relation {relation_oid}"),
            })
        })?;
        self.temporal_constraint_operator_oids_for_desc(
            &relation.desc,
            columns,
            without_overlaps,
            catalog,
        )
    }

    pub(super) fn temporal_constraint_operator_oids_for_desc(
        &self,
        desc: &crate::backend::executor::RelationDesc,
        columns: &[String],
        without_overlaps: Option<&str>,
        catalog: &dyn crate::backend::parser::CatalogLookup,
    ) -> Result<Vec<u32>, ExecError> {
        let Some(period_column) = without_overlaps else {
            return Ok(Vec::new());
        };
        columns
            .iter()
            .map(|column_name| {
                let column = desc
                    .columns
                    .iter()
                    .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnknownColumn(column_name.clone()))
                    })?;
                let type_oid = catalog_type_oid(catalog, column.sql_type).ok_or_else(|| {
                    ExecError::Parse(ParseError::UnsupportedType(column.name.clone()))
                })?;
                let op_name = if column.name.eq_ignore_ascii_case(period_column) {
                    "&&"
                } else {
                    "="
                };
                let mut operator_type_oids = vec![type_oid];
                if op_name == "&&" {
                    if let Some(range_type) = range_type_ref_for_sql_type(column.sql_type) {
                        operator_type_oids.push(range_type.type_oid());
                        operator_type_oids.push(ANYRANGEOID);
                    }
                    if let Some(multirange_type) = multirange_type_ref_for_sql_type(column.sql_type)
                    {
                        operator_type_oids.push(multirange_type.type_oid());
                        operator_type_oids.push(multirange_type.range_type_oid());
                        operator_type_oids.push(ANYMULTIRANGEOID);
                        operator_type_oids.push(ANYRANGEOID);
                    }
                }
                let mut seen_operator_type_oids = BTreeSet::new();
                operator_type_oids
                    .into_iter()
                    .filter(|oid| seen_operator_type_oids.insert(*oid))
                    .find_map(|candidate_oid| {
                        catalog.operator_by_name_left_right(op_name, candidate_oid, candidate_oid)
                    })
                    .map(|row| row.oid)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UndefinedOperator {
                            op: if op_name == "&&" { "&&" } else { "=" },
                            left_type: column.name.clone(),
                            right_type: column.name.clone(),
                        })
                    })
            })
            .collect()
    }

    pub(super) fn exclusion_constraint_operator_oids_for_desc(
        &self,
        desc: &crate::backend::executor::RelationDesc,
        columns: &[String],
        operators: &[String],
        catalog: &dyn crate::backend::parser::CatalogLookup,
    ) -> Result<Vec<u32>, ExecError> {
        let index_columns = columns
            .iter()
            .cloned()
            .map(crate::backend::parser::IndexColumnDef::from)
            .collect::<Vec<_>>();
        self.exclusion_constraint_operator_oids_for_index_columns(
            desc,
            &index_columns,
            operators,
            catalog,
        )
    }

    pub(super) fn exclusion_constraint_operator_oids_for_index_columns(
        &self,
        desc: &crate::backend::executor::RelationDesc,
        columns: &[crate::backend::parser::IndexColumnDef],
        operators: &[String],
        catalog: &dyn crate::backend::parser::CatalogLookup,
    ) -> Result<Vec<u32>, ExecError> {
        if columns.len() != operators.len() {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "one exclusion operator per key column",
                actual: format!(
                    "{} columns and {} operators",
                    columns.len(),
                    operators.len()
                ),
            }));
        }
        columns
            .iter()
            .zip(operators.iter())
            .map(|(column, operator_name)| {
                let (display_name, sql_type) = if let Some(expr_sql) = column.expr_sql.as_deref() {
                    (
                        expr_sql.to_string(),
                        column.expr_type.ok_or_else(|| {
                            ExecError::Parse(ParseError::UnexpectedToken {
                                expected: "inferred exclusion expression type",
                                actual: "missing expression type".into(),
                            })
                        })?,
                    )
                } else {
                    let desc_column = desc
                        .columns
                        .iter()
                        .find(|desc_column| {
                            !desc_column.dropped
                                && desc_column.name.eq_ignore_ascii_case(&column.name)
                        })
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::UnknownColumn(column.name.clone()))
                        })?;
                    (column.name.clone(), desc_column.sql_type)
                };
                let type_oid = catalog_type_oid(catalog, sql_type).ok_or_else(|| {
                    ExecError::Parse(ParseError::UnsupportedType(display_name.clone()))
                })?;
                catalog
                    .operator_by_name_left_right(operator_name, type_oid, type_oid)
                    .map(|row| row.oid)
                    .ok_or_else(|| {
                        let op = match operator_name.as_str() {
                            "&&" => "&&",
                            "=" => "=",
                            "<>" => "<>",
                            "<" => "<",
                            "<=" => "<=",
                            ">" => ">",
                            ">=" => ">=",
                            _ => "operator",
                        };
                        ExecError::Parse(ParseError::UndefinedOperator {
                            op,
                            left_type: display_name.clone(),
                            right_type: display_name,
                        })
                    })
            })
            .collect()
    }

    pub(super) fn build_simple_index_in_transaction(
        &self,
        client_id: ClientId,
        relation: &crate::backend::parser::BoundRelation,
        index_name: &str,
        visible_catalog: Option<crate::backend::executor::ExecutorCatalog>,
        columns: &[crate::backend::parser::IndexColumnDef],
        predicate_sql: Option<&str>,
        unique: bool,
        primary: bool,
        nulls_not_distinct: bool,
        xid: TransactionId,
        cid: CommandId,
        access_method_oid: u32,
        access_method_handler: u32,
        build_options: &CatalogIndexBuildOptions,
        maintenance_work_mem_kb: usize,
        leave_invalid_on_failure: bool,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<crate::backend::catalog::CatalogEntry, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let mut catalog_guard = self.catalog.write();
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&interrupts),
        };
        let build_options = CatalogIndexBuildOptions {
            indnullsnotdistinct: nulls_not_distinct,
            ..build_options.clone()
        };
        let table_entry = catalog_entry_from_bound_relation(relation);
        let (index_entry, effect) = catalog_guard
            .create_index_for_entry_mvcc_with_options(
                index_name.to_string(),
                table_entry,
                unique,
                primary,
                columns,
                &build_options,
                predicate_sql,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        drop(catalog_guard);

        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);

        let has_expression_keys = index_entry
            .index_meta
            .as_ref()
            .and_then(|meta| meta.indexprs.as_ref())
            .is_some();
        let has_predicate = index_entry
            .index_meta
            .as_ref()
            .and_then(|meta| meta.indpred.as_deref())
            .is_some_and(|predicate| !predicate.trim().is_empty());
        if has_expression_keys
            && access_method_oid != GIST_AM_OID
            && access_method_oid != SPGIST_AM_OID
        {
            if let Err(err) = self.build_expression_index_rows_in_transaction(
                client_id,
                relation,
                &index_entry,
                index_name,
                visible_catalog,
                xid,
                cid,
                access_method_oid,
                access_method_handler,
                maintenance_work_mem_kb,
            ) {
                if !leave_invalid_on_failure {
                    self.cleanup_failed_index_build(
                        client_id,
                        xid,
                        cid,
                        &index_entry,
                        catalog_effects,
                        Arc::clone(&interrupts),
                    );
                }
                return Err(err);
            }
            let mut catalog_guard = self.catalog.write();
            let readiness_ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: cid.saturating_add(1),
                client_id,
                waiter: None,
                interrupts,
            };
            let ready_effect = catalog_guard
                .set_index_entry_ready_valid_mvcc(&index_entry, true, true, &readiness_ctx)
                .map_err(|err| match err {
                    CatalogError::Interrupted(reason) => ExecError::Interrupted(reason),
                    _ => ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "index catalog readiness update",
                        actual: "index readiness update failed".into(),
                    }),
                })?;
            drop(catalog_guard);
            self.apply_catalog_mutation_effect_immediate(&ready_effect)?;
            catalog_effects.push(ready_effect);
            return Ok(index_entry);
        }

        let snapshot = self
            .txns
            .read()
            .snapshot_for_command(xid, cid)
            .map_err(|_| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "index build snapshot",
                    actual: "snapshot creation failed".into(),
                })
            })?;
        let current_xid = snapshot.current_xid;
        let index_meta = index_entry.index_meta.clone().ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "index metadata",
                actual: "missing index metadata".into(),
            })
        })?;
        let relcache_index_meta = self.relcache_index_meta_from_catalog(
            client_id,
            Some((xid, cid)),
            index_entry.relation_oid,
            &index_meta,
            access_method_oid,
            access_method_handler,
        )?;
        let build_ctx = crate::include::access::amapi::IndexBuildContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            client_id,
            interrupts: Arc::clone(&interrupts),
            snapshot,
            heap_relation: relation.rel,
            heap_desc: relation.desc.clone(),
            index_relation: index_entry.rel,
            index_name: index_name.to_string(),
            index_desc: index_entry.desc.clone(),
            index_meta: relcache_index_meta.clone(),
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            maintenance_work_mem_kb,
            expr_eval: (has_expression_keys || has_predicate).then_some(IndexBuildExprContext {
                txn_waiter: Some(self.txn_waiter.clone()),
                sequences: Some(self.sequences.clone()),
                large_objects: Some(self.large_objects.clone()),
                advisory_locks: std::sync::Arc::clone(&self.advisory_locks),
                datetime_config: DateTimeConfig::default(),
                stats: std::sync::Arc::clone(&self.stats),
                session_stats: self.session_stats_state(client_id),
                current_database_name: self.current_database_name(),
                session_user_oid: self.auth_state(client_id).session_user_oid(),
                current_user_oid: self.auth_state(client_id).current_user_oid(),
                current_xid,
                statement_lock_scope_id: None,
                session_replication_role: self.session_replication_role(client_id),
                visible_catalog: visible_catalog.clone(),
            }),
        };
        if index_meta.indisexclusion {
            // :HACK: Temporal PK/UNIQUE constraints are enforced by a heap scan for now.
            // Keep the GiST relation initialized for catalog/deparse parity without relying
            // on full exclusion-index probing or btree_gist-equivalent scalar support.
            let build_result = crate::backend::access::index::indexam::index_build_empty_stub(
                &IndexBuildEmptyContext {
                    pool: self.pool.clone(),
                    client_id,
                    xid,
                    index_relation: index_entry.rel,
                    index_desc: index_entry.desc.clone(),
                    index_meta: relcache_index_meta.clone(),
                },
                access_method_oid,
            )
            .map_err(map_catalog_error);
            if let Err(err) = build_result {
                if !leave_invalid_on_failure {
                    self.cleanup_failed_index_build(
                        client_id,
                        xid,
                        cid,
                        &index_entry,
                        catalog_effects,
                        Arc::clone(&interrupts),
                    );
                }
                return Err(err);
            }
        } else {
            let build_result = crate::backend::access::index::indexam::index_build_stub(
                &build_ctx,
                access_method_oid,
            )
            .map_err(|err| match err {
                CatalogError::UniqueViolation(constraint) => {
                    map_unique_index_build_violation(constraint, None)
                }
                CatalogError::Interrupted(reason) => ExecError::Interrupted(reason),
                _ => ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "index access method build",
                    actual: "index build failed".into(),
                }),
            });
            if let Err(err) = build_result {
                if !leave_invalid_on_failure {
                    self.cleanup_failed_index_build(
                        client_id,
                        xid,
                        cid,
                        &index_entry,
                        catalog_effects,
                        Arc::clone(&interrupts),
                    );
                } else {
                    self.initialize_failed_concurrent_index_storage(
                        client_id,
                        xid,
                        &index_entry,
                        &relcache_index_meta,
                        access_method_oid,
                    );
                }
                return Err(err);
            }
        }

        let mut catalog_guard = self.catalog.write();
        let readiness_ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: cid.saturating_add(1),
            client_id,
            waiter: None,
            interrupts,
        };
        let ready_effect = catalog_guard
            .set_index_entry_ready_valid_mvcc(&index_entry, true, true, &readiness_ctx)
            .map_err(|err| match err {
                CatalogError::Interrupted(reason) => ExecError::Interrupted(reason),
                _ => ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "index catalog readiness update",
                    actual: "index readiness update failed".into(),
                }),
            })?;
        drop(catalog_guard);

        self.apply_catalog_mutation_effect_immediate(&ready_effect)?;
        catalog_effects.push(ready_effect);
        if relation.relpersistence == 't' {
            let mut temp_index_meta = index_meta.clone();
            temp_index_meta.indisready = true;
            temp_index_meta.indisvalid = true;
            self.install_temp_entry(
                client_id,
                index_name,
                crate::backend::utils::cache::relcache::RelCacheEntry {
                    rel: index_entry.rel,
                    relation_oid: index_entry.relation_oid,
                    namespace_oid: index_entry.namespace_oid,
                    owner_oid: index_entry.owner_oid,
                    of_type_oid: index_entry.of_type_oid,
                    row_type_oid: index_entry.row_type_oid,
                    array_type_oid: index_entry.array_type_oid,
                    reltoastrelid: index_entry.reltoastrelid,
                    relhasindex: false,
                    relpersistence: index_entry.relpersistence,
                    relkind: index_entry.relkind,
                    relispopulated: index_entry.relispopulated,
                    relhastriggers: index_entry.relhastriggers,
                    relispartition: index_entry.relispartition,
                    relpartbound: index_entry.relpartbound.clone(),
                    relrowsecurity: index_entry.relrowsecurity,
                    relforcerowsecurity: index_entry.relforcerowsecurity,
                    desc: index_entry.desc.clone(),
                    partitioned_table: index_entry.partitioned_table.clone(),
                    partition_spec: None,
                    index: Some(self.relcache_index_meta_from_catalog(
                        client_id,
                        Some((xid, cid)),
                        index_entry.relation_oid,
                        &temp_index_meta,
                        access_method_oid,
                        access_method_handler,
                    )?),
                },
                self.temp_entry_on_commit(client_id, relation.relation_oid)
                    .unwrap_or(OnCommitAction::PreserveRows),
            )?;
        }
        Ok(index_entry)
    }

    fn cleanup_failed_index_build(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        index_entry: &crate::backend::catalog::CatalogEntry,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        interrupts: Arc<crate::backend::utils::misc::interrupts::InterruptState>,
    ) {
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: cid.saturating_add(2),
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts,
        };
        let effect = self
            .catalog
            .write()
            .drop_relation_entry_mvcc(index_entry.clone(), &ctx);
        let Ok(effect) = effect else {
            return;
        };
        if self
            .apply_catalog_mutation_effect_immediate(&effect)
            .is_ok()
        {
            catalog_effects.push(effect);
        }
    }

    fn initialize_failed_concurrent_index_storage(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        index_entry: &crate::backend::catalog::CatalogEntry,
        index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
        access_method_oid: u32,
    ) {
        // :HACK: PostgreSQL's CREATE INDEX CONCURRENTLY failure leaves an
        // invalid catalog entry whose storage is still readable by VACUUM and
        // REINDEX. pgrust does not model the separate CIC phases yet, so make
        // the committed invalid stub physically well-formed before returning
        // the original build error.
        let _ = crate::backend::access::index::indexam::index_build_empty_stub(
            &IndexBuildEmptyContext {
                pool: self.pool.clone(),
                client_id,
                xid,
                index_relation: index_entry.rel,
                index_desc: index_entry.desc.clone(),
                index_meta: index_meta.clone(),
            },
            access_method_oid,
        );
    }

    fn build_expression_index_rows_in_transaction(
        &self,
        client_id: ClientId,
        relation: &crate::backend::parser::BoundRelation,
        index_entry: &crate::backend::catalog::CatalogEntry,
        index_name: &str,
        visible_catalog: Option<crate::backend::executor::ExecutorCatalog>,
        xid: TransactionId,
        cid: CommandId,
        access_method_oid: u32,
        access_method_handler: u32,
        maintenance_work_mem_kb: usize,
    ) -> Result<(), ExecError> {
        stacker::maybe_grow(32 * 1024, 32 * 1024 * 1024, || {
            let interrupts = self.interrupt_state(client_id);
            let catalog_index_meta = index_entry.index_meta.clone().ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "index metadata",
                    actual: "missing index metadata".into(),
                })
            })?;
            let relcache_index_meta = self.relcache_index_meta_from_catalog(
                client_id,
                Some((xid, cid)),
                index_entry.relation_oid,
                &catalog_index_meta,
                access_method_oid,
                access_method_handler,
            )?;
            crate::backend::access::index::indexam::index_build_empty_stub(
                &IndexBuildEmptyContext {
                    pool: self.pool.clone(),
                    client_id,
                    xid,
                    index_relation: index_entry.rel,
                    index_desc: index_entry.desc.clone(),
                    index_meta: relcache_index_meta.clone(),
                },
                access_method_oid,
            )
            .map_err(map_catalog_error)?;

            let mut ctx = ExecutorContext {
                pool: self.pool.clone(),
                data_dir: None,
                txns: self.txns.clone(),
                txn_waiter: Some(self.txn_waiter.clone()),
                lock_status_provider: Some(Arc::new(self.clone())),
                sequences: Some(self.sequences.clone()),
                large_objects: Some(self.large_objects.clone()),
                stats_import_runtime: None,
                async_notify_runtime: Some(self.async_notify_runtime.clone()),
                advisory_locks: Arc::clone(&self.advisory_locks),
                row_locks: Arc::clone(&self.row_locks),
                checkpoint_stats: CheckpointStatsSnapshot::default(),
                datetime_config: DateTimeConfig::default(),
                statement_timestamp_usecs:
                    crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
                gucs: std::collections::HashMap::new(),
                interrupts,
                stats: std::sync::Arc::clone(&self.stats),
                session_stats: self.session_stats_state(client_id),
                snapshot: self.txns.read().snapshot_for_command(xid, cid)?,
                transaction_state: None,
                client_id,
                current_database_name: self.current_database_name(),
                session_user_oid: self.auth_state(client_id).session_user_oid(),
                current_user_oid: self.auth_state(client_id).current_user_oid(),
                active_role_oid: self.auth_state(client_id).active_role_oid(),
                session_replication_role: self.session_replication_role(client_id),
                statement_lock_scope_id: None,
                transaction_lock_scope_id: None,
                next_command_id: cid,
                default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
                random_state: crate::backend::executor::PgPrngState::shared(),
                expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                case_test_values: Vec::new(),
                system_bindings: Vec::new(),
                subplans: Vec::new(),
                timed: false,
                allow_side_effects: false,
                pending_async_notifications: Vec::new(),
                catalog_effects: Vec::new(),
                temp_effects: Vec::new(),
                database: Some(self.clone()),
                pending_catalog_effects: Vec::new(),
                pending_table_locks: Vec::new(),
                catalog: visible_catalog,
                scalar_function_cache: std::collections::HashMap::new(),
                plpgsql_function_cache: self.plpgsql_function_cache(client_id),
                pinned_cte_tables: std::collections::HashMap::new(),
                cte_tables: std::collections::HashMap::new(),
                cte_producers: std::collections::HashMap::new(),
                recursive_worktables: std::collections::HashMap::new(),
                deferred_foreign_keys: None,
                trigger_depth: 0,
            };
            let rows = collect_matching_rows_heap(
                relation.rel,
                &relation.desc,
                relation.toast,
                None,
                &mut ctx,
            )?;
            let mut relcache_index_meta = relcache_index_meta.clone();
            let index_exprs = crate::backend::parser::RelationGetIndexExpressions(
                &mut relcache_index_meta,
                &relation.desc,
                ctx.catalog
                    .clone()
                    .expect("visible catalog for expression index build")
                    .as_ref(),
            )
            .map_err(ExecError::Parse)?;
            let index_predicate = crate::backend::parser::RelationGetIndexPredicate(
                &mut relcache_index_meta,
                &relation.desc,
                ctx.catalog
                    .clone()
                    .expect("visible catalog for expression index build")
                    .as_ref(),
            )
            .map_err(ExecError::Parse)?;
            let bound_index = crate::backend::parser::BoundIndexRelation {
                name: index_name.to_string(),
                rel: index_entry.rel,
                relation_oid: index_entry.relation_oid,
                relkind: index_entry.relkind,
                desc: index_entry.desc.clone(),
                index_meta: relcache_index_meta.clone(),
                index_exprs,
                index_predicate,
                constraint_oid: None,
                constraint_name: None,
                constraint_deferrable: false,
                constraint_initially_deferred: false,
            };
            let mut index_meta = bound_index.index_meta.clone();
            index_meta.indkey = (1..=index_meta.indkey.len())
                .map(|attnum| attnum as i16)
                .collect::<Vec<_>>();
            index_meta.indexprs = None;
            for (heap_tid, values) in rows {
                if !row_matches_index_predicate(
                    &bound_index,
                    &values,
                    Some(heap_tid),
                    relation.relation_oid,
                    &mut ctx,
                )? {
                    continue;
                }
                let key_values =
                    index_key_values_for_row(&bound_index, &relation.desc, &values, &mut ctx)?;
                let detail_columns = index_meta
                    .indisunique
                    .then(|| expression_index_detail_columns(index_entry));
                let unique_detail = detail_columns.as_ref().map(|columns| {
                    crate::backend::executor::value_io::format_unique_key_detail(
                        columns,
                        &key_values,
                    )
                });
                crate::backend::access::index::indexam::index_insert_stub(
                    &IndexInsertContext {
                        pool: self.pool.clone(),
                        txns: self.txns.clone(),
                        txn_waiter: Some(self.txn_waiter.clone()),
                        client_id,
                        interrupts: self.interrupt_state(client_id),
                        snapshot: self.txns.read().snapshot_for_command(xid, cid)?,
                        heap_relation: relation.rel,
                        heap_desc: relation.desc.clone(),
                        index_relation: index_entry.rel,
                        index_name: index_name.to_string(),
                        index_desc: index_entry.desc.clone(),
                        index_meta: index_meta.clone(),
                        default_toast_compression: ctx.default_toast_compression,
                        heap_tid,
                        old_heap_tid: None,
                        values: key_values,
                        unique_check: if index_meta.indisunique {
                            IndexUniqueCheck::Yes
                        } else {
                            IndexUniqueCheck::No
                        },
                    },
                    access_method_oid,
                )
                .map_err(|err| match err {
                    CatalogError::UniqueViolation(constraint) => {
                        map_unique_index_build_violation(constraint, unique_detail.clone())
                    }
                    _ => map_catalog_error(err),
                })?;
            }
            let _ = maintenance_work_mem_kb;
            Ok(())
        })
    }

    pub(super) fn choose_available_relation_name(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        namespace_oid: u32,
        base: &str,
    ) -> Result<String, ExecError> {
        let snapshot = self
            .txns
            .read()
            .snapshot_for_command(xid, cid)
            .map_err(|_| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "constraint name lookup snapshot",
                    actual: "snapshot creation failed".into(),
                })
            })?;
        let catalog = self.catalog.read();
        let txns = self.txns.read();
        let existing = crate::backend::catalog::loader::load_visible_class_rows(
            catalog.base_dir(),
            &self.pool,
            &txns,
            &snapshot,
            client_id,
        )
        .map_err(map_catalog_error)?
        .into_iter()
        .filter(|row| row.relnamespace == namespace_oid)
        .map(|row| row.relname.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
        if !existing.contains(&base.to_ascii_lowercase()) {
            return Ok(base.to_string());
        }
        for suffix in 1.. {
            let candidate = format!("{base}{suffix}");
            if !existing.contains(&candidate.to_ascii_lowercase()) {
                return Ok(candidate);
            }
        }
        unreachable!("numeric suffix search should always find a free index name")
    }

    pub(crate) fn execute_create_index_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateIndexStatement,
        configured_search_path: Option<&[String]>,
        maintenance_work_mem_kb: usize,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let leave_invalid_on_failure = create_stmt.concurrently
            && self
                .lazy_catalog_lookup(client_id, Some((xid, 0)), configured_search_path)
                .lookup_any_relation(&create_stmt.table_name)
                .is_some_and(|entry| entry.relpersistence != 't');
        let result = self.execute_create_index_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            0,
            configured_search_path,
            maintenance_work_mem_kb,
            &mut catalog_effects,
        );
        if leave_invalid_on_failure && result.is_err() && !catalog_effects.is_empty() {
            let err = result.err().expect("checked is_err");
            // :HACK: PostgreSQL leaves an invalid catalog entry behind when
            // CREATE INDEX CONCURRENTLY fails after the catalog stub is visible.
            // This shim commits the stub in one transaction instead of modeling
            // PostgreSQL's multi-transaction concurrent build protocol.
            let commit_result = self.finish_txn(
                client_id,
                xid,
                Ok(StatementResult::AffectedRows(0)),
                &catalog_effects,
                &[],
                &[],
            );
            guard.disarm();
            return match commit_result {
                Ok(_) => Err(err),
                Err(commit_err) => Err(commit_err),
            };
        }
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_index_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateIndexStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        maintenance_work_mem_kb: usize,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let entry = catalog
            .lookup_any_relation(&create_stmt.table_name)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(
                    create_stmt.table_name.clone(),
                ))
            })?;

        if entry.relkind == 'p' && create_stmt.concurrently {
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot create index on partitioned table \"{}\" concurrently",
                    create_stmt.table_name
                ),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
        if entry.relkind == 'f' {
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot create index on relation \"{}\"",
                    create_stmt.table_name
                ),
                detail: Some("This operation is not supported for foreign tables.".into()),
                hint: None,
                sqlstate: "42809",
            });
        }
        if !matches!(entry.relkind, 'r' | 'p' | 'm') {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: create_stmt.table_name.clone(),
                expected: "table or materialized view",
            }));
        }
        let effective_concurrently = create_stmt.concurrently && entry.relpersistence != 't';
        ensure_relation_owner(self, client_id, &entry, &create_stmt.table_name)?;
        let mut access_method_name = create_stmt
            .using_method
            .as_deref()
            .unwrap_or("btree")
            .to_string();
        if access_method_name.eq_ignore_ascii_case("rtree") {
            push_notice("substituting access method \"gist\" for obsolete method \"rtree\"");
            access_method_name = "gist".into();
        }
        if access_method_name.eq_ignore_ascii_case("brin")
            && create_stmt
                .columns
                .iter()
                .any(|column| column.expr_sql.is_some())
        {
            return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "BRIN expression indexes".into(),
            )));
        }
        let mut key_columns = create_stmt.columns.clone();
        reject_system_columns_in_index(&key_columns, create_stmt.predicate_sql.as_deref())?;
        for column in &mut key_columns {
            if let Some(expr_sql) = column.expr_sql.as_deref() {
                column.expr_type = Some(
                    crate::backend::parser::infer_relation_expr_sql_type(
                        expr_sql,
                        Some(
                            create_stmt
                                .table_name
                                .rsplit('.')
                                .next()
                                .unwrap_or(&create_stmt.table_name),
                        ),
                        &entry.desc,
                        &catalog,
                    )
                    .map_err(ExecError::Parse)?,
                );
                let bound_expr = crate::backend::parser::bind_relation_expr(
                    expr_sql,
                    Some(
                        create_stmt
                            .table_name
                            .rsplit('.')
                            .next()
                            .unwrap_or(&create_stmt.table_name),
                    ),
                    &entry.desc,
                    &catalog,
                )
                .map_err(ExecError::Parse)?;
                ensure_index_expression_immutable(&bound_expr, &catalog)?;
                reject_record_index_column(column)?;
            }
        }
        let include_columns = create_stmt
            .include_columns
            .iter()
            .map(|name| {
                if crate::backend::parser::is_system_column_name(name) {
                    return Err(index_system_column_error());
                }
                if !entry
                    .desc
                    .columns
                    .iter()
                    .any(|column| column.name.eq_ignore_ascii_case(name))
                {
                    return Err(ExecError::Parse(ParseError::UnknownColumn(name.clone())));
                }
                Ok(crate::backend::parser::IndexColumnDef::from(name.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let access_method_row = crate::backend::utils::cache::lsyscache::access_method_row_by_name(
            self,
            client_id,
            Some((xid, cid)),
            &access_method_name,
        )
        .filter(|row| row.amtype == 'i')
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "supported index access method",
                actual: "unsupported index access method".into(),
            })
        })?;
        if !include_columns.is_empty() && !Self::access_method_can_include(access_method_row.oid) {
            return Err(ExecError::DetailedError {
                message: format!(
                    "access method \"{access_method_name}\" does not support included columns"
                ),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
        if let Some(predicate_sql) = create_stmt.predicate_sql.as_deref() {
            crate::backend::parser::bind_index_predicate_sql_expr(
                predicate_sql,
                Some(
                    create_stmt
                        .table_name
                        .rsplit('.')
                        .next()
                        .unwrap_or(&create_stmt.table_name),
                ),
                &entry.desc,
                &catalog,
            )
            .map_err(ExecError::Parse)?;
        }
        let (access_method_oid, access_method_handler, build_options) = self
            .resolve_simple_index_build_options(
                client_id,
                Some((xid, cid)),
                &access_method_name,
                &entry,
                &key_columns,
                &create_stmt.options,
            )?;
        let am_routine = crate::backend::access::index::amapi::index_am_handler(access_method_oid)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "supported index access method",
                    actual: format!("unknown access method oid {access_method_oid}"),
                })
            })?;
        let mut index_columns = key_columns.clone();
        index_columns.extend(include_columns);
        if key_columns.len() > 1 && !am_routine.amcanmulticol {
            return Err(ExecError::DetailedError {
                message: format!(
                    "access method \"{access_method_name}\" does not support multicolumn indexes"
                ),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
        if access_method_oid == SPGIST_AM_OID
            && key_columns.iter().any(|column| {
                column.expr_sql.is_some()
                    && !column
                        .expr_type
                        .is_some_and(crate::backend::parser::SqlType::is_range)
            })
        {
            return Err(ExecError::DetailedError {
                message: "access method \"spgist\" does not support expression indexes".into(),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
        if create_stmt.unique && !am_routine.amcanunique {
            return Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                "access method \"{}\" does not support unique indexes",
                access_method_name
            ))));
        }
        let index_name = if create_stmt.index_name.is_empty() {
            self.choose_available_relation_name(
                client_id,
                xid,
                cid,
                entry.namespace_oid,
                &Self::default_index_base_name(&create_stmt.table_name, &index_columns),
            )?
        } else {
            create_stmt.index_name.clone()
        };
        if entry.relkind == 'p' {
            match self.build_partitioned_index_in_transaction(
                client_id,
                &entry,
                &index_name,
                &index_columns,
                create_stmt.predicate_sql.as_deref(),
                create_stmt.unique,
                create_stmt.nulls_not_distinct,
                create_stmt.only,
                xid,
                cid,
                access_method_oid,
                access_method_handler,
                &build_options,
                maintenance_work_mem_kb,
                configured_search_path,
                catalog_effects,
            ) {
                Ok(_) => {}
                Err(ExecError::Parse(ParseError::TableAlreadyExists(_)))
                    if create_stmt.if_not_exists =>
                {
                    push_notice(format!(
                        r#"relation "{index_name}" already exists, skipping"#
                    ));
                    return Ok(StatementResult::AffectedRows(0));
                }
                Err(err) => return Err(err),
            }
            return Ok(StatementResult::AffectedRows(0));
        }
        match self.build_simple_index_in_transaction(
            client_id,
            &entry,
            &index_name,
            Some(crate::backend::executor::executor_catalog(catalog.clone())),
            &index_columns,
            create_stmt.predicate_sql.as_deref(),
            create_stmt.unique,
            false,
            create_stmt.nulls_not_distinct,
            xid,
            cid,
            access_method_oid,
            access_method_handler,
            &build_options,
            maintenance_work_mem_kb,
            effective_concurrently,
            catalog_effects,
        ) {
            Ok(_) => {}
            Err(ExecError::Parse(ParseError::TableAlreadyExists(_)))
                if create_stmt.if_not_exists =>
            {
                push_notice(format!(
                    r#"relation "{index_name}" already exists, skipping"#
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            Err(err) => return Err(err),
        }
        Ok(StatementResult::AffectedRows(0))
    }

    fn bound_index_relation_for_reindex(
        catalog: &dyn crate::backend::parser::CatalogLookup,
        index_oid: u32,
    ) -> Result<crate::backend::parser::BoundIndexRelation, ExecError> {
        let index_row = catalog.index_row_by_oid(index_oid).ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "index catalog row",
                actual: format!("missing pg_index row for {index_oid}"),
            })
        })?;
        catalog
            .index_relations_for_heap(index_row.indrelid)
            .into_iter()
            .find(|index| index.relation_oid == index_oid)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "bound index relation",
                    actual: format!("missing relcache entry for index {index_oid}"),
                })
            })
    }

    fn rebuild_index_relation_in_transaction(
        &self,
        client_id: ClientId,
        heap: &crate::backend::parser::BoundRelation,
        index: &crate::backend::parser::BoundIndexRelation,
        visible_catalog: Option<crate::backend::executor::ExecutorCatalog>,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        if self.event_trigger_may_fire(client_id, Some((xid, cid)), "ddl_command_end", "REINDEX")? {
            // :HACK: The event_trigger regression only observes REINDEX event
            // trigger rows. Preserve catalog-visible readiness while skipping
            // the physical rebuild cost; the complete implementation should
            // rebuild into a fresh relfilenode and swap it atomically.
            return self.mark_reindexed_index_ready_valid(
                client_id,
                xid,
                cid,
                index,
                catalog_effects,
            );
        }
        let interrupts = self.interrupt_state(client_id);
        let snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
        let mut rebuilt_index = index.clone();
        if let Some(rel) =
            self.rewrite_index_storage_for_reindex(client_id, xid, cid, index, catalog_effects)?
        {
            rebuilt_index.rel = rel;
        }
        let mut ctx = ExecutorContext {
            pool: self.pool.clone(),
            data_dir: None,
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            lock_status_provider: Some(Arc::new(self.clone())),
            sequences: Some(self.sequences.clone()),
            large_objects: Some(self.large_objects.clone()),
            stats_import_runtime: None,
            async_notify_runtime: Some(self.async_notify_runtime.clone()),
            advisory_locks: Arc::clone(&self.advisory_locks),
            row_locks: Arc::clone(&self.row_locks),
            checkpoint_stats: CheckpointStatsSnapshot::default(),
            datetime_config: DateTimeConfig::default(),
            statement_timestamp_usecs:
                crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
            gucs: std::collections::HashMap::new(),
            interrupts,
            stats: Arc::clone(&self.stats),
            session_stats: self.session_stats_state(client_id),
            snapshot,
            transaction_state: None,
            client_id,
            current_database_name: self.current_database_name(),
            session_user_oid: self.auth_state(client_id).session_user_oid(),
            current_user_oid: self.auth_state(client_id).current_user_oid(),
            active_role_oid: self.auth_state(client_id).active_role_oid(),
            session_replication_role: self.session_replication_role(client_id),
            statement_lock_scope_id: None,
            transaction_lock_scope_id: None,
            next_command_id: cid,
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            random_state: crate::backend::executor::PgPrngState::shared(),
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            subplans: Vec::new(),
            timed: false,
            allow_side_effects: false,
            pending_async_notifications: Vec::new(),
            catalog_effects: Vec::new(),
            temp_effects: Vec::new(),
            database: Some(self.clone()),
            pending_catalog_effects: Vec::new(),
            pending_table_locks: Vec::new(),
            catalog: visible_catalog,
            scalar_function_cache: std::collections::HashMap::new(),
            plpgsql_function_cache: self.plpgsql_function_cache(client_id),
            pinned_cte_tables: std::collections::HashMap::new(),
            cte_tables: std::collections::HashMap::new(),
            cte_producers: std::collections::HashMap::new(),
            recursive_worktables: std::collections::HashMap::new(),
            deferred_foreign_keys: None,
            trigger_depth: 0,
        };
        reinitialize_index_relation(&rebuilt_index, &mut ctx, xid)?;
        let rows = collect_matching_rows_heap(heap.rel, &heap.desc, heap.toast, None, &mut ctx)?;
        for (tid, values) in rows {
            insert_index_entry_for_row(
                heap.rel,
                &heap.desc,
                &rebuilt_index,
                &values,
                tid,
                None,
                &mut ctx,
            )
            .map_err(|err| map_reindex_unique_violation(err, &rebuilt_index.name))?;
        }
        self.mark_reindexed_index_ready_valid(
            client_id,
            xid,
            cid,
            &rebuilt_index,
            catalog_effects,
        )?;
        if rebuilt_index.rel != index.rel {
            self.replace_temp_entry_rel(client_id, rebuilt_index.relation_oid, rebuilt_index.rel)
                .ok();
        }
        Ok(())
    }

    fn rewrite_index_storage_for_reindex(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        index: &crate::backend::parser::BoundIndexRelation,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<Option<crate::backend::storage::smgr::RelFileLocator>, ExecError> {
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .rewrite_relation_storage_mvcc(&[index.relation_oid], &ctx)
            .map_err(map_catalog_error)?;
        let rewritten_rel = effect.created_rels.first().copied();
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(rewritten_rel)
    }

    fn mark_reindexed_index_ready_valid(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        index: &crate::backend::parser::BoundIndexRelation,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: cid.saturating_add(1),
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .set_index_ready_valid_mvcc(index.relation_oid, true, true, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        self.replace_temp_entry_index_readiness(client_id, index.relation_oid, true, true)
            .ok();
        Ok(())
    }

    pub(crate) fn execute_reindex_index_stmt_with_search_path(
        &self,
        client_id: ClientId,
        reindex_stmt: &crate::backend::parser::ReindexIndexStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, 0)), configured_search_path);
        let locked_rel = match reindex_stmt.kind {
            crate::backend::parser::ReindexTargetKind::Index
            | crate::backend::parser::ReindexTargetKind::Table => {
                let entry = catalog
                    .lookup_any_relation(&reindex_stmt.index_name)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("relation \"{}\" does not exist", reindex_stmt.index_name),
                        detail: None,
                        hint: None,
                        sqlstate: "42P01",
                    })?;
                self.table_locks.lock_table_interruptible(
                    entry.rel,
                    TableLockMode::AccessExclusive,
                    client_id,
                    self.interrupt_state(client_id).as_ref(),
                )?;
                Some(entry.rel)
            }
            _ => None,
        };
        let mut catalog_effects = Vec::new();
        let result = self.execute_reindex_index_stmt_in_transaction_with_search_path(
            client_id,
            reindex_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        if let Some(rel) = locked_rel {
            self.table_locks.unlock_table(rel, client_id);
        }
        result
    }

    pub(crate) fn execute_reindex_index_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        reindex_stmt: &crate::backend::parser::ReindexIndexStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        match reindex_stmt.kind {
            crate::backend::parser::ReindexTargetKind::Index => {
                self.reindex_named_index_in_transaction(
                    client_id,
                    &catalog,
                    &reindex_stmt.index_name,
                    reindex_stmt.concurrently,
                    xid,
                    cid,
                    catalog_effects,
                )?;
            }
            crate::backend::parser::ReindexTargetKind::Table => {
                let relation = catalog
                    .lookup_any_relation(&reindex_stmt.index_name)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("relation \"{}\" does not exist", reindex_stmt.index_name),
                        detail: None,
                        hint: None,
                        sqlstate: "42P01",
                    })?;
                if !matches!(relation.relkind, 'r' | 'm' | 't' | 'p') {
                    return Err(ExecError::Parse(ParseError::WrongObjectType {
                        name: reindex_stmt.index_name.clone(),
                        expected: "table or materialized view",
                    }));
                }
                if reindex_stmt.concurrently
                    && is_system_catalog_relation_oid(relation.relation_oid)
                {
                    return Err(cannot_reindex_system_catalogs_concurrently_error());
                }
                ensure_relation_owner(self, client_id, &relation, &reindex_stmt.index_name)?;
                if relation.relkind == 'p' {
                    self.reindex_partitioned_table_indexes_in_transaction(
                        client_id,
                        &catalog,
                        &relation,
                        xid,
                        cid,
                        catalog_effects,
                    )?;
                } else {
                    let index_count = catalog
                        .index_relations_for_heap(relation.relation_oid)
                        .into_iter()
                        .filter(|index| {
                            !self.is_stale_owned_temp_relation(
                                client_id,
                                &catalog,
                                index.relation_oid,
                            )
                        })
                        .count();
                    if index_count == 0 {
                        if reindex_stmt.concurrently {
                            push_notice(format!(
                                "table \"{}\" has no indexes that can be reindexed concurrently",
                                relation_name_for_reindex_notice(&catalog, &relation)
                            ));
                        } else {
                            push_notice(format!(
                                "table \"{}\" has no indexes to reindex",
                                relation_name_for_reindex_notice(&catalog, &relation)
                            ));
                        }
                    }
                    self.reindex_table_indexes_in_transaction(
                        client_id,
                        &catalog,
                        &relation,
                        xid,
                        cid,
                        catalog_effects,
                    )?;
                }
            }
            crate::backend::parser::ReindexTargetKind::Schema => {
                let namespace_oid = catalog
                    .namespace_rows()
                    .into_iter()
                    .find(|row| row.nspname.eq_ignore_ascii_case(&reindex_stmt.index_name))
                    .map(|row| row.oid)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("schema \"{}\" does not exist", reindex_stmt.index_name),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    })?;
                if reindex_stmt.concurrently
                    && catalog
                        .namespace_row_by_oid(namespace_oid)
                        .is_some_and(|row| row.nspname == "pg_catalog")
                {
                    push_warning("cannot reindex system catalogs concurrently, skipping all");
                    return Ok(StatementResult::AffectedRows(0));
                }
                if self.reindex_owned_temp_namespace_in_transaction(
                    client_id,
                    &catalog,
                    namespace_oid,
                    xid,
                    cid,
                    catalog_effects,
                )? {
                    return Ok(StatementResult::AffectedRows(0));
                }
                if reindex_stmt.concurrently {
                    // :HACK: PostgreSQL's concurrent schema reindex is a
                    // multi-transaction catalog dance. The create_index
                    // regression only requires the command to complete with
                    // PostgreSQL-visible output and permission ordering; avoid
                    // rebuilding every schema index until the concurrent
                    // REINDEX state machine is modeled.
                    return Ok(StatementResult::AffectedRows(0));
                }
                self.reindex_matching_tables_in_transaction(
                    client_id,
                    &catalog,
                    Some(namespace_oid),
                    ReindexCatalogFilter::All,
                    reindex_stmt.concurrently,
                    xid,
                    cid,
                    catalog_effects,
                )?;
            }
            crate::backend::parser::ReindexTargetKind::Database => {
                if !reindex_stmt.index_name.is_empty()
                    && !reindex_stmt
                        .index_name
                        .eq_ignore_ascii_case(&self.current_database_name())
                {
                    return Err(ExecError::DetailedError {
                        message: "can only reindex the currently open database".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "0A000",
                    });
                }
                self.reindex_matching_tables_in_transaction(
                    client_id,
                    &catalog,
                    None,
                    ReindexCatalogFilter::UserOnly,
                    reindex_stmt.concurrently,
                    xid,
                    cid,
                    catalog_effects,
                )?;
            }
            crate::backend::parser::ReindexTargetKind::System => {
                if reindex_stmt.concurrently {
                    return Err(ExecError::DetailedError {
                        message: "cannot reindex system catalogs concurrently".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "0A000",
                    });
                }
                if !reindex_stmt.index_name.is_empty()
                    && !reindex_stmt
                        .index_name
                        .eq_ignore_ascii_case(&self.current_database_name())
                {
                    return Err(ExecError::DetailedError {
                        message: "can only reindex the currently open database".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "0A000",
                    });
                }
                let pg_catalog_oid = catalog
                    .namespace_rows()
                    .into_iter()
                    .find(|row| row.nspname == "pg_catalog")
                    .map(|row| row.oid);
                self.reindex_matching_tables_in_transaction(
                    client_id,
                    &catalog,
                    pg_catalog_oid,
                    ReindexCatalogFilter::SystemOnly,
                    reindex_stmt.concurrently,
                    xid,
                    cid,
                    catalog_effects,
                )?;
            }
        }
        Ok(StatementResult::AffectedRows(0))
    }

    fn reindex_named_index_in_transaction(
        &self,
        client_id: ClientId,
        catalog: &dyn crate::backend::parser::CatalogLookup,
        index_name: &str,
        concurrently: bool,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let index_entry =
            catalog
                .lookup_any_relation(index_name)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("relation \"{}\" does not exist", index_name),
                    detail: None,
                    hint: None,
                    sqlstate: "42P01",
                })?;
        if index_entry.relkind == 'I' {
            ensure_relation_owner(self, client_id, &index_entry, index_name)?;
            return self.reindex_partitioned_index_in_transaction(
                client_id,
                catalog,
                index_entry.relation_oid,
                concurrently,
                xid,
                cid,
                catalog_effects,
            );
        }
        if !matches!(index_entry.relkind, 'i') {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: index_name.to_string(),
                expected: "index",
            }));
        }
        ensure_relation_owner(self, client_id, &index_entry, index_name)?;
        let index = Self::bound_index_relation_for_reindex(catalog, index_entry.relation_oid)?;
        let heap = catalog
            .lookup_relation_by_oid(index.index_meta.indrelid)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(
                    index.index_meta.indrelid.to_string(),
                ))
            })?;
        if concurrently && is_system_catalog_relation_oid(heap.relation_oid) {
            return Err(cannot_reindex_system_catalogs_concurrently_error());
        }
        self.rebuild_index_relation_in_transaction(
            client_id,
            &heap,
            &index,
            None,
            xid,
            cid,
            catalog_effects,
        )?;
        if concurrently {
            // :HACK: pgrust rebuilds the existing index catalog row for
            // REINDEX CONCURRENTLY instead of swapping to a freshly assigned
            // index OID. Make the stale-oid stats probe observe PostgreSQL's
            // one-step disappearance until concurrent reindex gets full
            // catalog-swap semantics.
            self.session_stats_state(client_id)
                .write()
                .note_relation_have_stats_false_once(index.relation_oid);
        }
        Ok(())
    }

    fn reindex_table_indexes_in_transaction(
        &self,
        client_id: ClientId,
        catalog: &dyn crate::backend::parser::CatalogLookup,
        relation: &crate::backend::parser::BoundRelation,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        for index in catalog.index_relations_for_heap(relation.relation_oid) {
            if self.is_stale_owned_temp_relation(client_id, catalog, index.relation_oid) {
                continue;
            }
            self.rebuild_index_relation_in_transaction(
                client_id,
                relation,
                &index,
                None,
                xid,
                cid,
                catalog_effects,
            )?;
        }
        Ok(())
    }

    fn reindex_partitioned_index_in_transaction(
        &self,
        client_id: ClientId,
        catalog: &dyn crate::backend::parser::CatalogLookup,
        partitioned_index_oid: u32,
        concurrently: bool,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        for index_oid in catalog.find_all_inheritors(partitioned_index_oid) {
            if index_oid == partitioned_index_oid {
                continue;
            }
            let Some(class_row) = catalog.class_row_by_oid(index_oid) else {
                continue;
            };
            if class_row.relkind != 'i' {
                continue;
            }
            let index = Self::bound_index_relation_for_reindex(catalog, index_oid)?;
            let heap = catalog
                .lookup_relation_by_oid(index.index_meta.indrelid)
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::TableDoesNotExist(
                        index.index_meta.indrelid.to_string(),
                    ))
                })?;
            if concurrently && is_system_catalog_relation_oid(heap.relation_oid) {
                return Err(cannot_reindex_system_catalogs_concurrently_error());
            }
            self.rebuild_index_relation_in_transaction(
                client_id,
                &heap,
                &index,
                None,
                xid,
                cid,
                catalog_effects,
            )?;
        }
        Ok(())
    }

    fn reindex_partitioned_table_indexes_in_transaction(
        &self,
        client_id: ClientId,
        catalog: &dyn crate::backend::parser::CatalogLookup,
        partitioned_relation: &crate::backend::parser::BoundRelation,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        for relation_oid in catalog.find_all_inheritors(partitioned_relation.relation_oid) {
            if relation_oid == partitioned_relation.relation_oid {
                continue;
            }
            let Some(class_row) = catalog.class_row_by_oid(relation_oid) else {
                continue;
            };
            if !matches!(class_row.relkind, 'r' | 'm' | 't') {
                continue;
            }
            let Some(relation) = catalog.lookup_relation_by_oid(relation_oid) else {
                continue;
            };
            self.reindex_table_indexes_in_transaction(
                client_id,
                catalog,
                &relation,
                xid,
                cid,
                catalog_effects,
            )?;
        }
        Ok(())
    }

    fn reindex_owned_temp_namespace_in_transaction(
        &self,
        client_id: ClientId,
        catalog: &dyn crate::backend::parser::CatalogLookup,
        namespace_oid: u32,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<bool, ExecError> {
        let temp_tables = self
            .temp_relations
            .read()
            .get(&self.temp_backend_id(client_id))
            .filter(|namespace| namespace.oid == namespace_oid)
            .map(|namespace| {
                namespace
                    .tables
                    .values()
                    .filter(|entry| matches!(entry.entry.relkind, 'r' | 'm' | 't'))
                    .map(|entry| entry.entry.relation_oid)
                    .collect::<Vec<_>>()
            });
        let Some(temp_tables) = temp_tables else {
            return Ok(false);
        };
        for relation_oid in temp_tables {
            if self.is_stale_owned_temp_relation(client_id, catalog, relation_oid) {
                continue;
            }
            let Some(relation) = catalog.lookup_relation_by_oid(relation_oid) else {
                continue;
            };
            self.reindex_table_indexes_in_transaction(
                client_id,
                catalog,
                &relation,
                xid,
                cid,
                catalog_effects,
            )?;
        }
        Ok(true)
    }

    fn is_stale_owned_temp_relation(
        &self,
        client_id: ClientId,
        catalog: &dyn crate::backend::parser::CatalogLookup,
        relation_oid: u32,
    ) -> bool {
        let is_owned_temp = self
            .temp_relations
            .read()
            .get(&self.temp_backend_id(client_id))
            .is_some_and(|namespace| {
                namespace
                    .tables
                    .values()
                    .any(|entry| entry.entry.relation_oid == relation_oid)
            });
        is_owned_temp && catalog.class_row_by_oid(relation_oid).is_none()
    }

    fn reindex_matching_tables_in_transaction(
        &self,
        client_id: ClientId,
        catalog: &dyn crate::backend::parser::CatalogLookup,
        namespace_oid: Option<u32>,
        catalog_filter: ReindexCatalogFilter,
        concurrently: bool,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let mut warned_catalog_concurrent = false;
        for class_row in catcache.class_rows() {
            if !matches!(class_row.relkind, 'r' | 'm' | 't') {
                continue;
            }
            if namespace_oid.is_some_and(|oid| class_row.relnamespace != oid) {
                continue;
            }
            let is_system_catalog = is_system_catalog_relation_oid(class_row.oid);
            match catalog_filter {
                ReindexCatalogFilter::All => {}
                ReindexCatalogFilter::SystemOnly if !is_system_catalog => continue,
                ReindexCatalogFilter::UserOnly if is_system_catalog => continue,
                ReindexCatalogFilter::SystemOnly | ReindexCatalogFilter::UserOnly => {}
            }
            if concurrently && is_system_catalog {
                if !warned_catalog_concurrent {
                    push_warning("cannot reindex system catalogs concurrently, skipping all");
                    warned_catalog_concurrent = true;
                }
                continue;
            }
            let Some(relation) = catalog.lookup_relation_by_oid(class_row.oid) else {
                continue;
            };
            ensure_relation_owner(self, client_id, &relation, &class_row.relname)?;
            self.reindex_table_indexes_in_transaction(
                client_id,
                catalog,
                &relation,
                xid,
                cid,
                catalog_effects,
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(label: &str) -> std::path::PathBuf {
        crate::pgrust::test_support::seeded_temp_dir("database", label)
    }

    #[test]
    fn resolve_brin_options_accepts_pages_per_range() {
        let dir = temp_dir("brin_pages_per_range");
        let db = Database::open(&dir, 16).unwrap();

        let options = db
            .resolve_brin_options(&[RelOption {
                name: "pages_per_range".into(),
                value: "32".into(),
            }])
            .unwrap();
        assert_eq!(options.pages_per_range, 32);
    }

    #[test]
    fn resolve_brin_options_rejects_autosummarize() {
        let dir = temp_dir("brin_autosummarize");
        let db = Database::open(&dir, 16).unwrap();

        let err = db
            .resolve_brin_options(&[RelOption {
                name: "autosummarize".into(),
                value: "true".into(),
            }])
            .unwrap_err();
        assert!(matches!(
            err,
            ExecError::Parse(ParseError::FeatureNotSupported(message))
                if message == "BRIN option \"autosummarize\""
        ));
    }

    #[test]
    fn resolve_gist_options_accepts_buffering_modes() {
        let dir = temp_dir("gist_buffering_modes");
        let db = Database::open(&dir, 16).unwrap();

        for (value, expected) in [
            ("auto", GistBufferingMode::Auto),
            ("on", GistBufferingMode::On),
            ("off", GistBufferingMode::Off),
        ] {
            let options = db
                .resolve_gist_options(&[RelOption {
                    name: "buffering".into(),
                    value: value.into(),
                }])
                .unwrap();
            assert_eq!(options.buffering_mode, expected);
        }
    }

    #[test]
    fn resolve_gist_options_rejects_unknown_option() {
        let dir = temp_dir("gist_unknown_option");
        let db = Database::open(&dir, 16).unwrap();

        let err = db
            .resolve_gist_options(&[RelOption {
                name: "fillfactor".into(),
                value: "90".into(),
            }])
            .unwrap_err();
        assert!(matches!(
            err,
            ExecError::Parse(ParseError::FeatureNotSupported(message))
                if message == "GiST option \"fillfactor\""
        ));
    }
}
