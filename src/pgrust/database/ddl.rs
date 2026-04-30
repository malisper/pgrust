use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::{CatalogTxnContext, ClientId, Database};
use crate::backend::access::common::toast_compression::ensure_attribute_compression_supported;
use crate::backend::catalog::CatalogError;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::catalog::pg_depend::collect_sql_expr_column_names;
use crate::backend::executor::{ColumnDesc, ExecError, Expr, RelationDesc};
use crate::backend::parser::{
    AlterColumnExpressionAction, BoundRelation, CatalogLookup, CheckConstraintAction, ColumnDef,
    NotNullConstraintAction, OwnedSequenceSpec, ParseError, RawTypeName, SerialKind, SqlExpr,
    SqlType, SqlTypeKind, bind_generated_expr, bind_scalar_expr_in_scope,
    derive_literal_default_value, expr_references_column, is_collatable_type,
    normalize_alter_table_add_column_constraints, parse_expr, raw_type_name_hint,
    resolve_collation_oid, resolve_raw_type_name, sql_type_name,
};
use crate::backend::utils::cache::relcache::RelCacheEntry;
use crate::backend::utils::cache::syscache::{
    SearchSysCache1, SearchSysCacheList1, SearchSysCacheList2, SysCacheId, SysCacheTuple,
    ensure_class_rows, ensure_depend_rows, ensure_namespace_rows, ensure_rewrite_rows,
};
use crate::backend::utils::misc::notices::push_notice;
use crate::include::access::htup::{AttributeCompression, AttributeStorage};
use crate::include::catalog::{
    CONSTRAINT_FOREIGN, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL, PG_CATALOG_NAMESPACE_OID,
    PG_CLASS_RELATION_OID, PG_PROC_RELATION_OID, PG_REWRITE_RELATION_OID, PG_TRIGGER_RELATION_OID,
    PG_TYPE_RELATION_OID, PUBLIC_NAMESPACE_OID, builtin_range_name_for_sql_type,
    builtin_type_name_for_oid, relkind_is_analyzable,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::{ColumnGeneratedKind, Query, RangeTblEntryKind};
use crate::include::nodes::primnodes::{Var, user_attrno};
use crate::pgrust::database::{
    CatalogMutationEffect, CatalogWriteContext, CommandId, TransactionId,
};

pub(super) fn is_system_column_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "tableoid" | "ctid" | "xmin" | "xmax" | "cmin" | "cmax"
    )
}

pub(super) fn reject_column_referenced_by_generated_columns(
    catalog: &dyn CatalogLookup,
    desc: &RelationDesc,
    column_index: usize,
    operation: &str,
) -> Result<(), ExecError> {
    for (generated_index, generated_column) in desc.columns.iter().enumerate() {
        if generated_index == column_index || generated_column.generated.is_none() {
            continue;
        }
        let Some(expr) =
            bind_generated_expr(desc, generated_index, catalog).map_err(ExecError::Parse)?
        else {
            continue;
        };
        if expr_references_column(&expr, column_index) {
            let referenced = &desc.columns[column_index];
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot {operation} column \"{}\" because other objects depend on it",
                    referenced.name
                ),
                detail: Some(format!(
                    "Column \"{}\" is used by generated column \"{}\".",
                    referenced.name, generated_column.name
                )),
                hint: None,
                sqlstate: "2BP01",
            });
        }
    }
    Ok(())
}

pub(super) fn lookup_heap_relation_for_ddl(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Result<BoundRelation, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'r' | 'f') => Ok(entry),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table",
        })),
        None => Err(ExecError::Parse(ParseError::TableDoesNotExist(
            name.to_string(),
        ))),
    }
}

pub(super) fn lookup_trigger_relation_for_ddl(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Result<BoundRelation, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'r' | 'p' | 'v') => Ok(entry),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table or view",
        })),
        None => Err(ExecError::Parse(ParseError::TableDoesNotExist(
            name.to_string(),
        ))),
    }
}

pub(super) fn lookup_heap_relation_for_alter_table(
    catalog: &dyn CatalogLookup,
    name: &str,
    if_exists: bool,
) -> Result<Option<BoundRelation>, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'r' | 'f') => Ok(Some(entry)),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table",
        })),
        None if if_exists => {
            push_notice(format!(r#"relation "{name}" does not exist, skipping"#));
            Ok(None)
        }
        None => Err(ExecError::Parse(ParseError::UnknownTable(name.to_string()))),
    }
}

pub(super) fn lookup_table_or_partitioned_table_for_alter_table(
    catalog: &dyn CatalogLookup,
    name: &str,
    if_exists: bool,
) -> Result<Option<BoundRelation>, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'r' | 'p' | 'f') => Ok(Some(entry)),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table",
        })),
        None if if_exists => {
            push_notice(format!(r#"relation "{name}" does not exist, skipping"#));
            Ok(None)
        }
        None => Err(ExecError::Parse(ParseError::UnknownTable(name.to_string()))),
    }
}

pub(super) fn lookup_index_relation_for_alter_index(
    catalog: &dyn CatalogLookup,
    name: &str,
    if_exists: bool,
) -> Result<Option<BoundRelation>, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'i' | 'I') => Ok(Some(entry)),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "index",
        })),
        None if if_exists => Ok(None),
        None => Err(ExecError::Parse(ParseError::TableDoesNotExist(
            name.to_string(),
        ))),
    }
}

pub(super) fn lookup_index_or_partitioned_index_for_alter_index_rename(
    catalog: &dyn CatalogLookup,
    name: &str,
    if_exists: bool,
) -> Result<Option<BoundRelation>, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'i' | 'I') => Ok(Some(entry)),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "index",
        })),
        None if if_exists => Ok(None),
        None => Err(ExecError::Parse(ParseError::TableDoesNotExist(
            name.to_string(),
        ))),
    }
}

pub(super) fn lookup_rule_relation_for_ddl(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Result<BoundRelation, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if matches!(entry.relkind, 'r' | 'p' | 'v') => Ok(entry),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table or view",
        })),
        None => Err(ExecError::Parse(ParseError::TableDoesNotExist(
            name.to_string(),
        ))),
    }
}

pub(super) fn lookup_analyzable_relation_for_ddl(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Result<BoundRelation, ExecError> {
    match catalog.lookup_any_relation(name) {
        Some(entry) if relkind_is_analyzable(entry.relkind) => Ok(entry),
        Some(_) => Err(ExecError::Parse(ParseError::WrongObjectType {
            name: name.to_string(),
            expected: "table",
        })),
        None => Err(ExecError::Parse(ParseError::TableDoesNotExist(
            name.to_string(),
        ))),
    }
}

fn auth_catalog_for_ddl(
    db: &Database,
    client_id: ClientId,
) -> Result<crate::pgrust::auth::AuthCatalog, ExecError> {
    db.auth_catalog(client_id, None).map_err(|err| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "authorization catalog",
            actual: format!("{err:?}"),
        })
    })
}

fn ddl_oid_key(oid: u32) -> Value {
    Value::Int64(i64::from(oid))
}

fn role_row_by_oid_for_ddl(
    db: &Database,
    client_id: ClientId,
    role_oid: u32,
) -> Result<Option<crate::include::catalog::PgAuthIdRow>, ExecError> {
    Ok(SearchSysCache1(
        db,
        client_id,
        None,
        SysCacheId::AUTHOID,
        ddl_oid_key(role_oid),
    )
    .map_err(map_catalog_error)?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::AuthId(row) => Some(row),
        _ => None,
    }))
}

fn membership_rows_for_member_for_ddl(
    db: &Database,
    client_id: ClientId,
    member_oid: u32,
) -> Result<Vec<crate::include::catalog::PgAuthMembersRow>, ExecError> {
    Ok(SearchSysCacheList1(
        db,
        client_id,
        None,
        SysCacheId::AUTHMEMMEMROLE,
        ddl_oid_key(member_oid),
    )
    .map_err(map_catalog_error)?
    .into_iter()
    .filter_map(|tuple| match tuple {
        SysCacheTuple::AuthMembers(row) => Some(row),
        _ => None,
    })
    .collect())
}

fn has_effective_membership_for_ddl(
    db: &Database,
    client_id: ClientId,
    target_oid: u32,
) -> Result<bool, ExecError> {
    let auth = db.auth_state(client_id);
    let current_user_oid = auth.current_user_oid();
    if current_user_oid == target_oid {
        return Ok(true);
    }
    if role_row_by_oid_for_ddl(db, client_id, current_user_oid)?.is_some_and(|row| row.rolsuper) {
        return Ok(true);
    }

    let mut pending = VecDeque::from([current_user_oid]);
    let mut visited = BTreeSet::new();
    while let Some(member_oid) = pending.pop_front() {
        if !visited.insert(member_oid) {
            continue;
        }
        for membership in membership_rows_for_member_for_ddl(db, client_id, member_oid)? {
            if !membership.inherit_option {
                continue;
            }
            if membership.roleid == target_oid {
                return Ok(true);
            }
            pending.push_back(membership.roleid);
        }
    }
    Ok(false)
}

pub(super) fn relation_kind_name(relkind: char) -> &'static str {
    match relkind {
        'c' => "type",
        'f' => "foreign table",
        'm' => "materialized view",
        'p' => "partitioned table",
        'S' => "sequence",
        'v' => "view",
        'i' | 'I' => "index",
        _ => "table",
    }
}

pub(super) fn ensure_relation_owner(
    db: &Database,
    client_id: ClientId,
    relation: &BoundRelation,
    display_name: &str,
) -> Result<(), ExecError> {
    if has_effective_membership_for_ddl(db, client_id, relation.owner_oid)? {
        return Ok(());
    }
    let owner_object_kind = match relation.relkind {
        'p' => "table",
        _ => relation_kind_name(relation.relkind),
    };
    Err(ExecError::DetailedError {
        message: format!("must be owner of {} {}", owner_object_kind, display_name),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}

pub(super) fn ensure_can_set_role(
    db: &Database,
    client_id: ClientId,
    role_oid: u32,
    role_name: &str,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = auth_catalog_for_ddl(db, client_id)?;
    if auth.can_set_role(role_oid, &auth_catalog) {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!("must be able to SET ROLE \"{role_name}\""),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}

pub(super) fn namespace_oid_for_relation_name(name: &str) -> u32 {
    match name.split_once('.') {
        Some(("pg_catalog", _)) => PG_CATALOG_NAMESPACE_OID,
        Some(("public", _)) | None => PUBLIC_NAMESPACE_OID,
        _ => PUBLIC_NAMESPACE_OID,
    }
}

fn dependent_view_names_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    relation_oid: u32,
) -> Vec<String> {
    let namespaces = ensure_namespace_rows(db, client_id, txn_ctx)
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let classes = ensure_class_rows(db, client_id, txn_ctx)
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();
    let rewrites = ensure_rewrite_rows(db, client_id, txn_ctx)
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();

    let mut names = ensure_depend_rows(db, client_id, txn_ctx)
        .into_iter()
        .filter(|row| {
            row.classid == PG_REWRITE_RELATION_OID
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == relation_oid
                && row.deptype == DEPENDENCY_NORMAL
        })
        .filter_map(|row| {
            let rewrite = rewrites.get(&row.objid)?;
            let class = classes.get(&rewrite.ev_class)?;
            let schema = namespaces
                .get(&class.relnamespace)
                .cloned()
                .unwrap_or_else(|| "public".to_string());
            Some(match schema.as_str() {
                "public" | "pg_catalog" => class.relname.clone(),
                _ => format!("{schema}.{}", class.relname),
            })
        })
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

#[derive(Debug, Clone)]
pub(crate) struct DependentViewRewrite {
    pub(crate) relation_oid: u32,
    pub(crate) relation_desc: RelationDesc,
    pub(crate) query: Query,
    pub(crate) check_option: crate::include::nodes::parsenodes::ViewCheckOption,
}

pub(crate) fn dependent_view_rewrites_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    relation_oid: u32,
) -> Result<Vec<DependentViewRewrite>, ExecError> {
    let catalog = db.lazy_catalog_lookup(client_id, txn_ctx, None);
    let mut rewrites = Vec::new();
    for dependent_oid in direct_dependent_view_oids(db, client_id, txn_ctx, relation_oid) {
        let Some(relation) = catalog.lookup_relation_by_oid(dependent_oid) else {
            continue;
        };
        let (_, check_option) = crate::backend::rewrite::split_stored_view_definition_sql(
            &rewrite_return_rule_sql(&catalog, dependent_oid)?,
        );
        let query = crate::backend::rewrite::load_view_return_query(
            dependent_oid,
            &relation.desc,
            None,
            &catalog,
            &[],
        )
        .map_err(ExecError::Parse)?;
        rewrites.push(DependentViewRewrite {
            relation_oid: dependent_oid,
            relation_desc: relation.desc,
            query,
            check_option,
        });
    }
    Ok(rewrites)
}

fn direct_dependent_view_oids(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    relation_oid: u32,
) -> Vec<u32> {
    let mut oids = rewrite_dependency_rows_for_relation(db, client_id, txn_ctx, relation_oid)
        .into_iter()
        .filter_map(|row| rewrite_row_by_oid_for_ddl(db, client_id, txn_ctx, row.objid))
        .map(|rewrite| rewrite.ev_class)
        .collect::<Vec<_>>();
    oids.sort_unstable();
    oids.dedup();
    oids
}

fn rewrite_row_by_oid_for_ddl(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    rewrite_oid: u32,
) -> Option<crate::include::catalog::PgRewriteRow> {
    SearchSysCache1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::REWRITEOID,
        ddl_oid_key(rewrite_oid),
    )
    .ok()?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::Rewrite(row) => Some(row),
        _ => None,
    })
}

fn class_row_by_oid_for_ddl(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    relation_oid: u32,
) -> Option<crate::include::catalog::PgClassRow> {
    SearchSysCache1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::RELOID,
        ddl_oid_key(relation_oid),
    )
    .ok()?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::Class(row) => Some(row),
        _ => None,
    })
}

fn rewrite_dependency_rows_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    relation_oid: u32,
) -> Vec<crate::include::catalog::PgDependRow> {
    SearchSysCacheList2(
        db,
        client_id,
        txn_ctx,
        SysCacheId::DEPENDREFERENCE,
        ddl_oid_key(PG_CLASS_RELATION_OID),
        ddl_oid_key(relation_oid),
    )
    .unwrap_or_default()
    .into_iter()
    .filter_map(|tuple| match tuple {
        SysCacheTuple::Depend(row) => Some(row),
        _ => None,
    })
    .filter(|row| {
        row.classid == PG_REWRITE_RELATION_OID
            && row.refclassid == PG_CLASS_RELATION_OID
            && row.refobjid == relation_oid
            && row.deptype == DEPENDENCY_NORMAL
    })
    .collect()
}

fn rewrite_return_rule_sql(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Result<String, ExecError> {
    let mut rows = catalog.rewrite_rows_for_relation(relation_oid);
    rows.retain(|row| row.rulename == "_RETURN");
    match rows.as_slice() {
        [row] => Ok(row.ev_action.clone()),
        [] => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "view _RETURN rule",
            actual: format!("missing rewrite rule for view {relation_oid}"),
        })),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "single view _RETURN rule",
            actual: format!("multiple rewrite rules for view {relation_oid}"),
        })),
    }
}

pub(super) fn reject_relation_with_dependent_views(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    relation_oid: u32,
    operation: &'static str,
) -> Result<(), ExecError> {
    let dependent_views = dependent_view_names_for_relation(db, client_id, txn_ctx, relation_oid);
    if dependent_views.is_empty() {
        return Ok(());
    }
    Err(ExecError::Parse(ParseError::UnexpectedToken {
        expected: operation,
        actual: format!(
            "cannot {operation}; view depends on it: {}",
            dependent_views.join(", ")
        ),
    }))
}

pub(crate) fn rewrite_dependent_views(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    dependent_views: &[DependentViewRewrite],
    xid: TransactionId,
    cid: CommandId,
    catalog_effects: &mut Vec<CatalogMutationEffect>,
) -> Result<(), ExecError> {
    let catalog = db.lazy_catalog_lookup(client_id, txn_ctx, None);
    for (index, dependent_view) in dependent_views.iter().enumerate() {
        let mut query = dependent_view.query.clone();
        crate::backend::rewrite::refresh_query_relation_descriptors(&mut query, &catalog);
        let rewrite_oid = catalog
            .rewrite_rows_for_relation(dependent_view.relation_oid)
            .into_iter()
            .find(|row| row.rulename == "_RETURN")
            .map(|row| row.oid)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "view _RETURN rule",
                    actual: format!(
                        "missing rewrite rule for view {}",
                        dependent_view.relation_oid
                    ),
                })
            })?;
        if crate::backend::rewrite::has_stored_view_query(rewrite_oid) {
            crate::backend::rewrite::register_stored_view_query(rewrite_oid, query);
            continue;
        }
        let mut sql = crate::backend::rewrite::render_view_query_sql(&query, &catalog);
        sql = append_view_check_option(sql, dependent_view.check_option);
        let rule_ctx = CatalogWriteContext {
            pool: db.pool.clone(),
            txns: db.txns.clone(),
            xid,
            cid: cid.saturating_add(index as u32),
            client_id,
            waiter: None,
            interrupts: db.interrupt_state(client_id),
        };
        let drop_effect = db
            .catalog
            .write()
            .drop_rule_mvcc(rewrite_oid, &rule_ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(drop_effect);
        let create_ctx = CatalogWriteContext {
            cid: rule_ctx.cid.saturating_add(1),
            ..rule_ctx
        };
        let referenced = direct_relation_oids_in_query(&query);
        let create_effect = db
            .catalog
            .write()
            .create_rule_mvcc_with_owner_dependency(
                dependent_view.relation_oid,
                "_RETURN",
                '1',
                true,
                String::new(),
                sql,
                &referenced,
                &[],
                crate::backend::catalog::store::RuleOwnerDependency::Internal,
                &create_ctx,
            )
            .map_err(map_catalog_error)?;
        catalog_effects.push(create_effect);
    }
    Ok(())
}

pub(crate) fn append_view_check_option(
    sql: String,
    check_option: crate::include::nodes::parsenodes::ViewCheckOption,
) -> String {
    let sql = sql.trim().trim_end_matches(';').trim();
    match check_option {
        crate::include::nodes::parsenodes::ViewCheckOption::None => sql.to_string(),
        crate::include::nodes::parsenodes::ViewCheckOption::Local => {
            format!("{sql} WITH LOCAL CHECK OPTION")
        }
        crate::include::nodes::parsenodes::ViewCheckOption::Cascaded => {
            format!("{sql} WITH CASCADED CHECK OPTION")
        }
    }
}

fn direct_relation_oids_in_query(query: &Query) -> Vec<u32> {
    let mut oids = Vec::new();
    for rte in &query.rtable {
        match &rte.kind {
            RangeTblEntryKind::Relation { relation_oid, .. } => oids.push(*relation_oid),
            RangeTblEntryKind::Subquery { query } => {
                oids.extend(direct_relation_oids_in_query(query))
            }
            _ => {}
        }
    }
    oids.sort_unstable();
    oids.dedup();
    oids
}

pub(crate) fn any_dependent_view_references_column(
    dependent_views: &[DependentViewRewrite],
    relation_oid: u32,
    attnum: i16,
) -> bool {
    dependent_views
        .iter()
        .any(|view| query_references_relation_column(&view.query, relation_oid, attnum))
}

fn query_references_relation_column(query: &Query, relation_oid: u32, attnum: i16) -> bool {
    query
        .target_list
        .iter()
        .any(|target| expr_references_relation_column(&target.expr, query, relation_oid, attnum))
        || query
            .where_qual
            .as_ref()
            .is_some_and(|expr| expr_references_relation_column(expr, query, relation_oid, attnum))
        || query
            .group_by
            .iter()
            .any(|expr| expr_references_relation_column(expr, query, relation_oid, attnum))
        || query
            .having_qual
            .as_ref()
            .is_some_and(|expr| expr_references_relation_column(expr, query, relation_oid, attnum))
        || query
            .sort_clause
            .iter()
            .any(|sort| expr_references_relation_column(&sort.expr, query, relation_oid, attnum))
        || query.rtable.iter().any(|rte| match &rte.kind {
            RangeTblEntryKind::Subquery { query } => {
                query_references_relation_column(query, relation_oid, attnum)
            }
            _ => false,
        })
}

fn expr_references_relation_column(
    expr: &Expr,
    query: &Query,
    relation_oid: u32,
    attnum: i16,
) -> bool {
    match expr {
        Expr::Var(var) => var_references_relation_column(var, query, relation_oid, attnum),
        Expr::GroupingKey(grouping_key) => {
            expr_references_relation_column(&grouping_key.expr, query, relation_oid, attnum)
        }
        Expr::GroupingFunc(grouping_func) => grouping_func
            .args
            .iter()
            .any(|arg| expr_references_relation_column(arg, query, relation_oid, attnum)),
        Expr::Aggref(aggref) => {
            aggref
                .direct_args
                .iter()
                .any(|arg| expr_references_relation_column(arg, query, relation_oid, attnum))
                || aggref
                    .args
                    .iter()
                    .any(|arg| expr_references_relation_column(arg, query, relation_oid, attnum))
                || aggref.aggfilter.as_ref().is_some_and(|expr| {
                    expr_references_relation_column(expr, query, relation_oid, attnum)
                })
        }
        Expr::WindowFunc(window_func) => window_func
            .args
            .iter()
            .any(|arg| expr_references_relation_column(arg, query, relation_oid, attnum)),
        Expr::Op(op) => op
            .args
            .iter()
            .any(|arg| expr_references_relation_column(arg, query, relation_oid, attnum)),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(|arg| expr_references_relation_column(arg, query, relation_oid, attnum)),
        Expr::Case(case_expr) => {
            case_expr.arg.as_ref().is_some_and(|arg| {
                expr_references_relation_column(arg, query, relation_oid, attnum)
            }) || case_expr.args.iter().any(|when| {
                expr_references_relation_column(&when.expr, query, relation_oid, attnum)
                    || expr_references_relation_column(&when.result, query, relation_oid, attnum)
            }) || expr_references_relation_column(&case_expr.defresult, query, relation_oid, attnum)
        }
        Expr::Func(func) => func
            .args
            .iter()
            .any(|arg| expr_references_relation_column(arg, query, relation_oid, attnum)),
        Expr::SqlJsonQueryFunction(func) => func
            .child_exprs()
            .into_iter()
            .any(|expr| expr_references_relation_column(expr, query, relation_oid, attnum)),
        Expr::SetReturning(_) => false,
        Expr::SubLink(sublink) => {
            sublink.testexpr.as_ref().is_some_and(|expr| {
                expr_references_relation_column(expr, query, relation_oid, attnum)
            }) || query_references_relation_column(&sublink.subselect, relation_oid, attnum)
        }
        Expr::SubPlan(subplan) => subplan
            .args
            .iter()
            .any(|arg| expr_references_relation_column(arg, query, relation_oid, attnum)),
        Expr::ScalarArrayOp(saop) => {
            expr_references_relation_column(&saop.left, query, relation_oid, attnum)
                || expr_references_relation_column(&saop.right, query, relation_oid, attnum)
        }
        Expr::Xml(xml) => xml
            .args
            .iter()
            .any(|arg| expr_references_relation_column(arg, query, relation_oid, attnum)),
        Expr::Cast(inner, _) => expr_references_relation_column(inner, query, relation_oid, attnum),
        Expr::Collate { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::FieldSelect { expr, .. } => {
            expr_references_relation_column(expr, query, relation_oid, attnum)
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
            expr_references_relation_column(expr, query, relation_oid, attnum)
                || expr_references_relation_column(pattern, query, relation_oid, attnum)
                || escape.as_ref().is_some_and(|expr| {
                    expr_references_relation_column(expr, query, relation_oid, attnum)
                })
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_references_relation_column(left, query, relation_oid, attnum)
                || expr_references_relation_column(right, query, relation_oid, attnum)
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .any(|expr| expr_references_relation_column(expr, query, relation_oid, attnum)),
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| expr_references_relation_column(expr, query, relation_oid, attnum)),
        Expr::ArraySubscript { array, subscripts } => {
            expr_references_relation_column(array, query, relation_oid, attnum)
                || subscripts.iter().any(|subscript| {
                    subscript.lower.as_ref().is_some_and(|expr| {
                        expr_references_relation_column(expr, query, relation_oid, attnum)
                    }) || subscript.upper.as_ref().is_some_and(|expr| {
                        expr_references_relation_column(expr, query, relation_oid, attnum)
                    })
                })
        }
        Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}

fn var_references_relation_column(
    var: &Var,
    query: &Query,
    relation_oid: u32,
    attnum: i16,
) -> bool {
    query
        .rtable
        .get(var.varno.saturating_sub(1))
        .is_some_and(|rte| match &rte.kind {
            RangeTblEntryKind::Relation {
                relation_oid: rte_relation_oid,
                ..
            } => {
                *rte_relation_oid == relation_oid
                    && usize::try_from(attnum)
                        .ok()
                        .is_some_and(|index| var.varattno == user_attrno(index.saturating_sub(1)))
            }
            _ => false,
        })
}

fn relation_name_for_oid(catalog: &dyn CatalogLookup, relation_oid: u32) -> String {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation_oid.to_string())
}

pub(crate) fn reject_relation_with_referencing_foreign_keys(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    operation: &'static str,
) -> Result<(), ExecError> {
    reject_relation_with_referencing_foreign_keys_except(catalog, relation_oid, &[], operation)
}

pub(crate) fn reject_relation_with_referencing_foreign_keys_except(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    ignored_relation_oids: &[u32],
    operation: &'static str,
) -> Result<(), ExecError> {
    let mut references = catalog
        .foreign_key_constraint_rows_referencing_relation(relation_oid)
        .into_iter()
        .filter(|row| !ignored_relation_oids.contains(&row.conrelid))
        .map(|row| (row.conname, relation_name_for_oid(catalog, row.conrelid)))
        .collect::<Vec<_>>();
    references.sort();
    references.dedup();

    let Some((constraint_name, child_relation_name)) = references.into_iter().next() else {
        return Ok(());
    };
    Err(ExecError::Parse(ParseError::UnexpectedToken {
        expected: operation,
        actual: format!(
            "foreign key constraint \"{constraint_name}\" on table \"{child_relation_name}\" references this table"
        ),
    }))
}

pub(crate) fn reject_column_with_foreign_key_dependencies(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    column_name: &str,
    attnum: i16,
    operation: &'static str,
) -> Result<(), ExecError> {
    let outbound = catalog
        .constraint_rows_for_relation(relation_oid)
        .into_iter()
        .filter(|row| row.contype == CONSTRAINT_FOREIGN);
    let inbound = catalog
        .foreign_key_constraint_rows_referencing_relation(relation_oid)
        .into_iter();
    let mut messages = outbound
        .chain(inbound)
        .filter_map(|row| {
            if row.conrelid == relation_oid
                && row
                    .conkey
                    .as_ref()
                    .is_some_and(|attnums| attnums.contains(&attnum))
            {
                return Some(format!(
                    "column \"{column_name}\" is used by foreign key constraint \"{}\"",
                    row.conname
                ));
            }
            if row.confrelid == relation_oid
                && row
                    .confkey
                    .as_ref()
                    .is_some_and(|attnums| attnums.contains(&attnum))
            {
                return Some(format!(
                    "column \"{column_name}\" is referenced by foreign key constraint \"{}\" on table \"{}\"",
                    row.conname,
                    relation_name_for_oid(catalog, row.conrelid),
                ));
            }
            None
        })
        .collect::<Vec<_>>();
    messages.sort();
    messages.dedup();

    let Some(actual) = messages.into_iter().next() else {
        return Ok(());
    };
    Err(ExecError::Parse(ParseError::UnexpectedToken {
        expected: operation,
        actual,
    }))
}

pub(crate) fn reject_column_with_trigger_dependencies(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    column_name: &str,
    attnum: i16,
) -> Result<(), ExecError> {
    let relation_name = relation_name_for_oid(catalog, relation_oid);
    let trigger_rows = catalog.trigger_rows_for_relation(relation_oid);
    let details = catalog
        .depend_rows_referencing(PG_CLASS_RELATION_OID, relation_oid, Some(i32::from(attnum)))
        .into_iter()
        .filter(|row| {
            row.classid == PG_TRIGGER_RELATION_OID
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == relation_oid
                && row.refobjsubid == i32::from(attnum)
        })
        .filter_map(|row| {
            trigger_rows
                .iter()
                .find(|trigger| trigger.oid == row.objid)
                .map(|trigger| {
                    format!(
                        "trigger {} on table {} depends on column {} of table {}",
                        trigger.tgname, relation_name, column_name, relation_name
                    )
                })
        })
        .collect::<Vec<_>>();
    if details.is_empty() {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!(
            "cannot drop column {} of table {} because other objects depend on it",
            column_name, relation_name
        ),
        detail: Some(details.join("\n")),
        hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
        sqlstate: "2BP01",
    })
}

pub(crate) fn reject_column_with_publication_dependencies(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    column_name: &str,
    attnum: i16,
) -> Result<(), ExecError> {
    let relation_name = relation_name_for_oid(catalog, relation_oid);
    let publications = catalog
        .publication_rows()
        .into_iter()
        .map(|row| (row.oid, row.pubname))
        .collect::<BTreeMap<_, _>>();
    let mut details = catalog
        .publication_rel_rows()
        .into_iter()
        .filter(|row| row.prrelid == relation_oid)
        .filter(|row| {
            row.prattrs
                .as_ref()
                .is_some_and(|attrs| attrs.contains(&attnum))
                || row.prqual.as_ref().is_some_and(|qual| {
                    parse_expr(qual).is_ok_and(|expr| {
                        let mut column_names = BTreeSet::new();
                        collect_sql_expr_column_names(&expr, &mut column_names);
                        column_names
                            .iter()
                            .any(|name| name.eq_ignore_ascii_case(column_name))
                    })
                })
        })
        .filter_map(|row| {
            let publication_name = publications.get(&row.prpubid)?;
            Some(format!(
                "publication of table {relation_name} in publication {publication_name} depends on column {column_name} of table {relation_name}"
            ))
        })
        .collect::<Vec<_>>();
    details.sort();
    details.dedup();
    if details.is_empty() {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!(
            "cannot drop column {column_name} of table {relation_name} because other objects depend on it"
        ),
        detail: Some(details.join("\n")),
        hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
        sqlstate: "2BP01",
    })
}

pub(crate) fn reject_column_with_rule_dependencies(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    relation_oid: u32,
    column_name: &str,
    attnum: i16,
) -> Result<(), ExecError> {
    let rewrites = rewrite_dependency_rows_for_relation(db, client_id, txn_ctx, relation_oid)
        .into_iter()
        .filter(|row| row.refobjsubid == i32::from(attnum))
        .filter_map(|row| rewrite_row_by_oid_for_ddl(db, client_id, txn_ctx, row.objid))
        .collect::<Vec<_>>();
    if rewrites.is_empty() {
        return Ok(());
    }
    let relation_name = class_row_by_oid_for_ddl(db, client_id, txn_ctx, relation_oid)
        .map(|row| row.relname.clone())
        .unwrap_or_else(|| relation_oid.to_string());
    let mut details = rewrites
        .into_iter()
        .filter_map(|rewrite| {
            let view = class_row_by_oid_for_ddl(db, client_id, txn_ctx, rewrite.ev_class)?;
            Some(format!(
                "view {} depends on column {} of table {}",
                view.relname, column_name, relation_name
            ))
        })
        .collect::<Vec<_>>();
    details.sort();
    details.dedup();
    if details.is_empty() {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!(
            "cannot drop column {column_name} of table {relation_name} because other objects depend on it"
        ),
        detail: Some(details.join("\n")),
        hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
        sqlstate: "2BP01",
    })
}

pub(crate) fn reject_column_type_change_with_rule_dependencies(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    relation_oid: u32,
    column_name: &str,
    attnum: i16,
) -> Result<(), ExecError> {
    let rewrites = rewrite_dependency_rows_for_relation(db, client_id, txn_ctx, relation_oid)
        .into_iter()
        .filter(|row| row.refobjsubid == i32::from(attnum))
        .filter_map(|row| rewrite_row_by_oid_for_ddl(db, client_id, txn_ctx, row.objid))
        .collect::<Vec<_>>();
    let mut details = rewrites
        .into_iter()
        .filter_map(|rewrite| {
            let view = class_row_by_oid_for_ddl(db, client_id, txn_ctx, rewrite.ev_class)?;
            Some(format!(
                "rule {} on view {} depends on column \"{}\"",
                rewrite.rulename, view.relname, column_name
            ))
        })
        .collect::<Vec<_>>();
    details.sort();
    details.dedup();
    if details.is_empty() {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: "cannot alter type of a column used by a view or rule".into(),
        detail: Some(details.join("\n")),
        hint: None,
        sqlstate: "2BP01",
    })
}

pub(crate) fn reject_index_with_referencing_foreign_keys(
    catalog: &dyn CatalogLookup,
    index_oid: u32,
    operation: &'static str,
) -> Result<(), ExecError> {
    let mut references = catalog
        .foreign_key_constraint_rows_referencing_index(index_oid)
        .into_iter()
        .map(|row| (row.conname, relation_name_for_oid(catalog, row.conrelid)))
        .collect::<Vec<_>>();
    references.sort();
    references.dedup();

    let Some((constraint_name, child_relation_name)) = references.into_iter().next() else {
        return Ok(());
    };
    Err(ExecError::Parse(ParseError::UnexpectedToken {
        expected: operation,
        actual: format!(
            "foreign key constraint \"{constraint_name}\" on table \"{child_relation_name}\" depends on the referenced key"
        ),
    }))
}

pub(super) fn reject_type_with_dependents(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    type_oid: u32,
    display_name: &str,
) -> Result<(), ExecError> {
    let catcache = db.backend_catcache(client_id, txn_ctx).map_err(|err| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "catalog lookup",
            actual: format!("{err:?}"),
        })
    })?;
    let format_name = |namespace_oid: u32, object_name: &str| {
        let schema_name = catcache
            .namespace_by_oid(namespace_oid)
            .map(|row| row.nspname.clone())
            .unwrap_or_else(|| "public".to_string());
        match schema_name.as_str() {
            "public" | "pg_catalog" => object_name.to_string(),
            _ => format!("{schema_name}.{object_name}"),
        }
    };

    let mut dependents = catcache
        .depend_rows()
        .into_iter()
        .filter(|row| {
            row.refclassid == PG_TYPE_RELATION_OID
                && row.refobjid == type_oid
                && row.deptype != DEPENDENCY_INTERNAL
        })
        .filter_map(|row| match row.classid {
            PG_CLASS_RELATION_OID => {
                let class = catcache.class_by_oid(row.objid)?;
                Some((
                    row.objid,
                    format!(
                        "{} {}",
                        relation_kind_name(class.relkind),
                        format_name(class.relnamespace, &class.relname)
                    ),
                ))
            }
            PG_PROC_RELATION_OID => {
                let proc_row = catcache.proc_by_oid(row.objid)?;
                Some((
                    row.objid,
                    format!(
                        "function {}()",
                        format_name(proc_row.pronamespace, &proc_row.proname)
                    ),
                ))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    dependents.sort_by_key(|(oid, _)| *oid);
    dependents.dedup();
    if dependents.is_empty() {
        return Ok(());
    }

    Err(ExecError::DetailedError {
        message: format!("cannot drop type {display_name} because other objects depend on it"),
        detail: Some(
            dependents
                .into_iter()
                .map(|(_, name)| format!("{name} depends on type {display_name}"))
                .collect::<Vec<_>>()
                .join("\n"),
        ),
        hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
        sqlstate: "2BP01",
    })
}

pub(super) fn reject_inheritance_tree_ddl(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    operation: &'static str,
) -> Result<(), ExecError> {
    if catalog.has_subclass(relation_oid) || !catalog.inheritance_parents(relation_oid).is_empty() {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            operation.to_string(),
        )));
    }
    Ok(())
}

pub(super) fn validate_alter_table_add_column(
    table_name: &str,
    relation_desc: &RelationDesc,
    column: &ColumnDef,
    existing_constraints: &[crate::include::catalog::PgConstraintRow],
    catalog: &dyn CatalogLookup,
) -> Result<AlterTableAddColumnPlan, ExecError> {
    let serial_kind = match column.ty {
        RawTypeName::Serial(kind) => Some(kind),
        _ => None,
    };
    if column.primary_key() {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ADD COLUMN without PRIMARY KEY",
            actual: "PRIMARY KEY".into(),
        }));
    }
    if column.unique() {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ADD COLUMN without UNIQUE",
            actual: "UNIQUE".into(),
        }));
    }
    if is_system_column_name(&column.name) {
        return Err(ExecError::DetailedError {
            message: format!(
                "column name \"{}\" conflicts with a system column name",
                column.name
            ),
            detail: None,
            hint: None,
            sqlstate: "42701",
        });
    }
    if relation_desc
        .columns
        .iter()
        .any(|existing| existing.name.eq_ignore_ascii_case(&column.name))
    {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "new column name",
            actual: format!("column already exists: {}", column.name),
        }));
    }

    let sql_type = match column.ty {
        RawTypeName::Serial(_) => raw_type_name_hint(&column.ty),
        _ => resolve_raw_type_name(&column.ty, catalog).map_err(ExecError::Parse)?,
    };
    let mut desc = column_desc(
        column.name.clone(),
        sql_type,
        serial_kind.is_none() && column.identity.is_none(),
    );
    if let Some(collation) = column.collation.as_deref() {
        if !is_collatable_type(sql_type) {
            return Err(ExecError::Parse(ParseError::DetailedError {
                message: format!(
                    "collations are not supported by type {}",
                    sql_type_name(sql_type)
                ),
                detail: None,
                hint: None,
                sqlstate: "42804",
            }));
        }
        desc.collation_oid = resolve_collation_oid(collation, catalog).map_err(ExecError::Parse)?;
    }
    if column.generated.is_some() && column.default_expr.is_some() {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "generated column without DEFAULT",
            actual: format!(
                "both default and generation expression specified for column \"{}\"",
                column.name
            ),
        }));
    }
    if column.identity.is_some()
        && (column.generated.is_some() || column.default_expr.is_some() || serial_kind.is_some())
    {
        let actual = if column.generated.is_some() {
            format!(
                "both identity and generation expression specified for column \"{}\"",
                column.name
            )
        } else {
            format!(
                "conflicting identity definition for column \"{}\"",
                column.name
            )
        };
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "identity column without DEFAULT, generated expression, or serial type",
            actual,
        }));
    }
    if serial_kind.is_some() && column.generated.is_some() {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "non-serial generated column",
            actual: format!(
                "both serial and generation expression specified for column \"{}\"",
                column.name
            ),
        }));
    }
    if serial_kind.is_some() && column.default_expr.is_some() {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "serial column without explicit DEFAULT",
            actual: format!(
                "multiple default values specified for column \"{}\"",
                column.name
            ),
        }));
    }
    if let Some(generated) = &column.generated {
        desc.default_expr = Some(generated.expr_sql.clone());
        desc.generated = Some(generated.kind);
    } else if let Some(identity) = &column.identity {
        desc.identity = Some(identity.kind);
    } else if serial_kind.is_none() {
        desc.default_expr = column.default_expr.clone();
        if desc.default_expr.is_none()
            && let Some(type_oid) = catalog.type_oid_for_sql_type(sql_type)
            && let Some(type_default) = catalog.type_default_sql(type_oid)
        {
            desc.default_expr = Some(type_default);
        }
        if let Some(sql) = desc.default_expr.as_deref() {
            desc.missing_default_value = Some(derive_literal_default_value(sql, desc.sql_type)?);
        }
    }
    if let Some(storage) = column.storage {
        desc.storage.attstorage = storage;
    }
    let mut relation_with_new_column = relation_desc.clone();
    relation_with_new_column.columns.push(desc.clone());
    crate::backend::parser::validate_generated_columns(&relation_with_new_column, catalog)
        .map_err(ExecError::Parse)?;
    let constraint_actions =
        normalize_alter_table_add_column_constraints(table_name, column, existing_constraints)
            .map_err(ExecError::Parse)?;
    let owned_sequence = if let Some(serial_kind) = serial_kind {
        Some((
            serial_kind,
            crate::backend::parser::SequenceOptionsSpec::default(),
        ))
    } else if let Some(identity) = &column.identity {
        Some((
            serial_kind_for_identity_sql_type(sql_type).map_err(ExecError::Parse)?,
            identity.options.clone(),
        ))
    } else {
        None
    };
    Ok(AlterTableAddColumnPlan {
        column: desc,
        owned_sequence: owned_sequence.map(|(serial_kind, options)| OwnedSequenceSpec {
            column_index: relation_desc.columns.len(),
            column_name: column.name.clone(),
            serial_kind,
            sql_type,
            options,
        }),
        not_null_action: constraint_actions.not_null,
        check_actions: constraint_actions.checks,
    })
}

fn serial_kind_for_identity_sql_type(sql_type: SqlType) -> Result<SerialKind, ParseError> {
    match sql_type.kind {
        SqlTypeKind::Int2 if !sql_type.is_array => Ok(SerialKind::Small),
        SqlTypeKind::Int4 if !sql_type.is_array => Ok(SerialKind::Regular),
        SqlTypeKind::Int8 if !sql_type.is_array => Ok(SerialKind::Big),
        _ => Err(ParseError::UnexpectedToken {
            expected: "smallint, integer, or bigint identity column",
            actual: format_sql_type_name(sql_type),
        }),
    }
}

pub(super) fn validate_alter_table_rename_column(
    desc: &RelationDesc,
    relation_name: &str,
    column_name: &str,
    new_column_name: &str,
) -> Result<String, ExecError> {
    if is_system_column_name(column_name) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "user column name for RENAME COLUMN",
            actual: column_name.to_string(),
        }));
    }
    if is_system_column_name(new_column_name) {
        return Err(ExecError::DetailedError {
            message: format!(
                "column name \"{new_column_name}\" conflicts with a system column name"
            ),
            detail: None,
            hint: None,
            sqlstate: "42701",
        });
    }
    if new_column_name.contains('.') {
        return Err(ExecError::Parse(ParseError::UnsupportedQualifiedName(
            new_column_name.to_string(),
        )));
    }
    if !desc
        .columns
        .iter()
        .any(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
    {
        return Err(ExecError::Parse(ParseError::UnknownColumn(
            column_name.to_string(),
        )));
    }

    let normalized_new = new_column_name.to_ascii_lowercase();
    if desc.columns.iter().any(|column| {
        !column.dropped
            && !column.name.eq_ignore_ascii_case(column_name)
            && column.name.eq_ignore_ascii_case(&normalized_new)
    }) {
        return Err(ExecError::DetailedError {
            message: format!(
                "column \"{new_column_name}\" of relation \"{relation_name}\" already exists"
            ),
            detail: None,
            hint: None,
            sqlstate: "42701",
        });
    }

    Ok(normalized_new)
}

#[derive(Debug, Clone)]
pub(super) struct AlterTableAddColumnPlan {
    pub column: ColumnDesc,
    pub owned_sequence: Option<OwnedSequenceSpec>,
    pub not_null_action: Option<NotNullConstraintAction>,
    pub check_actions: Vec<CheckConstraintAction>,
}

#[derive(Debug, Clone)]
pub(super) struct AlterColumnTypePlan {
    pub column_index: usize,
    pub rewrite_expr: Expr,
    pub new_column: ColumnDesc,
}

#[derive(Debug, Clone)]
pub(super) struct AlterColumnDefaultPlan {
    pub column_name: String,
    pub default_expr_sql: Option<String>,
    pub default_sequence_oid: Option<u32>,
}

pub(super) struct AlterColumnExpressionPlan {
    pub column_name: String,
    pub default_expr_sql: Option<String>,
    pub generated: Option<crate::include::nodes::parsenodes::ColumnGeneratedKind>,
    pub noop: bool,
}

fn is_text_like_type(ty: SqlType) -> bool {
    !ty.is_array
        && matches!(
            ty.kind,
            SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar
        )
}

pub(crate) fn format_sql_type_name(sql_type: SqlType) -> String {
    if sql_type.is_range() {
        return builtin_range_name_for_sql_type(sql_type)
            .unwrap_or("range")
            .to_string();
    }
    if sql_type.is_multirange() {
        return crate::include::catalog::builtin_multirange_name_for_sql_type(sql_type)
            .unwrap_or("multirange")
            .to_string();
    }
    if let Some((precision, scale)) = sql_type.numeric_precision_scale() {
        return format!("numeric({precision},{scale})");
    }
    if sql_type.is_array {
        return format!("{}[]", format_sql_type_name(sql_type.element_type()));
    }
    if !sql_type.is_array
        && sql_type.type_oid != 0
        && let Some(name) = builtin_type_name_for_oid(sql_type.type_oid)
    {
        return name;
    }
    match sql_type.kind {
        SqlTypeKind::AnyElement => "anyelement",
        SqlTypeKind::AnyArray => "anyarray",
        SqlTypeKind::AnyRange => "anyrange",
        SqlTypeKind::AnyMultirange => "anymultirange",
        SqlTypeKind::AnyCompatible => "anycompatible",
        SqlTypeKind::AnyCompatibleArray => "anycompatiblearray",
        SqlTypeKind::AnyCompatibleRange => "anycompatiblerange",
        SqlTypeKind::AnyCompatibleMultirange => "anycompatiblemultirange",
        SqlTypeKind::AnyEnum => "anyenum",
        SqlTypeKind::Enum => return sql_type.type_oid.to_string(),
        SqlTypeKind::Record | SqlTypeKind::Composite => "record",
        SqlTypeKind::Shell => "shell",
        SqlTypeKind::Internal => "internal",
        SqlTypeKind::Cstring => "cstring",
        SqlTypeKind::Trigger => "trigger",
        SqlTypeKind::EventTrigger => "event_trigger",
        SqlTypeKind::Void => "void",
        SqlTypeKind::FdwHandler => "fdw_handler",
        SqlTypeKind::Int2 => "smallint",
        SqlTypeKind::Int2Vector => "int2vector",
        SqlTypeKind::Int4 => "integer",
        SqlTypeKind::Int8 => "bigint",
        SqlTypeKind::Name => "name",
        SqlTypeKind::Tid => "tid",
        SqlTypeKind::Xid => "xid",
        SqlTypeKind::Oid => "oid",
        SqlTypeKind::RegProc => "regproc",
        SqlTypeKind::RegClass => "regclass",
        SqlTypeKind::RegType => "regtype",
        SqlTypeKind::RegRole => "regrole",
        SqlTypeKind::RegNamespace => "regnamespace",
        SqlTypeKind::RegOper => "regoper",
        SqlTypeKind::RegOperator => "regoperator",
        SqlTypeKind::RegProcedure => "regprocedure",
        SqlTypeKind::RegCollation => "regcollation",
        SqlTypeKind::OidVector => "oidvector",
        SqlTypeKind::Bit => "bit",
        SqlTypeKind::VarBit => "bit varying",
        SqlTypeKind::Bytea => "bytea",
        SqlTypeKind::Uuid => "uuid",
        SqlTypeKind::Inet => "inet",
        SqlTypeKind::Cidr => "cidr",
        SqlTypeKind::MacAddr => "macaddr",
        SqlTypeKind::MacAddr8 => "macaddr8",
        SqlTypeKind::Float4 => "real",
        SqlTypeKind::Float8 => "double precision",
        SqlTypeKind::Money => "money",
        SqlTypeKind::Numeric => "numeric",
        SqlTypeKind::Json => "json",
        SqlTypeKind::Jsonb => "jsonb",
        SqlTypeKind::JsonPath => "jsonpath",
        SqlTypeKind::Xml => "xml",
        SqlTypeKind::Date => "date",
        SqlTypeKind::Time => "time without time zone",
        SqlTypeKind::TimeTz => "time with time zone",
        SqlTypeKind::TsVector => "tsvector",
        SqlTypeKind::TsQuery => "tsquery",
        SqlTypeKind::PgLsn => "pg_lsn",
        SqlTypeKind::RegConfig => "regconfig",
        SqlTypeKind::RegDictionary => "regdictionary",
        SqlTypeKind::Text => "text",
        SqlTypeKind::Bool => "boolean",
        SqlTypeKind::Point => "point",
        SqlTypeKind::Lseg => "lseg",
        SqlTypeKind::Path => "path",
        SqlTypeKind::Box => "box",
        SqlTypeKind::Polygon => "polygon",
        SqlTypeKind::Line => "line",
        SqlTypeKind::Circle => "circle",
        SqlTypeKind::Timestamp => "timestamp without time zone",
        SqlTypeKind::TimestampTz => "timestamp with time zone",
        SqlTypeKind::Interval => "interval",
        SqlTypeKind::PgNodeTree => "pg_node_tree",
        SqlTypeKind::InternalChar => "\"char\"",
        SqlTypeKind::Char => "character",
        SqlTypeKind::Varchar => "character varying",
        SqlTypeKind::Range
        | SqlTypeKind::Int4Range
        | SqlTypeKind::Int8Range
        | SqlTypeKind::NumericRange
        | SqlTypeKind::DateRange
        | SqlTypeKind::TimestampRange
        | SqlTypeKind::TimestampTzRange => unreachable!("range handled above"),
        SqlTypeKind::Multirange => unreachable!("multirange handled above"),
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::format_sql_type_name;
    use crate::backend::parser::SqlType;

    #[test]
    fn format_sql_type_name_includes_numeric_typmod() {
        assert_eq!(
            format_sql_type_name(SqlType::with_numeric_precision_scale(3, -6)),
            "numeric(3,-6)"
        );
        assert_eq!(
            format_sql_type_name(SqlType::with_numeric_precision_scale(12, 4)),
            "numeric(12,4)"
        );
        assert_eq!(
            format_sql_type_name(SqlType::array_of(SqlType::new(
                crate::backend::parser::SqlTypeKind::Int4
            ))),
            "integer[]"
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NormalizedStatisticsTarget {
    pub value: i16,
    pub warning: Option<&'static str>,
}

pub(super) fn normalize_statistics_target(
    statistics_target: i32,
) -> Result<NormalizedStatisticsTarget, ExecError> {
    if statistics_target == -1 {
        return Ok(NormalizedStatisticsTarget {
            value: -1,
            warning: None,
        });
    }
    if statistics_target < 0 {
        return Err(ExecError::DetailedError {
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
            ExecError::Parse(ParseError::InvalidInteger(statistics_target.to_string()))
        })?,
        warning: None,
    })
}

pub(super) fn automatic_alter_type_cast_allowed(
    catalog: &dyn CatalogLookup,
    from: SqlType,
    to: SqlType,
) -> bool {
    if from == to {
        return true;
    }
    if from.kind == to.kind && from.is_array == to.is_array {
        return true;
    }
    if !from.is_array
        && !to.is_array
        && matches!(from.kind, SqlTypeKind::Float4 | SqlTypeKind::Float8)
        && matches!(to.kind, SqlTypeKind::Numeric)
    {
        return true;
    }
    if !from.is_array
        && !to.is_array
        && matches!(
            (from.kind, to.kind),
            (SqlTypeKind::Timestamp, SqlTypeKind::TimestampTz)
                | (SqlTypeKind::TimestampTz, SqlTypeKind::Timestamp)
        )
    {
        return true;
    }
    if is_text_like_type(from) && is_text_like_type(to) {
        return true;
    }
    if !from.is_array
        && !to.is_array
        && matches!(from.kind, SqlTypeKind::Bool)
        && is_text_like_type(to)
    {
        return true;
    }
    let Some(source_oid) = catalog.type_oid_for_sql_type(from) else {
        return false;
    };
    let Some(target_oid) = catalog.type_oid_for_sql_type(to) else {
        return false;
    };
    catalog
        .cast_by_source_target(source_oid, target_oid)
        .is_some_and(|row| row.castcontext != 'e')
}

fn literal_default_cast_input(expr: &SqlExpr) -> Option<Value> {
    match expr {
        SqlExpr::Const(value) => Some(value.clone()),
        SqlExpr::IntegerLiteral(value) | SqlExpr::NumericLiteral(value) => {
            Some(Value::Text(value.clone().into()))
        }
        SqlExpr::UnaryPlus(inner) => literal_default_cast_input(inner),
        SqlExpr::Negate(inner) => match literal_default_cast_input(inner)? {
            Value::Text(text) => Some(Value::Text(format!("-{}", text.as_str()).into())),
            Value::Int16(value) => Some(Value::Int16(value.saturating_neg())),
            Value::Int32(value) => Some(Value::Int32(value.saturating_neg())),
            Value::Int64(value) => Some(Value::Int64(value.saturating_neg())),
            Value::Float64(value) => Some(Value::Float64(-value)),
            Value::Numeric(value) => Some(Value::Numeric(value.negate())),
            _ => None,
        },
        _ => None,
    }
}

pub(super) fn validate_alter_table_alter_column_default(
    catalog: &dyn CatalogLookup,
    desc: &RelationDesc,
    column_name: &str,
    default_expr: Option<&SqlExpr>,
    default_expr_sql: Option<&str>,
) -> Result<AlterColumnDefaultPlan, ExecError> {
    if is_system_column_name(column_name) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "user column name for ALTER COLUMN DEFAULT",
            actual: column_name.to_string(),
        }));
    }
    let column_index = desc
        .columns
        .iter()
        .enumerate()
        .find_map(|(index, column)| {
            (!column.dropped && column.name.eq_ignore_ascii_case(column_name)).then_some(index)
        })
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.to_string())))?;
    let current_column = &desc.columns[column_index];
    if current_column.generated.is_some() {
        return Err(ExecError::DetailedError {
            message: format!("column \"{}\" is a generated column", current_column.name),
            detail: None,
            hint: None,
            sqlstate: "428C9",
        });
    }
    if current_column.identity.is_some() {
        return Err(ExecError::DetailedError {
            message: format!(
                "column \"{}\" of relation is an identity column",
                current_column.name
            ),
            detail: None,
            hint: None,
            sqlstate: "428C9",
        });
    }

    let mut normalized_default_expr_sql = default_expr_sql.map(str::to_string);
    if let Some(expr) = default_expr {
        if matches!(expr, SqlExpr::Const(Value::Null)) {
            return Ok(AlterColumnDefaultPlan {
                column_name: current_column.name.clone(),
                default_expr_sql: normalized_default_expr_sql,
                default_sequence_oid: None,
            });
        }
        let (_bound, default_type) =
            bind_scalar_expr_in_scope(expr, &[], catalog).map_err(ExecError::Parse)?;
        if !automatic_alter_type_cast_allowed(catalog, default_type, current_column.sql_type) {
            if let Some(sql) = default_expr_sql
                && let Some(value) = literal_default_cast_input(expr)
            {
                crate::backend::executor::cast_value(value, current_column.sql_type)?;
                normalized_default_expr_sql = Some(format!(
                    "({sql})::{}",
                    format_sql_type_name(current_column.sql_type)
                ));
            } else if let Some(sql) = default_expr_sql
                && (current_column.sql_type.is_range() || current_column.sql_type.is_multirange())
                && let SqlExpr::Const(value) = expr
                && let Some(text) = value.as_text()
                && crate::backend::executor::cast_value(
                    Value::Text(text.into()),
                    current_column.sql_type,
                )
                .is_ok()
            {
                normalized_default_expr_sql = Some(format!(
                    "({sql})::{}",
                    format_sql_type_name(current_column.sql_type)
                ));
            } else {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "column \"{}\" is of type {} but default expression is of type {}",
                        current_column.name,
                        format_sql_type_name(current_column.sql_type),
                        format_sql_type_name(default_type),
                    ),
                    detail: None,
                    hint: Some("You will need to rewrite or cast the expression.".into()),
                    sqlstate: "42804",
                });
            }
        }
    }

    Ok(AlterColumnDefaultPlan {
        column_name: current_column.name.clone(),
        default_expr_sql: normalized_default_expr_sql,
        default_sequence_oid: default_expr_sql.and_then(|expr| {
            crate::pgrust::database::default_sequence_oid_from_default_expr_with_catalog(
                expr, catalog,
            )
        }),
    })
}

pub(super) fn validate_alter_table_alter_column_expression(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    namespace_oid: u32,
    desc: &RelationDesc,
    column_name: &str,
    action: &AlterColumnExpressionAction,
) -> Result<AlterColumnExpressionPlan, ExecError> {
    if is_system_column_name(column_name) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "user column name for ALTER COLUMN EXPRESSION",
            actual: column_name.to_string(),
        }));
    }
    let column_index = desc
        .columns
        .iter()
        .enumerate()
        .find_map(|(index, column)| {
            (!column.dropped && column.name.eq_ignore_ascii_case(column_name)).then_some(index)
        })
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.to_string())))?;
    let current_column = &desc.columns[column_index];

    match action {
        AlterColumnExpressionAction::Set { expr_sql, .. } => {
            let Some(kind) = current_column.generated else {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "column \"{}\" is not a generated column",
                        current_column.name
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "428C9",
                });
            };
            if kind == ColumnGeneratedKind::Virtual
                && relation_is_part_of_publication(catalog, relation_oid, namespace_oid)
            {
                return Err(ExecError::DetailedError {
                    message: "ALTER TABLE / SET EXPRESSION is not supported for virtual generated columns in tables that are part of a publication".into(),
                    detail: Some(format!(
                        "Column \"{}\" of relation \"{}\" is a virtual generated column.",
                        current_column.name,
                        relation_name_for_oid(catalog, relation_oid)
                    )),
                    hint: None,
                    sqlstate: "0A000",
                });
            }
            let mut desc_with_new_expression = desc.clone();
            let column = &mut desc_with_new_expression.columns[column_index];
            column.default_expr = Some(expr_sql.clone());
            column.default_sequence_oid = None;
            column.generated = Some(kind);
            crate::backend::parser::validate_generated_columns(&desc_with_new_expression, catalog)
                .map_err(ExecError::Parse)?;
            Ok(AlterColumnExpressionPlan {
                column_name: current_column.name.clone(),
                default_expr_sql: Some(expr_sql.clone()),
                generated: Some(kind),
                noop: false,
            })
        }
        AlterColumnExpressionAction::Drop { missing_ok } => {
            if current_column.generated.is_none() {
                if *missing_ok {
                    return Ok(AlterColumnExpressionPlan {
                        column_name: current_column.name.clone(),
                        default_expr_sql: None,
                        generated: None,
                        noop: true,
                    });
                }
                return Err(ExecError::DetailedError {
                    message: format!(
                        "column \"{}\" is not a generated column",
                        current_column.name
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                });
            }
            Ok(AlterColumnExpressionPlan {
                column_name: current_column.name.clone(),
                default_expr_sql: None,
                generated: None,
                noop: false,
            })
        }
    }
}

fn relation_is_part_of_publication(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    namespace_oid: u32,
) -> bool {
    let publications = catalog
        .publication_rows()
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();
    if catalog
        .publication_rel_rows()
        .into_iter()
        .any(|row| row.prrelid == relation_oid && !row.prexcept)
    {
        return true;
    }

    let excluded_publication_oids = catalog
        .publication_rel_rows()
        .into_iter()
        .filter(|row| row.prrelid == relation_oid && row.prexcept)
        .map(|row| row.prpubid)
        .collect::<BTreeSet<_>>();
    if publications.values().any(|publication| {
        publication.puballtables && !excluded_publication_oids.contains(&publication.oid)
    }) {
        return true;
    }

    catalog.publication_namespace_rows().into_iter().any(|row| {
        row.pnnspid == namespace_oid
            && publications.contains_key(&row.pnpubid)
            && !excluded_publication_oids.contains(&row.pnpubid)
    })
}

pub(super) fn validate_alter_table_alter_column_options(
    desc: &RelationDesc,
    column_name: &str,
) -> Result<String, ExecError> {
    if is_system_column_name(column_name) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "user column name for ALTER COLUMN SET/RESET options",
            actual: column_name.to_string(),
        }));
    }
    let column = desc
        .columns
        .iter()
        .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.to_string())))?;
    Ok(column.name.clone())
}

pub(super) fn validate_alter_table_alter_column_storage(
    desc: &RelationDesc,
    column_name: &str,
    storage: AttributeStorage,
) -> Result<(String, AttributeStorage), ExecError> {
    if is_system_column_name(column_name) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "user column name for ALTER COLUMN SET STORAGE",
            actual: column_name.to_string(),
        }));
    }
    let column = desc
        .columns
        .iter()
        .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.to_string())))?;

    let type_default = column_desc("attstorage_check", column.sql_type, column.storage.nullable)
        .storage
        .attstorage;
    if storage != AttributeStorage::Plain && type_default == AttributeStorage::Plain {
        return Err(ExecError::DetailedError {
            message: format!(
                "column data type {} can only have storage PLAIN",
                format_sql_type_name(column.sql_type)
            ),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }

    Ok((column.name.clone(), storage))
}

pub(super) fn validate_alter_table_alter_column_compression(
    desc: &RelationDesc,
    column_name: &str,
    compression: AttributeCompression,
) -> Result<(String, AttributeCompression), ExecError> {
    if is_system_column_name(column_name) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "user column name for ALTER COLUMN SET COMPRESSION",
            actual: column_name.to_string(),
        }));
    }
    let column = desc
        .columns
        .iter()
        .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.to_string())))?;

    ensure_attribute_compression_supported(compression)?;

    let type_default = column_desc(
        "attcompression_check",
        column.sql_type,
        column.storage.nullable,
    )
    .storage
    .attstorage;
    if compression != AttributeCompression::Default && type_default == AttributeStorage::Plain {
        return Err(ExecError::DetailedError {
            message: format!(
                "column data type {} does not support compression",
                format_sql_type_name(column.sql_type)
            ),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }

    Ok((column.name.clone(), compression))
}

pub(super) fn validate_alter_table_alter_column_statistics(
    desc: &RelationDesc,
    column_name: &str,
    statistics_target: i32,
) -> Result<(String, NormalizedStatisticsTarget), ExecError> {
    if is_system_column_name(column_name) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "user column name for ALTER COLUMN SET STATISTICS",
            actual: column_name.to_string(),
        }));
    }
    let column = desc
        .columns
        .iter()
        .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.to_string())))?;
    Ok((
        column.name.clone(),
        normalize_statistics_target(statistics_target)?,
    ))
}

pub(super) fn validate_alter_index_alter_column_statistics(
    entry: &RelCacheEntry,
    index_name: &str,
    column_number: i16,
    statistics_target: i32,
) -> Result<(String, NormalizedStatisticsTarget), ExecError> {
    let index_meta = entry
        .index
        .as_ref()
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("relation \"{index_name}\" is not an index"),
            detail: None,
            hint: None,
            sqlstate: "42809",
        })?;
    if column_number < 1 {
        return Err(ExecError::DetailedError {
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
        .ok_or_else(|| ExecError::DetailedError {
            message: format!(
                "column number {} of relation \"{}\" does not exist",
                column_number, index_name
            ),
            detail: None,
            hint: None,
            sqlstate: "42703",
        })?;
    if column_number > index_meta.indnkeyatts {
        return Err(ExecError::DetailedError {
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
        return Err(ExecError::DetailedError {
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

fn alter_column_type_error(
    message: String,
    hint: Option<String>,
) -> Result<AlterColumnTypePlan, ExecError> {
    Err(ExecError::DetailedError {
        message,
        detail: None,
        hint,
        sqlstate: "42804",
    })
}

pub(super) fn validate_alter_table_alter_column_type(
    catalog: &dyn CatalogLookup,
    desc: &RelationDesc,
    column_name: &str,
    ty: &RawTypeName,
    collation: Option<&str>,
    using_expr: Option<&SqlExpr>,
) -> Result<AlterColumnTypePlan, ExecError> {
    if is_system_column_name(column_name) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "user column name for ALTER COLUMN TYPE",
            actual: format!("cannot alter system column \"{column_name}\""),
        }));
    }

    let column_index = desc
        .columns
        .iter()
        .enumerate()
        .find_map(|(index, column)| {
            (!column.dropped && column.name.eq_ignore_ascii_case(column_name)).then_some(index)
        })
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.to_string())))?;
    let current_column = &desc.columns[column_index];
    if current_column.generated.is_some() {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "ALTER TABLE ALTER COLUMN TYPE on generated columns".into(),
        )));
    }
    reject_column_referenced_by_generated_columns(catalog, desc, column_index, "alter type of")?;
    let target_sql_type = match ty {
        RawTypeName::Builtin(sql_type) => *sql_type,
        RawTypeName::Serial(kind) => {
            return Err(ExecError::Parse(ParseError::FeatureNotSupported(format!(
                "{} is only allowed in CREATE TABLE / ALTER TABLE ADD COLUMN",
                match kind {
                    crate::backend::parser::SerialKind::Small => "smallserial",
                    crate::backend::parser::SerialKind::Regular => "serial",
                    crate::backend::parser::SerialKind::Big => "bigserial",
                }
            ))));
        }
        RawTypeName::Record => {
            return Err(ExecError::Parse(ParseError::UnsupportedType(
                "record".into(),
            )));
        }
        RawTypeName::Named { .. } => {
            resolve_raw_type_name(ty, catalog).map_err(ExecError::Parse)?
        }
    };

    if let Some(default_sql) = current_column.default_expr.as_deref() {
        let default_expr =
            crate::backend::parser::parse_expr(default_sql).map_err(ExecError::Parse)?;
        let (_bound_default, default_type) =
            bind_scalar_expr_in_scope(&default_expr, &[], catalog).map_err(ExecError::Parse)?;
        if !automatic_alter_type_cast_allowed(catalog, default_type, target_sql_type) {
            return alter_column_type_error(
                format!(
                    "default for column \"{}\" cannot be cast automatically to type {}",
                    current_column.name,
                    format_sql_type_name(target_sql_type),
                ),
                None,
            );
        }
    }

    let scope_columns = desc
        .columns
        .iter()
        .map(|column| (column.name.clone(), column.sql_type))
        .collect::<Vec<_>>();
    let (rewrite_expr, rewrite_type) = match using_expr {
        Some(expr) => {
            let (bound, from_type) = bind_scalar_expr_in_scope(expr, &scope_columns, catalog)
                .map_err(ExecError::Parse)?;
            (bound, from_type)
        }
        None => (
            Expr::Var(Var {
                varno: 1,
                varattno: user_attrno(column_index),
                varlevelsup: 0,
                vartype: current_column.sql_type,
            }),
            current_column.sql_type,
        ),
    };

    if !automatic_alter_type_cast_allowed(catalog, rewrite_type, target_sql_type) {
        if using_expr.is_some() {
            return alter_column_type_error(
                format!(
                    "result of USING clause for column \"{}\" cannot be cast automatically to type {}",
                    current_column.name,
                    format_sql_type_name(target_sql_type),
                ),
                Some("You might need to add an explicit cast.".into()),
            );
        }
        return alter_column_type_error(
            format!(
                "column \"{}\" cannot be cast automatically to type {}",
                current_column.name,
                format_sql_type_name(target_sql_type),
            ),
            Some(format!(
                "You might need to specify \"USING {}::{}\".",
                current_column.name,
                format_sql_type_name(target_sql_type),
            )),
        );
    }

    let mut new_column = column_desc(
        current_column.name.clone(),
        target_sql_type,
        current_column.storage.nullable,
    );
    if let Some(collation) = collation {
        if !is_collatable_type(target_sql_type) {
            return Err(ExecError::DetailedError {
                message: format!(
                    "collations are not supported by type {}",
                    sql_type_name(target_sql_type)
                ),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        }
        new_column.collation_oid =
            resolve_collation_oid(collation, catalog).map_err(ExecError::Parse)?;
    }
    new_column.storage.attstorage = current_column.storage.attstorage;
    new_column.storage.attcompression = current_column.storage.attcompression;
    new_column.attstattarget = current_column.attstattarget;
    new_column.not_null_constraint_oid = current_column.not_null_constraint_oid;
    new_column.not_null_constraint_name = current_column.not_null_constraint_name.clone();
    new_column.not_null_constraint_validated = current_column.not_null_constraint_validated;
    new_column.not_null_primary_key_owned = current_column.not_null_primary_key_owned;
    new_column.attrdef_oid = current_column.attrdef_oid;
    new_column.default_expr = current_column.default_expr.clone();
    new_column.default_sequence_oid = current_column.default_sequence_oid;
    new_column.identity = current_column.identity;
    new_column.fdw_options = current_column.fdw_options.clone();
    new_column.generated = current_column.generated;
    new_column.missing_default_value = if current_column.default_sequence_oid.is_some() {
        None
    } else {
        current_column
            .default_expr
            .as_deref()
            .and_then(|sql| derive_literal_default_value(sql, target_sql_type).ok())
    };

    Ok(AlterColumnTypePlan {
        column_index,
        rewrite_expr: if rewrite_type == target_sql_type {
            rewrite_expr
        } else {
            Expr::Cast(Box::new(rewrite_expr), target_sql_type)
        },
        new_column,
    })
}

pub(super) fn map_catalog_error(err: CatalogError) -> ExecError {
    match err {
        CatalogError::TableAlreadyExists(name) => {
            ExecError::Parse(ParseError::TableAlreadyExists(name))
        }
        CatalogError::TypeAlreadyExists(name) => ExecError::DetailedError {
            message: format!("type \"{name}\" already exists"),
            detail: None,
            hint: None,
            sqlstate: "42710",
        },
        CatalogError::UnknownTable(name) => ExecError::Parse(ParseError::TableDoesNotExist(name)),
        CatalogError::UnknownColumn(name) => ExecError::Parse(ParseError::UnknownColumn(name)),
        CatalogError::UnknownType(name) => ExecError::Parse(ParseError::UnsupportedType(name)),
        CatalogError::UniqueViolation(constraint) => ExecError::UniqueViolation {
            constraint,
            detail: None,
        },
        CatalogError::Interrupted(reason) => ExecError::Interrupted(reason),
        CatalogError::Io(message) if message.starts_with("index row size ") => {
            ExecError::DetailedError {
                message,
                detail: None,
                hint: Some("Values larger than 1/3 of a buffer page cannot be indexed.".into()),
                sqlstate: "54000",
            }
        }
        CatalogError::Io(_) | CatalogError::Corrupt(_) => {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "valid catalog state",
                actual: "catalog error".into(),
            })
        }
    }
}
