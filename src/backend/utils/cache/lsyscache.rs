use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use crate::ClientId;
use crate::backend::access::transam::xact::{CommandId, TransactionId};
use crate::backend::catalog::pg_constraint::derived_pg_constraint_rows;
use crate::backend::parser::{BoundRelation, CatalogLookup, DomainLookup};
use crate::backend::rewrite::{format_view_definition, relation_row_security_is_enabled_for_user};
use crate::backend::storage::smgr::{BLCKSZ, ForkNumber, StorageManager};
use crate::backend::utils::cache::catcache::normalize_catalog_name;
use crate::backend::utils::cache::relcache::RelCacheEntry;
use crate::backend::utils::cache::syscache::{
    RelationIdGetRelation, SearchSysCache1, SearchSysCache2, SearchSysCacheList1,
    SearchSysCacheList2, SearchSysCacheList3, SysCacheId, SysCacheTuple, backend_catcache,
    ensure_am_rows, ensure_amop_rows, ensure_amproc_rows, ensure_attribute_rows, ensure_class_rows,
    ensure_collation_rows, ensure_constraint_rows, ensure_index_rows, ensure_inherit_rows,
    ensure_namespace_rows, ensure_opclass_rows, ensure_proc_rows, ensure_rewrite_rows,
    ensure_statistic_rows, ensure_type_rows, with_backend_catcache,
};
use crate::backend::utils::cache::system_views::{
    build_pg_indexes_rows, build_pg_locks_rows, build_pg_matviews_rows, build_pg_policies_rows,
    build_pg_rules_rows, build_pg_stat_all_tables_rows, build_pg_stat_archiver_rows,
    build_pg_stat_bgwriter_rows, build_pg_stat_checkpointer_rows, build_pg_stat_database_rows,
    build_pg_stat_io_rows, build_pg_stat_recovery_prefetch_rows, build_pg_stat_slru_rows,
    build_pg_stat_user_functions_rows, build_pg_stat_user_tables_rows, build_pg_stat_wal_rows,
    build_pg_statio_user_tables_rows, build_pg_stats_rows, build_pg_tables_rows,
    build_pg_views_rows_with_definition_formatter,
};
use crate::include::access::brin_page::{
    BRIN_PAGE_CONTENT_OFFSET, BrinMetaPageData, brin_is_meta_page,
};
use crate::include::catalog::{
    CONSTRAINT_FOREIGN, CONSTRAINT_NOTNULL, PG_CLASS_RELATION_OID, PG_CONSTRAINT_RELATION_OID,
    PgAggregateRow, PgAmRow, PgAmopRow, PgAmprocRow, PgAuthIdRow, PgAuthMembersRow, PgCastRow,
    PgClassRow, PgCollationRow, PgConstraintRow, PgConversionRow, PgDatabaseRow, PgDependRow,
    PgEnumRow, PgEventTriggerRow, PgIndexRow, PgInheritsRow, PgLanguageRow, PgNamespaceRow,
    PgOpclassRow, PgOperatorRow, PgOpfamilyRow, PgProcRow, PgPublicationNamespaceRow,
    PgPublicationRelRow, PgPublicationRow, PgRewriteRow, PgStatisticExtDataRow, PgStatisticExtRow,
    PgStatisticRow, PgTriggerRow, PgTsConfigMapRow, PgTsConfigRow, PgTsDictRow, PgTsParserRow,
    PgTsTemplateRow, PgTypeRow,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::{SqlType, SqlTypeKind};
use crate::include::nodes::pathnodes::PlannerIndexExprCacheEntry;
use crate::pgrust::database::{
    Database, DatabaseStatsStore, TempNamespace, default_pg_stat_io_keys,
};

fn oid_key(oid: u32) -> Value {
    Value::Int64(i64::from(oid))
}

fn catalog_name_key(name: &str) -> Value {
    Value::Text(normalize_catalog_name(name).to_ascii_lowercase().into())
}

fn catalog_name_lookup_keys(name: &str) -> Vec<Value> {
    let normalized = normalize_catalog_name(name);
    let mut names = vec![normalized.to_string()];
    let folded = normalized.to_ascii_lowercase();
    if folded != normalized {
        names.push(folded);
    }
    names
        .into_iter()
        .map(|name| Value::Text(name.into()))
        .collect()
}

fn namespace_row_by_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    name: &str,
) -> Option<crate::include::catalog::PgNamespaceRow> {
    catalog_name_lookup_keys(name).into_iter().find_map(|key| {
        select_namespace_row(
            SearchSysCache1(db, client_id, txn_ctx, SysCacheId::NAMESPACENAME, key).ok()?,
        )
    })
}

fn namespace_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<crate::include::catalog::PgNamespaceRow> {
    select_namespace_row(
        SearchSysCache1(
            db,
            client_id,
            txn_ctx,
            SysCacheId::NAMESPACEOID,
            oid_key(oid),
        )
        .ok()?,
    )
}

fn select_namespace_row(
    tuples: Vec<SysCacheTuple>,
) -> Option<crate::include::catalog::PgNamespaceRow> {
    tuples.into_iter().fold(None, |selected, tuple| {
        let SysCacheTuple::Namespace(row) = tuple else {
            return selected;
        };
        match selected {
            Some(existing) if existing.nspacl.is_some() && row.nspacl.is_none() => Some(existing),
            _ => Some(row),
        }
    })
}

fn class_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<crate::include::catalog::PgClassRow> {
    SearchSysCache1(db, client_id, txn_ctx, SysCacheId::RELOID, oid_key(oid))
        .ok()?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Class(row) => Some(row),
            _ => None,
        })
}

fn class_row_by_name_namespace(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relname: &str,
    namespace_oid: u32,
) -> Option<crate::include::catalog::PgClassRow> {
    catalog_name_lookup_keys(relname)
        .into_iter()
        .find_map(|key| {
            SearchSysCache2(
                db,
                client_id,
                txn_ctx,
                SysCacheId::RELNAMENSP,
                key,
                oid_key(namespace_oid),
            )
            .ok()?
            .into_iter()
            .find_map(|tuple| match tuple {
                SysCacheTuple::Class(row)
                    if !db.other_session_temp_namespace_oid(client_id, row.relnamespace) =>
                {
                    Some(row)
                }
                _ => None,
            })
        })
}

fn attribute_rows_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<crate::include::catalog::PgAttributeRow> {
    let mut rows = SearchSysCacheList1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::ATTNUM,
        oid_key(relation_oid),
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Attribute(row) => Some(row),
                _ => None,
            })
            .collect::<Vec<_>>()
    })
    .unwrap_or_default();
    rows.sort_by_key(|row| row.attnum);
    rows
}

fn inheritance_parent_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<PgInheritsRow> {
    let mut rows = SearchSysCacheList1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::INHRELIDSEQNO,
        oid_key(relation_oid),
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Inherits(row) => Some(row),
                _ => None,
            })
            .collect::<Vec<_>>()
    })
    .unwrap_or_default();
    rows.sort_by_key(|row| (row.inhseqno, row.inhparent));
    rows
}

fn inheritance_child_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<PgInheritsRow> {
    let mut rows = SearchSysCacheList1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::INHPARENT,
        oid_key(relation_oid),
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Inherits(row) => Some(row),
                _ => None,
            })
            .collect::<Vec<_>>()
    })
    .unwrap_or_default();
    rows.sort_by_key(|row| (row.inhseqno, row.inhrelid));
    rows
}

fn partitioned_table_row_by_relid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Option<crate::include::catalog::PgPartitionedTableRow> {
    SearchSysCache1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::PARTRELID,
        oid_key(relation_oid),
    )
    .ok()?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::PartitionedTable(row) => Some(row),
        _ => None,
    })
}

fn constraint_rows_for_relation_syscache(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<PgConstraintRow> {
    SearchSysCacheList1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::CONSTRAINTRELID,
        oid_key(relation_oid),
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Constraint(row) => Some(row),
                _ => None,
            })
            .collect()
    })
    .unwrap_or_default()
}

fn constraint_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<PgConstraintRow> {
    SearchSysCache1(db, client_id, txn_ctx, SysCacheId::CONSTROID, oid_key(oid))
        .ok()?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Constraint(row) => Some(row),
            _ => None,
        })
}

fn constraint_rows_referencing_class_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    referenced_oid: u32,
) -> Vec<PgConstraintRow> {
    let mut seen = BTreeSet::new();
    let mut rows = SearchSysCacheList2(
        db,
        client_id,
        txn_ctx,
        SysCacheId::DEPENDREFERENCE,
        oid_key(PG_CLASS_RELATION_OID),
        oid_key(referenced_oid),
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Depend(row)
                    if row.classid == PG_CONSTRAINT_RELATION_OID && seen.insert(row.objid) =>
                {
                    constraint_row_by_oid(db, client_id, txn_ctx, row.objid)
                }
                _ => None,
            })
            .collect::<Vec<_>>()
    })
    .unwrap_or_default();
    crate::backend::catalog::pg_constraint::sort_pg_constraint_rows(&mut rows);
    rows
}

fn depend_rows_referencing(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    refclassid: u32,
    refobjid: u32,
    refobjsubid: Option<i32>,
) -> Vec<PgDependRow> {
    let tuples = match refobjsubid {
        Some(objsubid) => SearchSysCacheList3(
            db,
            client_id,
            txn_ctx,
            SysCacheId::DEPENDREFERENCE,
            oid_key(refclassid),
            oid_key(refobjid),
            Value::Int32(objsubid),
        ),
        None => SearchSysCacheList2(
            db,
            client_id,
            txn_ctx,
            SysCacheId::DEPENDREFERENCE,
            oid_key(refclassid),
            oid_key(refobjid),
        ),
    };

    let mut rows = tuples
        .unwrap_or_default()
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::Depend(row)
                if row.refclassid == refclassid
                    && row.refobjid == refobjid
                    && refobjsubid.is_none_or(|objsubid| row.refobjsubid == objsubid) =>
            {
                Some(row)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    crate::backend::catalog::pg_depend::sort_pg_depend_rows(&mut rows);
    rows
}

fn constraint_rows_for_index(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    index_oid: u32,
) -> Vec<PgConstraintRow> {
    let mut rows = constraint_rows_referencing_class_oid(db, client_id, txn_ctx, index_oid)
        .into_iter()
        .filter(|row| row.conindid == index_oid)
        .collect::<Vec<_>>();
    crate::backend::catalog::pg_constraint::sort_pg_constraint_rows(&mut rows);
    rows
}

fn foreign_key_constraint_rows_referencing_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<PgConstraintRow> {
    let mut rows = constraint_rows_referencing_class_oid(db, client_id, txn_ctx, relation_oid)
        .into_iter()
        .filter(|row| row.contype == CONSTRAINT_FOREIGN && row.confrelid == relation_oid)
        .collect::<Vec<_>>();
    crate::backend::catalog::pg_constraint::sort_pg_constraint_rows(&mut rows);
    rows
}

fn foreign_key_constraint_rows_referencing_index(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    index_oid: u32,
) -> Vec<PgConstraintRow> {
    let mut rows = constraint_rows_for_index(db, client_id, txn_ctx, index_oid)
        .into_iter()
        .filter(|row| row.contype == CONSTRAINT_FOREIGN)
        .collect::<Vec<_>>();
    crate::backend::catalog::pg_constraint::sort_pg_constraint_rows(&mut rows);
    rows
}

fn attrdef_rows_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<crate::include::catalog::PgAttrdefRow> {
    let mut rows = SearchSysCacheList1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::ATTRDEFAULT,
        oid_key(relation_oid),
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Attrdef(row) => Some(row),
                _ => None,
            })
            .collect::<Vec<_>>()
    })
    .unwrap_or_default();
    rows.sort_by_key(|row| row.adnum);
    rows
}

fn trigger_rows_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<PgTriggerRow> {
    SearchSysCacheList1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::TRIGGERRELIDNAME,
        oid_key(relation_oid),
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Trigger(row) => Some(row),
                _ => None,
            })
            .collect()
    })
    .unwrap_or_default()
}

fn policy_rows_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<crate::include::catalog::PgPolicyRow> {
    SearchSysCacheList1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::POLICYPOLRELIDPOLNAME,
        oid_key(relation_oid),
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Policy(row) => Some(row),
                _ => None,
            })
            .collect()
    })
    .unwrap_or_default()
}

fn type_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<PgTypeRow> {
    SearchSysCache1(db, client_id, txn_ctx, SysCacheId::TYPEOID, oid_key(oid))
        .ok()?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Type(row) => Some(row),
            _ => None,
        })
}

fn type_row_by_name_namespace(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    name: &str,
    namespace_oid: u32,
) -> Option<PgTypeRow> {
    let normalized = normalize_catalog_name(name);
    SearchSysCache2(
        db,
        client_id,
        txn_ctx,
        SysCacheId::TYPENAMENSP,
        Value::Text(normalized.to_ascii_lowercase().into()),
        oid_key(namespace_oid),
    )
    .ok()?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::Type(row) => Some(row),
        _ => None,
    })
    .or_else(|| {
        crate::include::catalog::builtin_type_rows()
            .into_iter()
            .chain(crate::include::catalog::bootstrap_composite_type_rows())
            .find(|row| {
                row.typnamespace == namespace_oid
                    && row.typname.eq_ignore_ascii_case(normalized.as_ref())
            })
    })
    .filter(|row| !db.other_session_temp_namespace_oid(client_id, row.typnamespace))
}

pub(crate) fn dynamic_type_rows_for_search_path(
    db: &Database,
    search_path: &[String],
) -> Vec<PgTypeRow> {
    let mut rows = db.domain_type_rows_for_search_path(search_path);
    rows.extend(db.enum_type_rows_for_search_path(search_path));
    rows.extend(db.range_type_rows_for_search_path(search_path));
    rows
}

fn visible_domain_by_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    search_path: &[String],
    name: &str,
) -> Option<DomainLookup> {
    let type_row = visible_type_row_by_name(db, client_id, txn_ctx, search_path, name)?;
    let domains = db.domains.read();
    let domain = domains.values().find(|domain| domain.oid == type_row.oid)?;
    Some(DomainLookup {
        oid: domain.oid,
        array_oid: domain.array_oid,
        name: domain.name.clone(),
        sql_type: domain.sql_type,
        default: domain.default.clone(),
        check: domain.check.clone(),
        not_null: domain.not_null,
        constraints: domain
            .constraints
            .iter()
            .map(
                |constraint| crate::backend::parser::DomainConstraintLookup {
                    name: constraint.name.clone(),
                    kind: match constraint.kind {
                        crate::pgrust::database::DomainConstraintKind::Check => {
                            crate::backend::parser::DomainConstraintLookupKind::Check
                        }
                        crate::pgrust::database::DomainConstraintKind::NotNull => {
                            crate::backend::parser::DomainConstraintLookupKind::NotNull
                        }
                    },
                    expr: constraint.expr.clone(),
                    validated: constraint.validated,
                    enforced: constraint.enforced,
                },
            )
            .collect(),
    })
}

fn range_proc_type_rows(db: &Database, search_path: &[String]) -> Vec<PgTypeRow> {
    let mut rows = crate::include::catalog::builtin_type_rows();
    rows.extend(db.range_type_rows_for_search_path(search_path));
    rows
}

fn is_visible_range_proc_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    search_path: &[String],
    name: &str,
) -> bool {
    crate::include::catalog::is_synthetic_range_proc_name(name)
        || visible_type_row_by_name(db, client_id, txn_ctx, search_path, name)
            .is_some_and(|row| row.sql_type.is_range() || row.sql_type.is_multirange())
}

fn dynamic_type_row_by_oid(db: &Database, search_path: &[String], oid: u32) -> Option<PgTypeRow> {
    dynamic_type_rows_for_search_path(db, search_path)
        .into_iter()
        .find(|row| row.oid == oid)
}

fn dynamic_type_row_by_name(
    db: &Database,
    search_path: &[String],
    name: &str,
) -> Option<PgTypeRow> {
    let normalized = crate::backend::parser::analyze::normalize_catalog_lookup_name(name);
    dynamic_type_rows_for_search_path(db, search_path)
        .into_iter()
        .find(|row| row.typname.eq_ignore_ascii_case(normalized))
}

fn dynamic_type_row_by_name_namespace(
    db: &Database,
    search_path: &[String],
    name: &str,
    namespace_oid: u32,
) -> Option<PgTypeRow> {
    let normalized = crate::backend::parser::analyze::normalize_catalog_lookup_name(name);
    dynamic_type_rows_for_search_path(db, search_path)
        .into_iter()
        .find(|row| {
            row.typnamespace == namespace_oid && row.typname.eq_ignore_ascii_case(normalized)
        })
}

fn visible_type_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    search_path: &[String],
    oid: u32,
) -> Option<PgTypeRow> {
    type_row_by_oid(db, client_id, txn_ctx, oid)
        .filter(|row| !db.other_session_temp_namespace_oid(client_id, row.typnamespace))
        .or_else(|| dynamic_type_row_by_oid(db, search_path, oid))
}

fn visible_type_row_by_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    search_path: &[String],
    name: &str,
) -> Option<PgTypeRow> {
    let lowered = name.to_ascii_lowercase();
    if let Some((schema, typname)) = lowered.split_once('.') {
        let namespace_oid = namespace_row_by_name(db, client_id, txn_ctx, schema)?.oid;
        return type_row_by_name_namespace(db, client_id, txn_ctx, typname, namespace_oid).or_else(
            || dynamic_type_row_by_name_namespace(db, search_path, typname, namespace_oid),
        );
    }

    let normalized = crate::backend::parser::analyze::normalize_catalog_lookup_name(name);
    let pg_catalog_in_search_path = search_path
        .iter()
        .any(|schema| schema.eq_ignore_ascii_case("pg_catalog"));
    if !pg_catalog_in_search_path
        && let Some(namespace) = namespace_row_by_name(db, client_id, txn_ctx, "pg_catalog")
        && let Some(row) =
            type_row_by_name_namespace(db, client_id, txn_ctx, normalized, namespace.oid)
    {
        return Some(row);
    }
    for schema in search_path {
        let Some(namespace) = namespace_row_by_name(db, client_id, txn_ctx, schema) else {
            continue;
        };
        if let Some(row) =
            type_row_by_name_namespace(db, client_id, txn_ctx, normalized, namespace.oid)
        {
            return Some(row);
        }
    }
    dynamic_type_row_by_name(db, search_path, normalized)
}

fn visible_type_row_for_sql_type(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    search_path: &[String],
    sql_type: SqlType,
) -> Option<PgTypeRow> {
    if !sql_type.is_array && sql_type.type_oid != 0 {
        return visible_type_row_by_oid(db, client_id, txn_ctx, search_path, sql_type.type_oid);
    }
    crate::include::catalog::builtin_type_rows()
        .into_iter()
        .find(|row| row.sql_type == sql_type)
        .or_else(|| {
            dynamic_type_rows_for_search_path(db, search_path)
                .into_iter()
                .find(|row| row.sql_type == sql_type)
        })
}

fn visible_type_oid_for_sql_type(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    search_path: &[String],
    sql_type: SqlType,
) -> Option<u32> {
    if sql_type.type_oid != 0
        && visible_type_row_by_oid(db, client_id, txn_ctx, search_path, sql_type.type_oid)
            .is_some_and(|row| row.sql_type == sql_type)
    {
        return Some(sql_type.type_oid);
    }
    if sql_type.is_array {
        let element = sql_type.element_type();
        let element_oid = if element.type_oid != 0 {
            element.type_oid
        } else {
            visible_type_row_for_sql_type(db, client_id, txn_ctx, search_path, element)?.oid
        };
        return visible_type_row_by_oid(db, client_id, txn_ctx, search_path, element_oid)
            .and_then(|row| (row.typarray != 0).then_some(row.typarray));
    }
    if let Some(range_type) = crate::include::catalog::range_type_ref_for_sql_type(sql_type) {
        return Some(range_type.type_oid());
    }
    if let Some(multirange_type) =
        crate::include::catalog::multirange_type_ref_for_sql_type(sql_type)
    {
        return Some(multirange_type.type_oid());
    }
    if sql_type.type_oid != 0 {
        return Some(sql_type.type_oid);
    }
    if let Some(row) = visible_type_row_for_sql_type(db, client_id, txn_ctx, search_path, sql_type)
    {
        return Some(row.oid);
    }
    crate::include::catalog::builtin_type_rows()
        .into_iter()
        .chain(dynamic_type_rows_for_search_path(db, search_path))
        .find(|row| {
            row.oid != crate::include::catalog::UNKNOWN_TYPE_OID
                && row.sql_type.kind == sql_type.kind
                && row.sql_type.is_array == sql_type.is_array
        })
        .map(|row| row.oid)
}

fn visible_catcache(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Option<crate::backend::utils::cache::catcache::CatCache> {
    backend_catcache(db, client_id, txn_ctx).ok()
}

fn proc_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<PgProcRow> {
    SearchSysCache1(db, client_id, txn_ctx, SysCacheId::PROCOID, oid_key(oid))
        .ok()?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Proc(row) => Some(row),
            _ => None,
        })
}

fn proc_rows_by_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    name: &str,
) -> Vec<PgProcRow> {
    let normalized = crate::backend::parser::analyze::normalize_catalog_lookup_name(name);
    let mut rows = SearchSysCacheList1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::PROCNAMEARGSNSP,
        catalog_name_key(normalized),
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Proc(row) => Some(row),
                _ => None,
            })
            .collect::<Vec<_>>()
    })
    .unwrap_or_default();
    crate::backend::catalog::pg_proc::sort_pg_proc_rows(&mut rows);
    rows
}

fn operator_row_by_name_left_right(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    name: &str,
    left_type_oid: u32,
    right_type_oid: u32,
) -> Option<PgOperatorRow> {
    let normalized = crate::backend::parser::analyze::normalize_catalog_lookup_name(name);
    let mut rows = SearchSysCacheList3(
        db,
        client_id,
        txn_ctx,
        SysCacheId::OPERNAMENSP,
        catalog_name_key(normalized),
        oid_key(left_type_oid),
        oid_key(right_type_oid),
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Operator(row) => Some(row),
                _ => None,
            })
            .collect::<Vec<_>>()
    })
    .unwrap_or_default();
    crate::backend::catalog::pg_operator::sort_pg_operator_rows(&mut rows);
    rows.into_iter().find(|row| {
        row.oprname.eq_ignore_ascii_case(normalized)
            && row.oprleft == left_type_oid
            && row.oprright == right_type_oid
    })
}

fn statistic_rows_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<PgStatisticRow> {
    let mut rows = SearchSysCacheList1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::STATRELATTINH,
        oid_key(relation_oid),
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Statistic(row) => Some(row),
                _ => None,
            })
            .collect::<Vec<_>>()
    })
    .unwrap_or_default();
    rows.sort_by_key(|row| (row.staattnum, row.stainherit));
    rows
}

fn statistic_ext_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<PgStatisticExtRow> {
    SearchSysCache1(db, client_id, txn_ctx, SysCacheId::STATEXTOID, oid_key(oid))
        .ok()?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::StatisticExt(row) => Some(row),
            _ => None,
        })
}

fn statistic_ext_row_by_name_namespace(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    name: &str,
    namespace_oid: u32,
) -> Option<PgStatisticExtRow> {
    let normalized = crate::backend::parser::analyze::normalize_catalog_lookup_name(name);
    SearchSysCache2(
        db,
        client_id,
        txn_ctx,
        SysCacheId::STATEXTNAMENSP,
        catalog_name_key(normalized),
        oid_key(namespace_oid),
    )
    .ok()?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::StatisticExt(row) => Some(row),
        _ => None,
    })
}

fn statistic_ext_rows_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<PgStatisticExtRow> {
    SearchSysCacheList1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::STATEXTRELID,
        oid_key(relation_oid),
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::StatisticExt(row) => Some(row),
                _ => None,
            })
            .collect()
    })
    .unwrap_or_default()
}

fn statistic_ext_data_row(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    stxoid: u32,
    stxdinherit: bool,
) -> Option<PgStatisticExtDataRow> {
    SearchSysCache2(
        db,
        client_id,
        txn_ctx,
        SysCacheId::STATEXTDATASTXOID,
        oid_key(stxoid),
        Value::Bool(stxdinherit),
    )
    .ok()?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::StatisticExtData(row) => Some(row),
        _ => None,
    })
}
fn dedup_proc_rows(rows: &mut Vec<PgProcRow>) {
    let mut seen = BTreeSet::new();
    rows.retain(|row| {
        seen.insert((
            row.proname.clone(),
            row.prorettype,
            row.proargtypes.clone(),
            row.prokind,
            row.proretset,
        ))
    });
}

fn aggregate_row_by_fnoid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    aggfnoid: u32,
) -> Option<PgAggregateRow> {
    SearchSysCache1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::AGGFNOID,
        oid_key(aggfnoid),
    )
    .ok()?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::Aggregate(row) => Some(row),
        _ => None,
    })
}

fn language_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<PgLanguageRow> {
    SearchSysCache1(db, client_id, txn_ctx, SysCacheId::LANGOID, oid_key(oid))
        .ok()?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Language(row) => Some(row),
            _ => None,
        })
}

fn language_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgLanguageRow> {
    visible_catcache(db, client_id, txn_ctx)
        .map(|catcache| catcache.language_rows())
        .unwrap_or_default()
}

fn language_row_by_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    name: &str,
) -> Option<PgLanguageRow> {
    SearchSysCache1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::LANGNAME,
        Value::Text(normalize_catalog_name(name).to_ascii_lowercase().into()),
    )
    .ok()?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::Language(row) => Some(row),
        _ => None,
    })
}

fn opclass_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<PgOpclassRow> {
    SearchSysCache1(db, client_id, txn_ctx, SysCacheId::CLAOID, oid_key(oid))
        .ok()?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Opclass(row) => Some(row),
            _ => None,
        })
}

pub fn opclass_rows_for_am(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    am_oid: u32,
) -> Vec<PgOpclassRow> {
    let mut rows: Vec<PgOpclassRow> = SearchSysCacheList1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::CLAAMNAMENSP,
        oid_key(am_oid),
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Opclass(row) => Some(row),
                _ => None,
            })
            .collect()
    })
    .unwrap_or_default();
    let seen = rows.iter().map(|row| row.oid).collect::<BTreeSet<_>>();
    rows.extend(
        crate::include::catalog::bootstrap_pg_opclass_rows()
            .into_iter()
            .filter(|row| row.opcmethod == am_oid && !seen.contains(&row.oid)),
    );
    rows
}

#[derive(Clone)]
pub struct LazyCatalogLookup {
    pub db: Database,
    pub client_id: ClientId,
    pub txn_ctx: Option<(TransactionId, CommandId)>,
    pub search_path: Vec<String>,
}

fn owned_temp_namespace(db: &Database, client_id: ClientId) -> Option<TempNamespace> {
    db.temp_relations
        .read()
        .get(&db.temp_backend_id(client_id))
        .cloned()
}

fn namespace_oid_for_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    name: &str,
) -> Option<u32> {
    namespace_row_by_name(db, client_id, txn_ctx, name).map(|row| row.oid)
}

fn type_for_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<PgTypeRow> {
    type_row_by_oid(db, client_id, txn_ctx, oid)
}

pub fn access_method_row_by_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    amname: &str,
) -> Option<PgAmRow> {
    SearchSysCache1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::AMNAME,
        Value::Text(normalize_catalog_name(amname).to_ascii_lowercase().into()),
    )
    .ok()?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::Am(row) => Some(row),
        _ => None,
    })
}

pub fn access_method_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    am_oid: u32,
) -> Option<PgAmRow> {
    SearchSysCache1(db, client_id, txn_ctx, SysCacheId::AMOID, oid_key(am_oid))
        .ok()?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Am(row) => Some(row),
            _ => None,
        })
}

pub fn default_opclass_for_am_and_type(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    am_oid: u32,
    input_type_oid: u32,
) -> Option<PgOpclassRow> {
    let opclasses = opclass_rows_for_am(db, client_id, txn_ctx, am_oid);
    if db
        .enum_rows_for_catalog()
        .iter()
        .any(|row| row.enumtypid == input_type_oid)
    {
        return opclasses.into_iter().find(|row| {
            row.opcmethod == am_oid
                && row.opcdefault
                && row.opcintype == crate::include::catalog::ANYENUMOID
        });
    }
    if am_oid == crate::include::catalog::BRIN_AM_OID
        && input_type_oid == crate::include::catalog::NAME_TYPE_OID
    {
        return opclasses
            .into_iter()
            .find(|row| row.oid == crate::include::catalog::TEXT_BRIN_MINMAX_OPCLASS_OID);
    }
    if let Some(row) = opclasses
        .iter()
        .find(|row| row.opcmethod == am_oid && row.opcdefault && row.opcintype == input_type_oid)
    {
        return Some(row.clone());
    }
    if am_oid == crate::include::catalog::BRIN_AM_OID
        && input_type_oid == crate::include::catalog::CIDR_TYPE_OID
    {
        return opclasses
            .into_iter()
            .find(|row| row.oid == crate::include::catalog::INET_BRIN_INCLUSION_OPCLASS_OID);
    }
    if am_oid == crate::include::catalog::BTREE_AM_OID
        && input_type_oid == crate::include::catalog::VARCHAR_TYPE_OID
    {
        return opclasses
            .into_iter()
            .find(|row| row.oid == crate::include::catalog::VARCHAR_BTREE_OPCLASS_OID);
    }
    let range_rows = db.range_rows();
    if range_rows.iter().any(|row| row.rngtypid == input_type_oid) {
        let opclass_oid = match am_oid {
            crate::include::catalog::BTREE_AM_OID => {
                crate::include::catalog::RANGE_BTREE_OPCLASS_OID
            }
            crate::include::catalog::HASH_AM_OID => crate::include::catalog::RANGE_HASH_OPCLASS_OID,
            crate::include::catalog::GIST_AM_OID => crate::include::catalog::RANGE_GIST_OPCLASS_OID,
            crate::include::catalog::SPGIST_AM_OID => {
                crate::include::catalog::RANGE_SPGIST_OPCLASS_OID
            }
            crate::include::catalog::BRIN_AM_OID => {
                crate::include::catalog::RANGE_BRIN_INCLUSION_OPCLASS_OID
            }
            _ => 0,
        };
        if opclass_oid != 0 {
            return opclasses.into_iter().find(|row| row.oid == opclass_oid);
        }
    }
    if range_rows
        .iter()
        .any(|row| row.rngmultitypid == input_type_oid)
    {
        return opclasses.into_iter().find(|row| {
            row.opcmethod == am_oid
                && row.opcdefault
                && row.opcintype == crate::include::catalog::ANYMULTIRANGEOID
        });
    }
    let search_path = db.effective_search_path(client_id, None);
    let input_type = visible_type_row_by_oid(db, client_id, txn_ctx, &search_path, input_type_oid)?;
    if input_type.typtype == 'd'
        && input_type.typbasetype != 0
        && let Some(row) =
            default_opclass_for_am_and_type(db, client_id, txn_ctx, am_oid, input_type.typbasetype)
    {
        return Some(row);
    }
    if am_oid == crate::include::catalog::BTREE_AM_OID
        && !input_type.sql_type.is_array
        && matches!(
            input_type.sql_type.kind,
            SqlTypeKind::Composite | SqlTypeKind::Record
        )
    {
        return opclasses
            .into_iter()
            .find(|row| row.oid == crate::include::catalog::RECORD_BTREE_OPCLASS_OID);
    }
    if input_type.sql_type.is_range() {
        let opclass_oid = match am_oid {
            crate::include::catalog::BTREE_AM_OID => {
                crate::include::catalog::RANGE_BTREE_OPCLASS_OID
            }
            crate::include::catalog::HASH_AM_OID => crate::include::catalog::RANGE_HASH_OPCLASS_OID,
            crate::include::catalog::GIST_AM_OID => crate::include::catalog::RANGE_GIST_OPCLASS_OID,
            crate::include::catalog::SPGIST_AM_OID => {
                crate::include::catalog::RANGE_SPGIST_OPCLASS_OID
            }
            crate::include::catalog::BRIN_AM_OID => {
                crate::include::catalog::RANGE_BRIN_INCLUSION_OPCLASS_OID
            }
            _ => 0,
        };
        if opclass_oid != 0 {
            return opclasses.into_iter().find(|row| row.oid == opclass_oid);
        }
    }
    if input_type.sql_type.is_multirange() {
        return opclasses.into_iter().find(|row| {
            row.opcmethod == am_oid
                && row.opcdefault
                && row.opcintype == crate::include::catalog::ANYMULTIRANGEOID
        });
    }
    if input_type.sql_type.is_array {
        return opclasses.into_iter().find(|row| {
            row.opcmethod == am_oid
                && row.opcdefault
                && row.opcintype == crate::include::catalog::ANYARRAYOID
        });
    }
    if matches!(
        input_type.sql_type.kind,
        crate::backend::parser::SqlTypeKind::Record
            | crate::backend::parser::SqlTypeKind::Composite
    ) {
        return opclasses.into_iter().find(|row| {
            row.opcmethod == am_oid
                && row.opcdefault
                && row.opcintype == crate::include::catalog::RECORD_TYPE_OID
        });
    }
    if matches!(
        input_type.sql_type.kind,
        crate::backend::parser::SqlTypeKind::Enum
    ) {
        return opclasses.into_iter().find(|row| {
            row.opcmethod == am_oid
                && row.opcdefault
                && row.opcintype == crate::include::catalog::ANYENUMOID
        });
    }
    (am_oid == crate::include::catalog::GIST_AM_OID
        && crate::include::catalog::builtin_range_rows()
            .iter()
            .any(|row| row.rngtypid == input_type_oid))
    .then(|| {
        opclasses
            .iter()
            .find(|row| row.oid == crate::include::catalog::RANGE_GIST_OPCLASS_OID)
            .cloned()
    })
    .flatten()
}

pub fn opfamily_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    family_oid: u32,
) -> Option<PgOpfamilyRow> {
    SearchSysCache1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::OPFAMILYOID,
        oid_key(family_oid),
    )
    .ok()?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::Opfamily(row) => Some(row),
        _ => None,
    })
}

pub fn collation_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    collation_oid: u32,
) -> Option<PgCollationRow> {
    SearchSysCache1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::COLLOID,
        oid_key(collation_oid),
    )
    .ok()?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::Collation(row) => Some(row),
        _ => None,
    })
}

pub fn amop_rows_for_family(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    family_oid: u32,
) -> Vec<PgAmopRow> {
    SearchSysCacheList1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::AMOPSTRATEGY,
        oid_key(family_oid),
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Amop(row) => Some(row),
                _ => None,
            })
            .collect()
    })
    .unwrap_or_default()
}

pub fn amproc_rows_for_family(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    family_oid: u32,
) -> Vec<PgAmprocRow> {
    SearchSysCacheList1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::AMPROCNUM,
        oid_key(family_oid),
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Amproc(row) => Some(row),
                _ => None,
            })
            .collect()
    })
    .unwrap_or_default()
}

pub fn index_row_by_indexrelid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Option<PgIndexRow> {
    SearchSysCache1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::INDEXRELID,
        oid_key(relation_oid),
    )
    .ok()?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::Index(row) => Some(row),
        _ => None,
    })
}

fn index_rows_for_heap(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<PgIndexRow> {
    SearchSysCacheList1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::INDEXINDRELID,
        oid_key(relation_oid),
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Index(row) => Some(row),
                _ => None,
            })
            .collect()
    })
    .unwrap_or_default()
}

pub fn relation_get_index_list(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<u32> {
    let mut index_oids = index_rows_for_heap(db, client_id, txn_ctx, relation_oid)
        .into_iter()
        .map(|row| row.indexrelid)
        .collect::<BTreeSet<_>>();
    if let Some(namespace) = db.temp_relations.read().get(&db.temp_backend_id(client_id)) {
        index_oids.extend(namespace.tables.values().filter_map(|temp| {
            let index = temp.entry.index.as_ref()?;
            (index.indrelid == relation_oid).then_some(temp.entry.relation_oid)
        }));
    }
    index_oids.into_iter().collect()
}

#[allow(non_snake_case)]
pub fn RelationGetIndexList(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<u32> {
    relation_get_index_list(db, client_id, txn_ctx, relation_oid)
}

pub fn index_relation_oids_for_heap(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<u32> {
    RelationGetIndexList(db, client_id, txn_ctx, relation_oid)
}

pub fn relation_entry_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Option<RelCacheEntry> {
    if let Some(entry) = db
        .temp_relations
        .read()
        .get(&db.temp_backend_id(client_id))
        .and_then(|namespace| {
            namespace
                .tables
                .values()
                .find(|temp| temp.entry.relation_oid == relation_oid)
                .map(|temp| temp.entry.clone())
        })
    {
        return Some(entry);
    }

    if let Ok(Some(entry)) = RelationIdGetRelation(db, client_id, txn_ctx, relation_oid) {
        return (!db.other_session_temp_namespace_oid(client_id, entry.namespace_oid))
            .then_some(entry);
    }

    None
}

fn relation_entry_by_name_namespace(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relname: &str,
    namespace_oid: u32,
) -> Option<RelCacheEntry> {
    let class = class_row_by_name_namespace(db, client_id, txn_ctx, relname, namespace_oid)?;
    relation_entry_by_oid(db, client_id, txn_ctx, class.oid)
}

fn temp_relation_entry_by_name(
    db: &Database,
    client_id: ClientId,
    relname: &str,
) -> Option<RelCacheEntry> {
    db.temp_relations
        .read()
        .get(&db.temp_backend_id(client_id))
        .and_then(|namespace| {
            namespace
                .tables
                .get(&normalize_catalog_name(relname).to_ascii_lowercase())
                .map(|entry| entry.entry.clone())
        })
}

fn toast_relation_from_entry(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    entry: &RelCacheEntry,
) -> Option<crate::include::nodes::primnodes::ToastRelationRef> {
    let toast_oid = entry.reltoastrelid;
    (toast_oid != 0)
        .then(|| relation_entry_by_oid(db, client_id, txn_ctx, toast_oid))
        .flatten()
        .map(|toast| crate::include::nodes::primnodes::ToastRelationRef {
            rel: toast.rel,
            relation_oid: toast.relation_oid,
        })
}

fn bound_relation_from_entry(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    entry: RelCacheEntry,
) -> BoundRelation {
    BoundRelation {
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        toast: toast_relation_from_entry(db, client_id, txn_ctx, &entry),
        namespace_oid: entry.namespace_oid,
        owner_oid: entry.owner_oid,
        of_type_oid: entry.of_type_oid,
        relpersistence: entry.relpersistence,
        relkind: entry.relkind,
        relispopulated: entry.relispopulated,
        desc: entry.desc.clone(),
        relispartition: entry.relispartition,
        relpartbound: entry.relpartbound.clone(),
        partitioned_table: entry.partitioned_table.clone(),
        partition_spec: entry.partition_spec.clone(),
    }
}

pub fn lookup_any_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    search_path: &[String],
    name: &str,
) -> Option<BoundRelation> {
    let catalog_name = normalize_catalog_name(name);
    if let Some((schema, relname)) = catalog_name.split_once('.') {
        if let Some(temp_namespace) = owned_temp_namespace(db, client_id)
            && (schema.eq_ignore_ascii_case("pg_temp")
                || temp_namespace.name.eq_ignore_ascii_case(schema))
        {
            return temp_namespace
                .tables
                .get(&normalize_catalog_name(relname).to_ascii_lowercase())
                .map(|entry| {
                    bound_relation_from_entry(db, client_id, txn_ctx, entry.entry.clone())
                });
        }
        let namespace_oid = namespace_row_by_name(db, client_id, txn_ctx, schema)?.oid;
        let entry =
            relation_entry_by_name_namespace(db, client_id, txn_ctx, relname, namespace_oid)?;
        return Some(bound_relation_from_entry(db, client_id, txn_ctx, entry));
    }

    let temp_name = catalog_name.to_ascii_lowercase();
    if let Some(temp) = temp_relation_entry_by_name(db, client_id, &temp_name) {
        return Some(bound_relation_from_entry(db, client_id, txn_ctx, temp));
    }

    for namespace_name in search_path {
        let Some(namespace_oid) =
            namespace_row_by_name(db, client_id, txn_ctx, namespace_name).map(|row| row.oid)
        else {
            continue;
        };
        if let Some(entry) =
            relation_entry_by_name_namespace(db, client_id, txn_ctx, catalog_name, namespace_oid)
        {
            return Some(bound_relation_from_entry(db, client_id, txn_ctx, entry));
        }
    }

    None
}

pub fn describe_relation_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Option<RelCacheEntry> {
    relation_entry_by_oid(db, client_id, txn_ctx, relation_oid)
}

pub fn relation_namespace_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Option<String> {
    let entry = relation_entry_by_oid(db, client_id, txn_ctx, relation_oid)?;
    namespace_row_by_oid(db, client_id, txn_ctx, entry.namespace_oid).map(|row| row.nspname)
}

pub fn relation_display_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    configured_search_path: Option<&[String]>,
    relation_oid: u32,
) -> Option<String> {
    let entry = relation_entry_by_oid(db, client_id, txn_ctx, relation_oid)?;
    let class = class_row_by_oid(db, client_id, txn_ctx, relation_oid)?;
    let namespace = relation_namespace_name(db, client_id, txn_ctx, relation_oid)?;
    if namespace.starts_with("pg_temp_") {
        return Some(format!("{namespace}.{}", class.relname));
    }
    let search_path = db.effective_search_path(client_id, configured_search_path);
    let first_match = search_path
        .iter()
        .find_map(|schema| {
            let namespace_oid = namespace_oid_for_name(db, client_id, txn_ctx, schema)?;
            let row =
                class_row_by_name_namespace(db, client_id, txn_ctx, &class.relname, namespace_oid)?;
            let visible_entry = relation_entry_by_oid(db, client_id, txn_ctx, row.oid)?;
            Some((row, visible_entry))
        })
        .and_then(|(row, visible_entry)| {
            visible_entry
                .relkind
                .eq(&entry.relkind)
                .then_some(())
                .map(|_| row.relnamespace)
        });
    if let Some(visible_namespace_oid) = first_match
        && visible_namespace_oid == entry.namespace_oid
    {
        Some(class.relname)
    } else {
        Some(format!("{namespace}.{}", class.relname))
    }
}

pub fn has_index_on_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> bool {
    !RelationGetIndexList(db, client_id, txn_ctx, relation_oid).is_empty()
}

pub fn access_method_name_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Option<String> {
    let class = class_row_by_oid(db, client_id, txn_ctx, relation_oid)?;
    access_method_row_by_oid(db, client_id, txn_ctx, class.relam)
        .map(|row| row.amname)
        .or_else(|| match class.relkind {
            'r' => Some("heap".to_string()),
            'i' => Some("btree".to_string()),
            _ => None,
        })
}

pub fn constraint_rows_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<PgConstraintRow> {
    let mut rows = constraint_rows_for_relation_syscache(db, client_id, txn_ctx, relation_oid);
    let Some(entry) = relation_entry_by_oid(db, client_id, txn_ctx, relation_oid) else {
        return rows;
    };
    let Some(class) = class_row_by_oid(db, client_id, txn_ctx, relation_oid) else {
        return rows;
    };
    append_missing_derived_not_null_constraints(
        &mut rows,
        relation_oid,
        &class.relname,
        entry.namespace_oid,
        &entry.desc,
    );
    rows
}

fn append_missing_derived_not_null_constraints(
    rows: &mut Vec<PgConstraintRow>,
    relation_oid: u32,
    relation_name: &str,
    namespace_oid: u32,
    desc: &crate::include::nodes::primnodes::RelationDesc,
) {
    for derived in derived_pg_constraint_rows(relation_oid, relation_name, namespace_oid, desc) {
        if rows.iter().any(|row| {
            row.oid == derived.oid
                || (row.contype == CONSTRAINT_NOTNULL
                    && row.conrelid == derived.conrelid
                    && row.conkey == derived.conkey)
        }) {
            continue;
        }
        rows.push(derived);
    }
}

impl CatalogLookup for LazyCatalogLookup {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation> {
        lookup_any_relation(
            &self.db,
            self.client_id,
            self.txn_ctx,
            &self.search_path,
            name,
        )
    }

    fn lookup_relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        self.relation_by_oid(relation_oid)
    }

    fn relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        let entry = relation_entry_by_oid(&self.db, self.client_id, self.txn_ctx, relation_oid)?;
        Some(BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            toast: toast_relation_from_entry(&self.db, self.client_id, self.txn_ctx, &entry),
            namespace_oid: entry.namespace_oid,
            owner_oid: entry.owner_oid,
            of_type_oid: entry.of_type_oid,
            relpersistence: entry.relpersistence,
            relkind: entry.relkind,
            relispopulated: entry.relispopulated,
            desc: entry.desc.clone(),
            relispartition: entry.relispartition,
            relpartbound: entry.relpartbound.clone(),
            partitioned_table: entry.partitioned_table.clone(),
            partition_spec: entry.partition_spec.clone(),
        })
    }

    fn index_row_by_oid(&self, index_oid: u32) -> Option<PgIndexRow> {
        index_row_by_indexrelid(&self.db, self.client_id, self.txn_ctx, index_oid)
    }

    fn operator_by_name_left_right(
        &self,
        name: &str,
        left_type_oid: u32,
        right_type_oid: u32,
    ) -> Option<PgOperatorRow> {
        operator_row_by_name_left_right(
            &self.db,
            self.client_id,
            self.txn_ctx,
            name,
            left_type_oid,
            right_type_oid,
        )
    }

    fn operator_by_oid(&self, oid: u32) -> Option<PgOperatorRow> {
        SearchSysCache1(
            &self.db,
            self.client_id,
            self.txn_ctx,
            SysCacheId::OPEROID,
            oid_key(oid),
        )
        .ok()?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Operator(row) => Some(row),
            _ => None,
        })
    }

    fn operator_rows(&self) -> Vec<PgOperatorRow> {
        backend_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|cache| cache.operator_rows())
            .unwrap_or_default()
    }

    fn cast_by_source_target(
        &self,
        source_type_oid: u32,
        target_type_oid: u32,
    ) -> Option<PgCastRow> {
        SearchSysCache2(
            &self.db,
            self.client_id,
            self.txn_ctx,
            SysCacheId::CASTSOURCETARGET,
            oid_key(source_type_oid),
            oid_key(target_type_oid),
        )
        .ok()?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Cast(row) => Some(row),
            _ => None,
        })
    }

    fn cast_rows(&self) -> Vec<PgCastRow> {
        backend_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|cache| cache.cast_rows())
            .unwrap_or_default()
    }

    fn conversion_rows(&self) -> Vec<PgConversionRow> {
        backend_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|cache| cache.conversion_rows())
            .unwrap_or_default()
    }

    fn current_user_oid(&self) -> u32 {
        self.db.auth_state(self.client_id).current_user_oid()
    }

    fn search_path(&self) -> Vec<String> {
        self.search_path.clone()
    }

    fn session_user_oid(&self) -> u32 {
        self.db.auth_state(self.client_id).session_user_oid()
    }

    fn authid_rows(&self) -> Vec<PgAuthIdRow> {
        self.db
            .auth_catalog(self.client_id, self.txn_ctx)
            .map(|catalog| catalog.roles().to_vec())
            .unwrap_or_default()
    }

    fn auth_members_rows(&self) -> Vec<PgAuthMembersRow> {
        self.db
            .auth_catalog(self.client_id, self.txn_ctx)
            .map(|catalog| catalog.memberships().to_vec())
            .unwrap_or_default()
    }

    fn depend_rows(&self) -> Vec<PgDependRow> {
        backend_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|catcache| catcache.depend_rows())
            .unwrap_or_default()
    }

    fn depend_rows_referencing(
        &self,
        refclassid: u32,
        refobjid: u32,
        refobjsubid: Option<i32>,
    ) -> Vec<PgDependRow> {
        depend_rows_referencing(
            &self.db,
            self.client_id,
            self.txn_ctx,
            refclassid,
            refobjid,
            refobjsubid,
        )
    }

    fn database_rows(&self) -> Vec<PgDatabaseRow> {
        backend_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|catcache| catcache.database_rows())
            .unwrap_or_default()
    }

    fn namespace_row_by_oid(&self, oid: u32) -> Option<PgNamespaceRow> {
        namespace_row_by_oid(&self.db, self.client_id, self.txn_ctx, oid)
    }

    fn namespace_rows(&self) -> Vec<PgNamespaceRow> {
        ensure_namespace_rows(&self.db, self.client_id, self.txn_ctx)
    }

    fn row_security_enabled(&self) -> bool {
        self.db.row_security_enabled(self.client_id)
    }

    fn current_relation_pages(&self, relation_oid: u32) -> Option<u32> {
        let relation = self.relation_by_oid(relation_oid)?;
        self.db
            .pool
            .with_storage_mut(|storage| storage.smgr.nblocks(relation.rel, ForkNumber::Main))
            .ok()
    }

    fn current_relation_live_tuples(&self, relation_oid: u32) -> Option<f64> {
        let session_stats = self.db.session_stats_state(self.client_id);
        let mut stats = session_stats.write();
        stats
            .visible_relation_entry(&self.db.stats, relation_oid)
            .map(|entry| entry.live_tuples.max(0) as f64)
    }

    fn brin_pages_per_range(&self, relation_oid: u32) -> Option<u32> {
        let relation = self.relation_by_oid(relation_oid)?;
        let mut page = [0u8; BLCKSZ];
        self.db
            .pool
            .with_storage_mut(|storage| {
                storage
                    .smgr
                    .read_block(relation.rel, ForkNumber::Main, 0, &mut page)
            })
            .ok()?;
        if !brin_is_meta_page(&page).ok()? {
            return None;
        }
        let bytes =
            page.get(BRIN_PAGE_CONTENT_OFFSET..BRIN_PAGE_CONTENT_OFFSET + BrinMetaPageData::SIZE)?;
        let meta = BrinMetaPageData {
            brin_magic: u32::from_le_bytes(bytes[0..4].try_into().ok()?),
            brin_version: u32::from_le_bytes(bytes[4..8].try_into().ok()?),
            pages_per_range: u32::from_le_bytes(bytes[8..12].try_into().ok()?),
            last_revmap_page: u32::from_le_bytes(bytes[12..16].try_into().ok()?),
        };
        (meta.pages_per_range > 0).then_some(meta.pages_per_range)
    }

    fn constraint_rows_for_relation(&self, relation_oid: u32) -> Vec<PgConstraintRow> {
        constraint_rows_for_relation(&self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn constraint_row_by_oid(&self, oid: u32) -> Option<PgConstraintRow> {
        constraint_row_by_oid(&self.db, self.client_id, self.txn_ctx, oid).or_else(|| {
            self.db
                .domain_constraint_rows_for_catalog()
                .into_iter()
                .find(|row| row.oid == oid)
        })
    }

    fn constraint_rows_for_index(&self, index_oid: u32) -> Vec<PgConstraintRow> {
        constraint_rows_for_index(&self.db, self.client_id, self.txn_ctx, index_oid)
    }

    fn foreign_key_constraint_rows_referencing_relation(
        &self,
        relation_oid: u32,
    ) -> Vec<PgConstraintRow> {
        foreign_key_constraint_rows_referencing_relation(
            &self.db,
            self.client_id,
            self.txn_ctx,
            relation_oid,
        )
    }

    fn foreign_key_constraint_rows_referencing_index(
        &self,
        index_oid: u32,
    ) -> Vec<PgConstraintRow> {
        foreign_key_constraint_rows_referencing_index(
            &self.db,
            self.client_id,
            self.txn_ctx,
            index_oid,
        )
    }

    fn constraint_rows(&self) -> Vec<PgConstraintRow> {
        let mut rows = ensure_constraint_rows(&self.db, self.client_id, self.txn_ctx);
        rows.extend(self.db.domain_constraint_rows_for_catalog());
        crate::backend::catalog::pg_constraint::sort_pg_constraint_rows(&mut rows);
        rows
    }

    fn proc_rows_by_name(&self, name: &str) -> Vec<PgProcRow> {
        let mut rows = proc_rows_by_name(&self.db, self.client_id, self.txn_ctx, name);
        if is_visible_range_proc_name(
            &self.db,
            self.client_id,
            self.txn_ctx,
            &self.search_path,
            name,
        ) {
            rows.extend(crate::include::catalog::synthetic_range_proc_rows_by_name(
                name,
                &range_proc_type_rows(&self.db, &self.search_path),
                &self.range_rows(),
            ));
        }
        dedup_proc_rows(&mut rows);
        rows
    }

    fn proc_row_by_oid(&self, oid: u32) -> Option<PgProcRow> {
        proc_row_by_oid(&self.db, self.client_id, self.txn_ctx, oid).or_else(|| {
            crate::include::catalog::synthetic_range_proc_row_by_oid(
                oid,
                &range_proc_type_rows(&self.db, &self.search_path),
                &self.range_rows(),
            )
        })
    }

    fn proc_rows(&self) -> Vec<PgProcRow> {
        ensure_proc_rows(&self.db, self.client_id, self.txn_ctx)
    }

    fn opclass_rows(&self) -> Vec<PgOpclassRow> {
        ensure_opclass_rows(&self.db, self.client_id, self.txn_ctx)
    }

    fn opfamily_rows(&self) -> Vec<PgOpfamilyRow> {
        backend_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|cache| cache.opfamily_rows())
            .unwrap_or_default()
    }

    fn amproc_rows(&self) -> Vec<PgAmprocRow> {
        ensure_amproc_rows(&self.db, self.client_id, self.txn_ctx)
    }

    fn amop_rows(&self) -> Vec<PgAmopRow> {
        ensure_amop_rows(&self.db, self.client_id, self.txn_ctx)
    }

    fn collation_rows(&self) -> Vec<PgCollationRow> {
        ensure_collation_rows(&self.db, self.client_id, self.txn_ctx)
    }

    fn ts_config_rows(&self) -> Vec<PgTsConfigRow> {
        backend_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|cache| cache.ts_config_rows())
            .unwrap_or_default()
    }

    fn ts_parser_rows(&self) -> Vec<PgTsParserRow> {
        backend_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|cache| cache.ts_parser_rows())
            .unwrap_or_default()
    }

    fn ts_dict_rows(&self) -> Vec<PgTsDictRow> {
        backend_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|cache| cache.ts_dict_rows())
            .unwrap_or_default()
    }

    fn ts_template_rows(&self) -> Vec<PgTsTemplateRow> {
        backend_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|cache| cache.ts_template_rows())
            .unwrap_or_default()
    }

    fn ts_config_map_rows(&self) -> Vec<PgTsConfigMapRow> {
        backend_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|cache| cache.ts_config_map_rows())
            .unwrap_or_default()
    }

    fn aggregate_by_fnoid(&self, aggfnoid: u32) -> Option<PgAggregateRow> {
        aggregate_row_by_fnoid(&self.db, self.client_id, self.txn_ctx, aggfnoid)
    }

    fn type_rows(&self) -> Vec<PgTypeRow> {
        let mut rows = ensure_type_rows(&self.db, self.client_id, self.txn_ctx);
        for row in dynamic_type_rows_for_search_path(&self.db, &self.search_path) {
            if rows.iter().all(|existing| existing.oid != row.oid) {
                rows.push(row);
            }
        }
        rows
    }

    fn type_by_oid(&self, oid: u32) -> Option<PgTypeRow> {
        visible_type_row_by_oid(
            &self.db,
            self.client_id,
            self.txn_ctx,
            &self.search_path,
            oid,
        )
    }

    fn type_by_name(&self, name: &str) -> Option<PgTypeRow> {
        visible_type_row_by_name(
            &self.db,
            self.client_id,
            self.txn_ctx,
            &self.search_path,
            name,
        )
    }

    fn domain_by_name(&self, name: &str) -> Option<DomainLookup> {
        visible_domain_by_name(
            &self.db,
            self.client_id,
            self.txn_ctx,
            &self.search_path,
            name,
        )
    }

    fn domain_by_type_oid(&self, domain_oid: u32) -> Option<DomainLookup> {
        self.db.domain_by_type_oid(domain_oid)
    }

    fn type_default_sql(&self, type_oid: u32) -> Option<String> {
        self.db.base_type_default(type_oid)
    }

    fn type_oid_for_sql_type(&self, sql_type: SqlType) -> Option<u32> {
        visible_type_oid_for_sql_type(
            &self.db,
            self.client_id,
            self.txn_ctx,
            &self.search_path,
            sql_type,
        )
    }

    fn range_rows(&self) -> Vec<crate::include::catalog::PgRangeRow> {
        let mut rows = crate::include::catalog::builtin_range_rows();
        rows.extend(self.db.range_rows());
        rows
    }

    fn enum_label_oid(&self, type_oid: u32, label: &str) -> Option<u32> {
        self.db.enum_label_oid(type_oid, label)
    }

    fn enum_label(&self, type_oid: u32, label_oid: u32) -> Option<String> {
        self.db.enum_label(type_oid, label_oid)
    }

    fn enum_rows(&self) -> Vec<PgEnumRow> {
        self.db.enum_rows_for_catalog()
    }

    fn enum_label_is_committed(&self, type_oid: u32, label_oid: u32) -> bool {
        self.db.enum_label_is_committed(type_oid, label_oid)
    }

    fn domain_allowed_enum_label_oids(&self, domain_oid: u32) -> Option<Vec<u32>> {
        self.db.domain_allowed_enum_label_oids(domain_oid)
    }

    fn domain_check_name(&self, domain_oid: u32) -> Option<String> {
        self.db.domain_check_name(domain_oid)
    }

    fn domain_check_by_type_oid(&self, domain_oid: u32) -> Option<String> {
        self.db.domain_check_by_type_oid(domain_oid)
    }

    fn language_rows(&self) -> Vec<PgLanguageRow> {
        language_rows(&self.db, self.client_id, self.txn_ctx)
    }

    fn language_row_by_oid(&self, oid: u32) -> Option<PgLanguageRow> {
        language_row_by_oid(&self.db, self.client_id, self.txn_ctx, oid)
    }

    fn language_row_by_name(&self, name: &str) -> Option<PgLanguageRow> {
        language_row_by_name(&self.db, self.client_id, self.txn_ctx, name)
    }

    fn rewrite_rows_for_relation(&self, relation_oid: u32) -> Vec<PgRewriteRow> {
        SearchSysCacheList1(
            &self.db,
            self.client_id,
            self.txn_ctx,
            SysCacheId::RULERELNAME,
            oid_key(relation_oid),
        )
        .map(|tuples| {
            tuples
                .into_iter()
                .filter_map(|tuple| match tuple {
                    SysCacheTuple::Rewrite(row) => Some(row),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default()
    }

    fn rewrite_rows(&self) -> Vec<PgRewriteRow> {
        ensure_rewrite_rows(&self.db, self.client_id, self.txn_ctx)
    }

    fn trigger_rows_for_relation(&self, relation_oid: u32) -> Vec<PgTriggerRow> {
        trigger_rows_for_relation(&self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn trigger_rows(&self) -> Vec<PgTriggerRow> {
        backend_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|catcache| catcache.trigger_rows())
            .unwrap_or_default()
    }

    fn event_trigger_rows(&self) -> Vec<PgEventTriggerRow> {
        backend_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|catcache| catcache.event_trigger_rows())
            .unwrap_or_default()
    }

    fn policy_rows_for_relation(
        &self,
        relation_oid: u32,
    ) -> Vec<crate::include::catalog::PgPolicyRow> {
        policy_rows_for_relation(&self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn class_row_by_oid(&self, relation_oid: u32) -> Option<PgClassRow> {
        class_row_by_oid(&self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn attribute_rows_for_relation(
        &self,
        relation_oid: u32,
    ) -> Vec<crate::include::catalog::PgAttributeRow> {
        attribute_rows_for_relation(&self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn attribute_rows(&self) -> Vec<crate::include::catalog::PgAttributeRow> {
        ensure_attribute_rows(&self.db, self.client_id, self.txn_ctx)
    }

    fn class_rows(&self) -> Vec<PgClassRow> {
        ensure_class_rows(&self.db, self.client_id, self.txn_ctx)
    }

    fn partitioned_table_row(
        &self,
        relation_oid: u32,
    ) -> Option<crate::include::catalog::PgPartitionedTableRow> {
        partitioned_table_row_by_relid(&self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn inheritance_parents(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
        inheritance_parent_rows(&self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn inheritance_children(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
        inheritance_child_rows(&self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn inheritance_rows(&self) -> Vec<PgInheritsRow> {
        ensure_inherit_rows(&self.db, self.client_id, self.txn_ctx)
    }

    fn publication_rows(&self) -> Vec<PgPublicationRow> {
        with_backend_catcache(&self.db, self.client_id, self.txn_ctx, |catcache| {
            catcache.publication_rows()
        })
        .unwrap_or_default()
    }

    fn publication_rel_rows(&self) -> Vec<PgPublicationRelRow> {
        with_backend_catcache(&self.db, self.client_id, self.txn_ctx, |catcache| {
            catcache.publication_rel_rows()
        })
        .unwrap_or_default()
    }

    fn publication_namespace_rows(&self) -> Vec<PgPublicationNamespaceRow> {
        with_backend_catcache(&self.db, self.client_id, self.txn_ctx, |catcache| {
            catcache.publication_namespace_rows()
        })
        .unwrap_or_default()
    }

    fn statistic_rows_for_relation(&self, relation_oid: u32) -> Vec<PgStatisticRow> {
        statistic_rows_for_relation(&self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn statistic_ext_row_by_oid(&self, oid: u32) -> Option<PgStatisticExtRow> {
        statistic_ext_row_by_oid(&self.db, self.client_id, self.txn_ctx, oid)
    }

    fn statistic_ext_row_by_name_namespace(
        &self,
        name: &str,
        namespace_oid: u32,
    ) -> Option<PgStatisticExtRow> {
        statistic_ext_row_by_name_namespace(
            &self.db,
            self.client_id,
            self.txn_ctx,
            name,
            namespace_oid,
        )
    }

    fn statistic_ext_rows_for_relation(&self, relation_oid: u32) -> Vec<PgStatisticExtRow> {
        statistic_ext_rows_for_relation(&self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn statistic_ext_rows(&self) -> Vec<PgStatisticExtRow> {
        backend_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|catcache| catcache.statistic_ext_rows())
            .unwrap_or_default()
    }

    fn statistic_ext_data_rows(&self) -> Vec<PgStatisticExtDataRow> {
        backend_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|catcache| catcache.statistic_ext_data_rows())
            .unwrap_or_default()
    }

    fn statistic_ext_data_row(
        &self,
        stxoid: u32,
        stxdinherit: bool,
    ) -> Option<PgStatisticExtDataRow> {
        statistic_ext_data_row(&self.db, self.client_id, self.txn_ctx, stxoid, stxdinherit)
    }

    fn foreign_data_wrapper_rows(&self) -> Vec<crate::include::catalog::PgForeignDataWrapperRow> {
        backend_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|catcache| catcache.foreign_data_wrapper_rows())
            .unwrap_or_default()
    }

    fn foreign_server_rows(&self) -> Vec<crate::include::catalog::PgForeignServerRow> {
        backend_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|catcache| catcache.foreign_server_rows())
            .unwrap_or_default()
    }

    fn foreign_table_rows(&self) -> Vec<crate::include::catalog::PgForeignTableRow> {
        backend_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|catcache| catcache.foreign_table_rows())
            .unwrap_or_default()
    }

    fn user_mapping_rows(&self) -> Vec<crate::include::catalog::PgUserMappingRow> {
        backend_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|catcache| catcache.user_mapping_rows())
            .unwrap_or_default()
    }

    fn pg_tables_rows(&self) -> Vec<Vec<Value>> {
        let authids = self
            .db
            .auth_catalog(self.client_id, self.txn_ctx)
            .map(|catalog| catalog.roles().to_vec())
            .unwrap_or_default();
        build_pg_tables_rows(
            ensure_namespace_rows(&self.db, self.client_id, self.txn_ctx),
            authids,
            ensure_class_rows(&self.db, self.client_id, self.txn_ctx),
        )
    }

    fn pg_views_rows(&self) -> Vec<Vec<Value>> {
        let authids = self
            .db
            .auth_catalog(self.client_id, self.txn_ctx)
            .map(|catalog| catalog.roles().to_vec())
            .unwrap_or_default();
        build_pg_views_rows_with_definition_formatter(
            ensure_namespace_rows(&self.db, self.client_id, self.txn_ctx),
            authids,
            ensure_class_rows(&self.db, self.client_id, self.txn_ctx),
            ensure_rewrite_rows(&self.db, self.client_id, self.txn_ctx),
            |class, definition| {
                self.relation_by_oid(class.oid)
                    .and_then(|relation| {
                        format_view_definition(class.oid, &relation.desc, self).ok()
                    })
                    .unwrap_or_else(|| definition.to_string())
            },
        )
    }

    fn pg_indexes_rows(&self) -> Vec<Vec<Value>> {
        build_pg_indexes_rows(
            ensure_namespace_rows(&self.db, self.client_id, self.txn_ctx),
            ensure_class_rows(&self.db, self.client_id, self.txn_ctx),
            ensure_attribute_rows(&self.db, self.client_id, self.txn_ctx),
            ensure_index_rows(&self.db, self.client_id, self.txn_ctx),
            ensure_am_rows(&self.db, self.client_id, self.txn_ctx),
        )
    }

    fn pg_matviews_rows(&self) -> Vec<Vec<Value>> {
        let authids = self
            .db
            .auth_catalog(self.client_id, self.txn_ctx)
            .map(|catalog| catalog.roles().to_vec())
            .unwrap_or_default();
        build_pg_matviews_rows(
            ensure_namespace_rows(&self.db, self.client_id, self.txn_ctx),
            authids,
            ensure_class_rows(&self.db, self.client_id, self.txn_ctx),
            ensure_index_rows(&self.db, self.client_id, self.txn_ctx),
            ensure_rewrite_rows(&self.db, self.client_id, self.txn_ctx),
        )
    }

    fn pg_policies_rows(&self) -> Vec<Vec<Value>> {
        let authids = self
            .db
            .auth_catalog(self.client_id, self.txn_ctx)
            .map(|catalog| catalog.roles().to_vec())
            .unwrap_or_default();
        let policy_rows = visible_catcache(&self.db, self.client_id, self.txn_ctx)
            .map(|catcache| catcache.policy_rows())
            .unwrap_or_default();
        build_pg_policies_rows(
            ensure_namespace_rows(&self.db, self.client_id, self.txn_ctx),
            authids,
            ensure_class_rows(&self.db, self.client_id, self.txn_ctx),
            policy_rows,
        )
    }

    fn pg_rules_rows(&self) -> Vec<Vec<Value>> {
        build_pg_rules_rows(
            ensure_namespace_rows(&self.db, self.client_id, self.txn_ctx),
            ensure_class_rows(&self.db, self.client_id, self.txn_ctx),
            ensure_rewrite_rows(&self.db, self.client_id, self.txn_ctx),
        )
    }

    fn pg_stats_rows(&self) -> Vec<Vec<Value>> {
        let classes = ensure_class_rows(&self.db, self.client_id, self.txn_ctx);
        let class_rows_by_oid = classes
            .iter()
            .map(|row| (row.oid, row))
            .collect::<BTreeMap<_, _>>();
        let statistics = ensure_statistic_rows(&self.db, self.client_id, self.txn_ctx)
            .into_iter()
            .filter(|stat| {
                let Some(class) = class_rows_by_oid.get(&stat.starelid) else {
                    return false;
                };
                !class.relrowsecurity
                    || !relation_row_security_is_enabled_for_user(
                        class.oid,
                        self.current_user_oid(),
                        self,
                    )
                    .unwrap_or(true)
            })
            .collect();
        build_pg_stats_rows(
            ensure_namespace_rows(&self.db, self.client_id, self.txn_ctx),
            classes,
            ensure_attribute_rows(&self.db, self.client_id, self.txn_ctx),
            statistics,
        )
    }

    fn pg_stat_activity_rows(&self) -> Vec<Vec<Value>> {
        self.db.pg_stat_activity_rows()
    }

    fn pg_stat_database_rows(&self) -> Vec<Vec<Value>> {
        let databases = self.database_rows();
        let mut stats = self.db.stats.read().clone();
        // :HACK: Until pgrust has PostgreSQL's backend stats collector lifecycle,
        // use the monotonic wire client id as a lower bound for database session
        // starts so reconnect checks observe the newly-created backend.
        stats.database_sessions = stats.database_sessions.max(i64::from(self.client_id));
        build_pg_stat_database_rows(databases, &stats)
    }

    fn pg_stat_checkpointer_rows(&self) -> Vec<Vec<Value>> {
        let stats = self.db.stats.read().clone();
        let checkpoint = self.db.checkpoint_stats_snapshot();
        build_pg_stat_checkpointer_rows(&checkpoint, &stats)
    }

    fn pg_stat_wal_rows(&self) -> Vec<Vec<Value>> {
        build_pg_stat_wal_rows(&self.db.stats.read())
    }

    fn pg_stat_slru_rows(&self) -> Vec<Vec<Value>> {
        build_pg_stat_slru_rows(&self.db.stats.read())
    }

    fn pg_stat_archiver_rows(&self) -> Vec<Vec<Value>> {
        build_pg_stat_archiver_rows(&self.db.stats.read())
    }

    fn pg_stat_bgwriter_rows(&self) -> Vec<Vec<Value>> {
        build_pg_stat_bgwriter_rows(&self.db.stats.read())
    }

    fn pg_stat_recovery_prefetch_rows(&self) -> Vec<Vec<Value>> {
        build_pg_stat_recovery_prefetch_rows(&self.db.stats.read())
    }

    fn pg_stat_subscription_stats_rows(&self) -> Vec<Vec<Value>> {
        self.db
            .object_addresses
            .read()
            .subscriptions
            .iter()
            .filter(|entry| entry.row.subdbid == self.db.database_oid)
            .map(|entry| {
                vec![
                    Value::Int64(i64::from(entry.row.oid)),
                    Value::Text(entry.row.subname.clone().into()),
                    Value::Int64(0),
                    Value::Int64(0),
                    Value::Int64(0),
                    Value::Int64(0),
                    Value::Int64(0),
                    Value::Int64(0),
                    Value::Int64(0),
                    Value::Int64(0),
                    Value::Int64(0),
                    entry
                        .stats_reset
                        .map(Value::TimestampTz)
                        .unwrap_or(Value::Null),
                ]
            })
            .collect()
    }

    fn pg_stat_all_tables_rows(&self) -> Vec<Vec<Value>> {
        let namespaces = ensure_namespace_rows(&self.db, self.client_id, self.txn_ctx);
        let classes = ensure_class_rows(&self.db, self.client_id, self.txn_ctx);
        let indexes = ensure_index_rows(&self.db, self.client_id, self.txn_ctx);
        let relation_oids = classes
            .iter()
            .flat_map(|class| {
                std::iter::once(class.oid)
                    .chain((class.reltoastrelid != 0).then_some(class.reltoastrelid))
            })
            .chain(indexes.iter().map(|row| row.indexrelid))
            .collect::<BTreeSet<_>>();
        let relation_stats = self
            .db
            .session_stats_state(self.client_id)
            .write()
            .visible_relation_entries(&self.db.stats, relation_oids);
        let stats = DatabaseStatsStore {
            relations: relation_stats,
            ..DatabaseStatsStore::default()
        };
        build_pg_stat_all_tables_rows(namespaces, classes, indexes, &stats)
    }

    fn pg_stat_user_tables_rows(&self) -> Vec<Vec<Value>> {
        let namespaces = ensure_namespace_rows(&self.db, self.client_id, self.txn_ctx);
        let classes = ensure_class_rows(&self.db, self.client_id, self.txn_ctx);
        let indexes = ensure_index_rows(&self.db, self.client_id, self.txn_ctx);
        let relation_oids = classes
            .iter()
            .flat_map(|class| {
                std::iter::once(class.oid)
                    .chain((class.reltoastrelid != 0).then_some(class.reltoastrelid))
            })
            .chain(indexes.iter().map(|row| row.indexrelid))
            .collect::<BTreeSet<_>>();
        let relation_stats = self
            .db
            .session_stats_state(self.client_id)
            .write()
            .visible_relation_entries(&self.db.stats, relation_oids);
        let stats = DatabaseStatsStore {
            relations: relation_stats,
            ..DatabaseStatsStore::default()
        };
        build_pg_stat_user_tables_rows(namespaces, classes, indexes, &stats)
    }

    fn pg_statio_user_tables_rows(&self) -> Vec<Vec<Value>> {
        let namespaces = ensure_namespace_rows(&self.db, self.client_id, self.txn_ctx);
        let classes = ensure_class_rows(&self.db, self.client_id, self.txn_ctx);
        let indexes = ensure_index_rows(&self.db, self.client_id, self.txn_ctx);
        let relation_oids = classes
            .iter()
            .flat_map(|class| {
                std::iter::once(class.oid)
                    .chain((class.reltoastrelid != 0).then_some(class.reltoastrelid))
            })
            .chain(indexes.iter().map(|row| row.indexrelid))
            .collect::<BTreeSet<_>>();
        let relation_stats = self
            .db
            .session_stats_state(self.client_id)
            .write()
            .visible_relation_entries(&self.db.stats, relation_oids);
        let stats = DatabaseStatsStore {
            relations: relation_stats,
            ..DatabaseStatsStore::default()
        };
        build_pg_statio_user_tables_rows(namespaces, classes, indexes, &stats)
    }

    fn pg_stat_user_functions_rows(&self) -> Vec<Vec<Value>> {
        let namespaces = ensure_namespace_rows(&self.db, self.client_id, self.txn_ctx);
        let procs = ensure_proc_rows(&self.db, self.client_id, self.txn_ctx);
        let function_stats = self
            .db
            .session_stats_state(self.client_id)
            .write()
            .visible_function_entries(&self.db.stats, procs.iter().map(|proc| proc.oid));
        let stats = DatabaseStatsStore {
            functions: function_stats,
            ..DatabaseStatsStore::default()
        };
        build_pg_stat_user_functions_rows(namespaces, procs, &stats)
    }

    fn pg_stat_io_rows(&self) -> Vec<Vec<Value>> {
        let io_stats = self
            .db
            .session_stats_state(self.client_id)
            .write()
            .visible_io_entries(&self.db.stats, default_pg_stat_io_keys());
        let stats = DatabaseStatsStore {
            io: io_stats,
            ..DatabaseStatsStore::default()
        };
        build_pg_stat_io_rows(&stats)
    }

    fn pg_locks_rows(&self) -> Vec<Vec<Value>> {
        // :HACK: `pg_locks` currently reuses the builtin system-view shim path
        // instead of a catalog-backed view or lock-status SRF.
        build_pg_locks_rows(self.db.pg_locks_rows())
    }

    fn index_relations_for_heap(
        &self,
        relation_oid: u32,
    ) -> Vec<crate::backend::parser::BoundIndexRelation> {
        self.index_relations_for_heap_with_cache(relation_oid, &RefCell::new(BTreeMap::new()))
    }

    fn index_relations_for_heap_with_cache(
        &self,
        relation_oid: u32,
        index_expr_cache: &RefCell<BTreeMap<u32, PlannerIndexExprCacheEntry>>,
    ) -> Vec<crate::backend::parser::BoundIndexRelation> {
        let heap_relation = self.relation_by_oid(relation_oid);
        RelationGetIndexList(&self.db, self.client_id, self.txn_ctx, relation_oid)
            .into_iter()
            .filter_map(|index_oid| {
                let entry =
                    relation_entry_by_oid(&self.db, self.client_id, self.txn_ctx, index_oid)?;
                let class = class_row_by_oid(&self.db, self.client_id, self.txn_ctx, index_oid)?;
                crate::backend::parser::bound_index_relation_from_relcache_entry_with_heap_and_cache(
                    class.relname,
                    &entry,
                    self,
                    heap_relation.as_ref(),
                    Some(index_expr_cache),
                )
            })
            .collect()
    }
}
