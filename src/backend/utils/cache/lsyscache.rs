use std::collections::BTreeSet;

use crate::ClientId;
use crate::backend::access::transam::xact::{CommandId, TransactionId};
use crate::backend::catalog::pg_constraint::derived_pg_constraint_rows;
use crate::backend::parser::{BoundRelation, CatalogLookup};
use crate::backend::storage::smgr::{BLCKSZ, ForkNumber, StorageManager};
use crate::backend::utils::cache::catcache::normalize_catalog_name;
use crate::backend::utils::cache::relcache::RelCacheEntry;
use crate::backend::utils::cache::syscache::{
    SysCacheId, SysCacheTuple, backend_catcache, backend_relcache, ensure_am_rows,
    ensure_attribute_rows, ensure_class_rows, ensure_constraint_rows, ensure_index_rows,
    ensure_namespace_rows, ensure_opclass_rows, ensure_proc_rows, ensure_rewrite_rows,
    ensure_statistic_rows, ensure_type_rows, relation_id_get_relation_db,
    search_sys_cache_list1_db, search_sys_cache_list2_db, search_sys_cache_list3_db,
    search_sys_cache1_db, search_sys_cache2_db,
};
use crate::backend::utils::cache::system_views::{
    build_pg_indexes_rows, build_pg_locks_rows, build_pg_matviews_rows, build_pg_policies_rows,
    build_pg_rules_rows, build_pg_stat_io_rows, build_pg_stat_user_functions_rows,
    build_pg_stat_user_tables_rows, build_pg_statio_user_tables_rows, build_pg_stats_rows,
    build_pg_views_rows,
};
use crate::backend::utils::cache::visible_catalog::VisibleCatalog;
use crate::include::access::brin_page::{
    BRIN_PAGE_CONTENT_OFFSET, BrinMetaPageData, brin_is_meta_page,
};
use crate::include::catalog::{
    CONSTRAINT_FOREIGN, PG_CLASS_RELATION_OID, PG_CONSTRAINT_RELATION_OID, PgAggregateRow, PgAmRow,
    PgAmopRow, PgAmprocRow, PgAuthIdRow, PgAuthMembersRow, PgClassRow, PgCollationRow,
    PgConstraintRow, PgEnumRow, PgIndexRow, PgInheritsRow, PgLanguageRow, PgNamespaceRow,
    PgOpclassRow, PgOperatorRow, PgOpfamilyRow, PgProcRow, PgRewriteRow, PgStatisticExtDataRow,
    PgStatisticExtRow, PgStatisticRow, PgTriggerRow, PgTypeRow,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::SqlType;
use crate::pgrust::database::{
    Database, DatabaseStatsStore, TempNamespace, default_pg_stat_io_keys,
};

fn oid_key(oid: u32) -> Value {
    Value::Int64(i64::from(oid))
}

fn catalog_name_key(name: &str) -> Value {
    Value::Text(normalize_catalog_name(name).to_ascii_lowercase().into())
}

fn namespace_row_by_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    name: &str,
) -> Option<crate::include::catalog::PgNamespaceRow> {
    search_sys_cache1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::NamespaceName,
        catalog_name_key(name),
    )
    .ok()?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::Namespace(row) => Some(row),
        _ => None,
    })
}

fn namespace_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<crate::include::catalog::PgNamespaceRow> {
    search_sys_cache1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::NamespaceOid,
        oid_key(oid),
    )
    .ok()?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::Namespace(row) => Some(row),
        _ => None,
    })
}

fn class_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Option<crate::include::catalog::PgClassRow> {
    search_sys_cache1_db(db, client_id, txn_ctx, SysCacheId::RelOid, oid_key(oid))
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
    search_sys_cache2_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::RelNameNsp,
        Value::Text(relname.to_ascii_lowercase().into()),
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
}

fn attribute_rows_for_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<crate::include::catalog::PgAttributeRow> {
    let mut rows = search_sys_cache_list1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::AttrNum,
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
    let mut rows = search_sys_cache_list1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::InheritsRelIdSeqNo,
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
    let mut rows = search_sys_cache_list1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::InheritsParent,
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
    search_sys_cache1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::PartRelId,
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
    search_sys_cache_list1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::ConstraintRelId,
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
    search_sys_cache1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::ConstraintOid,
        oid_key(oid),
    )
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
    let mut rows = search_sys_cache_list2_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::DependReference,
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
    let mut rows = search_sys_cache_list1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::AttrDefault,
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
    search_sys_cache_list1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::TriggerRelidName,
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
    search_sys_cache_list1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::PolicyPolrelidPolname,
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
    search_sys_cache1_db(db, client_id, txn_ctx, SysCacheId::TypeOid, oid_key(oid))
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
    search_sys_cache2_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::TypeNameNsp,
        catalog_name_key(name),
        oid_key(namespace_oid),
    )
    .ok()?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::Type(row)
            if !db.other_session_temp_namespace_oid(client_id, row.typnamespace) =>
        {
            Some(row)
        }
        _ => None,
    })
}

fn dynamic_type_rows_for_search_path(db: &Database, search_path: &[String]) -> Vec<PgTypeRow> {
    let mut rows = db.domain_type_rows_for_search_path(search_path);
    rows.extend(db.enum_type_rows_for_search_path(search_path));
    rows.extend(db.range_type_rows_for_search_path(search_path));
    rows
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
            row.sql_type.kind == sql_type.kind && row.sql_type.is_array == sql_type.is_array
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
    search_sys_cache1_db(db, client_id, txn_ctx, SysCacheId::ProcOid, oid_key(oid))
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
    let mut rows = search_sys_cache_list1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::ProcNameArgsNsp,
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
    let mut rows = search_sys_cache_list3_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::OperNameNsp,
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
    let mut rows = search_sys_cache_list1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::StatRelAttInh,
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
    search_sys_cache1_db(db, client_id, txn_ctx, SysCacheId::StatExtOid, oid_key(oid))
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
    search_sys_cache2_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::StatExtNameNsp,
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
    search_sys_cache_list1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::StatisticExtRelId,
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
    search_sys_cache2_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::StatisticExtDataStxoidInh,
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
            row.prosrc.clone(),
        ))
    });
}

fn aggregate_row_by_fnoid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    aggfnoid: u32,
) -> Option<PgAggregateRow> {
    search_sys_cache1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::AggFnoid,
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
    search_sys_cache1_db(db, client_id, txn_ctx, SysCacheId::LangOid, oid_key(oid))
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
    search_sys_cache1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::LangName,
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
    search_sys_cache1_db(db, client_id, txn_ctx, SysCacheId::OpclassOid, oid_key(oid))
        .ok()?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Opclass(row) => Some(row),
            _ => None,
        })
}

fn opclass_rows_for_am(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    am_oid: u32,
) -> Vec<PgOpclassRow> {
    search_sys_cache_list1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::ClaAmNameNsp,
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
    .unwrap_or_default()
}

pub struct LazyCatalogLookup<'a> {
    pub db: &'a Database,
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
    search_sys_cache1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::AmName,
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
    search_sys_cache1_db(db, client_id, txn_ctx, SysCacheId::AmOid, oid_key(am_oid))
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
    if let Some(row) = opclasses
        .iter()
        .find(|row| row.opcmethod == am_oid && row.opcdefault && row.opcintype == input_type_oid)
    {
        return Some(row.clone());
    }
    let input_type = type_row_by_oid(db, client_id, txn_ctx, input_type_oid)?;
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
    search_sys_cache1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::OpfamilyOid,
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
    search_sys_cache1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::CollOid,
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
    search_sys_cache_list1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::AmopStrategy,
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
    search_sys_cache_list1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::AmprocNum,
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
    search_sys_cache1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::IndexRelId,
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
    search_sys_cache_list1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::IndexIndRelId,
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

pub fn index_relation_oids_for_heap(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    relation_oid: u32,
) -> Vec<u32> {
    relation_get_index_list(db, client_id, txn_ctx, relation_oid)
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

    if let Ok(Some(entry)) = relation_id_get_relation_db(db, client_id, txn_ctx, relation_oid) {
        return (!db.other_session_temp_namespace_oid(client_id, entry.namespace_oid))
            .then_some(entry);
    }

    let entry = backend_relcache(db, client_id, txn_ctx)
        .ok()?
        .get_by_oid(relation_oid)
        .cloned()?;
    (!db.other_session_temp_namespace_oid(client_id, entry.namespace_oid)).then_some(entry)
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

pub fn lookup_any_relation(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    search_path: &[String],
    name: &str,
) -> Option<BoundRelation> {
    let exact = name.to_ascii_lowercase();
    if let Some((schema, relname)) = exact.split_once('.') {
        let schema_name = if schema == "pg_temp" {
            owned_temp_namespace(db, client_id)?.name
        } else {
            schema.to_string()
        };
        let mut relcache = backend_relcache(db, client_id, txn_ctx).ok()?;
        if let Some(temp_namespace) = owned_temp_namespace(db, client_id) {
            for (temp_name, entry) in temp_namespace.tables {
                relcache.insert(temp_name.clone(), entry.entry.clone());
                relcache.insert(
                    format!("{}.{}", temp_namespace.name, temp_name),
                    entry.entry,
                );
            }
        }
        let entry = relcache
            .get_by_name_exact(&format!("{schema_name}.{relname}"))
            .filter(|entry| !db.other_session_temp_namespace_oid(client_id, entry.namespace_oid))?
            .clone();
        return Some(BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            toast: toast_relation_from_entry(db, client_id, txn_ctx, &entry),
            namespace_oid: entry.namespace_oid,
            owner_oid: entry.owner_oid,
            relpersistence: entry.relpersistence,
            relkind: entry.relkind,
            relispopulated: entry.relispopulated,
            desc: entry.desc.clone(),
            relispartition: entry.relispartition,
            relpartbound: entry.relpartbound.clone(),
            partitioned_table: entry.partitioned_table.clone(),
        });
    }

    let normalized = normalize_catalog_name(name).to_ascii_lowercase();
    if let Some(temp) = db
        .temp_relations
        .read()
        .get(&db.temp_backend_id(client_id))
        .and_then(|namespace| {
            namespace
                .tables
                .get(&normalized)
                .map(|entry| entry.entry.clone())
        })
    {
        return Some(BoundRelation {
            rel: temp.rel,
            relation_oid: temp.relation_oid,
            toast: toast_relation_from_entry(db, client_id, txn_ctx, &temp),
            namespace_oid: temp.namespace_oid,
            owner_oid: temp.owner_oid,
            relpersistence: temp.relpersistence,
            relkind: temp.relkind,
            relispopulated: temp.relispopulated,
            desc: temp.desc.clone(),
            relispartition: temp.relispartition,
            relpartbound: temp.relpartbound.clone(),
            partitioned_table: temp.partitioned_table.clone(),
        });
    }

    let mut relcache = backend_relcache(db, client_id, txn_ctx).ok()?;
    if let Some(temp_namespace) = owned_temp_namespace(db, client_id) {
        for (temp_name, entry) in temp_namespace.tables {
            relcache.insert(temp_name.clone(), entry.entry.clone());
            relcache.insert(
                format!("{}.{}", temp_namespace.name, temp_name),
                entry.entry,
            );
        }
    }
    let relcache = relcache.with_search_path(search_path);
    if let Some(entry) = relcache
        .get_by_name(&normalized)
        .filter(|entry| !db.other_session_temp_namespace_oid(client_id, entry.namespace_oid))
    {
        return Some(BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            toast: toast_relation_from_entry(db, client_id, txn_ctx, entry),
            namespace_oid: entry.namespace_oid,
            owner_oid: entry.owner_oid,
            relpersistence: entry.relpersistence,
            relkind: entry.relkind,
            relispopulated: entry.relispopulated,
            desc: entry.desc.clone(),
            relispartition: entry.relispartition,
            relpartbound: entry.relpartbound.clone(),
            partitioned_table: entry.partitioned_table.clone(),
        });
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
    !relation_get_index_list(db, client_id, txn_ctx, relation_oid).is_empty()
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
    let rows = constraint_rows_for_relation_syscache(db, client_id, txn_ctx, relation_oid);
    if !rows.is_empty() {
        return rows;
    }
    let Some(entry) = relation_entry_by_oid(db, client_id, txn_ctx, relation_oid) else {
        return Vec::new();
    };
    let Some(class) = class_row_by_oid(db, client_id, txn_ctx, relation_oid) else {
        return Vec::new();
    };
    derived_pg_constraint_rows(
        relation_oid,
        &class.relname,
        entry.namespace_oid,
        &entry.desc,
    )
}

impl CatalogLookup for LazyCatalogLookup<'_> {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation> {
        lookup_any_relation(
            self.db,
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
        let entry = relation_entry_by_oid(self.db, self.client_id, self.txn_ctx, relation_oid)?;
        Some(BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            toast: toast_relation_from_entry(self.db, self.client_id, self.txn_ctx, &entry),
            namespace_oid: entry.namespace_oid,
            owner_oid: entry.owner_oid,
            relpersistence: entry.relpersistence,
            relkind: entry.relkind,
            relispopulated: entry.relispopulated,
            desc: entry.desc.clone(),
            relispartition: entry.relispartition,
            relpartbound: entry.relpartbound.clone(),
            partitioned_table: entry.partitioned_table.clone(),
        })
    }

    fn operator_by_name_left_right(
        &self,
        name: &str,
        left_type_oid: u32,
        right_type_oid: u32,
    ) -> Option<PgOperatorRow> {
        operator_row_by_name_left_right(
            self.db,
            self.client_id,
            self.txn_ctx,
            name,
            left_type_oid,
            right_type_oid,
        )
    }

    fn operator_by_oid(&self, oid: u32) -> Option<PgOperatorRow> {
        search_sys_cache1_db(
            self.db,
            self.client_id,
            self.txn_ctx,
            SysCacheId::OperOid,
            oid_key(oid),
        )
        .ok()?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::Operator(row) => Some(row),
            _ => None,
        })
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

    fn namespace_row_by_oid(&self, oid: u32) -> Option<PgNamespaceRow> {
        namespace_row_by_oid(self.db, self.client_id, self.txn_ctx, oid)
    }

    fn namespace_rows(&self) -> Vec<PgNamespaceRow> {
        ensure_namespace_rows(self.db, self.client_id, self.txn_ctx)
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
        constraint_rows_for_relation(self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn constraint_row_by_oid(&self, oid: u32) -> Option<PgConstraintRow> {
        constraint_row_by_oid(self.db, self.client_id, self.txn_ctx, oid)
    }

    fn constraint_rows_for_index(&self, index_oid: u32) -> Vec<PgConstraintRow> {
        constraint_rows_for_index(self.db, self.client_id, self.txn_ctx, index_oid)
    }

    fn foreign_key_constraint_rows_referencing_relation(
        &self,
        relation_oid: u32,
    ) -> Vec<PgConstraintRow> {
        foreign_key_constraint_rows_referencing_relation(
            self.db,
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
            self.db,
            self.client_id,
            self.txn_ctx,
            index_oid,
        )
    }

    fn constraint_rows(&self) -> Vec<PgConstraintRow> {
        ensure_constraint_rows(self.db, self.client_id, self.txn_ctx)
    }

    fn proc_rows_by_name(&self, name: &str) -> Vec<PgProcRow> {
        let mut rows = proc_rows_by_name(self.db, self.client_id, self.txn_ctx, name);
        if is_visible_range_proc_name(
            self.db,
            self.client_id,
            self.txn_ctx,
            &self.search_path,
            name,
        ) {
            rows.extend(crate::include::catalog::synthetic_range_proc_rows_by_name(
                name,
                &range_proc_type_rows(self.db, &self.search_path),
                &self.range_rows(),
            ));
        }
        dedup_proc_rows(&mut rows);
        rows
    }

    fn proc_row_by_oid(&self, oid: u32) -> Option<PgProcRow> {
        proc_row_by_oid(self.db, self.client_id, self.txn_ctx, oid).or_else(|| {
            crate::include::catalog::synthetic_range_proc_row_by_oid(
                oid,
                &range_proc_type_rows(self.db, &self.search_path),
                &self.range_rows(),
            )
        })
    }

    fn opclass_rows(&self) -> Vec<PgOpclassRow> {
        ensure_opclass_rows(self.db, self.client_id, self.txn_ctx)
    }

    fn aggregate_by_fnoid(&self, aggfnoid: u32) -> Option<PgAggregateRow> {
        aggregate_row_by_fnoid(self.db, self.client_id, self.txn_ctx, aggfnoid)
    }

    fn type_rows(&self) -> Vec<PgTypeRow> {
        let mut rows = ensure_type_rows(self.db, self.client_id, self.txn_ctx);
        rows.extend(self.db.domain_type_rows_for_search_path(&self.search_path));
        rows.extend(self.db.enum_type_rows_for_search_path(&self.search_path));
        rows.extend(self.db.range_type_rows_for_search_path(&self.search_path));
        rows
    }

    fn type_by_oid(&self, oid: u32) -> Option<PgTypeRow> {
        visible_type_row_by_oid(
            self.db,
            self.client_id,
            self.txn_ctx,
            &self.search_path,
            oid,
        )
    }

    fn type_by_name(&self, name: &str) -> Option<PgTypeRow> {
        visible_type_row_by_name(
            self.db,
            self.client_id,
            self.txn_ctx,
            &self.search_path,
            name,
        )
    }

    fn type_oid_for_sql_type(&self, sql_type: SqlType) -> Option<u32> {
        visible_type_oid_for_sql_type(
            self.db,
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

    fn language_rows(&self) -> Vec<PgLanguageRow> {
        language_rows(self.db, self.client_id, self.txn_ctx)
    }

    fn language_row_by_oid(&self, oid: u32) -> Option<PgLanguageRow> {
        language_row_by_oid(self.db, self.client_id, self.txn_ctx, oid)
    }

    fn language_row_by_name(&self, name: &str) -> Option<PgLanguageRow> {
        language_row_by_name(self.db, self.client_id, self.txn_ctx, name)
    }

    fn rewrite_rows_for_relation(&self, relation_oid: u32) -> Vec<PgRewriteRow> {
        search_sys_cache_list1_db(
            self.db,
            self.client_id,
            self.txn_ctx,
            SysCacheId::RuleRelName,
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

    fn trigger_rows_for_relation(&self, relation_oid: u32) -> Vec<PgTriggerRow> {
        trigger_rows_for_relation(self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn policy_rows_for_relation(
        &self,
        relation_oid: u32,
    ) -> Vec<crate::include::catalog::PgPolicyRow> {
        policy_rows_for_relation(self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn class_row_by_oid(&self, relation_oid: u32) -> Option<PgClassRow> {
        class_row_by_oid(self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn partitioned_table_row(
        &self,
        relation_oid: u32,
    ) -> Option<crate::include::catalog::PgPartitionedTableRow> {
        partitioned_table_row_by_relid(self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn inheritance_parents(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
        inheritance_parent_rows(self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn inheritance_children(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
        inheritance_child_rows(self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn statistic_rows_for_relation(&self, relation_oid: u32) -> Vec<PgStatisticRow> {
        statistic_rows_for_relation(self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn statistic_ext_row_by_oid(&self, oid: u32) -> Option<PgStatisticExtRow> {
        statistic_ext_row_by_oid(self.db, self.client_id, self.txn_ctx, oid)
    }

    fn statistic_ext_row_by_name_namespace(
        &self,
        name: &str,
        namespace_oid: u32,
    ) -> Option<PgStatisticExtRow> {
        statistic_ext_row_by_name_namespace(
            self.db,
            self.client_id,
            self.txn_ctx,
            name,
            namespace_oid,
        )
    }

    fn statistic_ext_rows_for_relation(&self, relation_oid: u32) -> Vec<PgStatisticExtRow> {
        statistic_ext_rows_for_relation(self.db, self.client_id, self.txn_ctx, relation_oid)
    }

    fn statistic_ext_rows(&self) -> Vec<PgStatisticExtRow> {
        backend_catcache(self.db, self.client_id, self.txn_ctx)
            .map(|catcache| catcache.statistic_ext_rows())
            .unwrap_or_default()
    }

    fn statistic_ext_data_rows(&self) -> Vec<PgStatisticExtDataRow> {
        backend_catcache(self.db, self.client_id, self.txn_ctx)
            .map(|catcache| catcache.statistic_ext_data_rows())
            .unwrap_or_default()
    }

    fn statistic_ext_data_row(
        &self,
        stxoid: u32,
        stxdinherit: bool,
    ) -> Option<PgStatisticExtDataRow> {
        statistic_ext_data_row(self.db, self.client_id, self.txn_ctx, stxoid, stxdinherit)
    }

    fn pg_views_rows(&self) -> Vec<Vec<Value>> {
        let authids = self
            .db
            .auth_catalog(self.client_id, self.txn_ctx)
            .map(|catalog| catalog.roles().to_vec())
            .unwrap_or_default();
        build_pg_views_rows(
            ensure_namespace_rows(self.db, self.client_id, self.txn_ctx),
            authids,
            ensure_class_rows(self.db, self.client_id, self.txn_ctx),
            ensure_rewrite_rows(self.db, self.client_id, self.txn_ctx),
        )
    }

    fn pg_indexes_rows(&self) -> Vec<Vec<Value>> {
        build_pg_indexes_rows(
            ensure_namespace_rows(self.db, self.client_id, self.txn_ctx),
            ensure_class_rows(self.db, self.client_id, self.txn_ctx),
            ensure_attribute_rows(self.db, self.client_id, self.txn_ctx),
            ensure_index_rows(self.db, self.client_id, self.txn_ctx),
            ensure_am_rows(self.db, self.client_id, self.txn_ctx),
        )
    }

    fn pg_matviews_rows(&self) -> Vec<Vec<Value>> {
        let authids = self
            .db
            .auth_catalog(self.client_id, self.txn_ctx)
            .map(|catalog| catalog.roles().to_vec())
            .unwrap_or_default();
        build_pg_matviews_rows(
            ensure_namespace_rows(self.db, self.client_id, self.txn_ctx),
            authids,
            ensure_class_rows(self.db, self.client_id, self.txn_ctx),
            ensure_index_rows(self.db, self.client_id, self.txn_ctx),
            ensure_rewrite_rows(self.db, self.client_id, self.txn_ctx),
        )
    }

    fn pg_policies_rows(&self) -> Vec<Vec<Value>> {
        let authids = self
            .db
            .auth_catalog(self.client_id, self.txn_ctx)
            .map(|catalog| catalog.roles().to_vec())
            .unwrap_or_default();
        let policy_rows = visible_catcache(self.db, self.client_id, self.txn_ctx)
            .map(|catcache| catcache.policy_rows())
            .unwrap_or_default();
        build_pg_policies_rows(
            ensure_namespace_rows(self.db, self.client_id, self.txn_ctx),
            authids,
            ensure_class_rows(self.db, self.client_id, self.txn_ctx),
            policy_rows,
        )
    }

    fn pg_rules_rows(&self) -> Vec<Vec<Value>> {
        build_pg_rules_rows(
            ensure_namespace_rows(self.db, self.client_id, self.txn_ctx),
            ensure_class_rows(self.db, self.client_id, self.txn_ctx),
            ensure_rewrite_rows(self.db, self.client_id, self.txn_ctx),
        )
    }

    fn pg_stats_rows(&self) -> Vec<Vec<Value>> {
        build_pg_stats_rows(
            ensure_namespace_rows(self.db, self.client_id, self.txn_ctx),
            ensure_class_rows(self.db, self.client_id, self.txn_ctx),
            ensure_attribute_rows(self.db, self.client_id, self.txn_ctx),
            ensure_statistic_rows(self.db, self.client_id, self.txn_ctx),
        )
    }

    fn pg_stat_activity_rows(&self) -> Vec<Vec<Value>> {
        self.db.pg_stat_activity_rows()
    }

    fn pg_stat_user_tables_rows(&self) -> Vec<Vec<Value>> {
        let namespaces = ensure_namespace_rows(self.db, self.client_id, self.txn_ctx);
        let classes = ensure_class_rows(self.db, self.client_id, self.txn_ctx);
        let indexes = ensure_index_rows(self.db, self.client_id, self.txn_ctx);
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
        let namespaces = ensure_namespace_rows(self.db, self.client_id, self.txn_ctx);
        let classes = ensure_class_rows(self.db, self.client_id, self.txn_ctx);
        let indexes = ensure_index_rows(self.db, self.client_id, self.txn_ctx);
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
        let namespaces = ensure_namespace_rows(self.db, self.client_id, self.txn_ctx);
        let procs = ensure_proc_rows(self.db, self.client_id, self.txn_ctx);
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
        relation_get_index_list(self.db, self.client_id, self.txn_ctx, relation_oid)
            .into_iter()
            .filter_map(|index_oid| {
                let entry =
                    relation_entry_by_oid(self.db, self.client_id, self.txn_ctx, index_oid)?;
                let class =
                    class_row_by_oid(self.db, self.client_id, self.txn_ctx, entry.relation_oid)?;
                crate::backend::parser::bound_index_relation_from_relcache_entry(
                    class.relname,
                    &entry,
                    self,
                )
            })
            .collect()
    }

    fn materialize_visible_catalog(&self) -> Option<VisibleCatalog> {
        let catcache = visible_catcache(self.db, self.client_id, self.txn_ctx)?;
        let mut relcache = backend_relcache(self.db, self.client_id, self.txn_ctx).ok()?;
        if let Some(temp_namespace) = owned_temp_namespace(self.db, self.client_id) {
            for (name, entry) in temp_namespace.tables {
                relcache.insert(name.clone(), entry.entry.clone());
                relcache.insert(format!("{}.{}", temp_namespace.name, name), entry.entry);
            }
        }
        let mut dynamic_type_rows = self.db.domain_type_rows_for_search_path(&self.search_path);
        dynamic_type_rows.extend(self.db.enum_type_rows_for_search_path(&self.search_path));
        dynamic_type_rows.extend(self.db.range_type_rows_for_search_path(&self.search_path));
        Some(
            VisibleCatalog::with_search_path(
                relcache.with_search_path(&self.search_path),
                Some(catcache),
                self.search_path.clone(),
            )
            .with_enum_rows(self.db.enum_rows_for_catalog())
            .with_dynamic_type_rows(dynamic_type_rows),
        )
    }
}
