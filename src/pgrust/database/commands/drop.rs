use super::super::*;
use super::create::{
    aggregate_signature_arg_oids, format_aggregate_signature, resolve_aggregate_proc_rows,
};
use super::dependency_drop::{CatalogDependencyGraph, DropBehavior, ObjectAddress};
use crate::backend::executor::expr_reg::{format_regprocedure_oid_optional, format_type_text};
use crate::backend::parser::{SqlType, parse_type_name, resolve_raw_type_name};
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::cache::syscache::{
    SearchSysCache1, SearchSysCacheList1, SearchSysCacheList2, SysCacheId, SysCacheTuple,
    search_sys_cache1_db,
};
use crate::backend::utils::misc::notices::{push_notice, push_notice_with_detail};
use crate::include::catalog::{
    CONSTRAINT_FOREIGN, DEPENDENCY_AUTO, DEPENDENCY_NORMAL, PG_CLASS_RELATION_OID,
    PG_CONSTRAINT_RELATION_OID, PG_POLICY_RELATION_OID, PG_PROC_RELATION_OID,
    PG_REWRITE_RELATION_OID, PgCastRow, PgConstraintRow, PgPolicyRow, PgProcRow, PgRewriteRow,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::{
    DropAggregateStatement, DropFunctionStatement, DropIndexStatement, DropProcedureStatement,
    DropSchemaStatement,
};
use crate::pgrust::auth::AuthCatalog;
use crate::pgrust::database::ddl::format_sql_type_name;
use crate::pgrust::database::{DomainEntry, save_range_type_entries};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
struct DropForeignKeyConstraintPlan {
    oid: u32,
    relation_oid: u32,
    constraint_name: String,
}

#[derive(Debug, Clone)]
struct DropRulePlan {
    rewrite_oid: u32,
}

#[derive(Debug, Clone)]
struct DropPolicyPlan {
    policy_oid: u32,
    relation_oid: u32,
    policy_name: String,
}

#[derive(Debug, Clone)]
struct DropDomainColumnDependency {
    relation_oid: u32,
    relation_name: String,
    column_name: String,
    attnum: i16,
}

fn expand_drop_function_statement(drop_stmt: &DropFunctionStatement) -> Vec<DropFunctionStatement> {
    let mut statements = Vec::with_capacity(1 + drop_stmt.additional_functions.len());
    statements.push(DropFunctionStatement {
        if_exists: drop_stmt.if_exists,
        schema_name: drop_stmt.schema_name.clone(),
        function_name: drop_stmt.function_name.clone(),
        arg_list_specified: drop_stmt.arg_list_specified,
        arg_types: drop_stmt.arg_types.clone(),
        additional_functions: Vec::new(),
        cascade: drop_stmt.cascade,
    });
    statements.extend(
        drop_stmt
            .additional_functions
            .iter()
            .map(|item| DropFunctionStatement {
                if_exists: drop_stmt.if_exists,
                schema_name: item.schema_name.clone(),
                function_name: item.routine_name.clone(),
                arg_list_specified: !item.arg_types.is_empty(),
                arg_types: item.arg_types.clone(),
                additional_functions: Vec::new(),
                cascade: drop_stmt.cascade,
            }),
    );
    statements
}

#[derive(Debug, Default)]
struct DropDomainPlan {
    explicit_domain_names: BTreeSet<String>,
    domain_keys: BTreeSet<String>,
    domain_oids: BTreeSet<u32>,
    dependent_domains: Vec<(String, String)>,
    dependent_ranges: Vec<(String, String)>,
    dependent_columns: Vec<DropDomainColumnDependency>,
}

fn domain_has_range_dependents_error(type_name: &str, dependent_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("cannot drop type {type_name} because other objects depend on it"),
        detail: Some(format!("type {dependent_name} depends on type {type_name}")),
        hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
        sqlstate: "2BP01",
    }
}

fn domain_type_dependency_error(type_name: &str, detail: String) -> ExecError {
    ExecError::DetailedError {
        message: format!("cannot drop type {type_name} because other objects depend on it"),
        detail: Some(detail),
        hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
        sqlstate: "2BP01",
    }
}

fn drop_domain_sql_type_depends_on_type(sql_type: SqlType, type_oid: u32) -> bool {
    sql_type.type_oid == type_oid
        || sql_type.typrelid == type_oid
        || sql_type.range_subtype_oid == type_oid
        || sql_type.range_multitype_oid == type_oid
        || sql_type.multirange_range_oid == type_oid
}

fn drop_domain_sql_type_depends_on_any(sql_type: SqlType, type_oids: &BTreeSet<u32>) -> bool {
    type_oids
        .iter()
        .any(|type_oid| drop_domain_sql_type_depends_on_type(sql_type, *type_oid))
}

fn drop_domain_cascade_notices(plan: &DropDomainPlan) -> Vec<String> {
    let mut notices = Vec::new();
    for (_, name) in &plan.dependent_domains {
        notices.push(format!("drop cascades to type {name}"));
    }
    for (_, name) in &plan.dependent_ranges {
        notices.push(format!("drop cascades to type {name}"));
    }
    for column in &plan.dependent_columns {
        notices.push(format!(
            "drop cascades to column {} of table {}",
            column.column_name, column.relation_name
        ));
    }
    notices
}

fn drop_proc_rows_depending_on_type(
    catcache: &CatCache,
    type_oid: u32,
    exclude_proc_oid: Option<u32>,
) -> Vec<PgProcRow> {
    let mut rows = catcache
        .proc_rows()
        .into_iter()
        .filter(|row| Some(row.oid) != exclude_proc_oid)
        .filter(|row| {
            row.prorettype == type_oid
                || drop_parse_proc_arg_oids(&row.proargtypes).contains(&type_oid)
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| (row.proname.clone(), row.oid));
    rows.dedup_by_key(|row| row.oid);
    rows
}

fn drop_parse_proc_arg_oids(argtypes: &str) -> Vec<u32> {
    argtypes
        .split_whitespace()
        .filter_map(|part| part.parse::<u32>().ok())
        .collect()
}

fn drop_proc_signature_text(row: &PgProcRow, catalog: &dyn CatalogLookup) -> String {
    let args = drop_parse_proc_arg_oids(&row.proargtypes)
        .into_iter()
        .map(|oid| format_type_text(oid, None, catalog))
        .collect::<Vec<_>>()
        .join(",");
    format!("{}({args})", row.proname)
}

fn drop_format_name(catcache: &CatCache, namespace_oid: u32, object_name: &str) -> String {
    let schema_name = catcache
        .namespace_by_oid(namespace_oid)
        .map(|row| row.nspname.clone())
        .unwrap_or_else(|| "public".to_string());
    let object_name = drop_quote_identifier_if_needed(object_name);
    match schema_name.as_str() {
        "public" | "pg_catalog" => object_name,
        _ => format!(
            "{}.{object_name}",
            drop_quote_identifier_if_needed(&schema_name)
        ),
    }
}

fn drop_quote_identifier_if_needed(identifier: &str) -> String {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        return "\"\"".into();
    };
    let is_simple_start = first == '_' || first.is_ascii_lowercase();
    let is_simple_rest =
        chars.all(|ch| ch == '_' || ch.is_ascii_lowercase() || ch.is_ascii_digit());
    let is_keyword = matches!(identifier, "user");
    if is_simple_start && is_simple_rest && !is_keyword {
        identifier.to_string()
    } else {
        format!("\"{}\"", identifier.replace('"', "\"\""))
    }
}

fn push_missing_relation_notice(
    catalog: &dyn CatalogLookup,
    relation_name: &str,
    object_kind: &str,
) {
    if let Some((schema_name, _)) = relation_name.split_once('.') {
        let schema_name = schema_name.trim_matches('"').replace("\"\"", "\"");
        if !catalog
            .namespace_rows()
            .into_iter()
            .any(|row| row.nspname.eq_ignore_ascii_case(&schema_name))
        {
            push_notice(format!("schema \"{schema_name}\" does not exist, skipping"));
            return;
        }
    }
    let display_name = relation_name.rsplit('.').next().unwrap_or(relation_name);
    let display_name = display_name.trim_matches('"').replace("\"\"", "\"");
    push_notice(format!(
        "{object_kind} \"{display_name}\" does not exist, skipping"
    ));
}

fn cast_drop_notice(
    catalog: &dyn crate::backend::parser::CatalogLookup,
    row: &PgCastRow,
) -> String {
    format!(
        "drop cascades to cast from {} to {}",
        format_type_text(row.castsource, None, catalog),
        format_type_text(row.casttarget, None, catalog)
    )
}

fn cast_dependency_detail(
    catalog: &dyn crate::backend::parser::CatalogLookup,
    row: &PgCastRow,
    function_display: &str,
) -> String {
    format!(
        "cast from {} to {} depends on function {function_display}",
        format_type_text(row.castsource, None, catalog),
        format_type_text(row.casttarget, None, catalog)
    )
}

fn drop_function_display(
    catalog: &dyn crate::backend::parser::CatalogLookup,
    row: &crate::include::catalog::PgProcRow,
) -> String {
    let args = parse_proc_argtype_oids(&row.proargtypes)
        .unwrap_or_default()
        .into_iter()
        .map(|oid| format_type_text(oid, None, catalog))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{}({args})", row.proname)
}

fn catalog_entry_from_bound_relation(
    catcache: &CatCache,
    relation: &crate::backend::parser::BoundRelation,
) -> crate::backend::catalog::CatalogEntry {
    let class = catcache.class_by_oid(relation.relation_oid);
    let row_type_oid = class.map(|row| row.reltype).unwrap_or(0);
    let array_type_oid = if row_type_oid == 0 {
        0
    } else {
        catcache
            .type_by_oid(row_type_oid)
            .map(|row| row.typarray)
            .unwrap_or(0)
    };
    crate::backend::catalog::CatalogEntry {
        rel: relation.rel,
        relation_oid: relation.relation_oid,
        namespace_oid: relation.namespace_oid,
        owner_oid: relation.owner_oid,
        relacl: class.and_then(|row| row.relacl.clone()),
        reloptions: class.and_then(|row| row.reloptions.clone()),
        of_type_oid: class
            .map(|row| row.reloftype)
            .unwrap_or(relation.of_type_oid),
        row_type_oid,
        array_type_oid,
        reltoastrelid: relation.toast.map(|toast| toast.relation_oid).unwrap_or(0),
        relhasindex: false,
        relpersistence: relation.relpersistence,
        relkind: relation.relkind,
        am_oid: class
            .map(|row| row.relam)
            .unwrap_or_else(|| crate::include::catalog::relam_for_relkind(relation.relkind)),
        relhassubclass: class.map(|row| row.relhassubclass).unwrap_or(false),
        relhastriggers: class.map(|row| row.relhastriggers).unwrap_or(false),
        relispartition: relation.relispartition,
        relispopulated: relation.relispopulated,
        relpartbound: relation.relpartbound.clone(),
        relrowsecurity: class.map(|row| row.relrowsecurity).unwrap_or(false),
        relforcerowsecurity: class.map(|row| row.relforcerowsecurity).unwrap_or(false),
        relpages: class.map(|row| row.relpages).unwrap_or(0),
        reltuples: class.map(|row| row.reltuples).unwrap_or(0.0),
        relallvisible: class.map(|row| row.relallvisible).unwrap_or(0),
        relallfrozen: class.map(|row| row.relallfrozen).unwrap_or(0),
        relfrozenxid: class
            .map(|row| row.relfrozenxid)
            .unwrap_or(crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID),
        desc: relation.desc.clone(),
        partitioned_table: relation.partitioned_table.clone(),
        index_meta: None,
    }
}

#[derive(Debug, Clone)]
enum DropTableDependency {
    Relation {
        relation_oid: u32,
        relkind: char,
        is_partition: bool,
        display_name: String,
    },
    ForeignKey {
        relation_oid: u32,
        constraint: DropForeignKeyConstraintPlan,
        relation_display_name: String,
    },
    Rule {
        relation_oid: u32,
        relation_kind: char,
        relation_display_name: String,
        rule: DropRulePlan,
        rule_name: String,
    },
    Policy {
        relation_oid: u32,
        relation_display_name: String,
        policy: DropPolicyPlan,
    },
}

impl DropTableDependency {
    fn blocker_detail(&self, referenced_kind: &'static str, referenced_name: &str) -> String {
        match self {
            Self::Relation {
                relkind,
                display_name,
                ..
            } => format!(
                "{} {display_name} depends on {referenced_kind} {referenced_name}",
                drop_table_relation_kind_name(*relkind)
            ),
            Self::ForeignKey {
                constraint,
                relation_display_name,
                ..
            } => format!(
                "constraint {} on table {relation_display_name} depends on {referenced_kind} {referenced_name}",
                constraint.constraint_name
            ),
            Self::Rule {
                relation_kind,
                relation_display_name,
                rule_name,
                ..
            } => format!(
                "rule {rule_name} on {} {relation_display_name} depends on {referenced_kind} {referenced_name}",
                drop_table_relation_kind_name(*relation_kind)
            ),
            Self::Policy {
                relation_display_name,
                policy,
                ..
            } => format!(
                "policy {} on table {relation_display_name} depends on {referenced_kind} {referenced_name}",
                policy.policy_name
            ),
        }
    }

    fn cascade_notice(&self) -> String {
        match self {
            Self::Relation {
                relkind,
                display_name,
                ..
            } => format!(
                "drop cascades to {} {display_name}",
                drop_table_relation_kind_name(*relkind)
            ),
            Self::ForeignKey {
                constraint,
                relation_display_name,
                ..
            } => format!(
                "drop cascades to constraint {} on table {relation_display_name}",
                constraint.constraint_name
            ),
            Self::Rule {
                relation_kind,
                relation_display_name,
                rule_name,
                ..
            } => format!(
                "drop cascades to rule {rule_name} on {} {relation_display_name}",
                drop_table_relation_kind_name(*relation_kind)
            ),
            Self::Policy {
                relation_display_name,
                policy,
                ..
            } => format!(
                "drop cascades to policy {} on table {relation_display_name}",
                policy.policy_name
            ),
        }
    }

    fn sort_key(&self) -> (u8, u32, String) {
        match self {
            Self::Relation {
                relation_oid,
                display_name,
                ..
            } => (0, *relation_oid, display_name.clone()),
            Self::ForeignKey {
                constraint,
                relation_display_name,
                ..
            } => (
                1,
                constraint.oid,
                format!("{relation_display_name}:{}", constraint.constraint_name),
            ),
            Self::Rule {
                rule,
                relation_display_name,
                rule_name,
                ..
            } => (
                2,
                rule.rewrite_oid,
                format!("{relation_display_name}:{rule_name}"),
            ),
            Self::Policy {
                policy,
                relation_display_name,
                ..
            } => (
                3,
                0,
                format!("{relation_display_name}:{}", policy.policy_name),
            ),
        }
    }
}

#[derive(Debug, Default)]
struct DropTablePlan {
    relation_drop_order: Vec<u32>,
    relation_drop_oids: BTreeSet<u32>,
    constraint_drop_oids: BTreeSet<u32>,
    constraint_drops: Vec<DropForeignKeyConstraintPlan>,
    rule_drop_oids: BTreeSet<u32>,
    rule_drops: Vec<DropRulePlan>,
    policy_drop_oids: BTreeSet<u32>,
    policy_drops: Vec<DropPolicyPlan>,
    blocker_details: Vec<String>,
    blocker_source: Option<(char, String)>,
    notices: Vec<String>,
}

struct DropTableDependencyContext<'a> {
    catalog: &'a dyn CatalogLookup,
    graph: &'a CatalogDependencyGraph,
    constraints_by_oid: BTreeMap<u32, PgConstraintRow>,
    rewrites_by_oid: BTreeMap<u32, PgRewriteRow>,
    policies_by_oid: BTreeMap<u32, PgPolicyRow>,
    search_path: &'a [String],
}

fn is_drop_table_relkind(relkind: char) -> bool {
    matches!(relkind, 'r' | 'p')
}

fn is_drop_foreign_table_relkind(relkind: char) -> bool {
    relkind == 'f'
}

fn drop_table_relation_kind_name(relkind: char) -> &'static str {
    match relkind {
        'c' => "type",
        'f' => "foreign table",
        'm' => "materialized view",
        'p' => "table",
        'S' => "sequence",
        'v' => "view",
        _ => "table",
    }
}

fn drop_schema_relation_drop_priority(relkind: char) -> u8 {
    match relkind {
        'f' | 'r' | 'p' | 'm' | 'S' => 0,
        'v' => 1,
        'c' => 2,
        'i' | 'I' | 't' => 3,
        _ => 4,
    }
}

fn owned_sequence_owner_relation_oid(catcache: &CatCache, sequence_oid: u32) -> Option<u32> {
    catcache.depend_rows().into_iter().find_map(|row| {
        (row.classid == PG_CLASS_RELATION_OID
            && row.objid == sequence_oid
            && row.objsubid == 0
            && row.refclassid == PG_CLASS_RELATION_OID
            && row.refobjsubid > 0
            && row.deptype == DEPENDENCY_AUTO)
            .then_some(row.refobjid)
    })
}

fn sequence_is_owned_by_relation_in_schema(
    catcache: &CatCache,
    sequence_oid: u32,
    schema_oid: u32,
) -> bool {
    owned_sequence_owner_relation_oid(catcache, sequence_oid)
        .and_then(|relation_oid| catcache.class_by_oid(relation_oid))
        .is_some_and(|row| row.relnamespace == schema_oid)
}

fn partition_has_parent_in_schema(catcache: &CatCache, relation_oid: u32, schema_oid: u32) -> bool {
    catcache
        .inherit_rows()
        .into_iter()
        .filter(|row| row.inhrelid == relation_oid)
        .filter_map(|row| catcache.class_by_oid(row.inhparent))
        .any(|parent| parent.relnamespace == schema_oid)
}

fn drop_table_display_relation_name(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    search_path: &[String],
) -> String {
    let Some(class) = catalog.class_row_by_oid(relation_oid) else {
        return relation_oid.to_string();
    };
    let schema_name = catalog
        .namespace_row_by_oid(class.relnamespace)
        .map(|row| row.nspname.clone())
        .unwrap_or_else(|| "public".to_string());
    match schema_name.as_str() {
        "public" | "pg_catalog" => class.relname.clone(),
        schema_name if schema_name.starts_with("pg_temp_") => class.relname.clone(),
        schema_name if search_path.iter().any(|entry| entry == schema_name) => class.relname,
        _ => format!("{schema_name}.{}", class.relname),
    }
}

fn drop_oid_key(oid: u32) -> Value {
    Value::Int64(i64::from(oid))
}

fn drop_dependents_for_reference(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    referenced: ObjectAddress,
) -> Vec<crate::include::catalog::PgDependRow> {
    SearchSysCacheList2(
        db,
        client_id,
        txn_ctx,
        SysCacheId::DEPENDREFERENCE,
        drop_oid_key(referenced.classid),
        drop_oid_key(referenced.objid),
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Depend(row) if row.refobjsubid == referenced.objsubid => Some(row),
                _ => None,
            })
            .collect()
    })
    .unwrap_or_default()
}

fn drop_inheritance_children(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    parent_oid: u32,
) -> Vec<crate::include::catalog::PgInheritsRow> {
    SearchSysCacheList1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::INHPARENT,
        drop_oid_key(parent_oid),
    )
    .map(|tuples| {
        tuples
            .into_iter()
            .filter_map(|tuple| match tuple {
                SysCacheTuple::Inherits(row) => Some(row),
                _ => None,
            })
            .collect()
    })
    .unwrap_or_default()
}

fn drop_constraint_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    oid: u32,
) -> Option<PgConstraintRow> {
    SearchSysCache1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::CONSTROID,
        drop_oid_key(oid),
    )
    .ok()?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::Constraint(row) => Some(row),
        _ => None,
    })
}

fn drop_rewrite_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    oid: u32,
) -> Option<PgRewriteRow> {
    SearchSysCache1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::REWRITEOID,
        drop_oid_key(oid),
    )
    .ok()?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::Rewrite(row) => Some(row),
        _ => None,
    })
}

fn drop_policy_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    oid: u32,
) -> Option<PgPolicyRow> {
    search_sys_cache1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::PolicyOid,
        drop_oid_key(oid),
    )
    .ok()?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::Policy(row) => Some(row),
        _ => None,
    })
}

fn build_drop_table_dependency_graph(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    relation_oids: &BTreeSet<u32>,
) -> (
    CatalogDependencyGraph,
    BTreeMap<u32, PgConstraintRow>,
    BTreeMap<u32, PgRewriteRow>,
    BTreeMap<u32, PgPolicyRow>,
) {
    let mut queue = relation_oids
        .iter()
        .copied()
        .map(ObjectAddress::relation)
        .collect::<Vec<_>>();
    let mut visited = BTreeSet::new();
    let mut depend_rows = Vec::new();
    let mut inherit_rows = Vec::new();
    let mut constraints_by_oid = BTreeMap::new();
    let mut rewrites_by_oid = BTreeMap::new();
    let mut policies_by_oid = BTreeMap::new();

    while let Some(address) = queue.pop() {
        if !visited.insert(address) {
            continue;
        }

        for row in drop_dependents_for_reference(db, client_id, txn_ctx, address) {
            if row.objsubid == 0 && row.classid == PG_CLASS_RELATION_OID {
                queue.push(ObjectAddress::relation(row.objid));
            } else if row.classid == PG_CONSTRAINT_RELATION_OID {
                if let std::collections::btree_map::Entry::Vacant(entry) =
                    constraints_by_oid.entry(row.objid)
                    && let Some(constraint) =
                        drop_constraint_row_by_oid(db, client_id, txn_ctx, row.objid)
                {
                    entry.insert(constraint);
                }
            } else if row.classid == PG_REWRITE_RELATION_OID {
                if let std::collections::btree_map::Entry::Vacant(entry) =
                    rewrites_by_oid.entry(row.objid)
                    && let Some(rewrite) =
                        drop_rewrite_row_by_oid(db, client_id, txn_ctx, row.objid)
                {
                    queue.push(ObjectAddress::relation(rewrite.ev_class));
                    entry.insert(rewrite);
                }
            } else if row.classid == PG_POLICY_RELATION_OID
                && let std::collections::btree_map::Entry::Vacant(entry) =
                    policies_by_oid.entry(row.objid)
                && let Some(policy) = drop_policy_row_by_oid(db, client_id, txn_ctx, row.objid)
            {
                entry.insert(policy);
            }
            depend_rows.push(row);
        }

        if address.classid == PG_CLASS_RELATION_OID && address.objsubid == 0 {
            for row in drop_inheritance_children(db, client_id, txn_ctx, address.objid) {
                queue.push(ObjectAddress::relation(row.inhrelid));
                inherit_rows.push(row);
            }
        }
    }

    (
        CatalogDependencyGraph::from_rows(depend_rows, inherit_rows),
        constraints_by_oid,
        rewrites_by_oid,
        policies_by_oid,
    )
}

fn drop_schema_visible_namespace_oids(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    configured_search_path: Option<&[String]>,
    auth_catalog: &AuthCatalog,
) -> BTreeSet<u32> {
    let mut search_path = db.effective_search_path(client_id, configured_search_path);
    let includes_user_schema = configured_search_path
        .map(|path| {
            path.iter()
                .any(|schema| schema.trim().eq_ignore_ascii_case("$user"))
        })
        .unwrap_or(true);
    if includes_user_schema
        && let Some(current_role) =
            auth_catalog.role_by_oid(db.auth_state(client_id).current_user_oid())
    {
        let current_schema = current_role.rolname.to_ascii_lowercase();
        if !search_path.iter().any(|schema| schema == &current_schema) {
            search_path.push(current_schema);
        }
    }
    search_path
        .into_iter()
        .filter_map(|schema| db.visible_namespace_oid_by_name(client_id, txn_ctx, &schema))
        .collect()
}

fn drop_schema_display_object_name(
    catcache: &CatCache,
    visible_namespaces: &BTreeSet<u32>,
    namespace_oid: u32,
    object_name: &str,
) -> String {
    if visible_namespaces.contains(&namespace_oid) {
        return drop_quote_identifier_if_needed(object_name);
    }
    drop_format_name(catcache, namespace_oid, object_name)
}

fn drop_schema_display_relation_name(
    catcache: &CatCache,
    visible_namespaces: &BTreeSet<u32>,
    namespace_oid: u32,
    relation_name: &str,
) -> String {
    if visible_namespaces.contains(&namespace_oid) {
        return drop_quote_identifier_if_needed(relation_name);
    }
    drop_format_name(catcache, namespace_oid, relation_name)
}

fn drop_schema_display_signature_name(
    catcache: &CatCache,
    visible_namespaces: &BTreeSet<u32>,
    namespace_oid: u32,
    signature: &str,
) -> String {
    if visible_namespaces.contains(&namespace_oid) {
        return signature.to_string();
    }
    let schema_name = catcache
        .namespace_by_oid(namespace_oid)
        .map(|row| row.nspname.clone())
        .unwrap_or_else(|| "public".to_string());
    match schema_name.as_str() {
        "public" | "pg_catalog" => signature.to_string(),
        _ => format!(
            "{}.{signature}",
            drop_quote_identifier_if_needed(&schema_name)
        ),
    }
}

fn drop_schema_display_operator_name(
    catalog: &dyn CatalogLookup,
    catcache: &CatCache,
    visible_namespaces: &BTreeSet<u32>,
    row: &crate::include::catalog::PgOperatorRow,
) -> String {
    let args = [row.oprleft, row.oprright]
        .into_iter()
        .map(|oid| format_type_text(oid, None, catalog))
        .collect::<Vec<_>>()
        .join(",");
    let name = format!("{}({args})", row.oprname);
    drop_schema_display_signature_name(catcache, visible_namespaces, row.oprnamespace, &name)
}

fn parse_proc_argtype_oids(argtypes: &str) -> Option<Vec<u32>> {
    if argtypes.trim().is_empty() {
        return Some(Vec::new());
    }
    argtypes
        .split_whitespace()
        .map(|oid| oid.parse::<u32>().ok())
        .collect()
}

fn strip_leading_sql_word(input: &str) -> Option<&str> {
    let trimmed = input.trim_start();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with('"') {
        let mut chars = trimmed.char_indices().skip(1);
        while let Some((idx, ch)) = chars.next() {
            if ch == '"' {
                if trimmed[idx + 1..].starts_with('"') {
                    chars.next();
                    continue;
                }
                return Some(trimmed[idx + 1..].trim_start());
            }
        }
        return None;
    }
    let end = trimmed
        .char_indices()
        .find(|(_, ch)| ch.is_whitespace())
        .map(|(idx, _)| idx)
        .unwrap_or(trimmed.len());
    Some(trimmed[end..].trim_start())
}

fn drop_function_arg_type_oid(
    arg: &str,
    catalog: &dyn crate::backend::parser::CatalogLookup,
) -> Result<Option<u32>, ParseError> {
    let mut text = arg.trim();
    if let Some((rest, callable)) = strip_drop_function_arg_mode(text) {
        if !callable {
            return Ok(None);
        }
        text = rest;
    }

    let raw_type = match parse_type_name(text).and_then(|raw_type| {
        resolve_raw_type_name(&raw_type, catalog).map(|sql_type| (raw_type, sql_type))
    }) {
        Ok((raw_type, _)) => raw_type,
        Err(first_err) => {
            let Some(rest) = strip_leading_sql_word(text) else {
                return Err(first_err);
            };
            let rest = strip_drop_function_arg_mode(rest)
                .map(|(rest, _)| rest)
                .unwrap_or(rest);
            parse_type_name(rest)?
        }
    };
    let sql_type = resolve_raw_type_name(&raw_type, catalog)?;
    catalog
        .type_oid_for_sql_type(sql_type)
        .or_else(|| {
            matches!(sql_type.kind, crate::backend::parser::SqlTypeKind::Record)
                .then_some(crate::include::catalog::RECORD_TYPE_OID)
        })
        .map(Some)
        .ok_or_else(|| ParseError::UnsupportedType(arg.to_string()))
}

fn strip_drop_function_arg_mode(text: &str) -> Option<(&str, bool)> {
    let trimmed = text.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    for (mode, callable) in [
        ("inout", true),
        ("variadic", true),
        ("in", true),
        ("out", false),
    ] {
        if lower == mode || lower.starts_with(&format!("{mode} ")) {
            return Some((trimmed[mode.len()..].trim_start(), callable));
        }
    }
    None
}

#[derive(Debug, Clone, Copy)]
struct DropRoutineArgSpec {
    mode: Option<u8>,
    type_oid: u32,
}

fn drop_routine_signature_display(
    name: &str,
    specs: &[DropRoutineArgSpec],
    catalog: &dyn crate::backend::parser::CatalogLookup,
) -> String {
    let args = specs
        .iter()
        .map(|spec| format_type_text(spec.type_oid, None, catalog))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{name}({args})")
}

fn drop_routine_arg_spec(
    arg: &str,
    catalog: &dyn crate::backend::parser::CatalogLookup,
) -> Result<DropRoutineArgSpec, ParseError> {
    let mut text = arg.trim();
    let mut parsed_mode = None;
    if let Some((rest, mode)) = strip_drop_routine_arg_mode(text) {
        parsed_mode = Some(mode);
        text = rest;
    }

    let raw_type = match parse_type_name(text).and_then(|raw_type| {
        resolve_raw_type_name(&raw_type, catalog).map(|sql_type| (raw_type, sql_type))
    }) {
        Ok((raw_type, _)) => raw_type,
        Err(first_err) => {
            let Some(rest) = strip_leading_sql_word(text) else {
                return Err(first_err);
            };
            let rest = if let Some((rest, mode)) = strip_drop_routine_arg_mode(rest) {
                parsed_mode = Some(mode);
                rest
            } else {
                rest
            };
            parse_type_name(rest)?
        }
    };
    let sql_type = resolve_raw_type_name(&raw_type, catalog)?;
    let type_oid = catalog
        .type_oid_for_sql_type(sql_type)
        .or_else(|| {
            matches!(sql_type.kind, crate::backend::parser::SqlTypeKind::Record)
                .then_some(crate::include::catalog::RECORD_TYPE_OID)
        })
        .ok_or_else(|| ParseError::UnsupportedType(arg.to_string()))?;
    Ok(DropRoutineArgSpec {
        mode: parsed_mode,
        type_oid,
    })
}

fn strip_drop_routine_arg_mode(text: &str) -> Option<(&str, u8)> {
    let trimmed = text.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    for (mode, code) in [
        ("inout", b'b'),
        ("variadic", b'v'),
        ("in", b'i'),
        ("out", b'o'),
    ] {
        if lower == mode || lower.starts_with(&format!("{mode} ")) {
            return Some((trimmed[mode.len()..].trim_start(), code));
        }
    }
    None
}

fn drop_table_direct_dependencies(
    ctx: &DropTableDependencyContext<'_>,
    relation_oid: u32,
) -> Vec<DropTableDependency> {
    let mut relation_oids = BTreeSet::new();
    let mut constraint_oids = BTreeSet::new();
    let mut rule_oids = BTreeSet::new();
    let mut policy_oids = BTreeSet::new();
    let mut deps = Vec::new();

    for row in ctx.graph.dependents(ObjectAddress::relation(relation_oid)) {
        if row.objsubid != 0 {
            continue;
        }
        match row.classid {
            PG_CLASS_RELATION_OID if row.deptype == DEPENDENCY_NORMAL => {
                if !relation_oids.insert(row.objid) {
                    continue;
                }
                let Some(class) = ctx.catalog.class_row_by_oid(row.objid) else {
                    continue;
                };
                if !matches!(class.relkind, 'r' | 'p' | 'S' | 'v' | 'm') {
                    continue;
                }
                deps.push(DropTableDependency::Relation {
                    relation_oid: row.objid,
                    relkind: class.relkind,
                    is_partition: class.relispartition,
                    display_name: drop_table_display_relation_name(
                        ctx.catalog,
                        row.objid,
                        ctx.search_path,
                    ),
                });
            }
            PG_CONSTRAINT_RELATION_OID if row.deptype == DEPENDENCY_NORMAL => {
                let Some(constraint) = ctx.constraints_by_oid.get(&row.objid) else {
                    continue;
                };
                if constraint.contype != CONSTRAINT_FOREIGN
                    || !constraint_oids.insert(constraint.oid)
                {
                    continue;
                }
                deps.push(DropTableDependency::ForeignKey {
                    relation_oid: constraint.conrelid,
                    relation_display_name: drop_table_display_relation_name(
                        ctx.catalog,
                        constraint.conrelid,
                        ctx.search_path,
                    ),
                    constraint: DropForeignKeyConstraintPlan {
                        oid: constraint.oid,
                        relation_oid: constraint.conrelid,
                        constraint_name: constraint.conname.clone(),
                    },
                });
            }
            PG_REWRITE_RELATION_OID if row.deptype == DEPENDENCY_NORMAL => {
                let Some(rewrite) = ctx.rewrites_by_oid.get(&row.objid) else {
                    continue;
                };
                let Some(owner) = ctx.catalog.class_row_by_oid(rewrite.ev_class) else {
                    continue;
                };
                if matches!(owner.relkind, 'v' | 'm') {
                    if !relation_oids.insert(owner.oid) {
                        continue;
                    }
                    deps.push(DropTableDependency::Relation {
                        relation_oid: owner.oid,
                        relkind: owner.relkind,
                        is_partition: owner.relispartition,
                        display_name: drop_table_display_relation_name(
                            ctx.catalog,
                            owner.oid,
                            ctx.search_path,
                        ),
                    });
                } else if rule_oids.insert(rewrite.oid) {
                    deps.push(DropTableDependency::Rule {
                        relation_oid: owner.oid,
                        relation_kind: owner.relkind,
                        relation_display_name: drop_table_display_relation_name(
                            ctx.catalog,
                            owner.oid,
                            ctx.search_path,
                        ),
                        rule: DropRulePlan {
                            rewrite_oid: rewrite.oid,
                        },
                        rule_name: rewrite.rulename.clone(),
                    });
                }
            }
            PG_POLICY_RELATION_OID if row.deptype == DEPENDENCY_NORMAL => {
                let Some(policy) = ctx.policies_by_oid.get(&row.objid) else {
                    continue;
                };
                if !policy_oids.insert(policy.oid) {
                    continue;
                }
                deps.push(DropTableDependency::Policy {
                    relation_oid: policy.polrelid,
                    relation_display_name: drop_table_display_relation_name(
                        ctx.catalog,
                        policy.polrelid,
                        ctx.search_path,
                    ),
                    policy: DropPolicyPlan {
                        policy_oid: policy.oid,
                        relation_oid: policy.polrelid,
                        policy_name: policy.polname.clone(),
                    },
                });
            }
            _ => {}
        }
    }

    for inherit in ctx.graph.inheritance_children(relation_oid) {
        if !relation_oids.insert(inherit.inhrelid) {
            continue;
        }
        let Some(class) = ctx.catalog.class_row_by_oid(inherit.inhrelid) else {
            continue;
        };
        if !matches!(class.relkind, 'r' | 'p') {
            continue;
        }
        deps.push(DropTableDependency::Relation {
            relation_oid: inherit.inhrelid,
            relkind: class.relkind,
            is_partition: class.relispartition,
            display_name: drop_table_display_relation_name(
                ctx.catalog,
                inherit.inhrelid,
                ctx.search_path,
            ),
        });
    }

    deps.sort_by_key(DropTableDependency::sort_key);
    deps
}

fn record_drop_table_blocker(
    plan: &mut DropTablePlan,
    source_relkind: char,
    source_name: String,
    detail: String,
) {
    if plan.blocker_source.is_none() {
        plan.blocker_source = Some((source_relkind, source_name));
    }
    if !plan.blocker_details.contains(&detail) {
        plan.blocker_details.push(detail);
    }
}

fn relation_in_explicit_drop_subtree(
    ctx: &DropTableDependencyContext<'_>,
    relation_oid: u32,
    explicit_relation_oids: &BTreeSet<u32>,
) -> bool {
    if explicit_relation_oids.contains(&relation_oid) {
        return true;
    }
    let mut stack = explicit_relation_oids.iter().copied().collect::<Vec<_>>();
    let mut visited = BTreeSet::new();
    while let Some(parent_oid) = stack.pop() {
        if !visited.insert(parent_oid) {
            continue;
        }
        for inherit in ctx.graph.inheritance_children(parent_oid) {
            if inherit.inhrelid == relation_oid {
                return true;
            }
            stack.push(inherit.inhrelid);
        }
    }
    false
}

fn collect_drop_table_restrict_blockers(
    ctx: &DropTableDependencyContext<'_>,
    relation_oid: u32,
    explicit_relation_oids: &BTreeSet<u32>,
    plan: &mut DropTablePlan,
    visited: &mut BTreeSet<u32>,
) {
    if !visited.insert(relation_oid) {
        return;
    }

    let Some(class) = ctx.catalog.class_row_by_oid(relation_oid) else {
        return;
    };
    let source_relkind = class.relkind;
    let source_name = drop_table_display_relation_name(ctx.catalog, relation_oid, ctx.search_path);
    let referenced_kind = drop_table_relation_kind_name(source_relkind);

    for dep in drop_table_direct_dependencies(ctx, relation_oid) {
        match dep {
            DropTableDependency::Relation {
                relation_oid: dependent_oid,
                is_partition,
                ..
            } => {
                if is_partition || explicit_relation_oids.contains(&dependent_oid) {
                    collect_drop_table_restrict_blockers(
                        ctx,
                        dependent_oid,
                        explicit_relation_oids,
                        plan,
                        visited,
                    );
                    continue;
                }
                record_drop_table_blocker(
                    plan,
                    source_relkind,
                    source_name.clone(),
                    dep.blocker_detail(referenced_kind, &source_name),
                );
                collect_drop_table_restrict_blockers(
                    ctx,
                    dependent_oid,
                    explicit_relation_oids,
                    plan,
                    visited,
                );
            }
            DropTableDependency::ForeignKey {
                relation_oid: dependent_relation_oid,
                ..
            }
            | DropTableDependency::Rule {
                relation_oid: dependent_relation_oid,
                ..
            }
            | DropTableDependency::Policy {
                relation_oid: dependent_relation_oid,
                ..
            } => {
                if relation_in_explicit_drop_subtree(
                    ctx,
                    dependent_relation_oid,
                    explicit_relation_oids,
                ) {
                    continue;
                }
                record_drop_table_blocker(
                    plan,
                    source_relkind,
                    source_name.clone(),
                    dep.blocker_detail(referenced_kind, &source_name),
                );
            }
        }
    }
}

fn plan_drop_table_relation(
    ctx: &DropTableDependencyContext<'_>,
    relation_oid: u32,
    explicit_relation_oids: &BTreeSet<u32>,
    behavior: DropBehavior,
    plan: &mut DropTablePlan,
) {
    if !plan.relation_drop_oids.insert(relation_oid) {
        return;
    }

    let Some(class) = ctx.catalog.class_row_by_oid(relation_oid) else {
        return;
    };
    let source_relkind = class.relkind;
    let source_name = drop_table_display_relation_name(ctx.catalog, relation_oid, ctx.search_path);
    let referenced_kind = drop_table_relation_kind_name(source_relkind);

    for dep in drop_table_direct_dependencies(ctx, relation_oid) {
        match dep {
            DropTableDependency::Relation {
                relation_oid: dependent_oid,
                is_partition,
                ..
            } => {
                if is_partition
                    || explicit_relation_oids.contains(&dependent_oid)
                    || plan.relation_drop_oids.contains(&dependent_oid)
                {
                    plan_drop_table_relation(
                        ctx,
                        dependent_oid,
                        explicit_relation_oids,
                        behavior,
                        plan,
                    );
                    continue;
                }
                if behavior.is_cascade() {
                    plan.notices.push(dep.cascade_notice());
                    plan_drop_table_relation(
                        ctx,
                        dependent_oid,
                        explicit_relation_oids,
                        behavior,
                        plan,
                    );
                } else {
                    record_drop_table_blocker(
                        plan,
                        source_relkind,
                        source_name.clone(),
                        dep.blocker_detail(referenced_kind, &source_name),
                    );
                    let mut visited = BTreeSet::new();
                    visited.insert(relation_oid);
                    collect_drop_table_restrict_blockers(
                        ctx,
                        dependent_oid,
                        explicit_relation_oids,
                        plan,
                        &mut visited,
                    );
                }
            }
            DropTableDependency::ForeignKey {
                relation_oid: dependent_relation_oid,
                ref constraint,
                ..
            } => {
                if relation_in_explicit_drop_subtree(
                    ctx,
                    dependent_relation_oid,
                    explicit_relation_oids,
                ) || plan.relation_drop_oids.contains(&dependent_relation_oid)
                    || !plan.constraint_drop_oids.insert(constraint.oid)
                {
                    continue;
                }
                if behavior.is_cascade() {
                    plan.notices.push(dep.cascade_notice());
                    plan.constraint_drops.push(constraint.clone());
                } else {
                    record_drop_table_blocker(
                        plan,
                        source_relkind,
                        source_name.clone(),
                        dep.blocker_detail(referenced_kind, &source_name),
                    );
                }
            }
            DropTableDependency::Rule {
                relation_oid: dependent_relation_oid,
                ref rule,
                ..
            } => {
                if relation_in_explicit_drop_subtree(
                    ctx,
                    dependent_relation_oid,
                    explicit_relation_oids,
                ) || plan.relation_drop_oids.contains(&dependent_relation_oid)
                    || !plan.rule_drop_oids.insert(rule.rewrite_oid)
                {
                    continue;
                }
                if behavior.is_cascade() {
                    plan.notices.push(dep.cascade_notice());
                    plan.rule_drops.push(rule.clone());
                } else {
                    record_drop_table_blocker(
                        plan,
                        source_relkind,
                        source_name.clone(),
                        dep.blocker_detail(referenced_kind, &source_name),
                    );
                }
            }
            DropTableDependency::Policy {
                relation_oid: dependent_relation_oid,
                ref policy,
                ..
            } => {
                if explicit_relation_oids.contains(&dependent_relation_oid)
                    || plan.relation_drop_oids.contains(&dependent_relation_oid)
                    || !plan.policy_drop_oids.insert(policy.policy_oid)
                {
                    continue;
                }
                if behavior.is_cascade() {
                    plan.notices.push(dep.cascade_notice());
                    plan.policy_drops.push(policy.clone());
                } else {
                    record_drop_table_blocker(
                        plan,
                        source_relkind,
                        source_name.clone(),
                        dep.blocker_detail(referenced_kind, &source_name),
                    );
                }
            }
        }
    }

    plan.relation_drop_order.push(relation_oid);
}

fn sort_policy_cascade_notices(ctx: &DropTableDependencyContext<'_>, plan: &mut DropTablePlan) {
    if plan.policy_drops.len() < 2 {
        return;
    }

    plan.policy_drops.sort_by_key(|policy| policy.policy_oid);
    let mut sorted_policy_notices = plan
        .policy_drops
        .iter()
        .map(|policy| {
            let relation_display_name =
                drop_table_display_relation_name(ctx.catalog, policy.relation_oid, ctx.search_path);
            format!(
                "drop cascades to policy {} on table {relation_display_name}",
                policy.policy_name
            )
        })
        .collect::<Vec<_>>()
        .into_iter();

    for notice in &mut plan.notices {
        if notice.starts_with("drop cascades to policy ")
            && let Some(sorted_notice) = sorted_policy_notices.next()
        {
            *notice = sorted_notice;
        }
    }
}

impl Database {
    pub(crate) fn execute_drop_aggregate_stmt_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropAggregateStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_aggregate_stmt_in_transaction_with_search_path(
            client_id,
            drop_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_drop_aggregate_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropAggregateStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let txn_ctx = Some((xid, cid));
        let catalog = self.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
        let arg_oids = aggregate_signature_arg_oids(&catalog, &drop_stmt.signature)
            .map_err(ExecError::Parse)?;
        let schema_oid = match &drop_stmt.schema_name {
            Some(schema_name) => Some(
                self.visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("schema \"{schema_name}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    })?,
            ),
            None => None,
        };
        let matches =
            resolve_aggregate_proc_rows(&catalog, &drop_stmt.aggregate_name, schema_oid, &arg_oids);
        let proc_row = match matches.as_slice() {
            [(row, _agg)] => row.clone(),
            [] if drop_stmt.if_exists => {
                push_notice(format!(
                    "aggregate {} does not exist, skipping",
                    format_aggregate_signature(
                        &drop_stmt.aggregate_name,
                        &drop_stmt.signature,
                        &catalog
                    )?
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            [] => {
                let signature =
                    drop_signature_for_oids(&catalog, &drop_stmt.aggregate_name, &arg_oids);
                return Err(ExecError::DetailedError {
                    message: format!("aggregate {signature} does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42883",
                });
            }
            _ => {
                return Err(ExecError::DetailedError {
                    message: format!("aggregate name {} is ambiguous", drop_stmt.aggregate_name),
                    detail: None,
                    hint: None,
                    sqlstate: "42725",
                });
            }
        };
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let (_dropped_row, effect) = self
            .catalog
            .write()
            .drop_proc_by_oid_mvcc(proc_row.oid, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_function_stmt_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropFunctionStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_function_stmt_in_transaction_with_search_path(
            client_id,
            drop_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_drop_function_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropFunctionStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        for item_stmt in expand_drop_function_statement(drop_stmt) {
            self.execute_drop_function_stmt_in_transaction_with_kind(
                client_id,
                &item_stmt,
                xid,
                cid,
                configured_search_path,
                catalog_effects,
                'f',
                "function",
            )?;
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_procedure_stmt_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropProcedureStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_procedure_stmt_in_transaction_with_search_path(
            client_id,
            drop_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_drop_procedure_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropProcedureStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        for procedure in &drop_stmt.procedures {
            let function_stmt = DropFunctionStatement {
                if_exists: drop_stmt.if_exists,
                schema_name: procedure.schema_name.clone(),
                function_name: procedure.routine_name.clone(),
                arg_list_specified: false,
                arg_types: procedure.arg_types.clone(),
                additional_functions: Vec::new(),
                cascade: drop_stmt.cascade,
            };
            self.execute_drop_function_stmt_in_transaction_with_kind(
                client_id,
                &function_stmt,
                xid,
                cid,
                configured_search_path,
                catalog_effects,
                'p',
                "procedure",
            )?;
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_routine_stmt_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropProcedureStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_routine_stmt_in_transaction_with_search_path(
            client_id,
            drop_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_drop_routine_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropProcedureStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        for routine in &drop_stmt.procedures {
            let function_stmt = DropFunctionStatement {
                if_exists: drop_stmt.if_exists,
                schema_name: routine.schema_name.clone(),
                function_name: routine.routine_name.clone(),
                arg_list_specified: false,
                arg_types: routine.arg_types.clone(),
                additional_functions: Vec::new(),
                cascade: drop_stmt.cascade,
            };
            self.execute_drop_function_stmt_in_transaction_with_kind(
                client_id,
                &function_stmt,
                xid,
                cid,
                configured_search_path,
                catalog_effects,
                'r',
                "routine",
            )?;
        }
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_drop_function_stmt_with_kind(
        &self,
        client_id: ClientId,
        drop_stmt: &DropFunctionStatement,
        configured_search_path: Option<&[String]>,
        proc_kind: char,
        object_kind: &'static str,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_function_stmt_in_transaction_with_kind(
            client_id,
            drop_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
            proc_kind,
            object_kind,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    fn execute_drop_function_stmt_in_transaction_with_kind(
        &self,
        client_id: ClientId,
        drop_stmt: &DropFunctionStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        proc_kind: char,
        object_kind: &'static str,
    ) -> Result<StatementResult, ExecError> {
        let txn_ctx = Some((xid, cid));
        let catalog = self.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
        let desired_arg_specs = drop_stmt
            .arg_types
            .iter()
            .map(|arg| drop_routine_arg_spec(arg, &catalog))
            .collect::<Result<Vec<_>, _>>()
            .map_err(ExecError::Parse)?;
        let schema_oid = match &drop_stmt.schema_name {
            Some(schema_name) => Some(
                self.visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("schema \"{schema_name}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    })?,
            ),
            None => None,
        };
        let matches = catalog
            .proc_rows_by_name(&drop_stmt.function_name)
            .into_iter()
            .filter(|row| {
                let effective_kind = if proc_kind == 'r' {
                    row.prokind
                } else {
                    proc_kind
                };
                drop_routine_signature_matches(
                    row,
                    &desired_arg_specs,
                    effective_kind,
                    drop_stmt.arg_list_specified,
                ) && (row.prokind == proc_kind
                    || (proc_kind == 'r' && matches!(row.prokind, 'f' | 'p')))
                    && schema_oid
                        .map(|schema_oid| row.pronamespace == schema_oid)
                        .unwrap_or(true)
            })
            .collect::<Vec<_>>();
        let wrong_kind_matches = catalog
            .proc_rows_by_name(&drop_stmt.function_name)
            .into_iter()
            .filter(|row| {
                proc_kind != 'r'
                    && drop_routine_signature_matches(
                        row,
                        &desired_arg_specs,
                        proc_kind,
                        drop_stmt.arg_list_specified,
                    )
                    && row.prokind != proc_kind
                    && schema_oid
                        .map(|schema_oid| row.pronamespace == schema_oid)
                        .unwrap_or(true)
            })
            .collect::<Vec<_>>();
        let display_signature =
            drop_routine_signature_display(&drop_stmt.function_name, &desired_arg_specs, &catalog);
        let proc_row = match matches.as_slice() {
            [row] => row.clone(),
            [] if !wrong_kind_matches.is_empty() => {
                return Err(ExecError::DetailedError {
                    message: format!("{display_signature} is not a {object_kind}"),
                    detail: None,
                    hint: None,
                    sqlstate: "42809",
                });
            }
            [] if drop_stmt.if_exists => {
                push_notice(format!(
                    "{object_kind} {display_signature} does not exist, skipping"
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            [] => {
                return Err(ExecError::DetailedError {
                    message: format!("{object_kind} {display_signature} does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42883",
                });
            }
            _ => {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "{object_kind} name \"{}\" is not unique",
                        drop_stmt.function_name
                    ),
                    detail: None,
                    hint: drop_stmt.arg_types.is_empty().then(|| {
                        format!(
                            "Specify the argument list to select the {object_kind} unambiguously."
                        )
                    }),
                    sqlstate: "42725",
                });
            }
        };
        let catcache = self
            .backend_catcache(client_id, txn_ctx)
            .map_err(map_catalog_error)?;
        let dependency_graph = CatalogDependencyGraph::new(&catcache);
        let dependent_aggregate_oids = dependency_graph
            .dependents(ObjectAddress::new(PG_PROC_RELATION_OID, proc_row.oid, 0))
            .iter()
            .filter(|row| {
                row.classid == PG_PROC_RELATION_OID
                    && row.objsubid == 0
                    && row.deptype == DEPENDENCY_NORMAL
                    && row.objid != proc_row.oid
            })
            .filter_map(|row| {
                catcache
                    .proc_by_oid(row.objid)
                    .filter(|dependent| dependent.prokind == 'a')
                    .map(|dependent| dependent.oid)
            })
            .collect::<BTreeSet<_>>();
        if !dependent_aggregate_oids.is_empty() && !drop_stmt.cascade {
            let target_name = format_regprocedure_oid_optional(proc_row.oid, Some(&catalog))
                .unwrap_or_else(|| {
                    format!("{}({})", proc_row.proname, drop_stmt.arg_types.join(","))
                });
            let dependent_name = dependent_aggregate_oids
                .iter()
                .next()
                .and_then(|oid| format_regprocedure_oid_optional(*oid, Some(&catalog)))
                .unwrap_or_else(|| "unknown".into());
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot drop {object_kind} {target_name} because other objects depend on it"
                ),
                detail: Some(format!(
                    "function {dependent_name} depends on {object_kind} {target_name}"
                )),
                hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
                sqlstate: "2BP01",
            });
        }
        if !drop_stmt.cascade
            && let Some(err) = self.drop_function_dependency_error(
                client_id,
                txn_ctx,
                &proc_row,
                &catalog,
                object_kind,
            )?
        {
            return Err(err);
        }
        let mut dependent_casts = catalog
            .cast_rows()
            .into_iter()
            .filter(|row| row.castfunc == proc_row.oid)
            .collect::<Vec<_>>();
        dependent_casts.sort_by_key(|row| (row.castsource, row.casttarget, row.oid));
        if !dependent_casts.is_empty() && !drop_stmt.cascade {
            let function_display = drop_function_display(&catalog, &proc_row);
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot drop {object_kind} {function_display} because other objects depend on it"
                ),
                detail: Some(
                    dependent_casts
                        .iter()
                        .map(|row| cast_dependency_detail(&catalog, row, &function_display))
                        .collect::<Vec<_>>()
                        .join("\n"),
                ),
                hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
                sqlstate: "2BP01",
            });
        }

        let interrupts = self.interrupt_state(client_id);
        let mut next_cid = cid;
        for cast_row in dependent_casts {
            push_notice(cast_drop_notice(&catalog, &cast_row));
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: next_cid,
                client_id,
                waiter: Some(self.txn_waiter.clone()),
                interrupts: Arc::clone(&interrupts),
            };
            let effect = self
                .catalog
                .write()
                .drop_cast_by_oid_mvcc(cast_row.oid, &ctx)
                .map(|(_, effect)| effect)
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
            next_cid = next_cid.saturating_add(1);
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: next_cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts,
        };
        let mut next_cid = cid;
        for dependent_aggregate_oid in dependent_aggregate_oids {
            if let Some(dependent_name) =
                format_regprocedure_oid_optional(dependent_aggregate_oid, Some(&catalog))
            {
                push_notice(format!("drop cascades to function {dependent_name}"));
            }
            let dependent_ctx = CatalogWriteContext {
                pool: ctx.pool.clone(),
                txns: ctx.txns.clone(),
                xid: ctx.xid,
                cid: next_cid,
                client_id: ctx.client_id,
                waiter: ctx.waiter.clone(),
                interrupts: ctx.interrupts.clone(),
            };
            let effect = self
                .catalog
                .write()
                .drop_proc_by_oid_mvcc(dependent_aggregate_oid, &dependent_ctx)
                .map(|(_, effect)| effect)
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
            self.session_stats_state(client_id)
                .write()
                .note_function_drop(dependent_aggregate_oid, &self.stats);
            next_cid = next_cid.saturating_add(1);
        }
        let target_ctx = CatalogWriteContext {
            cid: next_cid,
            ..ctx
        };
        let effect = self
            .catalog
            .write()
            .drop_proc_by_oid_mvcc(proc_row.oid, &target_ctx)
            .map(|(_, effect)| effect)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        self.session_stats_state(client_id)
            .write()
            .note_function_drop(proc_row.oid, &self.stats);
        Ok(StatementResult::AffectedRows(0))
    }

    fn drop_function_dependency_error(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        proc_row: &PgProcRow,
        catalog: &dyn CatalogLookup,
        object_kind: &'static str,
    ) -> Result<Option<ExecError>, ExecError> {
        let catcache = self
            .backend_catcache(client_id, txn_ctx)
            .map_err(map_catalog_error)?;
        let signature = drop_proc_signature_text(proc_row, catalog);
        let mut details = Vec::new();
        for fdw in catcache.foreign_data_wrapper_rows() {
            if fdw.fdwhandler == proc_row.oid || fdw.fdwvalidator == proc_row.oid {
                details.push(format!(
                    "foreign-data wrapper {} depends on {object_kind} {signature}",
                    fdw.fdwname
                ));
            }
        }
        let type_rows = catcache
            .type_rows()
            .into_iter()
            .filter(|row| {
                [
                    row.typinput,
                    row.typoutput,
                    row.typreceive,
                    row.typsend,
                    row.typmodin,
                    row.typmodout,
                    row.typanalyze,
                    row.typsubscript,
                ]
                .contains(&proc_row.oid)
            })
            .collect::<Vec<_>>();
        for type_row in &type_rows {
            let type_name = drop_format_name(&catcache, type_row.typnamespace, &type_row.typname);
            details.push(format!(
                "type {type_name} depends on {object_kind} {signature}"
            ));
            for row in drop_proc_rows_depending_on_type(&catcache, type_row.oid, Some(proc_row.oid))
            {
                details.push(format!(
                    "function {} depends on type {type_name}",
                    drop_proc_signature_text(&row, catalog)
                ));
            }
            for domain in self.domains.read().values() {
                if domain.sql_type.type_oid == type_row.oid {
                    details.push(format!(
                        "type {} depends on {object_kind} {signature}",
                        drop_format_name(&catcache, domain.namespace_oid, &domain.name)
                    ));
                }
            }
        }
        details.dedup();
        if details.is_empty() {
            return Ok(None);
        }
        Ok(Some(ExecError::DetailedError {
            message: format!(
                "cannot drop {object_kind} {signature} because other objects depend on it"
            ),
            detail: Some(details.join("\n")),
            hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
            sqlstate: "2BP01",
        }))
    }

    fn drop_schema_owned_objects_in_transaction(
        &self,
        client_id: ClientId,
        schema_oid: u32,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let interrupts = self.interrupt_state(client_id);
        let relation_rows = catcache
            .class_rows()
            .into_iter()
            .filter(|row| row.relnamespace == schema_oid)
            .filter(|row| matches!(row.relkind, 'c' | 'f' | 'r' | 'p' | 'm' | 'S' | 'v'))
            .filter(|row| {
                row.relkind != 'S'
                    || !sequence_is_owned_by_relation_in_schema(&catcache, row.oid, schema_oid)
            })
            .collect::<Vec<_>>();
        let proc_rows = catcache
            .proc_rows()
            .into_iter()
            .filter(|row| row.pronamespace == schema_oid)
            .collect::<Vec<_>>();
        let mut next_cid = cid;

        let mut relation_rows_by_drop_order = relation_rows.clone();
        relation_rows_by_drop_order
            .sort_by_key(|row| (drop_schema_relation_drop_priority(row.relkind), row.oid));

        let mut dropped_relation_oids = BTreeSet::new();
        for relation in relation_rows_by_drop_order {
            if dropped_relation_oids.contains(&relation.oid) {
                continue;
            }
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: next_cid,
                client_id,
                waiter: Some(self.txn_waiter.clone()),
                interrupts: Arc::clone(&interrupts),
            };
            let drop_result = match relation.relkind {
                'c' => self
                    .catalog
                    .write()
                    .drop_composite_type_by_oid_mvcc(relation.oid, &ctx)
                    .map(|(entry, effect)| (vec![entry], effect)),
                'v' => self
                    .catalog
                    .write()
                    .drop_view_by_oid_mvcc(relation.oid, &ctx)
                    .map(|(entry, effect)| (vec![entry], effect)),
                'i' => self
                    .catalog
                    .write()
                    .drop_relation_entry_by_oid_mvcc(relation.oid, &ctx)
                    .map(|(entry, effect)| (vec![entry], effect)),
                _ => self
                    .catalog
                    .write()
                    .drop_relation_by_oid_mvcc(relation.oid, &ctx),
            };
            let (dropped_relations, effect) = drop_result.map_err(map_catalog_error)?;
            dropped_relation_oids.extend(dropped_relations.iter().map(|entry| entry.relation_oid));
            if relation.relkind != 'v' {
                self.apply_catalog_mutation_effect_immediate(&effect)?;
            }
            if dropped_relations
                .iter()
                .any(|entry| is_drop_table_relkind(entry.relkind))
            {
                let stats_state = self.session_stats_state(client_id);
                let mut stats = stats_state.write();
                for entry in &dropped_relations {
                    if is_drop_table_relkind(entry.relkind) {
                        stats.note_relation_drop(entry.relation_oid, &self.stats);
                    }
                }
            }
            catalog_effects.push(effect);
            next_cid = next_cid.saturating_add(1);
        }

        for proc_row in proc_rows {
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: next_cid,
                client_id,
                waiter: Some(self.txn_waiter.clone()),
                interrupts: Arc::clone(&interrupts),
            };
            let effect = self
                .catalog
                .write()
                .drop_proc_by_oid_mvcc(proc_row.oid, &ctx)
                .map(|(_, effect)| effect)
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
            next_cid = next_cid.saturating_add(1);
        }

        Ok(next_cid)
    }

    pub(crate) fn execute_drop_domain_stmt_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropDomainStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new_for_client(&self.txns, &self.txn_waiter, xid, client_id);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_domain_stmt_in_transaction_with_search_path(
            client_id,
            drop_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_drop_domain_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropDomainStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let domain_names = if drop_stmt.domain_names.is_empty() {
            vec![drop_stmt.domain_name.clone()]
        } else {
            drop_stmt.domain_names.clone()
        };
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let search_path = self.effective_search_path(client_id, configured_search_path);
        let domains_guard = self.domains.read();
        let mut explicit_domains = Vec::new();
        for domain_name in &domain_names {
            let (normalized, _, _) = self.normalize_domain_name_for_create(
                client_id,
                domain_name,
                configured_search_path,
            )?;
            let Some(domain) = domains_guard.get(&normalized).cloned() else {
                if drop_stmt.if_exists {
                    continue;
                }
                return Err(ExecError::Parse(ParseError::UnsupportedType(
                    domain_name.clone(),
                )));
            };
            explicit_domains.push((normalized, domain));
        }
        drop(domains_guard);
        if explicit_domains.is_empty() {
            return Ok(StatementResult::AffectedRows(0));
        }

        let plan = self.plan_drop_domain_dependencies(&catalog, &search_path, &explicit_domains);
        let source_name = plan
            .explicit_domain_names
            .iter()
            .next()
            .cloned()
            .unwrap_or_else(|| drop_stmt.domain_name.clone());
        if !drop_stmt.cascade {
            if let Some((_, dependent_name)) = plan.dependent_domains.first() {
                return Err(domain_type_dependency_error(
                    &source_name,
                    format!("type {dependent_name} depends on type {source_name}"),
                ));
            }
            if let Some((_, dependent_name)) = plan.dependent_ranges.first() {
                return Err(domain_has_range_dependents_error(
                    &source_name,
                    dependent_name,
                ));
            }
            if let Some(dependent_column) = plan.dependent_columns.first() {
                return Err(domain_type_dependency_error(
                    &source_name,
                    format!(
                        "column {} of table {} depends on type {}",
                        dependent_column.column_name, dependent_column.relation_name, source_name
                    ),
                ));
            }
        }

        if drop_stmt.cascade && !plan.dependent_columns.is_empty() {
            let mut rels = Vec::new();
            let mut seen_relation_oids = BTreeSet::new();
            for column in &plan.dependent_columns {
                if seen_relation_oids.insert(column.relation_oid)
                    && let Some(relation) = catalog.lookup_relation_by_oid(column.relation_oid)
                {
                    rels.push(relation.rel);
                }
            }
            lock_tables_interruptible(
                &self.table_locks,
                client_id,
                &rels,
                TableLockMode::AccessExclusive,
                self.interrupt_state(client_id).as_ref(),
            )?;
        }

        if drop_stmt.cascade {
            let notices = drop_domain_cascade_notices(&plan);
            match notices.as_slice() {
                [] => {}
                [notice] => push_notice(notice.clone()),
                notices => push_notice_with_detail(
                    format!("drop cascades to {} other objects", notices.len()),
                    notices.join("\n"),
                ),
            }
        }

        if drop_stmt.cascade {
            let interrupts = self.interrupt_state(client_id);
            let mut next_cid = cid;
            for column in &plan.dependent_columns {
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: next_cid,
                    client_id,
                    waiter: None,
                    interrupts: Arc::clone(&interrupts),
                };
                let effect = self
                    .catalog
                    .write()
                    .alter_table_drop_column_mvcc(column.relation_oid, &column.column_name, &ctx)
                    .map_err(map_catalog_error)?;
                catalog_effects.push(effect);
                next_cid = next_cid.saturating_add(1);
            }
        }

        {
            let mut domains = self.domains.write();
            for key in &plan.domain_keys {
                domains.remove(key);
            }
        }

        if drop_stmt.cascade && !plan.dependent_ranges.is_empty() {
            let mut range_types = self.range_types.write();
            let mut removed_range = false;
            for (key, _) in &plan.dependent_ranges {
                removed_range |= range_types.remove(key).is_some();
            }
            if removed_range {
                save_range_type_entries(&self.cluster.base_dir, self.database_oid, &range_types)?;
            }
        }

        self.refresh_catalog_store_dynamic_type_rows(client_id, configured_search_path);
        self.invalidate_backend_cache_state(client_id);
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    fn plan_drop_domain_dependencies(
        &self,
        catalog: &dyn CatalogLookup,
        search_path: &[String],
        explicit_domains: &[(String, DomainEntry)],
    ) -> DropDomainPlan {
        let mut plan = DropDomainPlan::default();
        for (key, domain) in explicit_domains {
            plan.explicit_domain_names.insert(domain.name.clone());
            plan.domain_keys.insert(key.clone());
            plan.domain_oids.insert(domain.oid);
        }

        {
            let domains = self.domains.read();
            let mut changed = true;
            while changed {
                changed = false;
                for (key, domain) in domains.iter() {
                    if plan.domain_keys.contains(key) {
                        continue;
                    }
                    if drop_domain_sql_type_depends_on_any(domain.sql_type, &plan.domain_oids) {
                        plan.domain_keys.insert(key.clone());
                        plan.domain_oids.insert(domain.oid);
                        plan.dependent_domains
                            .push((key.clone(), domain.name.clone()));
                        changed = true;
                    }
                }
            }
        }

        let dropped_domain_names = {
            let domains = self.domains.read();
            plan.domain_keys
                .iter()
                .filter_map(|key| domains.get(key))
                .map(|domain| domain.name.to_lowercase())
                .collect::<BTreeSet<_>>()
        };
        {
            let range_types = self.range_types.read();
            plan.dependent_ranges = range_types
                .iter()
                .filter(|(_, entry)| {
                    plan.domain_oids
                        .contains(&entry.subtype_dependency_oid.unwrap_or(0))
                        || plan.domain_oids.contains(&entry.subtype.type_oid)
                        || dropped_domain_names.contains(&entry.name.to_lowercase())
                        || dropped_domain_names.contains(
                            &entry
                                .name
                                .strip_suffix("range")
                                .unwrap_or(&entry.name)
                                .to_lowercase(),
                        )
                })
                .map(|(key, entry)| (key.clone(), entry.name.clone()))
                .collect::<Vec<_>>();
        }
        plan.dependent_ranges
            .sort_by(|left, right| left.1.cmp(&right.1));

        for class in catalog.class_rows() {
            if !matches!(class.relkind, 'r' | 'p' | 'f' | 'm') {
                continue;
            }
            let Some(relation) = catalog.lookup_relation_by_oid(class.oid) else {
                continue;
            };
            let relation_name =
                drop_table_display_relation_name(catalog, relation.relation_oid, search_path);
            for (index, column) in relation.desc.columns.iter().enumerate() {
                if column.dropped
                    || !drop_domain_sql_type_depends_on_any(column.sql_type, &plan.domain_oids)
                {
                    continue;
                }
                plan.dependent_columns.push(DropDomainColumnDependency {
                    relation_oid: relation.relation_oid,
                    relation_name: relation_name.clone(),
                    column_name: column.name.clone(),
                    attnum: (index + 1) as i16,
                });
            }
        }
        plan.dependent_columns.sort_by(|left, right| {
            left.relation_name
                .cmp(&right.relation_name)
                .then_with(|| right.attnum.cmp(&left.attnum))
                .then_with(|| left.column_name.cmp(&right.column_name))
        });

        plan
    }

    pub(crate) fn execute_drop_table_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &crate::backend::parser::DropTableStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        self.refresh_catalog_store_dynamic_type_rows(client_id, configured_search_path);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let search_path = self.effective_search_path(client_id, configured_search_path);
        let mut dynamic_type_rows = self.domain_type_rows_for_search_path(&search_path);
        dynamic_type_rows.extend(self.enum_type_rows_for_search_path(&search_path));
        dynamic_type_rows.extend(self.range_type_rows_for_search_path(&search_path));
        let mut rels = Vec::new();
        let mut dropped = 0usize;
        let mut explicit_relation_oids = BTreeSet::new();
        let mut explicit_relation_order = Vec::new();

        for relation_name in &drop_stmt.table_names {
            let relation = match catalog.lookup_any_relation(relation_name) {
                Some(relation)
                    if if drop_stmt.foreign_table {
                        is_drop_foreign_table_relkind(relation.relkind)
                    } else {
                        is_drop_table_relkind(relation.relkind)
                    } =>
                {
                    relation
                }
                Some(_) => {
                    return Err(ExecError::Parse(ParseError::WrongObjectType {
                        name: relation_name.clone(),
                        expected: if drop_stmt.foreign_table {
                            "foreign table"
                        } else {
                            "table"
                        },
                    }));
                }
                None if drop_stmt.if_exists => {
                    let object_kind = if drop_stmt.foreign_table {
                        "foreign table"
                    } else {
                        "table"
                    };
                    push_missing_relation_notice(&catalog, relation_name, object_kind);
                    continue;
                }
                None => {
                    return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                        relation_name.clone(),
                    )));
                }
            };

            ensure_relation_owner(self, client_id, &relation, relation_name)?;
            if explicit_relation_oids.insert(relation.relation_oid) {
                explicit_relation_order.push(relation.relation_oid);
            }
            rels.push(relation.rel);
            dropped += 1;
        }

        if rels.is_empty() {
            return Ok(StatementResult::AffectedRows(0));
        }

        lock_tables_interruptible(
            &self.table_locks,
            client_id,
            &rels,
            TableLockMode::AccessExclusive,
            interrupts.as_ref(),
        )?;

        let result = (|| {
            let (graph, constraints_by_oid, rewrites_by_oid, policies_by_oid) =
                build_drop_table_dependency_graph(
                    self,
                    client_id,
                    Some((xid, cid)),
                    &explicit_relation_oids,
                );
            let behavior = DropBehavior::from_cascade(drop_stmt.cascade);
            let dependency_ctx = DropTableDependencyContext {
                catalog: &catalog,
                graph: &graph,
                constraints_by_oid,
                rewrites_by_oid,
                policies_by_oid,
                search_path: &search_path,
            };
            let mut plan = DropTablePlan::default();
            for &relation_oid in &explicit_relation_order {
                plan_drop_table_relation(
                    &dependency_ctx,
                    relation_oid,
                    &explicit_relation_oids,
                    behavior,
                    &mut plan,
                );
            }
            sort_policy_cascade_notices(&dependency_ctx, &mut plan);

            if !drop_stmt.cascade && !plan.blocker_details.is_empty() {
                let (_, source_name) = plan.blocker_source.unwrap_or(('r', "table".to_string()));
                return Err(ExecError::DetailedError {
                    message: format!(
                        "cannot drop table {source_name} because other objects depend on it"
                    ),
                    detail: Some(plan.blocker_details.join("\n")),
                    hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
                    sqlstate: "2BP01",
                });
            }

            match plan.notices.as_slice() {
                [] => {}
                [notice] => push_notice(notice.clone()),
                notices => push_notice_with_detail(
                    format!("drop cascades to {} other objects", notices.len()),
                    notices.join("\n"),
                ),
            }

            let mut next_cid = cid;

            for policy in &plan.policy_drops {
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: next_cid,
                    client_id,
                    waiter: None,
                    interrupts: Arc::clone(&interrupts),
                };
                let (_removed, effect) = self
                    .catalog
                    .write()
                    .drop_policy_mvcc(policy.relation_oid, &policy.policy_name, &ctx)
                    .map_err(map_catalog_error)?;
                catalog_effects.push(effect);
                next_cid = next_cid.saturating_add(1);
            }

            for rule in &plan.rule_drops {
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: next_cid,
                    client_id,
                    waiter: None,
                    interrupts: Arc::clone(&interrupts),
                };
                let effect = self
                    .catalog
                    .write()
                    .drop_rule_mvcc(rule.rewrite_oid, &ctx)
                    .map_err(map_catalog_error)?;
                catalog_effects.push(effect);
                next_cid = next_cid.saturating_add(1);
            }

            for constraint in &plan.constraint_drops {
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: next_cid,
                    client_id,
                    waiter: None,
                    interrupts: Arc::clone(&interrupts),
                };
                let (_removed, effect) = self
                    .catalog
                    .write()
                    .drop_relation_constraint_mvcc(
                        constraint.relation_oid,
                        &constraint.constraint_name,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                catalog_effects.push(effect);
                next_cid = next_cid.saturating_add(1);
            }

            for relation_oid in &plan.relation_drop_order {
                let (relkind, relpersistence) = catalog
                    .class_row_by_oid(*relation_oid)
                    .map(|row| (row.relkind, row.relpersistence))
                    .unwrap_or(('r', 'p'));
                if relpersistence == 't' {
                    let temp_name = self
                        .temp_relation_name_for_oid(client_id, *relation_oid)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::UnexpectedToken {
                                expected: "tracked temporary relation",
                                actual: relation_oid.to_string(),
                            })
                        })?;
                    if relkind == 'v' {
                        let ctx = CatalogWriteContext {
                            pool: self.pool.clone(),
                            txns: self.txns.clone(),
                            xid,
                            cid: next_cid,
                            client_id,
                            waiter: Some(self.txn_waiter.clone()),
                            interrupts: Arc::clone(&interrupts),
                        };
                        let effect = self
                            .catalog
                            .write()
                            .drop_view_by_oid_mvcc(*relation_oid, &ctx)
                            .map(|(_, effect)| effect)
                            .map_err(map_catalog_error)?;
                        catalog_effects.push(effect);
                        self.remove_temp_entry_after_catalog_drop(
                            client_id,
                            &temp_name,
                            temp_effects,
                        )?;
                    } else {
                        self.drop_temp_relation_in_transaction(
                            client_id,
                            &temp_name,
                            xid,
                            next_cid,
                            catalog_effects,
                            temp_effects,
                        )?;
                    }
                    next_cid = next_cid.saturating_add(1);
                    continue;
                }
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: next_cid,
                    client_id,
                    waiter: Some(self.txn_waiter.clone()),
                    interrupts: Arc::clone(&interrupts),
                };
                let effect = match relkind {
                    'v' => self
                        .catalog
                        .write()
                        .drop_view_by_oid_mvcc(*relation_oid, &ctx)
                        .map(|(entry, effect)| (vec![entry], effect)),
                    _ => self
                        .catalog
                        .write()
                        .drop_relation_by_oid_mvcc_with_extra_type_rows(
                            *relation_oid,
                            &ctx,
                            &dynamic_type_rows,
                        ),
                }
                .map_err(map_catalog_error)?;
                let (dropped_relations, effect) = effect;

                if relkind != 'v' {
                    self.apply_catalog_mutation_effect_immediate(&effect)?;
                }
                if dropped_relations
                    .iter()
                    .any(|entry| is_drop_table_relkind(entry.relkind))
                {
                    let stats_state = self.session_stats_state(client_id);
                    let mut stats = stats_state.write();
                    for entry in &dropped_relations {
                        if is_drop_table_relkind(entry.relkind) {
                            stats.note_relation_drop(entry.relation_oid, &self.stats);
                        }
                    }
                }
                catalog_effects.push(effect);
                next_cid = next_cid.saturating_add(1);
            }

            Ok(StatementResult::AffectedRows(dropped))
        })();

        for rel in rels {
            self.table_locks.unlock_table(rel, client_id);
        }

        result
    }

    pub(crate) fn execute_drop_view_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropViewStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_drop_relation_stmt_in_transaction_with_search_path(
            client_id,
            &drop_stmt.view_names,
            drop_stmt.if_exists,
            xid,
            cid,
            configured_search_path,
            catalog_effects,
            Some(temp_effects),
            drop_stmt.cascade,
            'v',
            "view",
        )
    }

    pub(crate) fn execute_drop_index_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropIndexStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        if drop_stmt.concurrently && drop_stmt.index_names.len() > 1 {
            return Err(ExecError::DetailedError {
                message: "DROP INDEX CONCURRENTLY does not support dropping multiple objects"
                    .into(),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let mut requested_oids = BTreeSet::new();
        let mut rels = Vec::new();
        for index_name in &drop_stmt.index_names {
            let Some(entry) = catalog.lookup_any_relation(index_name) else {
                if drop_stmt.if_exists {
                    let display_name = index_name.rsplit('.').next().unwrap_or(index_name);
                    push_notice(format!(
                        "index \"{}\" does not exist, skipping",
                        display_name.trim_matches('"')
                    ));
                    continue;
                }
                return Err(ExecError::DetailedError {
                    message: format!("index \"{index_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                });
            };
            if !matches!(entry.relkind, 'i' | 'I') {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: index_name.clone(),
                    expected: "index",
                }));
            }
            if drop_stmt.concurrently && entry.relkind == 'I' && entry.relpersistence != 't' {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "cannot drop partitioned index \"{}\" concurrently",
                        index_name
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                });
            }
            ensure_relation_owner(self, client_id, &entry, index_name)?;
            requested_oids.insert(entry.relation_oid);
            rels.push(entry.rel);
        }
        lock_tables_interruptible(
            &self.table_locks,
            client_id,
            &rels,
            TableLockMode::AccessExclusive,
            interrupts.as_ref(),
        )?;

        let result = (|| {
            let mut drop_oids = Vec::new();
            let mut seen = BTreeSet::new();
            for index_name in &drop_stmt.index_names {
                let Some(entry) = catalog.lookup_any_relation(index_name) else {
                    continue;
                };
                if !matches!(entry.relkind, 'i' | 'I') {
                    continue;
                }
                let parent_oids = catalog
                    .inheritance_parents(entry.relation_oid)
                    .into_iter()
                    .map(|row| row.inhparent)
                    .collect::<Vec<_>>();
                if !parent_oids.is_empty()
                    && !parent_oids
                        .iter()
                        .any(|parent_oid| requested_oids.contains(parent_oid))
                {
                    let parent_name = parent_oids
                        .first()
                        .and_then(|oid| catalog.class_row_by_oid(*oid))
                        .map(|row| row.relname)
                        .unwrap_or_else(|| parent_oids[0].to_string());
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "cannot drop index {} because index {} requires it",
                            index_name, parent_name
                        ),
                        detail: None,
                        hint: Some(format!("You can drop index {} instead.", parent_name)),
                        sqlstate: "2BP01",
                    });
                }
                if parent_oids
                    .iter()
                    .any(|parent_oid| requested_oids.contains(parent_oid))
                {
                    continue;
                }
                if let Some(constraint) = catalog
                    .constraint_rows_for_index(entry.relation_oid)
                    .into_iter()
                    .next()
                {
                    let table_name = catalog
                        .class_row_by_oid(constraint.conrelid)
                        .map(|row| row.relname)
                        .unwrap_or_else(|| constraint.conrelid.to_string());
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "cannot drop index {} because constraint {} on table {} requires it",
                            index_name, constraint.conname, table_name
                        ),
                        detail: None,
                        hint: Some(format!(
                            "You can drop constraint {} on table {} instead.",
                            constraint.conname, table_name
                        )),
                        sqlstate: "2BP01",
                    });
                }
                Self::collect_index_drop_oids(
                    &catalog,
                    entry.relation_oid,
                    &mut seen,
                    &mut drop_oids,
                )?;
            }

            let mut dropped = 0usize;
            let mut next_cid = cid;
            for index_oid in drop_oids {
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: next_cid,
                    client_id,
                    waiter: Some(self.txn_waiter.clone()),
                    interrupts: Arc::clone(&interrupts),
                };
                let effect = if let Some(entry) = catalog.relation_by_oid(index_oid) {
                    let mut catalog_guard = self.catalog.write();
                    catalog_guard
                        .drop_relation_entry_mvcc(
                            catalog_entry_from_bound_relation(&catcache, &entry),
                            &ctx,
                        )
                        .map_err(|err| match err {
                            CatalogError::UnknownTable(_) => ExecError::Parse(
                                ParseError::TableDoesNotExist(index_oid.to_string()),
                            ),
                            other => ExecError::Parse(ParseError::UnexpectedToken {
                                expected: "droppable index",
                                actual: format!("{other:?}"),
                            }),
                        })?
                } else {
                    self.catalog
                        .write()
                        .drop_relation_entry_by_oid_mvcc(index_oid, &ctx)
                        .map_err(|err| match err {
                            CatalogError::UnknownTable(_) => ExecError::Parse(
                                ParseError::TableDoesNotExist(index_oid.to_string()),
                            ),
                            other => ExecError::Parse(ParseError::UnexpectedToken {
                                expected: "droppable index",
                                actual: format!("{other:?}"),
                            }),
                        })?
                        .1
                };
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                dropped += 1;
                next_cid = next_cid.saturating_add(1);
            }
            Ok(StatementResult::AffectedRows(dropped))
        })();

        for rel in rels {
            self.table_locks.unlock_table(rel, client_id);
        }
        result
    }

    fn collect_index_drop_oids(
        catalog: &dyn crate::backend::parser::CatalogLookup,
        index_oid: u32,
        seen: &mut BTreeSet<u32>,
        out: &mut Vec<u32>,
    ) -> Result<(), ExecError> {
        if !seen.insert(index_oid) {
            return Ok(());
        }
        let mut children = catalog.inheritance_children(index_oid);
        children.sort_by_key(|row| (row.inhseqno, row.inhrelid));
        for child in children {
            Self::collect_index_drop_oids(catalog, child.inhrelid, seen, out)?;
        }
        let relation = catalog.relation_by_oid(index_oid).ok_or_else(|| {
            ExecError::Parse(ParseError::TableDoesNotExist(index_oid.to_string()))
        })?;
        if !matches!(relation.relkind, 'i' | 'I') {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: index_oid.to_string(),
                expected: "index",
            }));
        }
        out.push(index_oid);
        Ok(())
    }

    pub(crate) fn execute_drop_schema_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropSchemaStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let mut dropped = 0usize;
        let mut cascade_notice_groups = Vec::new();
        for schema_name in &drop_stmt.schema_names {
            let maybe_schema = catcache
                .namespace_by_name(schema_name)
                .cloned()
                .filter(|row| !self.other_session_temp_namespace_oid(client_id, row.oid));
            let schema = match maybe_schema {
                Some(schema) => schema,
                None if drop_stmt.if_exists => continue,
                None => {
                    return Err(ExecError::DetailedError {
                        message: format!("schema \"{schema_name}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    });
                }
            };
            if schema.oid == crate::include::catalog::PG_CATALOG_NAMESPACE_OID {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "cannot drop schema {schema_name} because it is required by the database system"
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "2BP01",
                });
            }
            let auth = self.auth_state(client_id);
            let auth_catalog = self.txn_auth_catalog(client_id, xid, cid).map_err(|err| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "authorization catalog",
                    actual: format!("{err:?}"),
                })
            })?;
            if !auth.has_effective_membership(schema.nspowner, &auth_catalog) {
                return Err(ExecError::DetailedError {
                    message: format!("must be owner of schema {schema_name}"),
                    detail: None,
                    hint: None,
                    sqlstate: "42501",
                });
            }
            let relation_rows = catcache
                .class_rows()
                .into_iter()
                .filter(|row| row.relnamespace == schema.oid)
                .collect::<Vec<_>>();
            let has_relations = !relation_rows.is_empty();
            let has_procs = catcache
                .proc_rows()
                .into_iter()
                .any(|row| row.pronamespace == schema.oid);
            if (has_relations || has_procs) && !drop_stmt.cascade {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "cannot drop schema {schema_name} because other objects depend on it"
                    ),
                    detail: Some("schema is not empty".into()),
                    hint: None,
                    sqlstate: "2BP01",
                });
            }
            if drop_stmt.cascade {
                let mut notices = Vec::new();
                let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), None);
                let visible_namespaces = drop_schema_visible_namespace_oids(
                    self,
                    client_id,
                    Some((xid, cid)),
                    configured_search_path,
                    &auth_catalog,
                );

                let mut conversion_rows = catcache
                    .conversion_rows()
                    .into_iter()
                    .filter(|row| row.connamespace == schema.oid)
                    .collect::<Vec<_>>();
                conversion_rows.sort_by_key(|row| row.oid);
                for row in conversion_rows {
                    notices.push(format!(
                        "drop cascades to conversion {}",
                        drop_schema_display_object_name(
                            &catcache,
                            &visible_namespaces,
                            row.connamespace,
                            &row.conname
                        )
                    ));
                }

                let mut operator_rows = catcache
                    .operator_rows()
                    .into_iter()
                    .filter(|row| row.oprnamespace == schema.oid)
                    .collect::<Vec<_>>();
                operator_rows.sort_by_key(|row| row.oid);
                for row in operator_rows {
                    notices.push(format!(
                        "drop cascades to operator {}",
                        drop_schema_display_operator_name(
                            &catalog,
                            &catcache,
                            &visible_namespaces,
                            &row
                        )
                    ));
                }

                let access_method_names = catcache
                    .am_rows()
                    .into_iter()
                    .map(|row| (row.oid, row.amname))
                    .collect::<BTreeMap<_, _>>();
                let mut opfamily_rows = catcache
                    .opfamily_rows()
                    .into_iter()
                    .filter(|row| row.opfnamespace == schema.oid)
                    .collect::<Vec<_>>();
                opfamily_rows.sort_by_key(|row| row.oid);
                for row in opfamily_rows {
                    notices.push(format!(
                        "drop cascades to operator family {} for access method {}",
                        drop_schema_display_object_name(
                            &catcache,
                            &visible_namespaces,
                            row.opfnamespace,
                            &row.opfname
                        ),
                        access_method_names
                            .get(&row.opfmethod)
                            .map(String::as_str)
                            .unwrap_or("unknown")
                    ));
                }

                let mut relation_notice_rows = relation_rows
                    .iter()
                    .filter(|row| {
                        (!row.relispartition
                            || !partition_has_parent_in_schema(&catcache, row.oid, schema.oid))
                            && matches!(row.relkind, 'c' | 'f' | 'r' | 'p' | 'm' | 'S' | 'v')
                            && (row.relkind != 'S'
                                || !sequence_is_owned_by_relation_in_schema(
                                    &catcache, row.oid, schema.oid,
                                ))
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let relation_notice_oids = relation_notice_rows
                    .iter()
                    .map(|row| row.oid)
                    .collect::<BTreeSet<_>>();
                let inheritance_parent_oids = catcache
                    .inherit_rows()
                    .into_iter()
                    .filter(|row| {
                        relation_notice_oids.contains(&row.inhparent)
                            && relation_notice_oids.contains(&row.inhrelid)
                    })
                    .map(|row| row.inhparent)
                    .collect::<BTreeSet<_>>();
                relation_notice_rows
                    .sort_by_key(|row| (!inheritance_parent_oids.contains(&row.oid), row.oid));
                for relation in relation_notice_rows {
                    notices.push(format!(
                        "drop cascades to {} {}",
                        drop_table_relation_kind_name(relation.relkind),
                        drop_schema_display_object_name(
                            &catcache,
                            &visible_namespaces,
                            relation.relnamespace,
                            &relation.relname
                        )
                    ));
                }

                let mut ts_dict_rows = catcache
                    .ts_dict_rows()
                    .into_iter()
                    .filter(|row| row.dictnamespace == schema.oid)
                    .collect::<Vec<_>>();
                ts_dict_rows.sort_by_key(|row| row.oid);
                for row in ts_dict_rows {
                    notices.push(format!(
                        "drop cascades to text search dictionary {}",
                        drop_schema_display_object_name(
                            &catcache,
                            &visible_namespaces,
                            row.dictnamespace,
                            &row.dictname
                        )
                    ));
                }

                let mut ts_config_rows = catcache
                    .ts_config_rows()
                    .into_iter()
                    .filter(|row| row.cfgnamespace == schema.oid)
                    .collect::<Vec<_>>();
                ts_config_rows.sort_by_key(|row| row.oid);
                for row in ts_config_rows {
                    notices.push(format!(
                        "drop cascades to text search configuration {}",
                        drop_schema_display_object_name(
                            &catcache,
                            &visible_namespaces,
                            row.cfgnamespace,
                            &row.cfgname
                        )
                    ));
                }

                let mut ts_template_rows = catcache
                    .ts_template_rows()
                    .into_iter()
                    .filter(|row| row.tmplnamespace == schema.oid)
                    .collect::<Vec<_>>();
                ts_template_rows.sort_by_key(|row| row.oid);
                for row in ts_template_rows {
                    notices.push(format!(
                        "drop cascades to text search template {}",
                        drop_schema_display_object_name(
                            &catcache,
                            &visible_namespaces,
                            row.tmplnamespace,
                            &row.tmplname
                        )
                    ));
                }

                let mut ts_parser_rows = catcache
                    .ts_parser_rows()
                    .into_iter()
                    .filter(|row| row.prsnamespace == schema.oid)
                    .collect::<Vec<_>>();
                ts_parser_rows.sort_by_key(|row| row.oid);
                for row in ts_parser_rows {
                    notices.push(format!(
                        "drop cascades to text search parser {}",
                        drop_schema_display_object_name(
                            &catcache,
                            &visible_namespaces,
                            row.prsnamespace,
                            &row.prsname
                        )
                    ));
                }

                let mut tail_notices = Vec::new();
                for row in catcache.type_rows().into_iter().filter(|row| {
                    row.typnamespace == schema.oid && matches!(row.typtype, 'd' | 'e')
                }) {
                    tail_notices.push((
                        row.oid,
                        format!(
                            "drop cascades to type {}",
                            drop_schema_display_object_name(
                                &catcache,
                                &visible_namespaces,
                                row.typnamespace,
                                &row.typname
                            )
                        ),
                    ));
                }
                for proc_row in catcache
                    .proc_rows()
                    .into_iter()
                    .filter(|row| row.pronamespace == schema.oid)
                {
                    let signature = drop_proc_signature_text(&proc_row, &catalog);
                    tail_notices.push((
                        proc_row.oid,
                        format!(
                            "drop cascades to function {}",
                            drop_schema_display_signature_name(
                                &catcache,
                                &visible_namespaces,
                                proc_row.pronamespace,
                                &signature
                            )
                        ),
                    ));
                }
                tail_notices.sort_by_key(|(oid, _)| *oid);
                notices.extend(tail_notices.into_iter().map(|(_, notice)| notice));

                if !notices.is_empty() {
                    cascade_notice_groups.push(notices);
                }
            }
            let mut namespace_cid = cid;
            if has_relations || has_procs {
                namespace_cid = self.drop_schema_owned_objects_in_transaction(
                    client_id,
                    schema.oid,
                    xid,
                    cid,
                    catalog_effects,
                )?;
            }
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: namespace_cid,
                client_id,
                waiter: Some(self.txn_waiter.clone()),
                interrupts: self.interrupt_state(client_id),
            };
            let effect = self
                .catalog
                .write()
                .drop_namespace_mvcc(
                    schema.oid,
                    &schema.nspname,
                    schema.nspowner,
                    schema.nspacl.clone(),
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
            dropped += 1;
        }
        let cascade_notices = cascade_notice_groups
            .into_iter()
            .rev()
            .flatten()
            .collect::<Vec<_>>();
        match cascade_notices.as_slice() {
            [] => {}
            [notice] => push_notice(notice.clone()),
            notices => push_notice_with_detail(
                format!("drop cascades to {} other objects", notices.len()),
                notices.join("\n"),
            ),
        }
        Ok(StatementResult::AffectedRows(dropped))
    }

    pub(super) fn execute_drop_relation_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        relation_names: &[String],
        if_exists: bool,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        mut temp_effects: Option<&mut Vec<TempMutationEffect>>,
        cascade: bool,
        expected_relkind: char,
        expected_name: &'static str,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        if cascade || matches!(expected_relkind, 'v' | 'm') {
            return self.execute_drop_relation_dependency_stmt_in_transaction_with_search_path(
                client_id,
                relation_names,
                if_exists,
                xid,
                cid,
                configured_search_path,
                catalog_effects,
                temp_effects,
                cascade,
                expected_relkind,
                expected_name,
            );
        }
        let rels = relation_names
            .iter()
            .filter_map(|name| catalog.lookup_any_relation(name).map(|e| e.rel))
            .collect::<Vec<_>>();
        lock_tables_interruptible(
            &self.table_locks,
            client_id,
            &rels,
            TableLockMode::AccessExclusive,
            interrupts.as_ref(),
        )?;

        let mut dropped = 0usize;
        let mut result = Ok(StatementResult::AffectedRows(0));
        for relation_name in relation_names {
            let maybe_entry = catalog.lookup_any_relation(relation_name);
            if expected_relkind == 'r'
                && maybe_entry
                    .as_ref()
                    .is_some_and(|entry| entry.relpersistence == 't')
            {
                if let Some(entry) = maybe_entry.as_ref() {
                    if let Err(err) = ensure_relation_owner(self, client_id, entry, relation_name) {
                        result = Err(err);
                        break;
                    }
                }
                match self.drop_temp_relation_in_transaction(
                    client_id,
                    relation_name,
                    xid,
                    cid,
                    catalog_effects,
                    temp_effects
                        .as_deref_mut()
                        .expect("temp effects required for DROP TABLE"),
                ) {
                    Ok(_) => dropped += 1,
                    Err(_) if if_exists => {}
                    Err(err) => {
                        result = Err(err);
                        break;
                    }
                }
                continue;
            }

            let relation_oid = match maybe_entry.as_ref() {
                Some(entry) if entry.relkind == expected_relkind => entry.relation_oid,
                Some(_) => {
                    result = Err(ExecError::Parse(ParseError::WrongObjectType {
                        name: relation_name.clone(),
                        expected: expected_name,
                    }));
                    break;
                }
                None if if_exists => continue,
                None => {
                    result = Err(ExecError::Parse(ParseError::TableDoesNotExist(
                        relation_name.clone(),
                    )));
                    break;
                }
            };
            if let Some(entry) = maybe_entry.as_ref() {
                if let Err(err) = ensure_relation_owner(self, client_id, entry, relation_name) {
                    result = Err(err);
                    break;
                }
            }
            if expected_relkind != 'i' {
                if expected_relkind == 'r' {
                    if let Err(err) = reject_relation_with_referencing_foreign_keys(
                        &catalog,
                        relation_oid,
                        "DROP TABLE on table without referencing foreign keys",
                    ) {
                        result = Err(err);
                        break;
                    }
                }
                if let Err(err) = reject_relation_with_dependent_views(
                    self,
                    client_id,
                    Some((xid, cid)),
                    relation_oid,
                    if expected_relkind == 'v' {
                        "DROP VIEW on relation without dependent views"
                    } else {
                        "DROP TABLE on relation without dependent views"
                    },
                ) {
                    result = Err(err);
                    break;
                }
            }
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid,
                client_id,
                waiter: Some(self.txn_waiter.clone()),
                interrupts: Arc::clone(&interrupts),
            };
            let drop_result = match expected_relkind {
                'v' => self
                    .catalog
                    .write()
                    .drop_view_by_oid_mvcc(relation_oid, &ctx)
                    .map(|(_, effect)| effect),
                'i' => self
                    .catalog
                    .write()
                    .drop_relation_entry_by_oid_mvcc(relation_oid, &ctx)
                    .map(|(_, effect)| effect),
                _ => self
                    .catalog
                    .write()
                    .drop_relation_by_oid_mvcc(relation_oid, &ctx)
                    .map(|(_, effect)| effect),
            };
            match drop_result {
                Ok(effect) => {
                    if maybe_entry
                        .as_ref()
                        .is_some_and(|entry| entry.relpersistence == 't')
                    {
                        self.apply_catalog_mutation_effect_immediate(&effect)?;
                        if expected_relkind == 'v' {
                            self.remove_temp_entry_after_catalog_drop(
                                client_id,
                                relation_name,
                                temp_effects
                                    .as_deref_mut()
                                    .expect("temp effects required for DROP VIEW"),
                            )?;
                        }
                    } else if expected_relkind != 'v' {
                        self.apply_catalog_mutation_effect_immediate(&effect)?;
                    }
                    if matches!(expected_relkind, 'r' | 'm') {
                        self.session_stats_state(client_id)
                            .write()
                            .note_relation_drop(relation_oid, &self.stats);
                    }
                    catalog_effects.push(effect);
                    dropped += 1;
                }
                Err(CatalogError::UnknownTable(_)) if if_exists => {}
                Err(CatalogError::UnknownTable(_)) => {
                    result = Err(ExecError::Parse(ParseError::TableDoesNotExist(
                        relation_name.clone(),
                    )));
                    break;
                }
                Err(other) => {
                    result = Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: match expected_relkind {
                            'i' => "droppable index",
                            'm' => "droppable materialized view",
                            'v' => "droppable view",
                            _ => "droppable table",
                        },
                        actual: format!("{other:?}"),
                    }));
                    break;
                }
            }
        }

        for rel in rels {
            self.table_locks.unlock_table(rel, client_id);
        }

        if result.is_ok() {
            Ok(StatementResult::AffectedRows(dropped))
        } else {
            result
        }
    }

    fn execute_drop_relation_dependency_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        relation_names: &[String],
        if_exists: bool,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        mut temp_effects: Option<&mut Vec<TempMutationEffect>>,
        cascade: bool,
        expected_relkind: char,
        expected_name: &'static str,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let search_path = self.effective_search_path(client_id, configured_search_path);
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let mut rels = Vec::new();
        let mut explicit_relation_oids = BTreeSet::new();
        let mut explicit_relation_order = Vec::new();
        let mut dropped = 0usize;

        for relation_name in relation_names {
            let relation = match catalog.lookup_any_relation(relation_name) {
                Some(relation) if relation.relkind == expected_relkind => relation,
                Some(_) => {
                    return Err(ExecError::Parse(ParseError::WrongObjectType {
                        name: relation_name.clone(),
                        expected: expected_name,
                    }));
                }
                None if if_exists => {
                    push_missing_relation_notice(&catalog, relation_name, expected_name);
                    continue;
                }
                None => {
                    return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                        relation_name.clone(),
                    )));
                }
            };
            ensure_relation_owner(self, client_id, &relation, relation_name)?;
            if explicit_relation_oids.insert(relation.relation_oid) {
                explicit_relation_order.push(relation.relation_oid);
            }
            rels.push(relation.rel);
            dropped += 1;
        }

        lock_tables_interruptible(
            &self.table_locks,
            client_id,
            &rels,
            TableLockMode::AccessExclusive,
            interrupts.as_ref(),
        )?;

        let result = (|| {
            let graph = CatalogDependencyGraph::new(&catcache);
            let dependency_ctx = DropTableDependencyContext {
                catalog: &catalog,
                graph: &graph,
                constraints_by_oid: catcache
                    .constraint_rows()
                    .into_iter()
                    .map(|row| (row.oid, row))
                    .collect(),
                rewrites_by_oid: catcache
                    .rewrite_rows()
                    .into_iter()
                    .map(|row| (row.oid, row))
                    .collect(),
                policies_by_oid: catcache
                    .policy_rows()
                    .into_iter()
                    .map(|row| (row.oid, row))
                    .collect(),
                search_path: &search_path,
            };
            let mut plan = DropTablePlan::default();
            let behavior = DropBehavior::from_cascade(cascade);
            for &relation_oid in &explicit_relation_order {
                plan_drop_table_relation(
                    &dependency_ctx,
                    relation_oid,
                    &explicit_relation_oids,
                    behavior,
                    &mut plan,
                );
            }
            sort_policy_cascade_notices(&dependency_ctx, &mut plan);

            if !cascade && !plan.blocker_details.is_empty() {
                let (_, source_name) = plan
                    .blocker_source
                    .unwrap_or((expected_relkind, expected_name.to_string()));
                return Err(ExecError::DetailedError {
                    message: format!(
                        "cannot drop {expected_name} {source_name} because other objects depend on it"
                    ),
                    detail: Some(plan.blocker_details.join("\n")),
                    hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
                    sqlstate: "2BP01",
                });
            }

            if cascade {
                match plan.notices.as_slice() {
                    [] => {}
                    [notice] => push_notice(notice.clone()),
                    notices => push_notice_with_detail(
                        format!("drop cascades to {} other objects", notices.len()),
                        notices.join("\n"),
                    ),
                }
            }

            let mut next_cid = cid;
            for policy in &plan.policy_drops {
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: next_cid,
                    client_id,
                    waiter: None,
                    interrupts: Arc::clone(&interrupts),
                };
                let (_removed, effect) = self
                    .catalog
                    .write()
                    .drop_policy_mvcc(policy.relation_oid, &policy.policy_name, &ctx)
                    .map_err(map_catalog_error)?;
                catalog_effects.push(effect);
                next_cid = next_cid.saturating_add(1);
            }

            for rule in &plan.rule_drops {
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: next_cid,
                    client_id,
                    waiter: None,
                    interrupts: Arc::clone(&interrupts),
                };
                let effect = self
                    .catalog
                    .write()
                    .drop_rule_mvcc(rule.rewrite_oid, &ctx)
                    .map_err(map_catalog_error)?;
                catalog_effects.push(effect);
                next_cid = next_cid.saturating_add(1);
            }

            for relation_oid in &plan.relation_drop_order {
                let (relkind, relpersistence) = catcache
                    .class_by_oid(*relation_oid)
                    .map(|row| (row.relkind, row.relpersistence))
                    .unwrap_or((expected_relkind, 'p'));
                if relpersistence == 't' {
                    let temp_name = self
                        .temp_relation_name_for_oid(client_id, *relation_oid)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::UnexpectedToken {
                                expected: "tracked temporary relation",
                                actual: relation_oid.to_string(),
                            })
                        })?;
                    if relkind == 'v' {
                        let ctx = CatalogWriteContext {
                            pool: self.pool.clone(),
                            txns: self.txns.clone(),
                            xid,
                            cid: next_cid,
                            client_id,
                            waiter: Some(self.txn_waiter.clone()),
                            interrupts: Arc::clone(&interrupts),
                        };
                        let effect = self
                            .catalog
                            .write()
                            .drop_view_by_oid_mvcc(*relation_oid, &ctx)
                            .map(|(_, effect)| effect)
                            .map_err(map_catalog_error)?;
                        catalog_effects.push(effect);
                        self.remove_temp_entry_after_catalog_drop(
                            client_id,
                            &temp_name,
                            temp_effects
                                .as_deref_mut()
                                .expect("temp effects required for DROP VIEW"),
                        )?;
                    } else {
                        self.drop_temp_relation_in_transaction(
                            client_id,
                            &temp_name,
                            xid,
                            next_cid,
                            catalog_effects,
                            temp_effects
                                .as_deref_mut()
                                .expect("temp effects required for DROP VIEW"),
                        )?;
                    }
                    next_cid = next_cid.saturating_add(1);
                    continue;
                }
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: next_cid,
                    client_id,
                    waiter: Some(self.txn_waiter.clone()),
                    interrupts: Arc::clone(&interrupts),
                };
                let effect = match relkind {
                    'v' => self
                        .catalog
                        .write()
                        .drop_view_by_oid_mvcc(*relation_oid, &ctx)
                        .map(|(_, effect)| effect),
                    _ => self
                        .catalog
                        .write()
                        .drop_relation_by_oid_mvcc(*relation_oid, &ctx)
                        .map(|(_, effect)| effect),
                }
                .map_err(map_catalog_error)?;
                if relkind != 'v' {
                    self.apply_catalog_mutation_effect_immediate(&effect)?;
                }
                if matches!(relkind, 'r' | 'p' | 'm') {
                    self.session_stats_state(client_id)
                        .write()
                        .note_relation_drop(*relation_oid, &self.stats);
                }
                catalog_effects.push(effect);
                next_cid = next_cid.saturating_add(1);
            }

            Ok(StatementResult::AffectedRows(dropped))
        })();

        for rel in rels {
            self.table_locks.unlock_table(rel, client_id);
        }

        result
    }
}

fn drop_signature_for_oids(catalog: &dyn CatalogLookup, name: &str, arg_oids: &[u32]) -> String {
    let args = arg_oids
        .iter()
        .map(|oid| drop_signature_type_name(catalog, *oid))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{name}({args})")
}

fn drop_signature_type_name(catalog: &dyn CatalogLookup, oid: u32) -> String {
    if let Some(row) = catalog.type_by_oid(oid) {
        let mut sql_type = row.sql_type;
        sql_type.type_oid = 0;
        return format_sql_type_name(sql_type);
    }
    oid.to_string()
}

fn drop_routine_signature_matches(
    row: &crate::include::catalog::PgProcRow,
    specs: &[DropRoutineArgSpec],
    proc_kind: char,
    arg_list_specified: bool,
) -> bool {
    if specs.is_empty() {
        if !arg_list_specified {
            return true;
        }
        return parse_proc_argtype_oids(&row.proargtypes)
            .map(|input_oids| input_oids.is_empty())
            .unwrap_or(false);
    }
    if proc_kind != 'p' || row.proallargtypes.is_none() {
        let input_oids = parse_proc_argtype_oids(&row.proargtypes).unwrap_or_default();
        let callable_specs = specs
            .iter()
            .filter(|spec| !matches!(spec.mode, Some(b'o')))
            .collect::<Vec<_>>();
        return input_oids.len() == callable_specs.len()
            && input_oids
                .iter()
                .zip(callable_specs)
                .all(|(oid, spec)| *oid == spec.type_oid);
    }

    let all_oids = row.proallargtypes.as_deref().unwrap_or_default();
    let modes = row.proargmodes.as_deref().unwrap_or_default();
    all_oids.len() == specs.len()
        && all_oids.iter().enumerate().all(|(index, oid)| {
            let row_mode = modes.get(index).copied().unwrap_or(b'i');
            let spec = &specs[index];
            *oid == spec.type_oid
                && spec
                    .mode
                    .map(|mode| mode == row_mode || (mode == b'v' && row.provariadic != 0))
                    .unwrap_or(true)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::utils::misc::notices::{
        clear_notices as clear_backend_notices, take_notices as take_backend_notices,
    };
    use crate::pgrust::session::Session;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "pgrust_drop_{}_{}_{}",
            label,
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn take_backend_notice_messages() -> Vec<String> {
        take_backend_notices()
            .into_iter()
            .map(|notice| notice.message)
            .collect()
    }

    fn take_backend_notices_with_detail() -> Vec<(String, Option<String>)> {
        take_backend_notices()
            .into_iter()
            .map(|notice| (notice.message, notice.detail))
            .collect()
    }

    #[test]
    fn drop_index_removes_index_relation() {
        let base = temp_dir("index");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        session
            .execute(&db, "create index widgets_id_idx on widgets(id)")
            .unwrap();

        assert!(
            db.backend_catcache(1, None)
                .unwrap()
                .class_by_name("widgets_id_idx")
                .is_some()
        );

        session.execute(&db, "drop index widgets_id_idx").unwrap();

        assert!(
            db.backend_catcache(1, None)
                .unwrap()
                .class_by_name("widgets_id_idx")
                .is_none()
        );
    }

    #[test]
    fn drop_table_restrict_reports_pg_style_foreign_key_dependency() {
        let base = temp_dir("table_fk_restrict");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table parents (id int4 primary key)")
            .unwrap();
        session
            .execute(
                &db,
                "create table children (id int4 primary key, parent_id int4 references parents)",
            )
            .unwrap();

        match session.execute(&db, "drop table parents") {
            Err(ExecError::DetailedError {
                message,
                detail: Some(detail),
                hint: Some(hint),
                sqlstate,
            }) => {
                assert_eq!(
                    message,
                    "cannot drop table parents because other objects depend on it"
                );
                assert_eq!(
                    detail,
                    "constraint children_parent_id_fkey on table children depends on table parents"
                );
                assert_eq!(
                    hint,
                    "Use DROP ... CASCADE to drop the dependent objects too."
                );
                assert_eq!(sqlstate, "2BP01");
            }
            other => panic!("expected detailed dependency error, got {other:?}"),
        }
    }

    #[test]
    fn drop_table_cascade_removes_foreign_key_constraint_and_emits_notice() {
        let base = temp_dir("table_fk_cascade");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table parents (id int4 primary key)")
            .unwrap();
        session
            .execute(
                &db,
                "create table children (id int4 primary key, parent_id int4 references parents)",
            )
            .unwrap();

        clear_backend_notices();
        session.execute(&db, "drop table parents cascade").unwrap();

        assert_eq!(
            take_backend_notice_messages(),
            vec![String::from(
                "drop cascades to constraint children_parent_id_fkey on table children",
            )]
        );
        let child_oid = db
            .lazy_catalog_lookup(1, None, None)
            .lookup_any_relation("children")
            .unwrap()
            .relation_oid;
        assert!(
            !db.backend_catcache(1, None)
                .unwrap()
                .constraint_rows()
                .into_iter()
                .any(|row| row.conrelid == child_oid && row.contype == CONSTRAINT_FOREIGN)
        );
    }

    #[test]
    fn drop_table_cascade_drops_dependent_view_and_emits_notice() {
        let base = temp_dir("table_view_cascade");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table base_items (id int4)")
            .unwrap();
        session
            .execute(&db, "create view base_view as select id from base_items")
            .unwrap();

        clear_backend_notices();
        session
            .execute(&db, "drop table base_items cascade")
            .unwrap();

        assert_eq!(
            take_backend_notice_messages(),
            vec![String::from("drop cascades to view base_view")]
        );
        assert!(
            db.backend_catcache(1, None)
                .unwrap()
                .class_by_name("base_view")
                .is_none()
        );
    }

    #[test]
    fn drop_table_cascade_drops_inherited_children() {
        let base = temp_dir("table_inherit_cascade");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create table p1 (id int4)").unwrap();
        session
            .execute(&db, "create table c1 () inherits (p1)")
            .unwrap();

        clear_backend_notices();
        session.execute(&db, "drop table p1 cascade").unwrap();

        assert_eq!(
            take_backend_notice_messages(),
            vec![String::from("drop cascades to table c1")]
        );
        let catcache = db.backend_catcache(1, None).unwrap();
        assert!(catcache.class_by_name("p1").is_none());
        assert!(catcache.class_by_name("c1").is_none());
    }

    #[test]
    fn drop_table_drops_many_partition_children_from_dependency_graph() {
        let base = temp_dir("table_many_partition_children");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(
                &db,
                "create table ddl_heavy_parted (a int4, b text) partition by range (a)",
            )
            .unwrap();
        for idx in 0..12 {
            session
                .execute(
                    &db,
                    &format!(
                        "create table ddl_heavy_parted_{idx} partition of ddl_heavy_parted for values from ({}) to ({})",
                        idx * 100,
                        (idx + 1) * 100
                    ),
                )
                .unwrap();
        }

        session.execute(&db, "drop table ddl_heavy_parted").unwrap();

        let remaining = db
            .backend_catcache(1, None)
            .unwrap()
            .class_rows()
            .into_iter()
            .filter(|row| row.relname.starts_with("ddl_heavy_parted"))
            .count();
        assert_eq!(remaining, 0);
    }

    #[test]
    fn drop_table_cascade_groups_many_view_notices_and_drops_views() {
        let base = temp_dir("table_many_view_cascade");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table base_tbl (a int primary key, b text default 'Unspecified')",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create view ro_view1 as select distinct a, b from base_tbl",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create view ro_view2 as select a, b from base_tbl group by a, b",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create view ro_view3 as select 1 from base_tbl having max(a) > 0",
            )
            .unwrap();
        session
            .execute(&db, "create view ro_view4 as select count(*) from base_tbl")
            .unwrap();
        session
            .execute(
                &db,
                "create view ro_view5 as select a, rank() over() from base_tbl",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create view ro_view6 as select a, b from base_tbl union select -a, b from base_tbl",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create view ro_view7 as with t as (select a, b from base_tbl) select * from t",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create view ro_view8 as select a, b from base_tbl order by a offset 1",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create view ro_view9 as select a, b from base_tbl order by a limit 1",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create view ro_view11 as select b1.a, b2.b from base_tbl b1, base_tbl b2",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create view ro_view13 as select a, b from (select * from base_tbl) as t",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create view rw_view14 as select ctid, a, b from base_tbl",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create view rw_view15 as select a, upper(b) from base_tbl",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create view rw_view16 as select a, b, a as aa from base_tbl",
            )
            .unwrap();
        session
            .execute(&db, "create view ro_view17 as select * from ro_view1")
            .unwrap();
        session
            .execute(
                &db,
                "create view ro_view20 as select a, b, generate_series(1, a) g from base_tbl",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create rule rw_view16_ins_rule as on insert to rw_view16 where new.a > 0 do instead insert into base_tbl values (new.a, new.b)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create rule rw_view16_upd_rule as on update to rw_view16 where old.a > 0 do instead update base_tbl set b = new.b where a = old.a",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create rule rw_view16_del_rule as on delete to rw_view16 where old.a > 0 do instead delete from base_tbl where a = old.a",
            )
            .unwrap();

        clear_backend_notices();
        session.execute(&db, "drop table base_tbl cascade").unwrap();

        assert_eq!(
            take_backend_notices_with_detail(),
            vec![(
                "drop cascades to 16 other objects".into(),
                Some(
                    [
                        "drop cascades to view ro_view1",
                        "drop cascades to view ro_view17",
                        "drop cascades to view ro_view2",
                        "drop cascades to view ro_view3",
                        "drop cascades to view ro_view4",
                        "drop cascades to view ro_view5",
                        "drop cascades to view ro_view6",
                        "drop cascades to view ro_view7",
                        "drop cascades to view ro_view8",
                        "drop cascades to view ro_view9",
                        "drop cascades to view ro_view11",
                        "drop cascades to view ro_view13",
                        "drop cascades to view rw_view14",
                        "drop cascades to view rw_view15",
                        "drop cascades to view rw_view16",
                        "drop cascades to view ro_view20",
                    ]
                    .join("\n"),
                ),
            )]
        );

        let catcache = db.backend_catcache(1, None).unwrap();
        for name in [
            "base_tbl",
            "ro_view1",
            "ro_view17",
            "ro_view2",
            "ro_view3",
            "ro_view4",
            "ro_view5",
            "ro_view6",
            "ro_view7",
            "ro_view8",
            "ro_view9",
            "ro_view11",
            "ro_view13",
            "rw_view14",
            "rw_view15",
            "rw_view16",
            "ro_view20",
        ] {
            assert!(
                catcache.class_by_name(name).is_none(),
                "{name} should be dropped"
            );
        }
    }

    #[test]
    fn drop_table_allows_explicitly_dropping_parent_and_child_together() {
        let base = temp_dir("table_fk_explicit_multi_drop");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table parents (id int4 primary key)")
            .unwrap();
        session
            .execute(
                &db,
                "create table children (id int4 primary key, parent_id int4 references parents)",
            )
            .unwrap();

        clear_backend_notices();
        session
            .execute(&db, "drop table parents, children")
            .unwrap();

        assert!(take_backend_notice_messages().is_empty());
        let catcache = db.backend_catcache(1, None).unwrap();
        assert!(catcache.class_by_name("parents").is_none());
        assert!(catcache.class_by_name("children").is_none());
    }

    #[test]
    fn drop_table_allows_explicit_partitioned_fk_table_with_referenced_parent() {
        let base = temp_dir("table_fk_explicit_partitioned_multi_drop");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table parents (id int4 primary key)")
            .unwrap();
        session
            .execute(
                &db,
                "create table children (id int4 references parents) partition by range (id)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table children_1 partition of children for values from (0) to (10)",
            )
            .unwrap();

        clear_backend_notices();
        session
            .execute(&db, "drop table parents, children")
            .unwrap();

        assert!(take_backend_notice_messages().is_empty());
        let catcache = db.backend_catcache(1, None).unwrap();
        assert!(catcache.class_by_name("parents").is_none());
        assert!(catcache.class_by_name("children").is_none());
        assert!(catcache.class_by_name("children_1").is_none());
    }

    #[test]
    fn drop_table_cascade_cleans_temp_inherited_child_namespace_state() {
        let base = temp_dir("table_temp_inherit_cascade");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table parents (id int4)")
            .unwrap();
        session
            .execute(&db, "create temp table temp_child () inherits (parents)")
            .unwrap();

        assert!(db.temp_entry(1, "temp_child").is_some());

        clear_backend_notices();
        session.execute(&db, "drop table parents cascade").unwrap();

        let notices = take_backend_notice_messages();
        assert_eq!(notices.len(), 1);
        assert!(notices[0].contains("temp_child"));
        assert!(db.temp_entry(1, "temp_child").is_none());
        let namespace = db.temp_relations.read().get(&1).cloned().unwrap();
        assert!(namespace.tables.is_empty());
        assert!(
            db.backend_catcache(1, None)
                .unwrap()
                .class_by_name("parents")
                .is_none()
        );
    }

    #[test]
    fn drop_schema_removes_empty_namespace() {
        let base = temp_dir("schema");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create schema tenant_drop").unwrap();

        assert!(
            db.backend_catcache(1, None)
                .unwrap()
                .namespace_by_name("tenant_drop")
                .is_some()
        );

        session.execute(&db, "drop schema tenant_drop").unwrap();

        assert!(
            db.backend_catcache(1, None)
                .unwrap()
                .namespace_by_name("tenant_drop")
                .is_none()
        );
    }

    #[test]
    fn drop_schema_cascade_removes_schema_relations_and_functions() {
        let base = temp_dir("schema_cascade");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create schema tenant_drop").unwrap();
        session
            .execute(&db, "set search_path = tenant_drop")
            .unwrap();
        session
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        session
            .execute(&db, "create view widget_view as select id from widgets")
            .unwrap();
        session
            .execute(&db, "create type tenant_pair as (x int4)")
            .unwrap();
        session
            .execute(
                &db,
                "create view pair_view as select row(id)::tenant_pair as pair from widgets",
            )
            .unwrap();
        session
            .execute(&db, "create table pair_log (old_row pair_view)")
            .unwrap();
        session
            .execute(
                &db,
                "create function tenant_fn() returns int4 language sql as $$ select 1 $$",
            )
            .unwrap();

        session
            .execute(&db, "drop schema tenant_drop cascade")
            .unwrap();

        let catcache = db.backend_catcache(1, None).unwrap();
        assert!(catcache.namespace_by_name("tenant_drop").is_none());
        assert!(!catcache.class_rows().into_iter().any(|row| matches!(
            row.relname.as_str(),
            "widgets" | "widget_view" | "tenant_pair" | "pair_view" | "pair_log"
        )));
        assert!(
            !catcache
                .type_rows()
                .into_iter()
                .any(|row| row.typname == "tenant_pair")
        );
        assert!(
            !catcache
                .proc_rows()
                .into_iter()
                .any(|row| row.proname == "tenant_fn")
        );
    }
}
