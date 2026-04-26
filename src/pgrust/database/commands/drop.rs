use super::super::*;
use super::create::{aggregate_signature_arg_oids, resolve_aggregate_proc_rows};
use super::dependency_drop::{CatalogDependencyGraph, DropBehavior, ObjectAddress};
use crate::backend::executor::expr_reg::format_type_text;
use crate::backend::parser::{parse_type_name, resolve_raw_type_name};
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::misc::notices::{push_notice, push_notice_with_detail};
use crate::include::catalog::{
    CONSTRAINT_FOREIGN, DEPENDENCY_NORMAL, PG_CLASS_RELATION_OID, PG_CONSTRAINT_RELATION_OID,
    PG_REWRITE_RELATION_OID, PgConstraintRow, PgProcRow, PgRewriteRow,
};
use crate::include::nodes::parsenodes::{
    DropAggregateStatement, DropFunctionStatement, DropIndexStatement, DropProcedureStatement,
    DropSchemaStatement,
};
use crate::pgrust::database::ddl::format_sql_type_name;
use crate::pgrust::database::save_range_type_entries;
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

fn domain_has_range_dependents_error(type_name: &str, dependent_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("cannot drop type {type_name} because other objects depend on it"),
        detail: Some(format!("type {dependent_name} depends on type {type_name}")),
        hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
        sqlstate: "2BP01",
    }
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
    match schema_name.as_str() {
        "public" | "pg_catalog" => object_name.to_string(),
        _ => format!("{schema_name}.{object_name}"),
    }
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
        row_type_oid,
        array_type_oid,
        reltoastrelid: relation.toast.map(|toast| toast.relation_oid).unwrap_or(0),
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
    blocker_details: Vec<String>,
    blocker_source: Option<(char, String)>,
    notices: Vec<String>,
}

struct DropTableDependencyContext<'a> {
    catcache: &'a CatCache,
    graph: &'a CatalogDependencyGraph,
    constraints_by_oid: BTreeMap<u32, PgConstraintRow>,
    rewrites_by_oid: BTreeMap<u32, PgRewriteRow>,
}

fn is_drop_table_relkind(relkind: char) -> bool {
    matches!(relkind, 'r' | 'p')
}

fn drop_table_relation_kind_name(relkind: char) -> &'static str {
    match relkind {
        'm' => "materialized view",
        'p' => "table",
        'S' => "sequence",
        'v' => "view",
        _ => "table",
    }
}

fn drop_table_display_relation_name(catcache: &CatCache, relation_oid: u32) -> String {
    let Some(class) = catcache.class_by_oid(relation_oid) else {
        return relation_oid.to_string();
    };
    let schema_name = catcache
        .namespace_by_oid(class.relnamespace)
        .map(|row| row.nspname.clone())
        .unwrap_or_else(|| "public".to_string());
    match schema_name.as_str() {
        "public" | "pg_catalog" => class.relname.clone(),
        schema_name if schema_name.starts_with("pg_temp_") => class.relname.clone(),
        _ => format!("{schema_name}.{}", class.relname),
    }
}

fn drop_schema_display_relation_name(
    catcache: &CatCache,
    relation_oid: u32,
    current_role_name: &str,
) -> String {
    let Some(class) = catcache.class_by_oid(relation_oid) else {
        return relation_oid.to_string();
    };
    let schema_name = catcache
        .namespace_by_oid(class.relnamespace)
        .map(|row| row.nspname.clone())
        .unwrap_or_else(|| "public".to_string());
    match schema_name.as_str() {
        "public" | "pg_catalog" => class.relname.clone(),
        schema_name if schema_name.starts_with("pg_temp_") => class.relname.clone(),
        schema_name if schema_name.eq_ignore_ascii_case(current_role_name) => class.relname.clone(),
        _ => format!("{schema_name}.{}", class.relname),
    }
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
    let lower = text.to_ascii_lowercase();
    for (mode, callable) in [
        ("inout", true),
        ("variadic", true),
        ("in", true),
        ("out", false),
    ] {
        if lower == mode || lower.starts_with(&format!("{mode} ")) {
            if !callable {
                return Ok(None);
            }
            text = text[mode.len()..].trim_start();
            break;
        }
    }

    let raw_type = match parse_type_name(text).and_then(|raw_type| {
        resolve_raw_type_name(&raw_type, catalog).map(|sql_type| (raw_type, sql_type))
    }) {
        Ok((raw_type, _)) => raw_type,
        Err(first_err) => {
            let Some(rest) = strip_leading_sql_word(text) else {
                return Err(first_err);
            };
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
    let lower = text.to_ascii_lowercase();
    let mut parsed_mode = None;
    for (mode, code) in [
        ("inout", b'b'),
        ("variadic", b'v'),
        ("in", b'i'),
        ("out", b'o'),
    ] {
        if lower == mode || lower.starts_with(&format!("{mode} ")) {
            parsed_mode = Some(code);
            text = text[mode.len()..].trim_start();
            break;
        }
    }

    let raw_type = match parse_type_name(text).and_then(|raw_type| {
        resolve_raw_type_name(&raw_type, catalog).map(|sql_type| (raw_type, sql_type))
    }) {
        Ok((raw_type, _)) => raw_type,
        Err(first_err) => {
            let Some(rest) = strip_leading_sql_word(text) else {
                return Err(first_err);
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

fn drop_table_direct_dependencies(
    ctx: &DropTableDependencyContext<'_>,
    relation_oid: u32,
) -> Vec<DropTableDependency> {
    let mut relation_oids = BTreeSet::new();
    let mut constraint_oids = BTreeSet::new();
    let mut rule_oids = BTreeSet::new();
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
                let Some(class) = ctx.catcache.class_by_oid(row.objid) else {
                    continue;
                };
                if !matches!(class.relkind, 'r' | 'p' | 'S' | 'v' | 'm') {
                    continue;
                }
                deps.push(DropTableDependency::Relation {
                    relation_oid: row.objid,
                    relkind: class.relkind,
                    is_partition: class.relispartition,
                    display_name: drop_table_display_relation_name(ctx.catcache, row.objid),
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
                        ctx.catcache,
                        constraint.conrelid,
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
                let Some(owner) = ctx.catcache.class_by_oid(rewrite.ev_class) else {
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
                        display_name: drop_table_display_relation_name(ctx.catcache, owner.oid),
                    });
                } else if rule_oids.insert(rewrite.oid) {
                    deps.push(DropTableDependency::Rule {
                        relation_oid: owner.oid,
                        relation_kind: owner.relkind,
                        relation_display_name: drop_table_display_relation_name(
                            ctx.catcache,
                            owner.oid,
                        ),
                        rule: DropRulePlan {
                            rewrite_oid: rewrite.oid,
                        },
                        rule_name: rewrite.rulename.clone(),
                    });
                }
            }
            _ => {}
        }
    }

    for inherit in ctx.graph.inheritance_children(relation_oid) {
        if !relation_oids.insert(inherit.inhrelid) {
            continue;
        }
        let Some(class) = ctx.catcache.class_by_oid(inherit.inhrelid) else {
            continue;
        };
        if !matches!(class.relkind, 'r' | 'p') {
            continue;
        }
        deps.push(DropTableDependency::Relation {
            relation_oid: inherit.inhrelid,
            relkind: class.relkind,
            is_partition: class.relispartition,
            display_name: drop_table_display_relation_name(ctx.catcache, inherit.inhrelid),
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

    let Some(class) = ctx.catcache.class_by_oid(relation_oid) else {
        return;
    };
    let source_relkind = class.relkind;
    let source_name = drop_table_display_relation_name(ctx.catcache, relation_oid);
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
                }
            }
            DropTableDependency::ForeignKey {
                relation_oid: dependent_relation_oid,
                ref constraint,
                ..
            } => {
                if explicit_relation_oids.contains(&dependent_relation_oid)
                    || plan.relation_drop_oids.contains(&dependent_relation_oid)
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
                if explicit_relation_oids.contains(&dependent_relation_oid)
                    || plan.relation_drop_oids.contains(&dependent_relation_oid)
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
        }
    }

    plan.relation_drop_order.push(relation_oid);
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
                    drop_stmt.aggregate_name
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
        self.execute_drop_function_stmt_in_transaction_with_kind(
            client_id,
            drop_stmt,
            xid,
            cid,
            configured_search_path,
            catalog_effects,
            'f',
            "function",
        )
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
                arg_types: procedure.arg_types.clone(),
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
                arg_types: routine.arg_types.clone(),
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
                drop_routine_signature_matches(row, &desired_arg_specs, effective_kind)
                    && (row.prokind == proc_kind
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
                    && drop_routine_signature_matches(row, &desired_arg_specs, proc_kind)
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
            .drop_proc_by_oid_mvcc(proc_row.oid, &ctx)
            .map(|(_, effect)| effect)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
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
    ) -> Result<(), ExecError> {
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let interrupts = self.interrupt_state(client_id);
        let relation_rows = catcache
            .class_rows()
            .into_iter()
            .filter(|row| row.relnamespace == schema_oid)
            .collect::<Vec<_>>();
        let proc_rows = catcache
            .proc_rows()
            .into_iter()
            .filter(|row| row.pronamespace == schema_oid)
            .collect::<Vec<_>>();
        let mut next_cid = cid;

        for relation in relation_rows {
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

        Ok(())
    }

    pub(crate) fn execute_drop_domain_stmt_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropDomainStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let (normalized, _, _) =
            self.normalize_domain_name_for_create(&drop_stmt.domain_name, configured_search_path)?;
        let Some(domain) = self.domains.read().get(&normalized).cloned() else {
            if drop_stmt.if_exists {
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::Parse(ParseError::UnsupportedType(
                drop_stmt.domain_name.clone(),
            )));
        };
        let default_range_name = format!("{}range", domain.name);
        let dependent_ranges = self
            .range_types
            .read()
            .iter()
            .filter(|(_, entry)| {
                entry.subtype_dependency_oid == Some(domain.oid)
                    || entry.subtype.type_oid == domain.oid
                    || entry.name.eq_ignore_ascii_case(&default_range_name)
            })
            .map(|(key, entry)| (key.clone(), entry.name.clone()))
            .collect::<Vec<_>>();
        if !drop_stmt.cascade
            && let Some((_, dependent_name)) = dependent_ranges.first()
        {
            return Err(domain_has_range_dependents_error(
                &domain.name,
                dependent_name,
            ));
        }
        let mut domains = self.domains.write();
        domains.remove(&normalized);
        drop(domains);
        if drop_stmt.cascade {
            if !dependent_ranges.is_empty() {
                let mut range_types = self.range_types.write();
                for (key, name) in dependent_ranges {
                    push_notice(format!("drop cascades to type {name}"));
                    range_types.remove(&key);
                }
                save_range_type_entries(&self.cluster.base_dir, self.database_oid, &range_types)?;
            }
        }

        self.refresh_catalog_store_dynamic_type_rows(client_id, configured_search_path);
        self.invalidate_backend_cache_state(client_id);
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
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
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let search_path = self.effective_search_path(client_id, configured_search_path);
        let mut dynamic_type_rows = self.domain_type_rows_for_search_path(&search_path);
        dynamic_type_rows.extend(self.enum_type_rows_for_search_path(&search_path));
        dynamic_type_rows.extend(self.range_type_rows_for_search_path(&search_path));
        let mut rels = Vec::new();
        let mut dropped = 0usize;
        let mut explicit_relation_oids = BTreeSet::new();

        for relation_name in &drop_stmt.table_names {
            let relation = match catalog.lookup_any_relation(relation_name) {
                Some(relation) if is_drop_table_relkind(relation.relkind) => relation,
                Some(_) => {
                    return Err(ExecError::Parse(ParseError::WrongObjectType {
                        name: relation_name.clone(),
                        expected: "table",
                    }));
                }
                None if drop_stmt.if_exists => continue,
                None => {
                    return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                        relation_name.clone(),
                    )));
                }
            };

            ensure_relation_owner(self, client_id, &relation, relation_name)?;
            explicit_relation_oids.insert(relation.relation_oid);
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
            let behavior = DropBehavior::from_cascade(drop_stmt.cascade);
            let dependency_ctx = DropTableDependencyContext {
                catcache: &catcache,
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
            };
            let mut plan = DropTablePlan::default();
            for &relation_oid in &explicit_relation_oids {
                plan_drop_table_relation(
                    &dependency_ctx,
                    relation_oid,
                    &explicit_relation_oids,
                    behavior,
                    &mut plan,
                );
            }

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
                let (relkind, relpersistence) = catcache
                    .class_by_oid(*relation_oid)
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
                    self.drop_temp_relation_in_transaction(
                        client_id,
                        &temp_name,
                        xid,
                        next_cid,
                        catalog_effects,
                        temp_effects,
                    )?;
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
    ) -> Result<StatementResult, ExecError> {
        self.execute_drop_relation_stmt_in_transaction_with_search_path(
            client_id,
            &drop_stmt.view_names,
            drop_stmt.if_exists,
            xid,
            cid,
            configured_search_path,
            catalog_effects,
            None,
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
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let mut requested_oids = BTreeSet::new();
        let mut rels = Vec::new();
        for index_name in &drop_stmt.index_names {
            let Some(entry) = catalog.lookup_any_relation(index_name) else {
                if drop_stmt.if_exists {
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
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let mut dropped = 0usize;
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
                let current_role_name = auth_catalog
                    .role_by_oid(auth.current_user_oid())
                    .map(|row| row.rolname.as_str())
                    .unwrap_or("");
                for relation in relation_rows
                    .iter()
                    .filter(|row| matches!(row.relkind, 'r' | 'p' | 'm' | 'S' | 'v'))
                {
                    push_notice(format!(
                        "drop cascades to {} {}",
                        drop_table_relation_kind_name(relation.relkind),
                        drop_schema_display_relation_name(
                            &catcache,
                            relation.oid,
                            current_role_name
                        )
                    ));
                }
            }
            if has_relations || has_procs {
                self.drop_schema_owned_objects_in_transaction(
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
                cid,
                client_id,
                waiter: Some(self.txn_waiter.clone()),
                interrupts: self.interrupt_state(client_id),
            };
            let effect = self
                .catalog
                .write()
                .drop_namespace_mvcc(schema.oid, &schema.nspname, schema.nspowner, &ctx)
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
            dropped += 1;
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
        expected_relkind: char,
        expected_name: &'static str,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
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
                    if expected_relkind != 'v' {
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
) -> bool {
    if specs.is_empty() {
        return true;
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
        assert!(
            !catcache
                .class_rows()
                .into_iter()
                .any(|row| row.relname == "widgets")
        );
        assert!(
            !catcache
                .proc_rows()
                .into_iter()
                .any(|row| row.proname == "tenant_fn")
        );
    }
}
