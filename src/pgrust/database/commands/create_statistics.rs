use std::collections::BTreeSet;
use std::sync::Arc;

use super::super::*;
use super::privilege::{acl_grants_privilege, effective_acl_grantee_names};
use crate::backend::parser::{
    AlterStatisticsAction, AlterStatisticsStatement, CommentOnStatisticsStatement,
    CreateStatisticsStatement, DropStatisticsStatement, ParseError,
};
use crate::backend::utils::misc::notices::{push_notice, push_warning};
use crate::include::catalog::{BTREE_AM_OID, PG_CATALOG_NAMESPACE_OID, PgStatisticExtRow};
use crate::include::nodes::parsenodes::ColumnGeneratedKind;
use crate::pgrust::database::ddl::{
    ensure_can_set_role, ensure_relation_owner, format_sql_type_name, is_system_column_name,
    normalize_statistics_target,
};

const SCHEMA_CREATE_PRIVILEGE_CHAR: char = 'C';

#[derive(Debug, Clone)]
struct ResolvedStatisticsTargets {
    column_keys: Vec<i16>,
    expression_texts: Vec<String>,
    kind_bytes: Vec<u8>,
}

fn ensure_statistics_schema_create_privilege(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    namespace_oid: u32,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    if auth_catalog
        .role_by_oid(auth.current_user_oid())
        .is_some_and(|row| row.rolsuper)
    {
        return Ok(());
    }
    let catalog = db.lazy_catalog_lookup(client_id, txn_ctx, None);
    let namespace =
        catalog
            .namespace_row_by_oid(namespace_oid)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("schema with OID {namespace_oid} does not exist"),
                detail: None,
                hint: None,
                sqlstate: "3F000",
            })?;
    if auth.has_effective_membership(namespace.nspowner, &auth_catalog) {
        return Ok(());
    }
    let owner_name = auth_catalog
        .role_by_oid(namespace.nspowner)
        .map(|row| row.rolname.clone())
        .unwrap_or_default();
    let acl = namespace
        .nspacl
        .clone()
        .unwrap_or_else(|| vec![format!("{owner_name}=UC/{owner_name}")]);
    let effective_names = effective_acl_grantee_names(&auth, &auth_catalog);
    if acl_grants_privilege(&acl, &effective_names, SCHEMA_CREATE_PRIVILEGE_CHAR) {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!("permission denied for schema {}", namespace.nspname),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}

impl Database {
    pub(crate) fn execute_create_statistics_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateStatisticsStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut temp_effects = Vec::new();
        let result = self.execute_create_statistics_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
            &mut temp_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects, &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_statistics_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateStatisticsStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let relation_name = normalize_statistics_from_clause(&create_stmt.from_clause)?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = match catalog.lookup_any_relation(&relation_name) {
            Some(entry) if statistics_relation_kind_supported(entry.relkind) => entry,
            Some(entry) => {
                return Err(unsupported_statistics_relation_error(
                    &relation_name,
                    entry.relkind,
                ));
            }
            None => return Err(ExecError::Parse(ParseError::UnknownTable(relation_name))),
        };
        if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "user table for CREATE STATISTICS",
                actual: "system catalog".into(),
            }));
        }
        ensure_relation_owner(self, client_id, &relation, &create_stmt.from_clause)?;

        let resolved_targets = resolve_statistics_targets(
            self,
            client_id,
            xid,
            cid,
            &relation,
            &create_stmt.targets,
            &create_stmt.kinds,
        )?;
        let (statistics_name, namespace_oid) = self.resolve_create_statistics_name(
            client_id,
            xid,
            cid,
            create_stmt,
            configured_search_path,
            &relation,
            temp_effects,
            catalog_effects,
        )?;
        ensure_statistics_schema_create_privilege(
            self,
            client_id,
            Some((xid, cid)),
            namespace_oid,
        )?;

        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        if catcache
            .statistic_ext_row_by_name_namespace(&statistics_name, namespace_oid)
            .is_some()
        {
            if create_stmt.if_not_exists {
                push_notice(format!(
                    "statistics object \"{}\" already exists, skipping",
                    display_statistics_name(namespace_oid, &statistics_name, &catcache)
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(duplicate_statistics_error(&display_statistics_name(
                namespace_oid,
                &statistics_name,
                &catcache,
            )));
        }

        let row = PgStatisticExtRow {
            oid: 0,
            stxrelid: relation.relation_oid,
            stxname: statistics_name,
            stxnamespace: namespace_oid,
            stxowner: self.auth_state(client_id).current_user_oid(),
            stxkeys: resolved_targets.column_keys,
            stxstattarget: None,
            stxkind: resolved_targets.kind_bytes,
            stxexprs: (!resolved_targets.expression_texts.is_empty()).then(|| {
                serde_json::to_string(&resolved_targets.expression_texts)
                    .expect("statistics expressions serialize")
            }),
        };

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&self.interrupt_state(client_id)),
        };
        let (_, effect) = self
            .catalog
            .write()
            .create_statistics_mvcc(row, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_statistics_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterStatisticsStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_statistics_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_statistics_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterStatisticsStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let Some(statistics) = lookup_statistics_row(
            self,
            client_id,
            Some((xid, cid)),
            configured_search_path,
            &stmt.statistics_name,
        ) else {
            if stmt.if_exists {
                push_notice(format!(
                    "statistics object \"{}\" does not exist, skipping",
                    stmt.statistics_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(statistics_does_not_exist_error(&stmt.statistics_name));
        };
        if !auth.can_set_role(statistics.stxowner, &auth_catalog) {
            return Err(must_be_statistics_owner_error(&statistics.stxname));
        }
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&self.interrupt_state(client_id)),
        };
        let statistics_oid = statistics.oid;
        let mut clear_statistics_data = false;
        let effect = match &stmt.action {
            AlterStatisticsAction::Rename { new_name } => {
                if new_name.contains('.') {
                    return Err(ExecError::Parse(ParseError::UnsupportedQualifiedName(
                        new_name.clone(),
                    )));
                }
                let normalized = new_name.to_ascii_lowercase();
                if catcache
                    .statistic_ext_row_by_name_namespace(&normalized, statistics.stxnamespace)
                    .is_some_and(|row| row.oid != statistics.oid)
                {
                    return Err(duplicate_statistics_in_schema_error(
                        new_name,
                        statistics.stxnamespace,
                        &catcache,
                    ));
                }
                self.catalog
                    .write()
                    .replace_statistics_row_mvcc(
                        PgStatisticExtRow {
                            stxname: normalized,
                            ..statistics
                        },
                        &ctx,
                    )
                    .map_err(map_catalog_error)?
            }
            AlterStatisticsAction::SetStatistics { target } => {
                let normalized = normalize_statistics_target(i32::from(*target))?;
                if let Some(warning) = normalized.warning {
                    push_warning(warning);
                }
                clear_statistics_data = normalized.value == 0;
                self.catalog
                    .write()
                    .replace_statistics_row_mvcc(
                        PgStatisticExtRow {
                            stxstattarget: (normalized.value >= 0).then_some(normalized.value),
                            ..statistics
                        },
                        &ctx,
                    )
                    .map_err(map_catalog_error)?
            }
            AlterStatisticsAction::OwnerTo { new_owner } => {
                let role = auth_catalog
                    .role_by_name(new_owner)
                    .cloned()
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("role \"{new_owner}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "42704",
                    })?;
                ensure_can_set_role(self, client_id, role.oid, &role.rolname)?;
                self.catalog
                    .write()
                    .replace_statistics_row_mvcc(
                        PgStatisticExtRow {
                            stxowner: role.oid,
                            ..statistics
                        },
                        &ctx,
                    )
                    .map_err(map_catalog_error)?
            }
            AlterStatisticsAction::SetSchema { new_schema } => {
                let schema_name = new_schema.to_ascii_lowercase();
                let namespace_oid = self
                    .visible_namespace_oid_by_name(client_id, Some((xid, cid)), &schema_name)
                    .ok_or_else(|| schema_does_not_exist_error(&schema_name))?;
                if catcache
                    .statistic_ext_row_by_name_namespace(&statistics.stxname, namespace_oid)
                    .is_some_and(|row| row.oid != statistics.oid)
                {
                    return Err(duplicate_statistics_in_schema_error(
                        &statistics.stxname,
                        namespace_oid,
                        &catcache,
                    ));
                }
                self.catalog
                    .write()
                    .replace_statistics_row_mvcc(
                        PgStatisticExtRow {
                            stxnamespace: namespace_oid,
                            ..statistics
                        },
                        &ctx,
                    )
                    .map_err(map_catalog_error)?
            }
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        if clear_statistics_data {
            let effect = self
                .catalog
                .write()
                .replace_statistics_data_rows_mvcc(statistics_oid, Vec::new(), &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_statistics_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropStatisticsStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_statistics_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_drop_statistics_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropStatisticsStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        if stmt.cascade {
            return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "DROP STATISTICS CASCADE".into(),
            )));
        }
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let mut current_cid = cid;
        let mut dropped = 0usize;
        for statistics_name in &stmt.statistics_names {
            let Some(statistics) = lookup_statistics_row(
                self,
                client_id,
                Some((xid, current_cid)),
                configured_search_path,
                statistics_name,
            ) else {
                if stmt.if_exists {
                    push_notice(format!(
                        "statistics object \"{}\" does not exist, skipping",
                        statistics_name
                    ));
                    continue;
                }
                return Err(statistics_does_not_exist_error(statistics_name));
            };
            if !auth.has_effective_membership(statistics.stxowner, &auth_catalog) {
                return Err(must_be_statistics_owner_error(&statistics.stxname));
            }
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: current_cid,
                client_id,
                waiter: None,
                interrupts: Arc::clone(&self.interrupt_state(client_id)),
            };
            let (_, effect) = self
                .catalog
                .write()
                .drop_statistics_mvcc(statistics.oid, &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            self.plan_cache.invalidate_all();
            catalog_effects.push(effect);
            dropped = dropped.saturating_add(1);
            current_cid = current_cid.saturating_add(1);
        }
        Ok(StatementResult::AffectedRows(dropped))
    }

    pub(crate) fn execute_comment_on_statistics_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CommentOnStatisticsStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_statistics_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_comment_on_statistics_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CommentOnStatisticsStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let statistics = lookup_statistics_row(
            self,
            client_id,
            Some((xid, cid)),
            configured_search_path,
            &stmt.statistics_name,
        )
        .ok_or_else(|| statistics_does_not_exist_error(&stmt.statistics_name))?;
        if !auth.has_effective_membership(statistics.stxowner, &auth_catalog) {
            return Err(must_be_statistics_owner_error(&statistics.stxname));
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: Arc::clone(&self.interrupt_state(client_id)),
        };
        let effect = self
            .catalog
            .write()
            .comment_statistics_mvcc(statistics.oid, stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn drop_statistics_for_relation_in_transaction(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        xid: TransactionId,
        cid: &mut CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let statistic_oids = self
            .backend_catcache(client_id, Some((xid, *cid)))
            .map_err(map_catalog_error)?
            .statistic_ext_rows_for_relation(relation_oid)
            .into_iter()
            .map(|row| row.oid)
            .collect::<BTreeSet<_>>();
        self.drop_statistics_by_oid_in_transaction(
            client_id,
            statistic_oids,
            xid,
            cid,
            catalog_effects,
        )
    }

    pub(crate) fn drop_statistics_for_namespace_in_transaction(
        &self,
        client_id: ClientId,
        namespace_oid: u32,
        xid: TransactionId,
        cid: &mut CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let statistic_oids = self
            .backend_catcache(client_id, Some((xid, *cid)))
            .map_err(map_catalog_error)?
            .statistic_ext_rows()
            .into_iter()
            .filter(|row| row.stxnamespace == namespace_oid)
            .map(|row| row.oid)
            .collect::<BTreeSet<_>>();
        self.drop_statistics_by_oid_in_transaction(
            client_id,
            statistic_oids,
            xid,
            cid,
            catalog_effects,
        )
    }

    pub(crate) fn drop_statistics_for_column_in_transaction(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        attnum: i16,
        xid: TransactionId,
        cid: &mut CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let statistic_oids = self
            .backend_catcache(client_id, Some((xid, *cid)))
            .map_err(map_catalog_error)?
            .statistic_ext_rows_for_relation(relation_oid)
            .into_iter()
            .filter(|row| row.stxkeys.contains(&attnum))
            .map(|row| row.oid)
            .collect::<BTreeSet<_>>();
        self.drop_statistics_by_oid_in_transaction(
            client_id,
            statistic_oids,
            xid,
            cid,
            catalog_effects,
        )
    }

    fn drop_statistics_by_oid_in_transaction(
        &self,
        client_id: ClientId,
        statistic_oids: BTreeSet<u32>,
        xid: TransactionId,
        cid: &mut CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        for statistics_oid in statistic_oids {
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: *cid,
                client_id,
                waiter: None,
                interrupts: Arc::clone(&self.interrupt_state(client_id)),
            };
            let (_, effect) = match self
                .catalog
                .write()
                .drop_statistics_mvcc(statistics_oid, &ctx)
            {
                Ok(result) => result,
                Err(CatalogError::UnknownTable(_)) => continue,
                Err(err) => return Err(map_catalog_error(err)),
            };
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            self.plan_cache.invalidate_all();
            catalog_effects.push(effect);
            *cid = (*cid).saturating_add(1);
        }
        Ok(())
    }

    fn resolve_create_statistics_name(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        stmt: &CreateStatisticsStatement,
        configured_search_path: Option<&[String]>,
        relation: &crate::backend::parser::BoundRelation,
        temp_effects: &mut Vec<TempMutationEffect>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(String, u32), ExecError> {
        if let Some(name) = stmt.statistics_name.as_deref() {
            return self.resolve_named_statistics_target(
                client_id,
                xid,
                cid,
                name,
                configured_search_path,
                temp_effects,
                catalog_effects,
            );
        }

        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let relation_name = catcache
            .class_by_oid(relation.relation_oid)
            .map(|row| row.relname.to_ascii_lowercase())
            .unwrap_or_else(|| relation.relation_oid.to_string());
        let base_addition = stmt
            .targets
            .iter()
            .map(|target| statistics_name_target_fragment(target))
            .collect::<Vec<_>>()
            .join("_");
        let mut candidate = format!("{relation_name}_{base_addition}_stat");
        let namespace_oid = relation.namespace_oid;
        let mut suffix = 1usize;
        while catcache
            .statistic_ext_row_by_name_namespace(&candidate, namespace_oid)
            .is_some()
        {
            candidate = format!("{relation_name}_{base_addition}_stat{suffix}");
            suffix = suffix.saturating_add(1);
        }
        Ok((candidate, namespace_oid))
    }

    fn resolve_named_statistics_target(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        name: &str,
        configured_search_path: Option<&[String]>,
        temp_effects: &mut Vec<TempMutationEffect>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(String, u32), ExecError> {
        match name.split_once('.') {
            Some((schema_name, object_name)) => {
                let schema_name = schema_name.to_ascii_lowercase();
                let namespace_oid = if is_statistics_temp_schema_name(self, client_id, &schema_name)
                {
                    let mut temp_cid = cid;
                    self.ensure_temp_namespace(
                        client_id,
                        xid,
                        &mut temp_cid,
                        catalog_effects,
                        temp_effects,
                    )?
                    .oid
                } else {
                    self.visible_namespace_oid_by_name(client_id, Some((xid, cid)), &schema_name)
                        .ok_or_else(|| schema_does_not_exist_error(&schema_name))?
                };
                Ok((object_name.to_ascii_lowercase(), namespace_oid))
            }
            None => {
                let search_path = self.effective_search_path(client_id, configured_search_path);
                for schema_name in search_path {
                    if schema_name.is_empty()
                        || schema_name == "$user"
                        || schema_name == "pg_catalog"
                    {
                        continue;
                    }
                    if is_statistics_temp_schema_name(self, client_id, &schema_name) {
                        continue;
                    }
                    if let Some(namespace_oid) = self.visible_namespace_oid_by_name(
                        client_id,
                        Some((xid, cid)),
                        &schema_name,
                    ) {
                        return Ok((name.to_ascii_lowercase(), namespace_oid));
                    }
                }
                Err(ExecError::Parse(ParseError::NoSchemaSelectedForCreate))
            }
        }
    }
}

fn normalize_statistics_from_clause(from_clause: &str) -> Result<String, ExecError> {
    let input = from_clause.trim();
    if input.is_empty() {
        return Err(ExecError::Parse(ParseError::UnexpectedEof));
    }
    if input.contains(char::is_whitespace) || input.contains('(') {
        return Err(ExecError::DetailedError {
            message: "CREATE STATISTICS only supports relation names in the FROM clause".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    Ok(input.trim_matches('"').to_ascii_lowercase())
}

fn statistics_relation_kind_supported(relkind: char) -> bool {
    matches!(relkind, 'r' | 'm' | 'p' | 'f')
}

fn unsupported_statistics_relation_error(relation_name: &str, relkind: char) -> ExecError {
    let base_name = relation_name
        .rsplit_once('.')
        .map(|(_, name)| name)
        .unwrap_or(relation_name)
        .trim_matches('"');
    let detail_kind = match relkind {
        'c' => "composite types",
        'f' => "foreign tables",
        'i' | 'I' => "indexes",
        'S' => "sequences",
        't' => "TOAST tables",
        'v' => "views",
        _ => "relations of this kind",
    };
    ExecError::DetailedError {
        message: format!("cannot define statistics for relation \"{base_name}\""),
        detail: Some(format!(
            "This operation is not supported for {detail_kind}."
        )),
        hint: None,
        sqlstate: "42809",
    }
}

fn resolve_statistics_targets(
    db: &Database,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    relation: &crate::backend::parser::BoundRelation,
    targets: &[String],
    kinds: &[String],
) -> Result<ResolvedStatisticsTargets, ExecError> {
    if targets.len() > 8 {
        return Err(ExecError::DetailedError {
            message: "cannot have more than 8 columns in statistics".into(),
            detail: None,
            hint: None,
            sqlstate: "54011",
        });
    }

    let mut column_keys = Vec::new();
    let mut expression_texts = Vec::new();
    let mut seen_columns = std::collections::BTreeSet::new();
    let mut seen_exprs = std::collections::BTreeSet::new();

    for target in targets {
        let trimmed = target.trim();
        if let Some(column_name) = simple_statistics_column(trimmed) {
            if is_system_column_name(column_name) {
                return Err(statistics_system_column_error());
            }
            let (index, column) = relation
                .desc
                .columns
                .iter()
                .enumerate()
                .find(|(_, col)| !col.dropped && col.name.eq_ignore_ascii_case(column_name))
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnknownColumn(column_name.to_string()))
                })?;
            if column.generated == Some(ColumnGeneratedKind::Virtual) {
                return Err(statistics_virtual_generated_column_error());
            }
            if !seen_columns.insert(column_name.to_ascii_lowercase()) {
                return Err(ExecError::DetailedError {
                    message: "duplicate column name in statistics definition".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42701",
                });
            }
            ensure_statistics_target_type_supported(
                db,
                client_id,
                xid,
                cid,
                column.sql_type,
                column_name,
            )?;
            column_keys.push(index.saturating_add(1) as i16);
        } else {
            let expr_text = strip_statistics_expression_parens(trimmed).to_string();
            if statistics_expression_references_system_column(&expr_text) {
                return Err(statistics_system_column_error());
            }
            if statistics_expression_references_virtual_generated_column(&expr_text, relation) {
                return Err(statistics_virtual_generated_column_error());
            }
            let normalized = expr_text.to_ascii_lowercase();
            if !seen_exprs.insert(normalized) {
                return Err(ExecError::DetailedError {
                    message: "duplicate expression in statistics definition".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42701",
                });
            }
            expression_texts.push(expr_text);
        }
    }

    if targets.len() < 2 && expression_texts.len() != 1 {
        return Err(ExecError::DetailedError {
            message: "extended statistics require at least 2 columns".into(),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }

    let kind_bytes =
        resolve_statistics_kind_bytes(kinds, targets.len(), !expression_texts.is_empty())?;
    column_keys.sort_unstable();
    Ok(ResolvedStatisticsTargets {
        column_keys,
        expression_texts,
        kind_bytes,
    })
}

fn ensure_statistics_target_type_supported(
    db: &Database,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    sql_type: crate::backend::parser::SqlType,
    target_name: &str,
) -> Result<(), ExecError> {
    let type_oid = crate::backend::utils::cache::catcache::sql_type_oid(sql_type);
    if type_oid == 0 {
        return Ok(());
    }
    let has_btree = crate::backend::utils::cache::lsyscache::default_opclass_for_am_and_type(
        db,
        client_id,
        Some((xid, cid)),
        BTREE_AM_OID,
        type_oid,
    )
    .is_some();
    if has_btree {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!(
            "column \"{target_name}\" cannot be used in statistics because its type {} has no default btree operator class",
            format_sql_type_name(sql_type)
        ),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    })
}

fn resolve_statistics_kind_bytes(
    kinds: &[String],
    target_count: usize,
    has_expressions: bool,
) -> Result<Vec<u8>, ExecError> {
    if target_count == 1 && has_expressions && !kinds.is_empty() {
        return Err(ExecError::DetailedError {
            message:
                "when building statistics on a single expression, statistics kinds may not be specified"
                    .into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }

    let mut ndistinct = false;
    let mut dependencies = false;
    let mut mcv = false;
    let mut requested = false;
    for kind in kinds {
        match kind.as_str() {
            "ndistinct" => {
                ndistinct = true;
                requested = true;
            }
            "dependencies" => {
                dependencies = true;
                requested = true;
            }
            "mcv" => {
                mcv = true;
                requested = true;
            }
            other => {
                return Err(ExecError::DetailedError {
                    message: format!("unrecognized statistics kind \"{other}\""),
                    detail: None,
                    hint: None,
                    sqlstate: "22023",
                });
            }
        }
    }
    if !requested && target_count >= 2 {
        ndistinct = true;
        dependencies = true;
        mcv = true;
    }

    let mut out = Vec::new();
    if ndistinct {
        out.push(b'd');
    }
    if dependencies {
        out.push(b'f');
    }
    if mcv {
        out.push(b'm');
    }
    if has_expressions {
        out.push(b'e');
    }
    if out.is_empty() && has_expressions {
        out.push(b'e');
    }
    Ok(out)
}

fn simple_statistics_column(target: &str) -> Option<&str> {
    let trimmed = target.trim();
    let inner = if trimmed.starts_with('(') && trimmed.ends_with(')') {
        trimmed[1..trimmed.len() - 1].trim()
    } else {
        trimmed
    };
    if inner.is_empty() {
        return None;
    }
    if inner
        .chars()
        .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
        && inner
            .chars()
            .next()
            .is_some_and(|ch| ch == '_' || ch.is_ascii_alphabetic())
    {
        Some(inner)
    } else {
        None
    }
}

fn statistics_name_target_fragment(target: &str) -> String {
    simple_statistics_column(target)
        .map(|column| column.to_ascii_lowercase())
        .unwrap_or_else(|| "expr".into())
}

fn statistics_system_column_error() -> ExecError {
    ExecError::DetailedError {
        message: "statistics creation on system columns is not supported".into(),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn statistics_virtual_generated_column_error() -> ExecError {
    ExecError::DetailedError {
        message: "statistics creation on virtual generated columns is not supported".into(),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn statistics_expression_references_system_column(expr: &str) -> bool {
    statistics_identifier_tokens(expr).any(|token| is_system_column_name(&token))
}

fn statistics_expression_references_virtual_generated_column(
    expr: &str,
    relation: &crate::backend::parser::BoundRelation,
) -> bool {
    relation
        .desc
        .columns
        .iter()
        .filter(|column| column.generated == Some(ColumnGeneratedKind::Virtual))
        .any(|column| statistics_expression_references_identifier(expr, &column.name))
}

fn statistics_expression_references_identifier(expr: &str, identifier: &str) -> bool {
    statistics_identifier_tokens(expr).any(|token| token.eq_ignore_ascii_case(identifier))
}

fn statistics_identifier_tokens(expr: &str) -> impl Iterator<Item = String> + '_ {
    struct IdentifierTokens<'a> {
        chars: std::iter::Peekable<std::str::CharIndices<'a>>,
        expr: &'a str,
    }

    impl<'a> Iterator for IdentifierTokens<'a> {
        type Item = String;

        fn next(&mut self) -> Option<Self::Item> {
            while let Some((idx, ch)) = self.chars.next() {
                if ch == '\'' {
                    while let Some((_, quoted)) = self.chars.next() {
                        if quoted == '\'' {
                            if self.chars.peek().is_some_and(|(_, next)| *next == '\'') {
                                self.chars.next();
                                continue;
                            }
                            break;
                        }
                    }
                    continue;
                }
                if ch == '"' {
                    let mut out = String::new();
                    while let Some((_, quoted)) = self.chars.next() {
                        if quoted == '"' {
                            if self.chars.peek().is_some_and(|(_, next)| *next == '"') {
                                self.chars.next();
                                out.push('"');
                                continue;
                            }
                            break;
                        }
                        out.push(quoted);
                    }
                    if !out.is_empty() {
                        return Some(out);
                    }
                    continue;
                }
                if ch == '_' || ch.is_ascii_alphabetic() {
                    let start = idx;
                    let mut end = idx + ch.len_utf8();
                    while let Some((next_idx, next)) = self.chars.peek().copied() {
                        if next == '_' || next.is_ascii_alphanumeric() {
                            self.chars.next();
                            end = next_idx + next.len_utf8();
                        } else {
                            break;
                        }
                    }
                    return Some(self.expr[start..end].to_string());
                }
            }
            None
        }
    }

    IdentifierTokens {
        chars: expr.char_indices().peekable(),
        expr,
    }
}

fn strip_statistics_expression_parens(target: &str) -> &str {
    let trimmed = target.trim();
    if trimmed.starts_with('(') && trimmed.ends_with(')') {
        trimmed[1..trimmed.len() - 1].trim()
    } else {
        trimmed
    }
}

fn lookup_statistics_row(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    configured_search_path: Option<&[String]>,
    name: &str,
) -> Option<PgStatisticExtRow> {
    let catcache = db.backend_catcache(client_id, txn_ctx).ok()?;
    match name.split_once('.') {
        Some((schema_name, object_name)) => {
            let namespace_oid = if is_statistics_temp_schema_name(db, client_id, schema_name) {
                db.owned_temp_namespace(client_id)?.oid
            } else {
                db.visible_namespace_oid_by_name(
                    client_id,
                    txn_ctx,
                    &schema_name.to_ascii_lowercase(),
                )?
            };
            catcache
                .statistic_ext_row_by_name_namespace(object_name, namespace_oid)
                .cloned()
        }
        None => {
            let search_path = db.effective_search_path(client_id, configured_search_path);
            for schema_name in search_path {
                if schema_name.is_empty() || schema_name == "$user" {
                    continue;
                }
                if is_statistics_temp_schema_name(db, client_id, &schema_name) {
                    continue;
                }
                let namespace_oid =
                    match db.visible_namespace_oid_by_name(client_id, txn_ctx, &schema_name) {
                        Some(oid) => oid,
                        None => continue,
                    };
                if let Some(row) = catcache
                    .statistic_ext_row_by_name_namespace(name, namespace_oid)
                    .cloned()
                {
                    return Some(row);
                }
            }
            None
        }
    }
}

fn is_statistics_temp_schema_name(db: &Database, client_id: ClientId, schema_name: &str) -> bool {
    schema_name.eq_ignore_ascii_case("pg_temp")
        || db
            .owned_temp_namespace(client_id)
            .as_ref()
            .is_some_and(|namespace| namespace.name.eq_ignore_ascii_case(schema_name))
}

fn display_statistics_name(
    namespace_oid: u32,
    statistics_name: &str,
    catcache: &crate::backend::utils::cache::catcache::CatCache,
) -> String {
    let schema_name = catcache
        .namespace_by_oid(namespace_oid)
        .map(|row| row.nspname.as_str())
        .unwrap_or("public");
    if schema_name == "public" {
        statistics_name.to_string()
    } else {
        format!("{schema_name}.{statistics_name}")
    }
}

fn duplicate_statistics_error(statistics_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("statistics object \"{statistics_name}\" already exists"),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn duplicate_statistics_in_schema_error(
    statistics_name: &str,
    namespace_oid: u32,
    catcache: &crate::backend::utils::cache::catcache::CatCache,
) -> ExecError {
    let schema_name = catcache
        .namespace_by_oid(namespace_oid)
        .map(|row| row.nspname.as_str())
        .unwrap_or("public");
    ExecError::DetailedError {
        message: format!(
            "statistics object \"{}\" already exists in schema \"{}\"",
            statistics_name.to_ascii_lowercase(),
            schema_name
        ),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn statistics_does_not_exist_error(statistics_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("statistics object \"{statistics_name}\" does not exist"),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn must_be_statistics_owner_error(statistics_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("must be owner of statistics object {statistics_name}"),
        detail: None,
        hint: None,
        sqlstate: "42501",
    }
}

fn schema_does_not_exist_error(schema_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("schema \"{schema_name}\" does not exist"),
        detail: None,
        hint: None,
        sqlstate: "3F000",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::executor::{StatementResult, Value};
    use crate::pgrust::session::Session;
    use std::path::PathBuf;

    fn temp_dir(label: &str) -> PathBuf {
        crate::pgrust::test_support::seeded_temp_dir("statistics", label)
    }

    fn query_rows(session: &mut Session, db: &Database, sql: &str) -> Vec<Vec<Value>> {
        match session.execute(db, sql).unwrap() {
            StatementResult::Query { rows, .. } => rows,
            other => panic!("expected query result, got {other:?}"),
        }
    }

    #[test]
    fn statistics_catalog_helpers_and_type_names_work() {
        let base = temp_dir("catalog_helpers");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(&db, "create table widgets (id int4, kind text, note text)")
            .unwrap();
        session
            .execute(&db, "create statistics s_basic on id, kind from widgets")
            .unwrap();
        session
            .execute(
                &db,
                "create statistics s_expr on (lower(note)) from widgets",
            )
            .unwrap();

        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select stxname from pg_statistic_ext \
                 where stxname in ('s_basic', 's_expr') \
                 order by stxname",
            ),
            vec![
                vec![Value::Text("s_basic".into())],
                vec![Value::Text("s_expr".into())],
            ]
        );

        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select \
                    pg_get_statisticsobjdef(oid), \
                    pg_get_statisticsobjdef_columns(oid), \
                    pg_get_statisticsobjdef_expressions(oid), \
                    pg_statistics_obj_is_visible(oid), \
                    pg_describe_object('pg_statistic_ext'::regclass, oid, 0) \
                 from pg_statistic_ext \
                 where stxname = 's_basic'",
            ),
            vec![vec![
                Value::Text("CREATE STATISTICS public.s_basic ON id, kind FROM widgets".into()),
                Value::Text("id, kind".into()),
                Value::Null,
                Value::Bool(true),
                Value::Text("statistics object public.s_basic".into()),
            ]]
        );

        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select \
                    pg_get_statisticsobjdef(oid), \
                    pg_get_statisticsobjdef_columns(oid), \
                    pg_get_statisticsobjdef_expressions(oid) \
                 from pg_statistic_ext \
                 where stxname = 's_expr'",
            ),
            vec![vec![
                Value::Text("CREATE STATISTICS public.s_expr ON lower(note) FROM widgets".into()),
                Value::Text("lower(note)".into()),
                Value::Array(vec![Value::Text("lower(note)".into())]),
            ]]
        );

        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select attname, atttypid::regtype::text \
                 from pg_attribute \
                 where attrelid = 'pg_statistic_ext_data'::regclass \
                   and attname in ('stxddependencies', 'stxdmcv', 'stxdndistinct') \
                 order by attname",
            ),
            vec![
                vec![
                    Value::Text("stxddependencies".into()),
                    Value::Text("pg_dependencies".into()),
                ],
                vec![
                    Value::Text("stxdmcv".into()),
                    Value::Text("pg_mcv_list".into()),
                ],
                vec![
                    Value::Text("stxdndistinct".into()),
                    Value::Text("pg_ndistinct".into()),
                ],
            ]
        );
    }

    #[test]
    fn statistics_visibility_honors_search_path() {
        let base = temp_dir("visibility");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(&db, "create table widgets (id int4, kind text)")
            .unwrap();
        session.execute(&db, "create schema analytics").unwrap();
        session
            .execute(
                &db,
                "create statistics public.shared_stats on id, kind from widgets",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create statistics analytics.shared_stats on id, kind from widgets",
            )
            .unwrap();
        session
            .execute(&db, "set search_path = analytics, public")
            .unwrap();

        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select n.nspname, pg_statistics_obj_is_visible(s.oid) \
                 from pg_statistic_ext s \
                 join pg_namespace n on n.oid = s.stxnamespace \
                 where s.stxname = 'shared_stats' \
                 order by n.nspname",
            ),
            vec![
                vec![Value::Text("analytics".into()), Value::Bool(true)],
                vec![Value::Text("public".into()), Value::Bool(false)],
            ]
        );
    }

    #[test]
    fn temp_statistics_are_hidden_from_unqualified_lookup() {
        let base = temp_dir("temp_visibility");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(&db, "create table ab1 (a int4, b int4)")
            .unwrap();
        session
            .execute(
                &db,
                "create statistics pg_temp.stats_ext_temp on a, b from ab1",
            )
            .unwrap();
        session
            .execute(&db, "create statistics ab1_a_b_stats on a, b from ab1")
            .unwrap();

        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select pg_statistics_obj_is_visible(oid) \
                 from pg_statistic_ext \
                 where stxname = 'stats_ext_temp'",
            ),
            vec![vec![Value::Bool(false)]]
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select n.nspname \
                 from pg_statistic_ext s \
                 join pg_namespace n on n.oid = s.stxnamespace \
                 where s.stxname = 'ab1_a_b_stats'",
            ),
            vec![vec![Value::Text("public".into())]]
        );

        let err = session
            .execute(&db, "drop statistics stats_ext_temp")
            .unwrap_err();
        match err {
            crate::backend::executor::ExecError::DetailedError { message, .. } => {
                assert_eq!(
                    message,
                    "statistics object \"stats_ext_temp\" does not exist"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }

        session
            .execute(&db, "drop statistics pg_temp.stats_ext_temp")
            .unwrap();
        session
            .execute(&db, "drop statistics ab1_a_b_stats")
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select count(*) from pg_statistic_ext \
                 where stxname in ('stats_ext_temp', 'ab1_a_b_stats')",
            ),
            vec![vec![Value::Int64(0)]]
        );
    }

    #[test]
    fn statistics_are_dropped_with_columns_and_relations() {
        let base = temp_dir("drop_cleanup");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(&db, "create table ab1 (a int4, b int4, c int4)")
            .unwrap();
        session
            .execute(&db, "create statistics ab1_a_b_stats on a, b from ab1")
            .unwrap();
        session
            .execute(&db, "create statistics ab1_b_c_stats on b, c from ab1")
            .unwrap();

        session
            .execute(&db, "alter table ab1 drop column a")
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select stxname from pg_statistic_ext \
                 where stxname like 'ab1_%' \
                 order by stxname",
            ),
            vec![vec![Value::Text("ab1_b_c_stats".into())]]
        );

        session.execute(&db, "drop table ab1").unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select stxname from pg_statistic_ext \
                 where stxname like 'ab1_%' \
                 order by stxname",
            ),
            Vec::<Vec<Value>>::new()
        );
    }

    #[test]
    fn statistics_comment_rename_and_drop_work() {
        let base = temp_dir("ddl_lifecycle");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(&db, "create table widgets (id int4, kind text)")
            .unwrap();
        session
            .execute(&db, "create statistics stats1 on id, kind from widgets")
            .unwrap();
        session
            .execute(&db, "comment on statistics stats1 is 'tracks widgets'")
            .unwrap();

        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select d.description \
                 from pg_description d \
                 join pg_statistic_ext s on s.oid = d.objoid \
                 where d.classoid = 'pg_statistic_ext'::regclass \
                   and s.stxname = 'stats1'",
            ),
            vec![vec![Value::Text("tracks widgets".into())]]
        );

        session
            .execute(&db, "alter statistics stats1 rename to stats2")
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select stxname from pg_statistic_ext where stxname = 'stats2'",
            ),
            vec![vec![Value::Text("stats2".into())]]
        );

        session.execute(&db, "drop statistics stats2").unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select count(*) from pg_statistic_ext where stxname in ('stats1', 'stats2')",
            ),
            vec![vec![Value::Int64(0)]]
        );
    }
}
