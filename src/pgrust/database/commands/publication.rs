use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use super::super::*;
use crate::backend::parser::analyze::infer_relation_expr_sql_type;
use crate::backend::parser::{
    AlterPublicationAction, AlterPublicationStatement, BoundRelation, CatalogLookup,
    CommentOnPublicationStatement, CreatePublicationStatement, DropPublicationStatement,
    PublicationObjectSpec, PublicationOption, PublicationOptions, PublicationSchemaName,
    PublicationTableSpec, PublicationTargetSpec, PublishGeneratedColumns, RawTypeName, SqlExpr,
    SqlType, SqlTypeKind, function_arg_values, is_system_column_name, parse_expr,
    resolve_raw_type_name,
};
use crate::backend::utils::cache::catcache::normalize_catalog_name;
use crate::backend::utils::cache::syscache::{
    SysCacheId, SysCacheTuple, scan_publication_namespace_rows_by_publication_db,
    search_sys_cache_list1_db, search_sys_cache1_db,
};
use crate::include::catalog::{
    INFORMATION_SCHEMA_NAMESPACE_OID, PG_CATALOG_NAMESPACE_OID, PG_TOAST_NAMESPACE_OID,
    PUBLISH_GENCOLS_NONE, PUBLISH_GENCOLS_STORED, PgAuthIdRow, PgAuthMembersRow, PgNamespaceRow,
    PgPublicationNamespaceRow, PgPublicationRelRow, PgPublicationRow,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::{ColumnGeneratedKind, RawXmlExprOp};
use crate::pgrust::database::ddl::{ensure_relation_owner, format_sql_type_name};

struct ResolvedPublicationTargets {
    relation_rows: Vec<PgPublicationRelRow>,
    namespace_rows: Vec<PgPublicationNamespaceRow>,
}

#[derive(Clone, Copy)]
enum PublicationMembershipKind {
    Table,
    Schema,
}

#[derive(Clone, Copy)]
enum MembershipMode {
    Inherit,
    Set,
}

fn oid_key(oid: u32) -> Value {
    Value::Int64(i64::from(oid))
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

fn role_row_by_oid_visible(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    role_oid: u32,
) -> Result<Option<PgAuthIdRow>, ExecError> {
    Ok(search_sys_cache1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::AuthIdOid,
        oid_key(role_oid),
    )
    .map_err(map_catalog_error)?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::AuthId(row) => Some(row),
        _ => None,
    }))
}

fn role_row_by_name_visible(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    role_name: &str,
) -> Result<Option<PgAuthIdRow>, ExecError> {
    for key in catalog_name_lookup_keys(role_name) {
        let row = search_sys_cache1_db(db, client_id, txn_ctx, SysCacheId::AuthIdRolname, key)
            .map_err(map_catalog_error)?
            .into_iter()
            .find_map(|tuple| match tuple {
                SysCacheTuple::AuthId(row) => Some(row),
                _ => None,
            });
        if row.is_some() {
            return Ok(row);
        }
    }
    Ok(None)
}

fn membership_rows_for_member_visible(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    member_oid: u32,
) -> Result<Vec<PgAuthMembersRow>, ExecError> {
    Ok(search_sys_cache_list1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::AuthMembersMemberRole,
        oid_key(member_oid),
    )
    .map_err(map_catalog_error)?
    .into_iter()
    .filter_map(|tuple| match tuple {
        SysCacheTuple::AuthMembers(row) => Some(row),
        _ => None,
    })
    .collect())
}

fn role_has_effective_membership_visible(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    member_oid: u32,
    target_oid: u32,
    mode: MembershipMode,
) -> Result<bool, ExecError> {
    if member_oid == target_oid {
        return Ok(true);
    }
    if role_row_by_oid_visible(db, client_id, txn_ctx, member_oid)?.is_some_and(|row| row.rolsuper)
    {
        return Ok(true);
    }

    let mut pending = VecDeque::from([member_oid]);
    let mut visited = BTreeSet::new();
    while let Some(next_member_oid) = pending.pop_front() {
        if !visited.insert(next_member_oid) {
            continue;
        }
        for membership in
            membership_rows_for_member_visible(db, client_id, txn_ctx, next_member_oid)?
        {
            let membership_allows_mode = match mode {
                MembershipMode::Inherit => membership.inherit_option,
                MembershipMode::Set => membership.set_option,
            };
            if !membership_allows_mode {
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

fn ensure_can_set_role_visible(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    role_oid: u32,
    role_name: &str,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    if role_has_effective_membership_visible(
        db,
        client_id,
        txn_ctx,
        auth.current_user_oid(),
        role_oid,
        MembershipMode::Set,
    )? {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!("must be able to SET ROLE \"{role_name}\""),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}

fn role_has_database_create_privilege_visible(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    role_oid: u32,
) -> Result<bool, ExecError> {
    if role_row_by_oid_visible(db, client_id, txn_ctx, role_oid)?.is_some_and(|row| row.rolsuper) {
        return Ok(true);
    }
    let grantee_oids = db
        .database_create_grants
        .read()
        .iter()
        .map(|grant| grant.grantee_oid)
        .collect::<Vec<_>>();
    for grantee_oid in grantee_oids {
        if role_has_effective_membership_visible(
            db,
            client_id,
            txn_ctx,
            role_oid,
            grantee_oid,
            MembershipMode::Inherit,
        )? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn namespace_row_by_name_visible(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    namespace_name: &str,
) -> Result<Option<PgNamespaceRow>, ExecError> {
    for key in catalog_name_lookup_keys(namespace_name) {
        let row = search_sys_cache1_db(db, client_id, txn_ctx, SysCacheId::NamespaceName, key)
            .map_err(map_catalog_error)?
            .into_iter()
            .find_map(|tuple| match tuple {
                SysCacheTuple::Namespace(row) => Some(row),
                _ => None,
            });
        if row.is_some() {
            return Ok(row);
        }
    }
    Ok(None)
}

fn namespace_row_by_oid_visible(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    namespace_oid: u32,
) -> Result<Option<PgNamespaceRow>, ExecError> {
    Ok(search_sys_cache1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::NamespaceOid,
        oid_key(namespace_oid),
    )
    .map_err(map_catalog_error)?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::Namespace(row) => Some(row),
        _ => None,
    }))
}

fn publication_row_by_name_visible(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    publication_name: &str,
) -> Result<Option<PgPublicationRow>, ExecError> {
    for key in catalog_name_lookup_keys(publication_name) {
        let row = search_sys_cache1_db(db, client_id, txn_ctx, SysCacheId::PublicationName, key)
            .map_err(map_catalog_error)?
            .into_iter()
            .find_map(|tuple| match tuple {
                SysCacheTuple::Publication(row) => Some(row),
                _ => None,
            });
        if row.is_some() {
            return Ok(row);
        }
    }
    Ok(None)
}

fn publication_rel_rows_for_publication_visible(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    publication_oid: u32,
) -> Result<Vec<PgPublicationRelRow>, ExecError> {
    let mut rows = search_sys_cache_list1_db(
        db,
        client_id,
        txn_ctx,
        SysCacheId::PublicationRelPrpubid,
        oid_key(publication_oid),
    )
    .map_err(map_catalog_error)?
    .into_iter()
    .filter_map(|tuple| match tuple {
        SysCacheTuple::PublicationRel(row) => Some(row),
        _ => None,
    })
    .collect::<Vec<_>>();
    rows.sort_by_key(|row| (row.prpubid, row.prrelid, row.oid));
    Ok(rows)
}

fn publication_namespace_rows_for_publication_visible(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    publication_oid: u32,
) -> Result<Vec<PgPublicationNamespaceRow>, ExecError> {
    let mut rows =
        scan_publication_namespace_rows_by_publication_db(db, client_id, txn_ctx, publication_oid)
            .map_err(map_catalog_error)?;
    rows.sort_by_key(|row| (row.pnpubid, row.pnnspid, row.oid));
    Ok(rows)
}

impl Database {
    pub(crate) fn execute_create_publication_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreatePublicationStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_publication_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_create_publication_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreatePublicationStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let txn_ctx = Some((xid, cid));
        let current_role =
            role_row_by_oid_visible(self, client_id, txn_ctx, auth.current_user_oid())?
                .ok_or_else(current_role_missing_error)?;
        if !role_has_database_create_privilege_visible(
            self,
            client_id,
            txn_ctx,
            auth.current_user_oid(),
        )? {
            return Err(permission_denied_for_database_error(
                &publication_database_name_for_permission_error(self),
            ));
        }

        if publication_row_by_name_visible(self, client_id, txn_ctx, &stmt.publication_name)?
            .is_some()
        {
            return Err(duplicate_publication_error(&stmt.publication_name));
        }
        reject_publication_column_list_schema_conflicts(
            &stmt.target,
            &stmt.publication_name,
            &[],
            &[],
        )?;
        if stmt.target.for_all_tables && !current_role.rolsuper {
            return Err(must_be_superuser_error(
                "must be superuser to create FOR ALL TABLES publication",
            ));
        }
        if stmt.target.for_all_sequences && !current_role.rolsuper {
            return Err(must_be_superuser_error(
                "must be superuser to create FOR ALL SEQUENCES publication",
            ));
        }
        let mut row = publication_row_defaults(&stmt.publication_name, auth.current_user_oid());
        row.puballtables = stmt.target.for_all_tables;
        row.puballsequences = stmt.target.for_all_sequences;
        apply_publication_options(&mut row, &stmt.options)?;

        let mut resolved = resolve_publication_targets(
            self,
            client_id,
            xid,
            cid,
            configured_search_path,
            &stmt.target,
            &stmt.publication_name,
            row.pubviaroot,
            DuplicateHandling::Error,
            true,
        )?;
        if stmt.target.for_all_tables {
            resolved.relation_rows = resolve_publication_except_tables(
                self,
                client_id,
                xid,
                cid,
                configured_search_path,
                &stmt.target.except_tables,
                &stmt.publication_name,
                row.pubviaroot,
            )?;
        }
        if !resolved.namespace_rows.is_empty() && !current_role.rolsuper {
            return Err(must_be_superuser_error(
                "must be superuser to create FOR TABLES IN SCHEMA publication",
            ));
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
        let (_, effect) = self
            .catalog
            .write()
            .create_publication_mvcc(row, resolved.relation_rows, resolved.namespace_rows, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_publication_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterPublicationStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_publication_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_alter_publication_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterPublicationStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let txn_ctx = Some((xid, cid));
        let current_role =
            role_row_by_oid_visible(self, client_id, txn_ctx, auth.current_user_oid())?
                .ok_or_else(current_role_missing_error)?;
        let publication =
            publication_row_by_name_visible(self, client_id, txn_ctx, &stmt.publication_name)?
                .ok_or_else(|| publication_does_not_exist_error(&stmt.publication_name))?;
        if !current_role.rolsuper
            && !role_has_effective_membership_visible(
                self,
                client_id,
                txn_ctx,
                auth.current_user_oid(),
                publication.pubowner,
                MembershipMode::Inherit,
            )?
        {
            return Err(must_be_publication_owner_error(&publication.pubname));
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

        let effect = match &stmt.action {
            AlterPublicationAction::Rename { new_name } => self
                .catalog
                .write()
                .replace_publication_row_mvcc(
                    PgPublicationRow {
                        pubname: new_name.to_ascii_lowercase(),
                        ..publication
                    },
                    &ctx,
                )
                .map_err(map_catalog_error)?,
            AlterPublicationAction::OwnerTo { new_owner } => {
                let new_owner_row = role_row_by_name_visible(self, client_id, txn_ctx, new_owner)?
                    .ok_or_else(|| role_does_not_exist_error(new_owner))?;
                if !current_role.rolsuper {
                    ensure_can_set_role_visible(
                        self,
                        client_id,
                        txn_ctx,
                        new_owner_row.oid,
                        &new_owner_row.rolname,
                    )?;
                    if !role_has_database_create_privilege_visible(
                        self,
                        client_id,
                        txn_ctx,
                        new_owner_row.oid,
                    )? {
                        return Err(permission_denied_for_database_error(
                            &publication_database_name_for_permission_error(self),
                        ));
                    }
                    if publication.puballtables && !new_owner_row.rolsuper {
                        return Err(publication_owner_change_requires_superuser_error(
                            &publication.pubname,
                            "The owner of a FOR ALL TABLES publication must be a superuser.",
                        ));
                    }
                    if !new_owner_row.rolsuper
                        && !publication_namespace_rows_for_publication_visible(
                            self,
                            client_id,
                            txn_ctx,
                            publication.oid,
                        )?
                        .is_empty()
                    {
                        return Err(publication_owner_change_requires_superuser_error(
                            &publication.pubname,
                            "The owner of a FOR TABLES IN SCHEMA publication must be a superuser.",
                        ));
                    }
                }
                // :HACK: PostgreSQL tracks publication ownership through shared
                // dependency state. Until pgrust grows pg_shdepend, owner
                // changes flow through the explicit owner field and role-owned
                // object scans instead.
                self.catalog
                    .write()
                    .replace_publication_row_mvcc(
                        PgPublicationRow {
                            pubowner: new_owner_row.oid,
                            ..publication
                        },
                        &ctx,
                    )
                    .map_err(map_catalog_error)?
            }
            AlterPublicationAction::SetOptions(options) => {
                let mut updated = publication.clone();
                apply_publication_options(&mut updated, options)?;
                if !updated.pubviaroot {
                    reject_existing_partitioned_root_memberships_when_not_via_root(
                        self,
                        client_id,
                        txn_ctx,
                        configured_search_path,
                        &updated,
                    )?;
                }
                self.catalog
                    .write()
                    .replace_publication_row_mvcc(updated, &ctx)
                    .map_err(map_catalog_error)?
            }
            AlterPublicationAction::AddObjects(target) => {
                if publication.puballtables {
                    return Err(publication_all_tables_membership_error(
                        &publication.pubname,
                        publication_membership_kind(target),
                    ));
                }
                let resolved = resolve_publication_targets(
                    self,
                    client_id,
                    xid,
                    cid,
                    configured_search_path,
                    target,
                    &publication.pubname,
                    publication.pubviaroot,
                    DuplicateHandling::Error,
                    true,
                )?;
                if !resolved.namespace_rows.is_empty() && !current_role.rolsuper {
                    return Err(must_be_superuser_error(
                        "must be superuser to add or set schemas",
                    ));
                }
                let existing_rel_rows = publication_rel_rows_for_publication_visible(
                    self,
                    client_id,
                    txn_ctx,
                    publication.oid,
                )?;
                let existing_namespace_rows = publication_namespace_rows_for_publication_visible(
                    self,
                    client_id,
                    txn_ctx,
                    publication.oid,
                )?;
                reject_publication_column_list_schema_conflicts(
                    target,
                    &publication.pubname,
                    &existing_rel_rows,
                    &existing_namespace_rows,
                )?;
                let existing_rel_oids = existing_rel_rows
                    .iter()
                    .map(|row| row.prrelid)
                    .collect::<BTreeSet<_>>();
                let existing_namespace_oids = existing_namespace_rows
                    .iter()
                    .map(|row| row.pnnspid)
                    .collect::<BTreeSet<_>>();
                for row in &resolved.relation_rows {
                    if existing_rel_oids.contains(&row.prrelid) {
                        return Err(publication_relation_already_member_error(
                            self,
                            client_id,
                            Some((xid, cid)),
                            configured_search_path,
                            &publication.pubname,
                            row.prrelid,
                        ));
                    }
                }
                for row in &resolved.namespace_rows {
                    if existing_namespace_oids.contains(&row.pnnspid) {
                        return Err(publication_schema_already_member_error(
                            self,
                            client_id,
                            Some((xid, cid)),
                            &publication.pubname,
                            row.pnnspid,
                        ));
                    }
                }
                let mut new_rel_rows = existing_rel_rows;
                new_rel_rows.extend(resolved.relation_rows);
                let mut new_namespace_rows = existing_namespace_rows;
                new_namespace_rows.extend(resolved.namespace_rows);
                self.catalog
                    .write()
                    .replace_publication_memberships_mvcc(
                        publication.oid,
                        new_rel_rows,
                        new_namespace_rows,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?
            }
            AlterPublicationAction::DropObjects(target) => {
                if publication.puballtables {
                    return Err(publication_all_tables_membership_error(
                        &publication.pubname,
                        publication_membership_kind(target),
                    ));
                }
                reject_publication_drop_filters(target)?;
                let resolved = resolve_publication_targets(
                    self,
                    client_id,
                    xid,
                    cid,
                    configured_search_path,
                    target,
                    &publication.pubname,
                    publication.pubviaroot,
                    DuplicateHandling::Error,
                    false,
                )?;
                let existing_rel_rows = publication_rel_rows_for_publication_visible(
                    self,
                    client_id,
                    txn_ctx,
                    publication.oid,
                )?;
                let existing_namespace_rows = publication_namespace_rows_for_publication_visible(
                    self,
                    client_id,
                    txn_ctx,
                    publication.oid,
                )?;
                let target_rel_oids = resolved
                    .relation_rows
                    .iter()
                    .map(|row| row.prrelid)
                    .collect::<BTreeSet<_>>();
                let target_namespace_oids = resolved
                    .namespace_rows
                    .iter()
                    .map(|row| row.pnnspid)
                    .collect::<BTreeSet<_>>();
                for target_oid in &target_rel_oids {
                    if !existing_rel_rows
                        .iter()
                        .any(|row| row.prrelid == *target_oid)
                    {
                        return Err(publication_relation_not_member_error(
                            self,
                            client_id,
                            Some((xid, cid)),
                            configured_search_path,
                            &publication.pubname,
                            *target_oid,
                        ));
                    }
                }
                for target_oid in &target_namespace_oids {
                    if !existing_namespace_rows
                        .iter()
                        .any(|row| row.pnnspid == *target_oid)
                    {
                        return Err(publication_schema_not_member_error(
                            self,
                            client_id,
                            Some((xid, cid)),
                            &publication.pubname,
                            *target_oid,
                        ));
                    }
                }
                let new_rel_rows = existing_rel_rows
                    .into_iter()
                    .filter(|row| !target_rel_oids.contains(&row.prrelid))
                    .collect::<Vec<_>>();
                let new_namespace_rows = existing_namespace_rows
                    .into_iter()
                    .filter(|row| !target_namespace_oids.contains(&row.pnnspid))
                    .collect::<Vec<_>>();
                self.catalog
                    .write()
                    .replace_publication_memberships_mvcc(
                        publication.oid,
                        new_rel_rows,
                        new_namespace_rows,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?
            }
            AlterPublicationAction::SetObjects(target) => {
                if publication_target_is_all_kind(target) {
                    if target.for_all_tables && !current_role.rolsuper {
                        return Err(must_be_superuser_error(
                            "must be superuser to set ALL TABLES",
                        ));
                    }
                    if target.for_all_sequences && !current_role.rolsuper {
                        return Err(must_be_superuser_error(
                            "must be superuser to set ALL SEQUENCES",
                        ));
                    }
                    if !publication_supports_all_target_operations(
                        self,
                        client_id,
                        txn_ctx,
                        &publication,
                    )? {
                        return Err(publication_all_target_unsupported_error(
                            &publication.pubname,
                            if target.for_all_tables {
                                "ALL TABLES"
                            } else {
                                "ALL SEQUENCES"
                            },
                        ));
                    }
                    let relation_rows = if target.for_all_tables {
                        resolve_publication_except_tables(
                            self,
                            client_id,
                            xid,
                            cid,
                            configured_search_path,
                            &target.except_tables,
                            &publication.pubname,
                            publication.pubviaroot,
                        )?
                    } else {
                        Vec::new()
                    };
                    let membership_effect = self
                        .catalog
                        .write()
                        .replace_publication_memberships_mvcc(
                            publication.oid,
                            relation_rows,
                            Vec::new(),
                            &ctx,
                        )
                        .map_err(map_catalog_error)?;
                    let row_effect = self
                        .catalog
                        .write()
                        .replace_publication_row_mvcc(
                            PgPublicationRow {
                                puballtables: target.for_all_tables,
                                puballsequences: target.for_all_sequences,
                                ..publication
                            },
                            &ctx,
                        )
                        .map_err(map_catalog_error)?;
                    merge_catalog_effects(membership_effect, row_effect)
                } else {
                    if publication.puballtables {
                        return Err(publication_all_tables_membership_error(
                            &publication.pubname,
                            publication_membership_kind(target),
                        ));
                    }
                    let resolved = resolve_publication_targets(
                        self,
                        client_id,
                        xid,
                        cid,
                        configured_search_path,
                        target,
                        &publication.pubname,
                        publication.pubviaroot,
                        DuplicateHandling::Dedup,
                        true,
                    )?;
                    if !resolved.namespace_rows.is_empty() && !current_role.rolsuper {
                        return Err(must_be_superuser_error(
                            "must be superuser to set TABLES IN SCHEMA for publication",
                        ));
                    }
                    reject_publication_column_list_schema_conflicts(
                        target,
                        &publication.pubname,
                        &[],
                        &[],
                    )?;
                    self.catalog
                        .write()
                        .replace_publication_memberships_mvcc(
                            publication.oid,
                            resolved.relation_rows,
                            resolved.namespace_rows,
                            &ctx,
                        )
                        .map_err(map_catalog_error)?
                }
            }
        };

        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_publication_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropPublicationStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_publication_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_drop_publication_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropPublicationStatement,
        xid: TransactionId,
        cid: CommandId,
        _configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        if stmt.cascade {
            return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "DROP PUBLICATION CASCADE".into(),
            )));
        }
        let auth = self.auth_state(client_id);
        let mut current_cid = cid;
        let mut dropped = 0usize;

        for publication_name in &stmt.publication_names {
            let txn_ctx = Some((xid, current_cid));
            let Some(publication) =
                publication_row_by_name_visible(self, client_id, txn_ctx, publication_name)?
            else {
                if stmt.if_exists {
                    continue;
                }
                return Err(publication_does_not_exist_error(publication_name));
            };
            if !role_has_effective_membership_visible(
                self,
                client_id,
                txn_ctx,
                auth.current_user_oid(),
                publication.pubowner,
                MembershipMode::Inherit,
            )? {
                return Err(must_be_publication_owner_error(&publication.pubname));
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
                .drop_publication_mvcc(publication.oid, &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            self.plan_cache.invalidate_all();
            catalog_effects.push(effect);
            dropped = dropped.saturating_add(1);
            current_cid = current_cid.saturating_add(1);
        }

        Ok(StatementResult::AffectedRows(dropped))
    }

    pub(crate) fn execute_comment_on_publication_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CommentOnPublicationStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_publication_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_comment_on_publication_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CommentOnPublicationStatement,
        xid: TransactionId,
        cid: CommandId,
        _configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let txn_ctx = Some((xid, cid));
        let publication =
            publication_row_by_name_visible(self, client_id, txn_ctx, &stmt.publication_name)?
                .ok_or_else(|| publication_does_not_exist_error(&stmt.publication_name))?;
        if !role_has_effective_membership_visible(
            self,
            client_id,
            txn_ctx,
            auth.current_user_oid(),
            publication.pubowner,
            MembershipMode::Inherit,
        )? {
            return Err(must_be_publication_owner_error(&publication.pubname));
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
            .comment_publication_mvcc(publication.oid, stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}

#[derive(Clone, Copy)]
enum DuplicateHandling {
    Error,
    Dedup,
}

#[derive(Clone, Copy)]
struct SeenPublicationTable {
    has_filter: bool,
    has_column_list: bool,
}

fn resolve_publication_targets(
    db: &Database,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    configured_search_path: Option<&[String]>,
    target: &PublicationTargetSpec,
    publication_name: &str,
    publish_via_partition_root: bool,
    duplicates: DuplicateHandling,
    require_relation_ownership: bool,
) -> Result<ResolvedPublicationTargets, ExecError> {
    let catalog = db.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
    if matches!(duplicates, DuplicateHandling::Error) {
        reject_conflicting_publication_table_duplicates(&catalog, target)?;
    }
    let mut seen_relations = BTreeMap::new();
    let mut seen_namespaces = BTreeSet::new();
    let mut relation_rows = Vec::new();
    let mut namespace_rows = Vec::new();

    for object in &target.objects {
        match object {
            PublicationObjectSpec::Table(table) => {
                let relation = lookup_publication_relation(&catalog, &table.relation_name)?;
                validate_publishable_relation(db, client_id, &relation, &table.relation_name)?;
                if require_relation_ownership {
                    ensure_relation_owner(db, client_id, &relation, &table.relation_name)?;
                }
                reject_partitioned_publication_qualifiers_when_not_via_root(
                    db,
                    client_id,
                    Some((xid, cid)),
                    configured_search_path,
                    publication_name,
                    &relation,
                    table,
                    publish_via_partition_root,
                )?;
                if let Some(existing_has_filter) =
                    seen_relations.insert(relation.relation_oid, table.where_clause.is_some())
                {
                    if matches!(duplicates, DuplicateHandling::Error) {
                        if existing_has_filter || table.where_clause.is_some() {
                            return Err(publication_relation_conflicting_filter_error(
                                &table.relation_name,
                            ));
                        }
                        return Err(publication_relation_duplicate_error(&table.relation_name));
                    }
                    continue;
                }
                let prqual = validate_publication_row_filter(&catalog, &relation, table)?;
                let row = PgPublicationRelRow {
                    oid: 0,
                    prpubid: 0,
                    prrelid: relation.relation_oid,
                    prexcept: false,
                    prqual,
                    prattrs: publication_column_numbers(
                        &relation,
                        &table.relation_name,
                        &table.column_names,
                    )?,
                };
                relation_rows.push(row.clone());
                if !table.only && row.prqual.is_none() && row.prattrs.is_none() {
                    for child_oid in catalog.find_all_inheritors(relation.relation_oid) {
                        if child_oid == relation.relation_oid
                            || seen_relations.contains_key(&child_oid)
                        {
                            continue;
                        }
                        let Some(child) = catalog.relation_by_oid(child_oid) else {
                            continue;
                        };
                        if child.relispartition {
                            continue;
                        }
                        seen_relations.insert(child_oid, false);
                        relation_rows.push(PgPublicationRelRow {
                            prrelid: child_oid,
                            ..row.clone()
                        });
                    }
                }
            }
            PublicationObjectSpec::Schema(schema) => {
                let namespace = resolve_publication_schema(
                    db,
                    client_id,
                    Some((xid, cid)),
                    configured_search_path,
                    schema,
                )?;
                validate_publishable_schema(db, client_id, &namespace.nspname, namespace.oid)?;
                if !seen_namespaces.insert(namespace.oid) {
                    if matches!(duplicates, DuplicateHandling::Error) {
                        return Err(publication_schema_duplicate_error(&namespace.nspname));
                    }
                    continue;
                }
                namespace_rows.push(PgPublicationNamespaceRow {
                    oid: 0,
                    pnpubid: 0,
                    pnnspid: namespace.oid,
                });
            }
        }
    }

    Ok(ResolvedPublicationTargets {
        relation_rows,
        namespace_rows,
    })
}

fn reject_conflicting_publication_table_duplicates(
    catalog: &dyn CatalogLookup,
    target: &PublicationTargetSpec,
) -> Result<(), ExecError> {
    let mut seen = BTreeMap::<u32, SeenPublicationTable>::new();
    for object in &target.objects {
        let PublicationObjectSpec::Table(table) = object else {
            continue;
        };
        let relation = lookup_publication_relation(catalog, &table.relation_name)?;
        let current = SeenPublicationTable {
            has_filter: table.where_clause.is_some(),
            has_column_list: !table.column_names.is_empty(),
        };
        if let Some(existing) = seen.insert(relation.relation_oid, current) {
            if existing.has_filter || current.has_filter {
                return Err(publication_relation_conflicting_filter_error(
                    &table.relation_name,
                ));
            }
            if existing.has_column_list || current.has_column_list {
                return Err(publication_relation_conflicting_column_list_error(
                    &table.relation_name,
                ));
            }
            return Err(publication_relation_duplicate_error(&table.relation_name));
        }
    }
    Ok(())
}

fn resolve_publication_except_tables(
    db: &Database,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    configured_search_path: Option<&[String]>,
    except_tables: &[PublicationTableSpec],
    publication_name: &str,
    publish_via_partition_root: bool,
) -> Result<Vec<PgPublicationRelRow>, ExecError> {
    let target = PublicationTargetSpec {
        for_all_tables: false,
        for_all_sequences: false,
        except_tables: Vec::new(),
        objects: except_tables
            .iter()
            .cloned()
            .map(PublicationObjectSpec::Table)
            .collect(),
    };
    let mut resolved = resolve_publication_targets(
        db,
        client_id,
        xid,
        cid,
        configured_search_path,
        &target,
        publication_name,
        publish_via_partition_root,
        DuplicateHandling::Error,
        true,
    )?;
    for row in &mut resolved.relation_rows {
        row.prexcept = true;
        row.prqual = None;
        row.prattrs = None;
    }
    Ok(resolved.relation_rows)
}

fn reject_publication_drop_filters(target: &PublicationTargetSpec) -> Result<(), ExecError> {
    if target.objects.iter().any(|object| {
        matches!(
            object,
            PublicationObjectSpec::Table(PublicationTableSpec {
                where_clause: Some(_),
                ..
            })
        )
    }) {
        return Err(ExecError::DetailedError {
            message: "cannot use a WHERE clause when removing a table from a publication".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        });
    }
    Ok(())
}

fn reject_partitioned_publication_qualifiers_when_not_via_root(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    configured_search_path: Option<&[String]>,
    publication_name: &str,
    relation: &BoundRelation,
    table: &PublicationTableSpec,
    publish_via_partition_root: bool,
) -> Result<(), ExecError> {
    if publish_via_partition_root || relation.relkind != 'p' {
        return Ok(());
    }
    if table.where_clause.is_some() {
        let relation_name =
            publication_relation_unqualified_name(db, client_id, txn_ctx, relation.relation_oid)
                .unwrap_or_else(|| unqualified_relation_name(&table.relation_name));
        return Err(ExecError::DetailedError {
            message: format!("cannot use publication WHERE clause for relation \"{relation_name}\""),
            detail: Some(
                "WHERE clause cannot be used for a partitioned table when publish_via_partition_root is false."
                    .into(),
            ),
            hint: None,
            sqlstate: "22023",
        });
    }
    if !table.column_names.is_empty() {
        let relation_name =
            publication_relation_qualified_name(db, client_id, txn_ctx, relation.relation_oid)
                .or_else(|| {
                    db.relation_display_name(
                        client_id,
                        txn_ctx,
                        configured_search_path,
                        relation.relation_oid,
                    )
                })
                .unwrap_or_else(|| table.relation_name.clone());
        return Err(ExecError::DetailedError {
            message: format!(
                "cannot use column list for relation \"{relation_name}\" in publication \"{publication_name}\""
            ),
            detail: Some(
                "Column lists cannot be specified for partitioned tables when publish_via_partition_root is false."
                    .into(),
            ),
            hint: None,
            sqlstate: "22023",
        });
    }
    Ok(())
}

fn reject_existing_partitioned_root_memberships_when_not_via_root(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    _configured_search_path: Option<&[String]>,
    publication: &PgPublicationRow,
) -> Result<(), ExecError> {
    for row in
        publication_rel_rows_for_publication_visible(db, client_id, txn_ctx, publication.oid)?
    {
        if row.prqual.is_none() && row.prattrs.is_none() {
            continue;
        }
        if !publication_relation_is_partitioned(db, client_id, txn_ctx, row.prrelid)? {
            continue;
        }
        let relation_name =
            publication_relation_unqualified_name(db, client_id, txn_ctx, row.prrelid)
                .unwrap_or_else(|| row.prrelid.to_string());
        if row.prqual.is_some() {
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot set parameter \"publish_via_partition_root\" to false for publication \"{}\"",
                    publication.pubname
                ),
                detail: Some(format!(
                    "The publication contains a WHERE clause for partitioned table \"{relation_name}\", which is not allowed when \"publish_via_partition_root\" is false."
                )),
                hint: None,
                sqlstate: "22023",
            });
        }
        return Err(ExecError::DetailedError {
            message: format!(
                "cannot set parameter \"publish_via_partition_root\" to false for publication \"{}\"",
                publication.pubname
            ),
            detail: Some(format!(
                "The publication contains a column list for partitioned table \"{relation_name}\", which is not allowed when \"publish_via_partition_root\" is false."
            )),
            hint: None,
            sqlstate: "22023",
        });
    }
    Ok(())
}

fn reject_publication_column_list_schema_conflicts(
    target: &PublicationTargetSpec,
    publication_name: &str,
    existing_rel_rows: &[PgPublicationRelRow],
    existing_namespace_rows: &[PgPublicationNamespaceRow],
) -> Result<(), ExecError> {
    let target_has_schema = target
        .objects
        .iter()
        .any(|object| matches!(object, PublicationObjectSpec::Schema(_)));
    if target_has_schema || !existing_namespace_rows.is_empty() {
        if let Some(table) = target.objects.iter().find_map(|object| match object {
            PublicationObjectSpec::Table(table) if !table.column_names.is_empty() => Some(table),
            _ => None,
        }) {
            return Err(publication_column_list_with_schema_error(
                &table.relation_name,
                publication_name,
            ));
        }
    }
    if target_has_schema
        && existing_rel_rows
            .iter()
            .any(|row| row.prattrs.as_ref().is_some_and(|attrs| !attrs.is_empty()))
    {
        return Err(publication_schema_with_existing_column_list_error(
            publication_name,
        ));
    }
    Ok(())
}

fn publication_column_numbers(
    relation: &BoundRelation,
    relation_name: &str,
    column_names: &[String],
) -> Result<Option<Vec<i16>>, ExecError> {
    if column_names.is_empty() {
        return Ok(None);
    }

    let mut attrs = Vec::with_capacity(column_names.len());
    for column_name in column_names {
        if is_system_column_name(column_name) {
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot use system column \"{column_name}\" in publication column list"
                ),
                detail: None,
                hint: None,
                sqlstate: "42P10",
            });
        }
        let Some((idx, column)) = relation
            .desc
            .columns
            .iter()
            .enumerate()
            .find(|(_, column)| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
        else {
            return Err(ExecError::DetailedError {
                message: format!(
                    "column \"{column_name}\" of relation \"{relation_name}\" does not exist"
                ),
                detail: None,
                hint: None,
                sqlstate: "42703",
            });
        };
        if column.generated == Some(ColumnGeneratedKind::Virtual) {
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot use virtual generated column \"{column_name}\" in publication column list"
                ),
                detail: None,
                hint: None,
                sqlstate: "42P10",
            });
        }
        let attr_no = i16::try_from(idx + 1).map_err(|_| ExecError::DetailedError {
            message: format!("too many columns in relation \"{relation_name}\""),
            detail: None,
            hint: None,
            sqlstate: "54011",
        })?;
        if attrs.contains(&attr_no) {
            return Err(ExecError::DetailedError {
                message: format!("duplicate column \"{column_name}\" in publication column list"),
                detail: None,
                hint: None,
                sqlstate: "42701",
            });
        }
        attrs.push(attr_no);
    }
    Ok(Some(attrs))
}

fn validate_publication_row_filter(
    catalog: &dyn CatalogLookup,
    relation: &BoundRelation,
    table: &PublicationTableSpec,
) -> Result<Option<String>, ExecError> {
    let Some(filter) = table.where_clause.as_deref() else {
        return Ok(None);
    };
    if filter.contains("=#>") {
        return Err(invalid_publication_where_error(
            "User-defined operators are not allowed.",
        ));
    }
    let expr = parse_expr(filter).map_err(ExecError::Parse)?;
    validate_publication_filter_expr(&expr)?;
    validate_publication_filter_types(&expr, relation, catalog)?;
    if !publication_filter_returns_bool_by_syntax(&expr)
        && !filter
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("xmlexists")
    {
        let sql_type = infer_relation_expr_sql_type(filter, None, &relation.desc, catalog)
            .map_err(ExecError::Parse)?;
        if sql_type != SqlType::new(SqlTypeKind::Bool) {
            return Err(ExecError::DetailedError {
                message: format!(
                    "argument of PUBLICATION WHERE must be type boolean, not type {}",
                    format_sql_type_name(sql_type)
                ),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        }
    }
    if filter
        .trim_start()
        .to_ascii_lowercase()
        .starts_with("xmlexists")
    {
        return Ok(Some(filter.trim().to_string()));
    }
    Ok(Some(
        render_publication_filter_expr(&expr).unwrap_or_else(|| filter.trim().to_string()),
    ))
}

fn publication_filter_returns_bool_by_syntax(expr: &SqlExpr) -> bool {
    matches!(
        expr,
        SqlExpr::Xml(xml) if xml.op == RawXmlExprOp::IsDocument
    )
}

fn validate_publication_filter_types(
    expr: &SqlExpr,
    relation: &BoundRelation,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    use SqlExpr::*;

    match expr {
        Column(name) => {
            let column_name = name.rsplit('.').next().unwrap_or(name);
            if let Some(column) = relation
                .desc
                .columns
                .iter()
                .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
                && publication_filter_type_is_user_defined(column.sql_type, catalog)
            {
                return Err(invalid_publication_where_error(
                    "User-defined types are not allowed.",
                ));
            }
        }
        Cast(inner, ty) => {
            validate_publication_filter_types(inner, relation, catalog)?;
            let sql_type = resolve_raw_type_name(ty, catalog).map_err(ExecError::Parse)?;
            if publication_filter_type_is_user_defined(sql_type, catalog) {
                return Err(invalid_publication_where_error(
                    "User-defined types are not allowed.",
                ));
            }
        }
        Collate {
            expr: inner,
            collation,
        } => {
            validate_publication_filter_types(inner, relation, catalog)?;
            if publication_filter_collation_is_user_defined(collation, catalog) {
                return Err(invalid_publication_where_error(
                    "User-defined collations are not allowed.",
                ));
            }
        }
        FuncCall { args, .. } => {
            for arg in function_arg_values(args) {
                validate_publication_filter_types(arg, relation, catalog)?;
            }
        }
        BinaryOperator { left, right, .. }
        | Add(left, right)
        | Sub(left, right)
        | BitAnd(left, right)
        | BitOr(left, right)
        | BitXor(left, right)
        | Shl(left, right)
        | Shr(left, right)
        | Mul(left, right)
        | Div(left, right)
        | Mod(left, right)
        | Concat(left, right)
        | Eq(left, right)
        | NotEq(left, right)
        | Lt(left, right)
        | LtEq(left, right)
        | Gt(left, right)
        | GtEq(left, right)
        | RegexMatch(left, right)
        | And(left, right)
        | Or(left, right)
        | IsDistinctFrom(left, right)
        | IsNotDistinctFrom(left, right)
        | Overlaps(left, right)
        | ArrayOverlap(left, right)
        | ArrayContains(left, right)
        | ArrayContained(left, right)
        | JsonbContains(left, right)
        | JsonbContained(left, right)
        | JsonbExists(left, right)
        | JsonbExistsAny(left, right)
        | JsonbExistsAll(left, right)
        | JsonbPathExists(left, right)
        | JsonbPathMatch(left, right)
        | JsonGet(left, right)
        | JsonGetText(left, right)
        | JsonPath(left, right)
        | JsonPathText(left, right)
        | AtTimeZone {
            expr: left,
            zone: right,
        } => {
            validate_publication_filter_types(left, relation, catalog)?;
            validate_publication_filter_types(right, relation, catalog)?;
        }
        UnaryPlus(inner)
        | Negate(inner)
        | BitNot(inner)
        | IsNull(inner)
        | IsNotNull(inner)
        | Not(inner)
        | FieldSelect { expr: inner, .. }
        | Subscript { expr: inner, .. } => {
            validate_publication_filter_types(inner, relation, catalog)?;
        }
        Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            validate_publication_filter_types(expr, relation, catalog)?;
            validate_publication_filter_types(pattern, relation, catalog)?;
            if let Some(escape) = escape {
                validate_publication_filter_types(escape, relation, catalog)?;
            }
        }
        Case {
            arg,
            args,
            defresult,
        } => {
            if let Some(arg) = arg {
                validate_publication_filter_types(arg, relation, catalog)?;
            }
            for when in args {
                validate_publication_filter_types(&when.expr, relation, catalog)?;
                validate_publication_filter_types(&when.result, relation, catalog)?;
            }
            if let Some(defresult) = defresult {
                validate_publication_filter_types(defresult, relation, catalog)?;
            }
        }
        ArrayLiteral(values) | Row(values) => {
            for value in values {
                validate_publication_filter_types(value, relation, catalog)?;
            }
        }
        QuantifiedArray { left, array, .. } => {
            validate_publication_filter_types(left, relation, catalog)?;
            validate_publication_filter_types(array, relation, catalog)?;
        }
        ArraySubscript { array, subscripts } => {
            validate_publication_filter_types(array, relation, catalog)?;
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    validate_publication_filter_types(lower, relation, catalog)?;
                }
                if let Some(upper) = &subscript.upper {
                    validate_publication_filter_types(upper, relation, catalog)?;
                }
            }
        }
        GeometryUnaryOp { expr, .. } | PrefixOperator { expr, .. } => {
            validate_publication_filter_types(expr, relation, catalog)?;
        }
        GeometryBinaryOp { left, right, .. } => {
            validate_publication_filter_types(left, relation, catalog)?;
            validate_publication_filter_types(right, relation, catalog)?;
        }
        InSubquery { expr, .. } => {
            validate_publication_filter_types(expr, relation, catalog)?;
        }
        QuantifiedSubquery { left, .. } => {
            validate_publication_filter_types(left, relation, catalog)?;
        }
        Xml(xml) => {
            for child in xml.child_exprs() {
                validate_publication_filter_types(child, relation, catalog)?;
            }
        }
        JsonQueryFunction(func) => {
            for child in func.child_exprs() {
                validate_publication_filter_types(child, relation, catalog)?;
            }
        }
        Const(Value::EnumOid(_)) => {
            return Err(invalid_publication_where_error(
                "User-defined types are not allowed.",
            ));
        }
        ScalarSubquery(_)
        | ArraySubquery(_)
        | Exists(_)
        | Parameter(_)
        | ParamRef(_)
        | Default
        | Const(_)
        | IntegerLiteral(_)
        | NumericLiteral(_)
        | Random
        | CurrentDate
        | CurrentCatalog
        | CurrentSchema
        | CurrentUser
        | SessionUser
        | CurrentRole
        | CurrentTime { .. }
        | CurrentTimestamp { .. }
        | LocalTime { .. }
        | LocalTimestamp { .. } => {}
    }
    Ok(())
}

fn publication_filter_type_is_user_defined(sql_type: SqlType, catalog: &dyn CatalogLookup) -> bool {
    if matches!(
        sql_type.kind,
        SqlTypeKind::Composite | SqlTypeKind::Enum | SqlTypeKind::Shell
    ) {
        return true;
    }
    let Some(type_oid) = (sql_type.type_oid != 0).then_some(sql_type.type_oid) else {
        return false;
    };
    let Some(row) = catalog.type_by_oid(type_oid) else {
        return false;
    };
    if row.typnamespace != PG_CATALOG_NAMESPACE_OID
        && row.typnamespace != INFORMATION_SCHEMA_NAMESPACE_OID
    {
        return true;
    }
    row.typelem != 0
        && catalog.type_by_oid(row.typelem).is_some_and(|elem| {
            elem.typnamespace != PG_CATALOG_NAMESPACE_OID
                && elem.typnamespace != INFORMATION_SCHEMA_NAMESPACE_OID
        })
}

fn publication_filter_collation_is_user_defined(
    collation: &str,
    catalog: &dyn CatalogLookup,
) -> bool {
    let (schema_name, collation_name) = collation
        .rsplit_once('.')
        .map(|(schema, name)| (Some(schema), name))
        .unwrap_or((None, collation));
    let collation_name = normalize_catalog_name(collation_name).to_ascii_lowercase();
    let schema_oid = schema_name.and_then(|schema| {
        let schema = normalize_catalog_name(schema).to_ascii_lowercase();
        catalog
            .namespace_rows()
            .into_iter()
            .find(|row| row.nspname.eq_ignore_ascii_case(&schema))
            .map(|row| row.oid)
    });
    catalog
        .collation_rows()
        .into_iter()
        .filter(|row| row.collname.eq_ignore_ascii_case(&collation_name))
        .filter(|row| {
            schema_oid
                .map(|oid| row.collnamespace == oid)
                .unwrap_or(true)
        })
        .any(|row| {
            row.collnamespace != PG_CATALOG_NAMESPACE_OID
                && row.collnamespace != INFORMATION_SCHEMA_NAMESPACE_OID
        })
}

fn validate_publication_filter_expr(expr: &SqlExpr) -> Result<(), ExecError> {
    use SqlExpr::*;

    // :HACK: PostgreSQL validates publication filters from the fully bound
    // expression tree, including function/operator provenance and volatility.
    // pgrust does not retain enough of that metadata here yet, so keep this
    // narrow syntactic guard until publication filters use a dedicated binder.
    match expr {
        FuncCall { name, args, .. } => {
            let normalized = name.rsplit('.').next().unwrap_or(name).to_ascii_lowercase();
            if matches!(normalized.as_str(), "avg" | "count" | "max" | "min" | "sum") {
                return Err(ExecError::DetailedError {
                    message: "aggregate functions are not allowed in WHERE".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42803",
                });
            }
            if normalized == "random" || normalized.starts_with("testpub_") {
                return Err(invalid_publication_where_error(
                    "User-defined or built-in mutable functions are not allowed.",
                ));
            }
            for arg in function_arg_values(args) {
                validate_publication_filter_expr(arg)?;
            }
        }
        BinaryOperator { left, right, .. } => {
            validate_publication_filter_expr(left)?;
            validate_publication_filter_expr(right)?;
            return Err(invalid_publication_where_error(
                "User-defined operators are not allowed.",
            ));
        }
        InSubquery { expr, .. } => {
            validate_publication_filter_expr(expr)?;
            return Err(invalid_publication_where_error(
                "Only columns, constants, built-in operators, built-in data types, built-in collations, and immutable built-in functions are allowed.",
            ));
        }
        ScalarSubquery(_) | ArraySubquery(_) | Exists(_) | QuantifiedSubquery { .. } => {
            return Err(invalid_publication_where_error(
                "Only columns, constants, built-in operators, built-in data types, built-in collations, and immutable built-in functions are allowed.",
            ));
        }
        Column(name) if name.eq_ignore_ascii_case("ctid") => {
            return Err(invalid_publication_where_error(
                "System columns are not allowed.",
            ));
        }
        Add(left, right)
        | Sub(left, right)
        | BitAnd(left, right)
        | BitOr(left, right)
        | BitXor(left, right)
        | Shl(left, right)
        | Shr(left, right)
        | Mul(left, right)
        | Div(left, right)
        | Mod(left, right)
        | Concat(left, right)
        | Eq(left, right)
        | NotEq(left, right)
        | Lt(left, right)
        | LtEq(left, right)
        | Gt(left, right)
        | GtEq(left, right)
        | RegexMatch(left, right)
        | And(left, right)
        | Or(left, right)
        | IsDistinctFrom(left, right)
        | IsNotDistinctFrom(left, right)
        | Overlaps(left, right)
        | ArrayOverlap(left, right)
        | ArrayContains(left, right)
        | ArrayContained(left, right)
        | JsonbContains(left, right)
        | JsonbContained(left, right)
        | JsonbExists(left, right)
        | JsonbExistsAny(left, right)
        | JsonbExistsAll(left, right)
        | JsonbPathExists(left, right)
        | JsonbPathMatch(left, right)
        | JsonGet(left, right)
        | JsonGetText(left, right)
        | JsonPath(left, right)
        | JsonPathText(left, right)
        | AtTimeZone {
            expr: left,
            zone: right,
        } => {
            validate_publication_filter_expr(left)?;
            validate_publication_filter_expr(right)?;
        }
        UnaryPlus(inner)
        | Negate(inner)
        | BitNot(inner)
        | Cast(inner, _)
        | Collate { expr: inner, .. }
        | IsNull(inner)
        | IsNotNull(inner)
        | Not(inner)
        | FieldSelect { expr: inner, .. }
        | Subscript { expr: inner, .. } => validate_publication_filter_expr(inner)?,
        Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            validate_publication_filter_expr(expr)?;
            validate_publication_filter_expr(pattern)?;
            if let Some(escape) = escape {
                validate_publication_filter_expr(escape)?;
            }
        }
        Case {
            arg,
            args,
            defresult,
        } => {
            if let Some(arg) = arg {
                validate_publication_filter_expr(arg)?;
            }
            for when in args {
                validate_publication_filter_expr(&when.expr)?;
                validate_publication_filter_expr(&when.result)?;
            }
            if let Some(defresult) = defresult {
                validate_publication_filter_expr(defresult)?;
            }
        }
        ArrayLiteral(values) | Row(values) => {
            for value in values {
                validate_publication_filter_expr(value)?;
            }
        }
        QuantifiedArray { left, array, .. } => {
            validate_publication_filter_expr(left)?;
            validate_publication_filter_expr(array)?;
        }
        ArraySubscript { array, subscripts } => {
            validate_publication_filter_expr(array)?;
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    validate_publication_filter_expr(lower)?;
                }
                if let Some(upper) = &subscript.upper {
                    validate_publication_filter_expr(upper)?;
                }
            }
        }
        GeometryUnaryOp { expr, .. } | PrefixOperator { expr, .. } => {
            validate_publication_filter_expr(expr)?;
        }
        GeometryBinaryOp { left, right, .. } => {
            validate_publication_filter_expr(left)?;
            validate_publication_filter_expr(right)?;
        }
        Random => {
            return Err(invalid_publication_where_error(
                "User-defined or built-in mutable functions are not allowed.",
            ));
        }
        Xml(xml) => {
            for child in xml.child_exprs() {
                validate_publication_filter_expr(child)?;
            }
        }
        JsonQueryFunction(func) => {
            for child in func.child_exprs() {
                validate_publication_filter_expr(child)?;
            }
        }
        Column(_)
        | Parameter(_)
        | ParamRef(_)
        | Default
        | Const(_)
        | IntegerLiteral(_)
        | NumericLiteral(_)
        | CurrentDate
        | CurrentCatalog
        | CurrentSchema
        | CurrentUser
        | SessionUser
        | CurrentRole
        | CurrentTime { .. }
        | CurrentTimestamp { .. }
        | LocalTime { .. }
        | LocalTimestamp { .. } => {}
    }
    Ok(())
}

fn render_publication_filter_expr(expr: &SqlExpr) -> Option<String> {
    use SqlExpr::*;

    Some(match expr {
        And(left, right) => format!(
            "({} AND {})",
            render_publication_filter_expr(left)?,
            render_publication_filter_expr(right)?
        ),
        Or(left, right) => format!(
            "({} OR {})",
            render_publication_filter_expr(left)?,
            render_publication_filter_expr(right)?
        ),
        Eq(left, right) => render_publication_binary_expr(left, "=", right)?,
        NotEq(left, right) => render_publication_binary_expr(left, "<>", right)?,
        Lt(left, right) => render_publication_binary_expr(left, "<", right)?,
        LtEq(left, right) => render_publication_binary_expr(left, "<=", right)?,
        Gt(left, right) => render_publication_binary_expr(left, ">", right)?,
        GtEq(left, right) => render_publication_binary_expr(left, ">=", right)?,
        IsNull(inner) => format!("({} IS NULL)", render_publication_filter_term(inner)?),
        IsNotNull(inner) => format!("({} IS NOT NULL)", render_publication_filter_term(inner)?),
        IsDistinctFrom(left, right) => format!(
            "({} IS DISTINCT FROM {})",
            render_publication_filter_term(left)?,
            render_publication_filter_term(right)?
        ),
        IsNotDistinctFrom(left, right) => format!(
            "({} IS NOT DISTINCT FROM {})",
            render_publication_filter_term(left)?,
            render_publication_filter_term(right)?
        ),
        Not(inner) => format!("(NOT {})", render_publication_filter_term(inner)?),
        _ => render_publication_filter_term(expr)?,
    })
}

fn render_publication_binary_expr(left: &SqlExpr, op: &str, right: &SqlExpr) -> Option<String> {
    Some(format!(
        "({} {} {})",
        render_publication_filter_term(left)?,
        op,
        render_publication_filter_term(right)?
    ))
}

fn render_publication_filter_term(expr: &SqlExpr) -> Option<String> {
    use SqlExpr::*;

    Some(match expr {
        Column(name) => name.clone(),
        IntegerLiteral(value) | NumericLiteral(value) => value.clone(),
        Const(value) => render_publication_const(value)?,
        Cast(inner, ty) => format!(
            "{}::{}",
            render_publication_filter_term(inner)?,
            render_publication_type_name(ty)
        ),
        Collate { expr, collation } => {
            format!(
                "{} COLLATE {}",
                render_publication_filter_term(expr)?,
                collation
            )
        }
        Add(left, right) => render_publication_arithmetic_expr(left, "+", right)?,
        Sub(left, right) => render_publication_arithmetic_expr(left, "-", right)?,
        Mul(left, right) => render_publication_arithmetic_expr(left, "*", right)?,
        Div(left, right) => render_publication_arithmetic_expr(left, "/", right)?,
        Mod(left, right) => render_publication_arithmetic_expr(left, "%", right)?,
        UnaryPlus(inner) => format!("+{}", render_publication_filter_term(inner)?),
        Negate(inner) => format!("-{}", render_publication_filter_term(inner)?),
        FuncCall { name, args, .. } => {
            let rendered_args = function_arg_values(args)
                .map(|arg| render_publication_filter_term(arg))
                .collect::<Option<Vec<_>>>()?
                .join(", ");
            format!("{name}({rendered_args})")
        }
        _ => return None,
    })
}

fn render_publication_arithmetic_expr(left: &SqlExpr, op: &str, right: &SqlExpr) -> Option<String> {
    Some(format!(
        "({} {} {})",
        render_publication_filter_term(left)?,
        op,
        render_publication_filter_term(right)?
    ))
}

fn render_publication_const(value: &Value) -> Option<String> {
    Some(match value {
        Value::Null => "NULL".into(),
        Value::Bool(true) => "true".into(),
        Value::Bool(false) => "false".into(),
        Value::Int16(value) => value.to_string(),
        Value::Int32(value) => value.to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Float64(value) => value.to_string(),
        Value::Numeric(value) => value.render(),
        Value::Text(text) => format!("'{}'::text", escape_publication_string_literal(text)),
        Value::TextRef(_, _) => format!(
            "'{}'::text",
            escape_publication_string_literal(value.as_text().unwrap_or_default())
        ),
        Value::Xml(text) => format!("'{}'::xml", escape_publication_string_literal(text)),
        _ => return None,
    })
}

fn render_publication_type_name(ty: &RawTypeName) -> String {
    match ty {
        RawTypeName::Builtin(sql_type) => format_sql_type_name(*sql_type).into(),
        RawTypeName::Serial(kind) => match kind {
            crate::backend::parser::SerialKind::Small => "smallserial".into(),
            crate::backend::parser::SerialKind::Regular => "serial".into(),
            crate::backend::parser::SerialKind::Big => "bigserial".into(),
        },
        RawTypeName::Named { name, .. } => name.clone(),
        RawTypeName::Record => "record".into(),
    }
}

fn escape_publication_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn lookup_publication_relation(
    catalog: &dyn CatalogLookup,
    relation_name: &str,
) -> Result<BoundRelation, ExecError> {
    match catalog.lookup_any_relation(relation_name) {
        Some(relation) => Ok(relation),
        None => Err(ExecError::DetailedError {
            message: format!("relation \"{relation_name}\" does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42P01",
        }),
    }
}

fn resolve_publication_schema(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    configured_search_path: Option<&[String]>,
    schema: &crate::backend::parser::PublicationSchemaSpec,
) -> Result<crate::include::catalog::PgNamespaceRow, ExecError> {
    let temp_namespace = db.owned_temp_namespace(client_id);
    let schema_name = match &schema.schema_name {
        PublicationSchemaName::Name(name) => name.clone(),
        PublicationSchemaName::CurrentSchema => db
            .effective_search_path(client_id, configured_search_path)
            .into_iter()
            .find(|schema_name| {
                !schema_name.is_empty()
                    && schema_name != "$user"
                    && schema_name != "pg_temp"
                    && !schema_name.starts_with("pg_temp_")
                    && !temp_namespace
                        .as_ref()
                        .is_some_and(|namespace| namespace.name == *schema_name)
                    && !schema_name.eq_ignore_ascii_case("pg_catalog")
            })
            .ok_or_else(no_schema_selected_for_current_schema_error)?,
    };
    namespace_row_by_name_visible(db, client_id, txn_ctx, &schema_name)?
        .filter(|row| !db.other_session_temp_namespace_oid(client_id, row.oid))
        .ok_or_else(|| schema_does_not_exist_error(&schema_name))
}

fn validate_publishable_relation(
    db: &Database,
    client_id: ClientId,
    relation: &BoundRelation,
    relation_name: &str,
) -> Result<(), ExecError> {
    if relation.relkind == 'v' {
        return Err(ExecError::DetailedError {
            message: format!("cannot add relation \"{relation_name}\" to publication"),
            detail: Some("This operation is not supported for views.".into()),
            hint: None,
            sqlstate: "22023",
        });
    }
    if !matches!(relation.relkind, 'r' | 'p') {
        return Err(ExecError::DetailedError {
            message: format!("cannot add relation \"{relation_name}\" to publication"),
            detail: Some("This operation is not supported for relation type.".into()),
            hint: None,
            sqlstate: "22023",
        });
    }
    if relation.relpersistence == 't'
        || db.other_session_temp_namespace_oid(client_id, relation.namespace_oid)
    {
        return Err(ExecError::DetailedError {
            message: format!("cannot add relation \"{relation_name}\" to publication"),
            detail: Some("This operation is not supported for temporary tables.".into()),
            hint: None,
            sqlstate: "22023",
        });
    }
    if relation.relpersistence == 'u' {
        return Err(ExecError::DetailedError {
            message: format!("cannot add relation \"{relation_name}\" to publication"),
            detail: Some("This operation is not supported for unlogged tables.".into()),
            hint: None,
            sqlstate: "22023",
        });
    }
    if relation.namespace_oid == PG_CATALOG_NAMESPACE_OID
        || relation.namespace_oid == PG_TOAST_NAMESPACE_OID
    {
        return Err(ExecError::DetailedError {
            message: format!("cannot add relation \"{relation_name}\" to publication"),
            detail: Some("This operation is not supported for system tables.".into()),
            hint: None,
            sqlstate: "22023",
        });
    }
    Ok(())
}

fn validate_publishable_schema(
    db: &Database,
    client_id: ClientId,
    schema_name: &str,
    namespace_oid: u32,
) -> Result<(), ExecError> {
    if namespace_oid == PG_CATALOG_NAMESPACE_OID || namespace_oid == PG_TOAST_NAMESPACE_OID {
        return Err(ExecError::DetailedError {
            message: format!("cannot add schema \"{schema_name}\" to publication"),
            detail: Some("This operation is not supported for system schemas.".into()),
            hint: None,
            sqlstate: "22023",
        });
    }
    if db.other_session_temp_namespace_oid(client_id, namespace_oid)
        || schema_name.starts_with("pg_temp_")
        || schema_name.starts_with("pg_toast_temp_")
    {
        return Err(ExecError::DetailedError {
            message: format!("cannot add schema \"{schema_name}\" to publication"),
            detail: Some("Temporary schemas cannot be replicated.".into()),
            hint: None,
            sqlstate: "22023",
        });
    }
    Ok(())
}

fn publication_membership_kind(target: &PublicationTargetSpec) -> PublicationMembershipKind {
    if target
        .objects
        .iter()
        .all(|object| matches!(object, PublicationObjectSpec::Schema(_)))
    {
        PublicationMembershipKind::Schema
    } else {
        PublicationMembershipKind::Table
    }
}

fn publication_target_is_all_kind(target: &PublicationTargetSpec) -> bool {
    target.for_all_tables || target.for_all_sequences
}

fn publication_supports_all_target_operations(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    publication: &PgPublicationRow,
) -> Result<bool, ExecError> {
    if publication.puballtables || publication.puballsequences {
        return Ok(true);
    }
    Ok(
        publication_rel_rows_for_publication_visible(db, client_id, txn_ctx, publication.oid)?
            .is_empty()
            && publication_namespace_rows_for_publication_visible(
                db,
                client_id,
                txn_ctx,
                publication.oid,
            )?
            .is_empty(),
    )
}

fn merge_catalog_effects(
    mut left: CatalogMutationEffect,
    right: CatalogMutationEffect,
) -> CatalogMutationEffect {
    left.touched_catalogs.extend(right.touched_catalogs);
    left.created_rels.extend(right.created_rels);
    left.dropped_rels.extend(right.dropped_rels);
    left.relation_oids.extend(right.relation_oids);
    left.namespace_oids.extend(right.namespace_oids);
    left.type_oids.extend(right.type_oids);
    left.full_reset |= right.full_reset;
    left
}

fn publication_row_defaults(publication_name: &str, owner_oid: u32) -> PgPublicationRow {
    PgPublicationRow {
        oid: 0,
        pubname: publication_name.to_ascii_lowercase(),
        pubowner: owner_oid,
        puballtables: false,
        puballsequences: false,
        pubinsert: true,
        pubupdate: true,
        pubdelete: true,
        pubtruncate: true,
        pubviaroot: false,
        pubgencols: PUBLISH_GENCOLS_NONE,
    }
}

fn apply_publication_options(
    publication: &mut PgPublicationRow,
    options: &PublicationOptions,
) -> Result<(), ExecError> {
    let mut seen = BTreeSet::new();
    for option in &options.options {
        let option_name = publication_option_name(option);
        if !seen.insert(option_name.clone()) {
            return Err(ExecError::Parse(
                ParseError::ConflictingOrRedundantOptions {
                    option: option_name,
                },
            ));
        }
        match option {
            PublicationOption::Publish(actions) => {
                publication.pubinsert = actions.insert;
                publication.pubupdate = actions.update;
                publication.pubdelete = actions.delete;
                publication.pubtruncate = actions.truncate;
            }
            PublicationOption::PublishViaPartitionRoot(value) => {
                publication.pubviaroot = *value;
            }
            PublicationOption::PublishGeneratedColumns(value) => {
                publication.pubgencols = match value {
                    PublishGeneratedColumns::None => PUBLISH_GENCOLS_NONE,
                    PublishGeneratedColumns::Stored => PUBLISH_GENCOLS_STORED,
                };
            }
            PublicationOption::Raw { name, .. } => {
                return Err(ExecError::Parse(
                    ParseError::UnrecognizedPublicationParameter(name.clone()),
                ));
            }
        }
    }
    Ok(())
}

fn publication_option_name(option: &PublicationOption) -> String {
    match option {
        PublicationOption::Publish(_) => "publish".into(),
        PublicationOption::PublishViaPartitionRoot(_) => "publish_via_partition_root".into(),
        PublicationOption::PublishGeneratedColumns(_) => "publish_generated_columns".into(),
        PublicationOption::Raw { name, .. } => name.clone(),
    }
}

fn current_role_missing_error() -> ExecError {
    ExecError::DetailedError {
        message: "current role does not exist".into(),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn permission_denied_for_database_error(database_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("permission denied for database {database_name}"),
        detail: None,
        hint: None,
        sqlstate: "42501",
    }
}

fn publication_database_name_for_permission_error(db: &Database) -> String {
    let current = db.current_database_name();
    // :HACK: pgrust still boots the single regression database as `postgres`,
    // while PostgreSQL's regression harness connects to a database named
    // `regression` and the GRANT path accepts that name as an alias.
    if current == "postgres" {
        "regression".into()
    } else {
        current
    }
}

fn duplicate_publication_error(publication_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("publication \"{publication_name}\" already exists"),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn publication_does_not_exist_error(publication_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("publication \"{publication_name}\" does not exist"),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn role_does_not_exist_error(role_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("role \"{role_name}\" does not exist"),
        detail: None,
        hint: None,
        sqlstate: "42704",
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

fn no_schema_selected_for_current_schema_error() -> ExecError {
    ExecError::DetailedError {
        message: "no schema has been selected for CURRENT_SCHEMA".into(),
        detail: None,
        hint: None,
        sqlstate: "3F000",
    }
}

fn must_be_superuser_error(message: &'static str) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "42501",
    }
}

fn publication_owner_change_requires_superuser_error(
    publication_name: &str,
    hint: &'static str,
) -> ExecError {
    ExecError::DetailedError {
        message: format!("permission denied to change owner of publication \"{publication_name}\""),
        detail: None,
        hint: Some(hint.into()),
        sqlstate: "42501",
    }
}

fn must_be_publication_owner_error(publication_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("must be owner of publication {publication_name}"),
        detail: None,
        hint: None,
        sqlstate: "42501",
    }
}

fn publication_all_tables_membership_error(
    publication_name: &str,
    membership_kind: PublicationMembershipKind,
) -> ExecError {
    ExecError::DetailedError {
        message: format!("publication \"{publication_name}\" is defined as FOR ALL TABLES"),
        detail: Some(match membership_kind {
            PublicationMembershipKind::Table => {
                "Tables cannot be added to or dropped from FOR ALL TABLES publications.".into()
            }
            PublicationMembershipKind::Schema => {
                "Schemas cannot be added to or dropped from FOR ALL TABLES publications.".into()
            }
        }),
        hint: None,
        sqlstate: "55000",
    }
}

fn publication_all_target_unsupported_error(publication_name: &str, target: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("publication \"{publication_name}\" does not support {target} operations"),
        detail: Some(
            "This operation requires the publication to be defined as FOR ALL TABLES/SEQUENCES or to be empty."
                .into(),
        ),
        hint: None,
        sqlstate: "55000",
    }
}

fn publication_relation_duplicate_error(relation_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("relation \"{relation_name}\" specified more than once"),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn publication_relation_conflicting_filter_error(relation_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("conflicting or redundant WHERE clauses for table \"{relation_name}\""),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn publication_relation_conflicting_column_list_error(relation_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("conflicting or redundant column lists for table \"{relation_name}\""),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn publication_column_list_with_schema_error(
    relation_name: &str,
    publication_name: &str,
) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "cannot use column list for relation \"{relation_name}\" in publication \"{publication_name}\""
        ),
        detail: Some(
            "Column lists cannot be specified in publications containing FOR TABLES IN SCHEMA elements."
                .into(),
        ),
        hint: None,
        sqlstate: "0A000",
    }
}

fn publication_schema_with_existing_column_list_error(publication_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("cannot add schema to publication \"{publication_name}\""),
        detail: Some(
            "Schemas cannot be added if any tables that specify a column list are already part of the publication."
                .into(),
        ),
        hint: None,
        sqlstate: "0A000",
    }
}

fn invalid_publication_where_error(detail: &str) -> ExecError {
    ExecError::DetailedError {
        message: "invalid publication WHERE expression".into(),
        detail: Some(detail.into()),
        hint: None,
        sqlstate: "42P17",
    }
}

fn publication_schema_duplicate_error(schema_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("schema \"{schema_name}\" specified more than once"),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn publication_relation_already_member_error(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    configured_search_path: Option<&[String]>,
    publication_name: &str,
    relation_oid: u32,
) -> ExecError {
    let relation_name = db
        .backend_catcache(client_id, txn_ctx)
        .ok()
        .and_then(|cache| {
            cache
                .class_by_oid(relation_oid)
                .map(|row| row.relname.clone())
        })
        .or_else(|| {
            db.relation_display_name(client_id, txn_ctx, configured_search_path, relation_oid)
        })
        .unwrap_or_else(|| relation_oid.to_string());
    ExecError::DetailedError {
        message: format!(
            "relation \"{relation_name}\" is already member of publication \"{publication_name}\""
        ),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn publication_schema_already_member_error(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    publication_name: &str,
    namespace_oid: u32,
) -> ExecError {
    let schema_name = publication_namespace_name(db, client_id, txn_ctx, namespace_oid)
        .unwrap_or_else(|| namespace_oid.to_string());
    ExecError::DetailedError {
        message: format!(
            "schema \"{schema_name}\" is already member of publication \"{publication_name}\""
        ),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn publication_relation_not_member_error(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    configured_search_path: Option<&[String]>,
    _publication_name: &str,
    relation_oid: u32,
) -> ExecError {
    let relation_name = db
        .backend_catcache(client_id, txn_ctx)
        .ok()
        .and_then(|cache| {
            cache
                .class_by_oid(relation_oid)
                .map(|row| row.relname.clone())
        })
        .or_else(|| {
            db.relation_display_name(client_id, txn_ctx, configured_search_path, relation_oid)
        })
        .unwrap_or_else(|| relation_oid.to_string());
    ExecError::DetailedError {
        message: format!("relation \"{relation_name}\" is not part of the publication"),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn publication_schema_not_member_error(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    _publication_name: &str,
    namespace_oid: u32,
) -> ExecError {
    let schema_name = publication_namespace_name(db, client_id, txn_ctx, namespace_oid)
        .unwrap_or_else(|| namespace_oid.to_string());
    ExecError::DetailedError {
        message: format!("tables from schema \"{schema_name}\" are not part of the publication"),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn publication_namespace_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    namespace_oid: u32,
) -> Option<String> {
    namespace_row_by_oid_visible(db, client_id, txn_ctx, namespace_oid)
        .ok()
        .flatten()
        .map(|row| row.nspname)
}

fn publication_relation_unqualified_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    relation_oid: u32,
) -> Option<String> {
    db.backend_catcache(client_id, txn_ctx)
        .ok()?
        .class_rows()
        .into_iter()
        .find(|row| row.oid == relation_oid)
        .map(|row| row.relname)
}

fn publication_relation_qualified_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    relation_oid: u32,
) -> Option<String> {
    let catcache = db.backend_catcache(client_id, txn_ctx).ok()?;
    let class = catcache
        .class_rows()
        .into_iter()
        .find(|row| row.oid == relation_oid)?;
    let namespace = catcache.namespace_by_oid(class.relnamespace)?;
    Some(format!("{}.{}", namespace.nspname, class.relname))
}

fn publication_relation_is_partitioned(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    relation_oid: u32,
) -> Result<bool, ExecError> {
    Ok(db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .class_rows()
        .into_iter()
        .find(|row| row.oid == relation_oid)
        .is_some_and(|row| row.relkind == 'p'))
}

fn unqualified_relation_name(relation_name: &str) -> String {
    relation_name
        .rsplit_once('.')
        .map(|(_, name)| name.to_string())
        .unwrap_or_else(|| relation_name.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::executor::{StatementResult, Value};
    use crate::pgrust::session::Session;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "pgrust_publication_cmds_{}_{}_{}",
            label,
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn role_oid(db: &Database, role_name: &str) -> u32 {
        db.backend_catcache(1, None)
            .unwrap()
            .authid_rows()
            .into_iter()
            .find(|row| row.rolname == role_name)
            .unwrap()
            .oid
    }

    fn publication_owner_name(db: &Database, publication_name: &str) -> String {
        let catcache = db.backend_catcache(1, None).unwrap();
        let owner_oid = catcache
            .publication_row_by_name(publication_name)
            .map(|row| row.pubowner)
            .unwrap();
        catcache
            .authid_rows()
            .into_iter()
            .find(|row| row.oid == owner_oid)
            .map(|row| row.rolname)
            .unwrap()
    }

    fn publication_all_flags(
        db: &Database,
        session: &mut Session,
        publication_name: &str,
    ) -> (bool, bool) {
        let result = session
            .execute(
                db,
                &format!(
                    "select puballtables, puballsequences from pg_publication where pubname = '{publication_name}'"
                ),
            )
            .unwrap();
        let StatementResult::Query { rows, .. } = result else {
            panic!("expected query result, got {result:?}");
        };
        assert_eq!(rows.len(), 1);
        match (&rows[0][0], &rows[0][1]) {
            (Value::Bool(all_tables), Value::Bool(all_sequences)) => (*all_tables, *all_sequences),
            other => panic!("expected boolean publication flags, got {other:?}"),
        }
    }

    #[test]
    fn create_publication_for_all_sequences_sets_catalog_flag() {
        let base = temp_dir("create_all_sequences");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(&db, "create publication pub for all sequences")
            .unwrap();

        assert_eq!(
            publication_all_flags(&db, &mut session, "pub"),
            (false, true)
        );
    }

    #[test]
    fn alter_publication_set_all_sequences_toggles_all_target_flags() {
        let base = temp_dir("alter_all_sequences");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create publication pub").unwrap();

        session
            .execute(&db, "alter publication pub set all tables, all sequences")
            .unwrap();
        assert_eq!(
            publication_all_flags(&db, &mut session, "pub"),
            (true, true)
        );

        session
            .execute(&db, "alter publication pub set all tables")
            .unwrap();
        assert_eq!(
            publication_all_flags(&db, &mut session, "pub"),
            (true, false)
        );

        session
            .execute(&db, "alter publication pub set all sequences")
            .unwrap();
        assert_eq!(
            publication_all_flags(&db, &mut session, "pub"),
            (false, true)
        );
    }

    #[test]
    fn alter_publication_set_all_sequences_rejects_non_empty_publication() {
        let base = temp_dir("alter_all_sequences_non_empty");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        session
            .execute(&db, "create publication pub for table widgets")
            .unwrap();

        let err = session
            .execute(&db, "alter publication pub set all sequences")
            .unwrap_err();
        match err {
            ExecError::DetailedError { message, .. } => assert_eq!(
                message,
                "publication \"pub\" does not support ALL SEQUENCES operations"
            ),
            other => panic!("expected detailed error, got {other:?}"),
        }
    }

    #[test]
    fn create_publication_for_all_tables_except_records_excluded_tables() {
        let base = temp_dir("create_all_tables_except");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        session
            .execute(&db, "create table gadgets (id int4)")
            .unwrap();

        session
            .execute(
                &db,
                "create publication pub for all tables except (table widgets, gadgets)",
            )
            .unwrap();

        assert_eq!(
            publication_all_flags(&db, &mut session, "pub"),
            (true, false)
        );
        let result = session
            .execute(
                &db,
                "select c.relname, pr.prexcept \
                 from pg_publication p \
                 join pg_publication_rel pr on p.oid = pr.prpubid \
                 join pg_class c on c.oid = pr.prrelid \
                 where p.pubname = 'pub' \
                 order by c.relname",
            )
            .unwrap();
        let StatementResult::Query { rows, .. } = result else {
            panic!("expected query result, got {result:?}");
        };
        assert_eq!(
            rows,
            vec![
                vec![Value::Text("gadgets".into()), Value::Bool(true)],
                vec![Value::Text("widgets".into()), Value::Bool(true)],
            ]
        );

        let result = session
            .execute(
                &db,
                "select n.nspname || '.' || c.relname \
                 from pg_class c \
                 join pg_namespace n on n.oid = c.relnamespace \
                 join pg_publication_rel pr on c.oid = pr.prrelid \
                 join pg_publication p on p.oid = pr.prpubid \
                 where p.pubname = 'pub' and pr.prexcept \
                 order by 1",
            )
            .unwrap();
        let StatementResult::Query { rows, .. } = result else {
            panic!("expected query result, got {result:?}");
        };
        assert_eq!(
            rows,
            vec![
                vec![Value::Text("public.gadgets".into())],
                vec![Value::Text("public.widgets".into())],
            ]
        );
    }

    #[test]
    fn alter_publication_set_all_tables_except_replaces_and_clears_exclusions() {
        let base = temp_dir("alter_all_tables_except");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        session
            .execute(&db, "create table gadgets (id int4)")
            .unwrap();
        session
            .execute(
                &db,
                "create publication pub for all tables except (table widgets)",
            )
            .unwrap();

        session
            .execute(
                &db,
                "alter publication pub set all tables except (table gadgets)",
            )
            .unwrap();
        let result = session
            .execute(
                &db,
                "select c.relname \
                 from pg_publication p \
                 join pg_publication_rel pr on p.oid = pr.prpubid \
                 join pg_class c on c.oid = pr.prrelid \
                 where p.pubname = 'pub' and pr.prexcept \
                 order by c.relname",
            )
            .unwrap();
        let StatementResult::Query { rows, .. } = result else {
            panic!("expected query result, got {result:?}");
        };
        assert_eq!(rows, vec![vec![Value::Text("gadgets".into())]]);

        session
            .execute(&db, "alter publication pub set all tables")
            .unwrap();
        let result = session
            .execute(
                &db,
                "select count(*) from pg_publication p \
                 join pg_publication_rel pr on p.oid = pr.prpubid \
                 where p.pubname = 'pub'",
            )
            .unwrap();
        let StatementResult::Query { rows, .. } = result else {
            panic!("expected query result, got {result:?}");
        };
        assert_eq!(rows, vec![vec![Value::Int64(0)]]);
        assert_eq!(
            publication_all_flags(&db, &mut session, "pub"),
            (true, false)
        );
    }

    #[test]
    fn publication_describe_queries_separate_included_and_except_publications() {
        let base = temp_dir("describe_all_tables_except");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        session
            .execute(&db, "create publication pub_all for all tables")
            .unwrap();
        session
            .execute(
                &db,
                "create publication pub_except for all tables except (table widgets)",
            )
            .unwrap();
        let entry = session
            .catalog_lookup(&db)
            .lookup_any_relation("widgets")
            .unwrap();

        let included_sql = format!(
            "select pubname \
             from pg_publication p \
             where p.puballtables \
               and pg_relation_is_publishable('{}') \
               and not exists ( \
                   select 1 \
                   from pg_publication_rel pr \
                   where pr.prpubid = p.oid and pr.prrelid = '{}' and pr.prexcept) \
             order by 1",
            entry.relation_oid, entry.relation_oid
        );
        let result = session.execute(&db, &included_sql).unwrap();
        let StatementResult::Query { rows, .. } = result else {
            panic!("expected query result, got {result:?}");
        };
        assert_eq!(rows, vec![vec![Value::Text("pub_all".into())]]);

        let except_sql = format!(
            "select pubname \
             from pg_publication p \
             join pg_publication_rel pr on p.oid = pr.prpubid \
             where pr.prrelid = '{}' and pr.prexcept \
             order by 1",
            entry.relation_oid
        );
        let result = session.execute(&db, &except_sql).unwrap();
        let StatementResult::Query { rows, .. } = result else {
            panic!("expected query result, got {result:?}");
        };
        assert_eq!(rows, vec![vec![Value::Text("pub_except".into())]]);
    }

    #[test]
    fn alter_publication_owner_to_checks_database_create_on_target_role() {
        let base = temp_dir("owner_target_create");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create role tenant").unwrap();
        superuser.execute(&db, "create role target").unwrap();
        superuser
            .execute(&db, "grant create on database regression to tenant")
            .unwrap();

        let tenant_oid = role_oid(&db, "tenant");
        let target_oid = role_oid(&db, "target");
        let mut tenant = Session::new(2);
        tenant.set_session_authorization_oid(tenant_oid);
        tenant
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        tenant
            .execute(&db, "create publication pub for table widgets")
            .unwrap();

        superuser
            .execute(&db, "revoke create on database regression from tenant")
            .unwrap();
        superuser
            .execute(&db, "grant create on database regression to target")
            .unwrap();
        superuser
            .execute(&db, "grant target to tenant with inherit false, set true")
            .unwrap();

        let auth = db.auth_state(2);
        let auth_catalog = db.auth_catalog(2, None).unwrap();
        assert!(!db.user_has_database_create_privilege(&auth, &auth_catalog));
        assert!(db.role_has_database_create_privilege(target_oid, &auth_catalog));

        tenant
            .execute(&db, "alter publication pub owner to target")
            .unwrap();
        assert_eq!(publication_owner_name(&db, "pub"), "target");
    }

    #[test]
    fn alter_publication_owner_to_rejects_target_without_database_create_privilege() {
        let base = temp_dir("owner_target_missing_create");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create role tenant").unwrap();
        superuser.execute(&db, "create role target").unwrap();
        superuser
            .execute(&db, "grant create on database regression to tenant")
            .unwrap();

        let mut tenant = Session::new(2);
        tenant.set_session_authorization_oid(role_oid(&db, "tenant"));
        tenant
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        tenant
            .execute(&db, "create publication pub for table widgets")
            .unwrap();

        superuser
            .execute(&db, "revoke create on database regression from tenant")
            .unwrap();
        superuser
            .execute(&db, "grant target to tenant with inherit false, set true")
            .unwrap();

        let err = tenant
            .execute(&db, "alter publication pub owner to target")
            .unwrap_err();
        assert!(matches!(
            err,
            ExecError::DetailedError {
                message,
                sqlstate: "42501",
                ..
            } if message == "permission denied for database regression"
        ));
        assert_eq!(publication_owner_name(&db, "pub"), "tenant");
    }

    #[test]
    fn superuser_can_transfer_for_all_tables_publication_to_non_superuser() {
        let base = temp_dir("owner_for_all_tables_non_superuser");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role target").unwrap();
        session
            .execute(&db, "grant create on database regression to target")
            .unwrap();
        session
            .execute(&db, "create publication pub for all tables")
            .unwrap();

        session
            .execute(&db, "alter publication pub owner to target")
            .unwrap();
        assert_eq!(publication_owner_name(&db, "pub"), "target");
    }

    #[test]
    fn superuser_can_transfer_schema_publication_to_non_superuser() {
        let base = temp_dir("owner_schema_publication_non_superuser");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role target").unwrap();
        session
            .execute(&db, "grant create on database regression to target")
            .unwrap();
        session.execute(&db, "create schema pub_test").unwrap();
        session
            .execute(&db, "create publication pub for tables in schema pub_test")
            .unwrap();

        session
            .execute(&db, "alter publication pub owner to target")
            .unwrap();
        assert_eq!(publication_owner_name(&db, "pub"), "target");
    }

    #[test]
    fn non_superuser_owner_to_requires_superuser_for_all_tables_publication() {
        let base = temp_dir("owner_for_all_tables_target_superuser");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create role tenant").unwrap();
        superuser.execute(&db, "create role target").unwrap();
        superuser
            .execute(&db, "grant create on database regression to tenant")
            .unwrap();
        superuser
            .execute(&db, "grant create on database regression to target")
            .unwrap();
        superuser
            .execute(&db, "grant target to tenant with inherit false, set true")
            .unwrap();
        superuser
            .execute(&db, "create publication pub for all tables")
            .unwrap();
        superuser
            .execute(&db, "alter publication pub owner to tenant")
            .unwrap();

        let mut tenant = Session::new(2);
        tenant.set_session_authorization_oid(role_oid(&db, "tenant"));
        let err = tenant
            .execute(&db, "alter publication pub owner to target")
            .unwrap_err();
        assert!(
            format!("{err:?}")
                .contains("The owner of a FOR ALL TABLES publication must be a superuser.")
        );
        assert_eq!(publication_owner_name(&db, "pub"), "tenant");
    }

    #[test]
    fn alter_publication_for_all_tables_errors_include_membership_detail() {
        let base = temp_dir("foralltables_detail");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        session
            .execute(&db, "create publication pub for all tables")
            .unwrap();

        let table_err = session
            .execute(&db, "alter publication pub add table widgets")
            .unwrap_err();
        match table_err {
            ExecError::DetailedError { detail, .. } => assert_eq!(
                detail.as_deref(),
                Some("Tables cannot be added to or dropped from FOR ALL TABLES publications.")
            ),
            other => panic!("expected detailed error, got {other:?}"),
        }

        let schema_err = session
            .execute(&db, "alter publication pub add tables in schema public")
            .unwrap_err();
        match schema_err {
            ExecError::DetailedError { detail, .. } => assert_eq!(
                detail.as_deref(),
                Some("Schemas cannot be added to or dropped from FOR ALL TABLES publications.")
            ),
            other => panic!("expected detailed error, got {other:?}"),
        }
    }

    #[test]
    fn publication_membership_errors_use_relation_names() {
        let base = temp_dir("membership_relation_names");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        session
            .execute(&db, "create table gadgets (id int4)")
            .unwrap();
        session
            .execute(&db, "create publication pub for table widgets")
            .unwrap();

        let duplicate = session
            .execute(&db, "alter publication pub add table widgets")
            .unwrap_err();
        let duplicate_text = format!("{duplicate:?}");
        assert!(duplicate_text.contains("widgets"));
        assert!(duplicate_text.contains("is already member of publication"));

        let missing = session
            .execute(&db, "alter publication pub drop table gadgets")
            .unwrap_err();
        let missing_text = format!("{missing:?}");
        assert!(missing_text.contains("gadgets"));
        assert!(missing_text.contains("is not part of the publication"));
    }

    #[test]
    fn publication_add_relation_errors_match_postgres_text() {
        let base = temp_dir("add_relation_errors");
        let db = Database::open(&base, 16).unwrap();
        let temp_relation = BoundRelation {
            rel: crate::backend::storage::smgr::RelFileLocator {
                spc_oid: 0,
                db_oid: 0,
                rel_number: 1,
            },
            relation_oid: 1,
            toast: None,
            namespace_oid: crate::include::catalog::PUBLIC_NAMESPACE_OID,
            owner_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
            of_type_oid: 0,
            relpersistence: 't',
            relkind: 'r',
            relispopulated: true,
            relispartition: false,
            relpartbound: None,
            desc: crate::include::nodes::primnodes::RelationDesc {
                columns: Vec::new(),
            },
            partitioned_table: None,
            partition_spec: None,
        };

        let temp_err =
            validate_publishable_relation(&db, 1, &temp_relation, "temp_items").unwrap_err();
        match temp_err {
            ExecError::DetailedError {
                message,
                detail,
                sqlstate,
                ..
            } => {
                assert_eq!(message, "cannot add relation \"temp_items\" to publication");
                assert_eq!(
                    detail.as_deref(),
                    Some("This operation is not supported for temporary tables.")
                );
                assert_eq!(sqlstate, "22023");
            }
            other => panic!("expected detailed error, got {other:?}"),
        }

        let unlogged_relation = BoundRelation {
            rel: crate::backend::storage::smgr::RelFileLocator {
                spc_oid: 0,
                db_oid: 0,
                rel_number: 2,
            },
            relation_oid: 2,
            toast: None,
            namespace_oid: crate::include::catalog::PUBLIC_NAMESPACE_OID,
            owner_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
            of_type_oid: 0,
            relpersistence: 'u',
            relkind: 'r',
            relispopulated: true,
            relispartition: false,
            relpartbound: None,
            desc: crate::include::nodes::primnodes::RelationDesc {
                columns: Vec::new(),
            },
            partitioned_table: None,
            partition_spec: None,
        };

        let unlogged_err =
            validate_publishable_relation(&db, 1, &unlogged_relation, "unlogged_items")
                .unwrap_err();
        match unlogged_err {
            ExecError::DetailedError {
                message,
                detail,
                sqlstate,
                ..
            } => {
                assert_eq!(
                    message,
                    "cannot add relation \"unlogged_items\" to publication"
                );
                assert_eq!(
                    detail.as_deref(),
                    Some("This operation is not supported for unlogged tables.")
                );
                assert_eq!(sqlstate, "22023");
            }
            other => panic!("expected detailed error, got {other:?}"),
        }
    }

    #[test]
    fn publication_add_schema_errors_match_postgres_text() {
        let base = temp_dir("add_schema_errors");
        let db = Database::open(&base, 16).unwrap();

        let system_err =
            validate_publishable_schema(&db, 1, "pg_catalog", PG_CATALOG_NAMESPACE_OID)
                .unwrap_err();
        match system_err {
            ExecError::DetailedError {
                message,
                detail,
                sqlstate,
                ..
            } => {
                assert_eq!(message, "cannot add schema \"pg_catalog\" to publication");
                assert_eq!(
                    detail.as_deref(),
                    Some("This operation is not supported for system schemas.")
                );
                assert_eq!(sqlstate, "22023");
            }
            other => panic!("expected detailed error, got {other:?}"),
        }
    }

    #[test]
    fn session_authorization_path_can_create_schema_qualified_publication_memberships() {
        let base = temp_dir("session_auth_schema_membership");
        let db = Database::open(&base, 16).unwrap();
        let mut admin = Session::new(1);
        admin
            .execute(&db, "create role regress_publication_user login superuser")
            .unwrap();

        let mut session = Session::new(2);
        session
            .execute(&db, "set session authorization regress_publication_user")
            .unwrap();
        session.execute(&db, "create schema pub_test").unwrap();
        session
            .execute(&db, "create table testpub_tbl1 (id int4)")
            .unwrap();
        session
            .execute(
                &db,
                "create table pub_test.testpub_nopk (foo int4, bar int4)",
            )
            .unwrap();
        session
            .execute(&db, "create publication pub for table testpub_tbl1")
            .unwrap();
        session
            .execute(&db, "alter publication pub add tables in schema pub_test")
            .unwrap();

        let publication_oid = db
            .backend_catcache(2, None)
            .unwrap()
            .publication_row_by_name("pub")
            .map(|row| row.oid)
            .unwrap();
        let rows = match session
            .execute(
                &db,
                &format!(
                    "select n.nspname \
                     from pg_catalog.pg_namespace n \
                          join pg_catalog.pg_publication_namespace pn on n.oid = pn.pnnspid \
                     where pn.pnpubid = '{}' \
                     order by 1",
                    publication_oid
                ),
            )
            .unwrap()
        {
            StatementResult::Query { rows, .. } => rows,
            other => panic!("expected query result, got {other:?}"),
        };
        assert_eq!(rows, vec![vec![Value::Text("pub_test".into())]]);
    }

    #[test]
    fn current_schema_publications_preserve_quoted_schema_and_table_names() {
        let base = temp_dir("current_schema_quoted_names");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create temp table temp_marker (id int4)")
            .unwrap();
        session
            .execute(&db, "create schema \"CURRENT_SCHEMA\"")
            .unwrap();
        session
            .execute(
                &db,
                "create table \"CURRENT_SCHEMA\".\"CURRENT_SCHEMA\" (id int4)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create publication pub_current for tables in schema CURRENT_SCHEMA",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create publication pub_quoted for tables in schema \"CURRENT_SCHEMA\"",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create publication pub_both for tables in schema CURRENT_SCHEMA, \"CURRENT_SCHEMA\"",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create publication pub_table for table \"CURRENT_SCHEMA\".\"CURRENT_SCHEMA\"",
            )
            .unwrap();

        let schema_rows = match session
            .execute(
                &db,
                "select p.pubname, n.nspname \
                 from pg_catalog.pg_publication p \
                      join pg_catalog.pg_publication_namespace pn on pn.pnpubid = p.oid \
                      join pg_catalog.pg_namespace n on n.oid = pn.pnnspid \
                 where p.pubname in ('pub_both', 'pub_current', 'pub_quoted') \
                 order by p.pubname, n.nspname",
            )
            .unwrap()
        {
            StatementResult::Query { rows, .. } => rows,
            other => panic!("expected query result, got {other:?}"),
        };
        assert_eq!(
            schema_rows,
            vec![
                vec![
                    Value::Text("pub_both".into()),
                    Value::Text("CURRENT_SCHEMA".into()),
                ],
                vec![Value::Text("pub_both".into()), Value::Text("public".into())],
                vec![
                    Value::Text("pub_current".into()),
                    Value::Text("public".into()),
                ],
                vec![
                    Value::Text("pub_quoted".into()),
                    Value::Text("CURRENT_SCHEMA".into()),
                ],
            ]
        );

        let table_rows = match session
            .execute(
                &db,
                "select p.pubname, n.nspname, c.relname \
                 from pg_catalog.pg_publication p \
                      join pg_catalog.pg_publication_rel pr on pr.prpubid = p.oid \
                      join pg_catalog.pg_class c on c.oid = pr.prrelid \
                      join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
                 where p.pubname = 'pub_table'",
            )
            .unwrap()
        {
            StatementResult::Query { rows, .. } => rows,
            other => panic!("expected query result, got {other:?}"),
        };
        assert_eq!(
            table_rows,
            vec![vec![
                Value::Text("pub_table".into()),
                Value::Text("CURRENT_SCHEMA".into()),
                Value::Text("CURRENT_SCHEMA".into()),
            ]]
        );

        session.execute(&db, "set search_path = ''").unwrap();
        let err = session
            .execute(
                &db,
                "create publication pub_empty for tables in schema CURRENT_SCHEMA",
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ExecError::DetailedError {
                message,
                sqlstate: "3F000",
                ..
            } if message == "no schema has been selected for CURRENT_SCHEMA"
        ));
        let err = session
            .execute(&db, "create publication pub_bad for table CURRENT_SCHEMA")
            .unwrap_err();
        assert!(matches!(
            err,
            ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
                actual,
                ..
            }) if actual == "syntax error at or near \"CURRENT_SCHEMA\""
        ));
    }

    #[test]
    fn create_publication_stores_table_filter_and_column_list() {
        let base = temp_dir("table_filter_column_list");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4, name text)")
            .unwrap();
        session
            .execute(
                &db,
                "create publication pub for table only widgets(id) where (id > 0)",
            )
            .unwrap();

        let catcache = db.backend_catcache(1, None).unwrap();
        let publication_oid = catcache.publication_row_by_name("pub").unwrap().oid;
        let rows = catcache.publication_rel_rows_for_publication(publication_oid);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].prqual.as_deref(), Some("(id > 0)"));
        assert_eq!(rows[0].prattrs, Some(vec![1]));
    }

    #[test]
    fn pg_publication_tables_lists_column_lists_and_filters() {
        let base = temp_dir("pg_publication_tables_view");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4, name text)")
            .unwrap();
        session
            .execute(
                &db,
                "create publication pub for table only widgets(id) where (id > 0)",
            )
            .unwrap();

        let result = session
            .execute(
                &db,
                "select pubname, schemaname, tablename, attnames, rowfilter \
                 from pg_publication_tables \
                 where pubname = 'pub'",
            )
            .unwrap();
        let StatementResult::Query { rows, .. } = result else {
            panic!("expected query result, got {result:?}");
        };
        assert_eq!(
            rows,
            vec![vec![
                Value::Text("pub".into()),
                Value::Text("public".into()),
                Value::Text("widgets".into()),
                Value::Array(vec![Value::Text("id".into())]),
                Value::Text("(id > 0)".into()),
            ]]
        );
    }

    #[test]
    fn pg_publication_tables_honors_all_tables_except() {
        let base = temp_dir("pg_publication_tables_except");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        session
            .execute(&db, "create table gadgets (id int4)")
            .unwrap();
        session
            .execute(
                &db,
                "create publication pub for all tables except (table gadgets)",
            )
            .unwrap();

        let result = session
            .execute(
                &db,
                "select tablename \
                 from pg_publication_tables \
                 where pubname = 'pub' and tablename in ('widgets', 'gadgets') \
                 order by tablename",
            )
            .unwrap();
        let StatementResult::Query { rows, .. } = result else {
            panic!("expected query result, got {result:?}");
        };
        assert_eq!(rows, vec![vec![Value::Text("widgets".into())]]);
    }

    #[test]
    fn pg_get_publication_tables_returns_attrs_and_qual() {
        let base = temp_dir("pg_get_publication_tables");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4, name text)")
            .unwrap();
        session
            .execute(
                &db,
                "create publication pub for table only widgets(id) where (id > 0)",
            )
            .unwrap();

        let result = session
            .execute(
                &db,
                "select attrs, qual from pg_get_publication_tables('pub')",
            )
            .unwrap();
        let StatementResult::Query { rows, .. } = result else {
            panic!("expected query result, got {result:?}");
        };
        assert_eq!(
            rows,
            vec![vec![
                Value::Array(vec![Value::Int16(1)]),
                Value::Text("(id > 0)".into()),
            ]]
        );
    }

    #[test]
    fn publication_column_list_rejects_duplicate_membership_and_system_columns() {
        let base = temp_dir("column_list_validation");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4)")
            .unwrap();

        let duplicate = session
            .execute(
                &db,
                "create publication pub_dup for table widgets(id), widgets with (publish = 'insert')",
            )
            .unwrap_err();
        match duplicate {
            ExecError::DetailedError { message, .. } => assert_eq!(
                message,
                "conflicting or redundant column lists for table \"widgets\""
            ),
            other => panic!("expected duplicate column list error, got {other:?}"),
        }

        session
            .execute(&db, "create publication pub for table widgets")
            .unwrap();
        let system_column = session
            .execute(&db, "alter publication pub set table widgets(id, ctid)")
            .unwrap_err();
        match system_column {
            ExecError::DetailedError {
                message, sqlstate, ..
            } => {
                assert_eq!(
                    message,
                    "cannot use system column \"ctid\" in publication column list"
                );
                assert_eq!(sqlstate, "42P10");
            }
            other => panic!("expected system column list error, got {other:?}"),
        }
    }

    #[test]
    fn publication_column_list_rejects_virtual_generated_columns() {
        let base = temp_dir("column_list_virtual_generated");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(
                &db,
                "create table widgets (id int4, v int4 generated always as (id + 1) virtual)",
            )
            .unwrap();

        let err = session
            .execute(&db, "create publication pub for table widgets(id, v)")
            .unwrap_err();
        match err {
            ExecError::DetailedError { message, .. } => assert_eq!(
                message,
                "cannot use virtual generated column \"v\" in publication column list"
            ),
            other => panic!("expected virtual generated column list error, got {other:?}"),
        }
    }

    #[test]
    fn publication_column_list_blocks_drop_column() {
        let base = temp_dir("column_list_drop_column");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (a int primary key, c int)")
            .unwrap();
        session
            .execute(&db, "create publication pub for table widgets(a, c)")
            .unwrap();

        let err = session
            .execute(&db, "alter table widgets drop column c")
            .unwrap_err();
        match err {
            ExecError::DetailedError {
                message,
                detail,
                hint,
                sqlstate,
            } => {
                assert_eq!(
                    message,
                    "cannot drop column c of table widgets because other objects depend on it"
                );
                assert_eq!(
                    detail.as_deref(),
                    Some(
                        "publication of table widgets in publication pub depends on column c of table widgets"
                    )
                );
                assert_eq!(
                    hint.as_deref(),
                    Some("Use DROP ... CASCADE to drop the dependent objects too.")
                );
                assert_eq!(sqlstate, "2BP01");
            }
            other => panic!("expected publication dependency error, got {other:?}"),
        }
    }

    #[test]
    fn publication_column_list_rejects_schema_publication_mix() {
        let base = temp_dir("column_list_schema_mix");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create schema pub_test").unwrap();
        session
            .execute(&db, "create table widgets (id int primary key)")
            .unwrap();

        let create_err = session
            .execute(
                &db,
                "create publication pub_bad for tables in schema pub_test, table public.widgets(id)",
            )
            .unwrap_err();
        match create_err {
            ExecError::DetailedError {
                message, detail, ..
            } => {
                assert_eq!(
                    message,
                    "cannot use column list for relation \"public.widgets\" in publication \"pub_bad\""
                );
                assert_eq!(
                    detail.as_deref(),
                    Some(
                        "Column lists cannot be specified in publications containing FOR TABLES IN SCHEMA elements."
                    )
                );
            }
            other => panic!("expected schema/column-list create error, got {other:?}"),
        }

        session
            .execute(
                &db,
                "create publication pub_schema for tables in schema pub_test",
            )
            .unwrap();
        let add_table_err = session
            .execute(
                &db,
                "alter publication pub_schema add table public.widgets(id)",
            )
            .unwrap_err();
        match add_table_err {
            ExecError::DetailedError { message, .. } => assert_eq!(
                message,
                "cannot use column list for relation \"public.widgets\" in publication \"pub_schema\""
            ),
            other => panic!("expected schema/column-list add table error, got {other:?}"),
        }

        session
            .execute(&db, "create publication pub_table for table widgets(id)")
            .unwrap();
        let add_schema_err = session
            .execute(
                &db,
                "alter publication pub_table add tables in schema pub_test",
            )
            .unwrap_err();
        match add_schema_err {
            ExecError::DetailedError {
                message, detail, ..
            } => {
                assert_eq!(message, "cannot add schema to publication \"pub_table\"");
                assert_eq!(
                    detail.as_deref(),
                    Some(
                        "Schemas cannot be added if any tables that specify a column list are already part of the publication."
                    )
                );
            }
            other => panic!("expected schema/column-list add schema error, got {other:?}"),
        }
    }

    #[test]
    fn publication_update_requires_replica_identity_even_without_rows() {
        let base = temp_dir("publication_update_empty_requires_ri");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int)")
            .unwrap();
        session
            .execute(&db, "create publication pub for table widgets")
            .unwrap();

        session
            .execute(&db, "update widgets set id = 1 where false")
            .unwrap();
        let err = session
            .execute(&db, "update widgets set id = 1")
            .unwrap_err();
        match err {
            ExecError::DetailedError { message, hint, .. } => {
                assert_eq!(
                    message,
                    "cannot update table \"widgets\" because it does not have a replica identity and publishes updates"
                );
                assert_eq!(
                    hint.as_deref(),
                    Some("To enable updating the table, set REPLICA IDENTITY using ALTER TABLE.")
                );
            }
            other => panic!("expected missing replica identity update error, got {other:?}"),
        }
    }

    #[test]
    fn virtual_generated_column_rejects_user_defined_function() {
        let base = temp_dir("virtual_generated_user_function");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(
                &db,
                "create function gen_func() returns integer immutable as $$ begin return 7; end; $$ language plpgsql",
            )
            .unwrap();

        let err = session
            .execute(
                &db,
                "create table widgets (id int primary key, x int, y int generated always as (x * gen_func()) virtual)",
            )
            .unwrap_err();
        match err {
            ExecError::Parse(ParseError::DetailedError {
                message, detail, ..
            })
            | ExecError::DetailedError {
                message, detail, ..
            } => {
                assert_eq!(message, "generation expression uses user-defined function");
                assert_eq!(
                    detail.as_deref(),
                    Some(
                        "Virtual generated columns that make use of user-defined functions are not yet supported."
                    )
                );
            }
            other => panic!("expected virtual generated function error, got {other:?}"),
        }
    }

    #[test]
    fn publication_blocks_virtual_generated_set_expression() {
        let base = temp_dir("publication_virtual_generated_set_expression");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(
                &db,
                "create table widgets (id int primary key, x int, y int generated always as (x * 111) virtual)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create publication pub for table widgets where (y > 100)",
            )
            .unwrap();

        let err = session
            .execute(
                &db,
                "alter table widgets alter column y set expression as (x * 222)",
            )
            .unwrap_err();
        match err {
            ExecError::DetailedError {
                message, detail, ..
            } => {
                assert_eq!(
                    message,
                    "ALTER TABLE / SET EXPRESSION is not supported for virtual generated columns in tables that are part of a publication"
                );
                assert_eq!(
                    detail.as_deref(),
                    Some("Column \"y\" of relation \"widgets\" is a virtual generated column.")
                );
            }
            other => panic!("expected virtual generated publication error, got {other:?}"),
        }
    }

    #[test]
    fn publication_rejects_views_with_publication_error() {
        let base = temp_dir("publication_view_error");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int)")
            .unwrap();
        session
            .execute(&db, "create view widgets_view as select * from widgets")
            .unwrap();

        let err = session
            .execute(&db, "create publication pub for table widgets_view")
            .unwrap_err();
        match err {
            ExecError::DetailedError {
                message, detail, ..
            } => {
                assert_eq!(
                    message,
                    "cannot add relation \"widgets_view\" to publication"
                );
                assert_eq!(
                    detail.as_deref(),
                    Some("This operation is not supported for views.")
                );
            }
            other => panic!("expected publication view error, got {other:?}"),
        }
    }

    #[test]
    fn publication_row_filter_rejects_user_defined_column_types() {
        let base = temp_dir("row_filter_user_type");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create type bug_status as enum ('new', 'open')")
            .unwrap();
        session
            .execute(&db, "create table bugs (status bug_status)")
            .unwrap();

        let err = session
            .execute(
                &db,
                "create publication pub for table bugs where (status = 'open') with (publish = 'insert')",
            )
            .unwrap_err();
        match err {
            ExecError::DetailedError {
                message, detail, ..
            } => {
                assert_eq!(message, "invalid publication WHERE expression");
                assert_eq!(
                    detail.as_deref(),
                    Some("User-defined types are not allowed.")
                );
            }
            other => panic!("expected user-defined type filter error, got {other:?}"),
        }
        assert!(
            db.backend_catcache(1, None)
                .unwrap()
                .publication_row_by_name("pub")
                .is_none()
        );
    }

    #[test]
    fn alter_publication_drop_rejects_where_clause_without_losing_publication() {
        let base = temp_dir("drop_filter_keeps_publication");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        session
            .execute(
                &db,
                "create publication pub for table widgets where (id > 0)",
            )
            .unwrap();

        let err = session
            .execute(
                &db,
                "alter publication pub drop table widgets where (id > 0)",
            )
            .unwrap_err();
        match err {
            ExecError::DetailedError {
                message, sqlstate, ..
            } => {
                assert_eq!(
                    message,
                    "cannot use a WHERE clause when removing a table from a publication"
                );
                assert_eq!(sqlstate, "42601");
            }
            other => panic!("expected detailed error, got {other:?}"),
        }

        assert!(
            db.backend_catcache(1, None)
                .unwrap()
                .publication_row_by_name("pub")
                .is_some()
        );
    }

    #[test]
    fn publication_row_filter_rejects_invalid_expressions() {
        let base = temp_dir("invalid_publication_row_filters");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4, note text)")
            .unwrap();
        session
            .execute(
                &db,
                "create publication pub for table widgets where (id > 0)",
            )
            .unwrap();

        let duplicate = session
            .execute(
                &db,
                "create publication dup for table widgets where (id > 0), widgets",
            )
            .unwrap_err();
        match duplicate {
            ExecError::DetailedError { message, .. } => assert_eq!(
                message,
                "conflicting or redundant WHERE clauses for table \"widgets\""
            ),
            other => panic!("expected duplicate filter error, got {other:?}"),
        }

        let non_bool = session
            .execute(&db, "alter publication pub set table widgets where (1234)")
            .unwrap_err();
        match non_bool {
            ExecError::DetailedError { message, .. } => assert_eq!(
                message,
                "argument of PUBLICATION WHERE must be type boolean, not type integer"
            ),
            other => panic!("expected non-boolean filter error, got {other:?}"),
        }

        let aggregate = session
            .execute(
                &db,
                "alter publication pub set table widgets where (id < avg(id))",
            )
            .unwrap_err();
        match aggregate {
            ExecError::DetailedError { message, .. } => {
                assert_eq!(message, "aggregate functions are not allowed in WHERE");
            }
            other => panic!("expected aggregate filter error, got {other:?}"),
        }
    }

    #[test]
    fn alter_publication_drop_after_column_list_update_error_keeps_membership_editable() {
        let base = temp_dir("drop_after_column_list_update_error");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(
                &db,
                "create table widgets (a int primary key, b text, c text)",
            )
            .unwrap();
        session
            .execute(&db, "alter table widgets alter column b set not null")
            .unwrap();
        session
            .execute(&db, "create unique index widgets_b_key on widgets(b)")
            .unwrap();
        session
            .execute(
                &db,
                "alter table widgets replica identity using index widgets_b_key",
            )
            .unwrap();
        session
            .execute(&db, "create publication pub for table widgets(a)")
            .unwrap();
        let err = session
            .execute(&db, "update widgets set a = 1")
            .unwrap_err();
        assert!(format!("{err:?}").contains("Column list used by the publication"));

        session
            .execute(&db, "alter publication pub drop table widgets")
            .unwrap();
    }
}
