use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use super::super::*;
use crate::backend::parser::{
    AlterSubscriptionAction, AlterSubscriptionStatement, CommentOnSubscriptionStatement,
    CreateSubscriptionStatement, DropSubscriptionStatement, SubscriptionOption,
    SubscriptionOptionValue,
};
use crate::backend::utils::cache::catcache::normalize_catalog_name;
use crate::backend::utils::cache::syscache::{
    SysCacheId, SysCacheTuple, search_sys_cache_list1_db, search_sys_cache1_db,
};
use crate::backend::utils::misc::notices::{push_notice, push_warning_with_hint};
use crate::include::catalog::{
    PG_CREATE_SUBSCRIPTION_OID, PgAuthIdRow, PgAuthMembersRow, PgSubscriptionRow,
};
use crate::include::nodes::datum::Value;
use crate::pgrust::database::ddl::map_catalog_error;

#[derive(Clone, Copy)]
enum MembershipMode {
    Inherit,
    Set,
}

#[derive(Debug, Default)]
struct CreateSubscriptionOptions {
    connect: Option<bool>,
    enabled: Option<bool>,
    create_slot: Option<bool>,
    copy_data: Option<bool>,
    binary: Option<bool>,
    streaming: Option<char>,
    two_phase: Option<bool>,
    disable_on_error: Option<bool>,
    password_required: Option<bool>,
    run_as_owner: Option<bool>,
    failover: Option<bool>,
    slot_name: Option<Option<String>>,
    synchronous_commit: Option<String>,
    origin: Option<String>,
}

#[derive(Debug, Default)]
struct AlterSubscriptionOptions {
    binary: Option<bool>,
    streaming: Option<char>,
    two_phase: Option<bool>,
    disable_on_error: Option<bool>,
    password_required: Option<bool>,
    run_as_owner: Option<bool>,
    failover: Option<bool>,
    slot_name: Option<Option<String>>,
    synchronous_commit: Option<String>,
    origin: Option<String>,
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

impl Database {
    pub(crate) fn execute_create_subscription_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateSubscriptionStatement,
        _configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let catalog_effects = Vec::new();
        let options = parse_create_subscription_options(&stmt.options);
        let result = options
            .and_then(|options| effective_create_options(&stmt.subscription_name, options))
            .and_then(|effective| {
                self.create_subscription_inner(client_id, stmt, Some((xid, 0)), effective)
            });
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_subscription_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateSubscriptionStatement,
        xid: TransactionId,
        cid: CommandId,
        _configured_search_path: Option<&[String]>,
        _catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let txn_ctx = Some((xid, cid));
        let options = parse_create_subscription_options(&stmt.options)?;
        let effective = effective_create_options(&stmt.subscription_name, options)?;

        if effective.create_slot {
            return Err(ExecError::DetailedError {
                message:
                    "CREATE SUBSCRIPTION ... WITH (create_slot = true) cannot run inside a transaction block"
                        .into(),
                detail: None,
                hint: None,
                sqlstate: "25001",
            });
        }

        self.create_subscription_inner(client_id, stmt, txn_ctx, effective)
    }

    fn create_subscription_inner(
        &self,
        client_id: ClientId,
        stmt: &CreateSubscriptionStatement,
        txn_ctx: CatalogTxnContext,
        effective: EffectiveCreateOptions,
    ) -> Result<StatementResult, ExecError> {
        reject_duplicate_publications(&stmt.publications)?;
        validate_connection_string(&stmt.connection, &stmt.subscription_name, effective.connect)?;
        let auth = self.auth_state(client_id);
        let current_role =
            role_row_by_oid_visible(self, client_id, txn_ctx, auth.current_user_oid())?
                .ok_or_else(current_role_missing_error)?;

        if !current_role.rolsuper
            && !role_has_effective_membership_visible(
                self,
                client_id,
                txn_ctx,
                auth.current_user_oid(),
                PG_CREATE_SUBSCRIPTION_OID,
                MembershipMode::Inherit,
            )?
        {
            return Err(ExecError::DetailedError {
                message: "permission denied to create subscription".into(),
                detail: Some(
                    "Only roles with privileges of the \"pg_create_subscription\" role may create subscriptions."
                        .into(),
                ),
                hint: None,
                sqlstate: "42501",
            });
        }
        if !role_has_database_create_privilege_visible(
            self,
            client_id,
            txn_ctx,
            auth.current_user_oid(),
        )? {
            return Err(permission_denied_for_database_error(
                &subscription_database_name_for_permission_error(self),
            ));
        }
        if self
            .object_addresses
            .read()
            .subscription_by_name(&stmt.subscription_name)
            .is_some()
        {
            return Err(subscription_already_exists_error(&stmt.subscription_name));
        }
        if !current_role.rolsuper && !effective.password_required {
            return Err(password_required_false_superuser_only_error());
        }
        if !current_role.rolsuper && !connection_has_password(&stmt.connection) {
            return Err(ExecError::DetailedError {
                message: "password is required".into(),
                detail: Some(
                    "Non-superusers must provide a password in the connection string.".into(),
                ),
                hint: None,
                sqlstate: "28000",
            });
        }

        let row = PgSubscriptionRow {
            oid: 0,
            subdbid: self.database_oid,
            subskiplsn: 0,
            subname: stmt.subscription_name.clone(),
            subowner: auth.current_user_oid(),
            subenabled: effective.enabled,
            subbinary: effective.binary,
            substream: effective.streaming,
            subtwophasestate: if effective.two_phase { 'p' } else { 'd' },
            subdisableonerr: effective.disable_on_error,
            subpasswordrequired: effective.password_required,
            subrunasowner: effective.run_as_owner,
            subfailover: effective.failover,
            subconninfo: stmt.connection.clone(),
            subslotname: effective.slot_name,
            subsynccommit: effective.synchronous_commit,
            subpublications: stmt.publications.clone(),
            suborigin: effective.origin,
        };
        self.object_addresses.write().insert_subscription_row(row);
        self.plan_cache.invalidate_all();
        if !effective.connect {
            push_warning_with_hint(
                "subscription was created, but is not connected",
                "To initiate replication, you must manually create the replication slot, enable the subscription, and refresh the subscription.",
            );
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_subscription_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterSubscriptionStatement,
        _configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let catalog_effects = Vec::new();
        let result = self.alter_subscription_inner(client_id, stmt, Some((xid, 0)));
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_subscription_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterSubscriptionStatement,
        xid: TransactionId,
        cid: CommandId,
        _configured_search_path: Option<&[String]>,
        _catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        if alter_subscription_refreshes(&stmt.action)? {
            return Err(ExecError::DetailedError {
                message: match &stmt.action {
                    AlterSubscriptionAction::RefreshPublication { .. } => {
                        "ALTER SUBSCRIPTION ... REFRESH cannot run inside a transaction block"
                    }
                    _ => "ALTER SUBSCRIPTION with refresh cannot run inside a transaction block",
                }
                .into(),
                detail: None,
                hint: None,
                sqlstate: "25001",
            });
        }
        if alter_subscription_sets_failover(&stmt.action)? {
            return Err(ExecError::DetailedError {
                message:
                    "ALTER SUBSCRIPTION ... SET (failover) cannot run inside a transaction block"
                        .into(),
                detail: None,
                hint: None,
                sqlstate: "25001",
            });
        }
        self.alter_subscription_inner(client_id, stmt, Some((xid, cid)))
    }

    fn alter_subscription_inner(
        &self,
        client_id: ClientId,
        stmt: &AlterSubscriptionStatement,
        txn_ctx: CatalogTxnContext,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let current_role =
            role_row_by_oid_visible(self, client_id, txn_ctx, auth.current_user_oid())?
                .ok_or_else(current_role_missing_error)?;
        let existing = self
            .object_addresses
            .read()
            .subscription_by_name(&stmt.subscription_name)
            .map(|entry| entry.row.clone())
            .ok_or_else(|| subscription_does_not_exist_error(&stmt.subscription_name))?;
        ensure_subscription_owner(
            self,
            client_id,
            txn_ctx,
            &current_role,
            &existing,
            auth.current_user_oid(),
        )?;

        match &stmt.action {
            AlterSubscriptionAction::Rename { new_name } => {
                if !current_role.rolsuper
                    && !role_has_database_create_privilege_visible(
                        self,
                        client_id,
                        txn_ctx,
                        auth.current_user_oid(),
                    )?
                {
                    return Err(permission_denied_for_database_error(
                        &subscription_database_name_for_permission_error(self),
                    ));
                }
                if self
                    .object_addresses
                    .read()
                    .subscription_by_name(new_name)
                    .is_some()
                {
                    return Err(subscription_already_exists_error(new_name));
                }
                self.object_addresses
                    .write()
                    .subscription_by_name_mut(&stmt.subscription_name)
                    .expect("subscription was checked")
                    .row
                    .subname = new_name.clone();
            }
            AlterSubscriptionAction::OwnerTo { new_owner } => {
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
                            &subscription_database_name_for_permission_error(self),
                        ));
                    }
                }
                self.object_addresses
                    .write()
                    .subscription_by_name_mut(&stmt.subscription_name)
                    .expect("subscription was checked")
                    .row
                    .subowner = new_owner_row.oid;
            }
            AlterSubscriptionAction::Connection(conninfo) => {
                validate_connection_string(conninfo, &existing.subname, false)?;
                self.object_addresses
                    .write()
                    .subscription_by_name_mut(&stmt.subscription_name)
                    .expect("subscription was checked")
                    .row
                    .subconninfo = conninfo.clone();
            }
            AlterSubscriptionAction::SetOptions(options) => {
                let parsed = parse_alter_subscription_options(options)?;
                if !current_role.rolsuper && parsed.password_required == Some(false) {
                    return Err(password_required_false_superuser_only_error());
                }
                let mut state = self.object_addresses.write();
                let entry = state
                    .subscription_by_name_mut(&stmt.subscription_name)
                    .expect("subscription was checked");
                apply_alter_subscription_options(&mut entry.row, parsed)?;
            }
            AlterSubscriptionAction::SetPublication {
                publications,
                options,
            } => {
                reject_duplicate_publications(publications)?;
                let refresh = refresh_option(options)?;
                if refresh && !existing.subenabled {
                    return Err(refresh_disabled_subscription_error());
                }
                self.object_addresses
                    .write()
                    .subscription_by_name_mut(&stmt.subscription_name)
                    .expect("subscription was checked")
                    .row
                    .subpublications = publications.clone();
            }
            AlterSubscriptionAction::AddPublication {
                publications,
                options,
            } => {
                reject_duplicate_publications(publications)?;
                let refresh = refresh_option(options)?;
                if refresh && !existing.subenabled {
                    return Err(refresh_disabled_subscription_error());
                }
                let existing_names = existing
                    .subpublications
                    .iter()
                    .map(|name| name.to_ascii_lowercase())
                    .collect::<BTreeSet<_>>();
                for publication in publications {
                    if existing_names.contains(&publication.to_ascii_lowercase()) {
                        return Err(ExecError::DetailedError {
                            message: format!(
                                "publication \"{publication}\" is already in subscription \"{}\"",
                                existing.subname
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42710",
                        });
                    }
                }
                self.object_addresses
                    .write()
                    .subscription_by_name_mut(&stmt.subscription_name)
                    .expect("subscription was checked")
                    .row
                    .subpublications
                    .extend(publications.iter().cloned());
            }
            AlterSubscriptionAction::DropPublication {
                publications,
                options,
            } => {
                reject_duplicate_publications(publications)?;
                let refresh = refresh_option(options)?;
                if refresh && !existing.subenabled {
                    return Err(refresh_disabled_subscription_error());
                }
                let target_names = publications
                    .iter()
                    .map(|name| name.to_ascii_lowercase())
                    .collect::<BTreeSet<_>>();
                if existing
                    .subpublications
                    .iter()
                    .all(|name| target_names.contains(&name.to_ascii_lowercase()))
                {
                    return Err(ExecError::DetailedError {
                        message: "cannot drop all the publications from a subscription".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "22023",
                    });
                }
                for publication in publications {
                    if !existing
                        .subpublications
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(publication))
                    {
                        return Err(ExecError::DetailedError {
                            message: format!(
                                "publication \"{publication}\" is not in subscription \"{}\"",
                                existing.subname
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42704",
                        });
                    }
                }
                self.object_addresses
                    .write()
                    .subscription_by_name_mut(&stmt.subscription_name)
                    .expect("subscription was checked")
                    .row
                    .subpublications
                    .retain(|name| !target_names.contains(&name.to_ascii_lowercase()));
            }
            AlterSubscriptionAction::RefreshPublication { options } => {
                let refresh = refresh_option(options)?;
                if refresh && !existing.subenabled {
                    return Err(refresh_disabled_subscription_error());
                }
            }
            AlterSubscriptionAction::Enable => {
                if existing.subslotname.is_none() {
                    return Err(ExecError::DetailedError {
                        message: "cannot enable subscription that does not have a slot name".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "55000",
                    });
                }
                self.object_addresses
                    .write()
                    .subscription_by_name_mut(&stmt.subscription_name)
                    .expect("subscription was checked")
                    .row
                    .subenabled = true;
            }
            AlterSubscriptionAction::Disable => {
                self.object_addresses
                    .write()
                    .subscription_by_name_mut(&stmt.subscription_name)
                    .expect("subscription was checked")
                    .row
                    .subenabled = false;
            }
            AlterSubscriptionAction::Skip(options) => {
                let lsn = skip_lsn_option(options)?;
                self.object_addresses
                    .write()
                    .subscription_by_name_mut(&stmt.subscription_name)
                    .expect("subscription was checked")
                    .row
                    .subskiplsn = lsn;
            }
        }
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_subscription_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropSubscriptionStatement,
        _configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.drop_subscription_inner(
            client_id,
            stmt,
            Some((xid, 0)),
            Some((xid, 0, &mut catalog_effects)),
            false,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_drop_subscription_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropSubscriptionStatement,
        xid: TransactionId,
        cid: CommandId,
        _configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        self.drop_subscription_inner(
            client_id,
            stmt,
            Some((xid, cid)),
            Some((xid, cid, catalog_effects)),
            true,
        )
    }

    fn drop_subscription_inner(
        &self,
        client_id: ClientId,
        stmt: &DropSubscriptionStatement,
        txn_ctx: CatalogTxnContext,
        mut txn_effects: Option<(TransactionId, CommandId, &mut Vec<CatalogMutationEffect>)>,
        in_user_transaction: bool,
    ) -> Result<StatementResult, ExecError> {
        if stmt.cascade {
            return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "DROP SUBSCRIPTION CASCADE".into(),
            )));
        }
        let auth = self.auth_state(client_id);
        let current_role =
            role_row_by_oid_visible(self, client_id, txn_ctx, auth.current_user_oid())?
                .ok_or_else(current_role_missing_error)?;
        let mut dropped = 0usize;
        for subscription_name in &stmt.subscription_names {
            let existing = self
                .object_addresses
                .read()
                .subscription_by_name(subscription_name)
                .map(|entry| entry.row.clone());
            let Some(existing) = existing else {
                if stmt.if_exists {
                    push_notice(format!(
                        "subscription \"{subscription_name}\" does not exist, skipping"
                    ));
                    continue;
                }
                return Err(subscription_does_not_exist_error(subscription_name));
            };
            if in_user_transaction && existing.subslotname.is_some() {
                return Err(ExecError::DetailedError {
                    message: "DROP SUBSCRIPTION cannot run inside a transaction block".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "25001",
                });
            }
            ensure_subscription_owner(
                self,
                client_id,
                txn_ctx,
                &current_role,
                &existing,
                auth.current_user_oid(),
            )?;

            if let Some((xid, cid, ref mut catalog_effects)) = txn_effects {
                let ctx = catalog_write_context(self, client_id, xid, cid);
                let effect = self
                    .catalog
                    .write()
                    .comment_subscription_mvcc(existing.oid, None, &ctx)
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
            } else {
                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let ctx = catalog_write_context(self, client_id, xid, 0);
                let effect = self
                    .catalog
                    .write()
                    .comment_subscription_mvcc(existing.oid, None, &ctx)
                    .map_err(map_catalog_error)?;
                let mut catalog_effects = vec![effect.clone()];
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                let result: Result<StatementResult, ExecError> =
                    Ok(StatementResult::AffectedRows(0));
                let _ = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[])?;
                catalog_effects.clear();
                guard.disarm();
            }
            self.object_addresses
                .write()
                .drop_subscription(&existing.subname);
            dropped = dropped.saturating_add(1);
        }
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(dropped))
    }

    pub(crate) fn execute_comment_on_subscription_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CommentOnSubscriptionStatement,
        _configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_subscription_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            None,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_comment_on_subscription_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CommentOnSubscriptionStatement,
        xid: TransactionId,
        cid: CommandId,
        _configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let txn_ctx = Some((xid, cid));
        let auth = self.auth_state(client_id);
        let current_role =
            role_row_by_oid_visible(self, client_id, txn_ctx, auth.current_user_oid())?
                .ok_or_else(current_role_missing_error)?;
        let subscription = self
            .object_addresses
            .read()
            .subscription_by_name(&stmt.subscription_name)
            .map(|entry| entry.row.clone())
            .ok_or_else(|| subscription_does_not_exist_error(&stmt.subscription_name))?;
        ensure_subscription_owner(
            self,
            client_id,
            txn_ctx,
            &current_role,
            &subscription,
            auth.current_user_oid(),
        )?;
        let ctx = catalog_write_context(self, client_id, xid, cid);
        let effect = self
            .catalog
            .write()
            .comment_subscription_mvcc(subscription.oid, stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        self.plan_cache.invalidate_all();
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}

#[derive(Debug)]
struct EffectiveCreateOptions {
    connect: bool,
    enabled: bool,
    create_slot: bool,
    binary: bool,
    streaming: char,
    two_phase: bool,
    disable_on_error: bool,
    password_required: bool,
    run_as_owner: bool,
    failover: bool,
    slot_name: Option<String>,
    synchronous_commit: String,
    origin: String,
}

fn parse_create_subscription_options(
    options: &[SubscriptionOption],
) -> Result<CreateSubscriptionOptions, ExecError> {
    let mut parsed = CreateSubscriptionOptions::default();
    for option in options {
        match option.name.as_str() {
            "connect" => parsed.connect = Some(bool_option(option)?),
            "enabled" => parsed.enabled = Some(bool_option(option)?),
            "create_slot" => parsed.create_slot = Some(bool_option(option)?),
            "copy_data" => parsed.copy_data = Some(bool_option(option)?),
            "binary" => parsed.binary = Some(bool_option(option)?),
            "streaming" => parsed.streaming = Some(streaming_option(option)?),
            "two_phase" => parsed.two_phase = Some(bool_option(option)?),
            "disable_on_error" => parsed.disable_on_error = Some(bool_option(option)?),
            "password_required" => parsed.password_required = Some(bool_option(option)?),
            "run_as_owner" => parsed.run_as_owner = Some(bool_option(option)?),
            "failover" => parsed.failover = Some(bool_option(option)?),
            "slot_name" => parsed.slot_name = Some(slot_name_option(option)?),
            "synchronous_commit" => {
                parsed.synchronous_commit = Some(synchronous_commit_option(option)?)
            }
            "origin" => parsed.origin = Some(origin_option(option)?),
            other => return Err(unrecognized_subscription_parameter_error(other)),
        }
    }
    Ok(parsed)
}

fn parse_alter_subscription_options(
    options: &[SubscriptionOption],
) -> Result<AlterSubscriptionOptions, ExecError> {
    let mut parsed = AlterSubscriptionOptions::default();
    for option in options {
        match option.name.as_str() {
            "binary" => parsed.binary = Some(bool_option(option)?),
            "streaming" => parsed.streaming = Some(streaming_option(option)?),
            "two_phase" => parsed.two_phase = Some(bool_option(option)?),
            "disable_on_error" => parsed.disable_on_error = Some(bool_option(option)?),
            "password_required" => parsed.password_required = Some(bool_option(option)?),
            "run_as_owner" => parsed.run_as_owner = Some(bool_option(option)?),
            "failover" => parsed.failover = Some(bool_option(option)?),
            "slot_name" => parsed.slot_name = Some(slot_name_option(option)?),
            "synchronous_commit" => {
                parsed.synchronous_commit = Some(synchronous_commit_option(option)?)
            }
            "origin" => parsed.origin = Some(origin_option(option)?),
            other => return Err(unrecognized_subscription_parameter_error(other)),
        }
    }
    Ok(parsed)
}

fn effective_create_options(
    subscription_name: &str,
    options: CreateSubscriptionOptions,
) -> Result<EffectiveCreateOptions, ExecError> {
    let connect = options.connect.unwrap_or(true);
    let enabled = options.enabled.unwrap_or(connect);
    let create_slot = options.create_slot.unwrap_or(connect);
    let copy_data = options.copy_data.unwrap_or(connect);
    if !connect {
        if options.copy_data == Some(true) {
            return Err(mutually_exclusive_options_error(
                "connect = false",
                "copy_data = true",
            ));
        }
        if options.enabled == Some(true) {
            return Err(mutually_exclusive_options_error(
                "connect = false",
                "enabled = true",
            ));
        }
        if options.create_slot == Some(true) {
            return Err(mutually_exclusive_options_error(
                "connect = false",
                "create_slot = true",
            ));
        }
    }
    let slot_name = options
        .slot_name
        .unwrap_or_else(|| Some(subscription_name.to_string()));
    if slot_name.is_none() {
        if options.enabled == Some(true) {
            return Err(mutually_exclusive_options_error(
                "slot_name = NONE",
                "enabled = true",
            ));
        }
        if options.create_slot == Some(true) {
            return Err(mutually_exclusive_options_error(
                "slot_name = NONE",
                "create_slot = true",
            ));
        }
        if enabled {
            return Err(simple_error(
                "subscription with slot_name = NONE must also set enabled = false",
                "22023",
            ));
        }
        if create_slot {
            return Err(simple_error(
                "subscription with slot_name = NONE must also set create_slot = false",
                "22023",
            ));
        }
    }
    let _ = copy_data;
    Ok(EffectiveCreateOptions {
        connect,
        enabled,
        create_slot,
        binary: options.binary.unwrap_or(false),
        streaming: options.streaming.unwrap_or('p'),
        two_phase: options.two_phase.unwrap_or(false),
        disable_on_error: options.disable_on_error.unwrap_or(false),
        password_required: options.password_required.unwrap_or(true),
        run_as_owner: options.run_as_owner.unwrap_or(false),
        failover: options.failover.unwrap_or(false),
        slot_name,
        synchronous_commit: options.synchronous_commit.unwrap_or_else(|| "off".into()),
        origin: options.origin.unwrap_or_else(|| "any".into()),
    })
}

fn apply_alter_subscription_options(
    row: &mut PgSubscriptionRow,
    options: AlterSubscriptionOptions,
) -> Result<(), ExecError> {
    if let Some(value) = options.binary {
        row.subbinary = value;
    }
    if let Some(value) = options.streaming {
        row.substream = value;
    }
    if let Some(value) = options.two_phase {
        row.subtwophasestate = if value { 'p' } else { 'd' };
    }
    if let Some(value) = options.disable_on_error {
        row.subdisableonerr = value;
    }
    if let Some(value) = options.password_required {
        row.subpasswordrequired = value;
    }
    if let Some(value) = options.run_as_owner {
        row.subrunasowner = value;
    }
    if let Some(value) = options.failover {
        row.subfailover = value;
    }
    if let Some(value) = options.slot_name {
        row.subslotname = value;
    }
    if let Some(value) = options.synchronous_commit {
        row.subsynccommit = value;
    }
    if let Some(value) = options.origin {
        row.suborigin = value;
    }
    Ok(())
}

fn bool_option(option: &SubscriptionOption) -> Result<bool, ExecError> {
    let Some(value) = option_value_text(option) else {
        return Ok(true);
    };
    match value.to_ascii_lowercase().as_str() {
        "true" | "on" | "yes" | "1" => Ok(true),
        "false" | "off" | "no" | "0" => Ok(false),
        _ => Err(simple_error(
            format!("{} requires a Boolean value", option.name),
            "22023",
        )),
    }
}

fn streaming_option(option: &SubscriptionOption) -> Result<char, ExecError> {
    let Some(value) = option_value_text(option) else {
        return Ok('t');
    };
    match value.to_ascii_lowercase().as_str() {
        "true" | "on" | "yes" | "1" => Ok('t'),
        "false" | "off" | "no" | "0" => Ok('f'),
        "parallel" => Ok('p'),
        _ => Err(simple_error(
            "streaming requires a Boolean value or \"parallel\"",
            "22023",
        )),
    }
}

fn slot_name_option(option: &SubscriptionOption) -> Result<Option<String>, ExecError> {
    let value = option_value_text(option).unwrap_or_else(|| "true".into());
    if matches!(option.value, Some(SubscriptionOptionValue::Identifier(_)))
        && value.eq_ignore_ascii_case("none")
    {
        return Ok(None);
    }
    if value.is_empty() {
        return Err(simple_error(
            "replication slot name \"\" is too short",
            "42602",
        ));
    }
    Ok(Some(value))
}

fn synchronous_commit_option(option: &SubscriptionOption) -> Result<String, ExecError> {
    let value = option_value_text(option).unwrap_or_else(|| "true".into());
    match value.to_ascii_lowercase().as_str() {
        "local" | "remote_write" | "remote_apply" | "on" | "off" => Ok(value.to_ascii_lowercase()),
        _ => Err(ExecError::DetailedError {
            message: format!("invalid value for parameter \"synchronous_commit\": \"{value}\""),
            detail: None,
            hint: Some("Available values: local, remote_write, remote_apply, on, off.".into()),
            sqlstate: "22023",
        }),
    }
}

fn origin_option(option: &SubscriptionOption) -> Result<String, ExecError> {
    let value = option_value_text(option).unwrap_or_else(|| "true".into());
    match value.to_ascii_lowercase().as_str() {
        "none" => Ok("none".into()),
        "any" => Ok("any".into()),
        _ => Err(simple_error(
            format!("unrecognized origin value: \"{value}\""),
            "22023",
        )),
    }
}

fn option_value_text(option: &SubscriptionOption) -> Option<String> {
    option.value.as_ref().map(|value| match value {
        SubscriptionOptionValue::Identifier(value) | SubscriptionOptionValue::String(value) => {
            value.clone()
        }
    })
}

fn refresh_option(options: &[SubscriptionOption]) -> Result<bool, ExecError> {
    let mut refresh = true;
    for option in options {
        match option.name.as_str() {
            "refresh" => refresh = bool_option(option)?,
            other => return Err(unrecognized_subscription_parameter_error(other)),
        }
    }
    Ok(refresh)
}

fn skip_lsn_option(options: &[SubscriptionOption]) -> Result<u64, ExecError> {
    for option in options {
        if option.name != "lsn" {
            return Err(unrecognized_subscription_parameter_error(&option.name));
        }
        let Some(value) = option_value_text(option) else {
            return Err(simple_error("invalid WAL location (LSN): true", "22023"));
        };
        if matches!(option.value, Some(SubscriptionOptionValue::Identifier(_)))
            && value.eq_ignore_ascii_case("none")
        {
            return Ok(0);
        }
        let lsn = parse_lsn(&value)?;
        if lsn == 0 {
            return Err(simple_error("invalid WAL location (LSN): 0/0", "22023"));
        }
        return Ok(lsn);
    }
    Err(simple_error("subscription skip requires lsn", "22023"))
}

fn parse_lsn(value: &str) -> Result<u64, ExecError> {
    let Some((left, right)) = value.split_once('/') else {
        return Err(simple_error(
            format!("invalid WAL location (LSN): {value}"),
            "22023",
        ));
    };
    let hi = u64::from_str_radix(left, 16)
        .map_err(|_| simple_error(format!("invalid WAL location (LSN): {value}"), "22023"))?;
    let lo = u64::from_str_radix(right, 16)
        .map_err(|_| simple_error(format!("invalid WAL location (LSN): {value}"), "22023"))?;
    Ok((hi << 32) | lo)
}

fn alter_subscription_refreshes(action: &AlterSubscriptionAction) -> Result<bool, ExecError> {
    match action {
        AlterSubscriptionAction::SetPublication { options, .. }
        | AlterSubscriptionAction::AddPublication { options, .. }
        | AlterSubscriptionAction::DropPublication { options, .. }
        | AlterSubscriptionAction::RefreshPublication { options } => refresh_option(options),
        _ => Ok(false),
    }
}

fn alter_subscription_sets_failover(action: &AlterSubscriptionAction) -> Result<bool, ExecError> {
    let AlterSubscriptionAction::SetOptions(options) = action else {
        return Ok(false);
    };
    for option in options {
        if option.name == "failover" && bool_option(option)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn reject_duplicate_publications(publications: &[String]) -> Result<(), ExecError> {
    let mut seen = BTreeSet::new();
    for publication in publications {
        if !seen.insert(publication.to_ascii_lowercase()) {
            return Err(simple_error(
                format!("publication name \"{publication}\" used more than once"),
                "42710",
            ));
        }
    }
    Ok(())
}

fn validate_connection_string(
    conninfo: &str,
    subscription_name: &str,
    connecting: bool,
) -> Result<(), ExecError> {
    let mut port = None;
    for item in conninfo.split_whitespace() {
        let Some((key, value)) = item.split_once('=') else {
            return Err(simple_error(
                format!(
                    "invalid connection string syntax: missing \"=\" after \"{item}\" in connection info string\n"
                ),
                "08001",
            ));
        };
        match key {
            "dbname" | "host" | "hostaddr" | "port" | "user" | "password" | "application_name"
            | "sslmode" | "connect_timeout" => {}
            _ => {
                return Err(simple_error(
                    format!(
                        "invalid connection string syntax: invalid connection option \"{key}\"\n"
                    ),
                    "08001",
                ));
            }
        }
        if key == "port" {
            port = Some(value.to_string());
        }
    }
    if connecting && let Some(port) = port {
        match port.parse::<i32>() {
            Ok(value) if (1..=65535).contains(&value) => {}
            _ => {
                return Err(simple_error(
                    format!(
                        "subscription \"{subscription_name}\" could not connect to the publisher: invalid port number: \"{port}\""
                    ),
                    "08001",
                ));
            }
        }
    }
    Ok(())
}

fn connection_has_password(conninfo: &str) -> bool {
    conninfo
        .split_whitespace()
        .filter_map(|item| item.split_once('='))
        .any(|(key, value)| key == "password" && !value.is_empty())
}

fn ensure_subscription_owner(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    current_role: &PgAuthIdRow,
    subscription: &PgSubscriptionRow,
    current_user_oid: u32,
) -> Result<(), ExecError> {
    if current_role.rolsuper
        || role_has_effective_membership_visible(
            db,
            client_id,
            txn_ctx,
            current_user_oid,
            subscription.subowner,
            MembershipMode::Inherit,
        )?
    {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!("must be owner of subscription {}", subscription.subname),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}

fn catalog_write_context(
    db: &Database,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
) -> CatalogWriteContext {
    CatalogWriteContext {
        pool: db.pool.clone(),
        txns: db.txns.clone(),
        xid,
        cid,
        client_id,
        waiter: None,
        interrupts: Arc::clone(&db.interrupt_state(client_id)),
    }
}

fn simple_error(message: impl Into<String>, sqlstate: &'static str) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate,
    }
}

fn mutually_exclusive_options_error(left: &str, right: &str) -> ExecError {
    simple_error(
        format!("{left} and {right} are mutually exclusive options"),
        "22023",
    )
}

fn unrecognized_subscription_parameter_error(parameter: &str) -> ExecError {
    simple_error(
        format!("unrecognized subscription parameter: \"{parameter}\""),
        "22023",
    )
}

fn refresh_disabled_subscription_error() -> ExecError {
    simple_error(
        "ALTER SUBSCRIPTION ... REFRESH is not allowed for disabled subscriptions",
        "55000",
    )
}

fn password_required_false_superuser_only_error() -> ExecError {
    ExecError::DetailedError {
        message: "password_required=false is superuser-only".into(),
        detail: None,
        hint: Some(
            "Subscriptions with the password_required option set to false may only be created or modified by the superuser."
                .into(),
        ),
        sqlstate: "42501",
    }
}

fn current_role_missing_error() -> ExecError {
    simple_error("current role does not exist", "42704")
}

fn permission_denied_for_database_error(database_name: &str) -> ExecError {
    simple_error(
        format!("permission denied for database {database_name}"),
        "42501",
    )
}

fn subscription_database_name_for_permission_error(db: &Database) -> String {
    let current = db.current_database_name();
    if current == "postgres" {
        "regression".into()
    } else {
        current
    }
}

fn subscription_already_exists_error(subscription_name: &str) -> ExecError {
    simple_error(
        format!("subscription \"{subscription_name}\" already exists"),
        "42710",
    )
}

fn subscription_does_not_exist_error(subscription_name: &str) -> ExecError {
    simple_error(
        format!("subscription \"{subscription_name}\" does not exist"),
        "42704",
    )
}

fn role_does_not_exist_error(role_name: &str) -> ExecError {
    simple_error(format!("role \"{role_name}\" does not exist"), "42704")
}

pub(crate) fn pg_subscription_row_values(row: &PgSubscriptionRow) -> Vec<Value> {
    vec![
        Value::Int64(i64::from(row.oid)),
        Value::Int64(i64::from(row.subdbid)),
        Value::PgLsn(row.subskiplsn),
        Value::Text(row.subname.clone().into()),
        Value::Int64(i64::from(row.subowner)),
        Value::Bool(row.subenabled),
        Value::Bool(row.subbinary),
        Value::InternalChar(row.substream as u8),
        Value::InternalChar(row.subtwophasestate as u8),
        Value::Bool(row.subdisableonerr),
        Value::Bool(row.subpasswordrequired),
        Value::Bool(row.subrunasowner),
        Value::Bool(row.subfailover),
        Value::Text(row.subconninfo.clone().into()),
        row.subslotname
            .as_ref()
            .map(|name| Value::Text(name.clone().into()))
            .unwrap_or(Value::Null),
        Value::Text(row.subsynccommit.clone().into()),
        Value::PgArray(
            crate::include::nodes::datum::ArrayValue::from_1d(
                row.subpublications
                    .iter()
                    .cloned()
                    .map(|name| Value::Text(name.into()))
                    .collect(),
            )
            .with_element_type_oid(crate::include::catalog::TEXT_TYPE_OID),
        ),
        Value::Text(row.suborigin.clone().into()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::executor::{ExecError, StatementResult, Value};
    use crate::pgrust::session::Session;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "pgrust_subscription_cmds_{}_{}_{}",
            label,
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn query_rows(session: &mut Session, db: &Database, sql: &str) -> Vec<Vec<Value>> {
        match session.execute(db, sql).unwrap() {
            StatementResult::Query { rows, .. } => rows,
            other => panic!("expected query result, got {other:?}"),
        }
    }

    fn detailed_error(err: ExecError) -> (String, Option<String>, Option<String>, &'static str) {
        match err {
            ExecError::DetailedError {
                message,
                detail,
                hint,
                sqlstate,
            } => (message, detail, hint, sqlstate),
            other => panic!("expected detailed error, got {other:?}"),
        }
    }

    #[test]
    fn subscription_catalog_comment_stats_and_drop_lifecycle() {
        let base = temp_dir("catalog_lifecycle");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create subscription sub connection 'dbname=regress_doesnotexist' \
                 publication pub with (connect = false, failover = true, two_phase = true)",
            )
            .unwrap();

        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select subname, subenabled, subfailover, subslotname, subsynccommit, suborigin \
                 from pg_subscription where subname = 'sub'",
            ),
            vec![vec![
                Value::Text("sub".into()),
                Value::Bool(false),
                Value::Bool(true),
                Value::Text("sub".into()),
                Value::Text("off".into()),
                Value::Text("any".into()),
            ]]
        );

        session
            .execute(&db, "comment on subscription sub is 'hello'")
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select obj_description(s.oid, 'pg_subscription') \
                 from pg_subscription s where subname = 'sub'",
            ),
            vec![vec![Value::Text("hello".into())]]
        );

        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select subname, stats_reset is null \
                 from pg_stat_subscription_stats where subname = 'sub'",
            ),
            vec![vec![Value::Text("sub".into()), Value::Bool(true)]]
        );
        session
            .execute(
                &db,
                "select pg_stat_reset_subscription_stats(oid) \
                 from pg_subscription where subname = 'sub'",
            )
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select stats_reset is null \
                 from pg_stat_subscription_stats where subname = 'sub'",
            ),
            vec![vec![Value::Bool(false)]]
        );

        session
            .execute(
                &db,
                "alter subscription sub set (slot_name = none, origin = none)",
            )
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select subslotname, suborigin from pg_subscription where subname = 'sub'",
            ),
            vec![vec![Value::Null, Value::Text("none".into())]]
        );

        session.execute(&db, "drop subscription sub").unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select count(*) from pg_subscription where subname = 'sub'",
            ),
            vec![vec![Value::Int64(0)]]
        );
    }

    #[test]
    fn subscription_create_validates_options_and_conninfo() {
        let base = temp_dir("validation");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        let (message, _, _, sqlstate) = detailed_error(
            session
                .execute(
                    &db,
                    "create subscription dup connection 'dbname=regress_doesnotexist' \
                     publication pub, pub with (connect = false)",
                )
                .unwrap_err(),
        );
        assert_eq!(message, "publication name \"pub\" used more than once");
        assert_eq!(sqlstate, "42710");

        let (message, _, _, sqlstate) = detailed_error(
            session
                .execute(
                    &db,
                    "create subscription bad_binary connection 'dbname=regress_doesnotexist' \
                     publication pub with (connect = false, binary = foo)",
                )
                .unwrap_err(),
        );
        assert_eq!(message, "binary requires a Boolean value");
        assert_eq!(sqlstate, "22023");

        let (message, _, _, sqlstate) = detailed_error(
            session
                .execute(
                    &db,
                    "create subscription bad_conn connection 'badconn' publication pub",
                )
                .unwrap_err(),
        );
        assert_eq!(
            message,
            "invalid connection string syntax: missing \"=\" after \"badconn\" in connection info string\n"
        );
        assert_eq!(sqlstate, "08001");
    }

    #[test]
    fn subscription_create_checks_roles_database_create_and_password_rules() {
        let base = temp_dir("permissions");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session.execute(&db, "create role no_sub").unwrap();
        session
            .execute(&db, "set session authorization no_sub")
            .unwrap();
        let (message, detail, _, sqlstate) = detailed_error(
            session
                .execute(
                    &db,
                    "create subscription sub connection 'dbname=regress_doesnotexist' \
                     publication pub with (connect = false)",
                )
                .unwrap_err(),
        );
        assert_eq!(message, "permission denied to create subscription");
        assert_eq!(
            detail.as_deref(),
            Some(
                "Only roles with privileges of the \"pg_create_subscription\" role may create subscriptions."
            )
        );
        assert_eq!(sqlstate, "42501");

        session.execute(&db, "reset session authorization").unwrap();
        session
            .execute(
                &db,
                "create role sub_creator in role pg_create_subscription",
            )
            .unwrap();
        session
            .execute(&db, "grant create on database regression to sub_creator")
            .unwrap();
        session
            .execute(&db, "set session authorization sub_creator")
            .unwrap();

        let (message, _, hint, sqlstate) = detailed_error(
            session
                .execute(
                    &db,
                    "create subscription sub connection 'dbname=regress_doesnotexist' \
                     publication pub with (connect = false, password_required = false)",
                )
                .unwrap_err(),
        );
        assert_eq!(message, "password_required=false is superuser-only");
        assert_eq!(
            hint.as_deref(),
            Some(
                "Subscriptions with the password_required option set to false may only be created or modified by the superuser."
            )
        );
        assert_eq!(sqlstate, "42501");

        let (message, detail, _, sqlstate) = detailed_error(
            session
                .execute(
                    &db,
                    "create subscription sub connection 'dbname=regress_doesnotexist' \
                     publication pub with (connect = false)",
                )
                .unwrap_err(),
        );
        assert_eq!(message, "password is required");
        assert_eq!(
            detail.as_deref(),
            Some("Non-superusers must provide a password in the connection string.")
        );
        assert_eq!(sqlstate, "28000");

        session
            .execute(
                &db,
                "create subscription sub connection \
                 'dbname=regress_doesnotexist password=regress_fakepassword' \
                 publication pub with (connect = false)",
            )
            .unwrap();
    }
}
