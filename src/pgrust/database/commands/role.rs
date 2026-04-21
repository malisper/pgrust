use super::super::*;
use crate::backend::catalog::roles::find_role_by_name;
use crate::backend::commands::rolecmds::{
    build_alter_role_spec, build_create_role_spec, can_rename_role, grant_membership_authorized,
    membership_row, normalize_drop_role_names, parse_createrole_self_grant, role_management_error,
};
use crate::backend::parser::{
    AlterRoleAction, AlterRoleStatement, CommentOnRoleStatement, CreateRoleStatement,
    DropOwnedStatement, DropRoleStatement, ReassignOwnedStatement,
};
use std::collections::{BTreeMap, BTreeSet};

impl Database {
    pub(crate) fn execute_create_role_stmt(
        &self,
        client_id: ClientId,
        stmt: &CreateRoleStatement,
        createrole_self_grant: Option<&str>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_role_stmt_in_transaction(
            client_id,
            stmt,
            createrole_self_grant,
            xid,
            0,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_role_stmt_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &CreateRoleStatement,
        createrole_self_grant: Option<&str>,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_role_catalog_error)?;
        let spec = build_create_role_spec(stmt).map_err(ExecError::Parse)?;
        if !auth.can_create_role_with_attrs(&spec.attrs, &auth_catalog) {
            return Err(create_role_permission_error(
                &auth,
                &auth_catalog,
                &spec.attrs,
            ));
        }
        let interrupts = self.interrupt_state(client_id);
        let mut current_cid = cid;
        let create_ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: current_cid,
            client_id,
            waiter: None,
            interrupts: interrupts.clone(),
        };
        let (created, effect) = self
            .shared_catalog
            .write()
            .create_role_mvcc(&stmt.role_name, &spec.attrs, &create_ctx)
            .map_err(map_role_catalog_error)?;
        catalog_effects.push(effect);
        current_cid = current_cid.saturating_add(1);

        let grant_membership =
            |db: &Database,
             current_cid: &mut CommandId,
             membership: crate::backend::catalog::role_memberships::NewRoleMembership,
             catalog_effects: &mut Vec<CatalogMutationEffect>|
             -> Result<(), ExecError> {
                let ctx = CatalogWriteContext {
                    pool: db.pool.clone(),
                    txns: db.txns.clone(),
                    xid,
                    cid: *current_cid,
                    client_id,
                    waiter: None,
                    interrupts: interrupts.clone(),
                };
                let (_, effect) = db
                    .shared_catalog
                    .write()
                    .grant_role_membership_mvcc(&membership, &ctx)
                    .map_err(map_role_catalog_error)?;
                catalog_effects.push(effect);
                *current_cid = current_cid.saturating_add(1);
                Ok(())
            };

        let current_user_oid = auth.current_user_oid();
        if !auth_catalog
            .role_by_oid(current_user_oid)
            .is_some_and(|row| row.rolsuper)
        {
            grant_membership(
                self,
                &mut current_cid,
                membership_row(
                    created.oid,
                    current_user_oid,
                    crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                    true,
                    false,
                    false,
                ),
                catalog_effects,
            )?;

            if let Some(raw) = createrole_self_grant {
                if let Some(options) = parse_createrole_self_grant(raw).map_err(ExecError::Parse)? {
                    grant_membership(
                        self,
                        &mut current_cid,
                        membership_row(
                            created.oid,
                            current_user_oid,
                            current_user_oid,
                            false,
                            options.inherit,
                            options.set,
                        ),
                        catalog_effects,
                    )?;
                }
            }
        }

        for role_name in &spec.add_role_to {
            let live_catalog = self
                .auth_catalog(client_id, Some((xid, current_cid)))
                .map_err(map_role_catalog_error)?;
            let parent = grant_membership_authorized(&auth, &live_catalog, role_name)
                .map_err(ExecError::Parse)?;
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: current_cid,
                client_id,
                waiter: None,
                interrupts: interrupts.clone(),
            };
            let (_, effect) = self
                .shared_catalog
                .write()
                .grant_role_membership_mvcc(
                    &membership_row(
                        parent.oid,
                        created.oid,
                        current_user_oid,
                        false,
                        false,
                        true,
                    ),
                    &ctx,
                )
                .map_err(|err| {
                    map_named_role_membership_error(
                        err,
                        created.oid,
                        &created.rolname,
                        parent.oid,
                        &parent.rolname,
                    )
                })?;
            catalog_effects.push(effect);
            current_cid = current_cid.saturating_add(1);
        }
        for member_name in &spec.role_members {
            let live_catalog = self
                .auth_catalog(client_id, Some((xid, current_cid)))
                .map_err(map_role_catalog_error)?;
            let member = lookup_membership_member(&live_catalog, member_name)?;
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: current_cid,
                client_id,
                waiter: None,
                interrupts: interrupts.clone(),
            };
            let (_, effect) = self
                .shared_catalog
                .write()
                .grant_role_membership_mvcc(
                    &membership_row(
                        created.oid,
                        member.oid,
                        current_user_oid,
                        false,
                        false,
                        true,
                    ),
                    &ctx,
                )
                .map_err(|err| {
                    map_named_role_membership_error(
                        err,
                        member.oid,
                        &member.rolname,
                        created.oid,
                        &created.rolname,
                    )
                })?;
            catalog_effects.push(effect);
            current_cid = current_cid.saturating_add(1);
        }
        for member_name in &spec.admin_members {
            let live_catalog = self
                .auth_catalog(client_id, Some((xid, current_cid)))
                .map_err(map_role_catalog_error)?;
            let member = lookup_membership_member(&live_catalog, member_name)?;
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: current_cid,
                client_id,
                waiter: None,
                interrupts: interrupts.clone(),
            };
            let (_, effect) = self
                .shared_catalog
                .write()
                .grant_role_membership_mvcc(
                    &membership_row(created.oid, member.oid, current_user_oid, true, false, true),
                    &ctx,
                )
                .map_err(|err| {
                    map_named_role_membership_error(
                        err,
                        member.oid,
                        &member.rolname,
                        created.oid,
                        &created.rolname,
                    )
                })?;
            catalog_effects.push(effect);
            current_cid = current_cid.saturating_add(1);
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_role_stmt(
        &self,
        client_id: ClientId,
        stmt: &AlterRoleStatement,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_role_stmt_in_transaction(
            client_id,
            stmt,
            xid,
            0,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_role_stmt_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &AlterRoleStatement,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_role_catalog_error)?;
        let existing = find_role_by_name(auth_catalog.roles(), &stmt.role_name)
            .cloned()
            .ok_or_else(|| {
                ExecError::Parse(role_management_error(format!(
                    "role \"{}\" does not exist",
                    stmt.role_name
                )))
            })?;
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };

        let effect = match &stmt.action {
            AlterRoleAction::Rename { new_name } => {
                if !can_rename_role(&auth, existing.oid, &auth_catalog) {
                    return Err(rename_role_permission_error(&existing));
                }
                self.shared_catalog
                    .write()
                    .rename_role_mvcc(&stmt.role_name, new_name, &ctx)
                    .map_err(map_role_catalog_error)?
                    .1
            }
            AlterRoleAction::Options(_) => {
                let spec = build_alter_role_spec(stmt, &existing)
                    .map_err(ExecError::Parse)?
                    .unwrap();
                if !auth.can_alter_role_attrs(existing.oid, &spec.attrs, &auth_catalog) {
                    return Err(alter_role_permission_error(
                        &auth,
                        &auth_catalog,
                        &existing,
                        &spec.attrs,
                    ));
                }
                self.shared_catalog
                    .write()
                    .alter_role_attributes_mvcc(&stmt.role_name, &spec.attrs, &ctx)
                    .map_err(map_role_catalog_error)?
                    .1
            }
        };

        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_role_stmt(
        &self,
        client_id: ClientId,
        stmt: &CommentOnRoleStatement,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_role_stmt_in_transaction(
            client_id,
            stmt,
            xid,
            0,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_comment_on_role_stmt_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &CommentOnRoleStatement,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_role_catalog_error)?;
        let target = lookup_role(&auth_catalog, &stmt.role_name)?;
        let current = auth_catalog
            .role_by_oid(auth.current_user_oid())
            .ok_or_else(|| {
                ExecError::Parse(role_management_error("current role does not exist"))
            })?;
        if !current.rolsuper && !auth.has_admin_option(target.oid, &auth_catalog) {
            return Err(ExecError::DetailedError {
                message: "permission denied".into(),
                detail: Some(format!(
                    "The current user must have the ADMIN option on role \"{}\".",
                    target.rolname
                )),
                hint: None,
                sqlstate: "42501",
            });
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .comment_role_mvcc(target.oid, stmt.comment.as_deref(), &ctx)
            .map_err(map_role_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_role_stmt(
        &self,
        client_id: ClientId,
        stmt: &DropRoleStatement,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_role_stmt_in_transaction(
            client_id,
            stmt,
            xid,
            0,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_drop_role_stmt_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &DropRoleStatement,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let interrupts = self.interrupt_state(client_id);
        let mut current_cid = cid;

        for role_name in normalize_drop_role_names(stmt) {
            let auth_catalog = self
                .auth_catalog(client_id, Some((xid, current_cid)))
                .map_err(map_role_catalog_error)?;
            let Some(existing) = find_role_by_name(auth_catalog.roles(), &role_name).cloned()
            else {
                if stmt.if_exists {
                    continue;
                }
                return Err(ExecError::Parse(role_management_error(format!(
                    "role \"{role_name}\" does not exist"
                ))));
            };
            if existing.oid == auth.current_user_oid() {
                return Err(ExecError::Parse(role_management_error(
                    "current user cannot be dropped",
                )));
            }
            if !auth.can_drop_role(existing.oid, &auth_catalog) {
                return Err(drop_role_permission_error(&existing));
            }
            let owned_objects = owned_objects_for_roles(
                self,
                client_id,
                Some((xid, current_cid)),
                &[existing.oid],
            )?;
            if !owned_objects.is_empty() {
                let detail = owned_objects
                    .iter()
                    .filter(|object| object.kind != OwnedObjectKind::Index)
                    .map(OwnedObject::drop_detail)
                    .collect::<Vec<_>>()
                    .join("\n");
                return Err(ExecError::DetailedError {
                    message: format!(
                        "role \"{}\" cannot be dropped because some objects depend on it",
                        existing.rolname
                    ),
                    detail: (!detail.is_empty()).then_some(detail),
                    hint: None,
                    sqlstate: "2BP01",
                });
            }
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: current_cid,
                client_id,
                waiter: None,
                interrupts: interrupts.clone(),
            };
            let (_, effect) = self
                .shared_catalog
                .write()
                .drop_role_mvcc(&role_name, &ctx)
                .map_err(map_role_catalog_error)?;
            catalog_effects.push(effect);
            current_cid = current_cid.saturating_add(1);
        }

        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_reassign_owned_stmt(
        &self,
        client_id: ClientId,
        stmt: &ReassignOwnedStatement,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_reassign_owned_stmt_in_transaction(
            client_id,
            stmt,
            xid,
            0,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_drop_owned_stmt(
        &self,
        client_id: ClientId,
        stmt: &DropOwnedStatement,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_owned_stmt_in_transaction(
            client_id,
            stmt,
            xid,
            0,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_drop_owned_stmt_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &DropOwnedStatement,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .txn_auth_catalog(client_id, xid, cid)
            .map_err(map_role_catalog_error)?;
        let roles = stmt
            .role_names
            .iter()
            .map(|role_name| lookup_role(&auth_catalog, role_name))
            .collect::<Result<Vec<_>, _>>()?;
        for role in &roles {
            if !auth.has_effective_membership(role.oid, &auth_catalog) {
                return Err(ExecError::DetailedError {
                    message: "permission denied to drop objects".into(),
                    detail: Some(format!(
                        "Only roles with privileges of role \"{}\" may drop objects owned by it.",
                        role.rolname
                    )),
                    hint: None,
                    sqlstate: "42501",
                });
            }
        }

        let role_oids = roles.iter().map(|role| role.oid).collect::<BTreeSet<_>>();
        let role_oid_list = role_oids.iter().copied().collect::<Vec<_>>();
        let mut owned_objects =
            owned_objects_for_roles(self, client_id, Some((xid, cid)), &role_oid_list)?;
        owned_objects.sort_by(|left, right| {
            owned_object_drop_priority(left.relkind)
                .cmp(&owned_object_drop_priority(right.relkind))
                .then(left.name.cmp(&right.name))
                .then(left.relkind.cmp(&right.relkind))
        });

        let interrupts = self.interrupt_state(client_id);
        let mut current_cid = cid;
        for object in owned_objects {
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: current_cid,
                client_id,
                waiter: Some(self.txn_waiter.clone()),
                interrupts: interrupts.clone(),
            };
            let effect = match object.relkind {
                'v' => self
                    .catalog
                    .write()
                    .drop_view_by_oid_mvcc(object.relation_oid, &ctx)
                    .map(|(_, effect)| effect),
                'i' => self
                    .catalog
                    .write()
                    .drop_relation_entry_by_oid_mvcc(object.relation_oid, &ctx)
                    .map(|(_, effect)| effect),
                _ => self
                    .catalog
                    .write()
                    .drop_relation_by_oid_mvcc(object.relation_oid, &ctx)
                    .map(|(_, effect)| effect),
            }
            .map_err(map_role_catalog_error)?;
            if object.relkind != 'v' {
                self.apply_catalog_mutation_effect_immediate(&effect)?;
            }
            if object.relkind == 'r' {
                self.session_stats_state(client_id)
                    .write()
                    .note_relation_drop(object.relation_oid, &self.stats);
            }
            catalog_effects.push(effect);
            current_cid = current_cid.saturating_add(1);
        }

        let auth_catalog = self
            .txn_auth_catalog(client_id, xid, current_cid)
            .map_err(map_role_catalog_error)?;
        let mut owned_memberships = auth_catalog
            .memberships()
            .iter()
            .filter(|row| {
                role_oids.contains(&row.roleid)
                    || role_oids.contains(&row.member)
                    || role_oids.contains(&row.grantor)
            })
            .cloned()
            .collect::<Vec<_>>();
        owned_memberships.sort_by_key(|row| (row.roleid, row.member, row.grantor));
        for membership in owned_memberships {
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: current_cid,
                client_id,
                waiter: None,
                interrupts: interrupts.clone(),
            };
            let (_, effect) = self
                .shared_catalog
                .write()
                .revoke_role_membership_mvcc(
                    membership.roleid,
                    membership.member,
                    membership.grantor,
                    &ctx,
                )
                .map_err(map_role_catalog_error)?;
            catalog_effects.push(effect);
            current_cid = current_cid.saturating_add(1);
        }

        // :HACK: DROP OWNED currently covers pgrust's tracked shared-role dependencies
        // (relation ownership, pg_auth_members rows, and database CREATE grants). Full
        // PostgreSQL-style shared dependency traversal should replace this with a single
        // dependency-driven path that also handles schemas, functions, and ACL entries.
        let _ = stmt.cascade;
        self.database_create_grants.write().retain(|grant| {
            !role_oids.contains(&grant.grantee_oid) && !role_oids.contains(&grant.grantor_oid)
        });

        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_reassign_owned_stmt_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &ReassignOwnedStatement,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .txn_auth_catalog(client_id, xid, cid)
            .map_err(map_role_catalog_error)?;
        let old_roles = stmt
            .old_roles
            .iter()
            .map(|role_name| lookup_role(&auth_catalog, role_name))
            .collect::<Result<Vec<_>, _>>()?;
        for role in &old_roles {
            if !auth.has_effective_membership(role.oid, &auth_catalog) {
                return Err(ExecError::DetailedError {
                    message: "permission denied to reassign objects".into(),
                    detail: Some(format!(
                        "Only roles with privileges of role \"{}\" may reassign objects owned by it.",
                        role.rolname
                    )),
                    hint: None,
                    sqlstate: "42501",
                });
            }
        }
        let new_role = lookup_role(&auth_catalog, &stmt.new_role)?;
        ensure_can_set_role(self, client_id, new_role.oid, &new_role.rolname)?;

        let old_role_oids = old_roles
            .iter()
            .map(|role| role.oid)
            .collect::<BTreeSet<_>>();
        let owned_objects = owned_objects_for_roles(
            self,
            client_id,
            Some((xid, cid)),
            &old_role_oids.into_iter().collect::<Vec<_>>(),
        )?;
        if owned_objects.is_empty() {
            return Ok(StatementResult::AffectedRows(0));
        }

        let interrupts = self.interrupt_state(client_id);
        for (offset, object) in owned_objects.iter().enumerate() {
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: cid.saturating_add(offset as u32),
                client_id,
                waiter: None,
                interrupts: Arc::clone(&interrupts),
            };
            let effect = match object.kind {
                OwnedObjectKind::Publication => {
                    let catcache = self
                        .txn_backend_catcache(client_id, xid, cid.saturating_add(offset as u32))
                        .map_err(map_role_catalog_error)?;
                    let publication = catcache
                        .publication_rows()
                        .into_iter()
                        .find(|row| row.oid == object.oid)
                        .ok_or_else(|| {
                            ExecError::Parse(role_management_error(format!(
                                "publication \"{}\" does not exist",
                                object.name
                            )))
                        })?;
                    self.catalog
                        .write()
                        .replace_publication_row_mvcc(
                            crate::include::catalog::PgPublicationRow {
                                pubowner: new_role.oid,
                                ..publication
                            },
                            &ctx,
                        )
                        .map_err(map_role_catalog_error)?
                }
                OwnedObjectKind::Index | OwnedObjectKind::Table | OwnedObjectKind::View => self
                    .catalog
                    .write()
                    .alter_relation_owner_mvcc(object.oid, new_role.oid, &ctx)
                    .map_err(map_role_catalog_error)?,
            };
            catalog_effects.push(effect);
        }

        Ok(StatementResult::AffectedRows(0))
    }
}

fn map_role_catalog_error(err: crate::backend::catalog::CatalogError) -> ExecError {
    match err {
        crate::backend::catalog::CatalogError::UniqueViolation(message) => {
            ExecError::Parse(role_management_error(message))
        }
        crate::backend::catalog::CatalogError::UnknownTable(name) => ExecError::Parse(
            role_management_error(format!("role \"{name}\" does not exist")),
        ),
        other => ExecError::Parse(role_management_error(format!("{other:?}"))),
    }
}

fn map_named_role_membership_error(
    err: crate::backend::catalog::CatalogError,
    member_oid: u32,
    member_name: &str,
    role_oid: u32,
    role_name: &str,
) -> ExecError {
    match err {
        crate::backend::catalog::CatalogError::UniqueViolation(message)
            if message == format!("role membership cycle: {member_oid} -> {role_oid}") =>
        {
            ExecError::Parse(role_management_error(format!(
                "role \"{member_name}\" is a member of role \"{role_name}\""
            )))
        }
        other => map_role_catalog_error(other),
    }
}

fn lookup_membership_member(
    catalog: &crate::pgrust::auth::AuthCatalog,
    role_name: &str,
) -> Result<crate::include::catalog::PgAuthIdRow, ExecError> {
    let role = find_role_by_name(catalog.roles(), role_name)
        .cloned()
        .ok_or_else(|| {
            ExecError::Parse(role_management_error(format!(
                "role \"{role_name}\" does not exist"
            )))
        })?;
    if role.oid == crate::include::catalog::PG_DATABASE_OWNER_OID {
        return Err(ExecError::Parse(role_management_error(format!(
            "role \"{}\" cannot be a member of any role",
            role.rolname
        ))));
    }
    Ok(role)
}

fn lookup_role(
    catalog: &crate::pgrust::auth::AuthCatalog,
    role_name: &str,
) -> Result<crate::include::catalog::PgAuthIdRow, ExecError> {
    find_role_by_name(catalog.roles(), role_name)
        .cloned()
        .ok_or_else(|| {
            ExecError::Parse(role_management_error(format!(
                "role \"{role_name}\" does not exist"
            )))
        })
}

fn create_role_permission_error(
    auth: &crate::pgrust::auth::AuthState,
    catalog: &crate::pgrust::auth::AuthCatalog,
    attrs: &crate::backend::catalog::roles::RoleAttributes,
) -> ExecError {
    let detail = catalog
        .role_by_oid(auth.current_user_oid())
        .map(|current| {
            if attrs.rolsuper && !current.rolsuper {
                Some("Only roles with the SUPERUSER attribute may create roles with the SUPERUSER attribute.".to_string())
            } else if attrs.rolreplication && !current.rolreplication {
                Some("Only roles with the REPLICATION attribute may create roles with the REPLICATION attribute.".to_string())
            } else if attrs.rolbypassrls && !current.rolbypassrls {
                Some("Only roles with the BYPASSRLS attribute may create roles with the BYPASSRLS attribute.".to_string())
            } else if attrs.rolcreatedb && !current.rolcreatedb {
                Some("Only roles with the CREATEDB attribute may create roles with the CREATEDB attribute.".to_string())
            } else {
                None
            }
        })
        .flatten();
    ExecError::DetailedError {
        message: "permission denied to create role".into(),
        detail,
        hint: None,
        sqlstate: "42501",
    }
}

fn alter_role_permission_error(
    auth: &crate::pgrust::auth::AuthState,
    catalog: &crate::pgrust::auth::AuthCatalog,
    existing: &crate::include::catalog::PgAuthIdRow,
    attrs: &crate::backend::catalog::roles::RoleAttributes,
) -> ExecError {
    let detail = catalog
        .role_by_oid(auth.current_user_oid())
        .map(|current| {
            if attrs.rolsuper != existing.rolsuper && !current.rolsuper {
                Some("Only roles with the SUPERUSER attribute may change the SUPERUSER attribute.".to_string())
            } else if attrs.rolreplication != existing.rolreplication && !current.rolreplication {
                Some("Only roles with the REPLICATION attribute may change the REPLICATION attribute.".to_string())
            } else if attrs.rolbypassrls != existing.rolbypassrls && !current.rolbypassrls {
                Some("Only roles with the BYPASSRLS attribute may change the BYPASSRLS attribute.".to_string())
            } else if attrs.rolcreatedb != existing.rolcreatedb && !current.rolcreatedb {
                Some("Only roles with the CREATEDB attribute may change the CREATEDB attribute.".to_string())
            } else {
                None
            }
        })
        .flatten();
    ExecError::DetailedError {
        message: "permission denied to alter role".into(),
        detail,
        hint: None,
        sqlstate: "42501",
    }
}

fn rename_role_permission_error(existing: &crate::include::catalog::PgAuthIdRow) -> ExecError {
    ExecError::DetailedError {
        message: "permission denied to rename role".into(),
        detail: Some(format!(
            "Only roles with the CREATEROLE attribute and the ADMIN option on role \"{}\" may rename this role.",
            existing.rolname
        )),
        hint: None,
        sqlstate: "42501",
    }
}

fn drop_role_permission_error(existing: &crate::include::catalog::PgAuthIdRow) -> ExecError {
    let detail = if existing.rolsuper {
        "Only roles with the SUPERUSER attribute may drop roles with the SUPERUSER attribute."
            .to_string()
    } else {
        format!(
            "Only roles with the CREATEROLE attribute and the ADMIN option on role \"{}\" may drop this role.",
            existing.rolname
        )
    };
    ExecError::DetailedError {
        message: "permission denied to drop role".into(),
        detail: Some(detail),
        hint: None,
        sqlstate: "42501",
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OwnedObject {
    oid: u32,
    kind: OwnedObjectKind,
    name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum OwnedObjectKind {
    Index,
    Publication,
    Table,
    View,
}

impl OwnedObject {
    fn kind_name(&self) -> &'static str {
        match self.kind {
            OwnedObjectKind::Index => "index",
            OwnedObjectKind::Publication => "publication",
            OwnedObjectKind::Table => "table",
            OwnedObjectKind::View => "view",
        }
    }

    fn drop_detail(&self) -> String {
        format!("owner of {} {}", self.kind_name(), self.name)
    }
}

fn owned_objects_for_roles(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    role_oids: &[u32],
) -> Result<Vec<OwnedObject>, ExecError> {
    let role_oids = role_oids.iter().copied().collect::<BTreeSet<_>>();
    let catcache = match txn_ctx {
        Some((xid, cid)) => db.txn_backend_catcache(client_id, xid, cid),
        None => db.backend_catcache(client_id, None),
    }
    .map_err(map_role_catalog_error)?;
    let namespaces = catcache
        .namespace_rows()
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let mut objects = catcache
        .class_rows()
        .into_iter()
        .filter(|row| role_oids.contains(&row.relowner))
        .filter(|row| row.relpersistence != 't')
        .filter(|row| matches!(row.relkind, 'r' | 'v' | 'i' | 'S'))
        .map(|row| OwnedObject {
            oid: row.oid,
            kind: match row.relkind {
                'i' => OwnedObjectKind::Index,
                'v' => OwnedObjectKind::View,
                _ => OwnedObjectKind::Table,
            },
            name: match namespaces.get(&row.relnamespace).map(String::as_str) {
                Some("public") | Some("pg_catalog") | None => row.relname,
                Some(schema) => format!("{schema}.{}", row.relname),
            },
        })
        .collect::<Vec<_>>();
    objects.extend(
        catcache
            .publication_rows()
            .into_iter()
            .filter(|row| role_oids.contains(&row.pubowner))
            .map(|row| OwnedObject {
                oid: row.oid,
                kind: OwnedObjectKind::Publication,
                name: row.pubname,
            }),
    );
    objects.sort_by(|left, right| left.name.cmp(&right.name).then(left.kind.cmp(&right.kind)));
    Ok(objects)
}

fn owned_object_drop_priority(relkind: char) -> u8 {
    match relkind {
        'v' => 0,
        'i' => 1,
        _ => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::role_memberships::memberships_for_member;
    use crate::backend::executor::StatementResult;
    use crate::backend::executor::Value;
    use crate::include::catalog::PgAuthIdRow;
    use crate::pgrust::session::Session;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "pgrust_rolecmds_{}_{}_{}",
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

    fn role_row(db: &Database, role_name: &str) -> PgAuthIdRow {
        db.backend_catcache(1, None)
            .unwrap()
            .authid_rows()
            .into_iter()
            .find(|row| row.rolname == role_name)
            .unwrap()
    }

    fn query_rows(db: &Database, client_id: ClientId, sql: &str) -> Vec<Vec<Value>> {
        match db.execute(client_id, sql).unwrap() {
            StatementResult::Query { rows, .. } => rows,
            other => panic!("expected query result, got {other:?}"),
        }
    }

    fn relation_owner_name(db: &Database, relname: &str) -> String {
        let catcache = db.backend_catcache(1, None).unwrap();
        let owner_oid = catcache
            .class_rows()
            .into_iter()
            .find(|row| row.relname == relname)
            .map(|row| row.relowner)
            .unwrap();
        catcache
            .authid_rows()
            .into_iter()
            .find(|row| row.oid == owner_oid)
            .map(|row| row.rolname)
            .unwrap()
    }

    fn schema_owner_name(db: &Database, schema_name: &str) -> String {
        let catcache = db.backend_catcache(1, None).unwrap();
        let owner_oid = catcache
            .namespace_by_name(schema_name)
            .map(|row| row.nspowner)
            .unwrap();
        catcache
            .authid_rows()
            .into_iter()
            .find(|row| row.oid == owner_oid)
            .map(|row| row.rolname)
            .unwrap()
    }

    fn relation_oid(db: &Database, relname: &str) -> u32 {
        db.backend_catcache(1, None)
            .unwrap()
            .class_rows()
            .into_iter()
            .find(|row| row.relname == relname)
            .map(|row| row.oid)
            .unwrap()
    }

    fn relation_exists(db: &Database, relname: &str) -> bool {
        db.backend_catcache(1, None)
            .unwrap()
            .class_rows()
            .into_iter()
            .any(|row| row.relname == relname)
    }

    fn set_relation_owner(db: &Database, relation_name: &str, owner_role: &str) {
        let xid = db.txns.write().begin();
        let relation_oid = relation_oid(db, relation_name);
        let owner_oid = role_oid(db, owner_role);
        let ctx = CatalogWriteContext {
            pool: db.pool.clone(),
            txns: db.txns.clone(),
            xid,
            cid: 0,
            client_id: 1,
            waiter: None,
            interrupts: db.interrupt_state(1),
        };
        let effect = db
            .catalog
            .write()
            .alter_relation_owner_mvcc(relation_oid, owner_oid, &ctx)
            .unwrap();
        db.finish_txn(
            1,
            xid,
            Ok(StatementResult::AffectedRows(0)),
            &[effect],
            &[],
            &[],
        )
        .unwrap();
    }

    fn update_membership_options(
        db: &Database,
        role_name: &str,
        member_name: &str,
        _grantor_name: &str,
        inherit_option: bool,
        set_option: bool,
    ) {
        let mut session = Session::new(99);
        session
            .execute(
                db,
                &format!(
                    "grant {role_name} to {member_name} with inherit {inherit_option}, set {set_option}"
                ),
            )
            .unwrap();
    }

    #[test]
    fn create_alter_drop_role_commands_work() {
        let base = temp_dir("create_alter_drop");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        assert_eq!(
            session
                .execute(&db, "create role app_user createdb login")
                .unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert!(role_row(&db, "app_user").rolcreatedb);
        assert!(role_row(&db, "app_user").rolcanlogin);

        assert_eq!(
            session
                .execute(&db, "alter role app_user nocreatedb connection limit 5")
                .unwrap(),
            StatementResult::AffectedRows(0)
        );
        let altered = role_row(&db, "app_user");
        assert!(!altered.rolcreatedb);
        assert_eq!(altered.rolconnlimit, 5);

        assert_eq!(
            session
                .execute(&db, "alter role app_user rename to app_owner")
                .unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert_eq!(role_row(&db, "app_owner").rolname, "app_owner");

        assert_eq!(
            session.execute(&db, "drop role app_owner").unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert!(
            db.shared_catalog
                .read()
                .catcache()
                .unwrap()
                .authid_rows()
                .into_iter()
                .all(|row| row.rolname != "app_owner")
        );
    }

    #[test]
    fn create_role_restricted_attrs_require_matching_privileges() {
        let base = temp_dir("restricted_attrs");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser
            .execute(&db, "create role limited_admin createrole")
            .unwrap();

        let mut limited = Session::new(2);
        limited.set_session_authorization_oid(role_oid(&db, "limited_admin"));
        let err = limited
            .execute(&db, "create role forbidden createdb")
            .unwrap_err();
        assert!(format!("{err:?}").contains("permission denied to create role"));
        assert!(format!("{err:?}").contains("CREATEDB attribute"));
    }

    #[test]
    fn comment_on_role_updates_pg_description() {
        let base = temp_dir("comment_role");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(&db, "create role app_role createrole")
            .unwrap();
        session
            .execute(&db, "comment on role app_role is 'hello role'")
            .unwrap();
        let app_role_oid = role_oid(&db, "app_role");
        assert_eq!(
            query_rows(
                &db,
                1,
                &format!(
                    "select description from pg_description where objoid = {app_role_oid} and classoid = 1260 and objsubid = 0"
                )
            ),
            vec![vec![Value::Text("hello role".into())]]
        );

        session
            .execute(&db, "comment on role app_role is null")
            .unwrap();
        assert_eq!(
            query_rows(
                &db,
                1,
                &format!(
                    "select count(*) from pg_description where objoid = {app_role_oid} and classoid = 1260 and objsubid = 0"
                )
            ),
            vec![vec![Value::Int64(0)]]
        );
    }

    #[test]
    fn alter_schema_owner_changes_namespace_owner() {
        let base = temp_dir("alter_schema_owner");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session.execute(&db, "create role tenant").unwrap();
        session.execute(&db, "create schema tenant").unwrap();
        assert_eq!(schema_owner_name(&db, "tenant"), "postgres");

        session
            .execute(&db, "alter schema tenant owner to tenant")
            .unwrap();
        assert_eq!(schema_owner_name(&db, "tenant"), "tenant");
    }

    #[test]
    fn create_user_implies_login_and_drop_blocks_current_user() {
        let base = temp_dir("create_user");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create user app_login").unwrap();
        assert!(role_row(&db, "app_login").rolcanlogin);

        let mut app_user = Session::new(2);
        app_user.set_session_authorization_oid(role_oid(&db, "app_login"));
        let err = app_user.execute(&db, "drop role app_login").unwrap_err();
        assert!(format!("{err:?}").contains("current user cannot be dropped"));
    }

    #[test]
    fn create_group_legacy_syntax_creates_memberships() {
        let base = temp_dir("create_group_legacy");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role admin_member").unwrap();
        session.execute(&db, "create role regular_member").unwrap();

        session
            .execute(
                &db,
                "create group app_group with admin admin_member user regular_member",
            )
            .unwrap();

        assert!(!role_row(&db, "app_group").rolcanlogin);
        let app_group_oid = role_oid(&db, "app_group");
        let admin_member_oid = role_oid(&db, "admin_member");
        let regular_member_oid = role_oid(&db, "regular_member");
        let grants = db
            .shared_catalog
            .read()
            .catcache()
            .unwrap()
            .auth_members_rows();
        assert!(grants.iter().any(|row| {
            row.roleid == app_group_oid && row.member == admin_member_oid && row.admin_option
        }));
        assert!(grants.iter().any(|row| {
            row.roleid == app_group_oid && row.member == regular_member_oid && !row.admin_option
        }));
    }

    #[test]
    fn alter_group_legacy_syntax_manages_memberships() {
        let base = temp_dir("alter_group_legacy");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create group app_group").unwrap();
        session.execute(&db, "create role app_member").unwrap();

        session
            .execute(&db, "alter group app_group add user app_member")
            .unwrap();
        let app_group_oid = role_oid(&db, "app_group");
        let app_member_oid = role_oid(&db, "app_member");
        assert!(
            db.shared_catalog
                .read()
                .catcache()
                .unwrap()
                .auth_members_rows()
                .into_iter()
                .any(|row| row.roleid == app_group_oid && row.member == app_member_oid)
        );

        session
            .execute(&db, "alter group app_group drop user app_member")
            .unwrap();
        assert!(
            !db.shared_catalog
                .read()
                .catcache()
                .unwrap()
                .auth_members_rows()
                .into_iter()
                .any(|row| row.roleid == app_group_oid && row.member == app_member_oid)
        );
    }

    #[test]
    fn create_role_membership_clauses_persist_memberships() {
        let base = temp_dir("membership_clauses");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role member_role").unwrap();
        session.execute(&db, "create role admin_role").unwrap();

        session
            .execute(
                &db,
                "create role child in role parent role member_role admin admin_role",
            )
            .unwrap();

        let catcache = db.shared_catalog.read().catcache().unwrap();
        let child_oid = role_oid(&db, "child");
        let parent_oid = role_oid(&db, "parent");
        let member_oid = role_oid(&db, "member_role");
        let admin_oid = role_oid(&db, "admin_role");

        assert!(catcache.auth_members_rows().into_iter().any(|row| {
            row.roleid == parent_oid
                && row.member == child_oid
                && !row.admin_option
                && !row.inherit_option
                && row.set_option
        }));
        let child_members = memberships_for_member(&catcache.auth_members_rows(), member_oid);
        assert!(
            child_members
                .iter()
                .any(|row| row.roleid == child_oid && !row.admin_option)
        );
        let admin_members = memberships_for_member(&catcache.auth_members_rows(), admin_oid);
        assert!(
            admin_members
                .iter()
                .any(|row| row.roleid == child_oid && row.admin_option)
        );
    }

    #[test]
    fn create_role_self_grant_guc_adds_inherit_and_set_membership() {
        let base = temp_dir("self_grant");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser
            .execute(&db, "create role limited_admin createrole")
            .unwrap();

        let mut limited = Session::new(2);
        limited.set_session_authorization_oid(role_oid(&db, "limited_admin"));
        limited
            .execute(&db, "set createrole_self_grant to 'set, inherit'")
            .unwrap();
        limited.execute(&db, "create role tenant").unwrap();

        let catcache = db.shared_catalog.read().catcache().unwrap();
        let tenant_oid = role_oid(&db, "tenant");
        let limited_oid = role_oid(&db, "limited_admin");
        let grants = memberships_for_member(&catcache.auth_members_rows(), limited_oid);
        assert!(grants.iter().any(|row| {
            row.roleid == tenant_oid
                && row.grantor == crate::include::catalog::BOOTSTRAP_SUPERUSER_OID
                && row.admin_option
                && !row.inherit_option
                && !row.set_option
        }));
        assert!(grants.iter().any(|row| {
            row.roleid == tenant_oid
                && row.grantor == limited_oid
                && !row.admin_option
                && row.inherit_option
                && row.set_option
        }));
    }

    #[test]
    fn create_role_in_role_requires_admin_on_target_role() {
        let base = temp_dir("in_role_admin");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser
            .execute(&db, "create role limited_admin createrole")
            .unwrap();
        superuser.execute(&db, "create role parent").unwrap();

        let mut limited = Session::new(2);
        limited.set_session_authorization_oid(role_oid(&db, "limited_admin"));
        let err = limited
            .execute(&db, "create role child in role parent")
            .unwrap_err();
        assert!(format!("{err:?}").contains("permission denied"));
    }

    #[test]
    fn owned_relations_use_session_owner_and_pg_views_reflect_it() {
        let base = temp_dir("owned_relations");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create role tenant").unwrap();

        let mut tenant = Session::new(2);
        tenant.set_session_authorization_oid(role_oid(&db, "tenant"));
        tenant
            .execute(&db, "create table tenant_table (id int4)")
            .unwrap();
        tenant
            .execute(&db, "create index tenant_idx on tenant_table (id)")
            .unwrap();
        tenant
            .execute(
                &db,
                "create view tenant_view as select id from tenant_table",
            )
            .unwrap();

        assert_eq!(relation_owner_name(&db, "tenant_table"), "tenant");
        assert_eq!(relation_owner_name(&db, "tenant_idx"), "tenant");
        assert_eq!(
            query_rows(
                &db,
                1,
                "select viewowner from pg_views where viewname = 'tenant_view'"
            ),
            vec![vec![Value::Text("tenant".into())]]
        );
    }

    #[test]
    fn alter_relation_owner_requires_owner_membership_and_set_role() {
        let base = temp_dir("alter_owner");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser
            .execute(&db, "create role limited_admin createrole noinherit")
            .unwrap();
        superuser.execute(&db, "create role tenant").unwrap();
        superuser.execute(&db, "create role target").unwrap();
        superuser.execute(&db, "create role target2").unwrap();

        let limited_oid = role_oid(&db, "limited_admin");
        let tenant_oid = role_oid(&db, "tenant");

        let mut tenant = Session::new(2);
        tenant.set_session_authorization_oid(tenant_oid);
        tenant
            .execute(&db, "create table tenant_table (id int4)")
            .unwrap();
        tenant
            .execute(
                &db,
                "create view tenant_view as select id from tenant_table",
            )
            .unwrap();

        for role_name in ["tenant", "target", "target2"] {
            superuser
                .execute(
                    &db,
                    &format!("grant {role_name} to limited_admin with inherit true, set true"),
                )
                .unwrap();
        }

        let mut limited = Session::new(3);
        limited.set_session_authorization_oid(limited_oid);
        limited
            .execute(&db, "alter table tenant_table owner to target")
            .unwrap();
        limited
            .execute(&db, "alter view tenant_view owner to target")
            .unwrap();

        assert_eq!(relation_owner_name(&db, "tenant_table"), "target");
        assert_eq!(
            query_rows(
                &db,
                1,
                "select viewowner from pg_views where viewname = 'tenant_view'"
            ),
            vec![vec![Value::Text("target".into())]]
        );

        update_membership_options(&db, "target", "limited_admin", "limited_admin", false, true);
        let err = limited
            .execute(&db, "alter table tenant_table owner to target2")
            .unwrap_err();
        assert!(format!("{err:?}").contains("must be owner of table tenant_table"));

        update_membership_options(&db, "target", "limited_admin", "limited_admin", true, true);
        update_membership_options(
            &db,
            "target2",
            "limited_admin",
            "limited_admin",
            true,
            false,
        );
        limited
            .execute(&db, "alter table tenant_table owner to limited_admin")
            .unwrap();
        let err = limited
            .execute(&db, "alter table tenant_table owner to target2")
            .unwrap_err();
        let err_text = format!("{err:?}");
        assert!(err_text.contains("must be able to SET ROLE"), "{err_text}");
    }

    #[test]
    fn ownership_checks_reassign_owned_and_drop_role_follow_role_membership() {
        let base = temp_dir("reassign_owned");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser
            .execute(&db, "create role limited_admin createrole noinherit")
            .unwrap();
        superuser.execute(&db, "create role tenant").unwrap();
        superuser.execute(&db, "create role target").unwrap();
        superuser.execute(&db, "create role tenant2").unwrap();
        superuser.execute(&db, "create role target2").unwrap();

        let mut limited = Session::new(2);
        limited.set_session_authorization_oid(role_oid(&db, "limited_admin"));

        superuser
            .execute(&db, "create table tenant_table (id int4)")
            .unwrap();
        superuser
            .execute(
                &db,
                "create view tenant_view as select id from tenant_table",
            )
            .unwrap();
        set_relation_owner(&db, "tenant_table", "tenant");
        set_relation_owner(&db, "tenant_view", "tenant");

        let err = limited
            .execute(&db, "alter table tenant_table add column note text")
            .unwrap_err();
        assert!(format!("{err:?}").contains("must be owner of table tenant_table"));

        let err = limited
            .execute(&db, "create index tenant_other_idx on tenant_table (id)")
            .unwrap_err();
        assert!(format!("{err:?}").contains("must be owner of table tenant_table"));

        let err = superuser.execute(&db, "drop role tenant").unwrap_err();
        match err {
            ExecError::DetailedError {
                message, detail, ..
            } => {
                assert_eq!(
                    message,
                    "role \"tenant\" cannot be dropped because some objects depend on it"
                );
                let detail = detail.unwrap();
                assert!(detail.contains("owner of table tenant_table"));
                assert!(detail.contains("owner of view tenant_view"));
            }
            other => panic!("expected detailed dependency error, got {other:?}"),
        }

        let err = limited
            .execute(&db, "reassign owned by tenant to target")
            .unwrap_err();
        assert!(format!("{err:?}").contains("permission denied to reassign objects"));

        superuser
            .execute(
                &db,
                "grant tenant2 to limited_admin with inherit true, set true",
            )
            .unwrap();
        superuser
            .execute(
                &db,
                "grant target2 to limited_admin with inherit true, set true",
            )
            .unwrap();

        superuser
            .execute(&db, "create table tenant2_table (id int4)")
            .unwrap();
        superuser
            .execute(
                &db,
                "create view tenant2_view as select id from tenant2_table",
            )
            .unwrap();
        set_relation_owner(&db, "tenant2_table", "tenant2");
        set_relation_owner(&db, "tenant2_view", "tenant2");

        limited
            .execute(&db, "reassign owned by tenant2 to target2")
            .unwrap();

        assert_eq!(relation_owner_name(&db, "tenant2_table"), "target2");
        assert_eq!(
            query_rows(
                &db,
                1,
                "select viewowner from pg_views where viewname = 'tenant2_view'"
            ),
            vec![vec![Value::Text("target2".into())]]
        );

        assert_eq!(
            superuser.execute(&db, "drop role tenant2").unwrap(),
            StatementResult::AffectedRows(0)
        );
    }

    #[test]
    fn drop_owned_requires_privileges_of_target_role() {
        let base = temp_dir("drop_owned_permissions");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create role owner_role").unwrap();
        superuser.execute(&db, "create role limited_admin").unwrap();

        let mut limited = Session::new(2);
        limited.set_session_authorization_oid(role_oid(&db, "limited_admin"));

        let err = limited
            .execute(&db, "drop owned by owner_role")
            .unwrap_err();
        match err {
            ExecError::DetailedError {
                message, detail, ..
            } => {
                assert_eq!(message, "permission denied to drop objects");
                assert_eq!(
                    detail.as_deref(),
                    Some(
                        "Only roles with privileges of role \"owner_role\" may drop objects owned by it."
                    )
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn drop_owned_removes_tracked_role_dependencies() {
        let base = temp_dir("drop_owned_dependencies");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create role tenant").unwrap();
        superuser.execute(&db, "create role parent").unwrap();
        superuser.execute(&db, "create role grantee").unwrap();
        superuser.execute(&db, "create role db_grantee").unwrap();

        superuser
            .execute(&db, "create table tenant_table (id int4)")
            .unwrap();
        superuser
            .execute(
                &db,
                "create view tenant_view as select id from tenant_table",
            )
            .unwrap();
        superuser
            .execute(&db, "create sequence tenant_seq")
            .unwrap();
        set_relation_owner(&db, "tenant_table", "tenant");
        set_relation_owner(&db, "tenant_view", "tenant");
        set_relation_owner(&db, "tenant_seq", "tenant");

        superuser
            .execute(&db, "grant parent to tenant with admin option")
            .unwrap();
        superuser
            .execute(&db, "grant parent to grantee granted by tenant")
            .unwrap();
        superuser
            .execute(
                &db,
                "grant create on database regression to tenant with grant option",
            )
            .unwrap();

        let mut tenant_session = Session::new(3);
        tenant_session.set_session_authorization_oid(role_oid(&db, "tenant"));
        tenant_session
            .execute(&db, "grant create on database regression to db_grantee")
            .unwrap();

        let err = superuser.execute(&db, "drop role tenant").unwrap_err();
        match err {
            ExecError::DetailedError {
                message, detail, ..
            } => {
                assert_eq!(
                    message,
                    "role \"tenant\" cannot be dropped because some objects depend on it"
                );
                let detail = detail.unwrap();
                assert!(detail.contains("owner of table tenant_table"));
                assert!(detail.contains("owner of view tenant_view"));
                assert!(detail.contains("owner of sequence tenant_seq"));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        superuser.execute(&db, "drop owned by tenant").unwrap();

        assert!(!relation_exists(&db, "tenant_table"));
        assert!(!relation_exists(&db, "tenant_view"));
        assert!(!relation_exists(&db, "tenant_seq"));

        let tenant_oid = role_oid(&db, "tenant");
        assert!(
            !db.backend_catcache(1, None)
                .unwrap()
                .auth_members_rows()
                .into_iter()
                .any(|row| {
                    row.roleid == tenant_oid
                        || row.member == tenant_oid
                        || row.grantor == tenant_oid
                })
        );
        assert!(
            db.database_create_grants
                .read()
                .iter()
                .all(|grant| grant.grantee_oid != tenant_oid && grant.grantor_oid != tenant_oid)
        );

        assert_eq!(
            superuser.execute(&db, "drop role tenant").unwrap(),
            StatementResult::AffectedRows(0)
        );
    }
}
