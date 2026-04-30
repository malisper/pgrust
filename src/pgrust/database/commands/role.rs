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
use crate::include::catalog::{DEPENDENCY_NORMAL, PG_CLASS_RELATION_OID, PG_POLICY_RELATION_OID};
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
                        created.rolinherit,
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
                        member.rolinherit,
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
                    &membership_row(
                        created.oid,
                        member.oid,
                        current_user_oid,
                        true,
                        member.rolinherit,
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
                    crate::backend::utils::misc::notices::push_notice(format!(
                        "role \"{role_name}\" does not exist, skipping"
                    ));
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
            let shared_dependency_details = shared_role_dependency_details_for_roles(
                self,
                client_id,
                xid,
                current_cid,
                &[existing.oid],
            )?;
            if !owned_objects.is_empty() || !shared_dependency_details.is_empty() {
                let mut detail_lines = owned_objects
                    .iter()
                    .filter(|object| object.kind != OwnedObjectKind::Index)
                    .map(OwnedObject::drop_detail)
                    .collect::<Vec<_>>();
                detail_lines.extend(shared_dependency_details);
                let detail = detail_lines.join("\n");
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
            owned_object_drop_priority(left.kind)
                .cmp(&owned_object_drop_priority(right.kind))
                .then(left.name.cmp(&right.name))
                .then(left.kind.cmp(&right.kind))
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
            let effect = match object.kind {
                OwnedObjectKind::CompositeType => self
                    .catalog
                    .write()
                    .drop_composite_type_by_oid_mvcc(object.oid, &ctx)
                    .map(|(_, effect)| effect),
                OwnedObjectKind::Function => self
                    .catalog
                    .write()
                    .drop_proc_by_oid_mvcc(object.oid, &ctx)
                    .map(|(_, effect)| effect),
                OwnedObjectKind::View => self
                    .catalog
                    .write()
                    .drop_view_by_oid_mvcc(object.oid, &ctx)
                    .map(|(_, effect)| effect),
                OwnedObjectKind::Index => self
                    .catalog
                    .write()
                    .drop_relation_entry_by_oid_mvcc(object.oid, &ctx)
                    .map(|(_, effect)| effect),
                OwnedObjectKind::Publication => self
                    .catalog
                    .write()
                    .drop_publication_mvcc(object.oid, &ctx)
                    .map(|(_, effect)| effect),
                OwnedObjectKind::Schema => {
                    let catcache = self
                        .txn_backend_catcache(client_id, xid, current_cid)
                        .map_err(map_role_catalog_error)?;
                    let schema = catcache.namespace_by_oid(object.oid).ok_or_else(|| {
                        ExecError::Parse(role_management_error(format!(
                            "schema \"{}\" does not exist",
                            object.name
                        )))
                    })?;
                    self.catalog.write().drop_namespace_mvcc(
                        schema.oid,
                        &schema.nspname,
                        schema.nspowner,
                        schema.nspacl.clone(),
                        &ctx,
                    )
                }
                OwnedObjectKind::Type => {
                    self.drop_owned_dynamic_type_by_oid(client_id, object.oid)?;
                    Ok(CatalogMutationEffect::default())
                }
                _ => self
                    .catalog
                    .write()
                    .drop_relation_by_oid_mvcc(object.oid, &ctx)
                    .map(|(_, effect)| effect),
            }
            .map_err(map_role_catalog_error)?;
            if !matches!(object.kind, OwnedObjectKind::View) {
                self.apply_catalog_mutation_effect_immediate(&effect)?;
            }
            if matches!(object.kind, OwnedObjectKind::Table) {
                self.session_stats_state(client_id)
                    .write()
                    .note_relation_drop(object.oid, &self.stats);
            }
            if matches!(object.kind, OwnedObjectKind::Function) {
                self.session_stats_state(client_id)
                    .write()
                    .note_function_drop(object.oid, &self.stats);
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

        let catcache = self
            .txn_backend_catcache(client_id, xid, current_cid)
            .map_err(map_role_catalog_error)?;
        let mut owned_user_mappings = catcache
            .user_mapping_rows()
            .into_iter()
            .filter(|row| role_oids.contains(&row.umuser))
            .collect::<Vec<_>>();
        owned_user_mappings.sort_by_key(|row| (row.umserver, row.umuser));
        for mapping in owned_user_mappings {
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: current_cid,
                client_id,
                waiter: None,
                interrupts: interrupts.clone(),
            };
            let effect = self
                .catalog
                .write()
                .drop_user_mapping_mvcc(&mapping, &ctx)
                .map_err(map_role_catalog_error)?;
            catalog_effects.push(effect);
            current_cid = current_cid.saturating_add(1);
        }

        let catcache = self
            .txn_backend_catcache(client_id, xid, current_cid)
            .map_err(map_role_catalog_error)?;
        let mut policy_rows = catcache
            .policy_rows()
            .into_iter()
            .filter(|row| row.polroles.iter().any(|oid| role_oids.contains(oid)))
            .collect::<Vec<_>>();
        policy_rows.sort_by(|left, right| {
            left.polrelid
                .cmp(&right.polrelid)
                .then(left.polname.cmp(&right.polname))
                .then(left.oid.cmp(&right.oid))
        });
        for policy in policy_rows {
            let retained_roles = policy
                .polroles
                .iter()
                .copied()
                .filter(|oid| !role_oids.contains(oid))
                .collect::<Vec<_>>();
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: current_cid,
                client_id,
                waiter: Some(self.txn_waiter.clone()),
                interrupts: interrupts.clone(),
            };
            let effect = if retained_roles.is_empty() {
                self.catalog
                    .write()
                    .drop_policy_mvcc(policy.polrelid, &policy.polname, &ctx)
                    .map(|(_, effect)| effect)
            } else {
                let referenced_relation_oids =
                    policy_normal_relation_dependencies(&catcache, policy.oid, policy.polrelid);
                let updated = crate::include::catalog::PgPolicyRow {
                    polroles: retained_roles,
                    ..policy.clone()
                };
                self.catalog
                    .write()
                    .replace_policy_mvcc(&policy, updated, &referenced_relation_oids, &ctx)
                    .map(|(_, effect)| effect)
            }
            .map_err(map_role_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            current_cid = current_cid.saturating_add(1);
        }

        self.drop_owned_acl_entries(client_id, xid, current_cid, &role_oids, catalog_effects)?;

        // :HACK: DROP OWNED currently covers pgrust's tracked shared-role dependencies
        // (relation ownership, pg_auth_members rows, and database CREATE grants). Full
        // PostgreSQL-style shared dependency traversal should replace this with a single
        // dependency-driven path that also handles schemas, functions, and ACL entries.
        let _ = stmt.cascade;
        self.database_create_grants.write().retain(|grant| {
            !role_oids.contains(&grant.grantee_oid) && !role_oids.contains(&grant.grantor_oid)
        });
        self.object_addresses
            .write()
            .default_acls
            .retain(|row| !role_oids.contains(&row.role_oid));

        Ok(StatementResult::AffectedRows(0))
    }

    fn drop_owned_acl_entries(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        role_oids: &BTreeSet<u32>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let mut current_cid = cid;
        let catcache = self
            .txn_backend_catcache(client_id, xid, current_cid)
            .map_err(map_role_catalog_error)?;
        let role_names = catcache
            .authid_rows()
            .into_iter()
            .filter(|row| role_oids.contains(&row.oid))
            .map(|row| row.rolname)
            .collect::<BTreeSet<_>>();
        if role_names.is_empty() {
            return Ok(current_cid);
        }

        let owner_names = catcache
            .authid_rows()
            .into_iter()
            .map(|row| (row.oid, row.rolname))
            .collect::<BTreeMap<_, _>>();
        let interrupts = self.interrupt_state(client_id);
        for class in catcache.class_rows() {
            let Some(relacl) = class.relacl.clone() else {
                continue;
            };
            let cleaned = remove_acl_role_mentions(relacl, &role_names);
            if cleaned == class.relacl {
                continue;
            }
            let owner_name = owner_names
                .get(&class.relowner)
                .map(String::as_str)
                .unwrap_or_default();
            let collapsed =
                collapse_relation_acl_after_role_drop(cleaned, owner_name, class.relkind);
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: current_cid,
                client_id,
                waiter: None,
                interrupts: interrupts.clone(),
            };
            let effect = self
                .catalog
                .write()
                .alter_relation_acl_mvcc(class.oid, collapsed, &ctx)
                .map_err(map_role_catalog_error)?;
            catalog_effects.push(effect);
            current_cid = current_cid.saturating_add(1);
        }

        let catcache = self
            .txn_backend_catcache(client_id, xid, current_cid)
            .map_err(map_role_catalog_error)?;
        let mut attribute_acls = BTreeMap::<u32, BTreeMap<i16, Option<Vec<String>>>>::new();
        for attribute in catcache.attribute_rows() {
            let Some(attacl) = attribute.attacl.clone() else {
                continue;
            };
            let cleaned = remove_acl_role_mentions(attacl, &role_names);
            if cleaned == attribute.attacl {
                continue;
            }
            attribute_acls
                .entry(attribute.attrelid)
                .or_default()
                .insert(attribute.attnum, cleaned.filter(|acl| !acl.is_empty()));
        }
        for (relation_oid, acl_by_attnum) in attribute_acls {
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: current_cid,
                client_id,
                waiter: None,
                interrupts: interrupts.clone(),
            };
            let effect = self
                .catalog
                .write()
                .alter_attribute_acls_mvcc(relation_oid, acl_by_attnum.into_iter().collect(), &ctx)
                .map_err(map_role_catalog_error)?;
            catalog_effects.push(effect);
            current_cid = current_cid.saturating_add(1);
        }

        Ok(current_cid)
    }

    fn drop_owned_dynamic_type_by_oid(
        &self,
        client_id: ClientId,
        type_oid: u32,
    ) -> Result<bool, ExecError> {
        {
            let mut enum_types = self.enum_types.write();
            if let Some(key) = enum_types
                .iter()
                .find_map(|(key, entry)| (entry.oid == type_oid).then_some(key.clone()))
            {
                enum_types.remove(&key);
                drop(enum_types);
                self.refresh_catalog_store_dynamic_type_rows(client_id, None);
                self.invalidate_backend_cache_state(client_id);
                self.plan_cache.invalidate_all();
                return Ok(true);
            }
        }

        let mut range_types = self.range_types.write();
        if let Some(key) = range_types
            .iter()
            .find_map(|(key, entry)| (entry.oid == type_oid).then_some(key.clone()))
        {
            range_types.remove(&key);
            save_range_type_entries(&self.cluster.base_dir, self.database_oid, &range_types)?;
            drop(range_types);
            self.refresh_catalog_store_dynamic_type_rows(client_id, None);
            self.invalidate_backend_cache_state(client_id);
            self.plan_cache.invalidate_all();
            return Ok(true);
        }

        Ok(false)
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
        if !auth.can_set_role(new_role.oid, &auth_catalog) {
            return Err(ExecError::DetailedError {
                message: "permission denied to reassign objects".into(),
                detail: Some(format!(
                    "Only roles with privileges of role \"{}\" may reassign objects to it.",
                    new_role.rolname
                )),
                hint: None,
                sqlstate: "42501",
            });
        }

        let old_role_oids = old_roles
            .iter()
            .map(|role| role.oid)
            .collect::<BTreeSet<_>>();
        let old_role_oid_list = old_role_oids.iter().copied().collect::<Vec<_>>();
        let owned_objects =
            owned_objects_for_roles(self, client_id, Some((xid, cid)), &old_role_oid_list)?;
        let database_rows = self
            .txn_backend_catcache(client_id, xid, cid)
            .map_err(map_role_catalog_error)?
            .database_rows()
            .into_iter()
            .filter(|row| old_role_oids.contains(&row.datdba))
            .collect::<Vec<_>>();
        if owned_objects.is_empty() && database_rows.is_empty() {
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
                OwnedObjectKind::EventTrigger => {
                    let catcache = self
                        .txn_backend_catcache(client_id, xid, cid.saturating_add(offset as u32))
                        .map_err(map_role_catalog_error)?;
                    let event_trigger =
                        catcache
                            .event_trigger_row_by_oid(object.oid)
                            .ok_or_else(|| {
                                ExecError::Parse(role_management_error(format!(
                                    "event trigger \"{}\" does not exist",
                                    object.name
                                )))
                            })?;
                    let (_, effect) = self
                        .catalog
                        .write()
                        .replace_event_trigger_mvcc(
                            &event_trigger.evtname,
                            crate::include::catalog::PgEventTriggerRow {
                                evtowner: new_role.oid,
                                ..event_trigger.clone()
                            },
                            &ctx,
                        )
                        .map_err(map_role_catalog_error)?;
                    effect
                }
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
                OwnedObjectKind::Schema => self
                    .catalog
                    .write()
                    .alter_namespace_owner_mvcc(object.oid, new_role.oid, &ctx)
                    .map_err(map_role_catalog_error)?,
                OwnedObjectKind::Function => {
                    let catcache = self
                        .txn_backend_catcache(client_id, xid, cid.saturating_add(offset as u32))
                        .map_err(map_role_catalog_error)?;
                    let proc_row = catcache
                        .proc_rows()
                        .into_iter()
                        .find(|row| row.oid == object.oid)
                        .ok_or_else(|| {
                            ExecError::Parse(role_management_error(format!(
                                "function \"{}\" does not exist",
                                object.name
                            )))
                        })?;
                    let (_, effect) = self
                        .catalog
                        .write()
                        .replace_proc_mvcc(
                            &proc_row,
                            crate::include::catalog::PgProcRow {
                                proowner: new_role.oid,
                                ..proc_row.clone()
                            },
                            &ctx,
                        )
                        .map_err(map_role_catalog_error)?;
                    effect
                }
                OwnedObjectKind::Type => {
                    self.reassign_owned_dynamic_type_by_oid(client_id, object.oid, new_role.oid)?;
                    CatalogMutationEffect::default()
                }
                OwnedObjectKind::Index
                | OwnedObjectKind::Sequence
                | OwnedObjectKind::Table
                | OwnedObjectKind::CompositeType
                | OwnedObjectKind::View => self
                    .catalog
                    .write()
                    .alter_relation_owner_mvcc(object.oid, new_role.oid, &ctx)
                    .map_err(map_role_catalog_error)?,
            };
            catalog_effects.push(effect);
        }
        for (offset, database) in database_rows.into_iter().enumerate() {
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: cid.saturating_add(owned_objects.len() as u32 + offset as u32),
                client_id,
                waiter: None,
                interrupts: interrupts.clone(),
            };
            let effect = self
                .shared_catalog
                .write()
                .replace_database_row_mvcc(
                    crate::include::catalog::PgDatabaseRow {
                        datdba: new_role.oid,
                        ..database
                    },
                    &ctx,
                )
                .map_err(map_role_catalog_error)?;
            catalog_effects.push(effect);
        }

        Ok(StatementResult::AffectedRows(0))
    }

    fn reassign_owned_dynamic_type_by_oid(
        &self,
        client_id: ClientId,
        type_oid: u32,
        new_owner_oid: u32,
    ) -> Result<bool, ExecError> {
        {
            let mut enum_types = self.enum_types.write();
            if let Some(entry) = enum_types.values_mut().find(|entry| entry.oid == type_oid) {
                entry.owner_oid = new_owner_oid;
                drop(enum_types);
                self.refresh_catalog_store_dynamic_type_rows(client_id, None);
                self.invalidate_backend_cache_state(client_id);
                self.plan_cache.invalidate_all();
                return Ok(true);
            }
        }

        let mut range_types = self.range_types.write();
        if let Some(entry) = range_types.values_mut().find(|entry| entry.oid == type_oid) {
            entry.owner_oid = new_owner_oid;
            entry.owner_usage = true;
            save_range_type_entries(&self.cluster.base_dir, self.database_oid, &range_types)?;
            drop(range_types);
            self.refresh_catalog_store_dynamic_type_rows(client_id, None);
            self.invalidate_backend_cache_state(client_id);
            self.plan_cache.invalidate_all();
            return Ok(true);
        }

        Ok(false)
    }
}

fn map_role_catalog_error(err: crate::backend::catalog::CatalogError) -> ExecError {
    match err {
        crate::backend::catalog::CatalogError::UniqueViolation(message) => ExecError::Parse(
            role_management_error(rewrite_role_catalog_message(&message)),
        ),
        crate::backend::catalog::CatalogError::UnknownTable(name) => ExecError::Parse(
            role_management_error(format!("role \"{name}\" does not exist")),
        ),
        other => ExecError::Parse(role_management_error(format!("{other:?}"))),
    }
}

fn rewrite_role_catalog_message(message: &str) -> String {
    if let Some(role_name) = message.strip_prefix("duplicate role name: ") {
        format!("role \"{role_name}\" already exists")
    } else {
        message.to_string()
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
    CompositeType,
    EventTrigger,
    Function,
    Index,
    Publication,
    Schema,
    Sequence,
    Table,
    Type,
    View,
}

impl OwnedObject {
    fn kind_name(&self) -> &'static str {
        match self.kind {
            OwnedObjectKind::CompositeType | OwnedObjectKind::Type => "type",
            OwnedObjectKind::EventTrigger => "event trigger",
            OwnedObjectKind::Function => "function",
            OwnedObjectKind::Index => "index",
            OwnedObjectKind::Publication => "publication",
            OwnedObjectKind::Schema => "schema",
            OwnedObjectKind::Sequence => "sequence",
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
        .filter(|row| matches!(row.relkind, 'r' | 'v' | 'i' | 'S' | 'c'))
        .map(|row| OwnedObject {
            oid: row.oid,
            kind: match row.relkind {
                'c' => OwnedObjectKind::CompositeType,
                'i' => OwnedObjectKind::Index,
                'S' => OwnedObjectKind::Sequence,
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
            .proc_rows()
            .into_iter()
            .filter(|row| role_oids.contains(&row.proowner))
            .map(|row| OwnedObject {
                oid: row.oid,
                kind: OwnedObjectKind::Function,
                name: function_owned_object_name(&catcache, &row),
            }),
    );
    objects.extend(
        catcache
            .type_rows()
            .into_iter()
            .filter(|row| role_oids.contains(&row.typowner))
            .filter(|row| matches!(row.typtype, 'e' | 'r'))
            .map(|row| OwnedObject {
                oid: row.oid,
                kind: OwnedObjectKind::Type,
                name: type_owned_object_name(&catcache, &row),
            }),
    );
    objects.extend(
        catcache
            .namespace_rows()
            .into_iter()
            .filter(|row| role_oids.contains(&row.nspowner))
            .filter(|row| !matches!(row.nspname.as_str(), "pg_catalog" | "information_schema"))
            .map(|row| OwnedObject {
                oid: row.oid,
                kind: OwnedObjectKind::Schema,
                name: row.nspname,
            }),
    );
    objects.extend(
        catcache
            .event_trigger_rows()
            .into_iter()
            .filter(|row| role_oids.contains(&row.evtowner))
            .map(|row| OwnedObject {
                oid: row.oid,
                kind: OwnedObjectKind::EventTrigger,
                name: row.evtname,
            }),
    );
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
    objects.sort_by(|left, right| left.oid.cmp(&right.oid).then(left.kind.cmp(&right.kind)));
    Ok(objects)
}

fn function_owned_object_name(
    catcache: &crate::backend::utils::cache::catcache::CatCache,
    row: &crate::include::catalog::PgProcRow,
) -> String {
    let args = row
        .proargtypes
        .split_whitespace()
        .filter_map(|oid| oid.parse::<u32>().ok())
        .map(|oid| {
            catcache
                .type_by_oid(oid)
                .map(|row| row.typname.clone())
                .unwrap_or_else(|| oid.to_string())
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "{}({args})",
        schema_qualified_name_for_role_deps(catcache, row.pronamespace, &row.proname)
    )
}

fn type_owned_object_name(
    catcache: &crate::backend::utils::cache::catcache::CatCache,
    row: &crate::include::catalog::PgTypeRow,
) -> String {
    schema_qualified_name_for_role_deps(catcache, row.typnamespace, &row.typname)
}

fn schema_qualified_name_for_role_deps(
    catcache: &crate::backend::utils::cache::catcache::CatCache,
    namespace_oid: u32,
    object_name: &str,
) -> String {
    match catcache
        .namespace_by_oid(namespace_oid)
        .map(|row| row.nspname.as_str())
    {
        Some("public") | Some("pg_catalog") | None => object_name.to_string(),
        Some(schema_name) => format!("{schema_name}.{object_name}"),
    }
}

fn owned_object_drop_priority(kind: OwnedObjectKind) -> u8 {
    match kind {
        OwnedObjectKind::EventTrigger => 0,
        OwnedObjectKind::View => 0,
        OwnedObjectKind::Function => 1,
        OwnedObjectKind::Index => 2,
        OwnedObjectKind::Publication => 3,
        OwnedObjectKind::CompositeType | OwnedObjectKind::Type => 4,
        OwnedObjectKind::Sequence => 5,
        OwnedObjectKind::Table => 6,
        OwnedObjectKind::Schema => 7,
    }
}

fn acl_item_depends_on_role(item: &str, role_names: &BTreeSet<&str>) -> bool {
    let Some((grantee, rest)) = item.split_once('=') else {
        return false;
    };
    let Some((_, grantor)) = rest.split_once('/') else {
        return false;
    };
    (!grantee.is_empty() && role_names.contains(grantee)) || role_names.contains(grantor)
}

fn acl_depends_on_role(acl: Option<&[String]>, role_names: &BTreeSet<&str>) -> bool {
    acl.unwrap_or_default()
        .iter()
        .any(|item| acl_item_depends_on_role(item, role_names))
}

fn acl_item_mentions_role_name(item: &str, role_names: &BTreeSet<String>) -> bool {
    let Some((grantee, rest)) = item.split_once('=') else {
        return false;
    };
    let Some((_, grantor)) = rest.split_once('/') else {
        return false;
    };
    (!grantee.is_empty() && role_names.contains(grantee)) || role_names.contains(grantor)
}

fn remove_acl_role_mentions(
    acl: Vec<String>,
    role_names: &BTreeSet<String>,
) -> Option<Vec<String>> {
    let retained = acl
        .into_iter()
        .filter(|item| !acl_item_mentions_role_name(item, role_names))
        .collect::<Vec<_>>();
    (!retained.is_empty()).then_some(retained)
}

fn collapse_relation_acl_after_role_drop(
    acl: Option<Vec<String>>,
    _owner_name: &str,
    _relkind: char,
) -> Option<Vec<String>> {
    let acl = acl?;
    match acl.as_slice() {
        [] => None,
        _ => Some(acl),
    }
}

fn shared_role_dependency_details_for_roles(
    db: &Database,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    role_oids: &[u32],
) -> Result<Vec<String>, ExecError> {
    let role_oids = role_oids.iter().copied().collect::<BTreeSet<_>>();
    let auth_catalog = db
        .txn_auth_catalog(client_id, xid, cid)
        .map_err(map_role_catalog_error)?;
    let role_names = auth_catalog
        .roles()
        .iter()
        .map(|row| (row.oid, row.rolname.as_str()))
        .collect::<BTreeMap<_, _>>();
    let target_role_names = role_oids
        .iter()
        .filter_map(|oid| role_names.get(oid).copied())
        .collect::<BTreeSet<_>>();

    let mut details = auth_catalog
        .memberships()
        .iter()
        .filter(|row| {
            role_oids.contains(&row.grantor)
                && !role_oids.contains(&row.roleid)
                && !role_oids.contains(&row.member)
        })
        .filter_map(|row| {
            Some(format!(
                "privileges for membership of role {} in role {}",
                role_names.get(&row.member)?,
                role_names.get(&row.roleid)?
            ))
        })
        .collect::<Vec<_>>();

    let catcache = db
        .txn_backend_catcache(client_id, xid, cid)
        .map_err(map_role_catalog_error)?;
    let class_rows = catcache.class_rows();
    let relation_names = class_rows
        .iter()
        .map(|row| (row.oid, relation_display_name_for_role_deps(row)))
        .collect::<BTreeMap<_, _>>();
    for class in &class_rows {
        if acl_depends_on_role(class.relacl.as_deref(), &target_role_names) {
            details.push(format!(
                "privileges for {} {}",
                relation_kind_name_for_role_deps(class.relkind),
                relation_display_name_for_role_deps(class)
            ));
        }
    }
    for policy in catcache.policy_rows() {
        if policy.polroles.iter().any(|oid| role_oids.contains(oid)) {
            let relation_name = relation_names
                .get(&policy.polrelid)
                .cloned()
                .unwrap_or_else(|| policy.polrelid.to_string());
            details.push(format!(
                "target of policy {} on table {}",
                policy.polname, relation_name
            ));
        }
    }
    for wrapper in catcache.foreign_data_wrapper_rows() {
        if role_oids.contains(&wrapper.fdwowner) {
            details.push(format!("owner of foreign-data wrapper {}", wrapper.fdwname));
        }
        if acl_depends_on_role(wrapper.fdwacl.as_deref(), &target_role_names) {
            details.push(format!(
                "privileges for foreign-data wrapper {}",
                wrapper.fdwname
            ));
        }
    }
    for server in catcache.foreign_server_rows() {
        if role_oids.contains(&server.srvowner) {
            details.push(format!("owner of server {}", server.srvname));
        }
        if acl_depends_on_role(server.srvacl.as_deref(), &target_role_names) {
            details.push(format!("privileges for foreign server {}", server.srvname));
        }
    }
    let server_names = catcache
        .foreign_server_rows()
        .into_iter()
        .map(|row| (row.oid, row.srvname))
        .collect::<BTreeMap<_, _>>();
    let user_mapping_rows = catcache.user_mapping_rows();
    for mapping in user_mapping_rows {
        if role_oids.contains(&mapping.umuser)
            && let (Some(role_name), Some(server_name)) = (
                role_names.get(&mapping.umuser),
                server_names.get(&mapping.umserver),
            )
        {
            details.push(format!(
                "owner of user mapping for {role_name} on server {server_name}"
            ));
        }
    }
    if db.database_create_grants.read().iter().any(|grant| {
        role_oids.contains(&grant.grantee_oid) || role_oids.contains(&grant.grantor_oid)
    }) {
        details.push("privileges for database regression".into());
    }

    let mut default_privilege_details = Vec::new();
    for row in db.object_addresses.read().default_acls.iter() {
        if !role_oids.contains(&row.role_oid) {
            continue;
        }
        let object_kind = default_acl_object_kind_for_role_deps(row.objtype);
        let role_name = role_names
            .get(&row.role_oid)
            .copied()
            .unwrap_or(row.role_name.as_str());
        let detail = match &row.namespace_name {
            Some(namespace_name) => format!(
                "owner of default privileges on new {object_kind} belonging to role {role_name} in schema {namespace_name}"
            ),
            None => format!(
                "owner of default privileges on new {object_kind} belonging to role {role_name}"
            ),
        };
        default_privilege_details.push(detail);
    }

    details.sort();
    details.dedup();
    default_privilege_details.sort();
    default_privilege_details.dedup();
    details.extend(default_privilege_details);
    Ok(details)
}

fn default_acl_object_kind_for_role_deps(objtype: char) -> &'static str {
    match objtype {
        'r' => "relations",
        'S' => "sequences",
        'f' => "functions",
        'T' => "types",
        'n' => "schemas",
        _ => "objects",
    }
}

fn policy_normal_relation_dependencies(
    catcache: &crate::backend::utils::cache::catcache::CatCache,
    policy_oid: u32,
    policy_relation_oid: u32,
) -> Vec<u32> {
    let mut relation_oids = catcache
        .depend_rows()
        .into_iter()
        .filter(|row| {
            row.classid == PG_POLICY_RELATION_OID
                && row.objid == policy_oid
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid != policy_relation_oid
                && row.deptype == DEPENDENCY_NORMAL
        })
        .map(|row| row.refobjid)
        .collect::<Vec<_>>();
    relation_oids.sort_unstable();
    relation_oids.dedup();
    relation_oids
}

fn relation_kind_name_for_role_deps(relkind: char) -> &'static str {
    match relkind {
        'S' => "sequence",
        'v' => "view",
        _ => "table",
    }
}

fn relation_display_name_for_role_deps(row: &crate::include::catalog::PgClassRow) -> String {
    row.relname.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::CatalogError;
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

    fn relation_acl(db: &Database, relname: &str) -> Option<Vec<String>> {
        db.backend_catcache(1, None)
            .unwrap()
            .class_rows()
            .into_iter()
            .find(|row| row.relname == relname)
            .unwrap()
            .relacl
    }

    fn type_owner_name(db: &Database, typname: &str) -> String {
        let catcache = db.backend_catcache(1, None).unwrap();
        let owner_oid = catcache
            .type_rows()
            .into_iter()
            .find(|row| row.typname == typname)
            .map(|row| row.typowner)
            .unwrap();
        catcache
            .authid_rows()
            .into_iter()
            .find(|row| row.oid == owner_oid)
            .map(|row| row.rolname)
            .unwrap()
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
    fn duplicate_role_name_errors_use_postgres_wording() {
        let err = map_role_catalog_error(CatalogError::UniqueViolation(
            "duplicate role name: regress_priv_user5".into(),
        ));
        match err {
            ExecError::Parse(parse_err) => assert_eq!(
                parse_err,
                role_management_error("role \"regress_priv_user5\" already exists")
            ),
            other => panic!("expected parse error, got {other:?}"),
        }
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
                && row.inherit_option
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
    fn alter_view_rename_requires_view_owner() {
        let base = temp_dir("alter_view_rename_owner");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create role tenant").unwrap();
        superuser.execute(&db, "create role other").unwrap();

        let tenant_oid = role_oid(&db, "tenant");
        let other_oid = role_oid(&db, "other");

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

        let mut other = Session::new(3);
        other.set_session_authorization_oid(other_oid);
        let err = other
            .execute(&db, "alter view tenant_view rename to tenant_view_new")
            .unwrap_err();
        assert!(format!("{err:?}").contains("must be owner of view tenant_view"));
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
    fn reassign_owned_target_permission_uses_postgres_detail() {
        let base = temp_dir("reassign_owned_target_permission");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create role owner_role").unwrap();
        superuser.execute(&db, "create role target_role").unwrap();

        let mut owner = Session::new(2);
        owner.set_session_authorization_oid(role_oid(&db, "owner_role"));
        let err = owner
            .execute(&db, "reassign owned by owner_role to target_role")
            .unwrap_err();
        match err {
            ExecError::DetailedError {
                message, detail, ..
            } => {
                assert_eq!(message, "permission denied to reassign objects");
                assert_eq!(
                    detail.as_deref(),
                    Some(
                        "Only roles with privileges of role \"target_role\" may reassign objects to it."
                    )
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn table_grant_option_allows_non_owner_regrant() {
        let base = temp_dir("table_grant_option_regrant");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create role owner_role").unwrap();
        superuser.execute(&db, "create role grantor_role").unwrap();
        superuser.execute(&db, "create role grantee_role").unwrap();

        let mut owner = Session::new(2);
        owner.set_session_authorization_oid(role_oid(&db, "owner_role"));
        owner
            .execute(&db, "create table grant_tbl (id int4)")
            .unwrap();
        owner
            .execute(
                &db,
                "grant all on grant_tbl to grantor_role with grant option",
            )
            .unwrap();

        let mut grantor = Session::new(3);
        grantor.set_session_authorization_oid(role_oid(&db, "grantor_role"));
        grantor
            .execute(&db, "grant all on grant_tbl to grantee_role")
            .unwrap();

        assert_eq!(
            relation_acl(&db, "grant_tbl").unwrap(),
            vec![
                "owner_role=arwdDxtm/owner_role".to_string(),
                "grantor_role=a*r*w*d*D*x*t*m*/owner_role".to_string(),
                "grantee_role=arwdDxtm/grantor_role".to_string(),
            ]
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

    #[test]
    fn drop_owned_removes_relation_acl_grantee_and_grantor_entries() {
        let base = temp_dir("drop_owned_acl_entries");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create role owner_role").unwrap();
        superuser.execute(&db, "create role grantor_role").unwrap();
        superuser.execute(&db, "create role grantee_role").unwrap();

        let mut owner = Session::new(2);
        owner.set_session_authorization_oid(role_oid(&db, "owner_role"));
        owner
            .execute(&db, "create table grant_tbl (id int4)")
            .unwrap();
        owner
            .execute(
                &db,
                "grant all on grant_tbl to grantor_role with grant option",
            )
            .unwrap();

        let mut grantor = Session::new(3);
        grantor.set_session_authorization_oid(role_oid(&db, "grantor_role"));
        grantor
            .execute(&db, "grant all on grant_tbl to grantee_role")
            .unwrap();

        superuser
            .execute(&db, "drop owned by grantor_role")
            .unwrap();
        assert_eq!(
            relation_acl(&db, "grant_tbl").unwrap(),
            vec!["owner_role=arwdDxtm/owner_role".to_string()]
        );
        assert_eq!(
            superuser.execute(&db, "drop role grantor_role").unwrap(),
            StatementResult::AffectedRows(0)
        );
    }

    #[test]
    fn drop_owned_drops_or_rewrites_policy_role_targets() {
        let base = temp_dir("drop_owned_policy_dependencies");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create role policy_owner1").unwrap();
        superuser.execute(&db, "create role policy_owner2").unwrap();
        superuser
            .execute(&db, "create table policy_owned_tbl (a int4)")
            .unwrap();

        superuser
            .execute(
                &db,
                "create policy p1 on policy_owned_tbl to policy_owner1 using (true)",
            )
            .unwrap();
        superuser
            .execute(&db, "drop owned by policy_owner1")
            .unwrap();
        let err = superuser
            .execute(&db, "drop policy p1 on policy_owned_tbl")
            .unwrap_err();
        assert!(matches!(
            err,
            ExecError::DetailedError {
                message,
                sqlstate: "42704",
                ..
            } if message == "policy \"p1\" for table \"policy_owned_tbl\" does not exist"
        ));

        superuser
            .execute(
                &db,
                "create policy p1 on policy_owned_tbl to policy_owner1, policy_owner1, \
                 policy_owner2 using (a > (select max(a) from policy_owned_tbl))",
            )
            .unwrap();
        superuser
            .execute(&db, "drop owned by policy_owner1")
            .unwrap();
        assert_eq!(
            query_rows(
                &db,
                1,
                "select count(*)::int4 from pg_shdepend \
                 where objid = (select oid from pg_policy where polname = 'p1') \
                   and refobjid = 'policy_owner1'::regrole"
            ),
            vec![vec![Value::Int32(0)]]
        );
        assert_eq!(
            query_rows(
                &db,
                1,
                "select count(*)::int4 from pg_shdepend \
                 where objid = (select oid from pg_policy where polname = 'p1') \
                   and refobjid = 'policy_owner2'::regrole"
            ),
            vec![vec![Value::Int32(1)]]
        );
        assert_eq!(
            query_rows(
                &db,
                1,
                "select count(*)::int4 from pg_depend \
                 where objid = (select oid from pg_policy where polname = 'p1') \
                   and refobjid = 'policy_owned_tbl'::regclass"
            ),
            vec![vec![Value::Int32(1)]]
        );
        assert_eq!(
            superuser
                .execute(&db, "drop policy p1 on policy_owned_tbl")
                .unwrap(),
            StatementResult::AffectedRows(0)
        );
    }

    #[test]
    fn reassign_owned_does_not_clear_granted_by_membership_dependencies() {
        let base = temp_dir("reassign_owned_membership_dependencies");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create role user1").unwrap();
        superuser.execute(&db, "create role user2").unwrap();
        superuser.execute(&db, "create role user3").unwrap();
        superuser.execute(&db, "create role user4").unwrap();

        superuser
            .execute(&db, "grant user1 to user2 with admin option")
            .unwrap();
        superuser
            .execute(&db, "grant user1 to user3 granted by user2")
            .unwrap();

        let err = superuser.execute(&db, "drop role user2").unwrap_err();
        match err {
            ExecError::DetailedError {
                message,
                detail,
                sqlstate,
                ..
            } => {
                assert_eq!(
                    message,
                    "role \"user2\" cannot be dropped because some objects depend on it"
                );
                assert_eq!(
                    detail.as_deref(),
                    Some("privileges for membership of role user3 in role user1")
                );
                assert_eq!(sqlstate, "2BP01");
            }
            other => panic!("unexpected error: {other:?}"),
        }

        superuser
            .execute(&db, "reassign owned by user2 to user4")
            .unwrap();

        let err = superuser.execute(&db, "drop role user2").unwrap_err();
        match err {
            ExecError::DetailedError {
                message,
                detail,
                sqlstate,
                ..
            } => {
                assert_eq!(
                    message,
                    "role \"user2\" cannot be dropped because some objects depend on it"
                );
                assert_eq!(
                    detail.as_deref(),
                    Some("privileges for membership of role user3 in role user1")
                );
                assert_eq!(sqlstate, "2BP01");
            }
            other => panic!("unexpected error: {other:?}"),
        }

        superuser.execute(&db, "drop owned by user2").unwrap();
        assert_eq!(
            superuser.execute(&db, "drop role user2").unwrap(),
            StatementResult::AffectedRows(0)
        );
    }

    #[test]
    fn drop_role_reports_table_acl_and_policy_dependencies() {
        let base = temp_dir("drop_role_policy_dependencies");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);

        superuser.execute(&db, "create role policy_user").unwrap();
        superuser.execute(&db, "create role policy_user2").unwrap();
        superuser
            .execute(&db, "create table policy_dep_tbl (a int4)")
            .unwrap();
        superuser
            .execute(&db, "grant select on policy_dep_tbl to policy_user")
            .unwrap();
        superuser
            .execute(
                &db,
                "create policy p1 on policy_dep_tbl to policy_user, policy_user2 using (true)",
            )
            .unwrap();

        let err = superuser.execute(&db, "drop role policy_user").unwrap_err();
        match err {
            ExecError::DetailedError {
                message,
                detail,
                sqlstate,
                ..
            } => {
                assert_eq!(
                    message,
                    "role \"policy_user\" cannot be dropped because some objects depend on it"
                );
                assert_eq!(
                    detail.as_deref(),
                    Some(
                        "privileges for table policy_dep_tbl\ntarget of policy p1 on table policy_dep_tbl"
                    )
                );
                assert_eq!(sqlstate, "2BP01");
            }
            other => panic!("unexpected error: {other:?}"),
        }

        superuser
            .execute(&db, "alter policy p1 on policy_dep_tbl to policy_user2")
            .unwrap();
        superuser
            .execute(&db, "revoke all on policy_dep_tbl from policy_user")
            .unwrap();
        assert_eq!(
            superuser.execute(&db, "drop role policy_user").unwrap(),
            StatementResult::AffectedRows(0)
        );

        let err = superuser
            .execute(&db, "drop role policy_user2")
            .unwrap_err();
        match err {
            ExecError::DetailedError {
                detail, sqlstate, ..
            } => {
                assert_eq!(
                    detail.as_deref(),
                    Some("target of policy p1 on table policy_dep_tbl")
                );
                assert_eq!(sqlstate, "2BP01");
            }
            other => panic!("unexpected error: {other:?}"),
        }

        superuser
            .execute(&db, "drop policy p1 on policy_dep_tbl")
            .unwrap();
        assert_eq!(
            superuser.execute(&db, "drop role policy_user2").unwrap(),
            StatementResult::AffectedRows(0)
        );
    }

    #[test]
    fn reassign_owned_updates_composite_enum_and_range_type_owners() {
        let base = temp_dir("reassign_owned_types");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create role type_owner").unwrap();
        superuser.execute(&db, "create role type_target").unwrap();

        let mut owner = Session::new(2);
        owner.set_session_authorization_oid(role_oid(&db, "type_owner"));
        owner
            .execute(&db, "create type owned_composite as (a int4)")
            .unwrap();
        owner
            .execute(&db, "create type owned_enum as enum ('red')")
            .unwrap();
        owner
            .execute(&db, "create type owned_range as range (subtype = int4)")
            .unwrap();

        assert_eq!(type_owner_name(&db, "owned_composite"), "type_owner");
        assert_eq!(type_owner_name(&db, "owned_enum"), "type_owner");
        assert_eq!(type_owner_name(&db, "owned_range"), "type_owner");

        superuser
            .execute(&db, "reassign owned by type_owner to type_target")
            .unwrap();

        assert_eq!(type_owner_name(&db, "owned_composite"), "type_target");
        assert_eq!(type_owner_name(&db, "owned_enum"), "type_target");
        assert_eq!(type_owner_name(&db, "owned_range"), "type_target");
    }

    #[test]
    fn drop_role_reports_function_type_database_and_default_acl_dependencies() {
        let base = temp_dir("drop_role_extended_dependencies");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create role dep_owner").unwrap();
        superuser.execute(&db, "create role dep_grantee").unwrap();
        superuser
            .execute(&db, "grant create on database regression to dep_owner")
            .unwrap();

        let mut owner = Session::new(2);
        owner.set_session_authorization_oid(role_oid(&db, "dep_owner"));
        owner.execute(&db, "create schema dep_schema").unwrap();
        owner
            .execute(
                &db,
                "alter default privileges for role dep_owner in schema dep_schema grant all on tables to dep_grantee",
            )
            .unwrap();
        owner
            .execute(
                &db,
                "create function dep_func() returns void language plpgsql as $$ begin end; $$",
            )
            .unwrap();
        owner
            .execute(&db, "create type dep_enum as enum ('red')")
            .unwrap();
        owner
            .execute(&db, "create type dep_range as range (subtype = int4)")
            .unwrap();
        owner
            .execute(&db, "create type dep_composite as (a int4)")
            .unwrap();

        let err = superuser.execute(&db, "drop role dep_owner").unwrap_err();
        match err {
            ExecError::DetailedError { detail, .. } => {
                let detail = detail.unwrap();
                assert!(detail.contains("privileges for database regression"));
                assert!(detail.contains("owner of default privileges on new relations belonging to role dep_owner in schema dep_schema"));
                assert!(detail.contains("owner of function dep_func()"));
                assert!(detail.contains("owner of type dep_enum"));
                assert!(detail.contains("owner of type dep_range"));
                assert!(detail.contains("owner of type dep_composite"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
