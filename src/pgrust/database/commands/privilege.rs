use super::super::*;
use crate::backend::commands::rolecmds::{
    GrantMembershipAuthorizationError, grant_membership_authorized_with_detail, membership_row,
    role_management_error,
};
use crate::backend::parser::{
    CatalogLookup, GrantObjectPrivilege, GrantObjectStatement, GrantRoleMembershipStatement,
    ParseError, RevokeObjectStatement, RevokeRoleMembershipStatement, RoleGrantorSpec,
    parse_type_name, resolve_raw_type_name,
};
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, CURRENT_DATABASE_NAME, CURRENT_DATABASE_OID, PgAuthIdRow,
};

fn parse_granted_function_signature(signature: &str) -> Result<(&str, Vec<&str>), ParseError> {
    let Some(open_paren) = signature.rfind('(') else {
        return Err(ParseError::UnexpectedToken {
            expected: "function signature",
            actual: signature.to_string(),
        });
    };
    if !signature.ends_with(')') {
        return Err(ParseError::UnexpectedToken {
            expected: "function signature",
            actual: signature.to_string(),
        });
    }
    let proc_name = signature[..open_paren].trim();
    if proc_name.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "function name",
            actual: signature.to_string(),
        });
    }
    let arg_sql = &signature[open_paren + 1..signature.len().saturating_sub(1)];
    let args = if arg_sql.trim().is_empty() {
        Vec::new()
    } else {
        arg_sql.split(',').map(str::trim).collect::<Vec<_>>()
    };
    Ok((proc_name, args))
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

fn ensure_function_signature_exists(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    configured_search_path: Option<&[String]>,
    signature: &str,
) -> Result<(), ExecError> {
    let catalog = db.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
    let (proc_name, arg_names) = parse_granted_function_signature(signature).map_err(ExecError::Parse)?;
    let (schema_name, base_name) = proc_name
        .rsplit_once('.')
        .map(|(schema, name)| (Some(schema.trim().to_ascii_lowercase()), name.trim()))
        .unwrap_or((None, proc_name));
    let desired_arg_oids = arg_names
        .into_iter()
        .map(|arg| {
            let raw_type = parse_type_name(arg)?;
            let sql_type = resolve_raw_type_name(&raw_type, &catalog)?;
            catalog
                .type_oid_for_sql_type(sql_type)
                .ok_or_else(|| ParseError::UnsupportedType(arg.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(ExecError::Parse)?;
    let schema_oid = match schema_name {
        Some(ref schema_name) => Some(
            db.visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("schema \"{schema_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "3F000",
                })?,
        ),
        None => None,
    };
    let normalized_name = base_name.trim_matches('"').to_ascii_lowercase();
    let exists = catalog.proc_rows_by_name(&normalized_name).into_iter().any(|row| {
        parse_proc_argtype_oids(&row.proargtypes) == Some(desired_arg_oids.clone())
            && schema_oid
                .map(|schema_oid| row.pronamespace == schema_oid)
                .unwrap_or(true)
    });
    if exists {
        Ok(())
    } else {
        Err(ExecError::DetailedError {
            message: format!("function {signature} does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42883",
        })
    }
}

impl Database {
    pub(crate) fn execute_grant_object_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &GrantObjectStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        match stmt.privilege {
            GrantObjectPrivilege::CreateOnDatabase => {
                self.execute_grant_database_create_stmt(client_id, stmt)
            }
            GrantObjectPrivilege::AllPrivilegesOnTable => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                catalog.lookup_relation(&stmt.object_name).ok_or_else(|| {
                    ExecError::Parse(ParseError::TableDoesNotExist(stmt.object_name.clone()))
                })?;
                Ok(StatementResult::AffectedRows(0))
            }
            GrantObjectPrivilege::SelectOnTable => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                catalog.lookup_relation(&stmt.object_name).ok_or_else(|| {
                    ExecError::Parse(ParseError::TableDoesNotExist(stmt.object_name.clone()))
                })?;
                Ok(StatementResult::AffectedRows(0))
            }
            GrantObjectPrivilege::AllPrivilegesOnSchema => {
                self.backend_catcache(client_id, None)
                    .map_err(map_catalog_error)?
                    .namespace_by_name(&stmt.object_name)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("schema \"{}\" does not exist", stmt.object_name),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    })?;
                Ok(StatementResult::AffectedRows(0))
            }
            GrantObjectPrivilege::ExecuteOnFunction => {
                ensure_function_signature_exists(
                    self,
                    client_id,
                    None,
                    configured_search_path,
                    &stmt.object_name,
                )?;
                Ok(StatementResult::AffectedRows(0))
            }
        }
    }

    pub(crate) fn execute_revoke_object_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &RevokeObjectStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        match stmt.privilege {
            GrantObjectPrivilege::CreateOnDatabase => {
                self.execute_revoke_database_create_stmt(client_id, stmt)
            }
            GrantObjectPrivilege::AllPrivilegesOnTable => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                catalog.lookup_relation(&stmt.object_name).ok_or_else(|| {
                    ExecError::Parse(ParseError::TableDoesNotExist(stmt.object_name.clone()))
                })?;
                Ok(StatementResult::AffectedRows(0))
            }
            GrantObjectPrivilege::SelectOnTable => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                catalog.lookup_relation(&stmt.object_name).ok_or_else(|| {
                    ExecError::Parse(ParseError::TableDoesNotExist(stmt.object_name.clone()))
                })?;
                Ok(StatementResult::AffectedRows(0))
            }
            GrantObjectPrivilege::AllPrivilegesOnSchema => {
                self.backend_catcache(client_id, None)
                    .map_err(map_catalog_error)?
                    .namespace_by_name(&stmt.object_name)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("schema \"{}\" does not exist", stmt.object_name),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    })?;
                Ok(StatementResult::AffectedRows(0))
            }
            GrantObjectPrivilege::ExecuteOnFunction => {
                ensure_function_signature_exists(
                    self,
                    client_id,
                    None,
                    configured_search_path,
                    &stmt.object_name,
                )?;
                Ok(StatementResult::AffectedRows(0))
            }
        }
    }

    pub(crate) fn execute_grant_role_membership_stmt(
        &self,
        client_id: ClientId,
        stmt: &GrantRoleMembershipStatement,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_grant_role_membership_stmt_in_transaction(
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

    pub(crate) fn execute_grant_role_membership_stmt_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &GrantRoleMembershipStatement,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let interrupts = self.interrupt_state(client_id);
        let mut current_cid = cid;

        for role_name in &stmt.role_names {
            for grantee_name in &stmt.grantee_names {
                let auth_catalog = self
                    .auth_catalog(client_id, Some((xid, current_cid)))
                    .map_err(map_role_grant_error)?;
                let role = if stmt.granted_by.is_some() {
                    lookup_membership_role(&auth_catalog, role_name)?
                } else {
                    authorize_grant_membership(&auth, &auth_catalog, role_name)?
                };
                let grantor_oid = resolve_role_grantor(
                    &auth,
                    &auth_catalog,
                    &role,
                    stmt.granted_by.as_ref(),
                    true,
                )?;
                let grantee = lookup_membership_grantee(&auth_catalog, grantee_name)?;
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: current_cid,
                    client_id,
                    waiter: None,
                    interrupts: interrupts.clone(),
                };
                upsert_role_membership_in_transaction(
                    self,
                    &auth_catalog,
                    role.oid,
                    grantee.oid,
                    grantor_oid,
                    stmt.admin_option,
                    stmt.inherit_option.unwrap_or(true),
                    stmt.set_option.unwrap_or(true),
                    &ctx,
                    catalog_effects,
                )?;
                current_cid = current_cid.saturating_add(1);
            }
        }

        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_revoke_role_membership_stmt(
        &self,
        client_id: ClientId,
        stmt: &RevokeRoleMembershipStatement,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_revoke_role_membership_stmt_in_transaction(
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

    pub(crate) fn execute_revoke_role_membership_stmt_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &RevokeRoleMembershipStatement,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let interrupts = self.interrupt_state(client_id);
        let mut current_cid = cid;

        for role_name in &stmt.role_names {
            for grantee_name in &stmt.grantee_names {
                let auth_catalog = self
                    .auth_catalog(client_id, Some((xid, current_cid)))
                    .map_err(map_role_grant_error)?;
                let role = lookup_membership_role(&auth_catalog, role_name)?;
                let grantor_oid = resolve_role_grantor(
                    &auth,
                    &auth_catalog,
                    &role,
                    stmt.granted_by.as_ref(),
                    false,
                )?;
                let role_rows = auth_catalog
                    .memberships()
                    .iter()
                    .filter(|row| row.roleid == role.oid)
                    .cloned()
                    .collect::<Vec<_>>();
                let grantee = lookup_membership_grantee(&auth_catalog, grantee_name)?;
                let existing_index = role_rows
                    .iter()
                    .position(|row| row.member == grantee.oid && row.grantor == grantor_oid)
                    .ok_or_else(|| {
                        ExecError::Parse(role_management_error(format!(
                            "role grant does not exist: \"{}\" to \"{}\"",
                            role.rolname, grantee.rolname
                        )))
                    })?;
                let planned_actions =
                    plan_role_membership_revoke(&role_rows, existing_index, stmt)?;
                for (row, action) in role_rows.iter().zip(planned_actions.iter()) {
                    let ctx = CatalogWriteContext {
                        pool: self.pool.clone(),
                        txns: self.txns.clone(),
                        xid,
                        cid: current_cid,
                        client_id,
                        waiter: None,
                        interrupts: interrupts.clone(),
                    };
                    match action {
                        PlannedRoleMembershipRevoke::Noop => {}
                        PlannedRoleMembershipRevoke::DeleteGrant => {
                            let (_, effect) = self
                                .shared_catalog
                                .write()
                                .revoke_role_membership_mvcc(
                                    row.roleid,
                                    row.member,
                                    row.grantor,
                                    &ctx,
                                )
                                .map_err(map_role_grant_error)?;
                            catalog_effects.push(effect);
                            current_cid = current_cid.saturating_add(1);
                        }
                        PlannedRoleMembershipRevoke::RemoveAdminOption => {
                            let (_, effect) = self
                                .shared_catalog
                                .write()
                                .update_role_membership_options_mvcc(
                                    row.roleid,
                                    row.member,
                                    row.grantor,
                                    false,
                                    row.inherit_option,
                                    row.set_option,
                                    &ctx,
                                )
                                .map_err(map_role_grant_error)?;
                            catalog_effects.push(effect);
                            current_cid = current_cid.saturating_add(1);
                        }
                        PlannedRoleMembershipRevoke::RemoveInheritOption => {
                            let (_, effect) = self
                                .shared_catalog
                                .write()
                                .update_role_membership_options_mvcc(
                                    row.roleid,
                                    row.member,
                                    row.grantor,
                                    row.admin_option,
                                    false,
                                    row.set_option,
                                    &ctx,
                                )
                                .map_err(map_role_grant_error)?;
                            catalog_effects.push(effect);
                            current_cid = current_cid.saturating_add(1);
                        }
                        PlannedRoleMembershipRevoke::RemoveSetOption => {
                            let (_, effect) = self
                                .shared_catalog
                                .write()
                                .update_role_membership_options_mvcc(
                                    row.roleid,
                                    row.member,
                                    row.grantor,
                                    row.admin_option,
                                    row.inherit_option,
                                    false,
                                    &ctx,
                                )
                                .map_err(map_role_grant_error)?;
                            catalog_effects.push(effect);
                            current_cid = current_cid.saturating_add(1);
                        }
                    }
                }
            }
        }

        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn user_has_database_create_privilege(
        &self,
        auth: &AuthState,
        auth_catalog: &AuthCatalog,
    ) -> bool {
        if auth_catalog
            .role_by_oid(auth.current_user_oid())
            .is_some_and(|row| row.rolsuper)
        {
            return true;
        }
        let grants = self.database_create_grants.read();
        auth_catalog.roles().iter().any(|role| {
            auth.has_effective_membership(role.oid, auth_catalog)
                && grants.iter().any(|grant| grant.grantee_oid == role.oid)
        })
    }
}

fn execute_database_name_matches_current(name: &str) -> bool {
    name.eq_ignore_ascii_case(CURRENT_DATABASE_NAME) || name.eq_ignore_ascii_case("regression")
}

fn current_database_owner_oid(db: &Database, client_id: ClientId) -> Result<u32, ExecError> {
    db.backend_catcache(client_id, None)
        .map_err(map_catalog_error)?
        .database_rows()
        .into_iter()
        .find(|row| row.oid == CURRENT_DATABASE_OID)
        .map(|row| row.datdba)
        .ok_or_else(|| ExecError::DetailedError {
            message: "current database does not exist".into(),
            detail: None,
            hint: None,
            sqlstate: "3D000",
        })
}

fn can_grant_database_create(
    db: &Database,
    auth: &AuthState,
    auth_catalog: &AuthCatalog,
    current_database_owner_oid: u32,
) -> bool {
    if auth_catalog
        .role_by_oid(auth.current_user_oid())
        .is_some_and(|row| row.rolsuper)
        || auth.current_user_oid() == current_database_owner_oid
    {
        return true;
    }
    let grants = db.database_create_grants.read();
    auth_catalog.roles().iter().any(|role| {
        auth.has_effective_membership(role.oid, auth_catalog)
            && grants
                .iter()
                .any(|grant| grant.grantee_oid == role.oid && grant.grant_option)
    })
}

fn can_revoke_database_create(
    grants: &[DatabaseCreateGrant],
    auth: &AuthState,
    auth_catalog: &AuthCatalog,
    current_database_owner_oid: u32,
    grantee_oid: u32,
) -> bool {
    if auth_catalog
        .role_by_oid(auth.current_user_oid())
        .is_some_and(|row| row.rolsuper)
        || auth.current_user_oid() == current_database_owner_oid
    {
        return true;
    }
    grants.iter().any(|grant| {
        grant.grantee_oid == grantee_oid && grant.grantor_oid == auth.current_user_oid()
    })
}

impl Database {
    fn execute_grant_database_create_stmt(
        &self,
        client_id: ClientId,
        stmt: &GrantObjectStatement,
    ) -> Result<StatementResult, ExecError> {
        if !execute_database_name_matches_current(&stmt.object_name) {
            return Err(ExecError::DetailedError {
                message: format!("database \"{}\" does not exist", stmt.object_name),
                detail: None,
                hint: None,
                sqlstate: "3D000",
            });
        }

        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, None)
            .map_err(map_catalog_error)?;
        let database_owner_oid = current_database_owner_oid(self, client_id)?;
        if !can_grant_database_create(self, &auth, &auth_catalog, database_owner_oid) {
            return Err(ExecError::DetailedError {
                message: "permission denied to grant CREATE on database".into(),
                detail: None,
                hint: None,
                sqlstate: "42501",
            });
        }

        let current_user_oid = auth.current_user_oid();
        let mut grants = self.database_create_grants.write();
        for grantee_name in &stmt.grantee_names {
            if grantee_name.eq_ignore_ascii_case("public") {
                continue;
            }
            let grantee = auth_catalog.role_by_name(grantee_name).ok_or_else(|| {
                ExecError::Parse(role_management_error(format!(
                    "role \"{}\" does not exist",
                    grantee_name
                )))
            })?;
            if let Some(existing) = grants.iter_mut().find(|grant| {
                grant.grantee_oid == grantee.oid && grant.grantor_oid == current_user_oid
            }) {
                existing.grant_option |= stmt.with_grant_option;
            } else {
                grants.push(DatabaseCreateGrant {
                    grantee_oid: grantee.oid,
                    grantor_oid: current_user_oid,
                    grant_option: stmt.with_grant_option,
                });
            }
        }
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_revoke_database_create_stmt(
        &self,
        client_id: ClientId,
        stmt: &RevokeObjectStatement,
    ) -> Result<StatementResult, ExecError> {
        if !execute_database_name_matches_current(&stmt.object_name) {
            return Err(ExecError::DetailedError {
                message: format!("database \"{}\" does not exist", stmt.object_name),
                detail: None,
                hint: None,
                sqlstate: "3D000",
            });
        }

        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, None)
            .map_err(map_catalog_error)?;
        let database_owner_oid = current_database_owner_oid(self, client_id)?;
        let current_user_oid = auth.current_user_oid();
        let is_owner_or_superuser = auth_catalog
            .role_by_oid(current_user_oid)
            .is_some_and(|row| row.rolsuper)
            || current_user_oid == database_owner_oid;
        let mut grants = self.database_create_grants.write();
        for grantee_name in &stmt.grantee_names {
            if grantee_name.eq_ignore_ascii_case("public") {
                continue;
            }
            let grantee = auth_catalog.role_by_name(grantee_name).ok_or_else(|| {
                ExecError::Parse(role_management_error(format!(
                    "role \"{}\" does not exist",
                    grantee_name
                )))
            })?;
            if !can_revoke_database_create(
                &grants,
                &auth,
                &auth_catalog,
                database_owner_oid,
                grantee.oid,
            ) {
                return Err(ExecError::DetailedError {
                    message: "permission denied to revoke CREATE on database".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42501",
                });
            }
            grants.retain(|grant| {
                grant.grantee_oid != grantee.oid
                    || (!is_owner_or_superuser && grant.grantor_oid != current_user_oid)
            });
        }
        let _ = stmt.cascade;
        Ok(StatementResult::AffectedRows(0))
    }
}

fn upsert_role_membership_in_transaction(
    db: &Database,
    auth_catalog: &AuthCatalog,
    roleid: u32,
    member: u32,
    grantor: u32,
    admin_option: bool,
    inherit_option: bool,
    set_option: bool,
    ctx: &CatalogWriteContext,
    catalog_effects: &mut Vec<CatalogMutationEffect>,
) -> Result<(), ExecError> {
    if auth_catalog
        .memberships()
        .iter()
        .any(|row| row.roleid == roleid && row.member == member && row.grantor == grantor)
    {
        let (_, effect) = db
            .shared_catalog
            .write()
            .update_role_membership_options_mvcc(
                roleid,
                member,
                grantor,
                admin_option,
                inherit_option,
                set_option,
                ctx,
            )
            .map_err(map_role_grant_error)?;
        catalog_effects.push(effect);
    } else {
        let (_, effect) = db
            .shared_catalog
            .write()
            .grant_role_membership_mvcc(
                &membership_row(
                    roleid,
                    member,
                    grantor,
                    admin_option,
                    inherit_option,
                    set_option,
                ),
                ctx,
            )
            .map_err(|err| {
                map_named_role_membership_error(
                    err,
                    member,
                    &member_name(db, auth_catalog, member),
                    roleid,
                    &role_name(db, auth_catalog, roleid),
                )
            })?;
        catalog_effects.push(effect);
    }
    Ok(())
}

fn lookup_membership_grantee(
    catalog: &AuthCatalog,
    role_name: &str,
) -> Result<PgAuthIdRow, ExecError> {
    let role = lookup_membership_role_by_name(catalog, role_name)?;
    if role.oid == crate::include::catalog::PG_DATABASE_OWNER_OID {
        return Err(ExecError::Parse(role_management_error(format!(
            "role \"{}\" cannot be a member of any role",
            role.rolname
        ))));
    }
    Ok(role)
}

fn lookup_membership_role(
    catalog: &AuthCatalog,
    role_name: &str,
) -> Result<PgAuthIdRow, ExecError> {
    let role = lookup_membership_role_by_name(catalog, role_name)?;
    if role.oid == crate::include::catalog::PG_DATABASE_OWNER_OID {
        return Err(ExecError::Parse(role_management_error(format!(
            "role \"{}\" cannot have explicit members",
            role.rolname
        ))));
    }
    Ok(role)
}

fn lookup_membership_role_by_name(
    catalog: &AuthCatalog,
    role_name: &str,
) -> Result<PgAuthIdRow, ExecError> {
    catalog.role_by_name(role_name).cloned().ok_or_else(|| {
        ExecError::Parse(role_management_error(format!(
            "role \"{role_name}\" does not exist"
        )))
    })
}

fn resolve_role_grantor(
    auth: &AuthState,
    catalog: &AuthCatalog,
    role: &PgAuthIdRow,
    grantor: Option<&RoleGrantorSpec>,
    is_grant: bool,
) -> Result<u32, ExecError> {
    let Some(grantor) = grantor else {
        return Ok(auth.current_user_oid());
    };
    let grantor = resolve_role_grantor_spec(auth, catalog, grantor)?;

    if is_grant {
        if !auth.has_effective_membership(grantor.oid, catalog) {
            return Err(ExecError::DetailedError {
                message: format!(
                    "permission denied to grant privileges as role \"{}\"",
                    grantor.rolname
                ),
                detail: Some(format!(
                    "Only roles with privileges of role \"{}\" may grant privileges as this role.",
                    grantor.rolname
                )),
                hint: None,
                sqlstate: "42501",
            });
        }
        if grantor.oid != BOOTSTRAP_SUPERUSER_OID
            && !catalog
                .memberships()
                .iter()
                .any(|row| row.roleid == role.oid && row.member == grantor.oid && row.admin_option)
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "permission denied to grant privileges as role \"{}\"",
                    grantor.rolname
                ),
                detail: Some(format!(
                    "The grantor must have the ADMIN option on role \"{}\".",
                    role.rolname
                )),
                hint: None,
                sqlstate: "42501",
            });
        }
    } else if !auth.has_effective_membership(grantor.oid, catalog) {
        return Err(ExecError::DetailedError {
            message: format!(
                "permission denied to revoke privileges granted by role \"{}\"",
                grantor.rolname
            ),
            detail: Some(format!(
                "Only roles with privileges of role \"{}\" may revoke privileges granted by this role.",
                grantor.rolname
            )),
            hint: None,
            sqlstate: "42501",
        });
    }

    Ok(grantor.oid)
}

fn resolve_role_grantor_spec(
    auth: &AuthState,
    catalog: &AuthCatalog,
    grantor: &RoleGrantorSpec,
) -> Result<PgAuthIdRow, ExecError> {
    match grantor {
        RoleGrantorSpec::CurrentUser | RoleGrantorSpec::CurrentRole => catalog
            .role_by_oid(auth.current_user_oid())
            .cloned()
            .ok_or_else(|| ExecError::Parse(role_management_error("current role does not exist"))),
        RoleGrantorSpec::RoleName(role_name) => {
            catalog.role_by_name(role_name).cloned().ok_or_else(|| {
                ExecError::Parse(role_management_error(format!(
                    "role \"{}\" does not exist",
                    role_name
                )))
            })
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PlannedRoleMembershipRevoke {
    Noop,
    DeleteGrant,
    RemoveAdminOption,
    RemoveInheritOption,
    RemoveSetOption,
}

fn plan_role_membership_revoke(
    role_rows: &[crate::include::catalog::PgAuthMembersRow],
    target_index: usize,
    stmt: &RevokeRoleMembershipStatement,
) -> Result<Vec<PlannedRoleMembershipRevoke>, ExecError> {
    let mut actions = vec![PlannedRoleMembershipRevoke::Noop; role_rows.len()];
    if stmt.inherit_option {
        actions[target_index] = PlannedRoleMembershipRevoke::RemoveInheritOption;
        return Ok(actions);
    }
    if stmt.set_option {
        actions[target_index] = PlannedRoleMembershipRevoke::RemoveSetOption;
        return Ok(actions);
    }
    let revoke_admin_option_only = stmt.admin_option;
    plan_recursive_role_revoke(
        role_rows,
        &mut actions,
        target_index,
        revoke_admin_option_only,
        stmt.cascade,
    )?;
    Ok(actions)
}

fn plan_recursive_role_revoke(
    role_rows: &[crate::include::catalog::PgAuthMembersRow],
    actions: &mut [PlannedRoleMembershipRevoke],
    index: usize,
    revoke_admin_option_only: bool,
    cascade: bool,
) -> Result<(), ExecError> {
    if actions[index] == PlannedRoleMembershipRevoke::DeleteGrant {
        return Ok(());
    }
    if actions[index] == PlannedRoleMembershipRevoke::RemoveAdminOption && revoke_admin_option_only
    {
        return Ok(());
    }

    let row = &role_rows[index];
    if !revoke_admin_option_only {
        actions[index] = PlannedRoleMembershipRevoke::DeleteGrant;
        if !row.admin_option {
            return Ok(());
        }
    } else {
        if !row.admin_option {
            return Ok(());
        }
        actions[index] = PlannedRoleMembershipRevoke::RemoveAdminOption;
    }

    let would_still_have_admin_option = role_rows.iter().enumerate().any(|(other_index, other)| {
        other_index != index
            && other.member == row.member
            && other.admin_option
            && actions[other_index] == PlannedRoleMembershipRevoke::Noop
    });
    if would_still_have_admin_option {
        return Ok(());
    }

    for (other_index, other) in role_rows.iter().enumerate() {
        if other.grantor == row.member
            && actions[other_index] != PlannedRoleMembershipRevoke::DeleteGrant
        {
            if !cascade {
                return Err(ExecError::DetailedError {
                    message: "dependent privileges exist".into(),
                    detail: None,
                    hint: Some("Use CASCADE to revoke them too.".into()),
                    sqlstate: "2BP01",
                });
            }
            plan_recursive_role_revoke(role_rows, actions, other_index, false, cascade)?;
        }
    }

    Ok(())
}

fn map_role_grant_error(err: crate::backend::catalog::CatalogError) -> ExecError {
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

fn authorize_grant_membership(
    auth: &AuthState,
    catalog: &AuthCatalog,
    role_name: &str,
) -> Result<PgAuthIdRow, ExecError> {
    grant_membership_authorized_with_detail(auth, catalog, role_name).map_err(|err| match err {
        GrantMembershipAuthorizationError::Parse(err) => ExecError::Parse(err),
        GrantMembershipAuthorizationError::PermissionDenied { role_name, detail } => {
            ExecError::DetailedError {
                message: format!("permission denied to grant role \"{role_name}\""),
                detail,
                hint: None,
                sqlstate: "42501",
            }
        }
    })
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
        other => map_role_grant_error(other),
    }
}

fn role_name(_db: &Database, auth_catalog: &AuthCatalog, role_oid: u32) -> String {
    auth_catalog
        .role_by_oid(role_oid)
        .map(|row| row.rolname.clone())
        .unwrap_or_else(|| role_oid.to_string())
}

fn member_name(db: &Database, auth_catalog: &AuthCatalog, member_oid: u32) -> String {
    role_name(db, auth_catalog, member_oid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::executor::StatementResult;
    use crate::pgrust::session::Session;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "pgrust_privilege_{}_{}_{}",
            label,
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn role_oid(db: &Database, role_name: &str) -> u32 {
        db.catalog
            .read()
            .catcache()
            .unwrap()
            .authid_rows()
            .into_iter()
            .find(|row| row.rolname == role_name)
            .unwrap()
            .oid
    }

    #[test]
    fn database_create_grant_allows_create_schema() {
        let base = temp_dir("db_create_grant");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role tenant login").unwrap();
        session
            .execute(
                &db,
                "grant create on database regression to tenant with grant option",
            )
            .unwrap();
        session
            .execute(&db, "set session authorization tenant")
            .unwrap();
        assert_eq!(
            session.execute(&db, "create schema tenant_schema").unwrap(),
            StatementResult::AffectedRows(0)
        );
    }

    #[test]
    fn grant_role_membership_updates_existing_options() {
        let base = temp_dir("grant_role_options");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "set createrole_self_grant to 'set, inherit'")
            .unwrap();
        session
            .execute(&db, "create role creator createrole noinherit")
            .unwrap();
        session
            .execute(&db, "set session authorization creator")
            .unwrap();
        session.execute(&db, "create role tenant2").unwrap();
        session
            .execute(&db, "grant tenant2 to creator with inherit true, set false")
            .unwrap();

        let tenant2_oid = role_oid(&db, "tenant2");
        let creator_oid = role_oid(&db, "creator");
        let membership = db
            .catalog
            .read()
            .catcache()
            .unwrap()
            .auth_members_rows()
            .into_iter()
            .find(|row| {
                row.roleid == tenant2_oid && row.member == creator_oid && row.grantor == creator_oid
            })
            .unwrap();
        assert!(membership.inherit_option);
        assert!(!membership.set_option);
    }

    #[test]
    fn grant_role_membership_records_explicit_grantor() {
        let base = temp_dir("grant_role_grantor");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role grantor").unwrap();
        session.execute(&db, "create role grantee").unwrap();
        session
            .execute(&db, "grant parent to grantor with admin option")
            .unwrap();
        session
            .execute(&db, "grant parent to grantee granted by grantor")
            .unwrap();

        let parent_oid = role_oid(&db, "parent");
        let grantor_oid = role_oid(&db, "grantor");
        let grantee_oid = role_oid(&db, "grantee");
        let membership = db
            .catalog
            .read()
            .catcache()
            .unwrap()
            .auth_members_rows()
            .into_iter()
            .find(|row| {
                row.roleid == parent_oid && row.member == grantee_oid && row.grantor == grantor_oid
            })
            .unwrap();
        assert!(!membership.admin_option);
    }

    #[test]
    fn explicit_role_grantor_must_have_admin_option() {
        let base = temp_dir("grant_role_grantor_admin");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role grantor").unwrap();
        session.execute(&db, "create role grantee").unwrap();

        let err = session
            .execute(&db, "grant parent to grantee granted by grantor")
            .unwrap_err();
        match err {
            ExecError::DetailedError {
                message, detail, ..
            } => {
                assert_eq!(
                    message,
                    "permission denied to grant privileges as role \"grantor\""
                );
                assert_eq!(
                    detail.as_deref(),
                    Some("The grantor must have the ADMIN option on role \"parent\".")
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn plain_revoke_role_membership_removes_explicit_grant() {
        let base = temp_dir("revoke_role_grantor");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role grantor").unwrap();
        session.execute(&db, "create role grantee").unwrap();
        session
            .execute(&db, "grant parent to grantor with admin option")
            .unwrap();
        session
            .execute(&db, "grant parent to grantee granted by grantor")
            .unwrap();
        session
            .execute(&db, "revoke parent from grantee granted by grantor")
            .unwrap();

        let parent_oid = role_oid(&db, "parent");
        let grantor_oid = role_oid(&db, "grantor");
        let grantee_oid = role_oid(&db, "grantee");
        assert!(
            !db.catalog
                .read()
                .catcache()
                .unwrap()
                .auth_members_rows()
                .into_iter()
                .any(|row| {
                    row.roleid == parent_oid
                        && row.member == grantee_oid
                        && row.grantor == grantor_oid
                })
        );
    }

    #[test]
    fn revoke_role_membership_requires_cascade_for_dependent_grants() {
        let base = temp_dir("revoke_role_grantor_dependents");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role grantor").unwrap();
        session.execute(&db, "create role grantee").unwrap();
        session.execute(&db, "create role child").unwrap();
        session
            .execute(&db, "grant parent to grantor with admin option")
            .unwrap();
        session
            .execute(
                &db,
                "grant parent to grantee with admin true granted by grantor",
            )
            .unwrap();
        session
            .execute(&db, "grant parent to child granted by grantee")
            .unwrap();

        let err = session
            .execute(&db, "revoke parent from grantee granted by grantor")
            .unwrap_err();
        match err {
            ExecError::DetailedError { message, hint, .. } => {
                assert_eq!(message, "dependent privileges exist");
                assert_eq!(hint.as_deref(), Some("Use CASCADE to revoke them too."));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn revoke_role_membership_cascade_removes_dependent_grants() {
        let base = temp_dir("revoke_role_grantor_cascade");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role grantor").unwrap();
        session.execute(&db, "create role grantee").unwrap();
        session.execute(&db, "create role child").unwrap();
        session
            .execute(&db, "grant parent to grantor with admin option")
            .unwrap();
        session
            .execute(
                &db,
                "grant parent to grantee with admin true granted by grantor",
            )
            .unwrap();
        session
            .execute(&db, "grant parent to child granted by grantee")
            .unwrap();
        session
            .execute(&db, "revoke parent from grantee granted by grantor cascade")
            .unwrap();

        let parent_oid = role_oid(&db, "parent");
        let grantor_oid = role_oid(&db, "grantor");
        let grantee_oid = role_oid(&db, "grantee");
        let child_oid = role_oid(&db, "child");
        let rows = db.catalog.read().catcache().unwrap().auth_members_rows();
        assert!(!rows.iter().any(|row| {
            row.roleid == parent_oid && row.member == grantee_oid && row.grantor == grantor_oid
        }));
        assert!(!rows.iter().any(|row| {
            row.roleid == parent_oid && row.member == child_oid && row.grantor == grantee_oid
        }));
    }
}
