use super::super::*;
use crate::backend::commands::rolecmds::{
    GrantMembershipAuthorizationError, grant_membership_authorized_with_detail, membership_row,
    role_management_error,
};
use crate::backend::parser::{
    GrantObjectPrivilege, GrantObjectStatement, GrantRoleMembershipStatement,
    RevokeObjectStatement, RevokeRoleMembershipStatement,
};
use crate::include::catalog::{CURRENT_DATABASE_NAME, CURRENT_DATABASE_OID, PgAuthIdRow};

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
        }
    }

    pub(crate) fn execute_grant_role_membership_stmt(
        &self,
        client_id: ClientId,
        stmt: &GrantRoleMembershipStatement,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, None)
            .map_err(map_role_grant_error)?;
        let current_user_oid = auth.current_user_oid();
        let mut touched = false;

        for role_name in &stmt.role_names {
            let role = authorize_grant_membership(&auth, &auth_catalog, role_name)?;
            for grantee_name in &stmt.grantee_names {
                let grantee = lookup_membership_grantee(&auth_catalog, grantee_name)?;
                let admin_option = stmt.admin_option;
                let inherit_option = stmt.inherit_option.unwrap_or(true);
                let set_option = stmt.set_option.unwrap_or(true);
                upsert_role_membership(
                    self,
                    &auth_catalog,
                    role.oid,
                    grantee.oid,
                    current_user_oid,
                    admin_option,
                    inherit_option,
                    set_option,
                )?;
                touched = true;
            }
        }

        if touched {
            publish_direct_auth_members_invalidation(self, client_id);
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_revoke_role_membership_stmt(
        &self,
        client_id: ClientId,
        stmt: &RevokeRoleMembershipStatement,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, None)
            .map_err(map_role_grant_error)?;
        let current_user_oid = auth.current_user_oid();
        let mut touched = false;

        for role_name in &stmt.role_names {
            let role = authorize_grant_membership(&auth, &auth_catalog, role_name)?;
            for grantee_name in &stmt.grantee_names {
                let grantee = lookup_membership_grantee(&auth_catalog, grantee_name)?;
                let existing = auth_catalog
                    .memberships()
                    .iter()
                    .find(|row| {
                        row.roleid == role.oid
                            && row.member == grantee.oid
                            && row.grantor == current_user_oid
                    })
                    .cloned()
                    .ok_or_else(|| {
                        ExecError::Parse(role_management_error(format!(
                            "role grant does not exist: \"{}\" to \"{}\"",
                            role.rolname, grantee.rolname
                        )))
                    })?;
                self.catalog
                    .write()
                    .update_role_membership_options(
                        role.oid,
                        grantee.oid,
                        current_user_oid,
                        if stmt.admin_option {
                            false
                        } else {
                            existing.admin_option
                        },
                        if stmt.inherit_option {
                            false
                        } else {
                            existing.inherit_option
                        },
                        if stmt.set_option {
                            false
                        } else {
                            existing.set_option
                        },
                    )
                    .map_err(map_role_grant_error)?;
                touched = true;
            }
        }

        if touched {
            publish_direct_auth_members_invalidation(self, client_id);
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

fn upsert_role_membership(
    db: &Database,
    auth_catalog: &AuthCatalog,
    roleid: u32,
    member: u32,
    grantor: u32,
    admin_option: bool,
    inherit_option: bool,
    set_option: bool,
) -> Result<(), ExecError> {
    if auth_catalog
        .memberships()
        .iter()
        .any(|row| row.roleid == roleid && row.member == member && row.grantor == grantor)
    {
        db.catalog
            .write()
            .update_role_membership_options(
                roleid,
                member,
                grantor,
                admin_option,
                inherit_option,
                set_option,
            )
            .map_err(map_role_grant_error)?;
    } else {
        db.catalog
            .write()
            .grant_role_membership(&membership_row(
                roleid,
                member,
                grantor,
                admin_option,
                inherit_option,
                set_option,
            ))
            .map_err(|err| {
                map_named_role_membership_error(
                    err,
                    member,
                    &member_name(db, auth_catalog, member),
                    roleid,
                    &role_name(db, auth_catalog, roleid),
                )
            })?;
    }
    Ok(())
}

fn lookup_membership_grantee(
    catalog: &AuthCatalog,
    role_name: &str,
) -> Result<PgAuthIdRow, ExecError> {
    let role = catalog.role_by_name(role_name).cloned().ok_or_else(|| {
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

fn publish_direct_auth_members_invalidation(db: &Database, client_id: ClientId) {
    let kind = crate::include::catalog::BootstrapCatalogKind::PgAuthMembers;
    let _ = db
        .pool
        .invalidate_relation(crate::backend::storage::smgr::RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: kind.relation_oid(),
        });
    let invalidation = crate::backend::utils::cache::inval::CatalogInvalidation {
        touched_catalogs: [kind].into_iter().collect(),
        ..Default::default()
    };
    db.publish_committed_catalog_invalidation(client_id, &invalidation);
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
}
