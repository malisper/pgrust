use super::super::*;
use crate::backend::catalog::roles::find_role_by_name;
use crate::backend::commands::rolecmds::{
    build_alter_role_spec, build_create_role_spec, can_rename_role, grant_membership_authorized,
    membership_row, normalize_drop_role_names, parse_createrole_self_grant, role_management_error,
};
use crate::backend::parser::{
    AlterRoleAction, AlterRoleStatement, CreateRoleStatement, DropRoleStatement,
};

impl Database {
    pub(crate) fn execute_create_role_stmt(
        &self,
        client_id: ClientId,
        stmt: &CreateRoleStatement,
        createrole_self_grant: Option<&str>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, None)
            .map_err(map_role_catalog_error)?;
        let spec = build_create_role_spec(stmt).map_err(ExecError::Parse)?;
        if !auth.can_create_role_with_attrs(&spec.attrs, &auth_catalog) {
            return Err(ExecError::Parse(role_management_error(
                "permission denied to create role",
            )));
        }
        self.catalog
            .write()
            .create_role(&stmt.role_name, &spec.attrs)
            .map_err(map_role_catalog_error)?;

        let current_user_oid = auth.current_user_oid();
        let created = self
            .catalog
            .read()
            .catcache()
            .map_err(map_role_catalog_error)?
            .authid_rows()
            .into_iter()
            .find(|row| row.rolname.eq_ignore_ascii_case(&stmt.role_name))
            .ok_or_else(|| ExecError::Parse(role_management_error("created role missing")))?;

        if !auth_catalog
            .role_by_oid(current_user_oid)
            .is_some_and(|row| row.rolsuper)
        {
            self.catalog
                .write()
                .grant_role_membership(&membership_row(
                    created.oid,
                    current_user_oid,
                    crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                    true,
                    false,
                    false,
                ))
                .map_err(map_role_catalog_error)?;

            if let Some(raw) = createrole_self_grant {
                if let Some(options) = parse_createrole_self_grant(raw).map_err(ExecError::Parse)? {
                    self.catalog
                        .write()
                        .grant_role_membership(&membership_row(
                            created.oid,
                            current_user_oid,
                            current_user_oid,
                            false,
                            options.inherit,
                            options.set,
                        ))
                        .map_err(map_role_catalog_error)?;
                }
            }
        }

        let live_catalog = self
            .auth_catalog(client_id, None)
            .map_err(map_role_catalog_error)?;
        for role_name in &spec.add_role_to {
            let parent =
                grant_membership_authorized(&auth, &live_catalog, role_name).map_err(ExecError::Parse)?;
            self.catalog
                .write()
                .grant_role_membership(&membership_row(
                    parent.oid,
                    created.oid,
                    current_user_oid,
                    false,
                    false,
                    true,
                ))
                .map_err(map_role_catalog_error)?;
        }
        for member_name in &spec.role_members {
            let member = lookup_membership_member(&live_catalog, member_name)?;
            self.catalog
                .write()
                .grant_role_membership(&membership_row(
                    created.oid,
                    member.oid,
                    current_user_oid,
                    false,
                    false,
                    true,
                ))
                .map_err(map_role_catalog_error)?;
        }
        for member_name in &spec.admin_members {
            let member = lookup_membership_member(&live_catalog, member_name)?;
            self.catalog
                .write()
                .grant_role_membership(&membership_row(
                    created.oid,
                    member.oid,
                    current_user_oid,
                    true,
                    false,
                    true,
                ))
                .map_err(map_role_catalog_error)?;
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_role_stmt(
        &self,
        client_id: ClientId,
        stmt: &AlterRoleStatement,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, None)
            .map_err(map_role_catalog_error)?;
        let existing = find_role_by_name(auth_catalog.roles(), &stmt.role_name)
            .cloned()
            .ok_or_else(|| ExecError::Parse(role_management_error(format!(
                "role \"{}\" does not exist",
                stmt.role_name
            ))))?;

        match &stmt.action {
            AlterRoleAction::Rename { new_name } => {
                if !can_rename_role(&auth, existing.oid, &auth_catalog) {
                    return Err(ExecError::Parse(role_management_error(
                        "permission denied to rename role",
                    )));
                }
                self.catalog
                    .write()
                    .rename_role(&stmt.role_name, new_name)
                    .map_err(map_role_catalog_error)?;
            }
            AlterRoleAction::Options(_) => {
                let spec =
                    build_alter_role_spec(stmt, &existing).map_err(ExecError::Parse)?.unwrap();
                if !auth.can_alter_role_attrs(existing.oid, &spec.attrs, &auth_catalog) {
                    return Err(ExecError::Parse(role_management_error(
                        "permission denied to alter role",
                    )));
                }
                self.catalog
                    .write()
                    .alter_role_attributes(&stmt.role_name, &spec.attrs)
                    .map_err(map_role_catalog_error)?;
            }
        }

        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_role_stmt(
        &self,
        client_id: ClientId,
        stmt: &DropRoleStatement,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, None)
            .map_err(map_role_catalog_error)?;

        for role_name in normalize_drop_role_names(stmt) {
            let Some(existing) = find_role_by_name(auth_catalog.roles(), &role_name).cloned() else {
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
                return Err(ExecError::Parse(role_management_error(
                    "permission denied to drop role",
                )));
            }
            self.catalog
                .write()
                .drop_role(&role_name)
                .map_err(map_role_catalog_error)?;
        }

        Ok(StatementResult::AffectedRows(0))
    }
}

fn map_role_catalog_error(err: crate::backend::catalog::CatalogError) -> ExecError {
    match err {
        crate::backend::catalog::CatalogError::UniqueViolation(message) => {
            ExecError::Parse(role_management_error(message))
        }
        crate::backend::catalog::CatalogError::UnknownTable(name) => {
            ExecError::Parse(role_management_error(format!("role \"{name}\" does not exist")))
        }
        other => ExecError::Parse(role_management_error(format!("{other:?}"))),
    }
}

fn lookup_membership_member(
    catalog: &crate::pgrust::auth::AuthCatalog,
    role_name: &str,
) -> Result<crate::include::catalog::PgAuthIdRow, ExecError> {
    let role = find_role_by_name(catalog.roles(), role_name)
        .cloned()
        .ok_or_else(|| ExecError::Parse(role_management_error(format!(
            "role \"{role_name}\" does not exist"
        ))))?;
    if role.oid == crate::include::catalog::PG_DATABASE_OWNER_OID {
        return Err(ExecError::Parse(role_management_error(format!(
            "role \"{}\" cannot be a member of any role",
            role.rolname
        ))));
    }
    Ok(role)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::executor::StatementResult;
    use crate::backend::catalog::role_memberships::memberships_for_member;
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

    fn role_row(db: &Database, role_name: &str) -> PgAuthIdRow {
        db.catalog
            .read()
            .catcache()
            .unwrap()
            .authid_rows()
            .into_iter()
            .find(|row| row.rolname == role_name)
            .unwrap()
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
            db.catalog
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

        let catcache = db.catalog.read().catcache().unwrap();
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
        assert!(child_members.iter().any(|row| row.roleid == child_oid && !row.admin_option));
        let admin_members = memberships_for_member(&catcache.auth_members_rows(), admin_oid);
        assert!(admin_members.iter().any(|row| row.roleid == child_oid && row.admin_option));
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

        let catcache = db.catalog.read().catcache().unwrap();
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
}
