use super::super::*;
use crate::backend::catalog::roles::find_role_by_name;
use crate::backend::commands::rolecmds::{
    build_alter_role_spec, build_create_role_spec, can_rename_role, grant_membership_authorized,
    membership_row, normalize_drop_role_names, parse_createrole_self_grant, role_management_error,
};
use crate::backend::parser::{
    AlterRoleAction, AlterRoleStatement, CreateRoleStatement, DropRoleStatement,
    ReassignOwnedStatement,
};
use std::collections::{BTreeMap, BTreeSet};

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
        let mut touched_catalogs = vec![crate::include::catalog::BootstrapCatalogKind::PgAuthId];
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
            touched_catalogs.push(crate::include::catalog::BootstrapCatalogKind::PgAuthMembers);

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
                    touched_catalogs
                        .push(crate::include::catalog::BootstrapCatalogKind::PgAuthMembers);
                }
            }
        }

        let live_catalog = live_auth_catalog(self).map_err(map_role_catalog_error)?;
        for role_name in &spec.add_role_to {
            let parent = grant_membership_authorized(&auth, &live_catalog, role_name)
                .map_err(ExecError::Parse)?;
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
            touched_catalogs.push(crate::include::catalog::BootstrapCatalogKind::PgAuthMembers);
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
            touched_catalogs.push(crate::include::catalog::BootstrapCatalogKind::PgAuthMembers);
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
            touched_catalogs.push(crate::include::catalog::BootstrapCatalogKind::PgAuthMembers);
        }
        publish_direct_catalog_invalidation(self, client_id, &touched_catalogs);
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
            .ok_or_else(|| {
                ExecError::Parse(role_management_error(format!(
                    "role \"{}\" does not exist",
                    stmt.role_name
                )))
            })?;

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
                publish_direct_catalog_invalidation(
                    self,
                    client_id,
                    &[crate::include::catalog::BootstrapCatalogKind::PgAuthId],
                );
            }
            AlterRoleAction::Options(_) => {
                let spec = build_alter_role_spec(stmt, &existing)
                    .map_err(ExecError::Parse)?
                    .unwrap();
                if !auth.can_alter_role_attrs(existing.oid, &spec.attrs, &auth_catalog) {
                    return Err(ExecError::Parse(role_management_error(
                        "permission denied to alter role",
                    )));
                }
                self.catalog
                    .write()
                    .alter_role_attributes(&stmt.role_name, &spec.attrs)
                    .map_err(map_role_catalog_error)?;
                publish_direct_catalog_invalidation(
                    self,
                    client_id,
                    &[crate::include::catalog::BootstrapCatalogKind::PgAuthId],
                );
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
        let mut dropped_any = false;

        for role_name in normalize_drop_role_names(stmt) {
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
                return Err(ExecError::Parse(role_management_error(
                    "permission denied to drop role",
                )));
            }
            let owned_objects = owned_objects_for_roles(self, client_id, &[existing.oid])?;
            if !owned_objects.is_empty() {
                let detail = owned_objects
                    .iter()
                    .filter(|object| object.relkind != 'i')
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
            self.catalog
                .write()
                .drop_role(&role_name)
                .map_err(map_role_catalog_error)?;
            dropped_any = true;
        }

        if dropped_any {
            publish_direct_catalog_invalidation(
                self,
                client_id,
                &[
                    crate::include::catalog::BootstrapCatalogKind::PgAuthId,
                    crate::include::catalog::BootstrapCatalogKind::PgAuthMembers,
                ],
            );
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
            .auth_catalog(client_id, None)
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
            let effect = self
                .catalog
                .write()
                .alter_relation_owner_mvcc(object.relation_oid, new_role.oid, &ctx)
                .map_err(map_role_catalog_error)?;
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

fn live_auth_catalog(db: &Database) -> Result<crate::pgrust::auth::AuthCatalog, CatalogError> {
    let cache = db.catalog.read().catcache()?;
    Ok(crate::pgrust::auth::AuthCatalog::new(
        cache.authid_rows(),
        cache.auth_members_rows(),
    ))
}

fn publish_direct_catalog_invalidation(
    db: &Database,
    client_id: ClientId,
    kinds: &[crate::include::catalog::BootstrapCatalogKind],
) {
    let invalidation = crate::backend::utils::cache::inval::CatalogInvalidation {
        touched_catalogs: kinds.iter().copied().collect(),
        ..Default::default()
    };
    db.publish_committed_catalog_invalidation(client_id, &invalidation);
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct OwnedObject {
    relation_oid: u32,
    relkind: char,
    name: String,
}

impl OwnedObject {
    fn kind_name(&self) -> &'static str {
        match self.relkind {
            'v' => "view",
            'i' => "index",
            _ => "table",
        }
    }

    fn drop_detail(&self) -> String {
        format!("owner of {} {}", self.kind_name(), self.name)
    }
}

fn owned_objects_for_roles(
    db: &Database,
    client_id: ClientId,
    role_oids: &[u32],
) -> Result<Vec<OwnedObject>, ExecError> {
    let role_oids = role_oids.iter().copied().collect::<BTreeSet<_>>();
    let catcache = db
        .backend_catcache(client_id, None)
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
        .filter(|row| matches!(row.relkind, 'r' | 'v' | 'i'))
        .map(|row| OwnedObject {
            relation_oid: row.oid,
            relkind: row.relkind,
            name: match namespaces.get(&row.relnamespace).map(String::as_str) {
                Some("public") | Some("pg_catalog") | None => row.relname,
                Some(schema) => format!("{schema}.{}", row.relname),
            },
        })
        .collect::<Vec<_>>();
    objects.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then(left.relkind.cmp(&right.relkind))
    });
    Ok(objects)
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

    fn relation_oid(db: &Database, relname: &str) -> u32 {
        db.backend_catcache(1, None)
            .unwrap()
            .class_rows()
            .into_iter()
            .find(|row| row.relname == relname)
            .map(|row| row.oid)
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
        db.finish_txn(1, xid, Ok(StatementResult::AffectedRows(0)), &[effect], &[], &[])
            .unwrap();
    }

    fn update_membership_options(
        db: &Database,
        role_name: &str,
        member_name: &str,
        grantor_name: &str,
        inherit_option: bool,
        set_option: bool,
    ) {
        let role_id = role_oid(db, role_name);
        let member_id = role_oid(db, member_name);
        let grantor_id = role_oid(db, grantor_name);
        db.catalog
            .write()
            .update_role_membership_options(
                role_id,
                member_id,
                grantor_id,
                false,
                inherit_option,
                set_option,
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
        let target_oid = role_oid(&db, "target");
        let target2_oid = role_oid(&db, "target2");

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

        for role_oid in [tenant_oid, target_oid, target2_oid] {
            db.catalog
                .write()
                .grant_role_membership(&membership_row(
                    role_oid,
                    limited_oid,
                    limited_oid,
                    false,
                    true,
                    true,
                ))
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

        let limited_oid = role_oid(&db, "limited_admin");
        let tenant2_oid = role_oid(&db, "tenant2");
        let target2_oid = role_oid(&db, "target2");
        db.catalog
            .write()
            .grant_role_membership(&membership_row(
                tenant2_oid,
                limited_oid,
                limited_oid,
                false,
                true,
                true,
            ))
            .unwrap();
        db.catalog
            .write()
            .grant_role_membership(&membership_row(
                target2_oid,
                limited_oid,
                limited_oid,
                false,
                true,
                true,
            ))
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
}
