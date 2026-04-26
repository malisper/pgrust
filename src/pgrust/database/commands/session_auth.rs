use super::super::*;
use crate::backend::catalog::roles::find_role_by_name;
use crate::backend::commands::rolecmds::role_management_error;
use crate::backend::parser::{
    ResetRoleStatement, ResetSessionAuthorizationStatement, SetRoleStatement,
    SetSessionAuthorizationStatement,
};
use crate::pgrust::auth::AuthState;

impl Database {
    pub(crate) fn execute_set_session_authorization_stmt(
        &self,
        client_id: ClientId,
        stmt: &SetSessionAuthorizationStatement,
    ) -> Result<AuthState, ExecError> {
        self.execute_set_session_authorization_stmt_with_txn(client_id, stmt, None)
    }

    pub(crate) fn execute_set_session_authorization_stmt_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &SetSessionAuthorizationStatement,
        xid: TransactionId,
        cid: CommandId,
    ) -> Result<AuthState, ExecError> {
        self.execute_set_session_authorization_stmt_with_txn(client_id, stmt, Some((xid, cid)))
    }

    fn execute_set_session_authorization_stmt_with_txn(
        &self,
        client_id: ClientId,
        stmt: &SetSessionAuthorizationStatement,
        txn_ctx: Option<(TransactionId, CommandId)>,
    ) -> Result<AuthState, ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, txn_ctx)
            .map_err(map_session_auth_error)?;
        let target = find_role_by_name(auth_catalog.roles(), &stmt.role_name)
            .cloned()
            .ok_or_else(|| {
                ExecError::Parse(role_management_error(format!(
                    "role \"{}\" does not exist",
                    stmt.role_name
                )))
            })?;
        if !auth.can_set_session_authorization(target.oid, &auth_catalog) {
            return Err(ExecError::Parse(role_management_error(format!(
                "permission denied to set session authorization to \"{}\"",
                target.rolname
            ))));
        }

        let mut next = auth.clone();
        next.set_session_authorization(target.oid);
        self.install_auth_state(client_id, next.clone());
        self.plan_cache.invalidate_all();
        Ok(next)
    }

    pub(crate) fn execute_reset_session_authorization_stmt(
        &self,
        client_id: ClientId,
        _stmt: &ResetSessionAuthorizationStatement,
    ) -> Result<AuthState, ExecError> {
        let mut next = self.auth_state(client_id);
        next.reset_session_authorization();
        self.install_auth_state(client_id, next.clone());
        self.plan_cache.invalidate_all();
        Ok(next)
    }

    pub(crate) fn execute_set_role_stmt(
        &self,
        client_id: ClientId,
        stmt: &SetRoleStatement,
    ) -> Result<AuthState, ExecError> {
        self.execute_set_role_stmt_with_txn(client_id, stmt, None)
    }

    pub(crate) fn execute_set_role_stmt_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &SetRoleStatement,
        xid: TransactionId,
        cid: CommandId,
    ) -> Result<AuthState, ExecError> {
        self.execute_set_role_stmt_with_txn(client_id, stmt, Some((xid, cid)))
    }

    fn execute_set_role_stmt_with_txn(
        &self,
        client_id: ClientId,
        stmt: &SetRoleStatement,
        txn_ctx: Option<(TransactionId, CommandId)>,
    ) -> Result<AuthState, ExecError> {
        let mut next = self.auth_state(client_id);
        let Some(role_name) = stmt.role_name.as_ref() else {
            next.reset_role();
            self.install_auth_state(client_id, next.clone());
            self.plan_cache.invalidate_all();
            return Ok(next);
        };
        let auth_catalog = self
            .auth_catalog(client_id, txn_ctx)
            .map_err(map_session_auth_error)?;
        let target = find_role_by_name(auth_catalog.roles(), role_name)
            .cloned()
            .ok_or_else(|| {
                ExecError::Parse(role_management_error(format!(
                    "role \"{}\" does not exist",
                    role_name
                )))
            })?;
        if !next.can_set_role_from_session(target.oid, &auth_catalog) {
            return Err(ExecError::Parse(role_management_error(format!(
                "permission denied to set role \"{}\"",
                target.rolname
            ))));
        }
        next.set_role(target.oid);
        self.install_auth_state(client_id, next.clone());
        self.plan_cache.invalidate_all();
        Ok(next)
    }

    pub(crate) fn execute_reset_role_stmt(
        &self,
        client_id: ClientId,
        _stmt: &ResetRoleStatement,
    ) -> Result<AuthState, ExecError> {
        let mut next = self.auth_state(client_id);
        next.reset_role();
        self.install_auth_state(client_id, next.clone());
        self.plan_cache.invalidate_all();
        Ok(next)
    }
}

fn map_session_auth_error(err: crate::backend::catalog::CatalogError) -> ExecError {
    ExecError::Parse(role_management_error(format!("{err:?}")))
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
            "pgrust_session_auth_{}_{}_{}",
            label,
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn role_oid(db: &Database, role_name: &str) -> u32 {
        db.shared_catalog
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
    fn set_and_reset_session_authorization_updates_session_identity() {
        let base = temp_dir("set_reset");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role tenant login").unwrap();

        assert_eq!(
            session.current_user_oid(),
            crate::include::catalog::BOOTSTRAP_SUPERUSER_OID
        );
        assert_eq!(
            session
                .execute(&db, "set session authorization tenant")
                .unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert_eq!(session.current_user_oid(), role_oid(&db, "tenant"));
        assert_eq!(session.session_user_oid(), role_oid(&db, "tenant"));

        assert_eq!(
            session
                .execute(&db, "set session authorization 'tenant'")
                .unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert_eq!(session.current_user_oid(), role_oid(&db, "tenant"));
        assert_eq!(session.session_user_oid(), role_oid(&db, "tenant"));

        assert_eq!(
            session.execute(&db, "reset session authorization").unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert_eq!(
            session.current_user_oid(),
            crate::include::catalog::BOOTSTRAP_SUPERUSER_OID
        );
        assert_eq!(
            session.session_user_oid(),
            crate::include::catalog::BOOTSTRAP_SUPERUSER_OID
        );
    }

    #[test]
    fn set_session_authorization_accepts_string_literal_role_name() {
        let base = temp_dir("set_reset_string_literal");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role tenant login").unwrap();

        assert_eq!(
            session
                .execute(&db, "set session authorization 'tenant'")
                .unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert_eq!(session.current_user_oid(), role_oid(&db, "tenant"));
    }

    #[test]
    fn non_superuser_cannot_set_session_authorization_without_set_role_path() {
        let base = temp_dir("denied");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser
            .execute(&db, "create role limited createrole")
            .unwrap();
        superuser.execute(&db, "create role tenant login").unwrap();

        let mut session = Session::new(2);
        session.set_session_authorization_oid(role_oid(&db, "limited"));
        let err = session
            .execute(&db, "set session authorization tenant")
            .unwrap_err();
        assert!(format!("{err:?}").contains("permission denied"));
    }

    #[test]
    fn grant_does_not_hide_previously_created_roles() {
        let base = temp_dir("grant_visibility");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(3);

        session
            .execute(&db, "create role limited_admin createrole")
            .unwrap();
        assert!(
            db.backend_catcache(3, None)
                .unwrap()
                .authid_rows()
                .into_iter()
                .any(|row| row.rolname == "limited_admin")
        );

        assert_eq!(
            session
                .execute(
                    &db,
                    "grant create on database regression to limited_admin with grant option",
                )
                .unwrap(),
            StatementResult::AffectedRows(0)
        );

        assert_eq!(
            session
                .execute(&db, "set session authorization limited_admin")
                .unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert_eq!(session.current_user_oid(), role_oid(&db, "limited_admin"));
    }

    #[test]
    fn authenticated_superuser_can_chain_session_authorization_switches() {
        let base = temp_dir("superuser_chain");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create role limited_admin createrole")
            .unwrap();
        session
            .execute(&db, "create role role_admin createrole")
            .unwrap();

        assert_eq!(
            session
                .execute(&db, "set session authorization limited_admin")
                .unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert_eq!(session.current_user_oid(), role_oid(&db, "limited_admin"));

        assert_eq!(
            session
                .execute(&db, "set session authorization role_admin")
                .unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert_eq!(session.current_user_oid(), role_oid(&db, "role_admin"));
        assert_eq!(session.session_user_oid(), role_oid(&db, "role_admin"));
    }

    #[test]
    fn set_and_reset_role_only_change_current_user() {
        let base = temp_dir("set_reset_role");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role member").unwrap();
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "grant parent to member").unwrap();
        session
            .execute(&db, "set session authorization member")
            .unwrap();

        assert_eq!(session.session_user_oid(), role_oid(&db, "member"));
        assert_eq!(session.current_user_oid(), role_oid(&db, "member"));
        assert_eq!(
            session.execute(&db, "set role parent").unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert_eq!(session.session_user_oid(), role_oid(&db, "member"));
        assert_eq!(session.current_user_oid(), role_oid(&db, "parent"));

        assert_eq!(
            session.execute(&db, "reset role").unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert_eq!(session.session_user_oid(), role_oid(&db, "member"));
        assert_eq!(session.current_user_oid(), role_oid(&db, "member"));
    }

    #[test]
    fn set_role_none_resets_to_session_user() {
        let base = temp_dir("set_role_none");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role member").unwrap();
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "grant parent to member").unwrap();
        session
            .execute(&db, "set session authorization member")
            .unwrap();
        session.execute(&db, "set role parent").unwrap();

        assert_eq!(
            session.execute(&db, "set role none").unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert_eq!(session.session_user_oid(), role_oid(&db, "member"));
        assert_eq!(session.current_user_oid(), role_oid(&db, "member"));
    }

    #[test]
    fn set_role_permission_uses_session_user_membership() {
        let base = temp_dir("set_role_permissions");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create role member").unwrap();
        superuser.execute(&db, "create role parent").unwrap();
        superuser.execute(&db, "create role child").unwrap();
        superuser.execute(&db, "grant parent to member").unwrap();
        superuser
            .execute(&db, "grant child to parent with set false")
            .unwrap();

        let mut session = Session::new(2);
        session.set_session_authorization_oid(role_oid(&db, "member"));
        session.execute(&db, "set role parent").unwrap();

        let err = session.execute(&db, "set role child").unwrap_err();
        assert!(format!("{err:?}").contains("permission denied to set role"));
    }

    #[test]
    fn sql_set_session_authorization_obeys_set_option_for_set_role() {
        let base = temp_dir("set_role_sql_session_auth");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create role member").unwrap();
        superuser.execute(&db, "create role child").unwrap();
        superuser
            .execute(&db, "grant child to member with set false")
            .unwrap();

        let mut session = Session::new(2);
        session
            .execute(&db, "set session authorization member")
            .unwrap();

        let err = session.execute(&db, "set role child").unwrap_err();
        assert!(format!("{err:?}").contains("permission denied to set role"));
    }

    #[test]
    fn rollback_restores_session_authorization_and_role_state() {
        let base = temp_dir("rollback_session_authorization");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role tenant login").unwrap();
        session.execute(&db, "create role manager").unwrap();
        session.execute(&db, "create role auditor login").unwrap();
        session.execute(&db, "grant manager to tenant").unwrap();
        session
            .execute(&db, "set session authorization tenant")
            .unwrap();
        session.execute(&db, "set role manager").unwrap();
        session.execute(&db, "begin").unwrap();
        session
            .execute(&db, "set session authorization auditor")
            .unwrap();

        assert!(matches!(
            session
                .execute(
                    &db,
                    "select session_user, current_user, current_role, current_setting('role')",
                )
                .unwrap(),
            StatementResult::Query { rows, .. }
                if rows == vec![vec![
                    crate::backend::executor::Value::Text("auditor".into()),
                    crate::backend::executor::Value::Text("auditor".into()),
                    crate::backend::executor::Value::Text("auditor".into()),
                    crate::backend::executor::Value::Text("none".into()),
                ]]
        ));

        session.execute(&db, "rollback").unwrap();

        assert!(matches!(
            session
                .execute(
                    &db,
                    "select session_user, current_user, current_role, current_setting('role')",
                )
                .unwrap(),
            StatementResult::Query { rows, .. }
                if rows == vec![vec![
                    crate::backend::executor::Value::Text("tenant".into()),
                    crate::backend::executor::Value::Text("manager".into()),
                    crate::backend::executor::Value::Text("manager".into()),
                    crate::backend::executor::Value::Text("manager".into()),
                ]]
        ));
    }
}
