use super::super::*;
use crate::backend::catalog::roles::find_role_by_name;
use crate::backend::commands::rolecmds::role_management_error;
use crate::backend::parser::{
    ResetSessionAuthorizationStatement, SetSessionAuthorizationStatement,
};
use crate::pgrust::auth::AuthState;

impl Database {
    pub(crate) fn execute_set_session_authorization_stmt(
        &self,
        client_id: ClientId,
        stmt: &SetSessionAuthorizationStatement,
    ) -> Result<AuthState, ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, None)
            .map_err(map_session_auth_error)?;
        let target = find_role_by_name(auth_catalog.roles(), &stmt.role_name)
            .cloned()
            .ok_or_else(|| {
                ExecError::Parse(role_management_error(format!(
                    "role \"{}\" does not exist",
                    stmt.role_name
                )))
            })?;
        let current = auth_catalog
            .role_by_oid(auth.current_user_oid())
            .ok_or_else(|| ExecError::Parse(role_management_error("permission denied")))?;
        if !current.rolsuper && !auth.can_set_role(target.oid, &auth_catalog) {
            return Err(ExecError::Parse(role_management_error(format!(
                "permission denied to set session authorization to \"{}\"",
                target.rolname
            ))));
        }

        let mut next = auth.clone();
        next.set_session_authorization(target.oid);
        self.install_auth_state(client_id, next.clone());
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
            session.execute(&db, "reset session authorization").unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert_eq!(session.current_user_oid(), role_oid(&db, "tenant"));
        assert_eq!(session.session_user_oid(), role_oid(&db, "tenant"));
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
    fn parse_error_does_not_hide_previously_created_roles() {
        let base = temp_dir("parse_error_visibility");
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

        let err = session
            .execute(
                &db,
                "grant create on database regression to limited_admin with grant option",
            )
            .unwrap_err();
        assert!(format!("{err:?}").contains("expected statement"));

        assert_eq!(
            session
                .execute(&db, "set session authorization limited_admin")
                .unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert_eq!(session.current_user_oid(), role_oid(&db, "limited_admin"));
    }
}
