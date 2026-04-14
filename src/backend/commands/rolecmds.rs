use crate::backend::catalog::roles::RoleAttributes;
use crate::backend::parser::{
    AlterRoleAction, AlterRoleStatement, CreateRoleStatement, DropRoleStatement, ParseError,
    RoleOption,
};
use crate::include::catalog::PgAuthIdRow;
use crate::pgrust::auth::{AuthCatalog, AuthState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltRoleSpec {
    pub attrs: RoleAttributes,
    pub saw_sysid: bool,
}

pub fn build_create_role_spec(stmt: &CreateRoleStatement) -> Result<BuiltRoleSpec, ParseError> {
    let mut attrs = RoleAttributes {
        rolcanlogin: stmt.is_user,
        ..RoleAttributes::default()
    };
    let saw_sysid = apply_role_options(&mut attrs, &stmt.options)?;
    Ok(BuiltRoleSpec { attrs, saw_sysid })
}

pub fn build_alter_role_spec(
    stmt: &AlterRoleStatement,
    existing: &PgAuthIdRow,
) -> Result<Option<BuiltRoleSpec>, ParseError> {
    match &stmt.action {
        AlterRoleAction::Rename { .. } => Ok(None),
        AlterRoleAction::Options(options) => {
            let mut attrs = RoleAttributes {
                rolsuper: existing.rolsuper,
                rolinherit: existing.rolinherit,
                rolcreaterole: existing.rolcreaterole,
                rolcreatedb: existing.rolcreatedb,
                rolcanlogin: existing.rolcanlogin,
                rolreplication: existing.rolreplication,
                rolbypassrls: existing.rolbypassrls,
                rolconnlimit: existing.rolconnlimit,
            };
            let saw_sysid = apply_role_options(&mut attrs, options)?;
            Ok(Some(BuiltRoleSpec { attrs, saw_sysid }))
        }
    }
}

pub fn can_rename_role(auth: &AuthState, target_oid: u32, catalog: &AuthCatalog) -> bool {
    let Some(current) = catalog.role_by_oid(auth.current_user_oid()) else {
        return false;
    };
    let target = catalog.role_by_oid(target_oid);
    current.rolsuper
        || (current.rolcreaterole
            && target.is_none_or(|row| !row.rolsuper)
            && auth.has_admin_option(target_oid, catalog))
}

pub fn normalize_drop_role_names(stmt: &DropRoleStatement) -> Vec<String> {
    let mut names = Vec::new();
    for role_name in &stmt.role_names {
        if !names
            .iter()
            .any(|existing: &String| existing.eq_ignore_ascii_case(role_name))
        {
            names.push(role_name.clone());
        }
    }
    names
}

pub fn role_management_error(message: impl Into<String>) -> ParseError {
    ParseError::UnexpectedToken {
        expected: "role management operation",
        actual: message.into(),
    }
}

fn apply_role_options(attrs: &mut RoleAttributes, options: &[RoleOption]) -> Result<bool, ParseError> {
    let mut saw_sysid = false;
    for option in options {
        match option {
            RoleOption::Superuser(enabled) => attrs.rolsuper = *enabled,
            RoleOption::CreateDb(enabled) => attrs.rolcreatedb = *enabled,
            RoleOption::CreateRole(enabled) => attrs.rolcreaterole = *enabled,
            RoleOption::Inherit(enabled) => attrs.rolinherit = *enabled,
            RoleOption::Login(enabled) => attrs.rolcanlogin = *enabled,
            RoleOption::Replication(enabled) => attrs.rolreplication = *enabled,
            RoleOption::BypassRls(enabled) => attrs.rolbypassrls = *enabled,
            RoleOption::ConnectionLimit(limit) => attrs.rolconnlimit = *limit,
            RoleOption::Password(_) | RoleOption::EncryptedPassword(_) => {}
            RoleOption::InRole(_) | RoleOption::Role(_) | RoleOption::Admin(_) => {
                // :HACK: Slice 4 only implements basic role DDL. Membership clause execution
                // lands in slice 5 once pg_auth_members privilege semantics are wired through.
                return Err(ParseError::FeatureNotSupported(
                    "role membership clauses".into(),
                ));
            }
            RoleOption::Sysid(_) => {
                // :HACK: PostgreSQL emits a NOTICE here. The parser keeps SYSID accepted as a
                // backwards-compatible noise word, but notice plumbing is deferred.
                saw_sysid = true;
            }
        }
    }
    Ok(saw_sysid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::catalog::{BOOTSTRAP_SUPERUSER_OID, PgAuthMembersRow};

    fn role(oid: u32, name: &str) -> PgAuthIdRow {
        PgAuthIdRow {
            oid,
            rolname: name.into(),
            rolsuper: false,
            rolinherit: true,
            rolcreaterole: false,
            rolcreatedb: false,
            rolcanlogin: false,
            rolreplication: false,
            rolbypassrls: false,
            rolconnlimit: -1,
        }
    }

    #[test]
    fn create_user_implies_login() {
        let spec = build_create_role_spec(&CreateRoleStatement {
            role_name: "app_user".into(),
            is_user: true,
            options: vec![],
        })
        .unwrap();
        assert!(spec.attrs.rolcanlogin);
    }

    #[test]
    fn membership_options_are_deferred() {
        let err = build_create_role_spec(&CreateRoleStatement {
            role_name: "app_user".into(),
            is_user: false,
            options: vec![RoleOption::InRole(vec!["parent".into()])],
        })
        .unwrap_err();
        assert!(matches!(err, ParseError::FeatureNotSupported(_)));
    }

    #[test]
    fn rename_requires_createrole_and_admin_option() {
        let mut creator = role(BOOTSTRAP_SUPERUSER_OID + 1, "creator");
        creator.rolcreaterole = true;
        let target = role(BOOTSTRAP_SUPERUSER_OID + 2, "tenant");
        let catalog = AuthCatalog::new(
            vec![role(BOOTSTRAP_SUPERUSER_OID, "postgres"), creator.clone(), target.clone()],
            vec![PgAuthMembersRow {
                oid: 1,
                roleid: target.oid,
                member: creator.oid,
                grantor: BOOTSTRAP_SUPERUSER_OID,
                admin_option: true,
                inherit_option: true,
                set_option: true,
            }],
        );
        let mut auth = AuthState::default();
        auth.set_session_authorization(creator.oid);

        assert!(can_rename_role(&auth, target.oid, &catalog));
    }
}
