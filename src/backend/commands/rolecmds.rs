use crate::backend::catalog::role_memberships::NewRoleMembership;
use crate::backend::catalog::roles::{RoleAttributes, find_role_by_name};
use crate::backend::parser::{
    AlterRoleAction, AlterRoleStatement, CreateRoleStatement, DropRoleStatement, ParseError,
    RoleOption,
};
use crate::include::catalog::{PG_DATABASE_OWNER_OID, PgAuthIdRow};
use crate::pgrust::auth::{AuthCatalog, AuthState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltRoleSpec {
    pub attrs: RoleAttributes,
    pub saw_sysid: bool,
    pub add_role_to: Vec<String>,
    pub role_members: Vec<String>,
    pub admin_members: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreateRoleSelfGrant {
    pub inherit: bool,
    pub set: bool,
}

pub fn build_create_role_spec(stmt: &CreateRoleStatement) -> Result<BuiltRoleSpec, ParseError> {
    let mut attrs = RoleAttributes {
        rolcanlogin: stmt.is_user,
        ..RoleAttributes::default()
    };
    let mut add_role_to = Vec::new();
    let mut role_members = Vec::new();
    let mut admin_members = Vec::new();
    let saw_sysid = apply_role_options(
        &mut attrs,
        &stmt.options,
        &mut add_role_to,
        &mut role_members,
        &mut admin_members,
    )?;
    Ok(BuiltRoleSpec {
        attrs,
        saw_sysid,
        add_role_to,
        role_members,
        admin_members,
    })
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
            let saw_sysid = apply_role_options(
                &mut attrs,
                options,
                &mut Vec::new(),
                &mut Vec::new(),
                &mut Vec::new(),
            )?;
            Ok(Some(BuiltRoleSpec {
                attrs,
                saw_sysid,
                add_role_to: Vec::new(),
                role_members: Vec::new(),
                admin_members: Vec::new(),
            }))
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

pub fn parse_createrole_self_grant(raw: &str) -> Result<Option<CreateRoleSelfGrant>, ParseError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let mut inherit = false;
    let mut set = false;
    for token in trimmed.split(',') {
        match token.trim().to_ascii_lowercase().as_str() {
            "" => {}
            "inherit" => inherit = true,
            "set" => set = true,
            other => {
                return Err(role_management_error(format!(
                    "invalid createrole_self_grant option: {other}"
                )));
            }
        }
    }

    Ok(Some(CreateRoleSelfGrant { inherit, set }))
}

pub fn grant_membership_authorized(
    auth: &AuthState,
    catalog: &AuthCatalog,
    role_name: &str,
) -> Result<PgAuthIdRow, ParseError> {
    grant_membership_authorized_with_detail(auth, catalog, role_name).map_err(|err| match err {
        GrantMembershipAuthorizationError::Parse(err) => err,
        GrantMembershipAuthorizationError::PermissionDenied { role_name, .. } => {
            role_management_error(format!("permission denied to grant role \"{role_name}\""))
        }
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantMembershipAuthorizationError {
    Parse(ParseError),
    PermissionDenied {
        role_name: String,
        detail: Option<String>,
    },
}

pub fn grant_membership_authorized_with_detail(
    auth: &AuthState,
    catalog: &AuthCatalog,
    role_name: &str,
) -> Result<PgAuthIdRow, GrantMembershipAuthorizationError> {
    let role = find_role_by_name(catalog.roles(), role_name)
        .cloned()
        .ok_or_else(|| {
            GrantMembershipAuthorizationError::Parse(role_management_error(format!(
                "role \"{role_name}\" does not exist"
            )))
        })?;
    if role.oid == PG_DATABASE_OWNER_OID {
        return Err(GrantMembershipAuthorizationError::Parse(
            role_management_error(format!(
                "role \"{}\" cannot have explicit members",
                role.rolname
            )),
        ));
    }
    if role.rolsuper {
        let current = catalog
            .role_by_oid(auth.current_user_oid())
            .ok_or_else(|| {
                GrantMembershipAuthorizationError::Parse(role_management_error(
                    "permission denied to grant role",
                ))
            })?;
        if !current.rolsuper {
            return Err(GrantMembershipAuthorizationError::PermissionDenied {
                role_name: role.rolname.clone(),
                detail: Some(
                    "Only roles with the SUPERUSER attribute may grant roles with the SUPERUSER attribute.".into(),
                ),
            });
        }
        return Ok(role);
    }
    if !auth.has_admin_option(role.oid, catalog) {
        return Err(GrantMembershipAuthorizationError::PermissionDenied {
            role_name: role.rolname.clone(),
            detail: Some(format!(
                "Only roles with the ADMIN option on role \"{}\" may grant this role.",
                role.rolname
            )),
        });
    }
    Ok(role)
}

pub fn membership_row(
    roleid: u32,
    member: u32,
    grantor: u32,
    admin_option: bool,
    inherit_option: bool,
    set_option: bool,
) -> NewRoleMembership {
    NewRoleMembership {
        roleid,
        member,
        grantor,
        admin_option,
        inherit_option,
        set_option,
    }
}

fn apply_role_options(
    attrs: &mut RoleAttributes,
    options: &[RoleOption],
    add_role_to: &mut Vec<String>,
    role_members: &mut Vec<String>,
    admin_members: &mut Vec<String>,
) -> Result<bool, ParseError> {
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
            RoleOption::InRole(names) => add_role_to.extend(names.iter().cloned()),
            RoleOption::Role(names) => role_members.extend(names.iter().cloned()),
            RoleOption::Admin(names) => admin_members.extend(names.iter().cloned()),
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
    fn membership_options_are_collected() {
        let spec = build_create_role_spec(&CreateRoleStatement {
            role_name: "app_user".into(),
            is_user: false,
            options: vec![
                RoleOption::InRole(vec!["parent".into()]),
                RoleOption::Role(vec!["member".into()]),
                RoleOption::Admin(vec!["admin".into()]),
            ],
        })
        .unwrap();
        assert_eq!(spec.add_role_to, vec!["parent"]);
        assert_eq!(spec.role_members, vec!["member"]);
        assert_eq!(spec.admin_members, vec!["admin"]);
    }

    #[test]
    fn rename_requires_createrole_and_admin_option() {
        let mut creator = role(BOOTSTRAP_SUPERUSER_OID + 1, "creator");
        creator.rolcreaterole = true;
        let target = role(BOOTSTRAP_SUPERUSER_OID + 2, "tenant");
        let catalog = AuthCatalog::new(
            vec![
                role(BOOTSTRAP_SUPERUSER_OID, "postgres"),
                creator.clone(),
                target.clone(),
            ],
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

    #[test]
    fn parse_createrole_self_grant_values() {
        assert_eq!(
            parse_createrole_self_grant("set, inherit").unwrap(),
            Some(CreateRoleSelfGrant {
                inherit: true,
                set: true,
            })
        );
        assert_eq!(parse_createrole_self_grant("").unwrap(), None);
        assert!(parse_createrole_self_grant("bogus").is_err());
    }

    #[test]
    fn grant_membership_authorization_checks_superuser_and_admin() {
        let mut creator = role(11, "creator");
        creator.rolcreaterole = true;
        let mut super_role = role(12, "super_role");
        super_role.rolsuper = true;
        let tenant = role(13, "tenant");
        let catalog = AuthCatalog::new(
            vec![
                role(BOOTSTRAP_SUPERUSER_OID, "postgres"),
                creator.clone(),
                super_role,
                tenant.clone(),
            ],
            vec![PgAuthMembersRow {
                oid: 1,
                roleid: tenant.oid,
                member: creator.oid,
                grantor: BOOTSTRAP_SUPERUSER_OID,
                admin_option: true,
                inherit_option: false,
                set_option: true,
            }],
        );
        let mut auth = AuthState::default();
        auth.set_session_authorization(creator.oid);
        assert!(grant_membership_authorized(&auth, &catalog, "tenant").is_ok());
        assert!(grant_membership_authorized(&auth, &catalog, "super_role").is_err());
    }
}
