use std::collections::{BTreeSet, VecDeque};

use crate::backend::catalog::roles::RoleAttributes;
use crate::include::catalog::{BOOTSTRAP_SUPERUSER_OID, PgAuthIdRow, PgAuthMembersRow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthCatalog {
    roles: Vec<PgAuthIdRow>,
    memberships: Vec<PgAuthMembersRow>,
}

impl AuthCatalog {
    pub fn new(roles: Vec<PgAuthIdRow>, memberships: Vec<PgAuthMembersRow>) -> Self {
        Self { roles, memberships }
    }

    pub fn role_by_oid(&self, oid: u32) -> Option<&PgAuthIdRow> {
        self.roles.iter().find(|row| row.oid == oid)
    }

    pub fn role_by_name(&self, role_name: &str) -> Option<&PgAuthIdRow> {
        self.roles
            .iter()
            .find(|row| row.rolname.eq_ignore_ascii_case(role_name))
    }

    pub fn roles(&self) -> &[PgAuthIdRow] {
        &self.roles
    }

    pub fn memberships(&self) -> &[PgAuthMembersRow] {
        &self.memberships
    }

    fn direct_membership(&self, member: u32, roleid: u32) -> Option<&PgAuthMembersRow> {
        self.memberships
            .iter()
            .find(|row| row.member == member && row.roleid == roleid)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthState {
    session_user_oid: u32,
    current_user_oid: u32,
}

impl Default for AuthState {
    fn default() -> Self {
        Self {
            session_user_oid: BOOTSTRAP_SUPERUSER_OID,
            current_user_oid: BOOTSTRAP_SUPERUSER_OID,
        }
    }
}

impl AuthState {
    pub fn session_user_oid(&self) -> u32 {
        self.session_user_oid
    }

    pub fn current_user_oid(&self) -> u32 {
        self.current_user_oid
    }

    pub fn set_session_authorization(&mut self, role_oid: u32) {
        self.session_user_oid = role_oid;
        self.current_user_oid = role_oid;
    }

    pub fn reset_session_authorization(&mut self) {
        self.current_user_oid = self.session_user_oid;
    }

    pub fn can_set_role(&self, target_oid: u32, catalog: &AuthCatalog) -> bool {
        if self.current_user_oid == target_oid {
            return true;
        }
        self.is_superuser(catalog)
            || has_membership_path(
                self.current_user_oid,
                target_oid,
                catalog.memberships(),
                MembershipMode::Set,
            )
    }

    pub fn has_effective_membership(&self, target_oid: u32, catalog: &AuthCatalog) -> bool {
        if self.current_user_oid == target_oid {
            return true;
        }
        self.is_superuser(catalog)
            || has_membership_path(
                self.current_user_oid,
                target_oid,
                catalog.memberships(),
                MembershipMode::Inherit,
            )
    }

    pub fn has_admin_option(&self, target_oid: u32, catalog: &AuthCatalog) -> bool {
        if self.current_user_oid == target_oid {
            return true;
        }
        self.is_superuser(catalog)
            || catalog
                .direct_membership(self.current_user_oid, target_oid)
                .is_some_and(|row| row.admin_option)
    }

    pub fn can_create_role_with_attrs(
        &self,
        attrs: &RoleAttributes,
        catalog: &AuthCatalog,
    ) -> bool {
        let Some(current) = catalog.role_by_oid(self.current_user_oid) else {
            return false;
        };
        current.rolsuper
            || (current.rolcreaterole
                && !attrs.rolsuper
                && (!attrs.rolcreatedb || current.rolcreatedb)
                && (!attrs.rolreplication || current.rolreplication)
                && (!attrs.rolbypassrls || current.rolbypassrls))
    }

    pub fn can_alter_role_attrs(
        &self,
        target_oid: u32,
        attrs: &RoleAttributes,
        catalog: &AuthCatalog,
    ) -> bool {
        let Some(current) = catalog.role_by_oid(self.current_user_oid) else {
            return false;
        };
        current.rolsuper
            || (current.rolcreaterole
                && self.has_admin_option(target_oid, catalog)
                && !attrs.rolsuper
                && (!attrs.rolcreatedb || current.rolcreatedb)
                && (!attrs.rolreplication || current.rolreplication)
                && (!attrs.rolbypassrls || current.rolbypassrls))
    }

    pub fn can_drop_role(&self, target_oid: u32, catalog: &AuthCatalog) -> bool {
        if self.current_user_oid == target_oid {
            return false;
        }
        let Some(current) = catalog.role_by_oid(self.current_user_oid) else {
            return false;
        };
        let target = catalog.role_by_oid(target_oid);
        current.rolsuper
            || (current.rolcreaterole
                && target.is_none_or(|row| !row.rolsuper)
                && self.has_admin_option(target_oid, catalog))
    }

    fn is_superuser(&self, catalog: &AuthCatalog) -> bool {
        catalog
            .role_by_oid(self.current_user_oid)
            .is_some_and(|row| row.rolsuper)
    }
}

#[derive(Clone, Copy)]
enum MembershipMode {
    Inherit,
    Set,
}

fn has_membership_path(
    start_member: u32,
    target_role: u32,
    memberships: &[PgAuthMembersRow],
    mode: MembershipMode,
) -> bool {
    let mut pending = VecDeque::from([start_member]);
    let mut visited = BTreeSet::new();
    while let Some(member) = pending.pop_front() {
        if !visited.insert(member) {
            continue;
        }
        for edge in memberships.iter().filter(|row| row.member == member) {
            let allowed = match mode {
                MembershipMode::Inherit => edge.inherit_option,
                MembershipMode::Set => edge.set_option,
            };
            if !allowed {
                continue;
            }
            if edge.roleid == target_role {
                return true;
            }
            pending.push_back(edge.roleid);
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(oid: u32, name: &str) -> PgAuthIdRow {
        PgAuthIdRow {
            oid,
            rolname: name.into(),
            rolsuper: false,
            rolinherit: true,
            rolcreaterole: false,
            rolcreatedb: false,
            rolcanlogin: true,
            rolreplication: false,
            rolbypassrls: false,
            rolconnlimit: -1,
        }
    }

    #[test]
    fn auth_state_tracks_session_and_current_user() {
        let mut auth = AuthState::default();
        assert_eq!(auth.session_user_oid(), BOOTSTRAP_SUPERUSER_OID);
        assert_eq!(auth.current_user_oid(), BOOTSTRAP_SUPERUSER_OID);

        auth.set_session_authorization(42);
        assert_eq!(auth.session_user_oid(), 42);
        assert_eq!(auth.current_user_oid(), 42);

        auth.set_session_authorization(43);
        auth.reset_session_authorization();
        assert_eq!(auth.current_user_oid(), 43);
    }

    #[test]
    fn auth_membership_checks_respect_inherit_and_set_bits() {
        let catalog = AuthCatalog::new(
            vec![row(10, "postgres"), row(20, "parent"), row(21, "child")],
            vec![PgAuthMembersRow {
                oid: 1,
                roleid: 20,
                member: 21,
                grantor: 10,
                admin_option: true,
                inherit_option: true,
                set_option: false,
            }],
        );
        let mut auth = AuthState::default();
        auth.set_session_authorization(21);

        assert!(auth.has_effective_membership(20, &catalog));
        assert!(!auth.can_set_role(20, &catalog));
        assert!(auth.has_admin_option(20, &catalog));
    }

    #[test]
    fn auth_privilege_checks_use_current_role_attributes() {
        let mut superuser = row(10, "postgres");
        superuser.rolsuper = true;
        let mut creator = row(11, "creator");
        creator.rolcreaterole = true;
        creator.rolcreatedb = true;
        let target = row(12, "target");
        let catalog = AuthCatalog::new(
            vec![superuser, creator.clone(), target.clone()],
            vec![PgAuthMembersRow {
                oid: 2,
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

        assert!(auth.can_create_role_with_attrs(
            &RoleAttributes {
                rolcreatedb: true,
                ..RoleAttributes::default()
            },
            &catalog
        ));
        assert!(!auth.can_create_role_with_attrs(
            &RoleAttributes {
                rolreplication: true,
                ..RoleAttributes::default()
            },
            &catalog
        ));
        assert!(auth.can_alter_role_attrs(
            target.oid,
            &RoleAttributes::default(),
            &catalog
        ));
        assert!(auth.can_drop_role(target.oid, &catalog));
        assert!(!auth.can_drop_role(creator.oid, &catalog));
    }
}
