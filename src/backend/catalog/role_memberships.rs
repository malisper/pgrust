use std::collections::{BTreeSet, VecDeque};

use crate::backend::catalog::CatalogError;
use crate::include::catalog::{PgAuthIdRow, PgAuthMembersRow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewRoleMembership {
    pub roleid: u32,
    pub member: u32,
    pub grantor: u32,
    pub admin_option: bool,
    pub inherit_option: bool,
    pub set_option: bool,
}

pub fn memberships_for_member(rows: &[PgAuthMembersRow], member: u32) -> Vec<PgAuthMembersRow> {
    rows.iter()
        .filter(|row| row.member == member)
        .cloned()
        .collect()
}

pub fn memberships_for_role(rows: &[PgAuthMembersRow], roleid: u32) -> Vec<PgAuthMembersRow> {
    rows.iter()
        .filter(|row| row.roleid == roleid)
        .cloned()
        .collect()
}

pub fn has_admin_option(rows: &[PgAuthMembersRow], roleid: u32, member: u32) -> bool {
    rows.iter()
        .any(|row| row.roleid == roleid && row.member == member && row.admin_option)
}

pub fn has_membership_path(start_member: u32, target_role: u32, rows: &[PgAuthMembersRow]) -> bool {
    let mut pending = VecDeque::from([start_member]);
    let mut visited = BTreeSet::new();
    while let Some(member) = pending.pop_front() {
        if !visited.insert(member) {
            continue;
        }
        for edge in rows.iter().filter(|row| row.member == member) {
            if !edge.inherit_option {
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

pub fn has_effective_membership(
    user_oid: u32,
    target_role_oid: u32,
    authid_rows: &[PgAuthIdRow],
    auth_members_rows: &[PgAuthMembersRow],
) -> bool {
    if user_oid == target_role_oid {
        return true;
    }
    authid_rows
        .iter()
        .find(|row| row.oid == user_oid)
        .is_some_and(|row| row.rolsuper)
        || has_membership_path(user_oid, target_role_oid, auth_members_rows)
}

pub fn would_create_membership_cycle(rows: &[PgAuthMembersRow], roleid: u32, member: u32) -> bool {
    if roleid == member {
        return true;
    }

    let mut pending = VecDeque::from([roleid]);
    let mut visited = BTreeSet::new();
    while let Some(current) = pending.pop_front() {
        if !visited.insert(current) {
            continue;
        }
        for existing in rows.iter().filter(|row| row.member == current) {
            if existing.roleid == member {
                return true;
            }
            pending.push_back(existing.roleid);
        }
    }
    false
}

pub fn grant_membership(
    rows: &mut Vec<PgAuthMembersRow>,
    next_oid: &mut u32,
    membership: &NewRoleMembership,
) -> Result<PgAuthMembersRow, CatalogError> {
    if would_create_membership_cycle(rows, membership.roleid, membership.member) {
        return Err(CatalogError::UniqueViolation(format!(
            "role membership cycle: {} -> {}",
            membership.member, membership.roleid
        )));
    }
    if rows.iter().any(|row| {
        row.roleid == membership.roleid
            && row.member == membership.member
            && row.grantor == membership.grantor
    }) {
        return Err(CatalogError::UniqueViolation(format!(
            "duplicate role membership: {} -> {}",
            membership.member, membership.roleid
        )));
    }
    let row = PgAuthMembersRow {
        oid: *next_oid,
        roleid: membership.roleid,
        member: membership.member,
        grantor: membership.grantor,
        admin_option: membership.admin_option,
        inherit_option: membership.inherit_option,
        set_option: membership.set_option,
    };
    *next_oid = next_oid.saturating_add(1);
    rows.push(row.clone());
    rows.sort_by_key(|existing| {
        (
            existing.oid,
            existing.roleid,
            existing.member,
            existing.grantor,
        )
    });
    Ok(row)
}

pub fn revoke_role_membership_option(
    rows: &mut [PgAuthMembersRow],
    roleid: u32,
    member: u32,
    grantor: u32,
    admin_option: bool,
    inherit_option: bool,
    set_option: bool,
) -> Result<PgAuthMembersRow, CatalogError> {
    let row = rows
        .iter_mut()
        .find(|row| row.roleid == roleid && row.member == member && row.grantor == grantor)
        .ok_or_else(|| CatalogError::UnknownTable(format!("{roleid}/{member}/{grantor}")))?;
    row.admin_option = admin_option;
    row.inherit_option = inherit_option;
    row.set_option = set_option;
    Ok(row.clone())
}

pub fn revoke_role_membership(
    rows: &mut Vec<PgAuthMembersRow>,
    roleid: u32,
    member: u32,
    grantor: u32,
) -> Result<PgAuthMembersRow, CatalogError> {
    let index = rows
        .iter()
        .position(|row| row.roleid == roleid && row.member == member && row.grantor == grantor)
        .ok_or_else(|| CatalogError::UnknownTable(format!("{roleid}/{member}/{grantor}")))?;
    Ok(rows.remove(index))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grant_and_update_membership_options() {
        let mut rows = Vec::new();
        let mut next_oid = 200;
        let created = grant_membership(
            &mut rows,
            &mut next_oid,
            &NewRoleMembership {
                roleid: 11,
                member: 12,
                grantor: 10,
                admin_option: false,
                inherit_option: true,
                set_option: true,
            },
        )
        .unwrap();
        assert_eq!(created.oid, 200);
        assert_eq!(memberships_for_member(&rows, 12).len(), 1);

        let updated =
            revoke_role_membership_option(&mut rows, 11, 12, 10, true, false, false).unwrap();
        assert!(updated.admin_option);
        assert!(!updated.inherit_option);
        assert!(!updated.set_option);
        let removed = revoke_role_membership(&mut rows, 11, 12, 10).unwrap();
        assert_eq!(removed.oid, 200);
        assert!(rows.is_empty());
    }

    #[test]
    fn membership_cycle_is_rejected() {
        let mut rows = vec![PgAuthMembersRow {
            oid: 200,
            roleid: 20,
            member: 21,
            grantor: 10,
            admin_option: false,
            inherit_option: true,
            set_option: true,
        }];
        let mut next_oid = 201;
        let err = grant_membership(
            &mut rows,
            &mut next_oid,
            &NewRoleMembership {
                roleid: 21,
                member: 20,
                grantor: 10,
                admin_option: false,
                inherit_option: true,
                set_option: true,
            },
        )
        .unwrap_err();
        assert!(matches!(err, CatalogError::UniqueViolation(_)));
    }
}
