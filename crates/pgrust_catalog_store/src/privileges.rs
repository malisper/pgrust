use std::collections::BTreeSet;

use pgrust_catalog_data::bootstrap::{
    PG_FOREIGN_DATA_WRAPPER_RELATION_OID, PG_FOREIGN_SERVER_RELATION_OID,
    PG_FOREIGN_TABLE_RELATION_OID, PG_USER_MAPPING_RELATION_OID,
};
use pgrust_catalog_data::{
    PG_CATALOG_NAMESPACE_OID, PG_MAINTAIN_OID, PG_READ_ALL_DATA_OID, PG_TOAST_NAMESPACE_OID,
    PG_WRITE_ALL_DATA_OID, PgAuthIdRow, PgAuthMembersRow, PgClassRow,
};

use crate::role_memberships::has_effective_membership;
use crate::roles::has_bypassrls_privilege;

pub fn relation_has_enabled_rls_for_user(
    class_row: &PgClassRow,
    authid_rows: &[PgAuthIdRow],
    auth_members_rows: &[PgAuthMembersRow],
    current_user_oid: u32,
) -> bool {
    if !class_row.relrowsecurity {
        return false;
    }
    if has_bypassrls_privilege(current_user_oid, authid_rows) {
        return false;
    }
    if !class_row.relforcerowsecurity
        && has_effective_membership(
            current_user_oid,
            class_row.relowner,
            authid_rows,
            auth_members_rows,
        )
    {
        return false;
    }
    true
}

pub fn relation_has_table_select_privilege(
    class_row: &PgClassRow,
    authid_rows: &[PgAuthIdRow],
    auth_members_rows: &[PgAuthMembersRow],
    current_user_oid: u32,
) -> bool {
    relation_has_table_privilege(
        class_row,
        authid_rows,
        auth_members_rows,
        current_user_oid,
        'r',
    )
}

fn predefined_role_grants_relation_privilege(
    class_row: &PgClassRow,
    authid_rows: &[PgAuthIdRow],
    auth_members_rows: &[PgAuthMembersRow],
    current_user_oid: u32,
    privilege: char,
) -> bool {
    if matches!(privilege, 'a' | 'w' | 'd' | 'm')
        && matches!(
            class_row.relnamespace,
            PG_CATALOG_NAMESPACE_OID | PG_TOAST_NAMESPACE_OID
        )
    {
        return false;
    }
    let target_role = match privilege {
        'r' => PG_READ_ALL_DATA_OID,
        'a' | 'w' | 'd' => PG_WRITE_ALL_DATA_OID,
        'm' => PG_MAINTAIN_OID,
        _ => return false,
    };
    has_effective_membership(
        current_user_oid,
        target_role,
        authid_rows,
        auth_members_rows,
    )
}

pub fn relation_has_table_privilege(
    class_row: &PgClassRow,
    authid_rows: &[PgAuthIdRow],
    auth_members_rows: &[PgAuthMembersRow],
    current_user_oid: u32,
    privilege: char,
) -> bool {
    if privilege == 'r'
        && matches!(
            class_row.oid,
            PG_FOREIGN_DATA_WRAPPER_RELATION_OID
                | PG_FOREIGN_SERVER_RELATION_OID
                | PG_USER_MAPPING_RELATION_OID
                | PG_FOREIGN_TABLE_RELATION_OID
        )
    {
        return true;
    }
    if has_effective_membership(
        current_user_oid,
        class_row.relowner,
        authid_rows,
        auth_members_rows,
    ) {
        return true;
    }
    if predefined_role_grants_relation_privilege(
        class_row,
        authid_rows,
        auth_members_rows,
        current_user_oid,
        privilege,
    ) {
        return true;
    }

    let effective_names =
        effective_acl_grantee_names(current_user_oid, authid_rows, auth_members_rows);
    class_row
        .relacl
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|item| {
            parse_acl_item(item).is_some_and(|(grantee, privileges)| {
                effective_names.contains(grantee) && privileges.contains(privilege)
            })
        })
}

fn effective_acl_grantee_names<'a>(
    current_user_oid: u32,
    authid_rows: &'a [PgAuthIdRow],
    auth_members_rows: &[PgAuthMembersRow],
) -> BTreeSet<&'a str> {
    let mut names = BTreeSet::from([""]);
    for role in authid_rows {
        if has_effective_membership(current_user_oid, role.oid, authid_rows, auth_members_rows) {
            names.insert(role.rolname.as_str());
        }
    }
    names
}

fn parse_acl_item(item: &str) -> Option<(&str, &str)> {
    let (grantee, rest) = item.split_once('=')?;
    let (privileges, _) = rest.split_once('/')?;
    Some((grantee, privileges))
}
