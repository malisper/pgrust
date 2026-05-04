use std::collections::{BTreeSet, VecDeque};

use pgrust_analyze::CatalogLookup;
use pgrust_catalog_data::{
    BOOTSTRAP_SUPERUSER_OID, PG_AUTHID_RELATION_OID, PG_CATALOG_NAMESPACE_OID,
    PG_DATABASE_OWNER_OID, PG_LARGEOBJECT_RELATION_OID, PG_MAINTAIN_OID, PG_READ_ALL_DATA_OID,
    PG_TOAST_NAMESPACE_OID, PG_WRITE_ALL_DATA_OID, PgAuthIdRow, PgAuthMembersRow, PgClassRow,
    PgNamespaceRow, PgProcRow, PgTypeRow, pg_proc::bootstrap_proc_acl_override,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionError {
    InvalidPrivilegeType(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PrivilegeSpec {
    pub acl_char: char,
    pub grant_option: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RolePrivilegeSpec {
    Usage,
    Member,
    Set,
    Admin,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrivilegeRelationKind {
    Table,
    Sequence,
}

pub const SCHEMA_PRIVILEGES: &[(&str, char, bool)] = &[
    ("CREATE", 'C', false),
    ("CREATE WITH GRANT OPTION", 'C', true),
    ("USAGE", 'U', false),
    ("USAGE WITH GRANT OPTION", 'U', true),
];

pub const TABLE_PRIVILEGES: &[(&str, char, bool)] = &[
    ("SELECT", 'r', false),
    ("SELECT WITH GRANT OPTION", 'r', true),
    ("INSERT", 'a', false),
    ("INSERT WITH GRANT OPTION", 'a', true),
    ("UPDATE", 'w', false),
    ("UPDATE WITH GRANT OPTION", 'w', true),
    ("DELETE", 'd', false),
    ("DELETE WITH GRANT OPTION", 'd', true),
    ("TRUNCATE", 'D', false),
    ("TRUNCATE WITH GRANT OPTION", 'D', true),
    ("REFERENCES", 'x', false),
    ("REFERENCES WITH GRANT OPTION", 'x', true),
    ("TRIGGER", 't', false),
    ("TRIGGER WITH GRANT OPTION", 't', true),
    ("MAINTAIN", 'm', false),
    ("MAINTAIN WITH GRANT OPTION", 'm', true),
];

pub const SEQUENCE_PRIVILEGES: &[(&str, char, bool)] = &[
    ("USAGE", 'U', false),
    ("USAGE WITH GRANT OPTION", 'U', true),
    ("SELECT", 'r', false),
    ("SELECT WITH GRANT OPTION", 'r', true),
    ("UPDATE", 'w', false),
    ("UPDATE WITH GRANT OPTION", 'w', true),
];

pub const COLUMN_PRIVILEGES: &[(&str, char, bool)] = &[
    ("SELECT", 'r', false),
    ("SELECT WITH GRANT OPTION", 'r', true),
    ("INSERT", 'a', false),
    ("INSERT WITH GRANT OPTION", 'a', true),
    ("UPDATE", 'w', false),
    ("UPDATE WITH GRANT OPTION", 'w', true),
    ("REFERENCES", 'x', false),
    ("REFERENCES WITH GRANT OPTION", 'x', true),
];

pub const FUNCTION_PRIVILEGES: &[(&str, char, bool)] = &[
    ("EXECUTE", 'X', false),
    ("EXECUTE WITH GRANT OPTION", 'X', true),
];

pub const TYPE_PRIVILEGES: &[(&str, char, bool)] = &[
    ("USAGE", 'U', false),
    ("USAGE WITH GRANT OPTION", 'U', true),
];

pub const LARGE_OBJECT_PRIVILEGES: &[(&str, char, bool)] = &[
    ("SELECT", 'r', false),
    ("SELECT WITH GRANT OPTION", 'r', true),
    ("UPDATE", 'w', false),
    ("UPDATE WITH GRANT OPTION", 'w', true),
];

pub fn parse_privilege_specs_text(
    privilege_text: &str,
    map: &[(&'static str, char, bool)],
) -> Result<Vec<PrivilegeSpec>, PermissionError> {
    privilege_text
        .split(',')
        .map(str::trim)
        .map(|chunk| {
            map.iter()
                .find(|(name, _, _)| chunk.eq_ignore_ascii_case(name))
                .map(|(_, acl_char, grant_option)| PrivilegeSpec {
                    acl_char: *acl_char,
                    grant_option: *grant_option,
                })
                .ok_or_else(|| PermissionError::InvalidPrivilegeType(chunk.into()))
        })
        .collect()
}

pub fn parse_role_privilege_specs_text(
    privilege_text: &str,
) -> Result<Vec<RolePrivilegeSpec>, PermissionError> {
    privilege_text
        .split(',')
        .map(str::trim)
        .map(|chunk| {
            if chunk.eq_ignore_ascii_case("USAGE") {
                Ok(RolePrivilegeSpec::Usage)
            } else if chunk.eq_ignore_ascii_case("MEMBER") {
                Ok(RolePrivilegeSpec::Member)
            } else if chunk.eq_ignore_ascii_case("SET") {
                Ok(RolePrivilegeSpec::Set)
            } else if chunk.eq_ignore_ascii_case("USAGE WITH GRANT OPTION")
                || chunk.eq_ignore_ascii_case("USAGE WITH ADMIN OPTION")
                || chunk.eq_ignore_ascii_case("MEMBER WITH GRANT OPTION")
                || chunk.eq_ignore_ascii_case("MEMBER WITH ADMIN OPTION")
                || chunk.eq_ignore_ascii_case("SET WITH GRANT OPTION")
                || chunk.eq_ignore_ascii_case("SET WITH ADMIN OPTION")
            {
                Ok(RolePrivilegeSpec::Admin)
            } else {
                Err(PermissionError::InvalidPrivilegeType(chunk.into()))
            }
        })
        .collect()
}

pub trait PermissionCatalog {
    fn class_row_by_oid(&self, relation_oid: u32) -> Option<PgClassRow>;
    fn authid_rows(&self) -> Vec<PgAuthIdRow>;
    fn auth_members_rows(&self) -> Vec<PgAuthMembersRow>;
}

pub fn relation_values_visible_for_error_detail(
    relation_oid: u32,
    current_user_oid: u32,
    catalog: Option<&dyn PermissionCatalog>,
) -> bool {
    if current_user_oid == BOOTSTRAP_SUPERUSER_OID {
        return true;
    }
    let Some(catalog) = catalog else {
        return true;
    };
    let Some(class_row) = catalog.class_row_by_oid(relation_oid) else {
        return true;
    };
    let authid_rows = catalog.authid_rows();
    let auth_members_rows = catalog.auth_members_rows();

    // :HACK: Match PostgreSQL's error-detail leak guard for RLS and
    // table-level SELECT. Once pg_attribute.attacl is modeled, this should
    // also allow users with SELECT on every displayed key column.
    !pgrust_catalog_store::relation_has_enabled_rls_for_user(
        &class_row,
        &authid_rows,
        &auth_members_rows,
        current_user_oid,
    ) && pgrust_catalog_store::relation_has_table_select_privilege(
        &class_row,
        &authid_rows,
        &auth_members_rows,
        current_user_oid,
    )
}

fn acl_item_parts(item: &str) -> Option<(&str, &str, &str)> {
    let (grantee, rest) = item.split_once('=')?;
    let (privileges, grantor) = rest.split_once('/')?;
    Some((grantee, privileges, grantor))
}

pub fn acl_privileges_contain(privileges: &str, spec: PrivilegeSpec) -> bool {
    let mut chars = privileges.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == spec.acl_char {
            return !spec.grant_option || matches!(chars.peek(), Some('*'));
        }
    }
    false
}

pub fn acl_grants_privilege_to_names(
    acl: &[String],
    effective_names: &BTreeSet<String>,
    spec: PrivilegeSpec,
) -> bool {
    acl.iter().any(|item| {
        acl_item_parts(item).is_some_and(|(grantee, privileges, _)| {
            effective_names.contains(grantee) && acl_privileges_contain(privileges, spec)
        })
    })
}

fn role_row_by_oid(authid_rows: &[PgAuthIdRow], role_oid: u32) -> Option<&PgAuthIdRow> {
    authid_rows.iter().find(|role| role.oid == role_oid)
}

pub fn role_is_superuser(authid_rows: &[PgAuthIdRow], role_oid: u32) -> bool {
    role_oid == BOOTSTRAP_SUPERUSER_OID
        || role_row_by_oid(authid_rows, role_oid).is_some_and(|role| role.rolsuper)
}

pub fn role_has_effective_membership(
    role_oid: u32,
    target_oid: u32,
    authid_rows: &[PgAuthIdRow],
    auth_members_rows: &[PgAuthMembersRow],
) -> bool {
    if role_oid == 0 {
        return false;
    }
    pgrust_catalog_store::role_memberships::has_effective_membership(
        role_oid,
        target_oid,
        authid_rows,
        auth_members_rows,
    )
}

pub fn effective_role_names_for_oid(
    catalog: &dyn CatalogLookup,
    role_oid: u32,
) -> BTreeSet<String> {
    let roles = catalog.authid_rows();
    let memberships = catalog.auth_members_rows();
    let mut names = BTreeSet::from([String::new()]);
    if role_oid == 0 {
        return names;
    }
    for role in &roles {
        if pgrust_catalog_store::role_memberships::has_effective_membership(
            role_oid,
            role.oid,
            &roles,
            &memberships,
        ) {
            names.insert(role.rolname.clone());
        }
    }
    names
}

fn is_protected_system_class(class_row: &PgClassRow) -> bool {
    matches!(
        class_row.relnamespace,
        PG_CATALOG_NAMESPACE_OID | PG_TOAST_NAMESPACE_OID
    ) && class_row.relkind != 'v'
}

fn system_catalog_public_select(class_row: &PgClassRow) -> bool {
    class_row.relnamespace == PG_CATALOG_NAMESPACE_OID
        && !matches!(
            class_row.oid,
            PG_AUTHID_RELATION_OID | PG_LARGEOBJECT_RELATION_OID
        )
}

pub fn relation_acl_allows_role(
    catalog: &dyn CatalogLookup,
    role_oid: u32,
    class_row: &PgClassRow,
    spec: PrivilegeSpec,
) -> bool {
    let authid_rows = catalog.authid_rows();
    let auth_members_rows = catalog.auth_members_rows();
    if !role_is_superuser(&authid_rows, role_oid)
        && is_protected_system_class(class_row)
        && matches!(spec.acl_char, 'a' | 'w' | 'd' | 'D' | 'm' | 'U')
    {
        return false;
    }
    if role_is_superuser(&authid_rows, role_oid) {
        return true;
    }
    if role_has_effective_membership(
        role_oid,
        class_row.relowner,
        &authid_rows,
        &auth_members_rows,
    ) {
        return true;
    }
    if spec.acl_char == 'r' && system_catalog_public_select(class_row) && !spec.grant_option {
        return true;
    }
    if spec.acl_char == 'r'
        && role_has_effective_membership(
            role_oid,
            PG_READ_ALL_DATA_OID,
            &authid_rows,
            &auth_members_rows,
        )
    {
        return true;
    }
    if matches!(spec.acl_char, 'a' | 'w' | 'd')
        && role_has_effective_membership(
            role_oid,
            PG_WRITE_ALL_DATA_OID,
            &authid_rows,
            &auth_members_rows,
        )
    {
        return true;
    }
    if spec.acl_char == 'm'
        && role_has_effective_membership(
            role_oid,
            PG_MAINTAIN_OID,
            &authid_rows,
            &auth_members_rows,
        )
    {
        return true;
    }
    class_row.relacl.as_deref().is_some_and(|acl| {
        acl_grants_privilege_to_names(acl, &effective_role_names_for_oid(catalog, role_oid), spec)
    })
}

fn schema_owner_default_acl(owner_name: &str) -> Vec<String> {
    vec![format!("{owner_name}=UC/{owner_name}")]
}

pub fn schema_acl_allows_role(
    catalog: &dyn CatalogLookup,
    role_oid: u32,
    namespace: &PgNamespaceRow,
    spec: PrivilegeSpec,
) -> bool {
    let authid_rows = catalog.authid_rows();
    let auth_members_rows = catalog.auth_members_rows();
    if role_is_superuser(&authid_rows, role_oid)
        || role_has_effective_membership(
            role_oid,
            namespace.nspowner,
            &authid_rows,
            &auth_members_rows,
        )
    {
        return true;
    }
    if spec.acl_char == 'U'
        && !spec.grant_option
        && (role_has_effective_membership(
            role_oid,
            PG_READ_ALL_DATA_OID,
            &authid_rows,
            &auth_members_rows,
        ) || role_has_effective_membership(
            role_oid,
            PG_WRITE_ALL_DATA_OID,
            &authid_rows,
            &auth_members_rows,
        ))
    {
        return true;
    }
    let owner_name = authid_rows
        .iter()
        .find(|row| row.oid == namespace.nspowner)
        .map(|row| row.rolname.clone())
        .unwrap_or_else(|| "postgres".into());
    let acl = namespace
        .nspacl
        .clone()
        .unwrap_or_else(|| schema_owner_default_acl(&owner_name));
    acl_grants_privilege_to_names(&acl, &effective_role_names_for_oid(catalog, role_oid), spec)
}

pub fn membership_path_with(
    start_member: u32,
    target_role: u32,
    rows: &[PgAuthMembersRow],
    edge_allows: impl Fn(&PgAuthMembersRow) -> bool,
) -> bool {
    let mut pending = VecDeque::from([start_member]);
    let mut visited = BTreeSet::new();
    while let Some(member) = pending.pop_front() {
        if !visited.insert(member) {
            continue;
        }
        for edge in rows
            .iter()
            .filter(|row| row.member == member && edge_allows(row))
        {
            if edge.roleid == target_role {
                return true;
            }
            pending.push_back(edge.roleid);
        }
    }
    false
}

pub fn current_database_owner_oid(catalog: &dyn CatalogLookup, database_name: &str) -> Option<u32> {
    catalog
        .database_rows()
        .into_iter()
        .find(|row| row.datname.eq_ignore_ascii_case(database_name))
        .map(|row| row.datdba)
}

pub fn effective_pg_has_role_target(
    target_oid: u32,
    catalog: &dyn CatalogLookup,
    database_name: &str,
) -> Option<u32> {
    if target_oid == PG_DATABASE_OWNER_OID {
        current_database_owner_oid(catalog, database_name)
    } else {
        Some(target_oid)
    }
}

pub fn role_privilege_allowed(
    role_oid: u32,
    target_oid: u32,
    spec: RolePrivilegeSpec,
    catalog: &dyn CatalogLookup,
    database_name: &str,
) -> bool {
    let authid_rows = catalog.authid_rows();
    let auth_members_rows = catalog.auth_members_rows();
    if role_is_superuser(&authid_rows, role_oid) {
        return true;
    }
    let Some(effective_target_oid) =
        effective_pg_has_role_target(target_oid, catalog, database_name)
    else {
        return false;
    };
    match spec {
        RolePrivilegeSpec::Usage => role_has_effective_membership(
            role_oid,
            effective_target_oid,
            &authid_rows,
            &auth_members_rows,
        ),
        RolePrivilegeSpec::Member => {
            role_oid == effective_target_oid
                || membership_path_with(role_oid, effective_target_oid, &auth_members_rows, |_| {
                    true
                })
        }
        RolePrivilegeSpec::Set => {
            role_oid == effective_target_oid
                || membership_path_with(
                    role_oid,
                    effective_target_oid,
                    &auth_members_rows,
                    |edge| edge.set_option,
                )
        }
        RolePrivilegeSpec::Admin => {
            target_oid != PG_DATABASE_OWNER_OID
                && membership_path_with(
                    role_oid,
                    effective_target_oid,
                    &auth_members_rows,
                    |edge| edge.admin_option,
                )
        }
    }
}

pub fn proc_execute_acl_allows_role(
    catalog: &dyn CatalogLookup,
    role_oid: u32,
    row: &PgProcRow,
    grant_option: bool,
) -> bool {
    let explicit_acl = bootstrap_proc_acl_override(row.oid).or_else(|| row.proacl.clone());
    if !grant_option && explicit_acl.is_none() {
        return true;
    }

    let authid_rows = catalog.authid_rows();
    let auth_members_rows = catalog.auth_members_rows();
    if role_is_superuser(&authid_rows, role_oid)
        || role_has_effective_membership(role_oid, row.proowner, &authid_rows, &auth_members_rows)
    {
        return true;
    }
    let owner_name = authid_rows
        .iter()
        .find(|role| role.oid == row.proowner)
        .map(|role| role.rolname.clone())
        .unwrap_or_else(|| "postgres".into());
    let acl = explicit_acl.unwrap_or_else(|| {
        vec![
            format!("{owner_name}=X/{owner_name}"),
            format!("=X/{owner_name}"),
        ]
    });
    let effective_names = effective_role_names_for_oid(catalog, role_oid);
    acl_grants_privilege_to_names(
        &acl,
        &effective_names,
        PrivilegeSpec {
            acl_char: 'X',
            grant_option,
        },
    )
}

pub fn proc_execute_permission_denied_detail(row: &PgProcRow) -> (&'static str, String) {
    let object_kind = match row.prokind {
        'a' => "aggregate",
        'p' => "procedure",
        _ => "function",
    };
    (object_kind, row.proname.clone())
}

pub fn type_privilege_acl_row(catalog: &dyn CatalogLookup, row: PgTypeRow) -> PgTypeRow {
    if row.typelem != 0
        && row.typtype == 'b'
        && let Some(element) = catalog.type_by_oid(row.typelem)
    {
        return type_privilege_acl_row(catalog, element);
    }
    if row.typtype == 'm'
        && let Some(range_row) = catalog
            .range_rows()
            .into_iter()
            .find(|range| range.rngmultitypid == row.oid)
            .and_then(|range| catalog.type_by_oid(range.rngtypid))
    {
        return range_row;
    }
    row
}

pub fn type_acl_allows_role(
    catalog: &dyn CatalogLookup,
    role_oid: u32,
    type_row: &PgTypeRow,
    spec: PrivilegeSpec,
) -> bool {
    let authid_rows = catalog.authid_rows();
    let auth_members_rows = catalog.auth_members_rows();
    if role_is_superuser(&authid_rows, role_oid)
        || role_has_effective_membership(
            role_oid,
            type_row.typowner,
            &authid_rows,
            &auth_members_rows,
        )
    {
        return true;
    }
    let owner_name = authid_rows
        .iter()
        .find(|row| row.oid == type_row.typowner)
        .map(|row| row.rolname.clone())
        .unwrap_or_else(|| "postgres".into());
    let acl = type_row.typacl.clone().unwrap_or_else(|| {
        vec![
            format!("{owner_name}=U/{owner_name}"),
            format!("=U/{owner_name}"),
        ]
    });
    acl_grants_privilege_to_names(&acl, &effective_role_names_for_oid(catalog, role_oid), spec)
}
