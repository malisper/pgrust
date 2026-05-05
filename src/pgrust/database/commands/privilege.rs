use super::super::*;
use crate::backend::access::heap::heapam::{heap_scan_begin_visible, heap_scan_next_visible};
use crate::backend::access::transam::xact::Snapshot;
use crate::backend::catalog::bootstrap::bootstrap_catalog_rel;
use crate::backend::catalog::persistence::{
    delete_catalog_rows_subset_mvcc, insert_catalog_rows_subset_mvcc,
};
use crate::backend::catalog::roles::find_role_by_name;
use crate::backend::catalog::rowcodec::{decode_catalog_tuple_values, pg_shdepend_row_from_values};
use crate::backend::catalog::rows::PhysicalCatalogRows;
use crate::backend::commands::rolecmds::{membership_row, role_management_error};
use crate::backend::parser::{
    AlterDefaultPrivilegesObjectType, AlterDefaultPrivilegesStatement, CatalogLookup,
    GrantAllInSchemaKind, GrantObjectPrivilege, GrantObjectStatement, GrantObjectTarget,
    GrantRoleMembershipStatement, ParseError, RevokeObjectStatement, RevokeRoleMembershipStatement,
    RoleGrantorSpec, RoutineKind, SqlType, SqlTypeKind, TypePrivilegeObjectKind, parse_type_name,
    resolve_raw_type_name,
};
use crate::backend::utils::misc::notices::{push_notice, push_warning};
use crate::include::catalog::pg_language::{
    bootstrap_language_acl_override, language_owner_default_acl,
    set_bootstrap_language_acl_override,
};
use crate::include::catalog::pg_proc::{is_bootstrap_proc_oid, set_bootstrap_proc_acl_override};
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, BootstrapCatalogKind, CURRENT_DATABASE_NAME, CURRENT_DATABASE_OID,
    PG_AUTHID_RELATION_OID, PG_DEFAULT_ACL_RELATION_OID, PgAuthIdRow, PgDefaultAclRow,
    PgForeignDataWrapperRow, PgForeignServerRow, PgShdependRow, PgTypeRow, SHARED_DEPENDENCY_ACL,
    bootstrap_relation_desc, sort_pg_shdepend_rows,
};
use crate::include::nodes::primnodes::RelationDesc;
use crate::pgrust::database::ddl::format_sql_type_name;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

const TABLE_ALL_PRIVILEGE_CHARS: &str = "arwdDxtm";
const TABLE_SELECT_PRIVILEGE_CHARS: &str = "r";
const TABLE_INSERT_PRIVILEGE_CHARS: &str = "a";
const TABLE_UPDATE_PRIVILEGE_CHARS: &str = "w";
const TABLE_DELETE_PRIVILEGE_CHARS: &str = "d";
const TABLE_TRUNCATE_PRIVILEGE_CHARS: &str = "D";
const TABLE_REFERENCES_PRIVILEGE_CHARS: &str = "x";
const TABLE_TRIGGER_PRIVILEGE_CHARS: &str = "t";
const TABLE_MAINTAIN_PRIVILEGE_CHARS: &str = "m";
const SCHEMA_ALL_PRIVILEGE_CHARS: &str = "UC";
const SCHEMA_USAGE_PRIVILEGE_CHARS: &str = "U";
const SCHEMA_CREATE_PRIVILEGE_CHARS: &str = "C";
const TYPE_USAGE_PRIVILEGE_CHARS: &str = "U";
const FUNCTION_EXECUTE_PRIVILEGE_CHARS: &str = "X";
const FOREIGN_USAGE_PRIVILEGE_CHARS: &str = "U";
const LARGE_OBJECT_ALL_PRIVILEGE_CHARS: &str = "rw";
const DEFAULT_ACL_RELATION: char = 'r';
const DEFAULT_ACL_SEQUENCE: char = 'S';
const DEFAULT_ACL_FUNCTION: char = 'f';
const DEFAULT_ACL_TYPE: char = 'T';
const DEFAULT_ACL_SCHEMA: char = 'n';
const DEFAULT_ACL_LARGE_OBJECT: char = 'L';

fn default_acl_objtype_for_statement(object_type: AlterDefaultPrivilegesObjectType) -> char {
    match object_type {
        AlterDefaultPrivilegesObjectType::Tables => DEFAULT_ACL_RELATION,
        AlterDefaultPrivilegesObjectType::Sequences => DEFAULT_ACL_SEQUENCE,
        AlterDefaultPrivilegesObjectType::Functions
        | AlterDefaultPrivilegesObjectType::Routines => DEFAULT_ACL_FUNCTION,
        AlterDefaultPrivilegesObjectType::Types => DEFAULT_ACL_TYPE,
        AlterDefaultPrivilegesObjectType::Schemas => DEFAULT_ACL_SCHEMA,
        AlterDefaultPrivilegesObjectType::LargeObjects => DEFAULT_ACL_LARGE_OBJECT,
    }
}

pub(crate) fn default_acl_allowed_privileges(objtype: char) -> &'static str {
    match objtype {
        DEFAULT_ACL_RELATION => TABLE_ALL_PRIVILEGE_CHARS,
        DEFAULT_ACL_SEQUENCE => "rwU",
        DEFAULT_ACL_FUNCTION => FUNCTION_EXECUTE_PRIVILEGE_CHARS,
        DEFAULT_ACL_TYPE => TYPE_USAGE_PRIVILEGE_CHARS,
        DEFAULT_ACL_SCHEMA => SCHEMA_ALL_PRIVILEGE_CHARS,
        DEFAULT_ACL_LARGE_OBJECT => LARGE_OBJECT_ALL_PRIVILEGE_CHARS,
        _ => "",
    }
}

pub(crate) fn default_acl_hardwired(owner_name: &str, objtype: char) -> Vec<String> {
    match objtype {
        DEFAULT_ACL_RELATION => table_owner_default_acl(owner_name, 'r')
            .into_iter()
            .collect(),
        DEFAULT_ACL_SEQUENCE => table_owner_default_acl(owner_name, 'S')
            .into_iter()
            .collect(),
        DEFAULT_ACL_FUNCTION => function_owner_default_acl(owner_name),
        DEFAULT_ACL_TYPE => type_owner_default_acl(owner_name),
        DEFAULT_ACL_SCHEMA => schema_owner_default_acl(owner_name),
        DEFAULT_ACL_LARGE_OBJECT => vec![format!(
            "{owner_name}={LARGE_OBJECT_ALL_PRIVILEGE_CHARS}/{owner_name}"
        )],
        _ => Vec::new(),
    }
}

fn table_privilege_chars(privilege: &GrantObjectPrivilege) -> Option<&str> {
    match privilege {
        GrantObjectPrivilege::AllPrivilegesOnTable => Some(TABLE_ALL_PRIVILEGE_CHARS),
        GrantObjectPrivilege::SelectOnTable => Some(TABLE_SELECT_PRIVILEGE_CHARS),
        GrantObjectPrivilege::InsertOnTable => Some(TABLE_INSERT_PRIVILEGE_CHARS),
        GrantObjectPrivilege::UpdateOnTable => Some(TABLE_UPDATE_PRIVILEGE_CHARS),
        GrantObjectPrivilege::DeleteOnTable => Some(TABLE_DELETE_PRIVILEGE_CHARS),
        GrantObjectPrivilege::TruncateOnTable => Some(TABLE_TRUNCATE_PRIVILEGE_CHARS),
        GrantObjectPrivilege::ReferencesOnTable => Some(TABLE_REFERENCES_PRIVILEGE_CHARS),
        GrantObjectPrivilege::TriggerOnTable => Some(TABLE_TRIGGER_PRIVILEGE_CHARS),
        GrantObjectPrivilege::MaintainOnTable => Some(TABLE_MAINTAIN_PRIVILEGE_CHARS),
        GrantObjectPrivilege::TablePrivileges(chars) => Some(chars.as_str()),
        _ => None,
    }
}

fn relation_privilege_chars(privilege: &GrantObjectPrivilege, relkind: char) -> Option<&str> {
    if relkind == 'S' && matches!(privilege, GrantObjectPrivilege::AllPrivilegesOnTable) {
        return Some("rwU");
    }
    table_privilege_chars(privilege)
}

fn table_column_privilege_specs(
    privilege: &GrantObjectPrivilege,
    columns: &[String],
) -> Option<Vec<(GrantObjectPrivilege, Vec<String>)>> {
    if !columns.is_empty() {
        return Some(vec![(privilege.clone(), columns.to_vec())]);
    }
    match privilege {
        GrantObjectPrivilege::TableColumnPrivileges(specs) => Some(
            specs
                .iter()
                .map(|spec| (spec.privilege.clone(), spec.columns.clone()))
                .collect(),
        ),
        _ => None,
    }
}

fn object_privilege_chars(privilege: GrantObjectPrivilege) -> Option<&'static str> {
    match privilege {
        GrantObjectPrivilege::AllPrivilegesOnSchema => Some(SCHEMA_ALL_PRIVILEGE_CHARS),
        GrantObjectPrivilege::CreateOnSchema => Some(SCHEMA_CREATE_PRIVILEGE_CHARS),
        GrantObjectPrivilege::UsageOnSchema => Some(SCHEMA_USAGE_PRIVILEGE_CHARS),
        GrantObjectPrivilege::UsageOnType(_) => Some(TYPE_USAGE_PRIVILEGE_CHARS),
        GrantObjectPrivilege::ExecuteOnFunction
        | GrantObjectPrivilege::ExecuteOnProcedure
        | GrantObjectPrivilege::ExecuteOnRoutine => Some(FUNCTION_EXECUTE_PRIVILEGE_CHARS),
        _ => None,
    }
}

#[derive(Clone, Copy)]
enum ForeignUsageObjectKind {
    ForeignDataWrapper,
    ForeignServer,
}

impl ForeignUsageObjectKind {
    fn owner_error_label(self) -> &'static str {
        match self {
            Self::ForeignDataWrapper => "foreign-data wrapper",
            Self::ForeignServer => "foreign server",
        }
    }

    fn usage_privilege_object_type(self) -> &'static str {
        match self {
            Self::ForeignDataWrapper => "foreign-data wrapper",
            Self::ForeignServer => "server",
        }
    }
}

fn foreign_usage_object_kind(privilege: GrantObjectPrivilege) -> Option<ForeignUsageObjectKind> {
    match privilege {
        GrantObjectPrivilege::UsageOnForeignDataWrapper
        | GrantObjectPrivilege::AllPrivilegesOnForeignDataWrapper => {
            Some(ForeignUsageObjectKind::ForeignDataWrapper)
        }
        GrantObjectPrivilege::UsageOnForeignServer
        | GrantObjectPrivilege::AllPrivilegesOnForeignServer => {
            Some(ForeignUsageObjectKind::ForeignServer)
        }
        _ => None,
    }
}

fn table_owner_default_acl(owner_name: &str, relkind: char) -> Option<String> {
    let privileges = match relkind {
        'r' | 'p' | 'v' | 'm' | 'f' => TABLE_ALL_PRIVILEGE_CHARS,
        'S' => "rwU",
        _ => return None,
    };
    Some(format!("{owner_name}={privileges}/{owner_name}"))
}

fn allowed_relation_privilege_chars(relkind: char) -> &'static str {
    match relkind {
        'S' => "rwU",
        _ => TABLE_ALL_PRIVILEGE_CHARS,
    }
}

fn parse_acl_item(item: &str) -> Option<(String, String, String)> {
    let (grantee, rest) = item.split_once('=')?;
    let (privileges, grantor) = rest.split_once('/')?;
    Some((
        grantee.to_string(),
        privileges.to_string(),
        grantor.to_string(),
    ))
}

fn canonicalize_acl_privileges(privileges: &str, allowed: &str) -> String {
    allowed
        .chars()
        .filter(|ch| privileges.contains(*ch))
        .collect()
}

fn acl_privilege_present(privileges: &str, privilege: char) -> bool {
    privileges.chars().any(|ch| ch == privilege)
}

fn acl_privilege_grantable(privileges: &str, privilege: char) -> bool {
    let mut chars = privileges.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == privilege {
            return matches!(chars.peek(), Some('*'));
        }
        if matches!(chars.peek(), Some('*')) {
            chars.next();
        }
    }
    false
}

fn canonicalize_acl_privileges_with_grant_options(
    existing_privileges: &str,
    added_privileges: &str,
    added_grantable: bool,
    allowed: &str,
) -> String {
    let mut result = String::new();
    for ch in allowed.chars() {
        let present =
            acl_privilege_present(existing_privileges, ch) || added_privileges.contains(ch);
        if !present {
            continue;
        }
        result.push(ch);
        if acl_privilege_grantable(existing_privileges, ch)
            || (added_grantable && added_privileges.contains(ch))
        {
            result.push('*');
        }
    }
    result
}

fn remove_acl_privileges_with_grant_options(
    existing_privileges: &str,
    removed_privileges: &str,
    allowed: &str,
) -> String {
    let mut result = String::new();
    for ch in allowed.chars() {
        if !acl_privilege_present(existing_privileges, ch) || removed_privileges.contains(ch) {
            continue;
        }
        result.push(ch);
        if acl_privilege_grantable(existing_privileges, ch) {
            result.push('*');
        }
    }
    result
}

fn acl_entry_grants_all_options(privileges: &str, required_privileges: &str) -> bool {
    required_privileges
        .chars()
        .all(|ch| acl_privilege_grantable(privileges, ch))
}

fn acl_grants_all_options(
    acl: &[String],
    effective_names: &BTreeSet<String>,
    required_privileges: &str,
) -> bool {
    acl.iter().any(|item| {
        parse_acl_item(item)
            .map(|(grantee, privileges, _)| {
                effective_names.contains(&grantee)
                    && acl_entry_grants_all_options(&privileges, required_privileges)
            })
            .unwrap_or(false)
    })
}

fn grantable_acl_privilege_chars(
    acl: &[String],
    effective_names: &BTreeSet<String>,
    requested_privileges: &str,
) -> String {
    requested_privileges
        .chars()
        .filter(|ch| acl_grants_all_options(acl, effective_names, &ch.to_string()))
        .collect()
}

fn warn_grant_privileges(object_name: &str, requested_privileges: &str, granted_privileges: &str) {
    if granted_privileges.is_empty() {
        push_warning(format!("no privileges were granted for \"{object_name}\""));
    } else if granted_privileges.chars().count() < requested_privileges.chars().count() {
        push_warning(format!(
            "not all privileges were granted for \"{object_name}\""
        ));
    }
}

fn warn_revoke_privileges(object_name: &str, requested_privileges: &str, revoked_privileges: &str) {
    if revoked_privileges.is_empty() {
        push_warning(format!(
            "no privileges could be revoked for \"{object_name}\""
        ));
    } else if revoked_privileges.chars().count() < requested_privileges.chars().count() {
        push_warning(format!(
            "not all privileges could be revoked for \"{object_name}\""
        ));
    }
}

fn grant_table_acl_entry(
    acl: &mut Vec<String>,
    grantee: &str,
    grantor: &str,
    privilege_chars: &str,
    allowed: &str,
    grantable: bool,
) {
    if let Some(existing) = acl.iter_mut().find(|item| {
        parse_acl_item(item)
            .map(|(item_grantee, _, item_grantor)| {
                item_grantee == grantee && item_grantor == grantor
            })
            .unwrap_or(false)
    }) {
        let (_, existing_privileges, _) = parse_acl_item(existing).expect("validated above");
        let merged = canonicalize_acl_privileges_with_grant_options(
            &existing_privileges,
            privilege_chars,
            grantable,
            allowed,
        );
        *existing = format!("{grantee}={merged}/{grantor}");
        return;
    }
    acl.push(format!(
        "{grantee}={}/{grantor}",
        canonicalize_acl_privileges_with_grant_options("", privilege_chars, grantable, allowed)
    ));
}

fn revoke_table_acl_entry(
    acl: &mut Vec<String>,
    grantee: &str,
    privilege_chars: &str,
    allowed: &str,
) {
    acl.retain_mut(|item| {
        let Some((item_grantee, existing_privileges, grantor)) = parse_acl_item(item) else {
            return true;
        };
        if item_grantee != grantee {
            return true;
        }
        let remaining = remove_acl_privileges_with_grant_options(
            &existing_privileges,
            privilege_chars,
            allowed,
        );
        if remaining.is_empty() {
            return false;
        }
        *item = format!("{grantee}={remaining}/{grantor}");
        true
    });
}

fn revoke_acl_grant_options_only(
    acl: &mut Vec<String>,
    grantee: &str,
    privilege_chars: &str,
    allowed: &str,
) {
    acl.retain_mut(|item| {
        let Some((item_grantee, existing_privileges, grantor)) = parse_acl_item(item) else {
            return true;
        };
        if item_grantee != grantee {
            return true;
        }
        let mut remaining = String::new();
        for ch in allowed.chars() {
            if !acl_privilege_present(&existing_privileges, ch) {
                continue;
            }
            remaining.push(ch);
            if acl_privilege_grantable(&existing_privileges, ch) && !privilege_chars.contains(ch) {
                remaining.push('*');
            }
        }
        if remaining.is_empty() {
            return false;
        }
        *item = format!("{grantee}={remaining}/{grantor}");
        true
    });
}

fn acl_grantee_has_grant_option(acl: &[String], grantee: &str, privilege: char) -> bool {
    acl.iter().any(|item| {
        parse_acl_item(item)
            .map(|(item_grantee, privileges, _)| {
                item_grantee == grantee && acl_privilege_grantable(&privileges, privilege)
            })
            .unwrap_or(false)
    })
}

fn transform_acl_privilege(
    existing_privileges: &str,
    privilege: char,
    allowed: &str,
    grant_option_for: bool,
) -> String {
    let mut remaining = String::new();
    for ch in allowed.chars() {
        if !acl_privilege_present(existing_privileges, ch) {
            continue;
        }
        if ch == privilege {
            if grant_option_for {
                remaining.push(ch);
            }
            continue;
        }
        remaining.push(ch);
        if acl_privilege_grantable(existing_privileges, ch) {
            remaining.push('*');
        }
    }
    remaining
}

fn revoke_acl_privilege_by_grantor(
    acl: &mut Vec<String>,
    grantee: &str,
    grantor_name: &str,
    privilege: char,
    grant_option_for: bool,
    allowed: &str,
) -> bool {
    let mut changed = false;
    acl.retain_mut(|item| {
        let Some((item_grantee, existing_privileges, grantor)) = parse_acl_item(item) else {
            return true;
        };
        if item_grantee != grantee || grantor != grantor_name {
            return true;
        }
        if !acl_privilege_present(&existing_privileges, privilege)
            || (grant_option_for && !acl_privilege_grantable(&existing_privileges, privilege))
        {
            return true;
        }
        changed = true;
        let remaining =
            transform_acl_privilege(&existing_privileges, privilege, allowed, grant_option_for);
        if remaining.is_empty() {
            return false;
        }
        *item = format!("{grantee}={remaining}/{grantor}");
        true
    });
    changed
}

fn table_acl_dependent_privileges_error() -> ExecError {
    ExecError::DetailedError {
        message: "dependent privileges exist".into(),
        detail: None,
        hint: Some("Use CASCADE to revoke them too.".into()),
        sqlstate: "2BP01",
    }
}

fn cascade_revoke_table_grants_by_grantor(
    acl: &mut Vec<String>,
    grantor_name: &str,
    privilege: char,
    cascade: bool,
    allowed: &str,
) -> Result<(), ExecError> {
    let dependent_grantees = acl
        .iter()
        .filter_map(|item| {
            parse_acl_item(item).and_then(|(item_grantee, privileges, grantor)| {
                (grantor == grantor_name && acl_privilege_present(&privileges, privilege))
                    .then_some(item_grantee)
            })
        })
        .collect::<Vec<_>>();
    if !dependent_grantees.is_empty() && !cascade {
        return Err(table_acl_dependent_privileges_error());
    }
    for dependent_grantee in dependent_grantees {
        revoke_acl_privilege_by_grantor(
            acl,
            &dependent_grantee,
            grantor_name,
            privilege,
            false,
            allowed,
        );
        if !acl_grantee_has_grant_option(acl, &dependent_grantee, privilege) {
            cascade_revoke_table_grants_by_grantor(
                acl,
                &dependent_grantee,
                privilege,
                cascade,
                allowed,
            )?;
        }
    }
    Ok(())
}

fn revoke_table_acl_entry_by_grantor(
    acl: &mut Vec<String>,
    grantee: &str,
    grantor_name: &str,
    privilege_chars: &str,
    allowed: &str,
    grant_option_for: bool,
    cascade: bool,
) -> Result<(), ExecError> {
    for privilege in privilege_chars.chars() {
        let changed = revoke_acl_privilege_by_grantor(
            acl,
            grantee,
            grantor_name,
            privilege,
            grant_option_for,
            allowed,
        );
        if changed && !acl_grantee_has_grant_option(acl, grantee, privilege) {
            cascade_revoke_table_grants_by_grantor(acl, grantee, privilege, cascade, allowed)?;
        }
    }
    Ok(())
}

fn collapse_relation_acl_defaults(
    acl: Vec<String>,
    owner_name: &str,
    relkind: char,
) -> Option<Vec<String>> {
    let default_owner = table_owner_default_acl(owner_name, relkind)?;
    match acl.as_slice() {
        [only] if only == &default_owner => None,
        _ => Some(acl),
    }
}

pub(crate) fn effective_acl_grantee_names(
    auth: &crate::pgrust::auth::AuthState,
    catalog: &crate::pgrust::auth::AuthCatalog,
) -> BTreeSet<String> {
    let mut names = BTreeSet::from([String::new()]);
    for role in catalog.roles() {
        if auth.has_effective_membership(role.oid, catalog) {
            names.insert(role.rolname.clone());
        }
    }
    names
}

pub(crate) fn acl_grants_privilege(
    acl: &[String],
    effective_names: &BTreeSet<String>,
    privilege: char,
) -> bool {
    acl.iter().any(|item| {
        parse_acl_item(item)
            .map(|(grantee, privileges, _)| {
                effective_names.contains(&grantee) && privileges.contains(privilege)
            })
            .unwrap_or(false)
    })
}

fn schema_owner_default_acl(owner_name: &str) -> Vec<String> {
    vec![format!(
        "{owner_name}={SCHEMA_ALL_PRIVILEGE_CHARS}/{owner_name}"
    )]
}

pub(crate) fn type_owner_default_acl(owner_name: &str) -> Vec<String> {
    vec![
        format!("{owner_name}={TYPE_USAGE_PRIVILEGE_CHARS}/{owner_name}"),
        format!("={TYPE_USAGE_PRIVILEGE_CHARS}/{owner_name}"),
    ]
}

pub(crate) fn function_owner_default_acl(owner_name: &str) -> Vec<String> {
    vec![
        format!("{owner_name}={FUNCTION_EXECUTE_PRIVILEGE_CHARS}/{owner_name}"),
        format!("={FUNCTION_EXECUTE_PRIVILEGE_CHARS}/{owner_name}"),
    ]
}

fn foreign_usage_owner_default_acl(owner_name: &str) -> Vec<String> {
    vec![format!(
        "{owner_name}={FOREIGN_USAGE_PRIVILEGE_CHARS}/{owner_name}"
    )]
}

fn collapse_acl_defaults(acl: Vec<String>, defaults: &[String]) -> Option<Vec<String>> {
    if acl == defaults { None } else { Some(acl) }
}

fn usage_acl_has_grant_option(privileges: &str) -> bool {
    let mut chars = privileges.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == 'U' {
            return matches!(chars.peek(), Some('*'));
        }
    }
    false
}

fn usage_acl_privileges(grantable: bool) -> &'static str {
    if grantable { "U*" } else { "U" }
}

fn grant_usage_acl_entry(acl: &mut Vec<String>, grantee: &str, grantor: &str, grantable: bool) {
    if let Some(existing) = acl.iter_mut().find(|item| {
        parse_acl_item(item)
            .map(|(item_grantee, _, item_grantor)| {
                item_grantee == grantee && item_grantor == grantor
            })
            .unwrap_or(false)
    }) {
        let (_, existing_privileges, _) = parse_acl_item(existing).expect("validated above");
        let grantable = grantable || usage_acl_has_grant_option(&existing_privileges);
        *existing = format!("{grantee}={}/{grantor}", usage_acl_privileges(grantable));
        return;
    }
    acl.push(format!(
        "{grantee}={}/{grantor}",
        usage_acl_privileges(grantable)
    ));
}

fn revoke_usage_acl_entry(acl: &mut Vec<String>, grantee: &str) {
    acl.retain_mut(|item| {
        let Some((item_grantee, existing_privileges, grantor)) = parse_acl_item(item) else {
            return true;
        };
        if item_grantee != grantee {
            return true;
        }
        let mut remaining = String::new();
        let mut chars = existing_privileges.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == 'U' {
                if matches!(chars.peek(), Some('*')) {
                    chars.next();
                }
                continue;
            }
            remaining.push(ch);
        }
        if !remaining.contains(FOREIGN_USAGE_PRIVILEGE_CHARS) {
            return false;
        }
        let grantable = usage_acl_has_grant_option(&remaining);
        *item = format!("{grantee}={}/{grantor}", usage_acl_privileges(grantable));
        true
    });
}

fn usage_acl_grantee_has_grant_option(acl: &[String], grantee: &str) -> bool {
    acl.iter().any(|item| {
        parse_acl_item(item)
            .map(|(item_grantee, privileges, _)| {
                item_grantee == grantee && usage_acl_has_grant_option(&privileges)
            })
            .unwrap_or(false)
    })
}

fn cascade_revoke_usage_acl_entry(
    acl: &mut Vec<String>,
    grantee: &str,
    cascade: bool,
) -> Result<(), ExecError> {
    let dependent_grantees = if usage_acl_grantee_has_grant_option(acl, grantee) {
        acl.iter()
            .filter_map(|item| {
                parse_acl_item(item).and_then(|(item_grantee, _, grantor)| {
                    (grantor == grantee && item_grantee != grantee).then_some(item_grantee)
                })
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    if !dependent_grantees.is_empty() && !cascade {
        return Err(ExecError::DetailedError {
            message: "dependent privileges exist".into(),
            detail: None,
            hint: Some("Use CASCADE to revoke them too.".into()),
            sqlstate: "2BP01",
        });
    }
    for dependent_grantee in dependent_grantees {
        cascade_revoke_usage_grants_by_grantor(acl, &dependent_grantee, cascade)?;
    }
    acl.retain(|item| {
        parse_acl_item(item)
            .map(|(_, _, grantor)| grantor != grantee)
            .unwrap_or(true)
    });
    revoke_usage_acl_entry(acl, grantee);
    Ok(())
}

fn cascade_revoke_usage_grants_by_grantor(
    acl: &mut Vec<String>,
    grantor_name: &str,
    cascade: bool,
) -> Result<(), ExecError> {
    let dependent_grantees = acl
        .iter()
        .filter_map(|item| {
            parse_acl_item(item).and_then(|(item_grantee, _, grantor)| {
                (grantor == grantor_name && item_grantee != grantor_name).then_some(item_grantee)
            })
        })
        .collect::<Vec<_>>();
    if !dependent_grantees.is_empty() && !cascade {
        return Err(ExecError::DetailedError {
            message: "dependent privileges exist".into(),
            detail: None,
            hint: Some("Use CASCADE to revoke them too.".into()),
            sqlstate: "2BP01",
        });
    }
    for dependent_grantee in dependent_grantees {
        cascade_revoke_usage_grants_by_grantor(acl, &dependent_grantee, cascade)?;
    }
    acl.retain(|item| {
        parse_acl_item(item)
            .map(|(_, _, grantor)| grantor != grantor_name)
            .unwrap_or(true)
    });
    Ok(())
}

fn grant_acl_entry(
    acl: &mut Vec<String>,
    grantee: &str,
    grantor: &str,
    privilege_chars: &str,
    allowed: &str,
) {
    if let Some(existing) = acl.iter_mut().find(|item| {
        parse_acl_item(item)
            .map(|(item_grantee, _, item_grantor)| {
                item_grantee == grantee && item_grantor == grantor
            })
            .unwrap_or(false)
    }) {
        let (_, existing_privileges, _) = parse_acl_item(existing).expect("validated above");
        let merged = canonicalize_acl_privileges(
            &format!("{existing_privileges}{privilege_chars}"),
            allowed,
        );
        *existing = format!("{grantee}={merged}/{grantor}");
        return;
    }
    acl.push(format!(
        "{grantee}={}/{grantor}",
        canonicalize_acl_privileges(privilege_chars, allowed)
    ));
}

fn revoke_acl_entry(acl: &mut Vec<String>, grantee: &str, privilege_chars: &str, allowed: &str) {
    acl.retain_mut(|item| {
        let Some((item_grantee, existing_privileges, grantor)) = parse_acl_item(item) else {
            return true;
        };
        if item_grantee != grantee {
            return true;
        }
        let remaining: String = existing_privileges
            .chars()
            .filter(|ch| !privilege_chars.contains(*ch))
            .collect();
        let remaining = canonicalize_acl_privileges(&remaining, allowed);
        if remaining.is_empty() {
            return false;
        }
        *item = format!("{grantee}={remaining}/{grantor}");
        true
    });
}

fn revoke_acl_entry_by_grantor(
    acl: &mut Vec<String>,
    grantee: &str,
    grantor_name: &str,
    privilege_chars: &str,
    allowed: &str,
) -> bool {
    let before = acl.clone();
    acl.retain_mut(|item| {
        let Some((item_grantee, existing_privileges, grantor)) = parse_acl_item(item) else {
            return true;
        };
        if item_grantee != grantee || grantor != grantor_name {
            return true;
        }
        let remaining: String = existing_privileges
            .chars()
            .filter(|ch| !privilege_chars.contains(*ch))
            .collect();
        let remaining = canonicalize_acl_privileges(&remaining, allowed);
        if remaining.is_empty() {
            return false;
        }
        *item = format!("{grantee}={remaining}/{grantor}");
        true
    });
    *acl != before
}

fn type_row_is_true_array(row: &PgTypeRow) -> bool {
    row.typelem != 0 && row.typtype == 'b'
}

fn type_row_is_domain(row: &PgTypeRow) -> bool {
    row.typtype == 'd'
}

fn range_row_for_multirange_oid(
    catalog: &dyn CatalogLookup,
    multirange_oid: u32,
) -> Option<PgTypeRow> {
    let range_oid = catalog
        .range_rows()
        .into_iter()
        .find(|row| row.rngmultitypid == multirange_oid)?
        .rngtypid;
    catalog.type_by_oid(range_oid)
}

fn type_usage_acl_target(
    catalog: &dyn CatalogLookup,
    type_oid: u32,
) -> Result<(PgTypeRow, String), ExecError> {
    let row = catalog
        .type_by_oid(type_oid)
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("type with OID {type_oid} does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42704",
        })?;
    if type_row_is_true_array(&row) {
        let element = catalog
            .type_by_oid(row.typelem)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("type with OID {} does not exist", row.typelem),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        let display_name = element.typname.clone();
        if element.typtype == 'm'
            && let Some(range_row) = range_row_for_multirange_oid(catalog, element.oid)
        {
            return Ok((range_row, display_name));
        }
        return Ok((element, display_name));
    }
    if row.typtype == 'm'
        && let Some(range_row) = range_row_for_multirange_oid(catalog, row.oid)
    {
        return Ok((range_row, row.typname));
    }
    Ok((row.clone(), row.typname))
}

fn cannot_set_array_privileges_error() -> ExecError {
    ExecError::DetailedError {
        message: "cannot set privileges of array types".into(),
        detail: None,
        hint: Some("Set the privileges of the element type instead.".into()),
        sqlstate: "42809",
    }
}
fn single_object_name<'a>(
    object_names: &'a [String],
    statement_name: &'static str,
) -> Result<&'a str, ExecError> {
    match object_names {
        [object_name] => Ok(object_name.as_str()),
        [] => Err(ExecError::Parse(ParseError::UnexpectedEof)),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: statement_name,
            actual: object_names.join(", "),
        })),
    }
}

fn parse_granted_function_signature(signature: &str) -> Result<(&str, Vec<&str>), ParseError> {
    let Some(open_paren) = signature.rfind('(') else {
        return Err(ParseError::UnexpectedToken {
            expected: "function signature",
            actual: signature.to_string(),
        });
    };
    if !signature.ends_with(')') {
        return Err(ParseError::UnexpectedToken {
            expected: "function signature",
            actual: signature.to_string(),
        });
    }
    let proc_name = signature[..open_paren].trim();
    if proc_name.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "function name",
            actual: signature.to_string(),
        });
    }
    let arg_sql = &signature[open_paren + 1..signature.len().saturating_sub(1)];
    let args = if arg_sql.trim().is_empty() {
        Vec::new()
    } else {
        arg_sql.split(',').map(str::trim).collect::<Vec<_>>()
    };
    Ok((proc_name, args))
}

fn parse_proc_argtype_oids(argtypes: &str) -> Option<Vec<u32>> {
    if argtypes.trim().is_empty() {
        return Some(Vec::new());
    }
    argtypes
        .split_whitespace()
        .map(|oid| oid.parse::<u32>().ok())
        .collect()
}

pub(crate) fn routine_kind_matches(kind: RoutineKind, prokind: char) -> bool {
    match kind {
        RoutineKind::Function => prokind == 'f',
        RoutineKind::Procedure => prokind == 'p',
        RoutineKind::Aggregate => prokind == 'a',
        RoutineKind::Routine => matches!(prokind, 'f' | 'p'),
    }
}

fn routine_privilege_kind_matches(kind: RoutineKind, prokind: char) -> bool {
    match kind {
        RoutineKind::Function => prokind != 'p',
        RoutineKind::Procedure => prokind == 'p',
        RoutineKind::Aggregate => prokind == 'a',
        RoutineKind::Routine => true,
    }
}

pub(crate) fn routine_kind_name(kind: RoutineKind) -> &'static str {
    match kind {
        RoutineKind::Function => "function",
        RoutineKind::Procedure => "procedure",
        RoutineKind::Aggregate => "aggregate",
        RoutineKind::Routine => "routine",
    }
}

fn routine_signature_display(name: &str, arg_types: &[SqlType]) -> String {
    let args = arg_types
        .iter()
        .copied()
        .map(routine_signature_type_display)
        .collect::<Vec<_>>()
        .join(", ");
    format!("{name}({args})")
}

fn routine_signature_type_display(mut sql_type: SqlType) -> String {
    if sql_type.is_array {
        return format!(
            "{}[]",
            routine_signature_type_display(sql_type.element_type())
        );
    }
    if matches!(
        sql_type.kind,
        SqlTypeKind::Int2
            | SqlTypeKind::Int4
            | SqlTypeKind::Int8
            | SqlTypeKind::Float4
            | SqlTypeKind::Float8
            | SqlTypeKind::Char
            | SqlTypeKind::Varchar
    ) {
        sql_type.type_oid = 0;
    }
    format_sql_type_name(sql_type)
}

fn ensure_function_signature_exists(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    configured_search_path: Option<&[String]>,
    signature: &str,
) -> Result<(), ExecError> {
    let catalog = db.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
    let (proc_name, arg_names) =
        parse_granted_function_signature(signature).map_err(ExecError::Parse)?;
    let (schema_name, base_name) = proc_name
        .rsplit_once('.')
        .map(|(schema, name)| (Some(schema.trim().to_ascii_lowercase()), name.trim()))
        .unwrap_or((None, proc_name));
    let desired_args = arg_names
        .into_iter()
        .map(|arg| {
            let raw_type = parse_type_name(arg)?;
            let sql_type = resolve_raw_type_name(&raw_type, &catalog)?;
            let oid = catalog
                .type_oid_for_sql_type(sql_type)
                .ok_or_else(|| ParseError::UnsupportedType(arg.to_string()))?;
            Ok((oid, sql_type))
        })
        .collect::<Result<Vec<_>, ParseError>>()
        .map_err(ExecError::Parse)?;
    let desired_arg_oids = desired_args.iter().map(|(oid, _)| *oid).collect::<Vec<_>>();
    let desired_arg_types = desired_args
        .iter()
        .map(|(_, sql_type)| *sql_type)
        .collect::<Vec<_>>();
    let display_signature = routine_signature_display(proc_name, &desired_arg_types);
    let schema_oid = match schema_name {
        Some(ref schema_name) => Some(
            db.visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("schema \"{schema_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "3F000",
                })?,
        ),
        None => None,
    };
    let normalized_name = base_name.trim_matches('"').to_ascii_lowercase();
    let exists = catalog
        .proc_rows_by_name(&normalized_name)
        .into_iter()
        .any(|row| {
            parse_proc_argtype_oids(&row.proargtypes) == Some(desired_arg_oids.clone())
                && schema_oid
                    .map(|schema_oid| row.pronamespace == schema_oid)
                    .unwrap_or(true)
        });
    if exists {
        Ok(())
    } else {
        Err(ExecError::DetailedError {
            message: format!("function {display_signature} does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42883",
        })
    }
}

fn lookup_function_row_by_signature(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    configured_search_path: Option<&[String]>,
    signature: &str,
    kind: RoutineKind,
) -> Result<crate::include::catalog::PgProcRow, ExecError> {
    let catalog = db.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
    let (proc_name, arg_names) =
        parse_granted_function_signature(signature).map_err(ExecError::Parse)?;
    let (schema_name, base_name) = proc_name
        .rsplit_once('.')
        .map(|(schema, name)| (Some(schema.trim().to_ascii_lowercase()), name.trim()))
        .unwrap_or((None, proc_name));
    let desired_args = arg_names
        .into_iter()
        .map(|arg| {
            let raw_type = parse_type_name(arg)?;
            let sql_type = resolve_raw_type_name(&raw_type, &catalog)?;
            let oid = catalog
                .type_oid_for_sql_type(sql_type)
                .ok_or_else(|| ParseError::UnsupportedType(arg.to_string()))?;
            Ok((oid, sql_type))
        })
        .collect::<Result<Vec<_>, ParseError>>()
        .map_err(ExecError::Parse)?;
    let desired_arg_oids = desired_args.iter().map(|(oid, _)| *oid).collect::<Vec<_>>();
    let desired_arg_types = desired_args
        .iter()
        .map(|(_, sql_type)| *sql_type)
        .collect::<Vec<_>>();
    let display_signature = routine_signature_display(proc_name, &desired_arg_types);
    let schema_oid = match schema_name {
        Some(ref schema_name) => Some(
            db.visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("schema \"{schema_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "3F000",
                })?,
        ),
        None => None,
    };
    let normalized_name = base_name.trim_matches('"').to_ascii_lowercase();
    let candidates = catalog
        .proc_rows_by_name(&normalized_name)
        .into_iter()
        .filter(|row| {
            parse_proc_argtype_oids(&row.proargtypes) == Some(desired_arg_oids.clone())
                && schema_oid
                    .map(|schema_oid| row.pronamespace == schema_oid)
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    candidates
        .iter()
        .find(|row| routine_privilege_kind_matches(kind, row.prokind))
        .cloned()
        .ok_or_else(|| {
            if !candidates.is_empty() {
                return ExecError::DetailedError {
                    message: format!("{} is not a {}", display_signature, routine_kind_name(kind)),
                    detail: None,
                    hint: None,
                    sqlstate: "42809",
                };
            }
            ExecError::DetailedError {
                message: format!(
                    "{} {display_signature} does not exist",
                    routine_kind_name(kind)
                ),
                detail: None,
                hint: None,
                sqlstate: "42883",
            }
        })
}

impl Database {
    fn execute_tablespace_acl_stmt(
        &self,
        client_id: ClientId,
        object_names: &[String],
    ) -> Result<StatementResult, ExecError> {
        let cache = self
            .backend_catcache(client_id, None)
            .map_err(map_catalog_error)?;
        for name in object_names {
            if !cache
                .tablespace_rows()
                .into_iter()
                .any(|row| row.spcname.eq_ignore_ascii_case(name))
            {
                return Err(ExecError::DetailedError {
                    message: format!("tablespace \"{name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                });
            }
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_default_privileges_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterDefaultPrivilegesStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_default_privileges_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_default_privileges_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterDefaultPrivilegesStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        if !stmt.schema_names.is_empty() {
            match stmt.object_type {
                AlterDefaultPrivilegesObjectType::Schemas => {
                    return Err(ExecError::DetailedError {
                        message: "cannot use IN SCHEMA clause when using GRANT/REVOKE ON SCHEMAS"
                            .into(),
                        detail: None,
                        hint: None,
                        sqlstate: "42601",
                    });
                }
                AlterDefaultPrivilegesObjectType::LargeObjects => {
                    return Err(ExecError::DetailedError {
                        message:
                            "cannot use IN SCHEMA clause when using GRANT/REVOKE ON LARGE OBJECTS"
                                .into(),
                        detail: None,
                        hint: None,
                        sqlstate: "42601",
                    });
                }
                _ => {}
            }
        }

        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let current_user_is_superuser = auth_catalog
            .role_by_oid(auth.current_user_oid())
            .is_some_and(|row| row.rolsuper);
        let mut target_role_oids = if stmt.role_names.is_empty() {
            vec![auth.current_user_oid()]
        } else {
            stmt.role_names
                .iter()
                .map(|role_name| {
                    let role = auth_catalog
                        .role_by_name(role_name)
                        .ok_or_else(|| role_does_not_exist_error(role_name))?;
                    if !current_user_is_superuser
                        && !auth.has_effective_membership(role.oid, &auth_catalog)
                    {
                        return Err(ExecError::DetailedError {
                            message: "permission denied to change default privileges".into(),
                            detail: None,
                            hint: None,
                            sqlstate: "42501",
                        });
                    }
                    Ok(role.oid)
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        target_role_oids.sort_unstable();
        target_role_oids.dedup();
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let mut namespace_oids = if stmt.schema_names.is_empty() {
            vec![0]
        } else {
            stmt.schema_names
                .iter()
                .map(|schema_name| {
                    catcache
                        .namespace_by_name(schema_name)
                        .map(|row| row.oid)
                        .ok_or_else(|| ExecError::DetailedError {
                            message: format!("schema \"{schema_name}\" does not exist"),
                            detail: None,
                            hint: None,
                            sqlstate: "3F000",
                        })
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        namespace_oids.sort_unstable();
        namespace_oids.dedup();
        let objtype = default_acl_objtype_for_statement(stmt.object_type);
        for role_oid in &target_role_oids {
            for namespace_oid in &namespace_oids {
                self.update_default_acl_entry(
                    client_id,
                    *role_oid,
                    *namespace_oid,
                    objtype,
                    &stmt.privilege_chars,
                    &stmt.grantee_names,
                    stmt.with_grant_option,
                    !stmt.is_grant,
                    stmt.grant_option_for,
                    xid,
                    cid,
                    catalog_effects,
                )?;
            }
        }
        self.sync_default_acl_object_address_state(
            &auth_catalog,
            &catcache,
            &target_role_oids,
            &namespace_oids,
            objtype,
            stmt,
        );
        let _ = configured_search_path;
        let _ = stmt.cascade;
        Ok(StatementResult::AffectedRows(0))
    }

    fn sync_default_acl_object_address_state(
        &self,
        auth_catalog: &AuthCatalog,
        catcache: &CatCache,
        target_role_oids: &[u32],
        namespace_oids: &[u32],
        objtype: char,
        stmt: &AlterDefaultPrivilegesStatement,
    ) {
        let mut object_addresses = self.object_addresses.write();
        for role_oid in target_role_oids {
            let Some(role) = auth_catalog.role_by_oid(*role_oid) else {
                continue;
            };
            for namespace_oid in namespace_oids {
                let namespace_oid_opt = (*namespace_oid != 0).then_some(*namespace_oid);
                let namespace_name = namespace_oid_opt
                    .and_then(|oid| catcache.namespace_by_oid(oid))
                    .map(|row| row.nspname.clone());
                if stmt.is_grant
                    && namespace_oid_opt.is_none()
                    && stmt
                        .grantee_names
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(&role.rolname))
                {
                    object_addresses.remove_default_acl(*role_oid, namespace_oid_opt, objtype);
                    continue;
                }
                let grantee_name = stmt.grantee_names.first().map(String::as_str);
                let acl_items =
                    crate::backend::catalog::object_address::default_acl_items_for_object_address(
                        &role.rolname,
                        objtype,
                        stmt.is_grant,
                        grantee_name,
                        &stmt.privilege_chars,
                    );
                object_addresses.upsert_default_acl(
                    *role_oid,
                    role.rolname.clone(),
                    namespace_oid_opt,
                    namespace_name,
                    objtype,
                    acl_items,
                );
            }
        }
    }

    fn update_default_acl_entry(
        &self,
        client_id: ClientId,
        role_oid: u32,
        namespace_oid: u32,
        objtype: char,
        privilege_chars: &str,
        grantee_names: &[String],
        with_grant_option: bool,
        revoke: bool,
        grant_option_for: bool,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let snapshot = snapshot_for_acl_command(self, xid, cid)?;
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let owner_name = auth_catalog
            .role_by_oid(role_oid)
            .map(|row| row.rolname.clone())
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("role with OID {role_oid} does not exist"),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        let existing = self
            .scan_default_acl_rows(client_id, &snapshot)?
            .into_iter()
            .find(|row| {
                row.defaclrole == role_oid
                    && row.defaclnamespace == namespace_oid
                    && row.defaclobjtype == objtype
            });
        let allowed = default_acl_allowed_privileges(objtype);
        let base_acl = if namespace_oid == 0 {
            default_acl_hardwired(&owner_name, objtype)
        } else {
            Vec::new()
        };
        let mut acl = existing
            .as_ref()
            .and_then(|row| row.defaclacl.clone())
            .unwrap_or_else(|| base_acl.clone());
        for grantee_name in grantee_names {
            let grantee_acl_name = if grantee_name.eq_ignore_ascii_case("public") {
                String::new()
            } else {
                auth_catalog
                    .role_by_name(grantee_name)
                    .map(|row| row.rolname.clone())
                    .ok_or_else(|| role_does_not_exist_error(grantee_name))?
            };
            if grant_option_for {
                revoke_acl_grant_options_only(
                    &mut acl,
                    &grantee_acl_name,
                    privilege_chars,
                    allowed,
                );
            } else if revoke {
                revoke_table_acl_entry(&mut acl, &grantee_acl_name, privilege_chars, allowed);
            } else {
                grant_table_acl_entry(
                    &mut acl,
                    &grantee_acl_name,
                    &owner_name,
                    privilege_chars,
                    allowed,
                    with_grant_option,
                );
            }
        }
        let collapsed = if acl == base_acl || acl.is_empty() {
            None
        } else {
            Some(acl)
        };
        let default_acl_oid = if let Some(row) = existing.as_ref() {
            Some(row.oid)
        } else if collapsed.is_some() {
            Some(
                self.catalog
                    .write()
                    .allocate_next_oid(0)
                    .map_err(map_catalog_error)?,
            )
        } else {
            None
        };
        let old_shdepends = existing
            .as_ref()
            .map(|row| self.default_acl_shdepend_rows(client_id, &snapshot, row.oid))
            .transpose()?
            .unwrap_or_default();
        let delete_rows = PhysicalCatalogRows {
            default_acls: existing.iter().cloned().collect(),
            shdepends: old_shdepends,
            ..PhysicalCatalogRows::default()
        };
        let (insert_default_acls, insert_shdepends) = if let (Some(oid), Some(defaclacl)) =
            (default_acl_oid, collapsed)
        {
            let row = PgDefaultAclRow {
                oid,
                defaclrole: role_oid,
                defaclnamespace: namespace_oid,
                defaclobjtype: objtype,
                defaclacl: Some(defaclacl.clone()),
            };
            (
                vec![row],
                default_acl_acl_shdepend_rows(self.database_oid, oid, &defaclacl, &auth_catalog),
            )
        } else {
            (Vec::new(), Vec::new())
        };
        let insert_rows = PhysicalCatalogRows {
            default_acls: insert_default_acls,
            shdepends: insert_shdepends,
            ..PhysicalCatalogRows::default()
        };
        let kinds = [
            BootstrapCatalogKind::PgDefaultAcl,
            BootstrapCatalogKind::PgShdepend,
        ];
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        delete_catalog_rows_subset_mvcc(&ctx, &delete_rows, self.database_oid, &kinds)
            .map_err(map_catalog_error)?;
        insert_catalog_rows_subset_mvcc(&ctx, &insert_rows, self.database_oid, &kinds)
            .map_err(map_catalog_error)?;
        catalog_effects.push(catalog_effect_for_acl(&kinds));
        Ok(())
    }

    pub(crate) fn default_acl_for_new_relation(
        &self,
        client_id: ClientId,
        owner_oid: u32,
        namespace_oid: u32,
        relkind: char,
        xid: TransactionId,
        cid: CommandId,
    ) -> Result<Option<Vec<String>>, ExecError> {
        let objtype = if relkind == 'S' {
            DEFAULT_ACL_SEQUENCE
        } else {
            DEFAULT_ACL_RELATION
        };
        self.default_acl_for_new_object(
            client_id,
            owner_oid,
            Some(namespace_oid),
            objtype,
            xid,
            cid,
        )
    }

    pub(crate) fn default_acl_for_new_type(
        &self,
        client_id: ClientId,
        owner_oid: u32,
        namespace_oid: u32,
        xid: TransactionId,
        cid: CommandId,
    ) -> Result<Option<Vec<String>>, ExecError> {
        self.default_acl_for_new_object(
            client_id,
            owner_oid,
            Some(namespace_oid),
            DEFAULT_ACL_TYPE,
            xid,
            cid,
        )
    }

    pub(crate) fn default_acl_for_new_proc(
        &self,
        client_id: ClientId,
        owner_oid: u32,
        namespace_oid: u32,
        xid: TransactionId,
        cid: CommandId,
    ) -> Result<Option<Vec<String>>, ExecError> {
        self.default_acl_for_new_object(
            client_id,
            owner_oid,
            Some(namespace_oid),
            DEFAULT_ACL_FUNCTION,
            xid,
            cid,
        )
    }

    pub(crate) fn default_acl_for_new_schema(
        &self,
        client_id: ClientId,
        owner_oid: u32,
        xid: TransactionId,
        cid: CommandId,
    ) -> Result<Option<Vec<String>>, ExecError> {
        self.default_acl_for_new_object(client_id, owner_oid, None, DEFAULT_ACL_SCHEMA, xid, cid)
    }

    pub(crate) fn delete_default_acls_for_namespace_in_transaction(
        &self,
        client_id: ClientId,
        namespace_oid: u32,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let snapshot = snapshot_for_acl_command(self, xid, cid)?;
        let mut rows = PhysicalCatalogRows::default();
        for row in self
            .scan_default_acl_rows(client_id, &snapshot)?
            .into_iter()
            .filter(|row| row.defaclnamespace == namespace_oid)
        {
            rows.shdepends
                .extend(self.default_acl_shdepend_rows(client_id, &snapshot, row.oid)?);
            rows.default_acls.push(row);
        }
        if rows.default_acls.is_empty() {
            return Ok(cid);
        }
        let kinds = [
            BootstrapCatalogKind::PgDefaultAcl,
            BootstrapCatalogKind::PgShdepend,
        ];
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        delete_catalog_rows_subset_mvcc(&ctx, &rows, self.database_oid, &kinds)
            .map_err(map_catalog_error)?;
        catalog_effects.push(catalog_effect_for_acl(&kinds));
        Ok(cid.saturating_add(1))
    }

    fn default_acl_for_new_object(
        &self,
        client_id: ClientId,
        owner_oid: u32,
        namespace_oid: Option<u32>,
        objtype: char,
        xid: TransactionId,
        cid: CommandId,
    ) -> Result<Option<Vec<String>>, ExecError> {
        let snapshot = snapshot_for_acl_command(self, xid, cid)?;
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let owner_name = auth_catalog
            .role_by_oid(owner_oid)
            .map(|row| row.rolname.clone())
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("role with OID {owner_oid} does not exist"),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        let hardwired = default_acl_hardwired(&owner_name, objtype);
        let rows = self.scan_default_acl_rows(client_id, &snapshot)?;
        let global_acl = rows
            .iter()
            .find(|row| {
                row.defaclrole == owner_oid
                    && row.defaclnamespace == 0
                    && row.defaclobjtype == objtype
            })
            .and_then(|row| row.defaclacl.clone())
            .unwrap_or_else(|| hardwired.clone());
        let mut merged_acl = global_acl;
        if let Some(namespace_oid) = namespace_oid
            && let Some(schema_acl) = rows
                .iter()
                .find(|row| {
                    row.defaclrole == owner_oid
                        && row.defaclnamespace == namespace_oid
                        && row.defaclobjtype == objtype
                })
                .and_then(|row| row.defaclacl.clone())
        {
            merge_default_acl_overlay(
                &mut merged_acl,
                &schema_acl,
                default_acl_allowed_privileges(objtype),
            );
        }
        Ok((merged_acl != hardwired).then_some(merged_acl))
    }

    pub(crate) fn default_acl_shdepend_rows(
        &self,
        client_id: ClientId,
        snapshot: &Snapshot,
        default_acl_oid: u32,
    ) -> Result<Vec<PgShdependRow>, ExecError> {
        let kind = BootstrapCatalogKind::PgShdepend;
        let rel = bootstrap_catalog_rel(kind, self.database_oid);
        let desc = bootstrap_relation_desc(kind);
        let mut scan = heap_scan_begin_visible(&self.pool, client_id, rel, snapshot.clone())
            .map_err(ExecError::Heap)?;
        let txns = self.txns.read();
        let mut rows = Vec::new();
        while let Some((_, tuple)) = heap_scan_next_visible(&self.pool, client_id, &txns, &mut scan)
            .map_err(ExecError::Heap)?
        {
            let row = pg_shdepend_row_from_values(
                decode_catalog_tuple_values(&desc, &tuple).map_err(map_catalog_error)?,
            )
            .map_err(map_catalog_error)?;
            if row.classid == PG_DEFAULT_ACL_RELATION_OID
                && row.objid == default_acl_oid
                && row.objsubid == 0
                && row.deptype == SHARED_DEPENDENCY_ACL
            {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    fn execute_all_in_schema_acl_stmt_with_search_path(
        &self,
        client_id: ClientId,
        privilege: GrantObjectPrivilege,
        target: &GrantObjectTarget,
        grantee_names: &[String],
        with_grant_option: bool,
        revoke: bool,
        grant_option_for: bool,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_all_in_schema_acl_stmt_in_transaction_with_search_path(
            client_id,
            privilege,
            target,
            grantee_names,
            with_grant_option,
            revoke,
            grant_option_for,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    fn execute_all_in_schema_acl_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        privilege: GrantObjectPrivilege,
        target: &GrantObjectTarget,
        grantee_names: &[String],
        with_grant_option: bool,
        revoke: bool,
        grant_option_for: bool,
        xid: TransactionId,
        cid: CommandId,
        _configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let GrantObjectTarget::AllInSchema { kind, schema_names } = target else {
            return Err(ExecError::Parse(ParseError::UnexpectedEof));
        };
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let grantor_name = auth_catalog
            .role_by_oid(auth.current_user_oid())
            .map(|row| row.rolname.clone())
            .ok_or_else(|| ExecError::DetailedError {
                message: "current user does not exist".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let namespace_oids = schema_names
            .iter()
            .map(|schema_name| {
                catcache
                    .namespace_by_name(schema_name)
                    .map(|row| row.oid)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("schema \"{schema_name}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut current_cid = cid;
        match kind {
            GrantAllInSchemaKind::Tables => {
                let requested_privilege = privilege.clone();
                for namespace_oid in namespace_oids {
                    let rows = catcache
                        .class_rows()
                        .into_iter()
                        .filter(|row| row.relnamespace == namespace_oid)
                        .filter(|row| matches!(row.relkind, 'r' | 'p' | 'v' | 'm' | 'f'))
                        .collect::<Vec<_>>();
                    for row in rows {
                        let owner_name = auth_catalog
                            .role_by_oid(row.relowner)
                            .map(|role| role.rolname.clone())
                            .ok_or_else(|| ExecError::DetailedError {
                                message: format!(
                                    "owner for table \"{}\" does not exist",
                                    row.relname
                                ),
                                detail: None,
                                hint: None,
                                sqlstate: "XX000",
                            })?;
                        if !auth_catalog
                            .role_by_oid(auth.current_user_oid())
                            .is_some_and(|role| role.rolsuper)
                            && !auth.has_effective_membership(row.relowner, &auth_catalog)
                        {
                            return Err(ExecError::DetailedError {
                                message: format!("must be owner of table {}", row.relname),
                                detail: None,
                                hint: None,
                                sqlstate: "42501",
                            });
                        }
                        let privilege_chars =
                            relation_privilege_chars(&requested_privilege, row.relkind)
                                .ok_or_else(|| ExecError::Parse(ParseError::UnexpectedEof))?;
                        let allowed_privilege_chars = allowed_relation_privilege_chars(row.relkind);
                        let mut acl = row.relacl.clone().unwrap_or_else(|| {
                            table_owner_default_acl(&owner_name, row.relkind)
                                .into_iter()
                                .collect()
                        });
                        for grantee_name in grantee_names {
                            let grantee_acl_name = if grantee_name.eq_ignore_ascii_case("public") {
                                String::new()
                            } else {
                                auth_catalog
                                    .role_by_name(grantee_name)
                                    .map(|role| role.rolname.clone())
                                    .ok_or_else(|| role_does_not_exist_error(grantee_name))?
                            };
                            if grant_option_for {
                                revoke_acl_grant_options_only(
                                    &mut acl,
                                    &grantee_acl_name,
                                    privilege_chars,
                                    allowed_privilege_chars,
                                );
                            } else if revoke {
                                revoke_table_acl_entry(
                                    &mut acl,
                                    &grantee_acl_name,
                                    privilege_chars,
                                    allowed_privilege_chars,
                                );
                            } else {
                                grant_table_acl_entry(
                                    &mut acl,
                                    &grantee_acl_name,
                                    &grantor_name,
                                    privilege_chars,
                                    allowed_privilege_chars,
                                    with_grant_option,
                                );
                            }
                        }
                        let ctx = CatalogWriteContext {
                            pool: self.pool.clone(),
                            txns: self.txns.clone(),
                            xid,
                            cid: current_cid,
                            client_id,
                            waiter: None,
                            interrupts: self.interrupt_state(client_id),
                        };
                        let effect = self
                            .catalog
                            .write()
                            .alter_relation_acl_mvcc(
                                row.oid,
                                collapse_relation_acl_defaults(acl, &owner_name, row.relkind),
                                &ctx,
                            )
                            .map_err(map_catalog_error)?;
                        catalog_effects.push(effect);
                        current_cid = current_cid.saturating_add(1);
                    }
                }
            }
            GrantAllInSchemaKind::Functions
            | GrantAllInSchemaKind::Procedures
            | GrantAllInSchemaKind::Routines => {
                let privilege_chars = object_privilege_chars(privilege)
                    .ok_or_else(|| ExecError::Parse(ParseError::UnexpectedEof))?;
                for namespace_oid in namespace_oids {
                    let rows = catcache
                        .proc_rows()
                        .into_iter()
                        .filter(|row| row.pronamespace == namespace_oid)
                        .filter(|row| match kind {
                            GrantAllInSchemaKind::Functions => row.prokind != 'p',
                            GrantAllInSchemaKind::Procedures => row.prokind == 'p',
                            GrantAllInSchemaKind::Routines => true,
                            GrantAllInSchemaKind::Tables => false,
                        })
                        .collect::<Vec<_>>();
                    for row in rows {
                        let owner_name = auth_catalog
                            .role_by_oid(row.proowner)
                            .map(|role| role.rolname.clone())
                            .ok_or_else(|| ExecError::DetailedError {
                                message: format!(
                                    "owner for function \"{}\" does not exist",
                                    row.proname
                                ),
                                detail: None,
                                hint: None,
                                sqlstate: "XX000",
                            })?;
                        if !auth_catalog
                            .role_by_oid(auth.current_user_oid())
                            .is_some_and(|role| role.rolsuper)
                            && !auth.has_effective_membership(row.proowner, &auth_catalog)
                        {
                            return Err(ExecError::DetailedError {
                                message: format!("must be owner of function {}", row.proname),
                                detail: None,
                                hint: None,
                                sqlstate: "42501",
                            });
                        }
                        let mut acl = row
                            .proacl
                            .clone()
                            .unwrap_or_else(|| function_owner_default_acl(&owner_name));
                        for grantee_name in grantee_names {
                            let grantee_acl_name = if grantee_name.eq_ignore_ascii_case("public") {
                                String::new()
                            } else {
                                auth_catalog
                                    .role_by_name(grantee_name)
                                    .map(|role| role.rolname.clone())
                                    .ok_or_else(|| role_does_not_exist_error(grantee_name))?
                            };
                            if grant_option_for {
                                revoke_acl_grant_options_only(
                                    &mut acl,
                                    &grantee_acl_name,
                                    privilege_chars,
                                    FUNCTION_EXECUTE_PRIVILEGE_CHARS,
                                );
                            } else if revoke {
                                revoke_table_acl_entry(
                                    &mut acl,
                                    &grantee_acl_name,
                                    privilege_chars,
                                    FUNCTION_EXECUTE_PRIVILEGE_CHARS,
                                );
                            } else {
                                grant_table_acl_entry(
                                    &mut acl,
                                    &grantee_acl_name,
                                    &grantor_name,
                                    privilege_chars,
                                    FUNCTION_EXECUTE_PRIVILEGE_CHARS,
                                    with_grant_option,
                                );
                            }
                        }
                        let new_acl =
                            collapse_acl_defaults(acl, &function_owner_default_acl(&owner_name));
                        if is_bootstrap_proc_oid(row.oid) {
                            // :HACK: bootstrap pg_proc rows are not physically
                            // replaceable yet; keep full EXECUTE ACL
                            // replacements in a process-local overlay.
                            set_bootstrap_proc_acl_override(row.oid, new_acl);
                            continue;
                        }
                        let ctx = CatalogWriteContext {
                            pool: self.pool.clone(),
                            txns: self.txns.clone(),
                            xid,
                            cid: current_cid,
                            client_id,
                            waiter: None,
                            interrupts: self.interrupt_state(client_id),
                        };
                        let effect = self
                            .catalog
                            .write()
                            .alter_proc_acl_mvcc(row.oid, new_acl, &ctx)
                            .map_err(map_catalog_error)?;
                        catalog_effects.push(effect);
                        current_cid = current_cid.saturating_add(1);
                    }
                }
            }
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_grant_object_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &GrantObjectStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        if matches!(stmt.target, GrantObjectTarget::AllInSchema { .. }) {
            return self.execute_all_in_schema_acl_stmt_with_search_path(
                client_id,
                stmt.privilege.clone(),
                &stmt.target,
                &stmt.grantee_names,
                stmt.with_grant_option,
                false,
                false,
                configured_search_path,
            );
        }
        match stmt.privilege {
            GrantObjectPrivilege::CreateOnDatabase => {
                self.execute_grant_database_create_stmt(client_id, stmt)
            }
            GrantObjectPrivilege::AllPrivilegesOnTable
            | GrantObjectPrivilege::SelectOnTable
            | GrantObjectPrivilege::InsertOnTable
            | GrantObjectPrivilege::UpdateOnTable
            | GrantObjectPrivilege::DeleteOnTable
            | GrantObjectPrivilege::TruncateOnTable
            | GrantObjectPrivilege::ReferencesOnTable
            | GrantObjectPrivilege::TriggerOnTable
            | GrantObjectPrivilege::MaintainOnTable
            | GrantObjectPrivilege::TablePrivileges(_)
            | GrantObjectPrivilege::TableColumnPrivileges(_) => self
                .execute_grant_table_acl_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
                ),
            GrantObjectPrivilege::AllPrivilegesOnSchema
            | GrantObjectPrivilege::CreateOnSchema
            | GrantObjectPrivilege::UsageOnSchema => self
                .execute_grant_schema_acl_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
                ),
            GrantObjectPrivilege::AllPrivilegesOnTablespace
            | GrantObjectPrivilege::CreateOnTablespace => {
                self.execute_tablespace_acl_stmt(client_id, stmt.named_object_names())
            }
            GrantObjectPrivilege::UsageOnType(_) => self
                .execute_grant_type_acl_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
                ),
            GrantObjectPrivilege::UsageOnLanguage
            | GrantObjectPrivilege::AllPrivilegesOnLanguage => self.execute_language_acl_stmt(
                client_id,
                stmt.named_object_names(),
                &stmt.grantee_names,
                stmt.with_grant_option,
                false,
                false,
            ),
            GrantObjectPrivilege::ExecuteOnFunction
            | GrantObjectPrivilege::ExecuteOnProcedure
            | GrantObjectPrivilege::ExecuteOnRoutine => self
                .execute_grant_function_acl_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
                ),
            GrantObjectPrivilege::AllPrivilegesOnLargeObject
            | GrantObjectPrivilege::SelectOnLargeObject
            | GrantObjectPrivilege::UpdateOnLargeObject
            | GrantObjectPrivilege::LargeObjectPrivileges(_) => self
                .execute_grant_large_object_acl_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
                ),
            GrantObjectPrivilege::UsageOnForeignDataWrapper
            | GrantObjectPrivilege::AllPrivilegesOnForeignDataWrapper
            | GrantObjectPrivilege::UsageOnForeignServer
            | GrantObjectPrivilege::AllPrivilegesOnForeignServer => {
                self.execute_grant_foreign_usage_acl_stmt(client_id, stmt)
            }
        }
    }

    pub(crate) fn execute_revoke_object_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &RevokeObjectStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        if matches!(stmt.target, GrantObjectTarget::AllInSchema { .. }) {
            return self.execute_all_in_schema_acl_stmt_in_transaction_with_search_path(
                client_id,
                stmt.privilege.clone(),
                &stmt.target,
                &stmt.grantee_names,
                false,
                true,
                stmt.grant_option_for,
                xid,
                cid,
                configured_search_path,
                catalog_effects,
            );
        }
        match stmt.privilege {
            GrantObjectPrivilege::CreateOnDatabase => {
                self.execute_revoke_database_create_stmt(client_id, stmt)
            }
            GrantObjectPrivilege::AllPrivilegesOnTable
            | GrantObjectPrivilege::SelectOnTable
            | GrantObjectPrivilege::InsertOnTable
            | GrantObjectPrivilege::UpdateOnTable
            | GrantObjectPrivilege::DeleteOnTable
            | GrantObjectPrivilege::TruncateOnTable
            | GrantObjectPrivilege::ReferencesOnTable
            | GrantObjectPrivilege::TriggerOnTable
            | GrantObjectPrivilege::MaintainOnTable
            | GrantObjectPrivilege::TablePrivileges(_)
            | GrantObjectPrivilege::TableColumnPrivileges(_) => self
                .execute_revoke_table_acl_stmt_in_transaction_with_search_path(
                    client_id,
                    stmt,
                    xid,
                    cid,
                    configured_search_path,
                    catalog_effects,
                ),
            GrantObjectPrivilege::AllPrivilegesOnSchema
            | GrantObjectPrivilege::CreateOnSchema
            | GrantObjectPrivilege::UsageOnSchema => self
                .execute_schema_acl_stmt_in_transaction_with_search_path(
                    client_id,
                    stmt.privilege.clone(),
                    stmt.named_object_names(),
                    &stmt.grantee_names,
                    xid,
                    cid,
                    configured_search_path,
                    catalog_effects,
                    true,
                ),
            GrantObjectPrivilege::AllPrivilegesOnTablespace
            | GrantObjectPrivilege::CreateOnTablespace => {
                self.execute_tablespace_acl_stmt(client_id, stmt.named_object_names())
            }
            GrantObjectPrivilege::UsageOnType(_) => self
                .execute_type_acl_stmt_in_transaction_with_search_path(
                    client_id,
                    stmt.privilege.clone(),
                    stmt.named_object_names(),
                    &stmt.grantee_names,
                    xid,
                    cid,
                    configured_search_path,
                    catalog_effects,
                    true,
                    false,
                ),
            GrantObjectPrivilege::UsageOnLanguage
            | GrantObjectPrivilege::AllPrivilegesOnLanguage => self.execute_language_acl_stmt(
                client_id,
                stmt.named_object_names(),
                &stmt.grantee_names,
                false,
                true,
                stmt.grant_option_for,
            ),
            GrantObjectPrivilege::ExecuteOnFunction
            | GrantObjectPrivilege::ExecuteOnProcedure
            | GrantObjectPrivilege::ExecuteOnRoutine => self
                .execute_function_acl_stmt_in_transaction_with_search_path(
                    client_id,
                    stmt.privilege.clone(),
                    stmt.named_object_names(),
                    &stmt.grantee_names,
                    xid,
                    cid,
                    configured_search_path,
                    catalog_effects,
                    true,
                ),
            GrantObjectPrivilege::AllPrivilegesOnLargeObject
            | GrantObjectPrivilege::SelectOnLargeObject
            | GrantObjectPrivilege::UpdateOnLargeObject
            | GrantObjectPrivilege::LargeObjectPrivileges(_) => self
                .execute_large_object_acl_stmt_in_transaction(
                    client_id,
                    stmt.privilege.clone(),
                    stmt.named_object_names(),
                    &stmt.grantee_names,
                    false,
                    xid,
                    cid,
                    catalog_effects,
                    true,
                ),
            GrantObjectPrivilege::UsageOnForeignDataWrapper
            | GrantObjectPrivilege::AllPrivilegesOnForeignDataWrapper
            | GrantObjectPrivilege::UsageOnForeignServer
            | GrantObjectPrivilege::AllPrivilegesOnForeignServer => self
                .execute_foreign_usage_acl_stmt_in_transaction(
                    client_id,
                    stmt.privilege.clone(),
                    stmt.named_object_names(),
                    &stmt.grantee_names,
                    false,
                    xid,
                    cid,
                    catalog_effects,
                    true,
                    stmt.cascade,
                ),
        }
    }

    pub(crate) fn execute_grant_object_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &GrantObjectStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        if matches!(stmt.target, GrantObjectTarget::AllInSchema { .. }) {
            return self.execute_all_in_schema_acl_stmt_in_transaction_with_search_path(
                client_id,
                stmt.privilege.clone(),
                &stmt.target,
                &stmt.grantee_names,
                stmt.with_grant_option,
                false,
                false,
                xid,
                cid,
                configured_search_path,
                catalog_effects,
            );
        }
        match stmt.privilege {
            GrantObjectPrivilege::CreateOnDatabase => {
                self.execute_grant_database_create_stmt(client_id, stmt)
            }
            GrantObjectPrivilege::AllPrivilegesOnTable
            | GrantObjectPrivilege::SelectOnTable
            | GrantObjectPrivilege::InsertOnTable
            | GrantObjectPrivilege::UpdateOnTable
            | GrantObjectPrivilege::DeleteOnTable
            | GrantObjectPrivilege::TruncateOnTable
            | GrantObjectPrivilege::ReferencesOnTable
            | GrantObjectPrivilege::TriggerOnTable
            | GrantObjectPrivilege::MaintainOnTable
            | GrantObjectPrivilege::TablePrivileges(_)
            | GrantObjectPrivilege::TableColumnPrivileges(_) => self
                .execute_grant_table_acl_stmt_in_transaction_with_search_path(
                    client_id,
                    stmt,
                    xid,
                    cid,
                    configured_search_path,
                    catalog_effects,
                ),
            GrantObjectPrivilege::AllPrivilegesOnSchema
            | GrantObjectPrivilege::CreateOnSchema
            | GrantObjectPrivilege::UsageOnSchema => self
                .execute_schema_acl_stmt_in_transaction_with_search_path(
                    client_id,
                    stmt.privilege.clone(),
                    stmt.named_object_names(),
                    &stmt.grantee_names,
                    xid,
                    cid,
                    configured_search_path,
                    catalog_effects,
                    false,
                ),
            GrantObjectPrivilege::AllPrivilegesOnTablespace
            | GrantObjectPrivilege::CreateOnTablespace => {
                self.execute_tablespace_acl_stmt(client_id, stmt.named_object_names())
            }
            GrantObjectPrivilege::UsageOnType(_) => self
                .execute_type_acl_stmt_in_transaction_with_search_path(
                    client_id,
                    stmt.privilege.clone(),
                    stmt.named_object_names(),
                    &stmt.grantee_names,
                    xid,
                    cid,
                    configured_search_path,
                    catalog_effects,
                    false,
                    stmt.with_grant_option,
                ),
            GrantObjectPrivilege::UsageOnLanguage
            | GrantObjectPrivilege::AllPrivilegesOnLanguage => self.execute_language_acl_stmt(
                client_id,
                stmt.named_object_names(),
                &stmt.grantee_names,
                stmt.with_grant_option,
                false,
                false,
            ),
            GrantObjectPrivilege::ExecuteOnFunction
            | GrantObjectPrivilege::ExecuteOnProcedure
            | GrantObjectPrivilege::ExecuteOnRoutine => self
                .execute_function_acl_stmt_in_transaction_with_search_path(
                    client_id,
                    stmt.privilege.clone(),
                    stmt.named_object_names(),
                    &stmt.grantee_names,
                    xid,
                    cid,
                    configured_search_path,
                    catalog_effects,
                    false,
                ),
            GrantObjectPrivilege::AllPrivilegesOnLargeObject
            | GrantObjectPrivilege::SelectOnLargeObject
            | GrantObjectPrivilege::UpdateOnLargeObject
            | GrantObjectPrivilege::LargeObjectPrivileges(_) => self
                .execute_large_object_acl_stmt_in_transaction(
                    client_id,
                    stmt.privilege.clone(),
                    stmt.named_object_names(),
                    &stmt.grantee_names,
                    stmt.with_grant_option,
                    xid,
                    cid,
                    catalog_effects,
                    false,
                ),
            GrantObjectPrivilege::UsageOnForeignDataWrapper
            | GrantObjectPrivilege::AllPrivilegesOnForeignDataWrapper
            | GrantObjectPrivilege::UsageOnForeignServer
            | GrantObjectPrivilege::AllPrivilegesOnForeignServer => self
                .execute_foreign_usage_acl_stmt_in_transaction(
                    client_id,
                    stmt.privilege.clone(),
                    stmt.named_object_names(),
                    &stmt.grantee_names,
                    stmt.with_grant_option,
                    xid,
                    cid,
                    catalog_effects,
                    false,
                    false,
                ),
        }
    }

    pub(crate) fn execute_revoke_object_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &RevokeObjectStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        if matches!(stmt.target, GrantObjectTarget::AllInSchema { .. }) {
            return self.execute_all_in_schema_acl_stmt_with_search_path(
                client_id,
                stmt.privilege.clone(),
                &stmt.target,
                &stmt.grantee_names,
                false,
                true,
                stmt.grant_option_for,
                configured_search_path,
            );
        }
        match stmt.privilege {
            GrantObjectPrivilege::CreateOnDatabase => {
                self.execute_revoke_database_create_stmt(client_id, stmt)
            }
            GrantObjectPrivilege::AllPrivilegesOnTable
            | GrantObjectPrivilege::SelectOnTable
            | GrantObjectPrivilege::InsertOnTable
            | GrantObjectPrivilege::UpdateOnTable
            | GrantObjectPrivilege::DeleteOnTable
            | GrantObjectPrivilege::TruncateOnTable
            | GrantObjectPrivilege::ReferencesOnTable
            | GrantObjectPrivilege::TriggerOnTable
            | GrantObjectPrivilege::MaintainOnTable
            | GrantObjectPrivilege::TablePrivileges(_)
            | GrantObjectPrivilege::TableColumnPrivileges(_) => self
                .execute_revoke_table_acl_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
                ),
            GrantObjectPrivilege::AllPrivilegesOnSchema
            | GrantObjectPrivilege::CreateOnSchema
            | GrantObjectPrivilege::UsageOnSchema => self
                .execute_revoke_schema_acl_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
                ),
            GrantObjectPrivilege::AllPrivilegesOnTablespace
            | GrantObjectPrivilege::CreateOnTablespace => {
                self.execute_tablespace_acl_stmt(client_id, stmt.named_object_names())
            }
            GrantObjectPrivilege::UsageOnType(_) => self
                .execute_revoke_type_acl_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
                ),
            GrantObjectPrivilege::UsageOnLanguage
            | GrantObjectPrivilege::AllPrivilegesOnLanguage => self.execute_language_acl_stmt(
                client_id,
                stmt.named_object_names(),
                &stmt.grantee_names,
                false,
                true,
                stmt.grant_option_for,
            ),
            GrantObjectPrivilege::ExecuteOnFunction
            | GrantObjectPrivilege::ExecuteOnProcedure
            | GrantObjectPrivilege::ExecuteOnRoutine => self
                .execute_revoke_function_acl_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
                ),
            GrantObjectPrivilege::AllPrivilegesOnLargeObject
            | GrantObjectPrivilege::SelectOnLargeObject
            | GrantObjectPrivilege::UpdateOnLargeObject
            | GrantObjectPrivilege::LargeObjectPrivileges(_) => self
                .execute_revoke_large_object_acl_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
                ),
            GrantObjectPrivilege::UsageOnForeignDataWrapper
            | GrantObjectPrivilege::AllPrivilegesOnForeignDataWrapper
            | GrantObjectPrivilege::UsageOnForeignServer
            | GrantObjectPrivilege::AllPrivilegesOnForeignServer => {
                self.execute_revoke_foreign_usage_acl_stmt(client_id, stmt)
            }
        }
    }

    fn execute_revoke_type_usage_stmt(
        &self,
        client_id: ClientId,
        stmt: &RevokeObjectStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let type_name = single_object_name(stmt.named_object_names(), "single type name")?;
        let search_path = self.effective_search_path(client_id, configured_search_path);
        let auth_catalog = self
            .auth_catalog(client_id, None)
            .map_err(map_catalog_error)?;
        let mut range_types = self.range_types.write();
        if let Some(entry) = range_types.values().find(|entry| {
            entry.multirange_name.eq_ignore_ascii_case(type_name)
                && type_namespace_visible(entry.namespace_oid, &search_path)
        }) {
            return Err(cannot_set_multirange_privileges_error(&entry.name));
        }
        let Some((range_key, _)) = range_types.iter().find(|(_, entry)| {
            entry.name.eq_ignore_ascii_case(type_name)
                && type_namespace_visible(entry.namespace_oid, &search_path)
        }) else {
            return Err(ExecError::Parse(ParseError::UnsupportedType(
                type_name.to_string(),
            )));
        };
        let range_key = range_key.clone();
        let entry = range_types
            .get_mut(&range_key)
            .expect("range key found in snapshot");
        for grantee_name in &stmt.grantee_names {
            if grantee_name.eq_ignore_ascii_case("public") {
                entry.public_usage = false;
                continue;
            }
            let grantee = find_role_by_name(auth_catalog.roles(), grantee_name)
                .ok_or_else(|| role_does_not_exist_error(grantee_name))?;
            if grantee.oid == entry.owner_oid {
                entry.owner_usage = false;
            }
        }
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_grant_table_acl_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &GrantObjectStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_grant_table_acl_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    fn execute_revoke_table_acl_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &RevokeObjectStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_revoke_table_acl_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    fn execute_grant_table_acl_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &GrantObjectStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        if stmt.named_object_names().is_empty() {
            return Err(ExecError::Parse(ParseError::UnexpectedEof));
        }
        let auth = self.auth_state(client_id);
        let mut current_cid = cid;
        for object_name in stmt.named_object_names() {
            let catalog = self.lazy_catalog_lookup(
                client_id,
                Some((xid, current_cid)),
                configured_search_path,
            );
            let relation = catalog.lookup_any_relation(object_name).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(object_name.to_string()))
            })?;
            if !matches!(relation.relkind, 'r' | 'p' | 'v' | 'm' | 'f' | 'S') {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: object_name.to_string(),
                    expected: "table",
                }));
            }
            let auth_catalog = self
                .auth_catalog(client_id, Some((xid, current_cid)))
                .map_err(map_catalog_error)?;
            let current_user_can_grant_as_owner = auth_catalog
                .role_by_oid(auth.current_user_oid())
                .is_some_and(|row| row.rolsuper)
                || auth.has_effective_membership(relation.owner_oid, &auth_catalog);
            let effective_names = (!current_user_can_grant_as_owner)
                .then(|| effective_acl_grantee_names(&auth, &auth_catalog));
            let owner_name = auth_catalog
                .role_by_oid(relation.owner_oid)
                .map(|row| row.rolname.clone())
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("owner for table \"{object_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })?;
            let grantor_name = auth_catalog
                .role_by_oid(auth.current_user_oid())
                .map(|row| row.rolname.clone())
                .ok_or_else(|| ExecError::DetailedError {
                    message: "current user does not exist".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })?;
            let catcache = self
                .backend_catcache(client_id, Some((xid, current_cid)))
                .map_err(map_catalog_error)?;
            if let Some(column_specs) = table_column_privilege_specs(&stmt.privilege, &stmt.columns)
            {
                let mut column_acls = BTreeMap::<i16, Vec<String>>::new();
                let mut relation_privilege_chars_acc = String::new();
                for (column_privilege, columns) in column_specs {
                    let privilege_chars = table_privilege_chars(&column_privilege)
                        .ok_or_else(|| ExecError::Parse(ParseError::UnexpectedEof))?;
                    if columns.is_empty() {
                        relation_privilege_chars_acc.push_str(privilege_chars);
                        continue;
                    }
                    for column_name in columns {
                        let Some((column_index, column)) = relation
                            .desc
                            .columns
                            .iter()
                            .enumerate()
                            .find(|(_, column)| {
                                !column.dropped && column.name.eq_ignore_ascii_case(&column_name)
                            })
                        else {
                            if column_name.eq_ignore_ascii_case("tableoid") {
                                continue;
                            }
                            return Err(ExecError::Parse(ParseError::UnknownColumn(
                                column_name.clone(),
                            )));
                        };
                        let attnum = column_index.saturating_add(1) as i16;
                        let acl = column_acls
                            .entry(attnum)
                            .or_insert_with(|| column.attacl.clone().unwrap_or_default());
                        let effective_privilege_chars;
                        let grant_privilege_chars = if current_user_can_grant_as_owner {
                            privilege_chars
                        } else {
                            effective_privilege_chars = grantable_acl_privilege_chars(
                                acl,
                                effective_names.as_ref().expect("effective names"),
                                privilege_chars,
                            );
                            if effective_privilege_chars.is_empty() {
                                if !privilege_chars.chars().any(|ch| {
                                    acl_grants_privilege(
                                        acl,
                                        effective_names.as_ref().expect("effective names"),
                                        ch,
                                    )
                                }) {
                                    return Err(ExecError::DetailedError {
                                        message: format!(
                                            "permission denied for table {object_name}"
                                        ),
                                        detail: None,
                                        hint: None,
                                        sqlstate: "42501",
                                    });
                                }
                                warn_grant_privileges(
                                    &format!("{column_name} of relation {object_name}"),
                                    privilege_chars,
                                    &effective_privilege_chars,
                                );
                                continue;
                            }
                            warn_grant_privileges(
                                &format!("{column_name} of relation {object_name}"),
                                privilege_chars,
                                &effective_privilege_chars,
                            );
                            effective_privilege_chars.as_str()
                        };
                        for grantee_name in &stmt.grantee_names {
                            let grantee_acl_name = if grantee_name.eq_ignore_ascii_case("public") {
                                String::new()
                            } else {
                                auth_catalog
                                    .role_by_name(grantee_name)
                                    .map(|row| row.rolname.clone())
                                    .ok_or_else(|| {
                                        ExecError::Parse(role_management_error(format!(
                                            "role \"{}\" does not exist",
                                            grantee_name
                                        )))
                                    })?
                            };
                            grant_table_acl_entry(
                                acl,
                                &grantee_acl_name,
                                &grantor_name,
                                grant_privilege_chars,
                                TABLE_ALL_PRIVILEGE_CHARS,
                                stmt.with_grant_option,
                            );
                        }
                    }
                }
                let relation_privilege_chars_acc = canonicalize_acl_privileges(
                    &relation_privilege_chars_acc,
                    TABLE_ALL_PRIVILEGE_CHARS,
                );
                if !relation_privilege_chars_acc.is_empty() {
                    let allowed_privilege_chars =
                        allowed_relation_privilege_chars(relation.relkind);
                    let mut acl = catcache
                        .class_by_oid(relation.relation_oid)
                        .and_then(|row| row.relacl.clone())
                        .unwrap_or_else(|| {
                            table_owner_default_acl(&owner_name, relation.relkind)
                                .into_iter()
                                .collect()
                        });
                    let effective_privilege_chars;
                    let grant_privilege_chars = if current_user_can_grant_as_owner {
                        relation_privilege_chars_acc.as_str()
                    } else {
                        effective_privilege_chars = grantable_acl_privilege_chars(
                            &acl,
                            effective_names.as_ref().expect("effective names"),
                            &relation_privilege_chars_acc,
                        );
                        if effective_privilege_chars.is_empty()
                            && !relation_privilege_chars_acc.chars().any(|ch| {
                                acl_grants_privilege(
                                    &acl,
                                    effective_names.as_ref().expect("effective names"),
                                    ch,
                                )
                            })
                        {
                            return Err(ExecError::DetailedError {
                                message: format!("permission denied for table {object_name}"),
                                detail: None,
                                hint: None,
                                sqlstate: "42501",
                            });
                        }
                        warn_grant_privileges(
                            object_name,
                            &relation_privilege_chars_acc,
                            &effective_privilege_chars,
                        );
                        effective_privilege_chars.as_str()
                    };
                    if !grant_privilege_chars.is_empty() {
                        for grantee_name in &stmt.grantee_names {
                            let grantee_acl_name = if grantee_name.eq_ignore_ascii_case("public") {
                                String::new()
                            } else {
                                auth_catalog
                                    .role_by_name(grantee_name)
                                    .map(|row| row.rolname.clone())
                                    .ok_or_else(|| {
                                        ExecError::Parse(role_management_error(format!(
                                            "role \"{}\" does not exist",
                                            grantee_name
                                        )))
                                    })?
                            };
                            grant_table_acl_entry(
                                &mut acl,
                                &grantee_acl_name,
                                &grantor_name,
                                grant_privilege_chars,
                                allowed_privilege_chars,
                                stmt.with_grant_option,
                            );
                        }
                        let ctx = CatalogWriteContext {
                            pool: self.pool.clone(),
                            txns: self.txns.clone(),
                            xid,
                            cid: current_cid,
                            client_id,
                            waiter: None,
                            interrupts: self.interrupt_state(client_id),
                        };
                        let effect = self
                            .catalog
                            .write()
                            .alter_relation_acl_mvcc(
                                relation.relation_oid,
                                collapse_relation_acl_defaults(acl, &owner_name, relation.relkind),
                                &ctx,
                            )
                            .map_err(map_catalog_error)?;
                        catalog_effects.push(effect);
                        current_cid = current_cid.saturating_add(1);
                    }
                }
                if !column_acls.is_empty() {
                    let ctx = CatalogWriteContext {
                        pool: self.pool.clone(),
                        txns: self.txns.clone(),
                        xid,
                        cid: current_cid,
                        client_id,
                        waiter: None,
                        interrupts: self.interrupt_state(client_id),
                    };
                    let effect = self
                        .catalog
                        .write()
                        .alter_attribute_acls_mvcc(
                            relation.relation_oid,
                            column_acls
                                .into_iter()
                                .map(|(attnum, acl)| (attnum, (!acl.is_empty()).then_some(acl)))
                                .collect(),
                            &ctx,
                        )
                        .map_err(map_catalog_error)?;
                    catalog_effects.push(effect);
                    current_cid = current_cid.saturating_add(1);
                }
                continue;
            }

            let privilege_chars = relation_privilege_chars(&stmt.privilege, relation.relkind)
                .ok_or_else(|| ExecError::Parse(ParseError::UnexpectedEof))?;
            let allowed_privilege_chars = allowed_relation_privilege_chars(relation.relkind);
            let mut acl = catcache
                .class_by_oid(relation.relation_oid)
                .and_then(|row| row.relacl.clone())
                .unwrap_or_else(|| {
                    table_owner_default_acl(&owner_name, relation.relkind)
                        .into_iter()
                        .collect()
                });
            let effective_privilege_chars;
            let grant_privilege_chars = if current_user_can_grant_as_owner {
                privilege_chars
            } else {
                effective_privilege_chars = grantable_acl_privilege_chars(
                    &acl,
                    effective_names.as_ref().expect("effective names"),
                    privilege_chars,
                );
                if effective_privilege_chars.is_empty() {
                    if !privilege_chars.chars().any(|ch| {
                        acl_grants_privilege(
                            &acl,
                            effective_names.as_ref().expect("effective names"),
                            ch,
                        )
                    }) {
                        return Err(ExecError::DetailedError {
                            message: format!("permission denied for table {object_name}"),
                            detail: None,
                            hint: None,
                            sqlstate: "42501",
                        });
                    }
                    warn_grant_privileges(object_name, privilege_chars, &effective_privilege_chars);
                    continue;
                }
                warn_grant_privileges(object_name, privilege_chars, &effective_privilege_chars);
                effective_privilege_chars.as_str()
            };
            for grantee_name in &stmt.grantee_names {
                let grantee_acl_name = if grantee_name.eq_ignore_ascii_case("public") {
                    String::new()
                } else {
                    auth_catalog
                        .role_by_name(grantee_name)
                        .map(|row| row.rolname.clone())
                        .ok_or_else(|| {
                            ExecError::Parse(role_management_error(format!(
                                "role \"{}\" does not exist",
                                grantee_name
                            )))
                        })?
                };
                grant_table_acl_entry(
                    &mut acl,
                    &grantee_acl_name,
                    &grantor_name,
                    grant_privilege_chars,
                    allowed_privilege_chars,
                    stmt.with_grant_option,
                );
            }
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: current_cid,
                client_id,
                waiter: None,
                interrupts: self.interrupt_state(client_id),
            };
            let effect = self
                .catalog
                .write()
                .alter_relation_acl_mvcc(
                    relation.relation_oid,
                    collapse_relation_acl_defaults(acl, &owner_name, relation.relkind),
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
            current_cid = current_cid.saturating_add(1);
        }
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_revoke_table_acl_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &RevokeObjectStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        if stmt.named_object_names().is_empty() {
            return Err(ExecError::Parse(ParseError::UnexpectedEof));
        }
        let auth = self.auth_state(client_id);
        let mut current_cid = cid;
        for object_name in stmt.named_object_names() {
            let catalog = self.lazy_catalog_lookup(
                client_id,
                Some((xid, current_cid)),
                configured_search_path,
            );
            let relation = catalog.lookup_any_relation(object_name).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(object_name.to_string()))
            })?;
            if !matches!(relation.relkind, 'r' | 'p' | 'v' | 'm' | 'f' | 'S') {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: object_name.to_string(),
                    expected: "table",
                }));
            }
            let auth_catalog = self
                .auth_catalog(client_id, Some((xid, current_cid)))
                .map_err(map_catalog_error)?;
            let current_user_can_revoke_as_owner = auth_catalog
                .role_by_oid(auth.current_user_oid())
                .is_some_and(|row| row.rolsuper)
                || auth.has_effective_membership(relation.owner_oid, &auth_catalog);
            let owner_name = auth_catalog
                .role_by_oid(relation.owner_oid)
                .map(|row| row.rolname.clone())
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("owner for table \"{object_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })?;
            let current_user_name = auth_catalog
                .role_by_oid(auth.current_user_oid())
                .map(|row| row.rolname.clone())
                .ok_or_else(|| ExecError::DetailedError {
                    message: "current user does not exist".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })?;
            if let Some(column_specs) = table_column_privilege_specs(&stmt.privilege, &stmt.columns)
            {
                let mut column_acls = BTreeMap::<i16, Vec<String>>::new();
                for (column_privilege, columns) in column_specs {
                    let privilege_chars = table_privilege_chars(&column_privilege)
                        .ok_or_else(|| ExecError::Parse(ParseError::UnexpectedEof))?;
                    for column_name in columns {
                        let Some((column_index, column)) = relation
                            .desc
                            .columns
                            .iter()
                            .enumerate()
                            .find(|(_, column)| {
                                !column.dropped && column.name.eq_ignore_ascii_case(&column_name)
                            })
                        else {
                            return Err(ExecError::Parse(ParseError::UnknownColumn(
                                column_name.clone(),
                            )));
                        };
                        let attnum = column_index.saturating_add(1) as i16;
                        let acl = column_acls
                            .entry(attnum)
                            .or_insert_with(|| column.attacl.clone().unwrap_or_default());
                        for grantee_name in &stmt.grantee_names {
                            let grantee_acl_name = if grantee_name.eq_ignore_ascii_case("public") {
                                String::new()
                            } else {
                                auth_catalog
                                    .role_by_name(grantee_name)
                                    .map(|row| row.rolname.clone())
                                    .ok_or_else(|| {
                                        ExecError::Parse(role_management_error(format!(
                                            "role \"{}\" does not exist",
                                            grantee_name
                                        )))
                                    })?
                            };
                            let grantor_name = if current_user_can_revoke_as_owner {
                                owner_name.as_str()
                            } else {
                                current_user_name.as_str()
                            };
                            let revoke_privilege_chars = if current_user_can_revoke_as_owner {
                                privilege_chars.to_string()
                            } else {
                                let effective_names =
                                    effective_acl_grantee_names(&auth, &auth_catalog);
                                let effective_privilege_chars = grantable_acl_privilege_chars(
                                    acl,
                                    &effective_names,
                                    privilege_chars,
                                );
                                warn_revoke_privileges(
                                    &format!("{column_name} of relation {object_name}"),
                                    privilege_chars,
                                    &effective_privilege_chars,
                                );
                                if effective_privilege_chars.is_empty() {
                                    continue;
                                }
                                effective_privilege_chars
                            };
                            revoke_table_acl_entry_by_grantor(
                                acl,
                                &grantee_acl_name,
                                grantor_name,
                                &revoke_privilege_chars,
                                TABLE_ALL_PRIVILEGE_CHARS,
                                stmt.grant_option_for,
                                stmt.cascade,
                            )?;
                        }
                    }
                }
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: current_cid,
                    client_id,
                    waiter: None,
                    interrupts: self.interrupt_state(client_id),
                };
                let effect = self
                    .catalog
                    .write()
                    .alter_attribute_acls_mvcc(
                        relation.relation_oid,
                        column_acls
                            .into_iter()
                            .map(|(attnum, acl)| (attnum, (!acl.is_empty()).then_some(acl)))
                            .collect(),
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                catalog_effects.push(effect);
                current_cid = current_cid.saturating_add(1);
                continue;
            }

            let catcache = self
                .backend_catcache(client_id, Some((xid, current_cid)))
                .map_err(map_catalog_error)?;
            let privilege_chars = relation_privilege_chars(&stmt.privilege, relation.relkind)
                .ok_or_else(|| ExecError::Parse(ParseError::UnexpectedEof))?;
            let allowed_privilege_chars = allowed_relation_privilege_chars(relation.relkind);
            let mut acl = catcache
                .class_by_oid(relation.relation_oid)
                .and_then(|row| row.relacl.clone())
                .unwrap_or_else(|| {
                    table_owner_default_acl(&owner_name, relation.relkind)
                        .into_iter()
                        .collect()
                });
            let effective_privilege_chars;
            let revoke_privilege_chars = if current_user_can_revoke_as_owner {
                privilege_chars
            } else {
                let effective_names = effective_acl_grantee_names(&auth, &auth_catalog);
                effective_privilege_chars =
                    grantable_acl_privilege_chars(&acl, &effective_names, privilege_chars);
                warn_revoke_privileges(object_name, privilege_chars, &effective_privilege_chars);
                if effective_privilege_chars.is_empty() {
                    continue;
                }
                effective_privilege_chars.as_str()
            };
            let grantor_name = if current_user_can_revoke_as_owner {
                owner_name.as_str()
            } else {
                current_user_name.as_str()
            };
            for grantee_name in &stmt.grantee_names {
                let grantee_acl_name = if grantee_name.eq_ignore_ascii_case("public") {
                    String::new()
                } else {
                    auth_catalog
                        .role_by_name(grantee_name)
                        .map(|row| row.rolname.clone())
                        .ok_or_else(|| {
                            ExecError::Parse(role_management_error(format!(
                                "role \"{}\" does not exist",
                                grantee_name
                            )))
                        })?
                };
                revoke_table_acl_entry_by_grantor(
                    &mut acl,
                    &grantee_acl_name,
                    grantor_name,
                    revoke_privilege_chars,
                    allowed_privilege_chars,
                    stmt.grant_option_for,
                    stmt.cascade,
                )?;
            }
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: current_cid,
                client_id,
                waiter: None,
                interrupts: self.interrupt_state(client_id),
            };
            let effect = self
                .catalog
                .write()
                .alter_relation_acl_mvcc(
                    relation.relation_oid,
                    collapse_relation_acl_defaults(acl, &owner_name, relation.relkind),
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
            current_cid = current_cid.saturating_add(1);
        }
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_language_acl_stmt(
        &self,
        client_id: ClientId,
        object_names: &[String],
        grantee_names: &[String],
        with_grant_option: bool,
        revoke: bool,
        grant_option_for: bool,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, None, None);
        let auth_catalog = self
            .auth_catalog(client_id, None)
            .map_err(map_catalog_error)?;
        let auth = self.auth_state(client_id);
        let is_superuser = auth_catalog
            .role_by_oid(auth.current_user_oid())
            .is_some_and(|row| row.rolsuper);
        let grantor_name = auth_catalog
            .role_by_oid(auth.current_user_oid())
            .map(|row| row.rolname.clone())
            .unwrap_or_else(|| "postgres".into());
        for grantee_name in grantee_names {
            if !grantee_name.eq_ignore_ascii_case("public")
                && auth_catalog.role_by_name(grantee_name).is_none()
            {
                return Err(ExecError::Parse(role_management_error(format!(
                    "role \"{}\" does not exist",
                    grantee_name
                ))));
            }
        }
        for object_name in object_names {
            let language = catalog.language_row_by_name(object_name).ok_or_else(|| {
                ExecError::DetailedError {
                    message: format!("language \"{object_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                }
            })?;
            if !language.lanpltrusted {
                return Err(ExecError::DetailedError {
                    message: format!("language \"{object_name}\" is not trusted"),
                    detail: Some(
                        "GRANT and REVOKE are not allowed on untrusted languages, because only superusers can use untrusted languages."
                            .into(),
                    ),
                    hint: None,
                    sqlstate: "42501",
                });
            }
            let owner_name = auth_catalog
                .role_by_oid(language.lanowner)
                .map(|row| row.rolname.clone())
                .unwrap_or_else(|| "postgres".into());
            if !is_superuser && !auth.has_effective_membership(language.lanowner, &auth_catalog) {
                let action = if revoke { "revoked" } else { "granted" };
                push_warning(format!(
                    "no privileges were {action} for \"{}\"",
                    language.lanname
                ));
                continue;
            }
            let defaults = language_owner_default_acl(&owner_name, language.lanpltrusted);
            let mut acl =
                bootstrap_language_acl_override(language.oid).unwrap_or_else(|| defaults.clone());
            for grantee_name in grantee_names {
                let grantee_acl_name = if grantee_name.eq_ignore_ascii_case("public") {
                    String::new()
                } else {
                    auth_catalog
                        .role_by_name(grantee_name)
                        .map(|row| row.rolname.clone())
                        .ok_or_else(|| {
                            ExecError::Parse(role_management_error(format!(
                                "role \"{}\" does not exist",
                                grantee_name
                            )))
                        })?
                };
                if grant_option_for {
                    revoke_acl_grant_options_only(
                        &mut acl,
                        &grantee_acl_name,
                        TYPE_USAGE_PRIVILEGE_CHARS,
                        TYPE_USAGE_PRIVILEGE_CHARS,
                    );
                } else if revoke {
                    revoke_table_acl_entry(
                        &mut acl,
                        &grantee_acl_name,
                        TYPE_USAGE_PRIVILEGE_CHARS,
                        TYPE_USAGE_PRIVILEGE_CHARS,
                    );
                } else {
                    grant_table_acl_entry(
                        &mut acl,
                        &grantee_acl_name,
                        &grantor_name,
                        TYPE_USAGE_PRIVILEGE_CHARS,
                        TYPE_USAGE_PRIVILEGE_CHARS,
                        with_grant_option,
                    );
                }
            }
            let collapsed = collapse_acl_defaults(acl, &defaults);
            set_bootstrap_language_acl_override(language.oid, collapsed);
        }
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_grant_schema_acl_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &GrantObjectStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_schema_acl_stmt_in_transaction_with_search_path(
            client_id,
            stmt.privilege.clone(),
            stmt.named_object_names(),
            &stmt.grantee_names,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
            false,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    fn execute_revoke_schema_acl_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &RevokeObjectStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_schema_acl_stmt_in_transaction_with_search_path(
            client_id,
            stmt.privilege.clone(),
            stmt.named_object_names(),
            &stmt.grantee_names,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
            true,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    fn execute_schema_acl_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        privilege: GrantObjectPrivilege,
        object_names: &[String],
        grantee_names: &[String],
        xid: TransactionId,
        cid: CommandId,
        _configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        revoke: bool,
    ) -> Result<StatementResult, ExecError> {
        let privilege_chars = object_privilege_chars(privilege.clone())
            .ok_or_else(|| ExecError::Parse(ParseError::UnexpectedEof))?;
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let grantor_name = auth_catalog
            .role_by_oid(auth.current_user_oid())
            .map(|row| row.rolname.clone())
            .ok_or_else(|| ExecError::DetailedError {
                message: "current user does not exist".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        for object_name in object_names {
            let namespace = catcache
                .namespace_by_name(object_name)
                .cloned()
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("schema \"{}\" does not exist", object_name),
                    detail: None,
                    hint: None,
                    sqlstate: "3F000",
                })?;
            if !auth_catalog
                .role_by_oid(auth.current_user_oid())
                .is_some_and(|row| row.rolsuper)
                && !auth.has_effective_membership(namespace.nspowner, &auth_catalog)
            {
                return Err(ExecError::DetailedError {
                    message: format!("must be owner of schema {object_name}"),
                    detail: None,
                    hint: None,
                    sqlstate: "42501",
                });
            }
            let owner_name = auth_catalog
                .role_by_oid(namespace.nspowner)
                .map(|row| row.rolname.clone())
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("owner for schema \"{object_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })?;
            let mut acl = namespace
                .nspacl
                .clone()
                .unwrap_or_else(|| schema_owner_default_acl(&owner_name));
            for grantee_name in grantee_names {
                let grantee_acl_name = if grantee_name.eq_ignore_ascii_case("public") {
                    String::new()
                } else {
                    auth_catalog
                        .role_by_name(grantee_name)
                        .map(|row| row.rolname.clone())
                        .ok_or_else(|| {
                            ExecError::Parse(role_management_error(format!(
                                "role \"{}\" does not exist",
                                grantee_name
                            )))
                        })?
                };
                if revoke {
                    revoke_acl_entry(
                        &mut acl,
                        &grantee_acl_name,
                        privilege_chars,
                        SCHEMA_ALL_PRIVILEGE_CHARS,
                    );
                } else {
                    grant_acl_entry(
                        &mut acl,
                        &grantee_acl_name,
                        &grantor_name,
                        privilege_chars,
                        SCHEMA_ALL_PRIVILEGE_CHARS,
                    );
                }
            }
            let new_acl = collapse_acl_defaults(acl, &schema_owner_default_acl(&owner_name));
            let effect = self
                .catalog
                .write()
                .alter_namespace_acl_mvcc(namespace.oid, new_acl, &ctx)
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
        }
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_grant_type_acl_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &GrantObjectStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_type_acl_stmt_with_search_path(
            client_id,
            stmt.privilege.clone(),
            stmt.named_object_names(),
            &stmt.grantee_names,
            configured_search_path,
            false,
            stmt.with_grant_option,
        )
    }

    fn execute_revoke_type_acl_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &RevokeObjectStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_type_acl_stmt_with_search_path(
            client_id,
            stmt.privilege.clone(),
            stmt.named_object_names(),
            &stmt.grantee_names,
            configured_search_path,
            true,
            false,
        )
    }

    fn execute_type_acl_stmt_with_search_path(
        &self,
        client_id: ClientId,
        privilege: GrantObjectPrivilege,
        object_names: &[String],
        grantee_names: &[String],
        configured_search_path: Option<&[String]>,
        revoke: bool,
        with_grant_option: bool,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_type_acl_stmt_in_transaction_with_search_path(
            client_id,
            privilege,
            object_names,
            grantee_names,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
            revoke,
            with_grant_option,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    fn execute_type_acl_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        privilege: GrantObjectPrivilege,
        object_names: &[String],
        grantee_names: &[String],
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        revoke: bool,
        with_grant_option: bool,
    ) -> Result<StatementResult, ExecError> {
        let privilege_chars = object_privilege_chars(privilege.clone())
            .ok_or_else(|| ExecError::Parse(ParseError::UnexpectedEof))?;
        let object_kind = match privilege {
            GrantObjectPrivilege::UsageOnType(kind) => kind,
            _ => return Err(ExecError::Parse(ParseError::UnexpectedEof)),
        };
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let grantor_name = auth_catalog
            .role_by_oid(auth.current_user_oid())
            .map(|row| row.rolname.clone())
            .ok_or_else(|| ExecError::DetailedError {
                message: "current user does not exist".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        for object_name in object_names {
            let raw_type = parse_type_name(object_name).map_err(ExecError::Parse)?;
            let sql_type = resolve_raw_type_name(&raw_type, &catalog).map_err(ExecError::Parse)?;
            let type_oid = catalog.type_oid_for_sql_type(sql_type).ok_or_else(|| {
                ExecError::DetailedError {
                    message: format!("type \"{}\" does not exist", object_name),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                }
            })?;
            let row = catalog
                .type_by_oid(type_oid)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("type \"{}\" does not exist", object_name),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                })?;
            if type_row_is_true_array(&row) {
                return Err(cannot_set_array_privileges_error());
            }
            if let Some(entry) = self
                .range_types
                .read()
                .values()
                .find(|entry| entry.multirange_oid == type_oid)
            {
                return Err(cannot_set_multirange_privileges_error(&entry.name));
            }
            if object_kind == TypePrivilegeObjectKind::Domain && !type_row_is_domain(&row) {
                return Err(ExecError::DetailedError {
                    message: format!("\"{object_name}\" is not a domain"),
                    detail: None,
                    hint: None,
                    sqlstate: "42809",
                });
            }
            let current_user_can_grant_as_owner = auth_catalog
                .role_by_oid(auth.current_user_oid())
                .is_some_and(|entry| entry.rolsuper)
                || auth.has_effective_membership(row.typowner, &auth_catalog);
            let owner_name = auth_catalog
                .role_by_oid(row.typowner)
                .map(|entry| entry.rolname.clone())
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("owner for type \"{object_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })?;
            let mut acl = row
                .typacl
                .clone()
                .unwrap_or_else(|| type_owner_default_acl(&owner_name));
            let effective_names = (!current_user_can_grant_as_owner)
                .then(|| effective_acl_grantee_names(&auth, &auth_catalog));
            let grant_privilege_chars = if current_user_can_grant_as_owner || revoke {
                privilege_chars.to_string()
            } else {
                let effective_privilege_chars = grantable_acl_privilege_chars(
                    &acl,
                    effective_names.as_ref().expect("effective names"),
                    privilege_chars,
                );
                warn_grant_privileges(object_name, privilege_chars, &effective_privilege_chars);
                if effective_privilege_chars.is_empty() {
                    continue;
                }
                effective_privilege_chars
            };
            let revoke_privilege_chars = if !revoke || current_user_can_grant_as_owner {
                privilege_chars.to_string()
            } else {
                let effective_privilege_chars = grantable_acl_privilege_chars(
                    &acl,
                    effective_names.as_ref().expect("effective names"),
                    privilege_chars,
                );
                if effective_privilege_chars.is_empty() {
                    if !acl_grants_privilege(
                        &acl,
                        effective_names.as_ref().expect("effective names"),
                        'U',
                    ) {
                        return Err(ExecError::DetailedError {
                            message: format!("permission denied for type {object_name}"),
                            detail: None,
                            hint: None,
                            sqlstate: "42501",
                        });
                    }
                    warn_revoke_privileges(object_name, privilege_chars, "");
                    continue;
                }
                warn_revoke_privileges(object_name, privilege_chars, &effective_privilege_chars);
                effective_privilege_chars
            };
            if revoke
                && !current_user_can_grant_as_owner
                && !acl_grants_privilege(
                    &acl,
                    effective_names.as_ref().expect("effective names"),
                    'U',
                )
            {
                return Err(ExecError::DetailedError {
                    message: format!("permission denied for type {object_name}"),
                    detail: None,
                    hint: None,
                    sqlstate: "42501",
                });
            }
            for grantee_name in grantee_names {
                let grantee_acl_name = if grantee_name.eq_ignore_ascii_case("public") {
                    String::new()
                } else {
                    auth_catalog
                        .role_by_name(grantee_name)
                        .map(|entry| entry.rolname.clone())
                        .ok_or_else(|| {
                            ExecError::Parse(role_management_error(format!(
                                "role \"{}\" does not exist",
                                grantee_name
                            )))
                        })?
                };
                if revoke {
                    if current_user_can_grant_as_owner {
                        revoke_acl_entry(
                            &mut acl,
                            &grantee_acl_name,
                            &revoke_privilege_chars,
                            TYPE_USAGE_PRIVILEGE_CHARS,
                        );
                    } else {
                        revoke_acl_entry_by_grantor(
                            &mut acl,
                            &grantee_acl_name,
                            &grantor_name,
                            &revoke_privilege_chars,
                            TYPE_USAGE_PRIVILEGE_CHARS,
                        );
                    }
                } else {
                    grant_table_acl_entry(
                        &mut acl,
                        &grantee_acl_name,
                        &grantor_name,
                        &grant_privilege_chars,
                        TYPE_USAGE_PRIVILEGE_CHARS,
                        with_grant_option,
                    );
                }
            }
            let new_acl = collapse_acl_defaults(acl, &type_owner_default_acl(&owner_name));
            let mut updated_dynamic = false;
            if let Some(domain) = self
                .domains
                .write()
                .values_mut()
                .find(|entry| entry.oid == type_oid)
            {
                domain.typacl = new_acl.clone();
                updated_dynamic = true;
            }
            if !updated_dynamic {
                let mut enum_types = self.enum_types.write();
                if let Some(entry) = enum_types
                    .values_mut()
                    .find(|entry| entry.oid == type_oid || entry.array_oid == type_oid)
                {
                    entry.typacl = new_acl.clone();
                    updated_dynamic = true;
                }
            }
            if !updated_dynamic {
                let mut range_types = self.range_types.write();
                if let Some(entry) = range_types.values_mut().find(|entry| {
                    entry.oid == type_oid
                        || entry.array_oid == type_oid
                        || entry.multirange_oid == type_oid
                        || entry.multirange_array_oid == type_oid
                }) {
                    entry.typacl = new_acl.clone();
                    updated_dynamic = true;
                }
            }
            if updated_dynamic {
                self.plan_cache.invalidate_all();
            } else {
                let effect = self
                    .catalog
                    .write()
                    .alter_type_acl_mvcc(type_oid, new_acl, &ctx)
                    .map_err(map_catalog_error)?;
                catalog_effects.push(effect);
            }
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn ensure_sql_type_usage_privilege(
        &self,
        client_id: ClientId,
        txn_ctx: Option<(TransactionId, CommandId)>,
        configured_search_path: Option<&[String]>,
        sql_type: SqlType,
    ) -> Result<(), ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
        let type_oid =
            catalog
                .type_oid_for_sql_type(sql_type)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("type {:?} does not exist", sql_type.kind),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                })?;
        self.ensure_type_usage_privilege_by_oid(
            client_id,
            txn_ctx,
            configured_search_path,
            type_oid,
        )
    }

    pub(crate) fn ensure_type_usage_privilege_by_oid(
        &self,
        client_id: ClientId,
        txn_ctx: Option<(TransactionId, CommandId)>,
        configured_search_path: Option<&[String]>,
        type_oid: u32,
    ) -> Result<(), ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, txn_ctx)
            .map_err(map_catalog_error)?;
        let catalog = self.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
        let (row, display_name) = type_usage_acl_target(&catalog, type_oid)?;
        // Preserve existing range/multirange behavior: owners can revoke their own USAGE.
        let owner_has_implicit_usage = row.typtype != 'r';
        if auth_catalog
            .role_by_oid(auth.current_user_oid())
            .is_some_and(|entry| entry.rolsuper)
            || (owner_has_implicit_usage
                && auth.has_effective_membership(row.typowner, &auth_catalog))
        {
            return Ok(());
        }
        let owner_name = auth_catalog
            .role_by_oid(row.typowner)
            .map(|entry| entry.rolname.clone())
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("owner for type \"{display_name}\" does not exist"),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
        let acl = row
            .typacl
            .clone()
            .unwrap_or_else(|| type_owner_default_acl(&owner_name));
        let effective_names = effective_acl_grantee_names(&auth, &auth_catalog);
        if acl_grants_privilege(&acl, &effective_names, 'U') {
            return Ok(());
        }
        Err(ExecError::DetailedError {
            message: format!("permission denied for type {display_name}"),
            detail: None,
            hint: None,
            sqlstate: "42501",
        })
    }

    pub(crate) fn ensure_relation_desc_type_usage_privileges(
        &self,
        client_id: ClientId,
        txn_ctx: Option<(TransactionId, CommandId)>,
        configured_search_path: Option<&[String]>,
        desc: &RelationDesc,
    ) -> Result<(), ExecError> {
        for column in desc.columns.iter().filter(|column| !column.dropped) {
            self.ensure_sql_type_usage_privilege(
                client_id,
                txn_ctx,
                configured_search_path,
                column.sql_type,
            )?;
        }
        Ok(())
    }

    fn execute_grant_function_acl_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &GrantObjectStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_function_acl_stmt_with_search_path(
            client_id,
            stmt.privilege.clone(),
            stmt.named_object_names(),
            &stmt.grantee_names,
            configured_search_path,
            false,
        )
    }

    fn execute_revoke_function_acl_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &RevokeObjectStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_function_acl_stmt_with_search_path(
            client_id,
            stmt.privilege.clone(),
            stmt.named_object_names(),
            &stmt.grantee_names,
            configured_search_path,
            true,
        )
    }

    fn execute_function_acl_stmt_with_search_path(
        &self,
        client_id: ClientId,
        privilege: GrantObjectPrivilege,
        object_names: &[String],
        grantee_names: &[String],
        configured_search_path: Option<&[String]>,
        revoke: bool,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_function_acl_stmt_in_transaction_with_search_path(
            client_id,
            privilege,
            object_names,
            grantee_names,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
            revoke,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    fn execute_function_acl_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        privilege: GrantObjectPrivilege,
        object_names: &[String],
        grantee_names: &[String],
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        revoke: bool,
    ) -> Result<StatementResult, ExecError> {
        let privilege_chars = object_privilege_chars(privilege.clone())
            .ok_or_else(|| ExecError::Parse(ParseError::UnexpectedEof))?;
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let grantor_name = auth_catalog
            .role_by_oid(auth.current_user_oid())
            .map(|row| row.rolname.clone())
            .ok_or_else(|| ExecError::DetailedError {
                message: "current user does not exist".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let routine_kind = match privilege {
            GrantObjectPrivilege::ExecuteOnProcedure => RoutineKind::Procedure,
            GrantObjectPrivilege::ExecuteOnRoutine => RoutineKind::Routine,
            _ => RoutineKind::Function,
        };
        for object_name in object_names {
            let row = lookup_function_row_by_signature(
                self,
                client_id,
                Some((xid, cid)),
                configured_search_path,
                object_name,
                routine_kind,
            )?;
            if !auth_catalog
                .role_by_oid(auth.current_user_oid())
                .is_some_and(|entry| entry.rolsuper)
                && !auth.has_effective_membership(row.proowner, &auth_catalog)
            {
                return Err(ExecError::DetailedError {
                    message: format!("must be owner of function {object_name}"),
                    detail: None,
                    hint: None,
                    sqlstate: "42501",
                });
            }
            let owner_name = auth_catalog
                .role_by_oid(row.proowner)
                .map(|entry| entry.rolname.clone())
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("owner for function \"{object_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })?;
            let mut acl = row
                .proacl
                .clone()
                .unwrap_or_else(|| function_owner_default_acl(&owner_name));
            for grantee_name in grantee_names {
                let grantee_acl_name = if grantee_name.eq_ignore_ascii_case("public") {
                    String::new()
                } else {
                    auth_catalog
                        .role_by_name(grantee_name)
                        .map(|entry| entry.rolname.clone())
                        .ok_or_else(|| {
                            ExecError::Parse(role_management_error(format!(
                                "role \"{}\" does not exist",
                                grantee_name
                            )))
                        })?
                };
                if revoke {
                    revoke_acl_entry(
                        &mut acl,
                        &grantee_acl_name,
                        privilege_chars,
                        FUNCTION_EXECUTE_PRIVILEGE_CHARS,
                    );
                } else {
                    grant_acl_entry(
                        &mut acl,
                        &grantee_acl_name,
                        &grantor_name,
                        privilege_chars,
                        FUNCTION_EXECUTE_PRIVILEGE_CHARS,
                    );
                }
            }
            let new_acl = collapse_acl_defaults(acl, &function_owner_default_acl(&owner_name));
            if is_bootstrap_proc_oid(row.oid) {
                // :HACK: bootstrap pg_proc rows are not physically replaceable
                // yet; keep full EXECUTE ACL replacements in a process-local overlay.
                set_bootstrap_proc_acl_override(row.oid, new_acl);
                continue;
            }
            let effect = self
                .catalog
                .write()
                .alter_proc_acl_mvcc(row.oid, new_acl, &ctx)
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
        }
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_grant_foreign_usage_acl_stmt(
        &self,
        client_id: ClientId,
        stmt: &GrantObjectStatement,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_foreign_usage_acl_stmt_in_transaction(
            client_id,
            stmt.privilege.clone(),
            stmt.named_object_names(),
            &stmt.grantee_names,
            stmt.with_grant_option,
            xid,
            0,
            &mut catalog_effects,
            false,
            false,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    fn execute_revoke_foreign_usage_acl_stmt(
        &self,
        client_id: ClientId,
        stmt: &RevokeObjectStatement,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_foreign_usage_acl_stmt_in_transaction(
            client_id,
            stmt.privilege.clone(),
            stmt.named_object_names(),
            &stmt.grantee_names,
            false,
            xid,
            0,
            &mut catalog_effects,
            true,
            stmt.cascade,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_foreign_usage_acl_stmt_in_transaction(
        &self,
        client_id: ClientId,
        privilege: GrantObjectPrivilege,
        object_names: &[String],
        grantee_names: &[String],
        with_grant_option: bool,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        revoke: bool,
        cascade: bool,
    ) -> Result<StatementResult, ExecError> {
        let kind = foreign_usage_object_kind(privilege)
            .ok_or_else(|| ExecError::Parse(ParseError::UnexpectedEof))?;
        if object_names.is_empty() {
            return Err(ExecError::Parse(ParseError::UnexpectedEof));
        }
        let auth = self.auth_state(client_id);
        let mut current_cid = cid;
        for object_name in object_names {
            let normalized_object_name = object_name.trim_matches('"').to_ascii_lowercase();
            let auth_catalog = self
                .auth_catalog(client_id, Some((xid, current_cid)))
                .map_err(map_catalog_error)?;
            let current_user_is_superuser = auth_catalog
                .role_by_oid(auth.current_user_oid())
                .is_some_and(|entry| entry.rolsuper);
            let grantor_name = auth_catalog
                .role_by_oid(auth.current_user_oid())
                .map(|entry| entry.rolname.clone())
                .ok_or_else(|| ExecError::DetailedError {
                    message: "current user does not exist".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })?;
            let grantee_acl_names = grantee_names
                .iter()
                .map(|grantee_name| {
                    if grantee_name.eq_ignore_ascii_case("public") {
                        Ok(String::new())
                    } else {
                        auth_catalog
                            .role_by_name(grantee_name)
                            .map(|entry| entry.rolname.clone())
                            .ok_or_else(|| {
                                ExecError::Parse(role_management_error(format!(
                                    "role \"{}\" does not exist",
                                    grantee_name
                                )))
                            })
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            let catcache = self
                .backend_catcache(client_id, Some((xid, current_cid)))
                .map_err(map_catalog_error)?;
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: current_cid,
                client_id,
                waiter: None,
                interrupts: self.interrupt_state(client_id),
            };
            match kind {
                ForeignUsageObjectKind::ForeignDataWrapper => {
                    let existing = catcache
                        .foreign_data_wrapper_rows()
                        .into_iter()
                        .find(|row| row.fdwname.eq_ignore_ascii_case(&normalized_object_name))
                        .ok_or_else(|| ExecError::DetailedError {
                            message: format!(
                                "foreign-data wrapper \"{}\" does not exist",
                                object_name
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42704",
                        })?;
                    let (_, effect) = self.replace_foreign_data_wrapper_acl(
                        existing,
                        object_name,
                        &auth_catalog,
                        current_user_is_superuser,
                        &auth,
                        &grantor_name,
                        &grantee_acl_names,
                        with_grant_option,
                        revoke,
                        cascade,
                        &ctx,
                    )?;
                    catalog_effects.push(effect);
                }
                ForeignUsageObjectKind::ForeignServer => {
                    let existing = catcache
                        .foreign_server_rows()
                        .into_iter()
                        .find(|row| row.srvname.eq_ignore_ascii_case(&normalized_object_name))
                        .ok_or_else(|| ExecError::DetailedError {
                            message: format!("server \"{}\" does not exist", object_name),
                            detail: None,
                            hint: None,
                            sqlstate: "42704",
                        })?;
                    let (_, effect) = self.replace_foreign_server_acl(
                        existing,
                        object_name,
                        &auth_catalog,
                        current_user_is_superuser,
                        &auth,
                        &grantor_name,
                        &grantee_acl_names,
                        with_grant_option,
                        revoke,
                        cascade,
                        &ctx,
                    )?;
                    catalog_effects.push(effect);
                }
            }
            current_cid = current_cid.saturating_add(1);
        }
        Ok(StatementResult::AffectedRows(0))
    }

    #[allow(clippy::too_many_arguments)]
    fn replace_foreign_data_wrapper_acl(
        &self,
        existing: PgForeignDataWrapperRow,
        object_name: &str,
        auth_catalog: &crate::pgrust::auth::AuthCatalog,
        current_user_is_superuser: bool,
        auth: &crate::pgrust::auth::AuthState,
        grantor_name: &str,
        grantee_acl_names: &[String],
        with_grant_option: bool,
        revoke: bool,
        cascade: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), ExecError> {
        let owner_name = auth_catalog
            .role_by_oid(existing.fdwowner)
            .map(|entry| entry.rolname.clone())
            .ok_or_else(|| ExecError::DetailedError {
                message: format!(
                    "owner for {} \"{}\" does not exist",
                    ForeignUsageObjectKind::ForeignDataWrapper.usage_privilege_object_type(),
                    object_name
                ),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
        let defaults = foreign_usage_owner_default_acl(&owner_name);
        let current_user_is_owner = current_user_is_superuser
            || auth.has_effective_membership(existing.fdwowner, auth_catalog);
        let effective_names = effective_acl_grantee_names(auth, auth_catalog);
        let existing_acl = existing.fdwacl.clone().unwrap_or_default();
        let has_usage = acl_grants_privilege(&existing_acl, &effective_names, 'U');
        let has_grant_option = acl_grants_all_options(
            &existing_acl,
            &effective_names,
            FOREIGN_USAGE_PRIVILEGE_CHARS,
        );
        if !current_user_is_owner && !has_grant_option {
            if !revoke && has_usage {
                push_warning(format!(r#"no privileges were granted for "{object_name}""#));
                return self
                    .catalog
                    .write()
                    .replace_foreign_data_wrapper_mvcc(&existing, existing.clone(), ctx)
                    .map_err(map_catalog_error);
            }
            return Err(ExecError::DetailedError {
                message: format!("permission denied for foreign-data wrapper {object_name}"),
                detail: None,
                hint: None,
                sqlstate: "42501",
            });
        }
        let mut acl = if revoke {
            existing.fdwacl.clone().unwrap_or_default()
        } else {
            existing.fdwacl.clone().unwrap_or_else(|| defaults.clone())
        };
        for grantee_acl_name in grantee_acl_names {
            if revoke {
                cascade_revoke_usage_acl_entry(&mut acl, grantee_acl_name, cascade)?;
            } else {
                grant_usage_acl_entry(&mut acl, grantee_acl_name, grantor_name, with_grant_option);
            }
        }
        let mut replacement = existing.clone();
        replacement.fdwacl = collapse_acl_defaults(acl, &defaults);
        self.catalog
            .write()
            .replace_foreign_data_wrapper_mvcc(&existing, replacement, ctx)
            .map_err(map_catalog_error)
    }

    #[allow(clippy::too_many_arguments)]
    fn replace_foreign_server_acl(
        &self,
        existing: PgForeignServerRow,
        object_name: &str,
        auth_catalog: &crate::pgrust::auth::AuthCatalog,
        current_user_is_superuser: bool,
        auth: &crate::pgrust::auth::AuthState,
        grantor_name: &str,
        grantee_acl_names: &[String],
        with_grant_option: bool,
        revoke: bool,
        cascade: bool,
        ctx: &CatalogWriteContext,
    ) -> Result<(u32, CatalogMutationEffect), ExecError> {
        let owner_name = auth_catalog
            .role_by_oid(existing.srvowner)
            .map(|entry| entry.rolname.clone())
            .ok_or_else(|| ExecError::DetailedError {
                message: format!(
                    "owner for {} \"{}\" does not exist",
                    ForeignUsageObjectKind::ForeignServer.usage_privilege_object_type(),
                    object_name
                ),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
        let defaults = foreign_usage_owner_default_acl(&owner_name);
        let current_user_is_owner = current_user_is_superuser
            || auth.has_effective_membership(existing.srvowner, auth_catalog);
        let effective_names = effective_acl_grantee_names(auth, auth_catalog);
        let existing_acl = existing.srvacl.clone().unwrap_or_default();
        let has_usage = acl_grants_privilege(&existing_acl, &effective_names, 'U');
        let has_grant_option = acl_grants_all_options(
            &existing_acl,
            &effective_names,
            FOREIGN_USAGE_PRIVILEGE_CHARS,
        );
        if !current_user_is_owner && !has_grant_option {
            if !revoke && has_usage {
                push_warning(format!(r#"no privileges were granted for "{object_name}""#));
                return self
                    .catalog
                    .write()
                    .replace_foreign_server_mvcc(&existing, existing.clone(), ctx)
                    .map_err(map_catalog_error);
            }
            return Err(ExecError::DetailedError {
                message: format!("permission denied for foreign server {object_name}"),
                detail: None,
                hint: None,
                sqlstate: "42501",
            });
        }
        let mut acl = if revoke {
            existing.srvacl.clone().unwrap_or_default()
        } else {
            existing.srvacl.clone().unwrap_or_else(|| defaults.clone())
        };
        for grantee_acl_name in grantee_acl_names {
            if revoke {
                cascade_revoke_usage_acl_entry(&mut acl, grantee_acl_name, cascade)?;
            } else {
                grant_usage_acl_entry(&mut acl, grantee_acl_name, grantor_name, with_grant_option);
            }
        }
        let mut replacement = existing.clone();
        replacement.srvacl = collapse_acl_defaults(acl, &defaults);
        self.catalog
            .write()
            .replace_foreign_server_mvcc(&existing, replacement, ctx)
            .map_err(map_catalog_error)
    }

    pub(crate) fn execute_grant_role_membership_stmt(
        &self,
        client_id: ClientId,
        stmt: &GrantRoleMembershipStatement,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_grant_role_membership_stmt_in_transaction(
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

    pub(crate) fn execute_grant_role_membership_stmt_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &GrantRoleMembershipStatement,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let interrupts = self.interrupt_state(client_id);
        let mut current_cid = cid;

        for role_name in &stmt.role_names {
            for grantee_name in &stmt.grantee_names {
                let auth_catalog = self
                    .auth_catalog(client_id, Some((xid, current_cid)))
                    .map_err(map_role_grant_error)?;
                let role = lookup_membership_role(&auth_catalog, role_name)?;
                let grantor_oid = resolve_role_grantor(
                    &auth,
                    &auth_catalog,
                    &role,
                    stmt.granted_by.as_ref(),
                    true,
                    stmt.legacy_group_syntax,
                )?;
                let grantee = lookup_membership_grantee(&auth_catalog, grantee_name)?;
                if stmt.admin_option {
                    reject_circular_admin_grant(&auth_catalog, role.oid, grantor_oid, grantee.oid)?;
                }
                let grant_options = GrantRoleMembershipOptions::from(stmt);
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: current_cid,
                    client_id,
                    waiter: None,
                    interrupts: interrupts.clone(),
                };
                upsert_role_membership_in_transaction(
                    self,
                    &auth_catalog,
                    role.oid,
                    grantee.oid,
                    grantor_oid,
                    grantee.rolinherit,
                    grant_options,
                    &ctx,
                    catalog_effects,
                )?;
                current_cid = current_cid.saturating_add(1);
            }
        }

        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_revoke_role_membership_stmt(
        &self,
        client_id: ClientId,
        stmt: &RevokeRoleMembershipStatement,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_revoke_role_membership_stmt_in_transaction(
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

    pub(crate) fn execute_revoke_role_membership_stmt_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &RevokeRoleMembershipStatement,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let auth = self.auth_state(client_id);
        let interrupts = self.interrupt_state(client_id);
        let mut current_cid = cid;

        for role_name in &stmt.role_names {
            for grantee_name in &stmt.grantee_names {
                let auth_catalog = self
                    .auth_catalog(client_id, Some((xid, current_cid)))
                    .map_err(map_role_grant_error)?;
                let role = lookup_membership_role(&auth_catalog, role_name)?;
                let grantor_oid = resolve_role_grantor(
                    &auth,
                    &auth_catalog,
                    &role,
                    stmt.granted_by.as_ref(),
                    false,
                    stmt.legacy_group_syntax,
                )?;
                let role_rows = auth_catalog
                    .memberships()
                    .iter()
                    .filter(|row| row.roleid == role.oid)
                    .cloned()
                    .collect::<Vec<_>>();
                let grantee = lookup_membership_grantee(&auth_catalog, grantee_name)?;
                let Some(existing_index) = role_rows
                    .iter()
                    .position(|row| row.member == grantee.oid && row.grantor == grantor_oid)
                else {
                    push_warning(format!(
                        "role \"{}\" has not been granted membership in role \"{}\" by role \"{}\"",
                        grantee.rolname,
                        role.rolname,
                        role_name_for_oid(self, &auth_catalog, grantor_oid)
                    ));
                    continue;
                };
                let planned_actions =
                    plan_role_membership_revoke(&role_rows, existing_index, stmt)?;
                for (row, action) in role_rows.iter().zip(planned_actions.iter()) {
                    let ctx = CatalogWriteContext {
                        pool: self.pool.clone(),
                        txns: self.txns.clone(),
                        xid,
                        cid: current_cid,
                        client_id,
                        waiter: None,
                        interrupts: interrupts.clone(),
                    };
                    match action {
                        PlannedRoleMembershipRevoke::Noop => {}
                        PlannedRoleMembershipRevoke::DeleteGrant => {
                            let (_, effect) = self
                                .shared_catalog
                                .write()
                                .revoke_role_membership_mvcc(
                                    row.roleid,
                                    row.member,
                                    row.grantor,
                                    &ctx,
                                )
                                .map_err(map_role_grant_error)?;
                            catalog_effects.push(effect);
                            current_cid = current_cid.saturating_add(1);
                        }
                        PlannedRoleMembershipRevoke::RemoveAdminOption => {
                            let (_, effect) = self
                                .shared_catalog
                                .write()
                                .update_role_membership_options_mvcc(
                                    row.roleid,
                                    row.member,
                                    row.grantor,
                                    false,
                                    row.inherit_option,
                                    row.set_option,
                                    &ctx,
                                )
                                .map_err(map_role_grant_error)?;
                            catalog_effects.push(effect);
                            current_cid = current_cid.saturating_add(1);
                        }
                        PlannedRoleMembershipRevoke::RemoveInheritOption => {
                            let (_, effect) = self
                                .shared_catalog
                                .write()
                                .update_role_membership_options_mvcc(
                                    row.roleid,
                                    row.member,
                                    row.grantor,
                                    row.admin_option,
                                    false,
                                    row.set_option,
                                    &ctx,
                                )
                                .map_err(map_role_grant_error)?;
                            catalog_effects.push(effect);
                            current_cid = current_cid.saturating_add(1);
                        }
                        PlannedRoleMembershipRevoke::RemoveSetOption => {
                            let (_, effect) = self
                                .shared_catalog
                                .write()
                                .update_role_membership_options_mvcc(
                                    row.roleid,
                                    row.member,
                                    row.grantor,
                                    row.admin_option,
                                    row.inherit_option,
                                    false,
                                    &ctx,
                                )
                                .map_err(map_role_grant_error)?;
                            catalog_effects.push(effect);
                            current_cid = current_cid.saturating_add(1);
                        }
                    }
                }
            }
        }

        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn role_has_database_create_privilege(
        &self,
        role_oid: u32,
        auth_catalog: &AuthCatalog,
    ) -> bool {
        if auth_catalog
            .role_by_oid(role_oid)
            .is_some_and(|row| row.rolsuper)
        {
            return true;
        }
        let mut role_auth = AuthState::default();
        role_auth.assume_authenticated_user(role_oid);
        let grants = self.database_create_grants.read();
        auth_catalog.roles().iter().any(|role| {
            role_auth.has_effective_membership(role.oid, auth_catalog)
                && grants.iter().any(|grant| grant.grantee_oid == role.oid)
        })
    }

    pub(crate) fn user_has_database_create_privilege(
        &self,
        auth: &AuthState,
        auth_catalog: &AuthCatalog,
    ) -> bool {
        self.role_has_database_create_privilege(auth.current_user_oid(), auth_catalog)
    }
}

fn execute_database_name_matches_current(name: &str) -> bool {
    name.eq_ignore_ascii_case(CURRENT_DATABASE_NAME) || name.eq_ignore_ascii_case("regression")
}

fn current_database_owner_oid(db: &Database, client_id: ClientId) -> Result<u32, ExecError> {
    db.backend_catcache(client_id, None)
        .map_err(map_catalog_error)?
        .database_rows()
        .into_iter()
        .find(|row| row.oid == CURRENT_DATABASE_OID)
        .map(|row| row.datdba)
        .ok_or_else(|| ExecError::DetailedError {
            message: "current database does not exist".into(),
            detail: None,
            hint: None,
            sqlstate: "3D000",
        })
}

fn can_grant_database_create(
    db: &Database,
    auth: &AuthState,
    auth_catalog: &AuthCatalog,
    current_database_owner_oid: u32,
) -> bool {
    if auth_catalog
        .role_by_oid(auth.current_user_oid())
        .is_some_and(|row| row.rolsuper)
        || auth.current_user_oid() == current_database_owner_oid
    {
        return true;
    }
    let grants = db.database_create_grants.read();
    auth_catalog.roles().iter().any(|role| {
        auth.has_effective_membership(role.oid, auth_catalog)
            && grants
                .iter()
                .any(|grant| grant.grantee_oid == role.oid && grant.grant_option)
    })
}

fn can_revoke_database_create(
    grants: &[DatabaseCreateGrant],
    auth: &AuthState,
    auth_catalog: &AuthCatalog,
    current_database_owner_oid: u32,
    grantee_oid: u32,
) -> bool {
    if auth_catalog
        .role_by_oid(auth.current_user_oid())
        .is_some_and(|row| row.rolsuper)
        || auth.current_user_oid() == current_database_owner_oid
    {
        return true;
    }
    grants.iter().any(|grant| {
        grant.grantee_oid == grantee_oid && grant.grantor_oid == auth.current_user_oid()
    })
}

fn snapshot_for_acl_command(
    db: &Database,
    xid: TransactionId,
    cid: CommandId,
) -> Result<Snapshot, ExecError> {
    db.txns
        .read()
        .snapshot_for_command(xid, cid)
        .map_err(|err| ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Mvcc(err)))
}

pub(crate) fn catalog_effect_for_acl(kinds: &[BootstrapCatalogKind]) -> CatalogMutationEffect {
    let mut effect = CatalogMutationEffect::default();
    for &kind in kinds {
        if !effect.touched_catalogs.contains(&kind) {
            effect.touched_catalogs.push(kind);
        }
    }
    effect
}

fn merge_default_acl_overlay(base: &mut Vec<String>, overlay: &[String], allowed: &str) {
    for item in overlay {
        let Some((grantee, privileges, grantor)) = parse_acl_item(item) else {
            continue;
        };
        if let Some(existing) = base.iter_mut().find(|base_item| {
            parse_acl_item(base_item)
                .map(|(base_grantee, _, base_grantor)| {
                    base_grantee == grantee && base_grantor == grantor
                })
                .unwrap_or(false)
        }) {
            let (_, existing_privileges, _) = parse_acl_item(existing).expect("validated above");
            let merged = canonicalize_acl_privileges_with_grant_options(
                &existing_privileges,
                &privileges,
                false,
                allowed,
            );
            *existing = format!("{grantee}={merged}/{grantor}");
        } else {
            base.push(item.clone());
        }
    }
}

pub(crate) fn default_acl_acl_shdepend_rows(
    db_oid: u32,
    default_acl_oid: u32,
    acl: &[String],
    auth_catalog: &crate::pgrust::auth::AuthCatalog,
) -> Vec<PgShdependRow> {
    let mut rows = acl
        .iter()
        .filter_map(|item| parse_acl_item(item).map(|(grantee, _, _)| grantee))
        .filter(|grantee| !grantee.is_empty())
        .filter_map(|grantee| auth_catalog.role_by_name(&grantee).map(|role| role.oid))
        .map(|role_oid| PgShdependRow {
            dbid: db_oid,
            classid: PG_DEFAULT_ACL_RELATION_OID,
            objid: default_acl_oid,
            objsubid: 0,
            refclassid: PG_AUTHID_RELATION_OID,
            refobjid: role_oid,
            deptype: SHARED_DEPENDENCY_ACL,
        })
        .collect::<Vec<_>>();
    sort_pg_shdepend_rows(&mut rows);
    rows.dedup();
    rows
}

impl Database {
    fn execute_grant_database_create_stmt(
        &self,
        client_id: ClientId,
        stmt: &GrantObjectStatement,
    ) -> Result<StatementResult, ExecError> {
        let object_name = single_object_name(stmt.named_object_names(), "single database name")?;
        if !execute_database_name_matches_current(object_name) {
            return Err(ExecError::DetailedError {
                message: format!("database \"{}\" does not exist", object_name),
                detail: None,
                hint: None,
                sqlstate: "3D000",
            });
        }

        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, None)
            .map_err(map_catalog_error)?;
        let database_owner_oid = current_database_owner_oid(self, client_id)?;
        if !can_grant_database_create(self, &auth, &auth_catalog, database_owner_oid) {
            return Err(ExecError::DetailedError {
                message: "permission denied to grant CREATE on database".into(),
                detail: None,
                hint: None,
                sqlstate: "42501",
            });
        }

        let current_user_oid = auth.current_user_oid();
        let mut grants = self.database_create_grants.write();
        for grantee_name in &stmt.grantee_names {
            if grantee_name.eq_ignore_ascii_case("public") {
                continue;
            }
            let grantee = auth_catalog.role_by_name(grantee_name).ok_or_else(|| {
                ExecError::Parse(role_management_error(format!(
                    "role \"{}\" does not exist",
                    grantee_name
                )))
            })?;
            if let Some(existing) = grants.iter_mut().find(|grant| {
                grant.grantee_oid == grantee.oid && grant.grantor_oid == current_user_oid
            }) {
                existing.grant_option |= stmt.with_grant_option;
            } else {
                grants.push(DatabaseCreateGrant {
                    grantee_oid: grantee.oid,
                    grantor_oid: current_user_oid,
                    grant_option: stmt.with_grant_option,
                });
            }
        }
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_revoke_database_create_stmt(
        &self,
        client_id: ClientId,
        stmt: &RevokeObjectStatement,
    ) -> Result<StatementResult, ExecError> {
        let object_name = single_object_name(stmt.named_object_names(), "single database name")?;
        if !execute_database_name_matches_current(object_name) {
            return Err(ExecError::DetailedError {
                message: format!("database \"{}\" does not exist", object_name),
                detail: None,
                hint: None,
                sqlstate: "3D000",
            });
        }

        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, None)
            .map_err(map_catalog_error)?;
        let database_owner_oid = current_database_owner_oid(self, client_id)?;
        let current_user_oid = auth.current_user_oid();
        let is_owner_or_superuser = auth_catalog
            .role_by_oid(current_user_oid)
            .is_some_and(|row| row.rolsuper)
            || current_user_oid == database_owner_oid;
        let mut grants = self.database_create_grants.write();
        for grantee_name in &stmt.grantee_names {
            if grantee_name.eq_ignore_ascii_case("public") {
                continue;
            }
            let grantee = auth_catalog.role_by_name(grantee_name).ok_or_else(|| {
                ExecError::Parse(role_management_error(format!(
                    "role \"{}\" does not exist",
                    grantee_name
                )))
            })?;
            if !can_revoke_database_create(
                &grants,
                &auth,
                &auth_catalog,
                database_owner_oid,
                grantee.oid,
            ) {
                return Err(ExecError::DetailedError {
                    message: "permission denied to revoke CREATE on database".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42501",
                });
            }
            revoke_database_create_grants(
                &mut grants,
                grantee.oid,
                current_user_oid,
                is_owner_or_superuser,
                stmt.cascade,
            );
        }
        Ok(StatementResult::AffectedRows(0))
    }
}

fn revoke_database_create_grants(
    grants: &mut Vec<DatabaseCreateGrant>,
    grantee_oid: u32,
    current_user_oid: u32,
    is_owner_or_superuser: bool,
    cascade: bool,
) {
    let mut removed_grantees = BTreeSet::new();
    grants.retain(|grant| {
        let revoke_direct = grant.grantee_oid == grantee_oid
            && (is_owner_or_superuser || grant.grantor_oid == current_user_oid);
        if revoke_direct {
            removed_grantees.insert(grant.grantee_oid);
            false
        } else {
            true
        }
    });

    if !cascade {
        return;
    }

    loop {
        let mut changed = false;
        grants.retain(|grant| {
            if removed_grantees.contains(&grant.grantor_oid) {
                removed_grantees.insert(grant.grantee_oid);
                changed = true;
                false
            } else {
                true
            }
        });
        if !changed {
            break;
        }
    }
}

fn upsert_role_membership_in_transaction(
    db: &Database,
    auth_catalog: &AuthCatalog,
    roleid: u32,
    member: u32,
    grantor: u32,
    member_inherit_default: bool,
    options: GrantRoleMembershipOptions,
    ctx: &CatalogWriteContext,
    catalog_effects: &mut Vec<CatalogMutationEffect>,
) -> Result<(), ExecError> {
    if let Some(existing) = auth_catalog
        .memberships()
        .iter()
        .find(|row| row.roleid == roleid && row.member == member && row.grantor == grantor)
    {
        let admin_option = if options.admin_option_specified {
            options.admin_option
        } else {
            existing.admin_option
        };
        let inherit_option = options.inherit_option.unwrap_or(existing.inherit_option);
        let set_option = options.set_option.unwrap_or(existing.set_option);
        if admin_option == existing.admin_option
            && inherit_option == existing.inherit_option
            && set_option == existing.set_option
        {
            push_role_membership_duplicate_notice(db, auth_catalog, roleid, member, grantor);
            return Ok(());
        }
        let (_, effect) = db
            .shared_catalog
            .write()
            .update_role_membership_options_mvcc(
                roleid,
                member,
                grantor,
                admin_option,
                inherit_option,
                set_option,
                ctx,
            )
            .map_err(map_role_grant_error)?;
        catalog_effects.push(effect);
    } else {
        let admin_option = if options.admin_option_specified {
            options.admin_option
        } else {
            false
        };
        let inherit_option = options.inherit_option.unwrap_or(member_inherit_default);
        let set_option = options.set_option.unwrap_or(true);
        let (_, effect) = db
            .shared_catalog
            .write()
            .grant_role_membership_mvcc(
                &membership_row(
                    roleid,
                    member,
                    grantor,
                    admin_option,
                    inherit_option,
                    set_option,
                ),
                ctx,
            )
            .map_err(|err| {
                map_named_role_membership_error(
                    err,
                    member,
                    &member_name(db, auth_catalog, member),
                    roleid,
                    &role_name_for_oid(db, auth_catalog, roleid),
                )
            })?;
        catalog_effects.push(effect);
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct GrantRoleMembershipOptions {
    admin_option: bool,
    admin_option_specified: bool,
    inherit_option: Option<bool>,
    set_option: Option<bool>,
}

impl From<&GrantRoleMembershipStatement> for GrantRoleMembershipOptions {
    fn from(stmt: &GrantRoleMembershipStatement) -> Self {
        Self {
            admin_option: stmt.admin_option,
            admin_option_specified: stmt.admin_option_specified,
            inherit_option: stmt.inherit_option,
            set_option: stmt.set_option,
        }
    }
}

fn push_role_membership_duplicate_notice(
    db: &Database,
    auth_catalog: &AuthCatalog,
    roleid: u32,
    member: u32,
    grantor: u32,
) {
    push_notice(format!(
        "role \"{}\" has already been granted membership in role \"{}\" by role \"{}\"",
        member_name(db, auth_catalog, member),
        role_name_for_oid(db, auth_catalog, roleid),
        role_name_for_oid(db, auth_catalog, grantor)
    ));
}

fn lookup_membership_grantee(
    catalog: &AuthCatalog,
    role_name: &str,
) -> Result<PgAuthIdRow, ExecError> {
    let role = lookup_membership_role_by_name(catalog, role_name)?;
    if role.oid == crate::include::catalog::PG_DATABASE_OWNER_OID {
        return Err(ExecError::Parse(role_management_error(format!(
            "role \"{}\" cannot be a member of any role",
            role.rolname
        ))));
    }
    Ok(role)
}

fn lookup_membership_role(
    catalog: &AuthCatalog,
    role_name: &str,
) -> Result<PgAuthIdRow, ExecError> {
    let role = lookup_membership_role_by_name(catalog, role_name)?;
    if role.oid == crate::include::catalog::PG_DATABASE_OWNER_OID {
        return Err(ExecError::Parse(role_management_error(format!(
            "role \"{}\" cannot have explicit members",
            role.rolname
        ))));
    }
    Ok(role)
}

fn lookup_membership_role_by_name(
    catalog: &AuthCatalog,
    role_name: &str,
) -> Result<PgAuthIdRow, ExecError> {
    catalog.role_by_name(role_name).cloned().ok_or_else(|| {
        ExecError::Parse(role_management_error(format!(
            "role \"{role_name}\" does not exist"
        )))
    })
}

fn resolve_role_grantor(
    auth: &AuthState,
    catalog: &AuthCatalog,
    role: &PgAuthIdRow,
    grantor: Option<&RoleGrantorSpec>,
    is_grant: bool,
    legacy_group_syntax: bool,
) -> Result<u32, ExecError> {
    let Some(grantor) = grantor else {
        return select_best_role_grantor(auth, catalog, role.oid, is_grant, legacy_group_syntax);
    };
    let grantor = resolve_role_grantor_spec(auth, catalog, grantor)?;

    if is_grant {
        if !auth.has_effective_membership(grantor.oid, catalog) {
            return Err(ExecError::DetailedError {
                message: format!(
                    "permission denied to grant privileges as role \"{}\"",
                    grantor.rolname
                ),
                detail: Some(format!(
                    "Only roles with privileges of role \"{}\" may grant privileges as this role.",
                    grantor.rolname
                )),
                hint: None,
                sqlstate: "42501",
            });
        }
        if grantor.oid != BOOTSTRAP_SUPERUSER_OID
            && !catalog
                .memberships()
                .iter()
                .any(|row| row.roleid == role.oid && row.member == grantor.oid && row.admin_option)
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "permission denied to grant privileges as role \"{}\"",
                    grantor.rolname
                ),
                detail: Some(format!(
                    "The grantor must have the ADMIN option on role \"{}\".",
                    role.rolname
                )),
                hint: None,
                sqlstate: "42501",
            });
        }
    } else if !auth.has_effective_membership(grantor.oid, catalog) {
        return Err(ExecError::DetailedError {
            message: format!(
                "permission denied to revoke privileges granted by role \"{}\"",
                grantor.rolname
            ),
            detail: Some(format!(
                "Only roles with privileges of role \"{}\" may revoke privileges granted by this role.",
                grantor.rolname
            )),
            hint: None,
            sqlstate: "42501",
        });
    }

    Ok(grantor.oid)
}

fn select_best_role_grantor(
    auth: &AuthState,
    catalog: &AuthCatalog,
    role_oid: u32,
    is_grant: bool,
    legacy_group_syntax: bool,
) -> Result<u32, ExecError> {
    if catalog
        .role_by_oid(auth.current_user_oid())
        .is_some_and(|row| row.rolsuper)
    {
        return Ok(BOOTSTRAP_SUPERUSER_OID);
    }
    if auth.current_user_oid() == role_oid {
        return Ok(role_oid);
    }

    let mut pending = VecDeque::from([(auth.current_user_oid(), 0usize)]);
    let mut visited = BTreeSet::new();
    let mut best: Option<(usize, u32)> = None;

    while let Some((member_oid, distance)) = pending.pop_front() {
        if !visited.insert(member_oid) {
            continue;
        }

        if member_oid == role_oid
            || catalog
                .memberships()
                .iter()
                .any(|row| row.member == member_oid && row.roleid == role_oid && row.admin_option)
        {
            match best {
                Some((best_distance, best_oid))
                    if best_distance < distance
                        || (best_distance == distance && best_oid <= member_oid) => {}
                _ => best = Some((distance, member_oid)),
            }
        }

        for edge in catalog
            .memberships()
            .iter()
            .filter(|row| row.member == member_oid && row.inherit_option)
        {
            pending.push_back((edge.roleid, distance.saturating_add(1)));
        }
    }

    best.map(|(_, oid)| oid).ok_or_else(|| {
        let role_name = catalog
            .role_by_oid(role_oid)
            .map(|row| row.rolname.as_str())
            .unwrap_or("unknown");
        let message = if legacy_group_syntax {
            "permission denied to alter role".to_string()
        } else {
            format!(
                "permission denied to {} role \"{}\"",
                if is_grant { "grant" } else { "revoke" },
                role_name,
            )
        };
        let detail = if legacy_group_syntax {
            format!(
                "Only roles with the ADMIN option on role \"{}\" may add or drop members.",
                role_name,
            )
        } else {
            format!(
                "Only roles with the ADMIN option on role \"{}\" may {} this role.",
                role_name,
                if is_grant { "grant" } else { "revoke" },
            )
        };
        ExecError::DetailedError {
            message,
            detail: Some(detail),
            hint: None,
            sqlstate: "42501",
        }
    })
}

fn reject_circular_admin_grant(
    catalog: &AuthCatalog,
    roleid: u32,
    grantor_oid: u32,
    grantee_oid: u32,
) -> Result<(), ExecError> {
    if grantor_oid == BOOTSTRAP_SUPERUSER_OID {
        return Ok(());
    }
    if grantee_oid == BOOTSTRAP_SUPERUSER_OID {
        return Err(ExecError::DetailedError {
            message: "ADMIN option cannot be granted back to your own grantor".into(),
            detail: None,
            hint: None,
            sqlstate: "0LP01",
        });
    }

    let role_rows = catalog
        .memberships()
        .iter()
        .filter(|row| row.roleid == roleid)
        .cloned()
        .collect::<Vec<_>>();
    let mut actions = vec![PlannedRoleMembershipRevoke::Noop; role_rows.len()];
    plan_member_revoke(&role_rows, &mut actions, grantee_oid)?;
    let grantor_retains_admin = role_rows.iter().enumerate().any(|(index, row)| {
        row.member == grantor_oid
            && row.admin_option
            && actions[index] == PlannedRoleMembershipRevoke::Noop
    });
    if grantor_retains_admin {
        Ok(())
    } else {
        Err(ExecError::DetailedError {
            message: "ADMIN option cannot be granted back to your own grantor".into(),
            detail: None,
            hint: None,
            sqlstate: "0LP01",
        })
    }
}

fn resolve_role_grantor_spec(
    auth: &AuthState,
    catalog: &AuthCatalog,
    grantor: &RoleGrantorSpec,
) -> Result<PgAuthIdRow, ExecError> {
    match grantor {
        RoleGrantorSpec::CurrentUser | RoleGrantorSpec::CurrentRole => catalog
            .role_by_oid(auth.current_user_oid())
            .cloned()
            .ok_or_else(|| ExecError::Parse(role_management_error("current role does not exist"))),
        RoleGrantorSpec::RoleName(role_name) => {
            catalog.role_by_name(role_name).cloned().ok_or_else(|| {
                ExecError::Parse(role_management_error(format!(
                    "role \"{}\" does not exist",
                    role_name
                )))
            })
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PlannedRoleMembershipRevoke {
    Noop,
    DeleteGrant,
    RemoveAdminOption,
    RemoveInheritOption,
    RemoveSetOption,
}

fn plan_member_revoke(
    role_rows: &[crate::include::catalog::PgAuthMembersRow],
    actions: &mut [PlannedRoleMembershipRevoke],
    member_oid: u32,
) -> Result<(), ExecError> {
    for (index, row) in role_rows.iter().enumerate() {
        if row.member == member_oid {
            plan_recursive_role_revoke(role_rows, actions, index, false, true)?;
        }
    }
    Ok(())
}

fn plan_role_membership_revoke(
    role_rows: &[crate::include::catalog::PgAuthMembersRow],
    target_index: usize,
    stmt: &RevokeRoleMembershipStatement,
) -> Result<Vec<PlannedRoleMembershipRevoke>, ExecError> {
    let mut actions = vec![PlannedRoleMembershipRevoke::Noop; role_rows.len()];
    if stmt.inherit_option {
        actions[target_index] = PlannedRoleMembershipRevoke::RemoveInheritOption;
        return Ok(actions);
    }
    if stmt.set_option {
        actions[target_index] = PlannedRoleMembershipRevoke::RemoveSetOption;
        return Ok(actions);
    }
    let revoke_admin_option_only = stmt.admin_option;
    plan_recursive_role_revoke(
        role_rows,
        &mut actions,
        target_index,
        revoke_admin_option_only,
        stmt.cascade,
    )?;
    Ok(actions)
}

fn plan_recursive_role_revoke(
    role_rows: &[crate::include::catalog::PgAuthMembersRow],
    actions: &mut [PlannedRoleMembershipRevoke],
    index: usize,
    revoke_admin_option_only: bool,
    cascade: bool,
) -> Result<(), ExecError> {
    if actions[index] == PlannedRoleMembershipRevoke::DeleteGrant {
        return Ok(());
    }
    if actions[index] == PlannedRoleMembershipRevoke::RemoveAdminOption && revoke_admin_option_only
    {
        return Ok(());
    }

    let row = &role_rows[index];
    if !revoke_admin_option_only {
        actions[index] = PlannedRoleMembershipRevoke::DeleteGrant;
        if !row.admin_option {
            return Ok(());
        }
    } else {
        if !row.admin_option {
            return Ok(());
        }
        actions[index] = PlannedRoleMembershipRevoke::RemoveAdminOption;
    }

    let would_still_have_admin_option = role_rows.iter().enumerate().any(|(other_index, other)| {
        other_index != index
            && other.member == row.member
            && other.admin_option
            && actions[other_index] == PlannedRoleMembershipRevoke::Noop
    });
    if would_still_have_admin_option {
        return Ok(());
    }

    for (other_index, other) in role_rows.iter().enumerate() {
        if other.grantor == row.member
            && actions[other_index] != PlannedRoleMembershipRevoke::DeleteGrant
        {
            if !cascade {
                return Err(ExecError::DetailedError {
                    message: "dependent privileges exist".into(),
                    detail: None,
                    hint: Some("Use CASCADE to revoke them too.".into()),
                    sqlstate: "2BP01",
                });
            }
            plan_recursive_role_revoke(role_rows, actions, other_index, false, cascade)?;
        }
    }

    Ok(())
}

fn map_role_grant_error(err: crate::backend::catalog::CatalogError) -> ExecError {
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

fn map_named_role_membership_error(
    err: crate::backend::catalog::CatalogError,
    member_oid: u32,
    member_name: &str,
    role_oid: u32,
    role_name: &str,
) -> ExecError {
    match err {
        crate::backend::catalog::CatalogError::UniqueViolation(message)
            if message == format!("role membership cycle: {member_oid} -> {role_oid}") =>
        {
            ExecError::Parse(role_management_error(format!(
                "role \"{member_name}\" is a member of role \"{role_name}\""
            )))
        }
        other => map_role_grant_error(other),
    }
}

fn role_name_for_oid(_db: &Database, auth_catalog: &AuthCatalog, role_oid: u32) -> String {
    auth_catalog
        .role_by_oid(role_oid)
        .map(|row| row.rolname.clone())
        .unwrap_or_else(|| role_oid.to_string())
}

fn member_name(db: &Database, auth_catalog: &AuthCatalog, member_oid: u32) -> String {
    role_name_for_oid(db, auth_catalog, member_oid)
}

fn type_namespace_visible(namespace_oid: u32, search_path: &[String]) -> bool {
    search_path.iter().any(|schema| {
        (schema == "public" && namespace_oid == crate::include::catalog::PUBLIC_NAMESPACE_OID)
            || (schema == "pg_catalog"
                && namespace_oid == crate::include::catalog::PG_CATALOG_NAMESPACE_OID)
    })
}

fn cannot_set_multirange_privileges_error(_range_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: "cannot set privileges of multirange types".into(),
        detail: None,
        hint: Some("Set the privileges of the range type instead.".into()),
        sqlstate: "42809",
    }
}

fn role_does_not_exist_error(role_name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("role \"{role_name}\" does not exist"),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::executor::StatementResult;
    use crate::include::nodes::datum::Value;
    use crate::pgrust::session::Session;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "pgrust_privilege_{}_{}_{}",
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

    fn table_acl(db: &Database, relname: &str) -> Option<Vec<String>> {
        db.catalog
            .read()
            .catcache()
            .unwrap()
            .class_rows()
            .into_iter()
            .find(|row| row.relname == relname)
            .and_then(|row| row.relacl)
    }

    fn query_rows(session: &mut Session, db: &Database, sql: &str) -> Vec<Vec<Value>> {
        match session.execute(db, sql).unwrap() {
            StatementResult::Query { rows, .. } => rows,
            other => panic!("expected query result, got {other:?}"),
        }
    }

    #[test]
    fn database_create_grant_allows_create_schema() {
        let base = temp_dir("db_create_grant");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role tenant login").unwrap();
        session
            .execute(
                &db,
                "grant create on database regression to tenant with grant option",
            )
            .unwrap();
        session
            .execute(&db, "set session authorization tenant")
            .unwrap();
        assert_eq!(
            session.execute(&db, "create schema tenant_schema").unwrap(),
            StatementResult::AffectedRows(0)
        );
    }

    #[test]
    fn table_grant_update_delete_and_revoke_delete_update_acl() {
        let base = temp_dir("table_grant_update_delete");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role tenant").unwrap();
        session.execute(&db, "create table acl_t1 (a int)").unwrap();
        session.execute(&db, "create table acl_t2 (a int)").unwrap();

        session
            .execute(&db, "grant update, delete on acl_t1, acl_t2 to tenant")
            .unwrap();
        session
            .execute(&db, "revoke delete on acl_t1 from tenant")
            .unwrap();

        let acl_t1 = table_acl(&db, "acl_t1").unwrap();
        let acl_t2 = table_acl(&db, "acl_t2").unwrap();
        assert!(acl_t1.iter().any(|item| item.starts_with("tenant=w/")));
        assert!(acl_t2.iter().any(|item| item.starts_with("tenant=wd/")));
    }

    #[test]
    fn default_table_acl_applies_only_to_new_tables() {
        let base = temp_dir("default_table_acl");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role reader").unwrap();
        session.execute(&db, "create schema defacl_ns").unwrap();
        session
            .execute(&db, "create table defacl_ns.before_acl (a int)")
            .unwrap();
        session
            .execute(
                &db,
                "alter default privileges in schema defacl_ns grant select on tables to reader",
            )
            .unwrap();
        session
            .execute(&db, "create table defacl_ns.after_acl (a int)")
            .unwrap();

        assert!(table_acl(&db, "before_acl").is_none());
        let after_acl = table_acl(&db, "after_acl").unwrap();
        assert!(after_acl.iter().any(|item| item == "reader=r/postgres"));
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select count(*)::int4 from pg_default_acl"
            ),
            vec![vec![Value::Int32(1)]]
        );
    }

    #[test]
    fn default_schema_acl_applies_to_new_schemas() {
        let base = temp_dir("default_schema_acl");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role schema_reader").unwrap();
        session
            .execute(
                &db,
                "alter default privileges grant usage on schemas to schema_reader",
            )
            .unwrap();
        session.execute(&db, "create schema schema_acl_a").unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select has_schema_privilege('schema_reader', 'schema_acl_a', 'usage'),
                        has_schema_privilege('schema_reader', 'schema_acl_a', 'create')"
            ),
            vec![vec![Value::Bool(true), Value::Bool(false)]]
        );

        session
            .execute(
                &db,
                "alter default privileges revoke usage on schemas from schema_reader",
            )
            .unwrap();
        session.execute(&db, "create schema schema_acl_b").unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select has_schema_privilege('schema_reader', 'schema_acl_b', 'usage')"
            ),
            vec![vec![Value::Bool(false)]]
        );
    }

    #[test]
    fn grant_all_tables_in_schema_updates_existing_tables() {
        let base = temp_dir("grant_all_tables_in_schema");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role bulk_reader").unwrap();
        session.execute(&db, "create schema bulk_acl_ns").unwrap();
        session
            .execute(&db, "create table bulk_acl_ns.bulk_a (a int)")
            .unwrap();
        session
            .execute(&db, "create table bulk_acl_ns.bulk_b (a int)")
            .unwrap();
        session
            .execute(
                &db,
                "grant select on all tables in schema bulk_acl_ns to bulk_reader",
            )
            .unwrap();

        for relname in ["bulk_a", "bulk_b"] {
            let acl = table_acl(&db, relname).unwrap();
            assert!(acl.iter().any(|item| item == "bulk_reader=r/postgres"));
        }
    }

    #[test]
    fn bulk_function_procedure_routine_acl_classification() {
        let base = temp_dir("bulk_routine_acl");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role routine_user").unwrap();
        session
            .execute(&db, "create schema routine_acl_ns")
            .unwrap();
        session
            .execute(
                &db,
                "create function routine_acl_ns.f_acl() returns int4 as 'select 1' language sql",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create aggregate routine_acl_ns.a_acl(int) (sfunc = int4pl, stype = int4)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create procedure routine_acl_ns.p_acl() language sql as 'select 1'",
            )
            .unwrap();

        session
            .execute(
                &db,
                "revoke execute on function routine_acl_ns.a_acl(int) from public",
            )
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select has_function_privilege('routine_user', 'routine_acl_ns.a_acl(int)', 'execute')"
            ),
            vec![vec![Value::Bool(false)]]
        );
        session
            .execute(
                &db,
                "grant execute on function routine_acl_ns.a_acl(int) to routine_user",
            )
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select has_function_privilege('routine_user', 'routine_acl_ns.a_acl(int)', 'execute')"
            ),
            vec![vec![Value::Bool(true)]]
        );
        session
            .execute(
                &db,
                "revoke execute on function routine_acl_ns.a_acl(int) from routine_user",
            )
            .unwrap();

        session
            .execute(
                &db,
                "revoke execute on all functions in schema routine_acl_ns from public",
            )
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select has_function_privilege('routine_user', 'routine_acl_ns.f_acl()', 'execute'),
                        has_function_privilege('routine_user', 'routine_acl_ns.a_acl(int)', 'execute'),
                        has_function_privilege('routine_user', 'routine_acl_ns.p_acl()', 'execute')"
            ),
            vec![vec![Value::Bool(false), Value::Bool(false), Value::Bool(true)]]
        );

        session
            .execute(
                &db,
                "revoke execute on all procedures in schema routine_acl_ns from public",
            )
            .unwrap();
        session
            .execute(
                &db,
                "grant execute on all routines in schema routine_acl_ns to routine_user",
            )
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select has_function_privilege('routine_user', 'routine_acl_ns.f_acl()', 'execute'),
                        has_function_privilege('routine_user', 'routine_acl_ns.a_acl(int)', 'execute'),
                        has_function_privilege('routine_user', 'routine_acl_ns.p_acl()', 'execute'),
                        has_function_privilege('sum(numeric)', 'execute')"
            ),
            vec![vec![
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(true)
            ]]
        );
    }

    #[test]
    fn grant_role_membership_updates_existing_options() {
        let base = temp_dir("grant_role_options");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "set createrole_self_grant to 'set, inherit'")
            .unwrap();
        session
            .execute(&db, "create role creator createrole noinherit")
            .unwrap();
        session
            .execute(&db, "set session authorization creator")
            .unwrap();
        session.execute(&db, "create role tenant2").unwrap();
        session
            .execute(&db, "grant tenant2 to creator with inherit true, set false")
            .unwrap();

        let tenant2_oid = role_oid(&db, "tenant2");
        let creator_oid = role_oid(&db, "creator");
        let membership = db
            .catalog
            .read()
            .catcache()
            .unwrap()
            .auth_members_rows()
            .into_iter()
            .find(|row| {
                row.roleid == tenant2_oid && row.member == creator_oid && row.grantor == creator_oid
            })
            .unwrap();
        assert!(membership.inherit_option);
        assert!(!membership.set_option);
    }

    #[test]
    fn duplicate_grant_role_membership_emits_notice_without_clearing_options() {
        let base = temp_dir("grant_role_duplicate_notice");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        crate::backend::utils::misc::notices::take_notices();
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role grantee").unwrap();
        session
            .execute(&db, "grant parent to grantee with admin option")
            .unwrap();
        crate::backend::utils::misc::notices::take_notices();

        session.execute(&db, "grant parent to grantee").unwrap();

        let notices = crate::backend::utils::misc::notices::take_notices();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].severity, "NOTICE");
        assert_eq!(
            notices[0].message,
            "role \"grantee\" has already been granted membership in role \"parent\" by role \"postgres\""
        );

        let parent_oid = role_oid(&db, "parent");
        let grantee_oid = role_oid(&db, "grantee");
        let membership = db
            .catalog
            .read()
            .catcache()
            .unwrap()
            .auth_members_rows()
            .into_iter()
            .find(|row| row.roleid == parent_oid && row.member == grantee_oid)
            .unwrap();
        assert!(membership.admin_option);
    }

    #[test]
    fn grant_role_membership_records_explicit_grantor() {
        let base = temp_dir("grant_role_grantor");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role grantor").unwrap();
        session.execute(&db, "create role grantee").unwrap();
        session
            .execute(&db, "grant parent to grantor with admin option")
            .unwrap();
        session
            .execute(&db, "grant parent to grantee granted by grantor")
            .unwrap();

        let parent_oid = role_oid(&db, "parent");
        let grantor_oid = role_oid(&db, "grantor");
        let grantee_oid = role_oid(&db, "grantee");
        let membership = db
            .catalog
            .read()
            .catcache()
            .unwrap()
            .auth_members_rows()
            .into_iter()
            .find(|row| {
                row.roleid == parent_oid && row.member == grantee_oid && row.grantor == grantor_oid
            })
            .unwrap();
        assert!(!membership.admin_option);
    }

    #[test]
    fn grant_role_membership_uses_inherited_admin_grantor() {
        let base = temp_dir("grant_role_inferred_grantor");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role grantor").unwrap();
        session.execute(&db, "create role acting").unwrap();
        session.execute(&db, "create role grantee").unwrap();
        session
            .execute(&db, "grant parent to grantor with admin option")
            .unwrap();
        session.execute(&db, "grant grantor to acting").unwrap();
        session.execute(&db, "set role acting").unwrap();
        session.execute(&db, "grant parent to grantee").unwrap();

        let parent_oid = role_oid(&db, "parent");
        let grantor_oid = role_oid(&db, "grantor");
        let grantee_oid = role_oid(&db, "grantee");
        let membership = db
            .catalog
            .read()
            .catcache()
            .unwrap()
            .auth_members_rows()
            .into_iter()
            .find(|row| {
                row.roleid == parent_oid && row.member == grantee_oid && row.grantor == grantor_oid
            })
            .unwrap();
        assert!(!membership.admin_option);
    }

    #[test]
    fn explicit_role_grantor_must_have_admin_option() {
        let base = temp_dir("grant_role_grantor_admin");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role grantor").unwrap();
        session.execute(&db, "create role grantee").unwrap();

        let err = session
            .execute(&db, "grant parent to grantee granted by grantor")
            .unwrap_err();
        match err {
            ExecError::DetailedError {
                message, detail, ..
            } => {
                assert_eq!(
                    message,
                    "permission denied to grant privileges as role \"grantor\""
                );
                assert_eq!(
                    detail.as_deref(),
                    Some("The grantor must have the ADMIN option on role \"parent\".")
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn explicit_role_grantor_cannot_be_target_role_without_admin_option() {
        let base = temp_dir("grant_role_self_grantor_admin");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role member").unwrap();

        let err = session
            .execute(
                &db,
                "grant parent to member with admin option granted by parent",
            )
            .unwrap_err();
        match err {
            ExecError::DetailedError {
                message, detail, ..
            } => {
                assert_eq!(
                    message,
                    "permission denied to grant privileges as role \"parent\""
                );
                assert_eq!(
                    detail.as_deref(),
                    Some("The grantor must have the ADMIN option on role \"parent\".")
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn revoke_missing_role_membership_emits_warning() {
        let base = temp_dir("revoke_role_missing_warning");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        crate::backend::utils::misc::notices::take_notices();
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role grantor").unwrap();
        session.execute(&db, "create role grantee").unwrap();
        session
            .execute(&db, "grant parent to grantor with admin option")
            .unwrap();
        crate::backend::utils::misc::notices::take_notices();

        session
            .execute(&db, "revoke parent from grantee granted by grantor")
            .unwrap();

        let notices = crate::backend::utils::misc::notices::take_notices();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].severity, "WARNING");
        assert_eq!(
            notices[0].message,
            "role \"grantee\" has not been granted membership in role \"parent\" by role \"grantor\""
        );
    }

    #[test]
    fn alter_group_permission_denied_uses_legacy_wording() {
        let base = temp_dir("alter_group_permission_denied");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create role regress_priv_group2")
            .unwrap();
        session
            .execute(&db, "create role regress_priv_user1 login")
            .unwrap();
        session
            .execute(&db, "create role regress_priv_user2 login")
            .unwrap();
        session
            .execute(&db, "create role regress_priv_user3 login")
            .unwrap();
        session
            .execute(
                &db,
                "grant regress_priv_group2 to regress_priv_user1 with admin option",
            )
            .unwrap();
        session
            .execute(
                &db,
                "grant regress_priv_group2 to regress_priv_user2 granted by regress_priv_user1",
            )
            .unwrap();
        session
            .execute(&db, "set session authorization regress_priv_user3")
            .unwrap();

        for sql in [
            "alter group regress_priv_group2 add user regress_priv_user2",
            "alter group regress_priv_group2 drop user regress_priv_user2",
        ] {
            let err = session.execute(&db, sql).unwrap_err();
            match err {
                ExecError::DetailedError {
                    message, detail, ..
                } => {
                    assert_eq!(message, "permission denied to alter role");
                    assert_eq!(
                        detail.as_deref(),
                        Some(
                            "Only roles with the ADMIN option on role \"regress_priv_group2\" may add or drop members."
                        )
                    );
                }
                other => panic!("unexpected error for {sql}: {other:?}"),
            }
        }
    }

    #[test]
    fn grant_role_membership_rejects_circular_admin_option() {
        let base = temp_dir("grant_role_admin_cycle");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role user2").unwrap();
        session.execute(&db, "create role user3").unwrap();
        session
            .execute(&db, "grant parent to user2 with admin option")
            .unwrap();
        session
            .execute(
                &db,
                "grant parent to user3 with admin option granted by user2",
            )
            .unwrap();

        let err = session
            .execute(
                &db,
                "grant parent to user2 with admin option granted by user3",
            )
            .unwrap_err();
        match err {
            ExecError::DetailedError { message, .. } => {
                assert_eq!(
                    message,
                    "ADMIN option cannot be granted back to your own grantor"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn plain_revoke_role_membership_removes_explicit_grant() {
        let base = temp_dir("revoke_role_grantor");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role grantor").unwrap();
        session.execute(&db, "create role grantee").unwrap();
        session
            .execute(&db, "grant parent to grantor with admin option")
            .unwrap();
        session
            .execute(&db, "grant parent to grantee granted by grantor")
            .unwrap();
        session
            .execute(&db, "revoke parent from grantee granted by grantor")
            .unwrap();

        let parent_oid = role_oid(&db, "parent");
        let grantor_oid = role_oid(&db, "grantor");
        let grantee_oid = role_oid(&db, "grantee");
        assert!(
            !db.catalog
                .read()
                .catcache()
                .unwrap()
                .auth_members_rows()
                .into_iter()
                .any(|row| {
                    row.roleid == parent_oid
                        && row.member == grantee_oid
                        && row.grantor == grantor_oid
                })
        );
    }

    #[test]
    fn revoke_admin_option_uses_inherited_grantor() {
        let base = temp_dir("revoke_role_admin_inferred_grantor");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role grantor").unwrap();
        session.execute(&db, "create role acting").unwrap();
        session.execute(&db, "create role grantee").unwrap();
        session
            .execute(&db, "grant parent to grantor with admin option")
            .unwrap();
        session.execute(&db, "grant grantor to acting").unwrap();
        session.execute(&db, "set role acting").unwrap();
        session
            .execute(&db, "grant parent to grantee with admin option")
            .unwrap();
        session
            .execute(&db, "revoke admin option for parent from grantee")
            .unwrap();

        let parent_oid = role_oid(&db, "parent");
        let grantor_oid = role_oid(&db, "grantor");
        let grantee_oid = role_oid(&db, "grantee");
        let membership = db
            .catalog
            .read()
            .catcache()
            .unwrap()
            .auth_members_rows()
            .into_iter()
            .find(|row| {
                row.roleid == parent_oid && row.member == grantee_oid && row.grantor == grantor_oid
            })
            .unwrap();
        assert!(!membership.admin_option);
    }

    #[test]
    fn revoke_admin_option_requires_cascade_for_dependent_grants() {
        let base = temp_dir("revoke_role_admin_dependents");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role grantor").unwrap();
        session.execute(&db, "create role grantee").unwrap();
        session.execute(&db, "create role child").unwrap();
        session
            .execute(&db, "grant parent to grantor with admin option")
            .unwrap();
        session
            .execute(
                &db,
                "grant parent to grantee with admin option granted by grantor",
            )
            .unwrap();
        session
            .execute(&db, "grant parent to child granted by grantee")
            .unwrap();

        let err = session
            .execute(
                &db,
                "revoke admin option for parent from grantee granted by grantor",
            )
            .unwrap_err();
        match err {
            ExecError::DetailedError { message, hint, .. } => {
                assert_eq!(message, "dependent privileges exist");
                assert_eq!(hint.as_deref(), Some("Use CASCADE to revoke them too."));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn revoke_admin_option_cascade_removes_dependent_grants() {
        let base = temp_dir("revoke_role_admin_cascade");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role grantor").unwrap();
        session.execute(&db, "create role grantee").unwrap();
        session.execute(&db, "create role child").unwrap();
        session
            .execute(&db, "grant parent to grantor with admin option")
            .unwrap();
        session
            .execute(
                &db,
                "grant parent to grantee with admin option granted by grantor",
            )
            .unwrap();
        session
            .execute(&db, "grant parent to child granted by grantee")
            .unwrap();
        session
            .execute(
                &db,
                "revoke admin option for parent from grantee granted by grantor cascade",
            )
            .unwrap();

        let parent_oid = role_oid(&db, "parent");
        let grantor_oid = role_oid(&db, "grantor");
        let grantee_oid = role_oid(&db, "grantee");
        let child_oid = role_oid(&db, "child");
        let rows = db.catalog.read().catcache().unwrap().auth_members_rows();
        let membership = rows
            .iter()
            .find(|row| {
                row.roleid == parent_oid && row.member == grantee_oid && row.grantor == grantor_oid
            })
            .unwrap();
        assert!(!membership.admin_option);
        assert!(!rows.iter().any(|row| {
            row.roleid == parent_oid && row.member == child_oid && row.grantor == grantee_oid
        }));
    }

    #[test]
    fn revoke_set_option_clears_set_flag() {
        let base = temp_dir("revoke_role_set_option");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role grantee").unwrap();
        session
            .execute(&db, "grant parent to grantee with set true")
            .unwrap();
        session
            .execute(&db, "revoke set option for parent from grantee")
            .unwrap();

        let parent_oid = role_oid(&db, "parent");
        let grantee_oid = role_oid(&db, "grantee");
        let membership = db
            .catalog
            .read()
            .catcache()
            .unwrap()
            .auth_members_rows()
            .into_iter()
            .find(|row| row.roleid == parent_oid && row.member == grantee_oid)
            .unwrap();
        assert!(!membership.set_option);
    }

    #[test]
    fn revoke_role_membership_requires_cascade_for_dependent_grants() {
        let base = temp_dir("revoke_role_grantor_dependents");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role grantor").unwrap();
        session.execute(&db, "create role grantee").unwrap();
        session.execute(&db, "create role child").unwrap();
        session
            .execute(&db, "grant parent to grantor with admin option")
            .unwrap();
        session
            .execute(
                &db,
                "grant parent to grantee with admin true granted by grantor",
            )
            .unwrap();
        session
            .execute(&db, "grant parent to child granted by grantee")
            .unwrap();

        let err = session
            .execute(&db, "revoke parent from grantee granted by grantor")
            .unwrap_err();
        match err {
            ExecError::DetailedError { message, hint, .. } => {
                assert_eq!(message, "dependent privileges exist");
                assert_eq!(hint.as_deref(), Some("Use CASCADE to revoke them too."));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn revoke_role_membership_cascade_removes_dependent_grants() {
        let base = temp_dir("revoke_role_grantor_cascade");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role parent").unwrap();
        session.execute(&db, "create role grantor").unwrap();
        session.execute(&db, "create role grantee").unwrap();
        session.execute(&db, "create role child").unwrap();
        session
            .execute(&db, "grant parent to grantor with admin option")
            .unwrap();
        session
            .execute(
                &db,
                "grant parent to grantee with admin true granted by grantor",
            )
            .unwrap();
        session
            .execute(&db, "grant parent to child granted by grantee")
            .unwrap();
        session
            .execute(&db, "revoke parent from grantee granted by grantor cascade")
            .unwrap();

        let parent_oid = role_oid(&db, "parent");
        let grantor_oid = role_oid(&db, "grantor");
        let grantee_oid = role_oid(&db, "grantee");
        let child_oid = role_oid(&db, "child");
        let rows = db.catalog.read().catcache().unwrap().auth_members_rows();
        assert!(!rows.iter().any(|row| {
            row.roleid == parent_oid && row.member == grantee_oid && row.grantor == grantor_oid
        }));
        assert!(!rows.iter().any(|row| {
            row.roleid == parent_oid && row.member == child_oid && row.grantor == grantee_oid
        }));
    }

    #[test]
    fn type_usage_privilege_blocks_and_allows_table_column_type() {
        let base = temp_dir("type_usage_table_column");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role type_acl_user").unwrap();
        session
            .execute(&db, "grant create on schema public to type_acl_user")
            .unwrap();
        session
            .execute(&db, "create type type_acl_composite as (a int)")
            .unwrap();
        session
            .execute(&db, "revoke usage on type type_acl_composite from public")
            .unwrap();
        session
            .execute(&db, "set session authorization type_acl_user")
            .unwrap();

        let err = session
            .execute(&db, "create table type_acl_denied (c type_acl_composite)")
            .unwrap_err();
        match err {
            ExecError::DetailedError { message, .. } => {
                assert_eq!(message, "permission denied for type type_acl_composite");
            }
            other => panic!("unexpected error: {other:?}"),
        }

        session.execute(&db, "reset session authorization").unwrap();
        session
            .execute(
                &db,
                "grant usage on type type_acl_composite to type_acl_user with grant option",
            )
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select has_type_privilege('type_acl_user', 'type_acl_composite', 'USAGE'),
                        has_type_privilege('type_acl_user', 'type_acl_composite', 'USAGE WITH GRANT OPTION')"
            ),
            vec![vec![Value::Bool(true), Value::Bool(true)]]
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select has_type_privilege(999999::oid, 'USAGE')"
            ),
            vec![vec![Value::Null]]
        );
        session
            .execute(&db, "set session authorization type_acl_user")
            .unwrap();
        session
            .execute(&db, "create table type_acl_allowed (c type_acl_composite)")
            .unwrap();
    }

    #[test]
    fn owner_revoke_type_usage_from_ungranted_role_does_not_warn() {
        let base = temp_dir("type_revoke_missing_role_acl");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role type_acl_user").unwrap();
        session
            .execute(&db, "create type type_acl_composite as (a int)")
            .unwrap();

        crate::backend::utils::misc::notices::clear_notices();
        session
            .execute(
                &db,
                "revoke usage on type type_acl_composite from type_acl_user",
            )
            .unwrap();
        assert!(crate::backend::utils::misc::notices::take_notices().is_empty());

        session
            .execute(&db, "revoke usage on type type_acl_composite from public")
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select has_type_privilege('type_acl_user', 'type_acl_composite', 'USAGE')"
            ),
            vec![vec![Value::Bool(false)]]
        );
    }

    #[test]
    fn non_owner_revoke_type_usage_without_grant_option_errors() {
        let base = temp_dir("type_revoke_requires_grant_option");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session.execute(&db, "create role type_acl_owner").unwrap();
        session.execute(&db, "create role type_acl_user").unwrap();
        session
            .execute(&db, "set session authorization type_acl_owner")
            .unwrap();
        session
            .execute(&db, "create type type_acl_composite as (a int)")
            .unwrap();
        session
            .execute(&db, "revoke usage on type type_acl_composite from public")
            .unwrap();
        session
            .execute(&db, "set session authorization type_acl_user")
            .unwrap();

        match session.execute(&db, "revoke usage on type type_acl_composite from public") {
            Err(ExecError::DetailedError {
                message, sqlstate, ..
            }) => {
                assert_eq!(
                    message,
                    "permission denied for type type_acl_composite".to_string()
                );
                assert_eq!(sqlstate, "42501");
            }
            other => panic!("expected type permission error, got {other:?}"),
        }
    }

    #[test]
    fn non_owner_grant_table_without_privilege_errors_without_warning() {
        let base = temp_dir("table_grant_requires_privilege");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session.execute(&db, "create role table_acl_owner").unwrap();
        session
            .execute(&db, "create role table_acl_grantor")
            .unwrap();
        session
            .execute(&db, "create role table_acl_grantee")
            .unwrap();
        session
            .execute(&db, "set session authorization table_acl_owner")
            .unwrap();
        session
            .execute(&db, "create table table_acl_t(a int)")
            .unwrap();
        session
            .execute(&db, "set session authorization table_acl_grantor")
            .unwrap();
        crate::backend::utils::misc::notices::clear_notices();

        match session.execute(&db, "grant select on table_acl_t to table_acl_grantee") {
            Err(ExecError::DetailedError {
                message, sqlstate, ..
            }) => {
                assert_eq!(message, "permission denied for table table_acl_t");
                assert_eq!(sqlstate, "42501");
            }
            other => panic!("expected table permission error, got {other:?}"),
        }
        assert!(crate::backend::utils::misc::notices::take_notices().is_empty());
    }

    #[test]
    fn type_acl_rejects_array_targets_and_domain_kind_mismatch() {
        let base = temp_dir("type_acl_object_kind");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role type_acl_user").unwrap();
        session
            .execute(&db, "create type type_acl_composite as (a int)")
            .unwrap();

        let err = session
            .execute(
                &db,
                "grant usage on type type_acl_composite[] to type_acl_user",
            )
            .unwrap_err();
        match err {
            ExecError::DetailedError { message, hint, .. } => {
                assert_eq!(message, "cannot set privileges of array types");
                assert_eq!(
                    hint.as_deref(),
                    Some("Set the privileges of the element type instead.")
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let err = session
            .execute(
                &db,
                "grant usage on domain type_acl_composite to type_acl_user",
            )
            .unwrap_err();
        match err {
            ExecError::DetailedError { message, .. } => {
                assert_eq!(message, "\"type_acl_composite\" is not a domain");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn default_type_privileges_apply_to_new_domains() {
        let base = temp_dir("default_type_privileges");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create role type_acl_user").unwrap();
        session.execute(&db, "create schema type_acl_ns").unwrap();
        session
            .execute(
                &db,
                "alter default privileges revoke usage on types from public",
            )
            .unwrap();
        session
            .execute(&db, "create domain type_acl_ns.private_domain as int")
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select has_type_privilege('type_acl_user', 'type_acl_ns.private_domain', 'USAGE')"
            ),
            vec![vec![Value::Bool(false)]]
        );

        session
            .execute(
                &db,
                "alter default privileges in schema type_acl_ns grant usage on types to public",
            )
            .unwrap();
        session
            .execute(&db, "create domain type_acl_ns.public_domain as int")
            .unwrap();
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select has_type_privilege('type_acl_user', 'type_acl_ns.public_domain', 'USAGE')"
            ),
            vec![vec![Value::Bool(true)]]
        );
    }
}
