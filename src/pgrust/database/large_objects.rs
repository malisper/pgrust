use parking_lot::RwLock;
use std::collections::{BTreeMap, BTreeSet};

use super::{AutoCommitGuard, Database};
use crate::ClientId;
use crate::backend::access::heap::heapam::{heap_scan_begin_visible, heap_scan_next_visible};
use crate::backend::access::transam::xact::{CommandId, Snapshot, TransactionId};
use crate::backend::catalog::bootstrap::bootstrap_catalog_rel;
use crate::backend::catalog::catalog::CatalogError;
use crate::backend::catalog::persistence::{
    delete_catalog_rows_subset_mvcc, insert_catalog_rows_subset_mvcc,
};
use crate::backend::catalog::rowcodec::{
    decode_catalog_tuple_values, pg_default_acl_row_from_values, pg_description_row_from_values,
    pg_largeobject_metadata_row_from_values, pg_largeobject_row_from_values,
};
use crate::backend::catalog::rows::PhysicalCatalogRows;
use crate::backend::catalog::store::{CatalogMutationEffect, CatalogWriteContext};
use crate::backend::commands::rolecmds::role_management_error;
use crate::backend::executor::{ExecError, ExecutorContext, StatementResult};
use crate::backend::parser::{
    GrantObjectPrivilege, GrantObjectStatement, ParseError, RevokeObjectStatement,
};
use crate::include::catalog::{
    BootstrapCatalogKind, PG_LARGEOBJECT_RELATION_OID, PgDefaultAclRow, PgDescriptionRow,
    PgLargeobjectMetadataRow, PgLargeobjectRow, bootstrap_relation_desc,
};
use crate::pgrust::auth::AuthCatalog;
use crate::pgrust::database::commands::privilege::effective_acl_grantee_names;
use crate::pgrust::database::ddl::ensure_can_set_role;

pub(crate) const INV_WRITE: i32 = 0x0002_0000;
pub(crate) const INV_READ: i32 = 0x0004_0000;
pub(crate) const LOBLKSIZE: usize = 8192 / 4;

const LARGE_OBJECT_ALL_PRIVILEGES: &str = "rw";
const DEFAULT_ACL_LARGE_OBJECT: char = 'L';

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LargeObjectDefaultPrivilegeSpec {
    pub role_name: Option<String>,
    pub grantee_names: Vec<String>,
    pub privilege_chars: String,
    pub revoke: bool,
    pub with_grant_option: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LargeObjectDescriptor {
    pub loid: u32,
    pub offset: i64,
    pub can_read: bool,
    pub can_write: bool,
}

#[derive(Debug, Default)]
struct ClientLargeObjectDescriptors {
    next_fd: i32,
    descriptors: BTreeMap<i32, LargeObjectDescriptor>,
}

#[derive(Debug, Default)]
pub struct LargeObjectRuntime {
    descriptors: RwLock<BTreeMap<ClientId, ClientLargeObjectDescriptors>>,
}

impl LargeObjectRuntime {
    pub(crate) fn new_ephemeral() -> Self {
        Self::default()
    }

    pub(crate) fn open_descriptor(
        &self,
        client_id: ClientId,
        loid: u32,
        can_read: bool,
        can_write: bool,
    ) -> i32 {
        let mut clients = self.descriptors.write();
        let state = clients.entry(client_id).or_default();
        let fd = state.next_fd;
        state.next_fd = state.next_fd.saturating_add(1);
        state.descriptors.insert(
            fd,
            LargeObjectDescriptor {
                loid,
                offset: 0,
                can_read,
                can_write,
            },
        );
        fd
    }

    pub(crate) fn close_descriptor(&self, client_id: ClientId, fd: i32) -> Result<i32, ExecError> {
        let mut clients = self.descriptors.write();
        let Some(state) = clients.get_mut(&client_id) else {
            return Err(invalid_large_object_descriptor(fd));
        };
        if state.descriptors.remove(&fd).is_none() {
            return Err(invalid_large_object_descriptor(fd));
        }
        Ok(0)
    }

    pub(crate) fn descriptor(
        &self,
        client_id: ClientId,
        fd: i32,
    ) -> Result<LargeObjectDescriptor, ExecError> {
        self.descriptors
            .read()
            .get(&client_id)
            .and_then(|state| state.descriptors.get(&fd).copied())
            .ok_or_else(|| invalid_large_object_descriptor(fd))
    }

    pub(crate) fn set_descriptor_offset(
        &self,
        client_id: ClientId,
        fd: i32,
        offset: i64,
    ) -> Result<(), ExecError> {
        let mut clients = self.descriptors.write();
        let Some(desc) = clients
            .get_mut(&client_id)
            .and_then(|state| state.descriptors.get_mut(&fd))
        else {
            return Err(invalid_large_object_descriptor(fd));
        };
        desc.offset = offset;
        Ok(())
    }

    pub(crate) fn close_descriptors_for_oid(&self, client_id: ClientId, loid: u32) {
        if let Some(state) = self.descriptors.write().get_mut(&client_id) {
            state.descriptors.retain(|_, desc| desc.loid != loid);
        }
    }

    pub(crate) fn close_all(&self, client_id: ClientId) {
        self.descriptors.write().remove(&client_id);
    }
}

fn invalid_large_object_descriptor(fd: i32) -> ExecError {
    ExecError::DetailedError {
        message: format!("invalid large-object descriptor: {fd}"),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn default_privilege_sql_tokens(sql: &str) -> Vec<String> {
    sql.split_whitespace()
        .map(|token| {
            let trimmed = token.trim_matches(|ch: char| matches!(ch, ';' | ',' | '(' | ')'));
            if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
                trimmed[1..trimmed.len() - 1].replace("\"\"", "\"")
            } else {
                trimmed.to_string()
            }
        })
        .filter(|token| !token.is_empty())
        .collect()
}

fn token_after(tokens: &[String], pattern: &[&str]) -> Option<String> {
    tokens
        .windows(pattern.len().saturating_add(1))
        .find(|window| {
            pattern.iter().enumerate().all(|(idx, expected)| {
                window
                    .get(idx)
                    .is_some_and(|actual| actual.eq_ignore_ascii_case(expected))
            })
        })
        .and_then(|window| window.get(pattern.len()).cloned())
}

fn unsupported_default_privileges(sql: &str) -> ExecError {
    ExecError::Parse(ParseError::FeatureNotSupported(format!(
        "ALTER DEFAULT PRIVILEGES: {sql}"
    )))
}

fn invalid_large_object_privilege_type(privilege: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "invalid privilege type {} for large object",
            privilege.to_ascii_uppercase()
        ),
        detail: None,
        hint: None,
        sqlstate: "42601",
    }
}

fn large_object_default_privilege_chars(tokens: &[String]) -> Result<String, ExecError> {
    let mut chars = BTreeSet::new();
    for token in tokens {
        let lowered = token.to_ascii_lowercase();
        match lowered.as_str() {
            "all" => return Ok(LARGE_OBJECT_ALL_PRIVILEGES.into()),
            "privileges" => {}
            "select" => {
                chars.insert('r');
            }
            "update" => {
                chars.insert('w');
            }
            _ => return Err(invalid_large_object_privilege_type(token)),
        }
    }
    if chars.is_empty() {
        return Err(unsupported_default_privileges(
            "missing large object privilege",
        ));
    }
    let mut privilege_chars = String::new();
    if chars.contains(&'r') {
        privilege_chars.push('r');
    }
    if chars.contains(&'w') {
        privilege_chars.push('w');
    }
    Ok(privilege_chars)
}

pub(crate) fn parse_large_object_default_privileges_sql(
    sql: &str,
) -> Result<Option<LargeObjectDefaultPrivilegeSpec>, ExecError> {
    let tokens = default_privilege_sql_tokens(sql);
    let lowered = tokens
        .iter()
        .map(|token| token.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let Some(on_idx) = lowered.iter().position(|token| token == "on") else {
        return Ok(None);
    };
    if lowered.get(on_idx + 1).map(String::as_str) != Some("large")
        || lowered.get(on_idx + 2).map(String::as_str) != Some("objects")
    {
        return Ok(None);
    }
    if token_after(&tokens, &["in", "schema"]).is_some() {
        return Err(ExecError::DetailedError {
            message: "cannot use IN SCHEMA clause when using GRANT/REVOKE ON LARGE OBJECTS".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        });
    }
    let Some(op_idx) = lowered
        .iter()
        .position(|token| token == "grant" || token == "revoke")
    else {
        return Err(unsupported_default_privileges(sql));
    };
    let revoke = lowered[op_idx] == "revoke";
    let mut privilege_start = op_idx + 1;
    if revoke
        && lowered.get(privilege_start).map(String::as_str) == Some("grant")
        && lowered.get(privilege_start + 1).map(String::as_str) == Some("option")
        && lowered.get(privilege_start + 2).map(String::as_str) == Some("for")
    {
        privilege_start += 3;
    }
    if privilege_start >= on_idx {
        return Err(unsupported_default_privileges(sql));
    }
    let privilege_chars = large_object_default_privilege_chars(&tokens[privilege_start..on_idx])?;
    let grantee_keyword = if revoke { "from" } else { "to" };
    let Some(grantee_idx) = lowered
        .iter()
        .enumerate()
        .skip(on_idx + 3)
        .find_map(|(idx, token)| (token == grantee_keyword).then_some(idx))
    else {
        return Err(unsupported_default_privileges(sql));
    };
    let grantee_end = lowered
        .iter()
        .enumerate()
        .skip(grantee_idx + 1)
        .find_map(|(idx, token)| matches!(token.as_str(), "with" | "granted").then_some(idx))
        .unwrap_or(tokens.len());
    let grantee_names = tokens[grantee_idx + 1..grantee_end]
        .iter()
        .filter(|name| !name.is_empty())
        .cloned()
        .collect::<Vec<_>>();
    if grantee_names.is_empty() {
        return Err(unsupported_default_privileges(sql));
    }
    let with_grant_option = !revoke
        && lowered
            .windows(3)
            .any(|window| window == ["with", "grant", "option"]);
    Ok(Some(LargeObjectDefaultPrivilegeSpec {
        role_name: token_after(&tokens, &["for", "role"]),
        grantee_names,
        privilege_chars,
        revoke,
        with_grant_option,
    }))
}

fn catalog_error(err: CatalogError) -> ExecError {
    ExecError::DetailedError {
        message: format!("catalog access failed: {err:?}"),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    }
}

fn catalog_effect(kinds: &[BootstrapCatalogKind]) -> CatalogMutationEffect {
    let mut effect = CatalogMutationEffect::default();
    for &kind in kinds {
        if !effect.touched_catalogs.contains(&kind) {
            effect.touched_catalogs.push(kind);
        }
    }
    effect
}

fn catalog_write_context(
    db: &Database,
    ctx: &mut ExecutorContext,
) -> Result<CatalogWriteContext, ExecError> {
    let xid = ctx.ensure_write_xid()?;
    Ok(CatalogWriteContext {
        pool: db.pool.clone(),
        txns: db.txns.clone(),
        xid,
        cid: ctx.next_command_id,
        client_id: ctx.client_id,
        waiter: Some(db.txn_waiter.clone()),
        interrupts: ctx.interrupts.clone(),
    })
}

fn txn_catalog_write_context(
    db: &Database,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
) -> CatalogWriteContext {
    CatalogWriteContext {
        pool: db.pool.clone(),
        txns: db.txns.clone(),
        xid,
        cid,
        client_id,
        waiter: Some(db.txn_waiter.clone()),
        interrupts: db.interrupt_state(client_id),
    }
}

fn snapshot_for_txn(
    db: &Database,
    xid: TransactionId,
    cid: CommandId,
) -> Result<Snapshot, ExecError> {
    db.txns
        .read()
        .snapshot_for_command(xid, cid)
        .map_err(|err| ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Mvcc(err)))
}

fn role_name_by_oid(auth_catalog: &AuthCatalog, oid: u32) -> Result<String, ExecError> {
    auth_catalog
        .role_by_oid(oid)
        .map(|row| row.rolname.clone())
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("role with OID {oid} does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42704",
        })
}

fn role_does_not_exist(role_name: &str) -> ExecError {
    ExecError::Parse(role_management_error(format!(
        "role \"{role_name}\" does not exist"
    )))
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
    grantable: bool,
) -> String {
    let mut result = String::new();
    for ch in LARGE_OBJECT_ALL_PRIVILEGES.chars() {
        let present =
            acl_privilege_present(existing_privileges, ch) || added_privileges.contains(ch);
        if !present {
            continue;
        }
        result.push(ch);
        if acl_privilege_grantable(existing_privileges, ch)
            || (grantable && added_privileges.contains(ch))
        {
            result.push('*');
        }
    }
    result
}

fn remove_acl_privileges_with_grant_options(
    existing_privileges: &str,
    removed_privileges: &str,
) -> String {
    let mut result = String::new();
    for ch in LARGE_OBJECT_ALL_PRIVILEGES.chars() {
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

fn acl_grants_privilege_to_names(
    acl: &[String],
    effective_names: &BTreeSet<String>,
    privilege: char,
) -> bool {
    acl.iter().any(|item| {
        parse_acl_item(item)
            .map(|(grantee, privileges, _)| {
                effective_names.contains(&grantee) && acl_privilege_present(&privileges, privilege)
            })
            .unwrap_or(false)
    })
}

fn grant_large_object_acl_entry(
    acl: &mut Vec<String>,
    grantee: &str,
    grantor: &str,
    privilege_chars: &str,
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
        );
        *existing = format!("{grantee}={merged}/{grantor}");
        return;
    }
    acl.push(format!(
        "{grantee}={}/{grantor}",
        canonicalize_acl_privileges_with_grant_options("", privilege_chars, grantable)
    ));
}

fn revoke_large_object_acl_entry(acl: &mut Vec<String>, grantee: &str, privilege_chars: &str) {
    acl.retain_mut(|item| {
        let Some((item_grantee, existing_privileges, grantor)) = parse_acl_item(item) else {
            return true;
        };
        if item_grantee != grantee {
            return true;
        }
        let remaining =
            remove_acl_privileges_with_grant_options(&existing_privileges, privilege_chars);
        if remaining.is_empty() {
            return false;
        }
        *item = format!("{grantee}={remaining}/{grantor}");
        true
    });
}

fn large_object_owner_default_acl(owner_name: &str) -> String {
    format!("{owner_name}={LARGE_OBJECT_ALL_PRIVILEGES}/{owner_name}")
}

fn expand_large_object_acl(row: &PgLargeobjectMetadataRow, owner_name: &str) -> Vec<String> {
    if row.lomacl.is_empty() {
        vec![large_object_owner_default_acl(owner_name)]
    } else {
        row.lomacl.clone()
    }
}

fn collapse_large_object_acl(acl: Vec<String>, owner_name: &str) -> Vec<String> {
    if acl.as_slice() == [large_object_owner_default_acl(owner_name)] {
        Vec::new()
    } else {
        acl
    }
}

fn large_object_privilege_chars(privilege: &GrantObjectPrivilege) -> Result<&str, ExecError> {
    match privilege {
        GrantObjectPrivilege::AllPrivilegesOnLargeObject => Ok(LARGE_OBJECT_ALL_PRIVILEGES),
        GrantObjectPrivilege::SelectOnLargeObject => Ok("r"),
        GrantObjectPrivilege::UpdateOnLargeObject => Ok("w"),
        GrantObjectPrivilege::LargeObjectPrivileges(chars) => Ok(chars.as_str()),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "large object privilege",
            actual: format!("{privilege:?}"),
        })),
    }
}

fn permission_denied_large_object(oid: u32) -> ExecError {
    ExecError::DetailedError {
        message: format!("permission denied for large object {oid}"),
        detail: None,
        hint: None,
        sqlstate: "42501",
    }
}

fn large_object_missing(oid: u32) -> ExecError {
    ExecError::DetailedError {
        message: format!("large object {oid} does not exist"),
        detail: None,
        hint: None,
        sqlstate: "42704",
    }
}

fn transaction_read_only(ctx: &ExecutorContext) -> bool {
    ctx.gucs
        .get("transaction_read_only")
        .is_some_and(|value| value.eq_ignore_ascii_case("on") || value.eq_ignore_ascii_case("true"))
}

pub(crate) fn lo_compat_privileges(ctx: &ExecutorContext) -> bool {
    ctx.gucs
        .get("lo_compat_privileges")
        .is_some_and(|value| matches!(value.to_ascii_lowercase().as_str(), "on" | "true" | "1"))
}

pub(crate) fn ensure_large_object_write_allowed(
    ctx: &ExecutorContext,
    function_name: &str,
) -> Result<(), ExecError> {
    if transaction_read_only(ctx) {
        let function_display = if function_name.ends_with(')') {
            function_name.to_string()
        } else {
            format!("{function_name}()")
        };
        return Err(ExecError::DetailedError {
            message: format!("cannot execute {function_display} in a read-only transaction"),
            detail: None,
            hint: None,
            sqlstate: "25006",
        });
    }
    Ok(())
}

fn large_object_operation_mode(mode: i32) -> Result<(bool, bool), ExecError> {
    let valid = mode & !(INV_READ | INV_WRITE) == 0;
    let can_write = mode & INV_WRITE != 0;
    let can_read = can_write || (mode & INV_READ != 0);
    if !valid || !can_read {
        return Err(ExecError::DetailedError {
            message: "invalid flags for opening a large object".into(),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    Ok((can_read, can_write))
}

fn pageno_for_offset(offset: i64) -> Result<i32, ExecError> {
    if offset < 0 {
        return Err(ExecError::DetailedError {
            message: "negative large object position".into(),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    let pageno = (offset as u64) / (LOBLKSIZE as u64);
    if pageno > i32::MAX as u64 {
        return Err(ExecError::DetailedError {
            message: "large object offset is too large".into(),
            detail: None,
            hint: None,
            sqlstate: "54000",
        });
    }
    Ok(pageno as i32)
}

fn page_offset(offset: i64) -> usize {
    (offset as usize) % LOBLKSIZE
}

fn object_length_from_pages(pages: &[PgLargeobjectRow]) -> i64 {
    pages
        .iter()
        .map(|row| i64::from(row.pageno) * LOBLKSIZE as i64 + row.data.len() as i64)
        .max()
        .unwrap_or(0)
}

fn large_object_read_request_too_large() -> ExecError {
    ExecError::DetailedError {
        message: "large object read request is too large".into(),
        detail: None,
        hint: None,
        sqlstate: "54000",
    }
}

impl Database {
    pub(crate) fn large_object_metadata_row_for_exec(
        &self,
        ctx: &ExecutorContext,
        oid: u32,
    ) -> Result<Option<PgLargeobjectMetadataRow>, ExecError> {
        self.large_object_metadata_row_with_snapshot(ctx.client_id, &ctx.snapshot, oid)
    }

    pub(crate) fn large_object_metadata_row_with_snapshot(
        &self,
        client_id: ClientId,
        snapshot: &Snapshot,
        oid: u32,
    ) -> Result<Option<PgLargeobjectMetadataRow>, ExecError> {
        Ok(self
            .scan_large_object_metadata_rows(client_id, snapshot)?
            .into_iter()
            .find(|row| row.oid == oid))
    }

    fn scan_large_object_metadata_rows(
        &self,
        client_id: ClientId,
        snapshot: &Snapshot,
    ) -> Result<Vec<PgLargeobjectMetadataRow>, ExecError> {
        let kind = BootstrapCatalogKind::PgLargeobjectMetadata;
        let rel = bootstrap_catalog_rel(kind, self.database_oid);
        let desc = bootstrap_relation_desc(kind);
        let mut scan = heap_scan_begin_visible(&self.pool, client_id, rel, snapshot.clone())
            .map_err(ExecError::Heap)?;
        let txns = self.txns.read();
        let mut rows = Vec::new();
        while let Some((_, tuple)) = heap_scan_next_visible(&self.pool, client_id, &txns, &mut scan)
            .map_err(ExecError::Heap)?
        {
            rows.push(
                pg_largeobject_metadata_row_from_values(
                    decode_catalog_tuple_values(&desc, &tuple).map_err(catalog_error)?,
                )
                .map_err(catalog_error)?,
            );
        }
        Ok(rows)
    }

    fn scan_large_object_page_rows(
        &self,
        client_id: ClientId,
        snapshot: &Snapshot,
        oid: u32,
    ) -> Result<Vec<PgLargeobjectRow>, ExecError> {
        let kind = BootstrapCatalogKind::PgLargeobject;
        let rel = bootstrap_catalog_rel(kind, self.database_oid);
        let desc = bootstrap_relation_desc(kind);
        let mut scan = heap_scan_begin_visible(&self.pool, client_id, rel, snapshot.clone())
            .map_err(ExecError::Heap)?;
        let txns = self.txns.read();
        let mut rows = Vec::new();
        while let Some((_, tuple)) = heap_scan_next_visible(&self.pool, client_id, &txns, &mut scan)
            .map_err(ExecError::Heap)?
        {
            let row = pg_largeobject_row_from_values(
                decode_catalog_tuple_values(&desc, &tuple).map_err(catalog_error)?,
            )
            .map_err(catalog_error)?;
            if row.loid == oid {
                rows.push(row);
            }
        }
        rows.sort_by_key(|row| row.pageno);
        Ok(rows)
    }

    pub(crate) fn scan_default_acl_rows(
        &self,
        client_id: ClientId,
        snapshot: &Snapshot,
    ) -> Result<Vec<PgDefaultAclRow>, ExecError> {
        let kind = BootstrapCatalogKind::PgDefaultAcl;
        let rel = bootstrap_catalog_rel(kind, self.database_oid);
        let desc = bootstrap_relation_desc(kind);
        let mut scan = heap_scan_begin_visible(&self.pool, client_id, rel, snapshot.clone())
            .map_err(ExecError::Heap)?;
        let txns = self.txns.read();
        let mut rows = Vec::new();
        while let Some((_, tuple)) = heap_scan_next_visible(&self.pool, client_id, &txns, &mut scan)
            .map_err(ExecError::Heap)?
        {
            rows.push(
                pg_default_acl_row_from_values(
                    decode_catalog_tuple_values(&desc, &tuple).map_err(catalog_error)?,
                )
                .map_err(catalog_error)?,
            );
        }
        Ok(rows)
    }

    fn scan_description_rows_for_large_object(
        &self,
        client_id: ClientId,
        snapshot: &Snapshot,
        oid: u32,
    ) -> Result<Vec<PgDescriptionRow>, ExecError> {
        let kind = BootstrapCatalogKind::PgDescription;
        let rel = bootstrap_catalog_rel(kind, self.database_oid);
        let desc = bootstrap_relation_desc(kind);
        let mut scan = heap_scan_begin_visible(&self.pool, client_id, rel, snapshot.clone())
            .map_err(ExecError::Heap)?;
        let txns = self.txns.read();
        let mut rows = Vec::new();
        while let Some((_, tuple)) = heap_scan_next_visible(&self.pool, client_id, &txns, &mut scan)
            .map_err(ExecError::Heap)?
        {
            let row = pg_description_row_from_values(
                decode_catalog_tuple_values(&desc, &tuple).map_err(catalog_error)?,
            )
            .map_err(catalog_error)?;
            if row.objoid == oid && row.classoid == PG_LARGEOBJECT_RELATION_OID && row.objsubid == 0
            {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    fn default_large_object_acl(
        &self,
        client_id: ClientId,
        snapshot: &Snapshot,
        owner_oid: u32,
    ) -> Result<Vec<String>, ExecError> {
        Ok(self
            .scan_default_acl_rows(client_id, snapshot)?
            .into_iter()
            .find(|row| {
                row.defaclrole == owner_oid
                    && row.defaclnamespace == 0
                    && row.defaclobjtype == DEFAULT_ACL_LARGE_OBJECT
            })
            .and_then(|row| row.defaclacl)
            .unwrap_or_default())
    }

    pub(crate) fn large_object_has_privilege(
        &self,
        ctx: &ExecutorContext,
        role_oid: u32,
        oid: u32,
        privilege: char,
        grant_option: bool,
    ) -> Result<Option<bool>, ExecError> {
        let Some(row) = self.large_object_metadata_row_for_exec(ctx, oid)? else {
            return Ok(None);
        };
        if lo_compat_privileges(ctx) {
            return Ok(Some(true));
        }
        let catalog = ctx
            .catalog
            .as_ref()
            .ok_or_else(|| ExecError::DetailedError {
                message: "catalog unavailable for large object privilege check".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
        let authid_rows = catalog.authid_rows();
        let auth_members_rows = catalog.auth_members_rows();
        if authid_rows
            .iter()
            .find(|role| role.oid == role_oid)
            .is_some_and(|role| role.rolsuper)
            || crate::backend::catalog::role_memberships::has_effective_membership(
                role_oid,
                row.lomowner,
                &authid_rows,
                &auth_members_rows,
            )
        {
            return Ok(Some(true));
        }
        let effective_names = {
            let mut names = BTreeSet::from([String::new()]);
            for role in &authid_rows {
                if crate::backend::catalog::role_memberships::has_effective_membership(
                    role_oid,
                    role.oid,
                    &authid_rows,
                    &auth_members_rows,
                ) || role.oid == role_oid
                {
                    names.insert(role.rolname.clone());
                }
            }
            names
        };
        Ok(Some(row.lomacl.iter().any(|item| {
            parse_acl_item(item)
                .map(|(grantee, privileges, _)| {
                    effective_names.contains(&grantee)
                        && if grant_option {
                            acl_privilege_grantable(&privileges, privilege)
                        } else {
                            acl_privilege_present(&privileges, privilege)
                        }
                })
                .unwrap_or(false)
        })))
    }

    pub(crate) fn ensure_large_object_privilege(
        &self,
        ctx: &ExecutorContext,
        oid: u32,
        privilege: char,
    ) -> Result<(), ExecError> {
        if lo_compat_privileges(ctx) {
            if self.large_object_metadata_row_for_exec(ctx, oid)?.is_some() {
                return Ok(());
            }
            return Err(large_object_missing(oid));
        }
        match self.large_object_has_privilege(ctx, ctx.current_user_oid, oid, privilege, false)? {
            Some(true) => Ok(()),
            Some(false) => Err(permission_denied_large_object(oid)),
            None => Err(large_object_missing(oid)),
        }
    }

    pub(crate) fn large_object_open(
        &self,
        ctx: &mut ExecutorContext,
        oid: u32,
        mode: i32,
    ) -> Result<i32, ExecError> {
        let (can_read, can_write) = large_object_operation_mode(mode)?;
        if can_write {
            ensure_large_object_write_allowed(ctx, "lo_open(INV_WRITE)")?;
        }
        if can_read && !lo_compat_privileges(ctx) {
            let privilege = if can_write { 'w' } else { 'r' };
            self.ensure_large_object_privilege(ctx, oid, privilege)?;
        } else if self.large_object_metadata_row_for_exec(ctx, oid)?.is_none() {
            return Err(large_object_missing(oid));
        }
        let runtime = ctx
            .large_objects
            .as_ref()
            .ok_or_else(|| ExecError::DetailedError {
                message: "large object runtime unavailable".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
        Ok(runtime.open_descriptor(ctx.client_id, oid, can_read, can_write))
    }

    pub(crate) fn large_object_create(
        &self,
        ctx: &mut ExecutorContext,
        requested_oid: u32,
        owner_oid: u32,
        initial_data: Option<&[u8]>,
    ) -> Result<u32, ExecError> {
        ensure_large_object_write_allowed(ctx, "lo_create")?;
        let oid = self
            .catalog
            .write()
            .allocate_next_oid(requested_oid)
            .map_err(catalog_error)?;
        if self.large_object_metadata_row_for_exec(ctx, oid)?.is_some() {
            return Err(ExecError::DetailedError {
                message: format!("large object {oid} already exists"),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }
        let default_acl = self.default_large_object_acl(ctx.client_id, &ctx.snapshot, owner_oid)?;
        let write_ctx = catalog_write_context(self, ctx)?;
        let mut insert_rows = PhysicalCatalogRows {
            largeobject_metadata: vec![PgLargeobjectMetadataRow {
                oid,
                lomowner: owner_oid,
                lomacl: default_acl,
            }],
            ..PhysicalCatalogRows::default()
        };
        let mut kinds = vec![BootstrapCatalogKind::PgLargeobjectMetadata];
        if let Some(data) = initial_data {
            insert_rows.largeobjects = page_rows_from_bytes(oid, data)?;
            kinds.push(BootstrapCatalogKind::PgLargeobject);
        }
        insert_catalog_rows_subset_mvcc(&write_ctx, &insert_rows, self.database_oid, &kinds)
            .map_err(catalog_error)?;
        ctx.record_catalog_effect(catalog_effect(&kinds));
        Ok(oid)
    }

    pub(crate) fn large_object_unlink(
        &self,
        ctx: &mut ExecutorContext,
        oid: u32,
    ) -> Result<i32, ExecError> {
        ensure_large_object_write_allowed(ctx, "lo_unlink")?;
        let row = self
            .large_object_metadata_row_for_exec(ctx, oid)?
            .ok_or_else(|| large_object_missing(oid))?;
        if !lo_compat_privileges(ctx) {
            let auth_catalog = self
                .auth_catalog(
                    ctx.client_id,
                    ctx.transaction_xid()
                        .map(|xid| (xid, ctx.snapshot.current_cid)),
                )
                .map_err(catalog_error)?;
            let current = auth_catalog.role_by_oid(ctx.current_user_oid);
            if !current.is_some_and(|role| role.rolsuper)
                && !self
                    .auth_state(ctx.client_id)
                    .has_effective_membership(row.lomowner, &auth_catalog)
            {
                return Err(permission_denied_large_object(oid));
            }
        }
        let pages = self.scan_large_object_page_rows(ctx.client_id, &ctx.snapshot, oid)?;
        let descriptions =
            self.scan_description_rows_for_large_object(ctx.client_id, &ctx.snapshot, oid)?;
        let delete_rows = PhysicalCatalogRows {
            largeobject_metadata: vec![row],
            largeobjects: pages,
            descriptions,
            ..PhysicalCatalogRows::default()
        };
        let kinds = [
            BootstrapCatalogKind::PgLargeobjectMetadata,
            BootstrapCatalogKind::PgLargeobject,
            BootstrapCatalogKind::PgDescription,
        ];
        let write_ctx = catalog_write_context(self, ctx)?;
        delete_catalog_rows_subset_mvcc(&write_ctx, &delete_rows, self.database_oid, &kinds)
            .map_err(catalog_error)?;
        ctx.record_catalog_effect(catalog_effect(&kinds));
        if let Some(runtime) = &ctx.large_objects {
            runtime.close_descriptors_for_oid(ctx.client_id, oid);
        }
        Ok(1)
    }

    pub(crate) fn large_object_read(
        &self,
        ctx: &ExecutorContext,
        oid: u32,
        offset: i64,
        len: i32,
    ) -> Result<Vec<u8>, ExecError> {
        if len < 0 {
            return Err(ExecError::DetailedError {
                message: "negative large object read length".into(),
                detail: None,
                hint: None,
                sqlstate: "22023",
            });
        }
        self.ensure_large_object_privilege(ctx, oid, 'r')?;
        let object_len = self.large_object_length(ctx, oid)?;
        let available = object_len.saturating_sub(offset);
        let len = (len as i64).min(available).max(0) as usize;
        let start_page = pageno_for_offset(offset)?;
        let end = offset
            .checked_add(len as i64)
            .ok_or_else(large_object_read_request_too_large)?;
        let end_page = if len == 0 {
            start_page
        } else {
            pageno_for_offset(end - 1)?
        };
        let pages = self.scan_large_object_page_rows(ctx.client_id, &ctx.snapshot, oid)?;
        let by_page = pages
            .into_iter()
            .map(|row| (row.pageno, row.data))
            .collect::<BTreeMap<_, _>>();
        let mut out = vec![0; len];
        for pageno in start_page..=end_page {
            let Some(page) = by_page.get(&pageno) else {
                continue;
            };
            let page_start = i64::from(pageno) * LOBLKSIZE as i64;
            let copy_start = offset.max(page_start);
            let copy_end = end.min(page_start + page.len() as i64);
            if copy_end <= copy_start {
                continue;
            }
            let dst = (copy_start - offset) as usize;
            let src = (copy_start - page_start) as usize;
            let count = (copy_end - copy_start) as usize;
            out[dst..dst + count].copy_from_slice(&page[src..src + count]);
        }
        Ok(out)
    }

    pub(crate) fn large_object_write(
        &self,
        ctx: &mut ExecutorContext,
        oid: u32,
        offset: i64,
        data: &[u8],
    ) -> Result<i32, ExecError> {
        ensure_large_object_write_allowed(ctx, "lowrite")?;
        self.ensure_large_object_privilege(ctx, oid, 'w')?;
        if data.is_empty() {
            return Ok(0);
        }
        let start_page = pageno_for_offset(offset)?;
        let end =
            offset
                .checked_add(data.len() as i64)
                .ok_or_else(|| ExecError::DetailedError {
                    message: "large object write request is too large".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "54000",
                })?;
        let end_page = pageno_for_offset(end - 1)?;
        let old_pages = self.scan_large_object_page_rows(ctx.client_id, &ctx.snapshot, oid)?;
        let old_pages_by_page = old_pages
            .into_iter()
            .map(|row| (row.pageno, row))
            .collect::<BTreeMap<_, _>>();
        let mut changed_pages = old_pages_by_page
            .iter()
            .filter(|(pageno, _)| **pageno >= start_page && **pageno <= end_page)
            .map(|(pageno, row)| (*pageno, row.data.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut data_offset = 0usize;
        for pageno in start_page..=end_page {
            let page_start = i64::from(pageno) * LOBLKSIZE as i64;
            let write_start = offset.max(page_start);
            let write_end = end.min(page_start + LOBLKSIZE as i64);
            let page = changed_pages.entry(pageno).or_default();
            let required = (write_end - page_start) as usize;
            if page.len() < required {
                page.resize(required, 0);
            }
            let dst = (write_start - page_start) as usize;
            let count = (write_end - write_start) as usize;
            page[dst..dst + count].copy_from_slice(&data[data_offset..data_offset + count]);
            data_offset += count;
        }
        let old_pages = old_pages_by_page
            .into_iter()
            .filter(|(pageno, _)| *pageno >= start_page && *pageno <= end_page)
            .map(|(_, row)| row)
            .collect::<Vec<_>>();
        let new_pages = changed_pages
            .into_iter()
            .map(|(pageno, data)| PgLargeobjectRow {
                loid: oid,
                pageno,
                data,
            })
            .collect::<Vec<_>>();
        self.replace_large_object_page_subset(ctx, oid, old_pages, new_pages)?;
        Ok(data.len() as i32)
    }

    pub(crate) fn large_object_length(
        &self,
        ctx: &ExecutorContext,
        oid: u32,
    ) -> Result<i64, ExecError> {
        if self.large_object_metadata_row_for_exec(ctx, oid)?.is_none() {
            return Err(large_object_missing(oid));
        }
        Ok(object_length_from_pages(
            &self.scan_large_object_page_rows(ctx.client_id, &ctx.snapshot, oid)?,
        ))
    }

    pub(crate) fn large_object_truncate(
        &self,
        ctx: &mut ExecutorContext,
        oid: u32,
        len: i64,
    ) -> Result<(), ExecError> {
        ensure_large_object_write_allowed(ctx, "lo_truncate")?;
        if len < 0 {
            return Err(ExecError::DetailedError {
                message: "negative large object truncate length".into(),
                detail: None,
                hint: None,
                sqlstate: "22023",
            });
        }
        self.ensure_large_object_privilege(ctx, oid, 'w')?;
        let old_pages = self.scan_large_object_page_rows(ctx.client_id, &ctx.snapshot, oid)?;
        let mut by_page = old_pages
            .iter()
            .cloned()
            .map(|row| (row.pageno, row.data))
            .collect::<BTreeMap<_, _>>();
        if len == 0 {
            by_page.clear();
        } else {
            let last_page = pageno_for_offset(len - 1)?;
            let last_len = page_offset(len - 1) + 1;
            by_page.retain(|pageno, _| *pageno <= last_page);
            let page = by_page.entry(last_page).or_default();
            page.resize(last_len, 0);
        }
        let new_pages = by_page
            .into_iter()
            .map(|(pageno, data)| PgLargeobjectRow {
                loid: oid,
                pageno,
                data,
            })
            .collect::<Vec<_>>();
        self.replace_large_object_pages(ctx, oid, old_pages, new_pages)
    }

    fn replace_large_object_page_subset(
        &self,
        ctx: &mut ExecutorContext,
        oid: u32,
        old_pages: Vec<PgLargeobjectRow>,
        new_pages: Vec<PgLargeobjectRow>,
    ) -> Result<(), ExecError> {
        if old_pages.is_empty() && new_pages.is_empty() {
            return Ok(());
        }
        let delete_rows = PhysicalCatalogRows {
            largeobjects: old_pages,
            ..PhysicalCatalogRows::default()
        };
        let insert_rows = PhysicalCatalogRows {
            largeobjects: new_pages,
            ..PhysicalCatalogRows::default()
        };
        let kind = [BootstrapCatalogKind::PgLargeobject];
        let write_ctx = catalog_write_context(self, ctx)?;
        delete_catalog_rows_subset_mvcc(&write_ctx, &delete_rows, self.database_oid, &kind)
            .map_err(catalog_error)?;
        insert_catalog_rows_subset_mvcc(&write_ctx, &insert_rows, self.database_oid, &kind)
            .map_err(catalog_error)?;
        let mut effect = catalog_effect(&kind);
        effect.relation_oids.push(oid);
        ctx.record_catalog_effect(effect);
        Ok(())
    }

    fn replace_large_object_pages(
        &self,
        ctx: &mut ExecutorContext,
        oid: u32,
        old_pages: Vec<PgLargeobjectRow>,
        new_pages: Vec<PgLargeobjectRow>,
    ) -> Result<(), ExecError> {
        let delete_rows = PhysicalCatalogRows {
            largeobjects: old_pages,
            ..PhysicalCatalogRows::default()
        };
        let insert_rows = PhysicalCatalogRows {
            largeobjects: new_pages,
            ..PhysicalCatalogRows::default()
        };
        let kind = [BootstrapCatalogKind::PgLargeobject];
        let write_ctx = catalog_write_context(self, ctx)?;
        delete_catalog_rows_subset_mvcc(&write_ctx, &delete_rows, self.database_oid, &kind)
            .map_err(catalog_error)?;
        insert_catalog_rows_subset_mvcc(&write_ctx, &insert_rows, self.database_oid, &kind)
            .map_err(catalog_error)?;
        let mut effect = catalog_effect(&kind);
        effect.relation_oids.push(oid);
        ctx.record_catalog_effect(effect);
        Ok(())
    }

    pub(crate) fn large_object_seek(
        &self,
        ctx: &ExecutorContext,
        desc: LargeObjectDescriptor,
        offset: i64,
        whence: i32,
    ) -> Result<i64, ExecError> {
        let base = match whence {
            0 => 0,
            1 => desc.offset,
            2 => self.large_object_length(ctx, desc.loid)?,
            _ => {
                return Err(ExecError::DetailedError {
                    message: "invalid large object seek whence".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "22023",
                });
            }
        };
        let new_offset = base
            .checked_add(offset)
            .ok_or_else(|| ExecError::DetailedError {
                message: "large object seek position is too large".into(),
                detail: None,
                hint: None,
                sqlstate: "54000",
            })?;
        if new_offset < 0 {
            return Err(ExecError::DetailedError {
                message: "negative large object seek position".into(),
                detail: None,
                hint: None,
                sqlstate: "22023",
            });
        }
        Ok(new_offset)
    }

    pub(crate) fn large_object_get(
        &self,
        ctx: &ExecutorContext,
        oid: u32,
        offset: Option<i64>,
        len: Option<i32>,
    ) -> Result<Vec<u8>, ExecError> {
        self.ensure_large_object_privilege(ctx, oid, 'r')?;
        let length = self.large_object_length(ctx, oid)?;
        let offset = offset.unwrap_or(0);
        if offset < 0 {
            return Err(large_object_read_request_too_large());
        }
        let available = length.saturating_sub(offset);
        let len = match len {
            Some(len) if len >= 0 => len,
            Some(_) => return Err(large_object_read_request_too_large()),
            None => {
                if available > i64::from(i32::MAX) {
                    return Err(large_object_read_request_too_large());
                }
                available as i32
            }
        };
        self.large_object_read(ctx, oid, offset, len)
    }

    pub(crate) fn large_object_put(
        &self,
        ctx: &mut ExecutorContext,
        oid: u32,
        offset: i64,
        data: &[u8],
    ) -> Result<(), ExecError> {
        self.large_object_write(ctx, oid, offset, data)?;
        Ok(())
    }

    pub(crate) fn execute_grant_large_object_acl_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &GrantObjectStatement,
        _configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_large_object_acl_stmt_in_transaction(
            client_id,
            stmt.privilege.clone(),
            stmt.named_object_names(),
            &stmt.grantee_names,
            stmt.with_grant_option,
            xid,
            0,
            &mut catalog_effects,
            false,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_revoke_large_object_acl_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &RevokeObjectStatement,
        _configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_large_object_acl_stmt_in_transaction(
            client_id,
            stmt.privilege.clone(),
            stmt.named_object_names(),
            &stmt.grantee_names,
            false,
            xid,
            0,
            &mut catalog_effects,
            true,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_large_object_acl_stmt_in_transaction(
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
    ) -> Result<StatementResult, ExecError> {
        let privilege_chars = large_object_privilege_chars(&privilege)?;
        let auth = self.auth_state(client_id);
        let mut current_cid = cid;
        for object_name in object_names {
            let oid = object_name.parse::<u32>().map_err(|_| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "large object oid",
                    actual: object_name.clone(),
                })
            })?;
            let snapshot = snapshot_for_txn(self, xid, current_cid)?;
            let mut row = self
                .large_object_metadata_row_with_snapshot(client_id, &snapshot, oid)?
                .ok_or_else(|| large_object_missing(oid))?;
            let auth_catalog = self
                .auth_catalog(client_id, Some((xid, current_cid)))
                .map_err(catalog_error)?;
            let current_user_can_grant_as_owner = auth_catalog
                .role_by_oid(auth.current_user_oid())
                .is_some_and(|row| row.rolsuper)
                || auth.has_effective_membership(row.lomowner, &auth_catalog);
            let owner_name = role_name_by_oid(&auth_catalog, row.lomowner)?;
            let grantor_name = role_name_by_oid(&auth_catalog, auth.current_user_oid())?;
            let mut acl = expand_large_object_acl(&row, &owner_name);
            if !current_user_can_grant_as_owner
                && !acl_grants_all_options(
                    &acl,
                    &effective_acl_grantee_names(&auth, &auth_catalog),
                    privilege_chars,
                )
            {
                return Err(permission_denied_large_object(oid));
            }
            for grantee_name in grantee_names {
                let grantee_acl_name = if grantee_name.eq_ignore_ascii_case("public") {
                    String::new()
                } else {
                    auth_catalog
                        .role_by_name(grantee_name)
                        .map(|row| row.rolname.clone())
                        .ok_or_else(|| role_does_not_exist(grantee_name))?
                };
                if revoke {
                    revoke_large_object_acl_entry(&mut acl, &grantee_acl_name, privilege_chars);
                } else {
                    grant_large_object_acl_entry(
                        &mut acl,
                        &grantee_acl_name,
                        &grantor_name,
                        privilege_chars,
                        with_grant_option,
                    );
                }
            }
            let old_row = row.clone();
            row.lomacl = collapse_large_object_acl(acl, &owner_name);
            let delete_rows = PhysicalCatalogRows {
                largeobject_metadata: vec![old_row],
                ..PhysicalCatalogRows::default()
            };
            let insert_rows = PhysicalCatalogRows {
                largeobject_metadata: vec![row],
                ..PhysicalCatalogRows::default()
            };
            let kind = [BootstrapCatalogKind::PgLargeobjectMetadata];
            let write_ctx = txn_catalog_write_context(self, client_id, xid, current_cid);
            delete_catalog_rows_subset_mvcc(&write_ctx, &delete_rows, self.database_oid, &kind)
                .map_err(catalog_error)?;
            insert_catalog_rows_subset_mvcc(&write_ctx, &insert_rows, self.database_oid, &kind)
                .map_err(catalog_error)?;
            catalog_effects.push(catalog_effect(&kind));
            current_cid = current_cid.saturating_add(1);
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_large_object_owner_stmt(
        &self,
        client_id: ClientId,
        oid: u32,
        new_owner_name: &str,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_large_object_owner_stmt_in_transaction(
            client_id,
            oid,
            new_owner_name,
            xid,
            0,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_large_object_owner_stmt_in_transaction(
        &self,
        client_id: ClientId,
        oid: u32,
        new_owner_name: &str,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let snapshot = snapshot_for_txn(self, xid, cid)?;
        let mut row = self
            .large_object_metadata_row_with_snapshot(client_id, &snapshot, oid)?
            .ok_or_else(|| large_object_missing(oid))?;
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(catalog_error)?;
        if !auth.has_effective_membership(row.lomowner, &auth_catalog) {
            return Err(permission_denied_large_object(oid));
        }
        let new_owner = if new_owner_name.eq_ignore_ascii_case("current_user") {
            auth_catalog
                .role_by_oid(auth.current_user_oid())
                .cloned()
                .ok_or_else(|| role_does_not_exist(new_owner_name))?
        } else {
            auth_catalog
                .role_by_name(new_owner_name)
                .cloned()
                .ok_or_else(|| role_does_not_exist(new_owner_name))?
        };
        ensure_can_set_role(self, client_id, new_owner.oid, &new_owner.rolname)?;
        let old_row = row.clone();
        row.lomowner = new_owner.oid;
        row.lomacl.clear();
        let kind = [BootstrapCatalogKind::PgLargeobjectMetadata];
        let write_ctx = txn_catalog_write_context(self, client_id, xid, cid);
        delete_catalog_rows_subset_mvcc(
            &write_ctx,
            &PhysicalCatalogRows {
                largeobject_metadata: vec![old_row],
                ..PhysicalCatalogRows::default()
            },
            self.database_oid,
            &kind,
        )
        .map_err(catalog_error)?;
        insert_catalog_rows_subset_mvcc(
            &write_ctx,
            &PhysicalCatalogRows {
                largeobject_metadata: vec![row],
                ..PhysicalCatalogRows::default()
            },
            self.database_oid,
            &kind,
        )
        .map_err(catalog_error)?;
        catalog_effects.push(catalog_effect(&kind));
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_comment_on_large_object_stmt(
        &self,
        client_id: ClientId,
        oid: u32,
        comment: Option<&str>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_large_object_stmt_in_transaction(
            client_id,
            oid,
            comment,
            xid,
            0,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_comment_on_large_object_stmt_in_transaction(
        &self,
        client_id: ClientId,
        oid: u32,
        comment: Option<&str>,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let snapshot = snapshot_for_txn(self, xid, cid)?;
        let row = self
            .large_object_metadata_row_with_snapshot(client_id, &snapshot, oid)?
            .ok_or_else(|| large_object_missing(oid))?;
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(catalog_error)?;
        if !auth.has_effective_membership(row.lomowner, &auth_catalog) {
            return Err(permission_denied_large_object(oid));
        }
        let existing = self.scan_description_rows_for_large_object(client_id, &snapshot, oid)?;
        let normalized = comment.and_then(|text| (!text.is_empty()).then_some(text));
        let delete_rows = PhysicalCatalogRows {
            descriptions: existing,
            ..PhysicalCatalogRows::default()
        };
        let insert_rows = PhysicalCatalogRows {
            descriptions: normalized
                .map(|description| {
                    vec![PgDescriptionRow {
                        objoid: oid,
                        classoid: PG_LARGEOBJECT_RELATION_OID,
                        objsubid: 0,
                        description: description.to_string(),
                    }]
                })
                .unwrap_or_default(),
            ..PhysicalCatalogRows::default()
        };
        let kind = [BootstrapCatalogKind::PgDescription];
        let write_ctx = txn_catalog_write_context(self, client_id, xid, cid);
        delete_catalog_rows_subset_mvcc(&write_ctx, &delete_rows, self.database_oid, &kind)
            .map_err(catalog_error)?;
        insert_catalog_rows_subset_mvcc(&write_ctx, &insert_rows, self.database_oid, &kind)
            .map_err(catalog_error)?;
        catalog_effects.push(catalog_effect(&kind));
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_default_privileges_large_objects(
        &self,
        client_id: ClientId,
        role_oid: u32,
        grantee_names: &[String],
        privilege_chars: &str,
        with_grant_option: bool,
        revoke: bool,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let snapshot = snapshot_for_txn(self, xid, cid)?;
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(catalog_error)?;
        let owner_name = role_name_by_oid(&auth_catalog, role_oid)?;
        let grantor_name =
            role_name_by_oid(&auth_catalog, self.auth_state(client_id).current_user_oid())?;
        let rows = self.scan_default_acl_rows(client_id, &snapshot)?;
        let existing = rows
            .iter()
            .find(|row| {
                row.defaclrole == role_oid
                    && row.defaclnamespace == 0
                    && row.defaclobjtype == DEFAULT_ACL_LARGE_OBJECT
            })
            .cloned();
        let mut acl = existing
            .as_ref()
            .and_then(|row| row.defaclacl.clone())
            .unwrap_or_else(|| vec![large_object_owner_default_acl(&owner_name)]);
        for grantee_name in grantee_names {
            let grantee_acl_name = if grantee_name.eq_ignore_ascii_case("public") {
                String::new()
            } else {
                auth_catalog
                    .role_by_name(grantee_name)
                    .map(|row| row.rolname.clone())
                    .ok_or_else(|| role_does_not_exist(grantee_name))?
            };
            if revoke {
                revoke_large_object_acl_entry(&mut acl, &grantee_acl_name, privilege_chars);
            } else {
                grant_large_object_acl_entry(
                    &mut acl,
                    &grantee_acl_name,
                    &grantor_name,
                    privilege_chars,
                    with_grant_option,
                );
            }
        }
        let collapsed = collapse_large_object_acl(acl, &owner_name);
        let delete_rows = PhysicalCatalogRows {
            default_acls: existing.iter().cloned().collect(),
            ..PhysicalCatalogRows::default()
        };
        let insert = if collapsed.is_empty() {
            Vec::new()
        } else {
            let oid = match existing.as_ref().map(|row| row.oid) {
                Some(oid) => oid,
                None => self
                    .catalog
                    .write()
                    .allocate_next_oid(0)
                    .map_err(catalog_error)?,
            };
            vec![PgDefaultAclRow {
                oid,
                defaclrole: role_oid,
                defaclnamespace: 0,
                defaclobjtype: DEFAULT_ACL_LARGE_OBJECT,
                defaclacl: Some(collapsed),
            }]
        };
        let insert_rows = PhysicalCatalogRows {
            default_acls: insert,
            ..PhysicalCatalogRows::default()
        };
        let kind = [BootstrapCatalogKind::PgDefaultAcl];
        let write_ctx = txn_catalog_write_context(self, client_id, xid, cid);
        delete_catalog_rows_subset_mvcc(&write_ctx, &delete_rows, self.database_oid, &kind)
            .map_err(catalog_error)?;
        insert_catalog_rows_subset_mvcc(&write_ctx, &insert_rows, self.database_oid, &kind)
            .map_err(catalog_error)?;
        catalog_effects.push(catalog_effect(&kind));
        Ok(())
    }
}

fn page_rows_from_bytes(oid: u32, bytes: &[u8]) -> Result<Vec<PgLargeobjectRow>, ExecError> {
    bytes
        .chunks(LOBLKSIZE)
        .enumerate()
        .map(|(pageno, chunk)| {
            if pageno > i32::MAX as usize {
                return Err(ExecError::DetailedError {
                    message: "large object value is too large".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "54000",
                });
            }
            Ok(PgLargeobjectRow {
                loid: oid,
                pageno: pageno as i32,
                data: chunk.to_vec(),
            })
        })
        .collect()
}
