use super::super::*;
use crate::backend::catalog::roles::find_role_by_name;
use crate::backend::commands::rolecmds::{membership_row, role_management_error};
use crate::backend::parser::{
    CatalogLookup, GrantObjectPrivilege, GrantObjectStatement, GrantRoleMembershipStatement,
    ParseError, RevokeObjectStatement, RevokeRoleMembershipStatement, RoleGrantorSpec,
    parse_type_name, resolve_raw_type_name,
};
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, CURRENT_DATABASE_NAME, CURRENT_DATABASE_OID, PgAuthIdRow,
};
use std::collections::{BTreeSet, VecDeque};

const TABLE_ALL_PRIVILEGE_CHARS: &str = "arwdDxtm";
const TABLE_SELECT_PRIVILEGE_CHARS: &str = "r";
const SCHEMA_ALL_PRIVILEGE_CHARS: &str = "UC";
const SCHEMA_USAGE_PRIVILEGE_CHARS: &str = "U";
const TYPE_USAGE_PRIVILEGE_CHARS: &str = "U";
const FUNCTION_EXECUTE_PRIVILEGE_CHARS: &str = "X";

fn table_privilege_chars(privilege: GrantObjectPrivilege) -> Option<&'static str> {
    match privilege {
        GrantObjectPrivilege::AllPrivilegesOnTable => Some(TABLE_ALL_PRIVILEGE_CHARS),
        GrantObjectPrivilege::SelectOnTable => Some(TABLE_SELECT_PRIVILEGE_CHARS),
        _ => None,
    }
}

fn object_privilege_chars(privilege: GrantObjectPrivilege) -> Option<&'static str> {
    match privilege {
        GrantObjectPrivilege::AllPrivilegesOnSchema => Some(SCHEMA_ALL_PRIVILEGE_CHARS),
        GrantObjectPrivilege::UsageOnSchema => Some(SCHEMA_USAGE_PRIVILEGE_CHARS),
        GrantObjectPrivilege::UsageOnType => Some(TYPE_USAGE_PRIVILEGE_CHARS),
        GrantObjectPrivilege::ExecuteOnFunction => Some(FUNCTION_EXECUTE_PRIVILEGE_CHARS),
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

fn grant_table_acl_entry(
    acl: &mut Vec<String>,
    grantee: &str,
    grantor: &str,
    privilege_chars: &str,
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
            TABLE_ALL_PRIVILEGE_CHARS,
        );
        *existing = format!("{grantee}={merged}/{grantor}");
        return;
    }
    acl.push(format!(
        "{grantee}={}/{grantor}",
        canonicalize_acl_privileges(privilege_chars, TABLE_ALL_PRIVILEGE_CHARS)
    ));
}

fn revoke_table_acl_entry(acl: &mut Vec<String>, grantee: &str, privilege_chars: &str) {
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
        let remaining = canonicalize_acl_privileges(&remaining, TABLE_ALL_PRIVILEGE_CHARS);
        if remaining.is_empty() {
            return false;
        }
        *item = format!("{grantee}={remaining}/{grantor}");
        true
    });
}

fn collapse_relation_acl_defaults(
    acl: Vec<String>,
    owner_name: &str,
    relkind: char,
) -> Option<Vec<String>> {
    let default_owner = table_owner_default_acl(owner_name, relkind)?;
    match acl.as_slice() {
        [] => None,
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

fn type_owner_default_acl(owner_name: &str) -> Vec<String> {
    vec![
        format!("{owner_name}={TYPE_USAGE_PRIVILEGE_CHARS}/{owner_name}"),
        format!("={TYPE_USAGE_PRIVILEGE_CHARS}/{owner_name}"),
    ]
}

fn function_owner_default_acl(owner_name: &str) -> Vec<String> {
    vec![
        format!("{owner_name}={FUNCTION_EXECUTE_PRIVILEGE_CHARS}/{owner_name}"),
        format!("={FUNCTION_EXECUTE_PRIVILEGE_CHARS}/{owner_name}"),
    ]
}

fn collapse_acl_defaults(acl: Vec<String>, defaults: &[String]) -> Option<Vec<String>> {
    if acl.is_empty() || acl == defaults {
        None
    } else {
        Some(acl)
    }
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
    let desired_arg_oids = arg_names
        .into_iter()
        .map(|arg| {
            let raw_type = parse_type_name(arg)?;
            let sql_type = resolve_raw_type_name(&raw_type, &catalog)?;
            catalog
                .type_oid_for_sql_type(sql_type)
                .ok_or_else(|| ParseError::UnsupportedType(arg.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(ExecError::Parse)?;
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
            message: format!("function {signature} does not exist"),
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
) -> Result<crate::include::catalog::PgProcRow, ExecError> {
    let catalog = db.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
    let (proc_name, arg_names) =
        parse_granted_function_signature(signature).map_err(ExecError::Parse)?;
    let (schema_name, base_name) = proc_name
        .rsplit_once('.')
        .map(|(schema, name)| (Some(schema.trim().to_ascii_lowercase()), name.trim()))
        .unwrap_or((None, proc_name));
    let desired_arg_oids = arg_names
        .into_iter()
        .map(|arg| {
            let raw_type = parse_type_name(arg)?;
            let sql_type = resolve_raw_type_name(&raw_type, &catalog)?;
            catalog
                .type_oid_for_sql_type(sql_type)
                .ok_or_else(|| ParseError::UnsupportedType(arg.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(ExecError::Parse)?;
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
    catalog
        .proc_rows_by_name(&normalized_name)
        .into_iter()
        .find(|row| {
            parse_proc_argtype_oids(&row.proargtypes) == Some(desired_arg_oids.clone())
                && schema_oid
                    .map(|schema_oid| row.pronamespace == schema_oid)
                    .unwrap_or(true)
        })
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("function {signature} does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42883",
        })
}

impl Database {
    pub(crate) fn execute_grant_object_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &GrantObjectStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        match stmt.privilege {
            GrantObjectPrivilege::CreateOnDatabase => {
                self.execute_grant_database_create_stmt(client_id, stmt)
            }
            GrantObjectPrivilege::AllPrivilegesOnTable | GrantObjectPrivilege::SelectOnTable => {
                self.execute_grant_table_acl_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
                )
            }
            GrantObjectPrivilege::AllPrivilegesOnSchema | GrantObjectPrivilege::UsageOnSchema => {
                self.execute_grant_schema_acl_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
                )
            }
            GrantObjectPrivilege::UsageOnType => self.execute_grant_type_acl_stmt_with_search_path(
                client_id,
                stmt,
                configured_search_path,
            ),
            GrantObjectPrivilege::ExecuteOnFunction => self
                .execute_grant_function_acl_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
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
        match stmt.privilege {
            GrantObjectPrivilege::CreateOnDatabase => {
                self.execute_grant_database_create_stmt(client_id, stmt)
            }
            GrantObjectPrivilege::AllPrivilegesOnTable | GrantObjectPrivilege::SelectOnTable => {
                self.execute_grant_table_acl_stmt_in_transaction_with_search_path(
                    client_id,
                    stmt,
                    xid,
                    cid,
                    configured_search_path,
                    catalog_effects,
                )
            }
            GrantObjectPrivilege::AllPrivilegesOnSchema | GrantObjectPrivilege::UsageOnSchema => {
                self.execute_schema_acl_stmt_in_transaction_with_search_path(
                    client_id,
                    stmt.privilege.clone(),
                    &stmt.object_names,
                    &stmt.grantee_names,
                    xid,
                    cid,
                    configured_search_path,
                    catalog_effects,
                    false,
                )
            }
            GrantObjectPrivilege::UsageOnType => {
                self.execute_grant_type_acl_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
                )
            }
            GrantObjectPrivilege::ExecuteOnFunction => {
                self.execute_grant_function_acl_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
                )
            }
        }
    }

    pub(crate) fn execute_revoke_object_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &RevokeObjectStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        match stmt.privilege {
            GrantObjectPrivilege::CreateOnDatabase => {
                self.execute_revoke_database_create_stmt(client_id, stmt)
            }
            GrantObjectPrivilege::AllPrivilegesOnTable | GrantObjectPrivilege::SelectOnTable => {
                self.execute_revoke_table_acl_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
                )
            }
            GrantObjectPrivilege::AllPrivilegesOnSchema | GrantObjectPrivilege::UsageOnSchema => {
                self.execute_revoke_schema_acl_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
                )
            }
            GrantObjectPrivilege::UsageOnType => self
                .execute_revoke_type_acl_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
                ),
            GrantObjectPrivilege::ExecuteOnFunction => self
                .execute_revoke_function_acl_stmt_with_search_path(
                    client_id,
                    stmt,
                    configured_search_path,
                ),
        }
    }

    fn execute_revoke_type_usage_stmt(
        &self,
        client_id: ClientId,
        stmt: &RevokeObjectStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let type_name = single_object_name(&stmt.object_names, "single type name")?;
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
        let object_name = single_object_name(&stmt.object_names, "single table name")?;
        let privilege_chars = table_privilege_chars(stmt.privilege.clone())
            .ok_or_else(|| ExecError::Parse(ParseError::UnexpectedEof))?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = catalog.lookup_relation(object_name).ok_or_else(|| {
            ExecError::Parse(ParseError::TableDoesNotExist(object_name.to_string()))
        })?;
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        if !auth_catalog
            .role_by_oid(auth.current_user_oid())
            .is_some_and(|row| row.rolsuper)
            && !auth.has_effective_membership(relation.owner_oid, &auth_catalog)
        {
            return Err(ExecError::DetailedError {
                message: format!("must be owner of table {object_name}"),
                detail: None,
                hint: None,
                sqlstate: "42501",
            });
        }
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
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let mut acl = catcache
            .class_by_oid(relation.relation_oid)
            .and_then(|row| row.relacl.clone())
            .unwrap_or_else(|| {
                table_owner_default_acl(&owner_name, relation.relkind)
                    .into_iter()
                    .collect()
            });
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
            grant_table_acl_entry(&mut acl, &grantee_acl_name, &grantor_name, privilege_chars);
        }
        let new_acl = collapse_relation_acl_defaults(acl, &owner_name, relation.relkind);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .alter_relation_acl_mvcc(relation.relation_oid, new_acl, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
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
        let object_name = single_object_name(&stmt.object_names, "single table name")?;
        let privilege_chars = table_privilege_chars(stmt.privilege.clone())
            .ok_or_else(|| ExecError::Parse(ParseError::UnexpectedEof))?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = catalog.lookup_relation(object_name).ok_or_else(|| {
            ExecError::Parse(ParseError::TableDoesNotExist(object_name.to_string()))
        })?;
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        if !auth_catalog
            .role_by_oid(auth.current_user_oid())
            .is_some_and(|row| row.rolsuper)
            && !auth.has_effective_membership(relation.owner_oid, &auth_catalog)
        {
            return Err(ExecError::DetailedError {
                message: format!("must be owner of table {object_name}"),
                detail: None,
                hint: None,
                sqlstate: "42501",
            });
        }
        let owner_name = auth_catalog
            .role_by_oid(relation.owner_oid)
            .map(|row| row.rolname.clone())
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("owner for table \"{object_name}\" does not exist"),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let mut acl = catcache
            .class_by_oid(relation.relation_oid)
            .and_then(|row| row.relacl.clone())
            .unwrap_or_default();
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
            revoke_table_acl_entry(&mut acl, &grantee_acl_name, privilege_chars);
        }
        let new_acl = collapse_relation_acl_defaults(acl, &owner_name, relation.relkind);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .alter_relation_acl_mvcc(relation.relation_oid, new_acl, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        let _ = stmt.cascade;
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
            &stmt.object_names,
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
            &stmt.object_names,
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
            &stmt.object_names,
            &stmt.grantee_names,
            configured_search_path,
            false,
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
            &stmt.object_names,
            &stmt.grantee_names,
            configured_search_path,
            true,
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
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let privilege_chars = object_privilege_chars(privilege.clone())
            .ok_or_else(|| ExecError::Parse(ParseError::UnexpectedEof))?;
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, 0)))
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
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, 0)), configured_search_path);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: 0,
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
            if !auth_catalog
                .role_by_oid(auth.current_user_oid())
                .is_some_and(|entry| entry.rolsuper)
                && !auth.has_effective_membership(row.typowner, &auth_catalog)
            {
                return Err(ExecError::DetailedError {
                    message: format!("must be owner of type {object_name}"),
                    detail: None,
                    hint: None,
                    sqlstate: "42501",
                });
            }
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
                        TYPE_USAGE_PRIVILEGE_CHARS,
                    );
                } else {
                    grant_acl_entry(
                        &mut acl,
                        &grantee_acl_name,
                        &grantor_name,
                        privilege_chars,
                        TYPE_USAGE_PRIVILEGE_CHARS,
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
        let result = self.finish_txn(
            client_id,
            xid,
            Ok(StatementResult::AffectedRows(0)),
            &catalog_effects,
            &[],
            &[],
        );
        guard.disarm();
        result
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
            &stmt.object_names,
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
            &stmt.object_names,
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
        let privilege_chars = object_privilege_chars(privilege.clone())
            .ok_or_else(|| ExecError::Parse(ParseError::UnexpectedEof))?;
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, 0)))
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
            cid: 0,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        for object_name in object_names {
            let row = lookup_function_row_by_signature(
                self,
                client_id,
                Some((xid, 0)),
                configured_search_path,
                object_name,
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
            let effect = self
                .catalog
                .write()
                .alter_proc_acl_mvcc(row.oid, new_acl, &ctx)
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
        }
        let result = self.finish_txn(
            client_id,
            xid,
            Ok(StatementResult::AffectedRows(0)),
            &catalog_effects,
            &[],
            &[],
        );
        guard.disarm();
        result
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
                    stmt.admin_option,
                    stmt.inherit_option.unwrap_or(true),
                    stmt.set_option.unwrap_or(true),
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
                let existing_index = role_rows
                    .iter()
                    .position(|row| row.member == grantee.oid && row.grantor == grantor_oid)
                    .ok_or_else(|| {
                        ExecError::Parse(role_management_error(format!(
                            "role grant does not exist: \"{}\" to \"{}\"",
                            role.rolname, grantee.rolname
                        )))
                    })?;
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

impl Database {
    fn execute_grant_database_create_stmt(
        &self,
        client_id: ClientId,
        stmt: &GrantObjectStatement,
    ) -> Result<StatementResult, ExecError> {
        let object_name = single_object_name(&stmt.object_names, "single database name")?;
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
        let object_name = single_object_name(&stmt.object_names, "single database name")?;
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
            grants.retain(|grant| {
                grant.grantee_oid != grantee.oid
                    || (!is_owner_or_superuser && grant.grantor_oid != current_user_oid)
            });
        }
        let _ = stmt.cascade;
        Ok(StatementResult::AffectedRows(0))
    }
}

fn upsert_role_membership_in_transaction(
    db: &Database,
    auth_catalog: &AuthCatalog,
    roleid: u32,
    member: u32,
    grantor: u32,
    admin_option: bool,
    inherit_option: bool,
    set_option: bool,
    ctx: &CatalogWriteContext,
    catalog_effects: &mut Vec<CatalogMutationEffect>,
) -> Result<(), ExecError> {
    if auth_catalog
        .memberships()
        .iter()
        .any(|row| row.roleid == roleid && row.member == member && row.grantor == grantor)
    {
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
                    &role_name(db, auth_catalog, roleid),
                )
            })?;
        catalog_effects.push(effect);
    }
    Ok(())
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
            && grantor.oid != role.oid
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

fn role_name(_db: &Database, auth_catalog: &AuthCatalog, role_oid: u32) -> String {
    auth_catalog
        .role_by_oid(role_oid)
        .map(|row| row.rolname.clone())
        .unwrap_or_else(|| role_oid.to_string())
}

fn member_name(db: &Database, auth_catalog: &AuthCatalog, member_oid: u32) -> String {
    role_name(db, auth_catalog, member_oid)
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
}
