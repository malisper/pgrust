use super::super::*;
use super::privilege::{acl_grants_privilege, effective_acl_grantee_names};
use crate::backend::executor::expr_reg::format_type_text;
use crate::backend::parser::{
    AlterOperatorAction, AlterOperatorOption, AlterOperatorStatement, CatalogLookup,
    CreateOperatorStatement, DropOperatorStatement, ParseError, QualifiedNameRef,
    resolve_raw_type_name,
};
use crate::backend::utils::cache::syscache::backend_catcache;
use crate::backend::utils::misc::notices::push_warning;
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, INT2_TYPE_OID, INT4_TYPE_OID, OID_TYPE_OID, PG_CATALOG_NAMESPACE_OID,
    PUBLIC_NAMESPACE_OID, PgOperatorRow,
};
use crate::include::nodes::parsenodes::RawTypeName;
use crate::pgrust::database::ddl::ensure_can_set_role;

const INTERNAL_TYPE_OID: u32 = 2281;
const SCHEMA_CREATE_PRIVILEGE_CHAR: char = 'C';
const TYPE_USAGE_PRIVILEGE_CHAR: char = 'U';
const FUNCTION_EXECUTE_PRIVILEGE_CHAR: char = 'X';

fn normalize_operator_namespace(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    schema_name: Option<&str>,
    configured_search_path: Option<&[String]>,
) -> Result<u32, ParseError> {
    match schema_name.map(str::to_ascii_lowercase) {
        Some(schema) if schema == "public" => Ok(PUBLIC_NAMESPACE_OID),
        Some(schema) if schema == "pg_catalog" => Ok(PG_CATALOG_NAMESPACE_OID),
        Some(schema) => db
            .visible_namespace_oid_by_name(client_id, txn_ctx, &schema)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "existing schema",
                actual: format!("schema \"{schema}\" does not exist"),
            }),
        None => {
            let search_path = db.effective_search_path(client_id, configured_search_path);
            let temp_namespace_name = db
                .owned_temp_namespace(client_id)
                .map(|namespace| namespace.name);
            for schema in search_path {
                let is_temp_schema = schema == "pg_temp"
                    || schema.starts_with("pg_temp_")
                    || temp_namespace_name
                        .as_deref()
                        .is_some_and(|temp| temp.eq_ignore_ascii_case(&schema));
                match schema.as_str() {
                    "" | "$user" | "pg_catalog" => continue,
                    _ if is_temp_schema => continue,
                    "public" => return Ok(PUBLIC_NAMESPACE_OID),
                    _ => {
                        if let Some(namespace_oid) =
                            db.visible_namespace_oid_by_name(client_id, txn_ctx, &schema)
                        {
                            return Ok(namespace_oid);
                        }
                    }
                }
            }
            Err(ParseError::NoSchemaSelectedForCreate)
        }
    }
}

pub(super) fn operator_signature_display(
    catalog: &dyn CatalogLookup,
    name: &str,
    left_type: u32,
    right_type: u32,
) -> String {
    match (left_type, right_type) {
        (0, 0) => name.to_string(),
        (0, right) => format!("{name} {}", format_type_text(right, None, catalog)),
        (left, 0) => format!("{} {name}", format_type_text(left, None, catalog)),
        (left, right) => format!(
            "{} {name} {}",
            format_type_text(left, None, catalog),
            format_type_text(right, None, catalog)
        ),
    }
}

pub(super) fn unsupported_postfix_operator_error() -> ExecError {
    ExecError::DetailedError {
        message: "postfix operators are not supported".into(),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

pub(super) fn lookup_operator_row(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    namespace_oid: Option<u32>,
    name: &str,
    left_type: u32,
    right_type: u32,
) -> Result<Option<PgOperatorRow>, ExecError> {
    Ok(backend_catcache(db, client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .operator_rows()
        .into_iter()
        .find(|row| {
            row.oprname.eq_ignore_ascii_case(name)
                && namespace_oid.is_none_or(|oid| row.oprnamespace == oid)
                && row.oprleft == left_type
                && row.oprright == right_type
        }))
}

fn lookup_operator_row_by_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    oid: u32,
) -> Result<Option<PgOperatorRow>, ExecError> {
    Ok(backend_catcache(db, client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .operator_rows()
        .into_iter()
        .find(|row| row.oid == oid))
}

pub(super) fn resolve_operator_type_oid(
    catalog: &dyn CatalogLookup,
    arg: &Option<RawTypeName>,
) -> Result<u32, ExecError> {
    match arg {
        Some(arg) => {
            if matches!(
                arg,
                RawTypeName::Named { name, .. } if name.eq_ignore_ascii_case("setof")
            ) {
                return Err(ExecError::DetailedError {
                    message: "SETOF type not allowed for operator argument".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42601",
                });
            }
            let sql_type = resolve_raw_type_name(arg, catalog).map_err(ExecError::Parse)?;
            catalog
                .type_oid_for_sql_type(sql_type)
                .ok_or_else(|| ExecError::Parse(ParseError::UnsupportedType(format!("{arg:?}"))))
        }
        None => Ok(0),
    }
}

fn resolve_proc_oid_for_name(
    catalog: &dyn CatalogLookup,
    target: &QualifiedNameRef,
    arg_type_oids: &[u32],
    missing_message: String,
) -> Result<u32, ExecError> {
    let desired = arg_type_oids
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    catalog
        .proc_rows_by_name(&target.name)
        .into_iter()
        .find(|row| {
            row.proname.eq_ignore_ascii_case(&target.name)
                && row.proargtypes == desired
                && target
                    .schema_name
                    .as_deref()
                    .map(|schema| {
                        (schema.eq_ignore_ascii_case("public")
                            && row.pronamespace == PUBLIC_NAMESPACE_OID)
                            || (schema.eq_ignore_ascii_case("pg_catalog")
                                && row.pronamespace == PG_CATALOG_NAMESPACE_OID)
                    })
                    .unwrap_or(true)
        })
        .map(|row| row.oid)
        .ok_or_else(|| {
            ExecError::Parse(ParseError::DetailedError {
                message: missing_message,
                detail: None,
                hint: None,
                sqlstate: "42883",
            })
        })
}

fn resolve_create_operator_proc_oid(
    catalog: &dyn CatalogLookup,
    stmt: &CreateOperatorStatement,
    left_type: u32,
    right_type: u32,
) -> Result<u32, ExecError> {
    let procedure = stmt
        .procedure
        .as_ref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "operator function must be specified".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        })?;
    let arg_types = [left_type, right_type]
        .into_iter()
        .filter(|oid| *oid != 0)
        .collect::<Vec<_>>();
    resolve_proc_oid_for_name(
        catalog,
        procedure,
        &arg_types,
        format!(
            "function {}({}) does not exist",
            procedure.name,
            arg_types
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    )
}

fn resolve_restriction_oid(
    catalog: &dyn CatalogLookup,
    function: &QualifiedNameRef,
) -> Result<u32, ExecError> {
    resolve_proc_oid_for_name(
        catalog,
        function,
        &[
            INTERNAL_TYPE_OID,
            OID_TYPE_OID,
            INTERNAL_TYPE_OID,
            INT4_TYPE_OID,
        ],
        format!(
            "function {}(internal, oid, internal, integer) does not exist",
            function.name
        ),
    )
}

fn resolve_join_oid(
    catalog: &dyn CatalogLookup,
    function: &QualifiedNameRef,
) -> Result<u32, ExecError> {
    resolve_proc_oid_for_name(
        catalog,
        function,
        &[
            INTERNAL_TYPE_OID,
            OID_TYPE_OID,
            INTERNAL_TYPE_OID,
            INT2_TYPE_OID,
            INTERNAL_TYPE_OID,
        ],
        format!(
            "function {}(internal, oid, internal, smallint, internal) does not exist",
            function.name
        ),
    )
}

fn owner_error(name: &str) -> ExecError {
    ExecError::Parse(ParseError::DetailedError {
        message: format!("must be owner of operator {name}"),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}

fn replace_single_operator(
    db: &Database,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    current: &PgOperatorRow,
    updated: PgOperatorRow,
    catalog_effects: &mut Vec<CatalogMutationEffect>,
) -> Result<StatementResult, ExecError> {
    let ctx = CatalogWriteContext {
        pool: db.pool.clone(),
        txns: db.txns.clone(),
        xid,
        cid,
        client_id,
        waiter: Some(db.txn_waiter.clone()),
        interrupts: db.interrupt_state(client_id),
    };
    let effect = db
        .catalog
        .write()
        .replace_operator_mvcc(current, updated, &ctx)
        .map_err(map_catalog_error)?
        .1;
    db.apply_catalog_mutation_effect_immediate(&effect)?;
    catalog_effects.push(effect);
    Ok(StatementResult::AffectedRows(0))
}

fn attribute_already_set_error(name: &str) -> ExecError {
    ExecError::Parse(ParseError::DetailedError {
        message: format!(
            "operator attribute \"{name}\" cannot be changed if it has already been set"
        ),
        detail: None,
        hint: None,
        sqlstate: "42601",
    })
}

fn attribute_not_recognized_error(name: &str) -> ExecError {
    ExecError::Parse(ParseError::DetailedError {
        message: format!("operator attribute \"{name}\" not recognized"),
        detail: None,
        hint: None,
        sqlstate: "42601",
    })
}

fn validate_alter_operator_option_names(action: &AlterOperatorAction) -> Result<(), ExecError> {
    let AlterOperatorAction::SetOptions(options) = action else {
        return Ok(());
    };
    for option in options {
        let (option_name, expected) = match option {
            AlterOperatorOption::Restrict { option_name, .. } => (option_name, "restrict"),
            AlterOperatorOption::Join { option_name, .. } => (option_name, "join"),
            AlterOperatorOption::Commutator { option_name, .. } => (option_name, "commutator"),
            AlterOperatorOption::Negator { option_name, .. } => (option_name, "negator"),
            AlterOperatorOption::Merges { option_name, .. } => (option_name, "merges"),
            AlterOperatorOption::Hashes { option_name, .. } => (option_name, "hashes"),
            AlterOperatorOption::Unrecognized { option_name, .. } => {
                return Err(attribute_not_recognized_error(option_name));
            }
        };
        if option_name != expected {
            return Err(attribute_not_recognized_error(option_name));
        }
    }
    Ok(())
}

fn default_schema_acl(owner_name: &str) -> Vec<String> {
    vec![format!("{owner_name}=UC/{owner_name}")]
}

fn default_type_acl(owner_name: &str) -> Vec<String> {
    vec![
        format!("{owner_name}=U/{owner_name}"),
        format!("=U/{owner_name}"),
    ]
}

fn default_function_acl(owner_name: &str) -> Vec<String> {
    vec![
        format!("{owner_name}=X/{owner_name}"),
        format!("=X/{owner_name}"),
    ]
}

fn self_negator_error() -> ExecError {
    ExecError::Parse(ParseError::DetailedError {
        message: "operator cannot be its own negator".into(),
        detail: None,
        hint: None,
        sqlstate: "42883",
    })
}

fn existing_partner_error(
    kind: &str,
    partner: &PgOperatorRow,
    existing: &PgOperatorRow,
) -> ExecError {
    ExecError::Parse(ParseError::DetailedError {
        message: format!(
            "{kind} operator {} is already the {kind} of operator {}",
            partner.oprname, existing.oprname
        ),
        detail: None,
        hint: None,
        sqlstate: "42601",
    })
}

fn shell_operator_row(
    namespace_oid: u32,
    name: &str,
    left_type: u32,
    right_type: u32,
) -> PgOperatorRow {
    PgOperatorRow {
        oid: 0,
        oprname: name.to_ascii_lowercase(),
        oprnamespace: namespace_oid,
        oprowner: BOOTSTRAP_SUPERUSER_OID,
        oprkind: if left_type == 0 {
            'r'
        } else if right_type == 0 {
            'l'
        } else {
            'b'
        },
        oprcanmerge: false,
        oprcanhash: false,
        oprleft: left_type,
        oprright: right_type,
        oprresult: 0,
        oprcom: 0,
        oprnegate: 0,
        oprcode: 0,
        oprrest: 0,
        oprjoin: 0,
    }
}

impl Database {
    fn ensure_create_operator_privileges(
        &self,
        client_id: ClientId,
        txn_ctx: Option<(TransactionId, CommandId)>,
        namespace_oid: u32,
        left_type: u32,
        right_type: u32,
        proc_oid: u32,
        result_type: u32,
    ) -> Result<(), ExecError> {
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, txn_ctx)
            .map_err(map_catalog_error)?;
        if auth_catalog
            .role_by_oid(auth.current_user_oid())
            .is_some_and(|row| row.rolsuper)
        {
            return Ok(());
        }
        let effective_names = effective_acl_grantee_names(&auth, &auth_catalog);
        let catalog = self.lazy_catalog_lookup(client_id, txn_ctx, None);

        let namespace = catalog.namespace_row_by_oid(namespace_oid).ok_or_else(|| {
            ExecError::DetailedError {
                message: format!("schema with OID {namespace_oid} does not exist"),
                detail: None,
                hint: None,
                sqlstate: "3F000",
            }
        })?;
        if !auth.has_effective_membership(namespace.nspowner, &auth_catalog) {
            let owner_name = auth_catalog
                .role_by_oid(namespace.nspowner)
                .map(|row| row.rolname.clone())
                .unwrap_or_default();
            let acl = namespace
                .nspacl
                .clone()
                .unwrap_or_else(|| default_schema_acl(&owner_name));
            if !acl_grants_privilege(&acl, &effective_names, SCHEMA_CREATE_PRIVILEGE_CHAR) {
                return Err(ExecError::DetailedError {
                    message: format!("permission denied for schema {}", namespace.nspname),
                    detail: None,
                    hint: None,
                    sqlstate: "42501",
                });
            }
        }

        for type_oid in [left_type, right_type, result_type] {
            if type_oid == 0 {
                continue;
            }
            let row = catalog
                .type_by_oid(type_oid)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("type with OID {type_oid} does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                })?;
            if auth.has_effective_membership(row.typowner, &auth_catalog) {
                continue;
            }
            let owner_name = auth_catalog
                .role_by_oid(row.typowner)
                .map(|entry| entry.rolname.clone())
                .unwrap_or_default();
            let acl = row
                .typacl
                .clone()
                .unwrap_or_else(|| default_type_acl(&owner_name));
            if !acl_grants_privilege(&acl, &effective_names, TYPE_USAGE_PRIVILEGE_CHAR) {
                return Err(ExecError::DetailedError {
                    message: format!("permission denied for type {}", row.typname),
                    detail: None,
                    hint: None,
                    sqlstate: "42501",
                });
            }
        }

        let proc_row =
            catalog
                .proc_row_by_oid(proc_oid)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("function with OID {proc_oid} does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42883",
                })?;
        if !auth.has_effective_membership(proc_row.proowner, &auth_catalog) {
            let owner_name = auth_catalog
                .role_by_oid(proc_row.proowner)
                .map(|row| row.rolname.clone())
                .unwrap_or_default();
            let acl = proc_row
                .proacl
                .clone()
                .unwrap_or_else(|| default_function_acl(&owner_name));
            if !acl_grants_privilege(&acl, &effective_names, FUNCTION_EXECUTE_PRIVILEGE_CHAR) {
                return Err(ExecError::DetailedError {
                    message: format!("permission denied for function {}", proc_row.proname),
                    detail: None,
                    hint: None,
                    sqlstate: "42501",
                });
            }
        }
        Ok(())
    }

    pub(crate) fn execute_create_operator_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateOperatorStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_operator_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[])
    }

    pub(crate) fn execute_create_operator_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateOperatorStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let mut current_cid = cid;
        let catalog =
            self.lazy_catalog_lookup(client_id, Some((xid, current_cid)), configured_search_path);
        let namespace_oid = normalize_operator_namespace(
            self,
            client_id,
            Some((xid, current_cid)),
            stmt.schema_name.as_deref(),
            configured_search_path,
        )?;
        let left_type = resolve_operator_type_oid(&catalog, &stmt.left_arg)?;
        let right_type = resolve_operator_type_oid(&catalog, &stmt.right_arg)?;
        for attribute in &stmt.unrecognized_attributes {
            push_warning(format!("operator attribute \"{attribute}\" not recognized"));
        }
        if stmt.procedure.is_none() {
            return Err(ExecError::DetailedError {
                message: "operator function must be specified".into(),
                detail: None,
                hint: None,
                sqlstate: "42601",
            });
        }
        if left_type == 0 && right_type == 0 {
            return Err(ExecError::DetailedError {
                message: "operator argument types must be specified".into(),
                detail: None,
                hint: None,
                sqlstate: "42601",
            });
        }
        if right_type == 0 {
            return Err(ExecError::DetailedError {
                message: "operator right argument type must be specified".into(),
                detail: Some("Postfix operators are not supported.".into()),
                hint: None,
                sqlstate: "42601",
            });
        }
        let proc_oid = resolve_create_operator_proc_oid(&catalog, stmt, left_type, right_type)?;
        let result_type = catalog
            .proc_row_by_oid(proc_oid)
            .map(|row| row.prorettype)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "existing procedure row",
                    actual: stmt
                        .procedure
                        .as_ref()
                        .map(|procedure| procedure.name.clone())
                        .unwrap_or_default(),
                })
            })?;
        self.ensure_create_operator_privileges(
            client_id,
            Some((xid, current_cid)),
            namespace_oid,
            left_type,
            right_type,
            proc_oid,
            result_type,
        )?;
        if stmt.negator.as_deref() == Some(stmt.operator_name.as_str()) {
            return Err(self_negator_error());
        }
        let existing_row = lookup_operator_row(
            self,
            client_id,
            Some((xid, current_cid)),
            Some(namespace_oid),
            &stmt.operator_name,
            left_type,
            right_type,
        )?;
        let replacing_shell = existing_row.clone().filter(|row| row.oprcode == 0);
        if existing_row.is_some() && replacing_shell.is_none() {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "new operator signature",
                actual: format!(
                    "operator {} already exists",
                    operator_signature_display(
                        &catalog,
                        &stmt.operator_name,
                        left_type,
                        right_type
                    )
                ),
            }));
        }
        let replacing_oid = replacing_shell.as_ref().map(|row| row.oid).unwrap_or(0);

        let restrict_oid = stmt
            .restrict
            .as_ref()
            .map(|f| resolve_restriction_oid(&catalog, f))
            .transpose()?
            .unwrap_or(0);
        let join_oid = stmt
            .join
            .as_ref()
            .map(|f| resolve_join_oid(&catalog, f))
            .transpose()?
            .unwrap_or(0);

        let mut commutator_partner = None;
        let mut negator_partner = None;
        if let Some(name) = &stmt.commutator
            && name != &stmt.operator_name
        {
            if let Some(partner) = lookup_operator_row(
                self,
                client_id,
                Some((xid, current_cid)),
                None,
                name,
                right_type,
                left_type,
            )? {
                if partner.oprcom != 0 && partner.oprcom != replacing_oid {
                    let existing = lookup_operator_row_by_oid(
                        self,
                        client_id,
                        Some((xid, current_cid)),
                        partner.oprcom,
                    )?
                    .unwrap_or(partner.clone());
                    return Err(existing_partner_error("commutator", &partner, &existing));
                }
                commutator_partner = Some(partner);
            } else {
                let effect = {
                    let mut catalog_store = self.catalog.write();
                    let ctx = CatalogWriteContext {
                        pool: self.pool.clone(),
                        txns: self.txns.clone(),
                        xid,
                        cid: current_cid,
                        client_id,
                        waiter: Some(self.txn_waiter.clone()),
                        interrupts: self.interrupt_state(client_id),
                    };
                    catalog_store
                        .create_operator_mvcc(
                            shell_operator_row(namespace_oid, name, right_type, left_type),
                            &ctx,
                        )
                        .map_err(map_catalog_error)?
                        .1
                };
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                current_cid = current_cid.saturating_add(1);
                commutator_partner = lookup_operator_row(
                    self,
                    client_id,
                    Some((xid, current_cid)),
                    Some(namespace_oid),
                    name,
                    right_type,
                    left_type,
                )?;
            }
        }
        if let Some(name) = &stmt.negator {
            if let Some(partner) = lookup_operator_row(
                self,
                client_id,
                Some((xid, current_cid)),
                None,
                name,
                left_type,
                right_type,
            )? {
                if partner.oprnegate != 0 && partner.oprnegate != replacing_oid {
                    let existing = lookup_operator_row_by_oid(
                        self,
                        client_id,
                        Some((xid, current_cid)),
                        partner.oprnegate,
                    )?
                    .unwrap_or(partner.clone());
                    return Err(existing_partner_error("negator", &partner, &existing));
                }
                negator_partner = Some(partner);
            } else {
                let effect = {
                    let mut catalog_store = self.catalog.write();
                    let ctx = CatalogWriteContext {
                        pool: self.pool.clone(),
                        txns: self.txns.clone(),
                        xid,
                        cid: current_cid,
                        client_id,
                        waiter: Some(self.txn_waiter.clone()),
                        interrupts: self.interrupt_state(client_id),
                    };
                    catalog_store
                        .create_operator_mvcc(
                            shell_operator_row(namespace_oid, name, left_type, right_type),
                            &ctx,
                        )
                        .map_err(map_catalog_error)?
                        .1
                };
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                current_cid = current_cid.saturating_add(1);
                negator_partner = lookup_operator_row(
                    self,
                    client_id,
                    Some((xid, current_cid)),
                    Some(namespace_oid),
                    name,
                    left_type,
                    right_type,
                )?;
            }
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: current_cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let base_row = PgOperatorRow {
            oid: 0,
            oprname: stmt.operator_name.to_ascii_lowercase(),
            oprnamespace: namespace_oid,
            oprowner: self.auth_state(client_id).current_user_oid(),
            oprkind: if left_type == 0 {
                'r'
            } else if right_type == 0 {
                'l'
            } else {
                'b'
            },
            oprcanmerge: stmt.merges,
            oprcanhash: stmt.hashes,
            oprleft: left_type,
            oprright: right_type,
            oprresult: result_type,
            oprcom: 0,
            oprnegate: 0,
            oprcode: proc_oid,
            oprrest: restrict_oid,
            oprjoin: join_oid,
        };
        let (operator_oid, effect) = {
            let mut catalog_store = self.catalog.write();
            if let Some(shell_row) = &replacing_shell {
                catalog_store
                    .replace_operator_mvcc(shell_row, base_row.clone(), &ctx)
                    .map_err(map_catalog_error)?
            } else {
                catalog_store
                    .create_operator_mvcc(base_row.clone(), &ctx)
                    .map_err(map_catalog_error)?
            }
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        current_cid = current_cid.saturating_add(1);

        let mut updated = base_row;
        updated.oid = operator_oid;
        if stmt.commutator.as_deref() == Some(stmt.operator_name.as_str()) {
            updated.oprcom = operator_oid;
        }
        if let Some(partner) = &commutator_partner {
            updated.oprcom = partner.oid;
        }
        if let Some(partner) = &negator_partner {
            updated.oprnegate = partner.oid;
        }
        if updated.oprcom != 0 || updated.oprnegate != 0 {
            let current_row = lookup_operator_row_by_oid(
                self,
                client_id,
                Some((xid, current_cid)),
                operator_oid,
            )?
            .unwrap_or_else(|| {
                let mut row = updated.clone();
                row.oprcom = 0;
                row.oprnegate = 0;
                row
            });
            let effect = {
                let mut catalog_store = self.catalog.write();
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: current_cid,
                    client_id,
                    waiter: Some(self.txn_waiter.clone()),
                    interrupts: self.interrupt_state(client_id),
                };
                catalog_store
                    .replace_operator_mvcc(&current_row, updated.clone(), &ctx)
                    .map_err(map_catalog_error)?
                    .1
            };
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            current_cid = current_cid.saturating_add(1);
        }
        for partner in commutator_partner
            .into_iter()
            .chain(negator_partner.into_iter())
        {
            let mut partner_updated = partner.clone();
            if updated.oprcom == partner.oid {
                partner_updated.oprcom = operator_oid;
            }
            if updated.oprnegate == partner.oid {
                partner_updated.oprnegate = operator_oid;
            }
            let effect = {
                let mut catalog_store = self.catalog.write();
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: current_cid,
                    client_id,
                    waiter: Some(self.txn_waiter.clone()),
                    interrupts: self.interrupt_state(client_id),
                };
                catalog_store
                    .replace_operator_mvcc(&partner, partner_updated, &ctx)
                    .map_err(map_catalog_error)?
                    .1
            };
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            current_cid = current_cid.saturating_add(1);
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_operator_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterOperatorStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_operator_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[])
    }

    pub(crate) fn execute_alter_operator_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterOperatorStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        validate_alter_operator_option_names(&stmt.action)?;
        let namespace_oid = stmt
            .schema_name
            .as_deref()
            .map(|schema| {
                normalize_operator_namespace(
                    self,
                    client_id,
                    Some((xid, cid)),
                    Some(schema),
                    configured_search_path,
                )
            })
            .transpose()?;
        let left_type = resolve_operator_type_oid(&catalog, &stmt.left_arg)?;
        let right_type = resolve_operator_type_oid(&catalog, &stmt.right_arg)?;
        let current = lookup_operator_row(
            self,
            client_id,
            Some((xid, cid)),
            namespace_oid,
            &stmt.operator_name,
            left_type,
            right_type,
        )?
        .ok_or_else(|| {
            ExecError::Parse(ParseError::DetailedError {
                message: format!(
                    "operator does not exist: {}",
                    operator_signature_display(
                        &catalog,
                        &stmt.operator_name,
                        left_type,
                        right_type
                    )
                ),
                detail: None,
                hint: None,
                sqlstate: "42883",
            })
        })?;
        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        if !auth.can_set_role(current.oprowner, &auth_catalog) {
            return Err(owner_error(&stmt.operator_name));
        }

        let mut updated = current.clone();
        match &stmt.action {
            AlterOperatorAction::OwnerTo { new_owner } => {
                let role = auth_catalog
                    .role_by_name(new_owner)
                    .cloned()
                    .ok_or_else(|| {
                        ExecError::Parse(crate::backend::commands::rolecmds::role_management_error(
                            format!("role \"{new_owner}\" does not exist"),
                        ))
                    })?;
                ensure_can_set_role(self, client_id, role.oid, &role.rolname)?;
                updated.oprowner = role.oid;
                return replace_single_operator(
                    self,
                    client_id,
                    xid,
                    cid,
                    &current,
                    updated,
                    catalog_effects,
                );
            }
            AlterOperatorAction::SetSchema { new_schema } => {
                let new_namespace_oid = self
                    .visible_namespace_oid_by_name(client_id, Some((xid, cid)), new_schema)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("schema \"{new_schema}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    })?;
                let duplicate = lookup_operator_row(
                    self,
                    client_id,
                    Some((xid, cid)),
                    Some(new_namespace_oid),
                    &stmt.operator_name,
                    left_type,
                    right_type,
                )?;
                if duplicate.is_some_and(|existing| existing.oid != current.oid) {
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "operator {} already exists in schema \"{}\"",
                            stmt.operator_name, new_schema
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42710",
                    });
                }
                updated.oprnamespace = new_namespace_oid;
                return replace_single_operator(
                    self,
                    client_id,
                    xid,
                    cid,
                    &current,
                    updated,
                    catalog_effects,
                );
            }
            AlterOperatorAction::SetOptions(_) => {}
        }
        let mut partner_updates = Vec::new();
        let AlterOperatorAction::SetOptions(options) = &stmt.action else {
            unreachable!("handled above");
        };
        for option in options {
            match option {
                AlterOperatorOption::Restrict {
                    option_name,
                    function,
                } => {
                    if option_name != "restrict" {
                        return Err(attribute_not_recognized_error(option_name));
                    }
                    updated.oprrest = function
                        .as_ref()
                        .map(|f| resolve_restriction_oid(&catalog, f))
                        .transpose()?
                        .unwrap_or(0);
                }
                AlterOperatorOption::Join {
                    option_name,
                    function,
                } => {
                    if option_name != "join" {
                        return Err(attribute_not_recognized_error(option_name));
                    }
                    updated.oprjoin = function
                        .as_ref()
                        .map(|f| resolve_join_oid(&catalog, f))
                        .transpose()?
                        .unwrap_or(0);
                }
                AlterOperatorOption::Commutator {
                    option_name,
                    operator_name,
                } => {
                    if option_name != "commutator" {
                        return Err(attribute_not_recognized_error(option_name));
                    }
                    let partner = lookup_operator_row(
                        self,
                        client_id,
                        Some((xid, cid)),
                        None,
                        operator_name,
                        right_type,
                        left_type,
                    )?
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::DetailedError {
                            message: format!(
                                "operator does not exist: {}({}, {})",
                                operator_name, right_type, left_type
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42883",
                        })
                    })?;
                    if updated.oprcom != 0 && updated.oprcom != partner.oid {
                        return Err(attribute_already_set_error("commutator"));
                    }
                    if partner.oprcom != 0 && partner.oprcom != updated.oid {
                        let existing = lookup_operator_row_by_oid(
                            self,
                            client_id,
                            Some((xid, cid)),
                            partner.oprcom,
                        )?
                        .unwrap_or(partner.clone());
                        return Err(ExecError::Parse(ParseError::DetailedError {
                            message: format!(
                                "commutator operator {} is already the commutator of operator {}",
                                partner.oprname, existing.oprname
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42601",
                        }));
                    }
                    updated.oprcom = partner.oid;
                    let mut partner_updated = partner.clone();
                    partner_updated.oprcom = updated.oid;
                    partner_updates.push((partner, partner_updated));
                }
                AlterOperatorOption::Negator {
                    option_name,
                    operator_name,
                } => {
                    if option_name != "negator" {
                        return Err(attribute_not_recognized_error(option_name));
                    }
                    let partner = lookup_operator_row(
                        self,
                        client_id,
                        Some((xid, cid)),
                        None,
                        operator_name,
                        left_type,
                        right_type,
                    )?
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::DetailedError {
                            message: format!(
                                "operator does not exist: {}({}, {})",
                                operator_name, left_type, right_type
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42883",
                        })
                    })?;
                    if partner.oid == updated.oid {
                        return Err(ExecError::Parse(ParseError::DetailedError {
                            message: "operator cannot be its own negator".into(),
                            detail: None,
                            hint: None,
                            sqlstate: "42883",
                        }));
                    }
                    if updated.oprnegate != 0 && updated.oprnegate != partner.oid {
                        return Err(attribute_already_set_error("negator"));
                    }
                    if partner.oprnegate != 0 && partner.oprnegate != updated.oid {
                        let existing = lookup_operator_row_by_oid(
                            self,
                            client_id,
                            Some((xid, cid)),
                            partner.oprnegate,
                        )?
                        .unwrap_or(partner.clone());
                        return Err(ExecError::Parse(ParseError::DetailedError {
                            message: format!(
                                "negator operator {} is already the negator of operator {}",
                                partner.oprname, existing.oprname
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42601",
                        }));
                    }
                    updated.oprnegate = partner.oid;
                    let mut partner_updated = partner.clone();
                    partner_updated.oprnegate = updated.oid;
                    partner_updates.push((partner, partner_updated));
                }
                AlterOperatorOption::Merges {
                    option_name,
                    enabled,
                } => {
                    if option_name != "merges" {
                        return Err(attribute_not_recognized_error(option_name));
                    }
                    if updated.oprcanmerge && !enabled {
                        return Err(attribute_already_set_error("merges"));
                    }
                    if *enabled {
                        updated.oprcanmerge = true;
                    }
                }
                AlterOperatorOption::Hashes {
                    option_name,
                    enabled,
                } => {
                    if option_name != "hashes" {
                        return Err(attribute_not_recognized_error(option_name));
                    }
                    if updated.oprcanhash && !enabled {
                        return Err(attribute_already_set_error("hashes"));
                    }
                    if *enabled {
                        updated.oprcanhash = true;
                    }
                }
                AlterOperatorOption::Unrecognized { option_name, .. } => {
                    return Err(attribute_not_recognized_error(option_name));
                }
            }
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let effect = {
            let mut catalog_store = self.catalog.write();
            catalog_store
                .replace_operator_mvcc(&current, updated.clone(), &ctx)
                .map_err(map_catalog_error)?
                .1
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);

        for (old_partner, new_partner) in partner_updates {
            if old_partner.oid == current.oid {
                continue;
            }
            let effect = {
                let mut catalog_store = self.catalog.write();
                catalog_store
                    .replace_operator_mvcc(&old_partner, new_partner, &ctx)
                    .map_err(map_catalog_error)?
                    .1
            };
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }

        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_operator_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropOperatorStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_operator_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[])
    }

    pub(crate) fn execute_drop_operator_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropOperatorStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let namespace_oid = stmt
            .schema_name
            .as_deref()
            .map(|schema| {
                normalize_operator_namespace(
                    self,
                    client_id,
                    Some((xid, cid)),
                    Some(schema),
                    configured_search_path,
                )
            })
            .transpose()?;
        let left_type = resolve_operator_type_oid(&catalog, &stmt.left_arg)?;
        let right_type = resolve_operator_type_oid(&catalog, &stmt.right_arg)?;
        if right_type == 0 {
            return Err(unsupported_postfix_operator_error());
        }
        let Some(row) = lookup_operator_row(
            self,
            client_id,
            Some((xid, cid)),
            namespace_oid,
            &stmt.operator_name,
            left_type,
            right_type,
        )?
        else {
            if stmt.if_exists {
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::Parse(ParseError::DetailedError {
                message: format!(
                    "operator does not exist: {}",
                    operator_signature_display(
                        &catalog,
                        &stmt.operator_name,
                        left_type,
                        right_type
                    )
                ),
                detail: None,
                hint: None,
                sqlstate: "42883",
            }));
        };

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let effect = {
            let mut catalog_store = self.catalog.write();
            catalog_store
                .drop_operator_by_oid_mvcc(row.oid, &ctx)
                .map_err(map_catalog_error)?
                .1
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}
