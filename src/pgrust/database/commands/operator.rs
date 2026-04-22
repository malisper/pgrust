use super::super::*;
use crate::backend::parser::{
    AlterOperatorOption, AlterOperatorStatement, CatalogLookup, CreateOperatorStatement,
    DropOperatorStatement, ParseError, QualifiedNameRef, resolve_raw_type_name,
};
use crate::backend::utils::cache::syscache::backend_catcache;
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, INT2_TYPE_OID, INT4_TYPE_OID, OID_TYPE_OID, PG_CATALOG_NAMESPACE_OID,
    PUBLIC_NAMESPACE_OID, PgOperatorRow,
};
use crate::include::nodes::parsenodes::RawTypeName;

const INTERNAL_TYPE_OID: u32 = 2281;

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
            for schema in search_path {
                match schema.as_str() {
                    "" | "$user" | "pg_temp" | "pg_catalog" => continue,
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

fn operator_signature_display(name: &str, left_type: u32, right_type: u32) -> String {
    format!("{name}({left_type},{right_type})")
}

fn lookup_operator_row(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
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

fn resolve_operator_type_oid(
    catalog: &dyn CatalogLookup,
    arg: &Option<RawTypeName>,
) -> Result<u32, ExecError> {
    match arg {
        Some(arg) => {
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
    let arg_types = [left_type, right_type]
        .into_iter()
        .filter(|oid| *oid != 0)
        .collect::<Vec<_>>();
    resolve_proc_oid_for_name(
        catalog,
        &stmt.procedure,
        &arg_types,
        format!(
            "function {}({}) does not exist",
            stmt.procedure.name,
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

impl Database {
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
        let catalog = self.lazy_catalog_lookup(
            client_id,
            Some((xid, current_cid)),
            configured_search_path,
        );
        let namespace_oid = normalize_operator_namespace(
            self,
            client_id,
            Some((xid, current_cid)),
            stmt.schema_name.as_deref(),
            configured_search_path,
        )?;
        let left_type = resolve_operator_type_oid(&catalog, &stmt.left_arg)?;
        let right_type = resolve_operator_type_oid(&catalog, &stmt.right_arg)?;
        let proc_oid = resolve_create_operator_proc_oid(&catalog, stmt, left_type, right_type)?;
        let result_type = catalog
            .proc_row_by_oid(proc_oid)
            .map(|row| row.prorettype)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "existing procedure row",
                    actual: stmt.procedure.name.clone(),
                })
            })?;
        if lookup_operator_row(
            self,
            client_id,
            Some((xid, current_cid)),
            &stmt.operator_name,
            left_type,
            right_type,
        )?
        .is_some()
        {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "new operator signature",
                actual: format!(
                    "operator {} already exists",
                    operator_signature_display(&stmt.operator_name, left_type, right_type)
                ),
            }));
        }

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
            oprowner: BOOTSTRAP_SUPERUSER_OID,
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
            catalog_store
                .create_operator_mvcc(base_row.clone(), &ctx)
                .map_err(map_catalog_error)?
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        current_cid = current_cid.saturating_add(1);

        let mut updated = base_row;
        updated.oid = operator_oid;
        if stmt.commutator.as_deref() == Some(stmt.operator_name.as_str()) {
            updated.oprcom = operator_oid;
        }
        if let Some(name) = &stmt.negator {
            if let Some(row) = lookup_operator_row(
                self,
                client_id,
                Some((xid, current_cid)),
                name,
                left_type,
                right_type,
            )? {
                updated.oprnegate = row.oid;
            }
        }
        if updated.oprcom != 0 || updated.oprnegate != 0 {
            let mut current_row = updated.clone();
            current_row.oprcom = 0;
            current_row.oprnegate = 0;
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
                    .replace_operator_mvcc(&current_row, updated, &ctx)
                    .map_err(map_catalog_error)?
                    .1
            };
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
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
        for option in &stmt.options {
            match option {
                AlterOperatorOption::Restrict { option_name, .. }
                | AlterOperatorOption::Join { option_name, .. }
                | AlterOperatorOption::Commutator { option_name, .. }
                | AlterOperatorOption::Negator { option_name, .. }
                | AlterOperatorOption::Merges { option_name, .. }
                | AlterOperatorOption::Hashes { option_name, .. }
                | AlterOperatorOption::Unrecognized { option_name, .. } => {
                    if !matches!(
                        option_name.as_str(),
                        "restrict" | "join" | "commutator" | "negator" | "merges" | "hashes"
                    ) {
                        return Err(attribute_not_recognized_error(option_name));
                    }
                }
            }
        }
        let left_type = resolve_operator_type_oid(&catalog, &stmt.left_arg)?;
        let right_type = resolve_operator_type_oid(&catalog, &stmt.right_arg)?;
        let current = lookup_operator_row(
            self,
            client_id,
            Some((xid, cid)),
            &stmt.operator_name,
            left_type,
            right_type,
        )?
        .ok_or_else(|| {
            ExecError::Parse(ParseError::DetailedError {
                message: format!(
                    "operator does not exist: {}",
                    operator_signature_display(&stmt.operator_name, left_type, right_type)
                ),
                detail: None,
                hint: None,
                sqlstate: "42883",
            })
        })?;
        if catalog.current_user_oid() != BOOTSTRAP_SUPERUSER_OID
            && catalog.current_user_oid() != current.oprowner
        {
            return Err(owner_error(&stmt.operator_name));
        }

        let mut updated = current.clone();
        let mut partner_updates = Vec::new();
        for option in &stmt.options {
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
        let left_type = resolve_operator_type_oid(&catalog, &stmt.left_arg)?;
        let right_type = resolve_operator_type_oid(&catalog, &stmt.right_arg)?;
        let Some(row) = lookup_operator_row(
            self,
            client_id,
            Some((xid, cid)),
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
                    operator_signature_display(&stmt.operator_name, left_type, right_type)
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
