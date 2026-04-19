use super::super::*;
use crate::backend::parser::{
    CatalogLookup, CreateOperatorClassItem, CreateOperatorClassStatement, ParseError,
    resolve_raw_type_name,
};
use crate::backend::utils::cache::lsyscache::access_method_row_by_name;
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, PUBLIC_NAMESPACE_OID, PgAmopRow, PgAmprocRow, PgOpclassRow,
    PgOpfamilyRow,
};

fn normalize_create_opclass_name_for_search_path(
    stmt: &CreateOperatorClassStatement,
    configured_search_path: Option<&[String]>,
) -> Result<(String, u32), ParseError> {
    let normalized = stmt.opclass_name.to_ascii_lowercase();
    match stmt.schema_name.as_deref().map(str::to_ascii_lowercase) {
        Some(schema) if schema == "public" => Ok((normalized, PUBLIC_NAMESPACE_OID)),
        Some(schema) if schema == "pg_temp" => Err(ParseError::UnexpectedToken {
            expected: "permanent operator class",
            actual: "temporary operator class".into(),
        }),
        Some(schema) => Err(ParseError::UnsupportedQualifiedName(format!(
            "{schema}.{}",
            stmt.opclass_name
        ))),
        None => {
            let search_path = configured_search_path
                .map(|path| {
                    path.iter()
                        .map(|s| s.trim().to_ascii_lowercase())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| vec!["public".into()]);
            for schema in search_path {
                match schema.as_str() {
                    "" | "$user" | "pg_temp" | "pg_catalog" => continue,
                    "public" => return Ok((normalized, PUBLIC_NAMESPACE_OID)),
                    _ => continue,
                }
            }
            Err(ParseError::NoSchemaSelectedForCreate)
        }
    }
}

fn resolve_proc_oid(
    catalog: &dyn CatalogLookup,
    schema_name: Option<&str>,
    function_name: &str,
    arg_type_oids: &[u32],
) -> Result<u32, ExecError> {
    let desired = arg_type_oids
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    let rows = catalog.proc_rows_by_name(function_name);
    rows.into_iter()
        .find(|row| {
            row.proname.eq_ignore_ascii_case(function_name)
                && row.proargtypes == desired
                && schema_name
                    .map(|schema| {
                        (schema.eq_ignore_ascii_case("public")
                            && row.pronamespace == PUBLIC_NAMESPACE_OID)
                            || schema.eq_ignore_ascii_case("pg_catalog")
                    })
                    .unwrap_or(true)
        })
        .map(|row| row.oid)
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "existing support function",
                actual: format!("function {}({}) does not exist", function_name, desired),
            })
        })
}

impl Database {
    pub(crate) fn execute_create_operator_class_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateOperatorClassStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_operator_class_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[])
    }

    pub(crate) fn execute_create_operator_class_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateOperatorClassStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let (opclass_name, namespace_oid) =
            normalize_create_opclass_name_for_search_path(stmt, configured_search_path)?;
        let access_method =
            access_method_row_by_name(self, client_id, Some((xid, cid)), &stmt.access_method)
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "supported access method",
                        actual: format!("USING {}", stmt.access_method),
                    })
                })?;
        if access_method.amtype != 'i' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "index access method",
                actual: format!("USING {}", stmt.access_method),
            }));
        }
        let input_type =
            resolve_raw_type_name(&stmt.data_type, &catalog).map_err(ExecError::Parse)?;
        let input_type_oid = catalog.type_oid_for_sql_type(input_type).ok_or_else(|| {
            ExecError::Parse(ParseError::UnsupportedType(format!("{:?}", stmt.data_type)))
        })?;

        let existing = crate::backend::utils::cache::syscache::ensure_opclass_rows(
            self,
            client_id,
            Some((xid, cid)),
        )
        .into_iter()
        .find(|row| {
            row.opcmethod == access_method.oid
                && row.opcnamespace == namespace_oid
                && row.opcname.eq_ignore_ascii_case(&opclass_name)
        });
        if existing.is_some() {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "new operator class",
                actual: format!(
                    "operator class {} for access method {} already exists",
                    opclass_name, stmt.access_method
                ),
            }));
        }

        let mut amop_rows = Vec::new();
        let mut amproc_rows = Vec::new();
        for item in &stmt.items {
            match item {
                CreateOperatorClassItem::Operator {
                    strategy_number,
                    operator_name,
                } => {
                    let operator = catalog
                        .operator_by_name_left_right(operator_name, input_type_oid, input_type_oid)
                        .ok_or_else(|| {
                            ExecError::Parse(ParseError::UnexpectedToken {
                                expected: "existing operator",
                                actual: format!(
                                    "operator {} for type oid {} does not exist",
                                    operator_name, input_type_oid
                                ),
                            })
                        })?;
                    amop_rows.push(PgAmopRow {
                        oid: 0,
                        amopfamily: 0,
                        amoplefttype: input_type_oid,
                        amoprighttype: input_type_oid,
                        amopstrategy: *strategy_number,
                        amoppurpose: 's',
                        amopopr: operator.oid,
                        amopmethod: access_method.oid,
                        amopsortfamily: 0,
                    });
                }
                CreateOperatorClassItem::Function {
                    support_number,
                    schema_name,
                    function_name,
                    arg_types,
                } => {
                    let arg_type_oids = arg_types
                        .iter()
                        .map(|ty| {
                            resolve_raw_type_name(ty, &catalog)
                                .map_err(ExecError::Parse)
                                .and_then(|sql_type| {
                                    catalog.type_oid_for_sql_type(sql_type).ok_or_else(|| {
                                        ExecError::Parse(ParseError::UnsupportedType(format!(
                                            "{:?}",
                                            ty
                                        )))
                                    })
                                })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let proc_oid = resolve_proc_oid(
                        &catalog,
                        schema_name.as_deref(),
                        function_name,
                        &arg_type_oids,
                    )?;
                    amproc_rows.push(PgAmprocRow {
                        oid: 0,
                        amprocfamily: 0,
                        amproclefttype: input_type_oid,
                        amprocrighttype: input_type_oid,
                        amprocnum: *support_number,
                        amproc: proc_oid,
                    });
                }
            }
        }

        let opfamily_row = PgOpfamilyRow {
            oid: 0,
            opfmethod: access_method.oid,
            opfname: opclass_name.clone(),
            opfnamespace: namespace_oid,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        };
        let opclass_row = PgOpclassRow {
            oid: 0,
            opcmethod: access_method.oid,
            opcname: opclass_name,
            opcnamespace: namespace_oid,
            opcowner: BOOTSTRAP_SUPERUSER_OID,
            opcfamily: 0,
            opcintype: input_type_oid,
            opcdefault: stmt.is_default,
            opckeytype: 0,
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
        let (_opclass_oid, effect) = {
            let mut catalog_guard = self.catalog.write();
            catalog_guard.create_operator_class_mvcc(
                opfamily_row,
                opclass_row,
                amop_rows,
                amproc_rows,
                &ctx,
            )?
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}
