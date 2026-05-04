use super::super::*;
use crate::backend::executor::expr_reg::format_type_text;
use crate::backend::parser::{
    CatalogLookup, CreateCastMethod, CreateCastStatement, DropCastStatement, ParseError,
    RawTypeName,
};
use crate::backend::utils::misc::notices::{push_notice, push_warning};
use crate::include::catalog::{PG_CATALOG_NAMESPACE_OID, PgCastRow, PgProcRow};
use pgrust_commands::cast::{
    cast_context_code, maybe_type_dependency, resolve_cast_type_oid, resolve_cast_type_row,
    validate_binary_cast_physical_compatibility, validate_cast_function,
};

fn cast_command_error_to_exec(error: pgrust_commands::cast::CastCommandError) -> ExecError {
    match error {
        pgrust_commands::cast::CastCommandError::Parse(error) => ExecError::Parse(error),
        pgrust_commands::cast::CastCommandError::Detailed {
            message,
            detail,
            hint,
            sqlstate,
        } => ExecError::DetailedError {
            message,
            detail,
            hint,
            sqlstate,
        },
    }
}

fn cast_display(catalog: &dyn CatalogLookup, source_oid: u32, target_oid: u32) -> String {
    format!(
        "cast from type {} to type {}",
        format_type_text(source_oid, None, catalog),
        format_type_text(target_oid, None, catalog)
    )
}

fn raw_cast_type_display_name(raw: &RawTypeName) -> String {
    match raw {
        RawTypeName::Builtin(sql_type) => {
            crate::pgrust::database::ddl::format_sql_type_name(*sql_type)
        }
        RawTypeName::Serial(kind) => match kind {
            crate::include::nodes::parsenodes::SerialKind::Small => "smallserial".into(),
            crate::include::nodes::parsenodes::SerialKind::Regular => "serial".into(),
            crate::include::nodes::parsenodes::SerialKind::Big => "bigserial".into(),
        },
        RawTypeName::Named { name, array_bounds } => {
            let mut display = name.clone();
            for _ in 0..*array_bounds {
                display.push_str("[]");
            }
            display
        }
        RawTypeName::Record => "record".into(),
    }
}

fn missing_schema_for_cast_type(catalog: &dyn CatalogLookup, raw: &RawTypeName) -> Option<String> {
    let RawTypeName::Named { name, .. } = raw else {
        return None;
    };
    let (schema_name, _) = name.split_once('.')?;
    let schema_name = schema_name.trim_matches('"').replace("\"\"", "\"");
    if schema_name.eq_ignore_ascii_case("pg_catalog")
        || catalog
            .namespace_rows()
            .into_iter()
            .any(|row| row.nspname.eq_ignore_ascii_case(&schema_name))
    {
        None
    } else {
        Some(schema_name)
    }
}

fn push_missing_cast_type_notice(catalog: &dyn CatalogLookup, raw: &RawTypeName) {
    if let Some(schema_name) = missing_schema_for_cast_type(catalog, raw) {
        push_notice(format!("schema \"{schema_name}\" does not exist, skipping"));
    } else {
        push_notice(format!(
            "type \"{}\" does not exist, skipping",
            raw_cast_type_display_name(raw)
        ));
    }
}

fn missing_cast_type_notice_pushed(catalog: &dyn CatalogLookup, raw: &RawTypeName) -> bool {
    if let Some(schema_name) = missing_schema_for_cast_type(catalog, raw) {
        push_notice(format!("schema \"{schema_name}\" does not exist, skipping"));
        return true;
    }
    match resolve_cast_type_oid(catalog, raw) {
        Ok(_) => false,
        Err(pgrust_commands::cast::CastCommandError::Parse(ParseError::UnsupportedType(_))) => {
            push_missing_cast_type_notice(catalog, raw);
            true
        }
        Err(_) => false,
    }
}

fn resolve_cast_function_row(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    catalog: &dyn CatalogLookup,
    schema_name: Option<&str>,
    function_name: &str,
    arg_types: &[RawTypeName],
    configured_search_path: Option<&[String]>,
) -> Result<PgProcRow, ExecError> {
    let namespace_oid = match schema_name {
        Some(schema_name) => Some(
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
    let arg_oids = arg_types
        .iter()
        .map(|arg| resolve_cast_type_oid(catalog, arg).map_err(cast_command_error_to_exec))
        .collect::<Result<Vec<_>, _>>()?;
    let mut matches = catalog
        .proc_rows_by_name(function_name)
        .into_iter()
        .filter(|row| row.prokind == 'f')
        .filter_map(|row| {
            cast_function_namespace_rank(catalog, &row, namespace_oid).map(|rank| (rank, row))
        })
        .filter(|row| {
            row.1
                .proargtypes
                .split_whitespace()
                .filter_map(|oid| oid.parse::<u32>().ok())
                .eq(arg_oids.iter().copied())
        })
        .collect::<Vec<_>>();
    matches.sort_by_key(|(rank, row)| (*rank, row.oid));
    match matches.as_slice() {
        [(rank, row)] | [(rank, row), ..]
            if matches
                .iter()
                .filter(|(candidate_rank, _)| candidate_rank == rank)
                .count()
                == 1 =>
        {
            Ok(row.clone())
        }
        [] => {
            let _ = configured_search_path;
            Err(ExecError::DetailedError {
                message: format!("function {function_name} does not exist"),
                detail: None,
                hint: None,
                sqlstate: "42883",
            })
        }
        _ => Err(ExecError::DetailedError {
            message: format!("function name {function_name} is ambiguous"),
            detail: None,
            hint: None,
            sqlstate: "42725",
        }),
    }
}

fn cast_function_namespace_rank(
    catalog: &dyn CatalogLookup,
    row: &PgProcRow,
    namespace_oid: Option<u32>,
) -> Option<usize> {
    if let Some(namespace_oid) = namespace_oid {
        return (row.pronamespace == namespace_oid).then_some(0);
    }
    if cast_namespace_is_temp(catalog, row.pronamespace) {
        return None;
    }
    if row.pronamespace == PG_CATALOG_NAMESPACE_OID {
        return Some(0);
    }

    let mut rank = 1usize;
    for schema in catalog.search_path() {
        if matches!(schema.as_str(), "" | "$user" | "pg_temp" | "pg_catalog") {
            continue;
        }
        let Some(namespace) = catalog.namespace_row_by_name(&schema) else {
            continue;
        };
        if cast_namespace_is_temp(catalog, namespace.oid) {
            continue;
        }
        if namespace.oid == row.pronamespace {
            return Some(rank);
        }
        rank = rank.saturating_add(1);
    }
    None
}

fn cast_namespace_is_temp(catalog: &dyn CatalogLookup, namespace_oid: u32) -> bool {
    catalog
        .namespace_row_by_oid(namespace_oid)
        .map(|row| row.nspname)
        .is_some_and(|name| {
            name.eq_ignore_ascii_case("pg_temp")
                || name.to_ascii_lowercase().starts_with("pg_temp_")
        })
}

impl Database {
    pub(crate) fn execute_create_cast_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateCastStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_cast_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[])
    }

    pub(crate) fn execute_create_cast_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateCastStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let source_row = resolve_cast_type_row(&catalog, &stmt.source_type)
            .map_err(cast_command_error_to_exec)?;
        let target_row = resolve_cast_type_row(&catalog, &stmt.target_type)
            .map_err(cast_command_error_to_exec)?;
        if source_row.oid == target_row.oid {
            return Err(ExecError::DetailedError {
                message: "source data type and target data type are the same".into(),
                detail: None,
                hint: None,
                sqlstate: "42P17",
            });
        }
        self.ensure_type_usage_privilege_by_oid(
            client_id,
            Some((xid, cid)),
            configured_search_path,
            source_row.oid,
        )?;
        self.ensure_type_usage_privilege_by_oid(
            client_id,
            Some((xid, cid)),
            configured_search_path,
            target_row.oid,
        )?;
        if catalog
            .cast_by_source_target(source_row.oid, target_row.oid)
            .is_some()
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "{} already exists",
                    cast_display(&catalog, source_row.oid, target_row.oid)
                ),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }
        if source_row.typtype == 'd' {
            push_warning("cast will be ignored because the source data type is a domain");
        }

        let mut depends = Vec::new();
        if let Some(depend) = maybe_type_dependency(source_row.oid) {
            depends.push(depend);
        }
        if let Some(depend) = maybe_type_dependency(target_row.oid) {
            depends.push(depend);
        }

        let (castfunc, castmethod) = match &stmt.method {
            CreateCastMethod::WithoutFunction => {
                validate_binary_cast_physical_compatibility(
                    &catalog,
                    &source_row,
                    &target_row,
                    |oid| format_type_text(oid, None, &catalog),
                )
                .map_err(cast_command_error_to_exec)?;
                (0, 'b')
            }
            CreateCastMethod::InOut => (0, 'i'),
            CreateCastMethod::Function {
                schema_name,
                function_name,
                arg_types,
            } => {
                let proc_row = resolve_cast_function_row(
                    self,
                    client_id,
                    Some((xid, cid)),
                    &catalog,
                    schema_name.as_deref(),
                    function_name,
                    arg_types,
                    configured_search_path,
                )?;
                depends.extend(
                    validate_cast_function(&catalog, &proc_row, source_row.oid, target_row.oid)
                        .map_err(cast_command_error_to_exec)?,
                );
                (proc_row.oid, 'f')
            }
        };

        let cast_row = PgCastRow {
            oid: 0,
            castsource: source_row.oid,
            casttarget: target_row.oid,
            castfunc,
            castcontext: cast_context_code(stmt.context),
            castmethod,
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
        let effect = self
            .catalog
            .write()
            .create_cast_mvcc(cast_row, depends, &ctx)
            .map(|(_, effect)| effect)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_cast_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropCastStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_cast_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[])
    }

    pub(crate) fn execute_drop_cast_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropCastStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let source_oid = match resolve_cast_type_oid(&catalog, &stmt.source_type)
            .map_err(cast_command_error_to_exec)
        {
            Ok(type_oid) => type_oid,
            Err(err) if stmt.if_exists => {
                if missing_cast_type_notice_pushed(&catalog, &stmt.source_type) {
                    return Ok(StatementResult::AffectedRows(0));
                }
                return Err(err);
            }
            Err(err) => return Err(err),
        };
        let target_oid = match resolve_cast_type_oid(&catalog, &stmt.target_type)
            .map_err(cast_command_error_to_exec)
        {
            Ok(type_oid) => type_oid,
            Err(err) if stmt.if_exists => {
                if missing_cast_type_notice_pushed(&catalog, &stmt.target_type) {
                    return Ok(StatementResult::AffectedRows(0));
                }
                return Err(err);
            }
            Err(err) => return Err(err),
        };
        let Some(cast_row) = catalog.cast_by_source_target(source_oid, target_oid) else {
            if stmt.if_exists {
                push_notice(format!(
                    "{} does not exist, skipping",
                    cast_display(&catalog, source_oid, target_oid)
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!(
                    "{} does not exist",
                    cast_display(&catalog, source_oid, target_oid)
                ),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        };
        let _ = stmt.cascade;
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .drop_cast_by_oid_mvcc(cast_row.oid, &ctx)
            .map(|(_, effect)| effect)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}
