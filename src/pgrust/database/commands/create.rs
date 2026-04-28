use super::super::*;
use super::privilege::{acl_grants_privilege, type_owner_default_acl};
use super::reloptions::normalize_create_table_reloptions;
use crate::backend::commands::partition::validate_new_partition_bound;
use crate::backend::parser::analyze::{
    ResolvedFunctionCall, is_binary_coercible_type, resolve_function_call,
};
use crate::backend::parser::{
    AggregateArgType, AggregateSignature, AggregateSignatureArg, AggregateSignatureKind,
    AlterAggregateRenameStatement, AlterRoutineOption, CreateAggregateStatement, CreateFunctionArg,
    CreateFunctionReturnSpec, CreateFunctionStatement, CreateProcedureStatement,
    CreateTableAsQuery, FunctionArgMode, FunctionParallel, FunctionVolatility, OwnedSequenceSpec,
    PartitionBoundSpec, RawTypeName, RelOption, RoutineSignature, SqlType, SqlTypeKind, Statement,
    parse_statement, pg_partitioned_table_row, resolve_raw_type_name, serialize_partition_bound,
};
use crate::backend::rewrite::render_view_query_sql;
use crate::backend::utils::cache::syscache::{
    SearchSysCache1, SearchSysCacheList1, SysCacheId, SysCacheTuple,
};
use crate::backend::utils::misc::guc::normalize_guc_name;
use crate::backend::utils::misc::notices::{
    push_backend_notice, push_notice, push_notice_with_detail,
};
use crate::include::catalog::{
    ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID, ANYCOMPATIBLENONARRAYOID,
    ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID, ANYMULTIRANGEOID,
    ANYNONARRAYOID, ANYOID, ANYRANGEOID, BYTEA_TYPE_OID, EVENT_TRIGGER_TYPE_OID, INTERNAL_TYPE_OID,
    PG_CATALOG_NAMESPACE_OID, PG_LANGUAGE_C_OID, PG_LANGUAGE_INTERNAL_OID, PG_LANGUAGE_PLPGSQL_OID,
    PG_LANGUAGE_SQL_OID, PgAggregateRow, PgAuthIdRow, PgAuthMembersRow, PgProcRow, RECORD_TYPE_OID,
    VOID_TYPE_OID,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::{
    AliasColumnSpec, ForeignKeyAction, ForeignKeyMatchType, FromItem, Query, RangeTblEntryKind,
    SelectStatement,
};
use crate::include::nodes::primnodes::{
    Expr, QueryColumn, RelationDesc, RowsFromSource, ScalarFunctionImpl, SetReturningCall,
    SqlJsonTableBehavior, SqlJsonTableColumnKind, SqlXmlTableColumnKind, Var, attrno_index,
};
use crate::pgrust::database::ddl::{append_view_check_option, format_sql_type_name};
use crate::pgrust::database::sequences::pg_sequence_row;
use crate::pgrust::database::{
    DomainConstraintEntry, DomainConstraintKind, SequenceData, SequenceRuntime,
    default_sequence_name_base, format_nextval_default_oid, initial_sequence_state,
    resolve_sequence_options_spec, sequence_type_oid_for_serial_kind,
};
use crate::pl::plpgsql::validate_create_function_body_with_options;

#[derive(Debug, Clone, Copy)]
pub(super) struct CreatedOwnedSequence {
    pub(super) column_index: usize,
    pub(super) sequence_oid: u32,
}

struct EffectiveTypeAclGrantees {
    names: std::collections::BTreeSet<String>,
    is_superuser: bool,
}

fn constraint_index_columns_with_expr_types(
    action: &crate::backend::parser::IndexBackedConstraintAction,
    relation: &crate::backend::parser::BoundRelation,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<crate::backend::parser::IndexColumnDef>, ExecError> {
    let mut columns = if action.index_columns.is_empty() {
        action
            .columns
            .iter()
            .cloned()
            .map(crate::backend::parser::IndexColumnDef::from)
            .collect::<Vec<_>>()
    } else {
        action.index_columns.clone()
    };
    for column in &mut columns {
        if let Some(expr_sql) = column.expr_sql.as_deref()
            && column.expr_type.is_none()
        {
            column.expr_type = Some(
                crate::backend::parser::infer_relation_expr_sql_type(
                    expr_sql,
                    None,
                    &relation.desc,
                    catalog,
                )
                .map_err(ExecError::Parse)?,
            );
        }
    }
    Ok(columns)
}

fn validate_sql_procedure_body(
    create_stmt: &CreateProcedureStatement,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    if !create_stmt.language.eq_ignore_ascii_case("sql") {
        return Ok(());
    }
    if create_stmt.sql_standard_body && sql_body_contains_create_table(&create_stmt.body) {
        return Err(ExecError::DetailedError {
            message: "CREATE TABLE is not yet supported in unquoted SQL function body".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    for stmt_sql in split_sql_body_statements(&create_stmt.body)? {
        let Ok(Statement::Call(call_stmt)) = parse_statement(&stmt_sql) else {
            continue;
        };
        if call_targets_procedure_with_output_args(&call_stmt, catalog) {
            return Err(ExecError::WithContext {
                source: Box::new(ExecError::DetailedError {
                    message:
                        "calling procedures with output arguments is not supported in SQL functions"
                            .into(),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                }),
                context: format!("SQL function \"{}\"", create_stmt.procedure_name),
            });
        }
    }
    Ok(())
}

fn proc_config_from_options(options: &[AlterRoutineOption]) -> Option<Vec<String>> {
    let mut config = Vec::<String>::new();
    for option in options {
        match option {
            AlterRoutineOption::SetConfig { name, value } => {
                let normalized = normalize_guc_name(name);
                config.retain(|entry| {
                    entry
                        .split_once('=')
                        .map(|(entry_name, _)| !entry_name.eq_ignore_ascii_case(&normalized))
                        .unwrap_or(true)
                });
                config.push(format!("{normalized}={value}"));
            }
            AlterRoutineOption::ResetConfig(name) => {
                let normalized = normalize_guc_name(name);
                config.retain(|entry| {
                    entry
                        .split_once('=')
                        .map(|(entry_name, _)| !entry_name.eq_ignore_ascii_case(&normalized))
                        .unwrap_or(true)
                });
            }
            AlterRoutineOption::ResetAll => config.clear(),
            _ => {}
        }
    }
    (!config.is_empty()).then_some(config)
}

fn sql_body_contains_create_table(body: &str) -> bool {
    split_sql_body_statements(body).is_ok_and(|statements| {
        statements.into_iter().any(|stmt| {
            stmt.split_whitespace()
                .map(str::to_ascii_lowercase)
                .collect::<Vec<_>>()
                .windows(2)
                .any(|words| words == ["create", "table"])
        })
    })
}

fn call_targets_procedure_with_output_args(
    call_stmt: &crate::backend::parser::CallStatement,
    catalog: &dyn CatalogLookup,
) -> bool {
    let actual_count = call_stmt.raw_arg_sql.len();
    catalog
        .proc_rows_by_name(&call_stmt.procedure_name)
        .into_iter()
        .filter(|row| row.prokind == 'p')
        .filter(|row| {
            call_stmt.schema_name.as_deref().is_none_or(|schema_name| {
                catalog
                    .namespace_row_by_oid(row.pronamespace)
                    .is_some_and(|namespace| namespace.nspname.eq_ignore_ascii_case(schema_name))
            })
        })
        .filter(|row| procedure_accepts_arg_count(row, actual_count))
        .any(|row| procedure_has_output_args(&row))
}

fn procedure_accepts_arg_count(row: &PgProcRow, actual: usize) -> bool {
    let input_count = row.pronargs.max(0) as usize;
    let all_count = row
        .proallargtypes
        .as_ref()
        .map(Vec::len)
        .unwrap_or(input_count);
    actual == input_count || actual == all_count
}

fn procedure_has_output_args(row: &PgProcRow) -> bool {
    row.proargmodes
        .as_deref()
        .is_some_and(|modes| modes.iter().any(|mode| matches!(*mode, b'o' | b'b')))
}

pub(super) fn describe_select_query_without_planning(
    stmt: &crate::backend::parser::SelectStatement,
    catalog: &dyn CatalogLookup,
) -> Result<(Vec<QueryColumn>, Vec<String>), ExecError> {
    let (query, _) = crate::backend::parser::analyze_select_query_with_outer(
        stmt,
        catalog,
        &[],
        None,
        None,
        &[],
        &[],
    )?;
    let mut rewritten = crate::backend::rewrite::pg_rewrite_query(query, catalog)?;
    if rewritten.len() != 1 {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "single rewritten SELECT query",
            actual: format!("{} queries", rewritten.len()),
        }));
    }
    let query = rewritten.remove(0);
    Ok((query.columns(), query.column_names()))
}

fn split_sql_body_statements(body: &str) -> Result<Vec<String>, ExecError> {
    let body = sql_standard_body_inner(body).unwrap_or(body);
    let mut statements = Vec::new();
    let mut start = 0usize;
    let bytes = body.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => i = scan_sql_delimited_end(bytes, i, b'\'')?,
            b'"' => i = scan_sql_delimited_end(bytes, i, b'"')?,
            b'$' => {
                if let Some(end) = scan_sql_dollar_string_end(body, i) {
                    i = end;
                }
            }
            b';' => {
                let statement = body[start..i].trim();
                if !statement.is_empty() {
                    statements.push(statement.to_string());
                }
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    let statement = body[start..].trim();
    if !statement.is_empty() && !statement.eq_ignore_ascii_case("end") {
        statements.push(statement.to_string());
    }
    Ok(statements)
}

fn sql_standard_body_inner(body: &str) -> Option<&str> {
    let trimmed = body.trim();
    let lowered = trimmed.to_ascii_lowercase();
    if !lowered.starts_with("begin atomic") {
        return None;
    }
    let without_trailing_semicolon = trimmed.trim_end_matches(';').trim_end();
    let lowered_without_semicolon = without_trailing_semicolon.to_ascii_lowercase();
    let end = if lowered_without_semicolon.ends_with("end") {
        without_trailing_semicolon.len().saturating_sub("end".len())
    } else {
        trimmed.len()
    };
    trimmed.get("begin atomic".len()..end).map(str::trim)
}

fn scan_sql_delimited_end(bytes: &[u8], start: usize, delimiter: u8) -> Result<usize, ExecError> {
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == delimiter {
            if i + 1 < bytes.len() && bytes[i + 1] == delimiter {
                i += 2;
                continue;
            }
            return Ok(i + 1);
        }
        i += 1;
    }
    Err(ExecError::Parse(ParseError::UnexpectedEof))
}

fn scan_sql_dollar_string_end(input: &str, start: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut tag_end = start + 1;
    while tag_end < bytes.len() {
        let byte = bytes[tag_end];
        if byte == b'$' {
            break;
        }
        if !(byte.is_ascii_alphanumeric() || byte == b'_') {
            return None;
        }
        tag_end += 1;
    }
    if bytes.get(tag_end) != Some(&b'$') {
        return None;
    }
    let tag = &input[start..=tag_end];
    input[tag_end + 1..]
        .find(tag)
        .map(|offset| tag_end + 1 + offset + tag.len())
}

fn relation_exists_in_namespace(
    catalog: &dyn CatalogLookup,
    name: &str,
    namespace_oid: u32,
) -> bool {
    catalog
        .lookup_any_relation(name)
        .is_some_and(|relation| relation.namespace_oid == namespace_oid)
}

fn created_relkind(lowered: &crate::backend::parser::LoweredCreateTable) -> char {
    if lowered.partition_spec.is_some() {
        'p'
    } else {
        'r'
    }
}

fn relation_persistence_char(persistence: TablePersistence) -> char {
    match persistence {
        TablePersistence::Permanent => 'p',
        TablePersistence::Temporary => 't',
        // :HACK: Unlogged tables currently use the normal heap storage path;
        // catalog relpersistence is preserved for SQL-visible compatibility.
        TablePersistence::Unlogged => 'u',
    }
}

fn validate_partitioned_table_ddl(
    table_name: &str,
    lowered: &crate::backend::parser::LoweredCreateTable,
) -> Result<(), ExecError> {
    let _ = (table_name, lowered);
    Ok(())
}

fn validate_create_or_replace_view_columns(
    old_desc: &crate::backend::executor::RelationDesc,
    new_desc: &crate::backend::executor::RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    if old_desc.columns.len() > new_desc.columns.len() {
        return Err(ExecError::DetailedError {
            message: "cannot drop columns from view".into(),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }

    for (old_column, new_column) in old_desc.columns.iter().zip(new_desc.columns.iter()) {
        if !old_column.name.eq_ignore_ascii_case(&new_column.name) {
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot change name of view column \"{}\" to \"{}\"",
                    old_column.name, new_column.name
                ),
                detail: None,
                hint: Some(
                    "Use ALTER VIEW ... RENAME COLUMN ... to change name of view column instead."
                        .into(),
                ),
                sqlstate: "42P16",
            });
        }
        if old_column.sql_type != new_column.sql_type {
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot change data type of view column \"{}\" from {} to {}",
                    old_column.name,
                    format_sql_type_name(old_column.sql_type),
                    format_sql_type_name(new_column.sql_type)
                ),
                detail: None,
                hint: None,
                sqlstate: "42P16",
            });
        }
        if old_column.collation_oid != new_column.collation_oid {
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot change collation of view column \"{}\" from \"{}\" to \"{}\"",
                    old_column.name,
                    collation_name(catalog, old_column.collation_oid),
                    collation_name(catalog, new_column.collation_oid)
                ),
                detail: None,
                hint: None,
                sqlstate: "42P16",
            });
        }
    }

    Ok(())
}

fn apply_create_view_column_names(
    desc: &mut crate::backend::executor::RelationDesc,
    column_names: &[String],
) -> Result<(), ExecError> {
    if column_names.len() > desc.columns.len() {
        return Err(ExecError::DetailedError {
            message: "CREATE VIEW specifies more column names than columns".into(),
            detail: None,
            hint: None,
            sqlstate: "42P16",
        });
    }
    for (column, name) in desc.columns.iter_mut().zip(column_names.iter()) {
        column.name = name.clone();
    }
    Ok(())
}

fn apply_create_view_column_names_to_query(query: &mut Query, column_names: &[String]) {
    for (target, name) in query
        .target_list
        .iter_mut()
        .filter(|target| !target.resjunk)
        .zip(column_names.iter())
    {
        target.name = name.clone();
    }
}

fn collect_rule_dependencies_from_query(
    query: &Query,
    catalog: &dyn CatalogLookup,
    deps: &mut crate::backend::catalog::store::RuleDependencies,
) {
    for rte in &query.rtable {
        for qual in &rte.security_quals {
            collect_expr_rule_dependencies(qual, query, catalog, deps);
        }
        match &rte.kind {
            RangeTblEntryKind::Relation { relation_oid, .. } => {
                deps.relation_oids.push(*relation_oid);
            }
            RangeTblEntryKind::Join { joinaliasvars, .. } => {
                for expr in joinaliasvars {
                    collect_expr_rule_dependencies(expr, query, catalog, deps);
                }
            }
            RangeTblEntryKind::Values { rows, .. } => {
                for expr in rows.iter().flatten() {
                    collect_expr_rule_dependencies(expr, query, catalog, deps);
                }
            }
            RangeTblEntryKind::Function { call } => {
                collect_set_returning_call_rule_dependencies(call, query, catalog, deps);
            }
            RangeTblEntryKind::Cte { query, .. } | RangeTblEntryKind::Subquery { query } => {
                collect_rule_dependencies_from_query(query, catalog, deps);
            }
            RangeTblEntryKind::Result | RangeTblEntryKind::WorkTable { .. } => {}
        }
    }
    for target in &query.target_list {
        collect_expr_rule_dependencies(&target.expr, query, catalog, deps);
        collect_sql_type_rule_dependency(target.sql_type, deps);
    }
    for expr in query
        .where_qual
        .iter()
        .chain(query.having_qual.iter())
        .chain(query.group_by.iter())
    {
        collect_expr_rule_dependencies(expr, query, catalog, deps);
    }
    for sort in &query.sort_clause {
        collect_expr_rule_dependencies(&sort.expr, query, catalog, deps);
    }
    for accumulator in &query.accumulators {
        if accumulator.aggfnoid != 0 {
            deps.proc_oids.push(accumulator.aggfnoid);
        }
        collect_sql_type_rule_dependency(accumulator.sql_type, deps);
        for expr in accumulator
            .direct_args
            .iter()
            .chain(accumulator.args.iter())
            .chain(accumulator.filter.iter())
        {
            collect_expr_rule_dependencies(expr, query, catalog, deps);
        }
        for order in &accumulator.order_by {
            collect_expr_rule_dependencies(&order.expr, query, catalog, deps);
        }
    }
    for clause in &query.window_clauses {
        for expr in &clause.spec.partition_by {
            collect_expr_rule_dependencies(expr, query, catalog, deps);
        }
        for order in &clause.spec.order_by {
            collect_expr_rule_dependencies(&order.expr, query, catalog, deps);
        }
        for func in &clause.functions {
            for expr in &func.args {
                collect_expr_rule_dependencies(expr, query, catalog, deps);
            }
            collect_sql_type_rule_dependency(func.result_type, deps);
        }
    }
    if let Some(recursive) = &query.recursive_union {
        collect_rule_dependencies_from_query(&recursive.anchor, catalog, deps);
        collect_rule_dependencies_from_query(&recursive.recursive, catalog, deps);
    }
    if let Some(set_operation) = &query.set_operation {
        for input in &set_operation.inputs {
            collect_rule_dependencies_from_query(input, catalog, deps);
        }
    }
}

fn collect_expr_rule_dependencies(
    expr: &Expr,
    query: &Query,
    catalog: &dyn CatalogLookup,
    deps: &mut crate::backend::catalog::store::RuleDependencies,
) {
    match expr {
        Expr::Var(var) => collect_var_rule_dependency(var, query, catalog, deps),
        Expr::Aggref(aggref) => {
            if aggref.aggfnoid != 0 {
                deps.proc_oids.push(aggref.aggfnoid);
            }
            collect_sql_type_rule_dependency(aggref.aggtype, deps);
            for expr in aggref
                .direct_args
                .iter()
                .chain(aggref.args.iter())
                .chain(aggref.aggfilter.iter())
            {
                collect_expr_rule_dependencies(expr, query, catalog, deps);
            }
            for order in &aggref.aggorder {
                collect_expr_rule_dependencies(&order.expr, query, catalog, deps);
            }
        }
        Expr::WindowFunc(window_func) => {
            if let crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) =
                &window_func.kind
            {
                if aggref.aggfnoid != 0 {
                    deps.proc_oids.push(aggref.aggfnoid);
                }
            }
            collect_sql_type_rule_dependency(window_func.result_type, deps);
            for expr in &window_func.args {
                collect_expr_rule_dependencies(expr, query, catalog, deps);
            }
        }
        Expr::Op(op) => {
            if op.opfuncid != 0 {
                deps.proc_oids.push(op.opfuncid);
            }
            collect_sql_type_rule_dependency(op.opresulttype, deps);
            for expr in &op.args {
                collect_expr_rule_dependencies(expr, query, catalog, deps);
            }
        }
        Expr::Bool(bool_expr) => {
            for expr in &bool_expr.args {
                collect_expr_rule_dependencies(expr, query, catalog, deps);
            }
        }
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                collect_expr_rule_dependencies(arg, query, catalog, deps);
            }
            for when in &case_expr.args {
                collect_expr_rule_dependencies(&when.expr, query, catalog, deps);
                collect_expr_rule_dependencies(&when.result, query, catalog, deps);
            }
            collect_expr_rule_dependencies(&case_expr.defresult, query, catalog, deps);
        }
        Expr::Func(func) => {
            if func.funcid != 0 {
                deps.proc_oids.push(func.funcid);
            }
            if let ScalarFunctionImpl::UserDefined { proc_oid } = func.implementation {
                deps.proc_oids.push(proc_oid);
            }
            if let Some(sql_type) = func.funcresulttype {
                collect_sql_type_rule_dependency(sql_type, deps);
            }
            for expr in &func.args {
                collect_expr_rule_dependencies(expr, query, catalog, deps);
            }
        }
        Expr::SqlJsonQueryFunction(func) => {
            collect_sql_type_rule_dependency(func.result_type, deps);
            for expr in func.child_exprs() {
                collect_expr_rule_dependencies(expr, query, catalog, deps);
            }
        }
        Expr::SetReturning(srf) => {
            collect_set_returning_call_rule_dependencies(&srf.call, query, catalog, deps);
            collect_sql_type_rule_dependency(srf.sql_type, deps);
        }
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = &sublink.testexpr {
                collect_expr_rule_dependencies(testexpr, query, catalog, deps);
            }
            collect_rule_dependencies_from_query(&sublink.subselect, catalog, deps);
        }
        Expr::SubPlan(subplan) => {
            if let Some(testexpr) = &subplan.testexpr {
                collect_expr_rule_dependencies(testexpr, query, catalog, deps);
            }
            if let Some(sql_type) = subplan.first_col_type {
                collect_sql_type_rule_dependency(sql_type, deps);
            }
            for expr in &subplan.args {
                collect_expr_rule_dependencies(expr, query, catalog, deps);
            }
        }
        Expr::ScalarArrayOp(saop) => {
            collect_expr_rule_dependencies(&saop.left, query, catalog, deps);
            collect_expr_rule_dependencies(&saop.right, query, catalog, deps);
        }
        Expr::Xml(xml) => {
            for expr in xml.child_exprs() {
                collect_expr_rule_dependencies(expr, query, catalog, deps);
            }
            if let Some(sql_type) = xml.target_type {
                collect_sql_type_rule_dependency(sql_type, deps);
            }
        }
        Expr::Cast(inner, sql_type) => {
            collect_expr_rule_dependencies(inner, query, catalog, deps);
            collect_sql_type_rule_dependency(*sql_type, deps);
        }
        Expr::Collate { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            collect_expr_rule_dependencies(expr, query, catalog, deps);
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_expr_rule_dependencies(expr, query, catalog, deps);
            collect_expr_rule_dependencies(pattern, query, catalog, deps);
            if let Some(escape) = escape {
                collect_expr_rule_dependencies(escape, query, catalog, deps);
            }
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            collect_expr_rule_dependencies(left, query, catalog, deps);
            collect_expr_rule_dependencies(right, query, catalog, deps);
        }
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => {
            collect_sql_type_rule_dependency(*array_type, deps);
            for expr in elements {
                collect_expr_rule_dependencies(expr, query, catalog, deps);
            }
        }
        Expr::Row { descriptor, fields } => {
            for field in &descriptor.fields {
                collect_sql_type_rule_dependency(field.sql_type, deps);
            }
            for (_, expr) in fields {
                collect_expr_rule_dependencies(expr, query, catalog, deps);
            }
        }
        Expr::ArraySubscript { array, subscripts } => {
            collect_expr_rule_dependencies(array, query, catalog, deps);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_expr_rule_dependencies(lower, query, catalog, deps);
                }
                if let Some(upper) = &subscript.upper {
                    collect_expr_rule_dependencies(upper, query, catalog, deps);
                }
            }
        }
        Expr::FieldSelect {
            expr, field_type, ..
        } => {
            collect_expr_rule_dependencies(expr, query, catalog, deps);
            collect_sql_type_rule_dependency(*field_type, deps);
        }
        Expr::Param(param) => collect_sql_type_rule_dependency(param.paramtype, deps),
        Expr::CaseTest(case_test) => collect_sql_type_rule_dependency(case_test.type_id, deps),
        Expr::Const(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => {}
    }
}

fn collect_var_rule_dependency(
    var: &Var,
    query: &Query,
    catalog: &dyn CatalogLookup,
    deps: &mut crate::backend::catalog::store::RuleDependencies,
) {
    if var.varlevelsup != 0 {
        return;
    }
    let Some(rte) = query.rtable.get(var.varno.saturating_sub(1)) else {
        return;
    };
    match &rte.kind {
        RangeTblEntryKind::Relation { relation_oid, .. } => {
            collect_attnum_dependency(*relation_oid, var.varattno, &rte.desc, deps);
        }
        RangeTblEntryKind::Function { call } => {
            if collect_set_returning_attnum_dependency(call, var.varattno, query, catalog, deps) {
                return;
            }
            if let Some(relation_oid) = set_returning_return_relation_oid(call, catalog, deps) {
                collect_attnum_dependency(relation_oid, var.varattno, &rte.desc, deps);
            }
        }
        RangeTblEntryKind::Join { joinaliasvars, .. } => {
            if let Some(index) = attrno_index(var.varattno)
                && let Some(expr) = joinaliasvars.get(index)
            {
                collect_expr_rule_dependencies(expr, query, catalog, deps);
            }
        }
        RangeTblEntryKind::Subquery { query: subquery }
        | RangeTblEntryKind::Cte {
            query: subquery, ..
        } => {
            if let Some(index) = attrno_index(var.varattno)
                && let Some(target) = subquery.target_list.get(index)
            {
                collect_expr_rule_dependencies(&target.expr, subquery, catalog, deps);
            }
        }
        RangeTblEntryKind::Result
        | RangeTblEntryKind::Values { .. }
        | RangeTblEntryKind::WorkTable { .. } => {}
    }
}

fn collect_set_returning_attnum_dependency(
    call: &SetReturningCall,
    varattno: i32,
    query: &Query,
    catalog: &dyn CatalogLookup,
    deps: &mut crate::backend::catalog::store::RuleDependencies,
) -> bool {
    let SetReturningCall::RowsFrom { items, .. } = call else {
        return false;
    };
    let Some(target_index) = attrno_index(varattno) else {
        if varattno == 0 {
            for item in items {
                collect_rows_from_item_attnum_dependencies(item, None, query, catalog, deps);
            }
            return true;
        }
        return false;
    };

    let mut base = 0usize;
    for item in items {
        let width = item.output_columns().len();
        if target_index < base + width {
            collect_rows_from_item_attnum_dependencies(
                item,
                Some(target_index - base),
                query,
                catalog,
                deps,
            );
            return true;
        }
        base += width;
    }
    false
}

fn collect_rows_from_item_attnum_dependencies(
    item: &crate::include::nodes::primnodes::RowsFromItem,
    target_index: Option<usize>,
    query: &Query,
    catalog: &dyn CatalogLookup,
    deps: &mut crate::backend::catalog::store::RuleDependencies,
) {
    match &item.source {
        RowsFromSource::Function(call) => {
            if let Some(relation_oid) = set_returning_return_relation_oid(call, catalog, deps)
                && let Some(relation) = catalog
                    .relation_by_oid(relation_oid)
                    .or_else(|| catalog.lookup_relation_by_oid(relation_oid))
            {
                match target_index {
                    Some(index) => {
                        let attno = index.saturating_add(1) as i32;
                        collect_attnum_dependency(relation_oid, attno, &relation.desc, deps);
                    }
                    None => collect_attnum_dependency(relation_oid, 0, &relation.desc, deps),
                }
            }
        }
        RowsFromSource::Project { output_exprs, .. } => match target_index {
            Some(index) => {
                if let Some(expr) = output_exprs.get(index) {
                    collect_expr_rule_dependencies(expr, &query, catalog, deps);
                }
            }
            None => {
                for expr in output_exprs {
                    collect_expr_rule_dependencies(expr, &query, catalog, deps);
                }
            }
        },
    }
}

fn collect_attnum_dependency(
    relation_oid: u32,
    varattno: i32,
    desc: &RelationDesc,
    deps: &mut crate::backend::catalog::store::RuleDependencies,
) {
    if varattno == 0 {
        for (index, column) in desc.columns.iter().enumerate() {
            if !column.dropped {
                deps.column_refs
                    .push((relation_oid, index.saturating_add(1) as i16));
            }
        }
        return;
    }
    if let Some(index) = attrno_index(varattno)
        && desc
            .columns
            .get(index)
            .is_some_and(|column| !column.dropped)
        && let Ok(attnum) = i16::try_from(varattno)
    {
        deps.column_refs.push((relation_oid, attnum));
    }
}

fn collect_set_returning_call_rule_dependencies(
    call: &SetReturningCall,
    query: &Query,
    catalog: &dyn CatalogLookup,
    deps: &mut crate::backend::catalog::store::RuleDependencies,
) {
    if let Some(proc_oid) = set_returning_proc_oid(call) {
        deps.proc_oids.push(proc_oid);
        if let Some(proc_row) = catalog.proc_row_by_oid(proc_oid) {
            deps.type_oids.push(proc_row.prorettype);
        }
    }
    match call {
        SetReturningCall::RowsFrom { items, .. } => {
            for item in items {
                match &item.source {
                    RowsFromSource::Function(call) => {
                        collect_set_returning_call_rule_dependencies(call, query, catalog, deps);
                    }
                    RowsFromSource::Project { output_exprs, .. } => {
                        for expr in output_exprs {
                            collect_expr_rule_dependencies(expr, query, catalog, deps);
                        }
                    }
                }
            }
        }
        SetReturningCall::GenerateSeries {
            start,
            stop,
            step,
            timezone,
            ..
        } => {
            for expr in [start, stop, step] {
                collect_expr_rule_dependencies(expr, query, catalog, deps);
            }
            if let Some(timezone) = timezone {
                collect_expr_rule_dependencies(timezone, query, catalog, deps);
            }
        }
        SetReturningCall::GenerateSubscripts {
            array,
            dimension,
            reverse,
            ..
        } => {
            collect_expr_rule_dependencies(array, query, catalog, deps);
            collect_expr_rule_dependencies(dimension, query, catalog, deps);
            if let Some(reverse) = reverse {
                collect_expr_rule_dependencies(reverse, query, catalog, deps);
            }
        }
        SetReturningCall::Unnest { args, .. }
        | SetReturningCall::JsonTableFunction { args, .. }
        | SetReturningCall::RegexTableFunction { args, .. }
        | SetReturningCall::StringTableFunction { args, .. }
        | SetReturningCall::TextSearchTableFunction { args, .. }
        | SetReturningCall::UserDefined { args, .. } => {
            for expr in args {
                collect_expr_rule_dependencies(expr, query, catalog, deps);
            }
        }
        SetReturningCall::JsonRecordFunction {
            args, record_type, ..
        } => {
            if let Some(sql_type) = record_type {
                collect_sql_type_rule_dependency(*sql_type, deps);
            }
            for expr in args {
                collect_expr_rule_dependencies(expr, query, catalog, deps);
            }
        }
        SetReturningCall::PartitionTree { relid, .. }
        | SetReturningCall::PartitionAncestors { relid, .. } => {
            collect_expr_rule_dependencies(relid, query, catalog, deps);
        }
        SetReturningCall::TxidSnapshotXip { arg, .. } => {
            collect_expr_rule_dependencies(arg, query, catalog, deps);
        }
        SetReturningCall::SqlJsonTable(table) => {
            collect_expr_rule_dependencies(&table.context, query, catalog, deps);
            for arg in &table.passing {
                collect_expr_rule_dependencies(&arg.expr, query, catalog, deps);
            }
            for column in &table.columns {
                collect_sql_type_rule_dependency(column.sql_type, deps);
                match &column.kind {
                    SqlJsonTableColumnKind::Scalar {
                        on_empty, on_error, ..
                    }
                    | SqlJsonTableColumnKind::Formatted {
                        on_empty, on_error, ..
                    } => {
                        collect_sql_json_behavior_rule_dependencies(on_empty, query, catalog, deps);
                        collect_sql_json_behavior_rule_dependencies(on_error, query, catalog, deps);
                    }
                    SqlJsonTableColumnKind::Exists { on_error, .. } => {
                        collect_sql_json_behavior_rule_dependencies(on_error, query, catalog, deps);
                    }
                    SqlJsonTableColumnKind::Ordinality => {}
                }
            }
            collect_sql_json_behavior_rule_dependencies(&table.on_error, query, catalog, deps);
        }
        SetReturningCall::SqlXmlTable(table) => {
            collect_expr_rule_dependencies(&table.row_path, query, catalog, deps);
            collect_expr_rule_dependencies(&table.document, query, catalog, deps);
            for namespace in &table.namespaces {
                collect_expr_rule_dependencies(&namespace.uri, query, catalog, deps);
            }
            for column in &table.columns {
                collect_sql_type_rule_dependency(column.sql_type, deps);
                if let SqlXmlTableColumnKind::Regular { path, default, .. } = &column.kind {
                    if let Some(path) = path {
                        collect_expr_rule_dependencies(path, query, catalog, deps);
                    }
                    if let Some(default) = default {
                        collect_expr_rule_dependencies(default, query, catalog, deps);
                    }
                }
            }
        }
        SetReturningCall::PgLockStatus { .. } => {}
    }
}

fn collect_sql_json_behavior_rule_dependencies(
    behavior: &SqlJsonTableBehavior,
    query: &Query,
    catalog: &dyn CatalogLookup,
    deps: &mut crate::backend::catalog::store::RuleDependencies,
) {
    if let SqlJsonTableBehavior::Default(expr) = behavior {
        collect_expr_rule_dependencies(expr, query, catalog, deps);
    }
}

fn set_returning_proc_oid(call: &SetReturningCall) -> Option<u32> {
    let proc_oid = match call {
        SetReturningCall::GenerateSeries { func_oid, .. }
        | SetReturningCall::GenerateSubscripts { func_oid, .. }
        | SetReturningCall::Unnest { func_oid, .. }
        | SetReturningCall::JsonTableFunction { func_oid, .. }
        | SetReturningCall::JsonRecordFunction { func_oid, .. }
        | SetReturningCall::RegexTableFunction { func_oid, .. }
        | SetReturningCall::StringTableFunction { func_oid, .. }
        | SetReturningCall::PartitionTree { func_oid, .. }
        | SetReturningCall::PartitionAncestors { func_oid, .. }
        | SetReturningCall::PgLockStatus { func_oid, .. }
        | SetReturningCall::TxidSnapshotXip { func_oid, .. } => *func_oid,
        SetReturningCall::UserDefined { proc_oid, .. } => *proc_oid,
        SetReturningCall::RowsFrom { .. } => 0,
        SetReturningCall::TextSearchTableFunction { .. }
        | SetReturningCall::SqlJsonTable(_)
        | SetReturningCall::SqlXmlTable(_) => 0,
    };
    (proc_oid != 0).then_some(proc_oid)
}

fn set_returning_return_relation_oid(
    call: &SetReturningCall,
    catalog: &dyn CatalogLookup,
    deps: &mut crate::backend::catalog::store::RuleDependencies,
) -> Option<u32> {
    let proc_oid = set_returning_proc_oid(call)?;
    let proc_row = catalog.proc_row_by_oid(proc_oid)?;
    deps.type_oids.push(proc_row.prorettype);
    catalog
        .type_by_oid(proc_row.prorettype)
        .and_then(|row| (row.typrelid != 0).then_some(row.typrelid))
}

fn collect_sql_type_rule_dependency(
    sql_type: SqlType,
    deps: &mut crate::backend::catalog::store::RuleDependencies,
) {
    if sql_type.type_oid != 0 {
        deps.type_oids.push(sql_type.type_oid);
    }
}

fn collation_name(catalog: &dyn CatalogLookup, oid: u32) -> String {
    catalog
        .collation_rows()
        .into_iter()
        .find(|row| row.oid == oid)
        .map(|row| row.collname)
        .unwrap_or_else(|| oid.to_string())
}

fn create_view_reloptions(options: &[RelOption]) -> Result<Option<Vec<String>>, ExecError> {
    let mut reloptions = Vec::new();
    for option in options {
        let name = option.name.to_ascii_lowercase();
        if !matches!(name.as_str(), "security_barrier" | "security_invoker") {
            return Err(ExecError::DetailedError {
                message: format!("unrecognized parameter \"{}\"", option.name),
                detail: None,
                hint: None,
                sqlstate: "22023",
            });
        }
        let value = match option.value.to_ascii_lowercase().as_str() {
            "true" | "on" => "true",
            "false" | "off" => "false",
            _ => {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "invalid value for boolean option \"{name}\": {}",
                        option.value
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "22023",
                });
            }
        };
        reloptions.push(format!("{name}={value}"));
    }
    Ok((!reloptions.is_empty()).then_some(reloptions))
}

fn validate_polymorphic_range_return_type(
    prorettype: u32,
    callable_arg_oids: &[u32],
) -> Result<(), ExecError> {
    let (type_name, inputs, required_inputs) = match prorettype {
        ANYRANGEOID => (
            "anyrange",
            "anyrange or anymultirange",
            [ANYRANGEOID, ANYMULTIRANGEOID],
        ),
        ANYMULTIRANGEOID => (
            "anymultirange",
            "anyrange or anymultirange",
            [ANYMULTIRANGEOID, ANYRANGEOID],
        ),
        ANYCOMPATIBLERANGEOID => (
            "anycompatiblerange",
            "anycompatiblerange or anycompatiblemultirange",
            [ANYCOMPATIBLERANGEOID, ANYCOMPATIBLEMULTIRANGEOID],
        ),
        ANYCOMPATIBLEMULTIRANGEOID => (
            "anycompatiblemultirange",
            "anycompatiblerange or anycompatiblemultirange",
            [ANYCOMPATIBLEMULTIRANGEOID, ANYCOMPATIBLERANGEOID],
        ),
        _ => return Ok(()),
    };
    if callable_arg_oids
        .iter()
        .any(|oid| required_inputs.contains(oid))
    {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: "cannot determine result data type".into(),
        detail: Some(format!(
            "A result of type {type_name} requires at least one input of type {inputs}."
        )),
        hint: None,
        sqlstate: "42P13",
    })
}

fn validate_polymorphic_output_types(
    prorettype: u32,
    proallargtypes: Option<&Vec<u32>>,
    proargmodes: Option<&Vec<u8>>,
    callable_arg_oids: &[u32],
) -> Result<(), ExecError> {
    let mut output_oids = vec![prorettype];
    if let (Some(all_argtypes), Some(argmodes)) = (proallargtypes, proargmodes) {
        output_oids.extend(
            all_argtypes
                .iter()
                .zip(argmodes.iter())
                .filter_map(|(oid, mode)| matches!(*mode, b'o' | b'b' | b't').then_some(*oid)),
        );
    }
    for output_oid in output_oids {
        match output_oid {
            ANYELEMENTOID | ANYARRAYOID | ANYNONARRAYOID | ANYENUMOID => {
                if !callable_arg_oids
                    .iter()
                    .copied()
                    .any(is_exact_family_polymorphic_oid)
                {
                    return Err(cannot_determine_polymorphic_result(
                        output_oid,
                        "anyelement",
                        "anyelement, anyarray, anynonarray, anyenum, anyrange, or anymultirange",
                    ));
                }
            }
            ANYCOMPATIBLEOID | ANYCOMPATIBLEARRAYOID | ANYCOMPATIBLENONARRAYOID => {
                if !callable_arg_oids
                    .iter()
                    .copied()
                    .any(is_compatible_family_polymorphic_oid)
                {
                    return Err(cannot_determine_polymorphic_result(
                        output_oid,
                        "anycompatible",
                        "anycompatible, anycompatiblearray, anycompatiblenonarray, anycompatiblerange, or anycompatiblemultirange",
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn is_exact_family_polymorphic_oid(oid: u32) -> bool {
    matches!(
        oid,
        ANYELEMENTOID | ANYARRAYOID | ANYNONARRAYOID | ANYENUMOID | ANYRANGEOID | ANYMULTIRANGEOID
    )
}

fn is_compatible_family_polymorphic_oid(oid: u32) -> bool {
    matches!(
        oid,
        ANYCOMPATIBLEOID
            | ANYCOMPATIBLEARRAYOID
            | ANYCOMPATIBLENONARRAYOID
            | ANYCOMPATIBLERANGEOID
            | ANYCOMPATIBLEMULTIRANGEOID
    )
}

fn cannot_determine_polymorphic_result(
    output_oid: u32,
    fallback_name: &'static str,
    inputs: &'static str,
) -> ExecError {
    let type_name = match output_oid {
        ANYELEMENTOID => "anyelement",
        ANYARRAYOID => "anyarray",
        ANYNONARRAYOID => "anynonarray",
        ANYENUMOID => "anyenum",
        ANYCOMPATIBLEOID => "anycompatible",
        ANYCOMPATIBLEARRAYOID => "anycompatiblearray",
        ANYCOMPATIBLENONARRAYOID => "anycompatiblenonarray",
        _ => fallback_name,
    };
    ExecError::DetailedError {
        message: "cannot determine result data type".into(),
        detail: Some(format!(
            "A result of type {type_name} requires at least one input of type {inputs}."
        )),
        hint: None,
        sqlstate: "42P13",
    }
}

fn validate_polymorphic_range_output_types(
    prorettype: u32,
    proallargtypes: Option<&Vec<u32>>,
    proargmodes: Option<&Vec<u8>>,
    callable_arg_oids: &[u32],
) -> Result<(), ExecError> {
    validate_polymorphic_range_return_type(prorettype, callable_arg_oids)?;
    let (Some(all_argtypes), Some(argmodes)) = (proallargtypes, proargmodes) else {
        return Ok(());
    };
    for (type_oid, mode) in all_argtypes.iter().zip(argmodes.iter()) {
        if matches!(*mode, b'o' | b'b' | b't') {
            validate_polymorphic_range_return_type(*type_oid, callable_arg_oids)?;
        }
    }
    Ok(())
}

pub(super) fn normalize_create_proc_name_for_search_path(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    schema_name: Option<&str>,
    proc_name: &str,
    object_kind: &'static str,
    configured_search_path: Option<&[String]>,
) -> Result<(String, u32), ParseError> {
    let normalized = proc_name.to_ascii_lowercase();
    match schema_name.map(str::to_ascii_lowercase) {
        Some(schema) if schema == "pg_catalog" => Ok((normalized, PG_CATALOG_NAMESPACE_OID)),
        Some(schema) if schema == "pg_temp" => Err(ParseError::UnexpectedToken {
            expected: "permanent database object",
            actual: format!("temporary {object_kind}"),
        }),
        Some(schema) => db
            .visible_namespace_oid_by_name(client_id, txn_ctx, &schema)
            .map(|namespace_oid| (normalized.clone(), namespace_oid))
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "existing schema",
                actual: format!("schema \"{schema}\" does not exist"),
            }),
        None => {
            let search_path = db.effective_search_path(client_id, configured_search_path);
            for schema in search_path {
                match schema.as_str() {
                    "" | "$user" | "pg_temp" => continue,
                    schema if schema.starts_with("pg_temp_") => continue,
                    "pg_catalog" => continue,
                    _ => {
                        if let Some(namespace_oid) =
                            db.visible_namespace_oid_by_name(client_id, txn_ctx, &schema)
                        {
                            return Ok((normalized.clone(), namespace_oid));
                        }
                    }
                }
            }
            Err(ParseError::NoSchemaSelectedForCreate)
        }
    }
}

fn proc_parallel_code(parallel: FunctionParallel) -> char {
    match parallel {
        FunctionParallel::Unsafe => 'u',
        FunctionParallel::Restricted => 'r',
        FunctionParallel::Safe => 's',
    }
}

pub(super) fn aggregate_signature_arg_oids(
    catalog: &dyn CatalogLookup,
    signature: &AggregateSignatureKind,
) -> Result<Vec<u32>, ParseError> {
    match signature {
        AggregateSignatureKind::Star => Ok(Vec::new()),
        AggregateSignatureKind::Args(signature) => signature
            .args
            .iter()
            .chain(signature.order_by.iter())
            .map(|arg| aggregate_signature_arg_oid(catalog, arg))
            .collect(),
    }
}

pub(super) fn format_aggregate_signature(
    aggregate_name: &str,
    signature: &AggregateSignatureKind,
    catalog: &dyn CatalogLookup,
) -> Result<String, ExecError> {
    Ok(match signature {
        AggregateSignatureKind::Star => format!("{aggregate_name}(*)"),
        AggregateSignatureKind::Args(signature) => {
            let args = signature
                .args
                .iter()
                .map(|arg| format_aggregate_signature_arg(arg, catalog))
                .collect::<Result<Vec<_>, _>>()?;
            let order_by = signature
                .order_by
                .iter()
                .map(|arg| format_aggregate_signature_arg(arg, catalog))
                .collect::<Result<Vec<_>, _>>()?;
            if order_by.is_empty() {
                format!("{aggregate_name}({})", args.join(", "))
            } else if args.is_empty() {
                format!("{aggregate_name}(ORDER BY {})", order_by.join(", "))
            } else {
                format!(
                    "{aggregate_name}({} ORDER BY {})",
                    args.join(", "),
                    order_by.join(", ")
                )
            }
        }
    })
}

fn format_aggregate_signature_arg(
    arg: &AggregateSignatureArg,
    catalog: &dyn CatalogLookup,
) -> Result<String, ExecError> {
    let type_name = match &arg.arg_type {
        AggregateArgType::AnyPseudo => "\"any\"".to_string(),
        AggregateArgType::Type(raw_type) => {
            let sql_type = resolve_raw_type_name(raw_type, catalog).map_err(ExecError::Parse)?;
            format_sql_type_name(sql_type)
        }
    };
    Ok(if arg.variadic {
        format!("VARIADIC {type_name}")
    } else {
        type_name
    })
}

fn aggregate_signature_arg_oid(
    catalog: &dyn CatalogLookup,
    arg: &AggregateSignatureArg,
) -> Result<u32, ParseError> {
    match &arg.arg_type {
        AggregateArgType::AnyPseudo => Ok(ANYOID),
        AggregateArgType::Type(raw_type) => {
            let sql_type = resolve_raw_type_name(raw_type, catalog)?;
            catalog
                .type_oid_for_sql_type(sql_type)
                .ok_or_else(|| ParseError::UnsupportedType(format!("{sql_type:?}")))
        }
    }
}

fn aggregate_transition_input_oids(
    catalog: &dyn CatalogLookup,
    signature: &AggregateSignatureKind,
) -> Result<Vec<u32>, ParseError> {
    match signature {
        AggregateSignatureKind::Star => Ok(Vec::new()),
        AggregateSignatureKind::Args(signature) if signature.order_by.is_empty() => signature
            .args
            .iter()
            .map(|arg| aggregate_signature_arg_oid(catalog, arg))
            .collect(),
        AggregateSignatureKind::Args(signature) => signature
            .order_by
            .iter()
            .map(|arg| aggregate_signature_arg_oid(catalog, arg))
            .collect(),
    }
}

fn validate_polymorphic_aggregate_transition_type(
    stype_oid: u32,
    transition_input_oids: &[u32],
) -> Result<(), ExecError> {
    if stype_oid != ANYARRAYOID {
        return Ok(());
    }
    if transition_input_oids.iter().copied().any(|oid| {
        matches!(
            oid,
            ANYELEMENTOID
                | ANYARRAYOID
                | ANYNONARRAYOID
                | ANYENUMOID
                | ANYRANGEOID
                | ANYMULTIRANGEOID
        )
    }) {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: "cannot determine transition data type".into(),
        detail: Some(
            "A result of type anyarray requires at least one input of type anyelement, anyarray, anynonarray, anyenum, anyrange, or anymultirange."
                .into(),
        ),
        hint: None,
        sqlstate: "42P13",
    })
}

fn aggregate_direct_arg_count(signature: &AggregateSignatureKind) -> i16 {
    match signature {
        AggregateSignatureKind::Star => 0,
        AggregateSignatureKind::Args(signature) if signature.order_by.is_empty() => 0,
        AggregateSignatureKind::Args(signature) => signature.args.len() as i16,
    }
}

fn aggregate_kind(create_stmt: &CreateAggregateStatement) -> char {
    if create_stmt.hypothetical {
        'h'
    } else if matches!(
        &create_stmt.signature,
        AggregateSignatureKind::Args(AggregateSignature { order_by, .. }) if !order_by.is_empty()
    ) {
        'o'
    } else {
        'n'
    }
}

fn routine_kind_detail(name: &str, prokind: char, aggkind: Option<char>) -> String {
    let kind = match (prokind, aggkind) {
        ('a', Some('o')) => "an ordered-set aggregate function",
        ('a', Some('h')) => "a hypothetical-set aggregate function",
        ('a', _) => "an ordinary aggregate function",
        ('p', _) => "a procedure",
        _ => "a function",
    };
    format!("\"{name}\" is {kind}.")
}

fn cannot_change_routine_kind_error(name: &str, prokind: char, aggkind: Option<char>) -> ExecError {
    ExecError::DetailedError {
        message: "cannot change routine kind".into(),
        detail: Some(routine_kind_detail(name, prokind, aggkind)),
        hint: None,
        sqlstate: "42809",
    }
}

fn aggregate_arg_modes(signature: &AggregateSignatureKind) -> Option<Vec<u8>> {
    let AggregateSignatureKind::Args(signature) = signature else {
        return None;
    };
    let modes = signature
        .args
        .iter()
        .chain(signature.order_by.iter())
        .map(|arg| if arg.variadic { b'v' } else { b'i' })
        .collect::<Vec<_>>();
    modes.iter().any(|mode| *mode == b'v').then_some(modes)
}

fn aggregate_arg_names(signature: &AggregateSignatureKind) -> Option<Vec<String>> {
    let AggregateSignatureKind::Args(signature) = signature else {
        return None;
    };
    let names = signature
        .args
        .iter()
        .chain(signature.order_by.iter())
        .map(|arg| arg.name.clone().unwrap_or_default())
        .collect::<Vec<_>>();
    names.iter().any(|name| !name.is_empty()).then_some(names)
}

fn aggregate_provariadic(
    catalog: &dyn CatalogLookup,
    signature: &AggregateSignatureKind,
) -> Result<u32, ParseError> {
    let AggregateSignatureKind::Args(signature) = signature else {
        return Ok(0);
    };
    signature
        .args
        .iter()
        .chain(signature.order_by.iter())
        .find(|arg| arg.variadic)
        .map(|arg| {
            aggregate_signature_arg_oid(catalog, arg)
                .map(|type_oid| variadic_element_type_oid(catalog, type_oid))
        })
        .transpose()
        .map(|oid| oid.unwrap_or(0))
}

fn variadic_element_type_oid(catalog: &dyn CatalogLookup, type_oid: u32) -> u32 {
    match type_oid {
        ANYARRAYOID => return ANYELEMENTOID,
        ANYCOMPATIBLEARRAYOID => return ANYCOMPATIBLEOID,
        ANYENUMOID => return ANYENUMOID,
        _ => {}
    }
    catalog
        .type_rows()
        .into_iter()
        .find(|row| row.typarray == type_oid)
        .map(|row| row.oid)
        .or_else(|| {
            catalog.type_by_oid(type_oid).map(|row| {
                if row.sql_type.is_array {
                    row.sql_type.type_oid
                } else {
                    row.typelem
                }
            })
        })
        .filter(|element_oid| *element_oid != 0)
        .unwrap_or(type_oid)
}

fn raw_named_shell_type_name(raw: &RawTypeName) -> Option<&str> {
    match raw {
        RawTypeName::Named {
            name,
            array_bounds: 0,
        } => Some(name.as_str()),
        _ => None,
    }
}

fn create_function_type_oid(
    catalog: &dyn CatalogLookup,
    sql_type: SqlType,
    fallback_name: impl Into<String>,
) -> Result<u32, ExecError> {
    catalog
        .type_oid_for_sql_type(sql_type)
        .or_else(|| matches!(sql_type.kind, SqlTypeKind::Record).then_some(RECORD_TYPE_OID))
        .ok_or_else(|| ExecError::Parse(ParseError::UnsupportedType(fallback_name.into())))
}

fn notice_name_for_type(raw: &RawTypeName, sql_type: SqlType) -> String {
    raw_named_shell_type_name(raw)
        .map(str::to_string)
        .unwrap_or_else(|| format!("{sql_type:?}"))
}

fn split_proc_name(name: &str) -> (Option<&str>, &str) {
    name.rsplit_once('.')
        .map(|(schema, proc_name)| (Some(schema), proc_name))
        .unwrap_or((None, name))
}

fn parse_proc_argtype_oids(argtypes: &str) -> Option<Vec<u32>> {
    if argtypes.trim().is_empty() {
        return Some(Vec::new());
    }
    argtypes
        .split_whitespace()
        .map(|part| part.parse::<u32>().ok())
        .collect()
}

fn resolve_exact_proc_row(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    catalog: &dyn CatalogLookup,
    proc_name: &str,
    arg_oids: &[u32],
    expected_kind: char,
) -> Result<PgProcRow, ExecError> {
    let (schema_name, base_name) = split_proc_name(proc_name);
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
    let matches = catalog
        .proc_rows_by_name(base_name)
        .into_iter()
        .filter(|row| {
            row.prokind == expected_kind
                && parse_proc_argtype_oids(&row.proargtypes)
                    .is_some_and(|row_arg_oids| row_arg_oids == arg_oids)
                && namespace_oid
                    .map(|namespace_oid| row.pronamespace == namespace_oid)
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [row] => Ok(row.clone()),
        [] => Err(ExecError::DetailedError {
            message: format!(
                "function {} does not exist",
                exact_proc_signature(catalog, proc_name, arg_oids)
            ),
            detail: None,
            hint: None,
            sqlstate: "42883",
        }),
        _ => Err(ExecError::DetailedError {
            message: format!(
                "function name {} is ambiguous",
                exact_proc_signature(catalog, proc_name, arg_oids)
            ),
            detail: None,
            hint: None,
            sqlstate: "42725",
        }),
    }
}

#[derive(Debug, Clone)]
struct AggregateSupportProc {
    row: PgProcRow,
    result_type: SqlType,
    declared_arg_types: Vec<SqlType>,
}

fn lookup_aggregate_support_proc_row(
    catalog: &dyn CatalogLookup,
    proc_name: &str,
    arg_oids: &[u32],
    explicit_variadic: bool,
) -> Result<AggregateSupportProc, ExecError> {
    let actual_types = arg_oids
        .iter()
        .map(|oid| {
            catalog
                .type_by_oid(*oid)
                .map(|row| row.sql_type)
                .ok_or_else(|| ExecError::Parse(ParseError::UnsupportedType(format!("oid {oid}"))))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let support = match resolve_function_call(catalog, proc_name, &actual_types, explicit_variadic)
    {
        Ok(resolved) => aggregate_support_from_resolved(catalog, proc_name, resolved)?,
        Err(
            err @ ParseError::DetailedError {
                sqlstate: "42883", ..
            },
        ) => {
            if let Some(retry_types) = aggregate_runtime_coercion_retry_types(&actual_types)
                && let Ok(resolved) =
                    resolve_function_call(catalog, proc_name, &retry_types, explicit_variadic)
            {
                aggregate_support_from_resolved(catalog, proc_name, resolved)?
            } else if let Some(support) =
                lookup_exact_aggregate_support_proc(catalog, proc_name, arg_oids)?
            {
                support
            } else {
                return Err(match err {
                    ParseError::DetailedError {
                        message, sqlstate, ..
                    } => ExecError::DetailedError {
                        message,
                        detail: None,
                        hint: None,
                        sqlstate,
                    },
                    err => ExecError::Parse(err),
                });
            }
        }
        Err(err @ ParseError::DetailedError { sqlstate, .. })
            if matches!(sqlstate, "42804" | "42883") =>
        {
            if let Some(support) =
                lookup_exact_aggregate_support_proc(catalog, proc_name, arg_oids)?
            {
                support
            } else if arg_oids
                .iter()
                .copied()
                .any(is_polymorphic_aggregate_signature_oid)
            {
                return Err(aggregate_support_proc_missing_error(
                    catalog, proc_name, arg_oids,
                ));
            } else {
                return Err(match err {
                    ParseError::DetailedError {
                        message, sqlstate, ..
                    } => ExecError::DetailedError {
                        message,
                        detail: None,
                        hint: None,
                        sqlstate,
                    },
                    err => ExecError::Parse(err),
                });
            }
        }
        Err(err) => return Err(ExecError::Parse(err)),
    };
    if support.row.prokind != 'f' {
        return Err(ExecError::DetailedError {
            message: format!("function {proc_name} does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42883",
        });
    }
    if support.row.proretset {
        return Err(ExecError::DetailedError {
            message: format!(
                "function {} returns a set",
                aggregate_support_signature(proc_name, &support.declared_arg_types)
            ),
            detail: None,
            hint: None,
            sqlstate: "42804",
        });
    }
    for (actual_type, declared_type) in actual_types
        .iter()
        .copied()
        .zip(support.declared_arg_types.iter().copied())
    {
        if !is_binary_coercible_type(actual_type, declared_type) {
            return Err(ExecError::DetailedError {
                message: format!(
                    "function {} requires run-time type coercion",
                    aggregate_support_signature(proc_name, &support.declared_arg_types)
                ),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        }
    }
    Ok(support)
}

fn aggregate_support_from_resolved(
    catalog: &dyn CatalogLookup,
    proc_name: &str,
    resolved: ResolvedFunctionCall,
) -> Result<AggregateSupportProc, ExecError> {
    let row =
        catalog
            .proc_row_by_oid(resolved.proc_oid)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("function {proc_name} does not exist"),
                detail: None,
                hint: None,
                sqlstate: "42883",
            })?;
    Ok(AggregateSupportProc {
        row,
        result_type: resolved.result_type,
        declared_arg_types: resolved.declared_arg_types,
    })
}

fn aggregate_support_proc_missing_error(
    catalog: &dyn CatalogLookup,
    proc_name: &str,
    arg_oids: &[u32],
) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "function {} does not exist",
            exact_proc_signature(catalog, proc_name, arg_oids)
        ),
        detail: None,
        hint: None,
        sqlstate: "42883",
    }
}

fn is_polymorphic_aggregate_signature_oid(oid: u32) -> bool {
    matches!(
        oid,
        ANYOID
            | ANYELEMENTOID
            | ANYARRAYOID
            | ANYNONARRAYOID
            | ANYENUMOID
            | ANYRANGEOID
            | ANYMULTIRANGEOID
            | ANYCOMPATIBLEOID
            | ANYCOMPATIBLENONARRAYOID
            | ANYCOMPATIBLEARRAYOID
            | ANYCOMPATIBLERANGEOID
            | ANYCOMPATIBLEMULTIRANGEOID
    )
}

fn aggregate_runtime_coercion_retry_types(actual_types: &[SqlType]) -> Option<Vec<SqlType>> {
    let first = actual_types.first().copied()?;
    if actual_types.len() < 2
        || first.kind == SqlTypeKind::Internal
        || actual_types.iter().copied().all(|ty| ty == first)
    {
        return None;
    }
    Some(vec![first; actual_types.len()])
}

fn lookup_exact_aggregate_support_proc(
    catalog: &dyn CatalogLookup,
    proc_name: &str,
    arg_oids: &[u32],
) -> Result<Option<AggregateSupportProc>, ExecError> {
    let (schema_name, base_name) = split_proc_name(proc_name);
    let mut rows = catalog
        .proc_rows_by_name(base_name)
        .into_iter()
        .filter(|row| {
            schema_name
                .map(|schema_name| {
                    catalog
                        .namespace_row_by_oid(row.pronamespace)
                        .is_some_and(|namespace| {
                            namespace.nspname.eq_ignore_ascii_case(schema_name)
                        })
                })
                .unwrap_or(true)
        })
        .filter(|row| {
            parse_proc_argtype_oids(&row.proargtypes).is_some_and(|oids| oids == arg_oids)
        })
        .collect::<Vec<_>>();
    match rows.len() {
        0 => Ok(None),
        1 => {
            let row = rows.remove(0);
            let declared_arg_types = sql_types_for_oids(catalog, arg_oids)?;
            let result_type = catalog
                .type_by_oid(row.prorettype)
                .map(|row| row.sql_type)
                .or_else(|| {
                    (row.prorettype == RECORD_TYPE_OID).then_some(SqlType::new(SqlTypeKind::Record))
                })
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnsupportedType(format!(
                        "oid {}",
                        row.prorettype
                    )))
                })?;
            Ok(Some(AggregateSupportProc {
                row,
                result_type,
                declared_arg_types,
            }))
        }
        _ => Err(ExecError::DetailedError {
            message: format!("function name {proc_name} is ambiguous"),
            detail: None,
            hint: None,
            sqlstate: "42725",
        }),
    }
}

fn sql_types_for_oids(
    catalog: &dyn CatalogLookup,
    arg_oids: &[u32],
) -> Result<Vec<SqlType>, ExecError> {
    arg_oids
        .iter()
        .map(|oid| {
            catalog
                .type_by_oid(*oid)
                .map(|row| row.sql_type)
                .ok_or_else(|| ExecError::Parse(ParseError::UnsupportedType(format!("oid {oid}"))))
        })
        .collect()
}

fn aggregate_support_signature(proc_name: &str, arg_types: &[SqlType]) -> String {
    let args = arg_types
        .iter()
        .map(|ty| format_sql_type_name(*ty))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{proc_name}({args})")
}

fn support_signature_name(signature: &RoutineSignature) -> String {
    match &signature.schema_name {
        Some(schema_name) => format!("{schema_name}.{}", signature.routine_name),
        None => signature.routine_name.clone(),
    }
}

fn resolve_support_proc_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    catalog: &dyn CatalogLookup,
    signature: &RoutineSignature,
) -> Result<u32, ExecError> {
    let name = support_signature_name(signature);
    let arg_oids = if signature.arg_types.is_empty() {
        vec![INTERNAL_TYPE_OID]
    } else {
        signature
            .arg_types
            .iter()
            .map(|arg| routine_support_arg_oid(catalog, arg))
            .collect::<Result<Vec<_>, _>>()?
    };
    resolve_exact_proc_row(db, client_id, txn_ctx, catalog, &name, &arg_oids, 'f')
        .map(|row| row.oid)
}

fn routine_support_arg_oid(catalog: &dyn CatalogLookup, arg: &str) -> Result<u32, ExecError> {
    let raw_type = crate::backend::parser::parse_type_name(arg).map_err(ExecError::Parse)?;
    let sql_type = resolve_raw_type_name(&raw_type, catalog).map_err(ExecError::Parse)?;
    catalog
        .type_oid_for_sql_type(sql_type)
        .ok_or_else(|| ExecError::Parse(ParseError::UnsupportedType(arg.into())))
}

fn exact_proc_signature(catalog: &dyn CatalogLookup, proc_name: &str, arg_oids: &[u32]) -> String {
    let args = arg_oids
        .iter()
        .map(|oid| proc_signature_type_name(catalog, *oid))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{proc_name}({args})")
}

fn support_result_type_oid(
    catalog: &dyn CatalogLookup,
    support: &AggregateSupportProc,
) -> Result<u32, ExecError> {
    catalog
        .type_oid_for_sql_type(support.result_type)
        .or_else(|| (support.result_type.kind == SqlTypeKind::Record).then_some(RECORD_TYPE_OID))
        .or_else(|| (support.row.prorettype != 0).then_some(support.row.prorettype))
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnsupportedType(format!(
                "{:?}",
                support.result_type
            )))
        })
}

fn proc_signature_type_name(catalog: &dyn CatalogLookup, oid: u32) -> String {
    if let Some(row) = catalog.type_by_oid(oid) {
        let mut sql_type = row.sql_type;
        sql_type.type_oid = 0;
        return format_sql_type_name(sql_type);
    }
    oid.to_string()
}

pub(super) fn resolve_aggregate_proc_rows(
    catalog: &dyn CatalogLookup,
    aggregate_name: &str,
    namespace_oid: Option<u32>,
    arg_oids: &[u32],
) -> Vec<(PgProcRow, PgAggregateRow)> {
    catalog
        .proc_rows_by_name(aggregate_name)
        .into_iter()
        .filter(|row| {
            row.prokind == 'a'
                && namespace_oid
                    .map(|namespace_oid| row.pronamespace == namespace_oid)
                    .unwrap_or(true)
                && parse_proc_argtype_oids(&row.proargtypes)
                    .is_some_and(|row_arg_oids| row_arg_oids == arg_oids)
        })
        .filter_map(|row| {
            catalog
                .aggregate_by_fnoid(row.oid)
                .map(|aggregate_row| (row, aggregate_row))
        })
        .collect()
}

fn proc_arg_mode(mode: FunctionArgMode) -> u8 {
    match mode {
        FunctionArgMode::In => b'i',
        FunctionArgMode::Out => b'o',
        FunctionArgMode::InOut => b'b',
    }
}

fn encode_proc_arg_defaults(defaults: &[Option<String>]) -> Option<String> {
    defaults
        .iter()
        .any(Option::is_some)
        .then(|| serde_json::to_string(defaults).expect("procedure defaults serialize"))
}

fn callable_proc_arg_names(row: &PgProcRow) -> Vec<String> {
    let input_count = row.pronargs.max(0) as usize;
    let names = row.proargnames.clone().unwrap_or_default();
    if let (Some(_all_argtypes), Some(modes)) =
        (row.proallargtypes.as_ref(), row.proargmodes.as_ref())
    {
        let mut input_names = Vec::with_capacity(input_count);
        for (index, mode) in modes.iter().copied().enumerate() {
            if matches!(mode, b'i' | b'b' | b'v') {
                input_names.push(names.get(index).cloned().unwrap_or_default());
            }
        }
        input_names.resize(input_count, String::new());
        return input_names;
    }
    let mut input_names = names;
    input_names.resize(input_count, String::new());
    input_names.truncate(input_count);
    input_names
}

fn proc_drop_signature_hint(row: &PgProcRow, catalog: &dyn CatalogLookup) -> String {
    let args = parse_proc_argtype_oids(&row.proargtypes)
        .unwrap_or_default()
        .into_iter()
        .map(|oid| proc_signature_type_name(catalog, oid))
        .collect::<Vec<_>>()
        .join(",");
    format!("Use DROP FUNCTION {}({}) first.", row.proname, args)
}

fn validate_replaced_proc_signature(
    existing: &PgProcRow,
    new_row: &PgProcRow,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    if existing.prorettype != new_row.prorettype || existing.proretset != new_row.proretset {
        return Err(ExecError::DetailedError {
            message: "cannot change return type of existing function".into(),
            detail: None,
            hint: Some(proc_drop_signature_hint(existing, catalog)),
            sqlstate: "42P13",
        });
    }

    if existing.pronargdefaults > new_row.pronargdefaults {
        return Err(ExecError::DetailedError {
            message: "cannot remove parameter defaults from existing function".into(),
            detail: None,
            hint: Some(proc_drop_signature_hint(existing, catalog)),
            sqlstate: "42P13",
        });
    }

    for (old_name, new_name) in callable_proc_arg_names(existing)
        .into_iter()
        .zip(callable_proc_arg_names(new_row))
    {
        if !old_name.is_empty() && old_name != new_name {
            return Err(ExecError::DetailedError {
                message: format!("cannot change name of input parameter \"{old_name}\""),
                detail: None,
                hint: Some(proc_drop_signature_hint(existing, catalog)),
                sqlstate: "42P13",
            });
        }
    }
    Ok(())
}

fn select_query_requires_original_view_sql(query: &SelectStatement) -> bool {
    !query.with.is_empty()
        || query
            .from
            .as_ref()
            .is_some_and(from_item_requires_original_view_sql)
        || query.set_operation.as_ref().is_some_and(|setop| {
            setop
                .inputs
                .iter()
                .any(select_query_requires_original_view_sql)
        })
}

fn from_item_requires_original_view_sql(item: &FromItem) -> bool {
    match item {
        FromItem::FunctionCall {
            with_ordinality, ..
        } => *with_ordinality,
        FromItem::RowsFrom { .. } => true,
        FromItem::Alias {
            source,
            column_aliases,
            ..
        } => {
            matches!(column_aliases, AliasColumnSpec::Definitions(_))
                || from_item_requires_original_view_sql(source)
        }
        FromItem::Lateral(source) | FromItem::TableSample { source, .. } => {
            from_item_requires_original_view_sql(source)
        }
        FromItem::DerivedTable(query) => select_query_requires_original_view_sql(query),
        FromItem::Join { left, right, .. } => {
            from_item_requires_original_view_sql(left)
                || from_item_requires_original_view_sql(right)
        }
        FromItem::Table { .. }
        | FromItem::Values { .. }
        | FromItem::JsonTable(_)
        | FromItem::XmlTable(_) => false,
    }
}

fn invalid_procedure_attribute() -> ExecError {
    ExecError::DetailedError {
        message: "invalid attribute in procedure definition".into(),
        detail: None,
        hint: None,
        sqlstate: "42P13",
    }
}

fn validate_proc_arg_order(args: &[CreateFunctionArg], proc_kind: char) -> Result<(), ExecError> {
    validate_proc_arg_names(args)?;
    let mut saw_variadic_input = false;
    let mut saw_default = false;
    for arg in args {
        let is_input = matches!(arg.mode, FunctionArgMode::In | FunctionArgMode::InOut);
        let is_output = matches!(arg.mode, FunctionArgMode::Out | FunctionArgMode::InOut);
        if arg.default_expr.is_some() && !is_input {
            return Err(ExecError::DetailedError {
                message: "only input parameters can have default values".into(),
                detail: None,
                hint: None,
                sqlstate: "42P13",
            });
        }
        if saw_variadic_input && is_input {
            return Err(ExecError::DetailedError {
                message: "VARIADIC parameter must be the last input parameter".into(),
                detail: None,
                hint: None,
                sqlstate: "42P13",
            });
        }
        if proc_kind == 'p' && saw_variadic_input && is_output {
            return Err(ExecError::DetailedError {
                message: "VARIADIC parameter must be the last parameter".into(),
                detail: None,
                hint: None,
                sqlstate: "42P13",
            });
        }
        if is_input && saw_default && arg.default_expr.is_none() {
            return Err(ExecError::DetailedError {
                message: "input parameters after one with a default value must also have defaults"
                    .into(),
                detail: None,
                hint: None,
                sqlstate: "42P13",
            });
        }
        if proc_kind == 'p' && is_output && !is_input && saw_default {
            return Err(ExecError::DetailedError {
                message: "procedure OUT parameters cannot appear after one with a default value"
                    .into(),
                detail: None,
                hint: None,
                sqlstate: "42P13",
            });
        }
        if arg.default_expr.is_some() {
            saw_default = true;
        }
        if arg.variadic && is_input {
            saw_variadic_input = true;
        }
    }
    Ok(())
}

fn validate_proc_arg_names(args: &[CreateFunctionArg]) -> Result<(), ExecError> {
    for (index, arg) in args.iter().enumerate() {
        let Some(name) = arg.name.as_deref().filter(|name| !name.is_empty()) else {
            continue;
        };
        let is_input =
            matches!(arg.mode, FunctionArgMode::In | FunctionArgMode::InOut) || arg.variadic;
        let is_output = matches!(arg.mode, FunctionArgMode::Out | FunctionArgMode::InOut);
        for prev in &args[..index] {
            if prev.name.as_deref() != Some(name) {
                continue;
            }
            let prev_is_input =
                matches!(prev.mode, FunctionArgMode::In | FunctionArgMode::InOut) || prev.variadic;
            let prev_is_output = matches!(prev.mode, FunctionArgMode::Out | FunctionArgMode::InOut);
            if (is_input && prev_is_output && !prev_is_input && !is_output)
                || (prev_is_input && is_output && !is_input && !prev_is_output)
            {
                continue;
            }
            return Err(ExecError::DetailedError {
                message: format!("parameter name \"{name}\" used more than once"),
                detail: None,
                hint: None,
                sqlstate: "42P13",
            });
        }
    }
    Ok(())
}

fn validate_range_polymorphic_result(
    prorettype: u32,
    proallargtypes: Option<&[u32]>,
    proargmodes: Option<&[u8]>,
    callable_arg_oids: &[u32],
) -> Result<(), ExecError> {
    let mut output_oids = vec![prorettype];
    if let (Some(all_argtypes), Some(argmodes)) = (proallargtypes, proargmodes) {
        output_oids.extend(
            all_argtypes
                .iter()
                .zip(argmodes.iter())
                .filter_map(|(oid, mode)| matches!(*mode, b'o' | b'b' | b't').then_some(*oid)),
        );
    }
    for output_oid in output_oids {
        if !is_range_family_polymorphic_oid(output_oid) {
            continue;
        }
        let has_range_family_input = callable_arg_oids
            .iter()
            .copied()
            .any(is_range_family_polymorphic_oid);
        if !has_range_family_input {
            return Err(cannot_determine_range_polymorphic_result(output_oid));
        }
    }
    Ok(())
}

fn is_range_family_polymorphic_oid(oid: u32) -> bool {
    matches!(
        oid,
        ANYRANGEOID | ANYMULTIRANGEOID | ANYCOMPATIBLERANGEOID | ANYCOMPATIBLEMULTIRANGEOID
    )
}

fn cannot_determine_range_polymorphic_result(oid: u32) -> ExecError {
    let (result, inputs) = match oid {
        ANYRANGEOID => ("anyrange", "anyrange or anymultirange"),
        ANYMULTIRANGEOID => ("anymultirange", "anyrange or anymultirange"),
        ANYCOMPATIBLERANGEOID => (
            "anycompatiblerange",
            "anycompatiblerange or anycompatiblemultirange",
        ),
        ANYCOMPATIBLEMULTIRANGEOID => (
            "anycompatiblemultirange",
            "anycompatiblerange or anycompatiblemultirange",
        ),
        _ => ("polymorphic", "compatible polymorphic"),
    };
    ExecError::DetailedError {
        message: "cannot determine result data type".into(),
        detail: Some(format!(
            "A result of type {result} requires at least one input of type {inputs}."
        )),
        hint: None,
        sqlstate: "42P13",
    }
}

fn foreign_key_action_code(action: ForeignKeyAction) -> char {
    match action {
        ForeignKeyAction::NoAction => 'a',
        ForeignKeyAction::Restrict => 'r',
        ForeignKeyAction::Cascade => 'c',
        ForeignKeyAction::SetNull => 'n',
        ForeignKeyAction::SetDefault => 'd',
    }
}

fn foreign_key_match_code(match_type: ForeignKeyMatchType) -> char {
    match match_type {
        ForeignKeyMatchType::Simple => 's',
        ForeignKeyMatchType::Full => 'f',
        ForeignKeyMatchType::Partial => 'p',
    }
}

fn create_table_like_statistics_name_base(
    relation_name: &str,
    source_row: &crate::include::catalog::PgStatisticExtRow,
    relation: &crate::backend::parser::BoundRelation,
) -> String {
    let target_name = source_row
        .stxkeys
        .iter()
        .find_map(|attnum| {
            usize::try_from(*attnum)
                .ok()
                .and_then(|index| index.checked_sub(1))
                .and_then(|index| relation.desc.columns.get(index))
                .map(|column| column.name.as_str())
        })
        .unwrap_or("expr");
    format!("{relation_name}_{target_name}_stat")
}

fn column_attnums_for_names(
    desc: &crate::backend::executor::RelationDesc,
    columns: &[String],
) -> Vec<i16> {
    columns
        .iter()
        .map(|column_name| {
            desc.columns
                .iter()
                .enumerate()
                .find_map(|(index, column)| {
                    (!column.dropped && column.name.eq_ignore_ascii_case(column_name))
                        .then_some(index as i16 + 1)
                })
                .unwrap_or_else(|| panic!("missing column for foreign key: {column_name}"))
        })
        .collect()
}

fn unique_domain_constraint_name(
    base_name: String,
    used_names: &mut std::collections::BTreeSet<String>,
) -> String {
    if used_names.insert(base_name.to_ascii_lowercase()) {
        return base_name;
    }
    for suffix in 1.. {
        let candidate = format!("{base_name}{suffix}");
        if used_names.insert(candidate.to_ascii_lowercase()) {
            return candidate;
        }
    }
    unreachable!("unbounded domain constraint suffix search")
}

impl Database {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn install_create_table_constraints_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        table_cid: CommandId,
        table_name: &str,
        relation: &crate::backend::parser::BoundRelation,
        lowered: &crate::backend::parser::LoweredCreateTable,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let mut next_cid = table_cid.saturating_add(1);
        if relation.relkind == 'p' || relation.relispartition {
            next_cid = self.install_partitioned_index_backed_constraints_in_transaction(
                client_id,
                xid,
                next_cid,
                relation,
                &lowered.constraint_actions,
                configured_search_path,
                catalog_effects,
            )?;
        } else {
            let catalog =
                self.lazy_catalog_lookup(client_id, Some((xid, table_cid)), configured_search_path);
            for action in &lowered.constraint_actions {
                let action_cid = next_cid;
                next_cid = next_cid.saturating_add(3);
                let constraint_name = action
                    .constraint_name
                    .clone()
                    .expect("normalized key constraint name");
                let index_name = self.choose_available_relation_name(
                    client_id,
                    xid,
                    action_cid,
                    relation.namespace_oid,
                    &constraint_name,
                )?;
                let index_columns =
                    constraint_index_columns_with_expr_types(action, relation, &catalog)?;
                let mut storage_columns = index_columns.clone();
                storage_columns.extend(
                    action
                        .include_columns
                        .iter()
                        .cloned()
                        .map(crate::backend::parser::IndexColumnDef::from),
                );
                let (access_method_oid, access_method_handler, build_options) = if action.exclusion
                {
                    self.resolve_exclusion_index_build_options(
                        client_id,
                        Some((xid, action_cid)),
                        action.access_method.as_deref().unwrap_or("gist"),
                        relation,
                        &index_columns,
                    )?
                } else if action.without_overlaps.is_some() {
                    self.resolve_temporal_index_build_options(
                        client_id,
                        Some((xid, action_cid)),
                        relation,
                        &index_columns,
                    )?
                } else {
                    self.resolve_simple_index_build_options(
                        client_id,
                        Some((xid, action_cid)),
                        "btree",
                        relation,
                        &index_columns,
                        &[],
                    )?
                };
                let build_options = crate::backend::catalog::CatalogIndexBuildOptions {
                    indimmediate: !action.deferrable,
                    indisexclusion: action.exclusion || build_options.indisexclusion,
                    ..build_options
                };
                let index_entry = self.build_simple_index_in_transaction(
                    client_id,
                    relation,
                    &index_name,
                    Some(crate::backend::executor::executor_catalog(catalog.clone())),
                    &storage_columns,
                    None,
                    !action.exclusion,
                    action.primary,
                    action.nulls_not_distinct,
                    xid,
                    action_cid,
                    access_method_oid,
                    access_method_handler,
                    &build_options,
                    65_536,
                    false,
                    catalog_effects,
                )?;
                let constraint_ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: action_cid.saturating_add(2),
                    client_id,
                    waiter: None,
                    interrupts: Arc::clone(&interrupts),
                };
                let primary_key_owned_not_null_oids = if action.primary {
                    action
                        .columns
                        .iter()
                        .filter_map(|column_name| {
                            relation.desc.columns.iter().find_map(|column| {
                                (column.name.eq_ignore_ascii_case(column_name)
                                    && column.not_null_primary_key_owned)
                                    .then_some(column.not_null_constraint_oid)
                                    .flatten()
                            })
                        })
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                let conexclop = if action.exclusion {
                    Some(self.exclusion_constraint_operator_oids_for_index_columns(
                        &relation.desc,
                        &index_columns,
                        &action.exclusion_operators,
                        &catalog,
                    )?)
                } else if action.without_overlaps.is_some() {
                    Some(self.temporal_constraint_operator_oids_for_desc(
                        &relation.desc,
                        &action.columns,
                        action.without_overlaps.as_deref(),
                        &catalog,
                    )?)
                } else {
                    None
                };
                let table_entry = super::index::catalog_entry_from_bound_relation(relation);
                let constraint_effect = self
                    .catalog
                    .write()
                    .create_index_backed_constraint_for_entries_mvcc_with_period(
                        &table_entry,
                        &index_entry,
                        constraint_name,
                        if action.exclusion {
                            crate::include::catalog::CONSTRAINT_EXCLUSION
                        } else if action.primary {
                            crate::include::catalog::CONSTRAINT_PRIMARY
                        } else if action.exclusion {
                            crate::include::catalog::CONSTRAINT_EXCLUSION
                        } else {
                            crate::include::catalog::CONSTRAINT_UNIQUE
                        },
                        &primary_key_owned_not_null_oids,
                        action.without_overlaps.is_some(),
                        conexclop,
                        action.deferrable,
                        action.initially_deferred,
                        &constraint_ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&constraint_effect)?;
                catalog_effects.push(constraint_effect);
            }
        }

        let check_base_cid = next_cid;
        for (index, action) in lowered.check_actions.iter().enumerate() {
            let catalog = self.lazy_catalog_lookup(
                client_id,
                Some((xid, check_base_cid)),
                configured_search_path,
            );
            crate::backend::parser::bind_check_constraint_expr(
                &action.expr_sql,
                Some(table_name),
                &relation.desc,
                &catalog,
            )?;
            let constraint_ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: check_base_cid.saturating_add(index as u32),
                client_id,
                waiter: None,
                interrupts: Arc::clone(&interrupts),
            };
            let constraint_effect = self
                .catalog
                .write()
                .create_check_constraint_mvcc(
                    relation.relation_oid,
                    action.constraint_name.clone(),
                    action.enforced,
                    action.enforced && (action.is_local || !action.not_valid),
                    action.no_inherit,
                    action.expr_sql.clone(),
                    action.parent_constraint_oid.unwrap_or(0),
                    action.is_local,
                    action.inhcount,
                    &constraint_ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&constraint_effect)?;
            catalog_effects.push(constraint_effect);
        }

        let mut next_foreign_key_cid =
            check_base_cid.saturating_add(lowered.check_actions.len() as u32);
        for action in &lowered.foreign_key_actions {
            let constraint_cid = next_foreign_key_cid;
            let catalog = self.lazy_catalog_lookup(
                client_id,
                Some((xid, constraint_cid)),
                configured_search_path,
            );
            let (referenced_relation, referenced_index) = if action.self_referential {
                let referenced_relation = catalog
                    .lookup_relation_by_oid(relation.relation_oid)
                    .unwrap_or_else(|| relation.clone());
                let referenced_attnums =
                    column_attnums_for_names(&referenced_relation.desc, &action.referenced_columns);
                let referenced_index = catalog
                    .index_relations_for_heap(referenced_relation.relation_oid)
                    .into_iter()
                    .find(|index| {
                        index.index_meta.indisunique
                            && index.index_meta.indkey == referenced_attnums
                    })
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::DetailedError {
                            message: format!(
                                "there is no unique constraint matching given keys for referenced table \"{table_name}\""
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42830",
                        })
                    })?;
                (referenced_relation, referenced_index)
            } else {
                let referenced_relation = catalog
                    .lookup_relation_by_oid(action.referenced_relation_oid)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnknownTable(action.referenced_table.clone()))
                    })?;
                let referenced_index = catalog
                    .index_relations_for_heap(referenced_relation.relation_oid)
                    .into_iter()
                    .find(|index| index.relation_oid == action.referenced_index_oid)
                    .ok_or_else(|| {
                        ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "referenced UNIQUE or PRIMARY KEY index",
                            actual: action.referenced_index_oid.to_string(),
                        })
                    })?;
                (referenced_relation, referenced_index)
            };
            let local_attnums = column_attnums_for_names(&relation.desc, &action.columns);
            let referenced_attnums =
                column_attnums_for_names(&referenced_relation.desc, &action.referenced_columns);
            let delete_set_attnums = action
                .on_delete_set_columns
                .as_deref()
                .map(|columns| column_attnums_for_names(&relation.desc, columns));
            let constraint_ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: constraint_cid,
                client_id,
                waiter: None,
                interrupts: Arc::clone(&interrupts),
            };
            let table_entry = super::index::catalog_entry_from_bound_relation(relation);
            let referenced_table_entry =
                super::index::catalog_entry_from_bound_relation(&referenced_relation);
            let referenced_index_entry = super::index::catalog_entry_from_bound_index_relation(
                &referenced_index,
                referenced_relation.namespace_oid,
                referenced_relation.owner_oid,
                referenced_relation.relpersistence,
            );
            let (constraint_row, constraint_effect) = self
                .catalog
                .write()
                .create_foreign_key_constraint_for_entries_mvcc(
                    &table_entry,
                    action.constraint_name.clone(),
                    action.deferrable,
                    action.initially_deferred,
                    action.enforced,
                    action.enforced && !action.not_valid,
                    &local_attnums,
                    &referenced_table_entry,
                    &referenced_index_entry,
                    &referenced_attnums,
                    foreign_key_action_code(action.on_update),
                    foreign_key_action_code(action.on_delete),
                    foreign_key_match_code(action.match_type),
                    delete_set_attnums.as_deref(),
                    action.period.is_some(),
                    0,
                    true,
                    0,
                    &constraint_ctx,
                )
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&constraint_effect)?;
            catalog_effects.push(constraint_effect);
            next_foreign_key_cid = if action.enforced {
                self.create_foreign_key_triggers_in_transaction(
                    client_id,
                    xid,
                    constraint_cid.saturating_add(1),
                    &constraint_row,
                    catalog_effects,
                )?
            } else {
                constraint_cid.saturating_add(1)
            };
            if referenced_relation.relkind == 'p' {
                next_foreign_key_cid = self
                    .create_referenced_partition_foreign_key_constraints_in_transaction(
                        client_id,
                        xid,
                        next_foreign_key_cid,
                        &referenced_relation,
                        &constraint_row,
                        &referenced_attnums,
                        configured_search_path,
                        catalog_effects,
                    )?;
            }
        }

        Ok(next_foreign_key_cid)
    }

    pub(super) fn refresh_partitioned_relation_metadata(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        _catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<crate::backend::parser::BoundRelation, ExecError> {
        self.invalidate_backend_cache_state(client_id);
        let visible_cid = cid.saturating_add(1);
        let catalog =
            self.lazy_catalog_lookup(client_id, Some((xid, visible_cid)), configured_search_path);
        catalog.relation_by_oid(relation_oid).ok_or_else(|| {
            ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn replace_relation_partition_metadata_in_transaction(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        relispartition: bool,
        relpartbound: Option<String>,
        partitioned_table: Option<crate::include::catalog::PgPartitionedTableRow>,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<crate::backend::parser::BoundRelation, ExecError> {
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
            .replace_relation_partitioning_mvcc(
                relation_oid,
                relispartition,
                relpartbound.clone(),
                partitioned_table.clone(),
                &ctx,
            )
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);

        let relation = self.refresh_partitioned_relation_metadata(
            client_id,
            relation_oid,
            xid,
            cid,
            configured_search_path,
            catalog_effects,
        )?;
        if relation.relpersistence == 't' {
            self.replace_temp_entry_partition_metadata(
                client_id,
                relation_oid,
                relation.relkind,
                relispartition,
                relpartbound,
                partitioned_table,
            )?;
        }
        Ok(relation)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn update_partitioned_table_default_partition_in_transaction(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        partdefid: u32,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let relation = self.refresh_partitioned_relation_metadata(
            client_id,
            relation_oid,
            xid,
            cid,
            configured_search_path,
            catalog_effects,
        )?;
        let Some(partitioned_table) = relation.partitioned_table.clone() else {
            return Ok(());
        };
        let updated = crate::include::catalog::PgPartitionedTableRow {
            partdefid,
            ..partitioned_table
        };
        let _ = self.replace_relation_partition_metadata_in_transaction(
            client_id,
            relation_oid,
            relation.relispartition,
            relation.relpartbound.clone(),
            Some(updated),
            xid,
            cid,
            configured_search_path,
            catalog_effects,
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn create_owned_sequence_for_serial_column(
        &self,
        client_id: ClientId,
        table_name: &str,
        namespace_oid: u32,
        persistence: TablePersistence,
        column: &OwnedSequenceSpec,
        xid: TransactionId,
        cid: CommandId,
        used_names: &mut std::collections::BTreeSet<String>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
        sequence_effects: &mut Vec<SequenceMutationEffect>,
    ) -> Result<CreatedOwnedSequence, ExecError> {
        let base_name = default_sequence_name_base(table_name, &column.column_name);
        let mut sequence_name =
            self.choose_available_relation_name(client_id, xid, cid, namespace_oid, &base_name)?;
        if !used_names.insert(sequence_name.to_ascii_lowercase()) {
            for suffix in 1.. {
                let candidate = format!("{base_name}{suffix}");
                if used_names.insert(candidate.to_ascii_lowercase()) {
                    sequence_name = candidate;
                    break;
                }
            }
        }

        let options = resolve_sequence_options_spec(
            &column.options,
            sequence_type_oid_for_serial_kind(column.serial_kind),
        )
        .map_err(ExecError::Parse)?;
        let data = SequenceData {
            state: initial_sequence_state(&options),
            options,
        };

        let sequence_oid = match persistence {
            TablePersistence::Permanent | TablePersistence::Unlogged => {
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid,
                    client_id,
                    waiter: None,
                    interrupts: self.interrupt_state(client_id),
                };
                let (entry, effect) = self
                    .catalog
                    .write()
                    .create_relation_mvcc_with_relkind(
                        sequence_name,
                        SequenceRuntime::sequence_relation_desc(),
                        namespace_oid,
                        1,
                        'p',
                        'S',
                        self.auth_state(client_id).current_user_oid(),
                        None,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                let pg_sequence_effect = self
                    .catalog
                    .write()
                    .upsert_sequence_row_mvcc(pg_sequence_row(entry.relation_oid, &data), &ctx)
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&pg_sequence_effect)?;
                catalog_effects.push(pg_sequence_effect);
                sequence_effects.push(self.sequences.apply_upsert(entry.relation_oid, data, true));
                entry.relation_oid
            }
            TablePersistence::Temporary => {
                let created = self.create_temp_relation_with_relkind_in_transaction(
                    client_id,
                    sequence_name,
                    SequenceRuntime::sequence_relation_desc(),
                    OnCommitAction::PreserveRows,
                    xid,
                    cid,
                    'S',
                    0,
                    None,
                    catalog_effects,
                    temp_effects,
                )?;
                sequence_effects.push(self.sequences.apply_upsert(
                    created.entry.relation_oid,
                    data,
                    false,
                ));
                created.entry.relation_oid
            }
        };

        Ok(CreatedOwnedSequence {
            column_index: column.column_index,
            sequence_oid,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_create_table_like_post_create_actions(
        &self,
        client_id: ClientId,
        relation: &crate::backend::parser::BoundRelation,
        lowered: &crate::backend::parser::LoweredCreateTable,
        xid: TransactionId,
        mut cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        for action in &lowered.like_post_create_actions {
            if action.include_comments {
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
                    .copy_relation_column_comments_mvcc(
                        action.source_relation_oid,
                        relation.relation_oid,
                        i32::try_from(relation.desc.columns.len()).unwrap_or(i32::MAX),
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                cid = cid.saturating_add(1);
            }

            if action.include_statistics {
                cid = self.copy_create_table_like_statistics(
                    client_id,
                    action.source_relation_oid,
                    relation,
                    action.include_comments,
                    xid,
                    cid,
                    catalog_effects,
                )?;
            }
        }
        Ok(cid)
    }

    #[allow(clippy::too_many_arguments)]
    fn copy_create_table_like_statistics(
        &self,
        client_id: ClientId,
        source_relation_oid: u32,
        relation: &crate::backend::parser::BoundRelation,
        include_comments: bool,
        xid: TransactionId,
        mut cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<CommandId, ExecError> {
        let source_statistics = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?
            .statistic_ext_rows_for_relation(source_relation_oid);
        if source_statistics.is_empty() {
            return Ok(cid);
        }

        let relation_name = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?
            .class_by_oid(relation.relation_oid)
            .map(|row| row.relname.to_ascii_lowercase())
            .unwrap_or_else(|| relation.relation_oid.to_string());
        let mut used_names = std::collections::BTreeSet::new();
        for source_row in source_statistics {
            let base_name =
                create_table_like_statistics_name_base(&relation_name, &source_row, relation);
            let mut candidate = base_name.clone();
            let mut suffix = 1usize;
            loop {
                let catalog = self
                    .backend_catcache(client_id, Some((xid, cid)))
                    .map_err(map_catalog_error)?;
                if !used_names.contains(&candidate)
                    && catalog
                        .statistic_ext_row_by_name_namespace(&candidate, relation.namespace_oid)
                        .is_none()
                {
                    break;
                }
                candidate = format!("{base_name}{suffix}");
                suffix = suffix.saturating_add(1);
            }
            used_names.insert(candidate.clone());

            let row = crate::include::catalog::PgStatisticExtRow {
                oid: 0,
                stxrelid: relation.relation_oid,
                stxname: candidate,
                stxnamespace: relation.namespace_oid,
                stxowner: self.auth_state(client_id).current_user_oid(),
                stxkeys: source_row.stxkeys.clone(),
                stxstattarget: source_row.stxstattarget,
                stxkind: source_row.stxkind.clone(),
                stxexprs: source_row.stxexprs.clone(),
            };
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid,
                client_id,
                waiter: None,
                interrupts: self.interrupt_state(client_id),
            };
            let (created_oid, effect) = self
                .catalog
                .write()
                .create_statistics_mvcc(row, &ctx)
                .map_err(map_catalog_error)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
            cid = cid.saturating_add(1);

            if include_comments {
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
                    .copy_object_comment_mvcc(
                        source_row.oid,
                        crate::include::catalog::PG_STATISTIC_EXT_RELATION_OID,
                        created_oid,
                        crate::include::catalog::PG_STATISTIC_EXT_RELATION_OID,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?;
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                cid = cid.saturating_add(1);
            }
        }

        self.plan_cache.invalidate_all();
        Ok(cid)
    }

    pub(crate) fn execute_create_domain_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateDomainStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let sql_type = crate::backend::parser::resolve_raw_type_name(&create_stmt.ty, &catalog)
            .map_err(ExecError::Parse)?;
        let enum_check = match &create_stmt.enum_check {
            Some(check) if matches!(sql_type.kind, crate::backend::parser::SqlTypeKind::Enum) => {
                let mut allowed_enum_label_oids = Vec::with_capacity(check.allowed_values.len());
                for value in &check.allowed_values {
                    let label_oid = catalog
                        .enum_label_oid(sql_type.type_oid, value)
                        .ok_or_else(|| ExecError::DetailedError {
                            message: format!(
                                "invalid input value for enum {}: \"{}\"",
                                catalog
                                    .type_by_oid(sql_type.type_oid)
                                    .map(|row| row.typname)
                                    .unwrap_or_else(|| sql_type.type_oid.to_string()),
                                value
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "22P02",
                        })?;
                    allowed_enum_label_oids.push(label_oid);
                }
                Some(crate::pgrust::database::DomainCheckEntry {
                    name: check
                        .name
                        .clone()
                        .unwrap_or_else(|| format!("{}_check", create_stmt.domain_name)),
                    allowed_enum_label_oids,
                })
            }
            Some(_) => None,
            None => None,
        };
        let (normalized, object_name, namespace_oid) = self.normalize_domain_name_for_create(
            client_id,
            &create_stmt.domain_name,
            configured_search_path,
        )?;
        let domains = self.domains.write();
        if domains.contains_key(&normalized) {
            return Err(ExecError::Parse(ParseError::UnsupportedType(
                create_stmt.domain_name.clone(),
            )));
        }
        drop(domains);
        let oid = self.allocate_dynamic_type_oids(
            2 + u32::try_from(create_stmt.constraints.len()).unwrap_or(0),
            None,
            None,
        )?;
        let array_oid = oid.saturating_add(1);
        let mut used_constraint_names = std::collections::BTreeSet::new();
        let constraints = create_stmt
            .constraints
            .iter()
            .enumerate()
            .map(|(index, constraint)| {
                let base_name = constraint
                    .name
                    .clone()
                    .unwrap_or_else(|| match constraint.kind {
                        crate::backend::parser::DomainConstraintSpecKind::Check { .. } => {
                            format!("{}_check", object_name)
                        }
                        crate::backend::parser::DomainConstraintSpecKind::NotNull => {
                            format!("{}_not_null", object_name)
                        }
                    });
                let name = unique_domain_constraint_name(base_name, &mut used_constraint_names);
                DomainConstraintEntry {
                    oid: oid.saturating_add(2 + u32::try_from(index).unwrap_or(0)),
                    name,
                    kind: match constraint.kind {
                        crate::backend::parser::DomainConstraintSpecKind::Check { .. } => {
                            DomainConstraintKind::Check
                        }
                        crate::backend::parser::DomainConstraintSpecKind::NotNull => {
                            DomainConstraintKind::NotNull
                        }
                    },
                    expr: match &constraint.kind {
                        crate::backend::parser::DomainConstraintSpecKind::Check { expr } => {
                            Some(expr.clone())
                        }
                        crate::backend::parser::DomainConstraintSpecKind::NotNull => None,
                    },
                    validated: !constraint.not_valid,
                    enforced: true,
                }
            })
            .collect::<Vec<_>>();
        let mut domains = self.domains.write();
        domains.insert(
            normalized,
            DomainEntry {
                oid,
                array_oid,
                name: object_name,
                namespace_oid,
                owner_oid: self.auth_state(client_id).current_user_oid(),
                sql_type,
                default: create_stmt.default.clone(),
                check: create_stmt.check.clone(),
                not_null: create_stmt.not_null,
                constraints,
                enum_check,
                typacl: None,
                comment: None,
            },
        );
        drop(domains);
        self.refresh_catalog_store_dynamic_type_rows(client_id, configured_search_path);
        self.invalidate_backend_cache_state(client_id);
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_create_function_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateFunctionStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_create_function_stmt_with_search_path_and_gucs(
            client_id,
            create_stmt,
            configured_search_path,
            None,
        )
    }

    pub(crate) fn execute_create_function_stmt_with_search_path_and_gucs(
        &self,
        client_id: ClientId,
        create_stmt: &CreateFunctionStatement,
        configured_search_path: Option<&[String]>,
        gucs: Option<&std::collections::HashMap<String, String>>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_function_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            0,
            configured_search_path,
            gucs,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_function_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateFunctionStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        gucs: Option<&std::collections::HashMap<String, String>>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_create_function_stmt_in_transaction_with_kind(
            client_id,
            create_stmt,
            xid,
            cid,
            configured_search_path,
            gucs,
            catalog_effects,
            'f',
            "function",
        )
    }

    pub(crate) fn execute_create_procedure_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateProcedureStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_procedure_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_procedure_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateProcedureStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        validate_sql_procedure_body(create_stmt, &catalog)?;
        let function_stmt = CreateFunctionStatement {
            schema_name: create_stmt.schema_name.clone(),
            function_name: create_stmt.procedure_name.clone(),
            replace_existing: create_stmt.replace_existing,
            cost: None,
            support: None,
            args: create_stmt.args.clone(),
            return_spec: if create_stmt
                .args
                .iter()
                .any(|arg| matches!(arg.mode, FunctionArgMode::Out | FunctionArgMode::InOut))
            {
                CreateFunctionReturnSpec::DerivedFromOutArgs {
                    setof_record: false,
                }
            } else {
                CreateFunctionReturnSpec::Type {
                    ty: RawTypeName::Builtin(SqlType::new(SqlTypeKind::Void)),
                    setof: false,
                }
            },
            strict: create_stmt.strict,
            leakproof: false,
            security_definer: false,
            volatility: create_stmt.volatility,
            parallel: FunctionParallel::Unsafe,
            language: create_stmt.language.clone(),
            body: create_stmt.body.clone(),
            link_symbol: None,
            config: Vec::new(),
        };
        self.execute_create_function_stmt_in_transaction_with_kind(
            client_id,
            &function_stmt,
            xid,
            cid,
            configured_search_path,
            None,
            catalog_effects,
            'p',
            "procedure",
        )
    }

    fn execute_create_function_stmt_in_transaction_with_kind(
        &self,
        client_id: ClientId,
        create_stmt: &CreateFunctionStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        gucs: Option<&std::collections::HashMap<String, String>>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        proc_kind: char,
        object_kind: &'static str,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let (function_name, namespace_oid) = normalize_create_proc_name_for_search_path(
            self,
            client_id,
            Some((xid, cid)),
            create_stmt.schema_name.as_deref(),
            &create_stmt.function_name,
            object_kind,
            configured_search_path,
        )?;
        validate_proc_arg_order(&create_stmt.args, proc_kind)?;
        if proc_kind == 'p' && create_stmt.strict {
            return Err(invalid_procedure_attribute());
        }

        let language_row = catalog
            .language_row_by_name(&create_stmt.language)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "LANGUAGE plpgsql, sql, internal, or c",
                    actual: format!("LANGUAGE {}", create_stmt.language),
                })
            })?;
        if !matches!(
            language_row.oid,
            PG_LANGUAGE_PLPGSQL_OID
                | PG_LANGUAGE_SQL_OID
                | PG_LANGUAGE_INTERNAL_OID
                | PG_LANGUAGE_C_OID
        ) {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "LANGUAGE plpgsql, sql, internal, or c",
                actual: format!("LANGUAGE {}", create_stmt.language),
            }));
        }

        let mut callable_arg_oids = Vec::new();
        let mut callable_arg_defaults = Vec::new();
        let mut all_arg_oids = Vec::new();
        let mut all_arg_modes = Vec::new();
        let mut all_arg_names = Vec::new();
        let mut output_args = Vec::new();
        let mut provariadic = 0;

        for arg in &create_stmt.args {
            let sql_type = resolve_raw_type_name(&arg.ty, &catalog).map_err(ExecError::Parse)?;
            if matches!(sql_type.kind, SqlTypeKind::Shell) {
                push_backend_notice(
                    "NOTICE",
                    "00000",
                    format!(
                        "argument type {} is only a shell",
                        notice_name_for_type(&arg.ty, sql_type)
                    ),
                    None,
                    arg.type_position,
                );
            }
            let type_oid = create_function_type_oid(
                &catalog,
                sql_type,
                arg.name.clone().unwrap_or_else(|| format!("{:?}", arg.ty)),
            )?;

            if matches!(arg.mode, FunctionArgMode::In | FunctionArgMode::InOut) {
                callable_arg_oids.push(type_oid);
                callable_arg_defaults.push(arg.default_expr.clone());
                if arg.variadic {
                    provariadic = variadic_element_type_oid(&catalog, type_oid);
                }
            }
            if matches!(arg.mode, FunctionArgMode::Out | FunctionArgMode::InOut) {
                output_args.push(QueryColumn {
                    name: arg.name.clone().unwrap_or_default(),
                    sql_type,
                    wire_type_oid: None,
                });
            }
            all_arg_oids.push(type_oid);
            all_arg_modes.push(if arg.variadic {
                b'v'
            } else {
                proc_arg_mode(arg.mode)
            });
            all_arg_names.push(arg.name.clone().unwrap_or_default());
        }

        let mut proretset = false;
        let prorettype: u32;
        let mut proallargtypes = None;
        let mut proargmodes = None;
        let mut proargnames = all_arg_names
            .iter()
            .any(|name| !name.is_empty())
            .then_some(all_arg_names.clone());

        match &create_stmt.return_spec {
            CreateFunctionReturnSpec::Type { ty, setof } => {
                let sql_type = match resolve_raw_type_name(ty, &catalog) {
                    Ok(sql_type) => {
                        if matches!(sql_type.kind, SqlTypeKind::Shell) {
                            push_notice(format!(
                                "return type {} is only a shell",
                                notice_name_for_type(ty, sql_type)
                            ));
                        }
                        sql_type
                    }
                    Err(ParseError::UnsupportedType(_)) => {
                        let Some(type_name) = raw_named_shell_type_name(ty) else {
                            return Err(ExecError::Parse(ParseError::UnsupportedType(format!(
                                "{ty:?}"
                            ))));
                        };
                        let (type_oid, object_name) = self
                            .create_shell_type_for_name_in_transaction(
                                client_id,
                                type_name,
                                xid,
                                cid,
                                configured_search_path,
                                catalog_effects,
                            )?;
                        push_notice_with_detail(
                            format!("type \"{object_name}\" is not yet defined"),
                            "Creating a shell type definition.",
                        );
                        SqlType::new(SqlTypeKind::Shell).with_identity(type_oid, 0)
                    }
                    Err(err) => return Err(ExecError::Parse(err)),
                };
                proretset = *setof;
                prorettype = create_function_type_oid(&catalog, sql_type, format!("{sql_type:?}"))?;
                if !output_args.is_empty() {
                    let required_rettype = if output_args.len() == 1 {
                        create_function_type_oid(
                            &catalog,
                            output_args[0].sql_type,
                            output_args[0].name.clone(),
                        )?
                    } else {
                        RECORD_TYPE_OID
                    };
                    if prorettype != required_rettype {
                        let message = if output_args.len() == 1 {
                            format!(
                                "function result type must be {} because of OUT parameters",
                                proc_signature_type_name(&catalog, required_rettype)
                            )
                        } else {
                            "function result type must be record because of OUT parameters".into()
                        };
                        return Err(ExecError::DetailedError {
                            message,
                            detail: None,
                            hint: None,
                            sqlstate: "42P13",
                        });
                    }
                    proallargtypes = Some(all_arg_oids.clone());
                    proargmodes = Some(all_arg_modes.clone());
                    proargnames = all_arg_names
                        .iter()
                        .any(|name| !name.is_empty())
                        .then_some(all_arg_names.clone());
                }
            }
            CreateFunctionReturnSpec::Table(columns) => {
                proretset = true;
                prorettype = RECORD_TYPE_OID;
                let mut table_oids = Vec::with_capacity(columns.len());
                let mut table_names = Vec::with_capacity(columns.len());
                for column in columns {
                    let sql_type =
                        resolve_raw_type_name(&column.ty, &catalog).map_err(ExecError::Parse)?;
                    if matches!(sql_type.kind, SqlTypeKind::Composite | SqlTypeKind::Record) {
                        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                            "record and composite RETURNS TABLE columns are not supported yet"
                                .into(),
                        )));
                    }
                    table_oids.push(catalog.type_oid_for_sql_type(sql_type).ok_or_else(|| {
                        ExecError::Parse(ParseError::UnsupportedType(column.name.clone()))
                    })?);
                    table_names.push(column.name.clone());
                }
                proallargtypes = Some(
                    callable_arg_oids
                        .iter()
                        .copied()
                        .chain(table_oids.iter().copied())
                        .collect(),
                );
                proargmodes = Some(
                    create_stmt
                        .args
                        .iter()
                        .map(|arg| proc_arg_mode(arg.mode))
                        .filter(|mode| matches!(*mode, b'i' | b'b'))
                        .chain(std::iter::repeat_n(b't', table_oids.len()))
                        .collect(),
                );
                let mut names = create_stmt
                    .args
                    .iter()
                    .filter(|arg| matches!(arg.mode, FunctionArgMode::In | FunctionArgMode::InOut))
                    .map(|arg| arg.name.clone().unwrap_or_default())
                    .collect::<Vec<_>>();
                names.extend(table_names);
                proargnames = Some(names);
            }
            CreateFunctionReturnSpec::DerivedFromOutArgs { setof_record } => {
                if output_args.is_empty() {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "OUT or INOUT arguments",
                        actual: create_stmt.function_name.clone(),
                    }));
                }
                proallargtypes = Some(all_arg_oids.clone());
                proargmodes = Some(all_arg_modes.clone());
                proargnames = all_arg_names
                    .iter()
                    .any(|name| !name.is_empty())
                    .then_some(all_arg_names.clone());
                if *setof_record {
                    proretset = true;
                    prorettype = RECORD_TYPE_OID;
                } else if output_args.len() == 1 {
                    prorettype = create_function_type_oid(
                        &catalog,
                        output_args[0].sql_type,
                        output_args[0].name.clone(),
                    )?;
                } else {
                    prorettype = RECORD_TYPE_OID;
                }
            }
        }
        if provariadic != 0 && proargmodes.is_none() {
            proallargtypes = Some(all_arg_oids.clone());
            proargmodes = Some(all_arg_modes.clone());
        }

        validate_polymorphic_output_types(
            prorettype,
            proallargtypes.as_ref(),
            proargmodes.as_ref(),
            &callable_arg_oids,
        )?;
        validate_polymorphic_range_output_types(
            prorettype,
            proallargtypes.as_ref(),
            proargmodes.as_ref(),
            &callable_arg_oids,
        )?;

        let proargtypes = callable_arg_oids
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(" ");
        validate_range_polymorphic_result(
            prorettype,
            proallargtypes.as_deref(),
            proargmodes.as_deref(),
            &callable_arg_oids,
        )?;
        if prorettype == EVENT_TRIGGER_TYPE_OID {
            if !callable_arg_oids.is_empty() {
                return Err(ExecError::WithContext {
                    source: Box::new(ExecError::DetailedError {
                        message: "event trigger functions cannot have declared arguments".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "42P13",
                    }),
                    context: format!(
                        "compilation of PL/pgSQL function \"{}\" near line 1",
                        create_stmt.function_name
                    ),
                });
            }
            if language_row.oid == PG_LANGUAGE_SQL_OID {
                return Err(ExecError::DetailedError {
                    message: "SQL functions cannot return type event_trigger".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                });
            }
        }
        if language_row.oid == PG_LANGUAGE_PLPGSQL_OID {
            let validation_notices = validate_create_function_body_with_options(
                &create_stmt.body,
                !output_args.is_empty(),
                proargnames.as_deref().unwrap_or(&[]),
                gucs,
            )
            .map_err(ExecError::Parse)?;
            for notice in validation_notices {
                push_backend_notice(notice.severity, notice.sqlstate, notice.message, None, None);
            }
        }
        let existing_proc = catalog
            .proc_rows_by_name(&function_name)
            .into_iter()
            .find(|row| row.pronamespace == namespace_oid && row.proargtypes == proargtypes);
        if existing_proc.is_some() && !create_stmt.replace_existing {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "unique routine signature",
                actual: format!(
                    "{object_kind} {}({}) already exists",
                    function_name, proargtypes
                ),
            }));
        }

        let prosrc = if language_row.oid == PG_LANGUAGE_C_OID {
            create_stmt
                .link_symbol
                .clone()
                .unwrap_or_else(|| function_name.clone())
        } else {
            create_stmt.body.clone()
        };
        let probin = if language_row.oid == PG_LANGUAGE_C_OID {
            Some(create_stmt.body.clone())
        } else {
            None
        };
        let prosupport = create_stmt
            .support
            .as_ref()
            .map(|signature| {
                resolve_support_proc_oid(self, client_id, Some((xid, cid)), &catalog, signature)
            })
            .transpose()?
            .unwrap_or_default();

        let proc_row = PgProcRow {
            oid: 0,
            proname: function_name.clone(),
            pronamespace: namespace_oid,
            proowner: self.auth_state(client_id).current_user_oid(),
            proacl: None,
            prolang: language_row.oid,
            procost: create_stmt
                .cost
                .as_deref()
                .map(|cost| {
                    cost.parse::<f64>()
                        .expect("validated CREATE FUNCTION COST must parse")
                })
                .unwrap_or(100.0),
            prorows: if proretset { 1000.0 } else { 0.0 },
            provariadic,
            prosupport,
            prokind: proc_kind,
            prosecdef: create_stmt.security_definer,
            proleakproof: create_stmt.leakproof,
            proisstrict: create_stmt.strict,
            proretset,
            provolatile: match create_stmt.volatility {
                FunctionVolatility::Volatile => 'v',
                FunctionVolatility::Stable => 's',
                FunctionVolatility::Immutable => 'i',
            },
            proparallel: proc_parallel_code(create_stmt.parallel),
            pronargs: callable_arg_oids.len() as i16,
            pronargdefaults: callable_arg_defaults
                .iter()
                .filter(|default_expr| default_expr.is_some())
                .count() as i16,
            prorettype: if proc_kind == 'p' {
                VOID_TYPE_OID
            } else {
                prorettype
            },
            proargtypes,
            proallargtypes,
            proargmodes,
            proargnames,
            proargdefaults: encode_proc_arg_defaults(&callable_arg_defaults),
            prosrc,
            probin,
            prosqlbody: None,
            proconfig: proc_config_from_options(&create_stmt.config),
        };
        if proc_kind == 'f'
            && let Some(existing) = existing_proc.as_ref()
        {
            validate_replaced_proc_signature(existing, &proc_row, &catalog)?;
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts,
        };
        let effect = {
            let mut catalog_store = self.catalog.write();
            let (_oid, effect) = if let Some(existing) = existing_proc {
                catalog_store
                    .replace_proc_mvcc(&existing, proc_row, &ctx)
                    .map_err(map_catalog_error)?
            } else {
                catalog_store
                    .create_proc_mvcc(proc_row, &ctx)
                    .map_err(map_catalog_error)?
            };
            effect
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_create_aggregate_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateAggregateStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_aggregate_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_aggregate_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateAggregateStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let txn_ctx = Some((xid, cid));
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
        let (aggregate_name, namespace_oid) = normalize_create_proc_name_for_search_path(
            self,
            client_id,
            txn_ctx,
            create_stmt.schema_name.as_deref(),
            &create_stmt.aggregate_name,
            "aggregate",
            configured_search_path,
        )?;
        let arg_oids = aggregate_signature_arg_oids(&catalog, &create_stmt.signature)?;
        let transition_input_oids =
            aggregate_transition_input_oids(&catalog, &create_stmt.signature)?;
        let provariadic = aggregate_provariadic(&catalog, &create_stmt.signature)?;
        let explicit_variadic = provariadic != 0;
        let stype =
            resolve_raw_type_name(&create_stmt.stype, &catalog).map_err(ExecError::Parse)?;
        let stype_oid = catalog
            .type_oid_for_sql_type(stype)
            .ok_or_else(|| ExecError::Parse(ParseError::UnsupportedType(format!("{stype:?}"))))?;
        validate_polymorphic_aggregate_transition_type(stype_oid, &transition_input_oids)?;
        if create_stmt.serialfunc_name.is_some() != create_stmt.deserialfunc_name.is_some() {
            return Err(ExecError::DetailedError {
                message:
                    "must specify both or neither of serialization and deserialization functions"
                        .into(),
                detail: None,
                hint: None,
                sqlstate: "42P13",
            });
        }
        let mut trans_arg_oids = Vec::with_capacity(transition_input_oids.len() + 1);
        trans_arg_oids.push(stype_oid);
        trans_arg_oids.extend(transition_input_oids.iter().copied());
        let transfn_row = lookup_aggregate_support_proc_row(
            &catalog,
            &create_stmt.sfunc_name,
            &trans_arg_oids,
            explicit_variadic,
        )?;
        if support_result_type_oid(&catalog, &transfn_row)? != stype_oid {
            return Err(ExecError::DetailedError {
                message: format!(
                    "return type of transition function {} is not {}",
                    create_stmt.sfunc_name,
                    format_sql_type_name(stype)
                ),
                detail: None,
                hint: None,
                sqlstate: "42P13",
            });
        }
        let mut final_arg_oids = vec![stype_oid];
        if create_stmt.finalfunc_extra {
            final_arg_oids.extend(arg_oids.iter().copied());
        }
        let finalfn_row = create_stmt
            .finalfunc_name
            .as_deref()
            .map(|name| lookup_aggregate_support_proc_row(&catalog, name, &final_arg_oids, false))
            .transpose()?;
        let combinefn_row = create_stmt
            .combinefunc_name
            .as_deref()
            .map(|name| {
                lookup_aggregate_support_proc_row(&catalog, name, &[stype_oid, stype_oid], false)
            })
            .transpose()?;
        if let Some(combinefn_row) = combinefn_row.as_ref()
            && support_result_type_oid(&catalog, combinefn_row)? != stype_oid
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "return type of combine function {} is not {}",
                    create_stmt.combinefunc_name.as_deref().unwrap_or_default(),
                    format_sql_type_name(stype)
                ),
                detail: None,
                hint: None,
                sqlstate: "42P13",
            });
        }
        let serialfn_row = create_stmt
            .serialfunc_name
            .as_deref()
            .map(|name| lookup_aggregate_support_proc_row(&catalog, name, &[stype_oid], false))
            .transpose()?;
        if let Some(serialfn_row) = serialfn_row.as_ref()
            && support_result_type_oid(&catalog, serialfn_row)? != BYTEA_TYPE_OID
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "return type of serialization function {} is not bytea",
                    create_stmt.serialfunc_name.as_deref().unwrap_or_default()
                ),
                detail: None,
                hint: None,
                sqlstate: "42P13",
            });
        }
        let deserialfn_row = create_stmt
            .deserialfunc_name
            .as_deref()
            .map(|name| {
                lookup_aggregate_support_proc_row(
                    &catalog,
                    name,
                    &[BYTEA_TYPE_OID, INTERNAL_TYPE_OID],
                    false,
                )
            })
            .transpose()?;
        if let Some(deserialfn_row) = deserialfn_row.as_ref()
            && support_result_type_oid(&catalog, deserialfn_row)? != stype_oid
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "return type of deserialization function {} is not {}",
                    create_stmt.deserialfunc_name.as_deref().unwrap_or_default(),
                    format_sql_type_name(stype)
                ),
                detail: None,
                hint: None,
                sqlstate: "42P13",
            });
        }
        let mstype_oid = create_stmt
            .mstype
            .as_ref()
            .map(|mtype| {
                resolve_raw_type_name(mtype, &catalog)
                    .map_err(ExecError::Parse)
                    .and_then(|sql_type| {
                        catalog.type_oid_for_sql_type(sql_type).ok_or_else(|| {
                            ExecError::Parse(ParseError::UnsupportedType(format!("{sql_type:?}")))
                        })
                    })
            })
            .transpose()?;
        let msfunc_row = create_stmt
            .msfunc_name
            .as_deref()
            .map(|name| {
                let mstype_oid = mstype_oid.unwrap_or(stype_oid);
                let mut args = Vec::with_capacity(transition_input_oids.len() + 1);
                args.push(mstype_oid);
                args.extend(transition_input_oids.iter().copied());
                lookup_aggregate_support_proc_row(&catalog, name, &args, explicit_variadic)
            })
            .transpose()?;
        if let Some(msfunc_row) = msfunc_row.as_ref()
            && support_result_type_oid(&catalog, msfunc_row)? != mstype_oid.unwrap_or(stype_oid)
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "return type of forward transition function {} is not {}",
                    create_stmt.msfunc_name.as_deref().unwrap_or_default(),
                    format_sql_type_name(stype)
                ),
                detail: None,
                hint: None,
                sqlstate: "42P13",
            });
        }
        let minvfunc_row = create_stmt
            .minvfunc_name
            .as_deref()
            .map(|name| {
                let mstype_oid = mstype_oid.unwrap_or(stype_oid);
                let mut args = Vec::with_capacity(transition_input_oids.len() + 1);
                args.push(mstype_oid);
                args.extend(transition_input_oids.iter().copied());
                lookup_aggregate_support_proc_row(&catalog, name, &args, explicit_variadic)
            })
            .transpose()?;
        if let Some(minvfunc_row) = minvfunc_row.as_ref()
            && support_result_type_oid(&catalog, minvfunc_row)? != mstype_oid.unwrap_or(stype_oid)
        {
            return Err(ExecError::DetailedError {
                message: format!(
                    "return type of inverse transition function {} is not {}",
                    create_stmt.minvfunc_name.as_deref().unwrap_or_default(),
                    format_sql_type_name(stype)
                ),
                detail: None,
                hint: None,
                sqlstate: "42P13",
            });
        }
        if let (Some(msfunc_row), Some(minvfunc_row)) = (&msfunc_row, &minvfunc_row)
            && msfunc_row.row.proisstrict != minvfunc_row.row.proisstrict
        {
            return Err(ExecError::DetailedError {
                message:
                    "strictness of aggregate's forward and inverse transition functions must match"
                        .into(),
                detail: None,
                hint: None,
                sqlstate: "42P13",
            });
        }
        let mut mfinal_arg_oids = vec![mstype_oid.unwrap_or(stype_oid)];
        if create_stmt.mfinalfunc_extra {
            mfinal_arg_oids.extend(arg_oids.iter().copied());
        }
        let mfinalfn_row = create_stmt
            .mfinalfunc_name
            .as_deref()
            .map(|name| lookup_aggregate_support_proc_row(&catalog, name, &mfinal_arg_oids, false))
            .transpose()?;
        let result_type_oid = finalfn_row
            .as_ref()
            .map(|support| support_result_type_oid(&catalog, support))
            .transpose()?
            .unwrap_or(stype_oid);
        let proargtypes = arg_oids
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(" ");
        let conflicting_non_aggregate = catalog
            .proc_rows_by_name(&aggregate_name)
            .into_iter()
            .find(|row| {
                row.pronamespace == namespace_oid
                    && parse_proc_argtype_oids(&row.proargtypes)
                        .is_some_and(|row_arg_oids| row_arg_oids == arg_oids)
                    && row.prokind != 'a'
            });
        if let Some(conflicting_non_aggregate) = conflicting_non_aggregate {
            if create_stmt.replace_existing {
                return Err(cannot_change_routine_kind_error(
                    &aggregate_name,
                    conflicting_non_aggregate.prokind,
                    None,
                ));
            }
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: aggregate_name.clone(),
                expected: "aggregate",
            }));
        }
        let existing =
            resolve_aggregate_proc_rows(&catalog, &aggregate_name, Some(namespace_oid), &arg_oids);
        if !existing.is_empty() && !create_stmt.replace_existing {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "unique aggregate signature",
                actual: format!("aggregate {aggregate_name}({proargtypes}) already exists"),
            }));
        }
        if let Some((old_proc_row, old_aggregate_row)) = existing.first()
            && create_stmt.replace_existing
        {
            let new_aggkind = aggregate_kind(create_stmt);
            if old_aggregate_row.aggkind != new_aggkind {
                return Err(cannot_change_routine_kind_error(
                    &aggregate_name,
                    old_proc_row.prokind,
                    Some(old_aggregate_row.aggkind),
                ));
            }
            if old_proc_row.prorettype != result_type_oid {
                return Err(ExecError::DetailedError {
                    message: "cannot change return type of existing function".into(),
                    detail: None,
                    hint: Some(format!(
                        "Use DROP AGGREGATE {} first.",
                        format_aggregate_signature(
                            &aggregate_name,
                            &create_stmt.signature,
                            &catalog,
                        )?
                    )),
                    sqlstate: "42P13",
                });
            }
        }

        let proc_row = PgProcRow {
            oid: 0,
            proname: aggregate_name.clone(),
            pronamespace: namespace_oid,
            proowner: self.auth_state(client_id).current_user_oid(),
            proacl: None,
            prolang: PG_LANGUAGE_INTERNAL_OID,
            procost: 1.0,
            prorows: 0.0,
            provariadic,
            prosupport: 0,
            prokind: 'a',
            prosecdef: false,
            proleakproof: false,
            proisstrict: false,
            proretset: false,
            provolatile: 'i',
            proparallel: create_stmt.parallel.map(proc_parallel_code).unwrap_or('u'),
            pronargs: arg_oids.len() as i16,
            pronargdefaults: 0,
            prorettype: result_type_oid,
            proargtypes,
            proallargtypes: (!matches!(create_stmt.signature, AggregateSignatureKind::Star)
                && (aggregate_arg_modes(&create_stmt.signature).is_some()
                    || aggregate_arg_names(&create_stmt.signature).is_some()))
            .then_some(arg_oids.clone()),
            proargmodes: aggregate_arg_modes(&create_stmt.signature),
            proargnames: aggregate_arg_names(&create_stmt.signature),
            proargdefaults: None,
            prosrc: aggregate_name.clone(),
            probin: None,
            prosqlbody: None,
            proconfig: None,
        };
        let aggregate_row = PgAggregateRow {
            aggfnoid: 0,
            aggkind: aggregate_kind(create_stmt),
            aggnumdirectargs: aggregate_direct_arg_count(&create_stmt.signature),
            aggtransfn: transfn_row.row.oid,
            aggfinalfn: finalfn_row
                .as_ref()
                .map(|support| support.row.oid)
                .unwrap_or(0),
            aggcombinefn: combinefn_row
                .as_ref()
                .map(|support| support.row.oid)
                .unwrap_or(0),
            aggserialfn: serialfn_row
                .as_ref()
                .map(|support| support.row.oid)
                .unwrap_or(0),
            aggdeserialfn: deserialfn_row
                .as_ref()
                .map(|support| support.row.oid)
                .unwrap_or(0),
            aggmtransfn: msfunc_row
                .as_ref()
                .map(|support| support.row.oid)
                .unwrap_or(0),
            aggminvtransfn: minvfunc_row
                .as_ref()
                .map(|support| support.row.oid)
                .unwrap_or(0),
            aggmfinalfn: mfinalfn_row
                .as_ref()
                .map(|support| support.row.oid)
                .unwrap_or(0),
            aggfinalextra: create_stmt.finalfunc_extra,
            aggmfinalextra: create_stmt.mfinalfunc_extra,
            aggfinalmodify: create_stmt.finalfunc_modify,
            aggmfinalmodify: create_stmt.mfinalfunc_modify,
            aggsortop: 0,
            aggtranstype: stype_oid,
            aggtransspace: create_stmt.transspace,
            aggmtranstype: mstype_oid.unwrap_or(0),
            aggmtransspace: create_stmt.mtransspace,
            agginitval: create_stmt.initcond.clone(),
            aggminitval: create_stmt.minitcond.clone(),
        };
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts,
        };
        let effect = {
            let mut catalog_store = self.catalog.write();
            let (_oid, effect) = if let Some((old_proc_row, old_aggregate_row)) = existing.first() {
                catalog_store
                    .replace_aggregate_mvcc(
                        old_proc_row,
                        old_aggregate_row,
                        proc_row,
                        aggregate_row,
                        &ctx,
                    )
                    .map_err(map_catalog_error)?
            } else {
                catalog_store
                    .create_aggregate_mvcc(proc_row, aggregate_row, &ctx)
                    .map_err(map_catalog_error)?
            };
            effect
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_aggregate_rename_stmt_with_search_path(
        &self,
        client_id: ClientId,
        rename_stmt: &AlterAggregateRenameStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_aggregate_rename_stmt_in_transaction_with_search_path(
            client_id,
            rename_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_aggregate_rename_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        rename_stmt: &AlterAggregateRenameStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let txn_ctx = Some((xid, cid));
        let catalog = self.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
        let arg_oids = aggregate_signature_arg_oids(&catalog, &rename_stmt.signature)?;
        let schema_oid = match &rename_stmt.schema_name {
            Some(schema_name) => Some(
                self.visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("schema \"{schema_name}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    })?,
            ),
            None => None,
        };
        let matches = resolve_aggregate_proc_rows(
            &catalog,
            &rename_stmt.aggregate_name,
            schema_oid,
            &arg_oids,
        );
        let (proc_row, aggregate_row) = match matches.as_slice() {
            [(proc_row, aggregate_row)] => (proc_row.clone(), aggregate_row.clone()),
            [] => {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "aggregate {} does not exist",
                        format_aggregate_signature(
                            &rename_stmt.aggregate_name,
                            &rename_stmt.signature,
                            &catalog
                        )?
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42883",
                });
            }
            _ => {
                return Err(ExecError::DetailedError {
                    message: format!("aggregate name {} is ambiguous", rename_stmt.aggregate_name),
                    detail: None,
                    hint: None,
                    sqlstate: "42725",
                });
            }
        };
        if !resolve_aggregate_proc_rows(&catalog, &rename_stmt.new_name, schema_oid, &arg_oids)
            .is_empty()
        {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "unused aggregate name",
                actual: format!("aggregate {} already exists", rename_stmt.new_name),
            }));
        }
        let mut new_proc_row = proc_row.clone();
        new_proc_row.proname = rename_stmt.new_name.clone();
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let (_oid, effect) = self
            .catalog
            .write()
            .replace_aggregate_mvcc(
                &proc_row,
                &aggregate_row,
                new_proc_row,
                aggregate_row.clone(),
                &ctx,
            )
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_create_table_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut temp_effects = Vec::new();
        let mut sequence_effects = Vec::new();
        let result = self.execute_create_table_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
            &mut temp_effects,
            &mut sequence_effects,
        );
        let result = self.finish_txn(
            client_id,
            xid,
            result,
            &catalog_effects,
            &temp_effects,
            &sequence_effects,
        );
        guard.disarm();
        result
    }

    fn apply_created_toast_reloptions_in_transaction(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        toast: Option<&crate::backend::catalog::toasting::ToastCatalogChanges>,
        reloptions: Option<&Vec<String>>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<(), ExecError> {
        let (Some(toast), Some(reloptions)) = (toast, reloptions) else {
            return Ok(());
        };
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
            .alter_relation_reloptions_mvcc(
                toast.toast_entry.relation_oid,
                Some(reloptions.clone()),
                &ctx,
            )
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(())
    }

    pub(crate) fn execute_create_view_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateViewStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut temp_effects = Vec::new();
        let result = self.execute_create_view_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
            &mut temp_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects, &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_table_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
        sequence_effects: &mut Vec<SequenceMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let (table_name, namespace_oid, persistence) = self
            .normalize_create_table_stmt_with_search_path(
                client_id,
                Some((xid, cid)),
                create_stmt,
                configured_search_path,
            )?;
        let mut table_cid = cid;
        if persistence == TablePersistence::Temporary {
            self.ensure_temp_namespace(
                client_id,
                xid,
                &mut table_cid,
                catalog_effects,
                temp_effects,
            )?;
        }
        let catalog =
            self.lazy_catalog_lookup(client_id, Some((xid, table_cid)), configured_search_path);
        let lowered = lower_create_table_with_catalog(create_stmt, &catalog, persistence)?;
        self.ensure_create_table_type_usage(
            client_id,
            Some((xid, table_cid)),
            &lowered.relation_desc,
        )?;
        if create_stmt.if_not_exists
            && relation_exists_in_namespace(&catalog, &table_name, namespace_oid)
        {
            push_notice(format!(
                "relation \"{table_name}\" already exists, skipping"
            ));
            return Ok(StatementResult::AffectedRows(0));
        }
        validate_partitioned_table_ddl(&table_name, &lowered)?;
        if let Some(parent_oid) = lowered.partition_parent_oid
            && let Some(bound) = lowered.partition_bound.as_ref()
        {
            let parent = catalog.relation_by_oid(parent_oid).ok_or_else(|| {
                ExecError::Parse(ParseError::UnknownTable(parent_oid.to_string()))
            })?;
            validate_new_partition_bound(&catalog, &parent, &table_name, bound, None)?;
        }

        let mut desc = lowered.relation_desc.clone();
        let mut used_sequence_names = std::collections::BTreeSet::new();
        let mut created_sequences = Vec::with_capacity(lowered.owned_sequences.len());
        for serial_column in &lowered.owned_sequences {
            created_sequences.push(self.create_owned_sequence_for_serial_column(
                client_id,
                &table_name,
                namespace_oid,
                persistence,
                serial_column,
                xid,
                table_cid,
                &mut used_sequence_names,
                catalog_effects,
                temp_effects,
                sequence_effects,
            )?);
        }
        for created in &created_sequences {
            let column = desc
                .columns
                .get_mut(created.column_index)
                .expect("serial column index must exist");
            column.default_expr = Some(format_nextval_default_oid(
                created.sequence_oid,
                column.sql_type,
            ));
            column.default_sequence_oid = Some(created.sequence_oid);
            column.missing_default_value = None;
        }

        let relation_relkind = created_relkind(&lowered);
        let reloptions = normalize_create_table_reloptions(&create_stmt.options)?;
        if relation_relkind == 'p' && persistence == TablePersistence::Unlogged {
            return Err(ExecError::Parse(ParseError::DetailedError {
                message: "partitioned tables cannot be unlogged".into(),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            }));
        }
        let relpersistence = match persistence {
            TablePersistence::Permanent => 'p',
            TablePersistence::Unlogged => 'u',
            TablePersistence::Temporary => 't',
        };
        match persistence {
            TablePersistence::Permanent | TablePersistence::Unlogged => {
                let mut catalog_guard = self.catalog.write();
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: table_cid,
                    client_id,
                    waiter: None,
                    interrupts: Arc::clone(&interrupts),
                };
                let result = if relation_relkind == 'r' {
                    catalog_guard.create_typed_table_mvcc_with_options(
                        table_name.clone(),
                        desc.clone(),
                        namespace_oid,
                        self.database_oid,
                        relpersistence,
                        crate::include::catalog::PG_TOAST_NAMESPACE_OID,
                        crate::backend::catalog::toasting::PG_TOAST_NAMESPACE,
                        self.auth_state(client_id).current_user_oid(),
                        lowered.of_type_oid,
                        reloptions.heap.clone(),
                        &ctx,
                    )
                } else {
                    catalog_guard
                        .create_relation_mvcc_with_relkind(
                            table_name.clone(),
                            desc.clone(),
                            namespace_oid,
                            self.database_oid,
                            relpersistence,
                            relation_relkind,
                            self.auth_state(client_id).current_user_oid(),
                            reloptions.heap.clone(),
                            &ctx,
                        )
                        .map(|(entry, effect)| {
                            (
                                crate::backend::catalog::store::CreateTableResult {
                                    entry,
                                    toast: None,
                                },
                                effect,
                            )
                        })
                };
                match result {
                    Err(CatalogError::TableAlreadyExists(_name)) if create_stmt.if_not_exists => {
                        push_notice(format!(
                            "relation \"{table_name}\" already exists, skipping"
                        ));
                        Ok(StatementResult::AffectedRows(0))
                    }
                    Err(err) => Err(map_catalog_error(err)),
                    Ok((created, effect)) => {
                        drop(catalog_guard);
                        self.apply_catalog_mutation_effect_immediate(&effect)?;
                        catalog_effects.push(effect);
                        self.apply_created_toast_reloptions_in_transaction(
                            client_id,
                            xid,
                            table_cid.saturating_add(1),
                            created.toast.as_ref(),
                            reloptions.toast.as_ref(),
                            catalog_effects,
                        )?;
                        if !lowered.parent_oids.is_empty() {
                            let inherit_ctx = CatalogWriteContext {
                                pool: self.pool.clone(),
                                txns: self.txns.clone(),
                                xid,
                                cid: table_cid.saturating_add(1),
                                client_id,
                                waiter: None,
                                interrupts: Arc::clone(&interrupts),
                            };
                            let inherit_effect = self
                                .catalog
                                .write()
                                .create_relation_inheritance_mvcc(
                                    created.entry.relation_oid,
                                    &lowered.parent_oids,
                                    &inherit_ctx,
                                )
                                .map_err(map_catalog_error)?;
                            self.apply_catalog_mutation_effect_immediate(&inherit_effect)?;
                            catalog_effects.push(inherit_effect);
                        }
                        let relpartbound = lowered
                            .partition_bound
                            .as_ref()
                            .map(serialize_partition_bound)
                            .transpose()
                            .map_err(ExecError::Parse)?;
                        let partitioned_table = lowered.partition_spec.as_ref().map(|spec| {
                            pg_partitioned_table_row(created.entry.relation_oid, spec, 0)
                        });
                        let relation = if lowered.partition_parent_oid.is_some()
                            || lowered.partition_spec.is_some()
                        {
                            let relation = self
                                .replace_relation_partition_metadata_in_transaction(
                                    client_id,
                                    created.entry.relation_oid,
                                    lowered.partition_parent_oid.is_some(),
                                    relpartbound,
                                    partitioned_table,
                                    xid,
                                    table_cid.saturating_add(2),
                                    configured_search_path,
                                    catalog_effects,
                                )?;
                            if lowered
                                .partition_bound
                                .as_ref()
                                .is_some_and(PartitionBoundSpec::is_default)
                                && let Some(parent_oid) = lowered.partition_parent_oid
                            {
                                self.update_partitioned_table_default_partition_in_transaction(
                                    client_id,
                                    parent_oid,
                                    created.entry.relation_oid,
                                    xid,
                                    table_cid.saturating_add(3),
                                    configured_search_path,
                                    catalog_effects,
                                )?;
                            }
                            relation
                        } else {
                            crate::backend::parser::BoundRelation {
                                rel: created.entry.rel,
                                relation_oid: created.entry.relation_oid,
                                toast: created.toast.as_ref().map(|toast| {
                                    crate::include::nodes::primnodes::ToastRelationRef {
                                        rel: toast.toast_entry.rel,
                                        relation_oid: toast.toast_entry.relation_oid,
                                    }
                                }),
                                namespace_oid: created.entry.namespace_oid,
                                owner_oid: created.entry.owner_oid,
                                of_type_oid: created.entry.of_type_oid,
                                relpersistence: created.entry.relpersistence,
                                relkind: created.entry.relkind,
                                relispopulated: created.entry.relispopulated,
                                relispartition: created.entry.relispartition,
                                relpartbound: created.entry.relpartbound.clone(),
                                desc: created.entry.desc.clone(),
                                partitioned_table: created.entry.partitioned_table.clone(),
                                partition_spec: None,
                            }
                        };
                        for created_sequence in &created_sequences {
                            let ctx = CatalogWriteContext {
                                pool: self.pool.clone(),
                                txns: self.txns.clone(),
                                xid,
                                cid: table_cid.saturating_add(1),
                                client_id,
                                waiter: None,
                                interrupts: Arc::clone(&interrupts),
                            };
                            let effect = self
                                .catalog
                                .write()
                                .set_sequence_owned_by_dependency_mvcc(
                                    created_sequence.sequence_oid,
                                    Some((
                                        relation.relation_oid,
                                        created_sequence.column_index.saturating_add(1) as i32,
                                    )),
                                    &ctx,
                                )
                                .map_err(map_catalog_error)?;
                            self.apply_catalog_mutation_effect_immediate(&effect)?;
                            catalog_effects.push(effect);
                        }
                        let mut constraint_cid_base = table_cid.saturating_add(1);
                        if !lowered.parent_oids.is_empty() {
                            constraint_cid_base =
                                constraint_cid_base.max(table_cid.saturating_add(2));
                        }
                        if lowered.partition_spec.is_some()
                            || lowered.partition_parent_oid.is_some()
                        {
                            constraint_cid_base =
                                constraint_cid_base.max(table_cid.saturating_add(3));
                        }
                        if lowered
                            .partition_bound
                            .as_ref()
                            .is_some_and(PartitionBoundSpec::is_default)
                        {
                            constraint_cid_base =
                                constraint_cid_base.max(table_cid.saturating_add(4));
                        }
                        let next_cid = self.install_create_table_constraints_in_transaction(
                            client_id,
                            xid,
                            constraint_cid_base,
                            &table_name,
                            &relation,
                            &lowered,
                            configured_search_path,
                            catalog_effects,
                        )?;
                        let next_cid = self.apply_create_table_like_post_create_actions(
                            client_id,
                            &relation,
                            &lowered,
                            xid,
                            next_cid,
                            catalog_effects,
                        )?;
                        if let Some(parent_oid) = lowered.partition_parent_oid {
                            let next_cid = self
                                .reconcile_partitioned_parent_keys_for_attached_child_in_transaction(
                                    client_id,
                                    xid,
                                    next_cid,
                                    parent_oid,
                                    relation.relation_oid,
                                    configured_search_path,
                                    catalog_effects,
                                )?;
                            let next_cid = self
                                .reconcile_partitioned_parent_indexes_for_attached_child_in_transaction(
                                    client_id,
                                    xid,
                                    next_cid,
                                    parent_oid,
                                    relation.relation_oid,
                                    configured_search_path,
                                    catalog_effects,
                                )?;
                            let next_cid = self
                                .reconcile_partitioned_parent_foreign_keys_for_attached_child_in_transaction(
                                    client_id,
                                    xid,
                                    next_cid,
                                    parent_oid,
                                    relation.relation_oid,
                                    configured_search_path,
                                    catalog_effects,
                                )?;
                            self.clone_parent_row_triggers_to_partition_in_transaction(
                                client_id,
                                xid,
                                next_cid,
                                parent_oid,
                                relation.relation_oid,
                                configured_search_path,
                                catalog_effects,
                            )?;
                        }
                        Ok(StatementResult::AffectedRows(0))
                    }
                }
            }
            TablePersistence::Temporary => {
                let created = self.create_temp_relation_with_relkind_in_transaction(
                    client_id,
                    table_name.clone(),
                    desc.clone(),
                    create_stmt.on_commit,
                    xid,
                    table_cid,
                    relation_relkind,
                    lowered.of_type_oid,
                    reloptions.heap.clone(),
                    catalog_effects,
                    temp_effects,
                )?;
                self.apply_created_toast_reloptions_in_transaction(
                    client_id,
                    xid,
                    table_cid.saturating_add(1),
                    created.toast.as_ref(),
                    reloptions.toast.as_ref(),
                    catalog_effects,
                )?;
                if !lowered.parent_oids.is_empty() {
                    let inherit_ctx = CatalogWriteContext {
                        pool: self.pool.clone(),
                        txns: self.txns.clone(),
                        xid,
                        cid: table_cid.saturating_add(1),
                        client_id,
                        waiter: None,
                        interrupts,
                    };
                    let inherit_effect = self
                        .catalog
                        .write()
                        .create_relation_inheritance_mvcc(
                            created.entry.relation_oid,
                            &lowered.parent_oids,
                            &inherit_ctx,
                        )
                        .map_err(map_catalog_error)?;
                    self.apply_catalog_mutation_effect_immediate(&inherit_effect)?;
                    catalog_effects.push(inherit_effect);
                }
                let relpartbound = lowered
                    .partition_bound
                    .as_ref()
                    .map(serialize_partition_bound)
                    .transpose()
                    .map_err(ExecError::Parse)?;
                let partitioned_table = lowered
                    .partition_spec
                    .as_ref()
                    .map(|spec| pg_partitioned_table_row(created.entry.relation_oid, spec, 0));
                let relation =
                    if lowered.partition_parent_oid.is_some() || lowered.partition_spec.is_some() {
                        let relation = self.replace_relation_partition_metadata_in_transaction(
                            client_id,
                            created.entry.relation_oid,
                            lowered.partition_parent_oid.is_some(),
                            relpartbound,
                            partitioned_table,
                            xid,
                            table_cid.saturating_add(2),
                            configured_search_path,
                            catalog_effects,
                        )?;
                        if lowered
                            .partition_bound
                            .as_ref()
                            .is_some_and(PartitionBoundSpec::is_default)
                            && let Some(parent_oid) = lowered.partition_parent_oid
                        {
                            self.update_partitioned_table_default_partition_in_transaction(
                                client_id,
                                parent_oid,
                                created.entry.relation_oid,
                                xid,
                                table_cid.saturating_add(3),
                                configured_search_path,
                                catalog_effects,
                            )?;
                        }
                        relation
                    } else {
                        crate::backend::parser::BoundRelation {
                            rel: created.entry.rel,
                            relation_oid: created.entry.relation_oid,
                            toast: created.toast.as_ref().map(|toast| {
                                crate::include::nodes::primnodes::ToastRelationRef {
                                    rel: toast.toast_entry.rel,
                                    relation_oid: toast.toast_entry.relation_oid,
                                }
                            }),
                            namespace_oid: created.entry.namespace_oid,
                            owner_oid: created.entry.owner_oid,
                            of_type_oid: created.entry.of_type_oid,
                            relpersistence: created.entry.relpersistence,
                            relkind: created.entry.relkind,
                            relispopulated: created.entry.relispopulated,
                            relispartition: created.entry.relispartition,
                            relpartbound: created.entry.relpartbound.clone(),
                            desc: created.entry.desc.clone(),
                            partitioned_table: created.entry.partitioned_table.clone(),
                            partition_spec: None,
                        }
                    };
                let mut constraint_cid_base = table_cid.saturating_add(1);
                if !lowered.parent_oids.is_empty() {
                    constraint_cid_base = constraint_cid_base.max(table_cid.saturating_add(2));
                }
                if lowered.partition_spec.is_some() || lowered.partition_parent_oid.is_some() {
                    constraint_cid_base = constraint_cid_base.max(table_cid.saturating_add(3));
                }
                if lowered
                    .partition_bound
                    .as_ref()
                    .is_some_and(PartitionBoundSpec::is_default)
                {
                    constraint_cid_base = constraint_cid_base.max(table_cid.saturating_add(4));
                }
                let next_cid = self.install_create_table_constraints_in_transaction(
                    client_id,
                    xid,
                    constraint_cid_base,
                    &table_name,
                    &relation,
                    &lowered,
                    configured_search_path,
                    catalog_effects,
                )?;
                if let Some(parent_oid) = lowered.partition_parent_oid {
                    let next_cid = self
                        .reconcile_partitioned_parent_keys_for_attached_child_in_transaction(
                            client_id,
                            xid,
                            next_cid,
                            parent_oid,
                            relation.relation_oid,
                            configured_search_path,
                            catalog_effects,
                        )?;
                    let next_cid = self
                        .reconcile_partitioned_parent_indexes_for_attached_child_in_transaction(
                            client_id,
                            xid,
                            next_cid,
                            parent_oid,
                            relation.relation_oid,
                            configured_search_path,
                            catalog_effects,
                        )?;
                    let next_cid = self
                        .reconcile_partitioned_parent_foreign_keys_for_attached_child_in_transaction(
                            client_id,
                            xid,
                            next_cid,
                            parent_oid,
                            relation.relation_oid,
                            configured_search_path,
                            catalog_effects,
                        )?;
                    self.clone_parent_row_triggers_to_partition_in_transaction(
                        client_id,
                        xid,
                        next_cid,
                        parent_oid,
                        relation.relation_oid,
                        configured_search_path,
                        catalog_effects,
                    )?;
                }
                Ok(StatementResult::AffectedRows(0))
            }
        }
    }

    fn ensure_create_table_type_usage(
        &self,
        client_id: ClientId,
        txn_ctx: Option<(TransactionId, CommandId)>,
        desc: &RelationDesc,
    ) -> Result<(), ExecError> {
        let used_range_types = {
            let range_types = self.range_types.read();
            desc.columns
                .iter()
                .filter_map(|column| {
                    let ty = column.sql_type.element_type();
                    range_types
                        .values()
                        .find(|entry| {
                            ty.type_oid == entry.oid || ty.type_oid == entry.multirange_oid
                        })
                        .map(|entry| {
                            let type_name = if ty.type_oid == entry.multirange_oid {
                                entry.multirange_name.clone()
                            } else {
                                entry.name.clone()
                            };
                            (entry.clone(), type_name)
                        })
                })
                .collect::<Vec<_>>()
        };
        if used_range_types.is_empty() {
            return Ok(());
        }

        let effective_grantees = self.effective_type_acl_grantees(client_id, txn_ctx)?;
        for (entry, type_name) in used_range_types {
            let owner_name = self
                .syscache_role_by_oid(client_id, txn_ctx, entry.owner_oid)?
                .map(|role| role.rolname)
                .unwrap_or_else(|| entry.owner_oid.to_string());
            let acl = entry
                .typacl
                .clone()
                .unwrap_or_else(|| type_owner_default_acl(&owner_name));
            if !effective_grantees.is_superuser
                && !acl_grants_privilege(&acl, &effective_grantees.names, 'U')
            {
                return Err(ExecError::DetailedError {
                    message: format!("permission denied for type {type_name}"),
                    detail: None,
                    hint: None,
                    sqlstate: "42501",
                });
            }
        }
        Ok(())
    }

    fn effective_type_acl_grantees(
        &self,
        client_id: ClientId,
        txn_ctx: Option<(TransactionId, CommandId)>,
    ) -> Result<EffectiveTypeAclGrantees, ExecError> {
        let auth = self.auth_state(client_id);
        let mut names = std::collections::BTreeSet::from([String::new()]);
        let mut pending = std::collections::VecDeque::from([auth.current_user_oid()]);
        let mut visited = std::collections::BTreeSet::new();

        while let Some(member_oid) = pending.pop_front() {
            if !visited.insert(member_oid) {
                continue;
            }
            if let Some(role) = self.syscache_role_by_oid(client_id, txn_ctx, member_oid)? {
                if member_oid == auth.current_user_oid() && role.rolsuper {
                    return Ok(EffectiveTypeAclGrantees {
                        names,
                        is_superuser: true,
                    });
                }
                names.insert(role.rolname);
            }
            for membership in
                self.syscache_auth_memberships_for_member(client_id, txn_ctx, member_oid)?
            {
                if membership.inherit_option {
                    pending.push_back(membership.roleid);
                }
            }
        }

        Ok(EffectiveTypeAclGrantees {
            names,
            is_superuser: false,
        })
    }

    fn syscache_role_by_oid(
        &self,
        client_id: ClientId,
        txn_ctx: Option<(TransactionId, CommandId)>,
        role_oid: u32,
    ) -> Result<Option<PgAuthIdRow>, ExecError> {
        Ok(SearchSysCache1(
            self,
            client_id,
            txn_ctx,
            SysCacheId::AUTHOID,
            Value::Int64(i64::from(role_oid)),
        )
        .map_err(map_catalog_error)?
        .into_iter()
        .find_map(|tuple| match tuple {
            SysCacheTuple::AuthId(row) => Some(row),
            _ => None,
        }))
    }

    fn syscache_auth_memberships_for_member(
        &self,
        client_id: ClientId,
        txn_ctx: Option<(TransactionId, CommandId)>,
        member_oid: u32,
    ) -> Result<Vec<PgAuthMembersRow>, ExecError> {
        Ok(SearchSysCacheList1(
            self,
            client_id,
            txn_ctx,
            SysCacheId::AUTHMEMMEMROLE,
            Value::Int64(i64::from(member_oid)),
        )
        .map_err(map_catalog_error)?
        .into_iter()
        .filter_map(|tuple| match tuple {
            SysCacheTuple::AuthMembers(row) => Some(row),
            _ => None,
        })
        .collect())
    }

    pub(crate) fn execute_create_view_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateViewStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let (view_name, namespace_oid) = self.normalize_create_view_stmt_with_search_path(
            client_id,
            Some((xid, cid)),
            create_stmt,
            configured_search_path,
        )?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let (analyzed_query, _) = crate::backend::parser::analyze_select_query_with_outer(
            &create_stmt.query,
            &catalog,
            &[],
            None,
            None,
            &[],
            &[],
        )?;
        let constraint_oids = analyzed_query.constraint_deps.clone();
        let plan = crate::backend::parser::pg_plan_query(&create_stmt.query, &catalog)?.plan_tree;
        let mut stored_query = analyzed_query.clone();
        apply_create_view_column_names_to_query(&mut stored_query, &create_stmt.column_names);
        let canonical_sql = if select_query_requires_original_view_sql(&create_stmt.query) {
            // :HACK: The analyzed `Query` does not yet retain enough CTE
            // and table-function alias structure to deparse every stored view
            // safely. Keep the original SELECT text for those shapes while the
            // display deparser remains free to render PostgreSQL-style SQL.
            create_stmt
                .query_sql
                .trim()
                .trim_end_matches(';')
                .to_string()
        } else {
            render_view_query_sql(&stored_query, &catalog)
        };
        let canonical_query_sql = append_view_check_option(canonical_sql, create_stmt.check_option);
        let mut desc = crate::backend::executor::RelationDesc {
            columns: plan
                .column_names()
                .into_iter()
                .zip(plan.columns())
                .map(|(name, column)| column_desc(name, column.sql_type, true))
                .collect(),
        };
        apply_create_view_column_names(&mut desc, &create_stmt.column_names)?;
        let reloptions = create_view_reloptions(&create_stmt.options)?;
        let mut referenced_relation_oids = std::collections::BTreeSet::new();
        collect_direct_relation_oids_from_select(
            &create_stmt.query,
            &catalog,
            &mut Vec::new(),
            &mut referenced_relation_oids,
        );
        let references_temporary_relation = referenced_relation_oids.iter().any(|oid| {
            catalog
                .relation_by_oid(*oid)
                .or_else(|| catalog.lookup_relation_by_oid(*oid))
                .is_some_and(|relation| relation.relpersistence == 't')
        });
        let effective_persistence = if create_stmt.persistence == TablePersistence::Permanent
            && references_temporary_relation
        {
            push_notice(format!(
                "view \"{}\" will be a temporary view",
                create_stmt.view_name.to_ascii_lowercase()
            ));
            if create_stmt.schema_name.is_some() {
                return Err(ExecError::Parse(ParseError::TempTableInNonTempSchema(
                    view_name.clone(),
                )));
            }
            TablePersistence::Temporary
        } else {
            create_stmt.persistence
        };
        let temp_lookup_name = view_name
            .strip_prefix("pg_temp.")
            .unwrap_or(&view_name)
            .to_ascii_lowercase();
        let existing_relation = if effective_persistence == TablePersistence::Permanent {
            catalog
                .lookup_any_relation(&view_name)
                .filter(|relation| relation.namespace_oid == namespace_oid)
        } else {
            self.owned_temp_namespace(client_id).and_then(|namespace| {
                namespace.tables.get(&temp_lookup_name).map(|entry| {
                    let entry = &entry.entry;
                    crate::backend::parser::BoundRelation {
                        rel: entry.rel,
                        relation_oid: entry.relation_oid,
                        toast: None,
                        namespace_oid: entry.namespace_oid,
                        owner_oid: entry.owner_oid,
                        of_type_oid: entry.of_type_oid,
                        relpersistence: entry.relpersistence,
                        relkind: entry.relkind,
                        relispopulated: entry.relispopulated,
                        relispartition: entry.relispartition,
                        relpartbound: entry.relpartbound.clone(),
                        desc: entry.desc.clone(),
                        partitioned_table: entry.partitioned_table.clone(),
                        partition_spec: None,
                    }
                })
            })
        };
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts,
        };
        let is_new_relation = existing_relation.is_none();
        let relation_oid = if let Some(existing_relation) = existing_relation {
            if !create_stmt.or_replace {
                return Err(ExecError::Parse(ParseError::TableAlreadyExists(view_name)));
            }
            if existing_relation.relkind != 'v' {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: create_stmt.view_name.clone(),
                    expected: "view",
                }));
            }
            validate_create_or_replace_view_columns(&existing_relation.desc, &desc, &catalog)?;
            let replace_effect = self
                .catalog
                .write()
                .alter_view_relation_desc_mvcc(
                    existing_relation.relation_oid,
                    desc.clone(),
                    reloptions.clone(),
                    &ctx,
                )
                .map_err(map_catalog_error)?;
            if existing_relation.relpersistence == 't' {
                self.replace_temp_entry_desc(client_id, existing_relation.relation_oid, desc)?;
            }
            catalog_effects.push(replace_effect);
            existing_relation.relation_oid
        } else {
            match effective_persistence {
                TablePersistence::Permanent => {
                    let (entry, create_effect) = self
                        .catalog
                        .write()
                        .create_view_relation_mvcc(
                            view_name.clone(),
                            desc,
                            namespace_oid,
                            self.auth_state(client_id).current_user_oid(),
                            reloptions.clone(),
                            &ctx,
                        )
                        .map_err(map_catalog_error)?;
                    catalog_effects.push(create_effect);
                    entry.relation_oid
                }
                TablePersistence::Unlogged => {
                    return Err(ExecError::Parse(ParseError::FeatureNotSupportedMessage(
                        "unlogged views are not supported".into(),
                    )));
                }
                TablePersistence::Temporary => {
                    let created = self.create_temp_relation_with_relkind_in_transaction(
                        client_id,
                        create_stmt.view_name.to_ascii_lowercase(),
                        desc,
                        OnCommitAction::PreserveRows,
                        xid,
                        cid,
                        'v',
                        0,
                        reloptions.clone(),
                        catalog_effects,
                        temp_effects,
                    )?;
                    created.entry.relation_oid
                }
            }
        };

        let rule_drop_ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: cid.saturating_add(1),
            client_id,
            waiter: None,
            interrupts: Arc::clone(&ctx.interrupts),
        };
        if create_stmt.or_replace
            && let Some(rewrite_oid) = catalog
                .rewrite_rows_for_relation(relation_oid)
                .into_iter()
                .find(|row| row.rulename == "_RETURN")
                .map(|row| row.oid)
        {
            let drop_effect = self
                .catalog
                .write()
                .drop_rule_mvcc(rewrite_oid, &rule_drop_ctx)
                .map_err(map_catalog_error)?;
            catalog_effects.push(drop_effect);
        }
        let rule_ctx = CatalogWriteContext {
            cid: cid.saturating_add(2),
            ..rule_drop_ctx
        };
        let mut rule_dependencies = crate::backend::catalog::store::RuleDependencies {
            relation_oids: referenced_relation_oids.into_iter().collect::<Vec<_>>(),
            constraint_oids,
            ..Default::default()
        };
        collect_rule_dependencies_from_query(&analyzed_query, &catalog, &mut rule_dependencies);
        let rule_result = self.catalog.write().create_rule_mvcc_with_dependencies(
            relation_oid,
            "_RETURN",
            '1',
            true,
            String::new(),
            canonical_query_sql,
            rule_dependencies,
            crate::backend::catalog::store::RuleOwnerDependency::Internal,
            &rule_ctx,
        );
        let rule_effect = match rule_result {
            Ok(effect) => effect,
            Err(err) => {
                let exec_err = map_catalog_error(err);
                if is_new_relation && effective_persistence == TablePersistence::Temporary {
                    let _ = self.drop_temp_relation_in_transaction(
                        client_id,
                        &temp_lookup_name,
                        xid,
                        cid.saturating_add(3),
                        catalog_effects,
                        temp_effects,
                    );
                }
                return Err(exec_err);
            }
        };
        catalog_effects.push(rule_effect);
        if is_new_relation {
            // :HACK: CREATE VIEW reserves an intermediate command id between creating
            // the relation row and publishing the _RETURN rule. Session command-end
            // bookkeeping advances by catalog effect count, so pad the effect list to
            // keep the next statement's cid beyond the reserved internal cids.
            catalog_effects.push(CatalogMutationEffect::default());
        }
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_create_table_as_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableAsStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        planner_config: crate::include::nodes::pathnodes::PlannerConfig,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        if create_stmt.object_type
            == crate::include::nodes::parsenodes::TableAsObjectType::MaterializedView
        {
            return self.execute_create_materialized_view_stmt_in_transaction_with_search_path(
                client_id,
                create_stmt,
                xid,
                cid,
                configured_search_path,
                catalog_effects,
            );
        }
        let interrupts = self.interrupt_state(client_id);
        let (table_name, namespace_oid, persistence) = self
            .normalize_create_table_as_stmt_with_search_path(
                client_id,
                Some((xid, cid)),
                create_stmt,
                configured_search_path,
            )?;
        let mut table_cid = cid;
        if persistence == TablePersistence::Temporary {
            self.ensure_temp_namespace(
                client_id,
                xid,
                &mut table_cid,
                catalog_effects,
                temp_effects,
            )?;
        }
        let catalog =
            self.lazy_catalog_lookup(client_id, Some((xid, table_cid)), configured_search_path);
        if catalog
            .lookup_any_relation(&table_name)
            .is_some_and(|relation| relation.namespace_oid == namespace_oid)
        {
            if create_stmt.if_not_exists {
                push_notice(format!(
                    "relation \"{table_name}\" already exists, skipping"
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::Parse(ParseError::DetailedError {
                message: format!("relation \"{table_name}\" already exists"),
                detail: None,
                hint: None,
                sqlstate: "42P07",
            }));
        }
        let select_query = match &create_stmt.query {
            CreateTableAsQuery::Select(query) => query,
            CreateTableAsQuery::Execute(execute) => {
                return Err(ExecError::Parse(ParseError::DetailedError {
                    message: format!("prepared statement \"{}\" does not exist", execute.name),
                    detail: None,
                    hint: None,
                    sqlstate: "26000",
                }));
            }
        };

        let snapshot = self.txns.read().snapshot_for_command(xid, table_cid)?;
        let mut ctx = ExecutorContext {
            pool: Arc::clone(&self.pool),
            data_dir: None,
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            lock_status_provider: Some(Arc::new(self.clone())),
            sequences: Some(self.sequences.clone()),
            large_objects: Some(self.large_objects.clone()),
            stats_import_runtime: None,
            async_notify_runtime: Some(self.async_notify_runtime.clone()),
            advisory_locks: Arc::clone(&self.advisory_locks),
            row_locks: Arc::clone(&self.row_locks),
            checkpoint_stats: self.checkpoint_stats_snapshot(),
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            statement_timestamp_usecs:
                crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
            gucs: std::collections::HashMap::new(),
            interrupts: Arc::clone(&interrupts),
            stats: Arc::clone(&self.stats),
            session_stats: self.session_stats_state(client_id),
            snapshot,
            transaction_state: None,
            client_id,
            current_database_name: self.current_database_name(),
            session_user_oid: self.auth_state(client_id).session_user_oid(),
            current_user_oid: self.auth_state(client_id).current_user_oid(),
            active_role_oid: self.auth_state(client_id).active_role_oid(),
            session_replication_role: self.session_replication_role(client_id),
            statement_lock_scope_id: None,
            transaction_lock_scope_id: None,
            next_command_id: table_cid,
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            random_state: crate::backend::executor::PgPrngState::shared(),
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            subplans: Vec::new(),
            timed: false,
            allow_side_effects: false,
            pending_async_notifications: Vec::new(),
            catalog_effects: Vec::new(),
            temp_effects: Vec::new(),
            database: Some(self.clone()),
            pending_catalog_effects: Vec::new(),
            pending_table_locks: Vec::new(),
            catalog: Some(crate::backend::executor::executor_catalog(catalog.clone())),
            scalar_function_cache: std::collections::HashMap::new(),
            srf_rows_cache: std::collections::HashMap::new(),
            plpgsql_function_cache: self.plpgsql_function_cache(client_id),
            pinned_cte_tables: std::collections::HashMap::new(),
            cte_tables: std::collections::HashMap::new(),
            cte_producers: std::collections::HashMap::new(),
            recursive_worktables: std::collections::HashMap::new(),
            deferred_foreign_keys: None,
            trigger_depth: 0,
        };
        let (columns, column_names, rows) = if create_stmt.skip_data {
            let (columns, column_names) =
                describe_select_query_without_planning(select_query, &catalog)?;
            (columns, column_names, Vec::new())
        } else {
            let query_result = crate::backend::executor::execute_readonly_statement_with_config(
                Statement::Select(select_query.clone()),
                &catalog,
                &mut ctx,
                planner_config,
            );
            let StatementResult::Query {
                columns,
                column_names,
                rows,
            } = query_result?
            else {
                unreachable!("ctas query should return rows");
            };
            (columns, column_names, rows)
        };

        let desc = crate::backend::executor::RelationDesc {
            columns: columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    let name = create_stmt
                        .column_names
                        .get(index)
                        .cloned()
                        .unwrap_or_else(|| column_names[index].clone());
                    column_desc(name, column.sql_type, true)
                })
                .collect(),
        };

        let (relation_oid, rel, toast, toast_index) = match persistence {
            TablePersistence::Permanent | TablePersistence::Unlogged => {
                let relpersistence = if persistence == TablePersistence::Unlogged {
                    'u'
                } else {
                    'p'
                };
                let stmt = CreateTableStatement {
                    schema_name: None,
                    table_name: table_name.clone(),
                    of_type_name: None,
                    persistence,
                    on_commit: create_stmt.on_commit,
                    elements: desc
                        .columns
                        .iter()
                        .map(|column| {
                            crate::backend::parser::CreateTableElement::Column(
                                crate::backend::parser::ColumnDef {
                                    name: column.name.clone(),
                                    ty: crate::backend::parser::RawTypeName::Builtin(
                                        column.sql_type,
                                    ),
                                    collation: None,
                                    default_expr: None,
                                    generated: None,
                                    identity: None,
                                    storage: None,
                                    compression: None,
                                    constraints: vec![],
                                },
                            )
                        })
                        .collect(),
                    options: Vec::new(),
                    inherits: Vec::new(),
                    partition_spec: None,
                    partition_of: None,
                    partition_bound: None,
                    if_not_exists: create_stmt.if_not_exists,
                };
                let mut catalog_guard = self.catalog.write();
                let write_ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: table_cid,
                    client_id,
                    waiter: None,
                    interrupts: Arc::clone(&interrupts),
                };
                let (created, effect) = catalog_guard
                    .create_table_mvcc_with_options(
                        table_name.clone(),
                        create_relation_desc(&stmt, &catalog)?,
                        namespace_oid,
                        self.database_oid,
                        relpersistence,
                        crate::include::catalog::PG_TOAST_NAMESPACE_OID,
                        crate::backend::catalog::toasting::PG_TOAST_NAMESPACE,
                        self.auth_state(client_id).current_user_oid(),
                        None,
                        &write_ctx,
                    )
                    .map_err(map_catalog_error)?;
                drop(catalog_guard);
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                let (toast, toast_index) = toast_bindings_from_create_result(&created);
                (
                    created.entry.relation_oid,
                    created.entry.rel,
                    toast,
                    toast_index,
                )
            }
            TablePersistence::Temporary => {
                let created = self.create_temp_relation_in_transaction(
                    client_id,
                    table_name.clone(),
                    desc.clone(),
                    create_stmt.on_commit,
                    xid,
                    table_cid,
                    catalog_effects,
                    temp_effects,
                )?;
                let (toast, toast_index) = toast_bindings_from_temp_relation(&created);
                (
                    created.entry.relation_oid,
                    created.entry.rel,
                    toast,
                    toast_index,
                )
            }
        };
        if rows.is_empty() {
            return Ok(StatementResult::AffectedRows(0));
        }

        let insert_catalog =
            self.lazy_catalog_lookup(client_id, Some((xid, table_cid)), configured_search_path);
        let snapshot = self.txns.read().snapshot_for_command(xid, table_cid)?;
        let mut insert_ctx = ExecutorContext {
            pool: Arc::clone(&self.pool),
            data_dir: None,
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            lock_status_provider: Some(Arc::new(self.clone())),
            sequences: Some(self.sequences.clone()),
            large_objects: Some(self.large_objects.clone()),
            stats_import_runtime: None,
            async_notify_runtime: Some(self.async_notify_runtime.clone()),
            advisory_locks: Arc::clone(&self.advisory_locks),
            row_locks: Arc::clone(&self.row_locks),
            checkpoint_stats: self.checkpoint_stats_snapshot(),
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            statement_timestamp_usecs:
                crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
            gucs: std::collections::HashMap::new(),
            interrupts,
            stats: Arc::clone(&self.stats),
            session_stats: self.session_stats_state(client_id),
            snapshot,
            transaction_state: None,
            client_id,
            current_database_name: self.current_database_name(),
            session_user_oid: self.auth_state(client_id).session_user_oid(),
            current_user_oid: self.auth_state(client_id).current_user_oid(),
            active_role_oid: self.auth_state(client_id).active_role_oid(),
            session_replication_role: self.session_replication_role(client_id),
            statement_lock_scope_id: None,
            transaction_lock_scope_id: None,
            next_command_id: table_cid,
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            random_state: crate::backend::executor::PgPrngState::shared(),
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            subplans: Vec::new(),
            timed: false,
            allow_side_effects: true,
            pending_async_notifications: Vec::new(),
            catalog_effects: Vec::new(),
            temp_effects: Vec::new(),
            database: Some(self.clone()),
            pending_catalog_effects: Vec::new(),
            pending_table_locks: Vec::new(),
            catalog: Some(crate::backend::executor::executor_catalog(
                insert_catalog.clone(),
            )),
            scalar_function_cache: std::collections::HashMap::new(),
            srf_rows_cache: std::collections::HashMap::new(),
            plpgsql_function_cache: self.plpgsql_function_cache(client_id),
            pinned_cte_tables: std::collections::HashMap::new(),
            cte_tables: std::collections::HashMap::new(),
            cte_producers: std::collections::HashMap::new(),
            recursive_worktables: std::collections::HashMap::new(),
            deferred_foreign_keys: None,
            trigger_depth: 0,
        };
        let inserted = crate::backend::commands::tablecmds::execute_insert_values(
            &table_name,
            relation_oid,
            rel,
            toast,
            toast_index.as_ref(),
            &desc,
            &crate::backend::parser::BoundRelationConstraints::default(),
            &[],
            &[],
            &rows,
            &mut insert_ctx,
            xid,
            cid,
        )?;
        {
            let stats_state = self.session_stats_state(client_id);
            let mut stats = stats_state.write();
            for _ in 0..inserted {
                stats.note_relation_insert_with_persistence(
                    relation_oid,
                    match create_stmt.persistence {
                        TablePersistence::Temporary => 't',
                        TablePersistence::Permanent | TablePersistence::Unlogged => 'p',
                    },
                );
            }
            stats.note_io_extend("client backend", "relation", "bulkwrite", 8192);
        }
        Ok(StatementResult::AffectedRows(inserted))
    }

    pub(crate) fn execute_create_table_as_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableAsStatement,
        xid: Option<TransactionId>,
        cid: u32,
        configured_search_path: Option<&[String]>,
        planner_config: crate::include::nodes::pathnodes::PlannerConfig,
    ) -> Result<StatementResult, ExecError> {
        if let Some(xid) = xid {
            let mut catalog_effects = Vec::new();
            let mut temp_effects = Vec::new();
            return self.execute_create_table_as_stmt_in_transaction_with_search_path(
                client_id,
                create_stmt,
                xid,
                cid,
                configured_search_path,
                planner_config,
                &mut catalog_effects,
                &mut temp_effects,
            );
        }
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut temp_effects = Vec::new();
        let result = self.execute_create_table_as_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            cid,
            configured_search_path,
            planner_config,
            &mut catalog_effects,
            &mut temp_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects, &[]);
        guard.disarm();
        result
    }
}
