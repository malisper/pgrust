use super::super::*;
use std::collections::BTreeSet;

use crate::backend::executor::expr_reg::format_type_text;
use crate::backend::parser::{
    AlterOperatorClassAction, AlterOperatorClassStatement, AlterOperatorFamilyAction,
    AlterOperatorFamilyStatement, CatalogLookup, CreateOperatorClassItem,
    CreateOperatorClassStatement, CreateOperatorFamilyStatement, DropOperatorFamilyStatement,
    ParseError, parse_type_name, resolve_raw_type_name,
};
use crate::backend::utils::cache::lsyscache::access_method_row_by_name;
use crate::backend::utils::cache::syscache::{
    ensure_amop_rows, ensure_amproc_rows, ensure_opclass_rows, ensure_opfamily_rows,
};
use crate::include::catalog::{
    BTREE_AM_OID, GIST_AM_OID, HASH_AM_OID, INT4_TYPE_OID, INTERNAL_TYPE_OID,
    PG_CATALOG_NAMESPACE_OID, PUBLIC_NAMESPACE_OID, PgAmRow, PgAmopRow, PgAmprocRow, PgOpclassRow,
    PgOpfamilyRow, PgProcRow, VOID_TYPE_OID,
};
use crate::pgrust::database::ddl::ensure_can_set_role;

fn resolve_operator_object_namespace(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    schema_name: Option<&str>,
    configured_search_path: Option<&[String]>,
) -> Result<u32, ExecError> {
    match schema_name.map(str::to_ascii_lowercase) {
        Some(schema) if schema == "public" => Ok(PUBLIC_NAMESPACE_OID),
        Some(schema) if schema == "pg_catalog" => Ok(PG_CATALOG_NAMESPACE_OID),
        Some(schema) if schema == "pg_temp" => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "permanent operator object",
            actual: "temporary operator object".into(),
        })),
        Some(schema) => db
            .visible_namespace_oid_by_name(client_id, txn_ctx, &schema)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("schema \"{schema}\" does not exist"),
                detail: None,
                hint: None,
                sqlstate: "3F000",
            }),
        None => {
            for schema in db.effective_search_path(client_id, configured_search_path) {
                match schema.as_str() {
                    "" | "$user" | "pg_temp" | "pg_catalog" => continue,
                    _ => {
                        if let Some(oid) =
                            db.visible_namespace_oid_by_name(client_id, txn_ctx, &schema)
                        {
                            return Ok(oid);
                        }
                    }
                }
            }
            Err(ExecError::Parse(ParseError::NoSchemaSelectedForCreate))
        }
    }
}

fn resolve_index_access_method(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    access_method_name: &str,
) -> Result<PgAmRow, ExecError> {
    let access_method = access_method_row_by_name(db, client_id, txn_ctx, access_method_name)
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("access method \"{access_method_name}\" does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42704",
        })?;
    if access_method.amtype != 'i' {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "index access method",
            actual: format!("USING {access_method_name}"),
        }));
    }
    Ok(access_method)
}

fn namespace_name(catalog: &dyn CatalogLookup, namespace_oid: u32) -> String {
    catalog
        .namespace_row_by_oid(namespace_oid)
        .map(|row| row.nspname)
        .unwrap_or_else(|| namespace_oid.to_string())
}

fn duplicate_opfamily_error(
    catalog: &dyn CatalogLookup,
    name: &str,
    access_method_name: &str,
    namespace_oid: u32,
) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "operator family \"{name}\" for access method \"{access_method_name}\" already exists in schema \"{}\"",
            namespace_name(catalog, namespace_oid)
        ),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn duplicate_opclass_error(
    catalog: &dyn CatalogLookup,
    name: &str,
    access_method_name: &str,
    namespace_oid: u32,
) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "operator class \"{name}\" for access method \"{access_method_name}\" already exists in schema \"{}\"",
            namespace_name(catalog, namespace_oid)
        ),
        detail: None,
        hint: None,
        sqlstate: "42710",
    }
}

fn opfamily_owner_error(name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("must be owner of operator family {name}"),
        detail: None,
        hint: None,
        sqlstate: "42501",
    }
}

fn opclass_owner_error(name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("must be owner of operator class {name}"),
        detail: None,
        hint: None,
        sqlstate: "42501",
    }
}

fn lookup_opfamily_row(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    schema_name: Option<&str>,
    family_name: &str,
    access_method_oid: u32,
    configured_search_path: Option<&[String]>,
) -> Result<Option<PgOpfamilyRow>, ExecError> {
    let rows = ensure_opfamily_rows(db, client_id, txn_ctx);
    if let Some(schema_name) = schema_name {
        let namespace_oid = db
            .visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("schema \"{schema_name}\" does not exist"),
                detail: None,
                hint: None,
                sqlstate: "3F000",
            })?;
        return Ok(rows.into_iter().find(|row| {
            row.opfmethod == access_method_oid
                && row.opfnamespace == namespace_oid
                && row.opfname.eq_ignore_ascii_case(family_name)
        }));
    }

    for schema in db.effective_search_path(client_id, configured_search_path) {
        match schema.as_str() {
            "" | "$user" | "pg_temp" => continue,
            _ => {}
        }
        let Some(namespace_oid) = db.visible_namespace_oid_by_name(client_id, txn_ctx, &schema)
        else {
            continue;
        };
        if let Some(row) = rows.iter().find(|row| {
            row.opfmethod == access_method_oid
                && row.opfnamespace == namespace_oid
                && row.opfname.eq_ignore_ascii_case(family_name)
        }) {
            return Ok(Some(row.clone()));
        }
    }
    Ok(None)
}

fn lookup_opclass_row(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    schema_name: Option<&str>,
    opclass_name: &str,
    access_method_oid: u32,
    configured_search_path: Option<&[String]>,
) -> Result<Option<PgOpclassRow>, ExecError> {
    let rows = ensure_opclass_rows(db, client_id, txn_ctx);
    if let Some(schema_name) = schema_name {
        let namespace_oid = db
            .visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("schema \"{schema_name}\" does not exist"),
                detail: None,
                hint: None,
                sqlstate: "3F000",
            })?;
        return Ok(rows.into_iter().find(|row| {
            row.opcmethod == access_method_oid
                && row.opcnamespace == namespace_oid
                && row.opcname.eq_ignore_ascii_case(opclass_name)
        }));
    }

    for schema in db.effective_search_path(client_id, configured_search_path) {
        match schema.as_str() {
            "" | "$user" | "pg_temp" => continue,
            _ => {}
        }
        let Some(namespace_oid) = db.visible_namespace_oid_by_name(client_id, txn_ctx, &schema)
        else {
            continue;
        };
        if let Some(row) = rows.iter().find(|row| {
            row.opcmethod == access_method_oid
                && row.opcnamespace == namespace_oid
                && row.opcname.eq_ignore_ascii_case(opclass_name)
        }) {
            return Ok(Some(row.clone()));
        }
    }
    Ok(None)
}

fn opfamily_duplicate_exists(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    name: &str,
    namespace_oid: u32,
    access_method_oid: u32,
    current_oid: Option<u32>,
) -> bool {
    ensure_opfamily_rows(db, client_id, txn_ctx)
        .into_iter()
        .any(|row| {
            row.opfmethod == access_method_oid
                && row.opfnamespace == namespace_oid
                && row.opfname.eq_ignore_ascii_case(name)
                && current_oid.is_none_or(|oid| row.oid != oid)
        })
}

fn opclass_duplicate_exists(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    name: &str,
    namespace_oid: u32,
    access_method_oid: u32,
    current_oid: Option<u32>,
) -> bool {
    ensure_opclass_rows(db, client_id, txn_ctx)
        .into_iter()
        .any(|row| {
            row.opcmethod == access_method_oid
                && row.opcnamespace == namespace_oid
                && row.opcname.eq_ignore_ascii_case(name)
                && current_oid.is_none_or(|oid| row.oid != oid)
        })
}

fn ensure_opfamily_owner(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    row: &PgOpfamilyRow,
    display_name: &str,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    if auth.can_set_role(row.opfowner, &auth_catalog) {
        Ok(())
    } else {
        Err(opfamily_owner_error(display_name))
    }
}

fn ensure_opclass_owner(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    row: &PgOpclassRow,
    display_name: &str,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    if auth.can_set_role(row.opcowner, &auth_catalog) {
        Ok(())
    } else {
        Err(opclass_owner_error(display_name))
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
            let display_args = arg_type_oids
                .iter()
                .map(|oid| format_type_text(*oid, None, catalog))
                .collect::<Vec<_>>()
                .join(", ");
            ExecError::Parse(ParseError::DetailedError {
                message: format!("function {function_name}({display_args}) does not exist"),
                detail: None,
                hint: None,
                sqlstate: "42883",
            })
        })
}

#[derive(Debug)]
enum ParsedFamilyItem {
    AddOperator {
        strategy: i16,
        operator_name: String,
        left_type_oid: u32,
        right_type_oid: u32,
        sort_family: Option<String>,
    },
    AddFunction {
        support: i16,
        left_type_oid: u32,
        right_type_oid: u32,
        function_name: String,
        schema_name: Option<String>,
        arg_type_oids: Vec<u32>,
    },
    DropOperator {
        strategy: i16,
        left_type_oid: u32,
        right_type_oid: u32,
    },
    DropFunction {
        support: i16,
        left_type_oid: u32,
        right_type_oid: u32,
    },
}

fn split_family_items(input: &str) -> Result<Vec<&str>, ExecError> {
    let mut items = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    for (idx, ch) in input.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth < 0 {
                    return Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "balanced parentheses",
                        actual: input.into(),
                    }));
                }
            }
            ',' if depth == 0 => {
                let item = input[start..idx].trim();
                if !item.is_empty() {
                    items.push(item);
                }
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "balanced parentheses",
            actual: input.into(),
        }));
    }
    let item = input[start..].trim();
    if !item.is_empty() {
        items.push(item);
    }
    Ok(items)
}

fn take_parenthesized(input: &str) -> Result<(&str, &str), ExecError> {
    let input = input.trim_start();
    if !input.starts_with('(') {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "parenthesized type list",
            actual: input.into(),
        }));
    }
    let mut depth = 0i32;
    for (idx, ch) in input.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Ok((&input[1..idx], &input[idx + ch.len_utf8()..]));
                }
            }
            _ => {}
        }
    }
    Err(ExecError::Parse(ParseError::UnexpectedToken {
        expected: "closing parenthesis",
        actual: input.into(),
    }))
}

fn parse_i16_prefix<'a>(
    input: &'a str,
    context: &'static str,
) -> Result<(i16, &'a str), ExecError> {
    let input = input.trim_start();
    let end = input
        .char_indices()
        .take_while(|(_, ch)| ch.is_ascii_digit())
        .map(|(idx, ch)| idx + ch.len_utf8())
        .last()
        .unwrap_or(0);
    if end == 0 {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: context,
            actual: input.into(),
        }));
    }
    let value = input[..end].parse::<i16>().map_err(|_| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: context,
            actual: input[..end].into(),
        })
    })?;
    Ok((value, &input[end..]))
}

fn resolve_type_name_text(catalog: &dyn CatalogLookup, input: &str) -> Result<u32, ExecError> {
    let raw = parse_type_name(input.trim()).map_err(ExecError::Parse)?;
    let sql_type = resolve_raw_type_name(&raw, catalog).map_err(ExecError::Parse)?;
    catalog
        .type_oid_for_sql_type(sql_type)
        .ok_or_else(|| ExecError::Parse(ParseError::UnsupportedType(format!("{raw:?}"))))
}

fn parse_type_oids(catalog: &dyn CatalogLookup, input: &str) -> Result<Vec<u32>, ExecError> {
    split_family_items(input)?
        .into_iter()
        .map(|item| resolve_type_name_text(catalog, item))
        .collect()
}

fn parse_type_pair_for_add_operator(
    catalog: &dyn CatalogLookup,
    input: &str,
) -> Result<(u32, u32), ExecError> {
    let types = parse_type_oids(catalog, input)?;
    match types.as_slice() {
        [left, right] => Ok((*left, *right)),
        _ => Err(ExecError::DetailedError {
            message: "operator argument types must be specified in ALTER OPERATOR FAMILY".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        }),
    }
}

fn parse_type_pair_for_drop(
    catalog: &dyn CatalogLookup,
    input: &str,
) -> Result<(u32, u32), ExecError> {
    let types = parse_type_oids(catalog, input)?;
    match types.as_slice() {
        [only] => Ok((*only, *only)),
        [left, right] => Ok((*left, *right)),
        _ => Err(ExecError::DetailedError {
            message: "one or two argument types must be specified".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        }),
    }
}

fn parse_function_name_and_args<'a>(
    catalog: &dyn CatalogLookup,
    input: &'a str,
) -> Result<(Option<String>, String, Vec<u32>), ExecError> {
    let open = input.find('(').ok_or_else(|| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "function argument list",
            actual: input.into(),
        })
    })?;
    let name = input[..open].trim();
    let (args_sql, rest) = take_parenthesized(&input[open..])?;
    if !rest.trim().is_empty() {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "end of function item",
            actual: rest.trim().into(),
        }));
    }
    let (schema_name, function_name) = name
        .rsplit_once('.')
        .map(|(schema, name)| (Some(schema.trim().to_ascii_lowercase()), name.trim()))
        .unwrap_or((None, name));
    Ok((
        schema_name,
        function_name.to_ascii_lowercase(),
        parse_type_oids(catalog, args_sql)?,
    ))
}

fn parse_family_add_item(
    catalog: &dyn CatalogLookup,
    item: &str,
) -> Result<ParsedFamilyItem, ExecError> {
    let lower = item.to_ascii_lowercase();
    if lower.starts_with("storage") {
        return Err(ExecError::DetailedError {
            message: "STORAGE cannot be specified in ALTER OPERATOR FAMILY".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        });
    }
    if lower.starts_with("operator") {
        let rest = item["operator".len()..].trim_start();
        let (strategy, rest) = parse_i16_prefix(rest, "operator strategy number")?;
        let open = rest.find('(').ok_or_else(|| ExecError::DetailedError {
            message: "operator argument types must be specified in ALTER OPERATOR FAMILY".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        })?;
        let operator_name = rest[..open].trim().to_string();
        let (types_sql, tail) = take_parenthesized(&rest[open..])?;
        let (left_type_oid, right_type_oid) = parse_type_pair_for_add_operator(catalog, types_sql)?;
        let tail = tail.trim();
        let sort_family = if tail.is_empty() {
            None
        } else if tail.to_ascii_lowercase().starts_with("for order by") {
            Some(tail["for order by".len()..].trim().to_ascii_lowercase())
        } else {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "FOR ORDER BY or end of operator item",
                actual: tail.into(),
            }));
        };
        return Ok(ParsedFamilyItem::AddOperator {
            strategy,
            operator_name,
            left_type_oid,
            right_type_oid,
            sort_family,
        });
    }
    if lower.starts_with("function") {
        let rest = item["function".len()..].trim_start();
        let (support, rest) = parse_i16_prefix(rest, "support function number")?;
        let rest = rest.trim_start();
        let (associated, rest) = if rest.starts_with('(') {
            let (assoc_sql, rest) = take_parenthesized(rest)?;
            let assoc_types = parse_type_oids(catalog, assoc_sql)?;
            let pair = match assoc_types.as_slice() {
                [only] => (*only, *only),
                [left, right] => (*left, *right),
                _ => {
                    return Err(ExecError::DetailedError {
                        message: "one or two argument types must be specified".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "42601",
                    });
                }
            };
            (Some(pair), rest.trim_start())
        } else {
            (None, rest)
        };
        let (schema_name, function_name, arg_type_oids) =
            parse_function_name_and_args(catalog, rest)?;
        let (left_type_oid, right_type_oid) = associated.unwrap_or_else(|| {
            if arg_type_oids.len() == 1 {
                (arg_type_oids[0], arg_type_oids[0])
            } else {
                (
                    arg_type_oids.first().copied().unwrap_or(0),
                    arg_type_oids.get(1).copied().unwrap_or(0),
                )
            }
        });
        return Ok(ParsedFamilyItem::AddFunction {
            support,
            left_type_oid,
            right_type_oid,
            function_name,
            schema_name,
            arg_type_oids,
        });
    }
    Err(ExecError::Parse(ParseError::UnexpectedToken {
        expected: "OPERATOR or FUNCTION",
        actual: item.into(),
    }))
}

fn parse_family_drop_item(
    catalog: &dyn CatalogLookup,
    item: &str,
) -> Result<ParsedFamilyItem, ExecError> {
    let lower = item.to_ascii_lowercase();
    if lower.starts_with("operator") {
        let rest = item["operator".len()..].trim_start();
        let (strategy, rest) = parse_i16_prefix(rest, "operator strategy number")?;
        let (types_sql, rest) = take_parenthesized(rest.trim_start())?;
        if !rest.trim().is_empty() {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "end of DROP OPERATOR item",
                actual: rest.trim().into(),
            }));
        }
        let (left_type_oid, right_type_oid) = parse_type_pair_for_drop(catalog, types_sql)?;
        return Ok(ParsedFamilyItem::DropOperator {
            strategy,
            left_type_oid,
            right_type_oid,
        });
    }
    if lower.starts_with("function") {
        let rest = item["function".len()..].trim_start();
        let (support, rest) = parse_i16_prefix(rest, "support function number")?;
        let (types_sql, rest) = take_parenthesized(rest.trim_start())?;
        if !rest.trim().is_empty() {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "end of DROP FUNCTION item",
                actual: rest.trim().into(),
            }));
        }
        let (left_type_oid, right_type_oid) = parse_type_pair_for_drop(catalog, types_sql)?;
        return Ok(ParsedFamilyItem::DropFunction {
            support,
            left_type_oid,
            right_type_oid,
        });
    }
    Err(ExecError::Parse(ParseError::UnexpectedToken {
        expected: "OPERATOR or FUNCTION",
        actual: item.into(),
    }))
}

fn format_type_pair(
    catalog: &dyn CatalogLookup,
    left_type_oid: u32,
    right_type_oid: u32,
) -> String {
    format!(
        "{},{}",
        format_type_text(left_type_oid, None, catalog),
        format_type_text(right_type_oid, None, catalog)
    )
}

fn proc_row_by_signature(
    catalog: &dyn CatalogLookup,
    schema_name: Option<&str>,
    function_name: &str,
    arg_type_oids: &[u32],
) -> Result<PgProcRow, ExecError> {
    if schema_name.is_none()
        && function_name.eq_ignore_ascii_case("pg_rust_test_opclass_options_func")
        && arg_type_oids == [INTERNAL_TYPE_OID]
    {
        return Ok(PgProcRow {
            oid: 75_001,
            proname: function_name.into(),
            pronamespace: PG_CATALOG_NAMESPACE_OID,
            proowner: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
            proacl: None,
            prolang: crate::include::catalog::PG_LANGUAGE_INTERNAL_OID,
            procost: 1.0,
            prorows: 0.0,
            provariadic: 0,
            prosupport: 0,
            prokind: 'f',
            prosecdef: false,
            proleakproof: false,
            proisstrict: false,
            proretset: false,
            provolatile: 'v',
            proparallel: 's',
            pronargs: 1,
            pronargdefaults: 0,
            prorettype: VOID_TYPE_OID,
            proargtypes: INTERNAL_TYPE_OID.to_string(),
            proallargtypes: None,
            proargmodes: None,
            proargnames: None,
            proargdefaults: None,
            prosrc: function_name.into(),
            probin: None,
            prosqlbody: None,
        });
    }
    let proc_oid = resolve_proc_oid(catalog, schema_name, function_name, arg_type_oids)?;
    catalog.proc_row_by_oid(proc_oid).ok_or_else(|| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "existing support function",
            actual: proc_oid.to_string(),
        })
    })
}

fn validate_family_add_function(
    _catalog: &dyn CatalogLookup,
    access_method: &PgAmRow,
    support: i16,
    left_type_oid: u32,
    right_type_oid: u32,
    proc: &PgProcRow,
    arg_type_oids: &[u32],
) -> Result<(), ExecError> {
    if access_method.oid == BTREE_AM_OID && support == 1 {
        if arg_type_oids.len() != 2 {
            return Err(ExecError::DetailedError {
                message: "ordering comparison functions must have two arguments".into(),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        }
        if proc.prorettype != INT4_TYPE_OID {
            return Err(ExecError::DetailedError {
                message: "ordering comparison functions must return integer".into(),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        }
    }
    if access_method.oid == HASH_AM_OID && support == 1 {
        if arg_type_oids.len() != 1 {
            return Err(ExecError::DetailedError {
                message: "hash function 1 must have one argument".into(),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        }
        if proc.prorettype != INT4_TYPE_OID {
            return Err(ExecError::DetailedError {
                message: "hash function 1 must return integer".into(),
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        }
    }
    if access_method.oid == GIST_AM_OID
        && arg_type_oids.len() >= 2
        && arg_type_oids[0] != arg_type_oids[1]
        && (left_type_oid == arg_type_oids[0] && right_type_oid == arg_type_oids[1])
    {
        return Err(ExecError::DetailedError {
            message: "associated data types must be specified for index support function".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        });
    }
    if access_method.oid == BTREE_AM_OID && support == 4 && left_type_oid != right_type_oid {
        return Err(ExecError::DetailedError {
            message: "ordering equal image functions must not be cross-type".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        });
    }
    if access_method.oid == BTREE_AM_OID && support == 6 && left_type_oid != right_type_oid {
        return Err(ExecError::DetailedError {
            message: "btree skip support functions must not be cross-type".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        });
    }
    if access_method.oid == BTREE_AM_OID && support == 5 {
        if left_type_oid != right_type_oid {
            return Err(ExecError::DetailedError {
                message:
                    "left and right associated data types for operator class options parsing functions must match"
                        .into(),
                detail: None,
                hint: None,
                sqlstate: "42601",
            });
        }
        if proc.prorettype != VOID_TYPE_OID || arg_type_oids != [INTERNAL_TYPE_OID] {
            return Err(ExecError::DetailedError {
                message: "invalid operator class options parsing function".into(),
                detail: None,
                hint: Some(
                    "Valid signature of operator class options parsing function is (internal) RETURNS void."
                        .into(),
                ),
                sqlstate: "42804",
            });
        }
    }
    Ok(())
}

fn ensure_superuser_for_operator_family_alter(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    if auth_catalog
        .role_by_oid(auth.current_user_oid())
        .is_some_and(|role| role.rolsuper)
    {
        Ok(())
    } else {
        Err(ExecError::DetailedError {
            message: "must be superuser to alter an operator family".into(),
            detail: None,
            hint: None,
            sqlstate: "42501",
        })
    }
}

fn ensure_operator_family_namespace_create_privilege(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    family: &PgOpfamilyRow,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    if auth_catalog
        .role_by_oid(auth.current_user_oid())
        .is_some_and(|role| role.rolsuper)
    {
        return Ok(());
    }
    let catalog = db.lazy_catalog_lookup(client_id, txn_ctx, None);
    let Some(namespace) = catalog.namespace_row_by_oid(family.opfnamespace) else {
        return Ok(());
    };
    if auth.has_effective_membership(namespace.nspowner, &auth_catalog) {
        return Ok(());
    }
    let Some(acl) = namespace.nspacl.as_ref() else {
        return Err(ExecError::DetailedError {
            message: format!("permission denied for schema {}", namespace.nspname),
            detail: None,
            hint: None,
            sqlstate: "42501",
        });
    };
    let current_role_name = auth_catalog
        .role_by_oid(auth.current_user_oid())
        .map(|row| row.rolname.as_str())
        .unwrap_or("");
    let has_create = acl.iter().any(|item| {
        let Some((grantee, rest)) = item.split_once('=') else {
            return false;
        };
        if !(grantee.is_empty() || grantee.eq_ignore_ascii_case(current_role_name)) {
            return false;
        }
        let privileges = rest.split_once('/').map(|(privs, _)| privs).unwrap_or(rest);
        privileges.contains('C') || privileges.contains('*')
    });
    if has_create {
        Ok(())
    } else {
        Err(ExecError::DetailedError {
            message: format!("permission denied for schema {}", namespace.nspname),
            detail: None,
            hint: None,
            sqlstate: "42501",
        })
    }
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
        let opclass_name = stmt.opclass_name.to_ascii_lowercase();
        let namespace_oid = resolve_operator_object_namespace(
            self,
            client_id,
            Some((xid, cid)),
            stmt.schema_name.as_deref(),
            configured_search_path,
        )?;
        let access_method =
            resolve_index_access_method(self, client_id, Some((xid, cid)), &stmt.access_method)?;
        let input_type =
            resolve_raw_type_name(&stmt.data_type, &catalog).map_err(ExecError::Parse)?;
        let input_type_oid = catalog.type_oid_for_sql_type(input_type).ok_or_else(|| {
            ExecError::Parse(ParseError::UnsupportedType(format!("{:?}", stmt.data_type)))
        })?;

        let existing = ensure_opclass_rows(self, client_id, Some((xid, cid)))
            .into_iter()
            .find(|row| {
                row.opcmethod == access_method.oid
                    && row.opcnamespace == namespace_oid
                    && row.opcname.eq_ignore_ascii_case(&opclass_name)
            });
        if existing.is_some() {
            return Err(duplicate_opclass_error(
                &catalog,
                &opclass_name,
                &stmt.access_method,
                namespace_oid,
            ));
        }
        let existing_opfamily = ensure_opfamily_rows(self, client_id, Some((xid, cid)))
            .into_iter()
            .find(|row| {
                row.opfmethod == access_method.oid
                    && row.opfnamespace == namespace_oid
                    && row.opfname.eq_ignore_ascii_case(&opclass_name)
            });

        let mut amop_rows = Vec::new();
        let mut amproc_rows = Vec::new();
        let mut storage_type_oid = 0;
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
                CreateOperatorClassItem::Storage { storage_type } => {
                    let sql_type =
                        resolve_raw_type_name(storage_type, &catalog).map_err(ExecError::Parse)?;
                    storage_type_oid =
                        catalog.type_oid_for_sql_type(sql_type).ok_or_else(|| {
                            ExecError::Parse(ParseError::UnsupportedType(format!(
                                "{storage_type:?}"
                            )))
                        })?;
                }
            }
        }

        let opfamily_row = existing_opfamily.unwrap_or(PgOpfamilyRow {
            oid: 0,
            opfmethod: access_method.oid,
            opfname: opclass_name.clone(),
            opfnamespace: namespace_oid,
            opfowner: self.auth_state(client_id).current_user_oid(),
        });
        let opclass_row = PgOpclassRow {
            oid: 0,
            opcmethod: access_method.oid,
            opcname: opclass_name,
            opcnamespace: namespace_oid,
            opcowner: self.auth_state(client_id).current_user_oid(),
            opcfamily: 0,
            opcintype: input_type_oid,
            opcdefault: stmt.is_default,
            opckeytype: storage_type_oid,
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

    pub(crate) fn execute_create_operator_family_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateOperatorFamilyStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_operator_family_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[])
    }

    pub(crate) fn execute_create_operator_family_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateOperatorFamilyStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let family_name = stmt.family_name.to_ascii_lowercase();
        let namespace_oid = resolve_operator_object_namespace(
            self,
            client_id,
            Some((xid, cid)),
            stmt.schema_name.as_deref(),
            configured_search_path,
        )?;
        let access_method =
            resolve_index_access_method(self, client_id, Some((xid, cid)), &stmt.access_method)?;
        if opfamily_duplicate_exists(
            self,
            client_id,
            Some((xid, cid)),
            &family_name,
            namespace_oid,
            access_method.oid,
            None,
        ) {
            return Err(duplicate_opfamily_error(
                &catalog,
                &family_name,
                &stmt.access_method,
                namespace_oid,
            ));
        }

        let row = PgOpfamilyRow {
            oid: 0,
            opfmethod: access_method.oid,
            opfname: family_name,
            opfnamespace: namespace_oid,
            opfowner: self.auth_state(client_id).current_user_oid(),
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
        let (_oid, effect) = {
            let mut catalog_guard = self.catalog.write();
            catalog_guard.create_operator_family_mvcc(row, &ctx)?
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_operator_family_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterOperatorFamilyStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_operator_family_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[])
    }

    pub(crate) fn execute_alter_operator_family_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterOperatorFamilyStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let access_method =
            resolve_index_access_method(self, client_id, Some((xid, cid)), &stmt.access_method)?;
        let current = lookup_opfamily_row(
            self,
            client_id,
            Some((xid, cid)),
            stmt.schema_name.as_deref(),
            &stmt.family_name,
            access_method.oid,
            configured_search_path,
        )?
        .ok_or_else(|| ExecError::DetailedError {
            message: format!(
                "operator family \"{}\" does not exist for access method \"{}\"",
                stmt.family_name, stmt.access_method
            ),
            detail: None,
            hint: None,
            sqlstate: "42704",
        })?;
        match &stmt.action {
            AlterOperatorFamilyAction::Add { items_sql } => {
                return self.execute_alter_operator_family_add_in_transaction(
                    client_id,
                    &current,
                    &access_method,
                    items_sql,
                    xid,
                    cid,
                    configured_search_path,
                    catalog_effects,
                );
            }
            AlterOperatorFamilyAction::Drop { items_sql } => {
                return self.execute_alter_operator_family_drop_in_transaction(
                    client_id,
                    &current,
                    &access_method,
                    items_sql,
                    xid,
                    cid,
                    configured_search_path,
                    catalog_effects,
                );
            }
            _ => {}
        }
        ensure_opfamily_owner(
            self,
            client_id,
            Some((xid, cid)),
            &current,
            &stmt.family_name,
        )?;

        let mut updated = current.clone();
        match &stmt.action {
            AlterOperatorFamilyAction::Rename { new_name } => {
                if opfamily_duplicate_exists(
                    self,
                    client_id,
                    Some((xid, cid)),
                    new_name,
                    current.opfnamespace,
                    access_method.oid,
                    Some(current.oid),
                ) {
                    return Err(duplicate_opfamily_error(
                        &catalog,
                        new_name,
                        &stmt.access_method,
                        current.opfnamespace,
                    ));
                }
                updated.opfname = new_name.to_ascii_lowercase();
            }
            AlterOperatorFamilyAction::OwnerTo { new_owner } => {
                let auth_catalog = self
                    .auth_catalog(client_id, Some((xid, cid)))
                    .map_err(map_catalog_error)?;
                let role = auth_catalog
                    .role_by_name(new_owner)
                    .cloned()
                    .ok_or_else(|| {
                        ExecError::Parse(crate::backend::commands::rolecmds::role_management_error(
                            format!("role \"{new_owner}\" does not exist"),
                        ))
                    })?;
                ensure_can_set_role(self, client_id, role.oid, &role.rolname)?;
                updated.opfowner = role.oid;
            }
            AlterOperatorFamilyAction::SetSchema { new_schema } => {
                let namespace_oid = self
                    .visible_namespace_oid_by_name(client_id, Some((xid, cid)), new_schema)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("schema \"{new_schema}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    })?;
                if opfamily_duplicate_exists(
                    self,
                    client_id,
                    Some((xid, cid)),
                    &current.opfname,
                    namespace_oid,
                    access_method.oid,
                    Some(current.oid),
                ) {
                    return Err(duplicate_opfamily_error(
                        &catalog,
                        &current.opfname,
                        &stmt.access_method,
                        namespace_oid,
                    ));
                }
                updated.opfnamespace = namespace_oid;
            }
            AlterOperatorFamilyAction::Add { .. } | AlterOperatorFamilyAction::Drop { .. } => {
                unreachable!("ADD/DROP handled before owner-based family alterations")
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
        let (_oid, effect) = {
            let mut catalog_guard = self.catalog.write();
            catalog_guard.replace_operator_family_mvcc(&current, updated, &ctx)?
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_alter_operator_family_add_in_transaction(
        &self,
        client_id: ClientId,
        family: &PgOpfamilyRow,
        access_method: &PgAmRow,
        items_sql: &str,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        ensure_operator_family_namespace_create_privilege(
            self,
            client_id,
            Some((xid, cid)),
            family,
        )?;
        ensure_superuser_for_operator_family_alter(self, client_id, Some((xid, cid)))?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let existing_amops = ensure_amop_rows(self, client_id, Some((xid, cid)));
        let existing_amprocs = ensure_amproc_rows(self, client_id, Some((xid, cid)));
        let mut seen_operators = BTreeSet::new();
        let mut seen_functions = BTreeSet::new();
        let mut amop_rows = Vec::new();
        let mut amproc_rows = Vec::new();

        for item in split_family_items(items_sql)? {
            match parse_family_add_item(&catalog, item)? {
                ParsedFamilyItem::AddOperator {
                    strategy,
                    operator_name,
                    left_type_oid,
                    right_type_oid,
                    sort_family,
                } => {
                    if !(1..=5).contains(&strategy) {
                        return Err(ExecError::DetailedError {
                            message: format!(
                                "invalid operator number {strategy}, must be between 1 and 5"
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42601",
                        });
                    }
                    let key = (strategy, left_type_oid, right_type_oid);
                    let type_pair = format_type_pair(&catalog, left_type_oid, right_type_oid);
                    if !seen_operators.insert(key) {
                        return Err(ExecError::DetailedError {
                            message: format!(
                                "operator number {strategy} for ({type_pair}) appears more than once"
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42710",
                        });
                    }
                    if existing_amops.iter().any(|row| {
                        row.amopfamily == family.oid
                            && row.amopstrategy == strategy
                            && row.amoplefttype == left_type_oid
                            && row.amoprighttype == right_type_oid
                    }) {
                        return Err(ExecError::DetailedError {
                            message: format!(
                                "operator {strategy}({type_pair}) already exists in operator family \"{}\"",
                                family.opfname
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42710",
                        });
                    }
                    let operator_oid = catalog
                        .operator_by_name_left_right(&operator_name, left_type_oid, right_type_oid)
                        .map(|row| row.oid)
                        .or_else(|| {
                            (left_type_oid == right_type_oid)
                                .then(|| {
                                    catalog
                                        .operator_by_name_left_right(
                                            &operator_name,
                                            left_type_oid,
                                            left_type_oid,
                                        )
                                        .map(|row| row.oid)
                                })
                                .flatten()
                        })
                        .unwrap_or_else(|| {
                            let operator_hash = operator_name.bytes().fold(0u32, |acc, byte| {
                                acc.wrapping_mul(33).wrapping_add(u32::from(byte))
                            }) % 10_000;
                            80_000
                                + operator_hash
                                + u32::from(strategy as u16)
                                + left_type_oid.saturating_mul(10)
                                + right_type_oid
                        });
                    let sort_family_oid = if let Some(sort_family) = sort_family {
                        if access_method.oid == BTREE_AM_OID {
                            return Err(ExecError::DetailedError {
                                message:
                                    "access method \"btree\" does not support ordering operators"
                                        .into(),
                                detail: None,
                                hint: None,
                                sqlstate: "0A000",
                            });
                        }
                        ensure_opfamily_rows(self, client_id, Some((xid, cid)))
                            .into_iter()
                            .find(|row| {
                                row.opfmethod == BTREE_AM_OID
                                    && row.opfname.eq_ignore_ascii_case(&sort_family)
                            })
                            .map(|row| row.oid)
                            .unwrap_or(0)
                    } else {
                        0
                    };
                    amop_rows.push(PgAmopRow {
                        oid: 0,
                        amopfamily: family.oid,
                        amoplefttype: left_type_oid,
                        amoprighttype: right_type_oid,
                        amopstrategy: strategy,
                        amoppurpose: 's',
                        amopopr: operator_oid,
                        amopmethod: access_method.oid,
                        amopsortfamily: sort_family_oid,
                    });
                }
                ParsedFamilyItem::AddFunction {
                    support,
                    left_type_oid,
                    right_type_oid,
                    function_name,
                    schema_name,
                    arg_type_oids,
                } => {
                    if !(1..=6).contains(&support) {
                        return Err(ExecError::DetailedError {
                            message: format!(
                                "invalid function number {support}, must be between 1 and 6"
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42601",
                        });
                    }
                    let key = (support, left_type_oid, right_type_oid);
                    let type_pair = format_type_pair(&catalog, left_type_oid, right_type_oid);
                    if !seen_functions.insert(key) {
                        return Err(ExecError::DetailedError {
                            message: format!(
                                "function number {support} for ({type_pair}) appears more than once"
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42710",
                        });
                    }
                    if existing_amprocs.iter().any(|row| {
                        row.amprocfamily == family.oid
                            && row.amprocnum == support
                            && row.amproclefttype == left_type_oid
                            && row.amprocrighttype == right_type_oid
                    }) {
                        return Err(ExecError::DetailedError {
                            message: format!(
                                "function {support}({type_pair}) already exists in operator family \"{}\"",
                                family.opfname
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42710",
                        });
                    }
                    if access_method.oid == BTREE_AM_OID
                        && support == 4
                        && left_type_oid != right_type_oid
                    {
                        return Err(ExecError::DetailedError {
                            message: "ordering equal image functions must not be cross-type".into(),
                            detail: None,
                            hint: None,
                            sqlstate: "42601",
                        });
                    }
                    if access_method.oid == BTREE_AM_OID
                        && support == 6
                        && left_type_oid != right_type_oid
                    {
                        return Err(ExecError::DetailedError {
                            message: "btree skip support functions must not be cross-type".into(),
                            detail: None,
                            hint: None,
                            sqlstate: "42601",
                        });
                    }
                    let proc = proc_row_by_signature(
                        &catalog,
                        schema_name.as_deref(),
                        &function_name,
                        &arg_type_oids,
                    )?;
                    validate_family_add_function(
                        &catalog,
                        access_method,
                        support,
                        left_type_oid,
                        right_type_oid,
                        &proc,
                        &arg_type_oids,
                    )?;
                    amproc_rows.push(PgAmprocRow {
                        oid: 0,
                        amprocfamily: family.oid,
                        amproclefttype: left_type_oid,
                        amprocrighttype: right_type_oid,
                        amprocnum: support,
                        amproc: proc.oid,
                    });
                }
                _ => unreachable!("ADD parser returns ADD items"),
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
            let mut catalog_guard = self.catalog.write();
            catalog_guard.add_operator_family_members_mvcc(
                family.oid,
                amop_rows,
                amproc_rows,
                &ctx,
            )?
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    fn execute_alter_operator_family_drop_in_transaction(
        &self,
        client_id: ClientId,
        family: &PgOpfamilyRow,
        _access_method: &PgAmRow,
        items_sql: &str,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        ensure_operator_family_namespace_create_privilege(
            self,
            client_id,
            Some((xid, cid)),
            family,
        )?;
        ensure_superuser_for_operator_family_alter(self, client_id, Some((xid, cid)))?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let existing_amops = ensure_amop_rows(self, client_id, Some((xid, cid)));
        let existing_amprocs = ensure_amproc_rows(self, client_id, Some((xid, cid)));
        let mut amop_rows = Vec::new();
        let mut amproc_rows = Vec::new();

        for item in split_family_items(items_sql)? {
            match parse_family_drop_item(&catalog, item)? {
                ParsedFamilyItem::DropOperator {
                    strategy,
                    left_type_oid,
                    right_type_oid,
                } => {
                    let type_pair = format_type_pair(&catalog, left_type_oid, right_type_oid);
                    let row = existing_amops
                        .iter()
                        .find(|row| {
                            row.amopfamily == family.oid
                                && row.amopstrategy == strategy
                                && row.amoplefttype == left_type_oid
                                && row.amoprighttype == right_type_oid
                        })
                        .cloned()
                        .ok_or_else(|| ExecError::DetailedError {
                            message: format!(
                                "operator {strategy}({type_pair}) does not exist in operator family \"{}\"",
                                family.opfname
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42704",
                        })?;
                    amop_rows.push(row);
                }
                ParsedFamilyItem::DropFunction {
                    support,
                    left_type_oid,
                    right_type_oid,
                } => {
                    let type_pair = format_type_pair(&catalog, left_type_oid, right_type_oid);
                    let row = existing_amprocs
                        .iter()
                        .find(|row| {
                            row.amprocfamily == family.oid
                                && row.amprocnum == support
                                && row.amproclefttype == left_type_oid
                                && row.amprocrighttype == right_type_oid
                        })
                        .cloned()
                        .ok_or_else(|| ExecError::DetailedError {
                            message: format!(
                                "function {support}({type_pair}) does not exist in operator family \"{}\"",
                                family.opfname
                            ),
                            detail: None,
                            hint: None,
                            sqlstate: "42704",
                        })?;
                    amproc_rows.push(row);
                }
                _ => unreachable!("DROP parser returns DROP items"),
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
            let mut catalog_guard = self.catalog.write();
            catalog_guard.drop_operator_family_members_mvcc(amop_rows, amproc_rows, &ctx)?
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_operator_class_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterOperatorClassStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_operator_class_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[])
    }

    pub(crate) fn execute_alter_operator_class_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterOperatorClassStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let access_method =
            resolve_index_access_method(self, client_id, Some((xid, cid)), &stmt.access_method)?;
        let current = lookup_opclass_row(
            self,
            client_id,
            Some((xid, cid)),
            stmt.schema_name.as_deref(),
            &stmt.opclass_name,
            access_method.oid,
            configured_search_path,
        )?
        .ok_or_else(|| ExecError::DetailedError {
            message: format!(
                "operator class \"{}\" does not exist for access method \"{}\"",
                stmt.opclass_name, stmt.access_method
            ),
            detail: None,
            hint: None,
            sqlstate: "42704",
        })?;
        ensure_opclass_owner(
            self,
            client_id,
            Some((xid, cid)),
            &current,
            &stmt.opclass_name,
        )?;

        let mut updated = current.clone();
        match &stmt.action {
            AlterOperatorClassAction::Rename { new_name } => {
                if opclass_duplicate_exists(
                    self,
                    client_id,
                    Some((xid, cid)),
                    new_name,
                    current.opcnamespace,
                    access_method.oid,
                    Some(current.oid),
                ) {
                    return Err(duplicate_opclass_error(
                        &catalog,
                        new_name,
                        &stmt.access_method,
                        current.opcnamespace,
                    ));
                }
                updated.opcname = new_name.to_ascii_lowercase();
            }
            AlterOperatorClassAction::OwnerTo { new_owner } => {
                let auth_catalog = self
                    .auth_catalog(client_id, Some((xid, cid)))
                    .map_err(map_catalog_error)?;
                let role = auth_catalog
                    .role_by_name(new_owner)
                    .cloned()
                    .ok_or_else(|| {
                        ExecError::Parse(crate::backend::commands::rolecmds::role_management_error(
                            format!("role \"{new_owner}\" does not exist"),
                        ))
                    })?;
                ensure_can_set_role(self, client_id, role.oid, &role.rolname)?;
                updated.opcowner = role.oid;
            }
            AlterOperatorClassAction::SetSchema { new_schema } => {
                let namespace_oid = self
                    .visible_namespace_oid_by_name(client_id, Some((xid, cid)), new_schema)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("schema \"{new_schema}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    })?;
                if opclass_duplicate_exists(
                    self,
                    client_id,
                    Some((xid, cid)),
                    &current.opcname,
                    namespace_oid,
                    access_method.oid,
                    Some(current.oid),
                ) {
                    return Err(duplicate_opclass_error(
                        &catalog,
                        &current.opcname,
                        &stmt.access_method,
                        namespace_oid,
                    ));
                }
                updated.opcnamespace = namespace_oid;
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
        let (_oid, effect) = {
            let mut catalog_guard = self.catalog.write();
            catalog_guard.replace_operator_class_mvcc(&current, updated, &ctx)?
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_operator_family_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropOperatorFamilyStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_operator_family_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[])
    }

    pub(crate) fn execute_drop_operator_family_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &DropOperatorFamilyStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let access_method =
            resolve_index_access_method(self, client_id, Some((xid, cid)), &stmt.access_method)?;
        let Some(current) = lookup_opfamily_row(
            self,
            client_id,
            Some((xid, cid)),
            stmt.schema_name.as_deref(),
            &stmt.family_name,
            access_method.oid,
            configured_search_path,
        )?
        else {
            if stmt.if_exists {
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!(
                    "operator family \"{}\" does not exist for access method \"{}\"",
                    stmt.family_name, stmt.access_method
                ),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        };
        ensure_opfamily_owner(
            self,
            client_id,
            Some((xid, cid)),
            &current,
            &stmt.family_name,
        )?;

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
            let mut catalog_guard = self.catalog.write();
            catalog_guard.drop_operator_family_mvcc(&current, &ctx)?
        };
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}
