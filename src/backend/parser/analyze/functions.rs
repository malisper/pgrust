use super::*;
use crate::backend::parser::gram::{SQL_JSON_ARRAYAGG_FUNC, SQL_JSON_OBJECTAGG_FUNC};
use crate::backend::parser::parse_expr;
use crate::backend::utils::record::assign_anonymous_record_descriptor;
use crate::include::catalog::{
    ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID, ANYCOMPATIBLENONARRAYOID,
    ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID, ANYMULTIRANGEOID, ANYOID,
    ANYRANGEOID, CSTRING_TYPE_OID, OID_TYPE_OID, PG_CATALOG_NAMESPACE_OID, PG_LANGUAGE_SQL_OID,
    TEXT_TYPE_OID, UNKNOWN_TYPE_OID, bootstrap_pg_proc_rows_ref,
    builtin_hypothetical_aggregate_function_for_proc_oid, builtin_type_name_for_oid,
    builtin_type_rows, builtin_window_function_for_proc_oid,
};
use crate::include::catalog::{
    multirange_type_ref_for_sql_type, range_type_ref_for_multirange_sql_type,
    range_type_ref_for_sql_type,
};
use crate::include::nodes::primnodes::{
    BuiltinWindowFunction, HashFunctionKind, HypotheticalAggFunc, JsonRecordFunction,
    OrderedSetAggFunc, RegexTableFunction, StringTableFunction, TextSearchTableFunction,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

#[derive(Clone, Copy)]
enum NamedArgDefault {
    Bool(bool),
    Int4(i32),
    Float8(f64),
    Text(&'static str),
    JsonbEmptyObject,
}

struct NamedArgSignature {
    params: &'static [&'static str],
    required: usize,
    defaults: &'static [Option<NamedArgDefault>],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResolvedSrfImpl {
    GenerateSeries,
    Unnest,
    PartitionTree,
    PartitionAncestors,
    PgLockStatus,
    TxidSnapshotXip,
    TextSearch(TextSearchTableFunction),
    JsonTable(JsonTableFunction),
    RegexTable(RegexTableFunction),
    StringTable(StringTableFunction),
    TextSearchTable(TextSearchTableFunction),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ResolvedFunctionRowShape {
    None,
    AnonymousRecord,
    OutParameters(Vec<QueryColumn>),
    NamedComposite {
        relation_oid: u32,
        columns: Vec<QueryColumn>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedFunctionCall {
    pub proc_oid: u32,
    pub proname: String,
    pub prokind: char,
    pub proretset: bool,
    pub result_type: SqlType,
    pub declared_arg_oids: Vec<u32>,
    pub declared_arg_types: Vec<SqlType>,
    pub nvargs: usize,
    pub vatype_oid: u32,
    pub func_variadic: bool,
    pub scalar_impl: Option<BuiltinScalarFunction>,
    pub srf_impl: Option<ResolvedSrfImpl>,
    pub agg_impl: Option<AggFunc>,
    pub hypothetical_agg_impl: Option<HypotheticalAggFunc>,
    pub window_impl: Option<BuiltinWindowFunction>,
    pub row_shape: ResolvedFunctionRowShape,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CandidateMatch {
    declared_arg_types: Vec<SqlType>,
    cost: usize,
    nvargs: usize,
    vatype_oid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedFunctionCallWithArgs {
    pub resolved: ResolvedFunctionCall,
    pub args: Vec<SqlExpr>,
    pub actual_types: Vec<SqlType>,
}

#[derive(Debug, Clone)]
struct NormalizedFunctionCallArgs {
    args: Vec<SqlExpr>,
    actual_types: Vec<SqlType>,
    func_variadic: bool,
    used_defaults: bool,
}

pub(crate) fn resolve_function_call(
    catalog: &dyn CatalogLookup,
    name: &str,
    actual_types: &[SqlType],
    func_variadic: bool,
) -> Result<ResolvedFunctionCall, ParseError> {
    let mut best: Option<(ResolvedFunctionCall, usize, bool, bool)> = None;
    let mut ambiguous = false;
    let Some((lookup_name, namespace_oid)) = function_lookup_name_and_namespace(catalog, name)
    else {
        return Err(undefined_function_error(catalog, name, actual_types));
    };

    for row in catalog.proc_rows_by_name(lookup_name) {
        if namespace_oid.is_some_and(|oid| row.pronamespace != oid) {
            continue;
        }
        let Some(candidate) = match_proc_signature(catalog, &row, actual_types, func_variadic)
        else {
            continue;
        };
        let Some(resolved) =
            resolved_function_call_for_candidate(catalog, &row, &candidate, func_variadic)
        else {
            continue;
        };

        let is_variadic = row.provariadic != 0;
        let expanded = row.provariadic != 0 && !func_variadic && candidate.nvargs > 0;
        match &best {
            None => {
                best = Some((resolved, candidate.cost, is_variadic, expanded));
                ambiguous = false;
            }
            Some((_, best_cost, best_variadic, best_expanded)) => {
                let current_rank = (candidate.cost, is_variadic, expanded);
                let best_rank = (*best_cost, *best_variadic, *best_expanded);
                if current_rank < best_rank {
                    best = Some((resolved, candidate.cost, is_variadic, expanded));
                    ambiguous = false;
                } else if current_rank == best_rank {
                    ambiguous = true;
                }
            }
        }
    }

    if ambiguous {
        return Err(ParseError::UnexpectedToken {
            expected: "unambiguous function call",
            actual: format!("{name}({} args)", actual_types.len()),
        });
    }

    best.map(|(resolved, _, _, _)| resolved).ok_or_else(|| {
        polymorphic_resolution_error_for_candidates(
            catalog,
            lookup_name,
            namespace_oid,
            actual_types,
        )
        .unwrap_or_else(|| undefined_function_error(catalog, name, actual_types))
    })
}

fn resolved_function_call_for_candidate(
    catalog: &dyn CatalogLookup,
    row: &crate::include::catalog::PgProcRow,
    candidate: &CandidateMatch,
    func_variadic: bool,
) -> Option<ResolvedFunctionCall> {
    if !polymorphic_candidate_is_consistent(row, candidate) {
        return None;
    }
    let result_type = resolve_proc_result_type(catalog, row, candidate)?;
    let row_shape = resolve_function_row_shape(catalog, row, candidate, result_type)?;
    let declared_arg_oids = candidate_declared_arg_oids(row, candidate)?;
    let declared_arg_types = concrete_declared_arg_types_for_candidate(row, candidate)
        .unwrap_or_else(|| candidate.declared_arg_types.clone());
    let result_type = match (&result_type.kind, &row_shape) {
        (SqlTypeKind::Record, ResolvedFunctionRowShape::OutParameters(columns)) => {
            assign_anonymous_record_descriptor(
                columns
                    .iter()
                    .map(|column| (column.name.clone(), column.sql_type))
                    .collect(),
            )
            .sql_type()
        }
        _ => result_type,
    };
    Some(ResolvedFunctionCall {
        proc_oid: row.oid,
        proname: row.proname.clone(),
        prokind: row.prokind,
        proretset: row.proretset,
        result_type,
        declared_arg_oids,
        declared_arg_types,
        nvargs: candidate.nvargs,
        vatype_oid: candidate.vatype_oid,
        func_variadic: row.provariadic != 0
            && (func_variadic || (row.provariadic != ANYOID && candidate.nvargs > 0)),
        scalar_impl: builtin_scalar_function_for_proc_row(row),
        srf_impl: builtin_srf_impl_for_proc_row(row),
        agg_impl: aggregate_func_for_proname(&row.proname),
        hypothetical_agg_impl: builtin_hypothetical_aggregate_function_for_proc_oid(row.oid),
        window_impl: builtin_window_function_for_proc_oid(row.oid)
            .or_else(|| builtin_window_function_for_proc_row(row)),
        row_shape,
    })
}

pub(crate) fn resolve_function_call_with_arg_defaults(
    catalog: &dyn CatalogLookup,
    name: &str,
    args: &[SqlFunctionArg],
    actual_types: &[SqlType],
    func_variadic: bool,
) -> Result<ResolvedFunctionCallWithArgs, ParseError> {
    validate_function_call_arg_order(args)?;
    let Some((lookup_name, namespace_oid)) = function_lookup_name_and_namespace(catalog, name)
    else {
        return Err(undefined_function_error(catalog, name, actual_types));
    };

    let mut best: Option<(ResolvedFunctionCallWithArgs, usize, bool, bool)> = None;
    let mut ambiguous = false;
    for row in catalog.proc_rows_by_name(lookup_name) {
        if namespace_oid.is_some_and(|oid| row.pronamespace != oid) {
            continue;
        }
        let Some(normalized) =
            normalize_function_call_args(catalog, &row, args, actual_types, func_variadic)?
        else {
            continue;
        };
        let Some(candidate) = match_proc_signature(
            catalog,
            &row,
            &normalized.actual_types,
            normalized.func_variadic,
        ) else {
            continue;
        };
        let Some(resolved) = resolved_function_call_for_candidate(
            catalog,
            &row,
            &candidate,
            normalized.func_variadic,
        ) else {
            continue;
        };
        let is_variadic = row.provariadic != 0;
        let expanded = row.provariadic != 0 && !normalized.func_variadic && candidate.nvargs > 0;
        let total_cost = candidate.cost;
        let normalized_call = ResolvedFunctionCallWithArgs {
            resolved,
            args: normalized.args,
            actual_types: normalized.actual_types,
        };
        match &best {
            None => {
                best = Some((normalized_call, total_cost, is_variadic, expanded));
                ambiguous = false;
            }
            Some((_, best_cost, best_variadic, best_expanded)) => {
                let current_rank = (total_cost, is_variadic, expanded);
                let best_rank = (*best_cost, *best_variadic, *best_expanded);
                if current_rank < best_rank {
                    best = Some((normalized_call, total_cost, is_variadic, expanded));
                    ambiguous = false;
                } else if current_rank == best_rank {
                    ambiguous = true;
                }
            }
        }
    }

    if ambiguous {
        return Err(ambiguous_function_error(catalog, name, actual_types));
    }
    best.map(|(resolved, _, _, _)| resolved).ok_or_else(|| {
        polymorphic_resolution_error_for_candidates(
            catalog,
            lookup_name,
            namespace_oid,
            actual_types,
        )
        .unwrap_or_else(|| undefined_function_call_error(catalog, name, args, actual_types))
    })
}

fn validate_function_call_arg_order(args: &[SqlFunctionArg]) -> Result<(), ParseError> {
    let mut saw_named = false;
    let mut seen_names = BTreeSet::new();
    for arg in args {
        if let Some(name) = arg.name.as_ref() {
            saw_named = true;
            if !seen_names.insert(name.to_ascii_lowercase()) {
                return Err(ParseError::DetailedError {
                    message: format!("argument name \"{name}\" used more than once"),
                    detail: None,
                    hint: None,
                    sqlstate: "42701",
                });
            }
        } else if saw_named {
            return Err(ParseError::UnexpectedToken {
                expected: "named arguments after positional arguments",
                actual: "positional argument cannot follow named argument".into(),
            });
        }
    }
    Ok(())
}

fn normalize_function_call_args(
    catalog: &dyn CatalogLookup,
    row: &crate::include::catalog::PgProcRow,
    args: &[SqlFunctionArg],
    actual_types: &[SqlType],
    func_variadic: bool,
) -> Result<Option<NormalizedFunctionCallArgs>, ParseError> {
    if args.len() != actual_types.len() {
        return Ok(None);
    }
    let input_oids = parse_proc_argtype_oids(&row.proargtypes).unwrap_or_default();
    let input_count = input_oids.len();
    let has_named = args.iter().any(|arg| arg.name.is_some());
    if !has_named
        && row.provariadic != 0
        && !func_variadic
        && args.len() >= input_count
        && input_count > 0
    {
        return Ok(Some(NormalizedFunctionCallArgs {
            args: args.iter().map(|arg| arg.value.clone()).collect(),
            actual_types: actual_types.to_vec(),
            func_variadic,
            used_defaults: false,
        }));
    }

    let names = callable_proc_arg_names(row, input_count);
    let defaults = decode_proc_arg_defaults(row, input_count);
    let mut assigned = vec![None::<(SqlExpr, SqlType)>; input_count];
    let mut positional_index = 0usize;
    for (arg, actual_type) in args.iter().zip(actual_types.iter().copied()) {
        let input_index = if let Some(name) = arg.name.as_ref() {
            let Some(index) = names
                .iter()
                .position(|candidate| candidate.eq_ignore_ascii_case(name))
            else {
                return Ok(None);
            };
            index
        } else {
            let index = positional_index;
            positional_index += 1;
            if index >= input_count {
                return Ok(None);
            }
            index
        };
        if assigned[input_index].is_some() {
            return Ok(None);
        }
        assigned[input_index] = Some((arg.value.clone(), actual_type));
    }

    let empty_scope = super::scope::empty_scope();
    let default_outer_scopes = [];
    let variadic_input_index = (row.provariadic != 0).then_some(input_count.saturating_sub(1));
    let mut effective_func_variadic = func_variadic;
    let mut used_defaults = false;
    for (index, slot) in assigned.iter_mut().enumerate() {
        if slot.is_some() {
            continue;
        }
        let Some(default_sql) = defaults.get(index).and_then(|default| default.as_deref()) else {
            return Ok(None);
        };
        let default_expr = parse_expr(default_sql)?;
        let default_type = default_proc_arg_type(
            catalog,
            input_oids.get(index).copied().unwrap_or_default(),
            &default_expr,
            &empty_scope,
            &default_outer_scopes,
        );
        *slot = Some((default_expr, default_type));
        if Some(index) == variadic_input_index {
            effective_func_variadic = true;
        }
        used_defaults = true;
    }

    let (args, actual_types): (Vec<_>, Vec<_>) = assigned.into_iter().flatten().unzip();
    Ok(Some(NormalizedFunctionCallArgs {
        args,
        actual_types,
        func_variadic: effective_func_variadic,
        used_defaults,
    }))
}

fn default_proc_arg_type(
    catalog: &dyn CatalogLookup,
    target_oid: u32,
    default_expr: &SqlExpr,
    scope: &BoundScope,
    outer_scopes: &[BoundScope],
) -> SqlType {
    if matches!(
        target_oid,
        ANYOID
            | ANYELEMENTOID
            | ANYARRAYOID
            | ANYENUMOID
            | ANYRANGEOID
            | ANYMULTIRANGEOID
            | ANYCOMPATIBLEOID
            | ANYCOMPATIBLENONARRAYOID
            | ANYCOMPATIBLEARRAYOID
            | ANYCOMPATIBLERANGEOID
            | ANYCOMPATIBLEMULTIRANGEOID
    ) {
        super::infer::infer_sql_expr_function_arg_type_with_ctes(
            default_expr,
            scope,
            catalog,
            outer_scopes,
            None,
            &[],
        )
    } else {
        let inferred =
            super::infer::infer_sql_expr_type(default_expr, scope, catalog, outer_scopes, None);
        catalog
            .type_by_oid(target_oid)
            .map(|row| row.sql_type)
            .unwrap_or(inferred)
    }
}

pub(crate) fn decode_proc_arg_defaults(
    row: &crate::include::catalog::PgProcRow,
    input_count: usize,
) -> Vec<Option<String>> {
    let Some(defaults) = row.proargdefaults.as_deref() else {
        return vec![None; input_count];
    };
    if let Ok(parsed) = serde_json::from_str::<Vec<Option<String>>>(defaults)
        && parsed.len() == input_count
    {
        return parsed;
    }
    let legacy = defaults
        .split_whitespace()
        .map(|default| Some(default.to_string()))
        .collect::<Vec<_>>();
    let mut aligned = vec![None; input_count.saturating_sub(legacy.len())];
    aligned.extend(legacy);
    aligned.resize(input_count, None);
    aligned
}

fn callable_proc_arg_names(
    row: &crate::include::catalog::PgProcRow,
    input_count: usize,
) -> Vec<String> {
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

fn function_lookup_name_and_namespace<'a>(
    catalog: &dyn CatalogLookup,
    name: &'a str,
) -> Option<(&'a str, Option<u32>)> {
    let Some((schema_name, base_name)) = name.rsplit_once('.') else {
        return Some((name, None));
    };
    if schema_name.eq_ignore_ascii_case("pg_temp")
        && let Some(namespace_oid) = catalog.search_path().into_iter().find_map(|schema| {
            schema
                .to_ascii_lowercase()
                .starts_with("pg_temp_")
                .then(|| {
                    catalog
                        .namespace_rows()
                        .into_iter()
                        .find(|row| row.nspname.eq_ignore_ascii_case(&schema))
                        .map(|row| row.oid)
                })
                .flatten()
        })
    {
        return Some((base_name, Some(namespace_oid)));
    }
    let namespace_oid = catalog
        .namespace_rows()
        .into_iter()
        .find(|row| row.nspname.eq_ignore_ascii_case(schema_name))
        .map(|row| row.oid);
    namespace_oid.map(|oid| (base_name, Some(oid)))
}

fn undefined_function_error(
    catalog: &dyn CatalogLookup,
    name: &str,
    actual_types: &[SqlType],
) -> ParseError {
    undefined_function_error_with_signature(function_signature_text(catalog, name, actual_types))
}

fn undefined_function_call_error(
    catalog: &dyn CatalogLookup,
    name: &str,
    args: &[SqlFunctionArg],
    actual_types: &[SqlType],
) -> ParseError {
    undefined_function_error_with_signature(function_call_signature_text(
        catalog,
        name,
        args,
        actual_types,
    ))
}

fn undefined_function_error_with_signature(signature: String) -> ParseError {
    ParseError::DetailedError {
        message: format!("function {signature} does not exist"),
        detail: None,
        hint: Some(
            "No function matches the given name and argument types. You might need to add explicit type casts."
                .into(),
        ),
        sqlstate: "42883",
    }
}

pub(crate) fn sql_function_anyarray_return_resolution_error(
    catalog: &dyn CatalogLookup,
    name: &str,
    actual_types: &[SqlType],
) -> Option<ParseError> {
    let (lookup_name, namespace_oid) = function_lookup_name_and_namespace(catalog, name)?;
    for row in catalog.proc_rows_by_name(lookup_name) {
        if namespace_oid.is_some_and(|oid| row.pronamespace != oid) {
            continue;
        }
        if row.prokind != 'f' || row.prolang != PG_LANGUAGE_SQL_OID || row.prorettype != ANYARRAYOID
        {
            continue;
        }
        let Some(declared_oids) = parse_proc_argtype_oids(&row.proargtypes) else {
            continue;
        };
        if declared_oids.len() != actual_types.len() {
            continue;
        }
        if declared_oids
            .iter()
            .copied()
            .zip(actual_types.iter().copied())
            .any(|(declared_oid, actual_type)| {
                declared_oid == ANYARRAYOID
                    && (matches!(actual_type.kind, SqlTypeKind::AnyArray)
                        || actual_type.type_oid == ANYARRAYOID)
            })
        {
            return Some(ParseError::DetailedError {
                message: "return type anyarray is not supported for SQL functions".into(),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
    }
    None
}

fn is_unknown_sql_type(ty: SqlType) -> bool {
    ty.type_oid == UNKNOWN_TYPE_OID
}

fn polymorphic_resolution_error_for_candidates(
    catalog: &dyn CatalogLookup,
    lookup_name: &str,
    namespace_oid: Option<u32>,
    actual_types: &[SqlType],
) -> Option<ParseError> {
    let saw_unknown = actual_types.iter().copied().any(is_unknown_sql_type);
    for row in catalog.proc_rows_by_name(lookup_name) {
        if namespace_oid.is_some_and(|oid| row.pronamespace != oid) {
            continue;
        }
        let Some(declared_oids) = parse_proc_argtype_oids(&row.proargtypes) else {
            continue;
        };
        if declared_oids.len() != actual_types.len() {
            continue;
        }
        if let Some(err) = anyarray_pseudotype_resolution_error(&row, &declared_oids, actual_types)
        {
            return Some(err);
        }
        if saw_unknown
            && let Some(err) =
                unknown_polymorphic_resolution_error(&row, &declared_oids, actual_types)
        {
            return Some(err);
        }
    }
    None
}

fn anyarray_pseudotype_resolution_error(
    row: &crate::include::catalog::PgProcRow,
    declared_oids: &[u32],
    actual_types: &[SqlType],
) -> Option<ParseError> {
    let has_anyarray_argument = declared_oids
        .iter()
        .copied()
        .zip(actual_types.iter().copied())
        .any(|(declared_oid, actual_type)| {
            declared_oid == ANYARRAYOID
                && (matches!(actual_type.kind, SqlTypeKind::AnyArray)
                    || actual_type.type_oid == ANYARRAYOID)
        });
    if has_anyarray_argument && matches!(row.prorettype, ANYELEMENTOID | ANYARRAYOID) {
        return Some(ParseError::DetailedError {
            message: "cannot determine element type of \"anyarray\" argument".into(),
            detail: None,
            hint: None,
            sqlstate: "42804",
        });
    }
    None
}

fn unknown_polymorphic_resolution_error(
    row: &crate::include::catalog::PgProcRow,
    declared_oids: &[u32],
    actual_types: &[SqlType],
) -> Option<ParseError> {
    for (declared_oid, actual_type) in declared_oids
        .iter()
        .copied()
        .zip(actual_types.iter().copied())
    {
        if !is_unknown_sql_type(actual_type) {
            continue;
        }
        let type_name = match declared_oid {
            ANYCOMPATIBLERANGEOID => Some("anycompatiblerange"),
            ANYCOMPATIBLEMULTIRANGEOID if row.prorettype == ANYCOMPATIBLERANGEOID => {
                Some("anycompatiblerange")
            }
            ANYCOMPATIBLEMULTIRANGEOID => Some("anycompatiblemultirange"),
            ANYCOMPATIBLEARRAYOID => Some("anycompatiblearray"),
            ANYRANGEOID | ANYMULTIRANGEOID => None,
            ANYARRAYOID => Some("anyarray"),
            ANYELEMENTOID | ANYOID | ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => None,
            _ => continue,
        };
        let message = if let Some(type_name) = type_name {
            format!(
                "could not determine polymorphic type {type_name} because input has type unknown"
            )
        } else {
            "could not determine polymorphic type because input has type unknown".into()
        };
        if matches!(
            row.prorettype,
            ANYOID
                | ANYELEMENTOID
                | ANYARRAYOID
                | ANYRANGEOID
                | ANYMULTIRANGEOID
                | ANYCOMPATIBLEOID
                | ANYCOMPATIBLENONARRAYOID
                | ANYCOMPATIBLEARRAYOID
                | ANYCOMPATIBLERANGEOID
                | ANYCOMPATIBLEMULTIRANGEOID
        ) || declared_oids.iter().copied().any(|oid| {
            matches!(
                oid,
                ANYOID
                    | ANYELEMENTOID
                    | ANYARRAYOID
                    | ANYRANGEOID
                    | ANYMULTIRANGEOID
                    | ANYCOMPATIBLEOID
                    | ANYCOMPATIBLENONARRAYOID
                    | ANYCOMPATIBLEARRAYOID
                    | ANYCOMPATIBLERANGEOID
                    | ANYCOMPATIBLEMULTIRANGEOID
            )
        }) {
            return Some(ParseError::DetailedError {
                message,
                detail: None,
                hint: None,
                sqlstate: "42804",
            });
        }
    }
    None
}

fn ambiguous_function_error(
    catalog: &dyn CatalogLookup,
    name: &str,
    actual_types: &[SqlType],
) -> ParseError {
    ParseError::DetailedError {
        message: format!(
            "function {} is not unique",
            function_signature_text(catalog, name, actual_types)
        ),
        detail: None,
        hint: Some(
            "Could not choose a best candidate function. You might need to add explicit type casts."
                .into(),
        ),
        sqlstate: "42725",
    }
}

fn function_signature_text(
    catalog: &dyn CatalogLookup,
    name: &str,
    actual_types: &[SqlType],
) -> String {
    let signature = actual_types
        .iter()
        .map(|ty| function_signature_type_name(catalog, *ty))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{name}({signature})")
}

fn function_call_signature_text(
    catalog: &dyn CatalogLookup,
    name: &str,
    args: &[SqlFunctionArg],
    actual_types: &[SqlType],
) -> String {
    let signature = args
        .iter()
        .zip(actual_types.iter())
        .map(|(arg, ty)| {
            let type_name = function_signature_type_name(catalog, *ty);
            if let Some(name) = arg.name.as_ref() {
                format!("{name} => {type_name}")
            } else {
                type_name
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("{name}({signature})")
}

fn function_signature_type_name(catalog: &dyn CatalogLookup, ty: SqlType) -> String {
    if !ty.is_array
        && ty.type_oid != 0
        && let Some(row) = catalog.type_by_oid(ty.type_oid)
    {
        return row.typname;
    }
    sql_type_name(ty)
}

fn is_unknown_null_resolution_type(ty: SqlType) -> bool {
    !ty.is_array
        && ty.type_oid == 0
        && ty.typrelid == 0
        && matches!(ty.kind, SqlTypeKind::AnyElement)
}

fn polymorphic_candidate_is_consistent(
    row: &crate::include::catalog::PgProcRow,
    candidate: &CandidateMatch,
) -> bool {
    if row.proname.eq_ignore_ascii_case("array_larger") {
        return true;
    }
    let Some(declared_oids) = candidate_declared_arg_oids(row, candidate) else {
        return false;
    };
    let mut ordinary_base = None;
    let mut saw_ordinary = false;
    let mut saw_compatible = false;
    for (declared_oid, actual_type) in declared_oids
        .into_iter()
        .zip(candidate.declared_arg_types.iter().copied())
    {
        if matches!(
            declared_oid,
            ANYELEMENTOID | ANYARRAYOID | ANYRANGEOID | ANYMULTIRANGEOID
        ) {
            saw_ordinary = true;
            if is_unknown_null_resolution_type(actual_type) {
                continue;
            }
            let Some(base) = ordinary_polymorphic_base_type(declared_oid, actual_type) else {
                if is_unknown_sql_type(actual_type) {
                    continue;
                }
                if declared_oid == ANYARRAYOID && is_text_like_type(actual_type) {
                    continue;
                }
                return false;
            };
            let base = canonical_polymorphic_type(base);
            match ordinary_base {
                None => ordinary_base = Some(base),
                Some(existing) if existing == base => {}
                Some(_) => return false,
            }
        }
        if matches!(
            declared_oid,
            ANYCOMPATIBLEOID
                | ANYCOMPATIBLENONARRAYOID
                | ANYCOMPATIBLEARRAYOID
                | ANYCOMPATIBLERANGEOID
                | ANYCOMPATIBLEMULTIRANGEOID
        ) {
            saw_compatible = true;
        }
    }
    (!saw_ordinary || ordinary_base.is_some())
        && (!saw_compatible || resolve_anycompatible_element_type(row, candidate).is_some())
}

fn concrete_declared_arg_types_for_candidate(
    row: &crate::include::catalog::PgProcRow,
    candidate: &CandidateMatch,
) -> Option<Vec<SqlType>> {
    let declared_oids = candidate_declared_arg_oids(row, candidate)?;
    let anyelement = resolve_anyelement_result_type(row, candidate);
    let anyenum = resolve_anyenum_result_type(row, candidate);
    let anyarray = resolve_anyarray_result_type(row, candidate);
    let anyrange = resolve_anyrange_result_type(row, candidate);
    let anymultirange = resolve_anymultirange_result_type(row, candidate);
    let anycompatible = resolve_anycompatible_element_type(row, candidate);
    let anycompatiblearray = anycompatible.map(SqlType::array_of);
    let anycompatiblerange = resolve_anycompatible_range_result_type(row, candidate);
    let anycompatiblemultirange = resolve_anycompatible_multirange_result_type(row, candidate);

    Some(
        declared_oids
            .into_iter()
            .zip(candidate.declared_arg_types.iter().copied())
            .map(|(declared_oid, actual_type)| match declared_oid {
                ANYOID => actual_type,
                ANYELEMENTOID => anyelement.unwrap_or(actual_type),
                ANYENUMOID => anyenum.unwrap_or(actual_type),
                ANYARRAYOID => anyarray.unwrap_or(actual_type),
                ANYRANGEOID => anyrange.unwrap_or(actual_type),
                ANYMULTIRANGEOID => anymultirange.unwrap_or(actual_type),
                ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => anycompatible.unwrap_or(actual_type),
                ANYCOMPATIBLEARRAYOID => anycompatiblearray.unwrap_or(actual_type),
                ANYCOMPATIBLERANGEOID => anycompatiblerange.unwrap_or(actual_type),
                ANYCOMPATIBLEMULTIRANGEOID => anycompatiblemultirange.unwrap_or(actual_type),
                _ => actual_type,
            })
            .collect(),
    )
}

fn candidate_declared_arg_oids(
    row: &crate::include::catalog::PgProcRow,
    candidate: &CandidateMatch,
) -> Option<Vec<u32>> {
    let declared_oids = parse_proc_argtype_oids(&row.proargtypes)?;
    if row.provariadic == 0 || candidate.nvargs == 0 {
        return Some(declared_oids);
    }

    let fixed_prefix_len = declared_oids.len().saturating_sub(1);
    let mut expanded = Vec::with_capacity(candidate.declared_arg_types.len());
    expanded.extend_from_slice(&declared_oids[..fixed_prefix_len]);
    expanded.extend(std::iter::repeat_n(
        row.provariadic,
        candidate.declared_arg_types.len() - fixed_prefix_len,
    ));
    Some(expanded)
}

fn ordinary_polymorphic_base_type(declared_oid: u32, actual_type: SqlType) -> Option<SqlType> {
    if is_unknown_sql_type(actual_type) {
        return None;
    }
    match declared_oid {
        ANYELEMENTOID => Some(actual_type),
        ANYARRAYOID if actual_type.is_array => Some(actual_type.element_type()),
        ANYRANGEOID if actual_type.is_range() => {
            range_type_ref_for_sql_type(actual_type).map(|range_type| range_type.subtype)
        }
        ANYMULTIRANGEOID if actual_type.is_multirange() => {
            range_type_ref_for_multirange_sql_type(actual_type).map(|range_type| range_type.subtype)
        }
        _ => None,
    }
}

pub(super) fn resolve_scalar_function(name: &str) -> Option<BuiltinScalarFunction> {
    let normalized = normalize_builtin_function_name(name);
    scalar_functions_by_name().get(normalized).copied()
}

pub(super) fn resolve_builtin_aggregate(name: &str) -> Option<AggFunc> {
    aggregate_func_for_proname(name)
}

pub(super) fn resolve_builtin_hypothetical_aggregate(name: &str) -> Option<HypotheticalAggFunc> {
    match normalize_builtin_function_name(name) {
        "rank" => Some(HypotheticalAggFunc::Rank),
        "dense_rank" => Some(HypotheticalAggFunc::DenseRank),
        "percent_rank" => Some(HypotheticalAggFunc::PercentRank),
        "cume_dist" => Some(HypotheticalAggFunc::CumeDist),
        _ => None,
    }
}

pub(super) fn resolve_builtin_ordered_set_aggregate(name: &str) -> Option<OrderedSetAggFunc> {
    match normalize_builtin_function_name(name) {
        "percentile_disc" => Some(OrderedSetAggFunc::PercentileDisc),
        _ => None,
    }
}

pub(super) fn resolve_function_cast_type(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Option<SqlType> {
    let normalized = normalize_builtin_function_name(name);
    if let Some(row) = catalog.type_by_name(normalized) {
        if row.typrelid != 0 {
            return None;
        }
        if row.oid != TEXT_TYPE_OID && !catalog_text_input_cast_exists(catalog, row.oid) {
            return None;
        }
        return Some(match row.typname.as_str() {
            "bit" => SqlType::with_bit_len(SqlTypeKind::Bit, 1),
            _ => row.sql_type,
        });
    }
    for (alias, canonical) in function_cast_type_aliases() {
        if alias.eq_ignore_ascii_case(name) {
            return resolve_function_cast_type(catalog, canonical);
        }
    }
    None
}

pub(super) fn explicit_text_input_cast_exists(
    catalog: &dyn CatalogLookup,
    target: SqlType,
) -> bool {
    if target.type_oid != 0
        && let Some(domain) = catalog.domain_by_type_oid(target.type_oid)
        && let Some(base_oid) = catalog.type_oid_for_sql_type(domain.sql_type)
    {
        return base_oid == TEXT_TYPE_OID || catalog_text_input_cast_exists(catalog, base_oid);
    }
    let Some(target_oid) = catalog_builtin_type_oid(catalog, target) else {
        return false;
    };
    if target_oid == TEXT_TYPE_OID {
        return true;
    }
    catalog_text_input_cast_exists(catalog, target_oid)
}

pub(super) fn resolve_json_table_function(name: &str) -> Option<JsonTableFunction> {
    json_table_functions_by_name()
        .get(normalize_builtin_function_name(name))
        .copied()
}

pub(super) fn resolve_regex_table_function(name: &str) -> Option<RegexTableFunction> {
    match normalize_builtin_function_name(name) {
        "regexp_matches" => Some(RegexTableFunction::Matches),
        "regexp_split_to_table" => Some(RegexTableFunction::SplitToTable),
        _ => None,
    }
}

pub(super) fn resolve_string_table_function(name: &str) -> Option<StringTableFunction> {
    match normalize_builtin_function_name(name) {
        "string_to_table" => Some(StringTableFunction::StringToTable),
        _ => None,
    }
}

pub(super) fn resolve_text_search_table_function(name: &str) -> Option<TextSearchTableFunction> {
    match normalize_builtin_function_name(name) {
        "ts_token_type" => Some(TextSearchTableFunction::TokenType),
        "ts_parse" => Some(TextSearchTableFunction::Parse),
        "ts_debug" => Some(TextSearchTableFunction::Debug),
        "ts_stat" => Some(TextSearchTableFunction::Stat),
        _ => None,
    }
}

pub(super) fn text_search_table_function_columns(
    kind: TextSearchTableFunction,
) -> Vec<QueryColumn> {
    match kind {
        TextSearchTableFunction::TokenType => vec![
            QueryColumn {
                name: "tokid".into(),
                sql_type: SqlType::new(SqlTypeKind::Int4),
                wire_type_oid: None,
            },
            QueryColumn::text("alias"),
            QueryColumn::text("description"),
        ],
        TextSearchTableFunction::Parse => vec![
            QueryColumn {
                name: "tokid".into(),
                sql_type: SqlType::new(SqlTypeKind::Int4),
                wire_type_oid: None,
            },
            QueryColumn::text("token"),
        ],
        TextSearchTableFunction::Debug => vec![
            QueryColumn::text("alias"),
            QueryColumn::text("description"),
            QueryColumn::text("token"),
            QueryColumn {
                name: "dictionaries".into(),
                sql_type: SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                wire_type_oid: None,
            },
            QueryColumn::text("dictionary"),
            QueryColumn {
                name: "lexemes".into(),
                sql_type: SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
                wire_type_oid: None,
            },
        ],
        TextSearchTableFunction::Stat => vec![
            QueryColumn::text("word"),
            QueryColumn {
                name: "ndoc".into(),
                sql_type: SqlType::new(SqlTypeKind::Int4),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "nentry".into(),
                sql_type: SqlType::new(SqlTypeKind::Int4),
                wire_type_oid: None,
            },
        ],
    }
}

pub(super) fn resolve_json_record_function(name: &str) -> Option<JsonRecordFunction> {
    match normalize_builtin_function_name(name) {
        "json_populate_record" => Some(JsonRecordFunction::PopulateRecord),
        "json_populate_recordset" => Some(JsonRecordFunction::PopulateRecordSet),
        "json_to_record" => Some(JsonRecordFunction::ToRecord),
        "json_to_recordset" => Some(JsonRecordFunction::ToRecordSet),
        "jsonb_populate_record" => Some(JsonRecordFunction::JsonbPopulateRecord),
        "jsonb_populate_recordset" => Some(JsonRecordFunction::JsonbPopulateRecordSet),
        "jsonb_to_record" => Some(JsonRecordFunction::JsonbToRecord),
        "jsonb_to_recordset" => Some(JsonRecordFunction::JsonbToRecordSet),
        _ => None,
    }
}

pub(super) fn normalize_builtin_function_name(name: &str) -> &str {
    name.strip_prefix("pg_catalog.").unwrap_or(name)
}

fn builtin_scalar_function_for_proc_row(row: &PgProcRow) -> Option<BuiltinScalarFunction> {
    let builtin_by_src = builtin_scalar_function_for_proc_src(&row.prosrc);
    if row.pronamespace != PG_CATALOG_NAMESPACE_OID {
        return builtin_by_src.filter(|func| is_dynamic_range_scalar_function(*func));
    }
    if row.proname.eq_ignore_ascii_case("timestamptz")
        && matches!(row.proargtypes.trim(), "1082 1083" | "1082 1266")
    {
        return Some(BuiltinScalarFunction::TimestampTzConstructor);
    }
    builtin_by_src.or_else(|| builtin_scalar_function_for_proc_src(&row.proname))
}

fn is_dynamic_range_scalar_function(func: BuiltinScalarFunction) -> bool {
    matches!(
        func,
        BuiltinScalarFunction::RangeConstructor
            | BuiltinScalarFunction::RangeIsEmpty
            | BuiltinScalarFunction::RangeLower
            | BuiltinScalarFunction::RangeUpper
            | BuiltinScalarFunction::RangeLowerInc
            | BuiltinScalarFunction::RangeUpperInc
            | BuiltinScalarFunction::RangeLowerInf
            | BuiltinScalarFunction::RangeUpperInf
            | BuiltinScalarFunction::RangeContains
            | BuiltinScalarFunction::RangeContainedBy
            | BuiltinScalarFunction::RangeOverlap
            | BuiltinScalarFunction::RangeStrictLeft
            | BuiltinScalarFunction::RangeStrictRight
            | BuiltinScalarFunction::RangeOverLeft
            | BuiltinScalarFunction::RangeOverRight
            | BuiltinScalarFunction::RangeAdjacent
            | BuiltinScalarFunction::RangeUnion
            | BuiltinScalarFunction::RangeIntersect
            | BuiltinScalarFunction::RangeDifference
            | BuiltinScalarFunction::RangeMerge
    )
}

fn builtin_srf_impl_for_proc_row(row: &PgProcRow) -> Option<ResolvedSrfImpl> {
    match row.proname.to_ascii_lowercase().as_str() {
        "generate_series" => Some(ResolvedSrfImpl::GenerateSeries),
        "unnest" => Some(ResolvedSrfImpl::Unnest),
        "pg_partition_tree" => Some(ResolvedSrfImpl::PartitionTree),
        "pg_partition_ancestors" => Some(ResolvedSrfImpl::PartitionAncestors),
        "pg_lock_status" => Some(ResolvedSrfImpl::PgLockStatus),
        "txid_snapshot_xip" | "pg_snapshot_xip" => Some(ResolvedSrfImpl::TxidSnapshotXip),
        "ts_token_type" => Some(ResolvedSrfImpl::TextSearch(
            TextSearchTableFunction::TokenType,
        )),
        "ts_parse" => Some(ResolvedSrfImpl::TextSearch(TextSearchTableFunction::Parse)),
        "ts_debug" => Some(ResolvedSrfImpl::TextSearch(TextSearchTableFunction::Debug)),
        "ts_stat" => Some(ResolvedSrfImpl::TextSearch(TextSearchTableFunction::Stat)),
        other => resolve_json_table_function(other)
            .map(ResolvedSrfImpl::JsonTable)
            .or_else(|| resolve_regex_table_function(other).map(ResolvedSrfImpl::RegexTable))
            .or_else(|| resolve_string_table_function(other).map(ResolvedSrfImpl::StringTable))
            .or_else(|| {
                resolve_text_search_table_function(other).map(ResolvedSrfImpl::TextSearchTable)
            }),
    }
}

fn builtin_window_function_for_proc_row(row: &PgProcRow) -> Option<BuiltinWindowFunction> {
    if row.prokind != 'w' {
        return None;
    }
    match row.prosrc.to_ascii_lowercase().as_str() {
        "window_row_number" => Some(BuiltinWindowFunction::RowNumber),
        "window_rank" => Some(BuiltinWindowFunction::Rank),
        "window_dense_rank" => Some(BuiltinWindowFunction::DenseRank),
        "window_percent_rank" => Some(BuiltinWindowFunction::PercentRank),
        "window_cume_dist" => Some(BuiltinWindowFunction::CumeDist),
        "window_ntile" => Some(BuiltinWindowFunction::Ntile),
        "window_lag" | "window_lag_with_offset" | "window_lag_with_offset_and_default" => {
            Some(BuiltinWindowFunction::Lag)
        }
        "window_lead" | "window_lead_with_offset" | "window_lead_with_offset_and_default" => {
            Some(BuiltinWindowFunction::Lead)
        }
        "window_first_value" => Some(BuiltinWindowFunction::FirstValue),
        "window_last_value" => Some(BuiltinWindowFunction::LastValue),
        "window_nth_value" => Some(BuiltinWindowFunction::NthValue),
        _ => None,
    }
}

fn match_proc_signature(
    catalog: &dyn CatalogLookup,
    row: &PgProcRow,
    actual_types: &[SqlType],
    func_variadic: bool,
) -> Option<CandidateMatch> {
    let declared_oids = parse_proc_argtype_oids(&row.proargtypes)?;
    if matches!(row.prosrc.as_str(), "pg_num_nulls" | "pg_num_nonnulls") && actual_types.is_empty()
    {
        return None;
    }
    if row.provariadic == 0 {
        if actual_types.len() != declared_oids.len() {
            return None;
        }
        let mut declared_arg_types = Vec::with_capacity(actual_types.len());
        let mut cost = 0usize;
        for (actual_type, declared_oid) in actual_types.iter().zip(declared_oids.iter()) {
            let (arg_cost, target_type) =
                match_proc_arg_type(catalog, *actual_type, *declared_oid)?;
            cost += arg_cost;
            declared_arg_types.push(target_type);
        }
        if !polymorphic_signature_matches_declared(&declared_oids, &declared_arg_types) {
            return None;
        }
        return Some(CandidateMatch {
            declared_arg_types,
            cost,
            nvargs: 0,
            vatype_oid: 0,
        });
    }

    let fixed_prefix_len = declared_oids.len().saturating_sub(1);
    if actual_types.len() < fixed_prefix_len {
        return None;
    }
    let mut declared_arg_types = Vec::with_capacity(actual_types.len());
    let mut cost = 0usize;
    for (actual_type, declared_oid) in actual_types
        .iter()
        .take(fixed_prefix_len)
        .zip(declared_oids.iter().take(fixed_prefix_len))
    {
        let (arg_cost, target_type) = match_proc_arg_type(catalog, *actual_type, *declared_oid)?;
        cost += arg_cost;
        declared_arg_types.push(target_type);
    }

    if func_variadic {
        if actual_types.len() != declared_oids.len() {
            return None;
        }
        let (arg_cost, target_type) =
            match_explicit_variadic_arg(catalog, *actual_types.last()?, row.provariadic)?;
        cost += arg_cost;
        declared_arg_types.push(target_type);
        if !polymorphic_signature_matches_declared(&declared_oids, &declared_arg_types) {
            return None;
        }
        return Some(CandidateMatch {
            declared_arg_types,
            cost,
            nvargs: 0,
            vatype_oid: row.provariadic,
        });
    }

    let nvargs = actual_types.len().saturating_sub(fixed_prefix_len);
    for actual_type in actual_types.iter().skip(fixed_prefix_len) {
        let (arg_cost, target_type) =
            match_variadic_element_type(catalog, *actual_type, row.provariadic)?;
        cost += arg_cost;
        declared_arg_types.push(target_type);
    }

    let mut expanded_declared_oids = Vec::with_capacity(declared_arg_types.len());
    expanded_declared_oids.extend_from_slice(&declared_oids[..fixed_prefix_len]);
    expanded_declared_oids.extend(std::iter::repeat_n(row.provariadic, nvargs));
    if !polymorphic_signature_matches_declared(&expanded_declared_oids, &declared_arg_types) {
        return None;
    }

    Some(CandidateMatch {
        declared_arg_types,
        cost,
        nvargs,
        vatype_oid: row.provariadic,
    })
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

fn polymorphic_signature_matches_declared(declared_oids: &[u32], actual_types: &[SqlType]) -> bool {
    let mut exact_subtype = None;
    let mut compatible_range_anchor = None;
    let mut compatible_other_subtypes = Vec::new();

    for (declared_oid, actual_type) in declared_oids
        .iter()
        .copied()
        .zip(actual_types.iter().copied())
    {
        match declared_oid {
            ANYOID => {}
            ANYELEMENTOID => {
                if is_unknown_null_resolution_type(actual_type) {
                    continue;
                }
                if is_unknown_sql_type(actual_type) {
                    continue;
                }
                if !merge_exact_polymorphic_subtype(&mut exact_subtype, actual_type) {
                    return false;
                }
            }
            ANYARRAYOID if actual_type.is_array => {
                if is_unknown_sql_type(actual_type) {
                    continue;
                }
                if !merge_exact_polymorphic_subtype(&mut exact_subtype, actual_type.element_type())
                {
                    return false;
                }
            }
            ANYRANGEOID => {
                let Some(range_type) = range_type_ref_for_sql_type(actual_type) else {
                    return false;
                };
                if !merge_exact_polymorphic_subtype(&mut exact_subtype, range_type.subtype) {
                    return false;
                }
            }
            ANYMULTIRANGEOID => {
                let Some(multirange_type) = multirange_type_ref_for_sql_type(actual_type) else {
                    return false;
                };
                if !merge_exact_polymorphic_subtype(
                    &mut exact_subtype,
                    multirange_type.range_type.subtype,
                ) {
                    return false;
                }
            }
            ANYCOMPATIBLEOID => {
                if !is_unknown_sql_type(actual_type) {
                    compatible_other_subtypes.push(actual_type);
                }
            }
            ANYCOMPATIBLENONARRAYOID => {
                if actual_type.is_array {
                    return false;
                }
                if !is_unknown_sql_type(actual_type) {
                    compatible_other_subtypes.push(actual_type);
                }
            }
            ANYCOMPATIBLEARRAYOID if actual_type.is_array => {
                if !is_unknown_sql_type(actual_type) {
                    compatible_other_subtypes.push(actual_type.element_type());
                }
            }
            ANYCOMPATIBLERANGEOID => {
                let Some(range_type) = range_type_ref_for_sql_type(actual_type) else {
                    return false;
                };
                if !merge_exact_polymorphic_subtype(
                    &mut compatible_range_anchor,
                    range_type.subtype,
                ) {
                    return false;
                }
            }
            ANYCOMPATIBLEMULTIRANGEOID => {
                let Some(multirange_type) = multirange_type_ref_for_sql_type(actual_type) else {
                    return false;
                };
                if !merge_exact_polymorphic_subtype(
                    &mut compatible_range_anchor,
                    multirange_type.range_type.subtype,
                ) {
                    return false;
                }
            }
            _ => {}
        }
    }

    if let Some(anchor) = compatible_range_anchor {
        compatible_other_subtypes
            .into_iter()
            .all(|actual| can_coerce_to_polymorphic_range_anchor(actual, anchor))
    } else {
        true
    }
}

fn merge_exact_polymorphic_subtype(current: &mut Option<SqlType>, inferred: SqlType) -> bool {
    match *current {
        None => {
            *current = Some(inferred);
            true
        }
        Some(existing) => polymorphic_types_match(existing, inferred),
    }
}

fn can_coerce_to_polymorphic_range_anchor(actual: SqlType, target: SqlType) -> bool {
    if polymorphic_types_match(actual, target) {
        return true;
    }
    if actual.is_array || target.is_array {
        return false;
    }
    if is_text_like_type(actual) && is_text_like_type(target) {
        return true;
    }
    matches!(
        (actual.kind, target.kind),
        (SqlTypeKind::Int2, SqlTypeKind::Int4)
            | (SqlTypeKind::Int2, SqlTypeKind::Int8)
            | (SqlTypeKind::Int2, SqlTypeKind::Numeric)
            | (SqlTypeKind::Int2, SqlTypeKind::Float4)
            | (SqlTypeKind::Int2, SqlTypeKind::Float8)
            | (SqlTypeKind::Int4, SqlTypeKind::Int8)
            | (SqlTypeKind::Int4, SqlTypeKind::Numeric)
            | (SqlTypeKind::Int4, SqlTypeKind::Float4)
            | (SqlTypeKind::Int4, SqlTypeKind::Float8)
            | (SqlTypeKind::Int8, SqlTypeKind::Numeric)
            | (SqlTypeKind::Int8, SqlTypeKind::Float4)
            | (SqlTypeKind::Int8, SqlTypeKind::Float8)
            | (SqlTypeKind::Numeric, SqlTypeKind::Float4)
            | (SqlTypeKind::Numeric, SqlTypeKind::Float8)
            | (SqlTypeKind::Float4, SqlTypeKind::Float8)
    )
}

fn match_proc_arg_type(
    catalog: &dyn CatalogLookup,
    actual_type: SqlType,
    declared_oid: u32,
) -> Option<(usize, SqlType)> {
    if declared_oid == ANYOID || declared_oid == ANYELEMENTOID {
        return Some((2, actual_type));
    }
    if matches!(declared_oid, ANYARRAYOID | ANYCOMPATIBLEARRAYOID) {
        return (actual_type.is_array
            || actual_type.kind == SqlTypeKind::AnyArray
            || is_text_like_type(actual_type))
        .then_some((2, actual_type));
    }
    if declared_oid == ANYENUMOID {
        if !actual_type.is_array
            && (actual_type.kind == SqlTypeKind::Enum || actual_type.kind == SqlTypeKind::AnyEnum)
        {
            return Some((2, actual_type));
        }
        if is_text_like_type(actual_type) {
            return Some((4, SqlType::new(SqlTypeKind::AnyEnum)));
        }
        return None;
    }
    if declared_oid == ANYRANGEOID {
        return (actual_type.is_range() || actual_type.kind == SqlTypeKind::AnyRange)
            .then_some((2, actual_type));
    }
    if declared_oid == ANYMULTIRANGEOID {
        return (actual_type.is_multirange() || actual_type.kind == SqlTypeKind::AnyMultirange)
            .then_some((2, actual_type));
    }
    if declared_oid == ANYCOMPATIBLEOID {
        return Some((2, actual_type));
    }
    if declared_oid == ANYCOMPATIBLENONARRAYOID {
        return (!actual_type.is_array).then_some((2, actual_type));
    }
    if declared_oid == ANYCOMPATIBLEARRAYOID {
        return (actual_type.is_array || actual_type.kind == SqlTypeKind::AnyCompatibleArray)
            .then_some((2, actual_type));
    }
    if declared_oid == ANYCOMPATIBLERANGEOID {
        return (actual_type.is_range() || actual_type.kind == SqlTypeKind::AnyCompatibleRange)
            .then_some((2, actual_type));
    }
    if declared_oid == ANYCOMPATIBLEMULTIRANGEOID {
        return (actual_type.is_multirange()
            || actual_type.kind == SqlTypeKind::AnyCompatibleMultirange)
            .then_some((2, actual_type));
    }
    let declared_type = catalog.type_by_oid(declared_oid)?.sql_type;
    if is_unknown_null_resolution_type(actual_type) {
        return Some((4, declared_type));
    }
    if let Some(cost) = domain_arg_type_match_cost(catalog, actual_type, declared_oid) {
        return Some((cost, declared_type));
    }
    if is_reg_oid_alias_type(declared_type)
        && matches!(
            actual_type.kind,
            SqlTypeKind::Oid
                | SqlTypeKind::Int4
                | SqlTypeKind::Int8
                | SqlTypeKind::RegClass
                | SqlTypeKind::RegType
                | SqlTypeKind::RegProc
                | SqlTypeKind::RegProcedure
                | SqlTypeKind::RegOper
                | SqlTypeKind::RegOperator
        )
    {
        return Some((1, declared_type));
    }
    if declared_oid == CSTRING_TYPE_OID && is_text_like_type(actual_type) {
        return Some((3, declared_type));
    }
    if declared_oid == OID_TYPE_OID
        && matches!(
            actual_type.kind,
            SqlTypeKind::Oid | SqlTypeKind::RegType | SqlTypeKind::Int4 | SqlTypeKind::Int8
        )
    {
        return Some((1, declared_type));
    }
    if let Some(cost) = arg_type_match_cost(actual_type, declared_type) {
        return Some((cost, declared_type));
    }
    if inherited_composite_arg_can_coerce_to(catalog, actual_type, declared_type) {
        return Some((1, declared_type));
    }
    if catalog_implicit_cast_exists(catalog, actual_type, declared_oid) {
        return Some((3, declared_type));
    }
    if is_text_like_type(actual_type) && catalog_text_input_cast_exists(catalog, declared_oid) {
        return Some((3, declared_type));
    }
    if !actual_type.is_array
        && declared_type.is_array
        && is_text_like_type(actual_type)
        && catalog_text_input_cast_exists(catalog, declared_oid)
    {
        return Some((3, declared_type));
    }
    None
}

fn domain_arg_type_match_cost(
    catalog: &dyn CatalogLookup,
    actual_type: SqlType,
    declared_oid: u32,
) -> Option<usize> {
    let row = catalog.type_by_oid(declared_oid)?;
    if row.typtype != 'd' || row.typbasetype == 0 {
        return None;
    }
    if actual_type == row.sql_type {
        return Some(0);
    }
    let base_type = domain_base_sql_type(catalog, row.oid)?.with_typmod(row.sql_type.typmod);
    if same_type_ignoring_catalog_identity(actual_type, base_type) {
        return Some(1);
    }
    if let Some(cost) = arg_type_match_cost(actual_type, base_type) {
        return Some(cost + 1);
    }
    if let Some(base_oid) = catalog.type_oid_for_sql_type(base_type)
        && catalog_implicit_cast_exists(catalog, actual_type, base_oid)
    {
        return Some(4);
    }
    None
}

fn domain_base_sql_type(catalog: &dyn CatalogLookup, type_oid: u32) -> Option<SqlType> {
    let row = catalog.type_by_oid(type_oid)?;
    if row.typtype != 'd' || row.typbasetype == 0 {
        return None;
    }
    let base_row = catalog.type_by_oid(row.typbasetype)?;
    if base_row.typtype == 'd' {
        domain_base_sql_type(catalog, base_row.oid)
    } else {
        Some(base_row.sql_type)
    }
}

fn same_type_ignoring_catalog_identity(actual: SqlType, target: SqlType) -> bool {
    if actual == target {
        return true;
    }
    if actual.is_array != target.is_array {
        return false;
    }
    if actual.is_array {
        return same_type_ignoring_catalog_identity(actual.element_type(), target.element_type());
    }
    actual.kind == target.kind
        && actual.typrelid == target.typrelid
        && (target.typmod < SqlType::VARHDRSZ || actual.typmod == target.typmod)
}

fn inherited_composite_arg_can_coerce_to(
    catalog: &dyn CatalogLookup,
    actual_type: SqlType,
    declared_type: SqlType,
) -> bool {
    if actual_type.is_array
        || declared_type.is_array
        || actual_type.kind != SqlTypeKind::Composite
        || declared_type.kind != SqlTypeKind::Composite
        || actual_type.typrelid == 0
        || declared_type.typrelid == 0
        || actual_type.typrelid == declared_type.typrelid
    {
        return false;
    }

    let mut pending = vec![actual_type.typrelid];
    let mut seen = BTreeSet::new();
    while let Some(relation_oid) = pending.pop() {
        if !seen.insert(relation_oid) {
            continue;
        }
        for parent in catalog.inheritance_parents(relation_oid) {
            if parent.inhparent == declared_type.typrelid {
                return true;
            }
            pending.push(parent.inhparent);
        }
    }
    false
}

fn is_reg_oid_alias_type(ty: SqlType) -> bool {
    !ty.is_array
        && matches!(
            ty.kind,
            SqlTypeKind::RegClass
                | SqlTypeKind::RegType
                | SqlTypeKind::RegProc
                | SqlTypeKind::RegProcedure
                | SqlTypeKind::RegOper
                | SqlTypeKind::RegOperator
        )
}

fn resolve_proc_result_type(
    catalog: &dyn CatalogLookup,
    row: &crate::include::catalog::PgProcRow,
    candidate: &CandidateMatch,
) -> Option<SqlType> {
    if row.proname.eq_ignore_ascii_case("array_in") && row.prorettype == ANYARRAYOID {
        return Some(SqlType::new(SqlTypeKind::AnyArray));
    }
    if row.proname.eq_ignore_ascii_case("anyrange_in") && row.prorettype == ANYRANGEOID {
        return Some(SqlType::new(SqlTypeKind::AnyRange));
    }
    if row.proname.eq_ignore_ascii_case("array_larger") && row.prorettype == ANYARRAYOID {
        return Some(SqlType::new(SqlTypeKind::AnyArray));
    }
    match row.prorettype {
        ANYOID | ANYELEMENTOID => resolve_anyelement_result_type(row, candidate),
        ANYENUMOID => resolve_anyenum_result_type(row, candidate),
        ANYARRAYOID => resolve_anyarray_result_type(row, candidate),
        ANYRANGEOID => resolve_anyrange_result_type(row, candidate),
        ANYMULTIRANGEOID => resolve_anymultirange_result_type(row, candidate),
        ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => {
            resolve_anycompatible_element_type(row, candidate)
        }
        ANYCOMPATIBLEARRAYOID => {
            resolve_anycompatible_element_type(row, candidate).map(SqlType::array_of)
        }
        ANYCOMPATIBLERANGEOID => resolve_anycompatible_range_result_type(row, candidate),
        ANYCOMPATIBLEMULTIRANGEOID => resolve_anycompatible_multirange_result_type(row, candidate),
        _ => catalog.type_by_oid(row.prorettype).map(|row| row.sql_type),
    }
}

fn resolve_function_row_shape(
    catalog: &dyn CatalogLookup,
    row: &crate::include::catalog::PgProcRow,
    candidate: &CandidateMatch,
    result_type: SqlType,
) -> Option<ResolvedFunctionRowShape> {
    if let Some(row_shape) = resolve_out_parameter_row_shape(catalog, row, candidate) {
        return Some(row_shape);
    }
    match result_type.kind {
        SqlTypeKind::Record => Some(ResolvedFunctionRowShape::AnonymousRecord),
        SqlTypeKind::Composite => {
            let relation_oid = result_type.typrelid;
            let relation = catalog.lookup_relation_by_oid(relation_oid)?;
            let columns = relation
                .desc
                .columns
                .into_iter()
                .filter(|column| !column.dropped)
                .map(|column| QueryColumn {
                    name: column.name,
                    sql_type: column.sql_type,
                    wire_type_oid: None,
                })
                .collect();
            Some(ResolvedFunctionRowShape::NamedComposite {
                relation_oid,
                columns,
            })
        }
        _ => Some(ResolvedFunctionRowShape::None),
    }
}

fn resolve_out_parameter_row_shape(
    catalog: &dyn CatalogLookup,
    row: &crate::include::catalog::PgProcRow,
    candidate: &CandidateMatch,
) -> Option<ResolvedFunctionRowShape> {
    let (Some(arg_types), Some(arg_modes)) = (&row.proallargtypes, &row.proargmodes) else {
        return None;
    };
    if arg_types.len() != arg_modes.len() {
        return None;
    }

    let arg_names = row.proargnames.as_deref().unwrap_or(&[]);
    let mut output_columns = Vec::new();
    for (index, (type_oid, mode)) in arg_types.iter().zip(arg_modes.iter()).enumerate() {
        if !matches!(*mode, b'o' | b'b' | b't') {
            continue;
        }
        let sql_type = resolve_polymorphic_output_type(*type_oid, row, candidate)
            .or_else(|| catalog.type_by_oid(*type_oid).map(|row| row.sql_type))?;
        let name = arg_names
            .get(index)
            .filter(|name| !name.is_empty())
            .cloned()
            .unwrap_or_else(|| format!("column{}", output_columns.len() + 1));
        output_columns.push(QueryColumn {
            name,
            sql_type,
            wire_type_oid: None,
        });
    }

    if output_columns.is_empty() {
        None
    } else {
        Some(ResolvedFunctionRowShape::OutParameters(output_columns))
    }
}

fn resolve_polymorphic_output_type(
    type_oid: u32,
    row: &crate::include::catalog::PgProcRow,
    candidate: &CandidateMatch,
) -> Option<SqlType> {
    match type_oid {
        ANYOID | ANYELEMENTOID => resolve_anyelement_result_type(row, candidate),
        ANYENUMOID => resolve_anyenum_result_type(row, candidate),
        ANYARRAYOID => resolve_anyarray_result_type(row, candidate),
        ANYRANGEOID => resolve_anyrange_result_type(row, candidate),
        ANYMULTIRANGEOID => resolve_anymultirange_result_type(row, candidate),
        ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => {
            resolve_anycompatible_element_type(row, candidate)
        }
        ANYCOMPATIBLEARRAYOID => {
            resolve_anycompatible_element_type(row, candidate).map(SqlType::array_of)
        }
        ANYCOMPATIBLERANGEOID => resolve_anycompatible_range_result_type(row, candidate),
        ANYCOMPATIBLEMULTIRANGEOID => resolve_anycompatible_multirange_result_type(row, candidate),
        _ => None,
    }
}

fn resolve_anyelement_result_type(
    row: &crate::include::catalog::PgProcRow,
    candidate: &CandidateMatch,
) -> Option<SqlType> {
    let declared_oids = candidate_declared_arg_oids(row, candidate)?;
    let mut resolved = None;
    for (declared_oid, actual_type) in declared_oids
        .into_iter()
        .zip(candidate.declared_arg_types.iter().copied())
    {
        let inferred = match declared_oid {
            ANYOID | ANYELEMENTOID if is_unknown_null_resolution_type(actual_type) => None,
            _ if is_unknown_sql_type(actual_type) => None,
            ANYOID | ANYELEMENTOID => Some(actual_type),
            ANYENUMOID if matches!(actual_type.kind, SqlTypeKind::Enum) => Some(actual_type),
            ANYARRAYOID if actual_type.is_array => Some(actual_type.element_type()),
            ANYRANGEOID if actual_type.is_range() => {
                range_type_ref_for_sql_type(actual_type).map(|range_type| range_type.subtype)
            }
            ANYMULTIRANGEOID if actual_type.is_multirange() => {
                range_type_ref_for_multirange_sql_type(actual_type)
                    .map(|range_type| range_type.subtype)
            }
            _ => None,
        };
        if let Some(inferred) = inferred.map(canonical_polymorphic_type) {
            match resolved {
                None => resolved = Some(inferred),
                Some(existing) if existing == inferred => {}
                Some(_) => return None,
            }
        }
    }
    resolved
}

fn resolve_anyenum_result_type(
    row: &crate::include::catalog::PgProcRow,
    candidate: &CandidateMatch,
) -> Option<SqlType> {
    let declared_oids = candidate_declared_arg_oids(row, candidate)?;
    declared_oids
        .into_iter()
        .zip(candidate.declared_arg_types.iter().copied())
        .find_map(|(declared_oid, actual_type)| {
            (declared_oid == ANYENUMOID && matches!(actual_type.kind, SqlTypeKind::Enum))
                .then_some(actual_type)
        })
}

fn resolve_anyarray_result_type(
    row: &crate::include::catalog::PgProcRow,
    candidate: &CandidateMatch,
) -> Option<SqlType> {
    let declared_oids = candidate_declared_arg_oids(row, candidate)?;
    let mut resolved = None;
    for (declared_oid, actual_type) in declared_oids
        .into_iter()
        .zip(candidate.declared_arg_types.iter().copied())
    {
        let inferred = match declared_oid {
            _ if is_unknown_sql_type(actual_type) => None,
            ANYARRAYOID if actual_type.is_array => Some(actual_type),
            ANYENUMOID if matches!(actual_type.kind, SqlTypeKind::Enum) => {
                Some(SqlType::array_of(actual_type))
            }
            ANYOID | ANYELEMENTOID
                if !actual_type.is_array && actual_type.kind != SqlTypeKind::AnyArray =>
            {
                Some(SqlType::array_of(actual_type))
            }
            ANYRANGEOID if actual_type.is_range() => range_type_ref_for_sql_type(actual_type)
                .map(|range_type| SqlType::array_of(range_type.subtype)),
            ANYMULTIRANGEOID if actual_type.is_multirange() => {
                range_type_ref_for_multirange_sql_type(actual_type)
                    .map(|range_type| SqlType::array_of(range_type.subtype))
            }
            _ => None,
        };
        if let Some(inferred) = inferred {
            match resolved {
                None => resolved = Some(inferred),
                Some(existing) if existing == inferred => {}
                Some(_) => return None,
            }
        }
    }
    resolved
}

fn resolve_anyrange_result_type(
    row: &crate::include::catalog::PgProcRow,
    candidate: &CandidateMatch,
) -> Option<SqlType> {
    let declared_oids = candidate_declared_arg_oids(row, candidate)?;
    let mut resolved = None;
    for (declared_oid, actual_type) in declared_oids
        .into_iter()
        .zip(candidate.declared_arg_types.iter().copied())
    {
        let inferred = match declared_oid {
            ANYRANGEOID if actual_type.is_range() => Some(actual_type),
            ANYMULTIRANGEOID if actual_type.is_multirange() => {
                range_type_ref_for_multirange_sql_type(actual_type).map(|range_type| {
                    range_type
                        .sql_type
                        .with_identity(range_type.type_oid(), range_type.sql_type.typrelid)
                })
            }
            _ => None,
        };
        if let Some(inferred) = inferred {
            match resolved {
                None => resolved = Some(inferred),
                Some(existing) if existing == inferred => {}
                Some(_) => return None,
            }
        }
    }
    resolved
}

fn resolve_anymultirange_result_type(
    row: &crate::include::catalog::PgProcRow,
    candidate: &CandidateMatch,
) -> Option<SqlType> {
    let declared_oids = candidate_declared_arg_oids(row, candidate)?;
    let mut resolved = None;
    for (declared_oid, actual_type) in declared_oids
        .into_iter()
        .zip(candidate.declared_arg_types.iter().copied())
    {
        let inferred = match declared_oid {
            ANYMULTIRANGEOID if actual_type.is_multirange() => Some(actual_type),
            ANYRANGEOID if actual_type.is_range() => {
                let range_type = range_type_ref_for_sql_type(actual_type)?;
                let multirange_type = multirange_type_ref_for_sql_type(
                    SqlType::multirange(range_type.multirange_type_oid, range_type.type_oid())
                        .with_identity(range_type.multirange_type_oid, range_type.sql_type.typrelid)
                        .with_range_metadata(
                            range_type.subtype_oid(),
                            range_type.multirange_type_oid,
                            range_type.is_discrete(),
                        )
                        .with_multirange_range_oid(range_type.type_oid()),
                )?;
                Some(multirange_type.sql_type)
            }
            _ => None,
        };
        if let Some(inferred) = inferred {
            match resolved {
                None => resolved = Some(inferred),
                Some(existing) if existing == inferred => {}
                Some(_) => return None,
            }
        }
    }
    resolved
}

fn resolve_anycompatible_element_type(
    row: &crate::include::catalog::PgProcRow,
    candidate: &CandidateMatch,
) -> Option<SqlType> {
    let declared_oids = candidate_declared_arg_oids(row, candidate)?;
    let mut anchor = None;
    let mut loose = Vec::new();
    let mut saw_anycompatible_scalar = false;
    for (declared_oid, actual_type) in declared_oids
        .into_iter()
        .zip(candidate.declared_arg_types.iter().copied())
    {
        match declared_oid {
            ANYCOMPATIBLERANGEOID if actual_type.is_range() => {
                anchor = merge_exact_compatible_anchor(
                    anchor,
                    canonical_polymorphic_type(range_type_ref_for_sql_type(actual_type)?.subtype),
                )?;
            }
            ANYCOMPATIBLEMULTIRANGEOID if actual_type.is_multirange() => {
                anchor = merge_exact_compatible_anchor(
                    anchor,
                    canonical_polymorphic_type(
                        range_type_ref_for_multirange_sql_type(actual_type)?.subtype,
                    ),
                )?;
            }
            ANYCOMPATIBLEARRAYOID if actual_type.is_array => {
                if !is_unknown_sql_type(actual_type) {
                    loose.push(canonical_polymorphic_type(actual_type.element_type()));
                }
            }
            ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => {
                saw_anycompatible_scalar = true;
                if !is_unknown_sql_type(actual_type) {
                    loose.push(canonical_polymorphic_type(actual_type));
                }
            }
            _ => {}
        }
    }
    if let Some(anchor) = anchor {
        return loose
            .iter()
            .all(|ty| can_coerce_to_compatible_anchor(*ty, anchor))
            .then_some(anchor);
    }
    if loose.is_empty() && saw_anycompatible_scalar {
        return Some(SqlType::new(SqlTypeKind::Text));
    }
    loose
        .into_iter()
        .try_fold(None, merge_loose_compatible_type)?
}

fn resolve_anycompatible_range_result_type(
    row: &crate::include::catalog::PgProcRow,
    candidate: &CandidateMatch,
) -> Option<SqlType> {
    let declared_oids = candidate_declared_arg_oids(row, candidate)?;
    for (declared_oid, actual_type) in declared_oids
        .into_iter()
        .zip(candidate.declared_arg_types.iter().copied())
    {
        match declared_oid {
            ANYCOMPATIBLERANGEOID if actual_type.is_range() => return Some(actual_type),
            ANYCOMPATIBLEMULTIRANGEOID if actual_type.is_multirange() => {
                return range_type_ref_for_multirange_sql_type(actual_type)
                    .map(|range_type| range_type.sql_type);
            }
            _ => {}
        }
    }
    None
}

fn resolve_anycompatible_multirange_result_type(
    row: &crate::include::catalog::PgProcRow,
    candidate: &CandidateMatch,
) -> Option<SqlType> {
    let declared_oids = candidate_declared_arg_oids(row, candidate)?;
    for (declared_oid, actual_type) in declared_oids
        .into_iter()
        .zip(candidate.declared_arg_types.iter().copied())
    {
        match declared_oid {
            ANYCOMPATIBLEMULTIRANGEOID if actual_type.is_multirange() => return Some(actual_type),
            ANYCOMPATIBLERANGEOID if actual_type.is_range() => {
                let range_type = range_type_ref_for_sql_type(actual_type)?;
                return Some(
                    multirange_type_ref_for_sql_type(
                        SqlType::multirange(range_type.multirange_type_oid, range_type.type_oid())
                            .with_identity(
                                range_type.multirange_type_oid,
                                range_type.sql_type.typrelid,
                            )
                            .with_range_metadata(
                                range_type.subtype_oid(),
                                range_type.multirange_type_oid,
                                range_type.is_discrete(),
                            )
                            .with_multirange_range_oid(range_type.type_oid()),
                    )?
                    .sql_type,
                );
            }
            _ => {}
        }
    }
    None
}

fn merge_exact_compatible_anchor(
    existing: Option<SqlType>,
    next: SqlType,
) -> Option<Option<SqlType>> {
    match existing {
        None => Some(Some(next)),
        Some(existing) if existing == next => Some(Some(existing)),
        Some(_) => None,
    }
}

fn merge_loose_compatible_type(
    existing: Option<SqlType>,
    next: SqlType,
) -> Option<Option<SqlType>> {
    match existing {
        None => Some(Some(next)),
        Some(existing) if existing == next => Some(Some(existing)),
        Some(existing) if integer_common_supertype(existing, next).is_some() => {
            Some(integer_common_supertype(existing, next))
        }
        Some(existing)
            if matches!(
                existing.kind,
                SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8
            ) && next.kind == SqlTypeKind::Numeric =>
        {
            Some(Some(next))
        }
        Some(existing)
            if existing.kind == SqlTypeKind::Numeric
                && matches!(
                    next.kind,
                    SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8
                ) =>
        {
            Some(Some(existing))
        }
        Some(existing) if is_text_like_type(next) && !is_text_like_type(existing) => {
            Some(Some(existing))
        }
        Some(existing) if is_text_like_type(existing) && !is_text_like_type(next) => {
            Some(Some(next))
        }
        Some(_) => None,
    }
}

fn integer_common_supertype(left: SqlType, right: SqlType) -> Option<SqlType> {
    fn rank(ty: SqlType) -> Option<u8> {
        match ty.kind {
            SqlTypeKind::Int2 if !ty.is_array => Some(1),
            SqlTypeKind::Int4 if !ty.is_array => Some(2),
            SqlTypeKind::Int8 if !ty.is_array => Some(3),
            _ => None,
        }
    }
    match rank(left)?.max(rank(right)?) {
        1 => Some(SqlType::new(SqlTypeKind::Int2)),
        2 => Some(SqlType::new(SqlTypeKind::Int4)),
        3 => Some(SqlType::new(SqlTypeKind::Int8)),
        _ => None,
    }
}

fn can_coerce_to_compatible_anchor(value: SqlType, anchor: SqlType) -> bool {
    can_coerce_to_polymorphic_range_anchor(value, anchor)
}

fn catalog_implicit_cast_exists(
    catalog: &dyn CatalogLookup,
    actual_type: SqlType,
    declared_oid: u32,
) -> bool {
    let Some(source_oid) = catalog.type_oid_for_sql_type(actual_type) else {
        return false;
    };
    catalog
        .cast_by_source_target(source_oid, declared_oid)
        .is_some_and(|row| row.castcontext == 'i')
}

fn is_builtin_text_like_type(ty: SqlType) -> bool {
    !ty.is_array
        && matches!(
            ty.kind,
            SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar
        )
        && (ty.type_oid == 0 || builtin_type_name_for_oid(ty.type_oid).is_some())
}

fn canonical_polymorphic_type(mut ty: SqlType) -> SqlType {
    if !ty.is_array && !ty.is_range() && !ty.is_multirange() && ty.typrelid == 0 {
        ty.type_oid = 0;
    }
    ty
}

fn polymorphic_types_match(left: SqlType, right: SqlType) -> bool {
    left.kind == right.kind
        && left.is_array == right.is_array
        && (left.type_oid == 0 || right.type_oid == 0 || left.type_oid == right.type_oid)
        && (left.typrelid == 0 || right.typrelid == 0 || left.typrelid == right.typrelid)
}

fn match_variadic_element_type(
    catalog: &dyn CatalogLookup,
    actual_type: SqlType,
    variadic_oid: u32,
) -> Option<(usize, SqlType)> {
    if matches!(
        variadic_oid,
        ANYOID | ANYELEMENTOID | ANYCOMPATIBLEOID | ANYENUMOID
    ) {
        return Some((2, actual_type));
    }
    let declared_type = catalog.type_by_oid(variadic_oid)?.sql_type;
    arg_type_match_cost(actual_type, declared_type).map(|cost| (cost, declared_type))
}

fn match_explicit_variadic_arg(
    catalog: &dyn CatalogLookup,
    actual_type: SqlType,
    variadic_oid: u32,
) -> Option<(usize, SqlType)> {
    if matches!(variadic_oid, ANYOID | ANYELEMENTOID | ANYCOMPATIBLEOID) {
        return actual_type.is_array.then_some((2, actual_type));
    }
    if variadic_oid == ANYENUMOID {
        return (actual_type.is_array
            && matches!(actual_type.element_type().kind, SqlTypeKind::Enum))
        .then_some((2, actual_type));
    }
    if !actual_type.is_array {
        return None;
    }
    let element_type = catalog.type_by_oid(variadic_oid)?.sql_type;
    let target_type = SqlType::array_of(element_type);
    arg_type_match_cost(actual_type, target_type).map(|cost| (cost, target_type))
}

fn arg_type_match_cost(actual_type: SqlType, target_type: SqlType) -> Option<usize> {
    if actual_type == target_type {
        return Some(0);
    }
    if actual_type.is_array != target_type.is_array {
        return None;
    }
    if same_type_ignoring_function_typmod(actual_type, target_type) {
        return Some(0);
    }
    if !target_type.is_array
        && target_type.kind == SqlTypeKind::Record
        && matches!(
            actual_type.kind,
            SqlTypeKind::Record | SqlTypeKind::Composite
        )
    {
        return Some(1);
    }
    if !target_type.is_array
        && target_type.kind == SqlTypeKind::Composite
        && actual_type.kind == SqlTypeKind::Record
    {
        return Some(1);
    }
    if let Some(cost) = numeric_type_match_cost(actual_type.kind, target_type.kind) {
        return Some(cost);
    }
    if is_builtin_text_like_type(actual_type) && is_builtin_text_like_type(target_type) {
        return Some(1);
    }
    if is_bit_string_type(actual_type) && is_bit_string_type(target_type) {
        return Some(1);
    }
    if !target_type.is_array
        && matches!(target_type.kind, SqlTypeKind::Uuid)
        && is_text_like_type(actual_type)
    {
        return Some(2);
    }
    None
}

fn same_type_ignoring_function_typmod(actual: SqlType, target: SqlType) -> bool {
    if actual.is_array != target.is_array {
        return false;
    }
    if actual.is_array {
        return same_type_ignoring_function_typmod(actual.element_type(), target.element_type());
    }
    if actual.kind != target.kind || target.typmod != SqlType::NO_TYPEMOD {
        return false;
    }
    if actual.type_oid != 0 && target.type_oid != 0 && actual.type_oid != target.type_oid {
        return false;
    }
    if actual.typrelid != target.typrelid
        || actual.range_subtype_oid != target.range_subtype_oid
        || actual.range_multitype_oid != target.range_multitype_oid
        || actual.range_discrete != target.range_discrete
        || actual.multirange_range_oid != target.multirange_range_oid
    {
        return false;
    }
    matches!(
        actual.kind,
        SqlTypeKind::Numeric
            | SqlTypeKind::Char
            | SqlTypeKind::Varchar
            | SqlTypeKind::Bit
            | SqlTypeKind::VarBit
            | SqlTypeKind::Time
            | SqlTypeKind::TimeTz
            | SqlTypeKind::Timestamp
            | SqlTypeKind::TimestampTz
            | SqlTypeKind::Interval
    )
}

fn numeric_type_match_cost(actual: SqlTypeKind, target: SqlTypeKind) -> Option<usize> {
    use SqlTypeKind::*;
    Some(match (actual, target) {
        (Int2, Int4) | (Int4, Int8) | (Int8, Numeric) | (Float4, Float8) => 1,
        (Int2, Int8) | (Int4, Numeric) | (Int8, Float4) | (Numeric, Float4) => 2,
        (Int2, Numeric) | (Int4, Float4) | (Int8, Float8) | (Numeric, Float8) => 3,
        (Int2, Float4) | (Int4, Float8) => 4,
        (Int2, Float8) => 5,
        _ => return None,
    })
}

pub(super) fn validate_scalar_function_arity(
    func: BuiltinScalarFunction,
    args: &[SqlExpr],
) -> Result<(), ParseError> {
    let valid = scalar_function_arity_overrides()
        .iter()
        .find_map(|(candidate, arity)| (*candidate == func).then_some(arity))
        .map(|arity| match arity {
            ScalarFunctionArity::Exact(count) => args.len() == *count,
        })
        .unwrap_or_else(|| match func {
            BuiltinScalarFunction::ToTsVector
            | BuiltinScalarFunction::JsonToTsVector
            | BuiltinScalarFunction::JsonbToTsVector
            | BuiltinScalarFunction::ToTsQuery
            | BuiltinScalarFunction::PlainToTsQuery
            | BuiltinScalarFunction::PhraseToTsQuery
            | BuiltinScalarFunction::WebSearchToTsQuery => {
                if matches!(
                    func,
                    BuiltinScalarFunction::JsonToTsVector | BuiltinScalarFunction::JsonbToTsVector
                ) {
                    matches!(args.len(), 2 | 3)
                } else {
                    matches!(args.len(), 1 | 2)
                }
            }
            BuiltinScalarFunction::Int4Pl | BuiltinScalarFunction::Int4Mi => args.len() == 2,
            BuiltinScalarFunction::Int8Inc => args.len() == 1,
            BuiltinScalarFunction::Int8IncAny => args.len() == 2,
            BuiltinScalarFunction::Int4AvgAccum => args.len() == 2,
            BuiltinScalarFunction::Int8Avg => args.len() == 1,
            BuiltinScalarFunction::TsLexize => args.len() == 2,
            BuiltinScalarFunction::TsHeadline => matches!(args.len(), 2 | 3 | 4),
            BuiltinScalarFunction::TsQueryNot => args.len() == 1,
            BuiltinScalarFunction::TsVectorIn | BuiltinScalarFunction::TsQueryIn => {
                matches!(args.len(), 1 | 3)
            }
            BuiltinScalarFunction::TsQueryNumnode
            | BuiltinScalarFunction::TsVectorOut
            | BuiltinScalarFunction::TsQueryOut
            | BuiltinScalarFunction::TsVectorStrip
            | BuiltinScalarFunction::TsVectorToArray
            | BuiltinScalarFunction::ArrayToTsVector => args.len() == 1,
            BuiltinScalarFunction::TsQueryPhrase => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::TsVectorDelete
            | BuiltinScalarFunction::TsVectorFilter
            | BuiltinScalarFunction::TsRank
            | BuiltinScalarFunction::TsRankCd => matches!(args.len(), 2 | 3 | 4),
            BuiltinScalarFunction::TsRewrite => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::TsVectorSetWeight => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::TsMatch
            | BuiltinScalarFunction::TsQueryContains
            | BuiltinScalarFunction::TsQueryAnd
            | BuiltinScalarFunction::TsQueryOr
            | BuiltinScalarFunction::TsQueryContainedBy
            | BuiltinScalarFunction::TsVectorConcat
            | BuiltinScalarFunction::TextCat => args.len() == 2,
            BuiltinScalarFunction::CashLarger | BuiltinScalarFunction::CashSmaller => {
                args.len() == 2
            }
            BuiltinScalarFunction::CashWords => args.len() == 1,
            BuiltinScalarFunction::UnsupportedXmlFeature => true,
            BuiltinScalarFunction::XmlComment
            | BuiltinScalarFunction::XmlText
            | BuiltinScalarFunction::XmlIsWellFormed
            | BuiltinScalarFunction::XmlIsWellFormedDocument
            | BuiltinScalarFunction::XmlIsWellFormedContent => args.len() == 1,
            BuiltinScalarFunction::XPath | BuiltinScalarFunction::XPathExists => {
                matches!(args.len(), 2 | 3)
            }
            BuiltinScalarFunction::Pi => args.is_empty(),
            BuiltinScalarFunction::Sin => args.len() == 1,
            BuiltinScalarFunction::Random | BuiltinScalarFunction::RandomNormal => {
                matches!(args.len(), 0 | 2)
            }
            BuiltinScalarFunction::SetSeed => args.len() == 1,
            BuiltinScalarFunction::UuidIn
            | BuiltinScalarFunction::UuidOut
            | BuiltinScalarFunction::UuidRecv
            | BuiltinScalarFunction::UuidSend
            | BuiltinScalarFunction::UuidHash
            | BuiltinScalarFunction::UuidExtractVersion
            | BuiltinScalarFunction::UuidExtractTimestamp => args.len() == 1,
            BuiltinScalarFunction::UuidEq
            | BuiltinScalarFunction::UuidNe
            | BuiltinScalarFunction::UuidLt
            | BuiltinScalarFunction::UuidLe
            | BuiltinScalarFunction::UuidGt
            | BuiltinScalarFunction::UuidGe
            | BuiltinScalarFunction::UuidCmp
            | BuiltinScalarFunction::Xid8Cmp
            | BuiltinScalarFunction::UuidHashExtended => args.len() == 2,
            BuiltinScalarFunction::GenRandomUuid => args.is_empty(),
            BuiltinScalarFunction::UuidV7 => matches!(args.len(), 0 | 1),
            BuiltinScalarFunction::Now
            | BuiltinScalarFunction::TransactionTimestamp
            | BuiltinScalarFunction::StatementTimestamp
            | BuiltinScalarFunction::ClockTimestamp
            | BuiltinScalarFunction::TimeOfDay => args.is_empty(),
            BuiltinScalarFunction::PgSleep => args.len() == 1,
            BuiltinScalarFunction::Timezone => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::CurrentDatabase
            | BuiltinScalarFunction::Version
            | BuiltinScalarFunction::PgBackendPid
            | BuiltinScalarFunction::TxidCurrent
            | BuiltinScalarFunction::TxidCurrentIfAssigned
            | BuiltinScalarFunction::TxidCurrentSnapshot => args.is_empty(),
            BuiltinScalarFunction::CurrentSchemas => args.len() == 1,
            BuiltinScalarFunction::TxidSnapshotXmin | BuiltinScalarFunction::TxidSnapshotXmax => {
                args.len() == 1
            }
            BuiltinScalarFunction::TxidStatus => args.len() == 1,
            BuiltinScalarFunction::PgIdentifyObject
            | BuiltinScalarFunction::PgIdentifyObjectAsAddress
            | BuiltinScalarFunction::PgGetObjectAddress
            | BuiltinScalarFunction::PgDescribeObject => args.len() == 3,
            BuiltinScalarFunction::PgEventTriggerTableRewriteOid
            | BuiltinScalarFunction::PgEventTriggerTableRewriteReason
            | BuiltinScalarFunction::PgTriggerDepth => args.is_empty(),
            BuiltinScalarFunction::PgGetTriggerDef => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::PgPartitionRoot
            | BuiltinScalarFunction::PgGetPartKeyDef
            | BuiltinScalarFunction::PgTableIsVisible
            | BuiltinScalarFunction::PgTypeIsVisible
            | BuiltinScalarFunction::PgOperatorIsVisible
            | BuiltinScalarFunction::PgOpclassIsVisible
            | BuiltinScalarFunction::PgOpfamilyIsVisible
            | BuiltinScalarFunction::PgConversionIsVisible
            | BuiltinScalarFunction::PgTsParserIsVisible
            | BuiltinScalarFunction::PgTsDictIsVisible
            | BuiltinScalarFunction::PgTsTemplateIsVisible
            | BuiltinScalarFunction::PgTsConfigIsVisible => args.len() == 1,
            BuiltinScalarFunction::SatisfiesHashPartition => args.len() >= 3,
            BuiltinScalarFunction::DatePart | BuiltinScalarFunction::Extract => args.len() == 2,
            BuiltinScalarFunction::DateTrunc => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::DateBin => args.len() == 3,
            BuiltinScalarFunction::DateAdd | BuiltinScalarFunction::DateSubtract => {
                matches!(args.len(), 2 | 3)
            }
            BuiltinScalarFunction::Age => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::JustifyDays
            | BuiltinScalarFunction::JustifyHours
            | BuiltinScalarFunction::JustifyInterval
            | BuiltinScalarFunction::IsFinite => args.len() == 1,
            BuiltinScalarFunction::MakeInterval => args.len() <= 7,
            BuiltinScalarFunction::MakeDate | BuiltinScalarFunction::MakeTime => args.len() == 3,
            BuiltinScalarFunction::MakeTimestamp => args.len() == 6,
            BuiltinScalarFunction::MakeTimestampTz => matches!(args.len(), 6 | 7),
            BuiltinScalarFunction::TimestampTzConstructor => args.len() == 2,
            BuiltinScalarFunction::ToTimestamp => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::IntervalHash => args.len() == 1,
            BuiltinScalarFunction::HashValue(_) => args.len() == 1,
            BuiltinScalarFunction::HashValueExtended(_) => args.len() == 2,
            BuiltinScalarFunction::GetDatabaseEncoding => args.is_empty(),
            BuiltinScalarFunction::UnicodeVersion => args.is_empty(),
            BuiltinScalarFunction::UnicodeAssigned => args.len() == 1,
            BuiltinScalarFunction::Normalize | BuiltinScalarFunction::IsNormalized => {
                args.len() == 2
            }
            BuiltinScalarFunction::PgCharToEncoding | BuiltinScalarFunction::PgEncodingToChar => {
                args.len() == 1
            }
            BuiltinScalarFunction::PgMyTempSchema => args.is_empty(),
            BuiltinScalarFunction::PgRustInternalBinaryCoercible => args.len() == 2,
            BuiltinScalarFunction::PgRustDomainCheckUpperLessThan => args.len() == 3,
            BuiltinScalarFunction::PgRustTablesampleBernoulli => args.len() == 3,
            BuiltinScalarFunction::PgRustTestOpclassOptionsFunc => args.len() == 1,
            BuiltinScalarFunction::PgRustTestFdwHandler => args.is_empty(),
            BuiltinScalarFunction::PgRustTestEncSetup => args.is_empty(),
            BuiltinScalarFunction::PgRustTestEncConversion => args.len() == 4,
            BuiltinScalarFunction::PgRustIsCatalogTextUniqueIndexOid => args.len() == 1,
            BuiltinScalarFunction::PgRustTestWidgetIn
            | BuiltinScalarFunction::PgRustTestWidgetOut
            | BuiltinScalarFunction::PgRustTestInt44In
            | BuiltinScalarFunction::PgRustTestInt44Out => args.len() == 1,
            BuiltinScalarFunction::PgRustTestPtInWidget => args.len() == 2,
            BuiltinScalarFunction::CurrentSetting => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::SetConfig => args.len() == 3,
            BuiltinScalarFunction::PgSettingsGetFlags => args.len() == 1,
            BuiltinScalarFunction::AmValidate | BuiltinScalarFunction::BtEqualImage => {
                args.len() == 1
            }
            BuiltinScalarFunction::PgNotify => args.len() == 2,
            BuiltinScalarFunction::PgNotificationQueueUsage => args.is_empty(),
            BuiltinScalarFunction::PgTypeof
            | BuiltinScalarFunction::PgBaseType
            | BuiltinScalarFunction::PgColumnCompression
            | BuiltinScalarFunction::PgColumnToastChunkId
            | BuiltinScalarFunction::PgColumnSize => args.len() == 1,
            BuiltinScalarFunction::PgRelationSize => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::PgTableSize | BuiltinScalarFunction::PgTablespaceLocation => {
                args.len() == 1
            }
            BuiltinScalarFunction::PgRelationFilenode => args.len() == 1,
            BuiltinScalarFunction::PgFilenodeRelation => args.len() == 2,
            BuiltinScalarFunction::NumNulls | BuiltinScalarFunction::NumNonNulls => {
                !args.is_empty()
            }
            BuiltinScalarFunction::PgLogBackendMemoryContexts => args.len() == 1,
            BuiltinScalarFunction::HasFunctionPrivilege => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::HasTablePrivilege
            | BuiltinScalarFunction::HasSequencePrivilege
            | BuiltinScalarFunction::HasAnyColumnPrivilege
            | BuiltinScalarFunction::HasLargeObjectPrivilege
            | BuiltinScalarFunction::PgHasRole => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::RowSecurityActive => args.len() == 1,
            BuiltinScalarFunction::HasColumnPrivilege => matches!(args.len(), 3 | 4),
            BuiltinScalarFunction::PgCurrentLogfile => matches!(args.len(), 0 | 1),
            BuiltinScalarFunction::PgReadFile | BuiltinScalarFunction::PgReadBinaryFile => {
                matches!(args.len(), 1 | 2 | 3 | 4)
            }
            BuiltinScalarFunction::PgStatFile => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::PgWalfileName
            | BuiltinScalarFunction::PgWalfileNameOffset
            | BuiltinScalarFunction::PgSplitWalfileName
            | BuiltinScalarFunction::PgReplicationOriginCreate
            | BuiltinScalarFunction::GistTranslateCmpTypeCommon
            | BuiltinScalarFunction::TestCanonicalizePath => args.len() == 1,
            BuiltinScalarFunction::PgControlSystem
            | BuiltinScalarFunction::PgControlCheckpoint
            | BuiltinScalarFunction::PgControlRecovery
            | BuiltinScalarFunction::PgControlInit
            | BuiltinScalarFunction::TestRelpath => args.is_empty(),
            BuiltinScalarFunction::LastVal => args.is_empty(),
            BuiltinScalarFunction::NextVal | BuiltinScalarFunction::CurrVal => args.len() == 1,
            BuiltinScalarFunction::CurrTid2 => args.len() == 2,
            BuiltinScalarFunction::SetVal => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::PgGetSerialSequence => args.len() == 2,
            BuiltinScalarFunction::PgSequenceParameters
            | BuiltinScalarFunction::PgSequenceLastValue
            | BuiltinScalarFunction::PgGetSequenceData => args.len() == 1,
            BuiltinScalarFunction::PgGetAcl => args.len() == 3,
            BuiltinScalarFunction::MakeAclItem => args.len() == 4,
            BuiltinScalarFunction::PgGetUserById => args.len() == 1,
            BuiltinScalarFunction::ObjDescription => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::PgGetFunctionArguments
            | BuiltinScalarFunction::PgGetFunctionDef
            | BuiltinScalarFunction::PgGetFunctionResult
            | BuiltinScalarFunction::PgFunctionIsVisible => args.len() == 1,
            BuiltinScalarFunction::PgGetExpr => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::PgGetConstraintDef => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::PgGetPartitionConstraintDef => args.len() == 1,
            BuiltinScalarFunction::PgGetIndexDef => matches!(args.len(), 1 | 3),
            BuiltinScalarFunction::PgGetRuleDef => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::PgGetViewDef => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::PgGetStatisticsObjDef
            | BuiltinScalarFunction::PgGetStatisticsObjDefColumns
            | BuiltinScalarFunction::PgGetStatisticsObjDefExpressions
            | BuiltinScalarFunction::PgStatisticsObjIsVisible => args.len() == 1,
            BuiltinScalarFunction::PgRelationIsPublishable => args.len() == 1,
            BuiltinScalarFunction::PgIndexAmHasProperty => args.len() == 2,
            BuiltinScalarFunction::PgIndexHasProperty => args.len() == 2,
            BuiltinScalarFunction::PgIndexColumnHasProperty => args.len() == 3,
            BuiltinScalarFunction::PgSizePretty | BuiltinScalarFunction::PgSizeBytes => {
                args.len() == 1
            }
            BuiltinScalarFunction::PgAdvisoryUnlockAll => args.is_empty(),
            BuiltinScalarFunction::PgAdvisoryLock
            | BuiltinScalarFunction::PgAdvisoryXactLock
            | BuiltinScalarFunction::PgAdvisoryLockShared
            | BuiltinScalarFunction::PgAdvisoryXactLockShared
            | BuiltinScalarFunction::PgTryAdvisoryLock
            | BuiltinScalarFunction::PgTryAdvisoryXactLock
            | BuiltinScalarFunction::PgTryAdvisoryLockShared
            | BuiltinScalarFunction::PgTryAdvisoryXactLockShared
            | BuiltinScalarFunction::PgAdvisoryUnlock
            | BuiltinScalarFunction::PgAdvisoryUnlockShared => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::LoCreate | BuiltinScalarFunction::LoUnlink => args.len() == 1,
            BuiltinScalarFunction::PgStatGetCheckpointerNumTimed
            | BuiltinScalarFunction::PgStatGetCheckpointerNumRequested
            | BuiltinScalarFunction::PgStatGetCheckpointerNumPerformed
            | BuiltinScalarFunction::PgStatGetCheckpointerBuffersWritten
            | BuiltinScalarFunction::PgStatGetCheckpointerSlruWritten
            | BuiltinScalarFunction::PgStatGetCheckpointerWriteTime
            | BuiltinScalarFunction::PgStatGetCheckpointerSyncTime
            | BuiltinScalarFunction::PgStatGetCheckpointerStatResetTime
            | BuiltinScalarFunction::PgStatForceNextFlush
            | BuiltinScalarFunction::PgStatGetSnapshotTimestamp
            | BuiltinScalarFunction::PgStatClearSnapshot
            | BuiltinScalarFunction::PgStatReset => args.is_empty(),
            BuiltinScalarFunction::PgStatResetShared
            | BuiltinScalarFunction::PgStatResetReplicationSlot => args.len() == 1,
            BuiltinScalarFunction::PgStatResetSlru => matches!(args.len(), 0 | 1),
            BuiltinScalarFunction::PgStatResetSingleTableCounters
            | BuiltinScalarFunction::PgStatResetSingleFunctionCounters
            | BuiltinScalarFunction::PgStatResetBackendStats
            | BuiltinScalarFunction::PgStatGetBackendPid
            | BuiltinScalarFunction::PgStatGetBackendWal
            | BuiltinScalarFunction::PgStatGetReplicationSlot
            | BuiltinScalarFunction::PgStatGetSubscriptionStats
            | BuiltinScalarFunction::PgStatResetSubscriptionStats => args.len() == 1,
            BuiltinScalarFunction::ShobjDescription => args.len() == 2,
            BuiltinScalarFunction::PgStatHaveStats => args.len() == 3,
            BuiltinScalarFunction::PgStatGetNumscans
            | BuiltinScalarFunction::PgStatGetLastscan
            | BuiltinScalarFunction::PgStatGetTuplesReturned
            | BuiltinScalarFunction::PgStatGetTuplesFetched
            | BuiltinScalarFunction::PgStatGetTuplesInserted
            | BuiltinScalarFunction::PgStatGetTuplesUpdated
            | BuiltinScalarFunction::PgStatGetTuplesHotUpdated
            | BuiltinScalarFunction::PgStatGetTuplesDeleted
            | BuiltinScalarFunction::PgStatGetLiveTuples
            | BuiltinScalarFunction::PgStatGetDeadTuples
            | BuiltinScalarFunction::PgStatGetBlocksFetched
            | BuiltinScalarFunction::PgStatGetBlocksHit
            | BuiltinScalarFunction::PgStatGetXactNumscans
            | BuiltinScalarFunction::PgStatGetXactTuplesReturned
            | BuiltinScalarFunction::PgStatGetXactTuplesFetched
            | BuiltinScalarFunction::PgStatGetXactTuplesInserted
            | BuiltinScalarFunction::PgStatGetXactTuplesUpdated
            | BuiltinScalarFunction::PgStatGetXactTuplesDeleted
            | BuiltinScalarFunction::PgStatGetFunctionCalls
            | BuiltinScalarFunction::PgStatGetFunctionTotalTime
            | BuiltinScalarFunction::PgStatGetFunctionSelfTime
            | BuiltinScalarFunction::PgStatGetXactFunctionCalls
            | BuiltinScalarFunction::PgStatGetXactFunctionTotalTime
            | BuiltinScalarFunction::PgStatGetXactFunctionSelfTime => args.len() == 1,
            BuiltinScalarFunction::PgRestoreRelationStats
            | BuiltinScalarFunction::PgRestoreAttributeStats => true,
            BuiltinScalarFunction::PgClearRelationStats => args.len() == 2,
            BuiltinScalarFunction::PgClearAttributeStats => args.len() == 4,
            BuiltinScalarFunction::ParseIdent => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::ToJson | BuiltinScalarFunction::ToJsonb => args.len() == 1,
            BuiltinScalarFunction::ArrayLength
            | BuiltinScalarFunction::ArrayLower
            | BuiltinScalarFunction::ArrayUpper
            | BuiltinScalarFunction::Cardinality
            | BuiltinScalarFunction::ArrayNdims
            | BuiltinScalarFunction::ArrayDims => {
                args.len()
                    == if matches!(
                        func,
                        BuiltinScalarFunction::ArrayLength
                            | BuiltinScalarFunction::ArrayLower
                            | BuiltinScalarFunction::ArrayUpper
                    ) {
                        2
                    } else {
                        1
                    }
            }
            BuiltinScalarFunction::Concat => true,
            BuiltinScalarFunction::ConcatWs => !args.is_empty(),
            BuiltinScalarFunction::Format => !args.is_empty(),
            BuiltinScalarFunction::Abs
            | BuiltinScalarFunction::Log10
            | BuiltinScalarFunction::Length
            | BuiltinScalarFunction::OctetLength
            | BuiltinScalarFunction::BitLength
            | BuiltinScalarFunction::Lower
            | BuiltinScalarFunction::Upper
            | BuiltinScalarFunction::Unistr
            | BuiltinScalarFunction::Scale
            | BuiltinScalarFunction::MinScale
            | BuiltinScalarFunction::TrimScale
            | BuiltinScalarFunction::NumericInc
            | BuiltinScalarFunction::Factorial
            | BuiltinScalarFunction::PgLsn
            | BuiltinScalarFunction::Ceil
            | BuiltinScalarFunction::Ceiling
            | BuiltinScalarFunction::Floor
            | BuiltinScalarFunction::Sign
            | BuiltinScalarFunction::Sqrt
            | BuiltinScalarFunction::Cbrt
            | BuiltinScalarFunction::Exp
            | BuiltinScalarFunction::Ln
            | BuiltinScalarFunction::Sinh
            | BuiltinScalarFunction::Cosh
            | BuiltinScalarFunction::Tanh
            | BuiltinScalarFunction::Asinh
            | BuiltinScalarFunction::Acosh
            | BuiltinScalarFunction::Atanh
            | BuiltinScalarFunction::Sind
            | BuiltinScalarFunction::Cosd
            | BuiltinScalarFunction::Tand
            | BuiltinScalarFunction::Cotd
            | BuiltinScalarFunction::Asind
            | BuiltinScalarFunction::Acosd
            | BuiltinScalarFunction::Atand
            | BuiltinScalarFunction::Float4Send
            | BuiltinScalarFunction::Float8Send
            | BuiltinScalarFunction::Erf
            | BuiltinScalarFunction::Erfc
            | BuiltinScalarFunction::Gamma
            | BuiltinScalarFunction::Lgamma
            | BuiltinScalarFunction::Md5
            | BuiltinScalarFunction::ToBin
            | BuiltinScalarFunction::ToOct
            | BuiltinScalarFunction::ToHex
            | BuiltinScalarFunction::QuoteIdent
            | BuiltinScalarFunction::QuoteLiteral
            | BuiltinScalarFunction::QuoteNullable
            | BuiltinScalarFunction::BitcastIntegerToFloat4
            | BuiltinScalarFunction::BitcastBigintToFloat8
            | BuiltinScalarFunction::TextToRegClass
            | BuiltinScalarFunction::ToRegProc
            | BuiltinScalarFunction::ToRegProcedure
            | BuiltinScalarFunction::ToRegOper
            | BuiltinScalarFunction::ToRegOperator
            | BuiltinScalarFunction::ToRegClass
            | BuiltinScalarFunction::ToRegType
            | BuiltinScalarFunction::ToRegTypeMod
            | BuiltinScalarFunction::ToRegRole
            | BuiltinScalarFunction::ToRegNamespace
            | BuiltinScalarFunction::ToRegCollation
            | BuiltinScalarFunction::RegProcToText
            | BuiltinScalarFunction::RegClassToText
            | BuiltinScalarFunction::RegTypeToText
            | BuiltinScalarFunction::RegOperToText
            | BuiltinScalarFunction::RegOperatorToText
            | BuiltinScalarFunction::RegProcedureToText
            | BuiltinScalarFunction::RegCollationToText
            | BuiltinScalarFunction::RegRoleToText
            | BuiltinScalarFunction::BpcharToText
            | BuiltinScalarFunction::MacAddrNot
            | BuiltinScalarFunction::MacAddrTrunc
            | BuiltinScalarFunction::MacAddrToMacAddr8
            | BuiltinScalarFunction::MacAddr8Not
            | BuiltinScalarFunction::MacAddr8Trunc
            | BuiltinScalarFunction::MacAddr8ToMacAddr
            | BuiltinScalarFunction::MacAddr8Set7Bit
            | BuiltinScalarFunction::HashMacAddr
            | BuiltinScalarFunction::HashMacAddr8
            | BuiltinScalarFunction::NetworkHost
            | BuiltinScalarFunction::NetworkAbbrev
            | BuiltinScalarFunction::BitCount => args.len() == 1,
            BuiltinScalarFunction::Trunc | BuiltinScalarFunction::Round => {
                matches!(args.len(), 1 | 2)
            }
            BuiltinScalarFunction::Log => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::Power
            | BuiltinScalarFunction::Atan2d
            | BuiltinScalarFunction::FormatType
            | BuiltinScalarFunction::NetworkSetMasklen
            | BuiltinScalarFunction::NetworkSameFamily
            | BuiltinScalarFunction::NetworkMerge
            | BuiltinScalarFunction::NetworkSubnet
            | BuiltinScalarFunction::NetworkSubnetEq
            | BuiltinScalarFunction::NetworkSupernet
            | BuiltinScalarFunction::NetworkSupernetEq
            | BuiltinScalarFunction::NetworkOverlap
            | BuiltinScalarFunction::BoolEq
            | BuiltinScalarFunction::BoolNe
            | BuiltinScalarFunction::BoolAndStateFunc
            | BuiltinScalarFunction::BoolOrStateFunc
            | BuiltinScalarFunction::MacAddrEq
            | BuiltinScalarFunction::MacAddrNe
            | BuiltinScalarFunction::MacAddrLt
            | BuiltinScalarFunction::MacAddrLe
            | BuiltinScalarFunction::MacAddrGt
            | BuiltinScalarFunction::MacAddrGe
            | BuiltinScalarFunction::MacAddrCmp
            | BuiltinScalarFunction::MacAddrAnd
            | BuiltinScalarFunction::MacAddrOr
            | BuiltinScalarFunction::MacAddr8Eq
            | BuiltinScalarFunction::MacAddr8Ne
            | BuiltinScalarFunction::MacAddr8Lt
            | BuiltinScalarFunction::MacAddr8Le
            | BuiltinScalarFunction::MacAddr8Gt
            | BuiltinScalarFunction::MacAddr8Ge
            | BuiltinScalarFunction::MacAddr8Cmp
            | BuiltinScalarFunction::MacAddr8And
            | BuiltinScalarFunction::MacAddr8Or
            | BuiltinScalarFunction::HashMacAddrExtended
            | BuiltinScalarFunction::HashMacAddr8Extended
            | BuiltinScalarFunction::Div
            | BuiltinScalarFunction::Mod => args.len() == 2,
            BuiltinScalarFunction::HasForeignDataWrapperPrivilege
            | BuiltinScalarFunction::HasServerPrivilege => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::Float8Accum | BuiltinScalarFunction::Float8Combine => {
                args.len() == 2
            }
            BuiltinScalarFunction::Float8RegrAccum => args.len() == 3,
            BuiltinScalarFunction::Float8RegrCombine => args.len() == 2,
            BuiltinScalarFunction::WidthBucket => matches!(args.len(), 2 | 4),
            BuiltinScalarFunction::GetBit => args.len() == 2,
            BuiltinScalarFunction::SetBit => args.len() == 3,
            BuiltinScalarFunction::ArrayFill => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::ArrayIn => args.len() == 3,
            BuiltinScalarFunction::AnyRangeIn => args.len() == 3,
            BuiltinScalarFunction::ArrayLarger => args.len() == 2,
            BuiltinScalarFunction::StringToArray
            | BuiltinScalarFunction::ArrayToString
            | BuiltinScalarFunction::ArrayAppend
            | BuiltinScalarFunction::ArrayPrepend
            | BuiltinScalarFunction::ArrayCat
            | BuiltinScalarFunction::ArrayPosition
            | BuiltinScalarFunction::ArraySort => matches!(args.len(), 1 | 2 | 3),
            BuiltinScalarFunction::ArrayPositions | BuiltinScalarFunction::ArrayRemove => {
                args.len() == 2
            }
            BuiltinScalarFunction::ArrayReplace => args.len() == 3,
            BuiltinScalarFunction::TrimArray | BuiltinScalarFunction::ArraySample => {
                args.len() == 2
            }
            BuiltinScalarFunction::ArrayShuffle | BuiltinScalarFunction::ArrayReverse => {
                args.len() == 1
            }
            BuiltinScalarFunction::Gcd | BuiltinScalarFunction::Lcm => args.len() == 2,
            BuiltinScalarFunction::Greatest | BuiltinScalarFunction::Least => !args.is_empty(),
            BuiltinScalarFunction::BTrim
            | BuiltinScalarFunction::LTrim
            | BuiltinScalarFunction::RTrim => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::Initcap
            | BuiltinScalarFunction::Ascii
            | BuiltinScalarFunction::Chr
            | BuiltinScalarFunction::Reverse
            | BuiltinScalarFunction::Sha224
            | BuiltinScalarFunction::Sha256
            | BuiltinScalarFunction::Sha384
            | BuiltinScalarFunction::Sha512
            | BuiltinScalarFunction::Crc32
            | BuiltinScalarFunction::Crc32c => args.len() == 1,
            BuiltinScalarFunction::Position
            | BuiltinScalarFunction::Strpos
            | BuiltinScalarFunction::ConvertFrom
            | BuiltinScalarFunction::ConvertTo
            | BuiltinScalarFunction::Left
            | BuiltinScalarFunction::Right
            | BuiltinScalarFunction::Repeat
            | BuiltinScalarFunction::Encode
            | BuiltinScalarFunction::Decode
            | BuiltinScalarFunction::TextStartsWith
            | BuiltinScalarFunction::ToChar
            | BuiltinScalarFunction::ToDate
            | BuiltinScalarFunction::ToNumber
            | BuiltinScalarFunction::PgInputIsValid
            | BuiltinScalarFunction::PgInputErrorMessage
            | BuiltinScalarFunction::PgInputErrorDetail
            | BuiltinScalarFunction::PgInputErrorHint
            | BuiltinScalarFunction::PgInputErrorSqlState
            | BuiltinScalarFunction::TxidVisibleInSnapshot => args.len() == 2,
            BuiltinScalarFunction::RegexpLike => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::RegexpMatch => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::Replace
            | BuiltinScalarFunction::Translate
            | BuiltinScalarFunction::SplitPart
            | BuiltinScalarFunction::Convert => args.len() == 3,
            BuiltinScalarFunction::LPad | BuiltinScalarFunction::RPad => {
                matches!(args.len(), 2 | 3)
            }
            BuiltinScalarFunction::RegexpReplace => matches!(args.len(), 3..=6),
            BuiltinScalarFunction::RegexpCount => matches!(args.len(), 2..=4),
            BuiltinScalarFunction::RegexpInstr => matches!(args.len(), 2..=7),
            BuiltinScalarFunction::RegexpSubstr => matches!(args.len(), 2..=6),
            BuiltinScalarFunction::RegexpSplitToArray => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::Substring => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::SimilarSubstring => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::Overlay => matches!(args.len(), 3 | 4),
            BuiltinScalarFunction::GetByte => args.len() == 2,
            BuiltinScalarFunction::SetByte => args.len() == 3,
            BuiltinScalarFunction::ArrayToJson | BuiltinScalarFunction::RowToJson => {
                matches!(args.len(), 1 | 2)
            }
            BuiltinScalarFunction::SqlJsonConstructor
            | BuiltinScalarFunction::SqlJsonScalar
            | BuiltinScalarFunction::SqlJsonSerialize => args.len() == 1,
            BuiltinScalarFunction::SqlJsonObject | BuiltinScalarFunction::SqlJsonArray => {
                !args.is_empty()
            }
            BuiltinScalarFunction::SqlJsonIsJson => args.len() == 2,
            BuiltinScalarFunction::JsonBuildArray | BuiltinScalarFunction::JsonBuildObject => true,
            BuiltinScalarFunction::JsonObject => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::JsonPopulateRecord
            | BuiltinScalarFunction::JsonPopulateRecordValid => args.len() == 2,
            BuiltinScalarFunction::JsonToRecord => args.len() == 1,
            BuiltinScalarFunction::JsonStripNulls => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::JsonbObject => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::JsonbPopulateRecord
            | BuiltinScalarFunction::JsonbPopulateRecordValid => args.len() == 2,
            BuiltinScalarFunction::JsonbToRecord => args.len() == 1,
            BuiltinScalarFunction::JsonbStripNulls => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::JsonbPretty => args.len() == 1,
            BuiltinScalarFunction::JsonbContains
            | BuiltinScalarFunction::JsonbContained
            | BuiltinScalarFunction::JsonbExists
            | BuiltinScalarFunction::JsonbExistsAny
            | BuiltinScalarFunction::JsonbExistsAll => args.len() == 2,
            BuiltinScalarFunction::JsonbDelete => args.len() == 2,
            BuiltinScalarFunction::JsonbDeletePath => args.len() == 2,
            BuiltinScalarFunction::JsonbSet | BuiltinScalarFunction::JsonbInsert => {
                matches!(args.len(), 3 | 4)
            }
            BuiltinScalarFunction::JsonbSetLax => matches!(args.len(), 3..=5),
            BuiltinScalarFunction::JsonTypeof
            | BuiltinScalarFunction::JsonArrayLength
            | BuiltinScalarFunction::JsonbTypeof
            | BuiltinScalarFunction::JsonbArrayLength => args.len() == 1,
            BuiltinScalarFunction::JsonExtractPath
            | BuiltinScalarFunction::JsonExtractPathText
            | BuiltinScalarFunction::JsonbExtractPath
            | BuiltinScalarFunction::JsonbExtractPathText => !args.is_empty(),
            BuiltinScalarFunction::JsonbBuildArray | BuiltinScalarFunction::JsonbBuildObject => {
                true
            }
            BuiltinScalarFunction::JsonbConcat => args.len() == 2,
            BuiltinScalarFunction::JsonbPathExists
            | BuiltinScalarFunction::JsonbPathMatch
            | BuiltinScalarFunction::JsonbPathQueryArray
            | BuiltinScalarFunction::JsonbPathQueryFirst => matches!(args.len(), 2..=4),
            BuiltinScalarFunction::JsonExists
            | BuiltinScalarFunction::JsonValue
            | BuiltinScalarFunction::JsonQuery => args.len() == 2,
            BuiltinScalarFunction::GeoPoint => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::GeoBox => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::GeoLine => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::GeoLseg => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::GeoPath => args.len() == 1,
            BuiltinScalarFunction::GeoPolygon => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::GeoCircle => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::GeoArea
            | BuiltinScalarFunction::GeoCenter
            | BuiltinScalarFunction::GeoPolyCenter
            | BuiltinScalarFunction::GeoDiagonal
            | BuiltinScalarFunction::GeoLength
            | BuiltinScalarFunction::GeoRadius
            | BuiltinScalarFunction::GeoDiameter
            | BuiltinScalarFunction::GeoNpoints
            | BuiltinScalarFunction::GeoPclose
            | BuiltinScalarFunction::GeoPopen
            | BuiltinScalarFunction::GeoIsOpen
            | BuiltinScalarFunction::GeoIsClosed
            | BuiltinScalarFunction::GeoHeight
            | BuiltinScalarFunction::GeoWidth
            | BuiltinScalarFunction::GeoBoxHigh
            | BuiltinScalarFunction::GeoBoxLow
            | BuiltinScalarFunction::GeoPointX
            | BuiltinScalarFunction::GeoPointY => args.len() == 1,
            BuiltinScalarFunction::GeoBoundBox
            | BuiltinScalarFunction::GeoSlope
            | BuiltinScalarFunction::GeoEq
            | BuiltinScalarFunction::GeoNe
            | BuiltinScalarFunction::GeoLt
            | BuiltinScalarFunction::GeoLe
            | BuiltinScalarFunction::GeoGt
            | BuiltinScalarFunction::GeoGe
            | BuiltinScalarFunction::GeoSame
            | BuiltinScalarFunction::GeoDistance
            | BuiltinScalarFunction::GeoClosestPoint
            | BuiltinScalarFunction::GeoIntersection
            | BuiltinScalarFunction::GeoIntersects
            | BuiltinScalarFunction::GeoParallel
            | BuiltinScalarFunction::GeoPerpendicular
            | BuiltinScalarFunction::GeoContains
            | BuiltinScalarFunction::GeoContainedBy
            | BuiltinScalarFunction::GeoOverlap
            | BuiltinScalarFunction::GeoLeft
            | BuiltinScalarFunction::GeoOverLeft
            | BuiltinScalarFunction::GeoRight
            | BuiltinScalarFunction::GeoOverRight
            | BuiltinScalarFunction::GeoBelow
            | BuiltinScalarFunction::GeoOverBelow
            | BuiltinScalarFunction::GeoAbove
            | BuiltinScalarFunction::GeoOverAbove
            | BuiltinScalarFunction::GeoAdd
            | BuiltinScalarFunction::GeoSub
            | BuiltinScalarFunction::GeoMul
            | BuiltinScalarFunction::GeoDiv
            | BuiltinScalarFunction::GeoIsVertical
            | BuiltinScalarFunction::GeoIsHorizontal => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::RangeConstructor => matches!(args.len(), 0 | 2 | 3),
            BuiltinScalarFunction::RangeIsEmpty
            | BuiltinScalarFunction::NetworkBroadcast
            | BuiltinScalarFunction::NetworkNetwork
            | BuiltinScalarFunction::NetworkMasklen
            | BuiltinScalarFunction::NetworkFamily
            | BuiltinScalarFunction::NetworkNetmask
            | BuiltinScalarFunction::NetworkHostmask
            | BuiltinScalarFunction::RangeLower
            | BuiltinScalarFunction::RangeUpper
            | BuiltinScalarFunction::RangeLowerInc
            | BuiltinScalarFunction::RangeUpperInc
            | BuiltinScalarFunction::RangeLowerInf
            | BuiltinScalarFunction::RangeUpperInf => args.len() == 1,
            BuiltinScalarFunction::RangeContains
            | BuiltinScalarFunction::RangeContainedBy
            | BuiltinScalarFunction::RangeOverlap
            | BuiltinScalarFunction::RangeStrictLeft
            | BuiltinScalarFunction::RangeStrictRight
            | BuiltinScalarFunction::RangeOverLeft
            | BuiltinScalarFunction::RangeOverRight
            | BuiltinScalarFunction::RangeAdjacent
            | BuiltinScalarFunction::RangeUnion
            | BuiltinScalarFunction::RangeIntersect
            | BuiltinScalarFunction::RangeDifference
            | BuiltinScalarFunction::RangeMerge => args.len() == 2,
            BuiltinScalarFunction::EnumFirst | BuiltinScalarFunction::EnumLast => args.len() == 1,
            BuiltinScalarFunction::EnumRange => matches!(args.len(), 1 | 2),
        });

    if valid {
        Ok(())
    } else {
        Err(ParseError::UnexpectedToken {
            expected: "valid builtin function arity",
            actual: format!("{func:?}({} args)", args.len()),
        })
    }
}

pub(super) fn lower_named_scalar_function_args(
    func: BuiltinScalarFunction,
    args: &[SqlFunctionArg],
) -> Result<Vec<SqlExpr>, ParseError> {
    lower_named_function_args(
        scalar_named_arg_signature(func),
        args,
        "builtin scalar function",
    )
}

pub(super) fn lower_named_table_function_args(
    name: &str,
    args: &[SqlFunctionArg],
) -> Result<Vec<SqlExpr>, ParseError> {
    lower_named_function_args(
        table_function_named_arg_signature(name),
        args,
        "table function",
    )
}

pub(super) fn aggregate_args_are_named(args: &[SqlFunctionArg]) -> bool {
    args.iter().any(|arg| arg.name.is_some())
}

pub(super) fn reject_explicit_empty_aggregate_call(
    name: &str,
    args: &SqlCallArgs,
) -> Result<(), ParseError> {
    if name.eq_ignore_ascii_case("count")
        && matches!(args, SqlCallArgs::Args(args) if args.is_empty())
    {
        return Err(ParseError::DetailedError {
            message: "count(*) must be used to call a parameterless aggregate function".into(),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    }
    Ok(())
}

pub(super) fn validate_aggregate_arity(func: AggFunc, args: &[SqlExpr]) -> Result<(), ParseError> {
    let valid = aggregate_arity_overrides()
        .iter()
        .find_map(|(candidate, count)| (*candidate == func).then_some(*count))
        .map(|count| args.len() == count)
        .unwrap_or_else(|| match func {
            AggFunc::Count => args.len() <= 1,
            AggFunc::AnyValue
            | AggFunc::Sum
            | AggFunc::Avg
            | AggFunc::VarPop
            | AggFunc::VarSamp
            | AggFunc::StddevPop
            | AggFunc::StddevSamp
            | AggFunc::BoolAnd
            | AggFunc::BoolOr
            | AggFunc::BitAnd
            | AggFunc::BitOr
            | AggFunc::BitXor
            | AggFunc::Min
            | AggFunc::Max
            | AggFunc::ArrayAgg
            | AggFunc::JsonAgg
            | AggFunc::JsonbAgg
            | AggFunc::RangeAgg
            | AggFunc::XmlAgg
            | AggFunc::RangeIntersectAgg => args.len() == 1,
            AggFunc::RegrCount
            | AggFunc::RegrSxx
            | AggFunc::RegrSyy
            | AggFunc::RegrSxy
            | AggFunc::RegrAvgX
            | AggFunc::RegrAvgY
            | AggFunc::RegrR2
            | AggFunc::RegrSlope
            | AggFunc::RegrIntercept
            | AggFunc::CovarPop
            | AggFunc::CovarSamp
            | AggFunc::Corr => args.len() == 2,
            AggFunc::StringAgg
            | AggFunc::JsonObjectAgg
            | AggFunc::JsonObjectAggUnique
            | AggFunc::JsonObjectAggUniqueStrict
            | AggFunc::JsonbObjectAgg
            | AggFunc::JsonbObjectAggUnique
            | AggFunc::JsonbObjectAggUniqueStrict => args.len() == 2,
        });
    if valid {
        Ok(())
    } else {
        Err(ParseError::UnexpectedToken {
            expected: "valid aggregate arity",
            actual: format!("{}({} args)", func.name(), args.len()),
        })
    }
}

pub(super) fn comparison_operator_exists(
    catalog: &dyn CatalogLookup,
    op: &str,
    left: SqlType,
    right: SqlType,
) -> bool {
    let Some(left_oid) = catalog_builtin_type_oid(catalog, left) else {
        return false;
    };
    let Some(right_oid) = catalog_builtin_type_oid(catalog, right) else {
        return false;
    };
    catalog
        .operator_by_name_left_right(op, left_oid, right_oid)
        .is_some()
}

pub(super) fn fixed_scalar_return_type(func: BuiltinScalarFunction) -> Option<SqlType> {
    match func {
        BuiltinScalarFunction::TsMatch => return Some(SqlType::new(SqlTypeKind::Bool)),
        BuiltinScalarFunction::TsQueryContains | BuiltinScalarFunction::TsQueryContainedBy => {
            return Some(SqlType::new(SqlTypeKind::Bool));
        }
        BuiltinScalarFunction::ToTsVector
        | BuiltinScalarFunction::JsonToTsVector
        | BuiltinScalarFunction::JsonbToTsVector => {
            return Some(SqlType::new(SqlTypeKind::TsVector));
        }
        BuiltinScalarFunction::ToTsQuery
        | BuiltinScalarFunction::PlainToTsQuery
        | BuiltinScalarFunction::PhraseToTsQuery
        | BuiltinScalarFunction::WebSearchToTsQuery => {
            return Some(SqlType::new(SqlTypeKind::TsQuery));
        }
        BuiltinScalarFunction::TsLexize => {
            return Some(SqlType::array_of(SqlType::new(SqlTypeKind::Text)));
        }
        BuiltinScalarFunction::TsHeadline => {
            return Some(SqlType::new(SqlTypeKind::Text));
        }
        BuiltinScalarFunction::TsQueryAnd
        | BuiltinScalarFunction::TsQueryOr
        | BuiltinScalarFunction::TsQueryNot
        | BuiltinScalarFunction::TsQueryPhrase
        | BuiltinScalarFunction::TsRewrite => {
            return Some(SqlType::new(SqlTypeKind::TsQuery));
        }
        BuiltinScalarFunction::TsQueryNumnode => {
            return Some(SqlType::new(SqlTypeKind::Int4));
        }
        BuiltinScalarFunction::TsVectorIn => {
            return Some(SqlType::new(SqlTypeKind::TsVector));
        }
        BuiltinScalarFunction::TsVectorOut | BuiltinScalarFunction::TsQueryOut => {
            return Some(SqlType::new(SqlTypeKind::Text));
        }
        BuiltinScalarFunction::TsQueryIn => {
            return Some(SqlType::new(SqlTypeKind::TsQuery));
        }
        BuiltinScalarFunction::JsonExists => {
            return Some(SqlType::new(SqlTypeKind::Bool));
        }
        BuiltinScalarFunction::JsonValue => {
            return Some(SqlType::new(SqlTypeKind::Text));
        }
        BuiltinScalarFunction::JsonQuery => {
            return Some(SqlType::new(SqlTypeKind::Jsonb));
        }
        BuiltinScalarFunction::TsVectorConcat => {
            return Some(SqlType::new(SqlTypeKind::TsVector));
        }
        BuiltinScalarFunction::TsVectorStrip
        | BuiltinScalarFunction::TsVectorDelete
        | BuiltinScalarFunction::ArrayToTsVector
        | BuiltinScalarFunction::TsVectorSetWeight
        | BuiltinScalarFunction::TsVectorFilter => {
            return Some(SqlType::new(SqlTypeKind::TsVector));
        }
        BuiltinScalarFunction::TsVectorToArray => {
            return Some(SqlType::array_of(SqlType::new(SqlTypeKind::Text)));
        }
        BuiltinScalarFunction::TsRank | BuiltinScalarFunction::TsRankCd => {
            return Some(SqlType::new(SqlTypeKind::Float4));
        }
        BuiltinScalarFunction::CurrentSetting | BuiltinScalarFunction::SetConfig => {
            return Some(SqlType::new(SqlTypeKind::Text));
        }
        BuiltinScalarFunction::CurrentSchemas => {
            return Some(SqlType::array_of(SqlType::new(SqlTypeKind::Name)));
        }
        BuiltinScalarFunction::Extract => {
            return Some(SqlType::new(SqlTypeKind::Numeric));
        }
        BuiltinScalarFunction::MakeInterval => {
            return Some(SqlType::new(SqlTypeKind::Interval));
        }
        BuiltinScalarFunction::MakeTime => {
            return Some(SqlType::new(SqlTypeKind::Time));
        }
        BuiltinScalarFunction::MakeTimestamp => {
            return Some(SqlType::new(SqlTypeKind::Timestamp));
        }
        BuiltinScalarFunction::Age => {
            return Some(SqlType::new(SqlTypeKind::Interval));
        }
        BuiltinScalarFunction::IntervalHash => {
            return Some(SqlType::new(SqlTypeKind::Int4));
        }
        BuiltinScalarFunction::HashValue(_) => {
            return Some(SqlType::new(SqlTypeKind::Int4));
        }
        BuiltinScalarFunction::HashValueExtended(_) => {
            return Some(SqlType::new(SqlTypeKind::Int8));
        }
        BuiltinScalarFunction::SatisfiesHashPartition => {
            return Some(SqlType::new(SqlTypeKind::Bool));
        }
        BuiltinScalarFunction::TxidCurrent | BuiltinScalarFunction::TxidCurrentIfAssigned => {
            return Some(SqlType::new(SqlTypeKind::Int8));
        }
        BuiltinScalarFunction::TxidCurrentSnapshot => {
            return Some(SqlType::new(SqlTypeKind::Text));
        }
        BuiltinScalarFunction::TxidSnapshotXmin | BuiltinScalarFunction::TxidSnapshotXmax => {
            return Some(SqlType::new(SqlTypeKind::Int8));
        }
        BuiltinScalarFunction::TxidVisibleInSnapshot => {
            return Some(SqlType::new(SqlTypeKind::Bool));
        }
        BuiltinScalarFunction::TxidStatus => {
            return Some(SqlType::new(SqlTypeKind::Text));
        }
        BuiltinScalarFunction::TextStartsWith => {
            return Some(SqlType::new(SqlTypeKind::Bool));
        }
        BuiltinScalarFunction::ParseIdent => {
            return Some(SqlType::array_of(SqlType::new(SqlTypeKind::Text)));
        }
        BuiltinScalarFunction::XPath => {
            return Some(SqlType::array_of(SqlType::new(SqlTypeKind::Xml)));
        }
        BuiltinScalarFunction::XPathExists => {
            return Some(SqlType::new(SqlTypeKind::Bool));
        }
        _ => {}
    }
    scalar_fixed_return_types()
        .iter()
        .find_map(|(candidate, sql_type)| (*candidate == func).then_some(*sql_type))
}

pub(super) fn fixed_aggregate_return_type(func: AggFunc) -> Option<SqlType> {
    aggregate_fixed_return_types()
        .iter()
        .find_map(|(candidate, sql_type)| (*candidate == func).then_some(*sql_type))
}

fn scalar_functions_by_name() -> &'static BTreeMap<String, BuiltinScalarFunction> {
    static FUNCTIONS: OnceLock<BTreeMap<String, BuiltinScalarFunction>> = OnceLock::new();
    FUNCTIONS.get_or_init(|| {
        let mut by_name = BTreeMap::new();
        for row in bootstrap_pg_proc_rows_ref() {
            if row.prokind != 'f' || row.proretset {
                continue;
            }
            if let Some(func) = builtin_scalar_function_for_proc_row(&row) {
                by_name
                    .entry(row.proname.to_ascii_lowercase())
                    .or_insert(func);
            }
        }
        for (name, func) in legacy_scalar_function_entries() {
            by_name.insert((*name).into(), *func);
        }
        by_name.remove("num_nulls");
        by_name.remove("num_nonnulls");
        by_name.remove("pg_num_nulls");
        by_name.remove("pg_num_nonnulls");
        by_name
    })
}

fn lower_named_function_args(
    signature: Option<NamedArgSignature>,
    args: &[SqlFunctionArg],
    context: &'static str,
) -> Result<Vec<SqlExpr>, ParseError> {
    let has_named = args.iter().any(|arg| arg.name.is_some());
    if !has_named {
        return Ok(args.iter().map(|arg| arg.value.clone()).collect());
    }

    let Some(signature) = signature else {
        return Err(ParseError::UnexpectedToken {
            expected: "function supporting named arguments",
            actual: context.into(),
        });
    };

    let mut saw_named = false;
    let mut positional_count = 0usize;
    for arg in args {
        if arg.name.is_some() {
            saw_named = true;
        } else if saw_named {
            return Err(ParseError::UnexpectedToken {
                expected: "named arguments after positional arguments",
                actual: "positional argument cannot follow named argument".into(),
            });
        } else {
            positional_count += 1;
        }
    }

    if positional_count > signature.params.len() {
        return Err(ParseError::UnexpectedToken {
            expected: "valid builtin function arity",
            actual: format!("function call with {} args", args.len()),
        });
    }

    let mut lowered: Vec<Option<SqlExpr>> = vec![None; signature.params.len()];
    for (idx, arg) in args.iter().take(positional_count).enumerate() {
        lowered[idx] = Some(arg.value.clone());
    }

    let mut param_lookup = BTreeMap::new();
    for (idx, name) in signature.params.iter().enumerate() {
        param_lookup.insert((*name).to_ascii_lowercase(), idx);
    }

    for arg in args.iter().skip(positional_count) {
        let arg_name = arg.name.as_ref().expect("named arg");
        let Some(&idx) = param_lookup.get(&arg_name.to_ascii_lowercase()) else {
            return Err(ParseError::UnexpectedToken {
                expected: "known named function argument",
                actual: arg_name.clone(),
            });
        };
        if lowered[idx].is_some() {
            return Err(ParseError::UnexpectedToken {
                expected: "argument assigned once",
                actual: arg_name.clone(),
            });
        }
        lowered[idx] = Some(arg.value.clone());
    }

    for (idx, slot) in lowered.iter_mut().enumerate() {
        if slot.is_none() {
            *slot = signature
                .defaults
                .get(idx)
                .and_then(|default| *default)
                .map(default_sql_expr);
        }
    }

    if lowered
        .iter()
        .take(signature.required)
        .any(|slot| slot.is_none())
    {
        return Err(ParseError::UnexpectedToken {
            expected: "all required function arguments",
            actual: "missing required named argument".into(),
        });
    }

    Ok(lowered.into_iter().flatten().collect::<Vec<_>>())
}

fn default_sql_expr(default: NamedArgDefault) -> SqlExpr {
    match default {
        NamedArgDefault::Bool(value) => SqlExpr::Const(Value::Bool(value)),
        NamedArgDefault::Int4(value) => SqlExpr::Const(Value::Int32(value)),
        NamedArgDefault::Float8(value) => SqlExpr::Const(Value::Float64(value)),
        NamedArgDefault::Text(value) => SqlExpr::Const(Value::Text(value.into())),
        NamedArgDefault::JsonbEmptyObject => SqlExpr::Cast(
            Box::new(SqlExpr::Const(Value::Text("{}".into()))),
            RawTypeName::Builtin(SqlType::new(SqlTypeKind::Jsonb)),
        ),
    }
}

fn scalar_named_arg_signature(func: BuiltinScalarFunction) -> Option<NamedArgSignature> {
    match func {
        BuiltinScalarFunction::MakeInterval => Some(NamedArgSignature {
            params: &["years", "months", "weeks", "days", "hours", "mins", "secs"],
            required: 0,
            defaults: &[
                Some(NamedArgDefault::Int4(0)),
                Some(NamedArgDefault::Int4(0)),
                Some(NamedArgDefault::Int4(0)),
                Some(NamedArgDefault::Int4(0)),
                Some(NamedArgDefault::Int4(0)),
                Some(NamedArgDefault::Int4(0)),
                Some(NamedArgDefault::Float8(0.0)),
            ],
        }),
        BuiltinScalarFunction::ParseIdent => Some(NamedArgSignature {
            params: &["str", "strict"],
            required: 1,
            defaults: &[None, Some(NamedArgDefault::Bool(true))],
        }),
        BuiltinScalarFunction::RandomNormal => Some(NamedArgSignature {
            params: &["mean", "stddev"],
            required: 0,
            defaults: &[
                Some(NamedArgDefault::Float8(0.0)),
                Some(NamedArgDefault::Float8(1.0)),
            ],
        }),
        BuiltinScalarFunction::JsonbPathExists
        | BuiltinScalarFunction::JsonbPathMatch
        | BuiltinScalarFunction::JsonbPathQueryArray
        | BuiltinScalarFunction::JsonbPathQueryFirst => Some(NamedArgSignature {
            params: &["target", "path", "vars", "silent"],
            required: 2,
            defaults: &[
                None,
                None,
                Some(NamedArgDefault::JsonbEmptyObject),
                Some(NamedArgDefault::Bool(false)),
            ],
        }),
        BuiltinScalarFunction::JsonbSetLax => Some(NamedArgSignature {
            params: &[
                "target",
                "path",
                "new_value",
                "create_if_missing",
                "null_value_treatment",
            ],
            required: 3,
            defaults: &[
                None,
                None,
                None,
                Some(NamedArgDefault::Bool(true)),
                Some(NamedArgDefault::Text("use_json_null")),
            ],
        }),
        BuiltinScalarFunction::PgClearRelationStats => Some(NamedArgSignature {
            params: &["schemaname", "relname"],
            required: 2,
            defaults: &[None, None],
        }),
        BuiltinScalarFunction::PgClearAttributeStats => Some(NamedArgSignature {
            params: &["schemaname", "relname", "attname", "inherited"],
            required: 4,
            defaults: &[None, None, None, None],
        }),
        _ => None,
    }
}

fn table_function_named_arg_signature(name: &str) -> Option<NamedArgSignature> {
    let name = normalize_builtin_function_name(name);
    if name.eq_ignore_ascii_case("parse_ident") {
        return Some(NamedArgSignature {
            params: &["str", "strict"],
            required: 1,
            defaults: &[None, Some(NamedArgDefault::Bool(true))],
        });
    }
    if name.eq_ignore_ascii_case("generate_series") {
        return Some(NamedArgSignature {
            params: &["start", "stop", "step"],
            required: 2,
            defaults: &[None, None, None],
        });
    }
    if matches!(
        name.to_ascii_lowercase().as_str(),
        "json_each"
            | "json_each_text"
            | "json_object_keys"
            | "json_array_elements"
            | "json_array_elements_text"
            | "jsonb_each"
            | "jsonb_each_text"
            | "jsonb_object_keys"
            | "jsonb_array_elements"
            | "jsonb_array_elements_text"
    ) {
        return Some(NamedArgSignature {
            params: &["from_json"],
            required: 1,
            defaults: &[None],
        });
    }
    if name.eq_ignore_ascii_case("jsonb_path_query") {
        return Some(NamedArgSignature {
            params: &["target", "path", "vars", "silent"],
            required: 2,
            defaults: &[
                None,
                None,
                Some(NamedArgDefault::JsonbEmptyObject),
                Some(NamedArgDefault::Bool(false)),
            ],
        });
    }
    None
}

fn builtin_scalar_function_for_proc_src(proc_src: &str) -> Option<BuiltinScalarFunction> {
    hash_scalar_function_for_proc_src(proc_src).or_else(|| {
        legacy_scalar_function_entries()
            .iter()
            .find_map(|(name, func)| proc_src.eq_ignore_ascii_case(name).then_some(*func))
            .or_else(|| {
                range_prefixed_proc_src(proc_src).and_then(builtin_scalar_function_for_proc_src)
            })
            .or_else(|| {
                proc_src
                    .rsplit_once('_')
                    .filter(|(_, suffix)| suffix.chars().all(|ch| ch.is_ascii_digit()))
                    .and_then(|(base, _)| builtin_scalar_function_for_proc_src(base))
            })
    })
}

fn hash_scalar_function_for_proc_src(proc_src: &str) -> Option<BuiltinScalarFunction> {
    let normalized = proc_src.to_ascii_lowercase();
    let (base, extended) = normalized
        .strip_suffix("_extended")
        .map(|base| (base, true))
        .or_else(|| normalized.strip_suffix("extended").map(|base| (base, true)))
        .unwrap_or((normalized.as_str(), false));
    let kind = match base {
        "hashbool" => HashFunctionKind::Bool,
        "hashint2" => HashFunctionKind::Int2,
        "hashint4" => HashFunctionKind::Int4,
        "hashint8" => HashFunctionKind::Int8,
        "hashoid" => HashFunctionKind::Oid,
        "hashchar" => HashFunctionKind::InternalChar,
        "hashname" => HashFunctionKind::Name,
        "hashtext" => HashFunctionKind::Text,
        "hashvarchar" => HashFunctionKind::Varchar,
        "hashbpchar" => HashFunctionKind::BpChar,
        "hashfloat4" => HashFunctionKind::Float4,
        "hashfloat8" => HashFunctionKind::Float8,
        "hash_numeric" => HashFunctionKind::Numeric,
        "hashtimestamp" | "timestamp_hash" => HashFunctionKind::Timestamp,
        "hashtimestamptz" | "timestamptz_hash" => HashFunctionKind::TimestampTz,
        "hashdate" => HashFunctionKind::Date,
        "hashtime" | "time_hash" => HashFunctionKind::Time,
        "hashtimetz" | "timetz_hash" => HashFunctionKind::TimeTz,
        "hashbytea" => HashFunctionKind::Bytea,
        "hashoidvector" => HashFunctionKind::OidVector,
        "hash_aclitem" => HashFunctionKind::AclItem,
        "hashinet" => HashFunctionKind::Inet,
        "hashmacaddr" => HashFunctionKind::MacAddr,
        "hashmacaddr8" => HashFunctionKind::MacAddr8,
        "hash_array" => HashFunctionKind::Array,
        "interval_hash" => HashFunctionKind::Interval,
        "uuid_hash" => HashFunctionKind::Uuid,
        "pg_lsn_hash" => HashFunctionKind::PgLsn,
        "hashenum" => HashFunctionKind::Enum,
        "jsonb_hash" => HashFunctionKind::Jsonb,
        "hash_range" => HashFunctionKind::Range,
        "hash_multirange" => HashFunctionKind::Multirange,
        "hash_record" => HashFunctionKind::Record,
        _ => return None,
    };
    Some(if extended {
        BuiltinScalarFunction::HashValueExtended(kind)
    } else {
        BuiltinScalarFunction::HashValue(kind)
    })
}

fn range_prefixed_proc_src(proc_src: &str) -> Option<&str> {
    let stripped = [
        "int4range_",
        "int8range_",
        "numrange_",
        "daterange_",
        "tsrange_",
        "tstzrange_",
        "arrayrange_",
        "varbitrange_",
    ]
    .into_iter()
    .find_map(|prefix| proc_src.strip_prefix(prefix))?;

    [
        "range_constructor2",
        "range_constructor3",
        "range_isempty",
        "range_lower_inc",
        "range_upper_inc",
        "range_lower_inf",
        "range_upper_inf",
        "range_lower",
        "range_upper",
        "range_merge",
        "range_adjacent",
        "range_difference",
        "range_contains",
        "range_contained_by",
        "range_strict_left",
        "range_over_left",
        "range_strict_right",
        "range_over_right",
        "range_overlap",
        "range_union",
        "range_intersect",
    ]
    .into_iter()
    .find(|base| stripped == *base || stripped.starts_with(&format!("{base}_")))
}

fn legacy_scalar_function_entries() -> &'static [(&'static str, BuiltinScalarFunction)] {
    &[
        ("random", BuiltinScalarFunction::Random),
        ("drandom", BuiltinScalarFunction::Random),
        ("int4random", BuiltinScalarFunction::Random),
        ("int8random", BuiltinScalarFunction::Random),
        ("numeric_random", BuiltinScalarFunction::Random),
        ("random_normal", BuiltinScalarFunction::RandomNormal),
        ("drandom_normal", BuiltinScalarFunction::RandomNormal),
        ("drandom_normal_noargs", BuiltinScalarFunction::RandomNormal),
        ("setseed", BuiltinScalarFunction::SetSeed),
        ("pi", BuiltinScalarFunction::Pi),
        ("dpi", BuiltinScalarFunction::Pi),
        ("sin", BuiltinScalarFunction::Sin),
        ("dsin", BuiltinScalarFunction::Sin),
        ("uuid_in", BuiltinScalarFunction::UuidIn),
        ("uuid_out", BuiltinScalarFunction::UuidOut),
        ("uuid_recv", BuiltinScalarFunction::UuidRecv),
        ("uuid_send", BuiltinScalarFunction::UuidSend),
        ("uuid_eq", BuiltinScalarFunction::UuidEq),
        ("uuid_ne", BuiltinScalarFunction::UuidNe),
        ("uuid_lt", BuiltinScalarFunction::UuidLt),
        ("uuid_le", BuiltinScalarFunction::UuidLe),
        ("uuid_gt", BuiltinScalarFunction::UuidGt),
        ("uuid_ge", BuiltinScalarFunction::UuidGe),
        ("uuid_cmp", BuiltinScalarFunction::UuidCmp),
        ("xid8cmp", BuiltinScalarFunction::Xid8Cmp),
        ("uuid_hash", BuiltinScalarFunction::UuidHash),
        (
            "uuid_hash_extended",
            BuiltinScalarFunction::UuidHashExtended,
        ),
        ("gen_random_uuid", BuiltinScalarFunction::GenRandomUuid),
        ("uuidv7", BuiltinScalarFunction::UuidV7),
        ("uuidv7_interval", BuiltinScalarFunction::UuidV7),
        (
            "uuid_extract_version",
            BuiltinScalarFunction::UuidExtractVersion,
        ),
        (
            "uuid_extract_timestamp",
            BuiltinScalarFunction::UuidExtractTimestamp,
        ),
        ("current_database", BuiltinScalarFunction::CurrentDatabase),
        ("current_schemas", BuiltinScalarFunction::CurrentSchemas),
        ("version", BuiltinScalarFunction::Version),
        ("pgsql_version", BuiltinScalarFunction::Version),
        ("pg_backend_pid", BuiltinScalarFunction::PgBackendPid),
        ("txid_current", BuiltinScalarFunction::TxidCurrent),
        ("pg_current_xact_id", BuiltinScalarFunction::TxidCurrent),
        (
            "txid_current_if_assigned",
            BuiltinScalarFunction::TxidCurrentIfAssigned,
        ),
        (
            "pg_current_xact_id_if_assigned",
            BuiltinScalarFunction::TxidCurrentIfAssigned,
        ),
        (
            "txid_current_snapshot",
            BuiltinScalarFunction::TxidCurrentSnapshot,
        ),
        (
            "pg_current_snapshot",
            BuiltinScalarFunction::TxidCurrentSnapshot,
        ),
        (
            "txid_snapshot_xmin",
            BuiltinScalarFunction::TxidSnapshotXmin,
        ),
        ("pg_snapshot_xmin", BuiltinScalarFunction::TxidSnapshotXmin),
        (
            "txid_snapshot_xmax",
            BuiltinScalarFunction::TxidSnapshotXmax,
        ),
        ("pg_snapshot_xmax", BuiltinScalarFunction::TxidSnapshotXmax),
        (
            "txid_visible_in_snapshot",
            BuiltinScalarFunction::TxidVisibleInSnapshot,
        ),
        (
            "pg_visible_in_snapshot",
            BuiltinScalarFunction::TxidVisibleInSnapshot,
        ),
        ("txid_status", BuiltinScalarFunction::TxidStatus),
        ("pg_xact_status", BuiltinScalarFunction::TxidStatus),
        ("cashlarger", BuiltinScalarFunction::CashLarger),
        ("cashsmaller", BuiltinScalarFunction::CashSmaller),
        ("cash_words", BuiltinScalarFunction::CashWords),
        ("table_to_xml", BuiltinScalarFunction::UnsupportedXmlFeature),
        (
            "table_to_xmlschema",
            BuiltinScalarFunction::UnsupportedXmlFeature,
        ),
        (
            "table_to_xml_and_xmlschema",
            BuiltinScalarFunction::UnsupportedXmlFeature,
        ),
        ("query_to_xml", BuiltinScalarFunction::UnsupportedXmlFeature),
        (
            "query_to_xmlschema",
            BuiltinScalarFunction::UnsupportedXmlFeature,
        ),
        (
            "query_to_xml_and_xmlschema",
            BuiltinScalarFunction::UnsupportedXmlFeature,
        ),
        (
            "cursor_to_xml",
            BuiltinScalarFunction::UnsupportedXmlFeature,
        ),
        (
            "cursor_to_xmlschema",
            BuiltinScalarFunction::UnsupportedXmlFeature,
        ),
        (
            "schema_to_xml",
            BuiltinScalarFunction::UnsupportedXmlFeature,
        ),
        (
            "schema_to_xmlschema",
            BuiltinScalarFunction::UnsupportedXmlFeature,
        ),
        (
            "schema_to_xml_and_xmlschema",
            BuiltinScalarFunction::UnsupportedXmlFeature,
        ),
        (
            "pg_get_constraintdef",
            BuiltinScalarFunction::PgGetConstraintDef,
        ),
        (
            "pg_get_constraintdef_ext",
            BuiltinScalarFunction::PgGetConstraintDef,
        ),
        (
            "pg_get_partition_constraintdef",
            BuiltinScalarFunction::PgGetPartitionConstraintDef,
        ),
        ("pg_get_indexdef", BuiltinScalarFunction::PgGetIndexDef),
        ("pg_get_indexdef_ext", BuiltinScalarFunction::PgGetIndexDef),
        ("pg_get_partkeydef", BuiltinScalarFunction::PgGetPartKeyDef),
        ("pg_get_triggerdef", BuiltinScalarFunction::PgGetTriggerDef),
        ("pg_trigger_depth", BuiltinScalarFunction::PgTriggerDepth),
        ("now", BuiltinScalarFunction::Now),
        (
            "transaction_timestamp",
            BuiltinScalarFunction::TransactionTimestamp,
        ),
        (
            "statement_timestamp",
            BuiltinScalarFunction::StatementTimestamp,
        ),
        ("clock_timestamp", BuiltinScalarFunction::ClockTimestamp),
        ("timeofday", BuiltinScalarFunction::TimeOfDay),
        ("pg_sleep", BuiltinScalarFunction::PgSleep),
        ("pg_sleep_for", BuiltinScalarFunction::PgSleep),
        ("timezone", BuiltinScalarFunction::Timezone),
        ("date_part", BuiltinScalarFunction::DatePart),
        ("extract", BuiltinScalarFunction::Extract),
        ("date_trunc", BuiltinScalarFunction::DateTrunc),
        ("date_bin", BuiltinScalarFunction::DateBin),
        ("date_add", BuiltinScalarFunction::DateAdd),
        ("date_subtract", BuiltinScalarFunction::DateSubtract),
        ("age", BuiltinScalarFunction::Age),
        ("justify_days", BuiltinScalarFunction::JustifyDays),
        ("justify_hours", BuiltinScalarFunction::JustifyHours),
        ("justify_interval", BuiltinScalarFunction::JustifyInterval),
        ("isfinite", BuiltinScalarFunction::IsFinite),
        ("make_interval", BuiltinScalarFunction::MakeInterval),
        ("make_date", BuiltinScalarFunction::MakeDate),
        ("make_time", BuiltinScalarFunction::MakeTime),
        ("make_timestamp", BuiltinScalarFunction::MakeTimestamp),
        ("make_timestamptz", BuiltinScalarFunction::MakeTimestampTz),
        ("interval_hash", BuiltinScalarFunction::IntervalHash),
        (
            "getdatabaseencoding",
            BuiltinScalarFunction::GetDatabaseEncoding,
        ),
        ("unicode_version", BuiltinScalarFunction::UnicodeVersion),
        ("unicode_assigned", BuiltinScalarFunction::UnicodeAssigned),
        ("normalize", BuiltinScalarFunction::Normalize),
        ("unicode_normalize_func", BuiltinScalarFunction::Normalize),
        ("is_normalized", BuiltinScalarFunction::IsNormalized),
        ("unicode_is_normalized", BuiltinScalarFunction::IsNormalized),
        (
            "pg_char_to_encoding",
            BuiltinScalarFunction::PgCharToEncoding,
        ),
        (
            "pg_encoding_to_char",
            BuiltinScalarFunction::PgEncodingToChar,
        ),
        ("pg_partition_root", BuiltinScalarFunction::PgPartitionRoot),
        (
            "satisfies_hash_partition",
            BuiltinScalarFunction::SatisfiesHashPartition,
        ),
        (
            "pg_relation_filenode",
            BuiltinScalarFunction::PgRelationFilenode,
        ),
        (
            "pg_filenode_relation",
            BuiltinScalarFunction::PgFilenodeRelation,
        ),
        (
            "pg_tablespace_location",
            BuiltinScalarFunction::PgTablespaceLocation,
        ),
        ("pg_get_partkeydef", BuiltinScalarFunction::PgGetPartKeyDef),
        (
            "pg_table_is_visible",
            BuiltinScalarFunction::PgTableIsVisible,
        ),
        ("pg_type_is_visible", BuiltinScalarFunction::PgTypeIsVisible),
        (
            "pg_operator_is_visible",
            BuiltinScalarFunction::PgOperatorIsVisible,
        ),
        (
            "pg_opclass_is_visible",
            BuiltinScalarFunction::PgOpclassIsVisible,
        ),
        (
            "pg_opfamily_is_visible",
            BuiltinScalarFunction::PgOpfamilyIsVisible,
        ),
        (
            "pg_conversion_is_visible",
            BuiltinScalarFunction::PgConversionIsVisible,
        ),
        (
            "pg_ts_parser_is_visible",
            BuiltinScalarFunction::PgTsParserIsVisible,
        ),
        (
            "pg_ts_dict_is_visible",
            BuiltinScalarFunction::PgTsDictIsVisible,
        ),
        (
            "pg_ts_template_is_visible",
            BuiltinScalarFunction::PgTsTemplateIsVisible,
        ),
        (
            "pg_ts_config_is_visible",
            BuiltinScalarFunction::PgTsConfigIsVisible,
        ),
        ("pg_my_temp_schema", BuiltinScalarFunction::PgMyTempSchema),
        (
            "pg_rust_internal_binary_coercible",
            BuiltinScalarFunction::PgRustInternalBinaryCoercible,
        ),
        (
            "binary_coercible",
            BuiltinScalarFunction::PgRustInternalBinaryCoercible,
        ),
        (
            "pg_rust_test_opclass_options_func",
            BuiltinScalarFunction::PgRustTestOpclassOptionsFunc,
        ),
        (
            "pg_rust_test_fdw_handler",
            BuiltinScalarFunction::PgRustTestFdwHandler,
        ),
        (
            "test_fdw_handler",
            BuiltinScalarFunction::PgRustTestFdwHandler,
        ),
        ("interpt_pp", BuiltinScalarFunction::GeoIntersection),
        (
            "pg_rust_test_enc_setup",
            BuiltinScalarFunction::PgRustTestEncSetup,
        ),
        (
            "pg_rust_test_enc_conversion",
            BuiltinScalarFunction::PgRustTestEncConversion,
        ),
        (
            "pg_rust_is_catalog_text_unique_index_oid",
            BuiltinScalarFunction::PgRustIsCatalogTextUniqueIndexOid,
        ),
        (
            "pg_rust_test_widget_in",
            BuiltinScalarFunction::PgRustTestWidgetIn,
        ),
        ("widget_in", BuiltinScalarFunction::PgRustTestWidgetIn),
        (
            "pg_rust_test_widget_out",
            BuiltinScalarFunction::PgRustTestWidgetOut,
        ),
        ("widget_out", BuiltinScalarFunction::PgRustTestWidgetOut),
        (
            "pg_rust_test_int44in",
            BuiltinScalarFunction::PgRustTestInt44In,
        ),
        ("int44in", BuiltinScalarFunction::PgRustTestInt44In),
        (
            "pg_rust_test_int44out",
            BuiltinScalarFunction::PgRustTestInt44Out,
        ),
        ("int44out", BuiltinScalarFunction::PgRustTestInt44Out),
        (
            "pg_rust_test_pt_in_widget",
            BuiltinScalarFunction::PgRustTestPtInWidget,
        ),
        ("pt_in_widget", BuiltinScalarFunction::PgRustTestPtInWidget),
        ("amvalidate", BuiltinScalarFunction::AmValidate),
        ("btequalimage", BuiltinScalarFunction::BtEqualImage),
        ("pg_notify", BuiltinScalarFunction::PgNotify),
        (
            "pg_notification_queue_usage",
            BuiltinScalarFunction::PgNotificationQueueUsage,
        ),
        ("current_setting", BuiltinScalarFunction::CurrentSetting),
        ("set_config", BuiltinScalarFunction::SetConfig),
        (
            "pg_column_compression",
            BuiltinScalarFunction::PgColumnCompression,
        ),
        (
            "pg_column_toast_chunk_id",
            BuiltinScalarFunction::PgColumnToastChunkId,
        ),
        ("pg_column_size", BuiltinScalarFunction::PgColumnSize),
        ("pg_relation_size", BuiltinScalarFunction::PgRelationSize),
        ("pg_table_size", BuiltinScalarFunction::PgTableSize),
        ("pg_num_nulls", BuiltinScalarFunction::NumNulls),
        ("num_nulls", BuiltinScalarFunction::NumNulls),
        ("pg_num_nonnulls", BuiltinScalarFunction::NumNonNulls),
        ("num_nonnulls", BuiltinScalarFunction::NumNonNulls),
        (
            "pg_log_backend_memory_contexts",
            BuiltinScalarFunction::PgLogBackendMemoryContexts,
        ),
        (
            "has_function_privilege",
            BuiltinScalarFunction::HasFunctionPrivilege,
        ),
        (
            "has_function_privilege_name_name",
            BuiltinScalarFunction::HasFunctionPrivilege,
        ),
        (
            "has_function_privilege_name_id",
            BuiltinScalarFunction::HasFunctionPrivilege,
        ),
        (
            "has_function_privilege_id_name",
            BuiltinScalarFunction::HasFunctionPrivilege,
        ),
        (
            "has_function_privilege_id_id",
            BuiltinScalarFunction::HasFunctionPrivilege,
        ),
        (
            "has_function_privilege_name",
            BuiltinScalarFunction::HasFunctionPrivilege,
        ),
        (
            "has_function_privilege_id",
            BuiltinScalarFunction::HasFunctionPrivilege,
        ),
        (
            "has_table_privilege",
            BuiltinScalarFunction::HasTablePrivilege,
        ),
        (
            "has_table_privilege_name_name",
            BuiltinScalarFunction::HasTablePrivilege,
        ),
        (
            "has_table_privilege_name_id",
            BuiltinScalarFunction::HasTablePrivilege,
        ),
        (
            "has_table_privilege_id_name",
            BuiltinScalarFunction::HasTablePrivilege,
        ),
        (
            "has_table_privilege_id_id",
            BuiltinScalarFunction::HasTablePrivilege,
        ),
        (
            "has_table_privilege_name",
            BuiltinScalarFunction::HasTablePrivilege,
        ),
        (
            "has_table_privilege_id",
            BuiltinScalarFunction::HasTablePrivilege,
        ),
        (
            "row_security_active",
            BuiltinScalarFunction::RowSecurityActive,
        ),
        (
            "has_sequence_privilege",
            BuiltinScalarFunction::HasSequencePrivilege,
        ),
        (
            "has_sequence_privilege_name_name",
            BuiltinScalarFunction::HasSequencePrivilege,
        ),
        (
            "has_sequence_privilege_name_id",
            BuiltinScalarFunction::HasSequencePrivilege,
        ),
        (
            "has_sequence_privilege_id_name",
            BuiltinScalarFunction::HasSequencePrivilege,
        ),
        (
            "has_sequence_privilege_id_id",
            BuiltinScalarFunction::HasSequencePrivilege,
        ),
        (
            "has_sequence_privilege_name",
            BuiltinScalarFunction::HasSequencePrivilege,
        ),
        (
            "has_sequence_privilege_id",
            BuiltinScalarFunction::HasSequencePrivilege,
        ),
        ("pg_has_role", BuiltinScalarFunction::PgHasRole),
        ("pg_has_role_name_name", BuiltinScalarFunction::PgHasRole),
        ("pg_has_role_name_id", BuiltinScalarFunction::PgHasRole),
        ("pg_has_role_id_name", BuiltinScalarFunction::PgHasRole),
        ("pg_has_role_id_id", BuiltinScalarFunction::PgHasRole),
        ("pg_has_role_name", BuiltinScalarFunction::PgHasRole),
        ("pg_has_role_id", BuiltinScalarFunction::PgHasRole),
        (
            "has_column_privilege",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_name_name_name",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_name_name_attnum",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_name_id_name",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_name_id_attnum",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_id_name_name",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_id_name_attnum",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_id_id_name",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_id_id_attnum",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_name_name",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_name_attnum",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_id_name",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_column_privilege_id_attnum",
            BuiltinScalarFunction::HasColumnPrivilege,
        ),
        (
            "has_any_column_privilege",
            BuiltinScalarFunction::HasAnyColumnPrivilege,
        ),
        (
            "has_any_column_privilege_name_name",
            BuiltinScalarFunction::HasAnyColumnPrivilege,
        ),
        (
            "has_any_column_privilege_name_id",
            BuiltinScalarFunction::HasAnyColumnPrivilege,
        ),
        (
            "has_any_column_privilege_id_name",
            BuiltinScalarFunction::HasAnyColumnPrivilege,
        ),
        (
            "has_any_column_privilege_id_id",
            BuiltinScalarFunction::HasAnyColumnPrivilege,
        ),
        (
            "has_any_column_privilege_name",
            BuiltinScalarFunction::HasAnyColumnPrivilege,
        ),
        (
            "has_any_column_privilege_id",
            BuiltinScalarFunction::HasAnyColumnPrivilege,
        ),
        (
            "has_largeobject_privilege",
            BuiltinScalarFunction::HasLargeObjectPrivilege,
        ),
        (
            "has_largeobject_privilege_name_id",
            BuiltinScalarFunction::HasLargeObjectPrivilege,
        ),
        (
            "has_largeobject_privilege_id",
            BuiltinScalarFunction::HasLargeObjectPrivilege,
        ),
        (
            "has_largeobject_privilege_id_id",
            BuiltinScalarFunction::HasLargeObjectPrivilege,
        ),
        (
            "pg_current_logfile",
            BuiltinScalarFunction::PgCurrentLogfile,
        ),
        (
            "pg_current_logfile_1arg",
            BuiltinScalarFunction::PgCurrentLogfile,
        ),
        ("pg_read_file_off_len", BuiltinScalarFunction::PgReadFile),
        (
            "pg_read_file_off_len_missing",
            BuiltinScalarFunction::PgReadFile,
        ),
        ("pg_read_file_all", BuiltinScalarFunction::PgReadFile),
        (
            "pg_read_file_all_missing",
            BuiltinScalarFunction::PgReadFile,
        ),
        (
            "pg_read_binary_file_off_len",
            BuiltinScalarFunction::PgReadBinaryFile,
        ),
        (
            "pg_read_binary_file_off_len_missing",
            BuiltinScalarFunction::PgReadBinaryFile,
        ),
        (
            "pg_read_binary_file_all",
            BuiltinScalarFunction::PgReadBinaryFile,
        ),
        (
            "pg_read_binary_file_all_missing",
            BuiltinScalarFunction::PgReadBinaryFile,
        ),
        ("pg_stat_file", BuiltinScalarFunction::PgStatFile),
        ("pg_stat_file_1arg", BuiltinScalarFunction::PgStatFile),
        ("pg_walfile_name", BuiltinScalarFunction::PgWalfileName),
        (
            "pg_walfile_name_offset",
            BuiltinScalarFunction::PgWalfileNameOffset,
        ),
        (
            "pg_split_walfile_name",
            BuiltinScalarFunction::PgSplitWalfileName,
        ),
        ("pg_control_system", BuiltinScalarFunction::PgControlSystem),
        (
            "pg_control_checkpoint",
            BuiltinScalarFunction::PgControlCheckpoint,
        ),
        (
            "pg_control_recovery",
            BuiltinScalarFunction::PgControlRecovery,
        ),
        ("pg_control_init", BuiltinScalarFunction::PgControlInit),
        (
            "pg_replication_origin_create",
            BuiltinScalarFunction::PgReplicationOriginCreate,
        ),
        (
            "gist_translate_cmptype_common",
            BuiltinScalarFunction::GistTranslateCmpTypeCommon,
        ),
        (
            "test_canonicalize_path",
            BuiltinScalarFunction::TestCanonicalizePath,
        ),
        ("test_relpath", BuiltinScalarFunction::TestRelpath),
        ("nextval", BuiltinScalarFunction::NextVal),
        ("currval", BuiltinScalarFunction::CurrVal),
        ("lastval", BuiltinScalarFunction::LastVal),
        ("currtid2", BuiltinScalarFunction::CurrTid2),
        ("setval", BuiltinScalarFunction::SetVal),
        ("setval_oid", BuiltinScalarFunction::SetVal),
        ("setval_text", BuiltinScalarFunction::SetVal),
        ("setval3_oid", BuiltinScalarFunction::SetVal),
        ("setval3_text", BuiltinScalarFunction::SetVal),
        (
            "pg_get_serial_sequence",
            BuiltinScalarFunction::PgGetSerialSequence,
        ),
        (
            "pg_sequence_parameters",
            BuiltinScalarFunction::PgSequenceParameters,
        ),
        (
            "pg_sequence_last_value",
            BuiltinScalarFunction::PgSequenceLastValue,
        ),
        (
            "pg_get_sequence_data",
            BuiltinScalarFunction::PgGetSequenceData,
        ),
        ("pg_size_pretty", BuiltinScalarFunction::PgSizePretty),
        (
            "pg_size_pretty_numeric",
            BuiltinScalarFunction::PgSizePretty,
        ),
        ("pg_size_bytes", BuiltinScalarFunction::PgSizeBytes),
        ("parse_ident", BuiltinScalarFunction::ParseIdent),
        ("parse_ident_text", BuiltinScalarFunction::ParseIdent),
        ("pg_get_userbyid", BuiltinScalarFunction::PgGetUserById),
        ("obj_description", BuiltinScalarFunction::ObjDescription),
        (
            "pg_describe_object",
            BuiltinScalarFunction::PgDescribeObject,
        ),
        (
            "pg_identify_object",
            BuiltinScalarFunction::PgIdentifyObject,
        ),
        (
            "pg_identify_object_as_address",
            BuiltinScalarFunction::PgIdentifyObjectAsAddress,
        ),
        (
            "pg_get_object_address",
            BuiltinScalarFunction::PgGetObjectAddress,
        ),
        (
            "pg_get_function_arguments",
            BuiltinScalarFunction::PgGetFunctionArguments,
        ),
        (
            "pg_get_functiondef",
            BuiltinScalarFunction::PgGetFunctionDef,
        ),
        (
            "pg_get_function_result",
            BuiltinScalarFunction::PgGetFunctionResult,
        ),
        (
            "pg_function_is_visible",
            BuiltinScalarFunction::PgFunctionIsVisible,
        ),
        ("pg_get_expr", BuiltinScalarFunction::PgGetExpr),
        ("pg_get_expr_ext", BuiltinScalarFunction::PgGetExpr),
        ("pg_get_partkeydef", BuiltinScalarFunction::PgGetPartKeyDef),
        ("pg_get_ruledef", BuiltinScalarFunction::PgGetRuleDef),
        ("pg_get_ruledef_ext", BuiltinScalarFunction::PgGetRuleDef),
        ("pg_get_viewdef", BuiltinScalarFunction::PgGetViewDef),
        (
            "pg_get_statisticsobjdef",
            BuiltinScalarFunction::PgGetStatisticsObjDef,
        ),
        (
            "pg_get_statisticsobjdef_columns",
            BuiltinScalarFunction::PgGetStatisticsObjDefColumns,
        ),
        (
            "pg_get_statisticsobjdef_expressions",
            BuiltinScalarFunction::PgGetStatisticsObjDefExpressions,
        ),
        (
            "pg_statistics_obj_is_visible",
            BuiltinScalarFunction::PgStatisticsObjIsVisible,
        ),
        (
            "pg_relation_is_publishable",
            BuiltinScalarFunction::PgRelationIsPublishable,
        ),
        (
            "pg_indexam_has_property",
            BuiltinScalarFunction::PgIndexAmHasProperty,
        ),
        (
            "pg_index_has_property",
            BuiltinScalarFunction::PgIndexHasProperty,
        ),
        (
            "pg_index_column_has_property",
            BuiltinScalarFunction::PgIndexColumnHasProperty,
        ),
        ("pg_advisory_lock", BuiltinScalarFunction::PgAdvisoryLock),
        (
            "pg_advisory_lock_int8",
            BuiltinScalarFunction::PgAdvisoryLock,
        ),
        (
            "pg_advisory_lock_int4",
            BuiltinScalarFunction::PgAdvisoryLock,
        ),
        (
            "pg_advisory_xact_lock",
            BuiltinScalarFunction::PgAdvisoryXactLock,
        ),
        (
            "pg_advisory_xact_lock_int8",
            BuiltinScalarFunction::PgAdvisoryXactLock,
        ),
        (
            "pg_advisory_xact_lock_int4",
            BuiltinScalarFunction::PgAdvisoryXactLock,
        ),
        (
            "pg_advisory_lock_shared",
            BuiltinScalarFunction::PgAdvisoryLockShared,
        ),
        (
            "pg_advisory_lock_shared_int8",
            BuiltinScalarFunction::PgAdvisoryLockShared,
        ),
        (
            "pg_advisory_lock_shared_int4",
            BuiltinScalarFunction::PgAdvisoryLockShared,
        ),
        (
            "pg_advisory_xact_lock_shared",
            BuiltinScalarFunction::PgAdvisoryXactLockShared,
        ),
        (
            "pg_advisory_xact_lock_shared_int8",
            BuiltinScalarFunction::PgAdvisoryXactLockShared,
        ),
        (
            "pg_advisory_xact_lock_shared_int4",
            BuiltinScalarFunction::PgAdvisoryXactLockShared,
        ),
        (
            "pg_try_advisory_lock",
            BuiltinScalarFunction::PgTryAdvisoryLock,
        ),
        (
            "pg_try_advisory_lock_int8",
            BuiltinScalarFunction::PgTryAdvisoryLock,
        ),
        (
            "pg_try_advisory_lock_int4",
            BuiltinScalarFunction::PgTryAdvisoryLock,
        ),
        (
            "pg_try_advisory_xact_lock",
            BuiltinScalarFunction::PgTryAdvisoryXactLock,
        ),
        (
            "pg_try_advisory_xact_lock_int8",
            BuiltinScalarFunction::PgTryAdvisoryXactLock,
        ),
        (
            "pg_try_advisory_xact_lock_int4",
            BuiltinScalarFunction::PgTryAdvisoryXactLock,
        ),
        (
            "pg_try_advisory_lock_shared",
            BuiltinScalarFunction::PgTryAdvisoryLockShared,
        ),
        (
            "pg_try_advisory_lock_shared_int8",
            BuiltinScalarFunction::PgTryAdvisoryLockShared,
        ),
        (
            "pg_try_advisory_lock_shared_int4",
            BuiltinScalarFunction::PgTryAdvisoryLockShared,
        ),
        (
            "pg_try_advisory_xact_lock_shared",
            BuiltinScalarFunction::PgTryAdvisoryXactLockShared,
        ),
        (
            "pg_try_advisory_xact_lock_shared_int8",
            BuiltinScalarFunction::PgTryAdvisoryXactLockShared,
        ),
        (
            "pg_try_advisory_xact_lock_shared_int4",
            BuiltinScalarFunction::PgTryAdvisoryXactLockShared,
        ),
        (
            "pg_advisory_unlock",
            BuiltinScalarFunction::PgAdvisoryUnlock,
        ),
        (
            "pg_advisory_unlock_int8",
            BuiltinScalarFunction::PgAdvisoryUnlock,
        ),
        (
            "pg_advisory_unlock_int4",
            BuiltinScalarFunction::PgAdvisoryUnlock,
        ),
        (
            "pg_advisory_unlock_shared",
            BuiltinScalarFunction::PgAdvisoryUnlockShared,
        ),
        (
            "pg_advisory_unlock_shared_int8",
            BuiltinScalarFunction::PgAdvisoryUnlockShared,
        ),
        (
            "pg_advisory_unlock_shared_int4",
            BuiltinScalarFunction::PgAdvisoryUnlockShared,
        ),
        (
            "pg_advisory_unlock_all",
            BuiltinScalarFunction::PgAdvisoryUnlockAll,
        ),
        ("lo_create", BuiltinScalarFunction::LoCreate),
        ("lo_unlink", BuiltinScalarFunction::LoUnlink),
        ("pg_typeof", BuiltinScalarFunction::PgTypeof),
        ("pg_basetype", BuiltinScalarFunction::PgBaseType),
        (
            "pg_stat_get_checkpointer_num_timed",
            BuiltinScalarFunction::PgStatGetCheckpointerNumTimed,
        ),
        (
            "pg_stat_get_checkpointer_num_requested",
            BuiltinScalarFunction::PgStatGetCheckpointerNumRequested,
        ),
        (
            "pg_stat_get_checkpointer_num_performed",
            BuiltinScalarFunction::PgStatGetCheckpointerNumPerformed,
        ),
        (
            "pg_stat_get_checkpointer_buffers_written",
            BuiltinScalarFunction::PgStatGetCheckpointerBuffersWritten,
        ),
        (
            "pg_stat_get_checkpointer_slru_written",
            BuiltinScalarFunction::PgStatGetCheckpointerSlruWritten,
        ),
        (
            "pg_stat_get_checkpointer_write_time",
            BuiltinScalarFunction::PgStatGetCheckpointerWriteTime,
        ),
        (
            "pg_stat_get_checkpointer_sync_time",
            BuiltinScalarFunction::PgStatGetCheckpointerSyncTime,
        ),
        (
            "pg_stat_get_checkpointer_stat_reset_time",
            BuiltinScalarFunction::PgStatGetCheckpointerStatResetTime,
        ),
        (
            "pg_stat_force_next_flush",
            BuiltinScalarFunction::PgStatForceNextFlush,
        ),
        (
            "pg_stat_get_snapshot_timestamp",
            BuiltinScalarFunction::PgStatGetSnapshotTimestamp,
        ),
        (
            "pg_stat_clear_snapshot",
            BuiltinScalarFunction::PgStatClearSnapshot,
        ),
        (
            "pg_stat_get_backend_pid",
            BuiltinScalarFunction::PgStatGetBackendPid,
        ),
        (
            "pg_stat_get_backend_wal",
            BuiltinScalarFunction::PgStatGetBackendWal,
        ),
        ("pg_stat_reset", BuiltinScalarFunction::PgStatReset),
        (
            "pg_stat_reset_shared",
            BuiltinScalarFunction::PgStatResetShared,
        ),
        (
            "pg_stat_reset_single_table_counters",
            BuiltinScalarFunction::PgStatResetSingleTableCounters,
        ),
        (
            "pg_stat_reset_single_function_counters",
            BuiltinScalarFunction::PgStatResetSingleFunctionCounters,
        ),
        (
            "pg_stat_reset_backend_stats",
            BuiltinScalarFunction::PgStatResetBackendStats,
        ),
        ("pg_stat_reset_slru", BuiltinScalarFunction::PgStatResetSlru),
        (
            "pg_stat_reset_replication_slot",
            BuiltinScalarFunction::PgStatResetReplicationSlot,
        ),
        (
            "pg_stat_reset_subscription_stats",
            BuiltinScalarFunction::PgStatResetSubscriptionStats,
        ),
        (
            "pg_stat_get_replication_slot",
            BuiltinScalarFunction::PgStatGetReplicationSlot,
        ),
        (
            "pg_stat_get_subscription_stats",
            BuiltinScalarFunction::PgStatGetSubscriptionStats,
        ),
        ("shobj_description", BuiltinScalarFunction::ShobjDescription),
        ("pg_stat_have_stats", BuiltinScalarFunction::PgStatHaveStats),
        (
            "pg_stat_get_numscans",
            BuiltinScalarFunction::PgStatGetNumscans,
        ),
        (
            "pg_stat_get_lastscan",
            BuiltinScalarFunction::PgStatGetLastscan,
        ),
        (
            "pg_stat_get_tuples_returned",
            BuiltinScalarFunction::PgStatGetTuplesReturned,
        ),
        (
            "pg_stat_get_tuples_fetched",
            BuiltinScalarFunction::PgStatGetTuplesFetched,
        ),
        (
            "pg_stat_get_tuples_inserted",
            BuiltinScalarFunction::PgStatGetTuplesInserted,
        ),
        (
            "pg_stat_get_tuples_updated",
            BuiltinScalarFunction::PgStatGetTuplesUpdated,
        ),
        (
            "pg_stat_get_tuples_hot_updated",
            BuiltinScalarFunction::PgStatGetTuplesHotUpdated,
        ),
        (
            "pg_stat_get_tuples_deleted",
            BuiltinScalarFunction::PgStatGetTuplesDeleted,
        ),
        (
            "pg_stat_get_live_tuples",
            BuiltinScalarFunction::PgStatGetLiveTuples,
        ),
        (
            "pg_stat_get_dead_tuples",
            BuiltinScalarFunction::PgStatGetDeadTuples,
        ),
        (
            "pg_stat_get_blocks_fetched",
            BuiltinScalarFunction::PgStatGetBlocksFetched,
        ),
        (
            "pg_stat_get_blocks_hit",
            BuiltinScalarFunction::PgStatGetBlocksHit,
        ),
        (
            "pg_stat_get_xact_numscans",
            BuiltinScalarFunction::PgStatGetXactNumscans,
        ),
        (
            "pg_stat_get_xact_tuples_returned",
            BuiltinScalarFunction::PgStatGetXactTuplesReturned,
        ),
        (
            "pg_stat_get_xact_tuples_fetched",
            BuiltinScalarFunction::PgStatGetXactTuplesFetched,
        ),
        (
            "pg_stat_get_xact_tuples_inserted",
            BuiltinScalarFunction::PgStatGetXactTuplesInserted,
        ),
        (
            "pg_stat_get_xact_tuples_updated",
            BuiltinScalarFunction::PgStatGetXactTuplesUpdated,
        ),
        (
            "pg_stat_get_xact_tuples_deleted",
            BuiltinScalarFunction::PgStatGetXactTuplesDeleted,
        ),
        (
            "pg_stat_get_function_calls",
            BuiltinScalarFunction::PgStatGetFunctionCalls,
        ),
        (
            "pg_stat_get_function_total_time",
            BuiltinScalarFunction::PgStatGetFunctionTotalTime,
        ),
        (
            "pg_stat_get_function_self_time",
            BuiltinScalarFunction::PgStatGetFunctionSelfTime,
        ),
        (
            "pg_stat_get_xact_function_calls",
            BuiltinScalarFunction::PgStatGetXactFunctionCalls,
        ),
        (
            "pg_stat_get_xact_function_total_time",
            BuiltinScalarFunction::PgStatGetXactFunctionTotalTime,
        ),
        (
            "pg_stat_get_xact_function_self_time",
            BuiltinScalarFunction::PgStatGetXactFunctionSelfTime,
        ),
        (
            "pg_restore_relation_stats",
            BuiltinScalarFunction::PgRestoreRelationStats,
        ),
        (
            "pg_clear_relation_stats",
            BuiltinScalarFunction::PgClearRelationStats,
        ),
        (
            "pg_restore_attribute_stats",
            BuiltinScalarFunction::PgRestoreAttributeStats,
        ),
        (
            "pg_clear_attribute_stats",
            BuiltinScalarFunction::PgClearAttributeStats,
        ),
        ("to_json", BuiltinScalarFunction::ToJson),
        ("to_jsonb", BuiltinScalarFunction::ToJsonb),
        ("to_tsvector", BuiltinScalarFunction::ToTsVector),
        ("to_tsvector_byid", BuiltinScalarFunction::ToTsVector),
        ("json_to_tsvector", BuiltinScalarFunction::JsonToTsVector),
        (
            "json_to_tsvector_byid",
            BuiltinScalarFunction::JsonToTsVector,
        ),
        (
            "jsonb_string_to_tsvector",
            BuiltinScalarFunction::ToTsVector,
        ),
        (
            "jsonb_string_to_tsvector_byid",
            BuiltinScalarFunction::ToTsVector,
        ),
        ("jsonb_to_tsvector", BuiltinScalarFunction::JsonbToTsVector),
        (
            "jsonb_to_tsvector_byid",
            BuiltinScalarFunction::JsonbToTsVector,
        ),
        ("to_tsquery", BuiltinScalarFunction::ToTsQuery),
        ("to_tsquery_byid", BuiltinScalarFunction::ToTsQuery),
        ("plainto_tsquery", BuiltinScalarFunction::PlainToTsQuery),
        (
            "plainto_tsquery_byid",
            BuiltinScalarFunction::PlainToTsQuery,
        ),
        ("phraseto_tsquery", BuiltinScalarFunction::PhraseToTsQuery),
        (
            "phraseto_tsquery_byid",
            BuiltinScalarFunction::PhraseToTsQuery,
        ),
        (
            "websearch_to_tsquery",
            BuiltinScalarFunction::WebSearchToTsQuery,
        ),
        (
            "websearch_to_tsquery_byid",
            BuiltinScalarFunction::WebSearchToTsQuery,
        ),
        ("ts_lexize", BuiltinScalarFunction::TsLexize),
        ("ts_headline", BuiltinScalarFunction::TsHeadline),
        ("tsvectorin", BuiltinScalarFunction::TsVectorIn),
        ("tsvectorout", BuiltinScalarFunction::TsVectorOut),
        ("tsqueryin", BuiltinScalarFunction::TsQueryIn),
        ("tsqueryout", BuiltinScalarFunction::TsQueryOut),
        ("tsquery_phrase", BuiltinScalarFunction::TsQueryPhrase),
        (
            "tsquery_phrase_distance",
            BuiltinScalarFunction::TsQueryPhrase,
        ),
        ("ts_rewrite", BuiltinScalarFunction::TsRewrite),
        ("tsquery_numnode", BuiltinScalarFunction::TsQueryNumnode),
        ("numnode", BuiltinScalarFunction::TsQueryNumnode),
        ("tsq_mcontains", BuiltinScalarFunction::TsQueryContains),
        ("tsq_mcontained", BuiltinScalarFunction::TsQueryContainedBy),
        ("tsvector_strip", BuiltinScalarFunction::TsVectorStrip),
        ("strip", BuiltinScalarFunction::TsVectorStrip),
        ("tsvector_delete_str", BuiltinScalarFunction::TsVectorDelete),
        ("tsvector_delete_arr", BuiltinScalarFunction::TsVectorDelete),
        ("ts_delete", BuiltinScalarFunction::TsVectorDelete),
        ("tsvector_to_array", BuiltinScalarFunction::TsVectorToArray),
        ("array_to_tsvector", BuiltinScalarFunction::ArrayToTsVector),
        (
            "tsvector_setweight",
            BuiltinScalarFunction::TsVectorSetWeight,
        ),
        (
            "tsvector_setweight_by_filter",
            BuiltinScalarFunction::TsVectorSetWeight,
        ),
        ("setweight", BuiltinScalarFunction::TsVectorSetWeight),
        ("tsvector_filter", BuiltinScalarFunction::TsVectorFilter),
        ("ts_filter", BuiltinScalarFunction::TsVectorFilter),
        ("ts_rank", BuiltinScalarFunction::TsRank),
        ("ts_rank_wttf", BuiltinScalarFunction::TsRank),
        ("ts_rank_wtt", BuiltinScalarFunction::TsRank),
        ("ts_rank_ttf", BuiltinScalarFunction::TsRank),
        ("ts_rank_tt", BuiltinScalarFunction::TsRank),
        ("ts_rank_cd", BuiltinScalarFunction::TsRankCd),
        ("ts_rankcd_wttf", BuiltinScalarFunction::TsRankCd),
        ("ts_rankcd_wtt", BuiltinScalarFunction::TsRankCd),
        ("ts_rankcd_ttf", BuiltinScalarFunction::TsRankCd),
        ("ts_rankcd_tt", BuiltinScalarFunction::TsRankCd),
        ("tsq_mcontains", BuiltinScalarFunction::TsQueryContains),
        ("tsq_mcontained", BuiltinScalarFunction::TsQueryContains),
        ("ts_rewrite", BuiltinScalarFunction::TsRewrite),
        ("tsquery_rewrite", BuiltinScalarFunction::TsRewrite),
        ("tsquery_rewrite_query", BuiltinScalarFunction::TsRewrite),
        ("ts_headline", BuiltinScalarFunction::TsHeadline),
        ("ts_headline_byid", BuiltinScalarFunction::TsHeadline),
        ("ts_headline_opt", BuiltinScalarFunction::TsHeadline),
        ("ts_headline_byid_opt", BuiltinScalarFunction::TsHeadline),
        ("array_to_json", BuiltinScalarFunction::ArrayToJson),
        ("row_to_json", BuiltinScalarFunction::RowToJson),
        ("row_to_json_pretty", BuiltinScalarFunction::RowToJson),
        ("json_build_array", BuiltinScalarFunction::JsonBuildArray),
        ("json_build_object", BuiltinScalarFunction::JsonBuildObject),
        ("json_object", BuiltinScalarFunction::JsonObject),
        (
            "json_populate_record",
            BuiltinScalarFunction::JsonPopulateRecord,
        ),
        (
            "json_populate_record_valid",
            BuiltinScalarFunction::JsonPopulateRecordValid,
        ),
        ("json_to_record", BuiltinScalarFunction::JsonToRecord),
        ("json_strip_nulls", BuiltinScalarFunction::JsonStripNulls),
        ("json_typeof", BuiltinScalarFunction::JsonTypeof),
        ("json_array_length", BuiltinScalarFunction::JsonArrayLength),
        ("json_extract_path", BuiltinScalarFunction::JsonExtractPath),
        (
            "json_extract_path_text",
            BuiltinScalarFunction::JsonExtractPathText,
        ),
        // :HACK: PostgreSQL parses these as SQL/JSON expression nodes. Accept
        // the plain two-argument spelling as scalar builtins until pgrust has
        // dedicated JsonExpr parser/analyzer nodes.
        ("json_exists", BuiltinScalarFunction::JsonExists),
        ("json_value", BuiltinScalarFunction::JsonValue),
        ("json_query", BuiltinScalarFunction::JsonQuery),
        ("jsonb_typeof", BuiltinScalarFunction::JsonbTypeof),
        (
            "jsonb_array_length",
            BuiltinScalarFunction::JsonbArrayLength,
        ),
        (
            "jsonb_extract_path",
            BuiltinScalarFunction::JsonbExtractPath,
        ),
        (
            "jsonb_extract_path_text",
            BuiltinScalarFunction::JsonbExtractPathText,
        ),
        ("jsonb_object", BuiltinScalarFunction::JsonbObject),
        ("jsonb_object_two_arg", BuiltinScalarFunction::JsonbObject),
        (
            "jsonb_populate_record",
            BuiltinScalarFunction::JsonbPopulateRecord,
        ),
        (
            "jsonb_populate_record_valid",
            BuiltinScalarFunction::JsonbPopulateRecordValid,
        ),
        ("jsonb_to_record", BuiltinScalarFunction::JsonbToRecord),
        ("jsonb_strip_nulls", BuiltinScalarFunction::JsonbStripNulls),
        ("jsonb_pretty", BuiltinScalarFunction::JsonbPretty),
        ("jsonb_build_array", BuiltinScalarFunction::JsonbBuildArray),
        (
            "jsonb_build_object",
            BuiltinScalarFunction::JsonbBuildObject,
        ),
        ("jsonb_concat", BuiltinScalarFunction::JsonbConcat),
        ("jsonb_contains", BuiltinScalarFunction::JsonbContains),
        ("jsonb_contained", BuiltinScalarFunction::JsonbContained),
        ("jsonb_delete", BuiltinScalarFunction::JsonbDelete),
        ("jsonb_delete_path", BuiltinScalarFunction::JsonbDeletePath),
        ("jsonb_exists", BuiltinScalarFunction::JsonbExists),
        ("jsonb_exists_any", BuiltinScalarFunction::JsonbExistsAny),
        ("jsonb_exists_all", BuiltinScalarFunction::JsonbExistsAll),
        ("jsonb_set", BuiltinScalarFunction::JsonbSet),
        ("jsonb_set_lax", BuiltinScalarFunction::JsonbSetLax),
        ("jsonb_insert", BuiltinScalarFunction::JsonbInsert),
        ("jsonb_path_exists", BuiltinScalarFunction::JsonbPathExists),
        ("jsonb_path_match", BuiltinScalarFunction::JsonbPathMatch),
        (
            "jsonb_path_query_array",
            BuiltinScalarFunction::JsonbPathQueryArray,
        ),
        (
            "jsonb_path_query_first",
            BuiltinScalarFunction::JsonbPathQueryFirst,
        ),
        ("initcap", BuiltinScalarFunction::Initcap),
        ("textcat", BuiltinScalarFunction::TextCat),
        ("concat", BuiltinScalarFunction::Concat),
        ("concat_ws", BuiltinScalarFunction::ConcatWs),
        ("format", BuiltinScalarFunction::Format),
        ("left", BuiltinScalarFunction::Left),
        ("right", BuiltinScalarFunction::Right),
        ("lpad", BuiltinScalarFunction::LPad),
        ("rpad", BuiltinScalarFunction::RPad),
        ("repeat", BuiltinScalarFunction::Repeat),
        ("length", BuiltinScalarFunction::Length),
        ("octet_length", BuiltinScalarFunction::OctetLength),
        ("bit_length", BuiltinScalarFunction::BitLength),
        ("array_ndims", BuiltinScalarFunction::ArrayNdims),
        ("array_dims", BuiltinScalarFunction::ArrayDims),
        ("array_lower", BuiltinScalarFunction::ArrayLower),
        ("array_upper", BuiltinScalarFunction::ArrayUpper),
        ("array_fill", BuiltinScalarFunction::ArrayFill),
        ("array_in", BuiltinScalarFunction::ArrayIn),
        ("anyrange_in", BuiltinScalarFunction::AnyRangeIn),
        ("array_larger", BuiltinScalarFunction::ArrayLarger),
        ("string_to_array", BuiltinScalarFunction::StringToArray),
        ("array_to_string", BuiltinScalarFunction::ArrayToString),
        ("array_length", BuiltinScalarFunction::ArrayLength),
        ("cardinality", BuiltinScalarFunction::Cardinality),
        ("array_append", BuiltinScalarFunction::ArrayAppend),
        ("array_prepend", BuiltinScalarFunction::ArrayPrepend),
        ("array_cat", BuiltinScalarFunction::ArrayCat),
        ("array_position", BuiltinScalarFunction::ArrayPosition),
        ("array_positions", BuiltinScalarFunction::ArrayPositions),
        ("array_remove", BuiltinScalarFunction::ArrayRemove),
        ("array_replace", BuiltinScalarFunction::ArrayReplace),
        ("trim_array", BuiltinScalarFunction::TrimArray),
        ("array_shuffle", BuiltinScalarFunction::ArrayShuffle),
        ("array_sample", BuiltinScalarFunction::ArraySample),
        ("array_reverse", BuiltinScalarFunction::ArrayReverse),
        ("array_sort", BuiltinScalarFunction::ArraySort),
        ("enum_first", BuiltinScalarFunction::EnumFirst),
        ("enum_last", BuiltinScalarFunction::EnumLast),
        ("enum_range", BuiltinScalarFunction::EnumRange),
        ("enum_range_bounds", BuiltinScalarFunction::EnumRange),
        ("lower", BuiltinScalarFunction::Lower),
        ("upper", BuiltinScalarFunction::Upper),
        ("unistr", BuiltinScalarFunction::Unistr),
        ("ascii", BuiltinScalarFunction::Ascii),
        ("chr", BuiltinScalarFunction::Chr),
        ("quote_ident", BuiltinScalarFunction::QuoteIdent),
        ("quote_literal", BuiltinScalarFunction::QuoteLiteral),
        ("quote_nullable", BuiltinScalarFunction::QuoteNullable),
        ("replace", BuiltinScalarFunction::Replace),
        ("split_part", BuiltinScalarFunction::SplitPart),
        ("translate", BuiltinScalarFunction::Translate),
        ("host", BuiltinScalarFunction::NetworkHost),
        ("abbrev", BuiltinScalarFunction::NetworkAbbrev),
        ("broadcast", BuiltinScalarFunction::NetworkBroadcast),
        ("network", BuiltinScalarFunction::NetworkNetwork),
        ("masklen", BuiltinScalarFunction::NetworkMasklen),
        ("family", BuiltinScalarFunction::NetworkFamily),
        ("netmask", BuiltinScalarFunction::NetworkNetmask),
        ("hostmask", BuiltinScalarFunction::NetworkHostmask),
        ("set_masklen", BuiltinScalarFunction::NetworkSetMasklen),
        ("inet_same_family", BuiltinScalarFunction::NetworkSameFamily),
        ("inet_merge", BuiltinScalarFunction::NetworkMerge),
        ("network_sub", BuiltinScalarFunction::NetworkSubnet),
        ("network_subeq", BuiltinScalarFunction::NetworkSubnetEq),
        ("network_sup", BuiltinScalarFunction::NetworkSupernet),
        ("network_supeq", BuiltinScalarFunction::NetworkSupernetEq),
        ("network_overlap", BuiltinScalarFunction::NetworkOverlap),
        ("text_to_regclass", BuiltinScalarFunction::TextToRegClass),
        ("to_regproc", BuiltinScalarFunction::ToRegProc),
        ("to_regprocedure", BuiltinScalarFunction::ToRegProcedure),
        ("to_regoper", BuiltinScalarFunction::ToRegOper),
        ("to_regoperator", BuiltinScalarFunction::ToRegOperator),
        ("to_regclass", BuiltinScalarFunction::ToRegClass),
        ("to_regtype", BuiltinScalarFunction::ToRegType),
        ("to_regtypemod", BuiltinScalarFunction::ToRegTypeMod),
        ("to_regrole", BuiltinScalarFunction::ToRegRole),
        ("to_regnamespace", BuiltinScalarFunction::ToRegNamespace),
        ("to_regcollation", BuiltinScalarFunction::ToRegCollation),
        ("format_type", BuiltinScalarFunction::FormatType),
        (
            "has_foreign_data_wrapper_privilege",
            BuiltinScalarFunction::HasForeignDataWrapperPrivilege,
        ),
        (
            "has_server_privilege",
            BuiltinScalarFunction::HasServerPrivilege,
        ),
        ("regproc_to_text", BuiltinScalarFunction::RegProcToText),
        ("regprocout", BuiltinScalarFunction::RegProcToText),
        ("regclass_to_text", BuiltinScalarFunction::RegClassToText),
        ("regclassout", BuiltinScalarFunction::RegClassToText),
        ("regtype_to_text", BuiltinScalarFunction::RegTypeToText),
        ("regtypeout", BuiltinScalarFunction::RegTypeToText),
        ("regoper_to_text", BuiltinScalarFunction::RegOperToText),
        ("regoperout", BuiltinScalarFunction::RegOperToText),
        (
            "regoperator_to_text",
            BuiltinScalarFunction::RegOperatorToText,
        ),
        ("regoperatorout", BuiltinScalarFunction::RegOperatorToText),
        (
            "regprocedure_to_text",
            BuiltinScalarFunction::RegProcedureToText,
        ),
        ("regprocedureout", BuiltinScalarFunction::RegProcedureToText),
        (
            "regcollation_to_text",
            BuiltinScalarFunction::RegCollationToText,
        ),
        ("regcollationout", BuiltinScalarFunction::RegCollationToText),
        ("regrole_to_text", BuiltinScalarFunction::RegRoleToText),
        ("regroleout", BuiltinScalarFunction::RegRoleToText),
        ("pg_get_acl", BuiltinScalarFunction::PgGetAcl),
        ("makeaclitem", BuiltinScalarFunction::MakeAclItem),
        ("pg_get_userbyid", BuiltinScalarFunction::PgGetUserById),
        (
            "pg_indexam_has_property",
            BuiltinScalarFunction::PgIndexAmHasProperty,
        ),
        (
            "pg_index_has_property",
            BuiltinScalarFunction::PgIndexHasProperty,
        ),
        (
            "pg_index_column_has_property",
            BuiltinScalarFunction::PgIndexColumnHasProperty,
        ),
        (
            "pg_describe_object",
            BuiltinScalarFunction::PgDescribeObject,
        ),
        (
            "pg_identify_object",
            BuiltinScalarFunction::PgIdentifyObject,
        ),
        (
            "pg_identify_object_as_address",
            BuiltinScalarFunction::PgIdentifyObjectAsAddress,
        ),
        (
            "pg_get_object_address",
            BuiltinScalarFunction::PgGetObjectAddress,
        ),
        (
            "pg_get_function_arguments",
            BuiltinScalarFunction::PgGetFunctionArguments,
        ),
        (
            "pg_get_functiondef",
            BuiltinScalarFunction::PgGetFunctionDef,
        ),
        (
            "pg_get_function_result",
            BuiltinScalarFunction::PgGetFunctionResult,
        ),
        (
            "pg_function_is_visible",
            BuiltinScalarFunction::PgFunctionIsVisible,
        ),
        (
            "pg_get_statisticsobjdef",
            BuiltinScalarFunction::PgGetStatisticsObjDef,
        ),
        (
            "pg_get_statisticsobjdef_columns",
            BuiltinScalarFunction::PgGetStatisticsObjDefColumns,
        ),
        (
            "pg_get_statisticsobjdef_expressions",
            BuiltinScalarFunction::PgGetStatisticsObjDefExpressions,
        ),
        (
            "pg_statistics_obj_is_visible",
            BuiltinScalarFunction::PgStatisticsObjIsVisible,
        ),
        ("position", BuiltinScalarFunction::Position),
        ("strpos", BuiltinScalarFunction::Strpos),
        ("substring", BuiltinScalarFunction::Substring),
        ("substr", BuiltinScalarFunction::Substring),
        ("similar_substring", BuiltinScalarFunction::SimilarSubstring),
        ("overlay", BuiltinScalarFunction::Overlay),
        ("reverse", BuiltinScalarFunction::Reverse),
        ("starts_with", BuiltinScalarFunction::TextStartsWith),
        ("trim", BuiltinScalarFunction::BTrim),
        ("btrim", BuiltinScalarFunction::BTrim),
        ("ltrim", BuiltinScalarFunction::LTrim),
        ("rtrim", BuiltinScalarFunction::RTrim),
        ("regexp_match", BuiltinScalarFunction::RegexpMatch),
        ("regexp_like", BuiltinScalarFunction::RegexpLike),
        ("regexp_replace", BuiltinScalarFunction::RegexpReplace),
        ("regexp_count", BuiltinScalarFunction::RegexpCount),
        ("regexp_instr", BuiltinScalarFunction::RegexpInstr),
        ("regexp_substr", BuiltinScalarFunction::RegexpSubstr),
        (
            "regexp_split_to_array",
            BuiltinScalarFunction::RegexpSplitToArray,
        ),
        ("get_bit", BuiltinScalarFunction::GetBit),
        ("set_bit", BuiltinScalarFunction::SetBit),
        ("get_byte", BuiltinScalarFunction::GetByte),
        ("set_byte", BuiltinScalarFunction::SetByte),
        ("bit_count", BuiltinScalarFunction::BitCount),
        ("encode", BuiltinScalarFunction::Encode),
        ("decode", BuiltinScalarFunction::Decode),
        ("convert", BuiltinScalarFunction::Convert),
        ("pg_convert", BuiltinScalarFunction::Convert),
        ("convert_from", BuiltinScalarFunction::ConvertFrom),
        ("convert_to", BuiltinScalarFunction::ConvertTo),
        ("md5", BuiltinScalarFunction::Md5),
        ("sha224", BuiltinScalarFunction::Sha224),
        ("sha256", BuiltinScalarFunction::Sha256),
        ("sha384", BuiltinScalarFunction::Sha384),
        ("sha512", BuiltinScalarFunction::Sha512),
        ("crc32", BuiltinScalarFunction::Crc32),
        ("crc32c", BuiltinScalarFunction::Crc32c),
        ("to_bin", BuiltinScalarFunction::ToBin),
        ("to_oct", BuiltinScalarFunction::ToOct),
        ("to_hex", BuiltinScalarFunction::ToHex),
        ("to_char", BuiltinScalarFunction::ToChar),
        ("to_date", BuiltinScalarFunction::ToDate),
        ("to_number", BuiltinScalarFunction::ToNumber),
        ("to_timestamp", BuiltinScalarFunction::ToTimestamp),
        ("abs", BuiltinScalarFunction::Abs),
        ("log", BuiltinScalarFunction::Log),
        ("dlog10", BuiltinScalarFunction::Log),
        ("numeric_log", BuiltinScalarFunction::Log),
        ("numeric_log10", BuiltinScalarFunction::Log),
        ("log10", BuiltinScalarFunction::Log10),
        ("gcd", BuiltinScalarFunction::Gcd),
        ("lcm", BuiltinScalarFunction::Lcm),
        ("greatest", BuiltinScalarFunction::Greatest),
        ("least", BuiltinScalarFunction::Least),
        ("div", BuiltinScalarFunction::Div),
        ("mod", BuiltinScalarFunction::Mod),
        ("scale", BuiltinScalarFunction::Scale),
        ("min_scale", BuiltinScalarFunction::MinScale),
        ("trim_scale", BuiltinScalarFunction::TrimScale),
        ("numeric_inc", BuiltinScalarFunction::NumericInc),
        ("int4mi", BuiltinScalarFunction::Int4Mi),
        ("int4pl", BuiltinScalarFunction::Int4Pl),
        ("int8inc", BuiltinScalarFunction::Int8Inc),
        ("int8inc_any", BuiltinScalarFunction::Int8IncAny),
        ("int4_avg_accum", BuiltinScalarFunction::Int4AvgAccum),
        ("int8_avg", BuiltinScalarFunction::Int8Avg),
        ("factorial", BuiltinScalarFunction::Factorial),
        ("pg_lsn", BuiltinScalarFunction::PgLsn),
        ("trunc", BuiltinScalarFunction::Trunc),
        ("macaddr_eq", BuiltinScalarFunction::MacAddrEq),
        ("macaddr_ne", BuiltinScalarFunction::MacAddrNe),
        ("macaddr_lt", BuiltinScalarFunction::MacAddrLt),
        ("macaddr_le", BuiltinScalarFunction::MacAddrLe),
        ("macaddr_gt", BuiltinScalarFunction::MacAddrGt),
        ("macaddr_ge", BuiltinScalarFunction::MacAddrGe),
        ("macaddr_cmp", BuiltinScalarFunction::MacAddrCmp),
        ("macaddr_not", BuiltinScalarFunction::MacAddrNot),
        ("macaddr_and", BuiltinScalarFunction::MacAddrAnd),
        ("macaddr_or", BuiltinScalarFunction::MacAddrOr),
        ("macaddr_trunc", BuiltinScalarFunction::MacAddrTrunc),
        (
            "macaddrtomacaddr8",
            BuiltinScalarFunction::MacAddrToMacAddr8,
        ),
        ("macaddr8_eq", BuiltinScalarFunction::MacAddr8Eq),
        ("macaddr8_ne", BuiltinScalarFunction::MacAddr8Ne),
        ("macaddr8_lt", BuiltinScalarFunction::MacAddr8Lt),
        ("macaddr8_le", BuiltinScalarFunction::MacAddr8Le),
        ("macaddr8_gt", BuiltinScalarFunction::MacAddr8Gt),
        ("macaddr8_ge", BuiltinScalarFunction::MacAddr8Ge),
        ("macaddr8_cmp", BuiltinScalarFunction::MacAddr8Cmp),
        ("macaddr8_not", BuiltinScalarFunction::MacAddr8Not),
        ("macaddr8_and", BuiltinScalarFunction::MacAddr8And),
        ("macaddr8_or", BuiltinScalarFunction::MacAddr8Or),
        ("macaddr8_trunc", BuiltinScalarFunction::MacAddr8Trunc),
        (
            "macaddr8tomacaddr",
            BuiltinScalarFunction::MacAddr8ToMacAddr,
        ),
        ("macaddr8_set7bit", BuiltinScalarFunction::MacAddr8Set7Bit),
        ("hashmacaddr", BuiltinScalarFunction::HashMacAddr),
        (
            "hashmacaddrextended",
            BuiltinScalarFunction::HashMacAddrExtended,
        ),
        ("hashmacaddr8", BuiltinScalarFunction::HashMacAddr8),
        (
            "hashmacaddr8extended",
            BuiltinScalarFunction::HashMacAddr8Extended,
        ),
        ("round", BuiltinScalarFunction::Round),
        ("numeric_round", BuiltinScalarFunction::Round),
        ("width_bucket", BuiltinScalarFunction::WidthBucket),
        ("ceil", BuiltinScalarFunction::Ceil),
        ("ceiling", BuiltinScalarFunction::Ceiling),
        ("floor", BuiltinScalarFunction::Floor),
        ("sign", BuiltinScalarFunction::Sign),
        ("sqrt", BuiltinScalarFunction::Sqrt),
        ("dsqrt", BuiltinScalarFunction::Sqrt),
        ("numeric_sqrt", BuiltinScalarFunction::Sqrt),
        ("cbrt", BuiltinScalarFunction::Cbrt),
        ("dcbrt", BuiltinScalarFunction::Cbrt),
        ("power", BuiltinScalarFunction::Power),
        ("pow", BuiltinScalarFunction::Power),
        ("dpow", BuiltinScalarFunction::Power),
        ("numeric_power", BuiltinScalarFunction::Power),
        ("exp", BuiltinScalarFunction::Exp),
        ("dexp", BuiltinScalarFunction::Exp),
        ("numeric_exp", BuiltinScalarFunction::Exp),
        ("ln", BuiltinScalarFunction::Ln),
        ("numeric_ln", BuiltinScalarFunction::Ln),
        ("sin", BuiltinScalarFunction::Sin),
        ("dsin", BuiltinScalarFunction::Sin),
        ("sinh", BuiltinScalarFunction::Sinh),
        ("cosh", BuiltinScalarFunction::Cosh),
        ("tanh", BuiltinScalarFunction::Tanh),
        ("asinh", BuiltinScalarFunction::Asinh),
        ("acosh", BuiltinScalarFunction::Acosh),
        ("atanh", BuiltinScalarFunction::Atanh),
        ("sind", BuiltinScalarFunction::Sind),
        ("cosd", BuiltinScalarFunction::Cosd),
        ("tand", BuiltinScalarFunction::Tand),
        ("cotd", BuiltinScalarFunction::Cotd),
        ("asind", BuiltinScalarFunction::Asind),
        ("acosd", BuiltinScalarFunction::Acosd),
        ("atand", BuiltinScalarFunction::Atand),
        ("atan2d", BuiltinScalarFunction::Atan2d),
        ("float4send", BuiltinScalarFunction::Float4Send),
        ("float8send", BuiltinScalarFunction::Float8Send),
        ("float8_accum", BuiltinScalarFunction::Float8Accum),
        ("float8_combine", BuiltinScalarFunction::Float8Combine),
        ("float8_regr_accum", BuiltinScalarFunction::Float8RegrAccum),
        (
            "float8_regr_combine",
            BuiltinScalarFunction::Float8RegrCombine,
        ),
        ("erf", BuiltinScalarFunction::Erf),
        ("erfc", BuiltinScalarFunction::Erfc),
        ("gamma", BuiltinScalarFunction::Gamma),
        ("lgamma", BuiltinScalarFunction::Lgamma),
        ("point", BuiltinScalarFunction::GeoPoint),
        ("construct_point", BuiltinScalarFunction::GeoPoint),
        ("circle_center", BuiltinScalarFunction::GeoPoint),
        ("lseg_center", BuiltinScalarFunction::GeoPoint),
        ("box_center", BuiltinScalarFunction::GeoPoint),
        ("poly_center", BuiltinScalarFunction::GeoPoint),
        ("box", BuiltinScalarFunction::GeoBox),
        ("points_box", BuiltinScalarFunction::GeoBox),
        ("point_box", BuiltinScalarFunction::GeoBox),
        ("poly_box", BuiltinScalarFunction::GeoBox),
        ("circle_box", BuiltinScalarFunction::GeoBox),
        ("line", BuiltinScalarFunction::GeoLine),
        ("lseg", BuiltinScalarFunction::GeoLseg),
        ("lseg_construct", BuiltinScalarFunction::GeoLseg),
        ("box_diagonal", BuiltinScalarFunction::GeoLseg),
        ("path", BuiltinScalarFunction::GeoPath),
        ("polygon", BuiltinScalarFunction::GeoPolygon),
        ("box_poly", BuiltinScalarFunction::GeoPolygon),
        ("path_poly", BuiltinScalarFunction::GeoPolygon),
        ("circle_poly", BuiltinScalarFunction::GeoPolygon),
        ("circle_poly_12", BuiltinScalarFunction::GeoPolygon),
        ("circle", BuiltinScalarFunction::GeoCircle),
        ("cr_circle", BuiltinScalarFunction::GeoCircle),
        ("poly_circle", BuiltinScalarFunction::GeoCircle),
        ("box_circle", BuiltinScalarFunction::GeoCircle),
        ("area", BuiltinScalarFunction::GeoArea),
        ("box_area", BuiltinScalarFunction::GeoArea),
        ("path_area", BuiltinScalarFunction::GeoArea),
        ("circle_area", BuiltinScalarFunction::GeoArea),
        ("center", BuiltinScalarFunction::GeoCenter),
        ("poly_center", BuiltinScalarFunction::GeoPolyCenter),
        ("poly_path", BuiltinScalarFunction::GeoPath),
        ("bound_box", BuiltinScalarFunction::GeoBoundBox),
        ("diagonal", BuiltinScalarFunction::GeoDiagonal),
        ("radius", BuiltinScalarFunction::GeoRadius),
        ("diameter", BuiltinScalarFunction::GeoDiameter),
        ("npoints", BuiltinScalarFunction::GeoNpoints),
        ("path_npoints", BuiltinScalarFunction::GeoNpoints),
        ("poly_npoints", BuiltinScalarFunction::GeoNpoints),
        ("pclose", BuiltinScalarFunction::GeoPclose),
        ("popen", BuiltinScalarFunction::GeoPopen),
        ("isopen", BuiltinScalarFunction::GeoIsOpen),
        ("isclosed", BuiltinScalarFunction::GeoIsClosed),
        ("slope", BuiltinScalarFunction::GeoSlope),
        ("isvertical", BuiltinScalarFunction::GeoIsVertical),
        ("point_vert", BuiltinScalarFunction::GeoIsVertical),
        ("lseg_vertical", BuiltinScalarFunction::GeoIsVertical),
        ("line_vertical", BuiltinScalarFunction::GeoIsVertical),
        ("ishorizontal", BuiltinScalarFunction::GeoIsHorizontal),
        ("point_horiz", BuiltinScalarFunction::GeoIsHorizontal),
        ("lseg_horizontal", BuiltinScalarFunction::GeoIsHorizontal),
        ("line_horizontal", BuiltinScalarFunction::GeoIsHorizontal),
        ("height", BuiltinScalarFunction::GeoHeight),
        ("width", BuiltinScalarFunction::GeoWidth),
        ("booleq", BuiltinScalarFunction::BoolEq),
        ("boolne", BuiltinScalarFunction::BoolNe),
        ("booland_statefunc", BuiltinScalarFunction::BoolAndStateFunc),
        ("boolor_statefunc", BuiltinScalarFunction::BoolOrStateFunc),
        (
            "bitcast_integer_to_float4",
            BuiltinScalarFunction::BitcastIntegerToFloat4,
        ),
        (
            "bitcast_bigint_to_float8",
            BuiltinScalarFunction::BitcastBigintToFloat8,
        ),
        ("xmlcomment", BuiltinScalarFunction::XmlComment),
        ("xmltext", BuiltinScalarFunction::XmlText),
        ("xml_is_well_formed", BuiltinScalarFunction::XmlIsWellFormed),
        (
            "xml_is_well_formed_document",
            BuiltinScalarFunction::XmlIsWellFormedDocument,
        ),
        (
            "xml_is_well_formed_content",
            BuiltinScalarFunction::XmlIsWellFormedContent,
        ),
        ("xpath", BuiltinScalarFunction::XPath),
        ("xpath_exists", BuiltinScalarFunction::XPathExists),
        ("xmlexists", BuiltinScalarFunction::XPathExists),
        ("pg_input_is_valid", BuiltinScalarFunction::PgInputIsValid),
        (
            "pg_input_error_message",
            BuiltinScalarFunction::PgInputErrorMessage,
        ),
        (
            "pg_input_error_detail",
            BuiltinScalarFunction::PgInputErrorDetail,
        ),
        (
            "pg_input_error_hint",
            BuiltinScalarFunction::PgInputErrorHint,
        ),
        (
            "pg_input_error_sqlstate",
            BuiltinScalarFunction::PgInputErrorSqlState,
        ),
        ("range_constructor", BuiltinScalarFunction::RangeConstructor),
        (
            "range_constructor2",
            BuiltinScalarFunction::RangeConstructor,
        ),
        (
            "range_constructor3",
            BuiltinScalarFunction::RangeConstructor,
        ),
        ("range_isempty", BuiltinScalarFunction::RangeIsEmpty),
        ("range_lower", BuiltinScalarFunction::RangeLower),
        ("range_upper", BuiltinScalarFunction::RangeUpper),
        ("range_lower_inc", BuiltinScalarFunction::RangeLowerInc),
        ("range_upper_inc", BuiltinScalarFunction::RangeUpperInc),
        ("range_lower_inf", BuiltinScalarFunction::RangeLowerInf),
        ("range_upper_inf", BuiltinScalarFunction::RangeUpperInf),
        ("range_contains", BuiltinScalarFunction::RangeContains),
        ("range_contains_elem", BuiltinScalarFunction::RangeContains),
        (
            "range_contained_by",
            BuiltinScalarFunction::RangeContainedBy,
        ),
        (
            "elem_contained_by_range",
            BuiltinScalarFunction::RangeContainedBy,
        ),
        ("range_overlaps", BuiltinScalarFunction::RangeOverlap),
        ("range_overlap", BuiltinScalarFunction::RangeOverlap),
        ("range_before", BuiltinScalarFunction::RangeStrictLeft),
        ("range_strict_left", BuiltinScalarFunction::RangeStrictLeft),
        ("range_after", BuiltinScalarFunction::RangeStrictRight),
        (
            "range_strict_right",
            BuiltinScalarFunction::RangeStrictRight,
        ),
        ("range_over_left", BuiltinScalarFunction::RangeOverLeft),
        ("range_over_right", BuiltinScalarFunction::RangeOverRight),
        ("range_adjacent", BuiltinScalarFunction::RangeAdjacent),
        ("range_union", BuiltinScalarFunction::RangeUnion),
        ("range_intersect", BuiltinScalarFunction::RangeIntersect),
        ("range_difference", BuiltinScalarFunction::RangeDifference),
        ("range_merge", BuiltinScalarFunction::RangeMerge),
    ]
}

fn json_table_functions_by_name() -> &'static BTreeMap<String, JsonTableFunction> {
    static FUNCTIONS: OnceLock<BTreeMap<String, JsonTableFunction>> = OnceLock::new();
    FUNCTIONS.get_or_init(|| {
        let mut by_name = BTreeMap::new();
        for row in bootstrap_pg_proc_rows_ref() {
            if row.prokind != 'f' || !row.proretset {
                continue;
            }
            if let Some(func) = legacy_json_table_function_entries()
                .iter()
                .find_map(|(name, func)| row.proname.eq_ignore_ascii_case(name).then_some(*func))
            {
                by_name.insert(row.proname.to_ascii_lowercase(), func);
            }
        }
        for (name, func) in legacy_json_table_function_entries() {
            by_name.entry((*name).into()).or_insert(*func);
        }
        by_name
    })
}

fn legacy_json_table_function_entries() -> &'static [(&'static str, JsonTableFunction)] {
    &[
        ("json_object_keys", JsonTableFunction::ObjectKeys),
        ("json_each", JsonTableFunction::Each),
        ("json_each_text", JsonTableFunction::EachText),
        ("json_array_elements", JsonTableFunction::ArrayElements),
        (
            "json_array_elements_text",
            JsonTableFunction::ArrayElementsText,
        ),
        ("jsonb_path_query", JsonTableFunction::JsonbPathQuery),
        ("jsonb_path_query_tz", JsonTableFunction::JsonbPathQueryTz),
        ("jsonb_object_keys", JsonTableFunction::JsonbObjectKeys),
        ("jsonb_each", JsonTableFunction::JsonbEach),
        ("jsonb_each_text", JsonTableFunction::JsonbEachText),
        (
            "jsonb_array_elements",
            JsonTableFunction::JsonbArrayElements,
        ),
        (
            "jsonb_array_elements_text",
            JsonTableFunction::JsonbArrayElementsText,
        ),
    ]
}

fn function_cast_type_aliases() -> &'static [(&'static str, &'static str)] {
    &[
        ("smallint", "int2"),
        ("int", "int4"),
        ("integer", "int4"),
        ("bigint", "int8"),
        ("bit varying", "varbit"),
        ("real", "float4"),
        ("decimal", "numeric"),
        ("boolean", "bool"),
    ]
}

fn scalar_function_arity_overrides() -> &'static Vec<(BuiltinScalarFunction, ScalarFunctionArity)> {
    static ARITIES: OnceLock<Vec<(BuiltinScalarFunction, ScalarFunctionArity)>> = OnceLock::new();
    ARITIES.get_or_init(|| {
        let mut by_func = Vec::new();
        let mut overloaded = Vec::new();
        for row in bootstrap_pg_proc_rows_ref() {
            if row.prokind != 'f' || row.proretset || row.provariadic != 0 {
                continue;
            }
            if let Some(func) = builtin_scalar_function_for_proc_src(&row.prosrc) {
                if !supports_exact_proc_arity(func) {
                    continue;
                }
                let arity = ScalarFunctionArity::Exact(row.pronargs.max(0) as usize);
                if let Some((_, existing)) =
                    by_func.iter().find(|(candidate, _)| *candidate == func)
                {
                    if *existing != arity && !overloaded.contains(&func) {
                        overloaded.push(func);
                    }
                    continue;
                }
                if !overloaded.contains(&func) {
                    by_func.push((func, arity));
                }
            }
        }
        by_func.retain(|(func, _)| !overloaded.contains(func));
        by_func
    })
}

fn scalar_fixed_return_types() -> &'static Vec<(BuiltinScalarFunction, SqlType)> {
    static TYPES: OnceLock<Vec<(BuiltinScalarFunction, SqlType)>> = OnceLock::new();
    TYPES.get_or_init(|| {
        let mut by_func = Vec::new();
        for row in bootstrap_pg_proc_rows_ref() {
            if row.prokind != 'f' || row.proretset {
                continue;
            }
            let Some(func) = builtin_scalar_function_for_proc_src(&row.prosrc) else {
                continue;
            };
            if !supports_fixed_scalar_return_type(func) {
                continue;
            }
            let Some(sql_type) = builtin_sql_type_for_oid(row.prorettype) else {
                continue;
            };
            if by_func.iter().all(|(candidate, _)| *candidate != func) {
                by_func.push((func, sql_type));
            }
        }
        if by_func
            .iter()
            .all(|(candidate, _)| *candidate != BuiltinScalarFunction::Unistr)
        {
            by_func.push((
                BuiltinScalarFunction::Unistr,
                SqlType::new(SqlTypeKind::Text),
            ));
        }
        if by_func
            .iter()
            .all(|(candidate, _)| *candidate != BuiltinScalarFunction::ArrayNdims)
        {
            by_func.push((
                BuiltinScalarFunction::ArrayNdims,
                SqlType::new(SqlTypeKind::Int4),
            ));
        }
        if by_func
            .iter()
            .all(|(candidate, _)| *candidate != BuiltinScalarFunction::ArrayDims)
        {
            by_func.push((
                BuiltinScalarFunction::ArrayDims,
                SqlType::new(SqlTypeKind::Text),
            ));
        }
        if by_func
            .iter()
            .all(|(candidate, _)| *candidate != BuiltinScalarFunction::ArrayLower)
        {
            by_func.push((
                BuiltinScalarFunction::ArrayLower,
                SqlType::new(SqlTypeKind::Int4),
            ));
        }
        if by_func
            .iter()
            .all(|(candidate, _)| *candidate != BuiltinScalarFunction::ArrayUpper)
        {
            by_func.push((
                BuiltinScalarFunction::ArrayUpper,
                SqlType::new(SqlTypeKind::Int4),
            ));
        }
        for func in [
            BuiltinScalarFunction::PgGetSerialSequence,
            BuiltinScalarFunction::PgGetAcl,
            BuiltinScalarFunction::ObjDescription,
            BuiltinScalarFunction::PgDescribeObject,
            BuiltinScalarFunction::PgGetFunctionArguments,
            BuiltinScalarFunction::PgGetFunctionDef,
            BuiltinScalarFunction::PgGetFunctionResult,
            BuiltinScalarFunction::PgGetExpr,
            BuiltinScalarFunction::PgGetPartKeyDef,
            BuiltinScalarFunction::PgGetConstraintDef,
            BuiltinScalarFunction::PgGetPartitionConstraintDef,
            BuiltinScalarFunction::PgGetIndexDef,
            BuiltinScalarFunction::PgGetRuleDef,
            BuiltinScalarFunction::PgGetViewDef,
            BuiltinScalarFunction::PgGetStatisticsObjDef,
            BuiltinScalarFunction::PgGetStatisticsObjDefColumns,
        ] {
            if by_func.iter().all(|(candidate, _)| *candidate != func) {
                by_func.push((
                    func,
                    if func == BuiltinScalarFunction::PgGetAcl {
                        SqlType::array_of(SqlType::new(SqlTypeKind::Text))
                    } else {
                        SqlType::new(SqlTypeKind::Text)
                    },
                ));
            }
        }
        if by_func.iter().all(|(candidate, _)| {
            *candidate != BuiltinScalarFunction::PgGetStatisticsObjDefExpressions
        }) {
            by_func.push((
                BuiltinScalarFunction::PgGetStatisticsObjDefExpressions,
                SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
            ));
        }
        if by_func
            .iter()
            .all(|(candidate, _)| *candidate != BuiltinScalarFunction::PgStatisticsObjIsVisible)
        {
            by_func.push((
                BuiltinScalarFunction::PgStatisticsObjIsVisible,
                SqlType::new(SqlTypeKind::Bool),
            ));
        }
        if by_func
            .iter()
            .all(|(candidate, _)| *candidate != BuiltinScalarFunction::PgFunctionIsVisible)
        {
            by_func.push((
                BuiltinScalarFunction::PgFunctionIsVisible,
                SqlType::new(SqlTypeKind::Bool),
            ));
        }
        if by_func
            .iter()
            .all(|(candidate, _)| *candidate != BuiltinScalarFunction::RowSecurityActive)
        {
            by_func.push((
                BuiltinScalarFunction::RowSecurityActive,
                SqlType::new(SqlTypeKind::Bool),
            ));
        }
        if by_func
            .iter()
            .all(|(candidate, _)| *candidate != BuiltinScalarFunction::PgGetUserById)
        {
            by_func.push((
                BuiltinScalarFunction::PgGetUserById,
                SqlType::new(SqlTypeKind::Name),
            ));
        }
        if by_func
            .iter()
            .all(|(candidate, _)| *candidate != BuiltinScalarFunction::PgRelationIsPublishable)
        {
            by_func.push((
                BuiltinScalarFunction::PgRelationIsPublishable,
                SqlType::new(SqlTypeKind::Bool),
            ));
        }
        if by_func
            .iter()
            .all(|(candidate, _)| *candidate != BuiltinScalarFunction::PgColumnSize)
        {
            by_func.push((
                BuiltinScalarFunction::PgColumnSize,
                SqlType::new(SqlTypeKind::Int4),
            ));
        }
        if by_func
            .iter()
            .all(|(candidate, _)| *candidate != BuiltinScalarFunction::PgRelationSize)
        {
            by_func.push((
                BuiltinScalarFunction::PgRelationSize,
                SqlType::new(SqlTypeKind::Int8),
            ));
        }
        if by_func
            .iter()
            .all(|(candidate, _)| *candidate != BuiltinScalarFunction::PgNotify)
        {
            by_func.push((
                BuiltinScalarFunction::PgNotify,
                SqlType::new(SqlTypeKind::Void),
            ));
        }
        if by_func
            .iter()
            .all(|(candidate, _)| *candidate != BuiltinScalarFunction::PgNotificationQueueUsage)
        {
            by_func.push((
                BuiltinScalarFunction::PgNotificationQueueUsage,
                SqlType::new(SqlTypeKind::Float8),
            ));
        }
        for func in [
            BuiltinScalarFunction::PgIndexAmHasProperty,
            BuiltinScalarFunction::PgIndexHasProperty,
            BuiltinScalarFunction::PgIndexColumnHasProperty,
            BuiltinScalarFunction::PgTypeIsVisible,
            BuiltinScalarFunction::BoolAndStateFunc,
            BuiltinScalarFunction::BoolOrStateFunc,
        ] {
            if by_func.iter().all(|(candidate, _)| *candidate != func) {
                by_func.push((func, SqlType::new(SqlTypeKind::Bool)));
            }
        }
        for func in [
            BuiltinScalarFunction::Now,
            BuiltinScalarFunction::TransactionTimestamp,
            BuiltinScalarFunction::StatementTimestamp,
            BuiltinScalarFunction::ClockTimestamp,
        ] {
            if by_func.iter().all(|(candidate, _)| *candidate != func) {
                by_func.push((func, SqlType::new(SqlTypeKind::TimestampTz)));
            }
        }
        if by_func
            .iter()
            .all(|(candidate, _)| *candidate != BuiltinScalarFunction::TimeOfDay)
        {
            by_func.push((
                BuiltinScalarFunction::TimeOfDay,
                SqlType::new(SqlTypeKind::Text),
            ));
        }
        if by_func
            .iter()
            .all(|(candidate, _)| *candidate != BuiltinScalarFunction::PgSleep)
        {
            by_func.push((
                BuiltinScalarFunction::PgSleep,
                SqlType::new(SqlTypeKind::Void),
            ));
        }
        if by_func
            .iter()
            .all(|(candidate, _)| *candidate != BuiltinScalarFunction::XmlComment)
        {
            by_func.push((
                BuiltinScalarFunction::XmlComment,
                SqlType::new(SqlTypeKind::Xml),
            ));
        }
        if by_func
            .iter()
            .all(|(candidate, _)| *candidate != BuiltinScalarFunction::XmlText)
        {
            by_func.push((
                BuiltinScalarFunction::XmlText,
                SqlType::new(SqlTypeKind::Xml),
            ));
        }
        for func in [
            BuiltinScalarFunction::XmlIsWellFormed,
            BuiltinScalarFunction::XmlIsWellFormedDocument,
            BuiltinScalarFunction::XmlIsWellFormedContent,
        ] {
            if by_func.iter().all(|(candidate, _)| *candidate != func) {
                by_func.push((func, SqlType::new(SqlTypeKind::Bool)));
            }
        }
        by_func
    })
}

fn supports_fixed_scalar_return_type(func: BuiltinScalarFunction) -> bool {
    matches!(
        func,
        BuiltinScalarFunction::TsMatch
            | BuiltinScalarFunction::TsQueryContains
            | BuiltinScalarFunction::TsQueryContainedBy
            | BuiltinScalarFunction::TsQueryAnd
            | BuiltinScalarFunction::TsQueryOr
            | BuiltinScalarFunction::TsQueryNot
            | BuiltinScalarFunction::TsQueryPhrase
            | BuiltinScalarFunction::TsQueryNumnode
            | BuiltinScalarFunction::TsRewrite
            | BuiltinScalarFunction::TsHeadline
            | BuiltinScalarFunction::TsVectorIn
            | BuiltinScalarFunction::TsVectorOut
            | BuiltinScalarFunction::TsQueryIn
            | BuiltinScalarFunction::TsQueryOut
            | BuiltinScalarFunction::TsVectorConcat
            | BuiltinScalarFunction::TsVectorStrip
            | BuiltinScalarFunction::TsVectorDelete
            | BuiltinScalarFunction::TsVectorToArray
            | BuiltinScalarFunction::ArrayToTsVector
            | BuiltinScalarFunction::TsVectorSetWeight
            | BuiltinScalarFunction::TsVectorFilter
            | BuiltinScalarFunction::TsRank
            | BuiltinScalarFunction::TsRankCd
            | BuiltinScalarFunction::RandomNormal
            | BuiltinScalarFunction::SetSeed
            | BuiltinScalarFunction::UuidIn
            | BuiltinScalarFunction::UuidOut
            | BuiltinScalarFunction::UuidRecv
            | BuiltinScalarFunction::UuidSend
            | BuiltinScalarFunction::UuidEq
            | BuiltinScalarFunction::UuidNe
            | BuiltinScalarFunction::UuidLt
            | BuiltinScalarFunction::UuidLe
            | BuiltinScalarFunction::UuidGt
            | BuiltinScalarFunction::UuidGe
            | BuiltinScalarFunction::UuidCmp
            | BuiltinScalarFunction::Xid8Cmp
            | BuiltinScalarFunction::UuidHash
            | BuiltinScalarFunction::UuidHashExtended
            | BuiltinScalarFunction::HashValue(_)
            | BuiltinScalarFunction::HashValueExtended(_)
            | BuiltinScalarFunction::GenRandomUuid
            | BuiltinScalarFunction::UuidV7
            | BuiltinScalarFunction::UuidExtractVersion
            | BuiltinScalarFunction::UuidExtractTimestamp
            | BuiltinScalarFunction::CashLarger
            | BuiltinScalarFunction::CashSmaller
            | BuiltinScalarFunction::CashWords
            | BuiltinScalarFunction::Now
            | BuiltinScalarFunction::TransactionTimestamp
            | BuiltinScalarFunction::StatementTimestamp
            | BuiltinScalarFunction::ClockTimestamp
            | BuiltinScalarFunction::TimeOfDay
            | BuiltinScalarFunction::CurrentDatabase
            | BuiltinScalarFunction::CurrentSchemas
            | BuiltinScalarFunction::PgBackendPid
            | BuiltinScalarFunction::PgPartitionRoot
            | BuiltinScalarFunction::SatisfiesHashPartition
            | BuiltinScalarFunction::PgGetPartKeyDef
            | BuiltinScalarFunction::PgTableIsVisible
            | BuiltinScalarFunction::PgTypeIsVisible
            | BuiltinScalarFunction::PgOperatorIsVisible
            | BuiltinScalarFunction::PgOpclassIsVisible
            | BuiltinScalarFunction::PgOpfamilyIsVisible
            | BuiltinScalarFunction::PgConversionIsVisible
            | BuiltinScalarFunction::PgTsParserIsVisible
            | BuiltinScalarFunction::PgTsDictIsVisible
            | BuiltinScalarFunction::PgTsTemplateIsVisible
            | BuiltinScalarFunction::PgTsConfigIsVisible
            | BuiltinScalarFunction::NextVal
            | BuiltinScalarFunction::CurrVal
            | BuiltinScalarFunction::LastVal
            | BuiltinScalarFunction::CurrTid2
            | BuiltinScalarFunction::SetVal
            | BuiltinScalarFunction::PgGetSerialSequence
            | BuiltinScalarFunction::PgSequenceParameters
            | BuiltinScalarFunction::PgSequenceLastValue
            | BuiltinScalarFunction::PgGetSequenceData
            | BuiltinScalarFunction::PgGetAcl
            | BuiltinScalarFunction::MakeAclItem
            | BuiltinScalarFunction::PgGetUserById
            | BuiltinScalarFunction::ObjDescription
            | BuiltinScalarFunction::PgDescribeObject
            | BuiltinScalarFunction::PgIdentifyObject
            | BuiltinScalarFunction::PgIdentifyObjectAsAddress
            | BuiltinScalarFunction::PgGetObjectAddress
            | BuiltinScalarFunction::PgGetFunctionArguments
            | BuiltinScalarFunction::PgGetFunctionDef
            | BuiltinScalarFunction::PgGetFunctionResult
            | BuiltinScalarFunction::PgGetExpr
            | BuiltinScalarFunction::PgGetConstraintDef
            | BuiltinScalarFunction::PgGetPartitionConstraintDef
            | BuiltinScalarFunction::PgGetIndexDef
            | BuiltinScalarFunction::PgGetRuleDef
            | BuiltinScalarFunction::PgGetViewDef
            | BuiltinScalarFunction::PgGetStatisticsObjDef
            | BuiltinScalarFunction::PgGetStatisticsObjDefColumns
            | BuiltinScalarFunction::PgGetStatisticsObjDefExpressions
            | BuiltinScalarFunction::PgStatisticsObjIsVisible
            | BuiltinScalarFunction::PgFunctionIsVisible
            | BuiltinScalarFunction::RowSecurityActive
            | BuiltinScalarFunction::PgColumnSize
            | BuiltinScalarFunction::PgRelationSize
            | BuiltinScalarFunction::PgTableSize
            | BuiltinScalarFunction::PgTablespaceLocation
            | BuiltinScalarFunction::PgRelationIsPublishable
            | BuiltinScalarFunction::PgIndexAmHasProperty
            | BuiltinScalarFunction::PgIndexHasProperty
            | BuiltinScalarFunction::PgIndexColumnHasProperty
            | BuiltinScalarFunction::PgSizePretty
            | BuiltinScalarFunction::PgSizeBytes
            | BuiltinScalarFunction::PgAdvisoryLock
            | BuiltinScalarFunction::PgAdvisoryXactLock
            | BuiltinScalarFunction::PgAdvisoryLockShared
            | BuiltinScalarFunction::PgAdvisoryXactLockShared
            | BuiltinScalarFunction::PgTryAdvisoryLock
            | BuiltinScalarFunction::PgTryAdvisoryXactLock
            | BuiltinScalarFunction::PgTryAdvisoryLockShared
            | BuiltinScalarFunction::PgTryAdvisoryXactLockShared
            | BuiltinScalarFunction::PgAdvisoryUnlock
            | BuiltinScalarFunction::PgAdvisoryUnlockShared
            | BuiltinScalarFunction::PgAdvisoryUnlockAll
            | BuiltinScalarFunction::GetDatabaseEncoding
            | BuiltinScalarFunction::UnicodeVersion
            | BuiltinScalarFunction::UnicodeAssigned
            | BuiltinScalarFunction::Normalize
            | BuiltinScalarFunction::IsNormalized
            | BuiltinScalarFunction::PgCharToEncoding
            | BuiltinScalarFunction::PgEncodingToChar
            | BuiltinScalarFunction::PgMyTempSchema
            | BuiltinScalarFunction::PgRustTestFdwHandler
            | BuiltinScalarFunction::AmValidate
            | BuiltinScalarFunction::BtEqualImage
            | BuiltinScalarFunction::PgNotify
            | BuiltinScalarFunction::PgNotificationQueueUsage
            | BuiltinScalarFunction::PgStatGetCheckpointerNumTimed
            | BuiltinScalarFunction::PgStatGetCheckpointerNumRequested
            | BuiltinScalarFunction::PgStatGetCheckpointerNumPerformed
            | BuiltinScalarFunction::PgStatGetCheckpointerBuffersWritten
            | BuiltinScalarFunction::PgStatGetCheckpointerSlruWritten
            | BuiltinScalarFunction::PgStatGetCheckpointerWriteTime
            | BuiltinScalarFunction::PgStatGetCheckpointerSyncTime
            | BuiltinScalarFunction::PgStatGetCheckpointerStatResetTime
            | BuiltinScalarFunction::PgStatReset
            | BuiltinScalarFunction::PgStatResetShared
            | BuiltinScalarFunction::PgStatResetSingleTableCounters
            | BuiltinScalarFunction::PgStatResetSingleFunctionCounters
            | BuiltinScalarFunction::PgStatResetBackendStats
            | BuiltinScalarFunction::PgStatResetSlru
            | BuiltinScalarFunction::PgStatResetReplicationSlot
            | BuiltinScalarFunction::PgStatResetSubscriptionStats
            | BuiltinScalarFunction::PgStatGetBackendPid
            | BuiltinScalarFunction::PgStatGetBackendWal
            | BuiltinScalarFunction::PgStatGetReplicationSlot
            | BuiltinScalarFunction::PgStatGetSubscriptionStats
            | BuiltinScalarFunction::ShobjDescription
            | BuiltinScalarFunction::PgRestoreRelationStats
            | BuiltinScalarFunction::PgClearRelationStats
            | BuiltinScalarFunction::PgRestoreAttributeStats
            | BuiltinScalarFunction::PgClearAttributeStats
            | BuiltinScalarFunction::ToJson
            | BuiltinScalarFunction::ToJsonb
            | BuiltinScalarFunction::ArrayToJson
            | BuiltinScalarFunction::JsonBuildArray
            | BuiltinScalarFunction::JsonBuildObject
            | BuiltinScalarFunction::JsonObject
            | BuiltinScalarFunction::JsonStripNulls
            | BuiltinScalarFunction::JsonTypeof
            | BuiltinScalarFunction::JsonArrayLength
            | BuiltinScalarFunction::JsonExtractPath
            | BuiltinScalarFunction::JsonExtractPathText
            | BuiltinScalarFunction::JsonbObject
            | BuiltinScalarFunction::JsonbStripNulls
            | BuiltinScalarFunction::JsonbPretty
            | BuiltinScalarFunction::JsonbTypeof
            | BuiltinScalarFunction::JsonbArrayLength
            | BuiltinScalarFunction::JsonbExtractPath
            | BuiltinScalarFunction::JsonbExtractPathText
            | BuiltinScalarFunction::JsonbBuildArray
            | BuiltinScalarFunction::JsonbBuildObject
            | BuiltinScalarFunction::JsonbConcat
            | BuiltinScalarFunction::JsonbDelete
            | BuiltinScalarFunction::JsonbDeletePath
            | BuiltinScalarFunction::JsonbSet
            | BuiltinScalarFunction::JsonbSetLax
            | BuiltinScalarFunction::JsonbInsert
            | BuiltinScalarFunction::JsonbPathExists
            | BuiltinScalarFunction::JsonbPathMatch
            | BuiltinScalarFunction::JsonbPathQueryArray
            | BuiltinScalarFunction::JsonbPathQueryFirst
            | BuiltinScalarFunction::Initcap
            | BuiltinScalarFunction::Left
            | BuiltinScalarFunction::LPad
            | BuiltinScalarFunction::RPad
            | BuiltinScalarFunction::Repeat
            | BuiltinScalarFunction::Length
            | BuiltinScalarFunction::OctetLength
            | BuiltinScalarFunction::BitLength
            | BuiltinScalarFunction::ArrayNdims
            | BuiltinScalarFunction::ArrayDims
            | BuiltinScalarFunction::ArrayLower
            | BuiltinScalarFunction::ArrayUpper
            | BuiltinScalarFunction::Lower
            | BuiltinScalarFunction::Upper
            | BuiltinScalarFunction::Unistr
            | BuiltinScalarFunction::Ascii
            | BuiltinScalarFunction::Chr
            | BuiltinScalarFunction::Replace
            | BuiltinScalarFunction::SplitPart
            | BuiltinScalarFunction::Translate
            | BuiltinScalarFunction::Strpos
            | BuiltinScalarFunction::Position
            | BuiltinScalarFunction::SimilarSubstring
            | BuiltinScalarFunction::BTrim
            | BuiltinScalarFunction::LTrim
            | BuiltinScalarFunction::RTrim
            | BuiltinScalarFunction::Reverse
            | BuiltinScalarFunction::Convert
            | BuiltinScalarFunction::ConvertFrom
            | BuiltinScalarFunction::ConvertTo
            | BuiltinScalarFunction::Encode
            | BuiltinScalarFunction::Decode
            | BuiltinScalarFunction::Md5
            | BuiltinScalarFunction::Sha224
            | BuiltinScalarFunction::Sha256
            | BuiltinScalarFunction::Sha384
            | BuiltinScalarFunction::Sha512
            | BuiltinScalarFunction::Crc32
            | BuiltinScalarFunction::Crc32c
            | BuiltinScalarFunction::ToBin
            | BuiltinScalarFunction::ToOct
            | BuiltinScalarFunction::ToHex
            | BuiltinScalarFunction::ToChar
            | BuiltinScalarFunction::ToDate
            | BuiltinScalarFunction::ToNumber
            | BuiltinScalarFunction::TimestampTzConstructor
            | BuiltinScalarFunction::ToTimestamp
            | BuiltinScalarFunction::Age
            | BuiltinScalarFunction::RegexpMatch
            | BuiltinScalarFunction::RegexpReplace
            | BuiltinScalarFunction::RegexpCount
            | BuiltinScalarFunction::RegexpInstr
            | BuiltinScalarFunction::RegexpSubstr
            | BuiltinScalarFunction::RegexpSplitToArray
            | BuiltinScalarFunction::Scale
            | BuiltinScalarFunction::MinScale
            | BuiltinScalarFunction::TrimScale
            | BuiltinScalarFunction::NumericInc
            | BuiltinScalarFunction::Factorial
            | BuiltinScalarFunction::PgLsn
            | BuiltinScalarFunction::Div
            | BuiltinScalarFunction::Mod
            | BuiltinScalarFunction::WidthBucket
            | BuiltinScalarFunction::GetBit
            | BuiltinScalarFunction::GetByte
            | BuiltinScalarFunction::BitCount
            | BuiltinScalarFunction::Float4Send
            | BuiltinScalarFunction::Float8Send
            | BuiltinScalarFunction::BoolEq
            | BuiltinScalarFunction::BoolNe
            | BuiltinScalarFunction::BoolAndStateFunc
            | BuiltinScalarFunction::BoolOrStateFunc
            | BuiltinScalarFunction::BitcastIntegerToFloat4
            | BuiltinScalarFunction::BitcastBigintToFloat8
            | BuiltinScalarFunction::XmlComment
            | BuiltinScalarFunction::XmlText
            | BuiltinScalarFunction::XmlIsWellFormed
            | BuiltinScalarFunction::XmlIsWellFormedDocument
            | BuiltinScalarFunction::XmlIsWellFormedContent
            | BuiltinScalarFunction::XPath
            | BuiltinScalarFunction::XPathExists
            | BuiltinScalarFunction::PgInputIsValid
            | BuiltinScalarFunction::PgInputErrorMessage
            | BuiltinScalarFunction::PgInputErrorDetail
            | BuiltinScalarFunction::PgInputErrorHint
            | BuiltinScalarFunction::PgInputErrorSqlState
    )
}

fn supports_exact_proc_arity(func: BuiltinScalarFunction) -> bool {
    !matches!(
        func,
        BuiltinScalarFunction::Concat
            | BuiltinScalarFunction::ConcatWs
            | BuiltinScalarFunction::Format
            | BuiltinScalarFunction::Log
            | BuiltinScalarFunction::DateTrunc
            | BuiltinScalarFunction::Trunc
            | BuiltinScalarFunction::Round
            | BuiltinScalarFunction::Substring
            | BuiltinScalarFunction::SimilarSubstring
            | BuiltinScalarFunction::Overlay
            | BuiltinScalarFunction::LPad
            | BuiltinScalarFunction::RPad
            | BuiltinScalarFunction::ObjDescription
            | BuiltinScalarFunction::BTrim
            | BuiltinScalarFunction::LTrim
            | BuiltinScalarFunction::RTrim
            | BuiltinScalarFunction::RegexpMatch
            | BuiltinScalarFunction::RegexpReplace
            | BuiltinScalarFunction::RegexpCount
            | BuiltinScalarFunction::RegexpInstr
            | BuiltinScalarFunction::RegexpSubstr
            | BuiltinScalarFunction::RegexpSplitToArray
            | BuiltinScalarFunction::ArrayToJson
            | BuiltinScalarFunction::JsonBuildArray
            | BuiltinScalarFunction::JsonBuildObject
            | BuiltinScalarFunction::JsonObject
            | BuiltinScalarFunction::JsonStripNulls
            | BuiltinScalarFunction::JsonExtractPath
            | BuiltinScalarFunction::JsonExtractPathText
            | BuiltinScalarFunction::JsonbObject
            | BuiltinScalarFunction::JsonbStripNulls
            | BuiltinScalarFunction::JsonbExtractPath
            | BuiltinScalarFunction::JsonbExtractPathText
            | BuiltinScalarFunction::JsonbBuildArray
            | BuiltinScalarFunction::JsonbBuildObject
            | BuiltinScalarFunction::JsonbDelete
            | BuiltinScalarFunction::JsonbDeletePath
            | BuiltinScalarFunction::JsonbSet
            | BuiltinScalarFunction::JsonbSetLax
            | BuiltinScalarFunction::JsonbInsert
            | BuiltinScalarFunction::JsonbPathExists
            | BuiltinScalarFunction::JsonbPathMatch
            | BuiltinScalarFunction::JsonbPathQueryArray
            | BuiltinScalarFunction::JsonbPathQueryFirst
            | BuiltinScalarFunction::TsVectorIn
            | BuiltinScalarFunction::TsQueryIn
    )
}

fn aggregate_arity_overrides() -> &'static Vec<(AggFunc, usize)> {
    static ARITIES: OnceLock<Vec<(AggFunc, usize)>> = OnceLock::new();
    ARITIES.get_or_init(|| {
        let mut by_func = Vec::new();
        for row in bootstrap_pg_proc_rows_ref() {
            if row.prokind != 'a' {
                continue;
            }
            let Some(func) = aggregate_func_for_proname(&row.proname) else {
                continue;
            };
            if func == AggFunc::Count || by_func.iter().any(|(candidate, _)| *candidate == func) {
                continue;
            }
            by_func.push((func, row.pronargs.max(0) as usize));
        }
        by_func
    })
}

fn aggregate_fixed_return_types() -> &'static Vec<(AggFunc, SqlType)> {
    static TYPES: OnceLock<Vec<(AggFunc, SqlType)>> = OnceLock::new();
    TYPES.get_or_init(|| {
        let mut by_func = Vec::new();
        for row in bootstrap_pg_proc_rows_ref() {
            if row.prokind != 'a' {
                continue;
            }
            let Some(func) = aggregate_func_for_proname(&row.proname) else {
                continue;
            };
            if !supports_fixed_aggregate_return_type(func) {
                continue;
            }
            let Some(sql_type) = builtin_sql_type_for_oid(row.prorettype) else {
                continue;
            };
            if by_func.iter().all(|(candidate, _)| *candidate != func) {
                by_func.push((func, sql_type));
            }
        }
        if by_func
            .iter()
            .all(|(candidate, _)| *candidate != AggFunc::XmlAgg)
        {
            by_func.push((AggFunc::XmlAgg, SqlType::new(SqlTypeKind::Xml)));
        }
        by_func
    })
}

fn supports_fixed_aggregate_return_type(func: AggFunc) -> bool {
    matches!(
        func,
        AggFunc::Count
            | AggFunc::JsonAgg
            | AggFunc::JsonbAgg
            | AggFunc::JsonObjectAgg
            | AggFunc::JsonObjectAggUnique
            | AggFunc::JsonObjectAggUniqueStrict
            | AggFunc::JsonbObjectAgg
            | AggFunc::JsonbObjectAggUnique
            | AggFunc::JsonbObjectAggUniqueStrict
            | AggFunc::RangeAgg
            | AggFunc::XmlAgg
    )
}

fn builtin_sql_type_for_oid(oid: u32) -> Option<SqlType> {
    builtin_type_rows()
        .into_iter()
        .find_map(|row| (row.oid == oid).then_some(row.sql_type))
}

fn catalog_builtin_type_oid(catalog: &dyn CatalogLookup, sql_type: SqlType) -> Option<u32> {
    catalog.type_oid_for_sql_type(sql_type)
}

fn catalog_text_input_cast_exists(catalog: &dyn CatalogLookup, target_oid: u32) -> bool {
    if let Some(row) = catalog.type_by_oid(target_oid) {
        if row.typtype == 'd' && row.typbasetype != 0 {
            return catalog_text_input_cast_exists(catalog, row.typbasetype);
        }
        if row.sql_type.is_array {
            return true;
        }
        if row.sql_type.is_range() || row.sql_type.is_multirange() {
            return true;
        }
        if matches!(row.sql_type.kind, SqlTypeKind::Enum) {
            return true;
        }
        if is_builtin_text_like_type(row.sql_type)
            || matches!(
                row.sql_type.kind,
                SqlTypeKind::Int2Vector | SqlTypeKind::OidVector
            )
        {
            return true;
        }
        if matches!(
            row.sql_type.kind,
            SqlTypeKind::RegProc
                | SqlTypeKind::RegClass
                | SqlTypeKind::RegRole
                | SqlTypeKind::RegNamespace
                | SqlTypeKind::RegType
                | SqlTypeKind::RegOper
                | SqlTypeKind::RegOperator
                | SqlTypeKind::RegProcedure
                | SqlTypeKind::RegCollation
                | SqlTypeKind::RegConfig
                | SqlTypeKind::RegDictionary
        ) {
            return true;
        }
    }
    catalog
        .cast_by_source_target(TEXT_TYPE_OID, target_oid)
        .is_some_and(|row| row.castmethod == 'i')
}

fn aggregate_func_for_proname(name: &str) -> Option<AggFunc> {
    match name.to_ascii_lowercase().as_str() {
        "count" => Some(AggFunc::Count),
        "any_value" => Some(AggFunc::AnyValue),
        "sum" => Some(AggFunc::Sum),
        "avg" => Some(AggFunc::Avg),
        "variance" | "var_samp" => Some(AggFunc::VarSamp),
        "var_pop" => Some(AggFunc::VarPop),
        "stddev" | "stddev_samp" => Some(AggFunc::StddevSamp),
        "stddev_pop" => Some(AggFunc::StddevPop),
        "regr_count" => Some(AggFunc::RegrCount),
        "regr_sxx" => Some(AggFunc::RegrSxx),
        "regr_syy" => Some(AggFunc::RegrSyy),
        "regr_sxy" => Some(AggFunc::RegrSxy),
        "regr_avgx" => Some(AggFunc::RegrAvgX),
        "regr_avgy" => Some(AggFunc::RegrAvgY),
        "regr_r2" => Some(AggFunc::RegrR2),
        "regr_slope" => Some(AggFunc::RegrSlope),
        "regr_intercept" => Some(AggFunc::RegrIntercept),
        "covar_pop" => Some(AggFunc::CovarPop),
        "covar_samp" => Some(AggFunc::CovarSamp),
        "corr" => Some(AggFunc::Corr),
        "bool_and" | "every" => Some(AggFunc::BoolAnd),
        "bool_or" => Some(AggFunc::BoolOr),
        "bit_and" => Some(AggFunc::BitAnd),
        "bit_or" => Some(AggFunc::BitOr),
        "bit_xor" => Some(AggFunc::BitXor),
        "min" => Some(AggFunc::Min),
        "max" => Some(AggFunc::Max),
        "string_agg" => Some(AggFunc::StringAgg),
        "array_agg" => Some(AggFunc::ArrayAgg),
        SQL_JSON_ARRAYAGG_FUNC | "json_arrayagg" => Some(AggFunc::JsonAgg),
        "json_agg" => Some(AggFunc::JsonAgg),
        "jsonb_agg" => Some(AggFunc::JsonbAgg),
        SQL_JSON_OBJECTAGG_FUNC | "json_objectagg" => Some(AggFunc::JsonObjectAgg),
        "json_object_agg" => Some(AggFunc::JsonObjectAgg),
        "json_object_agg_unique" => Some(AggFunc::JsonObjectAggUnique),
        "json_object_agg_unique_strict" => Some(AggFunc::JsonObjectAggUniqueStrict),
        "jsonb_object_agg" => Some(AggFunc::JsonbObjectAgg),
        "jsonb_object_agg_unique" => Some(AggFunc::JsonbObjectAggUnique),
        "jsonb_object_agg_unique_strict" => Some(AggFunc::JsonbObjectAggUniqueStrict),
        "range_agg" => Some(AggFunc::RangeAgg),
        "xmlagg" => Some(AggFunc::XmlAgg),
        "range_intersect_agg" => Some(AggFunc::RangeIntersectAgg),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ScalarFunctionArity {
    Exact(usize),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::catalog::INT4_TYPE_OID;

    #[test]
    fn resolve_scalar_function_uses_pg_proc_and_filters_non_scalar_rows() {
        assert_eq!(
            resolve_scalar_function("random"),
            Some(BuiltinScalarFunction::Random)
        );
        assert_eq!(
            resolve_scalar_function("pg_catalog.array_length"),
            Some(BuiltinScalarFunction::ArrayLength)
        );
        assert_eq!(
            resolve_scalar_function("lower"),
            Some(BuiltinScalarFunction::Lower)
        );
        assert_eq!(
            resolve_scalar_function("upper"),
            Some(BuiltinScalarFunction::Upper)
        );
        assert_eq!(
            resolve_scalar_function("ceiling"),
            Some(BuiltinScalarFunction::Ceiling)
        );
        assert_eq!(
            resolve_scalar_function("jsonb_contains"),
            Some(BuiltinScalarFunction::JsonbContains)
        );
        assert_eq!(
            resolve_scalar_function("jsonb_exists_any"),
            Some(BuiltinScalarFunction::JsonbExistsAny)
        );
        assert_eq!(
            resolve_scalar_function("pg_advisory_lock"),
            Some(BuiltinScalarFunction::PgAdvisoryLock)
        );
        assert_eq!(
            resolve_scalar_function("pg_advisory_unlock_all"),
            Some(BuiltinScalarFunction::PgAdvisoryUnlockAll)
        );
        assert_eq!(
            resolve_scalar_function("float8_accum"),
            Some(BuiltinScalarFunction::Float8Accum)
        );
        assert_eq!(
            resolve_scalar_function("trunc"),
            Some(BuiltinScalarFunction::Trunc)
        );
        assert_eq!(
            resolve_scalar_function("pg_catalog.pg_restore_relation_stats"),
            Some(BuiltinScalarFunction::PgRestoreRelationStats)
        );
        assert_eq!(
            resolve_scalar_function("pg_clear_attribute_stats"),
            Some(BuiltinScalarFunction::PgClearAttributeStats)
        );
        assert_eq!(
            resolve_scalar_function("pg_relation_filenode"),
            Some(BuiltinScalarFunction::PgRelationFilenode)
        );
        assert_eq!(
            resolve_scalar_function("pg_catalog.pg_filenode_relation"),
            Some(BuiltinScalarFunction::PgFilenodeRelation)
        );
        assert_eq!(
            resolve_scalar_function("pg_catalog.pg_get_partkeydef"),
            Some(BuiltinScalarFunction::PgGetPartKeyDef)
        );
        assert_eq!(
            resolve_scalar_function("pg_catalog.pg_get_partition_constraintdef"),
            Some(BuiltinScalarFunction::PgGetPartitionConstraintDef)
        );
        assert_eq!(resolve_scalar_function("count"), None);
        assert_eq!(resolve_scalar_function("json_array_elements"), None);
        assert_eq!(resolve_scalar_function("int4"), None);
    }

    #[test]
    fn resolve_stats_import_calls_as_builtin_proc_rows() {
        let catalog = Catalog::default();
        let text = SqlType::new(SqlTypeKind::Text);
        let int4 = SqlType::new(SqlTypeKind::Int4);
        let bool_ty = SqlType::new(SqlTypeKind::Bool);
        let float4 = SqlType::new(SqlTypeKind::Float4);

        let restore = resolve_function_call(
            &catalog,
            "pg_catalog.pg_restore_relation_stats",
            &[text, text, text, text, text, int4, text, float4],
            false,
        )
        .unwrap();
        assert_eq!(
            restore.scalar_impl,
            Some(BuiltinScalarFunction::PgRestoreRelationStats)
        );

        let clear = resolve_function_call(
            &catalog,
            "pg_clear_attribute_stats",
            &[text, text, text, bool_ty],
            false,
        )
        .unwrap();
        assert_eq!(
            clear.scalar_impl,
            Some(BuiltinScalarFunction::PgClearAttributeStats)
        );

        let stmt = crate::backend::parser::parse_select(
            "select pg_catalog.pg_restore_relation_stats(
                'schemaname', 'stats_import',
                'relname', 'test',
                'relpages', 18::integer,
                'reltuples', 21::real)",
        )
        .unwrap();
        let planned = pg_plan_query(&stmt, &catalog).unwrap();
        let Plan::Projection { targets, .. } = planned.plan_tree else {
            panic!("expected projection plan");
        };
        let Expr::Func(func) = &targets[0].expr else {
            panic!("expected function target");
        };
        assert_eq!(
            crate::include::nodes::primnodes::expr_sql_type_hint(&func.args[7]).map(|ty| ty.kind),
            Some(SqlTypeKind::Float4)
        );
    }

    #[test]
    fn resolve_json_table_function_uses_pg_proc_and_legacy_fallback() {
        assert_eq!(
            resolve_json_table_function("json_array_elements"),
            Some(JsonTableFunction::ArrayElements)
        );
        assert_eq!(
            resolve_json_table_function("pg_catalog.json_array_elements"),
            Some(JsonTableFunction::ArrayElements)
        );
        assert_eq!(
            resolve_json_table_function("jsonb_array_elements"),
            Some(JsonTableFunction::JsonbArrayElements)
        );
        assert_eq!(
            resolve_json_table_function("json_each"),
            Some(JsonTableFunction::Each)
        );
        assert_eq!(resolve_json_table_function("random"), None);
    }

    #[test]
    fn resolve_function_cast_type_accepts_pg_catalog_prefix() {
        let catalog = Catalog::default();
        assert_eq!(
            resolve_function_cast_type(&catalog, "pg_catalog.text"),
            Some(SqlType::new(SqlTypeKind::Text))
        );
    }

    #[test]
    fn resolve_function_call_expands_ordinary_variadic_candidates() {
        let resolved = resolve_function_call(
            &Catalog::default(),
            "json_extract_path",
            &[
                SqlType::new(SqlTypeKind::Json),
                SqlType::new(SqlTypeKind::Text),
                SqlType::new(SqlTypeKind::Text),
            ],
            false,
        )
        .unwrap();
        assert_eq!(resolved.proc_oid, 3951);
        assert_eq!(resolved.vatype_oid, TEXT_TYPE_OID);
        assert_eq!(resolved.nvargs, 2);
        assert!(resolved.func_variadic);
        assert_eq!(
            resolved.scalar_impl,
            Some(BuiltinScalarFunction::JsonExtractPath)
        );
    }

    #[test]
    fn resolve_function_call_preserves_explicit_variadic_any_calls() {
        let resolved = resolve_function_call(
            &Catalog::default(),
            "json_build_array",
            &[SqlType::array_of(SqlType::new(SqlTypeKind::Text))],
            true,
        )
        .unwrap();
        assert_eq!(resolved.proc_oid, 6213);
        assert_eq!(resolved.vatype_oid, ANYOID);
        assert_eq!(resolved.nvargs, 0);
        assert!(resolved.func_variadic);
    }

    #[test]
    fn resolve_function_call_clears_variadic_for_non_variadic_target() {
        let resolved = resolve_function_call(
            &Catalog::default(),
            "lower",
            &[SqlType::new(SqlTypeKind::Text)],
            true,
        )
        .unwrap();
        assert_eq!(resolved.scalar_impl, Some(BuiltinScalarFunction::Lower));
        assert!(!resolved.func_variadic);
        assert_eq!(resolved.vatype_oid, 0);
    }

    #[test]
    fn resolve_function_call_prefers_text_upper_for_text_arguments() {
        let resolved = resolve_function_call(
            &Catalog::default(),
            "upper",
            &[SqlType::new(SqlTypeKind::Text)],
            false,
        )
        .unwrap();

        assert_eq!(resolved.scalar_impl, Some(BuiltinScalarFunction::Upper));
        assert_eq!(resolved.result_type, SqlType::new(SqlTypeKind::Text));
    }

    #[test]
    fn resolve_function_call_prefers_range_lower_for_range_arguments() {
        let resolved = resolve_function_call(
            &Catalog::default(),
            "lower",
            &[SqlType::range(
                crate::include::catalog::INT4RANGE_TYPE_OID,
                crate::include::catalog::INT4_TYPE_OID,
            )
            .with_identity(crate::include::catalog::INT4RANGE_TYPE_OID, 0)
            .with_range_metadata(
                crate::include::catalog::INT4_TYPE_OID,
                crate::include::catalog::INT4MULTIRANGE_TYPE_OID,
                true,
            )],
            false,
        )
        .unwrap();

        assert_eq!(
            resolved.scalar_impl,
            Some(BuiltinScalarFunction::RangeLower)
        );
        assert_eq!(resolved.result_type, SqlType::new(SqlTypeKind::Int4));
    }

    #[test]
    fn resolve_function_call_supports_range_merge_for_range_arguments() {
        let range_type = SqlType::range(
            crate::include::catalog::INT4RANGE_TYPE_OID,
            crate::include::catalog::INT4_TYPE_OID,
        )
        .with_identity(crate::include::catalog::INT4RANGE_TYPE_OID, 0)
        .with_range_metadata(
            crate::include::catalog::INT4_TYPE_OID,
            crate::include::catalog::INT4MULTIRANGE_TYPE_OID,
            true,
        );
        let resolved = resolve_function_call(
            &Catalog::default(),
            "range_merge",
            &[range_type, range_type],
            false,
        )
        .unwrap();

        assert_eq!(
            resolved.scalar_impl,
            Some(BuiltinScalarFunction::RangeMerge)
        );
        assert_eq!(resolved.result_type, range_type);
    }

    #[test]
    fn resolve_function_call_infers_anyelement_result_from_array_argument() {
        let resolved = resolve_function_call(
            &Catalog::default(),
            "unnest",
            &[SqlType::array_of(SqlType::new(SqlTypeKind::Int4))],
            false,
        )
        .unwrap();

        assert_eq!(resolved.proc_oid, 6267);
        assert_eq!(resolved.result_type, SqlType::new(SqlTypeKind::Int4));
    }

    #[test]
    fn resolve_function_call_does_not_guess_anyelement_from_anyarray_pseudotype() {
        let error = resolve_function_call(
            &Catalog::default(),
            "unnest",
            &[SqlType::new(SqlTypeKind::AnyArray)],
            false,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ParseError::DetailedError { message, sqlstate, .. }
                if message == "function unnest(anyarray) does not exist" && sqlstate == "42883"
        ));
    }

    #[test]
    fn resolve_function_call_supports_zero_arg_builtin_multirange_constructors() {
        let resolved =
            resolve_function_call(&Catalog::default(), "int4multirange", &[], false).unwrap();

        assert_eq!(
            resolved.result_type,
            SqlType::multirange(
                crate::include::catalog::INT4MULTIRANGE_TYPE_OID,
                crate::include::catalog::INT4RANGE_TYPE_OID,
            )
            .with_identity(crate::include::catalog::INT4MULTIRANGE_TYPE_OID, 0)
            .with_range_metadata(
                crate::include::catalog::INT4_TYPE_OID,
                crate::include::catalog::INT4MULTIRANGE_TYPE_OID,
                true,
            )
            .with_multirange_range_oid(crate::include::catalog::INT4RANGE_TYPE_OID)
        );
        assert_eq!(
            resolved.scalar_impl,
            Some(BuiltinScalarFunction::RangeConstructor)
        );
        assert!(resolved.declared_arg_types.is_empty());
    }

    #[test]
    fn resolve_function_call_supports_explicit_variadic_builtin_multirange_constructors() {
        let catalog = Catalog::default();
        let range_array_type = catalog
            .type_by_oid(crate::include::catalog::NUMRANGE_ARRAY_TYPE_OID)
            .unwrap()
            .sql_type;
        let resolved =
            resolve_function_call(&catalog, "nummultirange", &[range_array_type], true).unwrap();

        assert_eq!(
            resolved.result_type,
            catalog
                .type_by_oid(crate::include::catalog::NUMMULTIRANGE_TYPE_OID)
                .unwrap()
                .sql_type
        );
        assert!(resolved.func_variadic);
        assert_eq!(
            resolved.vatype_oid,
            crate::include::catalog::NUMRANGE_TYPE_OID
        );
        assert_eq!(resolved.declared_arg_types, vec![range_array_type]);
    }

    #[test]
    fn resolve_function_cast_type_uses_pg_type_catalog_and_aliases() {
        assert_eq!(
            resolve_function_cast_type(&Catalog::default(), "int4"),
            Some(SqlType::new(SqlTypeKind::Int4))
        );
        assert_eq!(
            resolve_function_cast_type(&Catalog::default(), "smallint"),
            Some(SqlType::new(SqlTypeKind::Int2))
        );
        assert_eq!(
            resolve_function_cast_type(&Catalog::default(), "bit"),
            Some(SqlType::with_bit_len(SqlTypeKind::Bit, 1))
        );
        assert_eq!(
            resolve_function_cast_type(&Catalog::default(), "boolean"),
            Some(SqlType::new(SqlTypeKind::Bool))
        );
        assert_eq!(
            resolve_function_cast_type(&Catalog::default(), "varchar"),
            Some(SqlType::new(SqlTypeKind::Varchar))
        );
        assert_eq!(
            resolve_function_cast_type(&Catalog::default(), "jsonb"),
            Some(SqlType::new(SqlTypeKind::Jsonb))
        );
        assert_eq!(
            resolve_function_cast_type(&Catalog::default(), "jsonpath"),
            Some(SqlType::new(SqlTypeKind::JsonPath))
        );
        assert_eq!(
            resolve_function_cast_type(&Catalog::default(), "timestamp"),
            Some(SqlType::new(SqlTypeKind::Timestamp))
        );
        assert_eq!(
            resolve_function_cast_type(&Catalog::default(), "time"),
            Some(SqlType::new(SqlTypeKind::Time))
        );
    }

    #[test]
    fn explicit_text_input_cast_exists_uses_pg_cast_catalog() {
        assert!(explicit_text_input_cast_exists(
            &Catalog::default(),
            SqlType::new(SqlTypeKind::Jsonb)
        ));
        assert!(explicit_text_input_cast_exists(
            &Catalog::default(),
            SqlType::new(SqlTypeKind::JsonPath)
        ));
        assert!(explicit_text_input_cast_exists(
            &Catalog::default(),
            SqlType::new(SqlTypeKind::Time)
        ));
        assert!(explicit_text_input_cast_exists(
            &Catalog::default(),
            SqlType::new(SqlTypeKind::Timestamp)
        ));
        assert!(explicit_text_input_cast_exists(
            &Catalog::default(),
            SqlType::new(SqlTypeKind::TimeTz)
        ));
        assert!(explicit_text_input_cast_exists(
            &Catalog::default(),
            SqlType::new(SqlTypeKind::TimestampTz)
        ));
        assert!(explicit_text_input_cast_exists(
            &Catalog::default(),
            SqlType::new(SqlTypeKind::Name)
        ));
        assert!(explicit_text_input_cast_exists(
            &Catalog::default(),
            SqlType::with_bit_len(SqlTypeKind::Bit, 4)
        ));
        assert!(explicit_text_input_cast_exists(
            &Catalog::default(),
            SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
        ));
        assert!(explicit_text_input_cast_exists(
            &Catalog::default(),
            SqlType::array_of(SqlType::new(SqlTypeKind::Name))
        ));
        assert!(explicit_text_input_cast_exists(
            &Catalog::default(),
            SqlType::array_of(SqlType::new(SqlTypeKind::Jsonb))
        ));
    }

    #[test]
    fn validate_scalar_function_arity_uses_pg_proc_for_exact_arity_rows() {
        assert!(
            validate_scalar_function_arity(BuiltinScalarFunction::Lower, &[SqlExpr::Default])
                .is_ok()
        );
        assert!(validate_scalar_function_arity(BuiltinScalarFunction::Lower, &[]).is_err());
        assert!(validate_scalar_function_arity(BuiltinScalarFunction::Random, &[]).is_ok());
        assert!(validate_scalar_function_arity(BuiltinScalarFunction::PgMyTempSchema, &[]).is_ok());
        assert!(
            validate_scalar_function_arity(BuiltinScalarFunction::CurrentDatabase, &[]).is_ok()
        );
        assert!(
            validate_scalar_function_arity(BuiltinScalarFunction::Random, &[SqlExpr::Default])
                .is_err()
        );
        assert!(
            validate_scalar_function_arity(
                BuiltinScalarFunction::Random,
                &[SqlExpr::Default, SqlExpr::Default]
            )
            .is_ok()
        );
        assert!(validate_scalar_function_arity(BuiltinScalarFunction::RandomNormal, &[]).is_ok());
        assert!(
            validate_scalar_function_arity(
                BuiltinScalarFunction::RandomNormal,
                &[SqlExpr::Default, SqlExpr::Default]
            )
            .is_ok()
        );
        assert!(
            validate_scalar_function_arity(
                BuiltinScalarFunction::JsonBuildArray,
                &[SqlExpr::Default, SqlExpr::Default]
            )
            .is_ok()
        );
        assert!(
            validate_scalar_function_arity(
                BuiltinScalarFunction::PgAdvisoryLock,
                &[SqlExpr::Default]
            )
            .is_ok()
        );
        assert!(
            validate_scalar_function_arity(
                BuiltinScalarFunction::PgAdvisoryLock,
                &[SqlExpr::Default, SqlExpr::Default]
            )
            .is_ok()
        );
        assert!(
            validate_scalar_function_arity(BuiltinScalarFunction::PgAdvisoryLock, &[]).is_err()
        );
        assert!(
            validate_scalar_function_arity(
                BuiltinScalarFunction::PgAdvisoryLock,
                &[SqlExpr::Default, SqlExpr::Default, SqlExpr::Default]
            )
            .is_err()
        );
        assert!(
            validate_scalar_function_arity(BuiltinScalarFunction::PgAdvisoryUnlockAll, &[]).is_ok()
        );
        assert!(
            validate_scalar_function_arity(
                BuiltinScalarFunction::PgAdvisoryUnlockAll,
                &[SqlExpr::Default]
            )
            .is_err()
        );
        assert!(
            validate_scalar_function_arity(
                BuiltinScalarFunction::PgNotify,
                &[SqlExpr::Default, SqlExpr::Default]
            )
            .is_ok()
        );
        assert!(validate_scalar_function_arity(BuiltinScalarFunction::PgNotify, &[]).is_err());
        assert!(
            validate_scalar_function_arity(BuiltinScalarFunction::PgNotificationQueueUsage, &[])
                .is_ok()
        );
        assert!(
            validate_scalar_function_arity(
                BuiltinScalarFunction::PgNotificationQueueUsage,
                &[SqlExpr::Default]
            )
            .is_err()
        );
    }

    #[test]
    fn validate_aggregate_arity_uses_pg_proc_for_exact_rows() {
        assert!(validate_aggregate_arity(AggFunc::Sum, &[SqlExpr::Default]).is_ok());
        assert!(validate_aggregate_arity(AggFunc::Sum, &[]).is_err());
        assert!(validate_aggregate_arity(AggFunc::AnyValue, &[SqlExpr::Default]).is_ok());
        assert!(
            validate_aggregate_arity(
                AggFunc::JsonObjectAgg,
                &[SqlExpr::Default, SqlExpr::Default]
            )
            .is_ok()
        );
        assert!(
            validate_aggregate_arity(AggFunc::StringAgg, &[SqlExpr::Default, SqlExpr::Default])
                .is_ok()
        );
        assert!(validate_aggregate_arity(AggFunc::JsonObjectAgg, &[SqlExpr::Default]).is_err());
        assert!(validate_aggregate_arity(AggFunc::StringAgg, &[SqlExpr::Default]).is_err());
        assert!(validate_aggregate_arity(AggFunc::Count, &[]).is_ok());
    }

    #[test]
    fn fixed_aggregate_return_type_uses_pg_proc_for_type_invariant_rows() {
        assert_eq!(
            fixed_aggregate_return_type(AggFunc::Count),
            Some(SqlType::new(SqlTypeKind::Int8))
        );
        assert_eq!(
            fixed_aggregate_return_type(AggFunc::JsonAgg),
            Some(SqlType::new(SqlTypeKind::Json))
        );
        assert_eq!(
            fixed_aggregate_return_type(AggFunc::JsonbObjectAgg),
            Some(SqlType::new(SqlTypeKind::Jsonb))
        );
        assert_eq!(fixed_aggregate_return_type(AggFunc::Sum), None);
        assert_eq!(fixed_aggregate_return_type(AggFunc::Max), None);
    }

    #[test]
    fn comparison_operator_exists_uses_pg_operator_catalog() {
        assert!(comparison_operator_exists(
            &Catalog::default(),
            "<",
            SqlType::new(SqlTypeKind::Text),
            SqlType::new(SqlTypeKind::Text)
        ));
        assert!(comparison_operator_exists(
            &Catalog::default(),
            ">=",
            SqlType::new(SqlTypeKind::Text),
            SqlType::new(SqlTypeKind::Text)
        ));
        assert!(comparison_operator_exists(
            &Catalog::default(),
            "=",
            SqlType::new(SqlTypeKind::Bool),
            SqlType::new(SqlTypeKind::Bool)
        ));
        assert!(comparison_operator_exists(
            &Catalog::default(),
            "=",
            SqlType::new(SqlTypeKind::Jsonb),
            SqlType::new(SqlTypeKind::Jsonb)
        ));
        assert!(!comparison_operator_exists(
            &Catalog::default(),
            "=",
            SqlType::array_of(SqlType::new(SqlTypeKind::Int4)),
            SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
        ));
    }

    #[test]
    fn fixed_scalar_return_type_uses_pg_proc_for_type_invariant_rows() {
        assert_eq!(
            fixed_scalar_return_type(BuiltinScalarFunction::RandomNormal),
            Some(SqlType::new(SqlTypeKind::Float8))
        );
        assert_eq!(
            fixed_scalar_return_type(BuiltinScalarFunction::Lower),
            Some(SqlType::new(SqlTypeKind::Text))
        );
        assert_eq!(
            fixed_scalar_return_type(BuiltinScalarFunction::BoolEq),
            Some(SqlType::new(SqlTypeKind::Bool))
        );
        assert_eq!(
            fixed_scalar_return_type(BuiltinScalarFunction::PgMyTempSchema),
            Some(SqlType::new(SqlTypeKind::Oid))
        );
        assert_eq!(
            fixed_scalar_return_type(BuiltinScalarFunction::CurrentDatabase),
            Some(SqlType::new(SqlTypeKind::Name))
        );
        assert_eq!(
            fixed_scalar_return_type(BuiltinScalarFunction::PgBackendPid),
            Some(SqlType::new(SqlTypeKind::Int4))
        );
        assert_eq!(
            fixed_scalar_return_type(BuiltinScalarFunction::ToJsonb),
            Some(SqlType::new(SqlTypeKind::Jsonb))
        );
        assert_eq!(
            fixed_scalar_return_type(BuiltinScalarFunction::PgAdvisoryLock),
            Some(SqlType::new(SqlTypeKind::Void))
        );
        assert_eq!(
            fixed_scalar_return_type(BuiltinScalarFunction::PgTryAdvisoryLock),
            Some(SqlType::new(SqlTypeKind::Bool))
        );
        assert_eq!(
            fixed_scalar_return_type(BuiltinScalarFunction::PgAdvisoryUnlockAll),
            Some(SqlType::new(SqlTypeKind::Void))
        );
        assert_eq!(
            fixed_scalar_return_type(BuiltinScalarFunction::PgNotify),
            Some(SqlType::new(SqlTypeKind::Void))
        );
        assert_eq!(
            fixed_scalar_return_type(BuiltinScalarFunction::PgNotificationQueueUsage),
            Some(SqlType::new(SqlTypeKind::Float8))
        );
        assert_eq!(fixed_scalar_return_type(BuiltinScalarFunction::Abs), None);
        assert_eq!(
            fixed_scalar_return_type(BuiltinScalarFunction::Substring),
            None
        );
    }

    #[test]
    fn non_record_functions_without_out_parameters_do_not_gain_record_row_shape() {
        let catalog = Catalog::default();
        let row = catalog
            .proc_rows_by_name("generate_series")
            .into_iter()
            .find(|row| row.proretset && row.prorettype == INT4_TYPE_OID && row.pronargs == 2)
            .expect("generate_series(int4, int4) row");
        let result_type = catalog
            .type_by_oid(row.prorettype)
            .expect("result type")
            .sql_type;
        let candidate = CandidateMatch {
            declared_arg_types: vec![SqlType::new(SqlTypeKind::Int4); row.pronargs as usize],
            cost: 0,
            nvargs: 0,
            vatype_oid: 0,
        };

        assert_eq!(
            resolve_out_parameter_row_shape(&catalog, &row, &candidate),
            None,
        );
        assert_eq!(
            resolve_function_row_shape(&catalog, &row, &candidate, result_type),
            Some(ResolvedFunctionRowShape::None),
        );
    }
}
