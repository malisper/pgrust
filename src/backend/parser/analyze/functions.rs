use super::*;
use crate::include::catalog::{
    ANYARRAYOID, ANYOID, TEXT_TYPE_OID, bootstrap_pg_proc_rows, builtin_type_rows,
    builtin_window_function_for_proc_oid,
};
use crate::include::nodes::primnodes::{
    BuiltinWindowFunction, JsonRecordFunction, RegexTableFunction,
};
use std::collections::BTreeMap;
use std::sync::OnceLock;

#[derive(Clone, Copy)]
enum NamedArgDefault {
    Bool(bool),
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
    JsonTable(JsonTableFunction),
    RegexTable(RegexTableFunction),
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
pub(super) struct ResolvedFunctionCall {
    pub proc_oid: u32,
    pub prokind: char,
    pub proretset: bool,
    pub result_type: SqlType,
    pub declared_arg_types: Vec<SqlType>,
    pub nvargs: usize,
    pub vatype_oid: u32,
    pub func_variadic: bool,
    pub scalar_impl: Option<BuiltinScalarFunction>,
    pub srf_impl: Option<ResolvedSrfImpl>,
    pub agg_impl: Option<AggFunc>,
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

pub(super) fn resolve_function_call(
    catalog: &dyn CatalogLookup,
    name: &str,
    actual_types: &[SqlType],
    func_variadic: bool,
) -> Result<ResolvedFunctionCall, ParseError> {
    let mut best: Option<(ResolvedFunctionCall, usize, bool, bool)> = None;
    let mut ambiguous = false;

    for row in catalog.proc_rows_by_name(name) {
        let scalar_impl = builtin_scalar_function_for_proc_row(&row);
        let srf_impl = builtin_srf_impl_for_proc_row(&row);
        let agg_impl = aggregate_func_for_proname(&row.proname);
        let window_impl = builtin_window_function_for_proc_oid(row.oid);

        let Some(candidate) = match_proc_signature(catalog, &row, actual_types, func_variadic)
        else {
            continue;
        };
        let Some(result_type) = resolve_proc_result_type(catalog, &row, &candidate) else {
            continue;
        };
        let Some(row_shape) = resolve_function_row_shape(catalog, &row, result_type) else {
            continue;
        };

        let resolved = ResolvedFunctionCall {
            proc_oid: row.oid,
            prokind: row.prokind,
            proretset: row.proretset,
            result_type,
            declared_arg_types: candidate.declared_arg_types,
            nvargs: candidate.nvargs,
            vatype_oid: candidate.vatype_oid,
            func_variadic: row.provariadic != 0
                && (func_variadic || (row.provariadic != ANYOID && candidate.nvargs > 0)),
            scalar_impl,
            srf_impl,
            agg_impl,
            window_impl,
            row_shape,
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

    best.map(|(resolved, _, _, _)| resolved)
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "supported function",
            actual: name.to_string(),
        })
}

pub(super) fn resolve_scalar_function(name: &str) -> Option<BuiltinScalarFunction> {
    let normalized = normalize_builtin_function_name(name);
    scalar_functions_by_name().get(normalized).copied()
}

pub(super) fn resolve_builtin_aggregate(name: &str) -> Option<AggFunc> {
    aggregate_func_for_proname(name)
}

pub(super) fn resolve_function_cast_type(
    catalog: &dyn CatalogLookup,
    name: &str,
) -> Option<SqlType> {
    let normalized = normalize_builtin_function_name(name);
    for row in catalog.type_rows() {
        if row.typrelid != 0 || !row.typname.eq_ignore_ascii_case(normalized) {
            continue;
        }
        if row.oid != TEXT_TYPE_OID && !catalog_text_input_cast_exists(catalog, row.oid) {
            continue;
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

fn normalize_builtin_function_name(name: &str) -> &str {
    name.strip_prefix("pg_catalog.").unwrap_or(name)
}

fn builtin_scalar_function_for_proc_row(row: &PgProcRow) -> Option<BuiltinScalarFunction> {
    builtin_scalar_function_for_proc_src(&row.prosrc)
        .or_else(|| builtin_scalar_function_for_proc_src(&row.proname))
}

fn builtin_srf_impl_for_proc_row(row: &PgProcRow) -> Option<ResolvedSrfImpl> {
    match row.proname.to_ascii_lowercase().as_str() {
        "generate_series" => Some(ResolvedSrfImpl::GenerateSeries),
        "unnest" => Some(ResolvedSrfImpl::Unnest),
        other => resolve_json_table_function(other)
            .map(ResolvedSrfImpl::JsonTable)
            .or_else(|| resolve_regex_table_function(other).map(ResolvedSrfImpl::RegexTable)),
    }
}

fn match_proc_signature(
    catalog: &dyn CatalogLookup,
    row: &PgProcRow,
    actual_types: &[SqlType],
    func_variadic: bool,
) -> Option<CandidateMatch> {
    let declared_oids = parse_proc_argtype_oids(&row.proargtypes)?;
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

fn match_proc_arg_type(
    catalog: &dyn CatalogLookup,
    actual_type: SqlType,
    declared_oid: u32,
) -> Option<(usize, SqlType)> {
    if declared_oid == ANYOID {
        return Some((2, actual_type));
    }
    if declared_oid == ANYARRAYOID {
        return (actual_type.is_array || actual_type.kind == SqlTypeKind::AnyArray)
            .then_some((2, actual_type));
    }
    let declared_type = catalog.type_by_oid(declared_oid)?.sql_type;
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
    arg_type_match_cost(actual_type, declared_type).map(|cost| (cost, declared_type))
}

fn resolve_proc_result_type(
    catalog: &dyn CatalogLookup,
    row: &crate::include::catalog::PgProcRow,
    candidate: &CandidateMatch,
) -> Option<SqlType> {
    match row.prorettype {
        ANYOID => resolve_anyelement_result_type(row, candidate),
        ANYARRAYOID => resolve_anyarray_result_type(row, candidate),
        _ => catalog.type_by_oid(row.prorettype).map(|row| row.sql_type),
    }
}

fn resolve_function_row_shape(
    catalog: &dyn CatalogLookup,
    row: &crate::include::catalog::PgProcRow,
    result_type: SqlType,
) -> Option<ResolvedFunctionRowShape> {
    match result_type.kind {
        SqlTypeKind::Record => resolve_record_row_shape(catalog, row),
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

fn resolve_record_row_shape(
    catalog: &dyn CatalogLookup,
    row: &crate::include::catalog::PgProcRow,
) -> Option<ResolvedFunctionRowShape> {
    let (Some(arg_types), Some(arg_modes)) = (&row.proallargtypes, &row.proargmodes) else {
        return Some(ResolvedFunctionRowShape::AnonymousRecord);
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
        let sql_type = catalog.type_by_oid(*type_oid)?.sql_type;
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
        Some(ResolvedFunctionRowShape::AnonymousRecord)
    } else {
        Some(ResolvedFunctionRowShape::OutParameters(output_columns))
    }
}

fn resolve_anyelement_result_type(
    row: &crate::include::catalog::PgProcRow,
    candidate: &CandidateMatch,
) -> Option<SqlType> {
    let declared_oids = parse_proc_argtype_oids(&row.proargtypes)?;
    let mut resolved = None;
    for (declared_oid, actual_type) in declared_oids
        .into_iter()
        .zip(candidate.declared_arg_types.iter().copied())
    {
        let inferred = match declared_oid {
            ANYOID => Some(actual_type),
            ANYARRAYOID if actual_type.is_array => Some(actual_type.element_type()),
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

fn resolve_anyarray_result_type(
    row: &crate::include::catalog::PgProcRow,
    candidate: &CandidateMatch,
) -> Option<SqlType> {
    let declared_oids = parse_proc_argtype_oids(&row.proargtypes)?;
    let mut resolved = None;
    for (declared_oid, actual_type) in declared_oids
        .into_iter()
        .zip(candidate.declared_arg_types.iter().copied())
    {
        let inferred = match declared_oid {
            ANYARRAYOID if actual_type.is_array => Some(actual_type),
            ANYOID if !actual_type.is_array && actual_type.kind != SqlTypeKind::AnyArray => {
                Some(SqlType::array_of(actual_type))
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

fn match_variadic_element_type(
    catalog: &dyn CatalogLookup,
    actual_type: SqlType,
    variadic_oid: u32,
) -> Option<(usize, SqlType)> {
    if variadic_oid == ANYOID {
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
    if variadic_oid == ANYOID {
        return actual_type.is_array.then_some((2, actual_type));
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
    if !target_type.is_array
        && target_type.kind == SqlTypeKind::Record
        && matches!(
            actual_type.kind,
            SqlTypeKind::Record | SqlTypeKind::Composite
        )
    {
        return Some(1);
    }
    if is_numeric_family(actual_type) && is_numeric_family(target_type) {
        return Some(1);
    }
    if is_text_like_type(actual_type) && is_text_like_type(target_type) {
        return Some(1);
    }
    if is_bit_string_type(actual_type) && is_bit_string_type(target_type) {
        return Some(1);
    }
    None
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
            | BuiltinScalarFunction::ToTsQuery
            | BuiltinScalarFunction::PlainToTsQuery
            | BuiltinScalarFunction::PhraseToTsQuery
            | BuiltinScalarFunction::WebSearchToTsQuery => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::TsLexize => args.len() == 2,
            BuiltinScalarFunction::TsQueryNot => args.len() == 1,
            BuiltinScalarFunction::TsMatch
            | BuiltinScalarFunction::TsQueryAnd
            | BuiltinScalarFunction::TsQueryOr
            | BuiltinScalarFunction::TsVectorConcat => args.len() == 2,
            BuiltinScalarFunction::CashLarger | BuiltinScalarFunction::CashSmaller => {
                args.len() == 2
            }
            BuiltinScalarFunction::CashWords => args.len() == 1,
            BuiltinScalarFunction::XmlComment
            | BuiltinScalarFunction::XmlIsWellFormed
            | BuiltinScalarFunction::XmlIsWellFormedDocument
            | BuiltinScalarFunction::XmlIsWellFormedContent => args.len() == 1,
            BuiltinScalarFunction::Random | BuiltinScalarFunction::RandomNormal => {
                matches!(args.len(), 0 | 2)
            }
            BuiltinScalarFunction::Now
            | BuiltinScalarFunction::TransactionTimestamp
            | BuiltinScalarFunction::StatementTimestamp
            | BuiltinScalarFunction::ClockTimestamp
            | BuiltinScalarFunction::TimeOfDay => args.is_empty(),
            BuiltinScalarFunction::DatePart | BuiltinScalarFunction::DateTrunc => args.len() == 2,
            BuiltinScalarFunction::IsFinite => args.len() == 1,
            BuiltinScalarFunction::MakeDate => args.len() == 3,
            BuiltinScalarFunction::GetDatabaseEncoding => args.is_empty(),
            BuiltinScalarFunction::PgMyTempSchema => args.is_empty(),
            BuiltinScalarFunction::PgRustInternalBinaryCoercible => args.len() == 2,
            BuiltinScalarFunction::PgRustTestFdwHandler => args.is_empty(),
            BuiltinScalarFunction::PgRustTestEncSetup => args.is_empty(),
            BuiltinScalarFunction::PgRustTestEncConversion => args.len() == 4,
            BuiltinScalarFunction::PgTypeof => args.len() == 1,
            BuiltinScalarFunction::NextVal | BuiltinScalarFunction::CurrVal => args.len() == 1,
            BuiltinScalarFunction::SetVal => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::PgGetSerialSequence => args.len() == 2,
            BuiltinScalarFunction::PgGetUserById => args.len() == 1,
            BuiltinScalarFunction::ObjDescription => args.len() == 2,
            BuiltinScalarFunction::PgGetExpr => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::PgRelationIsPublishable => args.len() == 1,
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
            | BuiltinScalarFunction::PgStatClearSnapshot => args.is_empty(),
            BuiltinScalarFunction::PgStatHaveStats => args.len() == 3,
            BuiltinScalarFunction::PgStatGetNumscans
            | BuiltinScalarFunction::PgStatGetLastscan
            | BuiltinScalarFunction::PgStatGetTuplesReturned
            | BuiltinScalarFunction::PgStatGetTuplesFetched
            | BuiltinScalarFunction::PgStatGetTuplesInserted
            | BuiltinScalarFunction::PgStatGetTuplesUpdated
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
            | BuiltinScalarFunction::Lower
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
            | BuiltinScalarFunction::QuoteLiteral
            | BuiltinScalarFunction::BitcastIntegerToFloat4
            | BuiltinScalarFunction::BitcastBigintToFloat8
            | BuiltinScalarFunction::RegProcedureToText
            | BuiltinScalarFunction::RegRoleToText
            | BuiltinScalarFunction::BpcharToText
            | BuiltinScalarFunction::BitCount => args.len() == 1,
            BuiltinScalarFunction::Trunc | BuiltinScalarFunction::Round => {
                matches!(args.len(), 1 | 2)
            }
            BuiltinScalarFunction::Log => matches!(args.len(), 1 | 2),
            BuiltinScalarFunction::Power
            | BuiltinScalarFunction::Atan2d
            | BuiltinScalarFunction::BoolEq
            | BuiltinScalarFunction::BoolNe
            | BuiltinScalarFunction::Div
            | BuiltinScalarFunction::Mod => args.len() == 2,
            BuiltinScalarFunction::Float8Accum | BuiltinScalarFunction::Float8Combine => {
                args.len() == 2
            }
            BuiltinScalarFunction::Float8RegrAccum => args.len() == 3,
            BuiltinScalarFunction::Float8RegrCombine => args.len() == 2,
            BuiltinScalarFunction::WidthBucket => matches!(args.len(), 2 | 4),
            BuiltinScalarFunction::GetBit => args.len() == 2,
            BuiltinScalarFunction::SetBit => args.len() == 3,
            BuiltinScalarFunction::ArrayFill => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::StringToArray
            | BuiltinScalarFunction::ArrayToString
            | BuiltinScalarFunction::ArrayAppend
            | BuiltinScalarFunction::ArrayPrepend
            | BuiltinScalarFunction::ArrayCat
            | BuiltinScalarFunction::ArrayPosition
            | BuiltinScalarFunction::ArraySort => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::ArrayPositions | BuiltinScalarFunction::ArrayRemove => {
                args.len() == 2
            }
            BuiltinScalarFunction::ArrayReplace => args.len() == 3,
            BuiltinScalarFunction::Gcd | BuiltinScalarFunction::Lcm => args.len() == 2,
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
            | BuiltinScalarFunction::Left
            | BuiltinScalarFunction::Right
            | BuiltinScalarFunction::Repeat
            | BuiltinScalarFunction::Encode
            | BuiltinScalarFunction::Decode
            | BuiltinScalarFunction::ToChar
            | BuiltinScalarFunction::ToNumber
            | BuiltinScalarFunction::PgInputIsValid
            | BuiltinScalarFunction::PgInputErrorMessage
            | BuiltinScalarFunction::PgInputErrorDetail
            | BuiltinScalarFunction::PgInputErrorHint
            | BuiltinScalarFunction::PgInputErrorSqlState => args.len() == 2,
            BuiltinScalarFunction::RegexpLike => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::RegexpMatch => matches!(args.len(), 2 | 3),
            BuiltinScalarFunction::Replace
            | BuiltinScalarFunction::Translate
            | BuiltinScalarFunction::SplitPart => args.len() == 3,
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
            BuiltinScalarFunction::JsonbPathExists
            | BuiltinScalarFunction::JsonbPathMatch
            | BuiltinScalarFunction::JsonbPathQueryArray
            | BuiltinScalarFunction::JsonbPathQueryFirst => matches!(args.len(), 2..=4),
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
            | AggFunc::Min
            | AggFunc::Max
            | AggFunc::ArrayAgg
            | AggFunc::JsonAgg
            | AggFunc::JsonbAgg
            | AggFunc::RangeAgg
            | AggFunc::XmlAgg
            | AggFunc::RangeIntersectAgg => args.len() == 1,
            AggFunc::StringAgg | AggFunc::JsonObjectAgg | AggFunc::JsonbObjectAgg => {
                args.len() == 2
            }
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
        BuiltinScalarFunction::ToTsVector => {
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
        BuiltinScalarFunction::TsQueryAnd
        | BuiltinScalarFunction::TsQueryOr
        | BuiltinScalarFunction::TsQueryNot => {
            return Some(SqlType::new(SqlTypeKind::TsQuery));
        }
        BuiltinScalarFunction::TsVectorConcat => {
            return Some(SqlType::new(SqlTypeKind::TsVector));
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
        for row in bootstrap_pg_proc_rows() {
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
            by_name.entry((*name).into()).or_insert(*func);
        }
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
                actual: "positional argument after named argument".into(),
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
        _ => None,
    }
}

fn table_function_named_arg_signature(name: &str) -> Option<NamedArgSignature> {
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
    legacy_scalar_function_entries()
        .iter()
        .find_map(|(name, func)| proc_src.eq_ignore_ascii_case(name).then_some(*func))
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
        ("cashlarger", BuiltinScalarFunction::CashLarger),
        ("cashsmaller", BuiltinScalarFunction::CashSmaller),
        ("cash_words", BuiltinScalarFunction::CashWords),
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
        ("date_part", BuiltinScalarFunction::DatePart),
        ("date_trunc", BuiltinScalarFunction::DateTrunc),
        ("isfinite", BuiltinScalarFunction::IsFinite),
        ("make_date", BuiltinScalarFunction::MakeDate),
        (
            "getdatabaseencoding",
            BuiltinScalarFunction::GetDatabaseEncoding,
        ),
        ("pg_my_temp_schema", BuiltinScalarFunction::PgMyTempSchema),
        (
            "pg_rust_internal_binary_coercible",
            BuiltinScalarFunction::PgRustInternalBinaryCoercible,
        ),
        (
            "pg_rust_test_fdw_handler",
            BuiltinScalarFunction::PgRustTestFdwHandler,
        ),
        (
            "pg_rust_test_enc_setup",
            BuiltinScalarFunction::PgRustTestEncSetup,
        ),
        (
            "pg_rust_test_enc_conversion",
            BuiltinScalarFunction::PgRustTestEncConversion,
        ),
        ("nextval", BuiltinScalarFunction::NextVal),
        ("currval", BuiltinScalarFunction::CurrVal),
        ("setval", BuiltinScalarFunction::SetVal),
        (
            "pg_get_serial_sequence",
            BuiltinScalarFunction::PgGetSerialSequence,
        ),
        ("pg_get_userbyid", BuiltinScalarFunction::PgGetUserById),
        ("obj_description", BuiltinScalarFunction::ObjDescription),
        ("pg_get_expr", BuiltinScalarFunction::PgGetExpr),
        ("pg_get_expr_ext", BuiltinScalarFunction::PgGetExpr),
        (
            "pg_relation_is_publishable",
            BuiltinScalarFunction::PgRelationIsPublishable,
        ),
        ("lo_create", BuiltinScalarFunction::LoCreate),
        ("lo_unlink", BuiltinScalarFunction::LoUnlink),
        ("pg_typeof", BuiltinScalarFunction::PgTypeof),
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
        ("to_json", BuiltinScalarFunction::ToJson),
        ("to_jsonb", BuiltinScalarFunction::ToJsonb),
        ("to_tsvector", BuiltinScalarFunction::ToTsVector),
        ("to_tsquery", BuiltinScalarFunction::ToTsQuery),
        ("plainto_tsquery", BuiltinScalarFunction::PlainToTsQuery),
        ("phraseto_tsquery", BuiltinScalarFunction::PhraseToTsQuery),
        (
            "websearch_to_tsquery",
            BuiltinScalarFunction::WebSearchToTsQuery,
        ),
        ("ts_lexize", BuiltinScalarFunction::TsLexize),
        ("array_to_json", BuiltinScalarFunction::ArrayToJson),
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
        ("concat", BuiltinScalarFunction::Concat),
        ("concat_ws", BuiltinScalarFunction::ConcatWs),
        ("format", BuiltinScalarFunction::Format),
        ("left", BuiltinScalarFunction::Left),
        ("right", BuiltinScalarFunction::Right),
        ("lpad", BuiltinScalarFunction::LPad),
        ("rpad", BuiltinScalarFunction::RPad),
        ("repeat", BuiltinScalarFunction::Repeat),
        ("length", BuiltinScalarFunction::Length),
        ("array_ndims", BuiltinScalarFunction::ArrayNdims),
        ("array_dims", BuiltinScalarFunction::ArrayDims),
        ("array_lower", BuiltinScalarFunction::ArrayLower),
        ("array_upper", BuiltinScalarFunction::ArrayUpper),
        ("array_fill", BuiltinScalarFunction::ArrayFill),
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
        ("array_sort", BuiltinScalarFunction::ArraySort),
        ("lower", BuiltinScalarFunction::Lower),
        ("unistr", BuiltinScalarFunction::Unistr),
        ("ascii", BuiltinScalarFunction::Ascii),
        ("chr", BuiltinScalarFunction::Chr),
        ("quote_literal", BuiltinScalarFunction::QuoteLiteral),
        ("replace", BuiltinScalarFunction::Replace),
        ("split_part", BuiltinScalarFunction::SplitPart),
        ("translate", BuiltinScalarFunction::Translate),
        (
            "regprocedure_to_text",
            BuiltinScalarFunction::RegProcedureToText,
        ),
        ("regprocedureout", BuiltinScalarFunction::RegProcedureToText),
        ("regprocout", BuiltinScalarFunction::RegProcedureToText),
        ("regrole_to_text", BuiltinScalarFunction::RegRoleToText),
        ("regroleout", BuiltinScalarFunction::RegRoleToText),
        ("pg_get_userbyid", BuiltinScalarFunction::PgGetUserById),
        ("position", BuiltinScalarFunction::Position),
        ("strpos", BuiltinScalarFunction::Strpos),
        ("substring", BuiltinScalarFunction::Substring),
        ("substr", BuiltinScalarFunction::Substring),
        ("similar_substring", BuiltinScalarFunction::SimilarSubstring),
        ("overlay", BuiltinScalarFunction::Overlay),
        ("reverse", BuiltinScalarFunction::Reverse),
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
        ("convert_from", BuiltinScalarFunction::ConvertFrom),
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
        ("to_number", BuiltinScalarFunction::ToNumber),
        ("abs", BuiltinScalarFunction::Abs),
        ("log", BuiltinScalarFunction::Log),
        ("log10", BuiltinScalarFunction::Log10),
        ("gcd", BuiltinScalarFunction::Gcd),
        ("lcm", BuiltinScalarFunction::Lcm),
        ("div", BuiltinScalarFunction::Div),
        ("mod", BuiltinScalarFunction::Mod),
        ("scale", BuiltinScalarFunction::Scale),
        ("min_scale", BuiltinScalarFunction::MinScale),
        ("trim_scale", BuiltinScalarFunction::TrimScale),
        ("numeric_inc", BuiltinScalarFunction::NumericInc),
        ("factorial", BuiltinScalarFunction::Factorial),
        ("pg_lsn", BuiltinScalarFunction::PgLsn),
        ("trunc", BuiltinScalarFunction::Trunc),
        ("round", BuiltinScalarFunction::Round),
        ("width_bucket", BuiltinScalarFunction::WidthBucket),
        ("ceil", BuiltinScalarFunction::Ceil),
        ("ceiling", BuiltinScalarFunction::Ceiling),
        ("floor", BuiltinScalarFunction::Floor),
        ("sign", BuiltinScalarFunction::Sign),
        ("sqrt", BuiltinScalarFunction::Sqrt),
        ("cbrt", BuiltinScalarFunction::Cbrt),
        ("power", BuiltinScalarFunction::Power),
        ("exp", BuiltinScalarFunction::Exp),
        ("ln", BuiltinScalarFunction::Ln),
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
        ("box", BuiltinScalarFunction::GeoBox),
        ("line", BuiltinScalarFunction::GeoLine),
        ("lseg", BuiltinScalarFunction::GeoLseg),
        ("path", BuiltinScalarFunction::GeoPath),
        ("polygon", BuiltinScalarFunction::GeoPolygon),
        ("circle", BuiltinScalarFunction::GeoCircle),
        ("area", BuiltinScalarFunction::GeoArea),
        ("center", BuiltinScalarFunction::GeoCenter),
        ("poly_center", BuiltinScalarFunction::GeoPolyCenter),
        ("bound_box", BuiltinScalarFunction::GeoBoundBox),
        ("diagonal", BuiltinScalarFunction::GeoDiagonal),
        ("radius", BuiltinScalarFunction::GeoRadius),
        ("diameter", BuiltinScalarFunction::GeoDiameter),
        ("npoints", BuiltinScalarFunction::GeoNpoints),
        ("pclose", BuiltinScalarFunction::GeoPclose),
        ("popen", BuiltinScalarFunction::GeoPopen),
        ("isopen", BuiltinScalarFunction::GeoIsOpen),
        ("isclosed", BuiltinScalarFunction::GeoIsClosed),
        ("slope", BuiltinScalarFunction::GeoSlope),
        ("isvertical", BuiltinScalarFunction::GeoIsVertical),
        ("ishorizontal", BuiltinScalarFunction::GeoIsHorizontal),
        ("height", BuiltinScalarFunction::GeoHeight),
        ("width", BuiltinScalarFunction::GeoWidth),
        ("booleq", BuiltinScalarFunction::BoolEq),
        ("boolne", BuiltinScalarFunction::BoolNe),
        (
            "bitcast_integer_to_float4",
            BuiltinScalarFunction::BitcastIntegerToFloat4,
        ),
        (
            "bitcast_bigint_to_float8",
            BuiltinScalarFunction::BitcastBigintToFloat8,
        ),
        ("xmlcomment", BuiltinScalarFunction::XmlComment),
        ("xml_is_well_formed", BuiltinScalarFunction::XmlIsWellFormed),
        (
            "xml_is_well_formed_document",
            BuiltinScalarFunction::XmlIsWellFormedDocument,
        ),
        (
            "xml_is_well_formed_content",
            BuiltinScalarFunction::XmlIsWellFormedContent,
        ),
        ("pg_input_is_valid", BuiltinScalarFunction::PgInputIsValid),
        ("range_constructor", BuiltinScalarFunction::RangeConstructor),
        ("range_isempty", BuiltinScalarFunction::RangeIsEmpty),
        ("range_lower", BuiltinScalarFunction::RangeLower),
        ("range_upper", BuiltinScalarFunction::RangeUpper),
        ("range_lower_inc", BuiltinScalarFunction::RangeLowerInc),
        ("range_upper_inc", BuiltinScalarFunction::RangeUpperInc),
        ("range_lower_inf", BuiltinScalarFunction::RangeLowerInf),
        ("range_upper_inf", BuiltinScalarFunction::RangeUpperInf),
        ("range_contains", BuiltinScalarFunction::RangeContains),
        (
            "range_contained_by",
            BuiltinScalarFunction::RangeContainedBy,
        ),
        ("range_overlap", BuiltinScalarFunction::RangeOverlap),
        ("range_strict_left", BuiltinScalarFunction::RangeStrictLeft),
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
        for row in bootstrap_pg_proc_rows() {
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
        for row in bootstrap_pg_proc_rows() {
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
        for row in bootstrap_pg_proc_rows() {
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
            BuiltinScalarFunction::ObjDescription,
            BuiltinScalarFunction::PgGetExpr,
        ] {
            if by_func.iter().all(|(candidate, _)| *candidate != func) {
                by_func.push((func, SqlType::new(SqlTypeKind::Text)));
            }
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
            .all(|(candidate, _)| *candidate != BuiltinScalarFunction::XmlComment)
        {
            by_func.push((
                BuiltinScalarFunction::XmlComment,
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
            | BuiltinScalarFunction::TsQueryAnd
            | BuiltinScalarFunction::TsQueryOr
            | BuiltinScalarFunction::TsQueryNot
            | BuiltinScalarFunction::TsVectorConcat
            | BuiltinScalarFunction::RandomNormal
            | BuiltinScalarFunction::CashLarger
            | BuiltinScalarFunction::CashSmaller
            | BuiltinScalarFunction::CashWords
            | BuiltinScalarFunction::Now
            | BuiltinScalarFunction::TransactionTimestamp
            | BuiltinScalarFunction::StatementTimestamp
            | BuiltinScalarFunction::ClockTimestamp
            | BuiltinScalarFunction::TimeOfDay
            | BuiltinScalarFunction::NextVal
            | BuiltinScalarFunction::CurrVal
            | BuiltinScalarFunction::SetVal
            | BuiltinScalarFunction::PgGetSerialSequence
            | BuiltinScalarFunction::PgGetUserById
            | BuiltinScalarFunction::ObjDescription
            | BuiltinScalarFunction::PgGetExpr
            | BuiltinScalarFunction::PgRelationIsPublishable
            | BuiltinScalarFunction::GetDatabaseEncoding
            | BuiltinScalarFunction::PgMyTempSchema
            | BuiltinScalarFunction::PgRustTestFdwHandler
            | BuiltinScalarFunction::PgStatGetCheckpointerNumTimed
            | BuiltinScalarFunction::PgStatGetCheckpointerNumRequested
            | BuiltinScalarFunction::PgStatGetCheckpointerNumPerformed
            | BuiltinScalarFunction::PgStatGetCheckpointerBuffersWritten
            | BuiltinScalarFunction::PgStatGetCheckpointerSlruWritten
            | BuiltinScalarFunction::PgStatGetCheckpointerWriteTime
            | BuiltinScalarFunction::PgStatGetCheckpointerSyncTime
            | BuiltinScalarFunction::PgStatGetCheckpointerStatResetTime
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
            | BuiltinScalarFunction::ArrayNdims
            | BuiltinScalarFunction::ArrayDims
            | BuiltinScalarFunction::ArrayLower
            | BuiltinScalarFunction::ArrayUpper
            | BuiltinScalarFunction::Lower
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
            | BuiltinScalarFunction::ConvertFrom
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
            | BuiltinScalarFunction::ToNumber
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
            | BuiltinScalarFunction::BitcastIntegerToFloat4
            | BuiltinScalarFunction::BitcastBigintToFloat8
            | BuiltinScalarFunction::XmlComment
            | BuiltinScalarFunction::XmlIsWellFormed
            | BuiltinScalarFunction::XmlIsWellFormedDocument
            | BuiltinScalarFunction::XmlIsWellFormedContent
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
            | BuiltinScalarFunction::Trunc
            | BuiltinScalarFunction::Round
            | BuiltinScalarFunction::Substring
            | BuiltinScalarFunction::SimilarSubstring
            | BuiltinScalarFunction::Overlay
            | BuiltinScalarFunction::LPad
            | BuiltinScalarFunction::RPad
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
    )
}

fn aggregate_arity_overrides() -> &'static Vec<(AggFunc, usize)> {
    static ARITIES: OnceLock<Vec<(AggFunc, usize)>> = OnceLock::new();
    ARITIES.get_or_init(|| {
        let mut by_func = Vec::new();
        for row in bootstrap_pg_proc_rows() {
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
        for row in bootstrap_pg_proc_rows() {
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
            | AggFunc::JsonbObjectAgg
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
    if catalog
        .type_by_oid(target_oid)
        .is_some_and(|row| row.sql_type.is_range() || row.sql_type.is_multirange())
    {
        return true;
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
        "min" => Some(AggFunc::Min),
        "max" => Some(AggFunc::Max),
        "string_agg" => Some(AggFunc::StringAgg),
        "array_agg" => Some(AggFunc::ArrayAgg),
        "json_agg" => Some(AggFunc::JsonAgg),
        "jsonb_agg" => Some(AggFunc::JsonbAgg),
        "json_object_agg" => Some(AggFunc::JsonObjectAgg),
        "jsonb_object_agg" => Some(AggFunc::JsonbObjectAgg),
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
            resolve_scalar_function("float8_accum"),
            Some(BuiltinScalarFunction::Float8Accum)
        );
        assert_eq!(resolve_scalar_function("count"), None);
        assert_eq!(resolve_scalar_function("json_array_elements"), None);
        assert_eq!(resolve_scalar_function("int4"), None);
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
        assert_eq!(resolved.proc_oid, 6243);
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
            ParseError::UnexpectedToken { expected, actual }
                if expected == "supported function" && actual == "unnest"
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
            Some(SqlType::new(SqlTypeKind::Text))
        );
        assert_eq!(
            fixed_scalar_return_type(BuiltinScalarFunction::ToJsonb),
            Some(SqlType::new(SqlTypeKind::Jsonb))
        );
        assert_eq!(fixed_scalar_return_type(BuiltinScalarFunction::Abs), None);
        assert_eq!(
            fixed_scalar_return_type(BuiltinScalarFunction::Substring),
            None
        );
    }
}
