//! FAMILY: split/join, the Split*String identifier/path parsers, format() /
//! concat(), and string_agg.
//!
//! `split_part`, `text_to_array`/`text_to_array_null`/`text_to_table*`,
//! `array_to_text`/`array_to_text_null`/`array_to_text_internal`,
//! `split_text`/`split_text_accum_result`, `text_isequal`,
//! `textToQualifiedNameList`, `SplitIdentifierString`,
//! `SplitDirectoriesString`, `SplitGUCList`, `appendStringInfoText`,
//! `text_concat`/`text_concat_ws`/`concat_internal`/`build_concat_foutcache`,
//! `text_format`/`text_format_nv` + the four `text_format_*` parse/append
//! helpers, the `makeStringAggState`/`string_agg_*`/`bytea_string_agg_*`
//! aggregate family, and `pg_column_size`/`pg_column_compression`/
//! `pg_column_toast_chunk_id`.
//!
//! Genuinely-external owners: the array build/deconstruct subsystem
//! (`arrayfuncs`) and tuplestore for split/join, the fmgr type-output
//! dispatch for `format()`/`concat()`, the regex owner for the
//! `replace_text_regexp` seam (declared here, body in `replace_text_regexp`),
//! and the TOAST owner for `pg_column_*` (chunk-id / compression method).
//!
//! Depends on the keystone for [`SplitTextOutputData`](crate::keystone) and
//! `appendStringInfoText`-style carrier building.

#![allow(unused_variables)]

use mcx::{Mcx, PgString, PgVec};
use types_core::Oid;
use types_error::PgResult;

/// C: `SplitIdentifierString(rawstring, separator, &namelist)` — parse a
/// `separator`-separated list of (possibly quoted) identifiers, downcasing
/// per identifier rules. `Ok(None)` is the C `false` (syntax error). Owner
/// seam `split_identifier_string` routes here.
pub fn split_identifier_string<'mcx>(
    mcx: Mcx<'mcx>,
    raw: &str,
    separator: char,
) -> PgResult<Option<PgVec<'mcx, PgString<'mcx>>>> {
    todo!("split_format family: port SplitIdentifierString")
}

/// C: `SplitDirectoriesString(rawstring, ',', &elemlist)` — split a
/// comma-separated, possibly-quoted directory list into canonicalized path
/// elements. Owner seam `split_directories_string` routes here.
pub fn split_directories_string<'mcx>(
    mcx: Mcx<'mcx>,
    rawstring: &str,
) -> PgResult<Option<PgVec<'mcx, PgString<'mcx>>>> {
    todo!("split_format family: port SplitDirectoriesString")
}

/// C: `SplitGUCList(rawstring, separator, &namelist)` — like
/// `SplitIdentifierString` but with GUC-list quoting rules (no downcasing).
pub fn split_guc_list<'mcx>(
    mcx: Mcx<'mcx>,
    rawstring: &str,
    separator: char,
) -> PgResult<Option<PgVec<'mcx, PgString<'mcx>>>> {
    todo!("split_format family: port SplitGUCList")
}

/// C: `textToQualifiedNameList(text *textval)` — split a qualified name on
/// `.` into its parts. Owner seam `text_to_qualified_name_list` routes here.
pub fn text_to_qualified_name_list<'mcx>(
    mcx: Mcx<'mcx>,
    textval: &[u8],
) -> PgResult<PgVec<'mcx, PgString<'mcx>>> {
    todo!("split_format family: port textToQualifiedNameList")
}

/// C: `split_part(PG_FUNCTION_ARGS)` — return the n'th field of `text` split
/// on `fldsep`.
pub fn split_part<'mcx>(
    mcx: Mcx<'mcx>,
    inputstring: &[u8],
    fldsep: &[u8],
    fldnum: i32,
    collid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("split_format family: port split_part")
}

/// C: `text_concat(PG_FUNCTION_ARGS)` — `concat()` over a variadic argument
/// list (fmgr type-output dispatch via seam).
pub fn text_concat<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u8>> {
    todo!("split_format family: port text_concat / concat_internal")
}

/// C: `text_format(PG_FUNCTION_ARGS)` — SQL `format()` with `%s`/`%I`/`%L`
/// conversions and positional/width specifiers.
pub fn text_format<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u8>> {
    todo!("split_format family: port text_format + text_format_* helpers")
}

/// C: `pg_column_size(PG_FUNCTION_ARGS)` — on-disk size of a value (consults
/// the TOAST owner for external/compressed attrs).
pub fn pg_column_size(attr: &[u8]) -> PgResult<i32> {
    todo!("split_format family: port pg_column_size (TOAST owner seam)")
}

/// C: `makeStringAggState` + `string_agg_transfn` — accumulate into a
/// per-aggcontext `StringInfo`. The full string_agg / bytea_string_agg family
/// (transfn/combine/serialize/deserialize/finalfn) is filled here.
pub fn string_agg_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<PgString<'mcx>>,
    value: Option<&[u8]>,
    delim: Option<&[u8]>,
) -> PgResult<Option<PgString<'mcx>>> {
    todo!("split_format family: port string_agg_transfn")
}
