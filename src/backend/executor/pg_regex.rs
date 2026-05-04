use super::expr_ops::TextCollationSemantics;
use super::{ExecError, Value};

pub(crate) type CompiledPgRegex = pgrust_expr::pg_regex::CompiledPgRegex;

// :HACK: Keep the historical root executor module path while PostgreSQL regex
// helpers live in `pgrust_expr`.
pub(crate) fn compile_pg_regex_predicate(pattern: &str) -> Result<CompiledPgRegex, ExecError> {
    pgrust_expr::pg_regex::compile_pg_regex_predicate(pattern).map_err(Into::into)
}

pub(crate) fn pg_regex_is_match(compiled: &CompiledPgRegex, text: &str) -> Result<bool, ExecError> {
    pgrust_expr::pg_regex::pg_regex_is_match(compiled, text).map_err(Into::into)
}

pub(super) fn eval_regex_match_operator(
    left: &Value,
    right: &Value,
    collation: TextCollationSemantics,
) -> Result<Value, ExecError> {
    pgrust_expr::pg_regex::eval_regex_match_operator(left, right, expr_collation(collation))
        .map_err(Into::into)
}

pub(super) fn eval_sql_regex_substring(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::pg_regex::eval_sql_regex_substring(values).map_err(Into::into)
}

pub(super) fn eval_similar_substring(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::pg_regex::eval_similar_substring(values).map_err(Into::into)
}

pub(super) fn eval_similar(
    left: &Value,
    pattern: &Value,
    escape: Option<&Value>,
    collation_oid: Option<u32>,
    negated: bool,
) -> Result<Value, ExecError> {
    pgrust_expr::pg_regex::eval_similar(left, pattern, escape, collation_oid, negated)
        .map_err(Into::into)
}

pub(super) fn eval_regexp_like(
    values: &[Value],
    collation: TextCollationSemantics,
) -> Result<Value, ExecError> {
    pgrust_expr::pg_regex::eval_regexp_like(values, expr_collation(collation)).map_err(Into::into)
}

pub(crate) fn eval_jsonpath_like_regex(
    text: &str,
    pattern: &str,
    flags: &str,
) -> Result<bool, ExecError> {
    pgrust_expr::pg_regex::eval_jsonpath_like_regex(text, pattern, flags).map_err(Into::into)
}

pub(crate) fn validate_jsonpath_like_regex(pattern: &str, flags: &str) -> Result<(), ExecError> {
    pgrust_expr::pg_regex::validate_jsonpath_like_regex(pattern, flags).map_err(Into::into)
}

pub(super) fn eval_regexp_match(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::pg_regex::eval_regexp_match(values).map_err(Into::into)
}

pub(super) fn eval_regexp_count(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::pg_regex::eval_regexp_count(values).map_err(Into::into)
}

pub(super) fn eval_regexp_instr(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::pg_regex::eval_regexp_instr(values).map_err(Into::into)
}

pub(super) fn eval_regexp_substr(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::pg_regex::eval_regexp_substr(values).map_err(Into::into)
}

pub(super) fn eval_regexp_replace(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::pg_regex::eval_regexp_replace(values).map_err(Into::into)
}

pub(super) fn eval_regexp_split_to_array(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::pg_regex::eval_regexp_split_to_array(values).map_err(Into::into)
}

pub(super) fn eval_regexp_matches_rows(values: &[Value]) -> Result<Vec<Value>, ExecError> {
    pgrust_expr::pg_regex::eval_regexp_matches_rows(values).map_err(Into::into)
}

pub(super) fn eval_regexp_split_to_table_rows(values: &[Value]) -> Result<Vec<Value>, ExecError> {
    pgrust_expr::pg_regex::eval_regexp_split_to_table_rows(values).map_err(Into::into)
}

pub(crate) fn explain_similar_pattern(
    pattern: &str,
    escape: Option<&str>,
) -> Result<String, ExecError> {
    pgrust_expr::pg_regex::explain_similar_pattern(pattern, escape).map_err(Into::into)
}

fn expr_collation(
    collation: TextCollationSemantics,
) -> pgrust_expr::expr_ops::TextCollationSemantics {
    match collation {
        TextCollationSemantics::Default => pgrust_expr::expr_ops::TextCollationSemantics::Default,
        TextCollationSemantics::Ascii => pgrust_expr::expr_ops::TextCollationSemantics::Ascii,
        TextCollationSemantics::PgCUtf8 => pgrust_expr::expr_ops::TextCollationSemantics::PgCUtf8,
        TextCollationSemantics::PgUnicodeFast => {
            pgrust_expr::expr_ops::TextCollationSemantics::PgUnicodeFast
        }
    }
}
