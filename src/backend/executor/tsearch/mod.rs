use std::cmp::Ordering;

use crate::backend::executor::ExecError;
use crate::backend::parser::CatalogLookup;
use crate::include::catalog::{PgTsConfigMapRow, PgTsConfigRow, PgTsDictRow};
use crate::include::nodes::datum::Value;
use crate::include::nodes::tsearch::{TsQuery, TsVector, TsWeight};

// :HACK: Keep the historical root executor module path while text search
// scalar helpers live in `pgrust_expr`.
pub(crate) fn ts_headline(
    config_name: Option<&str>,
    document: &str,
    query: &TsQuery,
    options: Option<&str>,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<String, ExecError> {
    let adapter = catalog.map(ExprCatalogAdapter);
    let catalog = adapter
        .as_ref()
        .map(|adapter| adapter as &dyn pgrust_expr::ExprCatalogLookup);
    pgrust_expr::tsearch::ts_headline(config_name, document, query, options, catalog)
        .map_err(Into::into)
}

struct ExprCatalogAdapter<'a>(&'a dyn CatalogLookup);

impl pgrust_expr::ExprCatalogLookup for ExprCatalogAdapter<'_> {
    fn ts_config_rows(&self) -> Vec<PgTsConfigRow> {
        self.0.ts_config_rows()
    }

    fn ts_dict_rows(&self) -> Vec<PgTsDictRow> {
        self.0.ts_dict_rows()
    }

    fn ts_config_map_rows(&self) -> Vec<PgTsConfigMapRow> {
        self.0.ts_config_map_rows()
    }
}

pub(crate) fn eval_tsvector_matches_tsquery(vector: &TsVector, query: &TsQuery) -> bool {
    pgrust_expr::tsearch::eval_tsvector_matches_tsquery(vector, query)
}

pub(crate) fn eval_tsquery_matches_tsvector(query: &TsQuery, vector: &TsVector) -> bool {
    pgrust_expr::tsearch::eval_tsquery_matches_tsvector(query, vector)
}

pub(crate) fn ts_rank(
    vector: &TsVector,
    query: &TsQuery,
    weights: Option<[f64; 4]>,
    normalization: i32,
) -> f64 {
    pgrust_expr::tsearch::ts_rank(vector, query, weights, normalization)
}

pub(crate) fn ts_rank_cd(
    vector: &TsVector,
    query: &TsQuery,
    weights: Option<[f64; 4]>,
    normalization: i32,
) -> f64 {
    pgrust_expr::tsearch::ts_rank_cd(vector, query, weights, normalization)
}

pub(crate) fn parse_tsquery_text(text: &str) -> Result<TsQuery, ExecError> {
    pgrust_expr::tsearch::parse_tsquery_text(text).map_err(Into::into)
}

pub(crate) fn render_tsquery_text(query: &TsQuery) -> String {
    pgrust_expr::tsearch::render_tsquery_text(query)
}

pub(crate) fn encode_tsquery_bytes(query: &TsQuery) -> Vec<u8> {
    pgrust_expr::tsearch::encode_tsquery_bytes(query)
}

pub(crate) fn decode_tsquery_bytes(bytes: &[u8]) -> Result<TsQuery, ExecError> {
    pgrust_expr::tsearch::decode_tsquery_bytes(bytes).map_err(Into::into)
}

pub(crate) fn tsquery_input_error(message: String) -> ExecError {
    pgrust_expr::tsearch::tsquery_input_error(message).into()
}

pub(crate) fn canonicalize_tsquery_rewrite_result(query: TsQuery) -> TsQuery {
    pgrust_expr::tsearch::canonicalize_tsquery_rewrite_result(query)
}

pub(crate) fn compare_tsquery(left: &TsQuery, right: &TsQuery) -> Ordering {
    pgrust_expr::tsearch::compare_tsquery(left, right)
}

pub(crate) fn numnode(query: &TsQuery) -> i32 {
    pgrust_expr::tsearch::numnode(query)
}

pub(crate) fn tsquery_and(left: TsQuery, right: TsQuery) -> TsQuery {
    pgrust_expr::tsearch::tsquery_and(left, right)
}

pub(crate) fn tsquery_contained_by(left: &TsQuery, right: &TsQuery) -> bool {
    pgrust_expr::tsearch::tsquery_contained_by(left, right)
}

pub(crate) fn tsquery_contains(left: &TsQuery, right: &TsQuery) -> bool {
    pgrust_expr::tsearch::tsquery_contains(left, right)
}

pub(crate) fn tsquery_not(query: TsQuery) -> TsQuery {
    pgrust_expr::tsearch::tsquery_not(query)
}

pub(crate) fn tsquery_operands(query: &TsQuery) -> Vec<String> {
    pgrust_expr::tsearch::tsquery_operands(query)
}

pub(crate) fn tsquery_or(left: TsQuery, right: TsQuery) -> TsQuery {
    pgrust_expr::tsearch::tsquery_or(left, right)
}

pub(crate) fn tsquery_phrase(left: TsQuery, right: TsQuery, distance: u16) -> TsQuery {
    pgrust_expr::tsearch::tsquery_phrase(left, right, distance)
}

pub(crate) fn tsquery_rewrite(query: TsQuery, target: TsQuery, substitute: TsQuery) -> TsQuery {
    pgrust_expr::tsearch::tsquery_rewrite(query, target, substitute)
}

pub(crate) fn parse_tsvector_text(text: &str) -> Result<TsVector, ExecError> {
    pgrust_expr::tsearch::parse_tsvector_text(text).map_err(Into::into)
}

pub(crate) fn render_tsvector_text(vector: &TsVector) -> String {
    pgrust_expr::tsearch::render_tsvector_text(vector)
}

pub(crate) fn encode_tsvector_bytes(vector: &TsVector) -> Vec<u8> {
    pgrust_expr::tsearch::encode_tsvector_bytes(vector)
}

pub(crate) fn decode_tsvector_bytes(bytes: &[u8]) -> Result<TsVector, ExecError> {
    pgrust_expr::tsearch::decode_tsvector_bytes(bytes).map_err(Into::into)
}

pub(crate) fn tsvector_input_error(message: String) -> ExecError {
    pgrust_expr::tsearch::tsvector_input_error(message).into()
}

pub(crate) fn array_to_tsvector(value: &Value) -> Result<TsVector, ExecError> {
    pgrust_expr::tsearch::array_to_tsvector(value).map_err(Into::into)
}

pub(crate) fn compare_tsvector(left: &TsVector, right: &TsVector) -> Ordering {
    pgrust_expr::tsearch::compare_tsvector(left, right)
}

pub(crate) fn concat_tsvector(left: &TsVector, right: &TsVector) -> TsVector {
    pgrust_expr::tsearch::concat_tsvector(left, right)
}

pub(crate) fn delete_tsvector_lexemes(vector: &TsVector, lexemes: &[String]) -> TsVector {
    pgrust_expr::tsearch::delete_tsvector_lexemes(vector, lexemes)
}

pub(crate) fn filter_tsvector(vector: &TsVector, weights: &Value) -> Result<TsVector, ExecError> {
    pgrust_expr::tsearch::filter_tsvector(vector, weights).map_err(Into::into)
}

pub(crate) fn parse_ts_weight(value: &Value, op: &'static str) -> Result<TsWeight, ExecError> {
    pgrust_expr::tsearch::parse_ts_weight(value, op).map_err(Into::into)
}

pub(crate) fn setweight_tsvector(
    vector: &TsVector,
    weight: TsWeight,
    filter: Option<&Value>,
) -> Result<TsVector, ExecError> {
    pgrust_expr::tsearch::setweight_tsvector(vector, weight, filter).map_err(Into::into)
}

pub(crate) fn strip_tsvector(vector: &TsVector) -> TsVector {
    pgrust_expr::tsearch::strip_tsvector(vector)
}

pub(crate) fn text_array_items(
    value: &Value,
    op: &'static str,
) -> Result<Vec<Option<String>>, ExecError> {
    pgrust_expr::tsearch::text_array_items(value, op).map_err(Into::into)
}

pub(crate) fn tsvector_to_array(vector: &TsVector) -> Value {
    pgrust_expr::tsearch::tsvector_to_array(vector)
}

pub(crate) fn unnest_tsvector(vector: &TsVector) -> Vec<Value> {
    pgrust_expr::tsearch::unnest_tsvector(vector)
}
