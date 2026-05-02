use crate::backend::utils::misc::notices::push_backend_notice_with_hint;
use crate::include::nodes::parsenodes::{
    ParseError, RawTypeName, SelectStatement, SqlExpr, Statement, ValuesStatement,
};

pub use pgrust_parser::gram::{
    ParseOptions, Rule, SQL_JSON_ARRAY_FUNC, SQL_JSON_ARRAYAGG_FUNC, SQL_JSON_FUNC,
    SQL_JSON_IS_JSON_FUNC, SQL_JSON_OBJECT_FUNC, SQL_JSON_OBJECTAGG_FUNC, SQL_JSON_SCALAR_FUNC,
    SQL_JSON_SERIALIZE_FUNC, security_label_provider_error,
};

fn replay_parser_notices() {
    for notice in pgrust_parser::take_notices() {
        push_backend_notice_with_hint(
            notice.severity,
            notice.sqlstate,
            notice.message,
            notice.detail,
            notice.hint,
            notice.position,
        );
    }
}

pub fn parse_statement(sql: &str) -> Result<Statement, ParseError> {
    let result = pgrust_parser::parse_statement(sql);
    replay_parser_notices();
    result
}

pub fn parse_statement_with_options(
    sql: &str,
    options: ParseOptions,
) -> Result<Statement, ParseError> {
    let result = pgrust_parser::parse_statement_with_options(sql, options);
    replay_parser_notices();
    result
}

pub fn parse_expr(sql: &str) -> Result<SqlExpr, ParseError> {
    let result = pgrust_parser::parse_expr(sql);
    replay_parser_notices();
    result
}

pub fn parse_type_name(sql: &str) -> Result<RawTypeName, ParseError> {
    let result = pgrust_parser::parse_type_name(sql);
    replay_parser_notices();
    result
}

pub fn parse_operator_name(input: &str) -> Result<((Option<String>, String), &str), ParseError> {
    let result = pgrust_parser::parse_operator_name(input);
    replay_parser_notices();
    result
}

pub fn parse_operator_argtypes(
    input: &str,
) -> Result<((Option<RawTypeName>, Option<RawTypeName>), &str), ParseError> {
    let result = pgrust_parser::parse_operator_argtypes(input);
    replay_parser_notices();
    result
}

pub(crate) fn pest_parse_keyword(rule: Rule, input: &str) -> Result<String, ParseError> {
    let result = pgrust_parser::pest_parse_keyword(rule, input);
    replay_parser_notices();
    result
}

pub(crate) fn wrap_values_as_select(stmt: ValuesStatement) -> SelectStatement {
    pgrust_parser::wrap_values_as_select(stmt)
}
