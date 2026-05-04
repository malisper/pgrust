#![allow(dead_code, private_interfaces)]

pub mod comments;
pub mod gram;
pub mod notices;

pub use gram::{
    ParseOptions, Rule, SQL_JSON_ARRAY_FUNC, SQL_JSON_ARRAYAGG_FUNC, SQL_JSON_FUNC,
    SQL_JSON_IS_JSON_FUNC, SQL_JSON_OBJECT_FUNC, SQL_JSON_OBJECTAGG_FUNC, SQL_JSON_SCALAR_FUNC,
    SQL_JSON_SERIALIZE_FUNC, parse_expr, parse_operator_argtypes, parse_operator_name,
    parse_statement, parse_statement_with_options, parse_type_name, pest_parse_keyword,
    security_label_provider_error, wrap_values_as_select,
};
pub use notices::{ParserNotice, clear_notices, take_notices};
pub use pgrust_nodes::parsenodes::{ParseError, SelectStatement, Statement};

pub fn parse_select(sql: &str) -> Result<SelectStatement, ParseError> {
    let stmt = parse_statement(sql)?;
    match stmt {
        Statement::Select(stmt) => Ok(stmt),
        other => Err(ParseError::UnexpectedToken {
            expected: "SELECT",
            actual: format!("{other:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use pgrust_core::stack_depth::MIN_MAX_STACK_DEPTH_KB;
    use pgrust_nodes::parsenodes::{RawTypeName, SqlExpr, SqlTypeKind, Statement};

    use super::{
        ParseOptions, Rule, clear_notices, parse_expr, parse_statement_with_options,
        parse_type_name, pest_parse_keyword, take_notices,
    };

    #[test]
    fn parses_statement_expression_and_type_name() {
        let stmt = parse_statement_with_options(
            "select 1::int4 as n",
            ParseOptions {
                standard_conforming_strings: true,
                max_stack_depth_kb: MIN_MAX_STACK_DEPTH_KB,
            },
        )
        .expect("statement parses");
        let Statement::Select(select) = stmt else {
            panic!("expected select");
        };
        assert_eq!(select.targets.len(), 1);

        let expr = parse_expr("1 + 2").expect("expression parses");
        assert!(matches!(expr, SqlExpr::Add(_, _)));

        let type_name = parse_type_name("text").expect("type name parses");
        assert!(matches!(
            type_name,
            RawTypeName::Builtin(ty) if ty.kind == SqlTypeKind::Text
        ));
    }

    #[test]
    fn exposes_pest_keyword_helper() {
        let keyword = pest_parse_keyword(Rule::kw_select_atom, "select").expect("keyword parses");
        assert_eq!(keyword, "select");
    }

    #[test]
    fn queues_parser_notices() {
        clear_notices();
        let _ = parse_statement_with_options(
            "select looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong",
            ParseOptions {
                standard_conforming_strings: true,
                max_stack_depth_kb: MIN_MAX_STACK_DEPTH_KB,
            },
        );
        let notices = take_notices();
        assert!(
            notices
                .iter()
                .any(|notice| notice.message.contains("will be truncated"))
        );
    }
}
