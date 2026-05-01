pub mod parsenodes {
    pub use crate::include::nodes::parsenodes::*;
}
pub mod analyze;
pub(crate) mod comments;
pub mod gram;

pub use crate::include::nodes::parsenodes::*;
pub use analyze::*;
pub(crate) use gram::wrap_values_as_select;
pub use gram::{
    ParseOptions, parse_expr, parse_statement, parse_statement_with_options, parse_type_name,
    security_label_provider_error,
};

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
mod tests;
