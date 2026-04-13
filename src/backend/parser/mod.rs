pub mod parsenodes {
    pub use crate::include::nodes::parsenodes::*;
}
pub mod analyze;
pub(crate) mod comments;
pub mod gram;

pub use crate::include::nodes::parsenodes::*;
pub use analyze::*;
pub use gram::{ParseOptions, parse_expr, parse_statement, parse_statement_with_options, parse_type_name};

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
