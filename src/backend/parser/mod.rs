pub mod parsenodes {
    pub use crate::include::nodes::parsenodes::*;
}
pub mod analyze;
pub mod gram;

pub use crate::include::nodes::parsenodes::*;
pub use analyze::*;
pub use gram::parse_statement;

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
