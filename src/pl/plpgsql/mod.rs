mod ast;
mod gram;

use crate::backend::executor::{ExecError, StatementResult};
use crate::backend::parser::{DoStatement, ParseError};

pub use ast::*;
pub use gram::parse_block;

pub fn execute_do(stmt: &DoStatement) -> Result<StatementResult, ExecError> {
    let language = stmt
        .language
        .as_deref()
        .unwrap_or("plpgsql")
        .to_ascii_lowercase();
    if language != "plpgsql" {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "LANGUAGE plpgsql",
            actual: format!("LANGUAGE {}", stmt.language.as_deref().unwrap_or("plpgsql")),
        }));
    }
    parse_block(&stmt.code)?;
    Err(ExecError::Parse(ParseError::UnexpectedToken {
        expected: "implemented plpgsql runtime",
        actual: "DO statement not yet implemented".into(),
    }))
}
