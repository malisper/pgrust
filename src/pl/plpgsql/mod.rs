mod ast;
mod compile;
mod exec;
mod gram;

use crate::backend::executor::{ExecError, StatementResult};
use crate::backend::parser::{Catalog, DoStatement, ParseError};

pub use ast::*;
pub use exec::{PlpgsqlNotice, clear_notices, take_notices};
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
    exec::clear_notices();
    let block = parse_block(&stmt.code)?;
    let compiled = compile::compile_do_block(&block, &Catalog::default())?;
    exec::execute_block(&compiled)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::executor::StatementResult;

    #[test]
    fn execute_do_runs_core_control_flow() {
        let stmt = DoStatement {
            language: None,
            code: r#"
                declare
                    total int4 := 0;
                begin
                    total := total + 1;
                    if total > 0 then
                        raise notice 'value %', total;
                    elsif total < 0 then
                        raise warning 'bad';
                    else
                        null;
                    end if;
                    for i in 1..3 loop
                        total := total + i;
                    end loop;
                    if total = 7 then
                        raise notice 'done %', total;
                    else
                        raise exception 'wrong total %', total;
                    end if;
                end
            "#
            .into(),
        };

        let result = execute_do(&stmt).unwrap();
        assert_eq!(result, StatementResult::AffectedRows(0));
        assert_eq!(
            take_notices(),
            vec![
                PlpgsqlNotice {
                    level: RaiseLevel::Notice,
                    message: "value 1".into(),
                },
                PlpgsqlNotice {
                    level: RaiseLevel::Notice,
                    message: "done 7".into(),
                }
            ]
        );
    }

    #[test]
    fn execute_do_rejects_non_plpgsql_language() {
        let stmt = DoStatement {
            language: Some("sql".into()),
            code: "begin null; end".into(),
        };
        let err = execute_do(&stmt).unwrap_err();
        assert!(matches!(
            err,
            ExecError::Parse(ParseError::UnexpectedToken { .. })
        ));
    }

    #[test]
    fn execute_do_raise_exception_surfaces_message() {
        let stmt = DoStatement {
            language: None,
            code: "begin raise exception 'boom %', 42; end".into(),
        };
        let err = execute_do(&stmt).unwrap_err();
        assert!(matches!(
            err,
            ExecError::RaiseException(message) if message == "boom 42"
        ));
    }
}
