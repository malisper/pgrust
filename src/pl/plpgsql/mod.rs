mod ast;
mod compile;
mod exec;
mod gram;

use crate::backend::executor::{ExecError, StatementResult};
use crate::backend::parser::{Catalog, DoStatement, ParseError};

pub use ast::*;
pub use compile::CompiledFunction;
pub use exec::{
    PlpgsqlNotice, TriggerCallContext, TriggerFunctionResult, TriggerOperation, clear_notices,
    take_notices,
};
pub(crate) use exec::{
    execute_user_defined_scalar_function, execute_user_defined_set_returning_function,
    execute_user_defined_trigger_function,
};
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
    fn execute_do_runs_elsif_branch() {
        let stmt = DoStatement {
            language: None,
            code: r#"
                begin
                    if 1 = 0 then
                        raise exception 'wrong if';
                    elsif 2 = 2 then
                        raise notice 'elsif';
                    else
                        raise exception 'wrong else';
                    end if;
                end
            "#
            .into(),
        };

        let result = execute_do(&stmt).unwrap();
        assert_eq!(result, StatementResult::AffectedRows(0));
        assert_eq!(
            take_notices(),
            vec![PlpgsqlNotice {
                level: RaiseLevel::Notice,
                message: "elsif".into(),
            }]
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

    #[test]
    fn execute_do_accepts_top_level_end_semicolon() {
        let stmt = DoStatement {
            language: None,
            code: "begin raise notice 'done'; end;".into(),
        };

        let result = execute_do(&stmt).unwrap();
        assert_eq!(result, StatementResult::AffectedRows(0));
        assert_eq!(
            take_notices(),
            vec![PlpgsqlNotice {
                level: RaiseLevel::Notice,
                message: "done".into(),
            }]
        );
    }

    #[test]
    fn execute_do_runs_while_loop() {
        std::thread::Builder::new()
            .name("execute_do_runs_while_loop".into())
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                let stmt = DoStatement {
                    language: None,
                    code: r#"
                        declare
                            o int;
                            a int[] := array[1,2,3,2,3,1,2];
                        begin
                            o := array_position(a, 2);
                            while o is not null
                            loop
                                raise notice '%', o;
                                o := array_position(a, 2, o + 1);
                            end loop;
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
                            message: "2".into(),
                        },
                        PlpgsqlNotice {
                            level: RaiseLevel::Notice,
                            message: "4".into(),
                        },
                        PlpgsqlNotice {
                            level: RaiseLevel::Notice,
                            message: "7".into(),
                        }
                    ]
                );
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn parse_block_accepts_comments_in_declare_section() {
        let block = parse_block(
            r#"
                declare
                    n int4 := 1000;        -- sample count
                    c float8 := 1.94947;   /* critical value */
                begin
                    null;
                end
            "#,
        )
        .unwrap();

        assert_eq!(block.declarations.len(), 2);
        let Decl::Var(n_decl) = &block.declarations[0] else {
            panic!("expected variable declaration");
        };
        let Decl::Var(c_decl) = &block.declarations[1] else {
            panic!("expected variable declaration");
        };
        assert_eq!(n_decl.name, "n");
        assert_eq!(n_decl.default_expr.as_deref(), Some("1000"));
        assert_eq!(c_decl.name, "c");
        assert_eq!(c_decl.default_expr.as_deref(), Some("1.94947"));
    }

    #[test]
    fn parse_block_accepts_top_level_end_semicolon() {
        let block = parse_block(
            r#"
                begin
                    null;
                end;
            "#,
        )
        .unwrap();

        assert_eq!(block.declarations.len(), 0);
        assert_eq!(block.statements.len(), 1);
    }
}
