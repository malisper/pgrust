mod ast;
pub mod cache;
mod compile;
mod exec;
mod gram;

use std::collections::HashMap;

use crate::backend::executor::{ExecError, ExecutorContext, StatementResult};
use crate::backend::parser::{Catalog, CatalogLookup, DoStatement, ParseError};

pub use cache::PlpgsqlFunctionCache;
pub use exec::{
    clear_notices, current_event_trigger_ddl_commands, current_event_trigger_dropped_objects,
    current_event_trigger_table_rewrite, take_notices,
};
pub(crate) use exec::{
    execute_user_defined_event_trigger_function, execute_user_defined_procedure_values,
    execute_user_defined_scalar_function, execute_user_defined_scalar_function_values,
    execute_user_defined_scalar_function_values_with_arg_types,
    execute_user_defined_set_returning_function, execute_user_defined_trigger_function,
};
pub use pgrust_nodes::{
    EventTriggerCallContext, EventTriggerDdlCommandRow, EventTriggerDroppedObjectRow,
    TriggerCallContext, TriggerFunctionResult, TriggerOperation, TriggerTransitionCapture,
    TriggerTransitionTable,
};
pub use pgrust_plpgsql::CompiledFunction;
pub use pgrust_plpgsql::PlpgsqlNotice;
pub use pgrust_plpgsql::*;

pub fn execute_do(stmt: &DoStatement) -> Result<StatementResult, ExecError> {
    let gucs = HashMap::new();
    execute_do_with_gucs(stmt, &gucs)
}

pub fn execute_do_with_gucs(
    stmt: &DoStatement,
    gucs: &HashMap<String, String>,
) -> Result<StatementResult, ExecError> {
    stacker::maybe_grow(32 * 1024, 32 * 1024 * 1024, || {
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
        let compiled = compile::compile_do_block_with_gucs(&block, &Catalog::default(), Some(gucs))
            .map_err(plpgsql_do_compile_error)?;
        exec::execute_block_with_gucs(&compiled, gucs)
    })
}

pub(crate) fn execute_do_with_context(
    stmt: &DoStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    execute_do_with_context_options(stmt, catalog, ctx, true)
}

pub(crate) fn execute_do_with_context_preserving_notices(
    stmt: &DoStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    execute_do_with_context_options(stmt, catalog, ctx, false)
}

fn execute_do_with_context_options(
    stmt: &DoStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    clear_notice_queue: bool,
) -> Result<StatementResult, ExecError> {
    stacker::maybe_grow(32 * 1024, 32 * 1024 * 1024, || {
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
        if clear_notice_queue {
            exec::clear_notices();
        }
        let block = parse_block(&stmt.code)?;
        let compiled = compile::compile_do_function(&block, catalog, Some(&ctx.gucs))
            .map_err(plpgsql_do_compile_error)?;
        exec::execute_do_function(&compiled, ctx)
    })
}

fn plpgsql_do_compile_error(err: ParseError) -> ExecError {
    let (err, _) = split_plpgsql_statement_line_context(err);
    ExecError::Parse(err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::executor::StatementResult;

    fn unline(stmt: &Stmt) -> &Stmt {
        match stmt {
            Stmt::WithLine { stmt, .. } => stmt,
            stmt => stmt,
        }
    }

    fn run_plpgsql_test<F>(name: &str, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        std::thread::Builder::new()
            .name(name.into())
            .stack_size(8 * 1024 * 1024)
            .spawn(f)
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn execute_do_runs_core_control_flow() {
        run_plpgsql_test("execute_do_runs_core_control_flow", || {
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
                    PlpgsqlNotice::new(RaiseLevel::Notice, "value 1"),
                    PlpgsqlNotice::new(RaiseLevel::Notice, "done 7"),
                ]
            );
        });
    }

    #[test]
    fn execute_do_runs_elsif_branch() {
        run_plpgsql_test("execute_do_runs_elsif_branch", || {
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
                vec![PlpgsqlNotice::new(RaiseLevel::Notice, "elsif")]
            );
        });
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
    fn execute_do_raise_info_queues_info_notice() {
        run_plpgsql_test("execute_do_raise_info_queues_info_notice", || {
            let stmt = DoStatement {
                language: None,
                code: r#"
                    declare r boolean;
                    begin
                        execute $e$ select 2 !=-- comment
                          1 $e$ into r;
                        raise info 'r = %', r;
                    end
                "#
                .into(),
            };

            assert_eq!(execute_do(&stmt).unwrap(), StatementResult::AffectedRows(0));
            assert_eq!(
                take_notices(),
                vec![PlpgsqlNotice::new(RaiseLevel::Info, "r = t")]
            );
        });
    }

    #[test]
    fn execute_do_raise_exception_surfaces_message() {
        run_plpgsql_test("execute_do_raise_exception_surfaces_message", || {
            let stmt = DoStatement {
                language: None,
                code: "begin raise exception 'boom %', 42; end".into(),
            };
            let err = execute_do(&stmt).unwrap_err();
            assert!(matches!(
                err,
                ExecError::RaiseException(message) if message == "boom 42"
            ));
        });
    }

    #[test]
    fn execute_do_exception_block_handles_raise() {
        run_plpgsql_test("execute_do_exception_block_handles_raise", || {
            let stmt = DoStatement {
                language: None,
                code: r#"
                    begin
                        begin
                            raise exception 'boom';
                        exception when others then
                            raise notice 'handled';
                        end;
                    end
                "#
                .into(),
            };

            assert_eq!(execute_do(&stmt).unwrap(), StatementResult::AffectedRows(0));
            assert_eq!(
                take_notices(),
                vec![PlpgsqlNotice::new(RaiseLevel::Notice, "handled")]
            );
        });
    }

    #[test]
    fn execute_do_handles_condition_raise_using_message() {
        run_plpgsql_test("execute_do_handles_condition_raise_using_message", || {
            let stmt = DoStatement {
                language: None,
                code: r#"
                    begin
                        begin
                            raise reading_sql_data_not_permitted using message = 'round and round again';
                        exception when reading_sql_data_not_permitted then
                            raise notice 'handled';
                        end;
                    end
                "#
                .into(),
            };

            assert_eq!(execute_do(&stmt).unwrap(), StatementResult::AffectedRows(0));
            assert_eq!(
                take_notices(),
                vec![PlpgsqlNotice::new(RaiseLevel::Notice, "handled")]
            );
        });
    }

    #[test]
    fn execute_do_condition_raise_uses_condition_sqlstate() {
        run_plpgsql_test("execute_do_condition_raise_uses_condition_sqlstate", || {
            let stmt = DoStatement {
                language: None,
                code: "begin raise data_corrupted using message = 'bad rows'; end".into(),
            };
            let err = execute_do(&stmt).unwrap_err();
            assert!(matches!(
                err,
                ExecError::DetailedError { message, sqlstate: "XX001", .. } if message == "bad rows"
            ));
        });
    }

    #[test]
    fn execute_do_raise_sqlstate_uses_literal_message_and_handler() {
        run_plpgsql_test(
            "execute_do_raise_sqlstate_uses_literal_message_and_handler",
            || {
                let err = execute_do(&DoStatement {
                    language: None,
                    code: "begin raise exception sqlstate 'U9999'; end".into(),
                })
                .unwrap_err();
                assert!(matches!(
                    err,
                    ExecError::DetailedError { message, sqlstate: "U9999", .. } if message == "U9999"
                ));

                let stmt = DoStatement {
                    language: None,
                    code: r#"
                    begin
                        begin
                            raise exception sqlstate 'U9999';
                        exception when sqlstate 'U9999' then
                            raise notice 'handled';
                        end;
                    end
                "#
                    .into(),
                };

                assert_eq!(execute_do(&stmt).unwrap(), StatementResult::AffectedRows(0));
                assert_eq!(
                    take_notices(),
                    vec![PlpgsqlNotice::new(RaiseLevel::Notice, "handled")]
                );
            },
        );
    }

    #[test]
    fn execute_do_assert_raises_assert_failure() {
        run_plpgsql_test("execute_do_assert_raises_assert_failure", || {
            let stmt = DoStatement {
                language: None,
                code: "begin assert false, 'bad assert'; end".into(),
            };
            let err = execute_do(&stmt).unwrap_err();
            assert!(matches!(
                err,
                ExecError::DetailedError { message, sqlstate: "P0004", .. } if message == "bad assert"
            ));
        });
    }

    #[test]
    fn execute_do_check_asserts_guc_disables_assert() {
        run_plpgsql_test("execute_do_check_asserts_guc_disables_assert", || {
            let stmt = DoStatement {
                language: None,
                code: "begin assert false, 'bad assert'; end".into(),
            };
            let mut gucs = std::collections::HashMap::new();
            gucs.insert("plpgsql.check_asserts".into(), "off".into());

            assert_eq!(
                execute_do_with_gucs(&stmt, &gucs).unwrap(),
                StatementResult::AffectedRows(0)
            );
        });
    }

    #[test]
    fn execute_do_raise_accepts_dollar_quoted_message() {
        run_plpgsql_test("execute_do_raise_accepts_dollar_quoted_message", || {
            let stmt = DoStatement {
                language: None,
                code: r#"begin raise exception $$Patchfield "%" does not exist$$, 'PF0_1'; end"#
                    .into(),
            };
            let err = execute_do(&stmt).unwrap_err();
            assert!(matches!(
                err,
                ExecError::RaiseException(message) if message == "Patchfield \"PF0_1\" does not exist"
            ));
        });
    }

    #[test]
    fn execute_do_accepts_top_level_end_semicolon() {
        run_plpgsql_test("execute_do_accepts_top_level_end_semicolon", || {
            let stmt = DoStatement {
                language: None,
                code: "begin raise notice 'done'; end;".into(),
            };

            let result = execute_do(&stmt).unwrap();
            assert_eq!(result, StatementResult::AffectedRows(0));
            assert_eq!(
                take_notices(),
                vec![PlpgsqlNotice::new(RaiseLevel::Notice, "done")]
            );
        });
    }

    #[test]
    fn execute_do_accepts_raise_info() {
        run_plpgsql_test("execute_do_accepts_raise_info", || {
            let stmt = DoStatement {
                language: None,
                code: "begin raise info 'progress: %', 3; end".into(),
            };

            let result = execute_do(&stmt).unwrap();
            assert_eq!(result, StatementResult::AffectedRows(0));
            assert_eq!(
                take_notices(),
                vec![PlpgsqlNotice::new(RaiseLevel::Info, "progress: 3")]
            );
        });
    }

    #[test]
    fn execute_do_raise_treats_double_percent_as_literal() {
        run_plpgsql_test("execute_do_raise_treats_double_percent_as_literal", || {
            let stmt = DoStatement {
                language: None,
                code: "begin raise notice 'done %%'; end".into(),
            };

            assert_eq!(execute_do(&stmt).unwrap(), StatementResult::AffectedRows(0));
            assert_eq!(
                take_notices(),
                vec![PlpgsqlNotice::new(RaiseLevel::Notice, "done %")]
            );
        });
    }

    #[test]
    fn execute_do_raise_using_message_detail_and_errcode() {
        run_plpgsql_test("execute_do_raise_using_message_detail_and_errcode", || {
            let stmt = DoStatement {
                language: None,
                code: "begin raise using message = 'custom' || ' message', detail = 'extra', errcode = '22012'; end".into(),
            };

            let err = execute_do(&stmt).unwrap_err();
            assert!(matches!(
                err,
                ExecError::DetailedError {
                    message,
                    detail: Some(detail),
                    sqlstate: "22012",
                    ..
                } if message == "custom message" && detail == "extra"
            ));
        });
    }

    #[test]
    fn execute_do_runs_while_loop() {
        run_plpgsql_test("execute_do_runs_while_loop", || {
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
                    PlpgsqlNotice::new(RaiseLevel::Notice, "2"),
                    PlpgsqlNotice::new(RaiseLevel::Notice, "4"),
                    PlpgsqlNotice::new(RaiseLevel::Notice, "7"),
                ]
            );
        });
    }

    fn execute_do_assigns_array_subscript() {
        run_plpgsql_test("execute_do_assigns_array_subscript", || {
            let stmt = DoStatement {
                language: None,
                code: r#"
                    declare
                        vals int4[] := array[1,2,3];
                    begin
                        vals[2] := 7;
                        raise notice '%', vals;
                    end
                "#
                .into(),
            };

            assert_eq!(execute_do(&stmt).unwrap(), StatementResult::AffectedRows(0));
            assert_eq!(
                take_notices(),
                vec![PlpgsqlNotice::new(RaiseLevel::Notice, "{1,7,3}")]
            );
        });
    }

    #[test]
    fn execute_do_runs_continue_in_loop() {
        run_plpgsql_test("execute_do_runs_continue_in_loop", || {
            let stmt = DoStatement {
                language: None,
                code: r#"
                    declare
                        total int4 := 0;
                    begin
                        for i in 1..4 loop
                            if i = 2 then
                                continue;
                            end if;
                            total := total + i;
                        end loop;
                        raise notice '%', total;
                    end
                "#
                .into(),
            };

            let result = execute_do(&stmt).unwrap();
            assert_eq!(result, StatementResult::AffectedRows(0));
            assert_eq!(
                take_notices(),
                vec![PlpgsqlNotice::new(RaiseLevel::Notice, "8")]
            );
        });
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

    #[test]
    fn parse_block_accepts_plpgsql_regression_syntax() {
        let block = parse_block(
            r#"
                declare
                    c refcursor;
                    cur cursor for select 1;
                    x int4 strict;
                begin
                    open cur;
                    fetch cur into x;
                    close cur;
                    get diagnostics x = row_count;
                    perform cast(1 as int4);
                    perform '{"a": 1}'::jsonb -> 'a';
                    perform f(a => 1);
                    savepoint s;
                end
            "#,
        )
        .unwrap();

        assert_eq!(block.declarations.len(), 3);
        assert!(matches!(
            &block.declarations[0],
            Decl::Var(decl) if decl.name == "c" && decl.type_name.eq_ignore_ascii_case("refcursor")
        ));
        assert!(matches!(
            &block.declarations[1],
            Decl::Cursor(decl) if decl.name == "cur"
        ));
        assert!(matches!(
            &block.declarations[2],
            Decl::Var(decl) if decl.name == "x" && decl.strict
        ));
        assert!(matches!(
            unline(&block.statements[0]),
            Stmt::OpenCursor { .. }
        ));
        assert!(matches!(
            unline(&block.statements[1]),
            Stmt::FetchCursor { .. }
        ));
        assert!(matches!(
            unline(&block.statements[2]),
            Stmt::CloseCursor { .. }
        ));
        assert!(matches!(
            unline(&block.statements[3]),
            Stmt::GetDiagnostics { .. }
        ));
    }
}
