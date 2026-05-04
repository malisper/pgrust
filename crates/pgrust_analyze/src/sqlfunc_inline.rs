use super::*;
use std::cell::RefCell;

#[derive(Debug, Clone)]
pub(super) struct SqlFunctionInlineArg {
    pub(super) function_name: Option<String>,
    pub(super) name: Option<String>,
    pub(super) expr: Expr,
    pub(super) sql_type: SqlType,
}

thread_local! {
    static SQL_FUNCTION_INLINE_ARGS: RefCell<Vec<Vec<SqlFunctionInlineArg>>> = const { RefCell::new(Vec::new()) };
}

pub(super) fn with_sql_function_inline_args<T>(
    args: Vec<SqlFunctionInlineArg>,
    f: impl FnOnce() -> T,
) -> T {
    SQL_FUNCTION_INLINE_ARGS.with(|stack| stack.borrow_mut().push(args));
    let result = f();
    SQL_FUNCTION_INLINE_ARGS.with(|stack| {
        stack.borrow_mut().pop();
    });
    result
}

pub(super) fn current_sql_function_inline_arg(index: usize) -> Option<SqlFunctionInlineArg> {
    if index == 0 {
        return None;
    }
    SQL_FUNCTION_INLINE_ARGS.with(|stack| {
        stack
            .borrow()
            .last()
            .and_then(|args| args.get(index - 1).cloned())
    })
}

pub(super) fn current_sql_function_inline_named_arg(name: &str) -> Option<SqlFunctionInlineArg> {
    SQL_FUNCTION_INLINE_ARGS.with(|stack| {
        stack.borrow().iter().rev().find_map(|args| {
            args.iter()
                .find(|arg| {
                    arg.name
                        .as_deref()
                        .is_some_and(|arg_name| arg_name.eq_ignore_ascii_case(name))
                })
                .cloned()
        })
    })
}

pub(super) fn current_sql_function_inline_qualified_arg(
    function_name: &str,
    name: &str,
) -> Option<SqlFunctionInlineArg> {
    SQL_FUNCTION_INLINE_ARGS.with(|stack| {
        stack.borrow().iter().rev().find_map(|args| {
            args.iter()
                .find(|arg| {
                    arg.function_name
                        .as_deref()
                        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(function_name))
                        && arg
                            .name
                            .as_deref()
                            .is_some_and(|arg_name| arg_name.eq_ignore_ascii_case(name))
                })
                .cloned()
        })
    })
}

pub(super) fn current_sql_function_inline_single_arg() -> Option<SqlFunctionInlineArg> {
    SQL_FUNCTION_INLINE_ARGS.with(|stack| {
        stack
            .borrow()
            .iter()
            .rev()
            .find_map(|args| (args.len() == 1).then(|| args[0].clone()))
    })
}
