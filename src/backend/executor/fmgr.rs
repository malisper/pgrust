use super::exec_expr::eval_builtin_function;
use super::sqlfunc::execute_user_defined_sql_scalar_function;
use super::{ExecError, ExecutorContext, TupleSlot, Value};
use crate::backend::parser::CatalogLookup;
use crate::include::catalog::{
    PG_LANGUAGE_C_OID, PG_LANGUAGE_INTERNAL_OID, PG_LANGUAGE_PLPGSQL_OID, PG_LANGUAGE_SQL_OID,
    PgProcRow, builtin_scalar_function_for_proc_oid, builtin_scalar_function_for_proc_row,
};
use crate::include::nodes::primnodes::{FuncExpr, ScalarFunctionImpl};
use crate::pl::plpgsql::execute_user_defined_scalar_function;

pub(crate) fn call_scalar_function(
    func: &FuncExpr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    if let Some(builtin) = builtin_scalar_function_for_proc_oid(func.funcid) {
        return eval_builtin_function(
            builtin,
            func.funcresulttype,
            &func.args,
            func.funcvariadic,
            slot,
            ctx,
        );
    }

    if func.funcid == 0 {
        // :HACK: Domain-check expressions are still compiler-generated helpers,
        // not real pg_proc rows. Keep this narrow fallback until those checks
        // are represented as catalog-backed constraints or support functions.
        if let ScalarFunctionImpl::Builtin(builtin) = func.implementation {
            return eval_builtin_function(
                builtin,
                func.funcresulttype,
                &func.args,
                func.funcvariadic,
                slot,
                ctx,
            );
        }
    }

    let row = proc_row_for_fmgr_call(func.funcid, ctx)?;
    if let Some(builtin) = builtin_scalar_function_for_proc_row(&row) {
        return eval_builtin_function(
            builtin,
            func.funcresulttype,
            &func.args,
            func.funcvariadic,
            slot,
            ctx,
        );
    }

    match row.prolang {
        PG_LANGUAGE_SQL_OID => {
            execute_user_defined_sql_scalar_function(&row, &func.args, slot, ctx)
        }
        PG_LANGUAGE_PLPGSQL_OID => execute_user_defined_scalar_function(
            func.funcid,
            func.funcresulttype,
            &func.args,
            slot,
            ctx,
        ),
        PG_LANGUAGE_INTERNAL_OID | PG_LANGUAGE_C_OID => Err(unsupported_internal_function(&row)),
        _ => execute_user_defined_scalar_function(
            func.funcid,
            func.funcresulttype,
            &func.args,
            slot,
            ctx,
        ),
    }
}

fn proc_row_for_fmgr_call(funcid: u32, ctx: &ExecutorContext) -> Result<PgProcRow, ExecError> {
    let catalog = ctx
        .catalog
        .as_ref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "function calls require executor catalog context".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        })?;
    catalog
        .proc_row_by_oid(funcid)
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("unknown function oid {funcid}").into(),
            detail: None,
            hint: None,
            sqlstate: "42883",
        })
}

fn unsupported_internal_function(row: &PgProcRow) -> ExecError {
    // :HACK: pgrust exposes some pg_proc rows for catalog compatibility before
    // implementing their runtime behavior. PostgreSQL still has real fmgr
    // entries for these; fail explicitly instead of silently treating them as
    // PL/pgSQL or another executable language.
    ExecError::DetailedError {
        message: format!("function {} is not supported", row.proname).into(),
        detail: Some(format!("internal symbol = {}", row.prosrc).into()),
        hint: None,
        sqlstate: "0A000",
    }
}
