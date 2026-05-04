use super::exec_expr::eval_builtin_function;
use super::expr_xml::unsupported_xml_feature_error;
use super::sqlfunc::execute_user_defined_sql_scalar_function;
use super::{ExecError, ExecutorContext, TupleSlot, Value};
use crate::backend::parser::CatalogLookup;
use crate::include::catalog::PgProcRow;
use crate::include::nodes::primnodes::{FuncExpr, ScalarFunctionImpl};
use crate::pl::plpgsql::execute_user_defined_scalar_function;
use pgrust_executor::{
    ScalarFunctionCallInfo, UnsupportedInternalFunctionDetail,
    scalar_function_call_info_for_proc_row, unsupported_internal_function_detail,
};

pub(crate) fn call_scalar_function(
    func: &FuncExpr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    super::exec_expr::ensure_proc_execute_allowed(func.funcid, ctx)?;
    let call_info = match func.implementation {
        ScalarFunctionImpl::Builtin(builtin) => ScalarFunctionCallInfo::Builtin(builtin),
        ScalarFunctionImpl::UserDefined { proc_oid } => scalar_function_call_info(proc_oid, ctx)?,
    };

    match call_info {
        ScalarFunctionCallInfo::Builtin(builtin) => eval_builtin_function(
            builtin,
            func.funcresulttype,
            &func.args,
            func.funcvariadic,
            slot,
            ctx,
        ),
        ScalarFunctionCallInfo::Sql(row) => {
            execute_user_defined_sql_scalar_function(&row, &func.args, slot, ctx)
        }
        ScalarFunctionCallInfo::PlPgSql { proc_oid }
        | ScalarFunctionCallInfo::PlHandler { proc_oid } => execute_user_defined_scalar_function(
            proc_oid,
            func.funcresulttype,
            &func.args,
            slot,
            ctx,
        ),
        ScalarFunctionCallInfo::UnsupportedInternal(row) => {
            Err(unsupported_internal_function(&row))
        }
    }
}

fn scalar_function_call_info(
    proc_oid: u32,
    ctx: &mut ExecutorContext,
) -> Result<ScalarFunctionCallInfo, ExecError> {
    if let Some(info) = ctx.scalar_function_cache.get(&proc_oid) {
        return Ok(info.clone());
    }

    let row = proc_row_for_fmgr_call(proc_oid, ctx)?;
    let info = scalar_function_call_info_for_proc_row(row);
    ctx.scalar_function_cache.insert(proc_oid, info.clone());
    Ok(info)
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
    match unsupported_internal_function_detail(row) {
        UnsupportedInternalFunctionDetail::UnsupportedXmlFeature => unsupported_xml_feature_error(),
        UnsupportedInternalFunctionDetail::UnsupportedInternal { proname, prosrc } => {
            // :HACK: pgrust exposes some pg_proc rows for catalog compatibility before
            // implementing their runtime behavior. PostgreSQL still has real fmgr
            // entries for these; fail explicitly instead of silently treating them as
            // PL/pgSQL or another executable language.
            ExecError::DetailedError {
                message: format!("function {proname} is not supported").into(),
                detail: Some(format!("internal symbol = {prosrc}").into()),
                hint: None,
                sqlstate: "0A000",
            }
        }
    }
}
