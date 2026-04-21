use super::{ExecError, ExecutorContext, Value};
use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
use crate::backend::storage::lmgr::{
    AdvisoryLockError, AdvisoryLockKey, AdvisoryLockMode, AdvisoryLockOwner,
};
use crate::include::nodes::primnodes::BuiltinScalarFunction;

pub(crate) fn eval_advisory_lock_builtin_function(
    func: BuiltinScalarFunction,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Option<Result<Value, ExecError>> {
    if !is_advisory_builtin(func) {
        return None;
    }
    Some(eval_advisory_lock_builtin_function_inner(func, values, ctx))
}

fn eval_advisory_lock_builtin_function_inner(
    func: BuiltinScalarFunction,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    if matches!(func, BuiltinScalarFunction::PgAdvisoryUnlockAll) {
        ctx.advisory_locks.unlock_all_session(ctx.client_id);
        return Ok(Value::Null);
    }
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }

    let key = advisory_key_from_values(func, values)?;
    let mode = advisory_mode(func);

    match func {
        BuiltinScalarFunction::PgAdvisoryLock
        | BuiltinScalarFunction::PgAdvisoryLockShared
        | BuiltinScalarFunction::PgAdvisoryXactLock
        | BuiltinScalarFunction::PgAdvisoryXactLockShared => {
            let owner = advisory_owner(func, ctx)?;
            ctx.advisory_locks
                .lock_interruptible(key, mode, owner, ctx.interrupts.as_ref())
                .map_err(|err| match err {
                    AdvisoryLockError::Interrupted(reason) => ExecError::Interrupted(reason),
                })?;
            Ok(Value::Null)
        }
        BuiltinScalarFunction::PgTryAdvisoryLock
        | BuiltinScalarFunction::PgTryAdvisoryLockShared
        | BuiltinScalarFunction::PgTryAdvisoryXactLock
        | BuiltinScalarFunction::PgTryAdvisoryXactLockShared => {
            let owner = advisory_owner(func, ctx)?;
            Ok(Value::Bool(ctx.advisory_locks.try_lock(key, mode, owner)))
        }
        BuiltinScalarFunction::PgAdvisoryUnlock | BuiltinScalarFunction::PgAdvisoryUnlockShared => {
            Ok(Value::Bool(ctx.advisory_locks.unlock(
                key,
                mode,
                AdvisoryLockOwner::session(ctx.client_id),
            )))
        }
        BuiltinScalarFunction::PgAdvisoryUnlockAll => unreachable!("handled above"),
        _ => unreachable!("non-advisory builtin reached advisory evaluator"),
    }
}

fn advisory_owner(
    func: BuiltinScalarFunction,
    ctx: &ExecutorContext,
) -> Result<AdvisoryLockOwner, ExecError> {
    if matches!(
        func,
        BuiltinScalarFunction::PgAdvisoryLock
            | BuiltinScalarFunction::PgAdvisoryLockShared
            | BuiltinScalarFunction::PgTryAdvisoryLock
            | BuiltinScalarFunction::PgTryAdvisoryLockShared
            | BuiltinScalarFunction::PgAdvisoryUnlock
            | BuiltinScalarFunction::PgAdvisoryUnlockShared
            | BuiltinScalarFunction::PgAdvisoryUnlockAll
    ) {
        return Ok(AdvisoryLockOwner::session(ctx.client_id));
    }
    if ctx.snapshot.current_xid != INVALID_TRANSACTION_ID {
        return Ok(AdvisoryLockOwner::transaction(
            ctx.client_id,
            ctx.snapshot.current_xid,
        ));
    }
    if let Some(scope_id) = ctx.statement_lock_scope_id {
        return Ok(AdvisoryLockOwner::statement(ctx.client_id, scope_id));
    }
    Err(ExecError::DetailedError {
        message: "transaction-scoped advisory lock requires statement scope".into(),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    })
}

fn advisory_mode(func: BuiltinScalarFunction) -> AdvisoryLockMode {
    match func {
        BuiltinScalarFunction::PgAdvisoryLockShared
        | BuiltinScalarFunction::PgAdvisoryXactLockShared
        | BuiltinScalarFunction::PgTryAdvisoryLockShared
        | BuiltinScalarFunction::PgTryAdvisoryXactLockShared
        | BuiltinScalarFunction::PgAdvisoryUnlockShared => AdvisoryLockMode::Shared,
        _ => AdvisoryLockMode::Exclusive,
    }
}

fn advisory_key_from_values(
    func: BuiltinScalarFunction,
    values: &[Value],
) -> Result<AdvisoryLockKey, ExecError> {
    match values {
        [value] => Ok(AdvisoryLockKey::BigInt(int8_arg(
            value,
            advisory_func_name(func),
        )?)),
        [first, second] => Ok(AdvisoryLockKey::TwoInt(
            int4_arg(first, advisory_func_name(func))?,
            int4_arg(second, advisory_func_name(func))?,
        )),
        _ => Err(ExecError::DetailedError {
            message: format!(
                "invalid advisory lock argument list for {}",
                advisory_func_name(func)
            ),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        }),
    }
}

fn int8_arg(value: &Value, op: &'static str) -> Result<i64, ExecError> {
    match value {
        Value::Int64(value) => Ok(*value),
        Value::Int32(value) => Ok(i64::from(*value)),
        other => Err(ExecError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::Int64(0),
        }),
    }
}

fn int4_arg(value: &Value, op: &'static str) -> Result<i32, ExecError> {
    match value {
        Value::Int32(value) => Ok(*value),
        Value::Int64(value) => i32::try_from(*value).map_err(|_| ExecError::TypeMismatch {
            op,
            left: Value::Int64(*value),
            right: Value::Int32(0),
        }),
        other => Err(ExecError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::Int32(0),
        }),
    }
}

fn advisory_func_name(func: BuiltinScalarFunction) -> &'static str {
    match func {
        BuiltinScalarFunction::PgAdvisoryLock => "pg_advisory_lock",
        BuiltinScalarFunction::PgAdvisoryXactLock => "pg_advisory_xact_lock",
        BuiltinScalarFunction::PgAdvisoryLockShared => "pg_advisory_lock_shared",
        BuiltinScalarFunction::PgAdvisoryXactLockShared => "pg_advisory_xact_lock_shared",
        BuiltinScalarFunction::PgTryAdvisoryLock => "pg_try_advisory_lock",
        BuiltinScalarFunction::PgTryAdvisoryXactLock => "pg_try_advisory_xact_lock",
        BuiltinScalarFunction::PgTryAdvisoryLockShared => "pg_try_advisory_lock_shared",
        BuiltinScalarFunction::PgTryAdvisoryXactLockShared => "pg_try_advisory_xact_lock_shared",
        BuiltinScalarFunction::PgAdvisoryUnlock => "pg_advisory_unlock",
        BuiltinScalarFunction::PgAdvisoryUnlockShared => "pg_advisory_unlock_shared",
        BuiltinScalarFunction::PgAdvisoryUnlockAll => "pg_advisory_unlock_all",
        _ => unreachable!("non-advisory builtin"),
    }
}

fn is_advisory_builtin(func: BuiltinScalarFunction) -> bool {
    matches!(
        func,
        BuiltinScalarFunction::PgAdvisoryLock
            | BuiltinScalarFunction::PgAdvisoryXactLock
            | BuiltinScalarFunction::PgAdvisoryLockShared
            | BuiltinScalarFunction::PgAdvisoryXactLockShared
            | BuiltinScalarFunction::PgTryAdvisoryLock
            | BuiltinScalarFunction::PgTryAdvisoryXactLock
            | BuiltinScalarFunction::PgTryAdvisoryLockShared
            | BuiltinScalarFunction::PgTryAdvisoryXactLockShared
            | BuiltinScalarFunction::PgAdvisoryUnlock
            | BuiltinScalarFunction::PgAdvisoryUnlockShared
            | BuiltinScalarFunction::PgAdvisoryUnlockAll
    )
}
