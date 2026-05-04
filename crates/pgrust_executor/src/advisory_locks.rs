use pgrust_access::transam::xact::{INVALID_TRANSACTION_ID, TransactionId};
use pgrust_nodes::Value;
use pgrust_nodes::primnodes::BuiltinScalarFunction;
use pgrust_storage::lmgr::{AdvisoryLockKey, AdvisoryLockMode, AdvisoryLockOwner};

#[derive(Debug, Clone, PartialEq)]
pub enum AdvisoryLockEvalError {
    TypeMismatch {
        op: &'static str,
        left: Value,
        right: Value,
    },
    InvalidArgumentList {
        function_name: &'static str,
    },
    TransactionScopeRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdvisoryLockOperation {
    Lock,
    TryLock,
    Unlock,
    UnlockAll,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdvisoryLockCall {
    pub operation: AdvisoryLockOperation,
    pub key: Option<AdvisoryLockKey>,
    pub mode: AdvisoryLockMode,
    pub owner: Option<AdvisoryLockOwner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdvisoryLockScope {
    pub client_id: u32,
    pub transaction_lock_scope_id: Option<u64>,
    pub current_xid: TransactionId,
    pub statement_lock_scope_id: Option<u64>,
}

pub trait AdvisoryLockRuntime {
    type Error;

    fn lock_interruptible(
        &mut self,
        key: AdvisoryLockKey,
        mode: AdvisoryLockMode,
        owner: AdvisoryLockOwner,
    ) -> Result<(), Self::Error>;

    fn try_lock(
        &mut self,
        key: AdvisoryLockKey,
        mode: AdvisoryLockMode,
        owner: AdvisoryLockOwner,
    ) -> bool;

    fn unlock_session(&mut self, key: AdvisoryLockKey, mode: AdvisoryLockMode) -> bool;

    fn unlock_all_session(&mut self);

    fn warn_lock_not_owned(&mut self, mode: AdvisoryLockMode);
}

pub fn execute_advisory_lock_call<R>(
    call: AdvisoryLockCall,
    runtime: &mut R,
) -> Result<Value, R::Error>
where
    R: AdvisoryLockRuntime,
{
    match call.operation {
        AdvisoryLockOperation::Lock => {
            let Some(key) = call.key else {
                return Ok(Value::Null);
            };
            let owner = call.owner.expect("advisory lock call must have an owner");
            runtime.lock_interruptible(key, call.mode, owner)?;
            Ok(Value::Null)
        }
        AdvisoryLockOperation::TryLock => {
            let Some(key) = call.key else {
                return Ok(Value::Null);
            };
            let owner = call
                .owner
                .expect("advisory try-lock call must have an owner");
            Ok(Value::Bool(runtime.try_lock(key, call.mode, owner)))
        }
        AdvisoryLockOperation::Unlock => {
            let Some(key) = call.key else {
                return Ok(Value::Null);
            };
            let released = runtime.unlock_session(key, call.mode);
            if !released {
                runtime.warn_lock_not_owned(call.mode);
            }
            Ok(Value::Bool(released))
        }
        AdvisoryLockOperation::UnlockAll => {
            runtime.unlock_all_session();
            Ok(Value::Null)
        }
    }
}

pub fn advisory_lock_call(
    func: BuiltinScalarFunction,
    values: &[Value],
    scope: AdvisoryLockScope,
) -> Option<Result<AdvisoryLockCall, AdvisoryLockEvalError>> {
    if !is_advisory_builtin(func) {
        return None;
    }
    Some(advisory_lock_call_inner(func, values, scope))
}

fn advisory_lock_call_inner(
    func: BuiltinScalarFunction,
    values: &[Value],
    scope: AdvisoryLockScope,
) -> Result<AdvisoryLockCall, AdvisoryLockEvalError> {
    if matches!(func, BuiltinScalarFunction::PgAdvisoryUnlockAll) {
        return Ok(AdvisoryLockCall {
            operation: AdvisoryLockOperation::UnlockAll,
            key: None,
            mode: AdvisoryLockMode::Exclusive,
            owner: None,
        });
    }
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(AdvisoryLockCall {
            operation: AdvisoryLockOperation::Lock,
            key: None,
            mode: AdvisoryLockMode::Exclusive,
            owner: None,
        });
    }

    let key = advisory_key_from_values(func, values)?;
    let mode = advisory_mode(func);
    let operation = advisory_operation(func);
    let owner = Some(advisory_owner(func, scope)?);

    Ok(AdvisoryLockCall {
        operation,
        key: Some(key),
        mode,
        owner,
    })
}

fn advisory_operation(func: BuiltinScalarFunction) -> AdvisoryLockOperation {
    match func {
        BuiltinScalarFunction::PgAdvisoryLock
        | BuiltinScalarFunction::PgAdvisoryLockShared
        | BuiltinScalarFunction::PgAdvisoryXactLock
        | BuiltinScalarFunction::PgAdvisoryXactLockShared => AdvisoryLockOperation::Lock,
        BuiltinScalarFunction::PgTryAdvisoryLock
        | BuiltinScalarFunction::PgTryAdvisoryLockShared
        | BuiltinScalarFunction::PgTryAdvisoryXactLock
        | BuiltinScalarFunction::PgTryAdvisoryXactLockShared => AdvisoryLockOperation::TryLock,
        BuiltinScalarFunction::PgAdvisoryUnlock | BuiltinScalarFunction::PgAdvisoryUnlockShared => {
            AdvisoryLockOperation::Unlock
        }
        BuiltinScalarFunction::PgAdvisoryUnlockAll => AdvisoryLockOperation::UnlockAll,
        _ => unreachable!("non-advisory builtin"),
    }
}

fn advisory_owner(
    func: BuiltinScalarFunction,
    scope: AdvisoryLockScope,
) -> Result<AdvisoryLockOwner, AdvisoryLockEvalError> {
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
        return Ok(AdvisoryLockOwner::session(scope.client_id));
    }
    if let Some(scope_id) = scope.transaction_lock_scope_id {
        return Ok(AdvisoryLockOwner::transaction(scope.client_id, scope_id));
    }
    if scope.current_xid != INVALID_TRANSACTION_ID {
        return Ok(AdvisoryLockOwner::transaction(
            scope.client_id,
            u64::from(scope.current_xid),
        ));
    }
    if let Some(scope_id) = scope.statement_lock_scope_id {
        return Ok(AdvisoryLockOwner::statement(scope.client_id, scope_id));
    }
    Err(AdvisoryLockEvalError::TransactionScopeRequired)
}

pub fn advisory_mode(func: BuiltinScalarFunction) -> AdvisoryLockMode {
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
) -> Result<AdvisoryLockKey, AdvisoryLockEvalError> {
    match values {
        [value] => Ok(AdvisoryLockKey::BigInt(int8_arg(
            value,
            advisory_func_name(func),
        )?)),
        [first, second] => Ok(AdvisoryLockKey::TwoInt(
            int4_arg(first, advisory_func_name(func))?,
            int4_arg(second, advisory_func_name(func))?,
        )),
        _ => Err(AdvisoryLockEvalError::InvalidArgumentList {
            function_name: advisory_func_name(func),
        }),
    }
}

fn int8_arg(value: &Value, op: &'static str) -> Result<i64, AdvisoryLockEvalError> {
    match value {
        Value::Int64(value) => Ok(*value),
        Value::Int32(value) => Ok(i64::from(*value)),
        other => Err(AdvisoryLockEvalError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::Int64(0),
        }),
    }
}

fn int4_arg(value: &Value, op: &'static str) -> Result<i32, AdvisoryLockEvalError> {
    match value {
        Value::Int32(value) => Ok(*value),
        Value::Int64(value) => {
            i32::try_from(*value).map_err(|_| AdvisoryLockEvalError::TypeMismatch {
                op,
                left: Value::Int64(*value),
                right: Value::Int32(0),
            })
        }
        other => Err(AdvisoryLockEvalError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::Int32(0),
        }),
    }
}

pub fn advisory_func_name(func: BuiltinScalarFunction) -> &'static str {
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

pub fn is_advisory_builtin(func: BuiltinScalarFunction) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct TestRuntime {
        locked: Vec<(AdvisoryLockKey, AdvisoryLockMode, AdvisoryLockOwner)>,
        unlock_result: bool,
        unlock_all_called: bool,
        warning_count: usize,
    }

    impl AdvisoryLockRuntime for TestRuntime {
        type Error = ();

        fn lock_interruptible(
            &mut self,
            key: AdvisoryLockKey,
            mode: AdvisoryLockMode,
            owner: AdvisoryLockOwner,
        ) -> Result<(), Self::Error> {
            self.locked.push((key, mode, owner));
            Ok(())
        }

        fn try_lock(
            &mut self,
            _key: AdvisoryLockKey,
            _mode: AdvisoryLockMode,
            _owner: AdvisoryLockOwner,
        ) -> bool {
            true
        }

        fn unlock_session(&mut self, _key: AdvisoryLockKey, _mode: AdvisoryLockMode) -> bool {
            self.unlock_result
        }

        fn unlock_all_session(&mut self) {
            self.unlock_all_called = true;
        }

        fn warn_lock_not_owned(&mut self, _mode: AdvisoryLockMode) {
            self.warning_count += 1;
        }
    }

    #[test]
    fn execute_advisory_lock_call_dispatches_lock_and_try_lock() {
        let mut runtime = TestRuntime::default();
        let owner = AdvisoryLockOwner::session(7);
        let call = AdvisoryLockCall {
            operation: AdvisoryLockOperation::Lock,
            key: Some(AdvisoryLockKey::BigInt(42)),
            mode: AdvisoryLockMode::Exclusive,
            owner: Some(owner),
        };
        assert_eq!(
            execute_advisory_lock_call(call, &mut runtime).unwrap(),
            Value::Null
        );
        assert_eq!(
            runtime.locked,
            vec![(
                AdvisoryLockKey::BigInt(42),
                AdvisoryLockMode::Exclusive,
                owner
            )]
        );

        let call = AdvisoryLockCall {
            operation: AdvisoryLockOperation::TryLock,
            key: Some(AdvisoryLockKey::BigInt(42)),
            mode: AdvisoryLockMode::Shared,
            owner: Some(owner),
        };
        assert_eq!(
            execute_advisory_lock_call(call, &mut runtime).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn execute_advisory_unlock_warns_when_not_owned() {
        let mut runtime = TestRuntime::default();
        let call = AdvisoryLockCall {
            operation: AdvisoryLockOperation::Unlock,
            key: Some(AdvisoryLockKey::BigInt(42)),
            mode: AdvisoryLockMode::Exclusive,
            owner: None,
        };
        assert_eq!(
            execute_advisory_lock_call(call, &mut runtime).unwrap(),
            Value::Bool(false)
        );
        assert_eq!(runtime.warning_count, 1);
    }
}
