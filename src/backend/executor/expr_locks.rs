use super::{ExecError, ExecutorContext, Value};
use crate::backend::storage::lmgr::{
    AdvisoryLockError, AdvisoryLockKey, AdvisoryLockMode, AdvisoryLockOwner,
};
use crate::backend::utils::misc::notices::push_warning;
use crate::include::nodes::primnodes::BuiltinScalarFunction;
use pgrust_executor::{
    AdvisoryLockEvalError, AdvisoryLockRuntime, AdvisoryLockScope, advisory_lock_call,
    execute_advisory_lock_call,
};

impl From<AdvisoryLockEvalError> for ExecError {
    fn from(error: AdvisoryLockEvalError) -> Self {
        match error {
            AdvisoryLockEvalError::TypeMismatch { op, left, right } => {
                ExecError::TypeMismatch { op, left, right }
            }
            AdvisoryLockEvalError::InvalidArgumentList { function_name } => {
                ExecError::DetailedError {
                    message: format!("invalid advisory lock argument list for {function_name}"),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                }
            }
            AdvisoryLockEvalError::TransactionScopeRequired => ExecError::DetailedError {
                message: "transaction-scoped advisory lock requires statement scope".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            },
        }
    }
}

pub(crate) fn eval_advisory_lock_builtin_function(
    func: BuiltinScalarFunction,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Option<Result<Value, ExecError>> {
    advisory_lock_call(
        func,
        values,
        AdvisoryLockScope {
            client_id: ctx.client_id,
            transaction_lock_scope_id: ctx.transaction_lock_scope_id,
            current_xid: ctx.snapshot.current_xid,
            statement_lock_scope_id: ctx.statement_lock_scope_id,
        },
    )
    .map(|call| {
        let call = call.map_err(ExecError::from)?;
        let mut runtime = RootAdvisoryLockRuntime { ctx };
        execute_advisory_lock_call(call, &mut runtime)
    })
}

struct RootAdvisoryLockRuntime<'a> {
    ctx: &'a mut ExecutorContext,
}

impl AdvisoryLockRuntime for RootAdvisoryLockRuntime<'_> {
    type Error = ExecError;

    fn lock_interruptible(
        &mut self,
        key: AdvisoryLockKey,
        mode: AdvisoryLockMode,
        owner: AdvisoryLockOwner,
    ) -> Result<(), Self::Error> {
        self.ctx
            .advisory_locks
            .lock_interruptible(key, mode, owner, self.ctx.interrupts.as_ref())
            .map_err(|err| match err {
                AdvisoryLockError::Interrupted(reason) => ExecError::Interrupted(reason),
            })
    }

    fn try_lock(
        &mut self,
        key: AdvisoryLockKey,
        mode: AdvisoryLockMode,
        owner: AdvisoryLockOwner,
    ) -> bool {
        self.ctx.advisory_locks.try_lock(key, mode, owner)
    }

    fn unlock_session(&mut self, key: AdvisoryLockKey, mode: AdvisoryLockMode) -> bool {
        self.ctx
            .advisory_locks
            .unlock(key, mode, AdvisoryLockOwner::session(self.ctx.client_id))
    }

    fn unlock_all_session(&mut self) {
        self.ctx
            .advisory_locks
            .unlock_all_session(self.ctx.client_id);
    }

    fn warn_lock_not_owned(&mut self, mode: AdvisoryLockMode) {
        push_warning(format!(
            "you don't own a lock of type {}",
            mode.pg_mode_name()
        ));
    }
}
