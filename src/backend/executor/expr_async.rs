use super::{ExecError, ExecutorContext, Value};
use pgrust_executor::{
    AsyncNotifyArgError, AsyncNotifyArgOrQueueError, AsyncNotifyEvalContext, AsyncNotifyQueueError,
    PendingNotification,
};

impl From<AsyncNotifyArgError> for ExecError {
    fn from(error: AsyncNotifyArgError) -> Self {
        match error {
            AsyncNotifyArgError::TypeMismatch {
                op,
                arg_index,
                value,
            } => ExecError::TypeMismatch {
                op,
                left: value,
                right: Value::Text(format!("arg{arg_index}").into()),
            },
        }
    }
}

impl From<AsyncNotifyQueueError> for ExecError {
    fn from(error: AsyncNotifyQueueError) -> Self {
        ExecError::DetailedError {
            message: error.message().to_string(),
            detail: None,
            hint: None,
            sqlstate: "22023",
        }
    }
}

impl From<AsyncNotifyArgOrQueueError> for ExecError {
    fn from(error: AsyncNotifyArgOrQueueError) -> Self {
        match error {
            AsyncNotifyArgOrQueueError::Arg(error) => error.into(),
            AsyncNotifyArgOrQueueError::Queue(error) => error.into(),
        }
    }
}

impl AsyncNotifyEvalContext for ExecutorContext {
    fn pending_notifications_mut(&mut self) -> &mut Vec<PendingNotification> {
        &mut self.pending_async_notifications
    }

    fn queue_usage(&self) -> Option<f64> {
        self.async_notify_runtime
            .as_ref()
            .map(|runtime| runtime.queue_usage())
    }
}

pub(crate) fn eval_pg_notify_function(
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    pgrust_executor::eval_pg_notify_function(values, ctx).map_err(Into::into)
}

pub(crate) fn eval_pg_notification_queue_usage_function(ctx: &ExecutorContext) -> Value {
    pgrust_executor::eval_pg_notification_queue_usage_function(ctx)
}
