use super::{ExecError, ExecutorContext, Value};
use crate::pgrust::database::queue_pending_notification;

pub(crate) fn eval_pg_notify_function(
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let channel = async_notify_text_arg(values.first(), "pg_notify", 0)?;
    let payload = async_notify_text_arg(values.get(1), "pg_notify", 1)?;
    queue_pending_notification(&mut ctx.pending_async_notifications, &channel, &payload)?;
    Ok(Value::Null)
}

pub(crate) fn eval_pg_notification_queue_usage_function(ctx: &ExecutorContext) -> Value {
    Value::Float64(
        ctx.async_notify_runtime
            .as_ref()
            .map(|runtime| runtime.queue_usage())
            .unwrap_or(0.0),
    )
}

fn async_notify_text_arg(
    value: Option<&Value>,
    op: &'static str,
    arg_index: usize,
) -> Result<String, ExecError> {
    match value.unwrap_or(&Value::Null) {
        Value::Null => Ok(String::new()),
        value => value
            .as_text()
            .map(ToOwned::to_owned)
            .ok_or_else(|| ExecError::TypeMismatch {
                op,
                left: value.clone(),
                right: Value::Text(format!("arg{arg_index}").into()),
            }),
    }
}
