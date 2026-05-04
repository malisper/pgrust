use crate::backend::executor::ExecError;

pub use pgrust_executor::{
    AsyncListenAction, AsyncListenOp, AsyncNotifyRuntime, PendingNotification,
};

pub(crate) fn validate_pending_notification(
    channel: &str,
    payload: &str,
) -> Result<PendingNotification, ExecError> {
    pgrust_executor::validate_pending_notification(channel, payload).map_err(invalid_notify_error)
}

pub(crate) fn queue_pending_notification(
    pending: &mut Vec<PendingNotification>,
    channel: &str,
    payload: &str,
) -> Result<(), ExecError> {
    pgrust_executor::queue_pending_notification(pending, channel, payload)
        .map_err(invalid_notify_error)
}

pub(crate) fn merge_pending_notifications(
    target: &mut Vec<PendingNotification>,
    source: Vec<PendingNotification>,
) {
    pgrust_executor::merge_pending_notifications(target, source);
}

fn invalid_notify_error(error: pgrust_executor::AsyncNotifyQueueError) -> ExecError {
    ExecError::DetailedError {
        message: error.message().to_string(),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}
