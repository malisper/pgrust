use std::collections::{BTreeSet, HashMap};

use parking_lot::RwLock;
use pgrust_core::ClientId;
use pgrust_nodes::Value;

pub const ASYNC_NOTIFY_CHANNEL_MAX_LEN: usize = 63;
pub const ASYNC_NOTIFY_PAYLOAD_MAX_LEN: usize = pgrust_storage::BLCKSZ - 64 - 128;
pub const ASYNC_NOTIFY_QUEUE_CAPACITY_BYTES: usize = pgrust_storage::BLCKSZ * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingNotification {
    pub channel: String,
    pub payload: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveredNotification {
    pub sender_pid: i32,
    pub channel: String,
    pub payload: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AsyncListenAction {
    Listen,
    Unlisten,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsyncListenOp {
    pub action: AsyncListenAction,
    pub channel: Option<String>,
}

#[derive(Debug, Default)]
struct AsyncNotifyState {
    listeners_by_client: HashMap<ClientId, BTreeSet<String>>,
    pending_by_client: HashMap<ClientId, Vec<DeliveredNotification>>,
    approx_queue_bytes: usize,
}

#[derive(Debug, Default)]
pub struct AsyncNotifyRuntime {
    state: RwLock<AsyncNotifyState>,
}

impl AsyncNotifyRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_listening(&self, client_id: ClientId, channel: &str) -> bool {
        self.state
            .read()
            .listeners_by_client
            .get(&client_id)
            .is_some_and(|channels| channels.contains(channel))
    }

    pub fn listening_channels(&self, client_id: ClientId) -> Vec<String> {
        self.state
            .read()
            .listeners_by_client
            .get(&client_id)
            .map(|channels| channels.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn pending_notifications(&self, client_id: ClientId) -> Vec<DeliveredNotification> {
        self.state
            .read()
            .pending_by_client
            .get(&client_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn queue_usage(&self) -> f64 {
        let state = self.state.read();
        if state.approx_queue_bytes == 0 {
            0.0
        } else {
            state.approx_queue_bytes as f64 / ASYNC_NOTIFY_QUEUE_CAPACITY_BYTES as f64
        }
    }

    pub fn listen(&self, client_id: ClientId, channel: &str) {
        self.state
            .write()
            .listeners_by_client
            .entry(client_id)
            .or_default()
            .insert(channel.to_string());
    }

    pub fn unlisten(&self, client_id: ClientId, channel: Option<&str>) {
        let mut state = self.state.write();
        match channel {
            Some(channel) => {
                if let Some(channels) = state.listeners_by_client.get_mut(&client_id) {
                    channels.remove(channel);
                    if channels.is_empty() {
                        state.listeners_by_client.remove(&client_id);
                    }
                }
            }
            None => {
                state.listeners_by_client.remove(&client_id);
            }
        }
    }

    pub fn apply_listener_ops(&self, client_id: ClientId, ops: &[AsyncListenOp]) {
        for op in ops {
            match op.action {
                AsyncListenAction::Listen => {
                    if let Some(channel) = op.channel.as_deref() {
                        self.listen(client_id, channel);
                    }
                }
                AsyncListenAction::Unlisten => {
                    self.unlisten(client_id, op.channel.as_deref());
                }
            }
        }
    }

    pub fn publish(&self, sender_client_id: ClientId, notifications: &[PendingNotification]) {
        if notifications.is_empty() {
            return;
        }
        let sender_pid = sender_client_id as i32;
        let mut state = self.state.write();
        for notification in notifications {
            let queue_bytes = pending_notification_bytes(notification);
            let recipients = state
                .listeners_by_client
                .iter()
                .filter_map(|(client_id, channels)| {
                    channels
                        .contains(&notification.channel)
                        .then_some(*client_id)
                })
                .collect::<Vec<_>>();
            for client_id in recipients {
                state
                    .pending_by_client
                    .entry(client_id)
                    .or_default()
                    .push(DeliveredNotification {
                        sender_pid,
                        channel: notification.channel.clone(),
                        payload: notification.payload.clone(),
                    });
                state.approx_queue_bytes = state.approx_queue_bytes.saturating_add(queue_bytes);
            }
        }
    }

    pub fn drain(&self, client_id: ClientId) -> Vec<DeliveredNotification> {
        let mut state = self.state.write();
        let notifications = state
            .pending_by_client
            .remove(&client_id)
            .unwrap_or_default();
        let reclaimed = notifications
            .iter()
            .map(delivered_notification_bytes)
            .sum::<usize>();
        state.approx_queue_bytes = state.approx_queue_bytes.saturating_sub(reclaimed);
        notifications
    }

    pub fn disconnect(&self, client_id: ClientId) {
        let mut state = self.state.write();
        state.listeners_by_client.remove(&client_id);
        let reclaimed = state
            .pending_by_client
            .remove(&client_id)
            .unwrap_or_default()
            .iter()
            .map(delivered_notification_bytes)
            .sum::<usize>();
        state.approx_queue_bytes = state.approx_queue_bytes.saturating_sub(reclaimed);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AsyncNotifyArgError {
    TypeMismatch {
        op: &'static str,
        arg_index: usize,
        value: Value,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AsyncNotifyQueueError {
    EmptyChannel,
    ChannelTooLong,
    PayloadTooLong,
}

impl AsyncNotifyQueueError {
    pub fn message(&self) -> &'static str {
        match self {
            Self::EmptyChannel => "channel name cannot be empty",
            Self::ChannelTooLong => "channel name too long",
            Self::PayloadTooLong => "payload string too long",
        }
    }
}

pub trait AsyncNotifyEvalContext {
    fn pending_notifications_mut(&mut self) -> &mut Vec<PendingNotification>;
    fn queue_usage(&self) -> Option<f64>;
}

pub fn eval_pg_notify_function<C>(
    values: &[Value],
    ctx: &mut C,
) -> Result<Value, AsyncNotifyArgOrQueueError>
where
    C: AsyncNotifyEvalContext,
{
    let (channel, payload) = pg_notify_args(values).map_err(AsyncNotifyArgOrQueueError::Arg)?;
    queue_pending_notification(ctx.pending_notifications_mut(), &channel, &payload)
        .map_err(AsyncNotifyArgOrQueueError::Queue)?;
    Ok(Value::Null)
}

pub fn eval_pg_notification_queue_usage_function<C>(ctx: &C) -> Value
where
    C: AsyncNotifyEvalContext,
{
    notification_queue_usage_value(ctx.queue_usage())
}

#[derive(Debug, Clone, PartialEq)]
pub enum AsyncNotifyArgOrQueueError {
    Arg(AsyncNotifyArgError),
    Queue(AsyncNotifyQueueError),
}

pub fn pg_notify_args(values: &[Value]) -> Result<(String, String), AsyncNotifyArgError> {
    Ok((
        async_notify_text_arg(values.first(), "pg_notify", 0)?,
        async_notify_text_arg(values.get(1), "pg_notify", 1)?,
    ))
}

pub fn notification_queue_usage_value(queue_usage: Option<f64>) -> Value {
    Value::Float64(queue_usage.unwrap_or(0.0))
}

pub fn validate_pending_notification(
    channel: &str,
    payload: &str,
) -> Result<PendingNotification, AsyncNotifyQueueError> {
    if channel.is_empty() {
        return Err(AsyncNotifyQueueError::EmptyChannel);
    }
    if channel.len() > ASYNC_NOTIFY_CHANNEL_MAX_LEN {
        return Err(AsyncNotifyQueueError::ChannelTooLong);
    }
    if payload.len() > ASYNC_NOTIFY_PAYLOAD_MAX_LEN {
        return Err(AsyncNotifyQueueError::PayloadTooLong);
    }
    Ok(PendingNotification {
        channel: channel.to_string(),
        payload: payload.to_string(),
    })
}

pub fn queue_pending_notification(
    pending: &mut Vec<PendingNotification>,
    channel: &str,
    payload: &str,
) -> Result<(), AsyncNotifyQueueError> {
    let notification = validate_pending_notification(channel, payload)?;
    push_pending_notification(pending, notification);
    Ok(())
}

pub fn merge_pending_notifications(
    target: &mut Vec<PendingNotification>,
    source: Vec<PendingNotification>,
) {
    for notification in source {
        push_pending_notification(target, notification);
    }
}

pub fn pending_notification_bytes(notification: &PendingNotification) -> usize {
    notification.channel.len() + notification.payload.len() + 2
}

fn delivered_notification_bytes(notification: &DeliveredNotification) -> usize {
    notification.channel.len() + notification.payload.len() + 2
}

fn async_notify_text_arg(
    value: Option<&Value>,
    op: &'static str,
    arg_index: usize,
) -> Result<String, AsyncNotifyArgError> {
    match value.unwrap_or(&Value::Null) {
        Value::Null => Ok(String::new()),
        value => value.as_text().map(ToOwned::to_owned).ok_or_else(|| {
            AsyncNotifyArgError::TypeMismatch {
                op,
                arg_index,
                value: value.clone(),
            }
        }),
    }
}

fn push_pending_notification(
    pending: &mut Vec<PendingNotification>,
    notification: PendingNotification,
) {
    if pending.iter().any(|existing| existing == &notification) {
        return;
    }
    pending.push(notification);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_pending_notification_rejects_invalid_inputs() {
        assert_eq!(
            validate_pending_notification("", ""),
            Err(AsyncNotifyQueueError::EmptyChannel)
        );
        assert_eq!(
            validate_pending_notification(&"x".repeat(ASYNC_NOTIFY_CHANNEL_MAX_LEN + 1), ""),
            Err(AsyncNotifyQueueError::ChannelTooLong)
        );
        assert_eq!(
            validate_pending_notification("chan", &"x".repeat(ASYNC_NOTIFY_PAYLOAD_MAX_LEN + 1)),
            Err(AsyncNotifyQueueError::PayloadTooLong)
        );
    }

    #[test]
    fn pending_notifications_are_deduped_when_queued_or_merged() {
        let mut pending = Vec::new();
        queue_pending_notification(&mut pending, "chan", "payload").unwrap();
        queue_pending_notification(&mut pending, "chan", "payload").unwrap();
        merge_pending_notifications(
            &mut pending,
            vec![
                PendingNotification {
                    channel: "chan".into(),
                    payload: "payload".into(),
                },
                PendingNotification {
                    channel: "other".into(),
                    payload: String::new(),
                },
            ],
        );

        assert_eq!(
            pending,
            vec![
                PendingNotification {
                    channel: "chan".into(),
                    payload: "payload".into(),
                },
                PendingNotification {
                    channel: "other".into(),
                    payload: String::new(),
                },
            ]
        );
    }

    #[test]
    fn async_notify_runtime_delivers_to_listeners_and_tracks_queue_usage() {
        let runtime = AsyncNotifyRuntime::new();
        runtime.listen(2, "chan");
        runtime.listen(3, "other");
        let notifications = vec![PendingNotification {
            channel: "chan".into(),
            payload: "payload".into(),
        }];

        runtime.publish(1, &notifications);

        assert_eq!(
            runtime.pending_notifications(2),
            vec![DeliveredNotification {
                sender_pid: 1,
                channel: "chan".into(),
                payload: "payload".into(),
            }]
        );
        assert!(runtime.pending_notifications(3).is_empty());
        assert!(runtime.queue_usage() > 0.0);
        assert_eq!(runtime.drain(2).len(), 1);
        assert_eq!(runtime.queue_usage(), 0.0);
    }
}
