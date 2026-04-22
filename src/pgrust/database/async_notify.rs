use crate::ClientId;
use crate::backend::executor::ExecError;
use crate::backend::storage::smgr::BLCKSZ;
use parking_lot::RwLock;
use std::collections::{BTreeSet, HashMap};

pub(crate) const ASYNC_NOTIFY_CHANNEL_MAX_LEN: usize = 63;
pub(crate) const ASYNC_NOTIFY_PAYLOAD_MAX_LEN: usize = BLCKSZ - 64 - 128;
pub(crate) const ASYNC_NOTIFY_QUEUE_CAPACITY_BYTES: usize = BLCKSZ * 1024;

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
pub(crate) enum AsyncListenAction {
    Listen,
    Unlisten,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AsyncListenOp {
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
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn is_listening(&self, client_id: ClientId, channel: &str) -> bool {
        self.state
            .read()
            .listeners_by_client
            .get(&client_id)
            .is_some_and(|channels| channels.contains(channel))
    }

    pub(crate) fn listening_channels(&self, client_id: ClientId) -> Vec<String> {
        self.state
            .read()
            .listeners_by_client
            .get(&client_id)
            .map(|channels| channels.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn pending_notifications(&self, client_id: ClientId) -> Vec<DeliveredNotification> {
        self.state
            .read()
            .pending_by_client
            .get(&client_id)
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn queue_usage(&self) -> f64 {
        let state = self.state.read();
        if state.approx_queue_bytes == 0 {
            0.0
        } else {
            state.approx_queue_bytes as f64 / ASYNC_NOTIFY_QUEUE_CAPACITY_BYTES as f64
        }
    }

    pub(crate) fn listen(&self, client_id: ClientId, channel: &str) {
        self.state
            .write()
            .listeners_by_client
            .entry(client_id)
            .or_default()
            .insert(channel.to_string());
    }

    pub(crate) fn unlisten(&self, client_id: ClientId, channel: Option<&str>) {
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

    pub(crate) fn apply_listener_ops(&self, client_id: ClientId, ops: &[AsyncListenOp]) {
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

    pub(crate) fn publish(
        &self,
        sender_client_id: ClientId,
        notifications: &[PendingNotification],
    ) {
        if notifications.is_empty() {
            return;
        }
        let sender_pid = sender_client_id as i32;
        let mut state = self.state.write();
        for notification in notifications {
            let queue_bytes = queued_notification_bytes(notification);
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

    pub(crate) fn drain(&self, client_id: ClientId) -> Vec<DeliveredNotification> {
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

    pub(crate) fn disconnect(&self, client_id: ClientId) {
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

pub(crate) fn validate_pending_notification(
    channel: &str,
    payload: &str,
) -> Result<PendingNotification, ExecError> {
    if channel.is_empty() {
        return Err(invalid_notify_parameter("channel name cannot be empty"));
    }
    if channel.len() > ASYNC_NOTIFY_CHANNEL_MAX_LEN {
        return Err(invalid_notify_parameter("channel name too long"));
    }
    if payload.len() > ASYNC_NOTIFY_PAYLOAD_MAX_LEN {
        return Err(invalid_notify_parameter("payload string too long"));
    }
    Ok(PendingNotification {
        channel: channel.to_string(),
        payload: payload.to_string(),
    })
}

pub(crate) fn queue_pending_notification(
    pending: &mut Vec<PendingNotification>,
    channel: &str,
    payload: &str,
) -> Result<(), ExecError> {
    let notification = validate_pending_notification(channel, payload)?;
    push_pending_notification(pending, notification);
    Ok(())
}

pub(crate) fn merge_pending_notifications(
    target: &mut Vec<PendingNotification>,
    source: Vec<PendingNotification>,
) {
    for notification in source {
        push_pending_notification(target, notification);
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

fn invalid_notify_parameter(message: &str) -> ExecError {
    ExecError::DetailedError {
        message: message.to_string(),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

fn queued_notification_bytes(notification: &PendingNotification) -> usize {
    notification.channel.len() + notification.payload.len() + 2
}

fn delivered_notification_bytes(notification: &DeliveredNotification) -> usize {
    notification.channel.len() + notification.payload.len() + 2
}
