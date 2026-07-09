// LISTEN / NOTIFY / UNLISTEN (commands/async.c).
#![allow(non_snake_case)]

use std::cell::{Cell, RefCell};
use std::hash::{BuildHasher, Hash, Hasher};

use elog::{elog, ereport};
use init_small::globals as g;
use mcx::{Mcx, MemoryContext};
use types_core::NAMEDATALEN;
use types_error::{
    PgResult, DEBUG1, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE, ERROR, INFO,
};

mod queue;
pub mod builtins;

pub use queue::{
    check_notify_buffers, AsyncNotifyFreezeXids, AsyncShmemInit, AsyncShmemResetAfterCrash,
    AsyncShmemSize, NOTIFY_PAYLOAD_MAX_LENGTH,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum ListenActionKind {
    Listen,
    Unlisten,
    UnlistenAll,
}

struct ListenAction {
    kind: ListenActionKind,
    channel: Box<[u8]>,
}

struct ActionList {
    nesting_level: i32,
    actions: Vec<ListenAction>,
    upper: Option<Box<ActionList>>,
}

/// data = channel bytes, NUL, payload bytes, NUL (C's wire/dedup layout).
pub(crate) struct Notification {
    pub channel_len: u16,
    pub payload_len: u16,
    pub data: Box<[u8]>,
}

const MIN_HASHABLE_NOTIFIES: usize = 16;

type FxMap = std::collections::HashMap<u64, Vec<usize>, rustc_hash::FxBuildHasher>;

struct NotificationList {
    nesting_level: i32,
    events: Vec<Notification>,
    // Content-hash -> event indices; probe compares full content (no false
    // dedup on collision).
    hashtab: Option<FxMap>,
    upper: Option<Box<NotificationList>>,
}

struct Local {
    listen_channels: RefCell<Vec<Box<[u8]>>>,
    pending_actions: RefCell<Option<Box<ActionList>>>,
    pending_notifies: RefCell<Option<Box<NotificationList>>>,
    notify_interrupt_pending: Cell<bool>,
    unlisten_exit_registered: Cell<bool>,
    am_registered_listener: Cell<bool>,
    try_advance_tail: Cell<bool>,
}

thread_local! {
    static LOCAL: Local = const {
        Local {
            listen_channels: RefCell::new(Vec::new()),
            pending_actions: RefCell::new(None),
            pending_notifies: RefCell::new(None),
            notify_interrupt_pending: Cell::new(false),
            unlisten_exit_registered: Cell::new(false),
            am_registered_listener: Cell::new(false),
            try_advance_tail: Cell::new(false),
        }
    };
}

pub(crate) fn set_try_advance_tail() {
    LOCAL.with(|s| s.try_advance_tail.set(true));
}

pub(crate) fn set_notify_interrupt_pending() {
    LOCAL.with(|s| s.notify_interrupt_pending.set(true));
}

pub fn notifyInterruptPending() -> bool {
    LOCAL.with(|s| s.notify_interrupt_pending.get())
}

thread_local! {
    static TRACE_NOTIFY: Cell<bool> = const { Cell::new(false) };
    static MAX_NOTIFY_QUEUE_PAGES: Cell<i32> = const { Cell::new(1048576) };
}

fn trace_notify() -> bool {
    TRACE_NOTIFY.get()
}

pub(crate) fn max_notify_queue_pages() -> i32 {
    MAX_NOTIFY_QUEUE_PAGES.get()
}

fn notification_content_hash(n: &Notification, hasher: &rustc_hash::FxBuildHasher) -> u64 {
    // C hashes channel + NUL + payload (payload's trailing NUL excluded).
    let mut h = hasher.build_hasher();
    n.data[..n.channel_len as usize + n.payload_len as usize + 1].hash(&mut h);
    h.finish()
}

fn notification_equal(a: &Notification, b: &Notification) -> bool {
    a.channel_len == b.channel_len && a.payload_len == b.payload_len && a.data == b.data
}

pub fn Async_Notify(channel: &str, payload: Option<&str>) -> PgResult<()> {
    let my_level = xact::GetCurrentTransactionNestLevel();

    // Seam, not the parallel crate: the direct edge cycles via bgworker→postgres.
    if parallel_seams::is_parallel_worker::call() {
        elog(ERROR, "cannot send notifications from a parallel worker")?;
    }
    if trace_notify() {
        elog(DEBUG1, format!("Async_Notify({channel})"))?;
    }

    let channel_len = channel.len();
    let payload_len = payload.map_or(0, str::len);

    if channel_len == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("channel name cannot be empty")
            .into_error()
            .into());
    }
    if channel_len >= NAMEDATALEN as usize {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("channel name too long")
            .into_error()
            .into());
    }
    if payload_len >= NOTIFY_PAYLOAD_MAX_LENGTH {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("payload string too long")
            .into_error()
            .into());
    }

    let mut data = Vec::with_capacity(channel_len + payload_len + 2);
    data.extend_from_slice(channel.as_bytes());
    data.push(0);
    if let Some(p) = payload {
        data.extend_from_slice(p.as_bytes());
    }
    data.push(0);
    let n = Notification {
        channel_len: channel_len as u16,
        payload_len: payload_len as u16,
        data: data.into_boxed_slice(),
    };

    LOCAL.with(|s| {
        let mut pending = s.pending_notifies.borrow_mut();
        match pending.as_mut() {
            Some(list) if my_level <= list.nesting_level => {
                if !exists_pending_notify(list, &n) {
                    add_event_to_pending_notifies(list, n);
                }
            }
            _ => {
                let list = Box::new(NotificationList {
                    nesting_level: my_level,
                    events: vec![n],
                    hashtab: None,
                    upper: pending.take(),
                });
                *pending = Some(list);
            }
        }
    });
    Ok(())
}

fn exists_pending_notify(list: &NotificationList, n: &Notification) -> bool {
    match &list.hashtab {
        Some(tab) => {
            let h = notification_content_hash(n, tab.hasher());
            tab.get(&h)
                .is_some_and(|v| v.iter().any(|&i| notification_equal(&list.events[i], n)))
        }
        None => list.events.iter().any(|e| notification_equal(e, n)),
    }
}

fn add_event_to_pending_notifies(list: &mut NotificationList, n: Notification) {
    debug_assert!(!list.events.is_empty());

    if list.events.len() >= MIN_HASHABLE_NOTIFIES && list.hashtab.is_none() {
        let mut tab = FxMap::default();
        for (i, e) in list.events.iter().enumerate() {
            let h = notification_content_hash(e, tab.hasher());
            tab.entry(h).or_default().push(i);
        }
        list.hashtab = Some(tab);
    }

    let idx = list.events.len();
    if let Some(tab) = &mut list.hashtab {
        let h = notification_content_hash(&n, tab.hasher());
        tab.entry(h).or_default().push(idx);
    }
    list.events.push(n);
}

fn queue_listen(kind: ListenActionKind, channel: &str) {
    // Duplicates are not collapsed: LISTEN/UNLISTEN/UNLISTEN* interactions
    // must replay in order (async.c:696).
    let my_level = xact::GetCurrentTransactionNestLevel();
    let action = ListenAction { kind, channel: channel.as_bytes().into() };

    LOCAL.with(|s| {
        let mut pending = s.pending_actions.borrow_mut();
        match pending.as_mut() {
            Some(list) if my_level <= list.nesting_level => list.actions.push(action),
            _ => {
                let list = Box::new(ActionList {
                    nesting_level: my_level,
                    actions: vec![action],
                    upper: pending.take(),
                });
                *pending = Some(list);
            }
        }
    });
}

pub fn Async_Listen(channel: &str) -> PgResult<()> {
    if trace_notify() {
        elog(DEBUG1, format!("Async_Listen({channel},{})", g::MyProcPid()))?;
    }
    queue_listen(ListenActionKind::Listen, channel);
    Ok(())
}

pub fn Async_Unlisten(channel: &str) -> PgResult<()> {
    if trace_notify() {
        elog(DEBUG1, format!("Async_Unlisten({channel},{})", g::MyProcPid()))?;
    }
    if LOCAL.with(|s| s.pending_actions.borrow().is_none() && !s.unlisten_exit_registered.get()) {
        return Ok(());
    }
    queue_listen(ListenActionKind::Unlisten, channel);
    Ok(())
}

pub fn Async_UnlistenAll() -> PgResult<()> {
    if trace_notify() {
        elog(DEBUG1, format!("Async_UnlistenAll({})", g::MyProcPid()))?;
    }
    if LOCAL.with(|s| s.pending_actions.borrow().is_none() && !s.unlisten_exit_registered.get()) {
        return Ok(());
    }
    queue_listen(ListenActionKind::UnlistenAll, "");
    Ok(())
}

/// pg_listening_channels row source; call_cntr indexes the (in-transaction
/// stable) channel list.
pub(crate) fn listening_channel_at(i: usize) -> Option<Box<[u8]>> {
    LOCAL.with(|s| s.listen_channels.borrow().get(i).cloned())
}

fn Async_UnlistenOnExit(_code: i32, _arg: datum::Datum) -> PgResult<()> {
    exec_unlisten_all_commit();
    async_queue_unregister()
}

pub fn AtPrepare_Notify() -> PgResult<()> {
    let has_pending = LOCAL
        .with(|s| s.pending_actions.borrow().is_some() || s.pending_notifies.borrow().is_some());
    if has_pending {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot PREPARE a transaction that has executed LISTEN, UNLISTEN, or NOTIFY")
            .into_error()
            .into());
    }
    Ok(())
}

pub fn PreCommit_Notify() -> PgResult<()> {
    let (has_actions, has_notifies) = LOCAL
        .with(|s| (s.pending_actions.borrow().is_some(), s.pending_notifies.borrow().is_some()));
    if !has_actions && !has_notifies {
        return Ok(());
    }
    if trace_notify() {
        elog(DEBUG1, "PreCommit_Notify")?;
    }

    if has_actions {
        let any_listen = LOCAL.with(|s| {
            s.pending_actions
                .borrow()
                .as_ref()
                .is_some_and(|l| l.actions.iter().any(|a| a.kind == ListenActionKind::Listen))
        });
        if any_listen {
            exec_listen_pre_commit()?;
        }
    }

    if has_notifies {
        // Take the events out for the enqueue borrow; AtCommit/AtAbort still
        // need the list present, so put them back.
        let list = LOCAL.with(|s| s.pending_notifies.borrow_mut().take());
        let list = list.expect("pending_notifies checked above");
        let r = queue::enqueue_pending(&list.events);
        LOCAL.with(|s| *s.pending_notifies.borrow_mut() = Some(list));
        r?;
    }
    Ok(())
}

pub fn AtCommit_Notify() -> PgResult<()> {
    let (has_actions, has_notifies) = LOCAL
        .with(|s| (s.pending_actions.borrow().is_some(), s.pending_notifies.borrow().is_some()));
    if !has_actions && !has_notifies {
        return Ok(());
    }
    if trace_notify() {
        elog(DEBUG1, "AtCommit_Notify")?;
    }

    if has_actions {
        let actions = LOCAL.with(|s| s.pending_actions.borrow_mut().take());
        let actions = actions.expect("pending_actions checked above");
        for a in &actions.actions {
            match a.kind {
                ListenActionKind::Listen => exec_listen_commit(&a.channel),
                ListenActionKind::Unlisten => exec_unlisten_commit(&a.channel)?,
                ListenActionKind::UnlistenAll => exec_unlisten_all_commit(),
            }
        }
    }

    let listening = LOCAL.with(|s| !s.listen_channels.borrow().is_empty());
    if LOCAL.with(|s| s.am_registered_listener.get()) && !listening {
        async_queue_unregister()?;
    }

    if has_notifies {
        queue::signal_backends()?;
    }

    if LOCAL.with(|s| s.try_advance_tail.replace(false)) {
        queue::advance_tail()?;
    }

    clear_pending_actions_and_notifies();
    Ok(())
}

fn exec_listen_pre_commit() -> PgResult<()> {
    if LOCAL.with(|s| s.am_registered_listener.get()) {
        return Ok(());
    }
    if trace_notify() {
        elog(DEBUG1, format!("Exec_ListenPreCommit({})", g::MyProcPid()))?;
    }

    if !LOCAL.with(|s| s.unlisten_exit_registered.get()) {
        ipc::before_shmem_exit(Async_UnlistenOnExit, datum::Datum::null())?;
        LOCAL.with(|s| s.unlisten_exit_registered.set(true));
    }

    let behind = queue::register_listener()?;
    LOCAL.with(|s| s.am_registered_listener.set(true));

    // Skip over already-committed notifications; we're listening on nothing
    // yet, so none are delivered.
    if behind {
        async_queue_read_all_notifications()?;
    }
    Ok(())
}

fn exec_listen_commit(channel: &[u8]) {
    LOCAL.with(|s| {
        let mut chans = s.listen_channels.borrow_mut();
        if !chans.iter().any(|c| c.as_ref() == channel) {
            chans.push(channel.into());
        }
    });
}

fn exec_unlisten_commit(channel: &[u8]) -> PgResult<()> {
    if trace_notify() {
        elog(
            DEBUG1,
            format!(
                "Exec_UnlistenCommit({},{})",
                String::from_utf8_lossy(channel),
                g::MyProcPid()
            ),
        )?;
    }
    LOCAL.with(|s| {
        let mut chans = s.listen_channels.borrow_mut();
        if let Some(i) = chans.iter().position(|c| c.as_ref() == channel) {
            chans.remove(i);
        }
    });
    Ok(())
}

fn exec_unlisten_all_commit() {
    LOCAL.with(|s| s.listen_channels.borrow_mut().clear());
}

fn is_listening_on(channel: &[u8]) -> bool {
    LOCAL.with(|s| s.listen_channels.borrow().iter().any(|c| c.as_ref() == channel))
}

fn async_queue_unregister() -> PgResult<()> {
    debug_assert!(LOCAL.with(|s| s.listen_channels.borrow().is_empty()));
    if !LOCAL.with(|s| s.am_registered_listener.get()) {
        return Ok(());
    }
    queue::unregister_listener()?;
    LOCAL.with(|s| s.am_registered_listener.set(false));
    Ok(())
}

pub fn AtAbort_Notify() {
    if LOCAL.with(|s| s.am_registered_listener.get() && s.listen_channels.borrow().is_empty()) {
        // Failure here would escalate an abort; C's LWLock usage cannot fail.
        async_queue_unregister().expect("asyncQueueUnregister failed during abort");
    }
    clear_pending_actions_and_notifies();
}

pub fn AtSubCommit_Notify() -> PgResult<()> {
    at_subcommit_merge(xact::GetCurrentTransactionNestLevel());
    Ok(())
}

fn at_subcommit_merge(my_level: i32) {
    LOCAL.with(|s| {
        let mut pending = s.pending_actions.borrow_mut();
        if let Some(mut list) = pending.take_if(|l| l.nesting_level >= my_level) {
            if list.upper.as_ref().is_none_or(|u| u.nesting_level < my_level - 1) {
                list.nesting_level -= 1;
                *pending = Some(list);
            } else {
                let mut parent = list.upper.take().expect("upper checked above");
                parent.actions.append(&mut list.actions);
                *pending = Some(parent);
            }
        }
        drop(pending);

        let mut pending = s.pending_notifies.borrow_mut();
        if let Some(mut list) = pending.take_if(|l| l.nesting_level >= my_level) {
            debug_assert!(list.nesting_level == my_level);
            if list.upper.as_ref().is_none_or(|u| u.nesting_level < my_level - 1) {
                list.nesting_level -= 1;
                *pending = Some(list);
            } else {
                let mut parent = list.upper.take().expect("upper checked above");
                for n in list.events.drain(..) {
                    if !exists_pending_notify(&parent, &n) {
                        add_event_to_pending_notifies(&mut parent, n);
                    }
                }
                *pending = Some(parent);
            }
        }
    });
}

pub fn AtSubAbort_Notify() {
    at_subabort_pop(xact::GetCurrentTransactionNestLevel());
}

fn at_subabort_pop(my_level: i32) {
    LOCAL.with(|s| {
        let mut pending = s.pending_actions.borrow_mut();
        while let Some(list) = pending.take_if(|l| l.nesting_level >= my_level) {
            *pending = list.upper;
        }
        drop(pending);
        let mut pending = s.pending_notifies.borrow_mut();
        while let Some(list) = pending.take_if(|l| l.nesting_level >= my_level) {
            *pending = list.upper;
        }
    });
}

fn clear_pending_actions_and_notifies() {
    LOCAL.with(|s| {
        *s.pending_actions.borrow_mut() = None;
        *s.pending_notifies.borrow_mut() = None;
    });
}

pub fn HandleNotifyInterrupt() {
    set_notify_interrupt_pending();
    if let Some(l) = g::MyLatch() {
        latch::SetLatch(l);
    }
}

/// Called just before ReadyForQuery (flush=false: RFQ flushes) and when a
/// notify interrupt lands during a client read (flush=true).
pub fn ProcessNotifyInterrupt(flush: bool) -> PgResult<()> {
    // Not really idle: reading the queue here would advance our listener pos
    // mid-transaction (C returns; the pinned pos is what async-notify tests).
    if xact::IsTransactionOrTransactionBlock() {
        return Ok(());
    }
    while notifyInterruptPending() {
        ProcessIncomingNotify(flush)?;
    }
    Ok(())
}

fn ProcessIncomingNotify(flush: bool) -> PgResult<()> {
    LOCAL.with(|s| s.notify_interrupt_pending.set(false));

    if LOCAL.with(|s| s.listen_channels.borrow().is_empty()) {
        return Ok(());
    }
    if trace_notify() {
        elog(DEBUG1, "ProcessIncomingNotify")?;
    }

    ps_status_seams::set_ps_display::call("notify interrupt");

    xact::StartTransactionCommand()?;
    async_queue_read_all_notifications()?;
    xact::CommitTransactionCommand()?;

    if flush {
        pqcomm_seams::pq_flush::call()?;
    }

    ps_status_seams::set_ps_display::call("idle");

    if trace_notify() {
        elog(DEBUG1, "ProcessIncomingNotify: done")?;
    }
    Ok(())
}

fn async_queue_read_all_notifications() -> PgResult<()> {
    let queue::ReadPositions { mut pos, head } = queue::fetch_read_positions()?;
    if pos == head {
        return Ok(());
    }

    // Uncommitted-xact visibility decisions use a fresh snapshot; entries from
    // xacts committing after it will re-signal us (async.c:1871-1908).
    let snapshot = snapmgr::GetLatestSnapshot()?;
    let snapshot = snapmgr::RegisterSnapshot(Some(&snapshot))?.expect("registered snapshot");

    // A send failure must not be retried against the same entry: escalate
    // ERROR->FATAL so the client sees a closed connection, not a lost notify.
    let save_exit_on_any_error = g::ExitOnAnyError();
    g::SetExitOnAnyError(true);

    let result = (|| -> PgResult<()> {
        let msgctx = MemoryContext::new("NotifyMessages");
        let listening = LOCAL.with(|s| !s.listen_channels.borrow().is_empty());
        let mut local: Vec<u8> = Vec::new();
        loop {
            local.clear();
            let reached_stop =
                queue::process_page_entries(&mut pos, head, &snapshot, listening, &mut local)?;
            deliver_local_entries(msgctx.mcx(), &local)?;
            if reached_stop {
                break;
            }
        }
        queue::update_my_read_position(pos)
    })();

    g::SetExitOnAnyError(save_exit_on_any_error);
    result?;

    snapmgr::UnregisterSnapshot(Some(&snapshot));
    Ok(())
}

fn deliver_local_entries(mcx: Mcx<'_>, local: &[u8]) -> PgResult<()> {
    let mut p = 0;
    while p < local.len() {
        let entry = queue::parse_entry(local, p);
        let data = entry.data;
        let channel_end = data.iter().position(|&b| b == 0).expect("NUL-terminated channel");
        let channel = &data[..channel_end];
        if is_listening_on(channel) {
            let rest = &data[channel_end + 1..];
            let payload_end = rest.iter().position(|&b| b == 0).expect("NUL-terminated payload");
            NotifyMyFrontEnd(mcx, channel, &rest[..payload_end], entry.src_pid)?;
        }
        p += entry.length as usize;
    }
    Ok(())
}

const PQMSG_NOTIFICATION_RESPONSE: u8 = b'A';

pub fn NotifyMyFrontEnd(
    mcx: Mcx<'_>,
    channel: &[u8],
    payload: &[u8],
    src_pid: i32,
) -> PgResult<()> {
    if elog::config::where_to_send_output() == types_dest::CommandDest::Remote {
        let mut buf = pqformat::pq_beginmessage(mcx, PQMSG_NOTIFICATION_RESPONSE)?;
        pqformat::pq_sendint32(&mut buf, src_pid as u32)?;
        pqformat::pq_sendstring(&mut buf, channel)?;
        pqformat::pq_sendstring(&mut buf, payload)?;
        // No flush: callers batch it with following messages (C: async.c:2360).
        pqformat::pq_endmessage(buf)
    } else {
        elog(
            INFO,
            format!(
                "NOTIFY for \"{}\" payload \"{}\"",
                String::from_utf8_lossy(channel),
                String::from_utf8_lossy(payload)
            ),
        )
    }
}

pub fn init_seams() {
    async_seams::pre_commit_notify::set(PreCommit_Notify);
    async_seams::at_commit_notify::set(AtCommit_Notify);
    async_seams::at_abort_notify::set(AtAbort_Notify);
    async_seams::at_subcommit_notify::set(AtSubCommit_Notify);
    async_seams::at_subabort_notify::set(AtSubAbort_Notify);
    async_seams::at_prepare_notify::set(AtPrepare_Notify);
    async_seams::handle_notify_interrupt::set(HandleNotifyInterrupt);
    async_seams::async_notify_freeze_xids::set(AsyncNotifyFreezeXids);

    fn check_hook(
        newval: &mut i32,
        _extra: &mut Option<guc_tables::GucHookExtra>,
        _source: types_guc::GucSource,
    ) -> PgResult<bool> {
        let (ok, detail) = check_notify_buffers(*newval);
        if !ok {
            if let Some(d) = detail {
                guc_seams::guc_check_errdetail::call(d);
            }
        }
        Ok(ok)
    }
    guc_tables::hooks::check_notify_buffers.install(check_hook);

    use guc_tables::GucVarAccessors;
    guc_tables::vars::Trace_notify.install(GucVarAccessors {
        get: || TRACE_NOTIFY.get(),
        set: |v| TRACE_NOTIFY.set(v),
    });
    guc_tables::vars::max_notify_queue_pages.install(GucVarAccessors {
        get: || MAX_NOTIFY_QUEUE_PAGES.get(),
        set: |v| MAX_NOTIFY_QUEUE_PAGES.set(v),
    });
}

#[cfg(test)]
mod tests;
