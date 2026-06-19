#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]
// `if_same_then_else` (QUEUE_POS_MAX's 2nd/3rd ternary branches yield the same
// value, kept distinct to mirror the C control flow exactly); `needless_range_loop`
// (C indexes pids[]/procnos[] by counter).
#![allow(clippy::if_same_then_else)]
#![allow(clippy::needless_range_loop)]

//! Port of `backend/commands/async.c` — asynchronous notification:
//! NOTIFY / LISTEN / UNLISTEN (PostgreSQL 18.3).
//!
//! # Shared-memory ownership
//!
//! C keeps `AsyncQueueControl` (a `ShmemInitStruct` segment: a fixed header plus
//! a per-backend `QueueBackendStatus backend[FLEXIBLE_ARRAY_MEMBER]` array) and a
//! file-static `SlruCtlData NotifyCtlData`. Following the established
//! repo pattern (multixact's `MultiXactStateData`, clog's `XactCtlData`), both are
//! owned in-crate as `thread_local!` slots: the queue-control header scalars plus
//! an owned `Vec<QueueBackendStatus>` for the per-backend array, and a
//! `SlruCtlData` instance built via `SimpleLruInit`. The queue control is
//! protected by the fixed `NotifyQueueLock` / `NotifyQueueTailLock` LWLocks and
//! the `NotifyCtl` SLRU bank locks, exactly as in C.
//!
//! The backend-local LISTEN/UNLISTEN action list, the outbound-notify pending
//! list, `listenChannels`, and the dedup hashtable are per-backend, exactly as in
//! C.

#[cfg(test)]
mod tests;

use std::cell::{Cell, RefCell};

use backend_utils_error::{ereport, PgError};
use types_error::{
    ErrorLocation, PgResult, DEBUG1, DEBUG3, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR, INFO, WARNING,
};

use backend_access_transam_slru::{
    self as slru, SimpleLruGetBankLock, SimpleLruInit, SimpleLruReadPage, SimpleLruReadPage_ReadOnly,
    SimpleLruShmemSize, SimpleLruTruncate, SimpleLruZeroPage, SlruCtlData, SlruScanDirCbDeleteAll,
    SlruScanDirectory, SLRU_PAGES_PER_SEGMENT,
};
use backend_storage_lmgr_lwlock::{main_lock_ref, LWLockAcquire, LWLockRelease};
use backend_utils_init_small::globals;

use types_async::{
    AsyncQueueControl, AsyncQueueEntry, AsyncQueueEntryEmptySize, InvalidPid, ListenActionKind,
    QueueBackendStatus, QueuePosition, ASYNC_QUEUE_CONTROL_HEADER_SIZE, MIN_HASHABLE_NOTIFIES,
    NAMEDATALEN, NOTIFY_PAYLOAD_MAX_LENGTH, QUEUEALIGN, QUEUE_CLEANUP_DELAY, QUEUE_FULL_WARN_INTERVAL,
    QUEUE_PAGESIZE,
};
use types_core::{
    InvalidOid, Oid, ProcNumber, Size, TransactionId, INVALID_PROC_NUMBER,
};
use types_storage::storage::{
    LW_EXCLUSIVE, LW_SHARED, NOTIFY_QUEUE_LOCK, NOTIFY_QUEUE_TAIL_LOCK,
};
use types_storage::sync::SyncRequestHandler;

// Seam aliases (cycle partners / unported owners).
use backend_access_transam_parallel as parallel_seams;
use backend_access_transam_transam_seams as transam_seams;
use backend_access_transam_xact_seams as xact_seams;
use backend_storage_ipc_dsm_core_seams as ipc_seams;
use backend_storage_ipc_latch_seams as latch_seams;
use backend_storage_ipc_procsignal_seams as procsignal_seams;
use backend_storage_lmgr_lmgr_seams as lmgr_seams;
use backend_utils_adt_timestamp_seams as timestamp_seams;
use backend_utils_init_small_seams as init_seams_decls;
use backend_utils_misc_ps_status_seams as ps_seams;
use backend_utils_time_snapmgr_pc_seams as snapmgr_pc_seams;
use backend_utils_time_snapmgr_seams as snapmgr_seams;

// ---------------------------------------------------------------------------
// transam constants / xact / dest seam-and-panic decls owned by the right owner
// ---------------------------------------------------------------------------

// `InvalidTransactionId` (transam.h) and `FirstNormalTransactionId`,
// `FrozenTransactionId`.
const InvalidTransactionId: TransactionId = 0;
const FirstNormalTransactionId: TransactionId = 3;
const FrozenTransactionId: TransactionId = 2;

// The two fixed NOTIFY LWLocks (`NotifyQueueLock` / `NotifyQueueTailLock`).
fn notify_queue_lock() -> &'static types_storage::storage::LWLock {
    main_lock_ref(NOTIFY_QUEUE_LOCK)
}
fn notify_queue_tail_lock() -> &'static types_storage::storage::LWLock {
    main_lock_ref(NOTIFY_QUEUE_TAIL_LOCK)
}

// ---------------------------------------------------------------------------
// Trace/log helpers
// ---------------------------------------------------------------------------

#[inline]
fn here() -> ErrorLocation {
    ErrorLocation::new("async.c", 0, "")
}

/// `Trace_notify` GUC — backend-local flag (async.c line 425). Default false.
fn trace_notify_enabled() -> bool {
    TRACE_NOTIFY.with(|f| f.get())
}

fn elog_error(msg: &str) -> PgError {
    ereport(ERROR).errmsg_internal(msg.to_string()).into_error()
}

// ---------------------------------------------------------------------------
// Backend-local file-scoped state (async.c lines 320-428)
// ---------------------------------------------------------------------------

thread_local! {
    /// `bool Trace_notify` GUC (async.c line 425).
    static TRACE_NOTIFY: Cell<bool> = const { Cell::new(false) };

    /// `int max_notify_queue_pages` GUC (async.c line 428). For 8 KB pages this
    /// gives 8 GB of disk space.
    static MAX_NOTIFY_QUEUE_PAGES: Cell<i32> = const { Cell::new(1_048_576) };

    /// `volatile sig_atomic_t notifyInterruptPending` (async.c line 413) — set by
    /// the signal handler, consumed by `ProcessNotifyInterrupt`.
    static NOTIFY_INTERRUPT_PENDING: Cell<bool> = const { Cell::new(false) };

    /// `static List *listenChannels` (async.c line 320) — channels we have
    /// committed a LISTEN on (TopMemoryContext). Backend-local C strings.
    static LISTEN_CHANNELS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };

    /// `static bool unlistenExitRegistered` (async.c line 416).
    static UNLISTEN_EXIT_REGISTERED: Cell<bool> = const { Cell::new(false) };

    /// `static bool amRegisteredListener` (async.c line 419).
    static AM_REGISTERED_LISTENER: Cell<bool> = const { Cell::new(false) };

    /// `static bool tryAdvanceTail` (async.c line 422).
    static TRY_ADVANCE_TAIL: Cell<bool> = const { Cell::new(false) };

    /// `static ActionList *pendingActions` (async.c line 352).
    static PENDING_ACTIONS: RefCell<Option<Box<ActionList>>> = const { RefCell::new(None) };

    /// `static NotificationList *pendingNotifies` (async.c line 404).
    static PENDING_NOTIFIES: RefCell<Option<Box<NotificationList>>> = const { RefCell::new(None) };

    /// `static AsyncQueueControl *asyncQueueControl` (async.c line 294) — the
    /// shared-memory queue control. Owned in-crate (multixact pattern): the
    /// fixed header scalars plus the per-backend `backend[]` array as an owned
    /// `Vec<QueueBackendStatus>`.
    static ASYNC_QUEUE_CONTROL: RefCell<Option<AsyncQueueControlData>> =
        const { RefCell::new(None) };

    /// `static SlruCtlData NotifyCtlData` (async.c line 308) / `NotifyCtl`.
    static NOTIFY_CTL: RefCell<Option<SlruCtlData>> = const { RefCell::new(None) };
}

/// The owned `AsyncQueueControl` segment: the fixed header plus the per-backend
/// `backend[FLEXIBLE_ARRAY_MEMBER]` array.
struct AsyncQueueControlData {
    header: AsyncQueueControl,
    backend: Vec<QueueBackendStatus>,
}

/// Run `f` with mutable access to the shared `AsyncQueueControl`.
fn with_queue<R>(f: impl FnOnce(&mut AsyncQueueControlData) -> R) -> R {
    ASYNC_QUEUE_CONTROL.with(|c| {
        let mut b = c.borrow_mut();
        f(b.as_mut()
            .expect("asyncQueueControl used before AsyncShmemInit"))
    })
}

/// Run `f` with mutable access to the `NotifyCtl` SLRU control.
fn with_notify_ctl<R>(f: impl FnOnce(&mut SlruCtlData) -> R) -> R {
    NOTIFY_CTL.with(|c| {
        let mut b = c.borrow_mut();
        f(b.as_mut().expect("NotifyCtl used before AsyncShmemInit"))
    })
}

/// Read `notifyInterruptPending` (async.c line 413) — read-only accessor for the
/// tcop main loop poll.
pub fn notify_interrupt_pending() -> bool {
    NOTIFY_INTERRUPT_PENDING.with(|f| f.get())
}

// ---------------------------------------------------------------------------
// QUEUE_* accessor macros (async.c lines 296-303) over the owned control
// ---------------------------------------------------------------------------

fn QUEUE_HEAD() -> QueuePosition {
    with_queue(|q| q.header.head)
}
fn set_QUEUE_HEAD(v: QueuePosition) {
    with_queue(|q| q.header.head = v);
}
fn QUEUE_TAIL() -> QueuePosition {
    with_queue(|q| q.header.tail)
}
fn set_QUEUE_TAIL(v: QueuePosition) {
    with_queue(|q| q.header.tail = v);
}
fn QUEUE_STOP_PAGE() -> i64 {
    with_queue(|q| q.header.stopPage)
}
fn set_QUEUE_STOP_PAGE(v: i64) {
    with_queue(|q| q.header.stopPage = v);
}
fn QUEUE_FIRST_LISTENER() -> ProcNumber {
    with_queue(|q| q.header.firstListener)
}
fn set_QUEUE_FIRST_LISTENER(v: ProcNumber) {
    with_queue(|q| q.header.firstListener = v);
}
fn QUEUE_LAST_FILL_WARN() -> types_core::TimestampTz {
    with_queue(|q| q.header.lastQueueFillWarn)
}
fn set_QUEUE_LAST_FILL_WARN(v: types_core::TimestampTz) {
    with_queue(|q| q.header.lastQueueFillWarn = v);
}
fn QUEUE_BACKEND_PID(i: ProcNumber) -> i32 {
    with_queue(|q| q.backend[i as usize].pid)
}
fn set_QUEUE_BACKEND_PID(i: ProcNumber, v: i32) {
    with_queue(|q| q.backend[i as usize].pid = v);
}
fn QUEUE_BACKEND_DBOID(i: ProcNumber) -> Oid {
    with_queue(|q| q.backend[i as usize].dboid)
}
fn set_QUEUE_BACKEND_DBOID(i: ProcNumber, v: Oid) {
    with_queue(|q| q.backend[i as usize].dboid = v);
}
fn QUEUE_NEXT_LISTENER(i: ProcNumber) -> ProcNumber {
    with_queue(|q| q.backend[i as usize].nextListener)
}
fn set_QUEUE_NEXT_LISTENER(i: ProcNumber, v: ProcNumber) {
    with_queue(|q| q.backend[i as usize].nextListener = v);
}
fn QUEUE_BACKEND_POS(i: ProcNumber) -> QueuePosition {
    with_queue(|q| q.backend[i as usize].pos)
}
fn set_QUEUE_BACKEND_POS(i: ProcNumber, v: QueuePosition) {
    with_queue(|q| q.backend[i as usize].pos = v);
}

// ---------------------------------------------------------------------------
// Backend-local record types (async.c lines 339-402)
// ---------------------------------------------------------------------------

/// `ListenAction` (async.c lines 339-343).
pub struct ListenAction {
    pub action: ListenActionKind,
    pub channel: String,
}

/// `ActionList` (async.c lines 345-350).
pub struct ActionList {
    pub nestingLevel: i32,
    pub actions: Vec<ListenAction>,
    pub upper: Option<Box<ActionList>>,
}

/// `Notification` (async.c lines 381-387). The null-terminated channel then
/// payload are packed into `data` to keep hashing/dedup byte-identical to C.
#[derive(Clone)]
pub struct Notification {
    pub channel_len: u16,
    pub payload_len: u16,
    pub data: Vec<u8>,
}

/// `NotificationList` (async.c lines 389-395). `hashtab` models C's `HTAB *` keyed
/// by `Notification *`; buckets store the index of each live event in `events`
/// (faithful because the hashtable's lifetime is strictly nested in `events`'s
/// and we never reorder/remove while it exists).
pub struct NotificationList {
    pub nestingLevel: i32,
    pub events: Vec<Notification>,
    pub hashtab: Option<NotificationHashTable>,
    pub upper: Option<Box<NotificationList>>,
}

/// `struct NotificationHash` (async.c lines 399-402) plus its backing HTAB.
#[derive(Default)]
pub struct NotificationHashTable {
    buckets: std::collections::HashMap<u32, Vec<usize>>,
}

impl NotificationHashTable {
    fn new() -> Self {
        Self {
            buckets: std::collections::HashMap::new(),
        }
    }

    /// `hash_search(hashtab, &n, HASH_FIND, NULL)`.
    fn find(&self, events: &[Notification], n: &Notification) -> bool {
        let h = notification_hash(n, core::mem::size_of::<usize>());
        if let Some(bucket) = self.buckets.get(&h) {
            for &idx in bucket {
                if notification_match(&events[idx], n, core::mem::size_of::<usize>()) == 0 {
                    return true;
                }
            }
        }
        false
    }

    /// `hash_search(hashtab, &n, HASH_ENTER, &found)`.
    fn enter(&mut self, events: &[Notification], n: &Notification, idx: usize) -> bool {
        let h = notification_hash(n, core::mem::size_of::<usize>());
        let bucket = self.buckets.entry(h).or_default();
        for &existing in bucket.iter() {
            if notification_match(&events[existing], n, core::mem::size_of::<usize>()) == 0 {
                return true;
            }
        }
        bucket.push(idx);
        false
    }
}

// ---------------------------------------------------------------------------
// QueuePosition helpers (async.c lines 200-225)
// ---------------------------------------------------------------------------

#[inline]
fn QUEUE_POS_PAGE(x: QueuePosition) -> i64 {
    x.page
}
#[inline]
fn QUEUE_POS_OFFSET(x: QueuePosition) -> i32 {
    x.offset
}
#[inline]
fn SET_QUEUE_POS(x: &mut QueuePosition, y: i64, z: i32) {
    x.page = y;
    x.offset = z;
}
#[inline]
fn QUEUE_POS_EQUAL(x: QueuePosition, y: QueuePosition) -> bool {
    x.page == y.page && x.offset == y.offset
}
#[inline]
fn QUEUE_POS_IS_ZERO(x: QueuePosition) -> bool {
    x.page == 0 && x.offset == 0
}
/// `QUEUE_POS_MIN(x,y)` (async.c 216-219).
#[inline]
fn QUEUE_POS_MIN(x: QueuePosition, y: QueuePosition) -> QueuePosition {
    if asyncQueuePagePrecedes(x.page, y.page) {
        x
    } else if x.page != y.page {
        y
    } else if x.offset < y.offset {
        x
    } else {
        y
    }
}
/// `QUEUE_POS_MAX(x,y)` (async.c 222-225).
#[inline]
fn QUEUE_POS_MAX(x: QueuePosition, y: QueuePosition) -> QueuePosition {
    if asyncQueuePagePrecedes(x.page, y.page) {
        y
    } else if x.page != y.page {
        x
    } else if x.offset > y.offset {
        x
    } else {
        y
    }
}

// ---------------------------------------------------------------------------
// Queue-position page helpers (async.c lines 464-478)
// ---------------------------------------------------------------------------

/// `asyncQueuePageDiff(p, q)` — `p - q` (async.c lines 464-468).
#[inline]
fn asyncQueuePageDiff(p: i64, q: i64) -> i64 {
    p - q
}

/// `asyncQueuePagePrecedes(p, q)` — `p < q` (async.c lines 474-478). Also wired as
/// `NotifyCtl->PagePrecedes`.
#[inline]
fn asyncQueuePagePrecedes(p: i64, q: i64) -> bool {
    p < q
}

// ---------------------------------------------------------------------------
// transam macros (access/transam/transam.h)
// ---------------------------------------------------------------------------

#[inline]
fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
}

#[inline]
fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 < id2;
    }
    (id1.wrapping_sub(id2) as i32) < 0
}

// ---------------------------------------------------------------------------
// Shared-memory sizing / init (async.c lines 483-548)
// ---------------------------------------------------------------------------

/// `AsyncShmemSize()` (async.c lines 483-495).
pub fn AsyncShmemSize() -> PgResult<Size> {
    let max_backends = init_seams_decls::max_backends::call() as usize;

    let mut size = max_backends
        .checked_mul(core::mem::size_of::<QueueBackendStatus>())
        .ok_or_else(size_overflow)?;
    size = size
        .checked_add(ASYNC_QUEUE_CONTROL_HEADER_SIZE)
        .ok_or_else(size_overflow)?;
    let notify_buffers = notify_buffers();
    let slru = SimpleLruShmemSize(notify_buffers, 0);
    size = size.checked_add(slru).ok_or_else(size_overflow)?;

    Ok(size)
}

/// `notify_buffers` GUC — defaulted via `SimpleLruAutotuneBuffers(16, 1024)` in C
/// guc_tables, mirrored here. Backend-local.
fn notify_buffers() -> i32 {
    slru::SimpleLruAutotuneBuffers(16, 1024)
}

/// `AsyncShmemInit()` (async.c lines 500-548).
pub fn AsyncShmemInit() -> PgResult<()> {
    let max_backends = init_seams_decls::max_backends::call() as usize;

    // Create or attach to the AsyncQueueControl structure. Owned in-crate as the
    // header + per-backend Vec; "first time through" iff not yet initialized.
    let found = ASYNC_QUEUE_CONTROL.with(|c| c.borrow().is_some());

    if !found {
        // First time through, so initialize it (async.c 515-530).
        let mut backend: Vec<QueueBackendStatus> = Vec::new();
        backend
            .try_reserve_exact(max_backends)
            .map_err(|_| oom("AsyncShmemInit"))?;
        for _ in 0..max_backends {
            backend.push(QueueBackendStatus {
                pid: InvalidPid,
                dboid: InvalidOid,
                nextListener: INVALID_PROC_NUMBER,
                pos: QueuePosition { page: 0, offset: 0 },
            });
        }
        let header = AsyncQueueControl {
            head: QueuePosition { page: 0, offset: 0 },
            tail: QueuePosition { page: 0, offset: 0 },
            stopPage: 0,
            firstListener: INVALID_PROC_NUMBER,
            lastQueueFillWarn: 0,
        };
        ASYNC_QUEUE_CONTROL.with(|c| {
            *c.borrow_mut() = Some(AsyncQueueControlData { header, backend });
        });
    }

    // Set up SLRU management of the pg_notify data.
    //   NotifyCtl->PagePrecedes = asyncQueuePagePrecedes;
    //   SimpleLruInit(NotifyCtl, "notify", notify_buffers, 0, "pg_notify",
    //                 LWTRANCHE_NOTIFY_BUFFER, LWTRANCHE_NOTIFY_SLRU,
    //                 SYNC_HANDLER_NONE, true);
    let mut ctl = SimpleLruInit(
        "notify",
        notify_buffers(),
        0,
        "pg_notify",
        types_storage::storage::LWTRANCHE_NOTIFY_BUFFER,
        types_storage::storage::LWTRANCHE_NOTIFY_SLRU,
        SyncRequestHandler::SYNC_HANDLER_NONE,
        true,
    )?;
    ctl.PagePrecedes = Some(asyncQueuePagePrecedes);
    NOTIFY_CTL.with(|c| *c.borrow_mut() = Some(ctl));

    if !found {
        // During start or reboot, clean out the pg_notify directory.
        with_notify_ctl(|ctl| {
            SlruScanDirectory(ctl, |ctl, filename, segpage| {
                SlruScanDirCbDeleteAll(ctl, filename, segpage)
            })
        })?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// SQL-callable entry points (fmgr/Datum value layer is the project-wide deferral)
// ---------------------------------------------------------------------------

/// `pg_notify(PG_FUNCTION_ARGS)` — the fmgr `PG_FUNCTION_ARGS` Datum marshaling
/// (text args -> cstring, `PG_RETURN_VOID`) is the accepted project-wide fmgr
/// value-layer deferral. The file's own logic (the NULL-decision + `Async_Notify`
/// call) is reachable via [`pg_notify_core`].
pub fn pg_notify_core(channel: &str, payload: &str) -> PgResult<()> {
    // For NOTIFY as a statement, this is checked in ProcessUtility.
    backend_tcop_utility_seams::prevent_command_during_recovery::call("NOTIFY")?;

    Async_Notify(channel, Some(payload))
}

/// `pg_listening_channels(PG_FUNCTION_ARGS)` core (async.c lines 788-812). The SRF
/// call-counter / `CStringGetTextDatum` materialization is funcapi-owned (the
/// project-wide fmgr deferral); the file's own logic — the
/// `list_nth(listenChannels, call_cntr)` walk — is `listenChannels` cloned in
/// order, observably identical to the multi-call C iteration.
pub fn pg_listening_channels_rows() -> Vec<String> {
    LISTEN_CHANNELS.with(|lc| lc.borrow().clone())
}

/// `pg_notification_queue_usage(PG_FUNCTION_ARGS)` core (async.c lines 1480-1493).
/// Returns the `float8` usage; the `PG_RETURN_FLOAT8` Datum encoding is the fmgr
/// value layer.
pub fn pg_notification_queue_usage_core() -> PgResult<f64> {
    // Advance the queue tail so we don't report a too-large result.
    asyncQueueAdvanceTail()?;

    LWLockAcquire(notify_queue_lock(), LW_SHARED, my_proc_number())?;
    let usage = asyncQueueUsage();
    LWLockRelease(notify_queue_lock())?;

    Ok(usage)
}

// ---------------------------------------------------------------------------
// NOTIFY / LISTEN / UNLISTEN statement entry points (async.c lines 589-779)
// ---------------------------------------------------------------------------

/// `Async_Notify(channel, payload)` (async.c lines 589-678).
pub fn Async_Notify(channel: &str, payload: Option<&str>) -> PgResult<()> {
    let my_level = xact_seams::get_current_transaction_nest_level::call();

    if parallel_seams::is_parallel_worker() {
        return Err(elog_error("cannot send notifications from a parallel worker"));
    }

    if trace_notify_enabled() {
        ereport(DEBUG1)
            .errmsg_internal(format!("Async_Notify({channel})"))
            .finish(here())?;
    }

    let channel_len = channel.len();
    let payload_len = payload.map_or(0, |p| p.len());

    if channel_len == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("channel name cannot be empty")
            .into_error());
    }

    if channel_len >= NAMEDATALEN {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("channel name too long")
            .into_error());
    }

    if payload_len >= NOTIFY_PAYLOAD_MAX_LENGTH {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("payload string too long")
            .into_error());
    }

    // We must construct the Notification entry, even if we end up not using it.
    //   n = palloc(offsetof(Notification, data) + channel_len + payload_len + 2);
    //   strcpy(n->data, channel); strcpy(n->data + channel_len + 1, payload);
    let total = channel_len + payload_len + 2;
    let mut data: Vec<u8> = Vec::new();
    data.try_reserve_exact(total).map_err(|_| oom("Async_Notify"))?;
    data.resize(total, 0u8);
    data[..channel_len].copy_from_slice(channel.as_bytes());
    data[channel_len] = 0;
    if let Some(p) = payload {
        data[channel_len + 1..channel_len + 1 + payload_len].copy_from_slice(p.as_bytes());
    }
    data[channel_len + 1 + payload_len] = 0;
    let n = Notification {
        channel_len: channel_len as u16,
        payload_len: payload_len as u16,
        data,
    };

    let need_new = PENDING_NOTIFIES.with(|pn| {
        let pn = pn.borrow();
        pn.is_none() || my_level > pn.as_ref().unwrap().nestingLevel
    });

    if need_new {
        // First notify event in current (sub)xact.
        PENDING_NOTIFIES.with(|pn| {
            let upper = pn.borrow_mut().take();
            let notifies = NotificationList {
                nestingLevel: my_level,
                events: vec![n],
                hashtab: None,
                upper,
            };
            *pn.borrow_mut() = Some(Box::new(notifies));
        });
    } else {
        if AsyncExistsPendingNotify(&n) {
            // It's a dup, so forget it (pfree(n); return;).
            return Ok(());
        }
        AddEventToPendingNotifies(n)?;
    }

    Ok(())
}

/// `queue_listen(action, channel)` (async.c lines 688-729).
fn queue_listen(action: ListenActionKind, channel: &str) -> PgResult<()> {
    let my_level = xact_seams::get_current_transaction_nest_level::call();

    let actrec = ListenAction {
        action,
        channel: channel.to_string(),
    };

    let need_new = PENDING_ACTIONS.with(|pa| {
        let pa = pa.borrow();
        pa.is_none() || my_level > pa.as_ref().unwrap().nestingLevel
    });

    if need_new {
        PENDING_ACTIONS.with(|pa| {
            let upper = pa.borrow_mut().take();
            let actions = ActionList {
                nestingLevel: my_level,
                actions: vec![actrec],
                upper,
            };
            *pa.borrow_mut() = Some(Box::new(actions));
        });
    } else {
        PENDING_ACTIONS.with(|pa| {
            pa.borrow_mut().as_mut().unwrap().actions.push(actrec);
        });
    }

    Ok(())
}

/// `Async_Listen(channel)` (async.c lines 736-743).
pub fn Async_Listen(channel: &str) -> PgResult<()> {
    if trace_notify_enabled() {
        let pid = my_proc_pid();
        ereport(DEBUG1)
            .errmsg_internal(format!("Async_Listen({channel},{pid})"))
            .finish(here())?;
    }

    queue_listen(ListenActionKind::LISTEN_LISTEN, channel)
}

/// `Async_Unlisten(channel)` (async.c lines 750-761).
pub fn Async_Unlisten(channel: &str) -> PgResult<()> {
    if trace_notify_enabled() {
        let pid = my_proc_pid();
        ereport(DEBUG1)
            .errmsg_internal(format!("Async_Unlisten({channel},{pid})"))
            .finish(here())?;
    }

    // If we couldn't possibly be listening, no need to queue anything.
    let no_pending = PENDING_ACTIONS.with(|pa| pa.borrow().is_none());
    if no_pending && !UNLISTEN_EXIT_REGISTERED.with(|f| f.get()) {
        return Ok(());
    }

    queue_listen(ListenActionKind::LISTEN_UNLISTEN, channel)
}

/// `Async_UnlistenAll()` (async.c lines 768-779).
pub fn Async_UnlistenAll() -> PgResult<()> {
    if trace_notify_enabled() {
        let pid = my_proc_pid();
        ereport(DEBUG1)
            .errmsg_internal(format!("Async_UnlistenAll({pid})"))
            .finish(here())?;
    }

    let no_pending = PENDING_ACTIONS.with(|pa| pa.borrow().is_none());
    if no_pending && !UNLISTEN_EXIT_REGISTERED.with(|f| f.get()) {
        return Ok(());
    }

    queue_listen(ListenActionKind::LISTEN_UNLISTEN_ALL, "")
}

/// `Async_UnlistenOnExit(code, arg)` — `before_shmem_exit` callback
/// (async.c lines 821-826). The `pg_on_exit_callback`-shaped signature is what
/// the ipc.c registry installs; the failure surface is carried on `PgResult`.
pub fn Async_UnlistenOnExit(
    _code: i32,
    _arg: types_tuple::Datum<'static>,
) -> PgResult<()> {
    Exec_UnlistenAllCommit()?;
    asyncQueueUnregister()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Transaction lifecycle hooks (async.c lines 834-1032, 1670-1793)
// ---------------------------------------------------------------------------

/// `AtPrepare_Notify()` (async.c lines 834-842).
pub fn AtPrepare_Notify() -> PgResult<()> {
    let have_actions = PENDING_ACTIONS.with(|pa| pa.borrow().is_some());
    let have_notifies = PENDING_NOTIFIES.with(|pn| pn.borrow().is_some());
    if have_actions || have_notifies {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot PREPARE a transaction that has executed LISTEN, UNLISTEN, or NOTIFY")
            .into_error());
    }
    Ok(())
}

/// `PreCommit_Notify()` (async.c lines 859-952).
pub fn PreCommit_Notify() -> PgResult<()> {
    let have_actions = PENDING_ACTIONS.with(|pa| pa.borrow().is_some());
    let have_notifies = PENDING_NOTIFIES.with(|pn| pn.borrow().is_some());
    if !have_actions && !have_notifies {
        return Ok(());
    }

    if trace_notify_enabled() {
        ereport(DEBUG1)
            .errmsg_internal("PreCommit_Notify")
            .finish(here())?;
    }

    // Preflight for any pending listen/unlisten actions.
    if have_actions {
        let n = PENDING_ACTIONS.with(|pa| pa.borrow().as_ref().unwrap().actions.len());
        for idx in 0..n {
            let action =
                PENDING_ACTIONS.with(|pa| pa.borrow().as_ref().unwrap().actions[idx].action);
            match action {
                ListenActionKind::LISTEN_LISTEN => Exec_ListenPreCommit()?,
                // there is no Exec_UnlistenPreCommit()
                ListenActionKind::LISTEN_UNLISTEN => {}
                // there is no Exec_UnlistenAllPreCommit()
                ListenActionKind::LISTEN_UNLISTEN_ALL => {}
            }
        }
    }

    // Queue any pending notifies (must happen after the above).
    if have_notifies {
        // Make sure that we have an XID assigned to the current transaction.
        let _ = xact_seams::get_current_transaction_id::call()?;

        // Serialize writers by acquiring the "database 0" heavyweight lock.
        //   LockSharedObject(DatabaseRelationId, InvalidOid, 0, AccessExclusiveLock)
        lmgr_seams::lock_shared_object::call(
            types_async::DatabaseRelationId,
            InvalidOid,
            0,
            types_storage::lock::AccessExclusiveLock,
        )?;

        let mut next_notify: Option<usize> = {
            let len = PENDING_NOTIFIES.with(|pn| pn.borrow().as_ref().unwrap().events.len());
            if len == 0 {
                None
            } else {
                Some(0)
            }
        };

        while next_notify.is_some() {
            LWLockAcquire(notify_queue_lock(), LW_EXCLUSIVE, my_proc_number())?;
            asyncQueueFillWarning()?;
            if asyncQueueIsFull() {
                let _ = LWLockRelease(notify_queue_lock());
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                    .errmsg("too many notifications in the NOTIFY queue")
                    .into_error());
            }
            match asyncQueueAddEntries(next_notify) {
                Ok(nn) => {
                    next_notify = nn;
                    LWLockRelease(notify_queue_lock())?;
                }
                Err(e) => {
                    let _ = LWLockRelease(notify_queue_lock());
                    return Err(e);
                }
            }
        }

        // Note that we don't clear pendingNotifies; AtCommit_Notify will.
    }

    Ok(())
}

/// `AtCommit_Notify()` (async.c lines 966-1032).
pub fn AtCommit_Notify() -> PgResult<()> {
    let have_actions = PENDING_ACTIONS.with(|pa| pa.borrow().is_some());
    let have_notifies = PENDING_NOTIFIES.with(|pn| pn.borrow().is_some());
    if !have_actions && !have_notifies {
        return Ok(());
    }

    if trace_notify_enabled() {
        ereport(DEBUG1)
            .errmsg_internal("AtCommit_Notify")
            .finish(here())?;
    }

    if have_actions {
        let n = PENDING_ACTIONS.with(|pa| pa.borrow().as_ref().unwrap().actions.len());
        for idx in 0..n {
            let (action, channel) = PENDING_ACTIONS.with(|pa| {
                let pa = pa.borrow();
                let a = &pa.as_ref().unwrap().actions[idx];
                (a.action, a.channel.clone())
            });
            match action {
                ListenActionKind::LISTEN_LISTEN => Exec_ListenCommit(&channel)?,
                ListenActionKind::LISTEN_UNLISTEN => Exec_UnlistenCommit(&channel)?,
                ListenActionKind::LISTEN_UNLISTEN_ALL => Exec_UnlistenAllCommit()?,
            }
        }
    }

    // If no longer listening to anything, get out of listener array.
    if AM_REGISTERED_LISTENER.with(|f| f.get()) && listen_channels_is_empty() {
        asyncQueueUnregister()?;
    }

    // Send signals to listening backends (only if there are pending notifies).
    if have_notifies {
        SignalBackends()?;
    }

    // If it's time to try to advance the global tail pointer, do that.
    if TRY_ADVANCE_TAIL.with(|f| f.get()) {
        TRY_ADVANCE_TAIL.with(|f| f.set(false));
        asyncQueueAdvanceTail()?;
    }

    ClearPendingActionsAndNotifies();

    Ok(())
}

// ---------------------------------------------------------------------------
// LISTEN/UNLISTEN commit subroutines (async.c lines 1039-1223)
// ---------------------------------------------------------------------------

/// `Exec_ListenPreCommit()` (async.c lines 1039-1127).
fn Exec_ListenPreCommit() -> PgResult<()> {
    if AM_REGISTERED_LISTENER.with(|f| f.get()) {
        return Ok(());
    }

    if trace_notify_enabled() {
        let pid = my_proc_pid();
        ereport(DEBUG1)
            .errmsg_internal(format!("Exec_ListenPreCommit({pid})"))
            .finish(here())?;
    }

    // Before registering, make sure we will unlisten before dying.
    if !UNLISTEN_EXIT_REGISTERED.with(|f| f.get()) {
        ipc_seams::before_shmem_exit::call(async_unlisten_on_exit_cb, datum_zero())?;
        UNLISTEN_EXIT_REGISTERED.with(|f| f.set(true));
    }

    let my_proc_number = my_proc_number();
    let my_database_id = my_database_id();
    let my_proc_pid = my_proc_pid();

    LWLockAcquire(notify_queue_lock(), LW_EXCLUSIVE, my_proc_number)?;
    let head = QUEUE_HEAD();
    let mut max = QUEUE_TAIL();
    let mut prev_listener = INVALID_PROC_NUMBER;
    let mut i = QUEUE_FIRST_LISTENER();
    while i != INVALID_PROC_NUMBER {
        if QUEUE_BACKEND_DBOID(i) == my_database_id {
            max = QUEUE_POS_MAX(max, QUEUE_BACKEND_POS(i));
        }
        // Also find last listening backend before this one.
        if i < my_proc_number {
            prev_listener = i;
        }
        i = QUEUE_NEXT_LISTENER(i);
    }
    set_QUEUE_BACKEND_POS(my_proc_number, max);
    set_QUEUE_BACKEND_PID(my_proc_number, my_proc_pid);
    set_QUEUE_BACKEND_DBOID(my_proc_number, my_database_id);
    // Insert backend into list of listeners at correct position.
    if prev_listener != INVALID_PROC_NUMBER {
        set_QUEUE_NEXT_LISTENER(my_proc_number, QUEUE_NEXT_LISTENER(prev_listener));
        set_QUEUE_NEXT_LISTENER(prev_listener, my_proc_number);
    } else {
        set_QUEUE_NEXT_LISTENER(my_proc_number, QUEUE_FIRST_LISTENER());
        set_QUEUE_FIRST_LISTENER(my_proc_number);
    }
    LWLockRelease(notify_queue_lock())?;

    AM_REGISTERED_LISTENER.with(|f| f.set(true));

    // Try to move our pointer forward as far as possible.
    if !QUEUE_POS_EQUAL(max, head) {
        asyncQueueReadAllNotifications()?;
    }

    Ok(())
}

/// `Exec_ListenCommit(channel)` (async.c lines 1134-1154).
fn Exec_ListenCommit(channel: &str) -> PgResult<()> {
    if IsListeningOn(channel) {
        return Ok(());
    }

    LISTEN_CHANNELS.with(|lc| {
        let mut v = lc.borrow_mut();
        v.try_reserve(1).map_err(|_| oom("Exec_ListenCommit"))?;
        v.push(channel.to_string());
        Ok(())
    })
}

/// `Exec_UnlistenCommit(channel)` (async.c lines 1161-1185).
fn Exec_UnlistenCommit(channel: &str) -> PgResult<()> {
    if trace_notify_enabled() {
        let pid = my_proc_pid();
        ereport(DEBUG1)
            .errmsg_internal(format!("Exec_UnlistenCommit({channel},{pid})"))
            .finish(here())?;
    }

    LISTEN_CHANNELS.with(|lc| {
        let mut v = lc.borrow_mut();
        if let Some(pos) = v.iter().position(|c| c == channel) {
            v.remove(pos);
        }
    });
    // We do not complain about unlistening something not being listened.
    Ok(())
}

/// `Exec_UnlistenAllCommit()` (async.c lines 1192-1200).
fn Exec_UnlistenAllCommit() -> PgResult<()> {
    if trace_notify_enabled() {
        let pid = my_proc_pid();
        ereport(DEBUG1)
            .errmsg_internal(format!("Exec_UnlistenAllCommit({pid})"))
            .finish(here())?;
    }

    LISTEN_CHANNELS.with(|lc| lc.borrow_mut().clear());
    Ok(())
}

/// `IsListeningOn(channel)` (async.c lines 1210-1223).
fn IsListeningOn(channel: &str) -> bool {
    LISTEN_CHANNELS.with(|lc| lc.borrow().iter().any(|lchan| lchan == channel))
}

#[inline]
fn listen_channels_is_empty() -> bool {
    LISTEN_CHANNELS.with(|lc| lc.borrow().is_empty())
}

/// `asyncQueueUnregister()` (async.c lines 1229-1263).
fn asyncQueueUnregister() -> PgResult<()> {
    debug_assert!(listen_channels_is_empty());

    if !AM_REGISTERED_LISTENER.with(|f| f.get()) {
        return Ok(());
    }

    let my_proc_number = my_proc_number();

    LWLockAcquire(notify_queue_lock(), LW_EXCLUSIVE, my_proc_number)?;
    set_QUEUE_BACKEND_PID(my_proc_number, InvalidPid);
    set_QUEUE_BACKEND_DBOID(my_proc_number, InvalidOid);
    if QUEUE_FIRST_LISTENER() == my_proc_number {
        set_QUEUE_FIRST_LISTENER(QUEUE_NEXT_LISTENER(my_proc_number));
    } else {
        let mut i = QUEUE_FIRST_LISTENER();
        while i != INVALID_PROC_NUMBER {
            if QUEUE_NEXT_LISTENER(i) == my_proc_number {
                set_QUEUE_NEXT_LISTENER(i, QUEUE_NEXT_LISTENER(my_proc_number));
                break;
            }
            i = QUEUE_NEXT_LISTENER(i);
        }
    }
    set_QUEUE_NEXT_LISTENER(my_proc_number, INVALID_PROC_NUMBER);
    LWLockRelease(notify_queue_lock())?;

    AM_REGISTERED_LISTENER.with(|f| f.set(false));
    Ok(())
}

// ---------------------------------------------------------------------------
// Queue write path (async.c lines 1270-1474)
// ---------------------------------------------------------------------------

/// `asyncQueueIsFull()` (async.c lines 1270-1278).
fn asyncQueueIsFull() -> bool {
    let head_page = QUEUE_POS_PAGE(QUEUE_HEAD());
    let tail_page = QUEUE_POS_PAGE(QUEUE_TAIL());
    let occupied = head_page - tail_page;
    occupied >= MAX_NOTIFY_QUEUE_PAGES.with(|c| c.get()) as i64
}

/// `asyncQueueAdvance(position, entryLength)` (async.c lines 1285-1313).
fn asyncQueueAdvance(position: &mut QueuePosition, entryLength: i32) -> bool {
    let mut pageno = QUEUE_POS_PAGE(*position);
    let mut offset = QUEUE_POS_OFFSET(*position);
    let mut page_jump = false;

    offset += entryLength;
    debug_assert!(offset as usize <= QUEUE_PAGESIZE);

    if offset as usize + QUEUEALIGN(AsyncQueueEntryEmptySize) > QUEUE_PAGESIZE {
        pageno += 1;
        offset = 0;
        page_jump = true;
    }

    SET_QUEUE_POS(position, pageno, offset);
    page_jump
}

/// `asyncQueueNotificationToEntry(n, qe)` (async.c lines 1318-1336).
fn asyncQueueNotificationToEntry(n: &Notification, qe: &mut AsyncQueueEntry) -> PgResult<()> {
    let channellen = n.channel_len as usize;
    let payloadlen = n.payload_len as usize;

    debug_assert!(channellen < NAMEDATALEN);
    debug_assert!(payloadlen < NOTIFY_PAYLOAD_MAX_LENGTH);

    let mut entry_length = AsyncQueueEntryEmptySize + payloadlen + channellen;
    entry_length = QUEUEALIGN(entry_length);
    qe.length = entry_length as i32;
    qe.dboid = my_database_id();
    qe.xid = xact_seams::get_current_transaction_id::call()?;
    qe.srcPid = my_proc_pid();
    let copy = channellen + payloadlen + 2;
    qe.data[..copy].copy_from_slice(&n.data[..copy]);
    Ok(())
}

/// `asyncQueueAddEntries(nextNotify)` (async.c lines 1354-1474). `NotifyQueueLock`
/// is held by the caller; SLRU bank locks are taken locally.
fn asyncQueueAddEntries(nextNotify: Option<usize>) -> PgResult<Option<usize>> {
    let mut next_notify = nextNotify;

    let mut queue_head = QUEUE_HEAD();

    let mut pageno = QUEUE_POS_PAGE(queue_head);
    // Acquire the SLRU bank lock for the current page, exclusive.
    bank_lock_acquire_exclusive(pageno)?;
    let mut prev_bankno = bank_number(pageno);

    let mut slotno = if QUEUE_POS_IS_ZERO(queue_head) {
        with_notify_ctl(|ctl| SimpleLruZeroPage(ctl, pageno))?
    } else {
        with_notify_ctl(|ctl| SimpleLruReadPage(ctl, pageno, true, InvalidTransactionId))?
    };

    // Note we mark the page dirty before writing in it.
    set_page_dirty(slotno);

    let n_events = PENDING_NOTIFIES.with(|pn| pn.borrow().as_ref().unwrap().events.len());

    while let Some(cur) = next_notify {
        let mut qe = blank_entry();
        let n_clone =
            PENDING_NOTIFIES.with(|pn| pn.borrow().as_ref().unwrap().events[cur].clone());
        asyncQueueNotificationToEntry(&n_clone, &mut qe)?;

        let offset = QUEUE_POS_OFFSET(queue_head);

        if offset as usize + qe.length as usize <= QUEUE_PAGESIZE {
            next_notify = if cur + 1 < n_events { Some(cur + 1) } else { None };
        } else {
            // Write a dummy entry to fill up the page.
            qe.length = QUEUE_PAGESIZE as i32 - offset;
            qe.dboid = InvalidOid;
            qe.xid = InvalidTransactionId;
            qe.data[0] = 0;
            qe.data[1] = 0;
        }

        // memcpy(page_buffer[slotno] + offset, &qe, qe.length);
        let bytes = entry_to_bytes(&qe);
        write_page_buffer(slotno, offset as usize, &bytes[..qe.length as usize]);

        if asyncQueueAdvance(&mut queue_head, qe.length) {
            pageno = QUEUE_POS_PAGE(queue_head);
            let bankno = bank_number(pageno);
            if bankno != prev_bankno {
                bank_lock_release(prev_bankno)?;
                bank_lock_acquire_exclusive(pageno)?;
                prev_bankno = bankno;
            }

            // Page is full; fill the next page with zeroes.
            slotno = with_notify_ctl(|ctl| SimpleLruZeroPage(ctl, QUEUE_POS_PAGE(queue_head)))?;
            let _ = slotno;

            if QUEUE_POS_PAGE(queue_head) % QUEUE_CLEANUP_DELAY == 0 {
                TRY_ADVANCE_TAIL.with(|f| f.set(true));
            }

            break;
        }
    }

    set_QUEUE_HEAD(queue_head);

    bank_lock_release(prev_bankno)?;

    Ok(next_notify)
}

// ---------------------------------------------------------------------------
// Usage / fill warning (async.c lines 1505-1563)
// ---------------------------------------------------------------------------

/// `asyncQueueUsage()` (async.c lines 1505-1516).
fn asyncQueueUsage() -> f64 {
    let head_page = QUEUE_POS_PAGE(QUEUE_HEAD());
    let tail_page = QUEUE_POS_PAGE(QUEUE_TAIL());
    let occupied = head_page - tail_page;

    if occupied == 0 {
        return 0.0;
    }

    occupied as f64 / MAX_NOTIFY_QUEUE_PAGES.with(|c| c.get()) as f64
}

/// `asyncQueueFillWarning()` (async.c lines 1526-1563).
fn asyncQueueFillWarning() -> PgResult<()> {
    let fill_degree = asyncQueueUsage();
    if fill_degree < 0.5 {
        return Ok(());
    }

    let t = timestamp_seams::get_current_timestamp::call();

    if timestamp_seams::timestamp_difference_exceeds::call(
        QUEUE_LAST_FILL_WARN(),
        t,
        QUEUE_FULL_WARN_INTERVAL,
    ) {
        let mut min = QUEUE_HEAD();
        let mut min_pid = InvalidPid;

        let mut i = QUEUE_FIRST_LISTENER();
        while i != INVALID_PROC_NUMBER {
            debug_assert!(QUEUE_BACKEND_PID(i) != InvalidPid);
            min = QUEUE_POS_MIN(min, QUEUE_BACKEND_POS(i));
            if QUEUE_POS_EQUAL(min, QUEUE_BACKEND_POS(i)) {
                min_pid = QUEUE_BACKEND_PID(i);
            }
            i = QUEUE_NEXT_LISTENER(i);
        }

        let mut builder =
            ereport(WARNING).errmsg(format!("NOTIFY queue is {:.0}% full", fill_degree * 100.0));
        if min_pid != InvalidPid {
            builder = builder
                .errdetail(format!(
                    "The server process with PID {min_pid} is among those with the oldest transactions."
                ))
                .errhint(
                    "The NOTIFY queue cannot be emptied until that process ends its current transaction.",
                );
        }
        builder.finish(here())?;

        set_QUEUE_LAST_FILL_WARN(t);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Signaling (async.c lines 1580-1660)
// ---------------------------------------------------------------------------

/// `SignalBackends()` (async.c lines 1580-1660).
fn SignalBackends() -> PgResult<()> {
    let max_backends = init_seams_decls::max_backends::call() as usize;
    let my_database_id = my_database_id();
    let my_proc_pid = my_proc_pid();

    let mut pids: Vec<i32> = Vec::new();
    pids.try_reserve_exact(max_backends)
        .map_err(|_| oom("SignalBackends"))?;
    let mut procnos: Vec<ProcNumber> = Vec::new();
    procnos
        .try_reserve_exact(max_backends)
        .map_err(|_| oom("SignalBackends"))?;

    LWLockAcquire(notify_queue_lock(), LW_EXCLUSIVE, my_proc_number())?;
    let head = QUEUE_HEAD();
    let mut i = QUEUE_FIRST_LISTENER();
    while i != INVALID_PROC_NUMBER {
        let pid = QUEUE_BACKEND_PID(i);
        debug_assert!(pid != InvalidPid);
        let pos = QUEUE_BACKEND_POS(i);
        if QUEUE_BACKEND_DBOID(i) == my_database_id {
            if QUEUE_POS_EQUAL(pos, head) {
                i = QUEUE_NEXT_LISTENER(i);
                continue;
            }
        } else if asyncQueuePageDiff(QUEUE_POS_PAGE(head), QUEUE_POS_PAGE(pos)) < QUEUE_CLEANUP_DELAY
        {
            i = QUEUE_NEXT_LISTENER(i);
            continue;
        }
        pids.push(pid);
        procnos.push(i);
        i = QUEUE_NEXT_LISTENER(i);
    }
    LWLockRelease(notify_queue_lock())?;

    for k in 0..pids.len() {
        let pid = pids[k];

        // If we are signaling our own process, set the flag directly.
        if pid == my_proc_pid {
            NOTIFY_INTERRUPT_PENDING.with(|f| f.set(true));
            continue;
        }

        // SendProcSignal(pid, PROCSIG_NOTIFY_INTERRUPT, procnos[i]) < 0 => DEBUG3
        if procsignal_seams::send_proc_signal::call(
            pid,
            types_storage::ProcSignalReason::PROCSIG_NOTIFY_INTERRUPT,
            procnos[k],
        ) < 0
        {
            ereport(DEBUG3)
                .errmsg_internal(format!("could not signal backend with PID {pid}"))
                .finish(here())?;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// AtAbort / subtransaction hooks (async.c lines 1670-1793)
// ---------------------------------------------------------------------------

/// `AtAbort_Notify()` (async.c lines 1670-1683).
pub fn AtAbort_Notify() -> PgResult<()> {
    if AM_REGISTERED_LISTENER.with(|f| f.get()) && listen_channels_is_empty() {
        asyncQueueUnregister()?;
    }
    ClearPendingActionsAndNotifies();
    Ok(())
}

/// `AtSubCommit_Notify()` (async.c lines 1690-1755).
pub fn AtSubCommit_Notify() -> PgResult<()> {
    let my_level = xact_seams::get_current_transaction_nest_level::call();

    let reparent_actions = PENDING_ACTIONS.with(|pa| {
        pa.borrow().as_ref().is_some_and(|a| a.nestingLevel >= my_level)
    });
    if reparent_actions {
        let (upper_is_none, upper_level_lt) = PENDING_ACTIONS.with(|pa| {
            let pa = pa.borrow();
            let a = pa.as_ref().unwrap();
            match a.upper.as_ref() {
                None => (true, false),
                Some(u) => (false, u.nestingLevel < my_level - 1),
            }
        });
        if upper_is_none || upper_level_lt {
            PENDING_ACTIONS.with(|pa| {
                pa.borrow_mut().as_mut().unwrap().nestingLevel -= 1;
            });
        } else {
            PENDING_ACTIONS.with(|pa| {
                let mut child = pa.borrow_mut().take().unwrap();
                *pa.borrow_mut() = child.upper.take();
                let mut binding = pa.borrow_mut();
                let parent = binding.as_mut().unwrap();
                parent.actions.append(&mut child.actions);
            });
        }
    }

    let reparent_notifies = PENDING_NOTIFIES.with(|pn| {
        pn.borrow().as_ref().is_some_and(|n| n.nestingLevel >= my_level)
    });
    if reparent_notifies {
        debug_assert_eq!(
            PENDING_NOTIFIES.with(|pn| pn.borrow().as_ref().unwrap().nestingLevel),
            my_level
        );

        let (upper_is_none, upper_level_lt) = PENDING_NOTIFIES.with(|pn| {
            let pn = pn.borrow();
            let n = pn.as_ref().unwrap();
            match n.upper.as_ref() {
                None => (true, false),
                Some(u) => (false, u.nestingLevel < my_level - 1),
            }
        });
        if upper_is_none || upper_level_lt {
            PENDING_NOTIFIES.with(|pn| {
                pn.borrow_mut().as_mut().unwrap().nestingLevel -= 1;
            });
        } else {
            let child_events = PENDING_NOTIFIES.with(|pn| {
                let mut child = pn.borrow_mut().take().unwrap();
                *pn.borrow_mut() = child.upper.take();
                core::mem::take(&mut child.events)
            });
            for childn in child_events {
                if !AsyncExistsPendingNotify(&childn) {
                    AddEventToPendingNotifies(childn)?;
                }
            }
        }
    }

    Ok(())
}

/// `AtSubAbort_Notify()` (async.c lines 1760-1793). Void in C; only reads the
/// current nesting level and pops the pending stacks (infallible).
pub fn AtSubAbort_Notify() {
    let my_level = xact_seams::get_current_transaction_nest_level::call();

    loop {
        let pop = PENDING_ACTIONS.with(|pa| {
            pa.borrow().as_ref().is_some_and(|a| a.nestingLevel >= my_level)
        });
        if !pop {
            break;
        }
        PENDING_ACTIONS.with(|pa| {
            let mut child = pa.borrow_mut().take().unwrap();
            *pa.borrow_mut() = child.upper.take();
        });
    }

    loop {
        let pop = PENDING_NOTIFIES.with(|pn| {
            pn.borrow().as_ref().is_some_and(|n| n.nestingLevel >= my_level)
        });
        if !pop {
            break;
        }
        PENDING_NOTIFIES.with(|pn| {
            let mut child = pn.borrow_mut().take().unwrap();
            *pn.borrow_mut() = child.upper.take();
        });
    }
}

// ---------------------------------------------------------------------------
// Interrupt handling (async.c lines 1803-1842)
// ---------------------------------------------------------------------------

/// `HandleNotifyInterrupt()` (async.c lines 1803-1816). Called by a SIGNAL HANDLER.
pub fn HandleNotifyInterrupt() {
    // signal that work needs to be done
    NOTIFY_INTERRUPT_PENDING.with(|f| f.set(true));

    // make sure the event is processed in due course
    latch_seams::set_latch_my_latch::call();
}

/// `ProcessNotifyInterrupt(flush)` (async.c lines 1833-1842).
pub fn ProcessNotifyInterrupt(flush: bool) -> PgResult<()> {
    if xact_seams::is_transaction_or_transaction_block::call() {
        return Ok(());
    }

    // Loop in case another signal arrives while sending messages.
    while NOTIFY_INTERRUPT_PENDING.with(|f| f.get()) {
        ProcessIncomingNotify(flush)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Queue read path (async.c lines 1850-2108)
// ---------------------------------------------------------------------------

/// `asyncQueueReadAllNotifications()` (async.c lines 1850-1965).
fn asyncQueueReadAllNotifications() -> PgResult<()> {
    let my_proc_number = my_proc_number();

    LWLockAcquire(notify_queue_lock(), LW_SHARED, my_proc_number)?;
    debug_assert_eq!(my_proc_pid(), QUEUE_BACKEND_PID(my_proc_number));
    let mut pos = QUEUE_BACKEND_POS(my_proc_number);
    let head = QUEUE_HEAD();
    LWLockRelease(notify_queue_lock())?;

    if QUEUE_POS_EQUAL(pos, head) {
        return Ok(());
    }

    // Get snapshot we'll use to decide which xacts are still in progress.
    let snapshot = snapmgr_seams::register_snapshot::call(snapmgr_seams::get_latest_snapshot::call()?)?;

    // Upgrade any ERRORs to FATAL so we don't resend the same message on failure.
    let save_exit_on_any_error = init_seams_decls::exit_on_any_error::call();
    init_seams_decls::set_exit_on_any_error::call(true);

    let inner = (|| -> PgResult<()> {
        loop {
            let reached_stop = asyncQueueProcessPageEntries(&mut pos, head, &snapshot)?;
            if reached_stop {
                break;
            }
        }

        LWLockAcquire(notify_queue_lock(), LW_SHARED, my_proc_number)?;
        set_QUEUE_BACKEND_POS(my_proc_number, pos);
        LWLockRelease(notify_queue_lock())?;
        Ok(())
    })();

    init_seams_decls::set_exit_on_any_error::call(save_exit_on_any_error);

    inner?;

    snapmgr_seams::unregister_snapshot::call(snapshot);
    Ok(())
}

/// `asyncQueueProcessPageEntries(current, stop, snapshot)` (async.c lines 1976-2108).
fn asyncQueueProcessPageEntries(
    current: &mut QueuePosition,
    stop: QueuePosition,
    snapshot: &types_snapshot::SnapshotData,
) -> PgResult<bool> {
    let curpage = QUEUE_POS_PAGE(*current);
    let mut reached_stop = false;
    let mut reached_end_of_page;

    let my_database_id = my_database_id();

    let mut local_buf = vec![0u8; QUEUE_PAGESIZE];
    let mut local_buf_end: usize = 0;

    // slotno = SimpleLruReadPage_ReadOnly(NotifyCtl, curpage, ...); leaves the
    // bank lock held. Copy the whole page out for entry parsing, then release.
    let slotno = with_notify_ctl(|ctl| {
        SimpleLruReadPage_ReadOnly(ctl, curpage, InvalidTransactionId)
    })?;
    let page_buffer =
        with_notify_ctl(|ctl| ctl.shared.page_buffer(slotno).to_vec());

    loop {
        let thisentry = *current;

        if QUEUE_POS_EQUAL(thisentry, stop) {
            break;
        }

        let qoff = QUEUE_POS_OFFSET(thisentry) as usize;
        let qe_length = read_i32(&page_buffer, qoff);
        let qe_dboid = read_u32(&page_buffer, qoff + 4) as Oid;
        let qe_xid = read_u32(&page_buffer, qoff + 8) as TransactionId;

        // Advance *current over this message, possibly to the next page.
        reached_end_of_page = asyncQueueAdvance(current, qe_length);

        if qe_dboid == my_database_id {
            if snapmgr_seams::xid_in_mvcc_snapshot::call(qe_xid, snapshot)? {
                // Source transaction still in progress; back up and stop.
                *current = thisentry;
                reached_stop = true;
                break;
            }

            // Quick check: if not listening on any channels, skip.
            if listen_channels_is_empty() {
                if reached_end_of_page {
                    break;
                }
                continue;
            }

            if transam_seams::transaction_id_did_commit::call(
                qe_xid,
                snapmgr_pc_seams::transaction_xmin::call()?,
            )? {
                let len = qe_length as usize;
                local_buf[local_buf_end..local_buf_end + len]
                    .copy_from_slice(&page_buffer[qoff..qoff + len]);
                local_buf_end += len;
            } else {
                // Source transaction aborted or crashed; ignore.
            }
        }

        if reached_end_of_page {
            break;
        }
    }

    // Release the bank lock we got from SimpleLruReadPage_ReadOnly().
    bank_lock_release(bank_number(curpage))?;

    debug_assert!(local_buf_end <= QUEUE_PAGESIZE);
    let mut p = 0usize;
    while p < local_buf_end {
        let qe_length = read_i32(&local_buf, p) as usize;
        // qe->data is the null-terminated channel name (offset 16).
        let data_off = p + 16;
        let channel = cstr_from(&local_buf, data_off);

        if IsListeningOn(&channel) {
            let payload_off = data_off + channel.len() + 1;
            let payload = cstr_from(&local_buf, payload_off);
            let src_pid = read_i32(&local_buf, p + 12);
            NotifyMyFrontEnd(&channel, &payload, src_pid)?;
        }

        p += qe_length;
    }

    if QUEUE_POS_EQUAL(*current, stop) {
        reached_stop = true;
    }

    Ok(reached_stop)
}

// ---------------------------------------------------------------------------
// Tail advance / truncation (async.c lines 2117-2179)
// ---------------------------------------------------------------------------

/// `asyncQueueAdvanceTail()` (async.c lines 2117-2179).
fn asyncQueueAdvanceTail() -> PgResult<()> {
    let mpn = my_proc_number();

    // Restrict task to one backend per cluster.
    LWLockAcquire(notify_queue_tail_lock(), LW_EXCLUSIVE, mpn)?;

    // Compute the new tail.
    LWLockAcquire(notify_queue_lock(), LW_EXCLUSIVE, mpn)?;
    let mut min = QUEUE_HEAD();
    let mut i = QUEUE_FIRST_LISTENER();
    while i != INVALID_PROC_NUMBER {
        debug_assert!(QUEUE_BACKEND_PID(i) != InvalidPid);
        min = QUEUE_POS_MIN(min, QUEUE_BACKEND_POS(i));
        i = QUEUE_NEXT_LISTENER(i);
    }
    set_QUEUE_TAIL(min);
    let oldtailpage = QUEUE_STOP_PAGE();
    LWLockRelease(notify_queue_lock())?;

    let newtailpage = QUEUE_POS_PAGE(min);
    let boundary = newtailpage - (newtailpage % SLRU_PAGES_PER_SEGMENT);
    if asyncQueuePagePrecedes(oldtailpage, boundary) {
        // SimpleLruTruncate() will ask for SLRU bank locks but release them too.
        with_notify_ctl(|ctl| SimpleLruTruncate(ctl, newtailpage))?;

        LWLockAcquire(notify_queue_lock(), LW_EXCLUSIVE, mpn)?;
        set_QUEUE_STOP_PAGE(newtailpage);
        LWLockRelease(notify_queue_lock())?;
    }

    LWLockRelease(notify_queue_tail_lock())?;
    Ok(())
}

/// `AsyncNotifyFreezeXids(newFrozenXid)` — VACUUM hook freezing queue XIDs that
/// would become inaccessible after CLOG truncation (async.c lines 2199-2293).
pub fn AsyncNotifyFreezeXids(newFrozenXid: TransactionId) -> PgResult<()> {
    let mut curpage: i64 = -1;
    let mut slotno: i64 = -1;
    let mut page_dirty = false;
    let mpn = my_proc_number();

    // Acquire locks in the correct order: TailLock, then QueueLock (SHARED).
    LWLockAcquire(notify_queue_tail_lock(), LW_SHARED, mpn)?;
    LWLockAcquire(notify_queue_lock(), LW_SHARED, mpn)?;

    let mut pos = QUEUE_TAIL();
    let head = QUEUE_HEAD();

    LWLockRelease(notify_queue_lock())?;

    while !QUEUE_POS_EQUAL(pos, head) {
        let pageno = QUEUE_POS_PAGE(pos);
        let offset = QUEUE_POS_OFFSET(pos) as usize;

        if pageno != curpage {
            if slotno >= 0 {
                if page_dirty {
                    set_page_dirty(slotno as usize);
                    page_dirty = false;
                }
                bank_lock_release(bank_number(curpage))?;
            }

            bank_lock_acquire_exclusive(pageno)?;
            slotno =
                with_notify_ctl(|ctl| SimpleLruReadPage(ctl, pageno, true, InvalidTransactionId))?
                    as i64;
            curpage = pageno;
        }

        // qe = (AsyncQueueEntry *)(page_buffer + offset); xid = qe->xid;
        let (qe_length, xid) = with_notify_ctl(|ctl| {
            let buf = ctl.shared.page_buffer(slotno as usize);
            (read_i32(buf, offset), read_u32(buf, offset + 8) as TransactionId)
        });

        if TransactionIdIsNormal(xid) && TransactionIdPrecedes(xid, newFrozenXid) {
            let newxid = if transam_seams::transaction_id_did_commit::call(
                xid,
                snapmgr_pc_seams::transaction_xmin::call()?,
            )? {
                FrozenTransactionId
            } else {
                InvalidTransactionId
            };
            with_notify_ctl(|ctl| {
                let buf = ctl.shared.page_buffer_mut(slotno as usize);
                buf[offset + 8..offset + 12].copy_from_slice(&newxid.to_ne_bytes());
            });
            page_dirty = true;
        }

        asyncQueueAdvance(&mut pos, qe_length);
    }

    if slotno >= 0 {
        if page_dirty {
            set_page_dirty(slotno as usize);
        }
        bank_lock_release(bank_number(curpage))?;
    }

    LWLockRelease(notify_queue_tail_lock())?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Incoming-notify delivery (async.c lines 2306-2368)
// ---------------------------------------------------------------------------

/// `ProcessIncomingNotify(flush)` (async.c lines 2306-2342).
fn ProcessIncomingNotify(flush: bool) -> PgResult<()> {
    // We *must* reset the flag.
    NOTIFY_INTERRUPT_PENDING.with(|f| f.set(false));

    // Do nothing else if we aren't actively listening.
    if listen_channels_is_empty() {
        return Ok(());
    }

    if trace_notify_enabled() {
        ereport(DEBUG1)
            .errmsg_internal("ProcessIncomingNotify")
            .finish(here())?;
    }

    ps_seams::set_ps_display::call("notify interrupt".to_string());

    // We must run asyncQueueReadAllNotifications inside a transaction.
    xact_seams::start_transaction_command::call()?;

    asyncQueueReadAllNotifications()?;

    xact_seams::commit_transaction_command::call()?;

    // If this isn't an end-of-command case, flush the notify messages.
    if flush {
        backend_libpq_pqcomm_seams::pq_flush::call()?;
    }

    ps_seams::set_ps_display::call("idle".to_string());

    if trace_notify_enabled() {
        ereport(DEBUG1)
            .errmsg_internal("ProcessIncomingNotify: done")
            .finish(here())?;
    }

    Ok(())
}

/// `PqMsg_NotificationResponse` ('A') — protocol.h.
const PqMsg_NotificationResponse: u8 = b'A';

/// `NotifyMyFrontEnd(channel, payload, srcPid)` (async.c lines 2347-2368).
pub fn NotifyMyFrontEnd(channel: &str, payload: &str, srcPid: i32) -> PgResult<()> {
    if backend_tcop_postgres_seams::where_to_send_output::call()
        == types_dest::dest::CommandDest::Remote
    {
        // pq_beginmessage(&buf, PqMsg_NotificationResponse);
        // pq_sendint32(&buf, srcPid); pq_sendstring(&buf, channel);
        // pq_sendstring(&buf, payload); pq_endmessage(&buf);  (no pq_flush)
        //
        // C builds the StringInfo in the current memory context; the message is
        // freed once `pq_endmessage` has handed the bytes to the comm layer, so
        // a short-lived local context is faithful. `pq_sendstring` runs the
        // mbutils.c `pg_server_to_client` encoding conversion.
        let ctx = mcx::MemoryContext::new("NotifyMyFrontEnd");
        let mut buf = backend_libpq_pqformat::pq_beginmessage(ctx.mcx(), PqMsg_NotificationResponse)?;
        backend_libpq_pqformat::pq_sendint32(&mut buf, srcPid as u32)?;
        backend_libpq_pqformat::pq_sendstring(&mut buf, channel.as_bytes())?;
        backend_libpq_pqformat::pq_sendstring(&mut buf, payload.as_bytes())?;
        backend_libpq_pqformat::pq_endmessage(buf)?;
    } else {
        ereport(INFO)
            .errmsg_internal(format!("NOTIFY for \"{channel}\" payload \"{payload}\""))
            .finish(here())?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Pending-notify dedup (async.c lines 2371-2498)
// ---------------------------------------------------------------------------

/// `AsyncExistsPendingNotify(n)` (async.c lines 2371-2404).
fn AsyncExistsPendingNotify(n: &Notification) -> bool {
    PENDING_NOTIFIES.with(|pn| {
        let pn = pn.borrow();
        let pn = match pn.as_ref() {
            None => return false,
            Some(p) => p,
        };

        if let Some(hashtab) = pn.hashtab.as_ref() {
            if hashtab.find(&pn.events, n) {
                return true;
            }
        } else {
            for oldn in &pn.events {
                if n.channel_len == oldn.channel_len
                    && n.payload_len == oldn.payload_len
                    && n.data[..(n.channel_len as usize + n.payload_len as usize + 2)]
                        == oldn.data[..(oldn.channel_len as usize + oldn.payload_len as usize + 2)]
                {
                    return true;
                }
            }
        }

        false
    })
}

/// `AddEventToPendingNotifies(n)` (async.c lines 2412-2464).
fn AddEventToPendingNotifies(n: Notification) -> PgResult<()> {
    PENDING_NOTIFIES.with(|pn| {
        let mut binding = pn.borrow_mut();
        let pn = binding.as_mut().unwrap();

        debug_assert!(!pn.events.is_empty());

        // Create the hash table if it's time to.
        if pn.events.len() >= MIN_HASHABLE_NOTIFIES as usize && pn.hashtab.is_none() {
            let mut hashtab = NotificationHashTable::new();
            for idx in 0..pn.events.len() {
                let oldn = pn.events[idx].clone();
                let found = hashtab.enter(&pn.events, &oldn, idx);
                debug_assert!(!found);
            }
            pn.hashtab = Some(hashtab);
        }

        // Add new event to the list, in order.
        pn.events.push(n);
        let new_idx = pn.events.len() - 1;

        if pn.hashtab.is_some() {
            let mut hashtab = pn.hashtab.take().unwrap();
            let n_ref = pn.events[new_idx].clone();
            let found = hashtab.enter(&pn.events, &n_ref, new_idx);
            debug_assert!(!found);
            pn.hashtab = Some(hashtab);
        }

        Ok(())
    })
}

/// `notification_hash(key, keysize)` (async.c lines 2471-2480).
fn notification_hash(key: &Notification, keysize: Size) -> u32 {
    debug_assert_eq!(keysize, core::mem::size_of::<usize>());
    // Don't include the payload's trailing null in the hash:
    //   hash_any(k->data, k->channel_len + k->payload_len + 1).
    let len = key.channel_len as usize + key.payload_len as usize + 1;
    common_hashfn::hash_bytes(&key.data[..len])
}

/// `notification_match(key1, key2, keysize)` (async.c lines 2485-2498).
fn notification_match(key1: &Notification, key2: &Notification, keysize: Size) -> i32 {
    debug_assert_eq!(keysize, core::mem::size_of::<usize>());
    if key1.channel_len == key2.channel_len
        && key1.payload_len == key2.payload_len
        && key1.data[..(key1.channel_len as usize + key1.payload_len as usize + 2)]
            == key2.data[..(key2.channel_len as usize + key2.payload_len as usize + 2)]
    {
        0
    } else {
        1
    }
}

// ---------------------------------------------------------------------------
// Cleanup / GUC hook (async.c lines 2501-2521)
// ---------------------------------------------------------------------------

/// `ClearPendingActionsAndNotifies()` (async.c lines 2501-2512).
fn ClearPendingActionsAndNotifies() {
    PENDING_ACTIONS.with(|pa| *pa.borrow_mut() = None);
    PENDING_NOTIFIES.with(|pn| *pn.borrow_mut() = None);
}

/// `check_notify_buffers(newval, extra, source)` (async.c lines 2517-2521).
pub fn check_notify_buffers(newval: &mut i32) -> bool {
    let (ok, _hint) = slru::check_slru_buffers("notify_buffers", *newval);
    ok
}

// ---------------------------------------------------------------------------
// SLRU bank-lock / page helpers over the in-crate NotifyCtl
// ---------------------------------------------------------------------------

fn bank_number(pageno: i64) -> usize {
    with_notify_ctl(|ctl| (pageno % ctl.nbanks as i64) as usize)
}

fn bank_lock_acquire_exclusive(pageno: i64) -> PgResult<()> {
    // SimpleLruGetBankLock returns a &LWLock borrowing the ctl; acquire it under
    // the borrow (LWLockAcquire does not touch NotifyCtl).
    with_notify_ctl(|ctl| {
        let lock = SimpleLruGetBankLock(ctl, pageno);
        LWLockAcquire(lock, LW_EXCLUSIVE, globals::MyProcNumber())
    })?;
    Ok(())
}

fn bank_lock_release(bankno: usize) -> PgResult<()> {
    with_notify_ctl(|ctl| LWLockRelease(&ctl.shared.bank_locks[bankno].lock))
}

fn set_page_dirty(slotno: usize) {
    with_notify_ctl(|ctl| ctl.shared.page_dirty[slotno] = true);
}

fn write_page_buffer(slotno: usize, offset: usize, bytes: &[u8]) {
    with_notify_ctl(|ctl| {
        let buf = ctl.shared.page_buffer_mut(slotno);
        buf[offset..offset + bytes.len()].copy_from_slice(bytes);
    });
}

// ---------------------------------------------------------------------------
// Backend identity (globals.c, read directly off init-small's thread-locals)
// ---------------------------------------------------------------------------

#[inline]
fn my_proc_number() -> ProcNumber {
    globals::MyProcNumber()
}
#[inline]
fn my_database_id() -> Oid {
    globals::MyDatabaseId()
}
#[inline]
fn my_proc_pid() -> i32 {
    globals::MyProcPid()
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

fn size_overflow() -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .errmsg("requested shared memory size overflows size_t")
        .into_error()
}

/// OOM error for a data-derived `try_reserve` failure in `funcname`.
fn oom(funcname: &'static str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .errmsg_internal(format!("out of memory in {funcname}"))
        .into_error()
}

/// `Datum 0` — the `before_shmem_exit(Async_UnlistenOnExit, 0)` arg.
fn datum_zero() -> types_tuple::Datum<'static> {
    types_tuple::Datum::ByVal(0)
}

/// `pg_on_exit_callback`-shaped wrapper installed via `before_shmem_exit`.
fn async_unlisten_on_exit_cb(code: i32, arg: types_tuple::Datum<'static>) -> PgResult<()> {
    Async_UnlistenOnExit(code, arg)
}

/// A blank `AsyncQueueEntry` (all-zero `data`), like C's local `AsyncQueueEntry qe;`.
#[inline]
fn blank_entry() -> AsyncQueueEntry {
    AsyncQueueEntry {
        length: 0,
        dboid: InvalidOid,
        xid: InvalidTransactionId,
        srcPid: 0,
        data: [0u8; NAMEDATALEN + NOTIFY_PAYLOAD_MAX_LENGTH],
    }
}

/// Serialize an `AsyncQueueEntry`'s leading `qe.length` bytes, matching the
/// repr(C) layout `int length; Oid dboid; TransactionId xid; int32 srcPid;
/// char data[]`. The returned vector is `QUEUE_PAGESIZE` bytes.
fn entry_to_bytes(qe: &AsyncQueueEntry) -> Vec<u8> {
    let mut buf = vec![0u8; QUEUE_PAGESIZE];
    buf[0..4].copy_from_slice(&qe.length.to_ne_bytes());
    buf[4..8].copy_from_slice(&qe.dboid.to_ne_bytes());
    buf[8..12].copy_from_slice(&qe.xid.to_ne_bytes());
    buf[12..16].copy_from_slice(&qe.srcPid.to_ne_bytes());
    let datalen = (qe.length as usize).saturating_sub(16).min(qe.data.len());
    buf[16..16 + datalen].copy_from_slice(&qe.data[..datalen]);
    buf
}

#[inline]
fn read_i32(buf: &[u8], off: usize) -> i32 {
    i32::from_ne_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}
#[inline]
fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

/// Read a NUL-terminated string starting at `off` in `buf`.
fn cstr_from(buf: &[u8], off: usize) -> String {
    let end = buf[off..]
        .iter()
        .position(|&b| b == 0)
        .map(|p| off + p)
        .unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[off..end]).into_owned()
}

// ---------------------------------------------------------------------------
// Trace_notify / max_notify_queue_pages GUC setters (assign hooks)
// ---------------------------------------------------------------------------

/// Assign hook for the `trace_notify` GUC.
pub fn set_trace_notify(value: bool) {
    TRACE_NOTIFY.with(|f| f.set(value));
}

/// Assign hook for the `max_notify_queue_pages` GUC.
pub fn set_max_notify_queue_pages(value: i32) {
    MAX_NOTIFY_QUEUE_PAGES.with(|c| c.set(value));
}

/// Read accessor for the `bool Trace_notify` GUC (async.c line 425) — the
/// `conf->variable` backing read by the GUC engine.
pub fn trace_notify() -> bool {
    TRACE_NOTIFY.with(|f| f.get())
}

/// Read accessor for the `int max_notify_queue_pages` GUC (async.c line 428) —
/// the `conf->variable` backing read by the GUC engine and by
/// `asyncQueueIsFull` / `asyncQueueUsage`.
pub fn max_notify_queue_pages() -> i32 {
    MAX_NOTIFY_QUEUE_PAGES.with(|c| c.get())
}

// ---------------------------------------------------------------------------
// Inward seam installation
// ---------------------------------------------------------------------------

/// Install every seam this crate owns (`backend-commands-async-seams`).
pub fn init_seams() {
    backend_commands_async_seams::handle_notify_interrupt::set(HandleNotifyInterrupt);
    backend_commands_async_seams::pre_commit_notify::set(PreCommit_Notify);
    backend_commands_async_seams::at_commit_notify::set(AtCommit_Notify);
    backend_commands_async_seams::at_abort_notify::set(AtAbort_Notify);
    backend_commands_async_seams::at_subcommit_notify::set(AtSubCommit_Notify);
    backend_commands_async_seams::at_subabort_notify::set(AtSubAbort_Notify);
    backend_commands_async_seams::at_prepare_notify::set(AtPrepare_Notify);
    backend_commands_async_seams::async_shmem_size::set(AsyncShmemSize);
    backend_commands_async_seams::async_shmem_init::set(AsyncShmemInit);
    backend_commands_async_seams::async_unlisten_all::set(Async_UnlistenAll);
    backend_commands_async_seams::async_notify_freeze_xids::set(AsyncNotifyFreezeXids);

    // --- ProcessUtility dispatch arms (utility.c NOTIFY/LISTEN/UNLISTEN) -----
    // The dispatch has already extracted the channel/payload strings from the
    // parse tree, so these arms forward directly to async.c.
    backend_tcop_utility_out_seams::async_notify::set(async_notify_arm);
    backend_tcop_utility_out_seams::async_listen::set(Async_Listen);
    backend_tcop_utility_out_seams::async_unlisten::set(Async_Unlisten);
    backend_tcop_utility_out_seams::async_unlisten_all::set(Async_UnlistenAll);

    // GUC variable backing storage owned by async.c. Both are plain GUC globals
    // (`bool Trace_notify`, `int max_notify_queue_pages`) read directly from the
    // `conf->variable` slot — not the ControlFile. The GUC engine seeds them
    // from boot_val and reads/writes them through these accessors.
    {
        use backend_utils_misc_guc_tables::{vars, GucVarAccessors};
        vars::Trace_notify.install(GucVarAccessors {
            get: trace_notify,
            set: set_trace_notify,
        });
        vars::max_notify_queue_pages.install(GucVarAccessors {
            get: max_notify_queue_pages,
            set: set_max_notify_queue_pages,
        });
    }

    // GUC check_hook for `notify_buffers` (async.c check_notify_buffers). Fired
    // during GUC option initialization at boot; the owning unit (async) must
    // install it, or initialize_one_guc_option_hooks panics on the uninstalled
    // slot. Mirrors clog/subtrans/commit-ts/multixact installing their
    // `check_*_buffers` hooks.
    {
        fn check_notify_buffers_hook(
            newval: &mut i32,
            _extra: &mut Option<backend_utils_misc_guc_tables::GucHookExtra>,
            _source: types_guc::guc::GucSource,
        ) -> types_error::PgResult<bool> {
            Ok(check_notify_buffers(newval))
        }
        backend_utils_misc_guc_tables::hooks::check_notify_buffers
            .install(check_notify_buffers_hook);
    }

    // Parallel-worker message handling forwards a NotificationResponse from a
    // worker back to the leader's frontend (parallel.c HandleParallelMessage
    // `NotifyMyFrontEnd(channel, payload, pid)`). The body is async.c's
    // `NotifyMyFrontEnd`; install the parallel-rt slot from the real owner.
    // The parallel-rt seam crate is a leaf (no cycle).
    backend_access_transam_parallel_rt_seams::notify_my_front_end::set(NotifyMyFrontEnd);
}

/// `case T_NotifyStmt: Async_Notify(stmt->conditionname, stmt->payload)`
/// (utility.c). The NOTIFY grammar always supplies a channel name, so the
/// nullable seam argument is unwrapped (mirroring the never-NULL C field).
fn async_notify_arm(conditionname: Option<&str>, payload: Option<&str>) -> PgResult<()> {
    let channel = conditionname.expect("NOTIFY: conditionname is NULL");
    Async_Notify(channel, payload)
}
