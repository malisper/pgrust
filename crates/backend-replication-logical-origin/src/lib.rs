// origin.c uses C identifier conventions (snake_case + CamelCase functions,
// ALLCAPS globals/constants). Mirror the sibling backend crates with crate-level
// allows rather than scattering per-item `#[allow]`s.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

//! `replication/logical/origin.c` — logical replication progress tracking.
//!
//! The infrastructure to name nodes in a replication setup and to efficiently
//! store and persist replication progress durably.
//!
//! # Shared state
//!
//! C keeps the replication-progress table as a `static ReplicationState
//! *replication_states` carved from a `repr(C)` `ReplicationStateCtl` shmem
//! block, plus a `static ReplicationState *session_replication_state` cache
//! pointer into that array. Here a backend is a thread and shared memory is
//! explicitly shared, synchronized state (AGENTS.md "Backend-global state"), so
//! the array is a process-global [`OriginShmem`] published once
//! ([`ReplicationOriginShmemInit`], mirroring `ShmemInitStruct`); each
//! [`ReplicationState`] embeds a *real* ported [`LWLock`]/`ConditionVariable`
//! and exposes its mutable scalar fields as atomics. The session cache pointer
//! and the per-backend GUC / session-origin globals are `thread_local!`. The
//! `ReplicationOriginLock` is one of lwlock.c's fixed individual LWLocks,
//! reached through the extern seam.
//!
//! # External calls (mirror-PG-and-panic)
//!
//! Subsystems origin.c only calls into — the catalog/syscache/heapam/genam
//! `pg_replication_origin` machinery, the lmgr object/relation locks, WAL
//! insertion + `XLogFlush`, the transaction/recovery predicates,
//! `on_shmem_exit`, the SRF/tuplestore plumbing, and the checkpoint file I/O +
//! CRC32C — go through
//! [`backend_replication_logical_origin_extern_seams`] and panic until their
//! owners land. `MyProcPid` is `backend-utils-init-small-seams::my_proc_pid`.
//!
//! # fmgr / `Datum` deferral
//!
//! The C SQL entry points have the `Datum f(PG_FUNCTION_ARGS)` shape; each is
//! exposed here with arguments already unwrapped to native Rust types and its
//! result returned typed (e.g. `Option<XLogRecPtr>` for an LSN-or-NULL return).

extern crate alloc;

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use ::core::cell::Cell;
use ::core::sync::atomic::Ordering;

use std::sync::OnceLock;

use backend_storage_lmgr_condition_variable_seams as cv;
use backend_storage_lmgr_lwlock::{LWLockAcquire, LWLockInitialize, LWLockRelease};
use backend_utils_init_small_seams::{my_proc_number, my_proc_pid};
use backend_replication_logical_origin_extern_seams as sx;

use types_core::{InvalidOid, Oid, OidIsValid, RepOriginId, TimestampTz, XLogRecPtr};
use types_error::{
    PgError, PgResult, ERRCODE_CONFIGURATION_LIMIT_EXCEEDED, ERRCODE_OBJECT_IN_USE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_OUT_OF_MEMORY, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
    ERRCODE_READ_ONLY_SQL_TRANSACTION, ERRCODE_RESERVED_NAME, ERRCODE_UNDEFINED_OBJECT, PANIC,
};
use types_storage::{LWTRANCHE_REPLICATION_ORIGIN_STATE, LW_EXCLUSIVE, LW_SHARED};
use types_wal::rmgr::XLogReaderState;

pub mod catalog_extern;
pub mod checkpoint_file;
pub mod core;
pub mod fmgr_builtins;

pub use crate::core::{
    xl_replorigin_drop, xl_replorigin_set, DoNotReplicateId, InvalidRepOriginId, InvalidXLogRecPtr,
    ReplicationOriginStatusRow, ReplicationState, ReplicationStateOnDisk,
    DEFAULT_MAX_ACTIVE_REPLICATION_ORIGINS, LOGICALREP_ORIGIN_ANY, LOGICALREP_ORIGIN_NONE,
    MAX_RONAME_LEN, PG_REPLORIGIN_CHECKPOINT_FILENAME, PG_REPLORIGIN_CHECKPOINT_TMPFILE,
    PG_UINT16_MAX, REPLICATION_STATE_MAGIC, RM_REPLORIGIN_ID, XLOG_REPLORIGIN_DROP,
    XLOG_REPLORIGIN_SET,
};

// ===========================================================================
// shared shmem state (the replication_states[] array) + per-backend globals
// ===========================================================================

/// The process-global analog of origin.c's shared-memory
/// `ReplicationStateCtl`/`replication_states[]`. Published once by
/// [`ReplicationOriginShmemInit`] (C's `ShmemInitStruct` first-init handshake),
/// then read by every entry point; the entries' embedded LWLocks /
/// `ReplicationOriginLock` serialize all mutation exactly as in C.
pub struct OriginShmem {
    /// Tranche to use for per-origin LWLocks
    /// (`replication_states_ctl->tranche_id`).
    pub tranche_id: i32,
    /// `ReplicationState replication_states[max_active_replication_origins]`.
    pub states: Vec<ReplicationState>,
}

static ORIGIN_SHMEM: OnceLock<OriginShmem> = OnceLock::new();

/// `replication_states[]` (or `replication_states_ctl`), panicking loudly (like
/// an uninstalled seam) if `ReplicationOriginShmemInit` has not run.
fn shmem() -> &'static OriginShmem {
    ORIGIN_SHMEM
        .get()
        .expect("ReplicationOriginShmemInit has not run (replication_states is NULL)")
}

/// `max_active_replication_origins` — read whenever needed. Established before
/// [`ReplicationOriginShmemInit`] carves the array, so it is a separate cell;
/// the published array's length is the authoritative slot count thereafter.
fn states() -> &'static [ReplicationState] {
    &shmem().states
}

thread_local! {
    /// `int max_active_replication_origins = 10;` — the GUC mirror (per-backend,
    /// inherited at fork / set via the GUC machinery).
    static MAX_ACTIVE_REPLICATION_ORIGINS: Cell<i32> =
        const { Cell::new(DEFAULT_MAX_ACTIVE_REPLICATION_ORIGINS) };

    /// `static ReplicationState *session_replication_state` — the cache
    /// pointer, re-expressed as the slot index (never a raw pointer).
    static SESSION_STATE: Cell<Option<usize>> = const { Cell::new(None) };

    /// `RepOriginId replorigin_session_origin` — assumed identity.
    static REPLORIGIN_SESSION_ORIGIN: Cell<RepOriginId> = const { Cell::new(InvalidRepOriginId) };
    /// `XLogRecPtr replorigin_session_origin_lsn`.
    static REPLORIGIN_SESSION_ORIGIN_LSN: Cell<XLogRecPtr> = const { Cell::new(InvalidXLogRecPtr) };
    /// `TimestampTz replorigin_session_origin_timestamp`.
    static REPLORIGIN_SESSION_ORIGIN_TIMESTAMP: Cell<TimestampTz> = const { Cell::new(0) };

    /// `static bool registered_cleanup;` inside `replorigin_session_setup`.
    static REGISTERED_CLEANUP: Cell<bool> = const { Cell::new(false) };
}

fn max_active_replication_origins() -> i32 {
    MAX_ACTIVE_REPLICATION_ORIGINS.with(Cell::get)
}

/// Set the `max_active_replication_origins` GUC mirror (the GUC machinery's
/// assign hook when it lands; also used by tests). Must be set before
/// [`ReplicationOriginShmemInit`].
pub fn set_max_active_replication_origins(value: i32) {
    MAX_ACTIVE_REPLICATION_ORIGINS.with(|c| c.set(value));
}

// ===========================================================================
// in-crate helpers for the reserved-name checks
// ===========================================================================

/// `pg_strcasecmp(a, b) == 0` for ASCII — origin.c only ever compares against
/// the literal lowercase "any"/"none", so an ASCII case-insensitive equality is
/// exactly `pg_strcasecmp`'s behavior here.
fn strcaseeq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .all(|(x, y)| x.to_ascii_lowercase() == y.to_ascii_lowercase())
}

/// `IsReservedName(name)` (catalog/catalog.c): true iff `name` starts with the
/// `"pg_"` prefix (`strncmp(name, "pg_", 3) == 0`).
fn IsReservedName(name: &str) -> bool {
    name.as_bytes().starts_with(b"pg_")
}

/// `static bool IsReservedOriginName(const char *name)` (origin.c lines 208-213):
/// true iff name is either "none" or "any".
pub fn IsReservedOriginName(name: &str) -> bool {
    strcaseeq(name, LOGICALREP_ORIGIN_NONE) || strcaseeq(name, LOGICALREP_ORIGIN_ANY)
}

/// `WAIT_EVENT_REPLICATION_ORIGIN_DROP` — the IPC-class wait event. Within the
/// generated `wait_event_types.h` the IPC events are alphabetically ordered;
/// `REPLICATION_ORIGIN_DROP` is the 49th (0-based index 0x30), one before
/// `REPLICATION_SLOT_DROP`. `PG_WAIT_IPC == 0x08000000`.
const WAIT_EVENT_REPLICATION_ORIGIN_DROP: u32 =
    types_pgstat_pg_wait_ipc::PG_WAIT_IPC | 0x30;

// types-pgstat is not a dependency (it would pull the full pgstat stack); the
// single PG_WAIT_IPC class base is reproduced here, matching
// `types_pgstat::wait_event::PG_WAIT_IPC`.
mod types_pgstat_pg_wait_ipc {
    pub const PG_WAIT_IPC: u32 = 0x0800_0000;
}

// ===========================================================================
// origin.c functions
// ===========================================================================

/// `static void replorigin_check_prerequisites(bool check_origins, bool recoveryOK)`
/// (origin.c lines 189-201).
pub fn replorigin_check_prerequisites(check_origins: bool, recoveryOK: bool) -> PgResult<()> {
    if check_origins && max_active_replication_origins() == 0 {
        return Err(PgError::error(
            "cannot query or manipulate replication origin when \"max_active_replication_origins\" is 0",
        )
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
    }

    if !recoveryOK && sx::RecoveryInProgress::call()? {
        return Err(
            PgError::error("cannot manipulate replication origins during recovery")
                .with_sqlstate(ERRCODE_READ_ONLY_SQL_TRANSACTION),
        );
    }

    Ok(())
}

/// `RepOriginId replorigin_by_name(const char *roname, bool missing_ok)`
/// (origin.c lines 225-249).
///
/// Check for a persistent replication origin identified by name. Returns
/// `InvalidOid` if the node isn't known yet and `missing_ok` is true.
pub fn replorigin_by_name(roname: &str, missing_ok: bool) -> PgResult<Oid> {
    let mut roident: Oid = InvalidOid;

    match sx::syscache_roident_by_name::call(roname)? {
        Some(found) => roident = found,
        None => {
            if !missing_ok {
                return Err(PgError::error(format!(
                    "replication origin \"{roname}\" does not exist"
                ))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
            }
        }
    }

    Ok(roident)
}

/// `RepOriginId replorigin_create(const char *roname)` (origin.c lines 256-363).
///
/// Create a replication origin. Needs to be called in a transaction.
pub fn replorigin_create(roname: &str) -> PgResult<Oid> {
    // To avoid needing a TOAST table for pg_replication_origin, we limit
    // replication origin names to 512 bytes.
    if roname.len() > MAX_RONAME_LEN {
        return Err(PgError::error("replication origin name is too long")
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .with_detail(format!(
                "Replication origin names must be no longer than {MAX_RONAME_LEN} bytes."
            )));
    }

    debug_assert!(sx::IsTransactionState::call()?);

    // The InitDirtySnapshot scan over pg_replication_origin under an
    // ExclusiveLock for the first unused roident, the heap_form_tuple +
    // CatalogTupleInsert + CommandCounterIncrement, and the table_open /
    // table_close are genuinely-external heap/genam/snapshot machinery
    // (origin.c lines 300-361). Returns the chosen roident, or None when every
    // id collided.
    let roident = sx::create_catalog_insert::call(roname)?;

    match roident {
        None => Err(PgError::error("could not find free replication origin ID")
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)),
        Some(roident) => Ok(roident),
    }
}

/// `static void replorigin_state_clear(RepOriginId roident, bool nowait)`
/// (origin.c lines 368-431).
///
/// Helper function to drop a replication origin.
pub fn replorigin_state_clear(roident: RepOriginId, nowait: bool) -> PgResult<()> {
    'restart: loop {
        // LWLockAcquire(ReplicationOriginLock, LW_EXCLUSIVE);
        sx::LWLockAcquireReplicationOriginLock::call(LW_EXCLUSIVE)?;

        let n = states().len();
        let max_active = max_active_replication_origins();
        let mut i: i32 = 0;
        while i < max_active && (i as usize) < n {
            let st = &states()[i as usize];

            // if (state->roident == roident)
            if st.roident.load(Ordering::Relaxed) == roident {
                // found our slot, is it busy?
                if st.acquired_by.load(Ordering::Relaxed) != 0 {
                    // C ereports here WITHOUT releasing ReplicationOriginLock;
                    // the error path's LWLockReleaseAll drops it.
                    if nowait {
                        let roi = st.roident.load(Ordering::Relaxed);
                        let acq = st.acquired_by.load(Ordering::Relaxed);
                        return Err(PgError::error(format!(
                            "could not drop replication origin with ID {roi}, in use by PID {acq}"
                        ))
                        .with_sqlstate(ERRCODE_OBJECT_IN_USE));
                    }

                    // We must wait and then retry. cv = &state->origin_cv;
                    // LWLockRelease(ReplicationOriginLock);
                    sx::LWLockReleaseReplicationOriginLock::call()?;

                    // ConditionVariableSleep(cv, WAIT_EVENT_REPLICATION_ORIGIN_DROP)
                    // is ConditionVariableTimedSleep(cv, -1, ev) (origin.c uses
                    // the untimed form; -1 timeout never times out).
                    cv::condition_variable_timed_sleep::call(
                        &st.origin_cv,
                        -1,
                        WAIT_EVENT_REPLICATION_ORIGIN_DROP,
                    )?;
                    // goto restart;
                    continue 'restart;
                }

                // first make a WAL log entry
                sx::wal_insert_replorigin_drop::call(roident)?;

                // then clear the in-memory slot
                st.roident.store(InvalidRepOriginId, Ordering::Relaxed);
                st.remote_lsn.store(InvalidXLogRecPtr, Ordering::Relaxed);
                st.local_lsn.store(InvalidXLogRecPtr, Ordering::Relaxed);
                break;
            }

            i += 1;
        }
        // LWLockRelease(ReplicationOriginLock);
        sx::LWLockReleaseReplicationOriginLock::call()?;
        // ConditionVariableCancelSleep();
        cv::condition_variable_cancel_sleep::call();

        return Ok(());
    }
}

/// `void replorigin_drop_by_name(const char *name, bool missing_ok, bool nowait)`
/// (origin.c lines 438-483).
///
/// Drop replication origin (by name). Needs to be called in a transaction.
pub fn replorigin_drop_by_name(name: &str, missing_ok: bool, nowait: bool) -> PgResult<()> {
    debug_assert!(sx::IsTransactionState::call()?);

    // rel = table_open(ReplicationOriginRelationId, RowExclusiveLock);
    sx::drop_open_relation::call()?;

    // roident = replorigin_by_name(name, missing_ok);
    let roident = replorigin_by_name(name, missing_ok)?;

    // Lock the origin to prevent concurrent drops.
    sx::LockSharedObjectOrigin::call(roident)?;

    // tuple = SearchSysCache1(REPLORIGIDENT, ...); if (!HeapTupleIsValid(tuple))
    if !sx::drop_tuple_exists::call(roident)? {
        if !missing_ok {
            return Err(PgError::error(format!(
                "cache lookup failed for replication origin with ID {roident}"
            )));
        }

        // We don't need to retain the locks if the origin is already dropped.
        sx::UnlockSharedObjectOrigin::call(roident)?;
        sx::drop_close_relation_keep_unlocked::call()?;
        return Ok(());
    }

    // replorigin_state_clear(roident, nowait);
    replorigin_state_clear(roident as RepOriginId, nowait)?;

    // Now, we can delete the catalog entry.
    sx::drop_delete_tuple::call(roident)?;

    // We keep the lock on pg_replication_origin until commit.
    sx::drop_close_relation_nolock::call()?;

    Ok(())
}

/// `bool replorigin_by_oid(RepOriginId roident, bool missing_ok, char **roname)`
/// (origin.c lines 492-525).
///
/// Lookup replication origin via its oid and return the name. Returns
/// `Ok(Some(name))` if the origin is known, `Ok(None)` otherwise.
pub fn replorigin_by_oid(roident: RepOriginId, missing_ok: bool) -> PgResult<Option<String>> {
    debug_assert!(OidIsValid(roident as Oid));
    debug_assert!(roident != InvalidRepOriginId);
    debug_assert!(roident != DoNotReplicateId);

    match sx::syscache_roname_by_oid::call(roident as Oid)? {
        Some(roname) => Ok(Some(roname)),
        None => {
            if !missing_ok {
                return Err(PgError::error(format!(
                    "replication origin with ID {roident} does not exist"
                ))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
            }
            Ok(None)
        }
    }
}

/// `Size ReplicationOriginShmemSize(void)` (origin.c lines 533-546).
pub fn ReplicationOriginShmemSize() -> usize {
    let max_active = max_active_replication_origins();
    if max_active == 0 {
        return 0;
    }
    // size = add_size(offsetof(ReplicationStateCtl, states),
    //                 mul_size(max_active_replication_origins, sizeof(ReplicationState)));
    (max_active as usize).saturating_mul(::core::mem::size_of::<ReplicationState>())
}

/// `void ReplicationOriginShmemInit(void)` (origin.c lines 548-577).
///
/// Allocates the owned `replication_states[]` array and initializes each
/// entry's embedded `LWLock`/`ConditionVariable` (mirroring the `!found`
/// first-init arm; publishing twice panics like a duplicate seam install,
/// matching C's single shmem carve).
pub fn ReplicationOriginShmemInit() -> PgResult<()> {
    if max_active_replication_origins() == 0 {
        return Ok(());
    }

    let n = max_active_replication_origins() as usize;

    // MemSet(replication_states_ctl, 0, ...): every entry starts zeroed.
    let mut states: Vec<ReplicationState> = Vec::new();
    states.try_reserve(n).map_err(|_| {
        PgError::error("out of memory allocating ReplicationOriginState")
            .with_sqlstate(ERRCODE_OUT_OF_MEMORY)
    })?;
    for _ in 0..n {
        states.push(ReplicationState::zeroed());
    }

    // replication_states_ctl->tranche_id = LWTRANCHE_REPLICATION_ORIGIN_STATE;
    let tranche_id = LWTRANCHE_REPLICATION_ORIGIN_STATE;
    for st in states.iter_mut() {
        // LWLockInitialize(&replication_states[i].lock, tranche_id);
        LWLockInitialize(&mut st.lock, tranche_id);
        // ConditionVariableInit(&replication_states[i].origin_cv) is in-place
        // construction (ConditionVariable::new in ReplicationState::zeroed).
    }

    if ORIGIN_SHMEM.set(OriginShmem { tranche_id, states }).is_err() {
        panic!("ReplicationOriginShmemInit ran twice (ReplicationOriginState already created)");
    }
    Ok(())
}

/// `void CheckPointReplicationOrigin(void)` (origin.c lines 595-711).
///
/// The transient-file create/write/CRC/durable_rename is the genuine-external
/// checkpoint-I/O seam; this keeps the in-memory part: skip unused slots, take
/// each slot's lock to snapshot `(roident, remote_lsn, local_lsn)`,
/// `XLogFlush(local_lsn)`, then hand the `(roident, remote_lsn)` snapshots to
/// the writer.
pub fn CheckPointReplicationOrigin() -> PgResult<()> {
    let max_active = max_active_replication_origins();
    if max_active == 0 {
        return Ok(());
    }

    // prevent concurrent creations/drops
    sx::LWLockAcquireReplicationOriginLock::call(LW_SHARED)?;

    let mut snapshot: Vec<(RepOriginId, XLogRecPtr)> = Vec::new();
    snapshot
        .try_reserve(max_active as usize)
        .map_err(|_| {
            PgError::error("out of memory in CheckPointReplicationOrigin")
                .with_sqlstate(ERRCODE_OUT_OF_MEMORY)
        })?;

    let n = states().len();
    let mut i: i32 = 0;
    while i < max_active && (i as usize) < n {
        let st = &states()[i as usize];

        // if (curstate->roident == InvalidRepOriginId) continue;
        if st.roident.load(Ordering::Relaxed) == InvalidRepOriginId {
            i += 1;
            continue;
        }

        // LWLockAcquire(&curstate->lock, LW_SHARED); snapshot; LWLockRelease.
        LWLockAcquire(&st.lock, LW_SHARED, my_proc_number::call())?;
        let roident = st.roident.load(Ordering::Relaxed);
        let remote_lsn = st.remote_lsn.load(Ordering::Relaxed);
        let local_lsn = st.local_lsn.load(Ordering::Relaxed);
        LWLockRelease(&st.lock)?;

        // make sure we only write out a commit that's persistent
        sx::XLogFlush::call(local_lsn)?;

        snapshot.push((roident, remote_lsn));

        i += 1;
    }

    // LWLockRelease(ReplicationOriginLock);
    sx::LWLockReleaseReplicationOriginLock::call()?;

    // unlink stale temp, open temp, write magic, write each disk_state, write
    // CRC, close, durable_rename — the genuinely-external transient-file I/O.
    sx::checkpoint_write::call(snapshot)?;

    Ok(())
}

/// `void StartupReplicationOrigin(void)` (origin.c lines 721-847).
///
/// The open/read/magic-verify/CRC-verify is the genuine-external checkpoint
/// reader; this keeps the in-memory part: the `last_state ==
/// max_active_replication_origins` overflow check and the copy of each decoded
/// `(roident, remote_lsn)` into the shared array.
pub fn StartupReplicationOrigin() -> PgResult<()> {
    #[cfg(debug_assertions)]
    already_started_guard();

    let max_active = max_active_replication_origins();
    if max_active == 0 {
        return Ok(());
    }

    // Open the checkpoint file, verify magic + CRC32C, decode the states.
    // `Ok(None)` is C's ENOENT early-return (no checkpoint yet / fresh standby).
    let decoded = match sx::checkpoint_read::call()? {
        None => return Ok(()),
        Some(states) => states,
    };

    let n = states().len();
    let mut last_state: i32 = 0;
    for (roident, remote_lsn) in decoded {
        // if (last_state == max_active_replication_origins)
        if last_state == max_active {
            return Err(PgError::new(
                PANIC,
                "could not find free replication state, increase \"max_active_replication_origins\"",
            )
            .with_sqlstate(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED));
        }

        // copy data to shared memory
        let idx = last_state as usize;
        debug_assert!(idx < n);
        let st = &states()[idx];
        st.roident.store(roident, Ordering::Relaxed);
        st.remote_lsn.store(remote_lsn, Ordering::Relaxed);
        last_state += 1;
    }

    Ok(())
}

/// `void replorigin_redo(XLogReaderState *record)` (origin.c lines 849-893):
/// switch on `XLogRecGetInfo(record) & ~XLR_INFO_MASK` and dispatch.
pub fn replorigin_redo(record: &mut XLogReaderState<'_>) -> PgResult<()> {
    let decoded = record.record.as_ref().ok_or_else(|| {
        PgError::new(PANIC, "replorigin_redo: record has no decoded payload")
    })?;
    let info = decoded.info() & !types_wal::wal::XLR_INFO_MASK;
    let data = decoded.data();
    let end_rec_ptr = record.EndRecPtr;

    match info {
        XLOG_REPLORIGIN_SET => {
            let xlrec = decode_replorigin_set(data)?;
            // replorigin_advance(xlrec->node_id, xlrec->remote_lsn,
            //                    record->EndRecPtr, xlrec->force, false);
            replorigin_advance(
                xlrec.node_id,
                xlrec.remote_lsn,
                end_rec_ptr,
                xlrec.force, // backward
                false,       // WAL log
            )
        }
        XLOG_REPLORIGIN_DROP => {
            let xlrec = decode_replorigin_drop(data)?;
            let max_active = max_active_replication_origins();
            let n = states().len();
            let mut i: i32 = 0;
            while i < max_active && (i as usize) < n {
                let st = &states()[i as usize];
                // found our slot
                if st.roident.load(Ordering::Relaxed) == xlrec.node_id {
                    // reset entry
                    st.roident.store(InvalidRepOriginId, Ordering::Relaxed);
                    st.remote_lsn.store(InvalidXLogRecPtr, Ordering::Relaxed);
                    st.local_lsn.store(InvalidXLogRecPtr, Ordering::Relaxed);
                    break;
                }
                i += 1;
            }
            Ok(())
        }
        // default: elog(PANIC, "replorigin_redo: unknown op code %u", info);
        _ => Err(PgError::new(
            PANIC,
            format!("replorigin_redo: unknown op code {info}"),
        )),
    }
}

/// Decode an `xl_replorigin_set` from `XLogRecGetData(record)`. C layout:
/// `{ XLogRecPtr remote_lsn; RepOriginId node_id; bool force; }` — remote_lsn
/// at offset 0 (8 bytes, native-endian), node_id at offset 8 (2 bytes), force
/// at offset 10 (1 byte).
fn decode_replorigin_set(data: &[u8]) -> PgResult<xl_replorigin_set> {
    if data.len() < 11 {
        return Err(PgError::new(
            PANIC,
            "replorigin_redo: xl_replorigin_set record too short",
        ));
    }
    let remote_lsn = XLogRecPtr::from_ne_bytes(data[0..8].try_into().unwrap());
    let node_id = RepOriginId::from_ne_bytes(data[8..10].try_into().unwrap());
    let force = data[10] != 0;
    Ok(xl_replorigin_set {
        remote_lsn,
        node_id,
        force,
    })
}

/// Decode an `xl_replorigin_drop` from `XLogRecGetData(record)`. C layout:
/// `{ RepOriginId node_id; }` — node_id at offset 0 (2 bytes, native-endian).
fn decode_replorigin_drop(data: &[u8]) -> PgResult<xl_replorigin_drop> {
    if data.len() < 2 {
        return Err(PgError::new(
            PANIC,
            "replorigin_redo: xl_replorigin_drop record too short",
        ));
    }
    let node_id = RepOriginId::from_ne_bytes(data[0..2].try_into().unwrap());
    Ok(xl_replorigin_drop { node_id })
}

/// `void replorigin_advance(RepOriginId node, XLogRecPtr remote_commit,
///   XLogRecPtr local_commit, bool go_backward, bool wal_log)`
/// (origin.c lines 910-1033).
pub fn replorigin_advance(
    node: RepOriginId,
    remote_commit: XLogRecPtr,
    local_commit: XLogRecPtr,
    go_backward: bool,
    wal_log: bool,
) -> PgResult<()> {
    // replication_state / free_state become slot indices (NULL == None).
    let mut replication_state: Option<usize> = None;
    let mut free_state: Option<usize> = None;

    debug_assert!(node != InvalidRepOriginId);

    // we don't track DoNotReplicateId
    if node == DoNotReplicateId {
        return Ok(());
    }

    // Lock exclusively, as we may have to create a new table entry.
    sx::LWLockAcquireReplicationOriginLock::call(LW_EXCLUSIVE)?;

    let max_active = max_active_replication_origins();
    let n = states().len();

    let mut i: i32 = 0;
    while i < max_active && (i as usize) < n {
        let idx = i as usize;
        let st = &states()[idx];
        let roident = st.roident.load(Ordering::Relaxed);

        // remember where to insert if necessary
        if roident == InvalidRepOriginId && free_state.is_none() {
            free_state = Some(idx);
            i += 1;
            continue;
        }

        // not our slot
        if roident != node {
            i += 1;
            continue;
        }

        // ok, found slot
        replication_state = Some(idx);

        // LWLockAcquire(&replication_state->lock, LW_EXCLUSIVE);
        LWLockAcquire(&st.lock, LW_EXCLUSIVE, my_proc_number::call())?;

        // Make sure it's not used by somebody else
        let acquired_by = st.acquired_by.load(Ordering::Relaxed);
        if acquired_by != 0 {
            let roi = st.roident.load(Ordering::Relaxed);
            return Err(PgError::error(format!(
                "replication origin with ID {roi} is already active for PID {acquired_by}"
            ))
            .with_sqlstate(ERRCODE_OBJECT_IN_USE));
        }

        break;
    }

    // if (replication_state == NULL && free_state == NULL)
    if replication_state.is_none() && free_state.is_none() {
        return Err(PgError::error(format!(
            "could not find free replication state slot for replication origin with ID {node}"
        ))
        .with_sqlstate(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
        .with_hint("Increase \"max_active_replication_origins\" and try again."));
    }

    // if (replication_state == NULL) { initialize new slot }
    if replication_state.is_none() {
        let free_idx = free_state
            .ok_or_else(|| PgError::error("replorigin_advance: free_state is NULL"))?;
        let st = &states()[free_idx];
        // LWLockAcquire(&free_state->lock, LW_EXCLUSIVE);
        LWLockAcquire(&st.lock, LW_EXCLUSIVE, my_proc_number::call())?;
        replication_state = Some(free_idx);
        debug_assert!(st.remote_lsn.load(Ordering::Relaxed) == InvalidXLogRecPtr);
        debug_assert!(st.local_lsn.load(Ordering::Relaxed) == InvalidXLogRecPtr);
        // replication_state->roident = node;
        st.roident.store(node, Ordering::Relaxed);
    }

    let rs = replication_state
        .ok_or_else(|| PgError::error("replorigin_advance: replication_state is NULL"))?;
    let st = &states()[rs];

    debug_assert!(st.roident.load(Ordering::Relaxed) != InvalidRepOriginId);

    // If somebody "forcefully" sets this slot, WAL log it.
    if wal_log {
        sx::wal_insert_replorigin_set::call(remote_commit, node, go_backward)?;
    }

    // Due to - harmless - race conditions during a checkpoint we could see
    // values older than the ones we already have; don't overwrite those.
    if go_backward || st.remote_lsn.load(Ordering::Relaxed) < remote_commit {
        st.remote_lsn.store(remote_commit, Ordering::Relaxed);
    }
    if local_commit != InvalidXLogRecPtr
        && (go_backward || st.local_lsn.load(Ordering::Relaxed) < local_commit)
    {
        st.local_lsn.store(local_commit, Ordering::Relaxed);
    }
    // LWLockRelease(&replication_state->lock);
    LWLockRelease(&st.lock)?;

    // Release *after* changing the LSNs.
    sx::LWLockReleaseReplicationOriginLock::call()?;

    Ok(())
}

/// `XLogRecPtr replorigin_get_progress(RepOriginId node, bool flush)`
/// (origin.c lines 1036-1071).
pub fn replorigin_get_progress(node: RepOriginId, flush: bool) -> PgResult<XLogRecPtr> {
    let mut local_lsn: XLogRecPtr = InvalidXLogRecPtr;
    let mut remote_lsn: XLogRecPtr = InvalidXLogRecPtr;

    // prevent slots from being concurrently dropped
    sx::LWLockAcquireReplicationOriginLock::call(LW_SHARED)?;

    let max_active = max_active_replication_origins();
    let n = states().len();
    let mut i: i32 = 0;
    while i < max_active && (i as usize) < n {
        let st = &states()[i as usize];
        if st.roident.load(Ordering::Relaxed) == node {
            LWLockAcquire(&st.lock, LW_SHARED, my_proc_number::call())?;
            remote_lsn = st.remote_lsn.load(Ordering::Relaxed);
            local_lsn = st.local_lsn.load(Ordering::Relaxed);
            LWLockRelease(&st.lock)?;
            break;
        }
        i += 1;
    }

    sx::LWLockReleaseReplicationOriginLock::call()?;

    if flush && local_lsn != InvalidXLogRecPtr {
        sx::XLogFlush::call(local_lsn)?;
    }

    Ok(remote_lsn)
}

/// `static void ReplicationOriginExitCleanup(int code, Datum arg)`
/// (origin.c lines 1077-1099).
///
/// Tear down a (possibly) configured session replication origin during process
/// exit. This is the handler [`replorigin_session_setup`] registers via the
/// extern seam; the integrating runtime arranges a trampoline that dispatches
/// here.
pub fn ReplicationOriginExitCleanup(_code: i32, _arg: usize) -> PgResult<()> {
    // if (session_replication_state == NULL) return;
    let session = SESSION_STATE.with(Cell::get);
    if session.is_none() {
        return Ok(());
    }

    sx::LWLockAcquireReplicationOriginLock::call(LW_EXCLUSIVE)?;

    // cv = NULL; if (session_replication_state->acquired_by == MyProcPid) { ... }
    let idx = session.expect("session_state non-NULL here");
    let st = &states()[idx];
    let cv_idx = if st.acquired_by.load(Ordering::Relaxed) == my_proc_pid::call() {
        st.acquired_by.store(0, Ordering::Relaxed);
        SESSION_STATE.with(|c| c.set(None));
        Some(idx)
    } else {
        None
    };

    sx::LWLockReleaseReplicationOriginLock::call()?;

    // if (cv) ConditionVariableBroadcast(cv);
    if let Some(idx) = cv_idx {
        cv::condition_variable_broadcast::call(&states()[idx].origin_cv);
    }

    Ok(())
}

/// `void replorigin_session_setup(RepOriginId node, int acquired_by)`
/// (origin.c lines 1119-1204).
pub fn replorigin_session_setup(node: RepOriginId, acquired_by: i32) -> PgResult<()> {
    let mut free_slot: i32 = -1;

    // static bool registered_cleanup; if (!registered_cleanup) { ... }
    if !REGISTERED_CLEANUP.with(Cell::get) {
        // on_shmem_exit(ReplicationOriginExitCleanup, 0);
        sx::register_origin_exit_cleanup::call()?;
        REGISTERED_CLEANUP.with(|c| c.set(true));
    }

    let max_active = max_active_replication_origins();
    debug_assert!(max_active > 0);

    // if (session_replication_state != NULL)
    if SESSION_STATE.with(Cell::get).is_some() {
        return Err(
            PgError::error("cannot setup replication origin when one is already setup")
                .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        );
    }

    // Lock exclusively, as we may have to create a new table entry.
    sx::LWLockAcquireReplicationOriginLock::call(LW_EXCLUSIVE)?;

    let n = states().len();
    let mut i: i32 = 0;
    while i < max_active && (i as usize) < n {
        let idx = i as usize;
        let st = &states()[idx];
        let roident = st.roident.load(Ordering::Relaxed);

        // remember where to insert if necessary
        if roident == InvalidRepOriginId && free_slot == -1 {
            free_slot = i;
            i += 1;
            continue;
        }

        // not our slot
        if roident != node {
            i += 1;
            continue;
        }
        // else if (curstate->acquired_by != 0 && acquired_by == 0)
        else if st.acquired_by.load(Ordering::Relaxed) != 0 && acquired_by == 0 {
            let roi = st.roident.load(Ordering::Relaxed);
            let acq = st.acquired_by.load(Ordering::Relaxed);
            return Err(PgError::error(format!(
                "replication origin with ID {roi} is already active for PID {acq}"
            ))
            .with_sqlstate(ERRCODE_OBJECT_IN_USE));
        }

        // ok, found slot
        SESSION_STATE.with(|c| c.set(Some(idx)));
        break;
    }

    // if (session_replication_state == NULL && free_slot == -1)
    if SESSION_STATE.with(Cell::get).is_none() && free_slot == -1 {
        return Err(PgError::error(format!(
            "could not find free replication state slot for replication origin with ID {node}"
        ))
        .with_sqlstate(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
        .with_hint("Increase \"max_active_replication_origins\" and try again."));
    }
    // else if (session_replication_state == NULL) { initialize new slot }
    else if SESSION_STATE.with(Cell::get).is_none() {
        let idx = free_slot as usize;
        SESSION_STATE.with(|c| c.set(Some(idx)));
        let st = &states()[idx];
        debug_assert!(st.remote_lsn.load(Ordering::Relaxed) == InvalidXLogRecPtr);
        debug_assert!(st.local_lsn.load(Ordering::Relaxed) == InvalidXLogRecPtr);
        // session_replication_state->roident = node;
        st.roident.store(node, Ordering::Relaxed);
    }

    let sess = SESSION_STATE
        .with(Cell::get)
        .ok_or_else(|| PgError::error("replorigin_session_setup: session_state is NULL"))?;
    let st = &states()[sess];

    debug_assert!(st.roident.load(Ordering::Relaxed) != InvalidRepOriginId);

    // if (acquired_by == 0) session_replication_state->acquired_by = MyProcPid;
    if acquired_by == 0 {
        st.acquired_by.store(my_proc_pid::call(), Ordering::Relaxed);
    }
    // else if (session_replication_state->acquired_by != acquired_by)
    else if st.acquired_by.load(Ordering::Relaxed) != acquired_by {
        return Err(PgError::error(format!(
            "could not find replication state slot for replication origin with OID {node} which was acquired by {acquired_by}"
        )));
    }

    sx::LWLockReleaseReplicationOriginLock::call()?;

    // probably this one is pointless
    cv::condition_variable_broadcast::call(&states()[sess].origin_cv);

    Ok(())
}

/// `void replorigin_session_reset(void)` (origin.c lines 1212-1233).
///
/// Reset replay state previously setup in this session.
pub fn replorigin_session_reset() -> PgResult<()> {
    debug_assert!(max_active_replication_origins() != 0);

    // if (session_replication_state == NULL)
    if SESSION_STATE.with(Cell::get).is_none() {
        return Err(PgError::error("no replication origin is configured")
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
    }

    sx::LWLockAcquireReplicationOriginLock::call(LW_EXCLUSIVE)?;

    let idx = SESSION_STATE.with(Cell::get).expect("session_state non-NULL here");
    let st = &states()[idx];
    // session_replication_state->acquired_by = 0;
    st.acquired_by.store(0, Ordering::Relaxed);
    // session_replication_state = NULL;
    SESSION_STATE.with(|c| c.set(None));

    sx::LWLockReleaseReplicationOriginLock::call()?;

    // ConditionVariableBroadcast(cv);
    cv::condition_variable_broadcast::call(&states()[idx].origin_cv);

    Ok(())
}

/// `void replorigin_session_advance(XLogRecPtr remote_commit, XLogRecPtr local_commit)`
/// (origin.c lines 1241-1253).
///
/// Do the same work `replorigin_advance()` does, just on the session's
/// configured origin. Noticeably cheaper than using `replorigin_advance()`.
pub fn replorigin_session_advance(
    remote_commit: XLogRecPtr,
    local_commit: XLogRecPtr,
) -> PgResult<()> {
    let idx = SESSION_STATE.with(Cell::get).ok_or_else(|| {
        PgError::error("replorigin_session_advance: session_replication_state is NULL")
    })?;
    let st = &states()[idx];
    debug_assert!(st.roident.load(Ordering::Relaxed) != InvalidRepOriginId);

    // LWLockAcquire(&session_replication_state->lock, LW_EXCLUSIVE);
    LWLockAcquire(&st.lock, LW_EXCLUSIVE, my_proc_number::call())?;
    if st.local_lsn.load(Ordering::Relaxed) < local_commit {
        st.local_lsn.store(local_commit, Ordering::Relaxed);
    }
    if st.remote_lsn.load(Ordering::Relaxed) < remote_commit {
        st.remote_lsn.store(remote_commit, Ordering::Relaxed);
    }
    LWLockRelease(&st.lock)?;

    Ok(())
}

/// `XLogRecPtr replorigin_session_get_progress(bool flush)`
/// (origin.c lines 1259-1276).
pub fn replorigin_session_get_progress(flush: bool) -> PgResult<XLogRecPtr> {
    let idx = SESSION_STATE.with(Cell::get).ok_or_else(|| {
        PgError::error("replorigin_session_get_progress: session_replication_state is NULL")
    })?;
    let st = &states()[idx];

    LWLockAcquire(&st.lock, LW_SHARED, my_proc_number::call())?;
    let remote_lsn = st.remote_lsn.load(Ordering::Relaxed);
    let local_lsn = st.local_lsn.load(Ordering::Relaxed);
    LWLockRelease(&st.lock)?;

    if flush && local_lsn != InvalidXLogRecPtr {
        sx::XLogFlush::call(local_lsn)?;
    }

    Ok(remote_lsn)
}

// ===========================================================================
// session-origin external-global accessors (origin.h PGDLLIMPORTs)
// ===========================================================================

/// Read `replorigin_session_origin` (origin.h external global).
pub fn replorigin_session_origin() -> RepOriginId {
    REPLORIGIN_SESSION_ORIGIN.with(Cell::get)
}
/// Write `replorigin_session_origin`.
pub fn set_replorigin_session_origin(value: RepOriginId) {
    REPLORIGIN_SESSION_ORIGIN.with(|c| c.set(value));
}
/// Read `replorigin_session_origin_lsn`.
pub fn replorigin_session_origin_lsn() -> XLogRecPtr {
    REPLORIGIN_SESSION_ORIGIN_LSN.with(Cell::get)
}
/// Read `replorigin_session_origin_timestamp`.
pub fn replorigin_session_origin_timestamp() -> TimestampTz {
    REPLORIGIN_SESSION_ORIGIN_TIMESTAMP.with(Cell::get)
}
/// Write `replorigin_session_origin_timestamp` (origin.h external global;
/// stored by twophase.c's RecordTransactionCommitPrepared path).
pub fn set_replorigin_session_origin_timestamp(value: TimestampTz) {
    REPLORIGIN_SESSION_ORIGIN_TIMESTAMP.with(|c| c.set(value));
}
/// Write `replorigin_session_origin_lsn` (origin.h external global; recorded
/// by the parallel-apply worker so streaming restarts at the right place).
pub fn set_replorigin_session_origin_lsn(value: XLogRecPtr) {
    REPLORIGIN_SESSION_ORIGIN_LSN.with(|c| c.set(value));
}

// ===========================================================================
// SQL functions (fmgr/Datum deferral: args unwrapped, results typed)
// ===========================================================================

/// `Datum pg_replication_origin_create(PG_FUNCTION_ARGS)` (origin.c lines 1291-1327).
///
/// Create replication origin for the passed in name, and return the assigned oid.
pub fn pg_replication_origin_create(name: &str) -> PgResult<Oid> {
    replorigin_check_prerequisites(false, false)?;

    // Replication origins "any" and "none" are reserved; "pg_xxx" too.
    if IsReservedName(name) || IsReservedOriginName(name) {
        return Err(PgError::error(format!(
            "replication origin name \"{name}\" is reserved"
        ))
        .with_sqlstate(ERRCODE_RESERVED_NAME)
        .with_detail(format!(
            "Origin names \"{LOGICALREP_ORIGIN_ANY}\", \"{LOGICALREP_ORIGIN_NONE}\", and names starting with \"pg_\" are reserved."
        )));
    }

    // ENFORCE_REGRESSION_TEST_NAME_RESTRICTIONS is a build-time switch (off by
    // default), so the regress_ name WARNING is compiled out.

    replorigin_create(name)
}

/// `Datum pg_replication_origin_drop(PG_FUNCTION_ARGS)` (origin.c lines 1332-1346).
pub fn pg_replication_origin_drop(name: &str) -> PgResult<()> {
    replorigin_check_prerequisites(false, false)?;
    replorigin_drop_by_name(name, false, true)
}

/// `Datum pg_replication_origin_oid(PG_FUNCTION_ARGS)` (origin.c lines 1351-1367).
///
/// `Ok(None)` is the C `PG_RETURN_NULL()`.
pub fn pg_replication_origin_oid(name: &str) -> PgResult<Option<Oid>> {
    replorigin_check_prerequisites(false, false)?;

    let roident = replorigin_by_name(name, true)?;

    if OidIsValid(roident) {
        Ok(Some(roident))
    } else {
        Ok(None)
    }
}

/// `Datum pg_replication_origin_session_setup(PG_FUNCTION_ARGS)`
/// (origin.c lines 1372-1389).
pub fn pg_replication_origin_session_setup(name: &str) -> PgResult<()> {
    replorigin_check_prerequisites(true, false)?;

    let origin = replorigin_by_name(name, false)?;
    replorigin_session_setup(origin as RepOriginId, 0)?;

    set_replorigin_session_origin(origin as RepOriginId);

    Ok(())
}

/// `Datum pg_replication_origin_session_reset(PG_FUNCTION_ARGS)`
/// (origin.c lines 1394-1406).
pub fn pg_replication_origin_session_reset() -> PgResult<()> {
    replorigin_check_prerequisites(true, false)?;

    replorigin_session_reset()?;

    REPLORIGIN_SESSION_ORIGIN.with(|c| c.set(InvalidRepOriginId));
    REPLORIGIN_SESSION_ORIGIN_LSN.with(|c| c.set(InvalidXLogRecPtr));
    REPLORIGIN_SESSION_ORIGIN_TIMESTAMP.with(|c| c.set(0));

    Ok(())
}

/// `Datum pg_replication_origin_session_is_setup(PG_FUNCTION_ARGS)`
/// (origin.c lines 1411-1417).
pub fn pg_replication_origin_session_is_setup() -> PgResult<bool> {
    replorigin_check_prerequisites(false, false)?;

    Ok(replorigin_session_origin() != InvalidRepOriginId)
}

/// `Datum pg_replication_origin_session_progress(PG_FUNCTION_ARGS)`
/// (origin.c lines 1427-1446). `Ok(None)` is the C `PG_RETURN_NULL()`.
pub fn pg_replication_origin_session_progress(flush: bool) -> PgResult<Option<XLogRecPtr>> {
    replorigin_check_prerequisites(true, false)?;

    // if (session_replication_state == NULL)
    if SESSION_STATE.with(Cell::get).is_none() {
        return Err(PgError::error("no replication origin is configured")
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
    }

    let remote_lsn = replorigin_session_get_progress(flush)?;

    if remote_lsn == InvalidXLogRecPtr {
        return Ok(None);
    }

    Ok(Some(remote_lsn))
}

/// `Datum pg_replication_origin_xact_setup(PG_FUNCTION_ARGS)`
/// (origin.c lines 1448-1464).
pub fn pg_replication_origin_xact_setup(
    location: XLogRecPtr,
    timestamp: TimestampTz,
) -> PgResult<()> {
    replorigin_check_prerequisites(true, false)?;

    // if (session_replication_state == NULL)
    if SESSION_STATE.with(Cell::get).is_none() {
        return Err(PgError::error("no replication origin is configured")
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
    }

    REPLORIGIN_SESSION_ORIGIN_LSN.with(|c| c.set(location));
    REPLORIGIN_SESSION_ORIGIN_TIMESTAMP.with(|c| c.set(timestamp));

    Ok(())
}

/// `Datum pg_replication_origin_xact_reset(PG_FUNCTION_ARGS)`
/// (origin.c lines 1466-1475).
pub fn pg_replication_origin_xact_reset() -> PgResult<()> {
    replorigin_check_prerequisites(true, false)?;

    REPLORIGIN_SESSION_ORIGIN_LSN.with(|c| c.set(InvalidXLogRecPtr));
    REPLORIGIN_SESSION_ORIGIN_TIMESTAMP.with(|c| c.set(0));

    Ok(())
}

/// `Datum pg_replication_origin_advance(PG_FUNCTION_ARGS)`
/// (origin.c lines 1478-1503).
pub fn pg_replication_origin_advance(name: &str, remote_commit: XLogRecPtr) -> PgResult<()> {
    replorigin_check_prerequisites(true, false)?;

    // lock to prevent the replication origin from vanishing
    sx::LockRelationOidOrigin::call()?;

    let node = replorigin_by_name(name, false)?;

    // Can't sensibly pass a local commit to be flushed at checkpoint.
    replorigin_advance(
        node as RepOriginId,
        remote_commit,
        InvalidXLogRecPtr,
        true, // go backward
        true, // WAL log
    )?;

    sx::UnlockRelationOidOrigin::call()?;

    Ok(())
}

/// `Datum pg_replication_origin_progress(PG_FUNCTION_ARGS)`
/// (origin.c lines 1513-1535). `Ok(None)` is the C `PG_RETURN_NULL()`.
pub fn pg_replication_origin_progress(name: &str, flush: bool) -> PgResult<Option<XLogRecPtr>> {
    replorigin_check_prerequisites(true, true)?;

    let roident = replorigin_by_name(name, false)?;
    debug_assert!(OidIsValid(roident));

    let remote_lsn = replorigin_get_progress(roident as RepOriginId, flush)?;

    if remote_lsn == InvalidXLogRecPtr {
        return Ok(None);
    }

    Ok(Some(remote_lsn))
}

/// `Datum pg_show_replication_origin_status(PG_FUNCTION_ARGS)`
/// (origin.c lines 1538-1607). Set-returning;
/// `#define REPLICATION_ORIGIN_PROGRESS_COLS 4`. Rows are emitted through the
/// extern seam exactly as the C calls `tuplestore_putvalues`.
pub fn pg_show_replication_origin_status() -> PgResult<()> {
    // we want to return 0 rows if slot is set to zero
    replorigin_check_prerequisites(false, true)?;

    // InitMaterializedSRF(fcinfo, 0);
    sx::InitMaterializedSRF::call()?;

    // prevent slots from being concurrently dropped
    sx::LWLockAcquireReplicationOriginLock::call(LW_SHARED)?;

    let max_active = max_active_replication_origins();
    let n = states().len();
    let mut i: i32 = 0;
    while i < max_active && (i as usize) < n {
        let st = &states()[i as usize];

        // unused slot, nothing to display
        let roident = st.roident.load(Ordering::Relaxed);
        if roident == InvalidRepOriginId {
            i += 1;
            continue;
        }

        // memset(values, 0, ...); memset(nulls, 1, ...);
        let mut row = ReplicationOriginStatusRow::default();

        // values[0] = ObjectIdGetDatum(state->roident); nulls[0] = false;
        row.local_id = roident as Oid;

        // We're not preventing the origin being dropped concurrently, so
        // silently accept that it might be gone.
        if let Some(roname) = replorigin_by_oid(roident, true)? {
            row.external_id = Some(roname);
        }

        // LWLockAcquire(&state->lock, LW_SHARED); snapshot LSNs; LWLockRelease.
        LWLockAcquire(&st.lock, LW_SHARED, my_proc_number::call())?;
        row.remote_lsn = st.remote_lsn.load(Ordering::Relaxed);
        row.local_lsn = st.local_lsn.load(Ordering::Relaxed);
        LWLockRelease(&st.lock)?;

        // tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
        sx::put_replication_origin_status_row::call(
            row.local_id,
            row.external_id,
            row.remote_lsn,
            row.local_lsn,
        )?;

        i += 1;
    }

    sx::LWLockReleaseReplicationOriginLock::call()?;

    // return (Datum) 0;
    Ok(())
}

// ===========================================================================
// `#ifdef USE_ASSERT_CHECKING static bool already_started` guard
// (StartupReplicationOrigin's process-local one-shot)
// ===========================================================================

#[cfg(debug_assertions)]
thread_local! {
    static ALREADY_STARTED: Cell<bool> = const { Cell::new(false) };
}

/// `#ifdef USE_ASSERT_CHECKING static bool already_started = false;
/// Assert(!already_started); already_started = true;`.
#[cfg(debug_assertions)]
fn already_started_guard() {
    ALREADY_STARTED.with(|c| {
        debug_assert!(!c.get());
        c.set(true);
    });
}

// ===========================================================================
// seam installation
// ===========================================================================

/// `replorigin_by_oid` seam adapter: this crate's `replorigin_by_oid` returns a
/// plain `String`; the seam contract (consumed by conflict.c's apply-error
/// detail) wants the name copied into the caller's memory context (C:
/// `text_to_cstring` palloc'd in the calling context). Marshal the `String`
/// into a `PgString` allocated in `mcx`; an OOM during the copy is `Err`.
fn replorigin_by_oid_seam<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    roident: RepOriginId,
    missing_ok: bool,
) -> PgResult<Option<mcx::PgString<'mcx>>> {
    match replorigin_by_oid(roident, missing_ok)? {
        Some(name) => Ok(Some(mcx::PgString::from_str_in(&name, mcx)?)),
        None => Ok(None),
    }
}

/// `set_replorigin_session_timestamp` seam (twophase.c's
/// `RecordTransactionCommitPrepared` write-back) is the same global write as
/// `set_replorigin_session_origin_timestamp` (xact.c's commit path); both set
/// the `replorigin_session_origin_timestamp` external global.
fn set_replorigin_session_timestamp_seam(ts: TimestampTz) {
    set_replorigin_session_origin_timestamp(ts);
}

/// Install every seam this crate owns. Called once from `seams-init`.
pub fn init_seams() {
    use backend_replication_logical_origin_seams as s;

    // Register this crate's SQL-callable builtins into the fmgr-core builtin
    // table (C: `fmgr_builtins[]`), so by-OID dispatch resolves them.
    fmgr_builtins::register_backend_replication_logical_origin_builtins();

    s::replorigin_redo::set(replorigin_redo);
    s::replorigin_session_origin::set(replorigin_session_origin);
    s::replorigin_session_origin_lsn::set(replorigin_session_origin_lsn);
    s::replorigin_session_origin_timestamp::set(replorigin_session_origin_timestamp);
    s::set_replorigin_session_origin_timestamp::set(set_replorigin_session_origin_timestamp);
    s::set_replorigin_session_origin_lsn::set(set_replorigin_session_origin_lsn);
    s::set_replorigin_session_timestamp::set(set_replorigin_session_timestamp_seam);
    s::replorigin_session_advance::set(replorigin_session_advance);
    s::replorigin_advance::set(replorigin_advance);
    s::replorigin_by_oid::set(replorigin_by_oid_seam);
    s::max_active_replication_origins::set(max_active_replication_origins);
    // Pure-wiring install (assemble/seam-wiring-guard): owner body matches.
    s::replication_origin_shmem_init::set(ReplicationOriginShmemInit);
    // Contract-reconciled install (assemble/seam-contract-reconciles): the seam
    // is now the infallible `-> Size` shape, matching the C `Size` return.
    s::replication_origin_shmem_size::set(ReplicationOriginShmemSize);

    // WAL-startup entry point called once by `StartupXLOG` (xlog.c:5695).
    s::startup_replication_origin::set(StartupReplicationOrigin);

    // The replorigin_checkpoint transient-file I/O codecs — declared as the
    // origin-extern-seams checkpoint_write / checkpoint_read. The file-fd /
    // CRC32C substrate is ported, so this crate owns them (the in-memory
    // halves already live here in CheckPointReplicationOrigin /
    // StartupReplicationOrigin).
    sx::checkpoint_write::set(checkpoint_file::checkpoint_write);
    sx::checkpoint_read::set(checkpoint_file::checkpoint_read);

    // The pg_replication_origin catalog mutation/lookup machinery (origin.c's
    // replorigin_create dirty-scan+insert, replorigin_by_name/by_oid syscache
    // lookups, replorigin_drop_by_name's tuple delete). The heap / genam /
    // syscache / indexing / xact substrate is ported, so this crate owns them.
    sx::create_catalog_insert::set(catalog_extern::create_catalog_insert);
    sx::syscache_roident_by_name::set(catalog_extern::syscache_roident_by_name);
    sx::syscache_roname_by_oid::set(catalog_extern::syscache_roname_by_oid);
    sx::drop_open_relation::set(catalog_extern::drop_open_relation);
    sx::drop_tuple_exists::set(catalog_extern::drop_tuple_exists);
    sx::drop_delete_tuple::set(catalog_extern::drop_delete_tuple);
    sx::drop_close_relation_keep_unlocked::set(catalog_extern::drop_close_relation_keep_unlocked);
    sx::drop_close_relation_nolock::set(catalog_extern::drop_close_relation_nolock);

    // The transaction/recovery predicates origin.c checks before mutating
    // (replorigin_check_prerequisites / the create+drop Asserts). Owners ported.
    sx::RecoveryInProgress::set(catalog_extern::recovery_in_progress);
    sx::IsTransactionState::set(catalog_extern::is_transaction_state);
    // `int max_active_replication_origins` (origin.c GUC, boot 10) — install the
    // guc-tables slot over this crate's backing mirror accessors.
    backend_utils_misc_guc_tables::vars::max_active_replication_origins.install(
        backend_utils_misc_guc_tables::GucVarAccessors {
            get: max_active_replication_origins,
            set: set_max_active_replication_origins,
        },
    );
}

#[cfg(test)]
mod tests;
