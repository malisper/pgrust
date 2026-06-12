//! Top-level transaction system support routines
//! (`src/backend/access/transam/xact.c`, PostgreSQL 18.3).
//!
//! The transaction-state stack, the command/block state machines, the WAL
//! commit/abort record format, and the redo dispatch are all in-crate. Every
//! call into another subsystem goes either to a direct dependency (elog,
//! mcx, pg-prng, pqsignal, backend_progress) or through that subsystem's
//! per-owner seam crate, one seam per C function, in the C call order.
//!
//! Sanctioned divergences from the C (per AGENTS.md / repo docs):
//!
//! * **Resource owners dissolve** (docs/query-lifecycle-raii.md): the
//!   `ResourceOwnerCreate/Release/Delete` calls and the `RESOURCE_RELEASE_*`
//!   phase walk become RAII owner values held by the eventual transaction
//!   driver. The `if (s->curTransactionOwner)` control flow is preserved via
//!   `has_resource_owner`.
//! * **No ambient memory context** (docs/mctx-design.md): the
//!   `MemoryContextSwitchTo`/`priorContext` choreography has no equivalent;
//!   the transaction-lifetime contexts (`TopTransactionContext`,
//!   `TransactionAbortContext`, per-subxact `CurTransactionContext`) are
//!   owned by the backend-local state here and created/reset/deleted at the
//!   same points the C does.
//! * **`AtEOXact_ComboCid` / `AtEOXact_HashTables` (and sub-xact twins)
//!   dissolve**: combocid state is an owned `ComboCidState<'mcx>` dropped by
//!   its owner; dynahash seq-scan tracking does not exist over `PgHashMap`.
//! * Backend-local file statics are `thread_local!` (one backend = one
//!   thread), never shared statics.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use std::cell::RefCell;

use backend_utils_error::{elog, ereport, message_level_is_interesting};
use mcx::MemoryContext;
use types_core::xact::*;
use types_core::{TimestampTz, TransactionId, XLogRecPtr};
use types_error::{
    ErrorLocation, PgError, PgResult, DEBUG5, ERRCODE_ACTIVE_SQL_TRANSACTION,
    ERRCODE_INVALID_TRANSACTION_STATE, ERRCODE_NO_ACTIVE_SQL_TRANSACTION,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_S_E_INVALID_SPECIFICATION, ERROR, FATAL, WARNING,
};

pub(crate) use backend_access_transam_parallel_seams as parallel_seams;
pub(crate) use backend_access_transam_varsup_seams as varsup_seams;
pub(crate) use backend_access_transam_xlog_seams as xlog_seams;
pub(crate) use backend_access_transam_xloginsert_seams as xloginsert_seams;
pub(crate) use backend_executor_spi_seams as spi_seams;
pub(crate) use backend_storage_ipc_sinval_seams as sinval_seams;
pub(crate) use backend_storage_lmgr_lmgr_seams as lmgr_seams;
pub(crate) use backend_storage_lmgr_predicate_seams as predicate_seams;
pub(crate) use backend_storage_lmgr_proc_seams as proc_seams;
pub(crate) use backend_utils_adt_timestamp_seams as timestamp_seams;
pub(crate) use backend_utils_cache_inval_seams as inval_seams;
pub(crate) use backend_utils_cache_relmapper_seams as relmapper_seams;
pub(crate) use backend_utils_misc_guc_seams as guc_seams;
pub(crate) use backend_utils_time_snapmgr_seams as snapmgr_seams;

mod engine;
mod redo;
mod wal;

pub use engine::{
    AbortCurrentTransaction, AbortOutOfAnyTransaction, CommitTransactionCommand,
    EndParallelWorkerTransaction, StartParallelWorkerTransaction, StartTransactionCommand,
};
pub use redo::{parse_abort_record, parse_commit_record, xact_redo, XactRedoInfo};
pub use wal::{XactLogAbortRecord, XactLogCommitRecord};

/// `STANDBY_DISABLED` etc. (`access/xlogutils.h`) — hot-standby states the
/// redo path branches on (via the xlogutils seam).
pub const STANDBY_DISABLED: i32 = 0;
pub const STANDBY_INITIALIZED: i32 = 1;
pub const STANDBY_SNAPSHOT_PENDING: i32 = 2;
pub const STANDBY_SNAPSHOT_READY: i32 = 3;

/// `MaxAllocSize` (1 GB - 1): bounds `childXids` (`AtSubCommit_childXids`).
pub(crate) const MAX_ALLOC_SIZE: usize = 0x3fff_ffff;

/// `PGPROC_MAX_CACHED_SUBXIDS` (`storage/proc.h`).
const PGPROC_MAX_CACHED_SUBXIDS: usize = 64;

// ---------------------------------------------------------------------------
//  Transaction-state stack (backend-local; C uses file-scope statics)
// ---------------------------------------------------------------------------

/// `TransactionStateData` (xact.c) — one entry on the state stack. The
/// pointer-linked C list is a Vec where index 0 is `TopTransactionStateData`
/// and `parent` is the previous element.
#[derive(Debug)]
pub(crate) struct TransactionNode {
    pub full_transaction_id: FullTransactionId,
    pub sub_transaction_id: SubTransactionId,
    pub name: Option<String>,
    pub savepoint_level: i32,
    pub state: TransState,
    pub block_state: TBlockState,
    pub nesting_level: i32,
    pub guc_nest_level: i32,
    /// `childXids`/`nChildXids` — subcommitted child XIDs, kept in XID order.
    /// (C keeps the array in TopTransactionContext; a plain Vec with fallible
    /// reserves carries the same bound + OOM surface.)
    pub child_xids: Vec<TransactionId>,
    /// `prevUser` / `prevSecContext` (GetUserIdAndSecContext at start).
    pub prev_user: types_core::Oid,
    pub prev_sec_context: i32,
    pub prev_xact_read_only: bool,
    pub started_in_recovery: bool,
    pub did_log_xid: bool,
    pub parallel_mode_level: i32,
    pub parallel_child_xact: bool,
    pub chain: bool,
    pub top_xid_logged: bool,
    /// whether this entry has a curTransactionOwner (set by AtStart/AtSubStart,
    /// cleared on commit/abort) — drives the `if (s->curTransactionOwner)`
    /// arms; the owner value itself dissolves into RAII guards.
    pub has_resource_owner: bool,
    /// `s->curTransactionContext` for subtransactions (child of the parent's);
    /// `None` on the top node, whose CurTransactionContext IS
    /// TopTransactionContext.
    pub cur_transaction_context: Option<MemoryContext>,
    /// Non-empty subxact CurTransactionContexts kept alive at subcommit (in C
    /// they survive as children of the parent context until top-level end).
    pub retained_child_contexts: Vec<MemoryContext>,
}

impl TransactionNode {
    fn top() -> Self {
        Self {
            full_transaction_id: InvalidFullTransactionId,
            sub_transaction_id: InvalidSubTransactionId,
            name: None,
            savepoint_level: 0,
            state: TRANS_DEFAULT,
            block_state: TBLOCK_DEFAULT,
            nesting_level: 0,
            guc_nest_level: 0,
            child_xids: Vec::new(),
            prev_user: 0,
            prev_sec_context: 0,
            prev_xact_read_only: false,
            started_in_recovery: false,
            did_log_xid: false,
            parallel_mode_level: 0,
            parallel_child_xact: false,
            chain: false,
            top_xid_logged: false,
            has_resource_owner: false,
            cur_transaction_context: None,
            retained_child_contexts: Vec::new(),
        }
    }
}

/// The xact.c file-scope statics, one owned value per backend.
#[derive(Debug)]
pub(crate) struct XactState {
    // user-tweakable parameters (GUC-backed globals owned by xact.c)
    pub DefaultXactIsoLevel: i32,
    pub XactIsoLevel: i32,
    pub DefaultXactReadOnly: bool,
    pub XactReadOnly: bool,
    pub DefaultXactDeferrable: bool,
    pub XactDeferrable: bool,
    pub synchronous_commit: i32,
    /// `CheckXidAlive` / `bsysscan` (logical-decoding concurrent-abort checks)
    pub CheckXidAlive: TransactionId,
    pub bsysscan: bool,
    pub MyXactFlags: i32,
    pub xact_is_sampled: bool,
    /// `XactTopFullTransactionId`
    pub xact_top_full_transaction_id: FullTransactionId,
    /// `ParallelCurrentXids` (sorted numerically); empty in a non-worker.
    pub parallel_current_xids: Vec<TransactionId>,
    /// `currentSubTransactionId`
    pub current_sub_transaction_id: SubTransactionId,
    pub current_command_id: CommandId,
    pub current_command_id_used: bool,
    pub xact_start_timestamp: TimestampTz,
    pub stmt_start_timestamp: TimestampTz,
    pub xact_stop_timestamp: TimestampTz,
    pub force_sync_commit: bool,
    /// `prepareGID`
    pub prepare_gid: Option<String>,
    /// `unreportedXids` / `nUnreportedXids`
    pub unreported_xids: Vec<TransactionId>,
    /// the static latch of `GetStableLatestTransactionId`
    pub stable_latest: (LocalTransactionId, TransactionId),
    /// the transaction-state stack; `[0]` is `TopTransactionStateData`.
    pub transaction_stack: Vec<TransactionNode>,
    pub xact_callbacks: Vec<XactCallbackRegistration>,
    pub subxact_callbacks: Vec<SubXactCallbackRegistration>,
    /// `TopTransactionContext` (mcxt.c global, managed by xact.c).
    pub top_transaction_context: Option<MemoryContext>,
    /// `TransactionAbortContext`.
    pub transaction_abort_context: Option<MemoryContext>,
}

type XactCallback = Box<dyn FnMut(u32) -> PgResult<()>>;
type SubXactCallback = Box<dyn FnMut(u32, SubTransactionId, SubTransactionId) -> PgResult<()>>;

pub(crate) struct XactCallbackRegistration {
    key: usize,
    callback: XactCallback,
}

impl std::fmt::Debug for XactCallbackRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XactCallbackRegistration")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

pub(crate) struct SubXactCallbackRegistration {
    key: usize,
    callback: SubXactCallback,
}

impl std::fmt::Debug for SubXactCallbackRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubXactCallbackRegistration")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl XactState {
    fn new() -> Self {
        Self {
            DefaultXactIsoLevel: XACT_READ_COMMITTED,
            XactIsoLevel: XACT_READ_COMMITTED,
            DefaultXactReadOnly: false,
            XactReadOnly: false,
            DefaultXactDeferrable: false,
            XactDeferrable: false,
            synchronous_commit: SYNCHRONOUS_COMMIT_ON,
            CheckXidAlive: InvalidTransactionId,
            bsysscan: false,
            MyXactFlags: 0,
            xact_is_sampled: false,
            xact_top_full_transaction_id: InvalidFullTransactionId,
            parallel_current_xids: Vec::new(),
            current_sub_transaction_id: InvalidSubTransactionId,
            current_command_id: FirstCommandId,
            current_command_id_used: false,
            xact_start_timestamp: 0,
            stmt_start_timestamp: 0,
            xact_stop_timestamp: 0,
            force_sync_commit: false,
            prepare_gid: None,
            unreported_xids: Vec::new(),
            stable_latest: (InvalidLocalTransactionId, InvalidTransactionId),
            transaction_stack: vec![TransactionNode::top()],
            xact_callbacks: Vec::new(),
            subxact_callbacks: Vec::new(),
            top_transaction_context: None,
            transaction_abort_context: None,
        }
    }

    /// `CurrentTransactionState`
    pub fn current(&self) -> &TransactionNode {
        self.transaction_stack
            .last()
            .expect("transaction stack is never empty")
    }

    pub fn current_mut(&mut self) -> &mut TransactionNode {
        self.transaction_stack
            .last_mut()
            .expect("transaction stack is never empty")
    }

    /// Is the current entry a subtransaction (has a parent)?
    pub fn is_subxact(&self) -> bool {
        self.transaction_stack.len() > 1
    }
}

thread_local! {
    static STATE: RefCell<XactState> = RefCell::new(XactState::new());
}

/// Access the backend-local xact state. Borrows must never be held across a
/// seam call or any other function that may re-enter this module.
pub(crate) fn xs<R>(f: impl FnOnce(&mut XactState) -> R) -> R {
    STATE.with(|s| f(&mut s.borrow_mut()))
}

/// Snapshot the current node's `blockState` without holding the borrow.
pub(crate) fn cur_block_state() -> TBlockState {
    xs(|s| s.current().block_state)
}

pub fn reset_xact_state_for_tests() {
    xs(|s| *s = XactState::new());
}

// ---------------------------------------------------------------------------
//  User-tweakable parameters (xact.c:78-87) — GUC-assigned globals this file
//  owns; getters/setters for the GUC layer and other crates.
// ---------------------------------------------------------------------------

macro_rules! scalar_get_set {
    ($get:ident, $set:ident, $field:ident, $ty:ty) => {
        pub fn $get() -> $ty {
            xs(|s| s.$field)
        }
        pub fn $set(value: $ty) {
            xs(|s| s.$field = value)
        }
    };
}

scalar_get_set!(
    DefaultXactIsoLevel,
    SetDefaultXactIsoLevel,
    DefaultXactIsoLevel,
    i32
);
scalar_get_set!(XactIsoLevel, SetXactIsoLevel, XactIsoLevel, i32);
scalar_get_set!(
    DefaultXactReadOnly,
    SetDefaultXactReadOnly,
    DefaultXactReadOnly,
    bool
);
scalar_get_set!(XactReadOnly, SetXactReadOnly, XactReadOnly, bool);
scalar_get_set!(
    DefaultXactDeferrable,
    SetDefaultXactDeferrable,
    DefaultXactDeferrable,
    bool
);
scalar_get_set!(XactDeferrable, SetXactDeferrable, XactDeferrable, bool);
scalar_get_set!(
    synchronous_commit,
    SetSynchronousCommit,
    synchronous_commit,
    i32
);
scalar_get_set!(CheckXidAlive, SetCheckXidAlive, CheckXidAlive, TransactionId);
scalar_get_set!(bsysscan, SetBsysscan, bsysscan, bool);
scalar_get_set!(MyXactFlags, SetMyXactFlags, MyXactFlags, i32);
scalar_get_set!(xact_is_sampled, SetXactIsSampled, xact_is_sampled, bool);

pub fn IsolationUsesXactSnapshot() -> bool {
    XactIsoLevel() >= XACT_REPEATABLE_READ
}

pub fn IsolationIsSerializable() -> bool {
    XactIsoLevel() == XACT_SERIALIZABLE
}

// ---------------------------------------------------------------------------
//  Predicates (xact.c:386-417)
// ---------------------------------------------------------------------------

/// `IsTransactionState` (xact.c:387). TRANS_INPROGRESS only: the transaction
/// is not considered valid during start/commit/abort processing.
pub fn IsTransactionState() -> bool {
    xs(|s| s.current().state == TRANS_INPROGRESS)
}

/// `IsAbortedTransactionBlockState` (xact.c:407)
pub fn IsAbortedTransactionBlockState() -> bool {
    matches!(cur_block_state(), TBLOCK_ABORT | TBLOCK_SUBABORT)
}

// ---------------------------------------------------------------------------
//  XID getters / AssignTransactionId (xact.c:425-785)
// ---------------------------------------------------------------------------

/// `GetTopTransactionId` (xact.c:426) — assigns one if not yet set.
pub fn GetTopTransactionId() -> PgResult<TransactionId> {
    if !GetTopFullTransactionIdIfAny().is_valid() {
        assign_transaction_id_at(0)?;
    }
    Ok(GetTopTransactionIdIfAny())
}

/// `GetTopTransactionIdIfAny` (xact.c:441)
pub fn GetTopTransactionIdIfAny() -> TransactionId {
    xs(|s| s.xact_top_full_transaction_id.xid())
}

/// `GetCurrentTransactionId` (xact.c:454)
pub fn GetCurrentTransactionId() -> PgResult<TransactionId> {
    if !GetCurrentFullTransactionIdIfAny().is_valid() {
        AssignTransactionId()?;
    }
    Ok(GetCurrentTransactionIdIfAny())
}

/// `GetCurrentTransactionIdIfAny` (xact.c:471)
pub fn GetCurrentTransactionIdIfAny() -> TransactionId {
    xs(|s| s.current().full_transaction_id.xid())
}

/// `GetTopFullTransactionId` (xact.c:483)
pub fn GetTopFullTransactionId() -> PgResult<FullTransactionId> {
    if !GetTopFullTransactionIdIfAny().is_valid() {
        assign_transaction_id_at(0)?;
    }
    Ok(GetTopFullTransactionIdIfAny())
}

/// `GetTopFullTransactionIdIfAny` (xact.c:499)
pub fn GetTopFullTransactionIdIfAny() -> FullTransactionId {
    xs(|s| s.xact_top_full_transaction_id)
}

/// `GetCurrentFullTransactionId` (xact.c:512)
pub fn GetCurrentFullTransactionId() -> PgResult<FullTransactionId> {
    if !GetCurrentFullTransactionIdIfAny().is_valid() {
        AssignTransactionId()?;
    }
    Ok(GetCurrentFullTransactionIdIfAny())
}

/// `GetCurrentFullTransactionIdIfAny` (xact.c:530)
pub fn GetCurrentFullTransactionIdIfAny() -> FullTransactionId {
    xs(|s| s.current().full_transaction_id)
}

/// `MarkCurrentTransactionIdLoggedIfAny` (xact.c:541)
pub fn MarkCurrentTransactionIdLoggedIfAny() {
    xs(|s| {
        if s.current().full_transaction_id.is_valid() {
            s.current_mut().did_log_xid = true;
        }
    });
}

/// `IsSubxactTopXidLogPending` (xact.c:559)
pub fn IsSubxactTopXidLogPending() -> bool {
    // check whether it has already been logged
    if xs(|s| s.current().top_xid_logged) {
        return false;
    }
    // wal_level has to be logical
    if !xlog_seams::xlog_logical_info_active::call() {
        return false;
    }
    xs(|s| {
        // we need to be in a transaction state
        if s.current().state != TRANS_INPROGRESS {
            return false;
        }
        // it has to be a subtransaction
        if !s.is_subxact() {
            return false;
        }
        // the subtransaction has to have a XID assigned
        s.current().full_transaction_id.is_valid()
    })
}

/// `MarkSubxactTopXidLogged` (xact.c:591)
pub fn MarkSubxactTopXidLogged() {
    debug_assert!(IsSubxactTopXidLogPending());
    xs(|s| s.current_mut().top_xid_logged = true);
}

/// `GetStableLatestTransactionId` (xact.c:607)
///
/// Get the transaction's XID if it has one, else read the next-to-be-assigned
/// XID; latch the value for the rest of the transaction (keyed on
/// `MyProc->vxid.lxid` changing). Reference point for `age(xid)`.
pub fn GetStableLatestTransactionId() -> PgResult<TransactionId> {
    let my_lxid = proc_seams::my_proc_lxid::call();
    let cached = xs(|s| {
        if s.stable_latest.0 == my_lxid {
            Some(s.stable_latest.1)
        } else {
            None
        }
    });
    if let Some(stablexid) = cached {
        debug_assert!(stablexid != InvalidTransactionId);
        return Ok(stablexid);
    }
    let mut stablexid = GetTopTransactionIdIfAny();
    if stablexid == InvalidTransactionId {
        stablexid = varsup_seams::read_next_transaction_id::call();
    }
    debug_assert!(stablexid != InvalidTransactionId);
    xs(|s| s.stable_latest = (my_lxid, stablexid));
    Ok(stablexid)
}

/// `AssignTransactionId` (xact.c:635) on the current state node.
pub fn AssignTransactionId() -> PgResult<()> {
    let idx = xs(|s| s.transaction_stack.len() - 1);
    assign_transaction_id_at(idx)
}

/// Core of `AssignTransactionId`, on stack index `idx` (the C argument `s`).
fn assign_transaction_id_at(idx: usize) -> PgResult<()> {
    let is_subxact = idx > 0;

    debug_assert!(!xs(|s| s.transaction_stack[idx].full_transaction_id.is_valid()));
    debug_assert!(xs(|s| s.transaction_stack[idx].state == TRANS_INPROGRESS));

    // Workers synchronize transaction state at the beginning of each parallel
    // operation, so we can't account for new XIDs at this point.
    if IsInParallelMode() || parallel_seams::is_parallel_worker::call() {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
            .errmsg("cannot assign transaction IDs during a parallel operation")
            .finish(xact_location("AssignTransactionId"));
    }

    // Ensure parent(s) have XIDs, so that a child always has an XID later
    // than its parent. Iterate up to the highest unassigned parent, then
    // assign down (C avoids deep recursion the same way).
    if is_subxact
        && !xs(|s| s.transaction_stack[idx - 1].full_transaction_id.is_valid())
    {
        let mut parents = Vec::new();
        let mut p = idx;
        while p > 0 && !xs(|s| s.transaction_stack[p - 1].full_transaction_id.is_valid()) {
            parents.push(p - 1);
            p -= 1;
        }
        while let Some(parent_idx) = parents.pop() {
            assign_transaction_id_at(parent_idx)?;
        }
    }

    // When wal_level=logical, guarantee that a subtransaction's xid can only
    // be seen in WAL if its toplevel xid has been logged before.
    let log_unknown_top = is_subxact
        && xlog_seams::xlog_logical_info_active::call()
        && !xs(|s| s.transaction_stack[0].did_log_xid);

    // Generate a new FullTransactionId and record its xid in PGPROC and
    // pg_subtrans (the subtrans entry must exist before the XID appears in
    // shared storage beyond PGPROC; GetNewTransactionId handles the PGPROC
    // side).
    let full = varsup_seams::get_new_transaction_id::call(is_subxact)?;
    xs(|s| {
        s.transaction_stack[idx].full_transaction_id = full;
        if !is_subxact {
            s.xact_top_full_transaction_id = full;
        }
    });

    if is_subxact {
        let parent_xid = xs(|s| s.transaction_stack[idx - 1].full_transaction_id.xid());
        backend_access_transam_subtrans_seams::sub_trans_set_parent::call(full.xid(), parent_xid)?;
    }

    // Top-level transaction: tell the predicate locking system too.
    if !is_subxact {
        predicate_seams::register_predicate_locking_xid::call(full.xid())?;
    }

    // Acquire lock on the transaction XID. (C swaps CurrentResourceOwner to
    // the xact's own owner around this; owners dissolve here.)
    lmgr_seams::xact_lock_table_insert::call(full.xid())?;

    // Every PGPROC_MAX_CACHED_SUBXIDS assigned xids within a top-level
    // transaction, issue a WAL record for the assignment (hot-standby
    // KnownAssignedXids bookkeeping).
    if is_subxact && xlog_seams::xlog_standby_info_active::call() {
        xs(|s| {
            s.unreported_xids
                .try_reserve(1)
                .map_err(|_| PgError::error("out of memory tracking unreported subtransaction IDs"))?;
            s.unreported_xids.push(full.xid());
            Ok::<(), PgError>(())
        })?;

        // ensure this test matches the one in RecoverPreparedTransactions()
        if xs(|s| s.unreported_xids.len()) >= PGPROC_MAX_CACHED_SUBXIDS || log_unknown_top {
            // xtop is always set by now: we recursed up the stack first.
            let xtop = GetTopTransactionId()?;
            debug_assert!(xtop != InvalidTransactionId);
            let subxids = xs(|s| s.unreported_xids.clone());

            // xl_xact_assignment { TransactionId xtop; int nsubxacts;
            //                      TransactionId xsub[FLEXIBLE_ARRAY_MEMBER]; }
            xloginsert_seams::xlog_begin_insert::call()?;
            let mut hdr = [0u8; 8];
            hdr[0..4].copy_from_slice(&xtop.to_ne_bytes());
            hdr[4..8].copy_from_slice(&(subxids.len() as i32).to_ne_bytes());
            xloginsert_seams::xlog_register_data::call(&hdr)?;
            let mut body: Vec<u8> = Vec::new();
            body.try_reserve(subxids.len() * 4)
                .map_err(|_| PgError::error("out of memory building xid-assignment record"))?;
            for x in &subxids {
                body.extend_from_slice(&x.to_ne_bytes());
            }
            xloginsert_seams::xlog_register_data::call(&body)?;
            xloginsert_seams::xlog_insert::call(types_wal::RM_XACT_ID, types_wal::XLOG_XACT_ASSIGNMENT)?;

            xs(|s| {
                s.unreported_xids.clear();
                // mark top, not current xact as having been logged
                s.transaction_stack[0].did_log_xid = true;
            });
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
//  Sub-transaction / command id / timestamps (xact.c:790-933)
// ---------------------------------------------------------------------------

/// `GetCurrentSubTransactionId` (xact.c:791)
pub fn GetCurrentSubTransactionId() -> SubTransactionId {
    xs(|s| s.current().sub_transaction_id)
}

/// `SubTransactionIsActive` (xact.c:805)
pub fn SubTransactionIsActive(subxid: SubTransactionId) -> bool {
    xs(|s| {
        for node in s.transaction_stack.iter().rev() {
            if node.state == TRANS_ABORT {
                continue;
            }
            if node.sub_transaction_id == subxid {
                return true;
            }
        }
        false
    })
}

/// `GetCurrentCommandId` (xact.c:829)
pub fn GetCurrentCommandId(used: bool) -> PgResult<CommandId> {
    // this is global to a transaction, not subtransaction-local
    if used {
        // Forbid setting currentCommandIdUsed in a parallel worker: there is
        // no provision for communicating this back to the leader.
        if parallel_seams::is_parallel_worker::call() {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
                .errmsg("cannot modify data in a parallel worker")
                .finish(xact_location("GetCurrentCommandId"))
                .map(|()| InvalidCommandId);
        }
        xs(|s| s.current_command_id_used = true);
    }
    Ok(xs(|s| s.current_command_id))
}

/// `SetParallelStartTimestamps` (xact.c:859)
pub fn SetParallelStartTimestamps(xact_ts: TimestampTz, stmt_ts: TimestampTz) {
    debug_assert!(parallel_seams::is_parallel_worker::call());
    xs(|s| {
        s.xact_start_timestamp = xact_ts;
        s.stmt_start_timestamp = stmt_ts;
    });
}

/// `GetCurrentTransactionStartTimestamp` (xact.c:870)
pub fn GetCurrentTransactionStartTimestamp() -> TimestampTz {
    xs(|s| s.xact_start_timestamp)
}

/// `GetCurrentStatementStartTimestamp` (xact.c:879)
pub fn GetCurrentStatementStartTimestamp() -> TimestampTz {
    xs(|s| s.stmt_start_timestamp)
}

/// `GetCurrentTransactionStopTimestamp` (xact.c:891) — sets it if unset.
pub fn GetCurrentTransactionStopTimestamp() -> TimestampTz {
    if xs(|s| s.xact_stop_timestamp) == 0 {
        let ts = timestamp_seams::get_current_timestamp::call();
        xs(|s| s.xact_stop_timestamp = ts);
    }
    xs(|s| s.xact_stop_timestamp)
}

/// `SetCurrentStatementStartTimestamp` (xact.c:914)
pub fn SetCurrentStatementStartTimestamp() {
    if !parallel_seams::is_parallel_worker::call() {
        let ts = timestamp_seams::get_current_timestamp::call();
        xs(|s| s.stmt_start_timestamp = ts);
    } else {
        debug_assert!(xs(|s| s.stmt_start_timestamp) != 0);
    }
}

/// `GetCurrentTransactionNestLevel` (xact.c:929)
pub fn GetCurrentTransactionNestLevel() -> i32 {
    xs(|s| s.current().nesting_level)
}

/// `TransactionIdIsCurrentTransactionId` (xact.c:941)
pub fn TransactionIdIsCurrentTransactionId(xid: TransactionId) -> bool {
    // Any non-normal XID (Invalid/Bootstrap/Frozen) is certainly not mine.
    if !transaction_id_is_normal(xid) {
        return false;
    }

    if xid == GetTopTransactionIdIfAny() {
        return true;
    }

    xs(|s| {
        // Parallel workers: the XIDs to consider current are in
        // ParallelCurrentXids, sorted numerically.
        if !s.parallel_current_xids.is_empty() {
            return s.parallel_current_xids.binary_search(&xid).is_ok();
        }

        // Current subxact, its subcommitted children, its parents, and their
        // previously-subcommitted children. An aborting node is not current.
        for node in s.transaction_stack.iter().rev() {
            if node.state == TRANS_ABORT {
                continue;
            }
            if !node.full_transaction_id.is_valid() {
                continue; // it can't have any child XIDs either
            }
            if xid == node.full_transaction_id.xid() {
                return true;
            }
            // childXids is in TransactionIdPrecedes order; binary search.
            if binary_search_xids(&node.child_xids, xid) {
                return true;
            }
        }
        false
    })
}

/// `TransactionIdIsNormal` (transam.h)
fn transaction_id_is_normal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
}

/// Binary search of `childXids` in `TransactionIdPrecedes` order.
fn binary_search_xids(child_xids: &[TransactionId], xid: TransactionId) -> bool {
    let mut low: isize = 0;
    let mut high: isize = child_xids.len() as isize - 1;
    while low <= high {
        let middle = low + (high - low) / 2;
        let probe = child_xids[middle as usize];
        if probe == xid {
            return true;
        } else if transaction_id_precedes(probe, xid) {
            low = middle + 1;
        } else {
            high = middle - 1;
        }
    }
    false
}

/// `TransactionIdPrecedes` (transam.c) — modulo-2^32 circular comparison.
pub(crate) fn transaction_id_precedes(a: TransactionId, b: TransactionId) -> bool {
    if !transaction_id_is_normal(a) || !transaction_id_is_normal(b) {
        return a < b;
    }
    (a.wrapping_sub(b) as i32) < 0
}

/// `TransactionStartedDuringRecovery` (xact.c:1042)
pub fn TransactionStartedDuringRecovery() -> bool {
    xs(|s| s.current().started_in_recovery)
}

// ---------------------------------------------------------------------------
//  Parallel mode (xact.c:1051-1093)
// ---------------------------------------------------------------------------

/// `EnterParallelMode` (xact.c:1051)
pub fn EnterParallelMode() {
    xs(|s| {
        debug_assert!(s.current().parallel_mode_level >= 0);
        s.current_mut().parallel_mode_level += 1;
    });
}

/// `ExitParallelMode` (xact.c:1064). (C also asserts
/// `!ParallelContextActive()` when leaving the last level.)
pub fn ExitParallelMode() {
    xs(|s| {
        debug_assert!(s.current().parallel_mode_level > 0);
        s.current_mut().parallel_mode_level -= 1;
    });
}

/// `IsInParallelMode` (xact.c:1089)
pub fn IsInParallelMode() -> bool {
    xs(|s| s.current().parallel_mode_level != 0 || s.current().parallel_child_xact)
}

// ---------------------------------------------------------------------------
//  CommandCounterIncrement (xact.c:1100)
// ---------------------------------------------------------------------------

/// `CommandCounterIncrement` (xact.c:1100)
pub fn CommandCounterIncrement() -> PgResult<()> {
    // If the current command counter value hasn't been "used" to mark tuples,
    // we need not increment it.
    if !xs(|s| s.current_command_id_used) {
        return Ok(());
    }

    // Workers synchronize transaction state at the beginning of each parallel
    // operation, so we can't account for new commands after that point.
    if IsInParallelMode() || parallel_seams::is_parallel_worker::call() {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
            .errmsg("cannot start commands during a parallel operation")
            .finish(xact_location("CommandCounterIncrement"));
    }

    let next = xs(|s| {
        s.current_command_id += 1;
        if s.current_command_id == InvalidCommandId {
            s.current_command_id -= 1;
            return None;
        }
        s.current_command_id_used = false;
        Some(s.current_command_id)
    });
    let Some(next) = next else {
        return ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg("cannot have more than 2^32-2 commands in a transaction")
            .finish(xact_location("CommandCounterIncrement"));
    };

    // Propagate new command ID into static snapshots.
    snapmgr_seams::snapshot_set_command_id::call(next);

    // Make catalog changes done by the just-completed command visible in the
    // local syscache.
    AtCCI_LocalCache()?;
    Ok(())
}

/// `ForceSyncCommit` (xact.c:1152)
pub fn ForceSyncCommit() {
    xs(|s| s.force_sync_commit = true);
}

// ---------------------------------------------------------------------------
//  AtStart / AtSubStart hooks (xact.c:1167-1299)
// ---------------------------------------------------------------------------

/// `AtStart_Cache` (xact.c:1167)
pub(crate) fn AtStart_Cache() -> PgResult<()> {
    inval_seams::accept_invalidation_messages::call()
}

/// `AtStart_Memory` (xact.c:1176). First time through, create
/// TransactionAbortContext and TopTransactionContext; in a top-level
/// transaction CurTransactionContext IS TopTransactionContext. (The
/// `priorContext` save + MemoryContextSwitchTo dissolve — no ambient context.)
pub(crate) fn AtStart_Memory() {
    xs(|s| {
        if s.transaction_abort_context.is_none() {
            s.transaction_abort_context = Some(MemoryContext::new("TransactionAbortContext"));
        }
        if s.top_transaction_context.is_none() {
            s.top_transaction_context = Some(MemoryContext::new("TopTransactionContext"));
        }
    });
}

/// `AtStart_ResourceOwner` (xact.c:1226). The owner value dissolves into RAII
/// guards (docs/query-lifecycle-raii.md); the flag preserves the
/// `if (s->curTransactionOwner)` control flow.
pub(crate) fn AtStart_ResourceOwner() {
    xs(|s| {
        debug_assert!(!s.current().has_resource_owner);
        s.current_mut().has_resource_owner = true;
    });
}

/// `AtSubStart_Memory` (xact.c:1254) — create the subxact's
/// CurTransactionContext as a child of the immediate parent's.
pub(crate) fn AtSubStart_Memory() {
    xs(|s| {
        let idx = s.transaction_stack.len() - 1;
        debug_assert!(idx > 0);
        let child = {
            let parent_ctx = s.transaction_stack[idx - 1]
                .cur_transaction_context
                .as_ref()
                .or(s.top_transaction_context.as_ref())
                .expect("CurTransactionContext exists for the parent");
            parent_ctx.new_child("CurTransactionContext")
        };
        s.transaction_stack[idx].cur_transaction_context = Some(child);
    });
}

/// `AtSubStart_ResourceOwner` (xact.c:1283) — see `AtStart_ResourceOwner`.
pub(crate) fn AtSubStart_ResourceOwner() {
    xs(|s| {
        debug_assert!(s.is_subxact());
        s.current_mut().has_resource_owner = true;
    });
}

/// `AtCCI_LocalCache` (xact.c:1579)
fn AtCCI_LocalCache() -> PgResult<()> {
    // Make any pending relation map changes visible BEFORE processing local
    // sinval messages, so the map changes reach the relcache on inval.
    relmapper_seams::at_cci_relation_map::call();
    // Make catalog changes visible to me for the next command.
    inval_seams::command_end_invalidation_messages::call()
}

/// `AtCommit_Memory` (xact.c:1598) — release all transaction-local memory;
/// TopTransactionContext survives but becomes empty.
pub(crate) fn AtCommit_Memory() {
    xs(|s| {
        s.transaction_stack[0].retained_child_contexts.clear();
        if let Some(ctx) = s.top_transaction_context.as_mut() {
            ctx.reset();
        }
    });
}

/// `AtSubCommit_Memory` (xact.c:1635) — return to the parent's context;
/// delete the child's CurTransactionContext if empty, else keep it alive (in
/// C it survives as a child of the parent until top-level end).
pub(crate) fn AtSubCommit_Memory() -> PgResult<()> {
    xs(|s| {
        let idx = s.transaction_stack.len() - 1;
        debug_assert!(idx > 0);
        if let Some(ctx) = s.transaction_stack[idx].cur_transaction_context.take() {
            if ctx.subtree_used() == 0 {
                drop(ctx); // MemoryContextDelete of a trivial subxact context
            } else {
                let parent = &mut s.transaction_stack[idx - 1];
                parent
                    .retained_child_contexts
                    .try_reserve(1)
                    .map_err(|_| PgError::error("out of memory keeping subtransaction context"))?;
                parent.retained_child_contexts.push(ctx);
            }
        }
        // Retained grandchildren ride along to the parent too.
        let mut kept = std::mem::take(&mut s.transaction_stack[idx].retained_child_contexts);
        let parent = &mut s.transaction_stack[idx - 1];
        parent
            .retained_child_contexts
            .try_reserve(kept.len())
            .map_err(|_| PgError::error("out of memory keeping subtransaction context"))?;
        parent.retained_child_contexts.append(&mut kept);
        Ok(())
    })
}

/// `AtSubCommit_childXids` (xact.c:1664) — pass my XID + child XIDs up to the
/// parent as committed children, keeping the array ordered (my XID precedes
/// my children's; all existing entries precede mine). Enforces MaxAllocSize.
pub(crate) fn AtSubCommit_childXids() -> PgResult<()> {
    xs(|s| {
        let idx = s.transaction_stack.len() - 1;
        debug_assert!(idx > 0);

        let my_full = s.transaction_stack[idx].full_transaction_id;
        let my_children = std::mem::take(&mut s.transaction_stack[idx].child_xids);

        let new_n = s.transaction_stack[idx - 1].child_xids.len() + my_children.len() + 1;
        let max_children = MAX_ALLOC_SIZE / std::mem::size_of::<TransactionId>();
        if new_n > max_children {
            return ereport(ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg(format!(
                    "maximum number of committed subtransactions ({max_children}) exceeded"
                ))
                .finish(xact_location("AtSubCommit_childXids"));
        }

        let parent = &mut s.transaction_stack[idx - 1].child_xids;
        parent
            .try_reserve(my_children.len() + 1)
            .map_err(|_| PgError::error("out of memory recording committed subtransactions"))?;
        parent.push(my_full.xid());
        parent.extend_from_slice(&my_children);
        Ok(())
    })
}

/// `AtAbort_Memory` (xact.c:1884) — C switches into TransactionAbortContext
/// (or TopMemoryContext); with no ambient context, ensure the abort context
/// exists for any abort-path allocations routed at it.
pub(crate) fn AtAbort_Memory() {
    xs(|s| {
        if s.transaction_abort_context.is_none() {
            s.transaction_abort_context = Some(MemoryContext::new("TransactionAbortContext"));
        }
    });
}

/// `AtSubAbort_Memory` (xact.c:1904)
pub(crate) fn AtSubAbort_Memory() {
    debug_assert!(xs(|s| s.transaction_abort_context.is_some()));
}

/// `AtAbort_ResourceOwner` (xact.c:1916) — `CurrentResourceOwner =
/// TopTransactionResourceOwner` dissolves with the ambient owner.
pub(crate) fn AtAbort_ResourceOwner() {}

/// `AtSubAbort_ResourceOwner` (xact.c:1929) — likewise.
pub(crate) fn AtSubAbort_ResourceOwner() {}

/// `AtSubAbort_childXids` (xact.c:1942)
pub(crate) fn AtSubAbort_childXids() {
    xs(|s| {
        // (We could prune the unreportedXids array here, but C doesn't bother.)
        s.current_mut().child_xids = Vec::new();
    });
}

/// `AtCleanup_Memory` (xact.c:1974) — clear the abort context and release all
/// transaction-local memory (TopTransactionContext may not exist if startup
/// failed early).
pub(crate) fn AtCleanup_Memory() {
    xs(|s| {
        debug_assert_eq!(s.transaction_stack.len(), 1);
        if let Some(ctx) = s.transaction_abort_context.as_mut() {
            ctx.reset();
        }
        s.transaction_stack[0].retained_child_contexts.clear();
        if let Some(ctx) = s.top_transaction_context.as_mut() {
            ctx.reset();
        }
    });
}

/// `AtSubCleanup_Memory` (xact.c:2022) — delete the subxact's local contexts
/// (including any retained from its own children).
pub(crate) fn AtSubCleanup_Memory() {
    xs(|s| {
        let idx = s.transaction_stack.len() - 1;
        debug_assert!(idx > 0);
        if let Some(ctx) = s.transaction_abort_context.as_mut() {
            ctx.reset();
        }
        s.transaction_stack[idx].cur_transaction_context = None;
        s.transaction_stack[idx].retained_child_contexts.clear();
    });
}

// ---------------------------------------------------------------------------
//  Callbacks (xact.c:3804-3911)
// ---------------------------------------------------------------------------

/// `RegisterXactCallback` (xact.c:3804). The C `(callback, arg)` pair is keyed
/// here by a caller-chosen `key` for unregistration.
pub fn RegisterXactCallback(key: usize, callback: impl FnMut(u32) -> PgResult<()> + 'static) {
    xs(|s| {
        s.xact_callbacks.push(XactCallbackRegistration {
            key,
            callback: Box::new(callback),
        })
    });
}

/// `UnregisterXactCallback` (xact.c:3817)
pub fn UnregisterXactCallback(key: usize) {
    xs(|s| s.xact_callbacks.retain(|item| item.key != key));
}

/// `RegisterSubXactCallback` (xact.c:3864)
pub fn RegisterSubXactCallback(
    key: usize,
    callback: impl FnMut(u32, SubTransactionId, SubTransactionId) -> PgResult<()> + 'static,
) {
    xs(|s| {
        s.subxact_callbacks.push(SubXactCallbackRegistration {
            key,
            callback: Box::new(callback),
        })
    });
}

/// `UnregisterSubXactCallback` (xact.c:3877)
pub fn UnregisterSubXactCallback(key: usize) {
    xs(|s| s.subxact_callbacks.retain(|item| item.key != key));
}

/// `CallXactCallbacks` (xact.c:3838) — index walk over the live list, so
/// callbacks registered mid-iteration are still seen (matches the C
/// `next = item->next` walk).
pub(crate) fn CallXactCallbacks(event: u32) -> PgResult<()> {
    let mut i = 0;
    loop {
        let cb = xs(|s| {
            if i >= s.xact_callbacks.len() {
                None
            } else {
                Some(std::mem::replace(
                    &mut s.xact_callbacks[i].callback,
                    Box::new(|_| Ok(())),
                ))
            }
        });
        let Some(mut cb) = cb else { break };
        let result = (cb)(event);
        xs(|s| {
            if i < s.xact_callbacks.len() {
                s.xact_callbacks[i].callback = cb;
            }
        });
        result?;
        i += 1;
    }
    Ok(())
}

/// `CallSubXactCallbacks` (xact.c:3898)
pub(crate) fn CallSubXactCallbacks(
    event: u32,
    my_subid: SubTransactionId,
    parent_subid: SubTransactionId,
) -> PgResult<()> {
    let mut i = 0;
    loop {
        let cb = xs(|s| {
            if i >= s.subxact_callbacks.len() {
                None
            } else {
                Some(std::mem::replace(
                    &mut s.subxact_callbacks[i].callback,
                    Box::new(|_, _, _| Ok(())),
                ))
            }
        });
        let Some(mut cb) = cb else { break };
        let result = (cb)(event, my_subid, parent_subid);
        xs(|s| {
            if i < s.subxact_callbacks.len() {
                s.subxact_callbacks[i].callback = cb;
            }
        });
        result?;
        i += 1;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
//  xactGetCommittedChildren (xact.c:5790)
// ---------------------------------------------------------------------------

/// `xactGetCommittedChildren` (xact.c:5790) — committed children of the
/// current transaction (C hands out the in-place array; we clone).
pub fn xactGetCommittedChildren() -> Vec<TransactionId> {
    xs(|s| s.current().child_xids.clone())
}

// ---------------------------------------------------------------------------
//  Helpers shared across the crate
// ---------------------------------------------------------------------------

pub(crate) fn xact_location(function: &'static str) -> ErrorLocation {
    ErrorLocation::new("xact.c", 0, function)
}

pub(crate) fn unexpected_block_state(function: &str, st: TBlockState) -> PgError {
    PgError::new(
        FATAL,
        format!("{function}: unexpected state {}", BlockStateAsString(st)),
    )
}

/// `elog(WARNING, ...)` — emitted through the elog crate; WARNING returns.
pub(crate) fn warn_internal(msg: &str) {
    let _ = elog(WARNING, msg.to_owned());
}

/// `ShowTransactionState` (xact.c:5648) — DEBUG5 dump of the state stack.
pub(crate) fn ShowTransactionState(str: &str) {
    // skip work if message will definitely not be printed
    if message_level_is_interesting(DEBUG5) {
        ShowTransactionStateRec(str);
    }
}

/// `ShowTransactionStateRec` (xact.c:5660). C recurses up the parent chain,
/// printing the parent first; the stack here is `[0]==top`, so front-to-back
/// iteration yields the same order (no stack-depth guard needed).
fn ShowTransactionStateRec(str: &str) {
    let lines = xs(|s| {
        s.transaction_stack
            .iter()
            .map(|node| {
                let mut buf = String::new();
                if !node.child_xids.is_empty() {
                    buf.push_str(&format!(", children: {}", node.child_xids[0]));
                    for xid in &node.child_xids[1..] {
                        buf.push_str(&format!(" {xid}"));
                    }
                }
                format!(
                    "{}({}) name: {}; blockState: {}; state: {}, xid/subid/cid: {}/{}/{}{}{}",
                    str,
                    node.nesting_level,
                    node.name.as_deref().unwrap_or("unnamed"),
                    BlockStateAsString(node.block_state),
                    TransStateAsString(node.state),
                    node.full_transaction_id.xid(),
                    node.sub_transaction_id,
                    s.current_command_id,
                    if s.current_command_id_used { " (used)" } else { "" },
                    buf,
                )
            })
            .collect::<Vec<_>>()
    });
    for line in lines {
        let _ = ereport(DEBUG5)
            .errmsg_internal(line)
            .finish(xact_location("ShowTransactionStateRec"));
    }
}

/// `BlockStateAsString` (xact.c:5707)
pub fn BlockStateAsString(state: TBlockState) -> &'static str {
    match state {
        TBLOCK_DEFAULT => "DEFAULT",
        TBLOCK_STARTED => "STARTED",
        TBLOCK_BEGIN => "BEGIN",
        TBLOCK_INPROGRESS => "INPROGRESS",
        TBLOCK_IMPLICIT_INPROGRESS => "IMPLICIT_INPROGRESS",
        TBLOCK_PARALLEL_INPROGRESS => "PARALLEL_INPROGRESS",
        TBLOCK_END => "END",
        TBLOCK_ABORT => "ABORT",
        TBLOCK_ABORT_END => "ABORT_END",
        TBLOCK_ABORT_PENDING => "ABORT_PENDING",
        TBLOCK_PREPARE => "PREPARE",
        TBLOCK_SUBBEGIN => "SUBBEGIN",
        TBLOCK_SUBINPROGRESS => "SUBINPROGRESS",
        TBLOCK_SUBRELEASE => "SUBRELEASE",
        TBLOCK_SUBCOMMIT => "SUBCOMMIT",
        TBLOCK_SUBABORT => "SUBABORT",
        TBLOCK_SUBABORT_END => "SUBABORT_END",
        TBLOCK_SUBABORT_PENDING => "SUBABORT_PENDING",
        TBLOCK_SUBRESTART => "SUBRESTART",
        TBLOCK_SUBABORT_RESTART => "SUBABORT_RESTART",
        _ => "UNRECOGNIZED",
    }
}

/// `TransStateAsString` (xact.c:5760)
pub fn TransStateAsString(state: TransState) -> &'static str {
    match state {
        TRANS_DEFAULT => "DEFAULT",
        TRANS_START => "START",
        TRANS_INPROGRESS => "INPROGRESS",
        TRANS_COMMIT => "COMMIT",
        TRANS_ABORT => "ABORT",
        TRANS_PREPARE => "PREPARE",
        _ => "UNRECOGNIZED",
    }
}

// ---------------------------------------------------------------------------
//  Predicates over the block state (xact.c:3648-3789, 4971-5044)
// ---------------------------------------------------------------------------

/// `PreventInTransactionBlock` (xact.c:3648)
pub fn PreventInTransactionBlock(isTopLevel: bool, stmtType: &str) -> PgResult<()> {
    // xact block?
    if IsTransactionBlock() {
        return ereport(ERROR)
            .errcode(ERRCODE_ACTIVE_SQL_TRANSACTION)
            // translator: %s represents an SQL statement name
            .errmsg(format!("{stmtType} cannot run inside a transaction block"))
            .finish(xact_location("PreventInTransactionBlock"));
    }
    // subtransaction?
    if IsSubTransaction() {
        return ereport(ERROR)
            .errcode(ERRCODE_ACTIVE_SQL_TRANSACTION)
            .errmsg(format!("{stmtType} cannot run inside a subtransaction"))
            .finish(xact_location("PreventInTransactionBlock"));
    }
    // inside a function call?
    if !isTopLevel {
        return ereport(ERROR)
            .errcode(ERRCODE_ACTIVE_SQL_TRANSACTION)
            .errmsg(format!("{stmtType} cannot be executed from a function"))
            .finish(xact_location("PreventInTransactionBlock"));
    }
    // If we got past IsTransactionBlock test, should be in default state.
    let bs = cur_block_state();
    if bs != TBLOCK_DEFAULT && bs != TBLOCK_STARTED {
        return Err(PgError::new(FATAL, "cannot prevent transaction chain"));
    }
    // All okay. Set the flag to make sure the right thing happens later.
    xs(|s| s.MyXactFlags |= XACT_FLAGS_NEEDIMMEDIATECOMMIT);
    Ok(())
}

/// `WarnNoTransactionBlock` (xact.c:3710)
pub fn WarnNoTransactionBlock(isTopLevel: bool, stmtType: &str) -> PgResult<()> {
    CheckTransactionBlock(isTopLevel, false, stmtType)
}

/// `RequireTransactionBlock` (xact.c:3716)
pub fn RequireTransactionBlock(isTopLevel: bool, stmtType: &str) -> PgResult<()> {
    CheckTransactionBlock(isTopLevel, true, stmtType)
}

/// `CheckTransactionBlock` (xact.c:3725)
fn CheckTransactionBlock(isTopLevel: bool, throwError: bool, stmtType: &str) -> PgResult<()> {
    if IsTransactionBlock() {
        return Ok(());
    }
    if IsSubTransaction() {
        return Ok(());
    }
    if !isTopLevel {
        return Ok(());
    }
    ereport(if throwError { ERROR } else { WARNING })
        .errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION)
        // translator: %s represents an SQL statement name
        .errmsg(format!("{stmtType} can only be used in transaction blocks"))
        .finish(xact_location("CheckTransactionBlock"))
}

/// `IsInTransactionBlock` (xact.c:3769)
pub fn IsInTransactionBlock(isTopLevel: bool) -> bool {
    // Return true on the same conditions PreventInTransactionBlock errors on.
    if IsTransactionBlock() {
        return true;
    }
    if IsSubTransaction() {
        return true;
    }
    if !isTopLevel {
        return true;
    }
    let bs = cur_block_state();
    bs != TBLOCK_DEFAULT && bs != TBLOCK_STARTED
}

/// `IsTransactionBlock` (xact.c:4971)
pub fn IsTransactionBlock() -> bool {
    let bs = cur_block_state();
    !(bs == TBLOCK_DEFAULT || bs == TBLOCK_STARTED)
}

/// `IsTransactionOrTransactionBlock` (xact.c:4989)
pub fn IsTransactionOrTransactionBlock() -> bool {
    cur_block_state() != TBLOCK_DEFAULT
}

/// `TransactionBlockStatusCode` (xact.c:5003)
pub fn TransactionBlockStatusCode() -> char {
    match cur_block_state() {
        TBLOCK_DEFAULT | TBLOCK_STARTED => 'I', // idle, not in a block
        TBLOCK_BEGIN
        | TBLOCK_SUBBEGIN
        | TBLOCK_INPROGRESS
        | TBLOCK_IMPLICIT_INPROGRESS
        | TBLOCK_PARALLEL_INPROGRESS
        | TBLOCK_SUBINPROGRESS
        | TBLOCK_END
        | TBLOCK_SUBRELEASE
        | TBLOCK_SUBCOMMIT
        | TBLOCK_PREPARE => 'T', // in a transaction
        TBLOCK_ABORT
        | TBLOCK_SUBABORT
        | TBLOCK_ABORT_END
        | TBLOCK_SUBABORT_END
        | TBLOCK_ABORT_PENDING
        | TBLOCK_SUBABORT_PENDING
        | TBLOCK_SUBRESTART
        | TBLOCK_SUBABORT_RESTART => 'E', // in a failed transaction
        other => {
            // C: elog(FATAL, "invalid transaction block state: %s")
            panic!(
                "invalid transaction block state: {}",
                BlockStateAsString(other)
            );
        }
    }
}

/// `IsSubTransaction` (xact.c:5044)
pub fn IsSubTransaction() -> bool {
    xs(|s| s.current().nesting_level >= 2)
}

// ---------------------------------------------------------------------------
//  Seam installation
// ---------------------------------------------------------------------------

/// Install this crate's implementations into `backend-access-transam-xact-seams`.
pub fn init_seams() {
    backend_access_transam_xact_seams::command_counter_increment::set(CommandCounterIncrement);
    backend_access_transam_xact_seams::transaction_id_is_current_transaction_id::set(
        TransactionIdIsCurrentTransactionId,
    );
}

// Re-export the engine's public functions at the crate root (they are defined
// in `engine.rs` for file-size sanity; this is one C translation unit).
pub use engine::{
    BeginImplicitTransactionBlock, BeginInternalSubTransaction, BeginTransactionBlock,
    DefineSavepoint, EndImplicitTransactionBlock, EndTransactionBlock,
    EstimateTransactionStateSpace, PrepareTransactionBlock, ReleaseCurrentSubTransaction,
    ReleaseSavepoint, RestoreTransactionCharacteristics, RollbackAndReleaseCurrentSubTransaction,
    RollbackToSavepoint, SaveTransactionCharacteristics, SerializeTransactionState,
    UserAbortTransactionBlock,
};
