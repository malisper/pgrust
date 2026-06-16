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
//! * **`AtEOXact_ComboCid`** is called through combocid's
//!   `at_eoxact_combocid` seam at commit/prepare/abort (mirroring C's
//!   `xact.c:2473,2767,2991`); the combocid owner resets its own
//!   backend-local `thread_local!` combo-CID state. **`AtEOXact_HashTables`
//!   (and the sub-xact twins) dissolve**: dynahash seq-scan tracking does not
//!   exist over `PgHashMap`.
//! * Backend-local file statics are `thread_local!` (one backend = one
//!   thread), never shared statics.
//! * **Transaction-lifetime collections are std `Vec`/`String`, not
//!   `PgVec<'mcx>`** (ledgered divergence): C allocates `childXids`,
//!   savepoint names, `prepareGID`, `unreportedXids`, and
//!   `ParallelCurrentXids` in `TopTransactionContext`/`TopMemoryContext`;
//!   here they live directly in the `thread_local!` `XactState`, which cannot
//!   borrow the `top_transaction_context` it also owns (self-referential
//!   state). Every allocating touch of these collections goes through
//!   fallible `try_reserve`-style calls carrying C's OOM failure surface;
//!   what is lost is only the context accounting/reset coupling. Tracked in
//!   DESIGN_DEBT.md.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use std::cell::RefCell;

use backend_utils_error::{elog, ereport, message_level_is_interesting};
use mcx::MemoryContext;
use types_core::xact::*;
use types_core::{LocalTransactionId, TimestampTz, TransactionId, XLogRecPtr};
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
pub(crate) use backend_utils_misc_guc_file_seams as guc_seams;
pub(crate) use backend_utils_misc_guc_seams as guc_core_seams;
pub(crate) use backend_utils_time_combocid_seams as combocid_seams;
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
    /// Source of `XactCallbackRegistration::serial` values.
    pub next_callback_serial: u64,
    /// `TopTransactionContext` (mcxt.c global, managed by xact.c).
    pub top_transaction_context: Option<MemoryContext>,
    /// `TransactionAbortContext`.
    pub transaction_abort_context: Option<MemoryContext>,
}

type XactCallback = Box<dyn FnMut(XactEvent) -> PgResult<()>>;
type SubXactCallback =
    Box<dyn FnMut(SubXactEvent, SubTransactionId, SubTransactionId) -> PgResult<()>>;

/// Registration token returned by `RegisterXactCallback`; C identifies the
/// registration by its `(callback, arg)` pair, which dissolves into closure
/// capture here — the token is the registration's identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct XactCallbackToken(u64);

/// See `XactCallbackToken`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubXactCallbackToken(u64);

pub(crate) struct XactCallbackRegistration {
    /// Unique registration id; lets Call*Callbacks track entries across
    /// re-entrant register/unregister (the C list uses node identity).
    serial: u64,
    callback: XactCallback,
}

impl std::fmt::Debug for XactCallbackRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XactCallbackRegistration")
            .field("serial", &self.serial)
            .finish_non_exhaustive()
    }
}

pub(crate) struct SubXactCallbackRegistration {
    /// See `XactCallbackRegistration::serial`.
    serial: u64,
    callback: SubXactCallback,
}

impl std::fmt::Debug for SubXactCallbackRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubXactCallbackRegistration")
            .field("serial", &self.serial)
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
            next_callback_serial: 0,
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
        // C: parents = palloc(sizeof(TransactionState) * parentOffset)
        let mut parents = Vec::new();
        let mut p = idx;
        while p > 0 && !xs(|s| s.transaction_stack[p - 1].full_transaction_id.is_valid()) {
            parents
                .try_reserve(1)
                .map_err(|_| PgError::error("out of memory assigning transaction IDs"))?;
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
            let mut hdr = [0u8; 8];
            hdr[0..4].copy_from_slice(&xtop.to_ne_bytes());
            hdr[4..8].copy_from_slice(&(subxids.len() as i32).to_ne_bytes());
            let mut body: Vec<u8> = Vec::new();
            body.try_reserve(subxids.len() * 4)
                .map_err(|_| PgError::error("out of memory building xid-assignment record"))?;
            for x in &subxids {
                body.extend_from_slice(&x.to_ne_bytes());
            }
            // No XLogSetRecordFlags in the C path for assignment records, so
            // flags = 0; the two registered fragments are the header and the
            // subxid array.
            xloginsert_seams::xlog_insert::call(
                types_wal::RM_XACT_ID,
                types_wal::XLOG_XACT_ASSIGNMENT,
                0,
                &[&hdr, &body],
            )?;

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
    relmapper_seams::at_cci_relation_map::call()?;
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

/// `RegisterXactCallback` (xact.c:3804). C identifies a registration by its
/// `(callback, arg)` pair; the returned token is that identity here. New
/// registrations are prepended, as in C (`item->next = Xact_callbacks;
/// Xact_callbacks = item`), so callbacks run most-recently-registered first
/// and a callback registered during `CallXactCallbacks` is not invoked in
/// the same round.
pub fn RegisterXactCallback(
    callback: impl FnMut(XactEvent) -> PgResult<()> + 'static,
) -> XactCallbackToken {
    xs(|s| {
        let serial = s.next_callback_serial;
        s.next_callback_serial += 1;
        s.xact_callbacks.insert(
            0,
            XactCallbackRegistration {
                serial,
                callback: Box::new(callback),
            },
        );
        XactCallbackToken(serial)
    })
}

/// `UnregisterXactCallback` (xact.c:3817).
pub fn UnregisterXactCallback(token: XactCallbackToken) {
    xs(|s| {
        if let Some(pos) = s
            .xact_callbacks
            .iter()
            .position(|item| item.serial == token.0)
        {
            s.xact_callbacks.remove(pos);
        }
    });
}

/// `RegisterSubXactCallback` (xact.c:3864) — see `RegisterXactCallback`.
pub fn RegisterSubXactCallback(
    callback: impl FnMut(SubXactEvent, SubTransactionId, SubTransactionId) -> PgResult<()> + 'static,
) -> SubXactCallbackToken {
    xs(|s| {
        let serial = s.next_callback_serial;
        s.next_callback_serial += 1;
        s.subxact_callbacks.insert(
            0,
            SubXactCallbackRegistration {
                serial,
                callback: Box::new(callback),
            },
        );
        SubXactCallbackToken(serial)
    })
}

/// `UnregisterSubXactCallback` (xact.c:3877) — see `UnregisterXactCallback`.
pub fn UnregisterSubXactCallback(token: SubXactCallbackToken) {
    xs(|s| {
        if let Some(pos) = s
            .subxact_callbacks
            .iter()
            .position(|item| item.serial == token.0)
        {
            s.subxact_callbacks.remove(pos);
        }
    });
}

/// `CallXactCallbacks` (xact.c:3838) — snapshot the registration serials up
/// front and walk that snapshot, mirroring the C `next = item->next` walk:
/// callbacks may unregister themselves while being called (the entry just
/// disappears), and registrations made mid-iteration (prepended) are not
/// invoked this round.
pub(crate) fn CallXactCallbacks(event: XactEvent) -> PgResult<()> {
    let serials: Vec<u64> = xs(|s| {
        let mut serials = Vec::new();
        serials
            .try_reserve(s.xact_callbacks.len())
            .map_err(|_| PgError::error("out of memory calling transaction callbacks"))?;
        serials.extend(s.xact_callbacks.iter().map(|item| item.serial));
        Ok::<_, PgError>(serials)
    })?;
    for serial in serials {
        // Temporarily take the closure out so the callback can re-enter this
        // module (register/unregister) without holding the state borrow.
        let cb = xs(|s| {
            s.xact_callbacks
                .iter_mut()
                .find(|item| item.serial == serial)
                .map(|item| std::mem::replace(&mut item.callback, Box::new(|_| Ok(()))))
        });
        let Some(mut cb) = cb else { continue };
        let result = (cb)(event);
        xs(|s| {
            if let Some(item) = s
                .xact_callbacks
                .iter_mut()
                .find(|item| item.serial == serial)
            {
                item.callback = cb;
            }
        });
        result?;
    }
    Ok(())
}

/// `CallSubXactCallbacks` (xact.c:3898) — see `CallXactCallbacks`.
pub(crate) fn CallSubXactCallbacks(
    event: SubXactEvent,
    my_subid: SubTransactionId,
    parent_subid: SubTransactionId,
) -> PgResult<()> {
    let serials: Vec<u64> = xs(|s| {
        let mut serials = Vec::new();
        serials
            .try_reserve(s.subxact_callbacks.len())
            .map_err(|_| PgError::error("out of memory calling subtransaction callbacks"))?;
        serials.extend(s.subxact_callbacks.iter().map(|item| item.serial));
        Ok::<_, PgError>(serials)
    })?;
    for serial in serials {
        let cb = xs(|s| {
            s.subxact_callbacks
                .iter_mut()
                .find(|item| item.serial == serial)
                .map(|item| std::mem::replace(&mut item.callback, Box::new(|_, _, _| Ok(()))))
        });
        let Some(mut cb) = cb else { continue };
        let result = (cb)(event, my_subid, parent_subid);
        xs(|s| {
            if let Some(item) = s
                .subxact_callbacks
                .iter_mut()
                .find(|item| item.serial == serial)
            {
                item.callback = cb;
            }
        });
        result?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
//  xactGetCommittedChildren (xact.c:5790)
// ---------------------------------------------------------------------------

/// `xactGetCommittedChildren` (xact.c:5790) — committed children of the
/// current transaction (C hands out the in-place array; we copy fallibly).
pub fn xactGetCommittedChildren() -> PgResult<Vec<TransactionId>> {
    xs(|s| {
        let src = &s.current().child_xids;
        let mut out = Vec::new();
        out.try_reserve_exact(src.len())
            .map_err(|_| PgError::error("out of memory copying committed subtransactions"))?;
        out.extend_from_slice(src);
        Ok(out)
    })
}

// ---------------------------------------------------------------------------
//  Helpers shared across the crate
// ---------------------------------------------------------------------------

pub(crate) fn xact_location(function: &'static str) -> ErrorLocation {
    ErrorLocation::new("xact.c", 0, function)
}

/// Fallible string copy standing in for C's
/// `MemoryContextStrdup(TopTransactionContext, ...)` — palloc can
/// `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)`.
pub(crate) fn try_strdup(s: &str, what: &'static str) -> PgResult<String> {
    let mut out = String::new();
    out.try_reserve_exact(s.len())
        .map_err(|_| PgError::error(what))?;
    out.push_str(s);
    Ok(out)
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
                                           // C's elog(FATAL, "invalid transaction block state")
                                           // default arm is statically unreachable: TBlockState
                                           // is a real enum and the match is exhaustive.
    }
}

/// `IsSubTransaction` (xact.c:5044)
pub fn IsSubTransaction() -> bool {
    xs(|s| s.current().nesting_level >= 2)
}

// ---------------------------------------------------------------------------
//  Seam-adapter helpers (thin named functions required by seam::set())
// ---------------------------------------------------------------------------

/// `MyXactFlags |= XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK` — seam adapter for
/// `set_my_xact_flags_acquired_access_exclusive_lock`.
fn seam_set_my_xact_flags_acquired_access_exclusive_lock() {
    xs(|s| {
        s.MyXactFlags |= types_core::xact::XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK;
    });
}

/// `MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPNAMESPACE` — seam adapter for
/// `set_xact_accessed_temp_namespace`.
fn seam_set_xact_accessed_temp_namespace() {
    xs(|s| {
        s.MyXactFlags |= types_core::xact::XACT_FLAGS_ACCESSEDTEMPNAMESPACE;
    });
}

/// Read `XactLastRecEnd` — the end LSN of the last WAL record written by this
/// backend. Owned by xlog.c; this crate forwards via the xlog seam (the xact
/// crate drives that global through `set_xact_last_rec_end`).
fn seam_xact_last_rec_end() -> types_core::XLogRecPtr {
    xlog_seams::xact_last_rec_end::call()
}

/// Seam adapter for `xact_redo` — the rmgr `rm_redo` slot hands us a
/// `&mut XLogReaderState`, exactly as C's `xact_redo(XLogReaderState *record)`.
/// Marshal the fields `xact_redo` reads in C (`XLogRecGetInfo` / `XLogRecGetXid`
/// / `XLogRecGetOrigin`, `ReadRecPtr` / `EndRecPtr`, `XLogRecGetData`) into the
/// in-crate [`redo::XactRedoInfo`] view and dispatch.
fn seam_xact_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> PgResult<()> {
    // C: Assert(!XLogRecHasAnyBlockRefs(record)). Backup blocks aren't used in
    // xact records; the decoded record must be present to redo.
    let decoded = record
        .record
        .as_ref()
        .ok_or_else(|| PgError::error("xact_redo: no decoded record"))?;
    let info = redo::XactRedoInfo {
        info: decoded.info(),
        xid: decoded.xid(),
        origin_id: decoded.record_origin(),
        read_rec_ptr: record.ReadRecPtr,
        end_rec_ptr: record.EndRecPtr,
        data: decoded.data(),
    };
    redo::xact_redo(info)
}

/// The xact-records argument bundles carry `types_wal`'s own `RelFileLocator`
/// and `types_core`'s `XlXactStatsItem`; the in-crate WAL builders take
/// `types_storage::RelFileLocator` / `types_wal::XlXactStatsItem`. These are the
/// same C structs under different nominal types; convert field-for-field.
fn convert_rels(rels: &[types_wal::RelFileLocator]) -> Vec<types_storage::RelFileLocator> {
    rels.iter()
        .map(|r| types_storage::RelFileLocator {
            spcOid: r.spc_oid(),
            dbOid: r.db_oid(),
            relNumber: r.rel_number(),
        })
        .collect()
}

fn convert_stats(stats: &[types_core::xact::XlXactStatsItem]) -> Vec<types_wal::XlXactStatsItem> {
    stats
        .iter()
        .map(|s| types_wal::XlXactStatsItem {
            kind: s.kind,
            dboid: s.dboid,
            objid: s.objid,
        })
        .collect()
}

/// Seam adapter for `XactLogCommitRecord` — the 2PC caller marshals its inputs
/// into a [`types_wal::xact_records::XactLogCommitRecordArgs`]; the in-crate
/// builder reads the remaining inputs (`forceSyncCommit`, `synchronous_commit`,
/// `XLogLogicalInfoActive`, `MyDatabaseId`/`MyDatabaseTableSpace`, the session
/// replication origin) from backend state itself, exactly as C's
/// `XactLogCommitRecord` does, so the marshaled copies of those globals are
/// ignored here.
fn seam_xact_log_commit_record(
    args: &types_wal::xact_records::XactLogCommitRecordArgs,
) -> PgResult<XLogRecPtr> {
    use types_storage::{SharedInvalidationMessage, SHARED_INVALIDATION_MESSAGE_SIZE};

    // C passes `SharedInvalidationMessage *invalmsgs`; the 2PC caller marshals
    // them as the on-the-wire bytes (their `to_wire_bytes` form, read from the
    // 2PC state file). Decode back to the typed array the in-crate builder
    // re-serializes, so the byte layout round-trips identically.
    let nmsgs = args.nmsgs as usize;
    let mut msgs: Vec<SharedInvalidationMessage> = Vec::new();
    msgs.try_reserve(nmsgs).map_err(|_| {
        PgError::error("out of memory decoding transaction commit invalidation messages")
    })?;
    for i in 0..nmsgs {
        let off = i * SHARED_INVALIDATION_MESSAGE_SIZE;
        let raw: [u8; SHARED_INVALIDATION_MESSAGE_SIZE] = args
            .msgs
            .get(off..off + SHARED_INVALIDATION_MESSAGE_SIZE)
            .ok_or_else(|| PgError::error("truncated transaction commit invalidation messages"))?
            .try_into()
            .unwrap();
        let msg = SharedInvalidationMessage::from_wire_bytes(raw).ok_or_else(|| {
            PgError::error("invalid shared-invalidation message in transaction commit record")
        })?;
        msgs.push(msg);
    }

    wal::XactLogCommitRecord(
        args.commit_time,
        &args.subxacts,
        &convert_rels(&args.rels),
        &convert_stats(&args.dropped_stats),
        &msgs,
        args.relcache_inval,
        args.xactflags,
        args.twophase_xid,
        args.twophase_gid.as_deref(),
    )
}

/// Seam adapter for `XactLogAbortRecord` — see [`seam_xact_log_commit_record`].
fn seam_xact_log_abort_record(
    args: &types_wal::xact_records::XactLogAbortRecordArgs,
) -> PgResult<XLogRecPtr> {
    wal::XactLogAbortRecord(
        args.abort_time,
        &args.subxacts,
        &convert_rels(&args.rels),
        &convert_stats(&args.dropped_stats),
        args.xactflags,
        args.twophase_xid,
        args.twophase_gid.as_deref(),
    )
}

/// `DefineSavepoint(name)` — seam adapter for `define_savepoint`. The sole
/// consumer always supplies a name, so the seam contract is `&str`; this
/// wraps it in `Some()` for the owner's `Option<&str>` body (same wrapping
/// pattern as `engine::BeginTransactionBlock`).
fn seam_define_savepoint(name: &str) -> PgResult<()> {
    engine::DefineSavepoint(Some(name))
}

/// `XactIsoLevel = XACT_READ_COMMITTED` — seam adapter for
/// `set_xact_iso_level_read_committed`.
fn seam_set_xact_iso_level_read_committed() {
    SetXactIsoLevel(XACT_READ_COMMITTED);
}

/// `XactIsoLevel = XACT_REPEATABLE_READ` — seam adapter.
fn seam_set_xact_iso_level_repeatable_read() {
    SetXactIsoLevel(XACT_REPEATABLE_READ);
}

// ---------------------------------------------------------------------------
//  Seam installation
// ---------------------------------------------------------------------------

/// Install this crate's implementations into `backend-access-transam-xact-seams`.
pub fn init_seams() {
    use backend_access_transam_xact_seams as seams;
    seams::command_counter_increment::set(CommandCounterIncrement);
    seams::get_current_transaction_nest_level::set(GetCurrentTransactionNestLevel);
    seams::transaction_id_is_current_transaction_id::set(TransactionIdIsCurrentTransactionId);
    seams::is_transaction_state::set(IsTransactionState);
    seams::is_aborted_transaction_block_state::set(IsAbortedTransactionBlockState);
    seams::get_current_command_id::set(GetCurrentCommandId);
    seams::check_xid_alive::set(CheckXidAlive);
    seams::bsysscan::set(bsysscan);
    seams::get_current_transaction_id::set(GetCurrentTransactionId);
    seams::set_my_xact_flags_acquired_access_exclusive_lock::set(
        seam_set_my_xact_flags_acquired_access_exclusive_lock,
    );
    seams::get_current_sub_transaction_id::set(GetCurrentSubTransactionId);
    seams::is_sub_transaction::set(IsSubTransaction);
    seams::set_xact_accessed_temp_namespace::set(seam_set_xact_accessed_temp_namespace);
    seams::start_transaction_command::set(StartTransactionCommand);
    seams::commit_transaction_command::set(CommitTransactionCommand);
    seams::abort_out_of_any_transaction::set(AbortOutOfAnyTransaction);
    seams::xact_last_rec_end::set(seam_xact_last_rec_end);
    seams::is_transaction_or_transaction_block::set(IsTransactionOrTransactionBlock);
    seams::get_top_transaction_id_if_any::set(GetTopTransactionIdIfAny);
    seams::get_top_transaction_id::set(GetTopTransactionId);
    seams::get_top_full_transaction_id::set(GetTopFullTransactionId);
    seams::get_top_full_transaction_id_if_any::set(GetTopFullTransactionIdIfAny);
    seams::get_current_transaction_id_if_any::set(GetCurrentTransactionIdIfAny);
    seams::is_subxact_top_xid_log_pending::set(IsSubxactTopXidLogPending);
    seams::set_check_xid_alive::set(SetCheckXidAlive);
    seams::set_bsysscan::set(SetBsysscan);
    seams::get_current_statement_start_timestamp::set(GetCurrentStatementStartTimestamp);
    seams::is_in_parallel_mode::set(IsInParallelMode);
    seams::require_transaction_block::set(RequireTransactionBlock);
    seams::xact_redo::set(seam_xact_redo);
    seams::xact_log_commit_record::set(seam_xact_log_commit_record);
    seams::xact_log_abort_record::set(seam_xact_log_abort_record);
    // Pure-wiring installs (assemble/seam-wiring-guard): owner bodies exist
    // with signatures matching the seam decls, they were just never set().
    seams::abort_current_transaction::set(AbortCurrentTransaction);
    seams::begin_transaction_block::set(engine::BeginTransactionBlock);
    seams::end_transaction_block::set(engine::EndTransactionBlock);
    seams::rollback_to_savepoint::set(engine::RollbackToSavepoint);
    seams::is_transaction_block::set(IsTransactionBlock);
    seams::isolation_uses_xact_snapshot::set(IsolationUsesXactSnapshot);
    seams::set_current_statement_start_timestamp::set(SetCurrentStatementStartTimestamp);
    // `PreventInTransactionBlock(isTopLevel, stmtType)` — signature matches the
    // owner body exactly.
    seams::prevent_in_transaction_block::set(PreventInTransactionBlock);
    // Reconciled adapters: seam contract differs slightly from the owner body.
    seams::define_savepoint::set(seam_define_savepoint);
    seams::set_xact_iso_level_read_committed::set(seam_set_xact_iso_level_read_committed);
    seams::set_xact_iso_level_repeatable_read::set(seam_set_xact_iso_level_repeatable_read);
    seams::set_xact_read_only::set(SetXactReadOnly);
    seams::xact_read_only::set(XactReadOnly);
    seams::xact_iso_level::set(XactIsoLevel);
    // `int synchronous_commit` (xact.c GUC) — read by walsender's SyncRepRequested.
    seams::synchronous_commit::set(synchronous_commit);
}

#[cfg(test)]
mod callback_tests {
    use super::*;
    use std::rc::Rc;

    /// C semantics: callbacks run most-recently-registered first.
    #[test]
    fn callbacks_run_newest_first() {
        reset_xact_state_for_tests();
        let log: Rc<RefCell<Vec<usize>>> = Rc::new(RefCell::new(Vec::new()));
        for tag in [1usize, 2, 3] {
            let log = log.clone();
            RegisterXactCallback(move |_| {
                log.borrow_mut().push(tag);
                Ok(())
            });
        }
        CallXactCallbacks(XACT_EVENT_COMMIT).unwrap();
        assert_eq!(*log.borrow(), vec![3, 2, 1]);
        reset_xact_state_for_tests();
    }

    /// C semantics: a callback may unregister itself while being called; the
    /// remaining callbacks still run, and the unregistered one stays gone.
    #[test]
    fn self_unregistration_is_safe() {
        reset_xact_state_for_tests();
        let log: Rc<RefCell<Vec<usize>>> = Rc::new(RefCell::new(Vec::new()));
        {
            let log = log.clone();
            RegisterXactCallback(move |_| {
                log.borrow_mut().push(1);
                Ok(())
            });
        }
        {
            let log = log.clone();
            let token: Rc<std::cell::Cell<Option<XactCallbackToken>>> =
                Rc::new(std::cell::Cell::new(None));
            let token_in_cb = token.clone();
            let registered = RegisterXactCallback(move |_| {
                log.borrow_mut().push(2);
                if let Some(token) = token_in_cb.get() {
                    UnregisterXactCallback(token);
                }
                Ok(())
            });
            token.set(Some(registered));
        }
        CallXactCallbacks(XACT_EVENT_COMMIT).unwrap();
        CallXactCallbacks(XACT_EVENT_COMMIT).unwrap();
        // round 1: 2 (self-unregisters) then 1; round 2: just 1.
        assert_eq!(*log.borrow(), vec![2, 1, 1]);
        reset_xact_state_for_tests();
    }

    /// C semantics: registrations are prepended, so a callback registered
    /// during CallXactCallbacks is not invoked in the same round.
    #[test]
    fn mid_iteration_registration_not_invoked_this_round() {
        reset_xact_state_for_tests();
        let log: Rc<RefCell<Vec<usize>>> = Rc::new(RefCell::new(Vec::new()));
        {
            let log = log.clone();
            RegisterXactCallback(move |_| {
                log.borrow_mut().push(1);
                let log = log.clone();
                let nested = RegisterXactCallback(move |_| {
                    log.borrow_mut().push(99);
                    Ok(())
                });
                // unregister again so only one nested registration
                // accumulates per round
                UnregisterXactCallback(nested);
                Ok(())
            });
        }
        CallXactCallbacks(XACT_EVENT_COMMIT).unwrap();
        assert_eq!(*log.borrow(), vec![1]);
        reset_xact_state_for_tests();
    }
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
