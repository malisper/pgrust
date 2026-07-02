//! xact.c: the transaction state machine. Sanctioned divergences: resource
//! owners are the resowner unit's RAII owner values reached through
//! `resowner_seams` (`has_resource_owner` keeps the C control flow); the
//! `MemoryContextSwitchTo`/`priorContext` choreography dissolves (no ambient
//! context); `AtEOXact_HashTables` dissolves (no dynahash seq-scan tracking
//! over PgHashMap); transaction-lifetime collections are std `Vec`/`String`
//! with fallible reserves (see state.rs).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

use datum::Datum;
use elog::{elog, ereport, message_level_is_interesting};
use mcx::MemoryContext;
use types_core::xact::*;
use types_core::{TimestampTz, TransactionId, XLogRecPtr};
use types_error::{
    ErrorLocation, PgError, PgResult, DEBUG5, ERRCODE_ACTIVE_SQL_TRANSACTION,
    ERRCODE_INVALID_TRANSACTION_STATE, ERRCODE_NO_ACTIVE_SQL_TRANSACTION,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_READ_ONLY_SQL_TRANSACTION,
    ERRCODE_S_E_INVALID_SPECIFICATION, ERROR, FATAL, WARNING,
};

pub(crate) use transam_xlog_seams as xlog_seams;

mod engine;
mod redo;
mod state;
mod wal;
#[cfg(test)]
mod tests;

pub(crate) use state::{xs, TransactionNode};

pub use engine::{
    AbortCurrentTransaction, AbortOutOfAnyTransaction, BeginImplicitTransactionBlock,
    BeginInternalSubTransaction, BeginTransactionBlock, CommitTransactionCommand,
    DefineSavepoint, EndImplicitTransactionBlock, EndParallelWorkerTransaction,
    EndTransactionBlock, EstimateTransactionStateSpace, PrepareTransactionBlock,
    ReleaseCurrentSubTransaction, ReleaseSavepoint, RestoreTransactionCharacteristics,
    RollbackAndReleaseCurrentSubTransaction, RollbackToSavepoint,
    SaveTransactionCharacteristics, SerializeTransactionState, StartParallelWorkerTransaction,
    StartTransactionCommand, UserAbortTransactionBlock,
};
pub use redo::{parse_abort_record, parse_commit_record, xact_redo, ParsedAbort, ParsedCommit,
    XactRedoInfo};
pub use wal::{XactLogAbortRecord, XactLogCommitRecord};

// Verified against access/xact.h / xlogrecord.h / xloginsert.h / rmgrlist.h.
pub const RM_XACT_ID: u8 = 1;
pub const XLOG_XACT_COMMIT: u8 = 0x00;
pub const XLOG_XACT_PREPARE: u8 = 0x10;
pub const XLOG_XACT_ABORT: u8 = 0x20;
pub const XLOG_XACT_COMMIT_PREPARED: u8 = 0x30;
pub const XLOG_XACT_ABORT_PREPARED: u8 = 0x40;
pub const XLOG_XACT_ASSIGNMENT: u8 = 0x50;
pub const XLOG_XACT_INVALIDATIONS: u8 = 0x60;
pub const XLOG_XACT_OPMASK: u8 = 0x70;
pub const XLOG_XACT_HAS_INFO: u8 = 0x80;

pub const XACT_XINFO_HAS_DBINFO: u32 = 1 << 0;
pub const XACT_XINFO_HAS_SUBXACTS: u32 = 1 << 1;
pub const XACT_XINFO_HAS_RELFILELOCATORS: u32 = 1 << 2;
pub const XACT_XINFO_HAS_INVALS: u32 = 1 << 3;
pub const XACT_XINFO_HAS_TWOPHASE: u32 = 1 << 4;
pub const XACT_XINFO_HAS_ORIGIN: u32 = 1 << 5;
pub const XACT_XINFO_HAS_AE_LOCKS: u32 = 1 << 6;
pub const XACT_XINFO_HAS_GID: u32 = 1 << 7;
pub const XACT_XINFO_HAS_DROPPED_STATS: u32 = 1 << 8;

pub const XACT_COMPLETION_APPLY_FEEDBACK: u32 = 1 << 29;
pub const XACT_COMPLETION_UPDATE_RELCACHE_FILE: u32 = 1 << 30;
pub const XACT_COMPLETION_FORCE_SYNC_COMMIT: u32 = 1 << 31;

pub const fn XactCompletionApplyFeedback(xinfo: u32) -> bool {
    (xinfo & XACT_COMPLETION_APPLY_FEEDBACK) != 0
}
pub const fn XactCompletionRelcacheInitFileInval(xinfo: u32) -> bool {
    (xinfo & XACT_COMPLETION_UPDATE_RELCACHE_FILE) != 0
}
pub const fn XactCompletionForceSyncCommit(xinfo: u32) -> bool {
    (xinfo & XACT_COMPLETION_FORCE_SYNC_COMMIT) != 0
}

pub const XLOG_INCLUDE_ORIGIN: u8 = 0x01;
pub const XLR_SPECIAL_REL_UPDATE: u8 = 0x01;

/// `MaxAllocSize` (1 GB - 1): bounds `childXids`.
pub(crate) const MAX_ALLOC_SIZE: usize = 0x3fff_ffff;

/// `PGPROC_MAX_CACHED_SUBXIDS` (storage/proc.h).
const PGPROC_MAX_CACHED_SUBXIDS: usize = 64;

pub type XactCallback = fn(event: XactEvent, arg: Datum) -> PgResult<()>;
pub type SubXactCallback = fn(
    event: SubXactEvent,
    my_subid: SubTransactionId,
    parent_subid: SubTransactionId,
    arg: Datum,
) -> PgResult<()>;

// C's XactCallbackItem: identity is the (callback, arg) pair.
#[derive(Clone, Copy)]
pub(crate) struct XactCallbackItem {
    pub(crate) callback: XactCallback,
    pub(crate) arg: Datum,
}

#[derive(Clone, Copy)]
pub(crate) struct SubXactCallbackItem {
    pub(crate) callback: SubXactCallback,
    pub(crate) arg: Datum,
}

impl std::fmt::Debug for XactCallbackItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XactCallbackItem").finish_non_exhaustive()
    }
}
impl std::fmt::Debug for SubXactCallbackItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubXactCallbackItem").finish_non_exhaustive()
    }
}

/// One-load read of `CurrentTransactionState->blockState` (mirror Cell).
pub(crate) fn cur_block_state() -> TBlockState {
    let v = state::mirror_block_state();
    debug_assert_eq!(v, xs(|s| s.current().block_state));
    v
}

pub fn reset_xact_state_for_tests() {
    xs(|s| s.reset_for_tests());
}

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

scalar_get_set!(DefaultXactIsoLevel, SetDefaultXactIsoLevel, DefaultXactIsoLevel, i32);
scalar_get_set!(XactIsoLevel, SetXactIsoLevel, XactIsoLevel, i32);
scalar_get_set!(DefaultXactReadOnly, SetDefaultXactReadOnly, DefaultXactReadOnly, bool);
scalar_get_set!(XactReadOnly, SetXactReadOnly, XactReadOnly, bool);
scalar_get_set!(DefaultXactDeferrable, SetDefaultXactDeferrable, DefaultXactDeferrable, bool);
scalar_get_set!(XactDeferrable, SetXactDeferrable, XactDeferrable, bool);
scalar_get_set!(synchronous_commit, SetSynchronousCommit, synchronous_commit, i32);
scalar_get_set!(CheckXidAlive, SetCheckXidAlive, CheckXidAlive, TransactionId);
scalar_get_set!(bsysscan, SetBsysscan, bsysscan, bool);
scalar_get_set!(MyXactFlags, SetMyXactFlags, MyXactFlags, i32);
scalar_get_set!(xact_is_sampled, SetXactIsSampled, xact_is_sampled, bool);

/// `MyXactFlags |= flags` — C callers OR the global directly; this is that
/// write path.
pub fn OrMyXactFlags(flags: i32) {
    xs(|s| s.MyXactFlags |= flags);
}

pub fn IsolationUsesXactSnapshot() -> bool {
    XactIsoLevel() >= XACT_REPEATABLE_READ
}

pub fn IsolationIsSerializable() -> bool {
    XactIsoLevel() == XACT_SERIALIZABLE
}

/// `IsTransactionState`: TRANS_INPROGRESS only — not valid during
/// start/commit/abort processing.
pub fn IsTransactionState() -> bool {
    let st = state::mirror_trans_state();
    debug_assert_eq!(st, xs(|s| s.current().state));
    st == TRANS_INPROGRESS
}

pub fn IsAbortedTransactionBlockState() -> bool {
    matches!(cur_block_state(), TBLOCK_ABORT | TBLOCK_SUBABORT)
}

/// `GetTopTransactionId` — assigns one if not yet set.
pub fn GetTopTransactionId() -> PgResult<TransactionId> {
    if !GetTopFullTransactionIdIfAny().is_valid() {
        assign_transaction_id_at(0)?;
    }
    Ok(GetTopTransactionIdIfAny())
}

pub fn GetTopTransactionIdIfAny() -> TransactionId {
    GetTopFullTransactionIdIfAny().xid()
}

pub fn GetCurrentTransactionId() -> PgResult<TransactionId> {
    if !GetCurrentFullTransactionIdIfAny().is_valid() {
        AssignTransactionId()?;
    }
    Ok(GetCurrentTransactionIdIfAny())
}

pub fn GetCurrentTransactionIdIfAny() -> TransactionId {
    GetCurrentFullTransactionIdIfAny().xid()
}

pub fn GetTopFullTransactionId() -> PgResult<FullTransactionId> {
    if !GetTopFullTransactionIdIfAny().is_valid() {
        assign_transaction_id_at(0)?;
    }
    Ok(GetTopFullTransactionIdIfAny())
}

pub fn GetTopFullTransactionIdIfAny() -> FullTransactionId {
    let v = state::mirror_top_full_xid();
    debug_assert_eq!(v, xs(|s| s.top_full_xid()));
    v
}

pub fn GetCurrentFullTransactionId() -> PgResult<FullTransactionId> {
    if !GetCurrentFullTransactionIdIfAny().is_valid() {
        AssignTransactionId()?;
    }
    Ok(GetCurrentFullTransactionIdIfAny())
}

pub fn GetCurrentFullTransactionIdIfAny() -> FullTransactionId {
    let v = state::mirror_cur_full_xid();
    debug_assert_eq!(v, xs(|s| s.current().full_transaction_id));
    v
}

pub fn MarkCurrentTransactionIdLoggedIfAny() {
    xs(|s| {
        if s.current().full_transaction_id.is_valid() {
            s.current_mut().did_log_xid = true;
        }
    });
}

pub fn IsSubxactTopXidLogPending() -> bool {
    if xs(|s| s.current().top_xid_logged) {
        return false;
    }
    if !xlog_seams::xlog_logical_info_active::call() {
        return false;
    }
    xs(|s| {
        if s.current().state != TRANS_INPROGRESS {
            return false;
        }
        if !s.is_subxact() {
            return false;
        }
        s.current().full_transaction_id.is_valid()
    })
}

pub fn MarkSubxactTopXidLogged() {
    debug_assert!(IsSubxactTopXidLogPending());
    xs(|s| s.current_mut().top_xid_logged = true);
}

/// Latches the value for the rest of the transaction, keyed on
/// `MyProc->vxid.lxid` changing; reference point for `age(xid)`.
pub fn GetStableLatestTransactionId() -> PgResult<TransactionId> {
    let procno = lmgr_proc::MyProc().expect("MyProc is not set");
    let my_lxid = lmgr_proc::GetPGProcByNumber(procno)
        .vxid
        .lxid
        .load(std::sync::atomic::Ordering::Relaxed);
    let cached = xs(|s| (s.stable_latest.0 == my_lxid).then_some(s.stable_latest.1));
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

pub fn AssignTransactionId() -> PgResult<()> {
    let idx = xs(|s| s.stack_len() - 1);
    assign_transaction_id_at(idx)
}

/// `AssignTransactionId` core, on stack index `idx` (the C argument `s`).
fn assign_transaction_id_at(idx: usize) -> PgResult<()> {
    let is_subxact = idx > 0;

    debug_assert!(!xs(|s| s.node(idx).full_transaction_id.is_valid()));
    debug_assert!(xs(|s| s.node(idx).state == TRANS_INPROGRESS));

    if IsInParallelMode() || parallel_seams::is_parallel_worker::call() {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
            .errmsg("cannot assign transaction IDs during a parallel operation")
            .finish(xact_location("AssignTransactionId"));
    }

    // Ensure parent(s) have XIDs, so a child's XID is always later than its
    // parent's; iterate up then assign down (C avoids deep recursion too).
    if is_subxact && !xs(|s| s.node(idx - 1).full_transaction_id.is_valid()) {
        let mut parents = Vec::new();
        let mut p = idx;
        while p > 0 && !xs(|s| s.node(p - 1).full_transaction_id.is_valid()) {
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

    // wal_level=logical: a subxact's xid may appear in WAL only after its
    // toplevel xid has been logged.
    let log_unknown_top = is_subxact
        && xlog_seams::xlog_logical_info_active::call()
        && !xs(|s| s.node(0).did_log_xid);

    let full = varsup_seams::get_new_transaction_id::call(is_subxact)?;
    xs(|s| {
        s.node_mut(idx).full_transaction_id = full;
        if !is_subxact {
            s.set_top_full_xid(full);
        }
    });

    if is_subxact {
        let parent_xid = xs(|s| s.node(idx - 1).full_transaction_id.xid());
        subtrans_seams::sub_trans_set_parent::call(full.xid(), parent_xid)?;
    }

    if !is_subxact {
        predicate_seams::register_predicate_locking_xid::call(full.xid())?;
    }

    // The XID lock must land on transaction idx's own curTransactionOwner
    // (not whatever CurrentResourceOwner happens to be), else it is released
    // by the wrong owner on (sub)abort and double-released at subcommit.
    // When ancestors were just assigned above, each got its own owner: the
    // owner tree mirrors the stack, so idx's owner is the (deepest-idx)-th
    // ancestor of the live CurTransactionResourceOwner.
    let levels_up = xs(|s| (s.stack_len() - 1 - idx) as u32);
    let saved = resowner_seams::swap_current_to_cur_transaction_ancestor::call(levels_up);
    let insert_result = lmgr_seams::xact_lock_table_insert::call(full.xid());
    resowner_seams::restore_current_resource_owner::call(saved);
    insert_result?;

    // Every PGPROC_MAX_CACHED_SUBXIDS assigned xids per top-level xact, WAL
    // the assignment (hot-standby KnownAssignedXids bookkeeping).
    if is_subxact && xlog_seams::xlog_standby_info_active::call() {
        xs(|s| {
            s.unreported_xids
                .try_reserve(1)
                .map_err(|_| {
                    PgError::error("out of memory tracking unreported subtransaction IDs")
                })?;
            s.unreported_xids.push(full.xid());
            Ok::<(), PgError>(())
        })?;

        // must match the test in RecoverPreparedTransactions()
        if xs(|s| s.unreported_xids.len()) >= PGPROC_MAX_CACHED_SUBXIDS || log_unknown_top {
            let xtop = GetTopTransactionId()?;
            debug_assert!(xtop != InvalidTransactionId);
            let subxids = xs(|s| s.unreported_xids.clone());

            // xl_xact_assignment { TransactionId xtop; int nsubxacts; xsub[] }
            let mut hdr = [0u8; 8];
            hdr[0..4].copy_from_slice(&xtop.to_ne_bytes());
            hdr[4..8].copy_from_slice(&(subxids.len() as i32).to_ne_bytes());
            let mut body: Vec<u8> = Vec::new();
            body.try_reserve(subxids.len() * 4)
                .map_err(|_| PgError::error("out of memory building xid-assignment record"))?;
            for x in &subxids {
                body.extend_from_slice(&x.to_ne_bytes());
            }
            xloginsert_seams::xlog_insert::call(RM_XACT_ID, XLOG_XACT_ASSIGNMENT, &[&hdr, &body])?;

            xs(|s| {
                s.unreported_xids.clear();
                s.node_mut(0).did_log_xid = true;
            });
        }
    }

    Ok(())
}

pub fn GetCurrentSubTransactionId() -> SubTransactionId {
    xs(|s| s.current().sub_transaction_id)
}

pub fn SubTransactionIsActive(subxid: SubTransactionId) -> bool {
    xs(|s| {
        for node in s.nodes_rev() {
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

/// `GetCurrentCommandId` — global to a transaction, not subxact-local.
pub fn GetCurrentCommandId(used: bool) -> PgResult<CommandId> {
    if used {
        // No provision for reporting currentCommandIdUsed back to a leader.
        if parallel_seams::is_parallel_worker::call() {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
                .errmsg("cannot modify data in a parallel worker")
                .finish(xact_location("GetCurrentCommandId"))
                .map(|()| InvalidCommandId);
        }
        xs(|s| s.set_command_id_used(true));
    }
    let v = state::mirror_command_id();
    debug_assert_eq!(v, xs(|s| s.command_id()));
    Ok(v)
}

pub fn SetParallelStartTimestamps(xact_ts: TimestampTz, stmt_ts: TimestampTz) {
    debug_assert!(parallel_seams::is_parallel_worker::call());
    xs(|s| {
        s.xact_start_timestamp = xact_ts;
        s.stmt_start_timestamp = stmt_ts;
    });
}

pub fn GetCurrentTransactionStartTimestamp() -> TimestampTz {
    xs(|s| s.xact_start_timestamp)
}

pub fn GetCurrentStatementStartTimestamp() -> TimestampTz {
    xs(|s| s.stmt_start_timestamp)
}

/// Sets the stop timestamp if unset (C's lazy latch).
pub fn GetCurrentTransactionStopTimestamp() -> TimestampTz {
    if xs(|s| s.xact_stop_timestamp) == 0 {
        let ts = timestamp_seams::get_current_timestamp::call();
        xs(|s| s.xact_stop_timestamp = ts);
    }
    xs(|s| s.xact_stop_timestamp)
}

pub fn SetCurrentStatementStartTimestamp() {
    if !parallel_seams::is_parallel_worker::call() {
        let ts = timestamp_seams::get_current_timestamp::call();
        xs(|s| s.stmt_start_timestamp = ts);
    } else {
        debug_assert!(xs(|s| s.stmt_start_timestamp) != 0);
    }
}

pub fn GetCurrentTransactionNestLevel() -> i32 {
    let v = state::mirror_nest_level();
    debug_assert_eq!(v, xs(|s| s.current().nesting_level));
    v
}

pub fn TransactionIdIsCurrentTransactionId(xid: TransactionId) -> bool {
    if !TransactionIdIsNormal(xid) {
        return false;
    }

    if xid == GetTopTransactionIdIfAny() {
        return true;
    }

    xs(|s| {
        // Parallel worker: sorted ParallelCurrentXids is the whole answer.
        if !s.parallel_current_xids.is_empty() {
            return s.parallel_current_xids.binary_search(&xid).is_ok();
        }

        for node in s.nodes_rev() {
            if node.state == TRANS_ABORT {
                continue;
            }
            if !node.full_transaction_id.is_valid() {
                continue; // it can't have any child XIDs either
            }
            if xid == node.full_transaction_id.xid() {
                return true;
            }
            if binary_search_xids(&node.child_xids, xid) {
                return true;
            }
        }
        false
    })
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
        } else if TransactionIdPrecedes(probe, xid) {
            low = middle + 1;
        } else {
            high = middle - 1;
        }
    }
    false
}

pub fn TransactionStartedDuringRecovery() -> bool {
    xs(|s| s.current().started_in_recovery)
}

pub fn EnterParallelMode() {
    xs(|s| {
        debug_assert!(s.current().parallel_mode_level >= 0);
        s.current_mut().parallel_mode_level += 1;
    });
}

/// (C also asserts `!ParallelContextActive()` when leaving the last level.)
pub fn ExitParallelMode() {
    xs(|s| {
        debug_assert!(s.current().parallel_mode_level > 0);
        s.current_mut().parallel_mode_level -= 1;
    });
}

pub fn IsInParallelMode() -> bool {
    xs(|s| s.current().parallel_mode_level != 0 || s.current().parallel_child_xact)
}

pub fn CommandCounterIncrement() -> PgResult<()> {
    // No-op unless the counter was "used" to mark tuples (hot short-circuit).
    let used = state::mirror_command_id_used();
    debug_assert_eq!(used, xs(|s| s.command_id_used()));
    if !used {
        return Ok(());
    }

    if IsInParallelMode() || parallel_seams::is_parallel_worker::call() {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
            .errmsg("cannot start commands during a parallel operation")
            .finish(xact_location("CommandCounterIncrement"));
    }

    // C increments then backs off on wraparound; checking before the write
    // leaves currentCommandId identically unchanged on failure.
    let next = xs(|s| {
        let next = s.command_id() + 1;
        if next == InvalidCommandId {
            return None;
        }
        s.set_command_id(next);
        s.set_command_id_used(false);
        Some(next)
    });
    let Some(next) = next else {
        return ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg("cannot have more than 2^32-2 commands in a transaction")
            .finish(xact_location("CommandCounterIncrement"));
    };

    snapmgr_seams::snapshot_set_command_id::call(next);

    AtCCI_LocalCache()?;
    Ok(())
}

pub fn ForceSyncCommit() {
    xs(|s| s.force_sync_commit = true);
}

pub(crate) fn AtStart_Cache() -> PgResult<()> {
    inval::AcceptInvalidationMessages()
}

/// First time through, create TransactionAbortContext and
/// TopTransactionContext; at top level CurTransactionContext IS
/// TopTransactionContext.
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

pub(crate) fn AtStart_ResourceOwner() -> PgResult<()> {
    xs(|s| {
        debug_assert!(!s.current().has_resource_owner);
        s.current_mut().has_resource_owner = true;
    });
    resowner_seams::at_start_resource_owner::call()
}

pub(crate) fn AtSubStart_Memory() {
    xs(|s| {
        let idx = s.stack_len() - 1;
        debug_assert!(idx > 0);
        let child = {
            let parent_ctx = s
                .node(idx - 1)
                .cur_transaction_context
                .as_ref()
                .or(s.top_transaction_context.as_ref())
                .expect("CurTransactionContext exists for the parent");
            parent_ctx.new_child("CurTransactionContext")
        };
        s.node_mut(idx).cur_transaction_context = Some(child);
    });
}

pub(crate) fn AtSubStart_ResourceOwner() -> PgResult<()> {
    xs(|s| {
        debug_assert!(s.is_subxact());
        s.current_mut().has_resource_owner = true;
    });
    resowner_seams::at_substart_resource_owner::call()
}

fn AtCCI_LocalCache() -> PgResult<()> {
    // Relation map changes must reach the relcache before local sinval runs.
    relmapper_seams::at_cci_relation_map::call()?;
    inval::CommandEndInvalidationMessages()
}

/// TopTransactionContext survives but becomes empty.
pub(crate) fn AtCommit_Memory() {
    xs(|s| {
        s.node_mut(0).retained_child_contexts.clear();
        if let Some(ctx) = s.top_transaction_context.as_mut() {
            ctx.reset();
        }
    });
}

/// Delete the subxact's CurTransactionContext if empty, else keep it alive
/// (in C it survives as a child of the parent until top-level end).
pub(crate) fn AtSubCommit_Memory() -> PgResult<()> {
    xs(|s| {
        let idx = s.stack_len() - 1;
        debug_assert!(idx > 0);
        let taken = s.node_mut(idx).cur_transaction_context.take();
        if let Some(ctx) = taken {
            if ctx.subtree_used() == 0 {
                drop(ctx);
            } else {
                let mut parent = s.node_mut(idx - 1);
                parent
                    .retained_child_contexts
                    .try_reserve(1)
                    .map_err(|_| PgError::error("out of memory keeping subtransaction context"))?;
                parent.retained_child_contexts.push(ctx);
            }
        }
        let mut kept = std::mem::take(&mut s.node_mut(idx).retained_child_contexts);
        let mut parent = s.node_mut(idx - 1);
        parent
            .retained_child_contexts
            .try_reserve(kept.len())
            .map_err(|_| PgError::error("out of memory keeping subtransaction context"))?;
        parent.retained_child_contexts.append(&mut kept);
        Ok(())
    })
}

/// Pass my XID + child XIDs up to the parent as committed children, in XID
/// order (my XID precedes my children's; existing entries precede mine).
pub(crate) fn AtSubCommit_childXids() -> PgResult<()> {
    xs(|s| {
        let idx = s.stack_len() - 1;
        debug_assert!(idx > 0);

        let my_full = s.node(idx).full_transaction_id;
        let my_children = std::mem::take(&mut s.node_mut(idx).child_xids);

        let new_n = s.node(idx - 1).child_xids.len() + my_children.len() + 1;
        let max_children = MAX_ALLOC_SIZE / std::mem::size_of::<TransactionId>();
        if new_n > max_children {
            return ereport(ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg(format!(
                    "maximum number of committed subtransactions ({max_children}) exceeded"
                ))
                .finish(xact_location("AtSubCommit_childXids"));
        }

        let mut parent = s.node_mut(idx - 1);
        parent
            .child_xids
            .try_reserve(my_children.len() + 1)
            .map_err(|_| PgError::error("out of memory recording committed subtransactions"))?;
        parent.child_xids.push(my_full.xid());
        parent.child_xids.extend_from_slice(&my_children);
        Ok(())
    })
}

pub(crate) fn AtAbort_Memory() {
    xs(|s| {
        if s.transaction_abort_context.is_none() {
            s.transaction_abort_context = Some(MemoryContext::new("TransactionAbortContext"));
        }
    });
}

pub(crate) fn AtSubAbort_Memory() {
    debug_assert!(xs(|s| s.transaction_abort_context.is_some()));
}

/// `CurrentResourceOwner = TopTransactionResourceOwner` dissolves with the
/// ambient owner.
pub(crate) fn AtAbort_ResourceOwner() {}

pub(crate) fn AtSubAbort_ResourceOwner() {
    resowner_seams::set_current_to_cur_transaction::call();
}

pub(crate) fn AtSubAbort_childXids() {
    xs(|s| {
        // (C doesn't bother pruning unreportedXids here either.)
        s.current_mut().child_xids = Vec::new();
    });
}

pub(crate) fn AtCleanup_Memory() {
    xs(|s| {
        debug_assert_eq!(s.stack_len(), 1);
        if let Some(ctx) = s.transaction_abort_context.as_mut() {
            ctx.reset();
        }
        s.node_mut(0).retained_child_contexts.clear();
        if let Some(ctx) = s.top_transaction_context.as_mut() {
            ctx.reset();
        }
    });
}

pub(crate) fn AtSubCleanup_Memory() {
    xs(|s| {
        let idx = s.stack_len() - 1;
        debug_assert!(idx > 0);
        if let Some(ctx) = s.transaction_abort_context.as_mut() {
            ctx.reset();
        }
        let mut n = s.node_mut(idx);
        n.cur_transaction_context = None;
        n.retained_child_contexts.clear();
    });
}

/// Prepended as in C, so callbacks run most-recently-registered first and a
/// registration made during `CallXactCallbacks` is not invoked this round.
pub fn RegisterXactCallback(callback: XactCallback, arg: Datum) {
    xs(|s| {
        s.xact_callbacks
            .try_reserve(1)
            .expect("out of memory registering transaction callback");
        s.xact_callbacks.insert(0, XactCallbackItem { callback, arg });
    });
}

pub fn UnregisterXactCallback(callback: XactCallback, arg: Datum) {
    xs(|s| {
        if let Some(pos) = s
            .xact_callbacks
            .iter()
            .position(|item| item.callback == callback && item.arg == arg)
        {
            s.xact_callbacks.remove(pos);
        }
    });
}

pub fn RegisterSubXactCallback(callback: SubXactCallback, arg: Datum) {
    xs(|s| {
        s.subxact_callbacks
            .try_reserve(1)
            .expect("out of memory registering subtransaction callback");
        s.subxact_callbacks.insert(0, SubXactCallbackItem { callback, arg });
    });
}

pub fn UnregisterSubXactCallback(callback: SubXactCallback, arg: Datum) {
    xs(|s| {
        if let Some(pos) = s
            .subxact_callbacks
            .iter()
            .position(|item| item.callback == callback && item.arg == arg)
        {
            s.subxact_callbacks.remove(pos);
        }
    });
}

/// Snapshot the registrations and call each one still registered when its
/// turn comes (the C `next = item->next` walk: self-unregistration is safe,
/// mid-iteration registrations don't run this round). Invocations hold no
/// state borrow, so callbacks may re-enter this crate.
pub(crate) fn CallXactCallbacks(event: XactEvent) -> PgResult<()> {
    let items: Vec<XactCallbackItem> = xs(|s| {
        let mut v = Vec::new();
        v.try_reserve(s.xact_callbacks.len())
            .map_err(|_| PgError::error("out of memory calling transaction callbacks"))?;
        v.extend(s.xact_callbacks.iter().copied());
        Ok::<_, PgError>(v)
    })?;
    for item in items {
        let live = xs(|s| {
            s.xact_callbacks
                .iter()
                .any(|it| it.callback == item.callback && it.arg == item.arg)
        });
        if live {
            (item.callback)(event, item.arg)?;
        }
    }
    Ok(())
}

pub(crate) fn CallSubXactCallbacks(
    event: SubXactEvent,
    my_subid: SubTransactionId,
    parent_subid: SubTransactionId,
) -> PgResult<()> {
    let items: Vec<SubXactCallbackItem> = xs(|s| {
        let mut v = Vec::new();
        v.try_reserve(s.subxact_callbacks.len())
            .map_err(|_| PgError::error("out of memory calling subtransaction callbacks"))?;
        v.extend(s.subxact_callbacks.iter().copied());
        Ok::<_, PgError>(v)
    })?;
    for item in items {
        let live = xs(|s| {
            s.subxact_callbacks
                .iter()
                .any(|it| it.callback == item.callback && it.arg == item.arg)
        });
        if live {
            (item.callback)(event, my_subid, parent_subid, item.arg)?;
        }
    }
    Ok(())
}

/// Committed children of the current transaction (C hands out the in-place
/// array; the callers here consume a fallible copy).
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

pub(crate) fn xact_location(function: &'static str) -> ErrorLocation {
    ErrorLocation::new("xact.c", 0, function)
}

/// `MemoryContextStrdup(TopTransactionContext, ...)` stand-in; palloc can
/// ereport OOM.
pub(crate) fn try_strdup(s: &str, what: &'static str) -> PgResult<String> {
    let mut out = String::new();
    out.try_reserve_exact(s.len())
        .map_err(|_| PgError::error(what))?;
    out.push_str(s);
    Ok(out)
}

pub(crate) fn unexpected_block_state(function: &str, st: TBlockState) -> Box<PgError> {
    Box::new(PgError::new(
        FATAL,
        format!("{function}: unexpected state {}", BlockStateAsString(st)),
    ))
}

pub(crate) fn warn_internal(msg: &str) {
    let _ = elog(WARNING, msg.to_owned());
}

pub(crate) fn ShowTransactionState(str: &str) {
    // skip work if message will definitely not be printed
    if message_level_is_interesting(DEBUG5) {
        ShowTransactionStateRec(str);
    }
}

/// C recurses parent-first; the stack's front-to-back order is the same.
fn ShowTransactionStateRec(str: &str) {
    let lines = xs(|s| {
        s.nodes()
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
                    s.command_id(),
                    if s.command_id_used() { " (used)" } else { "" },
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

/// `PreventCommandIfReadOnly` (utility.c; the flag it reads lives here).
pub fn PreventCommandIfReadOnly(cmdname: &str) -> PgResult<()> {
    if xs(|s| s.XactReadOnly) {
        return ereport(ERROR)
            .errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION)
            .errmsg(format!("cannot execute {cmdname} in a read-only transaction"))
            .finish(xact_location("PreventCommandIfReadOnly"));
    }
    Ok(())
}

/// `PreventCommandIfParallelMode` (utility.c).
pub fn PreventCommandIfParallelMode(cmdname: &str) -> PgResult<()> {
    if IsInParallelMode() {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_TRANSACTION_STATE)
            .errmsg(format!("cannot execute {cmdname} during a parallel operation"))
            .finish(xact_location("PreventCommandIfParallelMode"));
    }
    Ok(())
}

pub fn PreventInTransactionBlock(isTopLevel: bool, stmtType: &str) -> PgResult<()> {
    if IsTransactionBlock() {
        return ereport(ERROR)
            .errcode(ERRCODE_ACTIVE_SQL_TRANSACTION)
            .errmsg(format!("{stmtType} cannot run inside a transaction block"))
            .finish(xact_location("PreventInTransactionBlock"));
    }
    if IsSubTransaction() {
        return ereport(ERROR)
            .errcode(ERRCODE_ACTIVE_SQL_TRANSACTION)
            .errmsg(format!("{stmtType} cannot run inside a subtransaction"))
            .finish(xact_location("PreventInTransactionBlock"));
    }
    if !isTopLevel {
        return ereport(ERROR)
            .errcode(ERRCODE_ACTIVE_SQL_TRANSACTION)
            .errmsg(format!("{stmtType} cannot be executed from a function"))
            .finish(xact_location("PreventInTransactionBlock"));
    }
    let bs = cur_block_state();
    if bs != TBLOCK_DEFAULT && bs != TBLOCK_STARTED {
        return Err(Box::new(PgError::new(FATAL, "cannot prevent transaction chain")));
    }
    xs(|s| s.MyXactFlags |= XACT_FLAGS_NEEDIMMEDIATECOMMIT);
    Ok(())
}

pub fn WarnNoTransactionBlock(isTopLevel: bool, stmtType: &str) -> PgResult<()> {
    CheckTransactionBlock(isTopLevel, false, stmtType)
}

pub fn RequireTransactionBlock(isTopLevel: bool, stmtType: &str) -> PgResult<()> {
    CheckTransactionBlock(isTopLevel, true, stmtType)
}

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
        .errmsg(format!("{stmtType} can only be used in transaction blocks"))
        .finish(xact_location("CheckTransactionBlock"))
}

/// True on the same conditions PreventInTransactionBlock errors on.
pub fn IsInTransactionBlock(isTopLevel: bool) -> bool {
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

pub fn IsTransactionBlock() -> bool {
    let bs = cur_block_state();
    !(bs == TBLOCK_DEFAULT || bs == TBLOCK_STARTED)
}

pub fn IsTransactionOrTransactionBlock() -> bool {
    cur_block_state() != TBLOCK_DEFAULT
}

pub fn TransactionBlockStatusCode() -> u8 {
    match cur_block_state() {
        TBLOCK_DEFAULT | TBLOCK_STARTED => b'I',
        TBLOCK_BEGIN
        | TBLOCK_SUBBEGIN
        | TBLOCK_INPROGRESS
        | TBLOCK_IMPLICIT_INPROGRESS
        | TBLOCK_PARALLEL_INPROGRESS
        | TBLOCK_SUBINPROGRESS
        | TBLOCK_END
        | TBLOCK_SUBRELEASE
        | TBLOCK_SUBCOMMIT
        | TBLOCK_PREPARE => b'T',
        TBLOCK_ABORT
        | TBLOCK_SUBABORT
        | TBLOCK_ABORT_END
        | TBLOCK_SUBABORT_END
        | TBLOCK_ABORT_PENDING
        | TBLOCK_SUBABORT_PENDING
        | TBLOCK_SUBRESTART
        | TBLOCK_SUBABORT_RESTART => b'E',
    }
}

pub fn IsSubTransaction() -> bool {
    xs(|s| s.current().nesting_level >= 2)
}

fn seam_set_xact_accessed_temp_namespace() {
    xs(|s| s.MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPNAMESPACE);
}

pub fn init_seams() {
    use guc_tables::{vars, GucVarAccessors};

    vars::XactIsoLevel.install(GucVarAccessors { get: XactIsoLevel, set: SetXactIsoLevel });
    vars::DefaultXactIsoLevel
        .install(GucVarAccessors { get: DefaultXactIsoLevel, set: SetDefaultXactIsoLevel });
    vars::XactReadOnly.install(GucVarAccessors { get: XactReadOnly, set: SetXactReadOnly });
    vars::DefaultXactReadOnly
        .install(GucVarAccessors { get: DefaultXactReadOnly, set: SetDefaultXactReadOnly });
    vars::XactDeferrable.install(GucVarAccessors { get: XactDeferrable, set: SetXactDeferrable });
    vars::DefaultXactDeferrable
        .install(GucVarAccessors { get: DefaultXactDeferrable, set: SetDefaultXactDeferrable });
    vars::synchronous_commit
        .install(GucVarAccessors { get: synchronous_commit, set: SetSynchronousCommit });

    xact_seams::transaction_block_status_code::set(TransactionBlockStatusCode);
    xact_seams::get_current_sub_transaction_id::set(GetCurrentSubTransactionId);
    xact_seams::set_xact_accessed_temp_namespace::set(seam_set_xact_accessed_temp_namespace);
    xact_seams::get_current_command_id::set(GetCurrentCommandId);
    xact_seams::get_current_transaction_nest_level::set(GetCurrentTransactionNestLevel);
}
