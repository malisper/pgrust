//! xact.c: the transaction state machine. Sanctioned divergences: resource
//! owners are the resowner unit's RAII owner values called directly
//! (`has_resource_owner` keeps the C control flow); the
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
use types_resowner::{
    RESOURCE_RELEASE_AFTER_LOCKS, RESOURCE_RELEASE_BEFORE_LOCKS, RESOURCE_RELEASE_LOCKS,
};
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

pub(crate) use state::{xs, xs_ptr, TransactionNode, XsPtr};
pub use state::session_mem_teardown;

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
pub use redo::{parse_abort_record, parse_commit_record, parse_prepare_record, xact_redo,
    ParsedAbort, ParsedCommit, ParsedPrepare, XactRedoInfo};
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
        stablexid = varsup::ReadNextTransactionId()?;
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

    let log_unknown_top = is_subxact
        && xlog_seams::xlog_logical_info_active::call()
        && !xs(|s| s.node(0).did_log_xid);

    let full = varsup::GetNewTransactionId(is_subxact)?;
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
    let prev_owner = resowner::CurrentResourceOwner();
    let base = resowner::CurTransactionResourceOwner();
    if !base.is_null() {
        let mut owner = base;
        for _ in 0..levels_up {
            let parent = resowner::ResourceOwnerGetParent(owner);
            if parent.is_null() {
                // The owner tree mirrors the transaction stack; keep the
                // deepest owner rather than installing NULL on overshoot.
                owner = base;
                break;
            }
            owner = parent;
        }
        resowner::SetCurrentResourceOwner(owner);
    }
    let insert_result = lmgr::XactLockTableInsert(full.xid());
    resowner::SetCurrentResourceOwner(prev_owner);
    insert_result?;

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

        if xs(|s| s.unreported_xids.len()) >= PGPROC_MAX_CACHED_SUBXIDS || log_unknown_top {
            let xtop = GetTopTransactionId()?;
            debug_assert!(xtop != InvalidTransactionId);
            let (hdr, body) = xs(|s| {
                let mut hdr = [0u8; 8];
                hdr[0..4].copy_from_slice(&xtop.to_ne_bytes());
                hdr[4..8].copy_from_slice(&(s.unreported_xids.len() as i32).to_ne_bytes());
                let mut body: Vec<u8> = Vec::new();
                body.try_reserve(s.unreported_xids.len() * 4)
                    .map_err(|_| {
                        PgError::error("out of memory building xid-assignment record")
                    })?;
                for x in &s.unreported_xids {
                    body.extend_from_slice(&x.to_ne_bytes());
                }
                Ok::<_, PgError>((hdr, body))
            })?;
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

pub fn GetCurrentCommandId(used: bool) -> PgResult<CommandId> {
    if used {
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
    inval::local::AcceptInvalidationMessages()
}

pub(crate) fn AtStart_Memory(xp: XsPtr) {
    xp.with(|s| {
        if s.transaction_abort_context.is_none() {
            s.transaction_abort_context = Some(MemoryContext::new("TransactionAbortContext"));
        }
        if s.top_transaction_context.is_none() {
            s.top_transaction_context = Some(MemoryContext::new("TopTransactionContext"));
        }
    });
}

// Session pool for the per-statement TopTransaction owner: delete becomes
// recycle when the owner drained clean, start reuses it (per-statement-path.md
// §3.3). Holds at most one owner; NULL when unpooled.
thread_local! {
    static POOLED_TOP_OWNER: core::cell::Cell<types_resowner::ResourceOwner> =
        const { core::cell::Cell::new(types_resowner::ResourceOwner::NULL) };
}

pub fn ResourceOwnerBoundaryIssue() -> Option<&'static str> {
    resowner::ResourceOwnerStateIssueAllowing(POOLED_TOP_OWNER.get())
}

pub(crate) fn AtStart_ResourceOwner(xp: XsPtr) -> PgResult<()> {
    xp.with(|s| {
        debug_assert!(!s.current().has_resource_owner);
        s.current_mut().has_resource_owner = true;
    });
    debug_assert!(resowner::TopTransactionResourceOwner().is_null());
    let pooled = POOLED_TOP_OWNER.replace(types_resowner::ResourceOwner::NULL);
    let owner = if !pooled.is_null() {
        pooled
    } else {
        resowner::ResourceOwnerCreate(types_resowner::ResourceOwner::NULL, "TopTransaction")?
    };
    resowner::SetTopTransactionResourceOwner(owner);
    resowner::SetCurTransactionResourceOwner(owner);
    resowner::SetCurrentResourceOwner(owner);
    Ok(())
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
    let owner =
        resowner::ResourceOwnerCreate(resowner::CurTransactionResourceOwner(), "SubTransaction")?;
    resowner::SetCurTransactionResourceOwner(owner);
    resowner::SetCurrentResourceOwner(owner);
    Ok(())
}

pub(crate) fn release_transaction_owner_before_locks(is_commit: bool) -> PgResult<()> {
    let owner = resowner::TopTransactionResourceOwner();
    if !owner.is_null() {
        resowner::ResourceOwnerRelease(owner, RESOURCE_RELEASE_BEFORE_LOCKS, is_commit, true)?;
    }
    Ok(())
}

pub(crate) fn release_transaction_owner_locks(is_commit: bool) -> PgResult<()> {
    let owner = resowner::TopTransactionResourceOwner();
    if !owner.is_null() {
        resowner::ResourceOwnerRelease(owner, RESOURCE_RELEASE_LOCKS, is_commit, true)?;
        resowner::ResourceOwnerRelease(owner, RESOURCE_RELEASE_AFTER_LOCKS, is_commit, true)?;
    }
    Ok(())
}

pub(crate) fn release_subxact_owner_before_locks(is_commit: bool) -> PgResult<()> {
    let owner = resowner::CurTransactionResourceOwner();
    if !owner.is_null() {
        resowner::ResourceOwnerRelease(owner, RESOURCE_RELEASE_BEFORE_LOCKS, is_commit, false)?;
    }
    Ok(())
}

pub(crate) fn release_subxact_owner_locks(is_commit: bool) -> PgResult<()> {
    let owner = resowner::CurTransactionResourceOwner();
    if !owner.is_null() {
        resowner::ResourceOwnerRelease(owner, RESOURCE_RELEASE_LOCKS, is_commit, false)?;
        resowner::ResourceOwnerRelease(owner, RESOURCE_RELEASE_AFTER_LOCKS, is_commit, false)?;
    }
    Ok(())
}

pub(crate) fn delete_transaction_owner() -> PgResult<()> {
    let owner = resowner::TopTransactionResourceOwner();
    if !owner.is_null() {
        if POOLED_TOP_OWNER.get().is_null() && resowner::ResourceOwnerRecycle(owner) {
            POOLED_TOP_OWNER.set(owner);
        } else {
            resowner::ResourceOwnerDelete(owner);
        }
    }
    resowner::SetCurTransactionResourceOwner(types_resowner::ResourceOwner::NULL);
    resowner::SetTopTransactionResourceOwner(types_resowner::ResourceOwner::NULL);
    Ok(())
}

pub(crate) fn cleanup_subxact_owner() -> PgResult<()> {
    let owner = resowner::CurTransactionResourceOwner();
    if !owner.is_null() {
        let parent = resowner::ResourceOwnerGetParent(owner);
        resowner::SetCurrentResourceOwner(parent);
        resowner::SetCurTransactionResourceOwner(parent);
        resowner::ResourceOwnerDelete(owner);
    }
    Ok(())
}

fn AtCCI_LocalCache() -> PgResult<()> {
    relmapper::AtCCI_RelationMap()?;
    inval::eoxact::CommandEndInvalidationMessages()
}

pub(crate) fn AtCommit_Memory(xp: XsPtr) {
    xp.with(|s| {
        s.node_mut(0).retained_child_contexts.clear();
        if let Some(ctx) = s.top_transaction_context.as_mut() {
            ctx.reset();
        }
    });
}

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

pub(crate) fn AtAbort_Memory(xp: XsPtr) {
    xp.with(|s| {
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
    resowner::SetCurrentResourceOwner(resowner::CurTransactionResourceOwner());
}

pub(crate) fn AtSubAbort_childXids() {
    xs(|s| {
        s.current_mut().child_xids = Vec::new();
    });
}

pub(crate) fn AtCleanup_Memory(xp: XsPtr) {
    xp.with(|s| {
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
            .position(|item| std::ptr::fn_addr_eq(item.callback, callback) && item.arg == arg)
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
            .position(|item| std::ptr::fn_addr_eq(item.callback, callback) && item.arg == arg)
        {
            s.subxact_callbacks.remove(pos);
        }
    });
}

pub(crate) fn CallXactCallbacks(xp: XsPtr, event: XactEvent) -> PgResult<()> {
    let items: Vec<XactCallbackItem> = xp.with(|s| {
        let mut v = Vec::new();
        v.try_reserve(s.xact_callbacks.len())
            .map_err(|_| PgError::error("out of memory calling transaction callbacks"))?;
        v.extend(s.xact_callbacks.iter().copied());
        Ok::<_, PgError>(v)
    })?;
    for item in items {
        let live = xp.with(|s| {
            s.xact_callbacks
                .iter()
                .any(|it| std::ptr::fn_addr_eq(it.callback, item.callback) && it.arg == item.arg)
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
                .any(|it| std::ptr::fn_addr_eq(it.callback, item.callback) && it.arg == item.arg)
        });
        if live {
            (item.callback)(event, my_subid, parent_subid, item.arg)?;
        }
    }
    Ok(())
}

pub fn xactGetCommittedChildren() -> PgResult<Vec<TransactionId>> {
    committed_children_in(xs_ptr())
}

pub(crate) fn committed_children_in(xp: XsPtr) -> PgResult<Vec<TransactionId>> {
    xp.with(|s| {
        let src = &s.current().child_xids;
        let mut out = Vec::new();
        out.try_reserve_exact(src.len())
            .map_err(|_| PgError::error("out of memory copying committed subtransactions"))?;
        out.extend_from_slice(src);
        Ok(out)
    })
}

#[track_caller]
pub(crate) fn xact_location(function: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, function)
}

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
    if message_level_is_interesting(DEBUG5) {
        ShowTransactionStateRec(str);
    }
}

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

    xact_seams::mark_current_transaction_id_logged_if_any::set(MarkCurrentTransactionIdLoggedIfAny);
    xact_seams::mark_subxact_top_xid_logged::set(MarkSubxactTopXidLogged);

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
    xact_seams::get_xact_iso_level::set(XactIsoLevel);
    xact_seams::xact_get_committed_children::set(xactGetCommittedChildren);
    xact_seams::get_current_sub_transaction_id::set(GetCurrentSubTransactionId);
    xact_seams::set_xact_accessed_temp_namespace::set(seam_set_xact_accessed_temp_namespace);
    xact_seams::get_current_command_id::set(GetCurrentCommandId);
    xact_seams::get_current_transaction_id::set(GetCurrentTransactionId);
    xact_seams::or_my_xact_flags::set(OrMyXactFlags);
    xact_seams::get_current_transaction_nest_level::set(GetCurrentTransactionNestLevel);
    xact_seams::get_current_transaction_stop_timestamp::set(GetCurrentTransactionStopTimestamp);
    xact_seams::get_current_transaction_start_timestamp::set(GetCurrentTransactionStartTimestamp);
    xact_seams::get_top_transaction_id_if_any::set(GetTopTransactionIdIfAny);
    xact_seams::get_stable_latest_transaction_id::set(GetStableLatestTransactionId);
    xact_seams::xact_read_only::set(XactReadOnly);
    xact_seams::xact_deferrable::set(XactDeferrable);
    xact_seams::is_sub_transaction::set(IsSubTransaction);
    xact_seams::is_transaction_or_transaction_block::set(IsTransactionOrTransactionBlock);
    xact_seams::is_transaction_state::set(IsTransactionState);
    xact_seams::start_transaction_command::set(StartTransactionCommand);
    xact_seams::commit_transaction_command::set(CommitTransactionCommand);
    xact_seams::is_in_parallel_mode::set(IsInParallelMode);
    xact_seams::isolation_uses_xact_snapshot::set(IsolationUsesXactSnapshot);
    xact_seams::isolation_is_serializable::set(IsolationIsSerializable);
    xact_seams::transaction_id_is_current_transaction_id::set(TransactionIdIsCurrentTransactionId);
    xact_portal_seams::get_current_statement_start_timestamp::set(
        GetCurrentStatementStartTimestamp,
    );
}
