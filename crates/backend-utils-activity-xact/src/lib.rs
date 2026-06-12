//! Port of `src/backend/utils/activity/pgstat_xact.c` (PostgreSQL 18.3):
//! transactional integration for the cumulative statistics system.
//!
//! Owns the per-backend `pgStatXactStack` — a stack of `PgStat_SubXactStatus`
//! levels, one per active (sub)transaction nesting level, each carrying the
//! `pending_drops` list of stats objects the (sub)transaction scheduled for
//! creation or drop — and the commit/abort/2PC state machine that decides,
//! per pending item, whether the shared stats entry is actually dropped:
//! top-level commit drops entries for objects *dropped* in the transaction,
//! top-level abort drops entries for objects *created* in it, a
//! subtransaction abort drops its created objects' entries, and a
//! subtransaction commit hands its drop schedule up to the parent.
//!
//! Model notes:
//!
//! * `pgStatXactStack` is a backend-local C global, so it is a
//!   `thread_local!` here. The C allocates the stack nodes and pending items
//!   in `TopTransactionContext` purely for lifetime; the thread-local owns
//!   them instead, and the same teardown points the C uses
//!   (`AtEOXact_PgStat`, `AtEOSubXact_PgStat`, `PostPrepare_PgStat`) free
//!   them. Growth goes through `try_reserve`, surfacing C's
//!   `MemoryContextAlloc` out-of-memory `ereport(ERROR)` as `Err(PgError)`.
//! * The C `PgStat_SubXactStatus` also carries the `first` chain of
//!   per-relation `PgStat_TableXactStatus` nodes; that chain belongs to
//!   `pgstat_relation.c` (which models it in its own per-level state), so the
//!   relation/database hooks cross as scalar-only seams and this crate never
//!   touches `first`.
//! * `xl_xact_stats_item`'s split `objid_lo`/`objid_hi` words are the single
//!   `u64` of [`XlXactStatsItem`]; the `((uint64) objid_hi) << 32 | objid_lo`
//!   recombinations at the C call sites are plain field reads.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use std::cell::RefCell;
use std::collections::VecDeque;

use backend_access_transam_xact_seams as xact_seams;
use backend_utils_activity_pgstat_seams as pgstat_seams;
use backend_utils_activity_shmem_seams as shmem_seams;
use backend_utils_activity_stat_seams as stat_seams;
use backend_utils_error::ereport;
use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_core::primitive::Oid;
use types_core::xact::XlXactStatsItem;
use types_error::{ErrorLocation, PgError, PgResult, ERRCODE_OUT_OF_MEMORY, WARNING};
pub use types_pgstat::activity_pgstat::PgStat_Kind;

/// Install this crate's seam implementations. This unit's functions have no
/// seam crate yet (no cyclic consumer has needed one), so there is nothing to
/// install.
pub fn init_seams() {}

/// `PgStat_PendingDroppedStatsItem` (pgstat_xact.c): one scheduled
/// create/drop. The C `dlist_node` link is the containing deque.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PgStat_PendingDroppedStatsItem {
    item: XlXactStatsItem,
    is_create: bool,
}

/// The parts of `PgStat_SubXactStatus` (`pgstat.h`) this file owns: the
/// nesting level and the `pending_drops` dclist. The `prev` link is the
/// containing stack's order; the `first` per-relation chain belongs to
/// `pgstat_relation.c` (see the crate doc).
struct PgStat_SubXactStatus {
    nest_level: i32,
    pending_drops: VecDeque<PgStat_PendingDroppedStatsItem>,
}

thread_local! {
    /// `static PgStat_SubXactStatus *pgStatXactStack = NULL;` — the top of the
    /// stack (deepest active nesting level) is the last element; an empty vec
    /// is C's NULL.
    static PG_STAT_XACT_STACK: RefCell<Vec<PgStat_SubXactStatus>> =
        const { RefCell::new(Vec::new()) };
}

/// `MemoryContextAlloc(TopTransactionContext, ...)`'s out-of-memory
/// `ereport(ERROR)` for the thread-local stack's growth.
fn oom(request: usize) -> PgError {
    PgError::error("out of memory")
        .with_sqlstate(ERRCODE_OUT_OF_MEMORY)
        .with_detail(format!(
            "Failed on request of size {request} in memory context \"TopTransactionContext\"."
        ))
}

/// Called from access/transam/xact.c at top-level transaction commit/abort.
pub fn AtEOXact_PgStat(isCommit: bool, parallel: bool) -> PgResult<()> {
    stat_seams::at_eoxact_pgstat_database::call(isCommit, parallel);

    // handle transactional stats information
    let have_xact_state = PG_STAT_XACT_STACK.with(|cell| {
        let stack = cell.borrow();
        match stack.last() {
            Some(top) => {
                debug_assert_eq!(top.nest_level, 1);
                debug_assert_eq!(stack.len(), 1); // xact_state->prev == NULL
                true
            }
            None => false,
        }
    });
    if have_xact_state {
        stat_seams::at_eoxact_pgstat_relations::call(isCommit);
        AtEOXact_PgStat_DroppedStats(isCommit)?;
    }
    // pgStatXactStack = NULL;
    PG_STAT_XACT_STACK.with(|cell| cell.borrow_mut().clear());

    // Make sure any stats snapshot is thrown away
    pgstat_seams::pgstat_clear_snapshot::call();
    Ok(())
}

/// When committing, drop stats for objects dropped in the transaction. When
/// aborting, drop stats for objects created in the transaction.
///
/// Operates on the (single) top-level stack node, like the C, which receives
/// `pgStatXactStack` after the caller's nest-level-1 asserts.
fn AtEOXact_PgStat_DroppedStats(isCommit: bool) -> PgResult<()> {
    let mut not_freed_count = 0;

    // dclist_foreach_modify: visit each pending item front-to-back, removing
    // it (dclist_delete_from + pfree) after processing. On `Err` the
    // in-flight item is still queued, like the C, where the longjmp fires
    // before dclist_delete_from.
    loop {
        let pending = PG_STAT_XACT_STACK.with(|cell| {
            cell.borrow()
                .last()
                .and_then(|top| top.pending_drops.front().copied())
        });
        let Some(pending) = pending else { break };
        let it = &pending.item;

        if isCommit && !pending.is_create {
            // Transaction that dropped an object committed. Drop the stats
            // too.
            if !shmem_seams::pgstat_drop_entry::call(it.kind as PgStat_Kind, it.dboid, it.objid)? {
                not_freed_count += 1;
            }
        } else if !isCommit && pending.is_create {
            // Transaction that created an object aborted. Drop the stats
            // associated with the object.
            if !shmem_seams::pgstat_drop_entry::call(it.kind as PgStat_Kind, it.dboid, it.objid)? {
                not_freed_count += 1;
            }
        }

        PG_STAT_XACT_STACK.with(|cell| {
            cell.borrow_mut()
                .last_mut()
                .expect("stack level exists: peeked above")
                .pending_drops
                .pop_front();
        });
    }

    if not_freed_count > 0 {
        shmem_seams::pgstat_request_entry_refs_gc::call();
    }
    Ok(())
}

/// Called from access/transam/xact.c at subtransaction commit/abort.
pub fn AtEOSubXact_PgStat(isCommit: bool, nestDepth: i32) -> PgResult<()> {
    // merge the sub-transaction's transactional stats into the parent
    let xact_state = PG_STAT_XACT_STACK.with(|cell| {
        let mut stack = cell.borrow_mut();
        match stack.last() {
            // delink xact_state from stack immediately to simplify reuse case
            Some(top) if top.nest_level >= nestDepth => stack.pop(),
            _ => None,
        }
    });
    if let Some(xact_state) = xact_state {
        stat_seams::at_eosubxact_pgstat_relations::call(isCommit, nestDepth)?;
        AtEOSubXact_PgStat_DroppedStats(xact_state, isCommit, nestDepth)?;
        // pfree(xact_state): the popped level drops here.
    }
    Ok(())
}

/// Like [`AtEOXact_PgStat_DroppedStats`], but for subtransactions: on commit,
/// surviving drop items are passed up to the parent level.
fn AtEOSubXact_PgStat_DroppedStats(
    mut xact_state: PgStat_SubXactStatus,
    isCommit: bool,
    nestDepth: i32,
) -> PgResult<()> {
    let mut not_freed_count = 0;

    if xact_state.pending_drops.is_empty() {
        return Ok(());
    }

    // parent_xact_state = pgstat_get_xact_stack_level(nestDepth - 1): the
    // caller already delinked xact_state, so the parent (created here if
    // missing) is the stack top for the pushes below.
    ensure_xact_stack_level(nestDepth - 1)?;

    // dclist_foreach_modify with dclist_delete_from at the top of each
    // iteration: drain front-to-back.
    while let Some(pending) = xact_state.pending_drops.pop_front() {
        let it = &pending.item;

        if !isCommit && pending.is_create {
            // Subtransaction creating a new stats object aborted. Drop the
            // stats object.
            if !shmem_seams::pgstat_drop_entry::call(it.kind as PgStat_Kind, it.dboid, it.objid)? {
                not_freed_count += 1;
            }
            // pfree(pending)
        } else if isCommit {
            // Subtransaction dropping a stats object committed. Can't yet
            // remove the stats object, the surrounding transaction might
            // still abort. Pass it on to the parent. (The C relinks the
            // existing dlist node without allocating; the deque growth here
            // is the container cost of the Rust shape.)
            PG_STAT_XACT_STACK.with(|cell| {
                cell.borrow_mut()
                    .last_mut()
                    .expect("parent level ensured above")
                    .pending_drops
                    .push_back(pending);
            });
        }
        // else: pfree(pending)
    }

    debug_assert!(xact_state.pending_drops.is_empty());
    if not_freed_count > 0 {
        shmem_seams::pgstat_request_entry_refs_gc::call();
    }
    Ok(())
}

/// Save the transactional stats state at 2PC transaction prepare.
pub fn AtPrepare_PgStat() -> PgResult<()> {
    let have_xact_state = PG_STAT_XACT_STACK.with(|cell| {
        let stack = cell.borrow();
        match stack.last() {
            Some(top) => {
                debug_assert_eq!(top.nest_level, 1);
                debug_assert_eq!(stack.len(), 1); // xact_state->prev == NULL
                true
            }
            None => false,
        }
    });
    if have_xact_state {
        stat_seams::at_prepare_pgstat_relations::call()?;
    }
    Ok(())
}

/// Clean up after successful PREPARE.
///
/// Note: `AtEOXact_PgStat` is not called during PREPARE.
pub fn PostPrepare_PgStat() {
    // We don't bother to free any of the transactional state, since it's all
    // in TopTransactionContext and will go away anyway.
    let have_xact_state = PG_STAT_XACT_STACK.with(|cell| {
        let stack = cell.borrow();
        match stack.last() {
            Some(top) => {
                debug_assert_eq!(top.nest_level, 1);
                debug_assert_eq!(stack.len(), 1); // xact_state->prev == NULL
                true
            }
            None => false,
        }
    });
    if have_xact_state {
        stat_seams::post_prepare_pgstat_relations::call();
    }
    // pgStatXactStack = NULL;
    PG_STAT_XACT_STACK.with(|cell| cell.borrow_mut().clear());

    // Make sure any stats snapshot is thrown away
    pgstat_seams::pgstat_clear_snapshot::call();
}

/// Ensure (sub)transaction stack entry for the given nest_level exists, adding
/// it if needed.
///
/// The C returns the node pointer; the stack is crate-private here, so the
/// public entry point ensures existence (in-crate callers address the stack
/// top afterwards, which is the node the C would have returned).
pub fn pgstat_get_xact_stack_level(nest_level: i32) -> PgResult<()> {
    ensure_xact_stack_level(nest_level)
}

fn ensure_xact_stack_level(nest_level: i32) -> PgResult<()> {
    PG_STAT_XACT_STACK.with(|cell| {
        let mut stack = cell.borrow_mut();
        // C checks only the current top node.
        let need_new = match stack.last() {
            Some(top) => top.nest_level != nest_level,
            None => true,
        };
        if need_new {
            // MemoryContextAlloc(TopTransactionContext,
            //                    sizeof(PgStat_SubXactStatus))
            stack
                .try_reserve(1)
                .map_err(|_| oom(core::mem::size_of::<PgStat_SubXactStatus>()))?;
            stack.push(PgStat_SubXactStatus {
                nest_level,
                pending_drops: VecDeque::new(),
            });
        }
        Ok(())
    })
}

/// Get stat items that need to be dropped at commit / abort.
///
/// When committing, stats for objects that have been dropped in the
/// transaction are returned. When aborting, stats for newly created objects
/// are returned.
///
/// Used by COMMIT / ABORT and 2PC PREPARE processing when building their
/// respective WAL records, to ensure stats are dropped in case of a crash /
/// on standbys.
///
/// The C fills a palloc'd out-array in `CurrentMemoryContext` and returns the
/// item count; here `mcx` is the caller's current context and the returned
/// vec's length is the count. The caller frees it (directly or via context
/// reset), as in C.
pub fn pgstat_get_transactional_drops<'mcx>(
    mcx: Mcx<'mcx>,
    isCommit: bool,
) -> PgResult<PgVec<'mcx, XlXactStatsItem>> {
    PG_STAT_XACT_STACK.with(|cell| {
        let stack = cell.borrow();
        let Some(xact_state) = stack.last() else {
            return Ok(PgVec::new_in(mcx));
        };

        // We expect to be called for subtransaction abort (which logs a WAL
        // record), but not for subtransaction commit (which doesn't).
        debug_assert!(!isCommit || xact_state.nest_level == 1);
        debug_assert!(!isCommit || stack.len() == 1); // xact_state->prev == NULL

        // *items = palloc(dclist_count(...) * sizeof(xl_xact_stats_item));
        let mut items = vec_with_capacity_in(mcx, xact_state.pending_drops.len())?;
        for pending in &xact_state.pending_drops {
            if isCommit && pending.is_create {
                continue;
            }
            if !isCommit && !pending.is_create {
                continue;
            }
            debug_assert!(items.len() < xact_state.pending_drops.len());
            items.push(pending.item);
        }
        Ok(items)
    })
}

/// Execute scheduled drops post-commit. Called from `xact_redo_commit()` /
/// `xact_redo_abort()` during recovery, and from
/// `FinishPreparedTransaction()` during normal 2PC COMMIT/ABORT PREPARED
/// processing.
///
/// C's `(ndrops, items)` pair is the slice; `is_redo` is unused in the C body
/// too.
pub fn pgstat_execute_transactional_drops(
    items: &[XlXactStatsItem],
    _is_redo: bool,
) -> PgResult<()> {
    let mut not_freed_count = 0;

    if items.is_empty() {
        return Ok(());
    }

    for it in items {
        if !shmem_seams::pgstat_drop_entry::call(it.kind as PgStat_Kind, it.dboid, it.objid)? {
            not_freed_count += 1;
        }
    }

    if not_freed_count > 0 {
        shmem_seams::pgstat_request_entry_refs_gc::call();
    }
    Ok(())
}

fn create_drop_transactional_internal(
    kind: PgStat_Kind,
    dboid: Oid,
    objid: u64,
    is_create: bool,
) -> PgResult<()> {
    let nest_level = xact_seams::get_current_transaction_nest_level::call();

    let drop = PgStat_PendingDroppedStatsItem {
        is_create,
        item: XlXactStatsItem {
            kind: kind as i32,
            dboid,
            objid,
        },
    };

    ensure_xact_stack_level(nest_level)?;
    PG_STAT_XACT_STACK.with(|cell| {
        let mut stack = cell.borrow_mut();
        let xact_state = stack.last_mut().expect("stack level ensured above");
        // MemoryContextAlloc(TopTransactionContext,
        //                    sizeof(PgStat_PendingDroppedStatsItem))
        xact_state
            .pending_drops
            .try_reserve(1)
            .map_err(|_| oom(core::mem::size_of::<PgStat_PendingDroppedStatsItem>()))?;
        // dclist_push_tail(&xact_state->pending_drops, &drop->node);
        xact_state.pending_drops.push_back(drop);
        Ok(())
    })
}

/// Create a stats entry for a newly created database object in a
/// transactional manner.
///
/// I.e. if the current (sub-)transaction aborts, the stats entry will also be
/// dropped.
pub fn pgstat_create_transactional(kind: PgStat_Kind, dboid: Oid, objid: u64) -> PgResult<()> {
    if shmem_seams::pgstat_get_entry_ref_exists::call(kind, dboid, objid)? {
        let name = pgstat_seams::pgstat_get_kind_name::call(kind);
        ereport(WARNING)
            .errmsg(format!(
                "resetting existing statistics for kind {name}, db={dboid}, oid={objid}"
            ))
            .finish(ErrorLocation::new(
                "pgstat_xact.c",
                365,
                "pgstat_create_transactional",
            ))?;

        pgstat_seams::pgstat_reset::call(kind, dboid, objid)?;
    }

    create_drop_transactional_internal(kind, dboid, objid, /* create */ true)
}

/// Drop a stats entry for a just dropped database object in a transactional
/// manner.
///
/// I.e. if the current (sub-)transaction aborts, the stats entry will stay
/// alive.
pub fn pgstat_drop_transactional(kind: PgStat_Kind, dboid: Oid, objid: u64) -> PgResult<()> {
    create_drop_transactional_internal(kind, dboid, objid, /* create */ false)
}

#[cfg(test)]
mod tests;
