//! ON COMMIT bookkeeping for temporary tables — `register_on_commit_action`,
//! `remove_on_commit_action`, `PreCommit_on_commit_actions`,
//! `AtEOXact_on_commit_actions`, `AtEOSubXact_on_commit_actions`
//! (tablecmds.c:19261-19483). The backend-local `on_commits` list is a
//! `thread_local!` Vec; the C `CacheMemoryContext`-allocated list has the same
//! backend-lifetime semantics.

#![allow(non_upper_case_globals)]

use std::cell::RefCell;

use backend_access_transam_xact::GetCurrentSubTransactionId;
use types_core::primitive::Oid;
use types_core::SubTransactionId;
use types_error::PgResult;
use types_nodes::primnodes::OnCommitAction;
use types_nodes::primnodes::OnCommitAction::{
    ONCOMMIT_DELETE_ROWS, ONCOMMIT_DROP, ONCOMMIT_NOOP, ONCOMMIT_PRESERVE_ROWS,
};

use backend_catalog_dependency_seams as dep_seam;
use backend_commands_tablecmds_seams as seam;
use backend_utils_time_snapmgr_seams as snapmgr_seam;

use crate::helpers::RelationRelationId;

/// `InvalidSubTransactionId` — the C `0`.
const InvalidSubTransactionId: SubTransactionId = 0;

/// `OnCommitItem` (tablecmds.c) — an entry of the backend-local `on_commits`
/// list.
#[derive(Clone)]
struct OnCommitItem {
    /// `Oid relid` — relation to do something with.
    relid: Oid,
    /// `OnCommitAction oncommit` — what to do.
    oncommit: OnCommitAction,
    /// `SubTransactionId creating_subid` — the subxact that created the entry.
    creating_subid: SubTransactionId,
    /// `SubTransactionId deleting_subid` — the subxact that deleted the rel.
    deleting_subid: SubTransactionId,
}

thread_local! {
    /// The `static List *on_commits` of tablecmds.c (backend-local).
    static ON_COMMITS: RefCell<Vec<OnCommitItem>> = const { RefCell::new(Vec::new()) };
}

/// `register_on_commit_action(relid, action)` (tablecmds.c:19261).
pub fn register_on_commit_action(relid: Oid, action: OnCommitAction) -> PgResult<()> {
    /*
     * We needn't bother registering the relation unless there is an ON COMMIT
     * action we need to take.
     */
    if action == ONCOMMIT_NOOP || action == ONCOMMIT_PRESERVE_ROWS {
        return Ok(());
    }

    let oc = OnCommitItem {
        relid,
        oncommit: action,
        creating_subid: GetCurrentSubTransactionId(),
        deleting_subid: InvalidSubTransactionId,
    };

    /*
     * We use lcons() here so that ON COMMIT actions are processed in reverse
     * order of registration.
     */
    ON_COMMITS.with(|oc_list| oc_list.borrow_mut().insert(0, oc));
    Ok(())
}

/// `remove_on_commit_action(relid)` (tablecmds.c:19297).
pub fn remove_on_commit_action(relid: Oid) -> PgResult<()> {
    ON_COMMITS.with(|oc_list| {
        let mut list = oc_list.borrow_mut();
        for oc in list.iter_mut() {
            if oc.relid == relid {
                oc.deleting_subid = GetCurrentSubTransactionId();
                break;
            }
        }
    });
    Ok(())
}

/// `PreCommit_on_commit_actions(void)` (tablecmds.c:19320).
pub fn pre_commit_on_commit_actions() -> PgResult<()> {
    let mut oids_to_truncate: Vec<Oid> = Vec::new();
    let mut oids_to_drop: Vec<Oid> = Vec::new();

    let accessed_temp = seam::xact_accessed_temp_namespace::call()?;

    ON_COMMITS.with(|oc_list| -> PgResult<()> {
        for oc in oc_list.borrow().iter() {
            /* Ignore entry if already dropped in this xact */
            if oc.deleting_subid != InvalidSubTransactionId {
                continue;
            }

            match oc.oncommit {
                ONCOMMIT_NOOP | ONCOMMIT_PRESERVE_ROWS => {
                    /* Do nothing (there shouldn't be such entries, actually) */
                }
                ONCOMMIT_DELETE_ROWS => {
                    /*
                     * If this transaction hasn't accessed any temporary
                     * relations, we can skip truncating ON COMMIT DELETE ROWS
                     * tables, as they must still be empty.
                     */
                    if accessed_temp {
                        oids_to_truncate.push(oc.relid);
                    }
                }
                ONCOMMIT_DROP => {
                    oids_to_drop.push(oc.relid);
                }
            }
        }
        Ok(())
    })?;

    /*
     * Truncate relations before dropping so that all dependencies between
     * relations are removed after they are worked on.
     */
    if !oids_to_truncate.is_empty() {
        // The owner `heap_truncate` carries `mcx`; this caller has none in scope
        // (it runs at pre-commit), so allocate a scratch context for the scans.
        let ctx = mcx::MemoryContext::new("heap_truncate");
        seam::heap_truncate::call(ctx.mcx(), &oids_to_truncate)?;
    }

    if !oids_to_drop.is_empty() {
        let mut target_objects = dep_seam::new_object_addresses::call()?;

        for &relid in oids_to_drop.iter() {
            let object = crate::helpers::object_address_set(RelationRelationId, relid);

            debug_assert!(!dep_seam::object_address_present::call(object, &target_objects)?);

            dep_seam::add_exact_object_address::call(object, &mut target_objects)?;
        }

        /*
         * Object deletion might involve toast table access (to clean up
         * toasted catalog entries), so ensure we have a valid snapshot.
         */
        snapmgr_seam::push_active_snapshot_transaction::call()?;

        /*
         * Since this is an automatic drop, rather than one directly initiated
         * by the user, we pass the PERFORM_DELETION_INTERNAL flag.
         */
        dep_seam::perform_multiple_deletions::call(
            &target_objects.refs,
            types_nodes::parsenodes::DROP_CASCADE,
            dep_seam::PERFORM_DELETION_INTERNAL | dep_seam::PERFORM_DELETION_QUIETLY,
        )?;

        snapmgr_seam::pop_active_snapshot::call()?;

        dep_seam::free_object_addresses::call(target_objects)?;
    }

    Ok(())
}

/// `AtEOXact_on_commit_actions(isCommit)` (tablecmds.c:19427).
pub fn at_eoxact_on_commit_actions(is_commit: bool) {
    ON_COMMITS.with(|oc_list| {
        let mut list = oc_list.borrow_mut();
        list.retain_mut(|oc| {
            let remove = if is_commit {
                oc.deleting_subid != InvalidSubTransactionId
            } else {
                oc.creating_subid != InvalidSubTransactionId
            };
            if remove {
                /* cur_item must be removed */
                false
            } else {
                /* cur_item must be preserved */
                oc.creating_subid = InvalidSubTransactionId;
                oc.deleting_subid = InvalidSubTransactionId;
                true
            }
        });
    });
}

/// `AtEOSubXact_on_commit_actions(isCommit, mySubid, parentSubid)`
/// (tablecmds.c:19459).
pub fn at_eosubxact_on_commit_actions(
    is_commit: bool,
    my_subid: SubTransactionId,
    parent_subid: SubTransactionId,
) {
    ON_COMMITS.with(|oc_list| {
        let mut list = oc_list.borrow_mut();
        list.retain_mut(|oc| {
            if !is_commit && oc.creating_subid == my_subid {
                /* cur_item must be removed */
                false
            } else {
                /* cur_item must be preserved */
                if oc.creating_subid == my_subid {
                    oc.creating_subid = parent_subid;
                }
                if oc.deleting_subid == my_subid {
                    oc.deleting_subid = if is_commit {
                        parent_subid
                    } else {
                        InvalidSubTransactionId
                    };
                }
                true
            }
        });
    });
}
