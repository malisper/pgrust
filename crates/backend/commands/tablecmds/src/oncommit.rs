// tablecmds.c on_commits machinery.
use std::cell::RefCell;

use mcx::{Mcx, MemoryContext};
use types_core::{InvalidSubTransactionId, Oid, SubTransactionId, XACT_FLAGS_ACCESSEDTEMPNAMESPACE};
use types_error::PgResult;
use types_nodes::rawnodes::OnCommitAction;

struct OnCommitItem {
    relid: Oid,
    oncommit: OnCommitAction,
    creating_subid: SubTransactionId,
    deleting_subid: SubTransactionId,
}

thread_local! {
    static ON_COMMITS: RefCell<Vec<OnCommitItem>> = const { RefCell::new(Vec::new()) };
}

pub fn register_on_commit_action(relid: Oid, action: OnCommitAction) {
    if matches!(action, OnCommitAction::ONCOMMIT_NOOP | OnCommitAction::ONCOMMIT_PRESERVE_ROWS) {
        return;
    }
    ON_COMMITS.with_borrow_mut(|l| {
        // C lcons: reverse registration order; index 0 == list head.
        l.insert(
            0,
            OnCommitItem {
                relid,
                oncommit: action,
                creating_subid: xact::GetCurrentSubTransactionId(),
                deleting_subid: InvalidSubTransactionId,
            },
        )
    });
}

pub fn remove_on_commit_action(relid: Oid) {
    ON_COMMITS.with_borrow_mut(|l| {
        if let Some(oc) = l.iter_mut().find(|oc| oc.relid == relid) {
            oc.deleting_subid = xact::GetCurrentSubTransactionId();
        }
    });
}

pub fn PreCommit_on_commit_actions() -> PgResult<()> {
    let mut oids_to_truncate: Vec<Oid> = Vec::new();
    let mut oids_to_drop: Vec<Oid> = Vec::new();
    ON_COMMITS.with_borrow(|l| {
        for oc in l {
            if oc.deleting_subid != InvalidSubTransactionId {
                continue;
            }
            match oc.oncommit {
                OnCommitAction::ONCOMMIT_NOOP | OnCommitAction::ONCOMMIT_PRESERVE_ROWS => {}
                OnCommitAction::ONCOMMIT_DELETE_ROWS => {
                    if xact::MyXactFlags() & XACT_FLAGS_ACCESSEDTEMPNAMESPACE != 0 {
                        oids_to_truncate.push(oc.relid);
                    }
                }
                OnCommitAction::ONCOMMIT_DROP => oids_to_drop.push(oc.relid),
            }
        }
    });

    if !oids_to_truncate.is_empty() {
        let scratch = MemoryContext::new("PreCommit_on_commit_actions");
        let mcx: Mcx<'_> = scratch.mcx();
        catalog_heap::heap_truncate(mcx, &oids_to_truncate)?;
    }

    if !oids_to_drop.is_empty() {
        let scratch = MemoryContext::new("PreCommit_on_commit_actions");
        let mcx: Mcx<'_> = scratch.mcx();
        let mut target_objects = catalog_dependency::ObjectAddresses::new();
        for relid in &oids_to_drop {
            target_objects.add_exact_object_address(pg_depend::ObjectAddress::set(
                types_core::RELATION_RELATION_ID,
                *relid,
            ));
        }
        let snapshot = snapmgr::GetTransactionSnapshot()?;
        snapmgr::PushActiveSnapshot(&snapshot)?;
        let result = catalog_dependency::performMultipleDeletions(
            mcx,
            &target_objects,
            catalog_dependency::DropBehavior::DROP_CASCADE,
            catalog_dependency::PERFORM_DELETION_INTERNAL
                | catalog_dependency::PERFORM_DELETION_QUIETLY,
        );
        snapmgr::PopActiveSnapshot()?;
        result?;
        #[cfg(debug_assertions)]
        ON_COMMITS.with_borrow(|l| {
            for oc in l {
                if matches!(oc.oncommit, OnCommitAction::ONCOMMIT_DROP) {
                    debug_assert!(oc.deleting_subid != InvalidSubTransactionId);
                }
            }
        });
    }
    Ok(())
}

pub fn AtEOXact_on_commit_actions(is_commit: bool) {
    ON_COMMITS.with_borrow_mut(|l| {
        l.retain_mut(|oc| {
            if is_commit && oc.deleting_subid != InvalidSubTransactionId
                || !is_commit && oc.creating_subid != InvalidSubTransactionId
            {
                false
            } else {
                oc.creating_subid = InvalidSubTransactionId;
                oc.deleting_subid = InvalidSubTransactionId;
                true
            }
        })
    });
}

pub fn AtEOSubXact_on_commit_actions(
    is_commit: bool,
    my_subid: SubTransactionId,
    parent_subid: SubTransactionId,
) {
    ON_COMMITS.with_borrow_mut(|l| {
        l.retain_mut(|oc| {
            if !is_commit && oc.creating_subid == my_subid {
                false
            } else {
                if oc.creating_subid == my_subid {
                    oc.creating_subid = parent_subid;
                }
                if oc.deleting_subid == my_subid {
                    oc.deleting_subid =
                        if is_commit { parent_subid } else { InvalidSubTransactionId };
                }
                true
            }
        })
    });
}
