use super::*;

#[test]
fn pending_deletes_commit_keeps_abort_drops_shape() {
    // No smgr wiring: exercise only the list logic via get/preserve.
    let cx = mcx::MemoryContext::new("catalog-storage-test");
    let mcx = cx.mcx();
    PENDING.with_borrow_mut(|p| {
        p.clear();
        p.push(PendingRelDelete {
            rlocator: RelFileLocator::new(1663, 5, 16384),
            proc_number: INVALID_PROC_NUMBER,
            at_commit: false,
            nest_level: 1,
        });
        p.push(PendingRelDelete {
            rlocator: RelFileLocator::new(1663, 5, 16385),
            proc_number: INVALID_PROC_NUMBER,
            at_commit: true,
            nest_level: 1,
        });
    });
    let commit = smgrGetPendingDeletes(mcx, true).unwrap();
    assert_eq!(commit.len(), 1);
    assert_eq!(commit[0].relNumber, 16385);
    let abort = smgrGetPendingDeletes(mcx, false).unwrap();
    assert_eq!(abort.len(), 1);
    assert_eq!(abort[0].relNumber, 16384);

    RelationPreserveStorage(RelFileLocator::new(1663, 5, 16384), false);
    assert_eq!(smgrGetPendingDeletes(mcx, false).unwrap().len(), 0);
    PENDING.with_borrow_mut(|p| p.clear());
}

#[test]
fn subcommit_lowers_nest_level() {
    PENDING.with_borrow_mut(|p| {
        p.clear();
        p.push(PendingRelDelete {
            rlocator: RelFileLocator::new(1663, 5, 16400),
            proc_number: INVALID_PROC_NUMBER,
            at_commit: false,
            nest_level: 3,
        });
    });
    // GetCurrentTransactionNestLevel() == 0 outside a transaction, so entries
    // stay >= level and get lowered to -1 == level-1 shape; assert mechanics
    // through direct state.
    AtSubCommit_smgr();
    PENDING.with_borrow(|p| assert_eq!(p[0].nest_level, xact::GetCurrentTransactionNestLevel() - 1));
    PENDING.with_borrow_mut(|p| p.clear());
}

#[test]
fn pending_syncs_registry_abort_and_parallel_discard() {
    PENDING_SYNCS.with_borrow_mut(|p| p.clear());
    let a = RelFileLocator::new(1663, 5, 16500);
    AddPendingSync(a);
    assert!(RelFileLocatorSkippingWAL(a));
    assert!(!RelFileLocatorSkippingWAL(RelFileLocator::new(1663, 5, 16501)));

    smgrDoPendingSyncs(false, false).unwrap();
    assert!(!RelFileLocatorSkippingWAL(a));

    AddPendingSync(a);
    smgrDoPendingSyncs(true, true).unwrap();
    assert!(!RelFileLocatorSkippingWAL(a));
}

#[test]
fn pending_syncs_commit_skips_locators_pending_delete() {
    PENDING_SYNCS.with_borrow_mut(|p| p.clear());
    PENDING.with_borrow_mut(|p| p.clear());
    let a = RelFileLocator::new(1663, 5, 16510);
    AddPendingSync(a);
    PENDING.with_borrow_mut(|p| {
        p.push(PendingRelDelete {
            rlocator: a,
            proc_number: INVALID_PROC_NUMBER,
            at_commit: true,
            nest_level: 1,
        });
    });
    // The only registered sync is also a commit-time delete: the commit pass
    // must touch no storage (no smgr wiring installed here).
    smgrDoPendingSyncs(true, false).unwrap();
    assert!(!RelFileLocatorSkippingWAL(a));
    PENDING.with_borrow_mut(|p| p.clear());
}
