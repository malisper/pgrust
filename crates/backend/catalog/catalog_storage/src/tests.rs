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

// audit-18.6 b108 (a186-candidate-fp-catalog-storage-8c56f246053f276f9d42-1):
// storage.c:146 `elog(ERROR, "invalid relpersistence: %c", relpersistence)`
// — an ERROR (XX000) carrying the byte as a character, never a process
// panic and never the decimal rendering.
#[test]
fn relation_create_storage_invalid_relpersistence_is_internal_error() {
    let err = RelationCreateStorage(RelFileLocator::new(1663, 5, 16530), b'x', false)
        .expect_err("storage.c:146: an invalid relpersistence is elog(ERROR)");
    assert_eq!(err.level, types_error::ERROR);
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
    assert_eq!(err.message(), "invalid relpersistence: x");
}

// audit-18.6 b108 (a186-candidate-fp-catalog-storage-e1e9cb312d4a29373004-1):
// storage.c:626-630 SerializePendingSyncs removes every relation that a
// commit-time pendingDeletes entry drops (CREATE TABLE ... DROP TABLE in one
// transaction under wal_level=minimal) before handing the list to parallel
// workers; abort-time entries do not filter.
#[test]
fn serialize_pending_syncs_omits_commit_time_deletes() {
    PENDING_SYNCS.with_borrow_mut(|p| p.clear());
    PENDING.with_borrow_mut(|p| p.clear());
    let live = RelFileLocator::new(1663, 5, 16520);
    let dropped = RelFileLocator::new(1663, 5, 16521);
    AddPendingSync(live);
    AddPendingSync(dropped);
    PENDING.with_borrow_mut(|p| {
        p.push(PendingRelDelete {
            rlocator: dropped,
            proc_number: INVALID_PROC_NUMBER,
            at_commit: true,
            nest_level: 1,
        });
        p.push(PendingRelDelete {
            rlocator: live,
            proc_number: INVALID_PROC_NUMBER,
            at_commit: false,
            nest_level: 1,
        });
    });

    let serialized = SerializePendingSyncs();
    let numbers: Vec<u32> = serialized.iter().map(|(l, _)| l.relNumber).collect();
    assert_eq!(numbers, vec![16520], "dropped-at-commit relation must not reach workers");

    // The leader's own registry is untouched (only the serialized copy filters).
    assert!(RelFileLocatorSkippingWAL(dropped));
    PENDING_SYNCS.with_borrow_mut(|p| p.clear());
    PENDING.with_borrow_mut(|p| p.clear());
}
