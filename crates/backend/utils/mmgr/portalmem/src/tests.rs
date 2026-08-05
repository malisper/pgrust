use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Once;

use ::types_portal::{
    CachedPlanHandle, StmtListHandle, TuplestoreHandle, CMDTAG_SELECT, CURSOR_OPT_HOLD,
    CURSOR_OPT_SCROLL, PORTAL_DEFINED, PORTAL_DONE, PORTAL_FAILED, PORTAL_NEW, PORTAL_READY,
};
use ::types_resowner::ResourceOwner;

use crate::*;

thread_local! {
    static EVENTS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    static CUR_SUBID: Cell<SubTransactionId> = const { Cell::new(1) };
    static NEST_LEVEL: Cell<i32> = const { Cell::new(1) };
    static STMT_TS: Cell<TimestampTz> = const { Cell::new(777_000) };
    static NEXT_OWNER: Cell<u32> = const { Cell::new(0) };
    static ACTIVE_SNAPS: Cell<i32> = const { Cell::new(0) };
    static SHMEM_EXIT: Cell<bool> = const { Cell::new(false) };
    static CLEANUP_FAILS: Cell<bool> = const { Cell::new(false) };
}

fn log(event: String) {
    EVENTS.with(|e| e.borrow_mut().push(event));
}

fn events() -> Vec<String> {
    EVENTS.with(|e| e.borrow_mut().drain(..).collect())
}

fn install() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        xact_seams::get_current_sub_transaction_id::set(|| CUR_SUBID.get());
        xact_seams::get_current_transaction_nest_level::set(|| NEST_LEVEL.get());
        xact_portal_seams::get_current_statement_start_timestamp::set(|| STMT_TS.get());
        resowner_portal_seams::resource_owner_create_portal::set(|| {
            let id = NEXT_OWNER.with(|c| {
                c.set(c.get() + 1);
                c.get()
            });
            ResourceOwner::from_parts(id, 1)
        });
        resowner_portal_seams::resource_owner_release::set(|o, phase, is_commit, top| {
            log(format!("release({},{:?},{is_commit},{top})", o.slot(), phase));
        });
        resowner_portal_seams::resource_owner_delete::set(|o| {
            log(format!("owner_delete({})", o.slot()));
        });
        resowner_portal_seams::resource_owner_new_parent::set(|o, p| {
            log(format!("new_parent({},{})", o.slot(), p.slot()));
        });
        plancache_portal_seams::release_cached_plan::set(|c| {
            log(format!("release_cplan({})", c.0));
        });
        pquery_seams::stmt_list_free::set(|h| {
            log(format!("stmt_list_free({})", h.0));
        });
        portalcmds_seams::portal_cleanup::set(|p| {
            log(format!("cleanup({})", p.borrow().name.as_str()));
            if CLEANUP_FAILS.get() {
                return Err(ereport(ERROR).errmsg_internal("cleanup boom").into_error().into());
            }
            Ok(())
        });
        portalcmds_seams::persist_holdable_portal::set(|p| {
            log(format!("persist({})", p.borrow().name.as_str()));
            Ok(())
        });
        tuplestore_hold_seams::tuplestore_begin_heap_hold::set(|ra| {
            log(format!("ts_begin({ra})"));
            Ok(TuplestoreHandle(42))
        });
        tuplestore_hold_seams::tuplestore_end::set(|s| log(format!("ts_end({})", s.0)));
        ipc_portal_seams::shmem_exit_inprogress::set(|| SHMEM_EXIT.get());
        snapmgr_portal_seams::unregister_snapshot_from_owner::set(|_s, o| {
            log(format!("unreg_snap({})", o.slot()));
        });
        snapmgr_portal_seams::active_snapshot_set::set(|| ACTIVE_SNAPS.get() > 0);
        snapmgr_portal_seams::pop_active_snapshot::set(|| {
            ACTIVE_SNAPS.with(|c| c.set(c.get() - 1));
            Ok(())
        });
    });
}

fn setup() {
    install();
    EnablePortalManager();
    EVENTS.with(|e| e.borrow_mut().clear());
    CUR_SUBID.set(1);
    NEST_LEVEL.set(1);
    CLEANUP_FAILS.set(false);
    SHMEM_EXIT.set(false);
}

fn define_simple(portal: &Portal<'static>, source: &str) {
    PortalDefineQuery(
        portal,
        None,
        source,
        CMDTAG_SELECT,
        StmtListHandle(9),
        CachedPlanHandle::NULL,
    )
    .unwrap();
}

#[test]
fn create_lookup_drop() {
    setup();
    let portal = CreatePortal("c1", false, false).unwrap();
    {
        let p = portal.borrow();
        assert_eq!(p.name.as_str(), "c1");
        assert_eq!(p.status, PORTAL_NEW);
        assert_eq!(p.strategy, PORTAL_MULTI_QUERY);
        assert_eq!(p.cursorOptions, CURSOR_OPT_NO_SCROLL);
        assert!(p.atStart && p.atEnd && p.visible);
        assert_eq!(p.createSubid, 1);
        assert_eq!(p.creation_time, 777_000);
        assert!(p.portalContext.is_some());
    }
    assert!(GetPortalByName(Some("c1")).unwrap().ptr_eq(&portal));
    assert!(GetPortalByName(Some("nope")).is_none());
    assert!(GetPortalByName(None).is_none());

    PortalDrop(&portal, false).unwrap();
    assert!(GetPortalByName(Some("c1")).is_none());
    let ev = events();
    assert_eq!(ev[0], "cleanup(c1)");
    assert!(ev[1].starts_with("release(1,RESOURCE_RELEASE_BEFORE_LOCKS,true,false"));
    assert!(ev[2].contains("RESOURCE_RELEASE_LOCKS"));
    assert!(ev[3].contains("RESOURCE_RELEASE_AFTER_LOCKS"));
    assert_eq!(ev[4], "owner_delete(1)");
    assert!(portal.borrow().portalContext.is_none());
}

#[test]
fn duplicate_name_semantics() {
    setup();
    let a = CreatePortal("dup", false, false).unwrap();
    let Err(err) = CreatePortal("dup", false, false) else { panic!("expected error") };
    assert_eq!(err.sqlstate(), ERRCODE_DUPLICATE_CURSOR);
    assert!(err.message().contains("cursor \"dup\" already exists"));

    let b = CreatePortal("dup", true, true).unwrap();
    assert!(!a.ptr_eq(&b));
    assert!(GetPortalByName(Some("dup")).unwrap().ptr_eq(&b));
    PortalDrop(&b, false).unwrap();
}

#[test]
fn overlong_names_truncate_and_collide() {
    setup();
    let long_a = "x".repeat(80);
    let long_b = format!("{}{}", "x".repeat(63), "different-tail");
    let a = CreatePortal(&long_a, false, false).unwrap();
    assert_eq!(a.borrow().name.len(), MAX_PORTALNAME_LEN - 1);
    let Err(err) = CreatePortal(&long_b, false, false) else { panic!("expected error") };
    assert_eq!(err.sqlstate(), ERRCODE_DUPLICATE_CURSOR);
    PortalDrop(&a, false).unwrap();
}

#[test]
fn create_new_portal_skips_conflicts() {
    setup();
    let taken = CreatePortal("<unnamed portal 1>", false, false).unwrap();
    let fresh = CreateNewPortal().unwrap();
    assert_eq!(fresh.borrow().name.as_str(), "<unnamed portal 2>");
    PortalDrop(&taken, false).unwrap();
    PortalDrop(&fresh, false).unwrap();
}

#[test]
fn define_query_stores_and_shares_handles() {
    setup();
    let portal = CreatePortal("", false, false).unwrap();
    PortalDefineQuery(
        &portal,
        Some("ps1"),
        "select 1",
        CMDTAG_SELECT,
        StmtListHandle(5),
        CachedPlanHandle(11),
    )
    .unwrap();
    {
        let p = portal.borrow();
        assert_eq!(p.status, PORTAL_DEFINED);
        assert_eq!(p.sourceText.as_ref().unwrap().as_str(), "select 1");
        assert_eq!(p.prepStmtName.as_ref().unwrap().as_str(), "ps1");
        assert_eq!(p.stmts, StmtListHandle(5));
        assert_eq!(p.cplan, CachedPlanHandle(11));
        assert_eq!(p.qc.commandTag, CMDTAG_SELECT);
        assert_eq!(p.qc.nprocessed, 0);
    }
    PortalDrop(&portal, false).unwrap();
    assert!(events().contains(&"release_cplan(11)".to_owned()));
    assert!(portal.borrow().cplan.is_null());
    assert!(portal.borrow().stmts.is_null());
}

#[test]
fn mark_transitions() {
    setup();
    let portal = CreatePortal("t", false, false).unwrap();
    define_simple(&portal, "q");

    let err = MarkPortalActive(&portal).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);

    portal.borrow_mut().status = PORTAL_READY;
    CUR_SUBID.set(7);
    MarkPortalActive(&portal).unwrap();
    assert_eq!(portal.borrow().status, PORTAL_ACTIVE);
    assert_eq!(portal.borrow().activeSubid, 7);

    let err = PortalDrop(&portal, false).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_CURSOR_STATE);
    assert!(err.message().contains("cannot drop active portal"));

    MarkPortalDone(&portal).unwrap();
    assert_eq!(portal.borrow().status, PORTAL_DONE);
    assert_eq!(events(), vec!["cleanup(t)".to_owned()]);
    PortalDrop(&portal, false).unwrap();
    assert!(!events().contains(&"cleanup(t)".to_owned()));
}

#[test]
fn pinned_portal_rules() {
    setup();
    let portal = CreatePortal("p", false, false).unwrap();
    PinPortal(&portal).unwrap();
    assert!(PinPortal(&portal).is_err());
    let err = PortalDrop(&portal, false).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_CURSOR_STATE);
    assert!(PreCommit_Portals(false).is_err());
    UnpinPortal(&portal).unwrap();
    assert!(UnpinPortal(&portal).is_err());
    PortalDrop(&portal, false).unwrap();
}

#[test]
fn hold_store_lifecycle() {
    setup();
    let portal = CreatePortal("h", false, false).unwrap();
    portal.borrow_mut().cursorOptions |= CURSOR_OPT_SCROLL;
    PortalCreateHoldStore(&portal).unwrap();
    assert_eq!(portal.borrow().holdStore, TuplestoreHandle(42));
    assert!(portal.borrow().holdContext.is_some());
    assert_eq!(events(), vec!["ts_begin(true)".to_owned()]);

    PortalDrop(&portal, false).unwrap();
    assert!(events().contains(&"ts_end(42)".to_owned()));
    assert!(portal.borrow().holdStore.is_null());
    assert!(portal.borrow().holdContext.is_none());
}

#[test]
fn precommit_holds_holdable_and_drops_the_rest() {
    setup();
    let holdable = CreatePortal("holdme", false, false).unwrap();
    define_simple(&holdable, "q1");
    {
        let mut p = holdable.borrow_mut();
        p.cursorOptions |= CURSOR_OPT_HOLD;
        p.status = PORTAL_READY;
    }
    let plain = CreatePortal("plain", false, false).unwrap();
    define_simple(&plain, "q2");
    let held_over = CreatePortal("old", false, false).unwrap();
    held_over.borrow_mut().createSubid = InvalidSubTransactionId;

    assert!(PreCommit_Portals(false).unwrap());

    let h = holdable.borrow();
    assert_eq!(h.createSubid, InvalidSubTransactionId);
    assert_eq!(h.createLevel, 0);
    assert!(h.resowner.is_null());
    assert!(GetPortalByName(Some("holdme")).is_some());
    assert!(GetPortalByName(Some("plain")).is_none());
    assert!(GetPortalByName(Some("old")).is_some());
    let ev = events();
    assert!(ev.contains(&"persist(holdme)".to_owned()));

    drop(h);
    assert!(!PreCommit_Portals(false).unwrap());
}

#[test]
fn precommit_prepare_refuses_holdable() {
    setup();
    let holdable = CreatePortal("hp", false, false).unwrap();
    {
        let mut p = holdable.borrow_mut();
        p.cursorOptions |= CURSOR_OPT_HOLD;
        p.status = PORTAL_READY;
    }
    let err = PreCommit_Portals(true).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
}

#[test]
fn at_abort_fails_ready_portals_and_releases_plans() {
    setup();
    let portal = CreatePortal("ab", false, false).unwrap();
    PortalDefineQuery(
        &portal,
        None,
        "q",
        CMDTAG_SELECT,
        StmtListHandle(3),
        CachedPlanHandle(30),
    )
    .unwrap();
    portal.borrow_mut().status = PORTAL_READY;

    AtAbort_Portals().unwrap();
    {
        let p = portal.borrow();
        assert_eq!(p.status, PORTAL_FAILED);
        assert!(p.resowner.is_null());
        assert!(p.cplan.is_null());
        assert!(p.stmts.is_null());
    }
    let ev = events();
    assert!(ev.contains(&"cleanup(ab)".to_owned()));
    assert!(ev.contains(&"release_cplan(30)".to_owned()));

    AtCleanup_Portals().unwrap();
    assert!(GetPortalByName(Some("ab")).is_none());
}

#[test]
fn at_cleanup_unpins_and_warns_on_unrun_hook() {
    setup();
    let portal = CreatePortal("cl", false, false).unwrap();
    define_simple(&portal, "q");
    portal.borrow_mut().portalPinned = true;

    AtCleanup_Portals().unwrap();
    assert!(GetPortalByName(Some("cl")).is_none());
    assert!(!events().contains(&"cleanup(cl)".to_owned()));
}

#[test]
fn error_cleanup_drops_only_auto_held() {
    setup();
    let auto_held = CreatePortal("auto", false, false).unwrap();
    auto_held.borrow_mut().autoHeld = true;
    auto_held.borrow_mut().portalPinned = true;
    let normal = CreatePortal("norm", false, false).unwrap();

    PortalErrorCleanup().unwrap();
    assert!(GetPortalByName(Some("auto")).is_none());
    assert!(GetPortalByName(Some("norm")).is_some());
    PortalDrop(&normal, false).unwrap();
}

#[test]
fn subxact_lifecycle() {
    setup();
    CUR_SUBID.set(5);
    NEST_LEVEL.set(2);
    let portal = CreatePortal("sub", false, false).unwrap();
    define_simple(&portal, "q");
    assert_eq!(portal.borrow().createSubid, 5);

    let parent_owner = ResourceOwner::from_parts(900, 1);
    AtSubCommit_Portals(5, 1, 1, parent_owner);
    {
        let p = portal.borrow();
        assert_eq!(p.createSubid, 1);
        assert_eq!(p.createLevel, 1);
    }
    assert!(events().iter().any(|e| e.starts_with("new_parent(") && e.ends_with(",900)")));

    AtSubAbort_Portals(6, 1, ResourceOwner::from_parts(901, 1), parent_owner).unwrap();
    assert_eq!(portal.borrow().createSubid, 1);

    PortalDrop(&portal, false).unwrap();
}

#[test]
fn subxact_abort_fails_and_cleanup_drops() {
    setup();
    CUR_SUBID.set(9);
    let portal = CreatePortal("subab", false, false).unwrap();
    define_simple(&portal, "q");
    portal.borrow_mut().status = PORTAL_READY;

    AtSubAbort_Portals(9, 1, ResourceOwner::from_parts(902, 1), ResourceOwner::NULL).unwrap();
    {
        let p = portal.borrow();
        assert_eq!(p.status, PORTAL_FAILED);
        assert!(p.resowner.is_null());
    }
    assert!(events().contains(&"cleanup(subab)".to_owned()));

    AtSubCleanup_Portals(9).unwrap();
    assert!(GetPortalByName(Some("subab")).is_none());
}

#[test]
fn upper_portal_used_in_failed_subxact_reattaches() {
    setup();
    let portal = CreatePortal("upper", false, false).unwrap();
    define_simple(&portal, "q");
    portal.borrow_mut().status = PORTAL_FAILED;
    portal.borrow_mut().activeSubid = 4;
    let owner_slot = portal.borrow().resowner.slot();

    let my_owner = ResourceOwner::from_parts(950, 1);
    AtSubAbort_Portals(4, 2, my_owner, ResourceOwner::NULL).unwrap();
    {
        let p = portal.borrow();
        assert_eq!(p.activeSubid, 2);
        assert!(p.resowner.is_null());
    }
    assert!(events().contains(&format!("new_parent({owner_slot},950)")));
    portal.borrow_mut().createSubid = InvalidSubTransactionId;
    AtCleanup_Portals().unwrap();
    PortalDrop(&portal, false).unwrap();
}

#[test]
fn hold_pinned_portals_and_ready_scan() {
    setup();
    let pinned = CreatePortal("pin", false, false).unwrap();
    define_simple(&pinned, "q");
    {
        let mut p = pinned.borrow_mut();
        p.portalPinned = true;
        p.strategy = PORTAL_ONE_SELECT;
        p.status = PORTAL_READY;
    }
    assert!(!ThereAreNoReadyPortals());

    HoldPinnedPortals().unwrap();
    {
        let p = pinned.borrow();
        assert!(p.autoHeld);
        assert!(p.resowner.is_null());
        assert_eq!(p.createSubid, InvalidSubTransactionId);
    }
    assert!(events().contains(&"persist(pin)".to_owned()));

    pinned.borrow_mut().portalPinned = false;
    PortalDrop(&pinned, false).unwrap();
    assert!(ThereAreNoReadyPortals());
}

#[test]
fn hold_pinned_refuses_non_select() {
    setup();
    let pinned = CreatePortal("pin2", false, false).unwrap();
    pinned.borrow_mut().portalPinned = true;
    pinned.borrow_mut().status = PORTAL_READY;
    let err = HoldPinnedPortals().unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
}

#[test]
fn forget_portal_snapshots_balances() {
    setup();
    let portal = CreatePortal("fs", false, false).unwrap();
    let top = mgr("test", |m| m.top).unwrap();
    portal.borrow_mut().portalSnapshot = Some(Rc::new(
        ::types_snapshot::SnapshotData::sentinel(top.mcx(), ::types_snapshot::SNAPSHOT_MVCC),
    ));
    ACTIVE_SNAPS.set(1);
    ForgetPortalSnapshots().unwrap();
    assert!(portal.borrow().portalSnapshot.is_none());
    assert_eq!(ACTIVE_SNAPS.get(), 0);

    ACTIVE_SNAPS.set(1);
    let err = ForgetPortalSnapshots().unwrap_err();
    assert!(err.message().contains("did not account"));
    PortalDrop(&portal, false).unwrap();
}

#[test]
fn delete_all_skips_active() {
    setup();
    let active = CreatePortal("act", false, false).unwrap();
    define_simple(&active, "q");
    active.borrow_mut().status = PORTAL_READY;
    MarkPortalActive(&active).unwrap();
    let other = CreatePortal("oth", false, false).unwrap();

    PortalHashTableDeleteAll().unwrap();
    assert!(GetPortalByName(Some("act")).is_some());
    assert!(GetPortalByName(Some("oth")).is_none());
    drop(other);

    MarkPortalDone(&active).unwrap();
    PortalDrop(&active, false).unwrap();
}

#[test]
fn pg_cursor_rows_filters_and_orders() {
    setup();
    let first = CreatePortal("first", false, false).unwrap();
    define_simple(&first, "select 1");
    let hidden = CreatePortal("hidden", false, false).unwrap();
    define_simple(&hidden, "select 2");
    hidden.borrow_mut().visible = false;
    let undefined = CreatePortal("undef", false, false).unwrap();
    let second = CreatePortal("second", false, false).unwrap();
    define_simple(&second, "select 3");
    second.borrow_mut().cursorOptions |= CURSOR_OPT_HOLD;

    let ctx = MemoryContext::new("pg_cursor scratch");
    let rows = pg_cursor_rows(ctx.mcx()).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].name.as_str(), "first");
    assert_eq!(rows[0].statement.as_str(), "select 1");
    assert!(!rows[0].is_holdable);
    assert_eq!(rows[0].creation_time, 777_000);
    assert_eq!(rows[1].name.as_str(), "second");
    assert!(rows[1].is_holdable);
    drop(rows);

    for p in [&first, &hidden, &undefined, &second] {
        PortalDrop(p, false).unwrap();
    }
}

#[test]
fn xact_entry_points_roundtrip() {
    setup();
    let portal = CreatePortal("via_seam", false, false).unwrap();
    define_simple(&portal, "q");
    portal.borrow_mut().status = PORTAL_READY;
    AtAbort_Portals().unwrap();
    assert_eq!(portal.borrow().status, PORTAL_FAILED);
    AtCleanup_Portals().unwrap();
    assert!(GetPortalByName(Some("via_seam")).is_none());
    assert!(!PreCommit_Portals(false).unwrap());
    AtSubCommit_Portals(3, 1, 1, ResourceOwner::NULL);
    AtSubAbort_Portals(3, 1, ResourceOwner::NULL, ResourceOwner::NULL).unwrap();
    AtSubCleanup_Portals(3).unwrap();
}

#[test]
fn cleanup_hook_failure_leaves_hook_armed() {
    setup();
    let portal = CreatePortal("boom", false, false).unwrap();
    define_simple(&portal, "q");
    portal.borrow_mut().status = PORTAL_READY;
    portal.borrow_mut().status = PORTAL_ACTIVE;
    CLEANUP_FAILS.set(true);
    assert!(MarkPortalDone(&portal).is_err());
    // C: portal->cleanup = NULL only after a successful hook return.
    assert_eq!(portal.borrow().cleanup, PortalCleanupHook::PortalCleanup);
    CLEANUP_FAILS.set(false);
    PortalDrop(&portal, false).unwrap();
}
