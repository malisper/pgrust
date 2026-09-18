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

// dynahash keys the first 63 BYTES: names whose byte 63 splits a multibyte
// character differently are distinct portals (C accepts both Binds).
#[test]
fn split_character_at_byte_63_keeps_names_distinct() {
    setup();
    let name_a = format!("{}é", "a".repeat(62));
    let name_b = format!("{}中", "a".repeat(62));
    let a = CreatePortal(&name_a, false, false).unwrap();
    let b = CreatePortal(&name_b, false, false).unwrap();
    assert_eq!(a.borrow().name.as_str(), "a".repeat(62));
    assert!(GetPortalByName(Some(&name_a)).unwrap().ptr_eq(&a));
    assert!(GetPortalByName(Some(&name_b)).unwrap().ptr_eq(&b));
    PortalDrop(&a, false).unwrap();
    assert!(GetPortalByName(Some(&name_a)).is_none());
    assert!(GetPortalByName(Some(&name_b)).unwrap().ptr_eq(&b));
    PortalDrop(&b, false).unwrap();
}

// portalmem.c:595 deletes the context: a parked (pooled) PortalContext is
// not a visible child of TopPortalContext until a portal reuses it.
#[test]
fn parked_portal_contexts_leave_the_context_tree() {
    setup();
    let portal_children = || {
        with_mgr(|m| {
            m.top.stats_tree().children.iter().filter(|c| c.name == "PortalContext").count()
        })
        .unwrap()
    };
    let before = portal_children();
    let p = CreatePortal("b50_parked", false, false).unwrap();
    assert_eq!(portal_children(), before + 1);
    PortalDrop(&p, false).unwrap();
    assert_eq!(portal_children(), before);
    let q = CreatePortal("b50_reused", false, false).unwrap();
    assert_eq!(portal_children(), before + 1);
    PortalDrop(&q, false).unwrap();
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
        assert_eq!(p.sourceText.unwrap(), "select 1");
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

// A WITH HOLD cursor outlives its transaction (createSubid Invalid, no
// resowner) and AtCleanup_Portals leaves it alone, as C does; the session's
// Portals teardown phase must then PortalDrop it so its tuplestore is ended.
#[test]
fn session_teardown_drops_held_portals_and_ends_their_stores() {
    setup();
    let held = CreatePortal("held", false, false).unwrap();
    define_simple(&held, "select 1");
    held.borrow_mut().cursorOptions |= CURSOR_OPT_SCROLL | CURSOR_OPT_HOLD;
    PortalCreateHoldStore(&held).unwrap();
    {
        let mut p = held.borrow_mut();
        p.status = PORTAL_READY;
        p.resowner = ResourceOwner::NULL;
        p.createSubid = InvalidSubTransactionId;
        p.activeSubid = InvalidSubTransactionId;
        p.cleanup = PortalCleanupHook::None;
    }
    AtCleanup_Portals().unwrap();
    assert!(GetPortalByName(Some("held")).is_some(), "held cursor survives cleanup");
    EVENTS.with(|e| e.borrow_mut().clear());

    session_teardown_portals();

    assert!(events().contains(&"ts_end(42)".to_owned()), "hold store ended: {:?}", events());
    assert!(PORTAL_MGR.with(|m| m.borrow().is_none()), "manager torn down");
    // Idempotent: a second drain (or a never-enabled manager) is a no-op.
    session_teardown_portals();
    EnablePortalManager();
}

// A parked (retained-execution) shell pins its plan, query descriptor and
// statement list; the Portals teardown phase must release them like
// discard_shell does on displacement.
#[test]
fn session_teardown_releases_parked_shells() {
    setup();
    execmain_seams::release_query_desc::set(|q| log(format!("release_qd({})", q.0)));
    let shell = CreatePortal("", false, false).unwrap();
    remove_from_table(&shell).unwrap();
    {
        let mut p = shell.borrow_mut();
        p.queryDesc = QueryDescHandle(71);
        p.stmts = StmtListHandle(72);
        p.cplan = CachedPlanHandle(73);
    }
    with_mgr(|m| m.parked.push((PlanSourceHandle(5), shell.clone()))).unwrap();
    EVENTS.with(|e| e.borrow_mut().clear());

    session_teardown_portals();

    let ev = events();
    for want in ["release_qd(71)", "stmt_list_free(72)", "release_cplan(73)"] {
        assert!(ev.contains(&want.to_owned()), "{want} missing: {ev:?}");
    }
    assert!(shell.borrow().cplan.is_null());
    EnablePortalManager();
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
    assert!(ev.contains(&"stmt_list_free(3)".to_owned()), "{ev:?}");

    AtCleanup_Portals().unwrap();
    assert!(GetPortalByName(Some("ab")).is_none());
}

#[test]
fn hold_frees_the_stmts_handle_with_the_plan() {
    setup();
    let portal = CreatePortal("hs", false, false).unwrap();
    PortalDefineQuery(
        &portal,
        None,
        "q",
        CMDTAG_SELECT,
        StmtListHandle(12),
        CachedPlanHandle(13),
    )
    .unwrap();
    {
        let mut p = portal.borrow_mut();
        p.cursorOptions |= CURSOR_OPT_HOLD;
        p.status = PORTAL_READY;
    }

    assert!(PreCommit_Portals(false).unwrap());
    {
        let p = portal.borrow();
        assert!(p.cplan.is_null());
        assert!(p.stmts.is_null());
    }
    let ev = events();
    assert!(ev.contains(&"release_cplan(13)".to_owned()), "{ev:?}");
    assert!(ev.contains(&"stmt_list_free(12)".to_owned()), "{ev:?}");

    PortalDrop(&portal, false).unwrap();
    assert!(!events().contains(&"stmt_list_free(12)".to_owned()));
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
    // C's hash_seq order: string_hash('second') lands in bucket 9,
    // 'first' in bucket 10 (hashtext(name) & 15 on 18.6).
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1].name.as_str(), "first");
    assert_eq!(rows[1].statement.as_str(), "select 1");
    assert!(!rows[1].is_holdable);
    assert_eq!(rows[1].creation_time, 777_000);
    assert_eq!(rows[0].name.as_str(), "second");
    assert!(rows[0].is_holdable);
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

// portalmem.c:225 CreatePortal: MemoryContextSetIdentifier(portalContext,
// portal->name[0] ? portal->name : "<unnamed>") — the unnamed portal's
// context is identified too (pg_backend_memory_contexts.ident, the
// pg_log_backend_memory_contexts dump line).
#[test]
fn unnamed_portal_context_ident_is_unnamed() {
    setup();
    let unnamed = CreatePortal("", false, false).unwrap();
    let named = CreatePortal("c_ident", false, false).unwrap();
    let ident_of = |p: &Portal<'static>| {
        p.borrow().portalContext.as_ref().expect("portalContext").ident()
    };
    assert_eq!(ident_of(&unnamed).as_deref(), Some("<unnamed>"));
    assert_eq!(ident_of(&named).as_deref(), Some("c_ident"));
    PortalDrop(&named, false).unwrap();
    PortalDrop(&unnamed, false).unwrap();
    // A parked (recycled) context comes back re-identified, never stale.
    let again = CreatePortal("", false, false).unwrap();
    assert_eq!(ident_of(&again).as_deref(), Some("<unnamed>"));
    PortalDrop(&again, false).unwrap();
}

// C 18.6 (hashtext = hash_bytes): '' -> bucket 13, 'a' -> 1, 'b' -> 0 in
// the fresh 16-bucket table, so a walk holds b before a; the per-statement
// unnamed portal comes and goes without disturbing that.
#[test]
fn dynahash_scan_order_is_bucket_then_chain() {
    let mut o = DynaOrder::new();
    let unnamed = PortalName::new("");
    o.insert(unnamed);
    o.remove(&unnamed);
    o.insert(PortalName::new("a"));
    o.insert(unnamed);
    o.remove(&unnamed);
    o.insert(PortalName::new("b"));
    let names: Vec<&str> = o.iter().map(|n| n.as_str()).collect();
    assert_eq!(names, ["b", "a"]);
    assert_eq!(o.nentries, 2);
    // Chains keep insertion order (HASH_ENTER links new entries last).
    let mut o = DynaOrder::new();
    for n in ["a", "b", "a2"] {
        o.insert(PortalName::new(n));
    }
    let a_bucket = o.bucket(DynaOrder::hash(&PortalName::new("a")));
    let first_a = o.iter().position(|n| n.as_str() == "a").unwrap();
    assert_eq!(a_bucket, 1);
    assert!(first_a >= 1);
    // 17 live entries split bucket 0 into bucket 16 (max_bucket 16).
    let mut o = DynaOrder::new();
    for i in 0..17 {
        o.insert(PortalName::new(&format!("c{i}")));
    }
    assert_eq!(o.max_bucket, 16);
    assert_eq!(o.buckets.len(), 17);
    assert_eq!(o.iter().count(), 17);
    for (b, chain) in o.buckets.iter().enumerate() {
        for n in chain {
            assert_eq!(o.bucket(DynaOrder::hash(n)), b);
        }
    }
}
