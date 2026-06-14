use super::*;
use std::sync::Once;

use backend_access_transam_xact_seams as xact_seam;
use backend_commands_portalcmds_seams as portalcmds_seam;
use backend_utils_cache_plancache_portal_seams as plancache_seam;
use backend_utils_resowner_seams as resowner_seam;
use backend_utils_time_snapmgr_seams as snapmgr_seam;

/// Install a minimal runtime so the portal lifecycle can be exercised without a
/// real backend. The `portal_cleanup` seam is a no-op so `PortalDrop` does not
/// route into a missing portalcmds runtime.
fn ensure_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        xact_seam::get_current_sub_transaction_id::set(|| 1);
        xact_seam::get_current_transaction_nest_level::set(|| 1);
        xact_seam::get_current_statement_start_timestamp::set(|| 123);
        resowner_seam::resource_owner_create_portal::set(types_portal::ResourceOwner::default);
        resowner_seam::resource_owner_release::set(|_o, _p, _c, _t| {});
        resowner_seam::resource_owner_delete::set(|_o| {});
        resowner_seam::resource_owner_new_parent::set(|_o, _n| {});
        snapmgr_seam::unregister_snapshot_from_owner::set(|_s, _o| {});
        snapmgr_seam::active_snapshot_set::set(|| false);
        snapmgr_seam::pop_active_snapshot::set(|| Ok(()));
        plancache_seam::release_cached_plan::set(|_p| {});
        portalcmds_seam::portal_cleanup::set(|_p| Ok(()));
        portalcmds_seam::persist_holdable_portal::set(|_p| Ok(()));
    });
}

#[test]
fn portal_lifecycle() {
    ensure_seams();

    std::thread::spawn(|| {
        assert!(GetPortalByName(Some("c1")).is_none());

        EnablePortalManager().expect("enable");

        let p = CreatePortal("c1", false, false).expect("create c1");
        assert_eq!(p.borrow().name, "c1");
        assert!(GetPortalByName(Some("c1")).is_some());
        {
            let portal = p.borrow();
            assert_eq!(portal.status, PORTAL_NEW);
            assert_eq!(portal.strategy, PORTAL_MULTI_QUERY);
            assert_eq!(portal.cursorOptions, CURSOR_OPT_NO_SCROLL);
            assert!(portal.atStart && portal.atEnd && portal.visible);
            assert_eq!(portal.createSubid, 1);
            assert_eq!(portal.createLevel, 1);
            assert_eq!(portal.creation_time, 123);
            assert_eq!(portal.name, "c1");
        }

        assert!(ThereAreNoReadyPortals().expect("ready scan"));

        // Duplicate without allowDup -> ERROR (ERRCODE_DUPLICATE_CURSOR).
        match CreatePortal("c1", false, false) {
            Ok(_) => panic!("dup should error"),
            Err(err) => assert!(err.message().contains("cursor \"c1\" already exists")),
        }

        // Duplicate with allowDup drops the old one and makes a new portal.
        let p2 = CreatePortal("c1", true, true).expect("recreate c1");
        assert!(GetPortalByName(Some("c1")).is_some());

        assert!(GetPortalByName(None).is_none());

        let up = CreateNewPortal().expect("create new portal");
        assert!(up.borrow().name.starts_with("<unnamed portal "));

        PinPortal(&p2).expect("pin");
        assert!(PinPortal(&p2).is_err());
        UnpinPortal(&p2).expect("unpin");
        assert!(UnpinPortal(&p2).is_err());

        // From NEW, MarkPortalActive errors (requires READY).
        assert!(MarkPortalActive(&p2).is_err());

        PortalDefineQuery(
            &p2,
            None,
            "SELECT 1".to_string(),
            CMDTAG_UNKNOWN,
            None,
            types_portal::CachedPlanHandle::NULL,
        );
        {
            let p = p2.borrow();
            assert_eq!(p.status, PORTAL_DEFINED);
            assert_eq!(p.sourceText.as_deref(), Some("SELECT 1"));
        }

        // PortalGetPrimaryStmt walks the (real) stmt list in-crate.
        assert_eq!(PortalGetPrimaryStmt(&p2), None);
        {
            use types_nodes::nodeindexscan::PlannedStmt;
            let mut not_primary = PlannedStmt::default();
            not_primary.canSetTag = false;
            let mut primary = PlannedStmt::default();
            primary.canSetTag = true;
            p2.borrow_mut().stmts = Some(vec![not_primary, primary]);
        }
        assert_eq!(PortalGetPrimaryStmt(&p2), Some(1));
        assert_eq!(portal_num_stmts(&p2), 2);

        assert_eq!(
            pg_cursor(FcinfoHandle(0)).expect("pg_cursor"),
            Datum::null()
        );

        PortalDrop(&p2, false).expect("drop c1");
        assert!(GetPortalByName(Some("c1")).is_none());
        PortalHashTableDeleteAll().expect("delete all");
        assert!(ThereAreNoReadyPortals().expect("ready scan"));
    })
    .join()
    .expect("thread ok");
}

#[test]
fn hash_key_truncates_like_strlcpy() {
    let long_a = "x".repeat(MAX_PORTALNAME_LEN + 5);
    let key = hash_key(&long_a);
    assert_eq!(key.len(), MAX_PORTALNAME_LEN - 1);
    assert!(long_a.starts_with(&key));
}

#[test]
fn precommit_drops_non_holdable() {
    ensure_seams();
    std::thread::spawn(|| {
        EnablePortalManager().expect("enable");
        let _p = CreatePortal("d1", false, false).expect("create d1");
        // Non-holdable portal created in the current xact -> dropped.
        let changed = PreCommit_Portals(false).expect("precommit");
        assert!(changed);
        assert!(GetPortalByName(Some("d1")).is_none());
    })
    .join()
    .expect("thread ok");
}
