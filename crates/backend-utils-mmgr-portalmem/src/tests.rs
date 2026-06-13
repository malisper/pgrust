use super::*;
use std::sync::Once;

use backend_access_transam_xact_seams as xact_seam;
use backend_commands_portalcmds_seams as portalcmds_seam;
use backend_utils_cache_plancache_portal_seams as plancache_seam;
use backend_utils_resowner_seams as resowner_seam;
use backend_utils_sort_tuplestore_hold_seams as tuplestore_seam;
use backend_utils_time_snapmgr_seams as snapmgr_seam;

/// Install a minimal runtime so the portal lifecycle can be exercised without a
/// real backend. The cleanup hook is `NONE` so `PortalDrop` does not route into
/// a missing portalcmds runtime.
fn ensure_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        xact_seam::get_current_sub_transaction_id::set(|| 1);
        xact_seam::get_current_transaction_nest_level::set(|| 1);
        xact_seam::get_current_statement_start_timestamp::set(|| 123);
        resowner_seam::resource_owner_create_portal::set(|| ResourceOwnerHandle(0x5000));
        resowner_seam::resource_owner_release::set(|_o, _p, _c, _t| {});
        resowner_seam::resource_owner_delete::set(|_o| {});
        resowner_seam::resource_owner_new_parent::set(|_o, _n| {});
        snapmgr_seam::unregister_snapshot_from_owner::set(|_s, _o| {});
        snapmgr_seam::active_snapshot_set::set(|| false);
        snapmgr_seam::pop_active_snapshot::set(|| {});
        tuplestore_seam::tuplestore_begin_heap::set(|_ra| ExternHandle(0x6000));
        tuplestore_seam::tuplestore_end::set(|_s| {});
        plancache_seam::release_cached_plan::set(|_p| {});
        portalcmds_seam::portal_cleanup_hook::set(|| PortalCleanupHook::NONE);
        portalcmds_seam::run_cleanup_hook::set(|_h, _p| Ok(()));
        portalcmds_seam::persist_holdable_portal::set(|_p| Ok(()));
        portalcmds_seam::first_can_set_tag_stmt::set(|_p| ExternHandle::NONE);
        portalcmds_seam::pg_cursor_srf::set(|_f, _rows| Ok(types_datum::Datum::null()));
    });
}

#[test]
fn portal_lifecycle() {
    ensure_seams();

    std::thread::spawn(|| {
        assert!(GetPortalByName(Some("c1")).is_none());

        EnablePortalManager().expect("enable");

        let p = CreatePortal("c1", false, false).expect("create c1");
        assert_eq!(p, "c1");
        assert_eq!(GetPortalByName(Some("c1")).as_deref(), Some("c1"));
        with_portal("c1", |portal| {
            assert_eq!(portal.status, PORTAL_NEW);
            assert_eq!(portal.strategy, PORTAL_MULTI_QUERY);
            assert_eq!(portal.cursorOptions, CURSOR_OPT_NO_SCROLL);
            assert!(portal.atStart && portal.atEnd && portal.visible);
            assert_eq!(portal.createSubid, 1);
            assert_eq!(portal.createLevel, 1);
            assert_eq!(portal.creation_time, 123);
            assert_eq!(portal.name, "c1");
        })
        .expect("portal exists");

        assert!(ThereAreNoReadyPortals().expect("ready scan"));

        // Duplicate without allowDup -> ERROR (ERRCODE_DUPLICATE_CURSOR).
        let err = CreatePortal("c1", false, false).expect_err("dup errors");
        assert!(err.message().contains("cursor \"c1\" already exists"));

        // Duplicate with allowDup drops the old one and makes a new portal.
        let p2 = CreatePortal("c1", true, true).expect("recreate c1");
        assert_eq!(GetPortalByName(Some("c1")).as_deref(), Some(p2.as_str()));

        assert!(GetPortalByName(None).is_none());

        let up = CreateNewPortal().expect("create new portal");
        assert!(up.starts_with("<unnamed portal "));

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
            ExternHandle::NONE,
            types_portal::CachedPlanHandle::NULL,
        );
        with_portal(&p2, |p| {
            assert_eq!(p.status, PORTAL_DEFINED);
            assert_eq!(p.sourceText.as_deref(), Some("SELECT 1"));
        })
        .expect("portal exists");

        assert_eq!(
            pg_cursor(FcinfoHandle(0)).expect("pg_cursor"),
            types_datum::Datum::null()
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
