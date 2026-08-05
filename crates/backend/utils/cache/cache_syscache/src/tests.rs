use super::*;

#[test]
fn table_shape_invariants() {
    assert_eq!(CACHEINFO.len(), SYS_CACHE_SIZE);
    for (id, d) in CACHEINFO.iter().enumerate() {
        assert!(d.reloid != 0 && d.indoid != 0, "row {id}");
        assert!(d.nbuckets > 0 && (d.nbuckets & (d.nbuckets - 1)) == 0, "row {id}");
        assert!((1..=4).contains(&d.nkeys), "row {id}");
        for k in 0..4 {
            assert_eq!(d.key[k] != 0, k < d.nkeys as usize, "row {id} key {k}");
        }
        assert!(!RelationInvalidatesSnapshotsOnly(d.reloid), "row {id}");
    }
}

#[test]
fn landmark_rows_match_syscache_info_h() {
    let reloid_57 = CACHEINFO[RELOID as usize];
    assert_eq!(
        (reloid_57.reloid, reloid_57.indoid, reloid_57.nkeys, reloid_57.key, reloid_57.nbuckets),
        (1259, 2662, 1, [1, 0, 0, 0], 128)
    );
    let attnum = CACHEINFO[ATTNUM as usize];
    assert_eq!(
        (attnum.reloid, attnum.indoid, attnum.nkeys, attnum.key, attnum.nbuckets),
        (1249, 2659, 2, [1, 5, 0, 0], 128)
    );
    let pnans = CACHEINFO[PROCNAMEARGSNSP as usize];
    assert_eq!(
        (pnans.reloid, pnans.indoid, pnans.nkeys, pnans.key, pnans.nbuckets),
        (1255, 2691, 3, [2, 20, 3, 0], 128)
    );
    let umus = CACHEINFO[USERMAPPINGUSERSERVER as usize];
    assert_eq!(USERMAPPINGUSERSERVER, 84);
    assert_eq!(
        (umus.reloid, umus.indoid, umus.nkeys, umus.key, umus.nbuckets),
        (1418, 175, 2, [2, 3, 0, 0], 2)
    );
    assert_eq!(TYPEOID, 82);
    assert_eq!(CACHEINFO[TYPEOID as usize].reloid, 1247);
    assert_eq!(AGGFNOID, 0);
    assert_eq!(CACHEINFO[AGGFNOID as usize].indoid, 2650);
}

#[test]
fn snapshot_only_relids_match_syscache_c() {
    for relid in [2964, 2608, 1214, 2609, 2396, 3596, 3592] {
        assert!(RelationInvalidatesSnapshotsOnly(relid));
    }
    assert!(!RelationInvalidatesSnapshotsOnly(1259));
}

#[test]
fn init_registers_all_caches_and_relid_arrays() {
    InitCatalogCache().unwrap();
    // pg_class is a syscache relation; its index is supporting-only.
    assert!(RelationHasSysCache(1259));
    assert!(!RelationHasSysCache(2662));
    assert!(RelationSupportsSysCache(1259));
    assert!(RelationSupportsSysCache(2662));
    assert!(!RelationHasSysCache(2608)); /* pg_depend: no cache */
    assert!(!RelationSupportsSysCache(9999));

    // SysCacheInvalidate on a registered-but-uninitialized cache: no-op.
    SysCacheInvalidate(RELOID, 12345);

    // Warm hit end-to-end through the SearchSysCache wrapper (phase-2 init
    // bypassed via the catcache test fixture).
    catcache::testing::force_initialized(RELOID, [catcache::CCFastKind::Int4; 4]);
    let mut img = vec![0u8; 32];
    img[22] = 24;
    catcache::testing::insert_positive(
        RELOID,
        &[
            SysCacheKey::Value(datum::Datum::from_oid(1259)),
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
        ],
        &img,
    );
    let t = SearchSysCache1(RELOID, SysCacheKey::Value(datum::Datum::from_oid(1259)))
        .unwrap()
        .expect("hit");
    assert_eq!(t.tuple().t_len, 32);
    ReleaseSysCache(t);

    // A miss reaches the uninstalled scan seam: loud panic, not a stub.
    let miss = std::panic::catch_unwind(|| {
        SearchSysCache1(RELOID, SysCacheKey::Value(datum::Datum::from_oid(4242)))
    });
    assert!(miss.is_err());
}

#[test]
#[should_panic(expected = "invalid cache ID")]
fn cache_id_range_checked() {
    let _ = SearchSysCache1(85, SysCacheKey::UNUSED);
}
