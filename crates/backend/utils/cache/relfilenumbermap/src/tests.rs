use std::sync::Once;

use super::*;

fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        fmgr_seams::fmgr_info::set(|oid| {
            assert_eq!(oid, F_OIDEQ);
            Ok(types_fmgr::FmgrInfo::unresolved())
        });
    });
}

fn seed(entries: &[(Oid, RelFileNumber, Oid)]) {
    install();
    if MAP.with(|cell| cell.borrow().is_none()) {
        InitializeRelfilenumberMap().unwrap();
    }
    MAP.with(|cell| {
        let mut slot = cell.borrow_mut();
        let map = slot.as_mut().unwrap();
        map.hash.clear();
        for &(reltablespace, relfilenumber, relid) in entries {
            map.hash.insert(RelfilenumberMapKey { reltablespace, relfilenumber }, relid);
        }
    });
}

fn keys_left() -> usize {
    MAP.with(|cell| cell.borrow().as_ref().unwrap().hash.len())
}

#[test]
fn initialize_builds_skey() {
    install();
    InitializeRelfilenumberMap().unwrap();
    MAP.with(|cell| {
        let slot = cell.borrow();
        let map = slot.as_ref().unwrap();
        assert_eq!(map.skey[0].sk_attno, Anum_pg_class_reltablespace as i16);
        assert_eq!(map.skey[1].sk_attno, Anum_pg_class_relfilenode as i16);
        for k in &map.skey {
            assert_eq!(k.sk_strategy, BTEqualStrategyNumber);
            assert_eq!(k.sk_subtype, InvalidOid);
            assert_eq!(k.sk_collation, InvalidOid);
        }
    });
}

#[test]
fn invalidate_specific_relid_and_negative_entries() {
    seed(&[(0, 100, 16384), (0, 101, 16385), (0, 102, InvalidOid)]);
    RelfilenumberMapInvalidateCallback(Datum::null(), 16384);
    assert_eq!(keys_left(), 1);
    let survivor = MAP.with(|cell| {
        cell.borrow()
            .as_ref()
            .unwrap()
            .hash
            .get(&RelfilenumberMapKey { reltablespace: 0, relfilenumber: 101 })
            .copied()
    });
    assert_eq!(survivor, Some(16385));
}

#[test]
fn invalidate_invalid_oid_resets_everything() {
    seed(&[(0, 100, 16384), (1663, 101, 16385)]);
    RelfilenumberMapInvalidateCallback(Datum::null(), InvalidOid);
    assert_eq!(keys_left(), 0);
}

#[test]
fn cache_hit_returns_without_scan() {
    seed(&[(0, 424242, 90001), (0, 424243, InvalidOid)]);
    assert_eq!(RelidByRelfilenumber(0, 424242).unwrap(), 90001);
    // Negative entries hit the cache too.
    assert_eq!(RelidByRelfilenumber(0, 424243).unwrap(), InvalidOid);
}

#[test]
fn tablespace_normalized_to_zero_for_database_default() {
    seed(&[(0, 555555, 90002)]);
    let dbspc = init_small::globals::MyDatabaseTableSpace();
    assert_eq!(RelidByRelfilenumber(dbspc, 555555).unwrap(), 90002);
}
