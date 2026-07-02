use core::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Once;

use mcx::{Mcx, PgVec};
use types_core::{InvalidSubTransactionId, Oid, RELPERSISTENCE_PERMANENT};
use types_error::PgResult;
use types_rel::{
    FormData_pg_class, FormData_pg_index, RelationData, RELKIND_INDEX, RELKIND_RELATION,
    REPLICA_IDENTITY_DEFAULT,
};
use types_tuple::{FormData_pg_attribute, NameData};

use crate::schemapg::{self, CLASS_OID_INDEX_ID};
use crate::{initfile, invalidate, store, with_state};

thread_local! {
    static ROWS: RefCell<HashMap<Oid, FakeRel>> = RefCell::new(HashMap::new());
    static SCAN_LOG: RefCell<Vec<Oid>> = const { RefCell::new(Vec::new()) };
    static INVALIDATE_DURING_BUILD: Cell<Option<(Oid, u32)>> = const { Cell::new(None) };
    static IN_XACT: Cell<bool> = const { Cell::new(true) };
    static CUR_SUBID: Cell<u32> = const { Cell::new(1) };
    static HAS_SYSCACHE: RefCell<Vec<Oid>> = const { RefCell::new(Vec::new()) };
}

#[derive(Clone)]
struct FakeRel {
    form: FormData_pg_class,
    natts: i16,
    tupdesc_version: i32,
}

fn form(oid: Oid, name: &str, relkind: u8) -> FormData_pg_class {
    let mut relname = NameData::default();
    relname.namestrcpy(name);
    FormData_pg_class {
        relname,
        relnamespace: 2200,
        reltype: 0,
        relowner: 10,
        relam: 2,
        relfilenode: oid,
        reltablespace: 0,
        relpages: 0,
        reltuples: -1.0,
        relallvisible: 0,
        reltoastrelid: 0,
        relhasindex: false,
        relisshared: false,
        relpersistence: RELPERSISTENCE_PERMANENT,
        relkind,
        relhassubclass: false,
        relrowsecurity: false,
        relispopulated: true,
        relreplident: REPLICA_IDENTITY_DEFAULT,
        relispartition: false,
        relfrozenxid: 3,
        relminmxid: 1,
    }
}

fn seed(oid: Oid, name: &str, relkind: u8) {
    ROWS.with(|r| {
        r.borrow_mut().insert(oid, FakeRel { form: form(oid, name, relkind), natts: 2, tupdesc_version: 0 })
    });
}

fn bump_tupdesc_version(oid: Oid) {
    ROWS.with(|r| r.borrow_mut().get_mut(&oid).unwrap().tupdesc_version += 1);
}

fn fake_scan(target: Oid, _index_ok: bool, _fnh: bool) -> PgResult<Option<relcache_build_seams::ScannedPgClass>> {
    SCAN_LOG.with(|l| l.borrow_mut().push(target));
    if let Some((oid, n)) = INVALIDATE_DURING_BUILD.with(|c| c.get()) {
        if oid == target && n > 0 {
            INVALIDATE_DURING_BUILD.with(|c| c.set(Some((oid, n - 1))));
            invalidate::RelationCacheInvalidateEntry(target)?;
        }
    }
    Ok(ROWS.with(|r| {
        r.borrow().get(&target).map(|f| relcache_build_seams::ScannedPgClass {
            form: f.form.clone(),
            options: None,
        })
    }))
}

fn fake_tupdesc(
    mcx: Mcx<'static>,
    relid: Oid,
    _form: &FormData_pg_class,
) -> PgResult<Rc<types_tuple::TupleDescData<'static>>> {
    let (natts, version) =
        ROWS.with(|r| r.borrow().get(&relid).map(|f| (f.natts, f.tupdesc_version)).unwrap());
    let mut attrs = Vec::new();
    for i in 0..natts {
        let mut a = FormData_pg_attribute {
            attrelid: relid,
            atttypid: 23 + version as Oid,
            attlen: 4,
            attnum: i + 1,
            attbyval: true,
            attalign: b'i' as i8,
            attstorage: b'p' as i8,
            attislocal: true,
            ..Default::default()
        };
        a.attname.namestrcpy(&format!("c{i}"));
        attrs.push(a);
    }
    Ok(Rc::new(tupdesc::CreateTupleDesc(mcx, &attrs)?))
}

fn fake_index_info(
    mcx: Mcx<'static>,
    relid: Oid,
    _form: &FormData_pg_class,
) -> PgResult<relcache_build_seams::IndexAccessInfo> {
    let mut indkey = PgVec::new_in(mcx);
    indkey.push(1);
    Ok(relcache_build_seams::IndexAccessInfo {
        index: FormData_pg_index {
            indexrelid: relid,
            indrelid: 1,
            indnatts: 1,
            indnkeyatts: 1,
            indisunique: true,
            indnullsnotdistinct: false,
            indisprimary: false,
            indisexclusion: false,
            indimmediate: true,
            indisvalid: true,
            indisready: true,
            indkey,
            has_indpred: false,
        },
        opcintype: PgVec::new_in(mcx),
        opfamily: PgVec::new_in(mcx),
        indoption: PgVec::new_in(mcx),
        indcollation: PgVec::new_in(mcx),
    })
}

fn install() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        crate::init_seams();
        relcache_build_seams::scan_pg_relation::set(fake_scan);
        relcache_build_seams::relation_build_tuple_desc::set(fake_tupdesc);
        relcache_build_seams::relation_init_index_access_info::set(fake_index_info);
        catalog_seams::is_catalog_relation_oid::set(|_| false);
        miscinit_seams::is_bootstrap_processing_mode::set(|| false);
        xact_seams::is_transaction_state::set(|| IN_XACT.with(|c| c.get()));
        xact_seams::get_current_sub_transaction_id::set(|| CUR_SUBID.with(|c| c.get()));
        relmapper_seams::relation_map_invalidate_all::set(|| ());
        relmapper_seams::relation_map_initialize::set(|| ());
        relmapper_seams::relation_map_initialize_phase2::set(|| Ok(()));
        relmapper_seams::relation_map_initialize_phase3::set(|| Ok(()));
        relmapper_seams::relation_map_update_map::set(|_, _, _, _| Ok(()));
        namespace_seams::is_temp_or_temp_toast_namespace::set(|_| true);
        namespace_seams::get_temp_namespace_proc_number::set(|_| 7);
        syscache_seams::relation_has_sys_cache::set(|relid| {
            HAS_SYSCACHE.with(|v| v.borrow().contains(&relid))
        });
    });
}

fn get(oid: Oid) -> Rc<RelationData<'static>> {
    store::RelationIdGetRelation(oid).unwrap().unwrap()
}

fn strong_count_in_cache(oid: Oid) -> usize {
    with_state(|st| Rc::strong_count(&st.id_cache.get(&oid).unwrap().rel))
}

#[test]
fn miss_builds_then_hit_clones_same_entry() {
    install();
    seed(16384, "t1", RELKIND_RELATION);

    let a = get(16384);
    assert!(a.rd_isvalid.get());
    assert_eq!(a.name(), "t1");
    assert_eq!(strong_count_in_cache(16384), 2);

    let b = get(16384);
    assert!(Rc::ptr_eq(&a, &b));
    assert_eq!(strong_count_in_cache(16384), 3);
    assert_eq!(SCAN_LOG.with(|l| l.borrow().iter().filter(|&&o| o == 16384).count()), 1);

    drop(a);
    drop(b);
    assert_eq!(strong_count_in_cache(16384), 1);
}

#[test]
fn missing_pg_class_row_returns_none() {
    install();
    assert!(store::RelationIdGetRelation(99999).unwrap().is_none());
}

#[test]
fn dropped_entry_returns_none() {
    install();
    seed(16400, "t2", RELKIND_RELATION);
    let rel = get(16400);
    rel.rd_isvalid.set(false);
    rel.rd_createSubid.set(5);
    rel.rd_droppedSubid.set(5);
    drop(rel);
    assert!(store::RelationIdGetRelation(16400).unwrap().is_none());
}

#[test]
fn invalid_entry_rebuilds_on_lookup_preserving_state() {
    install();
    seed(16401, "t3", RELKIND_RELATION);
    let old = get(16401);
    old.rd_isvalid.set(false);
    old.rd_newRelfilelocatorSubid.set(9);
    old.pgstat_enabled.set(true);

    let new = get(16401);
    assert!(new.rd_isvalid.get());
    assert!(!Rc::ptr_eq(&old, &new));
    assert_eq!(new.rd_newRelfilelocatorSubid.get(), 9);
    assert!(new.pgstat_enabled.get());
    // Unchanged schema: rebuilt entry keeps the same tupdesc allocation.
    assert!(Rc::ptr_eq(&old.rd_att, &new.rd_att));
    drop(old);
    drop(new);
}

#[test]
fn rebuild_replaces_tupdesc_when_schema_changed() {
    install();
    seed(16402, "t4", RELKIND_RELATION);
    let old = get(16402);
    old.rd_isvalid.set(false);
    bump_tupdesc_version(16402);
    let new = get(16402);
    assert!(!Rc::ptr_eq(&old.rd_att, &new.rd_att));
    drop(old);
    drop(new);
}

#[test]
fn invalidate_entry_evicts_unreferenced() {
    install();
    seed(16403, "t5", RELKIND_RELATION);
    drop(get(16403));
    assert!(with_state(|st| st.id_cache.contains_key(&16403)));

    invalidate::RelationCacheInvalidateEntry(16403).unwrap();
    assert!(!with_state(|st| st.id_cache.contains_key(&16403)));
    assert_eq!(with_state(|st| st.invals_received), 1);
}

#[test]
fn invalidate_entry_rebuilds_referenced_holder_keeps_snapshot() {
    install();
    seed(16404, "t6", RELKIND_RELATION);
    let held = get(16404);
    bump_tupdesc_version(16404);

    invalidate::RelationCacheInvalidateEntry(16404).unwrap();
    assert!(!held.rd_isvalid.get());

    let new = get(16404);
    assert!(new.rd_isvalid.get());
    assert!(!Rc::ptr_eq(&held, &new));
    assert!(!Rc::ptr_eq(&held.rd_att, &new.rd_att));
    drop(held);
    drop(new);
}

#[test]
fn invalidate_entry_outside_xact_marks_invalid_only() {
    install();
    seed(16405, "t7", RELKIND_RELATION);
    let held = get(16405);
    let scans_before = SCAN_LOG.with(|l| l.borrow().len());

    IN_XACT.with(|c| c.set(false));
    invalidate::RelationCacheInvalidateEntry(16405).unwrap();
    IN_XACT.with(|c| c.set(true));

    assert!(!held.rd_isvalid.get());
    assert_eq!(SCAN_LOG.with(|l| l.borrow().len()), scans_before);
    drop(held);
}

#[test]
fn build_retries_when_invalidated_mid_build() {
    install();
    seed(16406, "t8", RELKIND_RELATION);
    INVALIDATE_DURING_BUILD.with(|c| c.set(Some((16406, 1))));
    let rel = get(16406);
    INVALIDATE_DURING_BUILD.with(|c| c.set(None));
    assert!(rel.rd_isvalid.get());
    assert_eq!(SCAN_LOG.with(|l| l.borrow().iter().filter(|&&o| o == 16406).count()), 2);
    drop(rel);
}

#[test]
fn cache_invalidate_orders_pg_class_and_nailed_first() {
    install();
    seed(types_core::RELATION_RELATION_ID, "pg_class", RELKIND_RELATION);
    seed(CLASS_OID_INDEX_ID, "pg_class_oid_index", RELKIND_INDEX);
    seed(16407, "nailed_rel", RELKIND_RELATION);
    seed(16408, "plain_held", RELKIND_RELATION);
    seed(16409, "plain_unref", RELKIND_RELATION);

    let pc = get(types_core::RELATION_RELATION_ID);
    let ci = get(CLASS_OID_INDEX_ID);
    let nr = get(16407);
    let ph = get(16408);
    drop(get(16409));
    for oid in [types_core::RELATION_RELATION_ID, CLASS_OID_INDEX_ID, 16407] {
        with_state(|st| st.id_cache.get_mut(&oid).unwrap().nailed = true);
    }
    with_state(|st| st.critical_relcaches_built = true);
    SCAN_LOG.with(|l| l.borrow_mut().clear());

    invalidate::RelationCacheInvalidate(false).unwrap();

    // Unreferenced non-nailed entry deleted in phase 1.
    assert!(!with_state(|st| st.id_cache.contains_key(&16409)));
    // Nailed entries with only the nail ref are invalidated, not rebuilt;
    // pg_class/its index/nailed rel are held here, so they rebuild in order.
    let log = SCAN_LOG.with(|l| l.borrow().clone());
    assert_eq!(
        log,
        vec![types_core::RELATION_RELATION_ID, CLASS_OID_INDEX_ID, 16407, 16408]
    );
    drop((pc, ci, nr, ph));
}

#[test]
fn cache_invalidate_defers_unused_nailed() {
    install();
    seed(16410, "nailed_unused", RELKIND_RELATION);
    drop(get(16410));
    with_state(|st| st.id_cache.get_mut(&16410).unwrap().nailed = true);
    SCAN_LOG.with(|l| l.borrow_mut().clear());

    invalidate::RelationCacheInvalidate(false).unwrap();

    let (rel, nailed) = store::lookup_ent(16410).unwrap();
    assert!(nailed);
    assert!(!rel.rd_isvalid.get());
    assert!(SCAN_LOG.with(|l| l.borrow().is_empty()));
    drop(rel);
}

#[test]
fn eoxact_abort_clears_created_in_xact() {
    install();
    seed(16411, "created", RELKIND_RELATION);
    let rel = get(16411);
    rel.rd_createSubid.set(1);
    drop(rel);
    store::eoxact_list_add(16411);

    invalidate::AtEOXact_RelationCache(false).unwrap();
    assert!(!with_state(|st| st.id_cache.contains_key(&16411)));
    assert_eq!(with_state(|st| st.eoxact_list_len), 0);
}

#[test]
fn eoxact_commit_clears_dropped_and_resets_subids() {
    install();
    seed(16412, "dropped", RELKIND_RELATION);
    seed(16413, "survivor", RELKIND_RELATION);
    let d = get(16412);
    d.rd_isvalid.set(false);
    d.rd_createSubid.set(1);
    d.rd_droppedSubid.set(1);
    drop(d);
    let s = get(16413);
    s.rd_createSubid.set(1);
    s.rd_newRelfilelocatorSubid.set(1);
    drop(s);
    store::eoxact_list_add(16412);
    store::eoxact_list_add(16413);

    invalidate::AtEOXact_RelationCache(true).unwrap();
    assert!(!with_state(|st| st.id_cache.contains_key(&16412)));
    let (s, _) = store::lookup_ent(16413).unwrap();
    assert_eq!(s.rd_createSubid.get(), InvalidSubTransactionId);
    assert_eq!(s.rd_newRelfilelocatorSubid.get(), InvalidSubTransactionId);
    drop(s);
}

#[test]
fn eoxact_overflow_scans_whole_cache() {
    install();
    seed(16414, "overflow", RELKIND_RELATION);
    let rel = get(16414);
    rel.rd_createSubid.set(1);
    drop(rel);
    with_state(|st| st.eoxact_list_overflowed = true);

    invalidate::AtEOXact_RelationCache(false).unwrap();
    assert!(!with_state(|st| st.id_cache.contains_key(&16414)));
    assert!(!with_state(|st| st.eoxact_list_overflowed));
}

#[test]
fn eosubxact_commit_transfers_abort_clears() {
    install();
    seed(16415, "sub_commit", RELKIND_RELATION);
    let rel = get(16415);
    rel.rd_createSubid.set(7);
    rel.rd_newRelfilelocatorSubid.set(7);
    drop(rel);
    store::eoxact_list_add(16415);

    invalidate::AtEOSubXact_RelationCache(true, 7, 3).unwrap();
    let (rel, _) = store::lookup_ent(16415).unwrap();
    assert_eq!(rel.rd_createSubid.get(), 3);
    assert_eq!(rel.rd_newRelfilelocatorSubid.get(), 3);
    drop(rel);

    invalidate::AtEOSubXact_RelationCache(false, 3, 1).unwrap();
    assert!(!with_state(|st| st.id_cache.contains_key(&16415)));
}

#[test]
fn forget_relation_clears_or_marks_dropped() {
    install();
    seed(16416, "forget_plain", RELKIND_RELATION);
    drop(get(16416));
    invalidate::RelationForgetRelation(16416).unwrap();
    assert!(!with_state(|st| st.id_cache.contains_key(&16416)));

    seed(16417, "forget_new", RELKIND_RELATION);
    let rel = get(16417);
    rel.rd_createSubid.set(1);
    drop(rel);
    CUR_SUBID.with(|c| c.set(4));
    invalidate::RelationForgetRelation(16417).unwrap();
    CUR_SUBID.with(|c| c.set(1));
    let (rel, _) = store::lookup_ent(16417).unwrap();
    assert_eq!(rel.rd_droppedSubid.get(), 4);
    assert!(!rel.rd_isvalid.get());
    drop(rel);

    seed(16418, "forget_open", RELKIND_RELATION);
    let held = get(16418);
    assert!(invalidate::RelationForgetRelation(16418).is_err());
    drop(held);
}

#[test]
fn formrdesc_builds_nailed_local_catalogs() {
    install();
    for cat in schemapg::LOCAL_BOOTSTRAP_CATALOGS {
        crate::build::formrdesc(cat).unwrap();
    }

    let (pg_class, nailed) = store::lookup_ent(types_core::RELATION_RELATION_ID).unwrap();
    assert!(nailed);
    assert!(pg_class.rd_isvalid.get());
    assert_eq!(pg_class.name(), "pg_class");
    assert_eq!(pg_class.rd_rel.relkind, RELKIND_RELATION);
    assert_eq!(pg_class.rd_rel.relowner, types_core::InvalidOid);
    assert_eq!(pg_class.rd_att.natts, 34);
    assert_eq!(pg_class.rd_att.tdtypeid, 83);
    assert_eq!(pg_class.rd_att.tdtypmod, -1);
    assert_eq!(pg_class.rd_att.compact_attrs[0].attcacheoff.get(), 0);
    assert!(pg_class.rd_att.constr.as_ref().unwrap().has_not_null);
    let relname = &pg_class.rd_att.attrs[1];
    assert_eq!(relname.attname.name_str(), b"relname");
    assert_eq!(relname.atttypid, 19);
    assert_eq!(relname.attlen, 64);
    drop(pg_class);

    // The nailed stub is directly servable through the hot lookup.
    let via_lookup = get(types_core::RELATION_RELATION_ID);
    assert_eq!(via_lookup.rd_id, types_core::RELATION_RELATION_ID);
    drop(via_lookup);

    let (pg_type, _) = store::lookup_ent(1247).unwrap();
    assert_eq!(pg_type.rd_att.natts, 32);
    drop(pg_type);
}

#[test]
fn formrdesc_shared_catalogs_are_shared_and_mapped() {
    install();
    for cat in schemapg::SHARED_BOOTSTRAP_CATALOGS {
        crate::build::formrdesc(cat).unwrap();
    }
    let (db, nailed) = store::lookup_ent(types_core::DATABASE_RELATION_ID).unwrap();
    assert!(nailed);
    assert!(db.rd_rel.relisshared);
    assert_eq!(db.rd_rel.reltablespace, crate::build::GLOBALTABLESPACE_OID);
    assert_eq!(db.rd_rel.relfilenode, types_core::InvalidRelFileNumber);
    assert!(db.is_mapped());
    drop(db);
}

#[test]
fn phase2_falls_back_to_formrdesc_without_init_file() {
    install();
    initfile::RelationCacheInitializePhase2().unwrap();
    for cat in schemapg::SHARED_BOOTSTRAP_CATALOGS {
        let (rel, nailed) = store::lookup_ent(cat.relid).unwrap();
        assert!(nailed, "{} not nailed", cat.name);
        assert_eq!(rel.rd_att.natts as usize, cat.attrs.len());
        drop(rel);
    }
}

#[test]
fn relation_id_is_in_init_file_matches_c() {
    install();
    assert!(initfile::RelationIdIsInInitFile(schemapg::CAT_PG_SHSECLABEL.relid));
    assert!(initfile::RelationIdIsInInitFile(schemapg::TRIGGER_RELID_NAME_INDEX_ID));
    assert!(initfile::RelationIdIsInInitFile(schemapg::DATABASE_NAME_INDEX_ID));
    assert!(initfile::RelationIdIsInInitFile(schemapg::SHARED_SEC_LABEL_OBJECT_INDEX_ID));
    assert!(!initfile::RelationIdIsInInitFile(16384));
    HAS_SYSCACHE.with(|v| v.borrow_mut().push(types_core::RELATION_RELATION_ID));
    assert!(initfile::RelationIdIsInInitFile(types_core::RELATION_RELATION_ID));
}

#[test]
fn relcache_init_lock_offset_matches_lwlock_table() {
    assert_eq!(lwlock::GetLWTrancheName(16), "RelCacheInit");
}

#[test]
fn bootstrap_descriptor_oids_match_headers() {
    // NUM_CRITICAL_* counts (relcache.c) and key OIDs vs catalog headers.
    assert_eq!(schemapg::SHARED_BOOTSTRAP_CATALOGS.len(), 5);
    assert_eq!(schemapg::LOCAL_BOOTSTRAP_CATALOGS.len(), 4);
    assert_eq!(schemapg::CAT_PG_CLASS.relid, 1259);
    assert_eq!(schemapg::CAT_PG_CLASS.rowtype_id, 83);
    assert_eq!(schemapg::CAT_PG_ATTRIBUTE.relid, 1249);
    assert_eq!(schemapg::CAT_PG_PROC.relid, 1255);
    assert_eq!(schemapg::CAT_PG_TYPE.relid, 1247);
    assert_eq!(schemapg::CAT_PG_DATABASE.relid, 1262);
    assert_eq!(schemapg::CAT_PG_AUTHID.relid, 1260);
    assert_eq!(schemapg::CAT_PG_AUTH_MEMBERS.relid, 1261);
    assert_eq!(schemapg::CAT_PG_SHSECLABEL.relid, 3592);
    assert_eq!(schemapg::CAT_PG_SUBSCRIPTION.relid, 6100);
    assert_eq!(schemapg::CLASS_OID_INDEX_ID, 2662);
    for cat in schemapg::LOCAL_BOOTSTRAP_CATALOGS.iter().chain(&schemapg::SHARED_BOOTSTRAP_CATALOGS) {
        for (i, a) in cat.attrs.iter().enumerate() {
            assert_eq!(a.attrelid, cat.relid);
            assert_eq!(a.attnum as usize, i + 1);
        }
    }
}
