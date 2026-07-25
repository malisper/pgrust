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
    // (trigger, victim, n): a catalog scan of `trigger` delivers an
    // invalidation for a DIFFERENT relation, as C's LockRelationOid ->
    // AcceptInvalidationMessages does inside a rebuild's catalog access.
    static INVALIDATE_OTHER_DURING_BUILD: Cell<Option<(Oid, Oid, u32)>> = const { Cell::new(None) };
    static IN_XACT: Cell<bool> = const { Cell::new(true) };
    static IS_BOOTSTRAP: Cell<bool> = const { Cell::new(false) };
    static CUR_SUBID: Cell<u32> = const { Cell::new(1) };
    static HAS_SYSCACHE: RefCell<Vec<Oid>> = const { RefCell::new(Vec::new()) };
    static PG_INDEX_ROWS: RefCell<Vec<(Oid, FakeIndexRow)>> = const { RefCell::new(Vec::new()) };
    static INDEX_SCANS: Cell<usize> = const { Cell::new(0) };
}

#[derive(Clone, Copy)]
struct FakeIndexRow {
    indexrelid: Oid,
    indislive: bool,
    indisunique: bool,
    indisprimary: bool,
    indimmediate: bool,
    indisvalid: bool,
    indisreplident: bool,
    has_indpred: bool,
}

fn fake_index_scan(
    mcx: Mcx<'_>,
    indrelid: Oid,
) -> PgResult<PgVec<'_, relcache_build_seams::PgIndexListShape>> {
    INDEX_SCANS.with(|c| c.set(c.get() + 1));
    let mut out = PgVec::new_in(mcx);
    PG_INDEX_ROWS.with(|rows| {
        for (rel, r) in rows.borrow().iter() {
            if *rel == indrelid {
                out.push(relcache_build_seams::PgIndexListShape {
                    indexrelid: r.indexrelid,
                    indislive: r.indislive,
                    indisunique: r.indisunique,
                    indisprimary: r.indisprimary,
                    indimmediate: r.indimmediate,
                    indisvalid: r.indisvalid,
                    indisreplident: r.indisreplident,
                    has_indpred: r.has_indpred,
                });
            }
        }
    });
    Ok(out)
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
    if let Some((trigger, victim, n)) = INVALIDATE_OTHER_DURING_BUILD.with(|c| c.get()) {
        if trigger == target && n > 0 {
            INVALIDATE_OTHER_DURING_BUILD.with(|c| c.set(Some((trigger, victim, n - 1))));
            invalidate::RelationCacheInvalidateEntry(victim)?;
        }
    }
    Ok(ROWS.with(|r| {
        r.borrow().get(&target).map(|f| relcache_build_seams::ScannedPgClass {
            relchecks: 0, relhastriggers: false, relhasrules: false,
            form: f.form.clone(),
            options: None,
        })
    }))
}

fn fake_tupdesc(
    mcx: Mcx<'static>,
    relid: Oid,
    _form: &FormData_pg_class,
    _relchecks: i16,
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
        indexprs_src: None,
        indpred_src: None,
        },
        opcintype: PgVec::new_in(mcx),
        opfamily: PgVec::new_in(mcx),
        indoption: PgVec::new_in(mcx),
        indcollation: PgVec::new_in(mcx),
        supportinfo: Vec::new(),
        support: PgVec::new_in(mcx),
    })
}

fn install() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        crate::init_seams();
        relcache_build_seams::scan_pg_relation::set(fake_scan);
        relcache_build_seams::relation_build_tuple_desc::set(fake_tupdesc);
        relcache_build_seams::relation_init_index_access_info::set(fake_index_info);
        relcache_build_seams::scan_pg_index_shapes::set(fake_index_scan);
        catalog_seams::is_catalog_relation_oid::set(|_| false);
        miscinit_seams::is_bootstrap_processing_mode::set(|| IS_BOOTSTRAP.with(|c| c.get()));
        xact_seams::is_transaction_state::set(|| IN_XACT.with(|c| c.get()));
        xact_seams::get_current_sub_transaction_id::set(|| CUR_SUBID.with(|c| c.get()));
        relmapper_seams::relation_map_invalidate_all::set(|| Ok(()));
        relmapper_seams::relation_map_initialize::set(|| ());
        relmapper_seams::relation_map_initialize_phase2::set(|| Ok(()));
        relmapper_seams::relation_map_initialize_phase3::set(|| Ok(()));
        relmapper_seams::relation_map_update_map::set(|_, _, _, _| Ok(()));
        relmapper_seams::relation_map_oid_to_filenumber::set(|relid, _| relid);
        namespace_seams::is_temp_or_temp_toast_namespace::set(|_| true);
        namespace_seams::get_temp_namespace_proc_number::set(|_| Ok(7));
        syscache_seams::relation_has_sys_cache::set(|relid| {
            HAS_SYSCACHE.with(|v| v.borrow().contains(&relid))
        });
        syscache_seams::relation_supports_sys_cache::set(|relid| {
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

// C's phase-2 rebuild list holds pointers to entries that cannot be deleted
// (rd_refcnt > 0) and that are rebuilt IN PLACE (relcache.c:2570-2582,
// 2971-2980), so a pointer is still the cache's entry when its turn comes, and
// nothing the list holds contributes to rd_refcnt. Our rebuild replaces the
// entry Rc, so a nested invalidation arriving during an earlier entry's
// catalog access must not leave a later list slot pointing at an orphan, and
// the list must not inflate the refcount that nested arm-selection reads.
#[test]
fn cache_invalidate_phase2_reresolves_across_nested_reload() {
    install();
    seed(types_core::RELATION_RELATION_ID, "pg_class", RELKIND_RELATION);
    seed(CLASS_OID_INDEX_ID, "pg_class_oid_index", RELKIND_INDEX);
    seed(16460, "nailed_unused_later", RELKIND_RELATION);

    // pg_class + its index are held, so phase 2 rebuilds them (first, and in
    // that order). 16460 is nailed and unreferenced: C defers it, never
    // rebuilds it, and it sorts to the front of the second list, i.e. last.
    let pc = get(types_core::RELATION_RELATION_ID);
    let ci = get(CLASS_OID_INDEX_ID);
    drop(get(16460));
    for oid in [types_core::RELATION_RELATION_ID, CLASS_OID_INDEX_ID, 16460] {
        with_state(|st| st.id_cache.get_mut(&oid).unwrap().nailed = true);
    }
    with_state(|st| st.critical_relcaches_built = true);
    INVALIDATE_OTHER_DURING_BUILD
        .with(|c| c.set(Some((types_core::RELATION_RELATION_ID, 16460, 1))));
    SCAN_LOG.with(|l| l.borrow_mut().clear());

    let r = invalidate::RelationCacheInvalidate(false);
    INVALIDATE_OTHER_DURING_BUILD.with(|c| c.set(None));
    r.unwrap();

    // The unused nailed entry is deferred by both the nested arm and phase 2,
    // so it is never scanned: C RelationFlushRelation relcache.c:2876-2883 and
    // RelationCacheInvalidate relcache.c:3090-3091.
    assert_eq!(SCAN_LOG.with(|l| l.borrow().iter().filter(|&&o| o == 16460).count()), 0);
    // C rd_refcnt for an unused nailed entry is exactly 1, i.e. zero user refs
    // and one live allocation: nothing was rebuilt off an orphan and written
    // back over the cache's entry. Measured before taking a probe clone.
    assert_eq!(crate::RelationUserRefcount(16460), 0);
    let (rel, nailed) = store::lookup_ent(16460).unwrap();
    assert!(nailed);
    assert!(!rel.rd_isvalid.get());
    drop(rel);
    drop((pc, ci));
}

// Same class, widened: a nailed-unused entry invalidated three times from
// inside one rebuild's catalog access, a SECOND nailed-unused entry that is
// never a nested victim, and a REFERENCED non-nailed entry, all in one phase-2
// list. Checks that the re-resolve holds up under repeated replacement of one
// list member and does not perturb its neighbours.
//
// The referenced entry is a NO-REGRESSION companion, not a second born-RED:
// measured, a referenced entry that IS a nested victim gets rebuilt twice at
// base -- and C rebuilds it twice too (nested RelationFlushRelation, then its
// phase-2 turn), so the scan count is not a divergence there. Our port's extra
// residue on that arm -- the outer rebuild deriving copy_preserved state from
// the orphan rather than from the entry the nested rebuild installed -- is
// unobservable unless something writes a preserved field inside the window
// (e.g. RelationAssumeNewRelfilelocator), which this fixture does not do. So
// what is pinned here is only that the referenced arm still behaves as C does:
// rebuilt exactly once by phase 2, one user reference, valid at the end.
#[test]
fn cache_invalidate_phase2_reresolves_multi_entry_and_referenced() {
    install();
    seed(types_core::RELATION_RELATION_ID, "pg_class", RELKIND_RELATION);
    seed(CLASS_OID_INDEX_ID, "pg_class_oid_index", RELKIND_INDEX);
    for (oid, name) in [(16470, "nailed_unused_a"), (16471, "nailed_unused_b")] {
        seed(oid, name, RELKIND_RELATION);
    }
    seed(16472, "plain_referenced", RELKIND_RELATION);

    let pc = get(types_core::RELATION_RELATION_ID);
    let ci = get(CLASS_OID_INDEX_ID);
    drop(get(16470));
    drop(get(16471));
    let held_ref = get(16472); // one real reference, C rd_refcnt == 1
    for oid in [types_core::RELATION_RELATION_ID, CLASS_OID_INDEX_ID, 16470, 16471] {
        with_state(|st| st.id_cache.get_mut(&oid).unwrap().nailed = true);
    }
    with_state(|st| st.critical_relcaches_built = true);
    // Three deliveries out of pg_class's rebuild scan, all for 16470 (the seam
    // decrements its repeat count; it does not rotate victims), so 16470 is
    // replaced repeatedly while the outer list still names it.
    INVALIDATE_OTHER_DURING_BUILD
        .with(|c| c.set(Some((types_core::RELATION_RELATION_ID, 16470, 3))));
    SCAN_LOG.with(|l| l.borrow_mut().clear());

    let r = invalidate::RelationCacheInvalidate(false);
    INVALIDATE_OTHER_DURING_BUILD.with(|c| c.set(None));
    r.unwrap();

    // Both unused nailed entries deferred, never scanned, one allocation each.
    for oid in [16470u32, 16471] {
        assert_eq!(
            SCAN_LOG.with(|l| l.borrow().iter().filter(|&&o| o == oid).count()),
            0,
            "unused nailed {oid} was rebuilt; C defers it"
        );
        assert_eq!(crate::RelationUserRefcount(oid), 0, "stale lineage for {oid}");
        let (rel, nailed) = store::lookup_ent(oid).unwrap();
        assert!(nailed);
        assert!(!rel.rd_isvalid.get());
    }
    // The referenced entry IS rebuilt (C relcache.c:3093), exactly once, and
    // the cache's entry is the rebuild's own output -- not something
    // reconstructed from an orphaned predecessor.
    assert_eq!(SCAN_LOG.with(|l| l.borrow().iter().filter(|&&o| o == 16472).count()), 1);
    let (fresh, _) = store::lookup_ent(16472).unwrap();
    assert!(fresh.rd_isvalid.get());
    assert!(!Rc::ptr_eq(&fresh, &held_ref), "rebuild must replace the entry Rc");
    // C rd_refcnt == 1: the pre-rebuild holder, counted through stale_refs.
    drop(fresh);
    assert_eq!(crate::RelationUserRefcount(16472), 1);
    drop(held_ref);
    drop((pc, ci));
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

// ---------------------------------------------------------------------------
// The superseded-lineage divergence class.
//
// A rebuild REPLACES the entry Rc, so a handle taken before the rebuild is a
// holder of a lineage the cache no longer points at. C has no such state: it
// rebuilds contents in place, one entry per oid forever, and rd_refcnt counts
// every holder. Rc::strong_count on the CURRENT entry therefore reads BELOW
// C's rd_refcnt exactly when such a holder exists, and every leak-detection
// (J3) / user-semantics (J4) site that keyed off it was silently permissive.
// The four sites below are the ones the module contract names as unsound;
// each test exhibits the disagreement directly.
// ---------------------------------------------------------------------------

// Leave exactly one holder on a superseded lineage. C rd_refcnt == 1 after
// this; strong_count on the current entry is 1 (the cache's own reference).
fn stale_holder(oid: Oid) -> Rc<RelationData<'static>> {
    let held = get(oid);
    held.rd_isvalid.set(false);
    // Rebuild: installs a new lineage as the cache entry, notes `held` stale.
    let fresh = get(oid);
    assert!(!Rc::ptr_eq(&held, &fresh), "rebuild must replace the entry Rc");
    drop(fresh);
    assert_eq!(strong_count_in_cache(oid), 1, "no handles on the current lineage");
    assert_eq!(crate::RelationUserRefcount(oid), 1, "one handle, on a stale lineage");
    held
}

// C relcache.c:2903 elog(ERROR, "relation %u is still open", rid). The drop
// path must refuse while ANY lineage is held, not just the current one.
#[test]
fn forget_relation_refuses_holder_of_superseded_lineage() {
    install();
    seed(16480, "forget_stale", RELKIND_RELATION);
    let stale = stale_holder(16480);

    let err = invalidate::RelationForgetRelation(16480).expect_err("C elog(ERROR) here");
    assert!(err.message().contains("relation 16480 is still open"), "{}", err.message());
    assert!(with_state(|st| st.id_cache.contains_key(&16480)), "entry must survive");
    drop(stale);
}

// C relcache.c:3319-3320 Assert(rd_refcnt == (rd_isnailed ? 1 : 0)) -- the
// tree's only systematic "somebody never closed this relation" detector. The
// leak shape this port newly introduces (hold, get rebuilt away, leak the old
// handle) was invisible to it.
#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "relcache reference leak at EOXact")]
fn eoxact_leak_assert_sees_superseded_lineage() {
    install();
    seed(16481, "eoxact_stale", RELKIND_RELATION);
    let stale = stale_holder(16481);
    store::eoxact_list_add(16481);

    let r = invalidate::AtEOXact_RelationCache(true);
    drop(stale);
    r.unwrap();
}

// C relcache.c:3348-3366: nonzero rd_refcnt => WARNING and keep the entry.
// Reachable only where the leak Assert above is skipped, which in C is the
// non-assert build or bootstrap mode (relcache.c:3315); bootstrap is the arm a
// unit test can take.
#[test]
fn eoxact_abort_keeps_entry_when_superseded_lineage_held() {
    install();
    seed(16483, "eoxact_keep", RELKIND_RELATION);
    let stale = stale_holder(16483);
    let (cur, _) = store::lookup_ent(16483).unwrap();
    cur.rd_createSubid.set(1);
    drop(cur);
    store::eoxact_list_add(16483);

    IS_BOOTSTRAP.with(|c| c.set(true));
    let r = invalidate::AtEOXact_RelationCache(false);
    IS_BOOTSTRAP.with(|c| c.set(false));
    r.unwrap();

    let (cur, _) = store::lookup_ent(16483).expect("C daren't remove it; entry must survive");
    assert_eq!(cur.rd_createSubid.get(), InvalidSubTransactionId);
    drop(cur);
    drop(stale);
}

// C relcache.c:3452-3475: same predicate on the subxact path, whose else arm
// additionally transfers rd_createSubid to the parent so cleanup can retry.
// No leak Assert guards this one, so it is reachable in every build.
#[test]
fn eosubxact_abort_keeps_entry_when_superseded_lineage_held() {
    install();
    seed(16482, "eosubxact_stale", RELKIND_RELATION);
    let stale = stale_holder(16482);
    let (cur, _) = store::lookup_ent(16482).unwrap();
    cur.rd_createSubid.set(7);
    drop(cur);
    store::eoxact_list_add(16482);

    invalidate::AtEOSubXact_RelationCache(false, 7, 3).unwrap();

    let (cur, _) = store::lookup_ent(16482).expect("C daren't remove it; entry must survive");
    assert_eq!(cur.rd_createSubid.get(), 3, "transferred to the parent subxact");
    drop(cur);
    drop(stale);
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

fn idxrow(indexrelid: Oid) -> FakeIndexRow {
    FakeIndexRow {
        indexrelid,
        indislive: true,
        indisunique: false,
        indisprimary: false,
        indimmediate: true,
        indisvalid: true,
        indisreplident: false,
        has_indpred: false,
    }
}

#[test]
fn index_list_scans_once_then_serves_cached() {
    install();
    seed(16500, "idxlist_tbl", RELKIND_RELATION);
    PG_INDEX_ROWS.with(|rows| {
        let mut rows = rows.borrow_mut();
        rows.push((16500, FakeIndexRow { indisunique: true, indisprimary: true, ..idxrow(16510) }));
        rows.push((16500, FakeIndexRow { indislive: false, ..idxrow(16511) }));
        rows.push((16500, idxrow(16505)));
        rows.push((16999, idxrow(16600)));
    });
    let ctx = mcx::MemoryContext::new("caller");
    let scans_before = INDEX_SCANS.with(|c| c.get());

    let first = crate::indexlist::RelationGetIndexList(ctx.mcx(), 16500).unwrap();
    assert_eq!(first.as_slice(), &[16505, 16510]);
    assert_eq!(INDEX_SCANS.with(|c| c.get()), scans_before + 1);

    let second = crate::indexlist::RelationGetIndexList(ctx.mcx(), 16500).unwrap();
    assert_eq!(second.as_slice(), &[16505, 16510]);
    assert_eq!(INDEX_SCANS.with(|c| c.get()), scans_before + 1);

    let rel = get(16500);
    {
        let cached = rel.rd_indexlist.borrow();
        let cached = cached.as_ref().expect("rd_indexvalid");
        assert_eq!(cached.list.as_slice(), &[16505, 16510]);
        assert_eq!(cached.pkindex, 16510);
        assert!(!cached.ispkdeferrable);
        // relreplident 'd' + live pkey => pkey is the replica identity.
        assert_eq!(cached.replidindex, 16510);
    }
    drop(rel);
}

#[test]
fn index_list_invalidation_forces_rescan() {
    install();
    seed(16520, "idxlist_inval", RELKIND_RELATION);
    PG_INDEX_ROWS.with(|rows| rows.borrow_mut().push((16520, idxrow(16521))));
    let ctx = mcx::MemoryContext::new("caller");

    let held = get(16520);
    assert_eq!(
        crate::indexlist::RelationGetIndexList(ctx.mcx(), 16520).unwrap().as_slice(),
        &[16521]
    );
    let scans = INDEX_SCANS.with(|c| c.get());

    invalidate::RelationCacheInvalidateEntry(16520).unwrap();
    assert!(held.rd_indexlist.borrow().is_none());

    PG_INDEX_ROWS.with(|rows| rows.borrow_mut().push((16520, idxrow(16522))));
    assert_eq!(
        crate::indexlist::RelationGetIndexList(ctx.mcx(), 16520).unwrap().as_slice(),
        &[16521, 16522]
    );
    assert_eq!(INDEX_SCANS.with(|c| c.get()), scans + 1);
    drop(held);
}

fn codec_rel(
    mcx: Mcx<'static>,
    oid: Oid,
    relkind: u8,
    relam: Oid,
    natts: i16,
) -> RelationData<'static> {
    let mut f = form(oid, &format!("codec_{oid}"), relkind);
    f.relam = relam;
    f.reltablespace = 1663;
    f.reltuples = 42.5;
    f.relfrozenxid = 3;
    f.relminmxid = 1;
    let mut attrs = Vec::new();
    for i in 0..natts {
        let mut a = FormData_pg_attribute {
            attrelid: oid,
            atttypid: 23,
            attlen: 4,
            attnum: i + 1,
            attbyval: true,
            attalign: b'i' as i8,
            attstorage: b'p' as i8,
            attnotnull: i == 0,
            attislocal: true,
            ..Default::default()
        };
        a.attname.namestrcpy(&format!("a{i}"));
        attrs.push(a);
    }
    let mut td = tupdesc::CreateTupleDesc(mcx, &attrs).unwrap();
    td.tdtypeid = 0;
    td.tdtypmod = -1;
    td.tdrefcount = 1;
    let rd_index = (relkind == RELKIND_INDEX).then(|| {
        let mut indkey = PgVec::new_in(mcx);
        indkey.push(1i16);
        FormData_pg_index {
            indexrelid: oid,
            indrelid: 1259,
            indnatts: 1,
            indnkeyatts: 1,
            indisunique: true,
            indnullsnotdistinct: false,
            indisprimary: true,
            indisexclusion: false,
            indimmediate: true,
            indisvalid: true,
            indisready: true,
            indkey,
            has_indpred: false,
            indexprs_src: None,
            indpred_src: None,
        }
    });
    let one = |v: Oid| {
        let mut p = PgVec::new_in(mcx);
        p.push(v);
        p
    };
    let (opcintype, opfamily, indcollation, support, indoption) = if relkind == RELKIND_INDEX {
        let mut sup = PgVec::new_in(mcx);
        sup.extend_from_slice(&[0, 0, 2743, 0, 0, 0, 0]);
        let mut opt = PgVec::new_in(mcx);
        opt.push(0i16);
        (one(3659), one(3659), one(0), sup, opt)
    } else {
        (PgVec::new_in(mcx), PgVec::new_in(mcx), PgVec::new_in(mcx), PgVec::new_in(mcx), PgVec::new_in(mcx))
    };
    RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
        rd_id: oid,
        rd_backend: types_core::INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(InvalidSubTransactionId),
        rd_newRelfilelocatorSubid: Cell::new(InvalidSubTransactionId),
        rd_firstRelfilelocatorSubid: Cell::new(InvalidSubTransactionId),
        rd_droppedSubid: Cell::new(InvalidSubTransactionId),
        rd_lockInfo: lmgr::RelationInitLockInfo(oid, false),
        rd_rel: f,
        rd_att: Rc::new(td),
        rd_index,
        rd_opcintype: opcintype,
        rd_opfamily: opfamily,
        rd_indoption: indoption,
        rd_indcollation: indcollation,
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(),
        rd_amcache_gin: Default::default(),
        rd_amcache_spgist: Default::default(),
        rd_support: support,
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
        rd_trigdesc: Default::default(),
        rd_hastriggers: false,
        rd_hasrules: true,
    }
}

fn codec_file(mcx: Mcx<'static>, rels: &[(&RelationData<'static>, bool)]) -> Vec<u8> {
    let mut buf: initfile::Buf<'_> = PgVec::new_in(mcx);
    buf.extend_from_slice(&initfile::RELCACHE_INIT_FILEMAGIC.to_ne_bytes());
    buf.extend_from_slice(&initfile::RELCACHE_INIT_FORMAT.to_ne_bytes());
    for (rel, nailed) in rels {
        initfile::encode_entry(&mut buf, rel, *nailed);
    }
    buf.to_vec()
}

#[test]
fn init_file_codec_roundtrip() {
    install();
    let mcx = crate::cache_mcx();
    let heap = codec_rel(mcx, 1259, RELKIND_RELATION, 2, 3);
    // GIN relam: supportinfo preload resolves lazily (no fmgr seam in tests).
    let idx = codec_rel(mcx, 2662, RELKIND_INDEX, 2742, 1);
    let bytes = codec_file(mcx, &[(&heap, true), (&idx, true)]);

    let (rels, nailed_rels, nailed_indexes) = initfile::parse_init_file(&bytes, mcx).unwrap();
    assert_eq!((nailed_rels, nailed_indexes), (1, 1));
    assert_eq!(rels.len(), 2);

    let (h, h_nailed) = &rels[0];
    assert!(*h_nailed);
    assert_eq!(h.rd_id, 1259);
    assert_eq!(h.name(), "codec_1259");
    assert_eq!(h.rd_rel.reltuples, 42.5);
    assert!(h.rd_hasrules);
    assert!(!h.rd_hastriggers);
    assert_eq!(h.rd_att.natts, 3);
    assert_eq!(h.rd_att.tdtypeid, types_core::RECORDOID);
    assert_eq!(h.rd_att.tdtypmod, -1);
    assert!(h.rd_att.constr.as_ref().unwrap().has_not_null);
    assert_eq!(h.rd_att.attr(0).attname.name_str(), b"a0");
    assert_eq!(h.rd_att.compact_attr(1).attlen, 4);
    assert!(h.rd_index.is_none());
    assert!(h.rd_isvalid.get());
    assert_eq!(h.rd_locator.get().relNumber, 1259);

    let (i, _) = &rels[1];
    assert_eq!(i.rd_id, 2662);
    let ind = i.rd_index.as_ref().unwrap();
    assert_eq!(ind.indnkeyatts, 1);
    assert!(ind.indisprimary);
    assert_eq!(ind.indkey.as_slice(), &[1i16]);
    assert_eq!(i.rd_opcintype.as_slice(), &[3659]);
    assert_eq!(i.rd_support.as_slice(), &[0, 0, 2743, 0, 0, 0, 0]);
    assert_eq!(i.rd_indoption.as_slice(), &[0i16]);
    assert_eq!(i.rd_supportinfo.borrow().len(), 1);
    assert!(i.rd_supportinfo.borrow()[0].is_none());
}

#[test]
fn init_file_rejects_bad_header_and_corruption() {
    install();
    let mcx = crate::cache_mcx();
    let heap = codec_rel(mcx, 1247, RELKIND_RELATION, 2, 2);
    let good = codec_file(mcx, &[(&heap, true)]);
    assert!(initfile::parse_init_file(&good, mcx).is_some());

    let mut bad_magic = good.clone();
    bad_magic[0] ^= 0xff;
    assert!(initfile::parse_init_file(&bad_magic, mcx).is_none());

    let mut bad_format = good.clone();
    bad_format[4] ^= 0xff;
    assert!(initfile::parse_init_file(&bad_format, mcx).is_none());

    // Truncation anywhere inside the entry stream must reject the file.
    for cut in 9..good.len() {
        assert!(initfile::parse_init_file(&good[..cut], mcx).is_none(), "cut={cut}");
    }
    assert!(initfile::parse_init_file(&[], mcx).is_none());
}

#[test]
fn fkey_list_caches_and_invalidation_forgets() {
    thread_local! {
        static FKEY_SCANS: Cell<usize> = const { Cell::new(0) };
    }
    install();
    relcache_build_seams::scan_pg_constraint_fkeys::set(|mcx, conrelid| {
        FKEY_SCANS.with(|c| c.set(c.get() + 1));
        let mut out = PgVec::new_in(mcx);
        let mut info = types_rel::ForeignKeyCacheInfo {
            conoid: 5001,
            conrelid,
            confrelid: 16390,
            nkeys: 2,
            conenforced: true,
            conkey: [0; 32],
            confkey: [0; 32],
            conpfeqop: [0; 32],
        };
        info.conkey[..2].copy_from_slice(&[1, 3]);
        info.confkey[..2].copy_from_slice(&[1, 2]);
        info.conpfeqop[..2].copy_from_slice(&[96, 96]);
        out.push(info);
        Ok(out)
    });
    seed(16385, "fk_child", RELKIND_RELATION);

    let a = crate::fkeylist::RelationGetFKeyList(16385).unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].confrelid, 16390);
    assert_eq!(&a[0].conkey[..2], &[1, 3]);
    let b = crate::fkeylist::RelationGetFKeyList(16385).unwrap();
    assert!(Rc::ptr_eq(&a, &b));
    assert_eq!(FKEY_SCANS.with(|c| c.get()), 1);

    invalidate::RelationCacheInvalidateEntry(16385).unwrap();
    let c = crate::fkeylist::RelationGetFKeyList(16385).unwrap();
    assert!(!Rc::ptr_eq(&a, &c));
    assert_eq!(FKEY_SCANS.with(|c| c.get()), 2);
}
