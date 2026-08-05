// InvalidateConstraintCacheCallBack (ri_triggers.c) selection rules. The
// regression these pin: without the callback, ALTER TABLE ... RENAME CONSTRAINT
// left the pre-rename conname in the constraint cache and every later violation
// in the session named a constraint that no longer existed.
use super::*;

fn stub_info(constraint_oid: Oid, root: Oid, oid_hv: u32, root_hv: u32) -> RiConstraintInfo {
    RiConstraintInfo {
        constraint_id: constraint_oid,
        constraint_root_id: root,
        oidHashValue: oid_hv,
        rootHashValue: root_hv,
        conname: NameData::default(),
        pk_relid: InvalidOid,
        fk_relid: InvalidOid,
        confmatchtype: FKCONSTR_MATCH_SIMPLE,
        nkeys: 0,
        ndelsetcols: 0,
        confdelsetcols: [0; RI_MAX_NUMKEYS],
        fk_attnums: [0; RI_MAX_NUMKEYS],
        pk_attnums: [0; RI_MAX_NUMKEYS],
        pf_eq_oprs: [InvalidOid; RI_MAX_NUMKEYS],
        pp_eq_oprs: [InvalidOid; RI_MAX_NUMKEYS],
        ff_eq_oprs: [InvalidOid; RI_MAX_NUMKEYS],
        hasperiod: false,
        period_contained_by_oper: InvalidOid,
        agged_period_contained_by_oper: InvalidOid,
        period_intersect_oper: InvalidOid,
    }
}

fn test_mcx() -> Mcx<'static> {
    thread_local! {
        static CTX: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("ri-inval-test")));
    }
    CTX.with(|c| c.mcx())
}

fn seed(entries: &[RiConstraintInfo]) {
    let mcx = test_mcx();
    RI_CONSTRAINT_CACHE.with(|c| {
        let mut b = c.borrow_mut();
        let m = b.get_or_insert_with(|| PgHashMap::new_in(mcx));
        m.clear();
        for e in entries {
            m.insert(e.constraint_id, e.clone());
        }
    });
}

fn cached() -> Vec<Oid> {
    let mut v = RI_CONSTRAINT_CACHE
        .with(|c| c.borrow().as_ref().map(|m| m.keys().copied().collect::<Vec<_>>()))
        .unwrap_or_default();
    v.sort_unstable();
    v
}

// A pg_constraint inval for one constraint drops exactly that entry.
#[test]
fn inval_drops_only_the_matching_entry() {
    seed(&[
        stub_info(100, 100, 0xAAAA, 0xAAAA),
        stub_info(200, 200, 0xBBBB, 0xBBBB),
        stub_info(300, 300, 0xCCCC, 0xCCCC),
    ]);
    InvalidateConstraintCacheCallBack(Datum::null(), cache_syscache::CONSTROID, 0xBBBB);
    assert_eq!(cached(), vec![100, 300]);
}

// An inval that matches nothing leaves the cache alone: pg_constraint update
// traffic must not behave like a blanket flush.
#[test]
fn inval_of_unrelated_constraint_keeps_entries() {
    seed(&[stub_info(100, 100, 0xAAAA, 0xAAAA), stub_info(200, 200, 0xBBBB, 0xBBBB)]);
    InvalidateConstraintCacheCallBack(Datum::null(), cache_syscache::CONSTROID, 0xDEAD);
    assert_eq!(cached(), vec![100, 200]);
}

// Inherited (partition) children carry the root's hash value; invalidating the
// root constraint must take them down too, even though their own hash differs.
#[test]
fn inval_of_root_drops_inherited_children() {
    seed(&[
        stub_info(10, 10, 0x1111, 0x1111),      // the root itself
        stub_info(11, 10, 0x2222, 0x1111),      // child of the root
        stub_info(12, 10, 0x3333, 0x1111),      // child of the root
        stub_info(20, 20, 0x4444, 0x4444),      // unrelated
    ]);
    InvalidateConstraintCacheCallBack(Datum::null(), cache_syscache::CONSTROID, 0x1111);
    assert_eq!(cached(), vec![20]);
}

// A child's own inval does not touch the root or its siblings.
#[test]
fn inval_of_child_leaves_root_and_siblings() {
    seed(&[
        stub_info(10, 10, 0x1111, 0x1111),
        stub_info(11, 10, 0x2222, 0x1111),
        stub_info(12, 10, 0x3333, 0x1111),
    ]);
    InvalidateConstraintCacheCallBack(Datum::null(), cache_syscache::CONSTROID, 0x2222);
    assert_eq!(cached(), vec![10, 12]);
}

// hashvalue == 0 is a reset message: everything goes.
#[test]
fn reset_message_flushes_everything() {
    seed(&[stub_info(100, 100, 0xAAAA, 0xAAAA), stub_info(200, 200, 0xBBBB, 0xBBBB)]);
    InvalidateConstraintCacheCallBack(Datum::null(), cache_syscache::CONSTROID, 0);
    assert!(cached().is_empty());
}

// Past 1000 live entries C stops matching and pretends it got a reset (the
// pg_dump-restore O(N^2) escape hatch), so even a non-matching hash value
// empties the cache. At exactly 1000 the selective path still applies.
#[test]
fn over_a_thousand_entries_degrades_to_a_reset() {
    let many: Vec<_> =
        (1..=1000u32).map(|i| stub_info(i, i, 0x10000 + i, 0x10000 + i)).collect();
    seed(&many);
    InvalidateConstraintCacheCallBack(Datum::null(), cache_syscache::CONSTROID, 0xDEAD);
    assert_eq!(cached().len(), 1000, "1000 entries: still selective");

    let mut many = many;
    many.push(stub_info(1001, 1001, 0x20001, 0x20001));
    seed(&many);
    InvalidateConstraintCacheCallBack(Datum::null(), cache_syscache::CONSTROID, 0xDEAD);
    assert!(cached().is_empty(), "1001 entries: reset");
}

// An inval arriving before the cache has ever been built is a no-op, not a panic.
#[test]
fn inval_on_unbuilt_cache_is_a_noop() {
    RI_CONSTRAINT_CACHE.with(|c| drop(c.borrow_mut().take()));
    InvalidateConstraintCacheCallBack(Datum::null(), cache_syscache::CONSTROID, 0xAAAA);
    assert!(cached().is_empty());
}
