use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Once;

use super::*;
use ::types_resowner::{RELEASE_PRIO_BUFFER_PINS, RELEASE_PRIO_CATCACHE_REFS};

thread_local! {
    static RELEASED: RefCell<Vec<(&'static str, usize)>> = const { RefCell::new(Vec::new()) };
    static PRINTED: Cell<u32> = const { Cell::new(0) };
}

fn released() -> Vec<(&'static str, usize)> {
    RELEASED.with(|r| r.borrow_mut().drain(..).collect())
}

fn release_pin(res: Datum) {
    RELEASED.with(|r| r.borrow_mut().push(("pin", res.as_usize())));
}

fn release_io(res: Datum) {
    RELEASED.with(|r| r.borrow_mut().push(("io", res.as_usize())));
}

fn release_catref(res: Datum) {
    RELEASED.with(|r| r.borrow_mut().push(("catref", res.as_usize())));
}

fn print_pin<'a>(mcx: ::mcx::Mcx<'a>, res: Datum) -> PgResult<::mcx::PgString<'a>> {
    PRINTED.with(|p| p.set(p.get() + 1));
    ::mcx::PgString::from_str_in(&format!("pin {}", res.as_usize()), mcx)
}

static PIN_DESC: ResourceOwnerDesc = ResourceOwnerDesc {
    name: "buffer pin",
    release_phase: RESOURCE_RELEASE_BEFORE_LOCKS,
    release_priority: RELEASE_PRIO_BUFFER_PINS,
    ReleaseResource: release_pin,
    DebugPrint: Some(print_pin),
};

static IO_DESC: ResourceOwnerDesc = ResourceOwnerDesc {
    name: "buffer io",
    release_phase: RESOURCE_RELEASE_BEFORE_LOCKS,
    release_priority: 100,
    ReleaseResource: release_io,
    DebugPrint: None,
};

static CATREF_DESC: ResourceOwnerDesc = ResourceOwnerDesc {
    name: "catcache reference",
    release_phase: RESOURCE_RELEASE_AFTER_LOCKS,
    release_priority: RELEASE_PRIO_CATCACHE_REFS,
    ReleaseResource: release_catref,
    DebugPrint: None,
};

static INIT: Once = Once::new();

fn setup() {
    INIT.call_once(|| {
        init_seams();
        ipc_seams::on_shmem_exit::set(|_cb, _arg| {});
        predicate_seams::release_predicate_locks::set(|_, _| Ok(()));
        predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
        predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
        snapmgr_seams::unregister_snapshot_no_owner::set(drop);
    });
}

fn owner(name: &'static str) -> ResourceOwner {
    ResourceOwnerCreate(ResourceOwner::NULL, name).unwrap()
}

fn remember(o: ResourceOwner, v: usize, kind: &'static ResourceOwnerDesc) {
    ResourceOwnerEnlarge(o).unwrap();
    ResourceOwnerRemember(o, Datum::from_usize(v), kind).unwrap();
}

fn narr(o: ResourceOwner) -> u32 {
    with_arena(|a| a.data(o).narr as u32)
}

fn nhash(o: ResourceOwner) -> u32 {
    with_arena(|a| a.data(o).nhash)
}

fn capacity(o: ResourceOwner) -> u32 {
    with_arena(|a| a.data(o).capacity)
}

fn release_all_phases(o: ResourceOwner, is_commit: bool) {
    ResourceOwnerRelease(o, RESOURCE_RELEASE_BEFORE_LOCKS, is_commit, false).unwrap();
    ResourceOwnerRelease(o, RESOURCE_RELEASE_LOCKS, is_commit, true).unwrap();
    ResourceOwnerRelease(o, RESOURCE_RELEASE_AFTER_LOCKS, is_commit, false).unwrap();
}

#[test]
fn remember_forget_array_round_trip() {
    setup();
    let o = owner("t");
    for v in 0..8usize {
        remember(o, v, &PIN_DESC);
    }
    assert_eq!(narr(o), 8);
    assert_eq!(nhash(o), 0);
    for v in (0..8usize).rev() {
        ResourceOwnerForget(o, Datum::from_usize(v), &PIN_DESC).unwrap();
    }
    assert_eq!(narr(o), 0);
    ResourceOwnerDelete(o);
}

#[test]
fn forget_matches_value_and_kind() {
    setup();
    let o = owner("t");
    remember(o, 7, &PIN_DESC);
    assert!(ResourceOwnerForget(o, Datum::from_usize(7), &IO_DESC).is_err());
    assert!(ResourceOwnerForget(o, Datum::from_usize(8), &PIN_DESC).is_err());
    ResourceOwnerForget(o, Datum::from_usize(7), &PIN_DESC).unwrap();
    ResourceOwnerDelete(o);
}

#[test]
fn remember_without_enlarge_errors_when_full() {
    setup();
    let o = owner("t");
    for v in 0..RESOWNER_ARRAY_SIZE {
        remember(o, v, &PIN_DESC);
    }
    assert_eq!(narr(o), 32);
    let err = ResourceOwnerRemember(o, Datum::from_usize(99), &PIN_DESC).unwrap_err();
    assert!(err.message().contains("array was full"));

    ResourceOwnerForget(o, Datum::from_usize(31), &PIN_DESC).unwrap();
    ResourceOwnerRemember(o, Datum::from_usize(99), &PIN_DESC).unwrap();

    for v in (0..31usize).chain([99]) {
        ResourceOwnerForget(o, Datum::from_usize(v), &PIN_DESC).unwrap();
    }
    ResourceOwnerDelete(o);
}

#[test]
fn spill_to_hash_at_c_threshold() {
    setup();
    let o = owner("t");
    for v in 0..33usize {
        remember(o, v, &PIN_DESC);
    }
    assert_eq!(nhash(o), 32);
    assert_eq!(narr(o), 1);
    assert_eq!(capacity(o), RESOWNER_HASH_INIT_SIZE);

    for v in 0..33usize {
        ResourceOwnerForget(o, Datum::from_usize(v), &PIN_DESC).unwrap();
    }
    assert_eq!(nhash(o), 0);
    assert_eq!(narr(o), 0);
    ResourceOwnerDelete(o);
}

#[test]
fn hash_grows_by_doubling() {
    setup();
    let o = owner("t");
    for v in 0..200usize {
        remember(o, v, &PIN_DESC);
    }
    // grow_at: 32 @64, 96 @128, 192 @256; the 193rd item doubles to 512.
    assert_eq!(capacity(o), 512);
    for v in 0..200usize {
        ResourceOwnerForget(o, Datum::from_usize(v), &PIN_DESC).unwrap();
    }
    ResourceOwnerDelete(o);
}

#[test]
fn release_orders_by_phase_then_priority() {
    setup();
    let o = owner("t");
    remember(o, 1, &CATREF_DESC);
    remember(o, 2, &PIN_DESC);
    remember(o, 3, &IO_DESC);
    remember(o, 4, &PIN_DESC);

    ResourceOwnerRelease(o, RESOURCE_RELEASE_BEFORE_LOCKS, false, false).unwrap();
    // BEFORE_LOCKS: ios (prio 100) before pins (prio 200); catref stays.
    let before = released();
    assert_eq!(before[0], ("io", 3));
    assert_eq!(
        {
            let mut pins: Vec<_> = before[1..].to_vec();
            pins.sort();
            pins
        },
        vec![("pin", 2), ("pin", 4)]
    );

    ResourceOwnerRelease(o, RESOURCE_RELEASE_LOCKS, false, true).unwrap();
    assert_eq!(released(), vec![]);

    ResourceOwnerRelease(o, RESOURCE_RELEASE_AFTER_LOCKS, false, false).unwrap();
    assert_eq!(released(), vec![("catref", 1)]);

    ResourceOwnerDelete(o);
}

#[test]
fn release_spilled_owner_sorts_hash() {
    setup();
    let o = owner("t");
    for v in 0..40usize {
        remember(o, v, if v % 2 == 0 { &PIN_DESC } else { &CATREF_DESC });
    }
    assert!(nhash(o) > 0);
    ResourceOwnerRelease(o, RESOURCE_RELEASE_BEFORE_LOCKS, false, false).unwrap();
    let pins = released();
    assert_eq!(pins.len(), 20);
    assert!(pins.iter().all(|&(k, v)| k == "pin" && v % 2 == 0));
    ResourceOwnerRelease(o, RESOURCE_RELEASE_LOCKS, false, true).unwrap();
    ResourceOwnerRelease(o, RESOURCE_RELEASE_AFTER_LOCKS, false, false).unwrap();
    let cats = released();
    assert_eq!(cats.len(), 20);
    assert!(cats.iter().all(|&(k, v)| k == "catref" && v % 2 == 1));
    ResourceOwnerDelete(o);
}

#[test]
fn release_recurses_children_first() {
    setup();
    let parent = owner("parent");
    let child = ResourceOwnerCreate(parent, "child").unwrap();
    remember(parent, 100, &PIN_DESC);
    remember(child, 200, &PIN_DESC);

    ResourceOwnerRelease(parent, RESOURCE_RELEASE_BEFORE_LOCKS, false, false).unwrap();
    assert_eq!(released(), vec![("pin", 200), ("pin", 100)]);

    release_rest_and_delete(parent);
}

fn release_rest_and_delete(o: ResourceOwner) {
    ResourceOwnerRelease(o, RESOURCE_RELEASE_LOCKS, false, true).unwrap();
    ResourceOwnerRelease(o, RESOURCE_RELEASE_AFTER_LOCKS, false, false).unwrap();
    let _ = released();
    ResourceOwnerDelete(o);
}

#[test]
fn forget_after_release_started_errors() {
    setup();
    let o = owner("t");
    remember(o, 1, &PIN_DESC);
    remember(o, 2, &CATREF_DESC);
    ResourceOwnerRelease(o, RESOURCE_RELEASE_BEFORE_LOCKS, false, false).unwrap();
    let err = ResourceOwnerForget(o, Datum::from_usize(2), &CATREF_DESC).unwrap_err();
    assert!(err.message().contains("after release started"));
    assert!(ResourceOwnerEnlarge(o).is_err());
    release_rest_and_delete(o);
}

#[test]
fn leak_warning_fires_debug_print_on_commit() {
    setup();
    let o = owner("t");
    remember(o, 42, &PIN_DESC);
    PRINTED.with(|p| p.set(0));
    ResourceOwnerRelease(o, RESOURCE_RELEASE_BEFORE_LOCKS, true, false).unwrap();
    assert_eq!(PRINTED.with(|p| p.get()), 1);
    assert_eq!(released(), vec![("pin", 42)]);
    release_rest_and_delete(o);
}

#[test]
fn release_all_of_kind_releases_only_that_kind() {
    setup();
    let o = owner("t");
    for v in 0..40usize {
        remember(o, v, if v % 2 == 0 { &PIN_DESC } else { &CATREF_DESC });
    }
    ResourceOwnerReleaseAllOfKind(o, &PIN_DESC).unwrap();
    let pins = released();
    assert_eq!(pins.len(), 20);
    assert!(pins.iter().all(|&(k, _)| k == "pin"));
    remember(o, 1000, &PIN_DESC);
    ResourceOwnerForget(o, Datum::from_usize(1000), &PIN_DESC).unwrap();
    release_all_phases(o, false);
    let _ = released();
    ResourceOwnerDelete(o);
}

#[test]
fn locks_cache_overflows_lossily() {
    setup();
    let o = owner("t");
    let tag = |i: i32| LOCALLOCKTAG {
        lock: Default::default(),
        mode: i,
    };
    for i in 0..15i32 {
        ResourceOwnerRememberLock(o, tag(i));
    }
    assert!(ResourceOwnerForgetLock(o, tag(77)).is_err());
    for i in 0..15i32 {
        ResourceOwnerForgetLock(o, tag(i)).unwrap();
    }

    for i in 0..16i32 {
        ResourceOwnerRememberLock(o, tag(i));
    }
    assert_eq!(with_arena(|a| a.data(o).nlocks), MAX_RESOWNER_LOCKS + 1);
    ResourceOwnerForgetLock(o, tag(3)).unwrap();
    assert_eq!(with_arena(|a| a.data(o).nlocks), MAX_RESOWNER_LOCKS + 1);
    ResourceOwnerDelete(o);
}

#[test]
fn reparent_and_delete_recurse() {
    setup();
    let a = owner("a");
    let b = owner("b");
    let c1 = ResourceOwnerCreate(a, "c1").unwrap();
    let c2 = ResourceOwnerCreate(a, "c2").unwrap();
    assert_eq!(ResourceOwnerGetParent(c1), a);

    ResourceOwnerNewParent(c1, b);
    assert_eq!(ResourceOwnerGetParent(c1), b);
    ResourceOwnerNewParent(c2, b);

    ResourceOwnerDelete(b);
    ResourceOwnerDelete(a);
}

// Miri gives fn items nondeterministic addresses; unregister-by-callback
// mirrors C's raw fn-pointer compare, which Miri intentionally breaks.
#[cfg_attr(miri, ignore)]
#[test]
fn release_callbacks_run_most_recent_first() {
    setup();
    fn cb1(phase: ResourceReleasePhase, _c: bool, _t: bool, arg: Datum) {
        RELEASED.with(|r| r.borrow_mut().push(("cb1", arg.as_usize() + phase as usize)));
    }
    fn cb2(phase: ResourceReleasePhase, _c: bool, _t: bool, arg: Datum) {
        RELEASED.with(|r| r.borrow_mut().push(("cb2", arg.as_usize() + phase as usize)));
    }
    RegisterResourceReleaseCallback(cb1, Datum::from_usize(10)).unwrap();
    RegisterResourceReleaseCallback(cb2, Datum::from_usize(20)).unwrap();

    let o = owner("t");
    ResourceOwnerRelease(o, RESOURCE_RELEASE_BEFORE_LOCKS, false, false).unwrap();
    assert_eq!(released(), vec![("cb2", 21), ("cb1", 11)]);

    UnregisterResourceReleaseCallback(cb2, Datum::from_usize(20));
    ResourceOwnerRelease(o, RESOURCE_RELEASE_LOCKS, false, true).unwrap();
    assert_eq!(released(), vec![("cb1", 12)]);
    UnregisterResourceReleaseCallback(cb1, Datum::from_usize(10));
    ResourceOwnerRelease(o, RESOURCE_RELEASE_AFTER_LOCKS, false, false).unwrap();
    assert_eq!(released(), vec![]);
    ResourceOwnerDelete(o);
}

#[test]
fn current_owner_points_to_releasing_owner_during_callbacks() {
    setup();
    fn observe(res: Datum) {
        let expected = ResourceOwner::from_parts(res.as_u32(), (res.as_u64() >> 32) as u32);
        assert_eq!(CurrentResourceOwner(), expected);
        RELEASED.with(|r| r.borrow_mut().push(("obs", 0)));
    }
    static OBSERVE_DESC: ResourceOwnerDesc = ResourceOwnerDesc {
        name: "observer",
        release_phase: RESOURCE_RELEASE_BEFORE_LOCKS,
        release_priority: 1,
        ReleaseResource: observe,
        DebugPrint: None,
    };
    let o = owner("t");
    let token = (o.slot() as u64) | ((o.generation() as u64) << 32);
    remember(o, token as usize, &OBSERVE_DESC);
    assert!(CurrentResourceOwner().is_null());
    ResourceOwnerRelease(o, RESOURCE_RELEASE_BEFORE_LOCKS, false, false).unwrap();
    assert!(CurrentResourceOwner().is_null());
    assert_eq!(released(), vec![("obs", 0)]);
    release_rest_and_delete(o);
}

#[test]
#[should_panic(expected = "stale ResourceOwner")]
#[cfg(debug_assertions)]
fn stale_handle_is_detected() {
    setup();
    let o = owner("t");
    ResourceOwnerDelete(o);
    let o2 = owner("t2");
    assert_eq!(o2.slot(), o.slot());
    let _ = narr(o);
    ResourceOwnerDelete(o2);
}

#[test]
fn aux_process_owner_lifecycle() {
    setup();
    CreateAuxProcessResourceOwner().unwrap();
    let aux = AuxProcessResourceOwner();
    assert!(!aux.is_null());
    assert_eq!(CurrentResourceOwner(), aux);
    remember(aux, 5, &PIN_DESC);
    ReleaseAuxProcessResources(false).unwrap();
    assert_eq!(released(), vec![("pin", 5)]);
    remember(aux, 6, &PIN_DESC);
    ReleaseAuxProcessResources(false).unwrap();
    assert_eq!(released(), vec![("pin", 6)]);
    SetCurrentResourceOwner(ResourceOwner::NULL);
}

#[test]
fn xact_owner_choreography() {
    setup();
    let top = ResourceOwnerCreate(ResourceOwner::NULL, "TopTransaction").unwrap();
    SetTopTransactionResourceOwner(top);
    SetCurTransactionResourceOwner(top);
    SetCurrentResourceOwner(top);
    assert!(!top.is_null());
    assert_eq!(resowner_seams::current_resource_owner::call(), top);

    let sub = ResourceOwnerCreate(CurTransactionResourceOwner(), "SubTransaction").unwrap();
    SetCurTransactionResourceOwner(sub);
    SetCurrentResourceOwner(sub);
    assert_ne!(sub, top);
    assert_eq!(ResourceOwnerGetParent(sub), top);

    remember(sub, 300, &PIN_DESC);
    ResourceOwnerRelease(sub, RESOURCE_RELEASE_BEFORE_LOCKS, false, false).unwrap();
    assert_eq!(released(), vec![("pin", 300)]);
    ResourceOwnerRelease(sub, RESOURCE_RELEASE_LOCKS, false, false).unwrap();
    ResourceOwnerRelease(sub, RESOURCE_RELEASE_AFTER_LOCKS, false, false).unwrap();
    SetCurrentResourceOwner(CurTransactionResourceOwner());
    let parent = ResourceOwnerGetParent(sub);
    SetCurrentResourceOwner(parent);
    SetCurTransactionResourceOwner(parent);
    ResourceOwnerDelete(sub);
    assert_eq!(CurTransactionResourceOwner(), top);

    remember(top, 400, &PIN_DESC);
    SetCurrentResourceOwner(ResourceOwner::NULL);
    assert!(CurrentResourceOwner().is_null());
    ResourceOwnerRelease(top, RESOURCE_RELEASE_BEFORE_LOCKS, true, true).unwrap();
    assert_eq!(released(), vec![("pin", 400)]);
    ResourceOwnerRelease(top, RESOURCE_RELEASE_LOCKS, true, true).unwrap();
    ResourceOwnerRelease(top, RESOURCE_RELEASE_AFTER_LOCKS, true, true).unwrap();
    ResourceOwnerDelete(top);
    SetCurTransactionResourceOwner(ResourceOwner::NULL);
    SetTopTransactionResourceOwner(ResourceOwner::NULL);
    assert!(TopTransactionResourceOwner().is_null());
    assert!(CurTransactionResourceOwner().is_null());
}

#[test]
fn portal_seams_create_release_delete() {
    setup();
    let top = ResourceOwnerCreate(ResourceOwner::NULL, "TopTransaction").unwrap();
    SetTopTransactionResourceOwner(top);
    SetCurTransactionResourceOwner(top);
    SetCurrentResourceOwner(top);

    let portal = resowner_portal_seams::resource_owner_create_portal::call();
    assert_eq!(ResourceOwnerGetParent(portal), top);

    remember(portal, 500, &PIN_DESC);
    resowner_portal_seams::resource_owner_release::call(
        portal,
        RESOURCE_RELEASE_BEFORE_LOCKS,
        false,
        false,
    );
    assert_eq!(released(), vec![("pin", 500)]);
    resowner_portal_seams::resource_owner_release::call(
        portal,
        RESOURCE_RELEASE_LOCKS,
        false,
        false,
    );
    resowner_portal_seams::resource_owner_release::call(
        portal,
        RESOURCE_RELEASE_AFTER_LOCKS,
        false,
        false,
    );
    resowner_portal_seams::resource_owner_new_parent::call(portal, ResourceOwner::NULL);
    resowner_portal_seams::resource_owner_delete::call(portal);

    SetCurrentResourceOwner(ResourceOwner::NULL);
    ResourceOwnerRelease(top, RESOURCE_RELEASE_BEFORE_LOCKS, true, true).unwrap();
    ResourceOwnerRelease(top, RESOURCE_RELEASE_LOCKS, true, true).unwrap();
    ResourceOwnerRelease(top, RESOURCE_RELEASE_AFTER_LOCKS, true, true).unwrap();
    ResourceOwnerDelete(top);
    SetCurTransactionResourceOwner(ResourceOwner::NULL);
    SetTopTransactionResourceOwner(ResourceOwner::NULL);
}

#[test]
fn lock_seams_delegate_to_cache() {
    setup();
    let o = owner("t");
    let tag = LOCALLOCKTAG {
        lock: Default::default(),
        mode: 3,
    };
    ResourceOwnerRememberLock(o, tag);
    assert_eq!(with_arena(|a| a.data(o).nlocks), 1);
    ResourceOwnerForgetLock(o, tag).unwrap();
    assert_eq!(with_arena(|a| a.data(o).nlocks), 0);
    ResourceOwnerDelete(o);
}

#[test]
fn snapshot_seams_track_rc_strong_count() {
    setup();
    let cx = Box::leak(Box::new(::mcx::MemoryContext::new("snap-test")));
    let mcx: ::mcx::Mcx<'static> = cx.mcx();

    let o = owner("t");
    let snap = Rc::new(types_snapshot::SnapshotData::sentinel(
        mcx,
        types_snapshot::SNAPSHOT_MVCC,
    ));

    resowner_seams::resource_owner_enlarge::call(o).unwrap();
    resowner_seams::resource_owner_remember_snapshot::call(o, Rc::clone(&snap));
    assert_eq!(Rc::strong_count(&snap), 2);

    resowner_seams::resource_owner_forget_snapshot::call(o, Rc::clone(&snap));
    assert_eq!(Rc::strong_count(&snap), 1);

    // A leaked registration is dropped by the AFTER_LOCKS release sweep.
    resowner_seams::resource_owner_enlarge::call(o).unwrap();
    resowner_seams::resource_owner_remember_snapshot::call(o, Rc::clone(&snap));
    assert_eq!(Rc::strong_count(&snap), 2);
    release_all_phases(o, false);
    assert_eq!(Rc::strong_count(&snap), 1);
    ResourceOwnerDelete(o);
}

#[test]
fn panic_inside_with_arena_does_not_poison_the_session() {
    setup();

    // Wedge regression (with_state class): a panic unwinding out of the
    // arena closure must clear ARENA_ENTERED or every later resowner call
    // asserts "resowner arena re-entered" forever.
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        with_arena(|_| -> () { panic!("injected loud inside with_arena") })
    }));
    assert!(unwound.is_err());

    let o = owner("post-panic");
    ResourceOwnerDelete(o);
}

#[test]
fn recycle_resets_released_owner_and_keeps_handle_valid() {
    setup();
    let o = owner("TopTransaction");
    remember(o, 41, &PIN_DESC);
    release_all_phases(o, true);
    let _ = released();

    // Drained, parentless, childless, no heap hash: recycle succeeds and the
    // same handle behaves like a freshly created owner.
    assert!(ResourceOwnerRecycle(o));
    with_arena(|a| {
        let d = a.data(o);
        assert!(!d.releasing);
        assert!(!d.sorted);
        assert_eq!(d.narr, 0);
        assert_eq!(d.nlocks, 0);
    });
    remember(o, 42, &PIN_DESC);
    release_all_phases(o, true);
    assert_eq!(released(), vec![("pin", 42)]);
    assert!(ResourceOwnerRecycle(o));
    ResourceOwnerDelete(o);
}

#[test]
fn recycle_refuses_children_parents_and_spilled_hash() {
    setup();
    // Child present: refuse.
    let parent = owner("parent");
    let child = ResourceOwnerCreate(parent, "child").unwrap();
    assert!(!ResourceOwnerRecycle(parent));
    // Parent link present: refuse.
    assert!(!ResourceOwnerRecycle(child));
    ResourceOwnerDelete(parent);

    // Spilled-to-hash owner keeps heap capacity after release: refuse so the
    // real Delete preserves C's pfree.
    let o = owner("spilled");
    for v in 0..(RESOWNER_ARRAY_SIZE + 1) {
        remember(o, v, &PIN_DESC);
    }
    assert!(capacity(o) > 0);
    release_all_phases(o, true);
    let _ = released();
    assert!(!ResourceOwnerRecycle(o));
    ResourceOwnerDelete(o);

    // Retained items: refuse.
    let o2 = owner("live");
    remember(o2, 7, &PIN_DESC);
    assert!(!ResourceOwnerRecycle(o2));
    release_all_phases(o2, true);
    let _ = released();
    ResourceOwnerDelete(o2);
}
