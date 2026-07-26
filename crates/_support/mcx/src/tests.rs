extern crate std;

use super::*;
use core::fmt::Write as _;

#[test]
fn alloc_size_gate_matches_palloc() {
    assert!(check_alloc_size(MAX_ALLOC_SIZE).is_ok());
    let err = check_alloc_size(MAX_ALLOC_SIZE + 1).unwrap_err();
    assert_eq!(
        err.message(),
        alloc::format!("invalid memory alloc request size {}", MAX_ALLOC_SIZE + 1)
    );

    let ctx = MemoryContext::new("t");
    let r: PgResult<PgVec<u64>> = vec_with_capacity_in(ctx.mcx(), (-1i32) as isize as usize);
    assert!(r
        .unwrap_err()
        .message()
        .starts_with("invalid memory alloc request size"));
}

#[test]
fn accounting_tracks_capacity_exactly() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut v: PgVec<u64> = PgVec::new_in(mcx);
    assert_eq!(ctx.used(), 0);
    for i in 0..100u64 {
        v.push(i);
        assert_eq!(ctx.used(), v.capacity() * 8, "after push {}", i);
    }
    v.shrink_to_fit();
    assert_eq!(ctx.used(), v.capacity() * 8);
    assert_eq!(v.capacity(), 100);
    drop(v);
    assert_eq!(ctx.used(), 0, "drop returns every byte");
    assert!(ctx.peak() >= 800);
}

#[test]
fn accounting_multiple_collections_compose() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let a = vec_with_capacity_in::<u8>(mcx, 64).unwrap();
    let b = vec_with_capacity_in::<u8>(mcx, 128).unwrap();
    let mut m: PgHashMap<u32, u32> = PgHashMap::new_in(mcx);
    m.insert(1, 2);
    assert!(ctx.used() >= 192 + core::mem::size_of::<(u32, u32)>());
    drop(a);
    drop(b);
    drop(m);
    assert_eq!(ctx.used(), 0);
}

#[test]
fn limit_enforced_via_try_reserve() {
    let ctx = MemoryContext::new("limited").with_limit(1024);
    let mcx = ctx.mcx();
    let mut v: PgVec<u8> = PgVec::new_in(mcx);
    v.try_reserve_exact(1024).expect("exactly at limit is fine");
    assert_eq!(ctx.used(), 1024);
    let mut w: PgVec<u8> = PgVec::new_in(mcx);
    let err = w.try_reserve_exact(1);
    assert!(err.is_err(), "limit must reject the 1025th byte");
    assert_eq!(ctx.used(), 1024, "failed reservation charged nothing");
}

#[test]
fn oom_error_shape_matches_mcxt_c() {
    let ctx = MemoryContext::new("ExprContext").with_limit(8);
    let mcx = ctx.mcx();
    let mut v: PgVec<u8> = PgVec::new_in(mcx);
    let e = match v.try_reserve_exact(64) {
        Err(_) => mcx.oom(64),
        Ok(()) => panic!("limit not enforced"),
    };
    assert_eq!(e.sqlstate, ERRCODE_OUT_OF_MEMORY);
    assert_eq!(e.message, "out of memory");
    assert_eq!(
        e.detail.as_deref(),
        Some("Failed on request of size 64 in memory context \"ExprContext\".")
    );
}

#[test]
fn bump_context_reset_reclaims_and_reuses() {
    let mut ctx = MemoryContext::new_bump("per-tuple");
    {
        let mcx = ctx.mcx();
        let mut v: PgVec<u32> = PgVec::new_in(mcx);
        for i in 0..1000 {
            v.push(i);
        }
        assert!(ctx.used() > 0);
    } // v drops; bump.c model — dealloc is a full no-op, the charge stays
    assert!(ctx.used() > 0, "bump free is unaccounted; reset releases the charge");
    let footprint_before = ctx.stats().arena_footprint;
    assert!(footprint_before > 0, "arena retains memory after drops");
    ctx.reset();
    assert_eq!(
        ctx.used(),
        ctx.stats().arena_footprint,
        "reset releases everything but the retained keeper (which stays charged)"
    );
    {
        let mcx = ctx.mcx();
        let mut v: PgVec<u32> = PgVec::new_in(mcx);
        for i in 0..1000 {
            v.push(i);
        }
        drop(v);
    }
    assert_eq!(ctx.peak(), ctx.stats().peak);
}

#[test]
fn reset_callbacks_fire_lifo_on_reset_and_drop() {
    use core::cell::RefCell;
    use alloc::rc::Rc;
    let order: Rc<RefCell<alloc::vec::Vec<u8>>> = Rc::default();

    let mut ctx = MemoryContext::new("cb");
    let (o1, o2) = (order.clone(), order.clone());
    ctx.register_reset_callback(move || o1.borrow_mut().push(1));
    ctx.register_reset_callback(move || o2.borrow_mut().push(2));
    ctx.reset();
    assert_eq!(&*order.borrow(), &[2, 1], "LIFO like PG");

    let o3 = order.clone();
    ctx.register_reset_callback(move || o3.borrow_mut().push(3));
    drop(ctx);
    assert_eq!(&*order.borrow(), &[2, 1, 3], "delete fires callbacks too");
}

#[test]
fn pg_string_basics() {
    let ctx = MemoryContext::new("s");
    let mcx = ctx.mcx();
    let mut s = PgString::from_str_in("héllo", mcx).unwrap();
    s.try_push(' ').unwrap();
    s.try_push_str("wörld").unwrap();
    assert_eq!(s, "héllo wörld");
    assert_eq!(ctx.used(), s.capacity_bytes());
    write!(s, " {}", 42).unwrap();
    assert_eq!(s.as_str(), "héllo wörld 42");
    drop(s);
    assert_eq!(ctx.used(), 0);
}

#[test]
fn nested_scopes_thread_explicitly() {
    fn build_row<'mcx>(mcx: Mcx<'mcx>, n: u32) -> PgResult<PgVec<'mcx, u32>> {
        let mut row = vec_with_capacity_in(mcx, n as usize)?;
        row.extend(0..n);
        Ok(row)
    }
    let per_query = MemoryContext::new("per-query");
    let rows = build_row(per_query.mcx(), 16).unwrap();
    assert_eq!(rows.len(), 16);
    assert_eq!(per_query.used(), 64);
}

#[test]
fn child_charges_propagate_to_ancestors() {
    let root = MemoryContext::new("root");
    let query = root.new_child("per-query");
    let tuple = query.new_child("per-tuple");

    let v = vec_with_capacity_in::<u8>(tuple.mcx(), 100).unwrap();
    assert_eq!(tuple.used(), 100);
    assert_eq!(query.used(), 0, "parent's own bytes unaffected");
    assert_eq!(query.subtree_used(), 100);
    assert_eq!(root.subtree_used(), 100);

    let w = vec_with_capacity_in::<u8>(query.mcx(), 50).unwrap();
    assert_eq!(query.subtree_used(), 150);
    assert_eq!(root.subtree_used(), 150);

    drop(v);
    drop(w);
    assert_eq!(root.subtree_used(), 0);
    assert_eq!(root.subtree_peak(), 150);
}

#[test]
fn ancestor_limit_caps_descendants() {
    let root = MemoryContext::new("hash-agg").with_limit(1000);
    let child = root.new_child("batch");

    let _a = vec_with_capacity_in::<u8>(root.mcx(), 600).unwrap();
    let mut v: PgVec<u8> = PgVec::new_in(child.mcx());
    assert!(v.try_reserve_exact(500).is_err(), "600+500 exceeds ancestor limit");
    assert_eq!(root.subtree_used(), 600, "failed charge applied nothing");
    assert_eq!(child.subtree_used(), 0);
    v.try_reserve_exact(400).expect("exactly at the ancestor limit");
    assert_eq!(root.subtree_used(), 1000);
}

#[test]
fn stats_tree_reflects_hierarchy_and_prunes_dropped() {
    let root = MemoryContext::new("root");
    let a = root.new_child("a");
    let _hold = vec_with_capacity_in::<u8>(a.mcx(), 64).unwrap();
    {
        let b = root.new_child("b");
        let t = root.stats_tree();
        assert_eq!(t.children.len(), 2);
        drop(b);
    }
    let t = root.stats_tree();
    assert_eq!(t.name, "root");
    assert_eq!(t.children.len(), 1, "dropped child pruned");
    assert_eq!(t.children[0].name, "a");
    assert_eq!(t.children[0].used, 64);
    assert_eq!(t.subtree_used, 64);
}

#[test]
fn child_may_outlive_parent_accounting_safely() {
    let child;
    {
        let root = MemoryContext::new("root");
        child = root.new_child("survivor");
    } // root dropped; its Acct node stays alive via the child's parent Rc
    let v = vec_with_capacity_in::<u8>(child.mcx(), 32).unwrap();
    assert_eq!(child.used(), 32);
    drop(v);
    assert_eq!(child.used(), 0);
}

#[test]
fn child_churn_does_not_grow_parent_child_list() {
    let root = MemoryContext::new("root");
    for _ in 0..10_000 {
        let child = root.new_child("per-tuple");
        let _v = vec_with_capacity_in::<u8>(child.mcx(), 16).unwrap();
    }
    let t = root.stats_tree();
    assert_eq!(t.children.len(), 0);
    assert_eq!(root.subtree_used(), 0);
}

#[test]
fn pg_string_round_trips_and_keys() {
    let ctx = MemoryContext::new("s");
    let other = MemoryContext::new("o");
    let mcx = ctx.mcx();

    let s = PgString::from_str_in("key", mcx).unwrap();
    let s2 = s.clone_in(other.mcx()).unwrap();
    assert_eq!(s, s2);
    assert_eq!(other.used(), s2.capacity_bytes());

    let mut m: PgHashMap<PgString, u32> = PgHashMap::new_in(mcx);
    m.insert(s, 7);
    assert_eq!(m.get("key"), Some(&7));

    let raw = slice_in(mcx, b"caf\xc3\xa9".as_slice()).unwrap();
    assert_eq!(PgString::from_utf8(raw).unwrap(), "café");
    let bad = slice_in(mcx, b"\xff\xfe".as_slice()).unwrap();
    assert!(PgString::from_utf8(bad).is_err());
}

#[test]
fn chomp_strips_only_trailing_newlines() {
    let ctx = MemoryContext::new("s");
    let mcx = ctx.mcx();
    assert_eq!(PgString::chomp_in("warn: x\n\n", mcx).unwrap(), "warn: x");
    assert_eq!(PgString::chomp_in("a\nb\n", mcx).unwrap(), "a\nb");
    assert_eq!(PgString::chomp_in("no newline", mcx).unwrap(), "no newline");
    assert_eq!(PgString::chomp_in("\n", mcx).unwrap(), "");
}

#[test]
fn ident_set_forget_and_stats() {
    let root = MemoryContext::new("CachedPlanSource");
    assert_eq!(root.ident(), None);
    root.set_ident(Some("SELECT 1"));
    assert_eq!(root.ident().as_deref(), Some("SELECT 1"));
    assert_eq!(root.stats().ident.as_deref(), Some("SELECT 1"));

    let child = root.new_child("CachedPlanQuery");
    child.set_ident(Some("q"));
    let t = root.stats_tree();
    assert_eq!(t.ident.as_deref(), Some("SELECT 1"));
    assert_eq!(t.children[0].ident.as_deref(), Some("q"));

    root.set_ident(None);
    assert_eq!(root.ident(), None, "NULL forgets the old identifier");
}

// Counters must be byte-identical to a naive reference model for any sequence.
#[test]
fn hotpath_invariance_counters_byte_identical() {
    struct Ref {
        used: alloc::vec::Vec<usize>,
        self_peak: alloc::vec::Vec<usize>,
        subtree_peak: alloc::vec::Vec<usize>,
        parent: alloc::vec::Vec<usize>,
    }
    impl Ref {
        fn subtree(&self, i: usize) -> usize {
            let mut total = self.used[i];
            for j in 0..self.used.len() {
                if j != i {
                    let mut p = self.parent[j];
                    while p != usize::MAX {
                        if p == i {
                            total += self.used[j];
                            break;
                        }
                        p = self.parent[p];
                    }
                }
            }
            total
        }
        fn charge(&mut self, i: usize, n: usize) {
            self.used[i] += n;
            if self.used[i] > self.self_peak[i] {
                self.self_peak[i] = self.used[i];
            }
            let mut k = i;
            loop {
                let st = self.subtree(k);
                if st > self.subtree_peak[k] {
                    self.subtree_peak[k] = st;
                }
                if self.parent[k] == usize::MAX {
                    break;
                }
                k = self.parent[k];
            }
        }
        fn uncharge(&mut self, i: usize, n: usize) {
            self.used[i] -= n;
        }
    }

    let root = MemoryContext::new("root");
    let a = root.new_child("a");
    let a1 = a.new_child("a1");
    let b = root.new_child("b");
    let ctxs = [&root, &a, &a1, &b];
    let mut refm = Ref {
        used: alloc::vec![0; 4],
        self_peak: alloc::vec![0; 4],
        subtree_peak: alloc::vec![0; 4],
        parent: alloc::vec![usize::MAX, 0, 1, 0],
    };

    let mut vecs: [PgVec<u8>; 4] = [
        PgVec::new_in(root.mcx()),
        PgVec::new_in(a.mcx()),
        PgVec::new_in(a1.mcx()),
        PgVec::new_in(b.mcx()),
    ];

    let check = |ctxs: &[&MemoryContext; 4], refm: &Ref| {
        for i in 0..4 {
            assert_eq!(ctxs[i].used(), refm.used[i], "used[{i}]");
            assert_eq!(ctxs[i].subtree_used(), refm.subtree(i), "subtree_used[{i}]");
            assert_eq!(ctxs[i].peak(), refm.self_peak[i], "peak[{i}]");
            assert_eq!(
                ctxs[i].subtree_peak(),
                refm.subtree_peak[i],
                "subtree_peak[{i}]"
            );
        }
    };

    let do_reserve = |vecs: &mut [PgVec<u8>; 4], refm: &mut Ref, i: usize, total: usize| {
        let before = vecs[i].capacity();
        vecs[i].reserve_exact(total - vecs[i].len());
        let after = vecs[i].capacity();
        refm.charge(i, after - before);
    };

    do_reserve(&mut vecs, &mut refm, 2, 100);
    check(&ctxs, &refm);
    do_reserve(&mut vecs, &mut refm, 1, 40);
    check(&ctxs, &refm);
    do_reserve(&mut vecs, &mut refm, 0, 7);
    check(&ctxs, &refm);
    do_reserve(&mut vecs, &mut refm, 3, 256);
    check(&ctxs, &refm);
    do_reserve(&mut vecs, &mut refm, 2, 500);
    check(&ctxs, &refm);

    let cap = vecs[2].capacity();
    vecs[2] = PgVec::new_in(a1.mcx());
    refm.uncharge(2, cap);
    check(&ctxs, &refm);

    assert!(root.subtree_peak() >= 7 + 40 + 500 + 256, "peaks persist after frees");

    for (i, v) in vecs.iter_mut().enumerate() {
        let cap = v.capacity();
        *v = PgVec::new_in(ctxs[i].mcx());
        refm.uncharge(i, cap);
    }
    check(&ctxs, &refm);
    assert_eq!(root.subtree_used(), 0);
}

#[test]
fn hotpath_limit_flag_lifecycle_and_enforcement() {
    let root = MemoryContext::new("root");
    let child = root.new_child("child");
    let mut v: PgVec<u8> = PgVec::new_in(child.mcx());
    v.try_reserve_exact(1 << 20).expect("unlimited, skip path");
    assert_eq!(root.subtree_used(), 1 << 20);
    drop(v);

    {
        let limited = MemoryContext::new("limited").with_limit(256);
        let mut w: PgVec<u8> = PgVec::new_in(limited.mcx());
        assert!(w.try_reserve_exact(257).is_err(), "over limit rejected");
        assert_eq!(limited.used(), 0, "failed charge applied nothing");
        w.try_reserve_exact(256).expect("at limit ok");
        assert_eq!(limited.used(), 256);

        let cap_root = MemoryContext::new("cap").with_limit(100);
        let kid = cap_root.new_child("kid");
        let mut k: PgVec<u8> = PgVec::new_in(kid.mcx());
        assert!(k.try_reserve_exact(101).is_err(), "ancestor limit caps kid");
        k.try_reserve_exact(100).expect("at ancestor limit ok");
        assert_eq!(cap_root.subtree_used(), 100);
    }

    let root2 = MemoryContext::new("root2");
    let mut x: PgVec<u8> = PgVec::new_in(root2.mcx());
    x.try_reserve_exact(1 << 20).expect("skip path restored after limits drop");
    assert_eq!(root2.used(), 1 << 20);
}

// BumpDrop: destructors run exactly once at reset/drop.
mod bumpdrop {
    use super::*;
    use alloc::rc::Rc;
    use core::cell::Cell;

    struct DropCounter {
        drops: Rc<Cell<u32>>,
    }
    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    #[test]
    fn destructor_runs_exactly_once_at_reset_not_at_alloc() {
        let drops = Rc::new(Cell::new(0u32));
        let mut ctx = MemoryContext::new_bumpdrop("arena");
        {
            let mcx = ctx.mcx();
            for _ in 0..5 {
                let _r: &mut DropCounter =
                    arena_box_in(mcx, DropCounter { drops: drops.clone() }).unwrap();
            }
            assert_eq!(drops.get(), 0, "no per-object Drop ran at allocation");
            assert!(ctx.used() > 0, "values are charged while live");
        }
        assert_eq!(drops.get(), 0, "leaked values do NOT drop when borrows end");

        ctx.reset();
        assert_eq!(drops.get(), 5, "all destructors run exactly once at reset");
        assert_eq!(ctx.used(), ctx.stats().arena_footprint, "only the keeper stays charged");

        ctx.reset();
        assert_eq!(drops.get(), 5, "no destructor runs twice");
    }

    #[test]
    fn destructor_runs_on_drop_when_context_dropped() {
        let drops = Rc::new(Cell::new(0u32));
        {
            let ctx = MemoryContext::new_bumpdrop("arena");
            let mcx = ctx.mcx();
            for _ in 0..3 {
                let _r = arena_box_in(mcx, DropCounter { drops: drops.clone() }).unwrap();
            }
            assert_eq!(drops.get(), 0);
        }
        assert_eq!(drops.get(), 3, "context drop runs the drop list");
    }

    #[test]
    fn destructors_run_lifo() {
        let order: Rc<RefCell<alloc::vec::Vec<u8>>> = Rc::default();
        struct OrderRec {
            id: u8,
            order: Rc<RefCell<alloc::vec::Vec<u8>>>,
        }
        impl Drop for OrderRec {
            fn drop(&mut self) {
                self.order.borrow_mut().push(self.id);
            }
        }
        let mut ctx = MemoryContext::new_bumpdrop("arena");
        {
            let mcx = ctx.mcx();
            for id in 1..=4u8 {
                let _r = arena_box_in(mcx, OrderRec { id, order: order.clone() }).unwrap();
            }
        }
        ctx.reset();
        assert_eq!(&*order.borrow(), &[4, 3, 2, 1], "drop list runs LIFO like C");
    }

    #[test]
    fn arena_vec_of_non_pod_reclaimed_at_reset() {
        let drops = Rc::new(Cell::new(0u32));
        let mut ctx = MemoryContext::new_bumpdrop("arena");
        {
            let mcx = ctx.mcx();
            let mut v: PgVec<DropCounter> = PgVec::new_in(mcx);
            for _ in 0..10 {
                v.push(DropCounter { drops: drops.clone() });
            }
            let _leaked: &mut PgVec<DropCounter> = arena_vec_in(mcx, v).unwrap();
            assert_eq!(drops.get(), 0, "elements not dropped while vec is live in arena");
        }
        assert_eq!(drops.get(), 0, "vec leaked, elements still live");
        ctx.reset();
        assert_eq!(drops.get(), 10, "all 10 elements dropped once at reset");
        assert_eq!(ctx.used(), ctx.stats().arena_footprint);
    }

    #[test]
    fn arena_string_reclaimed_at_reset() {
        let mut ctx = MemoryContext::new_bumpdrop("arena");
        {
            let mcx = ctx.mcx();
            let s = PgString::from_str_in("hello arena", mcx).unwrap();
            let leaked: &mut PgString = arena_string_in(mcx, s).unwrap();
            assert_eq!(leaked.as_str(), "hello arena");
            assert!(ctx.used() > 0);
        }
        ctx.reset();
        assert_eq!(ctx.used(), ctx.stats().arena_footprint, "string buffer reclaimed at reset");
    }

    #[test]
    fn used_and_subtree_used_invariant_across_alloc_reset() {
        let root = MemoryContext::new("root");
        let mut arena = root.new_child_bumpdrop("arena");
        let drops = Rc::new(Cell::new(0u32));
        for round in 0..3 {
            {
                let mcx = arena.mcx();
                for _ in 0..20 {
                    let _r = arena_box_in(mcx, DropCounter { drops: drops.clone() }).unwrap();
                }
                assert!(arena.used() > 0, "round {round}: charged while live");
                assert_eq!(
                    root.subtree_used(),
                    arena.used(),
                    "round {round}: ancestor subtree mirrors child"
                );
            }
            arena.reset();
            let keeper = arena.stats().arena_footprint;
            assert_eq!(arena.used(), keeper, "round {round}: only the keeper stays charged");
            assert_eq!(
                root.subtree_used(),
                keeper,
                "round {round}: reset propagates to ancestor subtree_used"
            );
            assert_eq!(drops.get(), 20 * (round + 1), "round {round}: 20 more drops");
        }
    }

    #[test]
    fn nested_arenas_reclaim_independently() {
        let drops_outer = Rc::new(Cell::new(0u32));
        let drops_inner = Rc::new(Cell::new(0u32));
        {
            let outer = MemoryContext::new_bumpdrop("outer");
            let _o = arena_box_in(outer.mcx(), DropCounter { drops: drops_outer.clone() }).unwrap();
            let mut inner = outer.new_child_bumpdrop("inner");
            {
                let _i =
                    arena_box_in(inner.mcx(), DropCounter { drops: drops_inner.clone() }).unwrap();
                assert_eq!(outer.subtree_used(), outer.used() + inner.used());
            }
            inner.reset();
            assert_eq!(drops_inner.get(), 1, "inner reset drops inner only");
            assert_eq!(drops_outer.get(), 0, "outer untouched by inner reset");
        }
        assert_eq!(drops_outer.get(), 1, "outer drop runs outer's list once");
        assert_eq!(drops_inner.get(), 1, "inner already drained; no double drop");
    }

    #[test]
    fn panic_in_drop_glue_does_not_double_run_remaining() {
        use std::panic::{catch_unwind, AssertUnwindSafe};

        let drops = Rc::new(Cell::new(0u32));
        struct PanicOnDrop {
            boom: bool,
            drops: Rc<Cell<u32>>,
        }
        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                self.drops.set(self.drops.get() + 1);
                if self.boom {
                    panic!("destructor panic");
                }
            }
        }

        let mut ctx = MemoryContext::new_bumpdrop("arena");
        {
            let mcx = ctx.mcx();
            // Registered A(no), B(BOOM), C(no); LIFO drop order C, B, A.
            let _a = arena_box_in(mcx, PanicOnDrop { boom: false, drops: drops.clone() }).unwrap();
            let _b = arena_box_in(mcx, PanicOnDrop { boom: true, drops: drops.clone() }).unwrap();
            let _c = arena_box_in(mcx, PanicOnDrop { boom: false, drops: drops.clone() }).unwrap();
        }
        let n_before = drops.get();
        assert_eq!(n_before, 0);
        let res = catch_unwind(AssertUnwindSafe(|| ctx.reset()));
        assert!(res.is_err(), "the panic propagates out of reset");
        assert_eq!(drops.get(), 2, "popped-before-run: C+B ran, no double drop");

        let res2 = catch_unwind(AssertUnwindSafe(|| ctx.reset()));
        assert!(res2.is_ok(), "second reset is clean");
        assert_eq!(drops.get(), 3, "A drained on the 2nd reset; B/C never re-run");
    }

    #[test]
    fn pod_value_in_arena_needs_no_drop_entry() {
        let mut ctx = MemoryContext::new_bumpdrop("arena");
        {
            let mcx = ctx.mcx();
            let r: &mut u64 = arena_box_in(mcx, 42u64).unwrap();
            assert_eq!(*r, 42);
        }
        ctx.reset();
        assert_eq!(ctx.used(), ctx.stats().arena_footprint);
    }

    // The hash-join batchCxt pattern: per-batch drop + wholesale reset + reuse.
    #[test]
    fn child_bump_owned_vec_wholesale_reset_and_reuse() {
        let drops = Rc::new(Cell::new(0u32));
        let root = MemoryContext::new("query");
        let mut batch = root.new_child_bump("HashBatchContext");

        let total_batches = 6usize;
        let per_batch = 50usize;
        for b in 0..total_batches {
            let mut v: PgVec<DropCounter> = {
                let mcx = batch.mcx();
                let mut v = PgVec::new_in(mcx);
                for _ in 0..per_batch {
                    v.push(DropCounter { drops: drops.clone() });
                }
                v
            };
            assert_eq!(drops.get() as usize, b * per_batch, "no drops mid-batch");
            assert!(batch.used() > 0, "arena charged while the batch is live");

            v.clear();
            drop(v);
            assert_eq!(
                drops.get() as usize,
                (b + 1) * per_batch,
                "every element dropped exactly once at the batch boundary"
            );
            batch.reset();
            assert_eq!(
                batch.used(),
                batch.stats().arena_footprint,
                "wholesale reset releases everything but the keeper"
            );
        }
        assert_eq!(drops.get() as usize, total_batches * per_batch);

        drop(batch);
        assert_eq!(drops.get() as usize, total_batches * per_batch);
        assert_eq!(root.subtree_used(), 0, "child charge fully released to parent");
    }

    #[test]
    fn child_bump_rebatch_move_preserves_elements() {
        let drops = Rc::new(Cell::new(0u32));
        let mut batch = MemoryContext::new_bump("HashBatchContext");
        {
            let mcx = batch.mcx();
            let mut old: PgVec<DropCounter> = PgVec::new_in(mcx);
            for _ in 0..8 {
                old.push(DropCounter { drops: drops.clone() });
            }
            let mut newv: PgVec<DropCounter> = PgVec::new_in(mcx);
            for e in old.into_iter() {
                newv.push(e);
            }
            assert_eq!(drops.get(), 0, "moved tuples are not dropped during rebatch");
            newv.clear();
            drop(newv);
            assert_eq!(drops.get(), 8, "elements drop once when the new arena drops");
        }
        batch.reset();
        assert_eq!(batch.used(), batch.stats().arena_footprint);
    }

    // OLD (Aset, per-tuple frees) vs NEW (bump, wholesale reset) churn counts.
    #[test]
    fn churn_measurement_per_tuple_free_vs_wholesale_reset() {
        use crate::{alloc_in, PgVec, PgBox};

        let tuples_per_batch = 500usize;
        let nbatch = 16usize;

        fn build_batch<'m>(
            mcx: Mcx<'m>,
            n: usize,
        ) -> alloc::vec::Vec<(PgBox<'m, [u8; 24]>, PgVec<'m, u8>)> {
            let mut v = alloc::vec::Vec::with_capacity(n);
            for _ in 0..n {
                let b = alloc_in(mcx, [0u8; 24]).unwrap();
                let mut data = PgVec::new_in(mcx);
                data.extend_from_slice(&[7u8; 40]);
                v.push((b, data));
            }
            v
        }

        let _ = crate::churn_probe::take();
        {
            let qcx = MemoryContext::new("query-old");
            for _ in 0..nbatch {
                let batch = build_batch(qcx.mcx(), tuples_per_batch);
                drop(batch);
                assert_eq!(qcx.used(), 0);
            }
        }
        let old_real_frees = crate::churn_probe::take();

        let mut wholesale_resets = 0u64;
        {
            let qcx = MemoryContext::new("query-new");
            let mut batch_cxt = qcx.new_child_bump("HashBatchContext");
            for _ in 0..nbatch {
                {
                    let batch = build_batch(batch_cxt.mcx(), tuples_per_batch);
                    assert!(batch_cxt.used() > 0, "batch tuples charged while live");
                    drop(batch);
                }
                batch_cxt.reset();
                wholesale_resets += 1;
                assert_eq!(
                    batch_cxt.used(),
                    batch_cxt.stats().arena_footprint,
                    "wholesale reset releases everything but the keeper"
                );
            }
        }
        let new_real_frees = crate::churn_probe::take();

        std::eprintln!(
            "\n==== HASH-JOIN batchCxt CHURN ({} tuples/batch x {} batches) ====\n\
             OLD (per-query Aset + Vec::clear): real per-chunk frees = {}\n\
             NEW (bump batchCxt + wholesale reset): real per-chunk frees = {} \
             (reclaimed by {} wholesale resets)\n\
             ELIMINATED per-tuple free operations: {}\n\
             ===============================================================",
            tuples_per_batch, nbatch,
            old_real_frees, new_real_frees, wholesale_resets,
            old_real_frees - new_real_frees,
        );

        // Pollution-tolerant: parallel tests also free on Asets.
        let expected_old = (tuples_per_batch * 2 * nbatch) as u64;
        assert!(
            old_real_frees >= expected_old,
            "old model frees every box+vec individually every batch (>= {}, got {})",
            expected_old, old_real_frees,
        );
        // Window-relative, not absolute: the probe counter is global, and the
        // parallel suite's own frees land in both measurement windows — a
        // fixed expected_old/4 bound sat inside that pollution band (fleet
        // 16-thread runs measured 3.9-4.2k ambient frees vs the 4.0k line).
        assert!(
            new_real_frees < old_real_frees / 3,
            "new model eliminates the bulk of per-tuple frees (got {}, old {})",
            new_real_frees, old_real_frees,
        );
        assert_eq!(wholesale_resets, nbatch as u64);
    }
}

mod owned {
    use crate::*;

    struct Plan<'mcx> {
        nodes: PgVec<'mcx, u64>,
    }
    crate::bind!(PlanTy => Plan<'mcx>);

    fn build_plan(root: &MemoryContext, n: u64) -> PgResult<McxOwned<PlanTy>> {
        McxOwned::try_new(root.new_child("cached-plan"), |mcx| {
            let mut nodes = vec_with_capacity_in(mcx, n as usize)?;
            nodes.extend(0..n);
            Ok(Plan { nodes })
        })
    }

    #[test]
    fn bundle_moves_and_outlives_its_builder_scope() {
        let cache_root = MemoryContext::new("CacheMemoryContext");
        let mut cache: alloc::vec::Vec<McxOwned<PlanTy>> = alloc::vec::Vec::new();
        {
            let plan = build_plan(&cache_root, 100).unwrap();
            assert_eq!(plan.with(|p| p.nodes.len()), 100);
            cache.push(plan);
        }
        let plan = &mut cache[0];
        assert_eq!(plan.with(|p| p.nodes.iter().sum::<u64>()), 4950);
        assert!(cache_root.subtree_used() >= 800, "bundle bytes visible from the cache root");

        let before = plan.context().used();
        plan.with_mut(|p| {
            for i in 0..1000 {
                p.nodes.push(i);
            }
        });
        assert!(plan.context().used() > before);

        drop(cache);
        assert_eq!(cache_root.subtree_used(), 0, "dropping the bundle returns every byte");
    }

    struct Tree<'mcx> {
        plan_tree: Option<PgBox<'mcx, u64>>,
    }
    crate::bind!(TreeTy => Tree<'mcx>);

    #[test]
    fn leak_projection_yields_honest_borrow_reclaimed_by_context_drop() {
        let root = MemoryContext::new("root");
        let mut bundle = McxOwned::<TreeTy>::try_new(root.new_child("ExecutorState"), |mcx| {
            Ok(Tree { plan_tree: Some(alloc_in(mcx, 42u64)?) })
        })
        .unwrap();

        let seen = bundle.with_mut(|t| {
            let leaked: Option<&u64> = t.plan_tree.take().map(|b| &*crate::leak_in(b));
            leaked.copied()
        });
        assert_eq!(seen, Some(42));

        drop(bundle);
        assert_eq!(root.subtree_used(), 0, "context drop reclaims the leaked plan node");
    }

    #[test]
    fn build_failure_passes_through_and_drops_context() {
        let root = MemoryContext::new("root");
        let r = McxOwned::<PlanTy>::try_new(root.new_child("doomed").with_limit(8), |mcx| {
            let mut nodes: PgVec<u64> = PgVec::new_in(mcx);
            nodes.try_reserve_exact(64).map_err(|_| mcx.oom(512))?;
            nodes.extend(0..64);
            Ok(Plan { nodes })
        });
        assert!(r.is_err());
        assert_eq!(root.subtree_used(), 0);
    }

    #[test]
    fn slice_in_bytes_roundtrip() {
        let ctx = MemoryContext::new("copy");
        let src: [u8; 64] = core::array::from_fn(|i| (i as u8).wrapping_mul(7).wrapping_add(3));
        let v = slice_in(ctx.mcx(), &src).unwrap();
        assert_eq!(&*v, &src);
        assert_eq!(v.len(), 64);
        assert_eq!(v.capacity(), 64);
    }

    #[test]
    fn slice_in_empty_is_empty() {
        let ctx = MemoryContext::new("copy_empty");
        let empty: [u8; 0] = [];
        let v = slice_in(ctx.mcx(), &empty).unwrap();
        assert!(v.is_empty());
        assert_eq!(v.len(), 0);
        assert_eq!(ctx.subtree_used(), 0, "empty source allocates nothing");
    }

    #[test]
    fn slice_in_charges_and_reclaims() {
        let root = MemoryContext::new("copy_acct");
        {
            let src = [9u8; 128];
            let v = slice_in(root.mcx(), &src).unwrap();
            assert!(root.subtree_used() >= 128, "charge reflects the copied bytes");
            drop(v);
        }
        assert_eq!(root.subtree_used(), 0, "dropping the vec uncharges");
    }

    #[test]
    fn slice_in_panic_safe_frees_buffer() {
        // A panicking clone() must free the buffer; the prefix needs no drops.
        use core::sync::atomic::{AtomicUsize, Ordering};
        use super::std::panic::{catch_unwind, AssertUnwindSafe};

        static CLONES: AtomicUsize = AtomicUsize::new(0);

        #[derive(Default)]
        struct Bomb(u8);
        impl Clone for Bomb {
            fn clone(&self) -> Self {
                if CLONES.fetch_add(1, Ordering::SeqCst) == 2 {
                    panic!("boom on the 3rd clone");
                }
                Bomb(self.0)
            }
        }

        let root = MemoryContext::new("panic_safe");
        let src = alloc::vec![Bomb(0), Bomb(1), Bomb(2), Bomb(3)];

        CLONES.store(0, Ordering::SeqCst);

        let mcx = root.mcx();
        let res = catch_unwind(AssertUnwindSafe(|| {
            let _ = slice_in(mcx, &src);
        }));
        assert!(res.is_err(), "the clone panic must propagate");
        assert_eq!(root.subtree_used(), 0, "the panic-aborted buffer was freed");

        drop(src);
    }
}

mod bump_pool {
    use super::*;

    // Keeper retention across reset (the pool itself is covered by keeper_pool_caps_and_recycles).
    #[test]
    fn bump_reset_retains_and_reuses_keeper_block() {
        let mut ctx = MemoryContext::new_bump("per-tuple");
        let first = {
            let mcx = ctx.mcx();
            let v = vec_with_capacity_in::<u8>(mcx, 64).unwrap();
            v.as_ptr() as usize
        };
        let footprint = ctx.stats().arena_footprint;
        assert!(footprint > 0);
        ctx.reset();
        assert_eq!(ctx.stats().arena_footprint, footprint, "keeper retained over reset");
        let again = {
            let mcx = ctx.mcx();
            let v = vec_with_capacity_in::<u8>(mcx, 64).unwrap();
            v.as_ptr() as usize
        };
        assert_eq!(first, again, "first post-reset chunk starts the retained keeper");
        assert_eq!(ctx.stats().arena_footprint, footprint, "no new block malloc'd");
    }
}

mod acct_pool {
    use super::*;
    use core::cell::{Cell, RefCell};
    use core::ptr::NonNull;

    fn dummy_acct(name: &'static str) -> Acct {
        Acct {
            name: Cell::new(name),
            ident: RefCell::new(None),
            self_used: Cell::new(0),
            self_peak: Cell::new(0),
            limit: Cell::new(usize::MAX),
            limited_path: Cell::new(false),
            arena_footprint: Cell::new(0),
            arena_nblocks: Cell::new(0),
            window_tail: Cell::new(0),
            is_bump: false,
            kind: "AllocSet",
            parent: None,
            children: RefCell::new(alloc::vec::Vec::new()),
        }
    }

    #[test]
    fn refcounts_match_rc_semantics() {
        let a = AcctRc::new(dummy_acct("a"));
        assert_eq!(a.name.get(), "a");
        let b = a.clone();
        let w = a.downgrade();
        assert_eq!(w.strong_count(), 2);
        drop(b);
        assert_eq!(w.strong_count(), 1);
        let c = w.upgrade().expect("value still alive");
        assert_eq!(c.name.get(), "a");
        assert_eq!(w.strong_count(), 2);
        drop(c);
        drop(a);
        assert_eq!(w.strong_count(), 0);
        assert!(w.upgrade().is_none(), "must not resurrect a dropped value");
        drop(w);
    }

    // A recycled allocation has no live handle, so reuse cannot alias.
    #[test]
    fn recycle_is_use_after_reset_free() {
        let pool = AcctPool::new(alloc::vec::Vec::new());

        let p1: NonNull<AcctInner> = acct_take_from(&pool);
        unsafe {
            core::ptr::addr_of_mut!((*p1.as_ptr()).strong).write(Cell::new(1));
            core::ptr::addr_of_mut!((*p1.as_ptr()).weak).write(Cell::new(1));
            core::ptr::addr_of_mut!((*p1.as_ptr()).val)
                .write(core::mem::MaybeUninit::new(dummy_acct("first")));
            assert_eq!((*p1.as_ptr()).val.assume_init_ref().name.get(), "first");
            core::ptr::drop_in_place(core::ptr::addr_of_mut!((*p1.as_ptr()).val).cast::<Acct>());
            (*p1.as_ptr()).strong.set(0);
            (*p1.as_ptr()).weak.set(0);
        }
        acct_give_to(&pool, p1);
        assert_eq!(pool.try_with(|s| s.len()).unwrap(), 1, "allocation parked for reuse");

        let p2 = acct_take_from(&pool);
        assert_eq!(p1.as_ptr(), p2.as_ptr(), "parked allocation reused");
        unsafe {
            core::ptr::addr_of_mut!((*p2.as_ptr()).strong).write(Cell::new(1));
            core::ptr::addr_of_mut!((*p2.as_ptr()).weak).write(Cell::new(1));
            core::ptr::addr_of_mut!((*p2.as_ptr()).val)
                .write(core::mem::MaybeUninit::new(dummy_acct("second")));
            assert_eq!((*p2.as_ptr()).val.assume_init_ref().name.get(), "second");
            core::ptr::drop_in_place(core::ptr::addr_of_mut!((*p2.as_ptr()).val).cast::<Acct>());
            Global.deallocate(p2.cast(), core::alloc::Layout::new::<AcctInner>());
        }
    }

    #[test]
    fn child_context_churn_is_sound() {
        let root = MemoryContext::new("root");
        for i in 0..50 {
            let child = root.new_child("child");
            {
                let mut v: PgVec<u64> = vec_with_capacity_in(child.mcx(), i).unwrap();
                v.push(i as u64);
            }
            drop(child);
        }
        let _ = root.stats_tree();
        assert_eq!(root.name(), "root");
    }


    #[derive(Clone, Copy)]
    #[allow(dead_code)]
    struct Node {
        oid: u32,
        cost: f64,
    }

    #[test]
    fn forget_on_reset_runs_no_destructor_for_arena_safe() {
        let mut ctx = MemoryContext::new_bumpforget("forget");
        assert_eq!(ctx.used(), 0);
        for _ in 0..8 {
            {
                let mcx = ctx.mcx();
                let n: &mut Node = arena_box_in_forget(mcx, Node { oid: 1, cost: 2.0 }).unwrap();
                assert_eq!(n.oid, 1);
                let mut v: PgVec<Node> = PgVec::new_in(mcx);
                for i in 0..100u32 {
                    v.push(Node { oid: i, cost: i as f64 });
                }
                let leaked: &mut PgVec<Node> = arena_vec_in_forget(mcx, v).unwrap();
                assert_eq!(leaked.len(), 100);
                assert!(ctx.used() > 0, "charged while live");
            }
            ctx.reset();
            assert_eq!(
                ctx.used(),
                ctx.stats().arena_footprint,
                "forget-reset returns the charge to the keeper baseline"
            );
        }
    }

    #[test]
    fn forget_on_reset_leak_harness_charge_returns_to_baseline() {
        let mut ctx = MemoryContext::new_bumpforget("planner-like");
        let baseline = ctx.used();
        assert_eq!(baseline, 0);
        for run in 0..32 {
            {
                let mcx = ctx.mcx();
                let mut graph: PgVec<Node> = PgVec::new_in(mcx);
                graph.extend((0..(run as u32 + 1) * 10).map(|i| Node { oid: i, cost: 0.0 }));
                let _leaked = arena_vec_in_forget(mcx, graph).unwrap();
                let _b = arena_box_in_forget(mcx, Node { oid: run, cost: 1.0 }).unwrap();
            }
            ctx.reset();
            assert_eq!(
                ctx.used(),
                ctx.stats().arena_footprint,
                "run {run}: no accounting leak beyond the keeper"
            );
        }
        drop(ctx);
    }

    #[test]
    fn forget_context_drop_without_reset_is_sound() {
        let drops = std::rc::Rc::new(std::cell::Cell::new(0u32));
        {
            let ctx = MemoryContext::new_bumpforget("forget");
            let mcx = ctx.mcx();
            let _a = arena_box_in_forget(mcx, Node { oid: 7, cost: 0.0 }).unwrap();
            let mut v: PgVec<u64> = PgVec::new_in(mcx);
            v.extend(0..256);
            let _leaked = arena_vec_in_forget(mcx, v).unwrap();
            let _ = &drops;
        }
        assert_eq!(drops.get(), 0, "no destructors run for forgotten values");
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "BumpForget")]
    fn forget_helper_rejects_non_forget_context() {
        let ctx = MemoryContext::new_bump("not-forget");
        let _ = arena_box_in_forget(ctx.mcx(), Node { oid: 0, cost: 0.0 }).unwrap();
    }
}

#[test]
fn generation_context_fifo_and_reset_roundtrip() {
    let mut ctx = MemoryContext::new_generation("gen");
    {
        let mcx = ctx.mcx();
        let mut ring: std::collections::VecDeque<PgBox<'_, [u64; 8]>> =
            std::collections::VecDeque::new();
        for i in 0..32u64 {
            ring.push_back(box_new_in(mcx, [i; 8]));
        }
        let peak0 = ctx.stats().arena_footprint;
        for i in 0..10_000u64 {
            ring.push_back(box_new_in(mcx, [i; 8]));
            let old = ring.pop_front().unwrap();
            assert_eq!(old[0], if i < 32 { i } else { i - 32 });
            drop(old);
        }
        assert!(ctx.stats().arena_footprint <= peak0.max(4 * 8192));
        assert!(ctx.used() > 0);
    }
    ctx.reset();
    assert_eq!(ctx.used(), 0);
    let p = box_new_in(ctx.mcx(), 7u64);
    assert_eq!(*p, 7);
    assert!(ctx.used() > 0, "keeper re-charged on first post-reset alloc");
}

#[test]
fn generation_vec_grows_and_frees_through_context() {
    let ctx = MemoryContext::new_generation("gen");
    let mcx = ctx.mcx();
    let mut v: PgVec<u64> = PgVec::new_in(mcx);
    for i in 0..10_000u64 {
        v.push(i);
    }
    assert_eq!(v.iter().sum::<u64>(), 10_000 * 9_999 / 2);
    drop(v);
    let s = ctx.stats();
    assert!(s.arena_footprint > 0);
}

#[test]
fn slab_context_boxes_roundtrip_and_uncharge() {
    let mut ctx = MemoryContext::new_slab("slab", 8 * 1024, core::mem::size_of::<[u64; 9]>());
    {
        let mcx = ctx.mcx();
        let mut held: std::vec::Vec<PgBox<'_, [u64; 9]>> = std::vec::Vec::new();
        for i in 0..5_000u64 {
            held.push(box_new_in(mcx, [i; 9]));
        }
        let peak = ctx.used();
        assert!(peak >= 5_000 * 72);
        for (i, b) in held.drain(..).enumerate() {
            assert_eq!(b[0], i as u64);
            drop(b);
        }
        // Up to 10 empty blocks stay parked; the rest uncharge.
        assert!(ctx.used() <= 10 * 8 * 1024, "used {} after drain", ctx.used());
        assert!(ctx.used() < peak);
    }
    ctx.reset();
    assert_eq!(ctx.used(), 0);
    assert_eq!(ctx.stats().arena_footprint, 0);
}

#[test]
fn slab_child_context_reports_in_parent_subtree() {
    let parent = MemoryContext::new("parent");
    let child = parent.new_child_slab("slab-child", 8 * 1024, 64);
    let b = box_new_in(child.mcx(), [0u8; 64]);
    assert!(parent.subtree_used() >= 8 * 1024);
    drop(b);
    drop(child);
}
// GL-MEMWATCH-1: the process-wide block-bytes counter balances across a
// context lifecycle. Other tests allocate concurrently, so the assertions
// are delta-based with a large dedicated allocation and a noise allowance.
#[test]
fn global_footprint_tracks_context_block_bytes() {
    const BIG: usize = 32 * 1024 * 1024;
    const NOISE: usize = 8 * 1024 * 1024;
    let base = global_footprint::bytes();
    {
        let ctx = MemoryContext::new("footprint-probe");
        // Dedicated (over chunk limit) allocation: counted at exact size.
        let v: PgVec<'_, u8> = vec_with_capacity_in(ctx.mcx(), BIG).unwrap();
        let held = global_footprint::bytes();
        drop(v);
        // NOISE rides BOTH directions: concurrent tests FREE between the
        // two samples of the process-global counter too (the lower bound
        // without the allowance was a latent flake — adjudicated by the
        // logdec lane, GL-CONCMEM-1 pays it in passing).
        assert!(
            held + NOISE >= base + BIG,
            "global footprint {held} did not grow by the dedicated {BIG} over base {base}"
        );
        drop(ctx);
        let after = global_footprint::bytes();
        assert!(
            after + NOISE >= base && after <= held.saturating_sub(BIG) + NOISE,
            "global footprint {after} did not return toward base {base} (held {held})"
        );
    }
    // Bump family balances too: create, spill past the keeper, drop.
    let base2 = global_footprint::bytes();
    {
        let mut ctx = MemoryContext::new_bump("footprint-bump-probe");
        for _ in 0..4000 {
            let _ = box_new_in(ctx.mcx(), [0u8; 4096]);
        }
        assert!(global_footprint::bytes() + NOISE >= base2 + 4000 * 4096);
        ctx.reset();
    }
    let after2 = global_footprint::bytes();
    assert!(
        after2 <= base2 + NOISE,
        "bump-family bytes not released: after {after2} base {base2}"
    );
}


// LocalStack (the per-thread pool container; the TLS statics themselves are
// cfg(not(test)) — the integration tests in tests/ exercise those).
#[cfg(feature = "std")]
mod local_stack {
    use crate::LocalStack;
    use core::sync::atomic::{AtomicUsize, Ordering};

    static DISPOSED: AtomicUsize = AtomicUsize::new(0);
    fn dispose(_v: u32) {
        DISPOSED.fetch_add(1, Ordering::Relaxed);
    }

    // One test fn: the dispose counter is shared, so the scenarios run
    // sequentially here rather than as parallel #[test]s.
    #[test]
    fn bounded_lifo_with_dispose() {
        // LIFO take/give.
        let s: LocalStack<u32> = LocalStack::new(3, dispose);
        assert_eq!(s.take(), None);
        s.give(1);
        s.give(2);
        assert_eq!(s.take(), Some(2));
        assert_eq!(s.take(), Some(1));
        assert_eq!(s.take(), None);

        // give: full list disposes the INCOMING item, keeps the cached ones.
        let before = DISPOSED.load(Ordering::Relaxed);
        s.give(1);
        s.give(2);
        s.give(3);
        s.give(4);
        assert_eq!(DISPOSED.load(Ordering::Relaxed), before + 1);
        assert_eq!(s.len(), 3);
        assert_eq!(s.take(), Some(3));

        // give_wholesale: full list drains EVERYTHING, keeps the incoming.
        s.give(3); // back to full: [1, 2, 3]
        let before = DISPOSED.load(Ordering::Relaxed);
        s.give_wholesale(9);
        assert_eq!(DISPOSED.load(Ordering::Relaxed), before + 3);
        assert_eq!(s.len(), 1);
        assert_eq!(s.take(), Some(9));

        // Drop drains the residue.
        s.give(7);
        s.give(8);
        let before = DISPOSED.load(Ordering::Relaxed);
        drop(s);
        assert_eq!(DISPOSED.load(Ordering::Relaxed), before + 2);
    }


}
