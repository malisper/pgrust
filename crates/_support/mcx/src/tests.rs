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

    // A negative C count sign-extended to usize is caught by the gate.
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
    // failed reservation charged nothing
    assert_eq!(ctx.used(), 1024);
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
    } // v drops; bump dealloc is a no-op but the bytes are uncharged
    assert_eq!(ctx.used(), 0);
    let footprint_before = ctx.stats().arena_footprint;
    assert!(footprint_before > 0, "arena retains memory after drops");
    ctx.reset();
    // arena is reusable without growing
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
    // The translation rule for C's CurrentMemoryContext: pass Mcx down.
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
    // failed charge applied nothing anywhere
    assert_eq!(root.subtree_used(), 600);
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
    // All children are dead; the weak list must have been pruned along the
    // way rather than holding 10k tombstones.
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

    // Borrow<str> + Hash-as-str: &str probes find PgString keys.
    let mut m: PgHashMap<PgString, u32> = PgHashMap::new_in(mcx);
    m.insert(s, 7);
    assert_eq!(m.get("key"), Some(&7));

    // from_utf8 reuses the allocation; invalid bytes are rejected.
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

/// Hot-path micro-opt invariance (#7a): the limit-walk elision + root
/// fast-path must produce byte-identical `used`/`subtree_used`/`peak`/
/// `subtree_peak` for ANY alloc/grow/shrink/free/reset sequence on a
/// multi-level tree. This recomputes the four counters with a deliberately
/// naive, branch-free reference model and asserts equality at every step.
#[test]
fn hotpath_invariance_counters_byte_identical() {
    // A reference accounting model: a flat tree of nodes, each tracking
    // self_used + peak; subtree counters are recomputed by summing the model.
    struct Ref {
        // self_used per node index
        used: alloc::vec::Vec<usize>,
        self_peak: alloc::vec::Vec<usize>,
        subtree_peak: alloc::vec::Vec<usize>,
        // parent index (usize::MAX for root)
        parent: alloc::vec::Vec<usize>,
    }
    impl Ref {
        fn subtree(&self, i: usize) -> usize {
            // sum of self_used over i and all transitive descendants
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
            // bump subtree_peak of self and every ancestor
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

    // Tree: root -> {a -> {a1}, b}
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

    // Helper: reserve_exact on a vec drives charge/grow; track the byte delta.
    let do_reserve = |vecs: &mut [PgVec<u8>; 4], refm: &mut Ref, i: usize, total: usize| {
        let before = vecs[i].capacity();
        vecs[i].reserve_exact(total - vecs[i].len());
        let after = vecs[i].capacity();
        refm.charge(i, after - before);
    };

    do_reserve(&mut vecs, &mut refm, 2, 100); // a1: 100
    check(&ctxs, &refm);
    do_reserve(&mut vecs, &mut refm, 1, 40); // a: 40
    check(&ctxs, &refm);
    do_reserve(&mut vecs, &mut refm, 0, 7); // root self: 7
    check(&ctxs, &refm);
    do_reserve(&mut vecs, &mut refm, 3, 256); // b: 256
    check(&ctxs, &refm);
    do_reserve(&mut vecs, &mut refm, 2, 500); // a1 grow: +400
    check(&ctxs, &refm);

    // Free a1 entirely (drop returns every byte).
    let cap = vecs[2].capacity();
    vecs[2] = PgVec::new_in(a1.mcx());
    refm.uncharge(2, cap);
    check(&ctxs, &refm);

    // Peaks must persist after frees.
    assert!(root.subtree_peak() >= 7 + 40 + 500 + 256);

    // Free everything; counters return to zero, peaks stay.
    for (i, v) in vecs.iter_mut().enumerate() {
        let cap = v.capacity();
        *v = PgVec::new_in(ctxs[i].mcx());
        refm.uncharge(i, cap);
    }
    check(&ctxs, &refm);
    assert_eq!(root.subtree_used(), 0);
}

/// The finite-limit count must drive the `charge` skip flag correctly across
/// set/drop, and the limit-check itself must still reject over-limit charges
/// (forcing the non-elided validation walk).
#[test]
fn hotpath_limit_flag_lifecycle_and_enforcement() {
    // No limits anywhere: unlimited charges always succeed (skip path).
    let root = MemoryContext::new("root");
    let child = root.new_child("child");
    let mut v: PgVec<u8> = PgVec::new_in(child.mcx());
    v.try_reserve_exact(1 << 20).expect("unlimited, skip path");
    assert_eq!(root.subtree_used(), 1 << 20);
    drop(v);

    // A limited sibling forces the validation walk process-wide; the unlimited
    // subtree above is unaffected, the limited one is enforced.
    {
        let limited = MemoryContext::new("limited").with_limit(256);
        let mut w: PgVec<u8> = PgVec::new_in(limited.mcx());
        assert!(w.try_reserve_exact(257).is_err(), "over limit rejected");
        assert_eq!(limited.used(), 0, "failed charge applied nothing");
        w.try_reserve_exact(256).expect("at limit ok");
        assert_eq!(limited.used(), 256);

        // Ancestor limit caps a descendant even while the descendant is unlimited.
        let cap_root = MemoryContext::new("cap").with_limit(100);
        let kid = cap_root.new_child("kid");
        let mut k: PgVec<u8> = PgVec::new_in(kid.mcx());
        assert!(k.try_reserve_exact(101).is_err(), "ancestor limit caps kid");
        k.try_reserve_exact(100).expect("at ancestor limit ok");
        assert_eq!(cap_root.subtree_used(), 100);
    } // limited + cap_root drop here -> finite-limit count returns toward 0

    // After the limited contexts drop, unlimited charges still work (and now
    // take the skip path again). This also exercises the Acct::drop decrement.
    let root2 = MemoryContext::new("root2");
    let mut x: PgVec<u8> = PgVec::new_in(root2.mcx());
    x.try_reserve_exact(1 << 20).expect("skip path restored after limits drop");
    assert_eq!(root2.used(), 1 << 20);
}

/// Drop-aware bump arena (`Backend::BumpDrop`): owned values registered via the
/// `arena_*_in` API have their destructor run **exactly once at reset/drop**,
/// not per-object, not twice.
mod bumpdrop {
    use super::*;
    use alloc::rc::Rc;
    use core::cell::Cell;

    /// A type whose `Drop` bumps a shared counter, so we can assert the
    /// destructor ran exactly once (and at the right time).
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
            // Allocate several Drop-having values into the arena.
            for _ in 0..5 {
                let _r: &mut DropCounter =
                    arena_box_in(mcx, DropCounter { drops: drops.clone() }).unwrap();
            }
            // Crucially: NONE have dropped yet — they were leaked into the arena.
            assert_eq!(drops.get(), 0, "no per-object Drop ran at allocation");
            assert!(ctx.used() > 0, "values are charged while live");
        }
        // Still zero after the borrow scope ends — leak suppressed individual Drop.
        assert_eq!(drops.get(), 0, "leaked values do NOT drop when borrows end");

        ctx.reset();
        // Exactly five destructors ran, once each, at reset.
        assert_eq!(drops.get(), 5, "all destructors run exactly once at reset");
        // Accounting zeroed by the single counter-reset.
        assert_eq!(ctx.used(), 0, "reset releases the whole live charge");

        // Reset again: nothing to run, no double-drop.
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
        } // ctx dropped here
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
        // A PgVec<DropCounter> stored in the arena: its OWN drop_in_place must
        // run, dropping every element exactly once at reset.
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
        assert_eq!(ctx.used(), 0);
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
        assert_eq!(ctx.used(), 0, "string buffer reclaimed at reset");
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
            assert_eq!(arena.used(), 0, "round {round}: reset zeroes self_used");
            assert_eq!(
                root.subtree_used(),
                0,
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
            // inner drops here (already drained — no double drop), then outer.
        }
        assert_eq!(drops_outer.get(), 1, "outer drop runs outer's list once");
        assert_eq!(drops_inner.get(), 1, "inner already drained; no double drop");
    }

    #[test]
    fn panic_in_drop_glue_does_not_double_run_remaining() {
        // A destructor that panics must not corrupt the drop list: entries are
        // popped before running, so a panic leaks the still-unrun earlier
        // entries (safe) but never double-runs the panicked or any other entry.
        use std::panic::{catch_unwind, AssertUnwindSafe};

        let drops = Rc::new(Cell::new(0u32));
        struct PanicOnDrop {
            // Boom on the 3rd-registered (== first popped is last-registered).
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
            // Registered order: A(no), B(BOOM), C(no). LIFO drop order: C, B, A.
            let _a = arena_box_in(mcx, PanicOnDrop { boom: false, drops: drops.clone() }).unwrap();
            let _b = arena_box_in(mcx, PanicOnDrop { boom: true, drops: drops.clone() }).unwrap();
            let _c = arena_box_in(mcx, PanicOnDrop { boom: false, drops: drops.clone() }).unwrap();
        }
        let n_before = drops.get();
        assert_eq!(n_before, 0);
        let res = catch_unwind(AssertUnwindSafe(|| ctx.reset()));
        assert!(res.is_err(), "the panic propagates out of reset");
        // C ran, then B panicked: 2 drops. A is leaked (never run), not double-run.
        assert_eq!(drops.get(), 2, "popped-before-run: C+B ran, no double drop");

        // A second reset must NOT re-run B or C (they were popped). It may run A
        // (still queued). No entry runs twice.
        let res2 = catch_unwind(AssertUnwindSafe(|| ctx.reset()));
        assert!(res2.is_ok(), "second reset is clean");
        assert_eq!(drops.get(), 3, "A drained on the 2nd reset; B/C never re-run");
    }

    #[test]
    fn pod_value_in_arena_needs_no_drop_entry() {
        // A Drop-trivial value (u64) bump-allocated and leaked is correct without
        // any destructor (Strategy A): reset just reclaims the bytes.
        let mut ctx = MemoryContext::new_bumpdrop("arena");
        {
            let mcx = ctx.mcx();
            let r: &mut u64 = arena_box_in(mcx, 42u64).unwrap();
            assert_eq!(*r, 42);
        }
        ctx.reset();
        assert_eq!(ctx.used(), 0);
    }

    /// Models the hash-join `batchCxt` usage (nodeHash): a CHILD bump context
    /// backs an OWNED `PgVec` of Drop-having values (the per-batch tuple arena).
    /// Per-batch reset: drop the owned Vec (each element's Drop runs a cheap bump
    /// *deallocate*, not a malloc/free), `reset()` the context wholesale, then
    /// re-stage a fresh Vec and reuse. Asserts: no double-drop, charge returns to
    /// zero each cycle, and the re-staged arena works across many batches. This is
    /// the exact pattern `ExecHashTableReset` relies on (NOT arena_leak/BumpDrop —
    /// the rebatch path `mem::replace`s the owned Vec, which arena_leak forbids).
    #[test]
    fn child_bump_owned_vec_wholesale_reset_and_reuse() {
        let drops = Rc::new(Cell::new(0u32));
        let root = MemoryContext::new("query");
        let mut batch = root.new_child_bump("HashBatchContext");

        let total_batches = 6usize;
        let per_batch = 50usize;
        for b in 0..total_batches {
            // Build the per-batch arena in the child bump context.
            let mut v: PgVec<DropCounter> = {
                // Reborrow the child's Mcx (the nodeHash code transmutes this to
                // the table's 'mcx; here a plain borrow suffices for the test).
                let mcx = batch.mcx();
                let mut v = PgVec::new_in(mcx);
                for _ in 0..per_batch {
                    v.push(DropCounter { drops: drops.clone() });
                }
                v
            };
            assert_eq!(drops.get() as usize, b * per_batch, "no drops mid-batch");
            assert!(batch.used() > 0, "arena charged while the batch is live");

            // Per-batch reset: drop the owned arena (cheap bump-deallocates that
            // uncharge), then reset the context wholesale.
            v.clear();
            drop(v);
            assert_eq!(
                drops.get() as usize,
                (b + 1) * per_batch,
                "every element dropped exactly once at the batch boundary"
            );
            batch.reset();
            assert_eq!(batch.used(), 0, "wholesale reset returns the charge to zero");
        }
        // No element dropped more than once across all batches.
        assert_eq!(drops.get() as usize, total_batches * per_batch);

        // Dropping the context after a clean reset is a no-op (no double-free).
        drop(batch);
        assert_eq!(drops.get() as usize, total_batches * per_batch);
        assert_eq!(root.subtree_used(), 0, "child charge fully released to parent");
    }

    /// Models the rebatch `mem::replace` move: tuples move from an old owned arena
    /// into a new one (both in the same child bump context) WITHOUT being dropped,
    /// exactly as `ExecHashIncreaseNumBatches` re-stages kept tuples. Asserts the
    /// moved elements are not double-dropped and survive to the final reset.
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
            // Rebatch: replace the old arena with a fresh one, MOVE every element.
            let mut newv: PgVec<DropCounter> = PgVec::new_in(mcx);
            for e in old.into_iter() {
                newv.push(e); // move, not clone — no Drop runs
            }
            assert_eq!(drops.get(), 0, "moved tuples are not dropped during rebatch");
            newv.clear();
            drop(newv);
            assert_eq!(drops.get(), 8, "elements drop once when the new arena drops");
        }
        batch.reset();
        assert_eq!(batch.used(), 0);
    }

    /// CHURN MEASUREMENT (the nodeHash batchCxt change). Models the per-batch
    /// hash-join build/reset cycle two ways over identical work, counting the
    /// REAL per-chunk free operations the backing allocator performs.
    ///
    ///   OLD model (per-query-context Aset arena + `Vec::clear`): each tuple's
    ///   `MinimalTuple` (a `PgBox` header + a `PgVec` data buffer) is freed
    ///   INDIVIDUALLY through the Aset allocator at every batch boundary — a real
    ///   per-chunk free (freelist routing) per allocation, per batch.
    ///
    ///   NEW model (bump batchCxt + wholesale `reset`): the same tuples bump-
    ///   allocate into the batch context; their per-object `deallocate` is a
    ///   bump NO-OP, and the whole batch is reclaimed by ONE `reset()`.
    ///
    /// We classify each `deallocate` by backend: an Aset/Malloc free is "real
    /// churn"; a Bump free is a no-op. The headline number is real-churn-frees:
    /// thousands in the old model, ZERO in the new (every byte reclaimed by the
    /// `nbatch` wholesale resets instead).
    #[test]
    fn churn_measurement_per_tuple_free_vs_wholesale_reset() {
        use crate::{alloc_in, PgVec, PgBox};

        // Count real (non-bump) per-chunk frees by snapshotting Aset bookkeeping
        // is awkward; instead we observe the live charge. A simpler, robust proxy:
        // the bump context performs NO per-object free work — assert it via the
        // fact that after dropping the spine (before reset) its live charge is
        // UNCHANGED-by-reclaim only at reset. We count "real frees" structurally:
        // the old model must execute one deallocate per (box+vec) per batch to
        // return memory; the new model returns it all in `nbatch` resets.
        let tuples_per_batch = 500usize;
        let nbatch = 16usize;

        // Helper: build one batch of tuple-like (PgBox header + PgVec data) in
        // `mcx`, returning the owned spine.
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

        // ---- OLD model: Aset query context, per-tuple free at each batch reset.
        // The deallocate probe counts every REAL per-chunk Aset free.
        let _ = crate::churn_probe::take(); // reset the probe
        {
            let qcx = MemoryContext::new("query-old");
            for _ in 0..nbatch {
                let batch = build_batch(qcx.mcx(), tuples_per_batch);
                // ExecHashTableReset (old): each tuple freed INDIVIDUALLY through
                // the Aset (real per-chunk free) as the spine drops.
                drop(batch);
                // Aset returns every freed byte to its freelists; charge is 0.
                assert_eq!(qcx.used(), 0);
            }
        }
        let old_real_frees = crate::churn_probe::take();

        // ---- NEW model: bump batchCxt; per-object frees are no-ops; ONE reset
        // per batch reclaims everything. The probe should count ZERO real frees.
        let mut wholesale_resets = 0u64;
        {
            let qcx = MemoryContext::new("query-new");
            let mut batch_cxt = qcx.new_child_bump("HashBatchContext");
            for _ in 0..nbatch {
                {
                    let batch = build_batch(batch_cxt.mcx(), tuples_per_batch);
                    assert!(batch_cxt.used() > 0, "batch tuples charged while live");
                    // Drop the spine: every PgBox/PgVec deallocate is a BUMP NO-OP
                    // (bump.c never frees a chunk) — no real per-chunk free work.
                    drop(batch);
                }
                // ExecHashTableReset (new): reclaim the whole batch in ONE reset.
                batch_cxt.reset();
                wholesale_resets += 1;
                assert_eq!(batch_cxt.used(), 0, "wholesale reset returns charge to zero");
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

        // The headline guarantee: the new model performs essentially ZERO real
        // per-chunk frees on the hot build/reset path (all reclamation is
        // wholesale), while the old model performs at LEAST one real free per
        // (box+vec) per batch. The global atomic probe can be perturbed by other
        // tests freeing on an Aset concurrently under `cargo test`'s default
        // parallel harness, so the asserts are pollution-tolerant ranges; the
        // exact counts (printed above, e.g. OLD=16000, NEW=0) come from an
        // isolated `--test-threads=1` run.
        let expected_old = (tuples_per_batch * 2 * nbatch) as u64;
        assert!(
            old_real_frees >= expected_old,
            "old model frees every box+vec individually every batch (>= {}, got {})",
            expected_old, old_real_frees,
        );
        assert!(
            new_real_frees < expected_old / 4,
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
        // The context is an accounting child of `root`, so the bundle's bytes
        // stay visible in root's subtree wherever the bundle moves.
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
            // built in an inner scope, moved out — the SetParent shape
            let plan = build_plan(&cache_root, 100).unwrap();
            assert_eq!(plan.with(|p| p.nodes.len()), 100);
            cache.push(plan);
        }
        let plan = &mut cache[0];
        assert_eq!(plan.with(|p| p.nodes.iter().sum::<u64>()), 4950);
        assert!(cache_root.subtree_used() >= 800, "bundle bytes visible from the cache root");

        // mutation through the universal closure; accounting follows
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

    // The leak-projection the executor's `InitPlan` needs (execMain B1): a
    // bundle owns a `PgBox` (here `plan_tree`); a `for<'mcx>`-universal accessor
    // leaks it into an honest `&'mcx` borrow that the closure reads but cannot
    // smuggle out, and the leaked value lives until the bundle's context drops
    // (faithful to C's "plan node freed with its context").
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

        // Mirror QueryDesc::with_plan_and_estate_mut: inside the bundle, leak the
        // owned PgBox into an honest &'mcx, run a for<'mcx> closure over it. The
        // closure returns an owned value so no borrow escapes.
        let seen = bundle.with_mut(|t| {
            let leaked: Option<&u64> = t.plan_tree.take().map(|b| &*crate::leak_in(b));
            // The leak consumed the box; the value still lives in the context.
            leaked.copied()
        });
        assert_eq!(seen, Some(42));

        // The per-context drop reclaims the leaked allocation; accounting tolerates
        // the leaked (never-individually-freed) bytes and returns them to ancestors.
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
}
