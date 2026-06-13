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
