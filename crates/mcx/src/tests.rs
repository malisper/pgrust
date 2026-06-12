use super::*;
use core::fmt::Write as _;

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
